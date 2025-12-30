"""
DiscourseLens Commercial Analyst (v3.0)
Features: Full Theory Injection, Dynamic Taxonomy (Sector X), Dashboard-Ready JSON
Model: Gemini 2.5 Pro (Long Context Required)
"""

import os
import json
import logging
import re
from datetime import datetime, date
from decimal import Decimal
from typing import Dict, Any, List, Optional
import textwrap
import time
import random

import google.generativeai as genai
from supabase import create_client, Client
from dotenv import load_dotenv
from analysis.quant_engine import perform_structure_mapping
from analysis.build_analysis_json import (
    build_and_validate_analysis_json,
    protect_core_fields,
    validate_analysis_json,
)
from analysis.phenomenon_enricher import PhenomenonEnricher
import uuid
from database.store import update_cluster_metadata, save_analysis_result
from analysis.schema import Phenomenon

# --- Configuration ---
load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("CommercialAnalyst")

def _safe_dump(x):
    if x is None:
        return {}
    if isinstance(x, dict):
        return x
    md = getattr(x, "model_dump", None)
    if callable(md):
        try:
            return md(exclude_none=True)
        except Exception:
            pass
    return dict(getattr(x, "__dict__", {}) or {})

def _get_post_id(x: Any):
    if x is None:
        return None
    if isinstance(x, dict):
        return x.get("post_id") or x.get("id")
    return getattr(x, "post_id", None) or getattr(x, "id", None)

def _to_json_safe(value: Any) -> Any:
    """
    Recursively convert values into JSON-serializable types:
    - datetime/date -> ISO 8601 strings
    - Decimal -> float
    - dict/list/tuple -> walk recursively
    Leaves other primitive types unchanged.
    """
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, dict):
        return {k: _to_json_safe(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_to_json_safe(v) for v in value]
    return value


def _call_gemini_with_retry(model, payload_str: str, max_attempts: int = 3):
    for attempt in range(1, max_attempts + 1):
        try:
            return model.generate_content(payload_str)
        except Exception as e:
            msg = str(e)
            transient = any(tok in msg for tok in ["InternalServerError", "500", "Overloaded", "ResourceExhausted", "UNAVAILABLE"])
            if not transient:
                raise
            if attempt == max_attempts:
                raise RuntimeError(f"Gemini transient error after {max_attempts} attempts: {msg}") from e
            sleep_seconds = (2 ** attempt) + random.uniform(0, 0.3)
            logger.warning(
                f"[Analyst] ⚠️ Gemini transient error (Attempt {attempt}/{max_attempts}). Retrying in {sleep_seconds:.1f}s..."
            )
            time.sleep(sleep_seconds)

SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY")

# 建議使用 Pro 模型以處理長文本與複雜推理
MODEL_NAME = "models/gemini-2.5-pro" 

phenomenon_enricher: Optional[PhenomenonEnricher] = None

# --- Helper Functions ---

def load_knowledge_base() -> str:
    """Ingests the 'Brain' of the system."""
    kb = ""
    base_path = "analysis/knowledge_base"
    try:
        with open(f"{base_path}/academic_theory.txt", "r") as f:
            kb += f"\n=== [PART 1: THEORY DEFINITIONS] ===\n{f.read()}\n"
        with open(f"{base_path}/step3_framework.txt", "r") as f:
            kb += f"\n=== [PART 2: ANALYTICAL PROTOCOL] ===\n{f.read()}\n"
    except Exception as e:
        logger.error(f"❌ Failed to load knowledge base: {e}")
        return "ERROR: Knowledge base missing."
    return kb


def get_like_count(comment: Dict[str, Any]) -> int:
    """Return a normalized like_count field from possible sources."""
    try:
        return int(comment.get("like_count", comment.get("likes", 0)) or 0)
    except Exception:
        return 0


def merge_cluster_insights(cluster_summary: Dict[str, Any], cluster_insights: Dict[str, Any]) -> Dict[str, Any]:
    """
    Merge optional name/summary/tactics fields into cluster_summary keyed by cluster_key.
    """
    if not cluster_summary or not isinstance(cluster_summary, dict):
        return cluster_summary or {}
    clusters = cluster_summary.get("clusters")
    if not isinstance(clusters, dict):
        return cluster_summary

    normalized: Dict[str, Dict[str, Any]] = {}
    if isinstance(cluster_insights, dict):
        for k, v in cluster_insights.items():
            if isinstance(v, dict):
                normalized[str(k)] = v
    elif isinstance(cluster_insights, list):
        for item in cluster_insights:
            if not isinstance(item, dict):
                continue
            ck = item.get("cluster_key")
            if ck is None:
                ck = item.get("key")
            if ck is None:
                ck = item.get("id")
            if ck is None:
                continue
            try:
                ck_int = int(ck)
            except Exception:
                continue
            normalized[str(ck_int)] = item

    for cid_key, info in clusters.items():
        if not isinstance(info, dict):
            continue
        insight = normalized.get(str(cid_key))
        if insight is None:
            try:
                insight = normalized.get(str(int(cid_key)))
            except Exception:
                insight = None
        if not isinstance(insight, dict):
            continue
        name = insight.get("name") or insight.get("label")
        summary = insight.get("summary") or insight.get("tactic_summary")
        tactics = insight.get("tactics")
        if isinstance(name, str) and name.strip():
            info["name"] = name.strip()
        if isinstance(summary, str) and summary.strip():
            info["summary"] = summary.strip()
        if tactics:
            info["tactics"] = tactics
    return cluster_summary


def normalize_cluster_insights(raw: Any) -> List[Dict[str, Any]]:
    """
    Accepts dict keyed by str/int OR list[dict].
    Returns list[dict] with cluster_key int and normalized fields.
    """
    normalized_list: List[Dict[str, Any]] = []

    def _norm_tactics(val: Any) -> Optional[List[str]]:
        if val is None:
            return None
        if isinstance(val, str):
            return [val]
        if isinstance(val, (list, tuple)):
            return [str(x) for x in val if x is not None]
        if isinstance(val, dict):
            name = val.get("name") or val.get("label") or val.get("tactic")
            return [str(name)] if name else None
        return None

    iterable = []
    if isinstance(raw, dict):
        iterable = [
            {**v, "cluster_key": k} for k, v in raw.items() if isinstance(v, dict)
        ]
    elif isinstance(raw, list):
        iterable = [item for item in raw if isinstance(item, dict)]

    for item in iterable:
        ck = item.get("cluster_key")
        if ck is None:
            ck = item.get("key")
        if ck is None:
            ck = item.get("id")
        if ck is None:
            ck = item.get("cluster_id")
        try:
            ck_int = int(ck)
        except Exception:
            continue
        label = item.get("label") or item.get("name")
        summary = item.get("summary") or item.get("tactic_summary")
        tactics = _norm_tactics(item.get("tactics"))
        tactic_summary = item.get("tactic_summary")
        normalized_list.append(
            {
                "cluster_key": ck_int,
                "label": label,
                "summary": summary,
                "tactics": tactics,
                "tactic_summary": tactic_summary,
            }
        )
    return normalized_list

def fetch_enriched_post(supabase: Client) -> Dict:
    """
    Fetches the latest post that has passed the Vision Worker stage.
    Criteria: images != null AND images[0].visual_rhetoric != null
    """
    # Fetch recent 50 posts to find a valid candidate
    resp = supabase.table("threads_posts").select("*").order("created_at", desc=True).limit(50).execute()
    
    for row in resp.data:
        imgs = row.get('images', [])
        # Check if the first image has been analyzed by Vision Worker
        if imgs and isinstance(imgs, list) and len(imgs) > 0:
            if imgs[0].get('visual_rhetoric'): 
                return row
    return None

def format_comments_for_context(comments: List[Dict]) -> str:
    """Formats comments to highlight HEAD vs TAIL dynamics for L3 Analysis."""
    if not comments: return "No comments available."
    
    # Sort by Likes (Head)
    sorted_likes = sorted(comments, key=lambda x: get_like_count(x), reverse=True)
    head = sorted_likes[:10]
    
    # Sort by Time/Index (Tail - utilizing ingestion order)
    tail = comments[-10:] if len(comments) > 10 else []
    
    txt = "--- [HEAD COMMENTS (Mainstream Consensus)] ---\n"
    for c in head:
        user = c.get('user', 'anon')
        text = str(c.get('text', '')).replace('\n', ' ')
        likes = get_like_count(c)
        txt += f"- [{user}] ({likes} likes): {text}\n"
        
    txt += "\n--- [TAIL COMMENTS (Recent/Emerging Dissent)] ---\n"
    for c in tail:
        user = c.get('user', 'anon')
        text = str(c.get('text', '')).replace('\n', ' ')
        likes = get_like_count(c)
        txt += f"- [{user}] ({likes} likes): {text}\n"
        
    return txt


def format_comments_for_ai(raw_comments: List[Dict[str, Any]], max_count: int = 40) -> str:
    """
    Prepare a compact, popularity-sorted comment list for the LLM, including cluster ids.
    """
    if not raw_comments:
        return "No public comments found."
    if not isinstance(raw_comments, list):
        return "Comments data format error."

    sorted_comments = sorted(raw_comments, key=get_like_count, reverse=True)
    output = []
    for i, c in enumerate(sorted_comments[:max_count]):
        user = c.get("user", "Unknown")
        text = str(c.get("text", "")).replace("\n", " ")
        likes = get_like_count(c)
        cluster_id = c.get("quant_cluster_id", -1)
        cluster_tag = f" [Cluster {cluster_id}]" if cluster_id != -1 else ""
        output.append(f"[{i+1}]{cluster_tag} User: {user} | Likes: {likes} | Content: {text}")
    return "\n".join(output)


def build_cluster_summary_and_samples(comments_with_quant: List[Dict[str, Any]], max_samples_per_cluster: int = 5) -> Dict[str, Any]:
    clusters: Dict[int, List[Dict[str, Any]]] = {}
    noise: List[Dict[str, Any]] = []
    total_count = len(comments_with_quant)
    for c in comments_with_quant:
        like_count = get_like_count(c)
        c["like_count"] = like_count
        cid_raw = c.get("quant_cluster_id", -1)
        try:
            cid = int(cid_raw) if cid_raw is not None else -1
        except Exception:
            cid = -1
        if cid >= 0:
            clusters.setdefault(cid, []).append(c)
        else:
            noise.append(c)

    cluster_summary: Dict[str, Any] = {}
    for cid, clist in clusters.items():
        sorted_comments = sorted(clist, key=get_like_count, reverse=True)
        samples = [
            {
                **comment,
                "like_count": get_like_count(comment),
                "cluster_key": cid,
            }
            for comment in sorted_comments[:max_samples_per_cluster]
        ]
        pct = (len(clist) / total_count) if total_count else 0
        cluster_summary[str(cid)] = {
            "cluster_id": cid,
            "cluster_key": cid,
            "count": len(clist),
            "pct": round(pct, 4),
            "pct_label": f"{round(pct * 100, 1)}%" if pct else "0%",
            "samples": samples,
        }

    noise_count = len(noise)
    noise_pct = (noise_count / total_count) if total_count else 0

    return {
        "clusters": cluster_summary,
        "noise": {
            "cluster_id": -1,
            "count": noise_count,
            "pct": round(noise_pct, 4),
            "pct_label": f"{round(noise_pct * 100, 1)}%" if noise_pct else "0%",
            "samples": [{**comment, "like_count": get_like_count(comment)} for comment in noise[:max_samples_per_cluster]],
        },
    }

def format_visuals(images: List[Dict]) -> str:
    """Formats Vision Worker output for the Analyst."""
    if not images: return "No visuals."
    txt = ""
    for i, img in enumerate(images):
        txt += f"[Image {i+1}]\n"
        txt += f"  - Scene Label: {img.get('scene_label', 'N/A')}\n"
        txt += f"  - Visual Rhetoric: {img.get('visual_rhetoric', 'N/A')}\n"
        txt += f"  - OCR Text: {img.get('full_text', 'N/A')}\n"
    return txt

def extract_json_block(text: str) -> Dict:
    """Robustly extracts JSON from Markdown text."""
    try:
        match = re.search(r"```json\n(.*?)\n```", text, re.DOTALL)
        if match:
            return json.loads(match.group(1))
        # Fallback: try finding just the brace structure
        match = re.search(r"\{.*\}", text, re.DOTALL)
        if match:
            return json.loads(match.group(0))
    except Exception as e:
        logger.warning(f"JSON Extraction failed: {e}")
    return {}


def extract_block_between(text: str, start_pattern: str, end_patterns: List[str]) -> str:
    """
    Find first occurrence of start_pattern (regex), and grab everything until
    the first occurrence of any end_pattern or end of string.
    Return a stripped string, or "" if not found.
    """
    try:
        start_match = re.search(start_pattern, text, re.DOTALL)
        if not start_match:
            return ""
        start_idx = start_match.start()

        end_idx = len(text)
        for ep in end_patterns:
            m = re.search(ep, text[start_idx + 1 :], re.DOTALL)
            if m:
                candidate_end = start_idx + 1 + m.start()
                end_idx = min(end_idx, candidate_end)

        block = text[start_idx:end_idx].strip()
        block = re.sub(r"\n{3,}", "\n\n", block)
        if len(block) > 1200:
            block = block[:1200].rstrip() + "..."
        return block
    except Exception:
        return ""


def extract_l1_summary(full_markdown: str) -> str:
    """
    Extract the L1 section (Illocutionary Act) from the markdown.
    """
    return extract_block_between(
        full_markdown,
        r"(?i)(?:^|\n|#|\*+)\s*L1[:\s-].*?",
        [
            r"(?i)(?:^|\n|#|\*+)\s*L2",
            r"(?i)SECTION",
            r"---\n\n####",
        ],
    )


def extract_l2_summary(full_markdown: str) -> str:
    """
    Extract the L2 section (Critical Strategy Analysis) from the markdown.
    """
    return extract_block_between(
        full_markdown,
        r"(?i)(?:^|\n|#|\*+)\s*L2[:\s-].*?",
        [
            r"(?i)(?:^|\n|#|\*+)\s*L3",
            r"(?i)SECTION",
            r"---\n\n####",
        ],
    )


def extract_l3_summary(full_markdown: str) -> str:
    """
    Extract the L3 Battlefield / Faction Analysis section.
    """
    return extract_block_between(
        full_markdown,
        r"(?i)(?:^|\n|#|\*+)\s*L3[:\s-].*?",
        [
            r"(?i)SECTION",
            r"---\n\n####",
            r"#### \*\*",
        ],
    )


def infer_tone_from_primary(primary: str) -> Dict[str, float]:
    """
    Very soft fallback when Tone_Fingerprint is missing.
    Only looks at Quantifiable_Tags.Primary_Emotion, which is a short label
    controlled by our schema (e.g. 'Weary Pride', 'Cynical Anger').
    """
    base = {"cynicism": 0.0, "anger": 0.0, "hope": 0.0, "despair": 0.0}
    if not primary:
        return base

    p = primary.lower().strip()

    if "cynic" in p or "weary" in p:
        base["cynicism"] = 0.7
    if "anger" in p or "indignation" in p:
        base["anger"] = 0.7
    if "hope" in p:
        base["hope"] = 0.7
    if "despair" in p or "hopeless" in p:
        base["despair"] = 0.7

    if all(v == 0.0 for v in base.values()) and primary:
        base["cynicism"] = 0.5
        base["hope"] = 0.5

    return base

# --- The Brain ---

def generate_commercial_report(post_data: Dict, supabase: Client):
    global phenomenon_enricher
    if phenomenon_enricher is None:
        phenomenon_enricher = PhenomenonEnricher(supabase, enabled=True, run_inline=True)

    def _failure_dict(post_id: str | None, version: str, build_id: str, reason: str, missing: list[str], error_type: str, error_detail: str, raw_preview: str = ""):
        return {
            "post_id": post_id or "",
            "analysis_json": None,
            "analysis_is_valid": False,
            "analysis_invalid_reason": reason,
            "analysis_missing_keys": missing,
            "analysis_version": version,
            "analysis_build_id": build_id,
            "error_type": error_type,
            "error_detail": error_detail,
            "raw_llm_preview": raw_preview[:1200] if raw_preview else "",
        }

    knowledge_base = load_knowledge_base()

    like_count = int(post_data.get("like_count") or 0)
    is_high_impact = like_count > 500
    reply_count = int(post_data.get("reply_count") or 0)
    view_count = int(
        post_data.get("view_count")
        or post_data.get("metrics", {}).get("views", 0)
        or 0
    )
    raw_comments = post_data.get("raw_comments") or post_data.get("comments") or []
    post_data["raw_comments"] = raw_comments

    # --- THE COMMERCIAL PROMPT (ENHANCED HUNTER VERSION) ---
    logger.info("Running L0.5 Structure Mapper...")
    post_row_id = _get_post_id(post_data)
    quant_result = perform_structure_mapping(post_data.get("comments", []), post_id=post_row_id)
    quant_summary = {}
    cluster_samples = {}
    if quant_result:
        post_data["comments"] = quant_result["node_data"]
        stats = quant_result.get("cluster_stats", {})
        echo_count = quant_result.get("high_sim_pairs", 0)
        dominant_cluster = max(stats, key=stats.get) if stats else "None"
        quant_summary = {
            "cluster_stats": stats,
            "high_sim_pairs": echo_count,
            "math_homogeneity": quant_result.get("math_homogeneity"),
            "clusters_ref": quant_result.get("clusters_ref"),
            "persistence": quant_result.get("persistence"),
        }
        cluster_samples = build_cluster_summary_and_samples(post_data.get("comments", []))
        quant_context = f"""
[L0.5 STRUCTURAL SIGNALS]
- Semantic Clusters Detected: {len(stats)} (Heuristic grouping)
- Cluster Sizes: {stats} (Cluster {dominant_cluster} is dominant)
- Math_Homogeneity_Reference: {quant_result.get('math_homogeneity', 'N/A')} (Use this as a baseline for your Homogeneity Score)
- High-Similarity Echo Pairs: {echo_count} (Comments with >94% cosine similarity across different users)
- Note: Comments are tagged with 'quant_cluster_id' and 'is_template_like'.
"""
    else:
        quant_context = "[L0.5 SIGNALS]: Insufficient data for structural mapping."

    # Prepare Data Dossier (after quant so comments carry clusters)
    comments_for_llm = format_comments_for_ai(post_data.get("comments", []))
    vox_populi_text = comments_for_llm

    cluster_payload_for_llm: List[Dict[str, Any]] = []
    if cluster_samples:
        clusters = cluster_samples.get("clusters") or {}
        noise = cluster_samples.get("noise") or {}
        if clusters:
            cluster_lines: List[str] = []
            clusters_sorted = sorted(
                clusters.items(),
                key=lambda kv: kv[1].get("count", 0),
                reverse=True,
            )
            for cid, info in clusters_sorted:
                cid_label = info.get("cluster_id", cid)
                count = info.get("count", 0)
                pct = info.get("pct", 0) or 0
                pct_label = info.get("pct_label") or f"{round(pct * 100, 1)}%"
                display_name = info.get("name") or f"Cluster {cid_label}"
                summary_text = info.get("summary") if isinstance(info, dict) else None
                cluster_lines.append(f"=== CLUSTER {cid_label} | {display_name} | Size: {count}, {pct_label} ===")
                cluster_lines.append(f"Summary: {summary_text.strip() if isinstance(summary_text, str) and summary_text.strip() else '暫無摘要。'}")
                samples = info.get("samples") or []
                if not samples:
                    cluster_lines.append("(No representative comments captured)")
                for idx, c in enumerate(samples, start=1):
                    user = c.get("user", "Unknown")
                    like_val = get_like_count(c)
                    text = str(c.get("text", "")).replace("\n", " ").strip()
                    cluster_lines.append(f"[C{cid_label}-{idx}] {user} ❤️ {like_val} | {text}")
                cluster_lines.append("")
                cluster_payload_for_llm.append(
                    {
                        "cluster_key": cid_label,
                        "size": count,
                        "keywords": info.get("keywords"),
                        "top_comment_ids": info.get("top_comment_ids"),
                        "samples": [
                            {
                                "comment_id": s.get("id") or s.get("comment_id"),
                                "cluster_key": cid_label,
                                "text": s.get("text"),
                                "likes": s.get("like_count") or s.get("likes"),
                            }
                            for s in samples
                        ],
                    }
                )
            if noise and noise.get("count", 0) > 0:
                noise_pct = noise.get("pct", 0) or 0
                noise_pct_label = noise.get("pct_label") or f"{round(noise_pct * 100, 1)}%"
                cluster_lines.append(f"=== NOISE / UNCLASSIFIED (Size: {noise.get('count', 0)}, {noise_pct_label}) ===")
                for idx, c in enumerate(noise.get("samples") or [], start=1):
                    like_val = get_like_count(c)
                    text = str(c.get("text", "")).replace("\n", " ").strip()
                    user = c.get("user", "Unknown")
                    cluster_lines.append(f"[Noise-{idx}] {user} ❤️ {like_val} | {text}")
            vox_populi_text = "\n".join(cluster_lines).strip()
    cluster_payload_json = json.dumps(cluster_payload_for_llm, ensure_ascii=False)

    dossier = f"""
    POST ID: {post_data['id']}
    AUTHOR: {post_data.get('author')}
    METRICS: Likes {like_count}, Replies {post_data.get('reply_count')}
    HIGH_IMPACT: {is_high_impact}
    POST TEXT: "{post_data.get('post_text')}"
    REAL_METRICS: Likes={like_count}, Replies={reply_count}, Views={view_count}
    
    [VISUAL EVIDENCE (from Vision Worker)]
    {format_visuals(post_data.get('images', []))}
    
    [COLLECTIVE DYNAMICS (Comments)]
    {format_comments_for_context(post_data.get('comments', []))}
    """

    user_content = f"""
SOURCE MATERIAL FOR ANALYSIS:

=== PART 1: THE ARTIFACT (Main Post) ===
**Author:** {post_data.get('author')}
**Post Text:** "{post_data.get('post_text')}"
**Visual Context:** The post contains {len(post_data.get('images', []))} images. (Refer to visual analysis if available).

=== PART 2: THE VOX POPULI (Public Reaction) ===
**Context:** Below are the top {len(raw_comments)} comments from the thread, sorted by engagement/likes.
**Instruction for AI:** Treat this section as the empirical evidence for "L3 Battlefield Analysis". Look for agreement, conflict, mockery, or expansion of the theme.

{vox_populi_text} 
(End of comments)

=== PART 3: STRUCTURED CLUSTERS (for grounding your Cluster_Insights) ===
Use this JSON to map cluster_key → tactics/label/summary. Always emit Cluster_Insights as a list with cluster_key.
{cluster_payload_json}
"""

    system_prompt = f"""
    You are 'DiscourseLens', an automated Sociological Analyst.
    Your goal is to produce a commercial-grade intelligence report.

    [KNOWLEDGE BASE (THEORY & RULES)]
    {knowledge_base}
    --------------------------------------------------
    [TARGET DATA DOSSIER]
    {dossier}
    --------------------------------------------------
    Structural Signals (L0.5): {quant_context}
    === PUBLIC COMMENTS (VOX POPULI) ===
    The following are real user comments from the post, sorted by popularity.
    Use these to analyze "Collective Dynamics", "Homogeneity Score", and identify any "Spiral of Silence" or conflict.
    {vox_populi_text}

    [PROTOCOL: NOVELTY DETECTION] (!!! CRITICAL !!!)
    1. **Avoid Lazy Categorization**: Do not default to generic tags. If a post feels different, describe WHY.
    2. **Detect Sub-Variants**: Even if a post fits Sector A/B/C/D, you must identify its specific *flavor*. 
       - Example: Instead of just "Sector D (Normalcy)", distinguish between "Compensatory Consumption" vs "Routine Check-in".
    3. **Activate Sector X**: If you see a NEW trend (e.g., a specific new scam, a viral challenge, a new slang), you MUST classify it as [SECTOR X] and propose a name.
    4. **Assess Author Influence**: Classify as Low / Medium / High_KOL based on engagement and tone (HIGH_IMPACT flag indicates likely KOL).

    INSTRUCTIONS:
    1. **De-contextualize**: Analyze the content AS IS. Do not hallucinate external political events unless explicitly mentioned in the text.
    2. **Apply Theory**: You MUST explicitly cite concepts from Part 1 & 2 (e.g., "This exhibits Ritualistic Reciprocity").
    3. **Detect Sector**: Classify into A, B, C, D based on the Knowledge Base. If it fits none, use [SECTOR X].
    4. **Analyze L3** with the VOX POPULI section: use public comments as evidence.
    5. Use Structural Signals as evidence only; refer to clusters as "Cluster 0/1/2" with size/tone. If echo pairs are high, describe as template-like/echo effect; do not claim bots unless other evidence exists.
    6. Perform cluster-level reasoning: identify ideological core per major cluster, compare majority vs minority clusters (volume vs engagement), and describe narrative collisions. Use math_homogeneity and cluster distribution to judge unity vs fragmentation.

    ### UPDATED INSTRUCTIONS FOR L3 & QUANTITATIVE METRICS:

    **On L3 Battlefield Analysis (集體動態戰場分析)**
    Do NOT assume the author is isolated. You must analyze the provided "VOX POPULI" (Comments) section:
    1.  Dominant Sentiment: Is the comment section an "Echo Chamber" (reinforcing the author) or a "Battleground" (challenging the author)?
    2.  Top Comment Check: Compare the most liked comment against the original post. Does the top comment "Ratio" the author (have more likes/support)? Or does it amplify the author's point?
    3.  Specific Dynamics: Look for:
        - The Spiral of Silence: Are dissenting views missing or being dog-piled?
        - Topic Hijacking: Are commenters changing the subject (e.g., from "Fire Accident" to "Government Incompetence")?

    [MANDATORY: FACTION ANALYSIS PROTOCOL]
    You must treat the "VOX POPULI" section as a map of factions, not a list of random users.
    - Identify Factions: Explicitly name Cluster 0 and Cluster 1 based on their ideology (e.g., "Cluster 0: The Technocratic Critics" vs "Cluster 1: The Cynical Observers").
    - Power Dynamics: Compare the Population (Size) vs. Engagement (Likes). Does a minority cluster control the most liked comments?
    - Narrative Collision: Describe exactly where the philosophical conflict lies between the clusters.
    - Use Math: Reference the Math_Homogeneity_Reference score to validate if the discourse is unified or fragmented.

    [MANDATORY: FACTION NAMING RULE — HONG KONG EDITION]
    When generating Cluster_Insights, you must name each cluster using clear, neutral Hong Kong written Chinese. The style should resemble Hong Kong newspaper commentary or public policy reports, not social media slang.
    - Avoid PRC/Taiwan internet slang (e.g., 吃瓜、杠精、反串、側翼等用語)
    - Avoid overly academic jargon (e.g., 象徵性抵抗、技術官僚式戲仿、後結構敘事等學術術語)
    - Names should be short (around 3–5 Chinese characters) and descriptive, expressing the cluster’s sentiment, stance or main concern.
    - Tone reference: 明報評論、香港電台時事節目、公共政策研究報告的用語風格。
    Examples of good names (for inspiration): 「本土情懷者」「質疑科技的一群」「對制度感到失望的聲音」「以幽默方式回應的用戶」「關注生活經驗的觀眾」.
    Academic theory (e.g., 技術官僚式戲仿) should stay in the L2/L3 narrative; Cluster_Insights.name must be everyday written labels that readers instantly grasp.

    [FACTION NAMING RULE — HONG KONG WRITTEN CHINESE]
    Cluster names MUST:
    - Be neutral, professional written Chinese
    - Avoid Mainland or Taiwanese slang (e.g., "吃瓜群", "酸民", "XX派")
    - Avoid academic jargon (e.g., "符號抵抗實踐者")
    - Be short (max 5 Chinese characters)
    - Examples of acceptable naming tone: 「政策質疑者」, 「制度關注者」, 「冷感旁觀者」, 「民生抱怨群」
    Cluster summaries should be one professional sentence.

    - When assessing Author Influence, you MUST use the provided REAL_METRICS (Likes/Replies/Views). Do NOT invent or zero-out these numbers.
    - Follower Count is unavailable; explicitly note that the assessment is based solely on post engagement signals.
    When evaluating Author Influence, you MUST cite the following real metrics verbatim:
    - 讚好數：{like_count}
    - 回應數：{reply_count}
    - 觀看次數：{view_count}
    You MUST NOT hallucinate other numbers.
    Follower count is NOT provided; explicitly state: 『本系統未包含作者之追蹤人數，以下評估僅根據可見互動數據。』

    **On Quantifiable Tags (Sociological Metrics)**
    - Homogeneity_Score (Float 0.0 - 1.0):
        * Theoretical Basis: Based on Sunstein's "Echo Chamber" and Noelle-Neumann's "Spiral of Silence".
        * Definition: Measures the diversity of opinion and the presence of dissenting voices.
        * Scale:
            * 0.8 - 1.0 (Echo Chamber): High consensus. Dissenting views are absent or mocked.
            * 0.4 - 0.6 (Polarization): A divided battlefield. Distinct camps are fighting.
            * 0.0 - 0.3 (Fragmentation): Chaotic/Diverse opinions. No dominant narrative.
            * Reference: Check 'Math_Homogeneity_Reference' in L0.5 signals. If math says 0.9, do not rate it 0.2 unless you detect heavy sarcasm.

    - Civil_Score (Int 0 - 10):
        * Theoretical Basis: Based on Papacharissi's "Online Incivility" and Discourse Ethics.
        * Definition: Measures deliberative quality, distinguishing "impoliteness" from "threats to democracy".
        * Scale:
            * 8 - 10 (Deliberative): Rational exchange. Disagreement focuses on arguments.
            * 4 - 7 (Heated): Emotional, sarcastic, but maintains communicative intent.
            * 0 - 3 (Toxic): Ad hominem attacks, dehumanization, silencing others.

    OUTPUT FORMAT:
    Language: Traditional Chinese (Taiwan/Hong Kong usage).
    You must output a single response containing two sections:
    
    SECTION 1: The Report (Markdown)
    - Executive Summary (執行摘要)
    - **Phenomenon Spotlight (現象焦點)**: Briefly explain the unique sub-variant or flavor of this post (moved up).
    - L1 & L2 Deep Dive (Tone & Strategy)
    - L3 Battlefield / Faction Analysis (Dynamics)
    - Strategic Implication
    - Author Influence assessment (Low / Medium / High_KOL) grounded in REAL_METRICS; explicitly state follower count is unavailable.

    SECTION 2: The Data Block (JSON)
    *Must be wrapped in ```json codes*
    {{
      "Analysis_Meta": {{
        "Post_ID": "{post_data['id']}",
        "Timestamp": "{datetime.now().isoformat()}",
        "High_Impact": {str(is_high_impact).lower()}
      }},
      "Quantifiable_Tags": {{
        "Sector_ID": "Enum: [Sector_A, Sector_B, Sector_C, Sector_D, Sector_X]",
        "Primary_Emotion": "String (e.g., Joy, Grief, Cynicism)",
        "Strategy_Code": "String (from Part 2 list)",
        "Civil_Score": "Integer (1-10)",
        "Homogeneity_Score": "Float (0.0-1.0)",
        "Author_Influence": "Enum: [Low, Medium, High_KOL]"
      }},
      "Post_Stats": {{
        "Likes": {like_count},
        "Replies": {reply_count},
        "Views": {view_count}
      }},
      "Cluster_Insights": [
        {{
          "cluster_key": 0,
          "label": "String (短標籤，約 3–5 字)",
          "summary": "String (一句話描述該群體的立場或關注點)",
          "tactics": ["String", "String"],
          "tactic_summary": "Optional: 1-2 sentence rationale"
        }}
      ],
      "Discovery_Channel": {{
        "Sub_Variant_Name": "String (e.g., 'Revenge_Consumption', 'Feng_Shui_Blame') - REQUIRED",
        "Is_New_Phenomenon": Boolean,
        "Phenomenon_Description": "String (Specific nuance observed)"
      }}
    }}
    """

    logger.info(f"🧠 Analyst thinking on Post {post_data['id']} ({MODEL_NAME})...")
    
    genai.configure(api_key=GEMINI_API_KEY)
    model = genai.GenerativeModel(MODEL_NAME)
    
    try:
        # drop heavy fields to avoid oversized prompt
        for k in ["archive_html", "archive_dom_json", "archive_captured_at", "archive_build_id"]:
            post_data.pop(k, None)
        payload_str = system_prompt + "\n\n" + user_content
        logger.info(f"[Analyst] LLM payload approx chars={len(payload_str)}")

        response = _call_gemini_with_retry(model, payload_str)
        full_text = response.text
        raw_llm_preview = (full_text or "")[:1200]
        
        # 1. Extract JSON
        json_data = extract_json_block(full_text)

        # 2. Save to Local File
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        os.makedirs("reports", exist_ok=True)
        with open(f"reports/Analysis_{post_data['id']}_{ts}.md", "w", encoding="utf-8") as f:
            f.write(full_text)

        # 3. Save to Supabase (via deterministic builder)
        ai_tags = {}
        cluster_insights = {}
        cluster_insights_list: List[Dict[str, Any]] = []
        if json_data:
            tags = json_data.get('Quantifiable_Tags', {}) or {}
            discovery = json_data.get('Discovery_Channel', {}) or {}
            raw_insights = json_data.get('Cluster_Insights', {}) or {}
            cluster_insights_list = normalize_cluster_insights(raw_insights)
            # map for backward-compatible merge
            cluster_insights = {str(item["cluster_key"]): item for item in cluster_insights_list}
            ai_tags = {
                **tags,
                "Sub_Variant": discovery.get("Sub_Variant_Name"),
                "Phenomenon_Desc": discovery.get("Phenomenon_Description")
            }

        if cluster_samples:
            cluster_samples = merge_cluster_insights(cluster_samples, cluster_insights)

        # First update: ai_tags + full_report
        try:
            first_update = _to_json_safe({"ai_tags": ai_tags, "full_report": full_text})
            supabase.table("threads_posts").update(first_update).eq("id", post_data.get("id")).execute()
            logger.info(f"[Analyst] ✅ Updated ai_tags/full_report for post {post_data.get('id')}")
        except Exception as e:
            logger.error(f"[Analyst] ❌ Failed to update ai_tags/full_report for post {post_data.get('id')}")
            logger.exception(e)

        # Refresh crawler row to ensure ground-truth metrics/text
        try:
            post_res = (
                supabase.table("threads_posts")
                .select("*")
                .eq("id", post_data.get("id"))
                .single()
                .execute()
            )
            if post_res and hasattr(post_res, "data") and post_res.data:
                post_data = post_res.data
        except Exception as fetch_err:
            logger.warning("Failed to refresh post_data from Supabase; using provided post_data", extra={"error": str(fetch_err)})

        raw_imgs = post_data.get("images") or []
        logger.info(f"[Analyst] Raw crawler images: {len(raw_imgs)}")

        try:
            analysis_v4 = build_and_validate_analysis_json(
                post_data=post_data,
                llm_data=json_data or {},
                cluster_data=cluster_samples or {},
                full_report=full_text,
            )
        except Exception as e:
            logger.error("[Analyst] ❌ AnalysisV4 validation failed")
            logger.exception(e)
            return None

        # Enforce crawler-first fields
        analysis_v4 = protect_core_fields(post_data, analysis_v4)

        # Phenomenon is registry-owned; mark pending to avoid LLM free-form drift.
        try:
            phen_dict = _safe_dump(analysis_v4.phenomenon)
            if not phen_dict.get("id") and not phen_dict.get("status"):
                phen_dict["status"] = "pending"
            phen_model = Phenomenon(**phen_dict) if isinstance(phen_dict, dict) else phen_dict
            analysis_v4 = analysis_v4.copy(update={"phenomenon": phen_model})
        except Exception:
            logger.exception("[Analyst] Failed to tag phenomenon pending")

        # Validate completeness
        is_valid, invalid_reason, missing_keys = validate_analysis_json(analysis_v4)
        analysis_version = "v4"
        analysis_build_id = str(uuid.uuid4())

        # Optional fallback: if L1/L2/L3 missing, use regex extraction as last resort
        if analysis_v4.narrative_stack and not any(
            [analysis_v4.narrative_stack.l1, analysis_v4.narrative_stack.l2, analysis_v4.narrative_stack.l3]
        ):
            analysis_v4 = analysis_v4.copy(
                update={
                    "narrative_stack": {
                        "l1": extract_l1_summary(full_text),
                        "l2": extract_l2_summary(full_text),
                        "l3": extract_l3_summary(full_text),
                    }
                }
            )

        analysis_payload = _safe_dump(analysis_v4)
        analysis_payload["analysis_version"] = analysis_version
        analysis_payload["analysis_build_id"] = analysis_build_id
        if missing_keys:
            analysis_payload["missing_keys"] = missing_keys

        logger.info(
            "[Analyst] analysis payload snapshot",
            extra={
                "type": str(type(analysis_payload)),
                "keys": list(analysis_payload.keys()),
                "post_id": post_data.get("id"),
                "is_valid": is_valid,
            },
        )

        print(
            "[ANALYST] Built AnalysisV4 for post",
            post_data.get("id"),
            "segments=",
            len(analysis_payload.get("segments", [])),
        )

        update_data = {
            "ai_tags": ai_tags,
            "full_report": full_text,
            "raw_comments": post_data.get("comments", []),
            "cluster_summary": cluster_samples if cluster_samples else {},
            "raw_json": json_data or {},
            "analysis_version": analysis_version,
            "analysis_build_id": analysis_build_id,
        }
        if quant_summary:
            update_data["quant_summary"] = quant_summary

        post_id = str(_get_post_id(post_data) or "")
        logger.info(
            "[Analyst] write payload snapshot",
            extra={
                "payload_type": str(type(update_data)),
                "keys": list(update_data.keys()),
                "post_id": post_id,
                "is_valid": is_valid,
            },
        )
        try:
            if analysis_payload is not None:
                save_analysis_result(post_id, analysis_payload)
            json_safe_payload = _to_json_safe(update_data)
            resp = supabase.table("threads_posts").update(json_safe_payload).eq("id", post_id).execute()
            logger.info(f"✅ Saved to DB: Sector={ai_tags.get('Sector_ID') if ai_tags else 'N/A'}")
            logger.info(f"💾 Supabase update: comments={len(post_data.get('comments', []))}, quant_summary={'present' if quant_summary else 'none'}")
            print(
                "[ANALYST] Supabase update result for post",
                post_id,
                "error=",
                getattr(resp, "error", None),
            )
            # Kick off async phenomenon Match-or-Mint (non-blocking)
            try:
                if phenomenon_enricher:
                    phenomenon_enricher.submit(
                        post_row=post_data,
                        analysis_payload=analysis_payload,
                        cluster_summary=cluster_samples or {},
                        comments=post_data.get("comments", []),
                    )
            except Exception:
                logger.exception("[Analyst] Phenomenon enrichment submission failed")
        except Exception as db_err:
            logger.error(f"[Analyst] ❌ Failed to update analysis_json/raw_json for post {post_id}")
            logger.exception(db_err)
            return None

        # Non-blocking cluster metadata writeback (Layer 0.5 registry)
        try:
            if analysis_payload:
                updates: List[Dict[str, Any]] = []

                # Prefer explicit Cluster_Insights with cluster_key
                if cluster_insights_list:
                    for item in cluster_insights_list:
                        ck = item.get("cluster_key")
                        if ck is None:
                            continue
                        try:
                            ck_int = int(ck)
                        except Exception:
                            continue
                        entry: Dict[str, Any] = {"cluster_key": ck_int}
                        if item.get("label"):
                            entry["label"] = item.get("label")
                        if item.get("summary"):
                            entry["summary"] = item.get("summary")
                        if item.get("tactics") is not None:
                            entry["tactics"] = item.get("tactics")
                        if item.get("tactic_summary"):
                            entry["tactic_summary"] = item.get("tactic_summary")
                        # keep only if any meaningful field
                        if any(k in entry for k in ("label", "summary", "tactics", "tactic_summary")):
                            updates.append(entry)

                # Fallback: derive from segments if no explicit insights
                if not updates:
                    segments = analysis_payload.get("segments") or []

                    def _cluster_key_from_segment(seg: Dict[str, Any]) -> Optional[int]:
                        for field in ("cluster_key", "cluster_id", "key"):
                            val = seg.get(field)
                            if val is not None:
                                try:
                                    return int(val)
                                except Exception:
                                    pass
                        label = seg.get("label")
                        if isinstance(label, str):
                            m = re.search(r"cluster\s*(\d+)", label, flags=re.IGNORECASE)
                            if m:
                                try:
                                    return int(m.group(1))
                                except Exception:
                                    return None
                        return None

                    def _norm_tactics(val: Any) -> Optional[List[str]]:
                        if val is None:
                            return None
                        if isinstance(val, str):
                            return [val]
                        if isinstance(val, (list, tuple)):
                            return [str(x) for x in val if x is not None]
                        if isinstance(val, dict):
                            name = val.get("name") or val.get("label") or val.get("tactic")
                            return [str(name)] if name else None
                        return None

                    for seg in segments:
                        if not isinstance(seg, dict):
                            continue
                        ck = _cluster_key_from_segment(seg)
                        if ck is None:
                            continue
                        entry = {"cluster_key": ck}
                        lbl = seg.get("label")
                        if lbl:
                            entry["label"] = lbl
                        tactics = seg.get("tactics") or seg.get("tactic") or seg.get("labels")
                        if tactics is not None:
                            entry["tactics"] = _norm_tactics(tactics)
                        tactic_summary = seg.get("summary") or seg.get("rationale") or seg.get("description")
                        if tactic_summary:
                            entry["tactic_summary"] = tactic_summary
                        summary = seg.get("summary")
                        if summary:
                            entry["summary"] = summary
                        if any(k in entry for k in ("label", "summary", "tactics", "tactic_summary")):
                            updates.append(entry)

                if updates:
                    ok, updated_count = update_cluster_metadata(int(post_id), updates)
                    if not ok:
                        logger.warning(f"[Analyst] Cluster metadata writeback failed for post {post_id}")
                    else:
                        logger.info(
                            f"[Analyst] Cluster metadata writeback post={post_id} attempted={len(updates)} updated={updated_count}"
                        )
                else:
                    logger.info("[Analyst] Cluster writeback skipped: no cluster_insights and no segment mapping")
        except Exception:
            logger.warning(f"[Analyst] Cluster metadata writeback encountered an error for post {post_id}", exc_info=True)

        return {
            "ai_tags": ai_tags,
            "full_report": full_text,
            "quant_summary": quant_summary,
            "comments": post_data.get("comments", []),
            "cluster_summary": cluster_samples,
            "analysis_is_valid": is_valid,
            "analysis_version": analysis_version,
            "analysis_build_id": analysis_build_id,
            "analysis_invalid_reason": invalid_reason,
            "analysis_missing_keys": missing_keys,
            "analysis_json": analysis_payload,
            "post_id": post_id,
        }

    except Exception as e:
        logger.error(f"❌ Analyst Failed: {e}")
        post_id = str(_get_post_id(post_data) or "")
        version = "v4"
        build_id = str(uuid.uuid4())
        return _failure_dict(
            post_id=post_id,
            version=version,
            build_id=build_id,
            reason="analysis_exception",
            missing=[],
            error_type=type(e).__name__,
            error_detail=str(e),
            raw_preview="",
        )

# --- Main Execution ---

if __name__ == "__main__":
    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
    
    print("🔍 Searching for specific posts to analyze...")
    target_post = fetch_enriched_post(supabase)
            
    if target_post:
        print(f"🎯 Target Acquired: Post {target_post['id']} by {target_post['author']}")
        generate_commercial_report(target_post, supabase)
    else:
        print("⚠️ No fully processed posts found.")
        print("💡 Action: Run 'python analysis/vision_worker.py' first to generate L2 data.")
