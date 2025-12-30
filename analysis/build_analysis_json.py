import logging
import re
from typing import Any, Dict, List, Optional

from pydantic import ValidationError

from .schema import (
    AnalysisV4,
    BattlefieldCompat,
    DangerBlock,
    Metrics,
    NarrativeStack,
    Phenomenon,
    PostBlock,
    Segment,
    SegmentSample,
    SummaryCompat,
    ToneProfile,
)

logger = logging.getLogger(__name__)


def safe_dump(x: Any) -> Dict[str, Any]:
    """Return a dict from pydantic model OR dict OR object; never raises."""
    if x is None:
        return {}
    if isinstance(x, dict):
        return x
    md = getattr(x, "model_dump", None)
    if callable(md):
        try:
            return md()
        except Exception:
            pass
    return dict(getattr(x, "__dict__", {}) or {})


def safe_get(x: Any, key: str, default: Any = None) -> Any:
    return safe_dump(x).get(key, default)


def _coerce_int(val: Any) -> Optional[int]:
    if val is None:
        return None
    try:
        iv = int(val)
        if iv < 0:
            return 0
        return iv
    except Exception:
        return None


def _clamp_fraction(val: Any) -> Optional[float]:
    if val is None:
        return None
    try:
        f = float(val)
    except Exception:
        return None
    if f > 1.0:
        f = f / 100.0 if f <= 100.0 else f
    if f < 0:
        f = 0.0
    if f > 1.0:
        f = 1.0
    return f


def _build_metrics(post_data: Dict[str, Any], llm_data: Dict[str, Any]) -> Metrics:
    llm_stats = llm_data.get("Post_Stats") if isinstance(llm_data, dict) else {}
    likes = _coerce_int(post_data.get("like_count") or post_data.get("likes"))
    views = _coerce_int(post_data.get("view_count") or post_data.get("impression_count"))
    replies = _coerce_int(post_data.get("reply_count") or post_data.get("comment_count"))

    # Fallback to LLM stats ONLY when crawler fields are missing
    if likes is None and isinstance(llm_stats, dict):
        likes = _coerce_int(llm_stats.get("Likes"))
    if views is None and isinstance(llm_stats, dict):
        views = _coerce_int(llm_stats.get("Views"))
    if replies is None and isinstance(llm_stats, dict):
        replies = _coerce_int(llm_stats.get("Replies"))

    if isinstance(llm_stats, dict) and likes is not None:
        llm_likes = _coerce_int(llm_stats.get("Likes"))
        if llm_likes is not None and abs(llm_likes - likes) > max(100, likes * 0.5):
            logger.warning(
                "LLM likes differ from crawler likes; keeping crawler value",
                extra={"crawler_likes": likes, "llm_likes": llm_likes, "post_id": post_data.get("id")},
            )

    metrics = Metrics(likes=likes or 0, views=views, replies=replies)
    crawler_like = _coerce_int(post_data.get("like_count") or post_data.get("likes"))
    if crawler_like and metrics.likes == 0:
        logger.warning(
            "Crawler likes present but metrics ended up zero",
            extra={"post_id": post_data.get("id"), "crawler_like": crawler_like},
        )
    return metrics


def _build_post_block(post_data: Dict[str, Any], metrics: Metrics) -> PostBlock:
    post_id = post_data.get("id") or post_data.get("post_id") or "unknown"
    text = (
        post_data.get("post_text")
        or post_data.get("text")
        or post_data.get("content")
        or post_data.get("caption")
    )

    raw_images = post_data.get("images") or []
    sanitized_images: List[str] = []
    for img in raw_images:
        if isinstance(img, str):
            sanitized_images.append(img)
        elif isinstance(img, dict):
            src = img.get("src") or img.get("proxy_url") or img.get("original_src")
            if isinstance(src, str):
                sanitized_images.append(src)

    logger.info(f"[Builder] Sanitized images: {len(sanitized_images)} extracted from raw {len(raw_images)}")

    return PostBlock(
        post_id=str(post_id),
        author=post_data.get("author") or post_data.get("author_handle"),
        text=text,
        link=post_data.get("url"),
        images=sanitized_images,
        timestamp=post_data.get("captured_at") or post_data.get("created_at") or post_data.get("timestamp"),
        metrics=metrics,
    )


def _build_phenomenon(llm_data: Dict[str, Any]) -> Phenomenon:
    # Phenomenon identity should be registry-driven; LLM text is optional only.
    phenomenon_id = None  # identity set only by registry/enrichment
    status = None
    name = None
    description = None
    ai_image = None

    discovery = llm_data.get("Discovery_Channel") if isinstance(llm_data, dict) else {}
    summary = llm_data.get("summary") if isinstance(llm_data, dict) else {}

    if isinstance(discovery, dict):
        description = discovery.get("Phenomenon_Description") or discovery.get("description") or description
    if not description and isinstance(summary, dict):
        description = summary.get("one_line")

    visuals = llm_data.get("visuals") if isinstance(llm_data, dict) else {}
    if isinstance(visuals, dict):
        ai_image = visuals.get("ai_image") or visuals.get("image_url")

    return Phenomenon(id=phenomenon_id, status=status, name=name, description=description, ai_image=ai_image)


def _build_tone(llm_data: Dict[str, Any]) -> ToneProfile:
    tone_candidates: List[Dict[str, Any]] = []
    if isinstance(llm_data, dict):
        for key in ["Tone_Fingerprint", "L2_Tone_Fingerprint", "Tone", "tone", "emotional_pulse"]:
            val = llm_data.get(key)
            if isinstance(val, dict):
                tone_candidates.append(val)
    tone = next(iter(tone_candidates), {})

    def get_score(key: str) -> Optional[float]:
        if not tone:
            return None
        return _clamp_fraction(tone.get(key) or tone.get(key.capitalize()))

    return ToneProfile(
        primary=(tone or {}).get("primary"),
        cynicism=get_score("cynicism"),
        hope=get_score("hope"),
        outrage=get_score("anger") or get_score("outrage"),
        notes=(tone or {}).get("notes"),
    )


def _build_segments(cluster_data: Optional[Dict[str, Any]], llm_data: Dict[str, Any]) -> List[Segment]:
    segments: List[Segment] = []

    def _samples_from_list(samples_raw: Any) -> List[SegmentSample]:
        samples: List[SegmentSample] = []
        if isinstance(samples_raw, list):
            for s in samples_raw:
                if not isinstance(s, dict):
                    continue
                samples.append(
                    SegmentSample(
                        comment_id=str(s.get("id")) if s.get("id") is not None else None,
                        user=s.get("user") or s.get("author_handle"),
                        text=str(s.get("text") or "").strip(),
                        likes=_coerce_int(s.get("likes") or s.get("like_count")),
                    )
                )
        return samples

    clusters = None
    if isinstance(cluster_data, dict):
        clusters = cluster_data.get("clusters") or cluster_data
    if isinstance(clusters, dict):
        iterable = clusters.items()
    elif isinstance(clusters, list):
        iterable = enumerate(clusters)
    else:
        iterable = []

    for idx, info in iterable:
        if not isinstance(info, dict):
            continue
        label = info.get("label") or info.get("name") or f"Cluster {idx}"
        share = _clamp_fraction(info.get("share") or info.get("pct") or info.get("percentage"))
        if share is None and info.get("pct") is not None:
            share = _clamp_fraction(info.get("pct"))
        samples = _samples_from_list(info.get("samples"))
        segments.append(Segment(label=label, share=share, samples=samples))

    if not segments and isinstance(llm_data, dict):
        factions = None
        battlefield = llm_data.get("battlefield")
        if isinstance(battlefield, dict):
            factions = battlefield.get("factions")
        if isinstance(factions, list):
            for idx, f in enumerate(factions):
                if not isinstance(f, dict):
                    continue
                label = f.get("label") or f.get("name") or f.get("id") or f"Cluster {idx}"
                share = _clamp_fraction(f.get("share") or f.get("share_pct"))
                samples = _samples_from_list(f.get("samples"))
                segments.append(Segment(label=label, share=share, samples=samples))

    return segments


def _build_narrative_stack(llm_data: Dict[str, Any], full_report: Optional[str]) -> NarrativeStack:
    layers = llm_data.get("layers") if isinstance(llm_data, dict) else {}
    l1 = l2 = l3 = None
    if isinstance(layers, dict):
        l1 = (layers.get("l1") or {}).get("summary") or (layers.get("L1") or {}).get("summary")
        l2 = (layers.get("l2") or {}).get("summary") or (layers.get("L2") or {}).get("summary")
        l3 = (layers.get("l3") or {}).get("summary") or (layers.get("L3") or {}).get("summary")
    if not l1 and isinstance(llm_data, dict):
        l1 = llm_data.get("L1") or llm_data.get("l1")
    if not l2 and isinstance(llm_data, dict):
        l2 = llm_data.get("L2") or llm_data.get("l2")
    if not l3 and isinstance(llm_data, dict):
        l3 = llm_data.get("L3") or llm_data.get("l3")
    if full_report:
        def _extract_block(text: str, start_pattern: str, stop_patterns: List[str]) -> Optional[str]:
            try:
                start_match = re.search(start_pattern, text, flags=re.IGNORECASE | re.MULTILINE)
                if not start_match:
                    return None
                start_idx = start_match.end()
                end_idx = len(text)
                for sp in stop_patterns:
                    m = re.search(sp, text[start_idx:], flags=re.IGNORECASE | re.MULTILINE)
                    if m:
                        end_idx = min(end_idx, start_idx + m.start())
                block = text[start_idx:end_idx].strip()
                return block if block else None
            except Exception:
                return None

        if not l1:
            l1 = _extract_block(
                full_report,
                r"L1[：:.\s].*?(語言行為理論|Speech Act Theory)",
                [r"L2[：:.\s]", r"L3[：:.\s]", r"^### "],
            )
        if not l2:
            l2 = _extract_block(
                full_report,
                r"L2[：:.\s].*?(批判性話語分析|Critical Discourse Analysis|策略)",
                [r"L3[：:.\s]", r"L1[：:.\s]", r"^### "],
            )
        if not l3:
            l3 = _extract_block(
                full_report,
                r"L3[：:.\s].*?(輿論戰場與派系分析|Battlefield|Factions)",
                [r"L1[：:.\s]", r"L2[：:.\s]", r"^### "],
            )
    return NarrativeStack(l1=l1, l2=l2, l3=l3)


def _build_danger(llm_data: Dict[str, Any]) -> Optional[DangerBlock]:
    danger = llm_data.get("danger") if isinstance(llm_data, dict) else None
    if isinstance(danger, dict):
        return DangerBlock(
            bot_homogeneity_score=_clamp_fraction(danger.get("bot_homogeneity_score") or danger.get("math_homogeneity")),
            notes=danger.get("notes"),
        )
    return None


def build_analysis_json(
    post_data: Dict[str, Any],
    llm_data: Dict[str, Any],
    cluster_data: Optional[Dict[str, Any]] = None,
    full_report: Optional[str] = None,
) -> AnalysisV4:
    """
    Deterministically merge crawler data (post_data), LLM analysis (llm_data),
    and clustering output (cluster_data) into an AnalysisV4 object.
    """
    metrics = _build_metrics(post_data, llm_data)
    post_block = _build_post_block(post_data, metrics)
    phenomenon = _build_phenomenon(llm_data)
    tone = _build_tone(llm_data)
    segments = _build_segments(cluster_data, llm_data)
    narrative_stack = _build_narrative_stack(llm_data, full_report)
    danger = _build_danger(llm_data)

    summary = None
    if isinstance(llm_data, dict) and isinstance(llm_data.get("summary"), dict):
        summary = SummaryCompat(
            one_line=llm_data["summary"].get("one_line"),
            narrative_type=llm_data["summary"].get("narrative_type"),
        )
    battlefield = BattlefieldCompat(factions=segments) if segments else None
    analysis = AnalysisV4(
        post=post_block,
        phenomenon=phenomenon,
        emotional_pulse=tone,
        segments=segments,
        narrative_stack=narrative_stack,
        danger=danger,
        full_report=full_report,
        summary=summary,
        battlefield=battlefield,
    )
    post_id_val = safe_get(post_block, "post_id")
    if post_id_val in (None, "unknown"):
        logger.warning("Post ID missing when building AnalysisV4", extra={"url": post_block.link})
    if safe_get(post_data, "like_count") and analysis.post.metrics.likes == 0:
        logger.warning(
            "Metrics likes zero despite crawler data",
            extra={"post_id": post_id_val, "crawler_like": post_data.get("like_count")},
        )
    if segments and not battlefield:
        logger.debug("Segments present without battlefield compat", extra={"post_id": post_id_val})
    return analysis


def build_and_validate_analysis_json(
    post_data: Optional[Dict[str, Any]] = None,
    llm_data: Optional[Dict[str, Any]] = None,
    cluster_data: Optional[Dict[str, Any]] = None,
    full_report: Optional[str] = None,
) -> AnalysisV4:
    def _looks_like_analysis_payload(payload: Dict[str, Any]) -> bool:
        if not isinstance(payload, dict):
            return False
        if "post" in payload and isinstance(payload.get("post"), dict):
            return True
        for key in ("segments", "narrative_stack", "emotional_pulse", "phenomenon"):
            if key in payload:
                return True
        return False

    try:
        if (
            llm_data is None
            and cluster_data is None
            and full_report is None
            and isinstance(post_data, dict)
            and _looks_like_analysis_payload(post_data)
        ):
            if hasattr(AnalysisV4, "model_validate"):
                return AnalysisV4.model_validate(post_data)
            return AnalysisV4.parse_obj(post_data)
        return build_analysis_json(
            post_data=post_data or {},
            llm_data=llm_data or {},
            cluster_data=cluster_data,
            full_report=full_report,
        )
    except ValidationError as e:
        logger.error(
            "Failed to validate AnalysisV4 payload",
            extra={"errors": e.errors(), "post_id": safe_dump(post_data).get("id") or safe_dump(post_data).get("post_id")},
        )
        raise


# --- Safe Fuse Helpers ---

def protect_core_fields(post_row: Dict[str, Any], analysis_v4: AnalysisV4) -> AnalysisV4:
    """
    Enforce crawler-first on core fields (text/author/timestamp/metrics).
    """
    crawler_likes = _coerce_int(post_row.get("like_count") or post_row.get("likes"))
    crawler_views = _coerce_int(post_row.get("view_count") or post_row.get("impression_count"))
    crawler_replies = _coerce_int(post_row.get("reply_count") or post_row.get("comment_count"))

    # warn if LLM metrics diverge
    if analysis_v4.post.metrics:
        llm_likes = analysis_v4.post.metrics.likes
        if llm_likes is not None and crawler_likes is not None and abs(llm_likes - crawler_likes) > max(100, crawler_likes * 0.5):
            logger.warning(
                "LLM likes differ from crawler likes; enforcing crawler",
                extra={"crawler_likes": crawler_likes, "llm_likes": llm_likes, "post_id": post_row.get("id")},
            )

    # override metrics with crawler truth when available
    metrics = analysis_v4.post.metrics.copy(
        update={
            "likes": crawler_likes if crawler_likes is not None else analysis_v4.post.metrics.likes,
            "views": crawler_views if crawler_views is not None else analysis_v4.post.metrics.views,
            "replies": crawler_replies if crawler_replies is not None else analysis_v4.post.metrics.replies,
        }
    )

    created_at = post_row.get("created_at") or post_row.get("captured_at") or post_row.get("timestamp")
    text = (
        post_row.get("post_text")
        or post_row.get("text")
        or post_row.get("content")
        or post_row.get("caption")
        or analysis_v4.post.text
    )
    author = post_row.get("author") or post_row.get("author_handle") or analysis_v4.post.author

    post_dict = safe_dump(analysis_v4.post)
    post_dict.update(
        {
            "text": text,
            "author": author,
            "timestamp": created_at or post_dict.get("timestamp"),
            "metrics": metrics,
        }
    )
    post_block = analysis_v4.post if isinstance(analysis_v4.post, PostBlock) else PostBlock(**post_dict)
    return analysis_v4.copy(update={"post": post_block})


def validate_analysis_json(analysis_v4: AnalysisV4) -> tuple[bool, str, list[str]]:
    """
    Validate minimal completeness. Return (is_valid, invalid_reason, missing_keys).
    """
    missing: list[str] = []
    # version allowlist
    version = safe_get(analysis_v4, "analysis_version", "v4")
    if version not in {"v4", "v4.1"}:
        return False, f"unsupported_version:{version}", ["analysis_version"]

    # required post fields
    if not safe_get(analysis_v4.post, "post_id"):
        missing.append("post.id")
    if not safe_get(analysis_v4.post, "text"):
        missing.append("post.text")
    if not safe_get(analysis_v4.post, "timestamp"):
        missing.append("post.created_at")

    # phenomenon identity (id preferred; name optional fallback)
    phen_id = safe_get(analysis_v4.phenomenon, "id")
    phen_name = safe_get(analysis_v4.phenomenon, "name")
    phen_status = safe_get(analysis_v4.phenomenon, "status")
    if not phen_id and not phen_name and phen_status != "pending":
        missing.append("phenomenon.id_or_name")

    # evidence count (best-effort: look for evidence.refs if present)
    evidence_count = 0
    if hasattr(analysis_v4, "evidence"):
        try:
            refs = getattr(analysis_v4, "evidence").get("refs", [])  # type: ignore
            if isinstance(refs, list):
                evidence_count = len(refs)
        except Exception:
            evidence_count = 0
    if evidence_count < 2:
        missing.append("phenomenon.evidence>=2")

    if missing:
        return False, "missing_required_fields", missing
    return True, "", []
