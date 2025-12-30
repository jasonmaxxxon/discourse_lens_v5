from __future__ import annotations

import copy
import logging
import os
import uuid
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from typing import Any, Dict, Optional, Sequence

from supabase import Client
from datetime import datetime, timezone, date

from .phenomenon_fingerprint import (
    EvidenceBundle,
    build_evidence_bundle,
    FINGERPRINT_VERSION,
    MATCH_RULESET_VERSION,
    REGISTRY_VERSION,
    NAMESPACE_UUID,
)
from .embeddings import embed_text, embedding_hash, EMBED_DIM, EMBED_MODEL
from database.store import save_analysis_result

logger = logging.getLogger("PhenomenonEnricher")


try:
    from pydantic import BaseModel
except Exception:
    BaseModel = None  # type: ignore


def make_json_safe(x: Any) -> Any:
    """Recursively convert python objects into JSON-serializable types."""
    if x is None:
        return None
    if isinstance(x, (datetime, date)):
        return x.isoformat()
    if isinstance(x, uuid.UUID):
        return str(x)
    if BaseModel is not None and isinstance(x, BaseModel):
        return make_json_safe(x.model_dump(mode="json"))
    if isinstance(x, dict):
        return {str(k): make_json_safe(v) for k, v in x.items()}
    if isinstance(x, (list, tuple, set)):
        return [make_json_safe(v) for v in x]
    if isinstance(x, (str, int, float, bool)):
        return x
    return str(x)


@dataclass
class PhenomenonMatchResult:
    phenomenon_id: str
    status: str  # matched | minted
    decision: str
    confidence: float
    ruleset_version: str
    case_id: str


class PhenomenonEnricher:
    """
    Non-blocking async phenomenon Match-or-Mint stage.
    - Deterministic: case_id + uuid5 minted ids
    - Non-blocking: background thread, pipeline response unaffected
    """

    def __init__(self, supabase: Optional[Client], enabled: Optional[bool] = None, run_inline: Optional[bool] = None) -> None:
        env_flag = os.getenv("ENABLE_PHENOMENON_ENRICHMENT")
        alt_flag = os.getenv("DL_ENABLE_PHENOMENON_ENRICHER")
        flag_val = env_flag if env_flag is not None else alt_flag
        # Default ON unless explicitly set false/off/0.
        self.enabled = enabled if enabled is not None else (str(flag_val).lower() not in {"0", "false", "off", "none"})
        inline_flag = os.getenv("DL_ENRICH_INLINE")
        self.run_inline = run_inline if run_inline is not None else (str(inline_flag).lower() not in {"0", "false", "off", "none"})
        self.supabase = supabase
        self.executor = ThreadPoolExecutor(max_workers=2)
        logger.info(f"[PhenomenonEnricher] Enabled={self.enabled} Inline={self.run_inline}")
        self.match_threshold = float(os.getenv("PHENOMENON_MATCH_THRESHOLD", "0.86"))
        self.match_topk = int(os.getenv("PHENOMENON_MATCH_TOPK", "5"))

    def submit(
        self,
        post_row: Dict[str, Any],
        analysis_payload: Dict[str, Any],
        cluster_summary: Dict[str, Any],
        comments: Sequence[Dict[str, Any]],
    ) -> None:
        if not self.enabled:
            logger.debug("[PhenomenonEnricher] Skipped (disabled).")
            return
        if not self.supabase:
            logger.warning("[PhenomenonEnricher] Supabase client missing; skipping enrichment.")
            return
        try:
            post_id = post_row.get("id") or post_row.get("post_id")
            if post_id:
                try:
                    self.supabase.table("threads_posts").update(
                        {
                            "enrichment_status": "processing",
                            "enrichment_started_at": datetime.now(timezone.utc).isoformat(),
                            "enrichment_last_error": None,
                        }
                    ).eq("id", post_id).execute()
                except Exception:
                    logger.warning("[PhenomenonEnricher] Failed to mark processing", exc_info=True)
            if self.run_inline:
                self._run_safe(
                    post_row,
                    copy.deepcopy(analysis_payload),
                    cluster_summary or {},
                    comments or [],
                )
            else:
                self.executor.submit(
                    self._run_safe,
                    post_row,
                    copy.deepcopy(analysis_payload),
                    cluster_summary or {},
                    comments or [],
                )
        except Exception:
            logger.exception("[PhenomenonEnricher] Failed to submit job")

    def _increment_retry_count(self, post_id: str) -> None:
        try:
            resp = (
                self.supabase.table("threads_posts")
                .select("enrichment_retry_count")
                .eq("id", post_id)
                .limit(1)
                .execute()
            )
            current = 0
            if resp.data:
                val = resp.data[0].get("enrichment_retry_count")
                if isinstance(val, int):
                    current = val
            self.supabase.table("threads_posts").update(
                {
                    "enrichment_retry_count": current + 1,
                }
            ).eq("id", post_id).execute()
        except Exception:
            logger.error("[PhenomenonEnricher] Failed to increment retry count", exc_info=True)

    def _run_safe(
        self,
        post_row: Dict[str, Any],
        analysis_payload: Dict[str, Any],
        cluster_summary: Dict[str, Any],
        comments: Sequence[Dict[str, Any]],
    ) -> None:
        post_id = post_row.get("id") or post_row.get("post_id")
        try:
            bundle = build_evidence_bundle(
                post_text=post_row.get("post_text") or post_row.get("text") or "",
                ocr_full_text=None,
                comments=comments,
                cluster_summary=cluster_summary or {},
                images=post_row.get("images") or [],
            )
            match = self._match_or_mint(bundle)
            self._patch_analysis(post_row, analysis_payload, match, bundle)
            if post_id:
                try:
                    self.supabase.table("threads_posts").update(
                        {
                            "enrichment_status": "completed",
                            "enrichment_completed_at": datetime.now(timezone.utc).isoformat(),
                            "enrichment_last_error": None,
                        }
                    ).eq("id", post_id).execute()
                except Exception:
                    logger.warning("[PhenomenonEnricher] Failed to mark completed", exc_info=True)
        except Exception as e:
            logger.exception("[PhenomenonEnricher] Job crashed")
            if post_id:
                try:
                    self.supabase.table("threads_posts").update(
                        {
                            "enrichment_status": "failed",
                            "enrichment_last_error": str(e)[:2000],
                        }
                    ).eq("id", post_id).execute()
                except Exception:
                    logger.error("[PhenomenonEnricher] Failed to mark failed state (crash path)", exc_info=True)
                try:
                    self._increment_retry_count(str(post_id))
                except Exception:
                    logger.error("[PhenomenonEnricher] Failed to increment retry (crash path)", exc_info=True)

    def _match_or_mint(self, bundle: EvidenceBundle) -> PhenomenonMatchResult:
        """
        Hybrid match-or-mint:
        - Compute embedding from fingerprint text.
        - Try vector search against registry; if best above threshold, match existing.
        - Else mint deterministic uuid5(namespace, fingerprint).
        """
        namespace = uuid.UUID(NAMESPACE_UUID)
        deterministic_id = str(uuid.uuid5(namespace, bundle.fingerprint))

        # Semantic match
        try:
            emb = embed_text(bundle.fingerprint)
            if len(emb) != EMBED_DIM:
                raise ValueError(f"Enricher embedding dim mismatch expected {EMBED_DIM} got {len(emb)}")
            logger.info(
                f"[PhenomenonEnricher] embedding ready model={EMBED_MODEL} dim={len(emb)} sample={emb[:5]}"
            )
            resp = self.supabase.rpc(
                "match_phenomena_v768",
                {
                    "query_embedding": emb,
                    "match_threshold": self.match_threshold,
                    "match_count": self.match_topk,
                },
            ).execute()
            candidates = resp.data or []
            if candidates:
                best = candidates[0]
                try:
                    best_score = float(best.get("similarity") or 0)
                except Exception:
                    best_score = 0.0
                if best_score >= self.match_threshold and best.get("id"):
                    return PhenomenonMatchResult(
                        phenomenon_id=str(best["id"]),
                        status="matched",
                        decision="MATCH_EXISTING",
                        confidence=best_score * 100,
                        ruleset_version=MATCH_RULESET_VERSION,
                        case_id=bundle.case_id,
                    )
        except Exception:
            logger.warning("[PhenomenonEnricher] Vector match failed; fallback to mint", exc_info=True)

        return PhenomenonMatchResult(
            phenomenon_id=deterministic_id,
            status="minted",
            decision="MINT_NEW",
            confidence=100.0,
            ruleset_version=MATCH_RULESET_VERSION,
            case_id=bundle.case_id,
        )

    def _patch_analysis(
        self,
        post_row: Dict[str, Any],
        analysis_payload: Dict[str, Any],
        match: PhenomenonMatchResult,
        bundle: EvidenceBundle,
    ) -> None:
        post_id = str(post_row.get("id") or post_row.get("post_id") or "")
        if not post_id:
            logger.warning("[PhenomenonEnricher] Missing post_id; skip patch.")
            return

        phen_block = analysis_payload.get("phenomenon") or {}
        existing_id = phen_block.get("id")
        existing_status = (phen_block.get("status") or "").lower()
        if existing_id and existing_status not in {"pending", "failed", "provisional"}:
            logger.info("[PhenomenonEnricher] Skip patch; phenomenon already finalized", extra={"post_id": post_id})
            return

        phen_block.update(
            {
                "id": match.phenomenon_id,
                "status": match.status,
            }
        )
        analysis_payload["phenomenon"] = phen_block
        analysis_payload["phenomenon_status"] = match.status
        analysis_payload["phenomenon_case_id"] = match.case_id
        analysis_payload["match_ruleset_version"] = match.ruleset_version
        analysis_payload["fingerprint_version"] = FINGERPRINT_VERSION
        analysis_payload["registry_version"] = REGISTRY_VERSION

        try:
            save_analysis_result(post_id, analysis_payload)
            safe_payload = make_json_safe(
                {
                    "phenomenon_id": match.phenomenon_id,
                    "phenomenon_status": match.status,
                    "phenomenon_case_id": match.case_id,
                }
            )
            resp = (
                self.supabase.table("threads_posts")
                .update(safe_payload)
                .eq("id", post_id)
                .execute()
            )
            logger.info(
                "[PhenomenonEnricher] Patched phenomenon_id",
                extra={"post_id": post_id, "phenomenon_id": match.phenomenon_id, "status": match.status, "resp_error": getattr(resp, "error", None)},
            )
        except Exception as e:
            logger.exception(
                "[PhenomenonEnricher] Failed to patch analysis_json",
                extra={"post_id": post_id, "stage": "_patch_analysis", "error": f"{type(e).__name__}: {e}"},
            )

        # Registry upsert with embedding; non-blocking.
        try:
            reg_status = match.status if match.status else "provisional"
            phen_desc = phen_block.get("description") or "(auto) pending governance"
            emb_vec = embed_text(bundle.fingerprint)
            if len(emb_vec) != EMBED_DIM:
                raise ValueError(f"Registry upsert embedding dim mismatch expected {EMBED_DIM} got {len(emb_vec)}")
            reg_payload = {
                "id": match.phenomenon_id,
                "canonical_name": phen_block.get("name") or f"MINTED_{match.phenomenon_id[:8]}",
                "description": phen_desc,
                "status": reg_status,
                "minted_by_case_id": match.case_id,
                "embedding_v768": emb_vec,
            }
            self.supabase.table("narrative_phenomena").upsert(reg_payload, on_conflict="id").execute()
            # Occurrence increment (fail loud if RPC missing or permission blocked)
            try:
                self.supabase.rpc(
                    "increment_occurrence",
                    {"phenomenon_id": match.phenomenon_id},
                ).execute()
            except Exception as e:
                hint = "Run: supabase db push (migration missing) or ensure backend uses SERVICE_ROLE key"
                raise RuntimeError(f"RPC increment_occurrence failed: {e}. {hint}") from e
        except Exception as e:
            logger.warning("[PhenomenonEnricher] Registry upsert failed", extra={"error": str(e), "phenomenon_id": match.phenomenon_id})


def _first_image_ocr(images: Sequence[Dict[str, Any]]) -> str:
    if not images:
        return ""
    for img in images:
        if not isinstance(img, dict):
            continue
        txt = img.get("full_text") or img.get("ocr_full_text") or img.get("text")
        if txt:
            return str(txt)
    return ""
