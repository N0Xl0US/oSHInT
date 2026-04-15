"""
resolver/linker.py — SplinkResolver: production entity resolution using Splink 4.

Implements the 4-stage resolution pipeline:
  1. Candidate Generation (Blocking): hashed-email, username, platform compound blocks
  2. Pairwise Matching: Jaro-Winkler + Levenshtein multi-threshold comparisons
  3. Graph Clustering: Connected Components via Splink (native CC implementation)
  4. Golden Record merge: handled downstream by golden.py

Uses a deterministic fallback when claims are too few for Splink's EM training.
Falls back gracefully on any Splink error (library/data edge cases).
"""

from __future__ import annotations

import logging
import uuid
from dataclasses import dataclass, field
from typing import Sequence

import pandas as pd

from maigret.agent.report import IdentityClaim
from maigret.resolver.config import ResolverConfig, get_resolver_config
from maigret.resolver.flatten import flatten_claims

logger = logging.getLogger(__name__)


@dataclass
class ResolvedCluster:
    """A cluster of claims resolved as referring to the same real-world entity."""

    cluster_id: str
    claims: list[IdentityClaim] = field(default_factory=list)
    resolution_method: str = "deterministic"  # or "probabilistic"
    pairwise_scores: list[float] = field(default_factory=list)  # match probs per pair


class SplinkResolver:
    """
    Production entity resolution using Splink 4 with DuckDB backend.

    Two resolution paths:
    - **Deterministic** (< min_claims_for_splink): cluster by exact username match
    - **Probabilistic** (>= min_claims_for_splink): full Fellegi-Sunter pipeline

    The probabilistic path runs:
      Stage 1: Candidate generation via blocking rules
      Stage 2: Pairwise matching with multi-column comparisons
      Stage 3: Connected Components clustering
      Stage 4: Cluster → GoldenRecord (handled by golden.py)

    Usage:
        resolver = SplinkResolver()
        clusters = resolver.resolve(claims)
    """

    def __init__(self, config: ResolverConfig | None = None) -> None:
        self._config = config or get_resolver_config()

    def resolve(self, claims: Sequence[IdentityClaim]) -> list[ResolvedCluster]:
        """Resolve IdentityClaims into entity clusters."""
        if not claims:
            return []

        claims_list = list(claims)
        logger.info("SplinkResolver: resolving %d claims", len(claims_list))

        if len(claims_list) < self._config.min_claims_for_splink:
            logger.info(
                "Using deterministic fallback (%d < %d claims)",
                len(claims_list), self._config.min_claims_for_splink,
            )
            return self._deterministic_resolve(claims_list)

        return self._probabilistic_resolve(claims_list)

    # ── Stage 0: Deterministic fallback ───────────────────────────────────

    def _deterministic_resolve(self, claims: list[IdentityClaim]) -> list[ResolvedCluster]:
        """
        Group claims by normalised username + email.

        Merge logic:
         - Claims with same normalised username → same cluster
         - Claims with same email hash → merge clusters (transitive closure)
        """
        from maigret.resolver.flatten import _hash_email, _normalise_username

        # First pass: group by normalised username
        username_groups: dict[str, list[IdentityClaim]] = {}
        for claim in claims:
            key = _normalise_username(claim.username)
            username_groups.setdefault(key, []).append(claim)

        # Second pass: merge groups that share an email (transitive closure)
        email_to_group_key: dict[str, str] = {}
        merge_map: dict[str, str] = {}  # maps group_key → canonical group_key

        for group_key, group_claims in username_groups.items():
            merge_map[group_key] = group_key  # self-reference initially

            for claim in group_claims:
                if claim.email:
                    e_hash = _hash_email(claim.email)
                    if e_hash in email_to_group_key:
                        # Merge: this group should join the existing email group
                        existing = email_to_group_key[e_hash]
                        merge_map[group_key] = existing
                    else:
                        email_to_group_key[e_hash] = group_key

        # Resolve transitivity
        def find_root(key: str) -> str:
            while merge_map[key] != key:
                key = merge_map[key]
            return key

        final_clusters: dict[str, list[IdentityClaim]] = {}
        for group_key, group_claims in username_groups.items():
            root = find_root(group_key)
            final_clusters.setdefault(root, []).extend(group_claims)

        result = [
            ResolvedCluster(
                cluster_id=str(uuid.uuid4()),
                claims=group,
                resolution_method="deterministic",
            )
            for group in final_clusters.values()
        ]

        logger.info("Deterministic: %d clusters from %d claims", len(result), len(claims))
        return result

    # ── Stage 1–3: Probabilistic (Splink) ─────────────────────────────────

    def _probabilistic_resolve(self, claims: list[IdentityClaim]) -> list[ResolvedCluster]:
        """Full Splink pipeline with graceful fallback."""
        try:
            return self._run_splink(claims)
        except Exception as exc:
            logger.warning("Splink failed (%s), falling back to deterministic", exc)
            return self._deterministic_resolve(claims)

    def _run_splink(self, claims: list[IdentityClaim]) -> list[ResolvedCluster]:
        """
        Run the 3-stage Splink pipeline:
          1. Candidate generation (blocking rules)
          2. Pairwise matching (comparisons + EM-trained model)
          3. Graph clustering (Connected Components)
        """
        import splink.comparison_library as cl
        from splink import DuckDBAPI, Linker, SettingsCreator, block_on

        # ── Flatten to DataFrame ─────────────────────────────────────────
        df = flatten_claims(claims)
        logger.info("Splink input: %d rows × %d cols", len(df), len(df.columns))

        if len(df) < 2:
            return self._deterministic_resolve(claims)

        # Check if we have email data (changes blocking strategy)
        has_emails = (df["email_hash"] != "").any()

        # ── STAGE 1: Candidate generation (blocking) ─────────────────────
        #
        # Multiple blocking rules — Splink OR's them (pair matches ANY rule).
        # Ordered from most specific to most permissive.
        blocking_rules = []

        if has_emails:
            # Strongest signal: exact email hash match
            blocking_rules.append(block_on("email_hash"))

        blocking_rules.extend([
            # Exact normalised username (catches cross-source same-user)
            block_on("username_norm"),
            # Raw username as a second exact key (guards against edge normalisation loss)
            block_on("username"),
            # Prefix-only fallback to avoid over-pruning candidate pairs
            block_on("username_prefix"),
            # Same platform + similar username prefix (catches variants)
            block_on("platform", "username_prefix"),
        ])

        # ── STAGE 2: Pairwise matching (comparisons) ────────────────────
        #
        # Multi-level comparisons with different similarity thresholds.
        comparisons = [
            # Exact normalised username (strongest signal in username-led runs)
            cl.ExactMatch("username_norm"),

            # Username: Jaro-Winkler (good for short strings like usernames)
            # Level 1: JW >= 0.92 → almost exact (johndoe vs john_doe)
            # Level 2: JW >= 0.70 → loose fuzzy (johndoe vs john42)
            cl.JaroWinklerAtThresholds("username_norm", [0.92, 0.70]),

            # Platform: exact match (strong corroborating signal)
            cl.ExactMatch("platform"),

            # URL domain: exact match (corroborating)
            cl.ExactMatch("url_domain"),
        ]

        if has_emails:
            # Email hash: exact match (very strong identity signal)
            comparisons.insert(0, cl.ExactMatch("email_hash"))

        settings = SettingsCreator(
            link_type="dedupe_only",
            blocking_rules_to_generate_predictions=blocking_rules,
            comparisons=comparisons,
            retain_intermediate_calculation_columns=False,
        )

        # ── Initialise Splink with DuckDB ────────────────────────────────
        db_api = DuckDBAPI()
        linker = Linker(df, settings, db_api=db_api)

        # ── Train model (multi-pass EM) ──────────────────────────────────
        #
        # estimate_u: random sampling for non-match (u) probabilities
        # estimate_parameters: EM algorithm for match (m) probabilities
        # Multiple training passes with different blocking = better weights
        try:
            linker.training.estimate_u_using_random_sampling(max_pairs=1e5)

            # Pass 1: block on username (most available signal)
            linker.training.estimate_parameters_using_expectation_maximisation(
                block_on("username_norm"),
                fix_u_probabilities=True,
            )

            # Pass 2: block on platform (different signal axis)
            try:
                linker.training.estimate_parameters_using_expectation_maximisation(
                    block_on("platform"),
                    fix_u_probabilities=True,
                )
            except Exception:
                logger.debug("Platform EM pass failed (may lack diversity), skipping")

            # Pass 3: block on email (if available, strongest signal)
            if has_emails:
                try:
                    linker.training.estimate_parameters_using_expectation_maximisation(
                        block_on("email_hash"),
                        fix_u_probabilities=True,
                    )
                except Exception:
                    logger.debug("Email EM pass failed, skipping")

        except Exception as exc:
            logger.warning("Splink training failed (%s), using untrained model", exc)

        # ── Predict pairwise match probabilities ─────────────────────────
        predictions = linker.inference.predict(
            threshold_match_probability=self._config.match_threshold,
        )

        # ── STAGE 3: Graph clustering (Connected Components) ─────────────
        #
        # Splink's cluster_pairwise_predictions_at_threshold uses
        # Connected Components on the match graph — exactly what the spec requires.
        # This is superior to Label Propagation for identity graphs.
        clusters_df = linker.clustering.cluster_pairwise_predictions_at_threshold(
            predictions,
            threshold_match_probability=self._config.cluster_threshold,
        )
        clusters_pandas = clusters_df.as_pandas_dataframe()

        # ── Map clusters back to IdentityClaim objects ───────────────────
        return self._clusters_to_resolved(clusters_pandas, claims)

    def _clusters_to_resolved(
        self,
        clusters_df: pd.DataFrame,
        original_claims: list[IdentityClaim],
    ) -> list[ResolvedCluster]:
        """
        Map Splink cluster assignments (unique_id, cluster_id) back to claims.

        Unclustered claims (singletons) get their own cluster.
        """
        from maigret.resolver.flatten import _make_unique_id

        uid_to_claim: dict[str, IdentityClaim] = {}
        for claim in original_claims:
            uid = _make_unique_id(claim)
            uid_to_claim[uid] = claim

        # Splink schema can vary by version/backend (e.g. unique_id, unique_id_l).
        uid_cols = [c for c in clusters_df.columns if c.lower().startswith("unique_id")]
        cid_cols = [c for c in clusters_df.columns if c.lower().startswith("cluster_id")]

        # Group by cluster_id
        cluster_map: dict[str, list[IdentityClaim]] = {}
        for _, row in clusters_df.iterrows():
            uid = ""
            for col in uid_cols:
                candidate = str(row.get(col, "")).strip()
                if candidate in uid_to_claim:
                    uid = candidate
                    break

            if not uid:
                # Last-resort scan in case the backend renamed columns unexpectedly.
                for value in row.values:
                    candidate = str(value).strip()
                    if candidate in uid_to_claim:
                        uid = candidate
                        break

            cid = ""
            for col in cid_cols:
                candidate = str(row.get(col, "")).strip()
                if candidate:
                    cid = candidate
                    break

            if not uid or not cid:
                continue

            claim = uid_to_claim.get(uid)
            if claim:
                cluster_map.setdefault(cid, []).append(claim)

        if clusters_df is not None and not clusters_df.empty and not cluster_map:
            logger.warning(
                "Splink returned %d cluster rows but none mapped to claims; "
                "falling back to deterministic clustering",
                len(clusters_df),
            )
            return self._deterministic_resolve(original_claims)

        # Singletons: unclustered claims
        clustered_uids = set()
        for group in cluster_map.values():
            for c in group:
                clustered_uids.add(_make_unique_id(c))

        for claim in original_claims:
            uid = _make_unique_id(claim)
            if uid not in clustered_uids:
                cluster_map[str(uuid.uuid4())] = [claim]

        result = [
            ResolvedCluster(
                cluster_id=cid,
                claims=group,
                resolution_method="probabilistic",
            )
            for cid, group in cluster_map.items()
        ]

        logger.info("Splink CC: %d clusters from %d claims", len(result), len(original_claims))
        return result
