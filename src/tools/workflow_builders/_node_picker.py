"""Pick a Databricks node_type_id from the workspace's available catalog.

Selection priority for data-generation workloads:
  1. num_cores matches target
  2. category matches (Storage Optimized when requested, else General Purpose)
  3. Not specialized (GPU / ML / IPU / Gaudi)
  4. ARM-based — prefer
       - Azure D/E series with 'p' in suffix (e.g. Standard_D8pds_v6)
       - AWS Graviton (e.g. m8g, m8gd, c8g, r8g)
       - GCP Axion or T2A (e.g. c4a-, t2a-)
  5. Has local NVMe disk — prefer (gives Spark fast shuffle + DIGen fast IO)
       - Storage-Optimized always has it.
       - Azure 'd' in the suffix indicates it (pds, ds, pld, ld).
       - AWS 'd' suffix in the family-suffix indicates it (m8gd, m7gd).
  6. Latest generation — prefer the highest version number in the name.

Falls back layer-by-layer if a stricter tier has no match. Pure heuristics
based on Databricks API response fields and well-known cloud naming
conventions; not exhaustive but covers the modern instance families on
Azure, AWS, and GCP.
"""
from __future__ import annotations

import re

_SPECIALIZED_TOKENS = ("GPU", "_ML", "IPU", "GAUDI")

# Generation extractors per cloud — return higher int for newer.
_GEN_AZURE = re.compile(r"_v(\d+)")
_GEN_AWS_GCP = re.compile(r"^([a-z]+?)(\d+)")


def _is_specialized(node_id: str) -> bool:
    return any(t in node_id.upper() for t in _SPECIALIZED_TOKENS)


def _is_arm(node_id: str, cloud: str) -> bool:
    """Heuristic ARM detection by name pattern."""
    nid = node_id.lower()
    if cloud == "Azure":
        # ARM has 'p' in the suffix: Standard_D8pds_v6 (yes), Standard_D8ds_v6 (no).
        m = re.search(r"standard_[a-z]+\d+([a-z]+)_v\d+", nid)
        return bool(m and "p" in m.group(1))
    if cloud == "AWS":
        # Graviton families use 'g' as the first letter of the family-suffix:
        # m8g, m8gd, c8g, r8g, x8g, etc.
        m = re.match(r"^[a-z]+\d+([a-z]+)\.", nid)
        return bool(m and m.group(1).startswith("g"))
    if cloud == "GCP":
        # Axion (c4a-) and Tau T2A (t2a-) are ARM.
        return nid.startswith(("t2a-", "c4a-"))
    return False


def _has_local_disk(node_id: str, info: dict, cloud: str) -> bool:
    """Heuristic local-NVMe detection — trusts API fields first, then names."""
    if (info.get("category") or "").strip().lower() == "storage optimized":
        return True
    if (info.get("num_local_disks") or info.get("local_disks") or 0) > 0:
        return True
    nid = node_id.lower()
    if cloud == "Azure":
        # D/E series: 'd' in the suffix indicates local NVMe (pds, ds, pld, ld).
        m = re.search(r"standard_[a-z]+\d+([a-z]+)_v\d+", nid)
        return bool(m and "d" in m.group(1))
    if cloud == "AWS":
        # 'd' in the family-suffix means local NVMe instance store
        # (m7gd, m8gd, c6gd, r7gd, …). i-series storage-opt was caught above.
        m = re.match(r"^[a-z]+\d+([a-z]+)\.", nid)
        return bool(m and "d" in m.group(1))
    if cloud == "GCP":
        # `-lssd` suffix indicates the bundled-local-SSD variant
        # (e.g. c4a-standard-16-lssd). Without it we attach local SSDs
        # via gcp_attributes.local_ssd_count from the caller.
        return nid.endswith("-lssd")
    return False


def gcp_local_ssd_count(target_cores: int) -> int:
    """Recommended number of 375 GB local SSDs to attach to a GCP node when
    the picked node_type doesn't include them by default.

    Rough rule: ~1 SSD per 4 cores. Sizing produced:
      8 cores  → 2 SSDs (750 GB)
      16 cores → 4 SSDs (1.5 TB)
      32 cores → 8 SSDs (3 TB)
      64 cores → 16 SSDs (6 TB)
    """
    return max(1, target_cores // 4)


def apply_gcp_local_ssd_if_needed(
    new_cluster: dict, node_id: str, info: dict, cloud: str, target_cores: int,
) -> None:
    """If running on GCP and the picked node has no built-in local SSD
    (i.e. no `-lssd` suffix, not Storage Optimized), populate
    `gcp_attributes.local_ssd_count` so Spark / DIGen still has fast local
    storage for shuffle and tmp output. No-op for Azure / AWS / nodes that
    already include local NVMe."""
    if cloud != "GCP":
        return
    if _has_local_disk(node_id, info, cloud):
        return
    new_cluster.setdefault("gcp_attributes", {})["local_ssd_count"] = (
        gcp_local_ssd_count(target_cores)
    )


def _gen_score(node_id: str, cloud: str) -> int:
    nid = node_id.lower()
    if cloud == "Azure":
        m = _GEN_AZURE.search(nid)
        return int(m.group(1)) if m else 0
    m = _GEN_AWS_GCP.match(nid)
    return int(m.group(2)) if m else 0


def pick_node(
    node_types: dict,
    cloud: str,
    *,
    target_cores: int,
    prefer_storage_opt: bool = False,
    fallback: str | None = None,
) -> str | None:
    """Return the best-matching node_type_id from the workspace's catalog.

    Args:
        node_types: ``{node_type_id: {full_info_dict}}`` from
            `tpcdi_config.node_types`.
        cloud: ``"Azure"``, ``"AWS"``, or ``"GCP"`` — drives the ARM and
            local-disk heuristics.
        target_cores: Required CPU core count.
        prefer_storage_opt: If True, prefer ``category="Storage Optimized"``
            (latest gen will likely be non-ARM since storage-optimized ARM
            SKUs are rare on Azure as of writing). Falls back to General
            Purpose if no storage-optimized node with target_cores exists.
        fallback: ``node_type_id`` to return if nothing matches at all.

    Returns the chosen node id, or ``fallback``.
    """
    target_cat = "Storage Optimized" if prefer_storage_opt else "General Purpose"

    # Tier 1: matching cores + matching category, not specialized.
    candidates = [
        (nid, info) for nid, info in node_types.items()
        if info.get("num_cores") == target_cores
        and (info.get("category") or "").strip().lower() == target_cat.lower()
        and not _is_specialized(nid)
    ]
    if not candidates:
        # Tier 2: matching cores, drop the category constraint.
        candidates = [
            (nid, info) for nid, info in node_types.items()
            if info.get("num_cores") == target_cores
            and not _is_specialized(nid)
        ]
    if not candidates:
        return fallback

    # Score: (is_arm, has_local_disk, gen) — higher tuple wins.
    return max(
        candidates,
        key=lambda c: (_is_arm(c[0], cloud), _has_local_disk(c[0], c[1], cloud), _gen_score(c[0], cloud)),
    )[0]


def needs_extra_local_disk(node_id: str, info: dict, cloud: str) -> bool:
    """True if the chosen node has no local NVMe and we should rely on
    ``enable_elastic_disk: True`` (which will attach managed EBS / temp
    disks dynamically). Spark workers + DIGen drivers prefer local NVMe
    for shuffle / temp output; without it, elastic-disk ensures we don't
    OOM on /local_disk0 instead of crashing."""
    return not _has_local_disk(node_id, info, cloud)
