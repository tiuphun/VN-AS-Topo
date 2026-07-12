"""
AS Path Sanitization Module v2 — Context-Aware VNIX Handling
==============================================================
Reference: Step (3) of the AS topology mapping methodology.

Differs from sanitize.py in one key way: VNIX route-server ASNs are
handled based on their POSITION in the AS path, not stripped blindly.

  - VNIX ASN in the MIDDLE of a path → route-server role → strip it,
    but record the two neighbors as a "VNIX-facilitated" peering edge.
  - VNIX ASN at the BEGINNING or END → VNNIC operational traffic →
    keep it as a real AS in the path.
  - All other IXP route-server ASNs → stripped unconditionally (unchanged).

Other sanitization steps are identical to v1:
  1. Remove special-purpose ASNs (IANA reserved ranges)
  2. Remove IXP route-server ASNs (context-aware for VNIX)
  3. Remove prepending (consecutive duplicate ASNs)
  4. Remove AS loops (non-adjacent repetition)
  5. Filter single-ASN paths
"""

import os
import csv
import logging

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# IANA Special-Purpose ASN Ranges (same as v1)
# ---------------------------------------------------------------------------
IANA_SPECIAL_RANGES = [
    (0, 0),                    # AS0 — Reserved (RFC 7607)
    (112, 112),                # AS112 — AS112 Project (RFC 7534)
    (23456, 23456),            # AS_TRANS — 2-to-4-byte transition (RFC 6793)
    (64496, 64511),            # Documentation and sample code (RFC 5398)
    (64512, 65534),            # Private use (RFC 6996)
    (65535, 65535),            # Reserved / Last16 (RFC 7300)
    (65536, 65551),            # Documentation and sample code (RFC 5398)
    (65552, 131071),           # Reserved by IANA
    (4200000000, 4294967294),  # Private use (RFC 6996)
    (4294967295, 4294967295),  # Reserved / Last32 (RFC 7300)
]

# ---------------------------------------------------------------------------
# VNIX Route-Server ASNs and their exchange locations
# Source: PeeringDB ix_id 1771, 1772, 1773
# ---------------------------------------------------------------------------
VNIX_RS_ASNS = {
    23899: 'VNIX-HN',    # VNIX Hanoi route server
    23962: 'VNIX-HCM',   # VNIX Ho Chi Minh City route server
    56156: 'VNIX-DN',     # VNIX Da Nang route server
}


def is_special_purpose_asn(asn: int) -> bool:
    """Check if an ASN falls within any IANA special-purpose range."""
    for low, high in IANA_SPECIAL_RANGES:
        if low <= asn <= high:
            return True
    return False


def load_ixp_asns(filepath: str) -> set:
    """
    Load the set of IXP route-server ASNs from a CSV file.
    Expected format: CSV with an 'asn' column.
    Falls back to an empty set if the file doesn't exist.
    """
    ixp_asns = set()
    if not os.path.exists(filepath):
        logger.warning(f"IXP ASN file not found: {filepath}. "
                       f"IXP ASN removal will be skipped.")
        return ixp_asns

    with open(filepath, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                ixp_asns.add(int(row['asn']))
            except (ValueError, KeyError):
                continue

    logger.info(f"Loaded {len(ixp_asns)} IXP route-server ASNs.")
    return ixp_asns


def load_special_asns(filepath: str) -> set:
    """
    Load additional special-purpose ASNs from a file (one ASN per line).
    This supplements the hardcoded IANA_SPECIAL_RANGES.
    """
    extra = set()
    if not os.path.exists(filepath):
        return extra

    with open(filepath, 'r') as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith('#'):
                continue
            try:
                extra.add(int(line))
            except ValueError:
                continue

    logger.info(f"Loaded {len(extra)} additional special-purpose ASNs.")
    return extra


def extract_edges_from_path(sanitized_path: list) -> list:
    """
    Extract AS-level edges from a sanitized AS path.

    Returns:
      List of (asn1, asn2) tuples representing adjacent AS pairs,
      sorted so the smaller ASN is first (canonical form).
    """
    edges = []
    for i in range(len(sanitized_path) - 1):
        edge = tuple(sorted([sanitized_path[i], sanitized_path[i + 1]]))
        edges.append(edge)
    return edges


# ---------------------------------------------------------------------------
# VNIX-facilitated edge record
# ---------------------------------------------------------------------------
class VNIXEdgeRecord:
    """Represents a peering edge facilitated by a VNIX route server."""
    __slots__ = ('asn_left', 'asn_right', 'vnix_asn', 'vnix_location')

    def __init__(self, asn_left: int, asn_right: int, vnix_asn: int, vnix_location: str):
        # Canonical ordering: smaller ASN first
        if asn_left <= asn_right:
            self.asn_left = asn_left
            self.asn_right = asn_right
        else:
            self.asn_left = asn_right
            self.asn_right = asn_left
        self.vnix_asn = vnix_asn
        self.vnix_location = vnix_location

    @property
    def edge_tuple(self):
        return (self.asn_left, self.asn_right)

    def __repr__(self):
        return (f"VNIXEdge(AS{self.asn_left} ↔ AS{self.asn_right} "
                f"via {self.vnix_location}/AS{self.vnix_asn})")


# ---------------------------------------------------------------------------
# Sanitization Statistics v2
# ---------------------------------------------------------------------------
class SanitizationStatsV2:
    """Track sanitization statistics with VNIX-specific counters."""

    def __init__(self):
        # General counters (same as v1)
        self.total_paths = 0
        self.paths_after_sanitize = 0
        self.paths_dropped_empty = 0
        self.special_asns_removed = 0
        self.prepends_removed = 0
        self.loops_removed = 0

        # IXP counters — split by type
        self.non_vnix_ixp_asns_removed = 0
        self.vnix_middle_removed = 0       # VNIX ASN stripped from middle (RS role)
        self.vnix_edge_retained = 0        # VNIX ASN kept at path edge (operator role)
        self.vnix_facilitated_edges = 0    # peering edges discovered through VNIX RS

    def report(self):
        """Print a summary of sanitization statistics."""
        total_ixp = self.non_vnix_ixp_asns_removed + self.vnix_middle_removed
        print("\n=== AS Path Sanitization Report (v2 — Context-Aware VNIX) ===")
        print(f"  Total raw paths processed:          {self.total_paths:,}")
        print(f"  Paths retained after sanitization:  {self.paths_after_sanitize:,}")
        print(f"  Paths dropped (empty/single):       {self.paths_dropped_empty:,}")
        print(f"  Special ASNs removed:               {self.special_asns_removed:,}")
        print(f"  Prepends removed:                   {self.prepends_removed:,}")
        print(f"  Loops removed:                      {self.loops_removed:,}")
        print(f"  ── IXP Route-Server Handling ──")
        print(f"  Non-VNIX IXP ASNs removed:          {self.non_vnix_ixp_asns_removed:,}")
        print(f"  VNIX ASNs removed (middle/RS role):  {self.vnix_middle_removed:,}")
        print(f"  VNIX ASNs retained (edge/operator):  {self.vnix_edge_retained:,}")
        print(f"  VNIX-facilitated edges discovered:   {self.vnix_facilitated_edges:,}")
        print(f"  Total IXP ASN removals:              {total_ixp:,}")
        print("=" * 60 + "\n")

    def to_report_string(self):
        """Return the report as a string (for writing to file)."""
        total_ixp = self.non_vnix_ixp_asns_removed + self.vnix_middle_removed
        lines = [
            "=== AS Path Sanitization Report (v2 — Context-Aware VNIX) ===",
            f"  Total raw paths processed:          {self.total_paths:,}",
            f"  Paths retained after sanitization:  {self.paths_after_sanitize:,}",
            f"  Paths dropped (empty/single):       {self.paths_dropped_empty:,}",
            f"  Special ASNs removed:               {self.special_asns_removed:,}",
            f"  Prepends removed:                   {self.prepends_removed:,}",
            f"  Loops removed:                      {self.loops_removed:,}",
            f"  ── IXP Route-Server Handling ──",
            f"  Non-VNIX IXP ASNs removed:          {self.non_vnix_ixp_asns_removed:,}",
            f"  VNIX ASNs removed (middle/RS role):  {self.vnix_middle_removed:,}",
            f"  VNIX ASNs retained (edge/operator):  {self.vnix_edge_retained:,}",
            f"  VNIX-facilitated edges discovered:   {self.vnix_facilitated_edges:,}",
            f"  Total IXP ASN removals:              {total_ixp:,}",
            "=" * 60,
        ]
        return "\n".join(lines)


# ---------------------------------------------------------------------------
# Context-Aware Sanitization
# ---------------------------------------------------------------------------
def sanitize_as_path_v2(
    raw_path: list,
    ixp_asns: set,
    special_asns: set,
    vnix_asns: dict = None,
    stats: SanitizationStatsV2 = None,
) -> tuple:
    """
    Sanitize a single AS path with context-aware VNIX handling.

    Parameters:
      raw_path:     List of ASN integers (raw from BGP dump).
      ixp_asns:     Set of all IXP route-server ASNs (includes VNIX).
      special_asns: Set of additional special-purpose ASNs.
      vnix_asns:    Dict mapping VNIX ASN -> location string.
                    If None, defaults to the module-level VNIX_RS_ASNS.
      stats:        Optional SanitizationStatsV2 to update.

    Returns:
      (sanitized_path, vnix_edges)
        sanitized_path: list of ASN ints (empty if dropped)
        vnix_edges:     list of VNIXEdgeRecord objects found in this path
    """
    if vnix_asns is None:
        vnix_asns = VNIX_RS_ASNS
    if stats:
        stats.total_paths += 1

    path = list(raw_path)
    vnix_edges_found = []

    # ── Step 1: Remove special-purpose ASNs ──────────────────────
    before = len(path)
    path = [asn for asn in path
            if not is_special_purpose_asn(asn) and asn not in special_asns]
    if stats:
        stats.special_asns_removed += (before - len(path))

    # ── Step 2: Remove prepending FIRST (so position logic works) ─
    if path:
        deduped = [path[0]]
        for asn in path[1:]:
            if asn != deduped[-1]:
                deduped.append(asn)
        if stats:
            stats.prepends_removed += (len(path) - len(deduped))
        path = deduped

    # ── Step 3: Context-aware IXP route-server removal ───────────
    #
    # We process the path in a single pass:
    #   - Non-VNIX IXP ASNs: always remove
    #   - VNIX ASNs in the MIDDLE: remove (RS role), record facilitated edge
    #   - VNIX ASNs at EDGE (first/last): keep (operator role)
    #
    # Important: we must determine positions BEFORE removing anything,
    # so we iterate once to classify, then build the cleaned path.

    clean_ixp = []
    path_len = len(path)

    for idx, asn in enumerate(path):
        is_first = (idx == 0)
        is_last = (idx == path_len - 1)

        if asn in vnix_asns:
            if is_first or is_last:
                # VNIX at path edge → VNNIC operational traffic → keep
                clean_ixp.append(asn)
                if stats:
                    stats.vnix_edge_retained += 1
            else:
                # VNIX in middle → route-server role → strip
                # Record the facilitated edge (left neighbor ↔ right neighbor)
                left_neighbor = path[idx - 1]
                right_neighbor = path[idx + 1]
                # Only record if neighbors are distinct (not a prepend artifact)
                if left_neighbor != right_neighbor:
                    record = VNIXEdgeRecord(
                        asn_left=left_neighbor,
                        asn_right=right_neighbor,
                        vnix_asn=asn,
                        vnix_location=vnix_asns[asn],
                    )
                    vnix_edges_found.append(record)
                    if stats:
                        stats.vnix_facilitated_edges += 1
                if stats:
                    stats.vnix_middle_removed += 1
                # Do NOT append to clean_ixp → stripped

        elif asn in ixp_asns:
            # Non-VNIX IXP route-server → always strip
            if stats:
                stats.non_vnix_ixp_asns_removed += 1

        else:
            # Normal ASN → keep
            clean_ixp.append(asn)

    path = clean_ixp

    # ── Step 4: Remove AS loops (non-adjacent repetition) ────────
    seen = set()
    clean = []
    for asn in path:
        if asn not in seen:
            seen.add(asn)
            clean.append(asn)
    if stats:
        stats.loops_removed += (len(path) - len(clean))
    path = clean

    # ── Step 5: Filter — single-ASN path carries no info ─────────
    if len(path) < 2:
        if stats:
            stats.paths_dropped_empty += 1
        return ([], vnix_edges_found)

    if stats:
        stats.paths_after_sanitize += 1
    return (path, vnix_edges_found)
