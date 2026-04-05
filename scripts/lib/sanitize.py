"""
AS Path Sanitization Module
============================
Reference: Step (3) of the AS topology mapping methodology.

Cleans raw BGP AS paths by removing:
  1. IXP route-server ASNs (from multilateral peering at IXPs)
  2. Prepended ASNs (consecutive duplicates from traffic engineering)
  3. AS loops (non-adjacent repetition of same ASN in a path)
  4. Single-ASN paths (carry no relationship information)
  5. Special-purpose ASNs (IANA reserved ranges)
"""

import os
import csv
import logging

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# IANA Special-Purpose ASN Ranges
# Source: https://www.iana.org/assignments/iana-as-numbers-special-registry
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


def sanitize_as_path(
    raw_path: list,
    ixp_asns: set = None,
    special_asns: set = None,
) -> list:
    """
    Sanitize a single AS path (list of ASN integers).

    Steps applied in order:
      1. Remove special-purpose ASNs (IANA reserved ranges + custom set)
      2. Remove IXP route-server ASNs
      3. Remove prepending (consecutive duplicate ASNs)
      4. Remove AS loops (non-adjacent duplicate ASNs)

    Returns:
      Sanitized path as a list of ASN integers.
      Returns empty list if the path becomes empty or single-ASN.
    """
    if ixp_asns is None:
        ixp_asns = set()
    if special_asns is None:
        special_asns = set()

    path = raw_path

    # Step 1: Remove special-purpose ASNs
    path = [asn for asn in path if not is_special_purpose_asn(asn) and asn not in special_asns]

    # Step 2: Remove IXP route-server ASNs
    path = [asn for asn in path if asn not in ixp_asns]

    # Step 3: Remove prepending (consecutive duplicates)
    if path:
        deduped = [path[0]]
        for asn in path[1:]:
            if asn != deduped[-1]:
                deduped.append(asn)
        path = deduped

    # Step 4: Remove AS loops (non-adjacent repetition)
    # Strategy: keep only the first occurrence of each ASN
    seen = set()
    clean = []
    for asn in path:
        if asn not in seen:
            seen.add(asn)
            clean.append(asn)
    path = clean

    # Step 5: Filter — a single-ASN path carries no relationship info
    if len(path) < 2:
        return []

    return path


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


class SanitizationStats:
    """Track sanitization statistics for reporting."""

    def __init__(self):
        self.total_paths = 0
        self.paths_after_sanitize = 0
        self.paths_dropped_empty = 0
        self.paths_dropped_single = 0
        self.ixp_asns_removed = 0
        self.special_asns_removed = 0
        self.prepends_removed = 0
        self.loops_removed = 0

    def report(self):
        """Print a summary of sanitization statistics."""
        print("\n=== AS Path Sanitization Report ===")
        print(f"  Total raw paths processed:  {self.total_paths}")
        print(f"  Paths retained after sanitization: {self.paths_after_sanitize}")
        print(f"  Paths dropped (empty/single):      {self.paths_dropped_empty}")
        print(f"  IXP ASNs removed:     {self.ixp_asns_removed}")
        print(f"  Special ASNs removed:  {self.special_asns_removed}")
        print(f"  Prepends removed:      {self.prepends_removed}")
        print(f"  Loops removed:         {self.loops_removed}")
        print("===================================\n")


def sanitize_as_path_with_stats(
    raw_path: list,
    ixp_asns: set,
    special_asns: set,
    stats: SanitizationStats,
) -> list:
    """
    Same as sanitize_as_path but updates a SanitizationStats object.
    """
    stats.total_paths += 1
    path = raw_path

    # Step 1: Remove special-purpose ASNs
    before = len(path)
    path = [asn for asn in path if not is_special_purpose_asn(asn) and asn not in special_asns]
    stats.special_asns_removed += (before - len(path))

    # Step 2: Remove IXP route-server ASNs
    before = len(path)
    path = [asn for asn in path if asn not in ixp_asns]
    stats.ixp_asns_removed += (before - len(path))

    # Step 3: Remove prepending
    if path:
        deduped = [path[0]]
        for asn in path[1:]:
            if asn != deduped[-1]:
                deduped.append(asn)
        stats.prepends_removed += (len(path) - len(deduped))
        path = deduped

    # Step 4: Remove AS loops
    seen = set()
    clean = []
    for asn in path:
        if asn not in seen:
            seen.add(asn)
            clean.append(asn)
    stats.loops_removed += (len(path) - len(clean))
    path = clean

    # Step 5: Filter single/empty
    if len(path) < 2:
        stats.paths_dropped_empty += 1
        return []

    stats.paths_after_sanitize += 1
    return path
