"""
07_parse_routeviews.py (REFACTORED)
=====================================
Parses MRT RIB dump files using bgpdump and extracts domestic VN-VN
AS edges with full AS path sanitization applied.

Reference: Steps (1), (3), (4) of the methodology.

Changes from original:
  - Integrates AS path sanitization (IXP removal, prepend removal,
    loop removal, special-ASN removal)
  - Reports sanitization statistics
  - Cleaner output format with source tracking

Input:
  - data/routeviews/*.bz2 (MRT RIB dumps)
  - data/processed/vn_asns.csv
  - data/processed/ixp_asns_global.csv (from 03b)
  - data/raw/iana_special_asns.txt (from 04)

Output:
  - data/processed/vn_bgp_edges.csv (sanitized domestic edges)
"""

import pandas as pd
import glob
import os
import subprocess
import re
import sys

# Add parent dir for lib imports
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from lib.sanitize import (
    load_ixp_asns,
    load_special_asns,
    sanitize_as_path_with_stats,
    extract_edges_from_path,
    SanitizationStats,
)


def parse_bgpdump_sanitized():
    base_dir = os.path.join(os.path.dirname(__file__), '..')
    processed_dir = os.path.join(base_dir, 'data', 'processed')
    routeviews_dir = os.path.join(base_dir, 'data', 'routeviews')
    raw_dir = os.path.join(base_dir, 'data', 'raw')

    # ── Load VN ASNs ─────────────────────────────────────────────
    print("Loading VN ASNs...")
    try:
        df_asns = pd.read_csv(os.path.join(processed_dir, 'vn_asns.csv'))
        vn_asns = set(df_asns['asn'].astype(int).tolist())
        print(f"  {len(vn_asns)} VN ASNs loaded.")
    except Exception as e:
        print(f"Error loading VN ASNs: {e}")
        return

    # ── Load sanitization datasets ───────────────────────────────
    ixp_asns = load_ixp_asns(os.path.join(processed_dir, 'ixp_asns_global.csv'))
    special_asns = load_special_asns(os.path.join(raw_dir, 'iana_special_asns.txt'))

    print(f"  {len(ixp_asns)} IXP route-server ASNs loaded.")
    print(f"  {len(special_asns)} additional special-purpose ASNs loaded.")

    # ── Find MRT files ───────────────────────────────────────────
    mrt_files = sorted(glob.glob(os.path.join(routeviews_dir, '*.bz2')))
    if not mrt_files:
        print("No MRT files found in data/routeviews/")
        return

    print(f"\nFound {len(mrt_files)} MRT files to process.")

    # ── Process each MRT file ────────────────────────────────────
    stats = SanitizationStats()
    vn_edges = {}  # edge_tuple -> set of source files

    for f in mrt_files:
        fname = os.path.basename(f)
        print(f"\n── Processing {fname} ──")
        try:
            process = subprocess.Popen(
                ['bgpdump', '-m', f],
                stdout=subprocess.PIPE,
                stderr=subprocess.DEVNULL,
                text=True,
            )

            line_count = 0
            for line in process.stdout:
                line_count += 1
                if line_count % 2_000_000 == 0:
                    print(f"  {line_count:,} routes parsed...")

                parts = line.split('|')
                if len(parts) < 7:
                    continue

                as_path_str = parts[6]
                # Parse AS path: handle AS_SET {x,y,z} by taking first element
                raw_asns = []
                for token in as_path_str.split():
                    token = token.strip('{}')
                    # Handle AS_SET notation
                    for sub in token.split(','):
                        sub = sub.strip()
                        if sub.isdigit():
                            raw_asns.append(int(sub))

                if not raw_asns:
                    continue

                # Sanitize the AS path
                clean_path = sanitize_as_path_with_stats(
                    raw_asns, ixp_asns, special_asns, stats
                )

                if not clean_path:
                    continue

                # Extract edges — domestic only (both ASNs must be VN)
                edges = extract_edges_from_path(clean_path)
                for edge in edges:
                    if edge[0] in vn_asns and edge[1] in vn_asns:
                        if edge not in vn_edges:
                            vn_edges[edge] = set()
                        vn_edges[edge].add(fname)

            process.wait()
            print(f"  Finished {fname}: {line_count:,} routes.")

        except FileNotFoundError:
            print("ERROR: bgpdump not found. Install it with: brew install bgpdump")
            return
        except Exception as e:
            print(f"  Error processing {fname}: {e}")

    # ── Report ───────────────────────────────────────────────────
    stats.report()

    print(f"Domestic VN↔VN edges found: {len(vn_edges)}")

    # ── Save ─────────────────────────────────────────────────────
    if vn_edges:
        records = []
        for (asn1, asn2), sources in sorted(vn_edges.items()):
            records.append({
                'asn1': asn1,
                'asn2': asn2,
                'num_sources': len(sources),
                'sources': ';'.join(sorted(sources)),
            })

        df_edges = pd.DataFrame(records)
        output_path = os.path.join(processed_dir, 'vn_bgp_edges.csv')
        df_edges.to_csv(output_path, index=False)
        print(f"Saved {len(df_edges)} sanitized edges to {output_path}")
    else:
        print("No edges found!")


if __name__ == "__main__":
    parse_bgpdump_sanitized()
