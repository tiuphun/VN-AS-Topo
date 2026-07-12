"""
07v2_parse_routeviews.py — Context-Aware VNIX Sanitization
============================================================
Parses MRT RIB dump files using bgpdump and extracts domestic VN-VN
AS edges with CONTEXT-AWARE VNIX route-server handling.

Differences from 07_parse_routeviews.py:
  - Uses sanitize_v2 (position-aware VNIX handling)
  - No separate pre-sanitization VNIX detection hack — it's integrated
  - Richer VNIX edge metadata (which exchange point, which RS ASN)
  - All output files use _v2 suffix (no overwriting)

Input:
  - data/routeviews/*.bz2 (MRT RIB dumps)
  - data/processed/vn_asns.csv
  - data/processed/ixp_asns_global.csv (from 03b)
  - data/raw/iana_special_asns.txt (from 04)

Output:
  - data/processed/vn_bgp_edges_v2.csv
  - data/processed/vn_bgp_vnix_edges_v2.csv
  - data/processed/vnix_sanitization_report_v2.txt
"""

import pandas as pd
import glob
import os
import subprocess
import sys

# Add parent dir for lib imports
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from lib.sanitize_v2 import (
    load_ixp_asns,
    load_special_asns,
    sanitize_as_path_v2,
    extract_edges_from_path,
    SanitizationStatsV2,
    VNIX_RS_ASNS,
)


def parse_bgpdump_v2():
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
    print(f"  VNIX route-server ASNs (context-aware): {sorted(VNIX_RS_ASNS.keys())}")

    # ── Find MRT files ───────────────────────────────────────────
    mrt_files = sorted(glob.glob(os.path.join(routeviews_dir, '*.bz2')))
    if not mrt_files:
        print("No MRT files found in data/routeviews/")
        return

    print(f"\nFound {len(mrt_files)} MRT files to process.")

    # ── Process each MRT file ────────────────────────────────────
    stats = SanitizationStatsV2()
    vn_edges = {}         # edge_tuple -> set of source files
    vnix_edges = {}       # (edge_tuple, vnix_asn) -> {sources, vnix_location}

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

                # ── Sanitize with context-aware VNIX handling ────
                clean_path, vnix_records = sanitize_as_path_v2(
                    raw_asns, ixp_asns, special_asns,
                    vnix_asns=VNIX_RS_ASNS, stats=stats,
                )

                # Collect VNIX-facilitated edges (regardless of domesticity)
                for rec in vnix_records:
                    key = (rec.edge_tuple, rec.vnix_asn)
                    if key not in vnix_edges:
                        vnix_edges[key] = {
                            'vnix_location': rec.vnix_location,
                            'sources': set(),
                        }
                    vnix_edges[key]['sources'].add(fname)

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

    # Filter VNIX-facilitated edges to domestic only
    vnix_domestic = {}
    vnix_all = {}
    for (edge_tuple, vnix_asn), info in vnix_edges.items():
        vnix_all[(edge_tuple, vnix_asn)] = info
        if edge_tuple[0] in vn_asns and edge_tuple[1] in vn_asns:
            vnix_domestic[(edge_tuple, vnix_asn)] = info

    print(f"Domestic VN↔VN edges found: {len(vn_edges)}")
    print(f"VNIX-facilitated edges (total): {len(vnix_all)}")
    print(f"VNIX-facilitated edges (domestic VN↔VN only): {len(vnix_domestic)}")

    # Unique domestic edges via VNIX (ignoring which RS)
    unique_vnix_domestic = set(et for (et, _) in vnix_domestic.keys())
    print(f"Unique domestic edge pairs via VNIX: {len(unique_vnix_domestic)}")

    if len(vn_edges) > 0:
        vnix_pct = (len(unique_vnix_domestic) / len(vn_edges)) * 100
        print(f"Percentage of domestic edges facilitated by VNIX: {vnix_pct:.2f}%")

    # Breakdown by exchange point
    by_location = {}
    for (_, vnix_asn), info in vnix_domestic.items():
        loc = info['vnix_location']
        by_location.setdefault(loc, 0)
        by_location[loc] += 1
    if by_location:
        print("\nVNIX-facilitated edges by exchange point:")
        for loc, count in sorted(by_location.items()):
            print(f"  {loc}: {count}")

    # ── Save domestic edges ──────────────────────────────────────
    if vn_edges:
        records = []
        for (asn1, asn2), sources in sorted(vn_edges.items()):
            # Check if this edge was also found via VNIX
            via_vnix_locs = set()
            for (et, vasn), info in vnix_domestic.items():
                if et == (asn1, asn2):
                    via_vnix_locs.add(info['vnix_location'])

            records.append({
                'asn1': asn1,
                'asn2': asn2,
                'num_sources': len(sources),
                'sources': ';'.join(sorted(sources)),
                'via_vnix': bool(via_vnix_locs),
                'vnix_locations': ';'.join(sorted(via_vnix_locs)) if via_vnix_locs else '',
            })

        df_edges = pd.DataFrame(records)
        output_path = os.path.join(processed_dir, 'vn_bgp_edges_v2.csv')
        df_edges.to_csv(output_path, index=False)
        print(f"\nSaved {len(df_edges)} sanitized edges to {output_path}")

    # ── Save VNIX-facilitated edges (domestic) ───────────────────
    if vnix_domestic:
        vnix_records = []
        for (edge_tuple, vnix_asn), info in sorted(vnix_domestic.items()):
            vnix_records.append({
                'asn1': edge_tuple[0],
                'asn2': edge_tuple[1],
                'vnix_asn': vnix_asn,
                'vnix_location': info['vnix_location'],
                'num_sources': len(info['sources']),
                'sources': ';'.join(sorted(info['sources'])),
            })
        df_vnix = pd.DataFrame(vnix_records)
        vnix_output_path = os.path.join(processed_dir, 'vn_bgp_vnix_edges_v2.csv')
        df_vnix.to_csv(vnix_output_path, index=False)
        print(f"Saved {len(df_vnix)} VNIX-facilitated edges to {vnix_output_path}")
    else:
        print("No VNIX-facilitated domestic edges found.")

    # ── Save sanitization report ─────────────────────────────────
    report_path = os.path.join(processed_dir, 'vnix_sanitization_report_v2.txt')
    with open(report_path, 'w') as f:
        f.write(stats.to_report_string())
        f.write("\n\n")
        f.write(f"Domestic VN↔VN edges: {len(vn_edges)}\n")
        f.write(f"VNIX-facilitated domestic edges: {len(vnix_domestic)}\n")
        f.write(f"Unique domestic edge pairs via VNIX: {len(unique_vnix_domestic)}\n")
        if by_location:
            f.write("\nVNIX breakdown by exchange:\n")
            for loc, count in sorted(by_location.items()):
                f.write(f"  {loc}: {count}\n")
    print(f"Saved report to {report_path}")


if __name__ == "__main__":
    parse_bgpdump_v2()
