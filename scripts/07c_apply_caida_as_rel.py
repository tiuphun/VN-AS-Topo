"""
07c_apply_caida_as_rel.py
==========================
Classifies VN domestic edges using CAIDA's AS Relationship dataset (serial-2).

Reference: Step (4) of the methodology.

CAIDA serial-2 format:
  <provider-asn>|<customer-asn>|-1|<source>   (provider → customer)
  <peer-asn>|<peer-asn>|0|<source>            (peer ↔ peer)

Input:
  - data/processed/vn_bgp_edges.csv (from 07)
  - data/raw/20260101.as-rel2.txt.bz2 (or any *as-rel2* file)

Output:
  - data/processed/vn_edges_classified.csv
"""

import pandas as pd
import os
import bz2
import gzip
import glob


def load_caida_as_rel(filepath: str) -> dict:
    """
    Parse CAIDA AS Relationship file (serial-2 format).

    Returns a dict: (min_asn, max_asn) -> {type, provider, customer}
    The key is canonical: (smaller_asn, larger_asn).
    """
    relationships = {}

    # Handle compressed formats
    if filepath.endswith('.bz2'):
        opener = lambda p: bz2.open(p, 'rt')
    elif filepath.endswith('.gz'):
        opener = lambda p: gzip.open(p, 'rt')
    else:
        opener = lambda p: open(p, 'r')

    print(f"  Loading {os.path.basename(filepath)}...")

    with opener(filepath) as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith('#'):
                continue

            parts = line.split('|')
            if len(parts) < 3:
                continue

            try:
                asn1 = int(parts[0])
                asn2 = int(parts[1])
                rel = int(parts[2])
            except ValueError:
                continue

            canonical_key = tuple(sorted([asn1, asn2]))

            if rel == -1:
                # asn1 is provider of asn2
                relationships[canonical_key] = {
                    'type': 'p2c',
                    'provider': asn1,
                    'customer': asn2,
                }
            elif rel == 0:
                relationships[canonical_key] = {
                    'type': 'p2p',
                    'provider': None,
                    'customer': None,
                }

    print(f"    {len(relationships)} AS relationships loaded.")
    return relationships


def classify_edges():
    base_dir = os.path.join(os.path.dirname(__file__), '..')
    processed_dir = os.path.join(base_dir, 'data', 'processed')
    raw_dir = os.path.join(base_dir, 'data', 'raw')

    # ── Load edges ───────────────────────────────────────────────
    edges_path = os.path.join(processed_dir, 'vn_bgp_edges.csv')
    if not os.path.exists(edges_path):
        print("ERROR: vn_bgp_edges.csv not found. Run 07_parse_routeviews.py first.")
        return

    df = pd.read_csv(edges_path)
    print(f"Loaded {len(df)} edges to classify.\n")

    # ── Find CAIDA AS-Rel file ───────────────────────────────────
    rel_files = glob.glob(os.path.join(raw_dir, '*as-rel2*'))
    if not rel_files:
        print("ERROR: No CAIDA AS-Rel file found in data/raw/.")
        print("  Run: python scripts/02b_fetch_caida_datasets.py")
        return

    # Use the most recent one
    caida_path = sorted(rel_files)[-1]

    # ── Load CAIDA data ──────────────────────────────────────────
    relationships = load_caida_as_rel(caida_path)

    # ── Classify each edge ───────────────────────────────────────
    print("\nClassifying edges...")
    rel_types = []
    providers = []
    customers = []
    matched = 0

    for _, row in df.iterrows():
        key = tuple(sorted([int(row['asn1']), int(row['asn2'])]))

        if key in relationships:
            rel = relationships[key]
            rel_types.append(rel['type'])
            providers.append(rel.get('provider', ''))
            customers.append(rel.get('customer', ''))
            matched += 1
        else:
            rel_types.append('unknown')
            providers.append('')
            customers.append('')

    df['relationship'] = rel_types
    df['provider_asn'] = providers
    df['customer_asn'] = customers

    # ── Summary ──────────────────────────────────────────────────
    print(f"\nClassification Results:")
    print(f"  Matched in CAIDA: {matched}/{len(df)} ({matched/len(df)*100:.1f}%)")
    counts = df['relationship'].value_counts()
    for rel_type, count in counts.items():
        pct = count / len(df) * 100
        print(f"  {rel_type}: {count} ({pct:.1f}%)")

    # ── Save ─────────────────────────────────────────────────────
    output_path = os.path.join(processed_dir, 'vn_edges_classified.csv')
    df.to_csv(output_path, index=False)
    print(f"\n✓ Saved to {output_path}")

    # Show some classified edges
    classified = df[df['relationship'] != 'unknown'].head(10)
    if len(classified) > 0:
        print("\nSample classified edges:")
        for _, r in classified.iterrows():
            if r['relationship'] == 'p2c':
                print(f"  AS{int(r['provider_asn'])} → AS{int(r['customer_asn'])} (provider→customer)")
            else:
                print(f"  AS{int(r['asn1'])} ↔ AS{int(r['asn2'])} (peer-to-peer)")


if __name__ == "__main__":
    classify_edges()
