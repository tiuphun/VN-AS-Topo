"""
11_compare_sanitization.py
===========================
Diagnostic script that compares old (v1) vs new (v2) sanitization results.

Loads both edge files side-by-side and reports:
  - Edges in common
  - Edges gained in v2 (not in v1)
  - Edges lost in v2 (in v1 but not v2)
  - VNIX edge coverage comparison
  - Classification comparison (if classified files exist)

Input:
  - data/processed/vn_bgp_edges.csv (v1)
  - data/processed/vn_bgp_edges_v2.csv (v2)
  - data/processed/vn_bgp_vnix_edges.csv (v1)
  - data/processed/vn_bgp_vnix_edges_v2.csv (v2)
  - data/processed/vn_edges_classified.csv (v1, optional)
  - data/processed/vn_edges_classified_v2.csv (v2, optional)

Output:
  - Console report
"""

import pandas as pd
import os


def load_edges(filepath: str) -> set:
    """Load edge tuples from a CSV with asn1, asn2 columns."""
    if not os.path.exists(filepath):
        return None
    df = pd.read_csv(filepath)
    return set(zip(df['asn1'].astype(int), df['asn2'].astype(int)))


def compare():
    base_dir = os.path.join(os.path.dirname(__file__), '..')
    processed_dir = os.path.join(base_dir, 'data', 'processed')

    print("=" * 70)
    print("  Sanitization Comparison: v1 (original) vs v2 (context-aware VNIX)")
    print("=" * 70)

    # ── 1. Compare domestic BGP edges ────────────────────────────
    print("\n── Domestic BGP Edges ──\n")

    v1_path = os.path.join(processed_dir, 'vn_bgp_edges.csv')
    v2_path = os.path.join(processed_dir, 'vn_bgp_edges_v2.csv')

    edges_v1 = load_edges(v1_path)
    edges_v2 = load_edges(v2_path)

    if edges_v1 is None:
        print(f"  ✗ v1 edges not found: {v1_path}")
        return
    if edges_v2 is None:
        print(f"  ✗ v2 edges not found: {v2_path}")
        print(f"    → Run: python 07v2_parse_routeviews.py")
        return

    common = edges_v1 & edges_v2
    gained = edges_v2 - edges_v1
    lost = edges_v1 - edges_v2

    print(f"  v1 edges:  {len(edges_v1):,}")
    print(f"  v2 edges:  {len(edges_v2):,}")
    print(f"  In common: {len(common):,}")
    print(f"  Gained (in v2, not v1): {len(gained):,}")
    print(f"  Lost (in v1, not v2):   {len(lost):,}")
    print(f"  Net change: {len(edges_v2) - len(edges_v1):+,}")

    # Show gained edges (likely involving VNIX ASNs at path edges)
    VNIX_ASNS = {23899, 23962, 56156}
    if gained:
        vnix_gained = [e for e in gained if e[0] in VNIX_ASNS or e[1] in VNIX_ASNS]
        non_vnix_gained = [e for e in gained if e[0] not in VNIX_ASNS and e[1] not in VNIX_ASNS]
        print(f"\n  Gained edges involving VNIX ASNs: {len(vnix_gained)}")
        for a1, a2 in sorted(vnix_gained):
            vnix_label = ""
            if a1 in VNIX_ASNS:
                vnix_label = f" ← AS{a1} is VNIX RS"
            elif a2 in VNIX_ASNS:
                vnix_label = f" ← AS{a2} is VNIX RS"
            print(f"    AS{a1} ↔ AS{a2}{vnix_label}")
        if non_vnix_gained:
            print(f"  Gained edges NOT involving VNIX: {len(non_vnix_gained)}")
            for a1, a2 in sorted(non_vnix_gained)[:10]:
                print(f"    AS{a1} ↔ AS{a2}")
            if len(non_vnix_gained) > 10:
                print(f"    ... and {len(non_vnix_gained) - 10} more")

    if lost:
        print(f"\n  Lost edges (in v1 but not v2):")
        for a1, a2 in sorted(lost)[:10]:
            print(f"    AS{a1} ↔ AS{a2}")
        if len(lost) > 10:
            print(f"    ... and {len(lost) - 10} more")

    # ── 2. Compare VNIX edge files ───────────────────────────────
    print("\n── VNIX-Facilitated Edges ──\n")

    vnix_v1_path = os.path.join(processed_dir, 'vn_bgp_vnix_edges.csv')
    vnix_v2_path = os.path.join(processed_dir, 'vn_bgp_vnix_edges_v2.csv')

    if os.path.exists(vnix_v1_path):
        df_vnix_v1 = pd.read_csv(vnix_v1_path)
        vnix_v1_edges = set(zip(df_vnix_v1['asn1'].astype(int), df_vnix_v1['asn2'].astype(int)))
        print(f"  v1 VNIX edges: {len(df_vnix_v1)} records, {len(vnix_v1_edges)} unique pairs")
    else:
        print(f"  v1 VNIX edges: not found")
        vnix_v1_edges = set()

    if os.path.exists(vnix_v2_path):
        df_vnix_v2 = pd.read_csv(vnix_v2_path)
        vnix_v2_pairs = set(zip(df_vnix_v2['asn1'].astype(int), df_vnix_v2['asn2'].astype(int)))
        print(f"  v2 VNIX edges: {len(df_vnix_v2)} records, {len(vnix_v2_pairs)} unique pairs")

        # Show breakdown by exchange
        if 'vnix_location' in df_vnix_v2.columns:
            print("\n  v2 VNIX edges by exchange point:")
            for loc, count in df_vnix_v2['vnix_location'].value_counts().items():
                print(f"    {loc}: {count}")

        # Show which route server was used
        if 'vnix_asn' in df_vnix_v2.columns:
            print("\n  v2 VNIX edges by route-server ASN:")
            for asn, count in df_vnix_v2['vnix_asn'].value_counts().items():
                print(f"    AS{asn}: {count}")
    else:
        print(f"  v2 VNIX edges: not found")
        vnix_v2_pairs = set()

    if vnix_v1_edges and vnix_v2_pairs:
        vnix_common = vnix_v1_edges & vnix_v2_pairs
        vnix_gained = vnix_v2_pairs - vnix_v1_edges
        vnix_lost = vnix_v1_edges - vnix_v2_pairs
        print(f"\n  VNIX common: {len(vnix_common)}")
        print(f"  VNIX gained (v2 only): {len(vnix_gained)}")
        print(f"  VNIX lost (v1 only):   {len(vnix_lost)}")

    # ── 3. Compare classified edges (if available) ───────────────
    print("\n── Classified Edges ──\n")

    cls_v1_path = os.path.join(processed_dir, 'vn_edges_classified.csv')
    cls_v2_path = os.path.join(processed_dir, 'vn_edges_classified_v2.csv')

    if os.path.exists(cls_v1_path) and os.path.exists(cls_v2_path):
        df_cls_v1 = pd.read_csv(cls_v1_path)
        df_cls_v2 = pd.read_csv(cls_v2_path)

        print(f"  v1 classified: {len(df_cls_v1)} edges")
        print(f"  v2 classified: {len(df_cls_v2)} edges")

        print("\n  v1 relationship breakdown:")
        for rt, cnt in df_cls_v1['relationship'].value_counts().items():
            print(f"    {rt}: {cnt} ({cnt/len(df_cls_v1)*100:.1f}%)")

        print("\n  v2 relationship breakdown:")
        for rt, cnt in df_cls_v2['relationship'].value_counts().items():
            print(f"    {rt}: {cnt} ({cnt/len(df_cls_v2)*100:.1f}%)")

        # VNIX-specific classification in v2
        if 'via_vnix' in df_cls_v2.columns:
            vnix_cls = df_cls_v2[df_cls_v2['via_vnix'] == True]
            if len(vnix_cls) > 0:
                print(f"\n  v2 VNIX-facilitated edges classification:")
                for rt, cnt in vnix_cls['relationship'].value_counts().items():
                    print(f"    {rt}: {cnt}")
    else:
        if not os.path.exists(cls_v1_path):
            print(f"  v1 classified: not found")
        if not os.path.exists(cls_v2_path):
            print(f"  v2 classified: not found")
            print(f"    → Run: python 07cv2_apply_caida_as_rel.py")

    # ── 4. via_vnix flag analysis on v2 edges ────────────────────
    if edges_v2 and os.path.exists(v2_path):
        df_v2 = pd.read_csv(v2_path)
        if 'via_vnix' in df_v2.columns:
            print("\n── v2 Domestic Edges: VNIX Facilitation ──\n")
            via_vnix_count = df_v2['via_vnix'].sum()
            total = len(df_v2)
            print(f"  Total domestic edges: {total}")
            print(f"  Facilitated by VNIX: {int(via_vnix_count)} ({via_vnix_count/total*100:.1f}%)")
            print(f"  Not via VNIX:        {total - int(via_vnix_count)} ({(total-via_vnix_count)/total*100:.1f}%)")

            if 'vnix_locations' in df_v2.columns:
                vnix_rows = df_v2[df_v2['via_vnix'] == True]
                if len(vnix_rows) > 0:
                    print("\n  Sample VNIX-facilitated edges:")
                    for _, r in vnix_rows.head(15).iterrows():
                        print(f"    AS{int(r['asn1'])} ↔ AS{int(r['asn2'])} via {r['vnix_locations']}")

    print("\n" + "=" * 70)
    print("  Comparison Complete")
    print("=" * 70)


if __name__ == "__main__":
    compare()
