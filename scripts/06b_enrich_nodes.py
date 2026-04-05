"""
06b_enrich_nodes.py
====================
Enriches VN ASN nodes with organizational information from:
  1. CAIDA AS-Org dataset (as-org2info.txt.gz) — org name, country
  2. CAIDA AS-Rank API data (caida_asrank_vn.jsonl) — rank, cone size, degree

Reference: Step (6) of the methodology.

CAIDA AS-Org file format (single file, two sections):
  # format: aut|changed|aut_name|org_id|opaque_id|source
  1|20120224|LVLT-1|LVLT-ARIN|...|ARIN
  # format: org_id|changed|org_name|country|source
  LVLT-ARIN|20120130|Level 3 Communications, Inc.|US|ARIN

Input:
  - data/processed/vn_asns.csv
  - data/raw/20260101.as-org2info.txt.gz (or any *as-org2info* file)
  - data/raw/caida_asrank_vn.jsonl (optional, from AS-Rank API)

Output:
  - data/processed/vn_asns_enriched.csv
"""

import pandas as pd
import os
import gzip
import glob
import json


def load_caida_as_org(filepath: str) -> dict:
    """
    Parse the CAIDA as-org2info file (single file, two sections).
    Returns dict: ASN (int) -> {org_name, country, aut_name, source}
    """
    orgs = {}       # org_id -> {org_name, country}
    asn_to_org = {} # aut (ASN) -> org_id
    asn_names = {}  # aut (ASN) -> aut_name

    # Determine how to open the file
    if filepath.endswith('.gz'):
        opener = lambda p: gzip.open(p, 'rt', encoding='utf-8', errors='replace')
    else:
        opener = lambda p: open(p, 'r', encoding='utf-8', errors='replace')

    current_format = None

    print(f"  Parsing {os.path.basename(filepath)}...")
    with opener(filepath) as f:
        for line in f:
            line = line.strip()
            if not line:
                continue

            # Detect section headers
            if line.startswith('# format:'):
                if 'aut|' in line:
                    current_format = 'aut'
                elif 'org_id|' in line:
                    current_format = 'org'
                continue

            if line.startswith('#'):
                continue

            parts = line.split('|')

            if current_format == 'aut' and len(parts) >= 4:
                try:
                    asn = int(parts[0].strip())
                except ValueError:
                    continue
                aut_name = parts[2].strip()
                org_id = parts[3].strip()
                asn_to_org[asn] = org_id
                asn_names[asn] = aut_name

            elif current_format == 'org' and len(parts) >= 4:
                org_id = parts[0].strip()
                org_name = parts[2].strip()
                country = parts[3].strip()
                orgs[org_id] = {'org_name': org_name, 'country': country}

    # Merge: ASN -> full info
    result = {}
    for asn, org_id in asn_to_org.items():
        if org_id in orgs:
            result[asn] = {
                'org_name': orgs[org_id]['org_name'],
                'country': orgs[org_id]['country'],
                'aut_name': asn_names.get(asn, ''),
            }
        else:
            result[asn] = {
                'org_name': asn_names.get(asn, ''),
                'country': '',
                'aut_name': asn_names.get(asn, ''),
            }

    print(f"    {len(orgs)} organizations, {len(asn_to_org)} ASN mappings loaded.")
    return result


def load_asrank_data(filepath: str) -> dict:
    """
    Parse the AS-Rank JSONL file (from GraphQL API).
    Returns dict: ASN (int) -> {rank, cone_asns, cone_prefixes, degree_*}
    """
    result = {}
    if not os.path.exists(filepath):
        return result

    print(f"  Parsing {os.path.basename(filepath)}...")
    with open(filepath, 'r') as f:
        for line in f:
            try:
                record = json.loads(line.strip())
            except json.JSONDecodeError:
                continue

            try:
                asn = int(record.get('asn', 0))
            except (ValueError, TypeError):
                continue

            cone = record.get('cone', {}) or {}
            degree = record.get('asnDegree', {}) or {}
            org = record.get('organization', {}) or {}

            result[asn] = {
                'rank': record.get('rank', ''),
                'asn_name': record.get('asnName', ''),
                'clique_member': record.get('cliqueMember', False),
                'seen': record.get('seen', False),
                'cone_asns': cone.get('numberAsns', ''),
                'cone_prefixes': cone.get('numberPrefixes', ''),
                'cone_addresses': cone.get('numberAddresses', ''),
                'degree_provider': degree.get('provider', ''),
                'degree_peer': degree.get('peer', ''),
                'degree_customer': degree.get('customer', ''),
                'degree_transit': degree.get('transit', ''),
                'degree_total': degree.get('total', ''),
                'org_id': org.get('orgId', ''),
                'org_name_rank': org.get('orgName', ''),
            }

    print(f"    {len(result)} AS Rank records loaded.")
    return result


def enrich_nodes():
    base_dir = os.path.join(os.path.dirname(__file__), '..')
    processed_dir = os.path.join(base_dir, 'data', 'processed')
    raw_dir = os.path.join(base_dir, 'data', 'raw')

    # ── Load VN ASNs ─────────────────────────────────────────────
    asns_path = os.path.join(processed_dir, 'vn_asns.csv')
    if not os.path.exists(asns_path):
        print("ERROR: vn_asns.csv not found. Run 01_fetch_vn_asns.py first.")
        return

    df = pd.read_csv(asns_path)
    print(f"Loaded {len(df)} VN ASNs.\n")

    # ── Find & load CAIDA AS-Org file ────────────────────────────
    as_org_data = {}
    # Search for any as-org2info file in data/raw/
    org_files = glob.glob(os.path.join(raw_dir, '*as-org2info*'))
    if org_files:
        # Use the most recent one
        org_file = sorted(org_files)[-1]
        as_org_data = load_caida_as_org(org_file)
    else:
        print("  ⚠ No as-org2info file found in data/raw/.")
        print("    Run: python scripts/02b_fetch_caida_datasets.py")

    # ── Load AS-Rank data ────────────────────────────────────────
    asrank_path = os.path.join(raw_dir, 'caida_asrank_vn.jsonl')
    asrank_data = load_asrank_data(asrank_path)

    # ── Enrich each ASN ──────────────────────────────────────────
    print("\nEnriching ASN records...")

    enrichment = {
        'org_name': [], 'country': [], 'aut_name': [],
        'rank': [], 'cone_asns': [], 'cone_prefixes': [],
        'degree_provider': [], 'degree_peer': [],
        'degree_customer': [], 'degree_transit': [],
        'clique_member': [],
    }

    enriched_org = 0
    enriched_rank = 0

    for _, row in df.iterrows():
        asn = int(row['asn'])

        # From AS-Org
        if asn in as_org_data:
            org = as_org_data[asn]
            enrichment['org_name'].append(org['org_name'])
            enrichment['country'].append(org['country'])
            enrichment['aut_name'].append(org['aut_name'])
            enriched_org += 1
        else:
            enrichment['org_name'].append('')
            enrichment['country'].append('')
            enrichment['aut_name'].append('')

        # From AS-Rank
        if asn in asrank_data:
            rank = asrank_data[asn]
            enrichment['rank'].append(rank['rank'])
            enrichment['cone_asns'].append(rank['cone_asns'])
            enrichment['cone_prefixes'].append(rank['cone_prefixes'])
            enrichment['degree_provider'].append(rank['degree_provider'])
            enrichment['degree_peer'].append(rank['degree_peer'])
            enrichment['degree_customer'].append(rank['degree_customer'])
            enrichment['degree_transit'].append(rank['degree_transit'])
            enrichment['clique_member'].append(rank['clique_member'])
            enriched_rank += 1
        else:
            for key in ['rank', 'cone_asns', 'cone_prefixes',
                         'degree_provider', 'degree_peer',
                         'degree_customer', 'degree_transit',
                         'clique_member']:
                enrichment[key].append('')

    for col, values in enrichment.items():
        df[col] = values

    print(f"\n  AS-Org enriched: {enriched_org}/{len(df)} ({enriched_org/len(df)*100:.1f}%)")
    print(f"  AS-Rank enriched: {enriched_rank}/{len(df)} ({enriched_rank/len(df)*100:.1f}%)")

    # ── Save ─────────────────────────────────────────────────────
    output_path = os.path.join(processed_dir, 'vn_asns_enriched.csv')
    df.to_csv(output_path, index=False)
    print(f"\n✓ Saved to {output_path}")

    # Show sample
    print("\nSample enriched ASNs:")
    enriched = df[df['org_name'] != ''].head(15)
    for _, r in enriched.iterrows():
        rank_str = f" [rank #{int(r['rank'])}]" if r.get('rank', '') != '' else ""
        print(f"  AS{int(r['asn'])}: {r['org_name']} ({r['country']}){rank_str}")


if __name__ == "__main__":
    enrich_nodes()
