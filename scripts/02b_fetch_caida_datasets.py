"""
02b_fetch_caida_datasets.py
============================
Downloads the three CAIDA datasets needed for the pipeline:

  1. AS-Org (as-org2info) — maps ASN → Organization name, country
     Format: Single file with two sections (AS entries + Org entries)
     URL: https://publicdata.caida.org/datasets/as-organizations/

  2. AS-Rel (serial-2) — AS relationship classifications (p2c/p2p)
     Format: <asn1>|<asn2>|<rel>|<source>  (-1 = p2c, 0 = p2p)
     URL: https://publicdata.caida.org/datasets/as-relationships/serial-2/

  3. AS-Rank — AS ranking by customer cone size (via GraphQL API)
     URL: https://api.asrank.caida.org/v2/graphql

Target date: January 2026 (to align with your RIB dumps)

Output:
  - data/raw/20260101.as-org2info.txt.gz
  - data/raw/20260101.as-rel2.txt.bz2
  - data/raw/caida_asrank_vn.jsonl
"""

import requests
import json
import os
import time
import sys

BASE_DIR = os.path.join(os.path.dirname(__file__), '..')
RAW_DIR = os.path.join(BASE_DIR, 'data', 'raw')

# ── Target date aligned with RIB dumps (January 2026) ────────────
AS_ORG_URL = "https://publicdata.caida.org/datasets/as-organizations/20260101.as-org2info.txt.gz"
AS_REL_URL = "https://publicdata.caida.org/datasets/as-relationships/serial-2/20260101.as-rel2.txt.bz2"
ASRANK_GRAPHQL = "https://api.asrank.caida.org/v2/graphql"


def download_file(url: str, output_path: str):
    """Download a file with progress reporting."""
    fname = os.path.basename(output_path)
    if os.path.exists(output_path):
        size = os.path.getsize(output_path)
        print(f"  {fname} already exists ({size:,} bytes). Skipping.")
        return True

    print(f"  Downloading {fname}...")
    try:
        response = requests.get(url, stream=True, timeout=60)
        response.raise_for_status()

        total = int(response.headers.get('content-length', 0))
        downloaded = 0

        with open(output_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=65536):
                f.write(chunk)
                downloaded += len(chunk)
                if total > 0:
                    pct = (downloaded / total) * 100
                    print(f"    {downloaded:,}/{total:,} bytes ({pct:.0f}%)", end='\r')

        print(f"\n  ✓ Saved to {output_path} ({downloaded:,} bytes)")
        return True

    except requests.exceptions.HTTPError as e:
        print(f"  ✗ HTTP Error: {e}")
        print(f"    URL: {url}")
        if e.response.status_code == 404:
            print("    File not found. Try a different date.")
        return False
    except Exception as e:
        print(f"  ✗ Error: {e}")
        return False


def fetch_asrank_vn_asns():
    """
    Fetch AS Rank data for all Vietnamese ASNs via the GraphQL API.
    This gives us: rank, customer cone size, degree info, clique membership.
    """
    import pandas as pd
    output_path = os.path.join(RAW_DIR, 'caida_asrank_vn.jsonl')

    if os.path.exists(output_path):
        print(f"  caida_asrank_vn.jsonl already exists. Skipping.")
        return True

    # Load VN ASNs
    processed_dir = os.path.join(BASE_DIR, 'data', 'processed')
    try:
        df = pd.read_csv(os.path.join(processed_dir, 'vn_asns.csv'))
        vn_asns = df['asn'].astype(str).tolist()
    except Exception as e:
        print(f"  Error loading VN ASNs: {e}")
        return False

    print(f"  Fetching AS Rank data for {len(vn_asns)} VN ASNs via GraphQL API...")

    results = []
    batch_size = 20  # query multiple ASNs at once via the list parameter
    failed = 0

    for i in range(0, len(vn_asns), batch_size):
        batch = vn_asns[i:i+batch_size]
        asn_list = json.dumps(batch)

        query = f'''{{
            asns(asns: {asn_list}) {{
                totalCount
                edges {{
                    node {{
                        asn
                        asnName
                        rank
                        source
                        cliqueMember
                        seen
                        organization {{
                            orgId
                            orgName
                        }}
                        cone {{
                            numberAsns
                            numberPrefixes
                            numberAddresses
                        }}
                        asnDegree {{
                            provider
                            peer
                            customer
                            sibling
                            transit
                            total
                        }}
                        country {{
                            iso
                            name
                        }}
                    }}
                }}
            }}
        }}'''

        try:
            response = requests.post(
                ASRANK_GRAPHQL,
                json={'query': query},
                headers={'Content-Type': 'application/json'},
                timeout=30,
            )
            response.raise_for_status()
            data = response.json()

            edges = data.get('data', {}).get('asns', {}).get('edges', [])
            for edge in edges:
                node = edge.get('node', {})
                if node:
                    results.append(node)

        except Exception as e:
            failed += 1
            if failed <= 3:
                print(f"    Warning: batch starting at {i} failed: {e}")

        if (i // batch_size) % 10 == 0 and i > 0:
            print(f"    Processed {i}/{len(vn_asns)} ASNs ({len(results)} results)...")

        time.sleep(0.2)  # rate limit

    print(f"  Fetched {len(results)} ASN records from AS Rank API.")

    # Save as JSONL
    with open(output_path, 'w') as f:
        for record in results:
            f.write(json.dumps(record) + '\n')

    print(f"  ✓ Saved to {output_path}")
    return True


def main():
    os.makedirs(RAW_DIR, exist_ok=True)

    print("=" * 60)
    print("  Downloading CAIDA Datasets")
    print("=" * 60)

    # 1. AS-Org
    print("\n[1/3] CAIDA AS Organizations (as-org2info)")
    ok1 = download_file(AS_ORG_URL, os.path.join(RAW_DIR, '20260101.as-org2info.txt.gz'))

    # 2. AS-Rel
    print("\n[2/3] CAIDA AS Relationships (serial-2)")
    ok2 = download_file(AS_REL_URL, os.path.join(RAW_DIR, '20260101.as-rel2.txt.bz2'))

    # 3. AS-Rank (via API — no file download needed)
    print("\n[3/3] CAIDA AS Rank (via GraphQL API)")
    ok3 = fetch_asrank_vn_asns()

    print("\n" + "=" * 60)
    print("  Download Summary")
    print("=" * 60)
    print(f"  AS-Org:  {'✓' if ok1 else '✗'}")
    print(f"  AS-Rel:  {'✓' if ok2 else '✗'}")
    print(f"  AS-Rank: {'✓' if ok3 else '✗'}")

    if not (ok1 and ok2 and ok3):
        print("\n  ⚠ Some downloads failed. Check errors above.")
        print("  You can re-run this script to retry failed downloads.")


if __name__ == "__main__":
    main()
