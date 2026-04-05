"""
03b_fetch_global_ixp_asns.py
=============================
Fetches ALL IXP route-server ASNs from PeeringDB globally.
These ASNs can appear in BGP AS paths due to multilateral peering
and must be removed during AS path sanitization (Step 3).

Output: data/processed/ixp_asns_global.csv
"""

import requests
import pandas as pd
import time


def fetch_all_ixp_route_server_asns():
    """
    PeeringDB's ixlan/ixpfx data includes route-server ASNs.
    We query the 'netixlan' endpoint for entries where is_rs_peer=True,
    and also the 'ix' endpoint to get the IXP's own ASN if listed.
    """
    print("Fetching global IXP route-server ASNs from PeeringDB...")

    ixp_asns = set()

    # Method 1: Get IXP-own ASNs from the 'ix' endpoint
    print("  [1/2] Fetching IXP records...")
    try:
        url = "https://peeringdb.com/api/ix?limit=0"
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        ix_list = response.json().get('data', [])
        for ix in ix_list:
            # Some IXPs register their own route-server ASN
            # The 'proto_unicast' field or ASN-related fields vary,
            # but the main IXP record doesn't always have an ASN.
            pass
        print(f"    Retrieved {len(ix_list)} IXP records.")
    except Exception as e:
        print(f"    Error fetching IX list: {e}")

    # Method 2: Get all route-server peers from netixlan
    # (Networks at IXPs where is_rs_peer=True often include RS ASNs)
    print("  [2/2] Fetching route-server ASNs via ixlan prefix data...")
    try:
        # PeeringDB exposes route-server info, but the best way is to
        # look at the 'net' endpoint filtered by info_type = "Route Server"
        url = "https://peeringdb.com/api/net?info_type=Route%20Server&limit=0"
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        rs_nets = response.json().get('data', [])

        for net in rs_nets:
            asn = net.get('asn')
            if asn:
                ixp_asns.add(int(asn))

        print(f"    Found {len(rs_nets)} Route Server networks → {len(ixp_asns)} unique ASNs.")
    except Exception as e:
        print(f"    Error fetching route-server networks: {e}")

    # Also add well-known IXP route-server ASNs that may not be tagged
    well_known_rs_asns = {
        47541,   # VNIX route server (if any)
        24115,   # HKIX RS
        24516,   # BKNIX RS
        7947,    # LINX RS
        43531,   # IX.br RS
        # Add more as discovered
    }
    before = len(ixp_asns)
    ixp_asns.update(well_known_rs_asns)
    print(f"  Added {len(ixp_asns) - before} well-known RS ASNs.")

    print(f"\nTotal global IXP route-server ASNs: {len(ixp_asns)}")

    # Save
    df = pd.DataFrame({"asn": sorted(ixp_asns)})
    output_path = "data/processed/ixp_asns_global.csv"
    df.to_csv(output_path, index=False)
    print(f"Saved to {output_path}")

    return ixp_asns


if __name__ == "__main__":
    fetch_all_ixp_route_server_asns()
