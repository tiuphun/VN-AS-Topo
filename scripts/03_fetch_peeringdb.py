import requests
import json
import pandas as pd

def fetch_vn_ixps():
    """Fetch all Internet Exchanges in Vietnam from PeeringDB."""
    print("Fetching IXPs in Vietnam from PeeringDB...")
    url = "https://peeringdb.com/api/ix?country=VN"
    response = requests.get(url)
    response.raise_for_status()
    ixps = response.json().get('data', [])
    print(f"Found {len(ixps)} IXPs in VN.")
    
    ixp_data = []
    for ix in ixps:
        ixp_data.append({
            'id': ix['id'],
            'name': ix['name'],
            'city': ix['city'],
            'tech_email': ix['tech_email']
        })
    df_ixps = pd.DataFrame(ixp_data)
    df_ixps.to_csv("../data/processed/vn_ixps.csv", index=False)
    return ixps

def fetch_ixp_networks(ixp_ids):
    """Fetch all networks (ASNs) present at given IXPs."""
    print(f"Fetching network presence at {len(ixp_ids)} IXPs...")
    netixlan_data = []
    
    for ixp_id in ixp_ids:
        # Get networks attached to the IXP LAN
        url = f"https://peeringdb.com/api/netixlan?ix_id={ixp_id}"
        response = requests.get(url)
        if response.status_code == 200:
            nets = response.json().get('data', [])
            for net in nets:
                netixlan_data.append({
                    'ix_id': net['ix_id'],
                    'ixlan_id': net['ixlan_id'],
                    'net_id': net['net_id'],
                    'asn': net['asn'],
                    'ipaddr4': net['ipaddr4'],
                    'ipaddr6': net['ipaddr6'],
                    'is_rs_peer': net['is_rs_peer'],
                    'operational': net['operational']
                })
                
    df_nets = pd.DataFrame(netixlan_data)
    df_nets.to_csv("../data/processed/vn_peeringdb_links.csv", index=False)
    print(f"Saved {len(df_nets)} network-IXP links.")

def fetch_vn_facilities():
    """Fetch Data Centers/Facilities in Vietnam."""
    print("Fetching Datacenters in Vietnam from PeeringDB...")
    url = "https://peeringdb.com/api/fac?country=VN"
    response = requests.get(url)
    response.raise_for_status()
    facs = response.json().get('data', [])
    print(f"Found {len(facs)} facilities in VN.")
    # Extract
    fac_data = []
    for fac in facs:
        fac_data.append({
            'id': fac['id'],
            'name': fac['name'],
            'city': fac['city']
        })
    pd.DataFrame(fac_data).to_csv("../data/processed/vn_facilities.csv", index=False)

if __name__ == "__main__":
    ixps = fetch_vn_ixps()
    fetch_vn_facilities()
    if ixps:
        ixp_ids = [ix['id'] for ix in ixps]
        fetch_ixp_networks(ixp_ids)
