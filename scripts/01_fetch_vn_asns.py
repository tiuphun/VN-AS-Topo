import pandas as pd
import requests

APNIC_URL = "https://ftp.apnic.net/stats/apnic/delegated-apnic-extended-latest"

def fetch_vn_asns():
    print("Downloading APNIC extended stats...")
    response = requests.get(APNIC_URL)
    response.raise_for_status()
    
    print("Parsing data...")
    lines = response.text.splitlines()
    vn_asns = []
    
    for line in lines:
        if line.startswith('#') or not line.strip():
            continue
        parts = line.split('|')
        if len(parts) >= 6:
            registry, cc, type_, start, value, date = parts[:6]
            if cc == 'VN' and type_ == 'asn':
                # Value for 'asn' describes the number of ASNs assigned in a block
                start_asn = int(start)
                count = int(value)
                for asn in range(start_asn, start_asn + count):
                    vn_asns.append(asn)
                    
    print(f"Found {len(vn_asns)} ASNs for country code VN.")
    
    # Save to CSV
    df = pd.DataFrame({"asn": sorted(vn_asns)})
    output_path = "../data/processed/vn_asns.csv"
    df.to_csv(output_path, index=False)
    print(f"Saved to {output_path}")

if __name__ == "__main__":
    fetch_vn_asns()
