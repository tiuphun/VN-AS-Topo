import pandas as pd
import requests
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

def fetch_prefixes_for_asn(asn):
    url = f"https://stat.ripe.net/data/announced-prefixes/data.json?resource=AS{asn}"
    try:
        response = requests.get(url, timeout=10)
        if response.status_code == 200:
            data = response.json()
            prefixes = [item['prefix'] for item in data.get('data', {}).get('prefixes', [])]
            return asn, prefixes
        elif response.status_code == 429:
            print(f"Rate limited for AS{asn}, retrying...")
            time.sleep(2)
            return fetch_prefixes_for_asn(asn)
    except Exception as e:
        print(f"Error fetching AS{asn}: {e}")
    return asn, []

def main():
    try:
        df_asns = pd.read_csv("../data/processed/vn_asns.csv")
    except FileNotFoundError:
        print("vn_asns.csv not found. Please run 01_fetch_vn_asns.py first.")
        return

    asns = df_asns['asn'].tolist()
    records = []
    
    print(f"Fetching prefixes for {len(asns)} ASNs using RIPE Stat API...")
    
    with ThreadPoolExecutor(max_workers=5) as executor:
        future_to_asn = {executor.submit(fetch_prefixes_for_asn, asn): asn for asn in asns}
        
        count = 0
        for future in as_completed(future_to_asn):
            asn, prefixes = future.result()
            for p in prefixes:
                records.append({'asn': asn, 'prefix': p})
            count += 1
            if count % 100 == 0:
                print(f"Processed {count}/{len(asns)} ASNs")
            time.sleep(0.1) # small delay to prevent rate limits inside the thread pool executor return cycle
            
    df_prefixes = pd.DataFrame(records)
    output_path = "../data/processed/vn_prefixes.csv"
    df_prefixes.to_csv(output_path, index=False)
    print(f"Saved {len(df_prefixes)} total prefixes to {output_path}")

if __name__ == "__main__":
    main()
