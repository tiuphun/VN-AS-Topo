import requests
from bs4 import BeautifulSoup
import re
import pandas as pd

def scrape_vnix_members():
    print("Fetching VNIX Member List...")
    url = "https://vnix.vn/en/network"
    
    # Using a generic browser header just in case
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
    }
    
    try:
        response = requests.get(url, headers=headers, verify=False, timeout=15)
        soup = BeautifulSoup(response.text, 'html.parser')
        
        vnix_members = []
        # Search for ASN patterns in the HTML text or specific tables
        # Typical ASN format: AS12345 or just a table column with numbers
        
        # In generic cases, look for tables
        tables = soup.find_all('table')
        if not tables:
            print("No tables found. Searching for ASNs via regex...")
            text = soup.get_text()
            asns = re.findall(r'\b(?:AS)?([1-9]\d{3,5})\b', text)
            # Filter against our known VN ASNs
            known_df = pd.read_csv("../data/processed/vn_asns.csv")
            known_asns = set(known_df['asn'].astype(str))
            for a in asns:
                if a in known_asns:
                    vnix_members.append(a)
        else:
            print(f"Found {len(tables)} tables. Parsing rows...")
            for table in tables:
                rows = table.find_all('tr')
                for row in rows:
                    cols = row.find_all(['td', 'th'])
                    col_texts = [c.get_text(strip=True) for c in cols]
                    for text in col_texts:
                        match = re.search(r'\b(?:AS)?([1-9]\d{3,5})\b', text)
                        if match:
                            vnix_members.append(match.group(1))

        # Deduplicate and sort
        vnix_members = sorted(list(set(vnix_members)), key=int)
        print(f"Discovered {len(vnix_members)} ASNs participating in VNIX.")
        
        if vnix_members:
            df = pd.DataFrame({"asn": vnix_members})
            df.to_csv("../data/processed/vnix_members.csv", index=False)
            print("Saved to vnix_members.csv")
    except Exception as e:
        print(f"Failed to scrape VNIX members: {e}")

if __name__ == "__main__":
    import urllib3
    urllib3.disable_warnings()
    scrape_vnix_members()
