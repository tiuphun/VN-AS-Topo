import requests
from bs4 import BeautifulSoup
import re
import time
import pandas as pd

# IPTP Looking Glass parameters
# Routers in VN according to user: Hanoi, HCMC, Da Nang. We will find their IDs on the form.
LG_URL = "https://www.iptp.net/iptp-tools/lg/"

def test_iptp_lg():
    print("Fetching IPTP LG homepage to find router IDs...")
    try:
        response = requests.get(LG_URL, verify=False, timeout=15)
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Look for the router select element
        routers = []
        selects = soup.find_all('select')
        for select in selects:
            if 'router' in select.get('name', '').lower() or 'node' in select.get('name', '').lower():
                options = select.find_all('option')
                for opt in options:
                    name = opt.text.lower()
                    if 'vietnam' in name or 'vn' in name or 'hanoi' in name or 'ho chi minh' in name or 'da nang' in name or 'han' in name or 'sgn' in name:
                        routers.append((opt.get('value'), opt.text.strip()))
        
        print("All matching Vietnam routers:")
        for r in routers:
            print(r)
            
    except Exception as e:
        print(f"Error connecting to IPTP LG: {e}")

if __name__ == "__main__":
    test_iptp_lg()
