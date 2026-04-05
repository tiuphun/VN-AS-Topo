"""
04_fetch_iana_special_asns.py
==============================
Downloads and parses the IANA Special-Purpose AS Numbers Registry.
Only stores small, enumerable ranges. Large ranges (e.g., private-use
4200000000-4294967294) are handled by range checks in the sanitize module.

Source: https://www.iana.org/assignments/iana-as-numbers-special-registry/
Output: data/raw/iana_special_asns.txt
"""

import requests
import re
import os


IANA_CSV_URL = (
    "https://www.iana.org/assignments/"
    "iana-as-numbers-special-registry/"
    "special-purpose-as-numbers.csv"
)

# Max range size to enumerate — anything larger is handled by range checks
MAX_ENUMERATE = 100_000


def fetch_iana_special_asns():
    """Attempt to fetch the IANA CSV; store only small enumerable ranges."""
    print("Fetching IANA Special-Purpose AS Numbers Registry...")

    special_asns = set()
    large_ranges = []

    try:
        response = requests.get(IANA_CSV_URL, timeout=15)
        response.raise_for_status()
        lines = response.text.strip().splitlines()

        for line in lines[1:]:
            first_col = line.split(',')[0].strip().strip('"')
            match_range = re.match(r'^(\d+)\s*-\s*(\d+)$', first_col)
            match_single = re.match(r'^(\d+)$', first_col)

            if match_range:
                low, high = int(match_range.group(1)), int(match_range.group(2))
                if (high - low) < MAX_ENUMERATE:
                    for asn in range(low, high + 1):
                        special_asns.add(asn)
                else:
                    large_ranges.append((low, high))
                    print(f"  Large range {low}-{high} skipped (handled by range checks)")
            elif match_single:
                special_asns.add(int(match_single.group(1)))

        print(f"  Parsed {len(special_asns)} enumerable special-purpose ASNs from IANA CSV.")
        if large_ranges:
            print(f"  {len(large_ranges)} large ranges rely on sanitize module range checks.")

    except Exception as e:
        print(f"  Could not fetch IANA CSV ({e}), using hardcoded small ranges.")
        # Hardcoded small ranges only
        for low, high in [
            (0, 0), (112, 112), (23456, 23456),
            (64496, 64511), (64512, 65534), (65535, 65535),
            (65536, 65551), (65552, 131071),
        ]:
            for asn in range(low, high + 1):
                special_asns.add(asn)
        print(f"  Hardcoded {len(special_asns)} special-purpose ASNs.")

    # Save to file (one ASN per line)
    os.makedirs("data/raw", exist_ok=True)
    output_path = "data/raw/iana_special_asns.txt"
    with open(output_path, 'w') as f:
        f.write("# IANA Special-Purpose AS Numbers (enumerable ranges only)\n")
        f.write("# Large private-use ranges handled by sanitize module range checks\n")
        for asn in sorted(special_asns):
            f.write(f"{asn}\n")

    print(f"  Saved {len(special_asns)} ASNs to {output_path}")
    return special_asns


if __name__ == "__main__":
    fetch_iana_special_asns()
