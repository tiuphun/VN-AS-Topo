import pandas as pd
import glob
import os
import subprocess
import re

def parse_bgpdump_paths():
    print("Loading VN ASNs...")
    try:
        df_asns = pd.read_csv("data/processed/vn_asns.csv")
        vn_asns = set(df_asns['asn'].astype(str).tolist())
    except Exception as e:
        print(f"Error loading VN ASNs: {e}")
        return

    mrt_files = glob.glob("data/routeviews/*.bz2")
    if not mrt_files:
        print("No MRT files found in ../data/routeviews/")
        return

    print(f"Found {len(mrt_files)} MRT files.")
    
    vn_edges = set()
    
    for f in mrt_files:
        print(f"\nRunning bgpdump on {os.path.basename(f)}...")
        try:
            # -m provides machine readable output (pipe separated)
            # Example format: TABLE_DUMP2|timestamp|B|peer_ip|peer_asn|prefix|as_path|origin
            process = subprocess.Popen(['bgpdump', '-m', f], stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True)
            
            line_count = 0
            for line in process.stdout:
                line_count += 1
                if line_count % 1000000 == 0:
                    print(f"  Parsed {line_count} routes from {os.path.basename(f)}...")
                    
                parts = line.split('|')
                if len(parts) >= 7:
                    as_path_str = parts[6]
                    # Extract sequence of ASNs
                    path = re.findall(r'\b\d+\b', as_path_str)
                    
                    for i in range(len(path) - 1):
                        asn1 = path[i]
                        asn2 = path[i+1]
                        if asn1 != asn2 and asn1 in vn_asns and asn2 in vn_asns:
                            edge = tuple(sorted([asn1, asn2]))
                            vn_edges.add(edge)
            process.wait()
        except Exception as e:
            print(f"Error processing {f} with bgpdump: {e}")

    print(f"\nFinished parsing. Found {len(vn_edges)} actual domestic peering edges from RouteViews data!")
    
    if vn_edges:
        records = [{'asn1': e[0], 'asn2': e[1]} for e in vn_edges]
        df_edges = pd.DataFrame(records)
        output_path = "data/processed/vn_bgp_edges_2.csv"
        df_edges.to_csv(output_path, index=False)
        print(f"Saved edges to {output_path}")

if __name__ == "__main__":
    parse_bgpdump_paths()
