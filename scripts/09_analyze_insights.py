import networkx as nx
import pandas as pd

def analyze():
    print("Loading comprehensive graph...")
    G = nx.read_gexf("data/graphs/vn_topology_comprehensive.gexf")
    
    # Basic stats
    print(f"Total Nodes: {G.number_of_nodes()}")
    print(f"Total Edges: {G.number_of_edges()}")
    
    # Calculate Degrees
    degrees = dict(G.degree())
    sorted_deg = sorted(degrees.items(), key=lambda x: x[1], reverse=True)
    print("\n--- Top 10 Hubs by Degree (Most Direct Peers) ---")
    for n, d in sorted_deg[:10]:
        print(f"{n} (Degree: {d})")
        
    # Calculate Betweenness Centrality (identifies transit bottlenecks/backbones)
    print("\nCalculating Betweenness Centrality...")
    betweenness = nx.betweenness_centrality(G)
    sorted_bet = sorted(betweenness.items(), key=lambda x: x[1], reverse=True)
    print("--- Top 10 Transit Backbones by Betweenness Centrality ---")
    for n, b in sorted_bet[:10]:
        print(f"{n} (Centrality: {b:.4f})")
        
    # Stub vs Transit
    stubs = [n for n, d in degrees.items() if d <= 1 and "AS" in str(n)]
    transits = [n for n, d in degrees.items() if d > 1 and "AS" in str(n)]
    print(f"\nIdentified {len(stubs)} strict Stub ASNs (Degree 1).")
    print(f"Identified {len(transits)} Transit/Multi-homed ASNs (Degree > 1).")
    
    # Density
    density = nx.density(G)
    print(f"\nNetwork Density: {density:.6f} (Typical for AS graphs to be highly central and sparse around the edges)")

if __name__ == "__main__":
    analyze()
