"""
10_export_datasets.py
======================
Exports clean, researcher-friendly CSV datasets of Vietnam AS nodes and edges.
This incorporates metrics from the graph topology and VNIX traffic analysis.

Input:
  - data/graphs/vn_topology_comprehensive.gexf

Output:
  - data/processed/vietnam_as_nodes.csv
  - data/processed/vietnam_as_edges.csv
"""

import networkx as nx
import pandas as pd
import os

def export_datasets():
    print("=" * 60)
    print("  Exporting Clean AS Datasets for Research")
    print("=" * 60)

    base_dir = os.path.join(os.path.dirname(__file__), '..')
    graphs_dir = os.path.join(base_dir, 'data', 'graphs')
    processed_dir = os.path.join(base_dir, 'data', 'processed')
    
    gexf_path = os.path.join(graphs_dir, 'vn_topology_comprehensive.gexf')
    
    if not os.path.exists(gexf_path):
        print(f"ERROR: Graph file not found at {gexf_path}")
        print("Please run 08_build_comprehensive_graph.py first.")
        return

    print("Loading comprehensive graph...")
    G = nx.read_gexf(gexf_path)

    # 1. Compute Centralities
    print("Computing network metrics...")
    
    # Isolate the AS-only subgraph for accurate AS-level metrics
    as_nodes = [n for n in G.nodes() if str(n).startswith('AS')]
    G_as = G.subgraph(as_nodes).copy()
    
    degrees = dict(G_as.degree())
    betweenness = nx.betweenness_centrality(G_as)
    
    # For closeness, we should compute on the largest connected component
    components = list(nx.connected_components(G_as))
    components.sort(key=len, reverse=True)
    if components:
        lcc = G_as.subgraph(components[0])
        closeness = nx.closeness_centrality(lcc)
    else:
        closeness = {}

    # 2. Build Nodes Dataset
    print("Compiling nodes dataset...")
    # Load enriched ASNs to get all CAIDA fields
    enriched_path = os.path.join(processed_dir, 'vn_asns_enriched.csv')
    if os.path.exists(enriched_path):
        df_asns = pd.read_csv(enriched_path)
    else:
        df_asns = pd.read_csv(os.path.join(processed_dir, 'vn_asns.csv'))

    # Prepare metrics
    metrics = []
    for _, row in df_asns.iterrows():
        asn = row['asn']
        node_id = f"AS{asn}"
        
        deg = degrees.get(node_id, 0)
        if deg <= 1:
            classification = "Stub"
        elif deg == 2:
            classification = "Multi-homed"
        else:
            classification = "Transit"
            
        metrics.append({
            'asn': asn,
            'topology_degree': deg,
            'topology_classification': classification,
            'betweenness_centrality': round(betweenness.get(node_id, 0.0), 6),
            'closeness_centrality': round(closeness.get(node_id, 0.0), 6)
        })
        
    df_metrics = pd.DataFrame(metrics)
    df_nodes = pd.merge(df_asns, df_metrics, on='asn', how='left')
    
    nodes_out = os.path.join(processed_dir, 'vietnam_as_nodes.csv')
    df_nodes.to_csv(nodes_out, index=False)
    print(f"  -> Exported {len(df_nodes)} nodes with all CAIDA and topology fields to {nodes_out}")

    # 3. Build Edges Dataset
    print("Compiling edges dataset...")
    # Load classified edges to get all fields
    edges_path = os.path.join(processed_dir, 'vn_edges_classified.csv')
    if os.path.exists(edges_path):
        df_edges = pd.read_csv(edges_path)
    else:
        df_edges = pd.read_csv(os.path.join(processed_dir, 'vn_bgp_edges.csv'))

    edges_out = os.path.join(processed_dir, 'vietnam_as_edges.csv')
    df_edges.to_csv(edges_out, index=False)
    print(f"  -> Exported {len(df_edges)} domestic AS edges with all BGP fields to {edges_out}")

    print("=" * 60)
    print("  Dataset Export Complete")
    print("=" * 60)

if __name__ == "__main__":
    export_datasets()
