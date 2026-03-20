import networkx as nx
import pandas as pd
from pyvis.network import Network
import os

def build_topology():
    print("Building topology graph...")
    G = nx.Graph()
    
    # 1. Load all VN ASNs
    try:
        df_asns = pd.read_csv("../data/processed/vn_asns.csv")
        for asn in df_asns['asn']:
            G.add_node(f"AS{asn}", group="VN_ASN", title=f"Autonomous System {asn}", size=10)
    except Exception as e:
        print(f"Error loading VN ASNs: {e}")
        
    # 2. Load VNIX Members
    try:
        df_vnix = pd.read_csv("../data/processed/vnix_members.csv")
        G.add_node("VNIX", group="IXP", title="Vietnam National Internet eXchange", size=30, color="red")
        
        for asn in df_vnix['asn']:
            node_id = f"AS{asn}"
            if not G.has_node(node_id):
                G.add_node(node_id, group="VN_ASN", title=f"AS{asn}", size=15)
            # Add edge
            G.add_edge(node_id, "VNIX", weight=2, title="Peers at VNIX")
    except Exception as e:
        print(f"Error loading VNIX members: {e}")
        
    # 3. Load PeeringDB IXP connections
    try:
        df_pdb = pd.read_csv("../data/processed/vn_peeringdb_links.csv")
        # Find unique IXPs to create nodes
        ixps = df_pdb['ix_id'].unique()
        for ix in ixps:
            ixp_node = f"IXP_{ix}"
            G.add_node(ixp_node, group="IXP", title=f"PeeringDB IXP {ix}", size=25, color="orange")
            
        for _, row in df_pdb.iterrows():
            asn_node = f"AS{row['asn']}"
            ixp_node = f"IXP_{row['ix_id']}"
            if not G.has_node(asn_node):
                G.add_node(asn_node, group="ASN", title=f"AS{row['asn']}", size=10)
            G.add_edge(asn_node, ixp_node, weight=1, title="Connected via PeeringDB IXP")
    except Exception as e:
        print(f"Error loading PeeringDB links: {e}")

    # Remove isolated nodes to make the graph cleaner for visualization (optional)
    isolated = list(nx.isolates(G))
    G.remove_nodes_from(isolated)
    print(f"Graph generated with {G.number_of_nodes()} nodes and {G.number_of_edges()} edges (removed {len(isolated)} unconnected ASNs for clarity).")

    # Export to Gephi GEXF format
    os.makedirs("../data/graphs", exist_ok=True)
    nx.write_gexf(G, "../data/graphs/vn_topology.gexf")
    print("Exported to vn_topology.gexf")
    
    # 4. Create Pyvis Visualization
    print("Generating Pyvis HTML visualization...")
    net = Network(height="800px", width="100%", bgcolor="#222222", font_color="white")
    # Tell pyvis to use the networkx graph
    net.from_nx(G)
    
    # Configure physics for better layout
    net.set_options("""
    var options = {
      "physics": {
        "forceAtlas2Based": {
          "gravitationalConstant": -50,
          "centralGravity": 0.01,
          "springLength": 100,
          "springConstant": 0.08
        },
        "maxVelocity": 50,
        "solver": "forceAtlas2Based",
        "timestep": 0.35,
        "stabilization": {"iterations": 150}
      }
    }
    """)
    
    output_html = "../data/graphs/vn_topology.html"
    net.save_graph(output_html)
    print(f"Saved interactive visualization to {output_html}")

if __name__ == "__main__":
    build_topology()
