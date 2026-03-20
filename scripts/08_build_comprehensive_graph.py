import networkx as nx
import pandas as pd
from pyvis.network import Network
import os

def build_topology():
    print("Building comprehensive topology graph...")
    G = nx.Graph()
    
    # Use relative paths from the root directory 'VN-AS-Topo'
    base_dir = "data/processed"
    
    # 1. Load VN ASNs
    try:
        df_asns = pd.read_csv(f"{base_dir}/vn_asns.csv")
        for asn in df_asns['asn'].dropna():
            asn_clean = str(int(asn)).strip()
            G.add_node(f"AS{asn_clean}", group="VN_ASN", title=f"Autonomous System {asn_clean}", size=10)
    except Exception as e:
        print(f"Error loading VN ASNs: {e}")
        
    # 2. Load VNIX Members
    try:
        df_vnix = pd.read_csv(f"{base_dir}/vnix_members.csv")
        G.add_node("VNIX", group="IXP", title="Vietnam National Internet eXchange", size=40, color="#FF5733")
        
        for asn in df_vnix['asn'].dropna():
            asn_clean = str(int(asn)).strip()
            node_id = f"AS{asn_clean}"
            if not G.has_node(node_id):
                G.add_node(node_id, group="VN_ASN", title=f"AS{asn_clean}", size=15)
            G.add_edge(node_id, "VNIX", weight=2, title="Peers at VNIX", color="#FF5733")
    except Exception as e:
        print(f"Error loading VNIX members: {e}")
        
    # 3. Load PeeringDB IXP connections
    try:
        df_pdb = pd.read_csv(f"{base_dir}/vn_peeringdb_links.csv")
        ixps = df_pdb['ix_id'].unique()
        for ix in ixps:
            ixp_node = f"IXP_{ix}"
            G.add_node(ixp_node, group="IXP", title=f"PeeringDB IXP {ix}", size=30, color="#FFC300")
            
        for _, row in df_pdb.dropna(subset=['asn', 'ix_id']).iterrows():
            asn_clean = str(int(row['asn'])).strip()
            asn_node = f"AS{asn_clean}"
            ixp_node = f"IXP_{row['ix_id']}"
            if not G.has_node(asn_node):
                G.add_node(asn_node, group="ASN", title=f"AS{asn_clean}", size=10)
            G.add_edge(asn_node, ixp_node, weight=1, title="Connected via PeeringDB IXP", color="#FFC300")
    except Exception as e:
        print(f"Error loading PeeringDB links: {e}")

    # 4. Load RouteViews Data (Actual BGP Edges)
    try:
        df_rv = pd.read_csv(f"{base_dir}/vn_bgp_edges_2.csv") # Updated filename
        bgp_edges_added = 0
        for _, row in df_rv.dropna(subset=['asn1', 'asn2']).iterrows():
            u_clean = str(int(row['asn1'])).strip()
            v_clean = str(int(row['asn2'])).strip()
            u = f"AS{u_clean}"
            v = f"AS{v_clean}"
            if not G.has_node(u):
                G.add_node(u, group="VN_ASN", title=u, size=10)
            if not G.has_node(v):
                G.add_node(v, group="VN_ASN", title=v, size=10)
            
            # Direct BGP Peering
            G.add_edge(u, v, weight=3, title="Direct BGP Peering (RouteViews)", color="#3498DB")
            bgp_edges_added += 1
        print(f"Added {bgp_edges_added} direct BGP peering edges from RouteViews.")
    except Exception as e:
        print(f"Error loading RouteViews edges: {e}")

    # Calculate Degree Centrality to size the nodes dynamically
    degrees = dict(G.degree())
    for node, deg in degrees.items():
        if "AS" in str(node):
            # Base size 10, plus degree multiplier
            G.nodes[node]['size'] = 10 + (deg * 1.5)

    # Remove isolated nodes to make the graph cleaner
    isolated = list(nx.isolates(G))
    G.remove_nodes_from(isolated)
    print(f"Graph generated with {G.number_of_nodes()} nodes and {G.number_of_edges()} edges (removed {len(isolated)} unconnected ASNs).")

    # Export to Gephi GEXF format
    os.makedirs("data/graphs", exist_ok=True)
    nx.write_gexf(G, "data/graphs/vn_topology_comprehensive.gexf")
    print("Exported to vn_topology_comprehensive.gexf")
    
    # Create Pyvis Visualization
    print("Generating comprehensive Pyvis HTML visualization...")
    net = Network(height="1000px", width="100%", bgcolor="#1A1A1D", font_color="#FFFFFF")
    net.from_nx(G)
    
    net.set_options("""
    var options = {
      "nodes": {
        "shape": "dot",
        "scaling": {
          "min": 10,
          "max": 80
        },
        "font": {
          "size": 14,
          "face": "Tahoma",
          "color": "#FFFFFF"
        }
      },
      "edges": {
        "smooth": {
          "type": "continuous",
          "roundness": 0.5
        }
      },
      "physics": {
        "forceAtlas2Based": {
          "gravitationalConstant": -70,
          "centralGravity": 0.01,
          "springLength": 100,
          "springConstant": 0.05
        },
        "maxVelocity": 50,
        "solver": "forceAtlas2Based",
        "timestep": 0.35,
        "stabilization": {"iterations": 200}
      }
    }
    """)
    
    output_html = "data/graphs/vn_topology_comprehensive.html"
    net.save_graph(output_html)
    print(f"Saved interactive visualization to {output_html}")

if __name__ == "__main__":
    build_topology()
