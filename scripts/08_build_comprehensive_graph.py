"""
08_build_comprehensive_graph.py (OVERHAULED)
=============================================
Builds the comprehensive VN AS topology graph with:
  - Enriched node properties (org name, country) when available
  - Classified edges (p2c, p2p, unknown) when available
  - Proper IXP node handling
  - Degree-based node sizing
  - Community detection

Reference: Steps (5), (6), (7), (8) of the methodology.

Input:
  - data/processed/vn_asns.csv (or vn_asns_enriched.csv)
  - data/processed/vn_bgp_edges.csv
  - data/processed/vn_edges_classified.csv (if available)
  - data/processed/vnix_members.csv
  - data/processed/vn_peeringdb_links.csv
  - data/processed/vn_ixps.csv

Output:
  - data/graphs/vn_topology_comprehensive.gexf (for Gephi)
  - data/graphs/vn_topology_comprehensive.html (interactive Pyvis)
"""

import networkx as nx
import pandas as pd
from pyvis.network import Network
import os
import sys

# ── Color Palette ────────────────────────────────────────────────
COLORS = {
    'p2c':       '#E74C3C',   # Red — provider→customer
    'p2p':       '#3498DB',   # Blue — peer↔peer
    'unknown':   '#95A5A6',   # Gray — unknown relationship
    'ixp_link':  '#FFC300',   # Gold — IXP connection
    'ixp_node':  '#FF5733',   # Orange — IXP node
    'bgp_edge':  '#2ECC71',   # Green — direct BGP peering (unclassified)
    'stub':      '#BDC3C7',   # Light gray — stub ASN
    'transit':   '#8E44AD',   # Purple — transit ASN
    'default':   '#1ABC9C',   # Teal — default node
}


def build_topology():
    print("=" * 60)
    print("  Building Comprehensive VN AS Topology Graph")
    print("=" * 60)

    base_dir = os.path.join(os.path.dirname(__file__), '..')
    processed_dir = os.path.join(base_dir, 'data', 'processed')
    graphs_dir = os.path.join(base_dir, 'data', 'graphs')
    os.makedirs(graphs_dir, exist_ok=True)

    G = nx.Graph()

    # ──────────────────────────────────────────────────────────────
    # Step 5: Import VN ASNs as nodes
    # ──────────────────────────────────────────────────────────────
    print("\n[Step 5] Importing VN ASNs as nodes...")

    # Try enriched version first, fall back to basic
    enriched_path = os.path.join(processed_dir, 'vn_asns_enriched.csv')
    basic_path = os.path.join(processed_dir, 'vn_asns.csv')

    if os.path.exists(enriched_path):
        print("  Using enriched ASN data (with org names).")
        df_asns = pd.read_csv(enriched_path)
        has_enrichment = True
    else:
        print("  Using basic ASN data (no org names — run 06b_enrich_nodes.py for enrichment).")
        df_asns = pd.read_csv(basic_path)
        has_enrichment = False

    for _, row in df_asns.iterrows():
        asn = int(row['asn'])
        node_id = f"AS{asn}"

        attrs = {
            'group': 'VN_ASN',
            'size': 10,
            'asn': asn,
        }

        if has_enrichment and pd.notna(row.get('org_name', '')) and row.get('org_name', '') != '':
            org = row['org_name']
            attrs['org_name'] = org
            attrs['title'] = f"AS{asn}\n{org}"
            attrs['label'] = f"AS{asn}"
        else:
            attrs['title'] = f"AS{asn}"
            attrs['label'] = f"AS{asn}"

        G.add_node(node_id, **attrs)

    print(f"  Added {len(df_asns)} VN ASN nodes.")

    # ──────────────────────────────────────────────────────────────
    # Step 7a: Add IXP connections from PeeringDB
    # ──────────────────────────────────────────────────────────────
    print("\n[Step 7a] Adding IXP connections from PeeringDB...")

    # Load IXP names
    ixp_names = {}
    try:
        df_ixps = pd.read_csv(os.path.join(processed_dir, 'vn_ixps.csv'))
        for _, row in df_ixps.iterrows():
            ixp_names[row['id']] = row['name']
    except Exception:
        pass

    try:
        df_pdb = pd.read_csv(os.path.join(processed_dir, 'vn_peeringdb_links.csv'))
        ixps = df_pdb['ix_id'].unique()

        for ix in ixps:
            ixp_node = f"IXP_{ix}"
            ixp_name = ixp_names.get(ix, f"IXP {ix}")
            G.add_node(ixp_node,
                       group='IXP',
                       title=ixp_name,
                       label=ixp_name,
                       size=30,
                       color=COLORS['ixp_node'],
                       shape='diamond')

        pdb_edges = 0
        for _, row in df_pdb.dropna(subset=['asn', 'ix_id']).iterrows():
            asn = int(row['asn'])
            asn_node = f"AS{asn}"
            ixp_node = f"IXP_{int(row['ix_id'])}"
            if not G.has_node(asn_node):
                G.add_node(asn_node, group='ASN', title=f"AS{asn}", label=f"AS{asn}", size=10)
            G.add_edge(asn_node, ixp_node,
                       weight=1,
                       title=f"Peers at {ixp_names.get(int(row['ix_id']), 'IXP')}",
                       color=COLORS['ixp_link'],
                       data_source='peeringdb')
            pdb_edges += 1
        print(f"  Added {len(ixps)} IXP nodes and {pdb_edges} IXP-peering edges.")
    except Exception as e:
        print(f"  Error loading PeeringDB: {e}")

    # ──────────────────────────────────────────────────────────────
    # Step 7b: Add VNIX connections
    # ──────────────────────────────────────────────────────────────
    print("\n[Step 7b] Adding VNIX member connections...")
    try:
        df_vnix = pd.read_csv(os.path.join(processed_dir, 'vnix_members.csv'))
        G.add_node("VNIX",
                    group='IXP',
                    title="Vietnam National Internet eXchange",
                    label="VNIX",
                    size=40,
                    color=COLORS['ixp_node'],
                    shape='diamond')

        vnix_edges = 0
        for asn in df_vnix['asn'].dropna():
            asn = int(asn)
            node_id = f"AS{asn}"
            if not G.has_node(node_id):
                G.add_node(node_id, group='VN_ASN', title=f"AS{asn}", label=f"AS{asn}", size=15)
            G.add_edge(node_id, "VNIX",
                       weight=2,
                       title="Peers at VNIX",
                       color=COLORS['ixp_link'],
                       data_source='vnix')
            vnix_edges += 1
        print(f"  Added {vnix_edges} VNIX member edges.")
    except Exception as e:
        print(f"  Error loading VNIX: {e}")

    # ──────────────────────────────────────────────────────────────
    # Step 7c: Add BGP edges (classified if available)
    # ──────────────────────────────────────────────────────────────
    print("\n[Step 7c] Adding BGP peering edges...")

    classified_path = os.path.join(processed_dir, 'vn_edges_classified.csv')
    basic_edges_path = os.path.join(processed_dir, 'vn_bgp_edges.csv')

    if os.path.exists(classified_path):
        print("  Using classified edges (with p2c/p2p relationship types).")
        df_edges = pd.read_csv(classified_path)
        has_classification = True
    elif os.path.exists(basic_edges_path):
        print("  Using unclassified edges (run 07c_apply_caida_as_rel.py for classification).")
        df_edges = pd.read_csv(basic_edges_path)
        has_classification = False
    else:
        print("  WARNING: No BGP edge data found! Run 07_parse_routeviews.py first.")
        df_edges = pd.DataFrame()
        has_classification = False

    bgp_edge_count = 0
    rel_counts = {'p2c': 0, 'p2p': 0, 'unknown': 0}

    for _, row in df_edges.iterrows():
        asn1 = int(row['asn1'])
        asn2 = int(row['asn2'])
        u = f"AS{asn1}"
        v = f"AS{asn2}"

        if not G.has_node(u):
            G.add_node(u, group='VN_ASN', title=u, label=u, size=10)
        if not G.has_node(v):
            G.add_node(v, group='VN_ASN', title=v, label=v, size=10)

        if has_classification:
            rel_type = row.get('relationship', 'unknown')
            edge_color = COLORS.get(rel_type, COLORS['unknown'])

            if rel_type == 'p2c':
                provider = int(row.get('provider_asn', asn1))
                customer = int(row.get('customer_asn', asn2))
                title = f"AS{provider} → AS{customer} (provider→customer)"
            elif rel_type == 'p2p':
                title = f"AS{asn1} ↔ AS{asn2} (peer-to-peer)"
            else:
                title = f"AS{asn1} — AS{asn2} (unknown)"

            rel_counts[rel_type] = rel_counts.get(rel_type, 0) + 1
        else:
            edge_color = COLORS['bgp_edge']
            title = f"AS{asn1} — AS{asn2} (BGP peering)"
            rel_type = 'bgp'

        # Add edge weight from number of sources if available
        weight = int(row.get('num_sources', 1))

        G.add_edge(u, v,
                   weight=weight,
                   title=title,
                   color=edge_color,
                   relationship=rel_type,
                   data_source='routeviews')
        bgp_edge_count += 1

    print(f"  Added {bgp_edge_count} BGP edges.")
    if has_classification:
        for rtype, cnt in rel_counts.items():
            print(f"    {rtype}: {cnt}")

    # ──────────────────────────────────────────────────────────────
    # Dynamic node sizing based on degree
    # ──────────────────────────────────────────────────────────────
    print("\n[Sizing] Computing node sizes from degree...")
    degrees = dict(G.degree())
    max_deg = max(degrees.values()) if degrees else 1

    for node, deg in degrees.items():
        if 'AS' in str(node):
            # Scale: base 8, max ~60
            G.nodes[node]['size'] = 8 + (deg / max_deg) * 52
            # Color by role
            if deg <= 1:
                G.nodes[node]['color'] = COLORS['stub']
                G.nodes[node]['role'] = 'stub'
            else:
                G.nodes[node]['color'] = COLORS['transit']
                G.nodes[node]['role'] = 'transit'

    # Top-10 hubs get highlighted
    sorted_degrees = sorted(degrees.items(), key=lambda x: x[1], reverse=True)
    for node, deg in sorted_degrees[:10]:
        if 'AS' in str(node):
            G.nodes[node]['color'] = '#E74C3C'  # Red for top hubs

    # ──────────────────────────────────────────────────────────────
    # Remove isolates
    # ──────────────────────────────────────────────────────────────
    isolated = list(nx.isolates(G))
    G.remove_nodes_from(isolated)
    print(f"\n[Cleanup] Removed {len(isolated)} isolated nodes.")
    print(f"Final graph: {G.number_of_nodes()} nodes, {G.number_of_edges()} edges.")

    # ──────────────────────────────────────────────────────────────
    # Step 8a: Export GEXF for Gephi
    # ──────────────────────────────────────────────────────────────
    gexf_path = os.path.join(graphs_dir, 'vn_topology_comprehensive.gexf')
    nx.write_gexf(G, gexf_path)
    print(f"\n[Export] GEXF saved to {gexf_path}")
    print("  → Open in Gephi for advanced layout and analysis.")

    # ──────────────────────────────────────────────────────────────
    # Step 8b: Interactive Pyvis HTML
    # ──────────────────────────────────────────────────────────────
    print("[Export] Generating Pyvis HTML visualization...")

    net = Network(
        height="1000px",
        width="100%",
        bgcolor="#0D1117",
        font_color="#C9D1D9",
        directed=False,
        select_menu=True,
        filter_menu=True,
    )
    net.from_nx(G)

    net.set_options("""
    var options = {
      "nodes": {
        "borderWidth": 2,
        "borderWidthSelected": 4,
        "shape": "dot",
        "scaling": {
          "min": 8,
          "max": 60,
          "label": {
            "enabled": true,
            "min": 10,
            "max": 24
          }
        },
        "font": {
          "size": 12,
          "face": "Inter, Tahoma, sans-serif",
          "color": "#C9D1D9",
          "strokeWidth": 3,
          "strokeColor": "#0D1117"
        }
      },
      "edges": {
        "width": 1,
        "smooth": {
          "type": "continuous",
          "roundness": 0.3
        },
        "color": {
          "inherit": false
        }
      },
      "physics": {
        "forceAtlas2Based": {
          "gravitationalConstant": -80,
          "centralGravity": 0.005,
          "springLength": 120,
          "springConstant": 0.04,
          "damping": 0.4
        },
        "maxVelocity": 40,
        "solver": "forceAtlas2Based",
        "timestep": 0.3,
        "stabilization": {
          "enabled": true,
          "iterations": 300,
          "updateInterval": 25
        }
      },
      "interaction": {
        "hover": true,
        "tooltipDelay": 200,
        "navigationButtons": true,
        "keyboard": true
      }
    }
    """)

    html_path = os.path.join(graphs_dir, 'vn_topology_comprehensive.html')
    net.save_graph(html_path)
    print(f"  HTML saved to {html_path}")

    # ──────────────────────────────────────────────────────────────
    # Summary
    # ──────────────────────────────────────────────────────────────
    print("\n" + "=" * 60)
    print("  Graph Construction Complete")
    print("=" * 60)
    print(f"  Nodes: {G.number_of_nodes()}")
    print(f"  Edges: {G.number_of_edges()}")
    print(f"  GEXF:  {gexf_path}")
    print(f"  HTML:  {html_path}")
    if not has_classification:
        print("\n  ⚠ Edges are unclassified. Run 07c for p2c/p2p labels.")
    if not has_enrichment:
        print("  ⚠ Nodes lack org names. Run 06b for enrichment.")
    print("=" * 60)


if __name__ == "__main__":
    build_topology()
