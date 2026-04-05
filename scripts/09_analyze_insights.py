"""
09_analyze_insights.py (ENHANCED)
==================================
Comprehensive topological metric analysis of the VN AS map.

Reference: Step (9) of the methodology.

Metrics computed:
  - Degree distribution + power-law check
  - Betweenness centrality (transit backbone identification)
  - Closeness centrality
  - Clustering coefficient (local and global)
  - Average shortest path length & diameter (on largest component)
  - Connected components
  - Network density
  - Assortativity (degree correlation)
  - k-core decomposition
  - Stub vs. Transit classification
  - Provider/Customer cone sizes (if classified edges available)

Input:
  - data/graphs/vn_topology_comprehensive.gexf

Output:
  - Console report
  - data/graphs/degree_distribution.png
  - data/graphs/analysis_report.txt
"""

import networkx as nx
import pandas as pd
import matplotlib
matplotlib.use('Agg')  # Non-interactive backend
import matplotlib.pyplot as plt
import numpy as np
from collections import Counter
import os
import sys


def analyze():
    base_dir = os.path.join(os.path.dirname(__file__), '..')
    graphs_dir = os.path.join(base_dir, 'data', 'graphs')
    gexf_path = os.path.join(graphs_dir, 'vn_topology_comprehensive.gexf')

    print("=" * 60)
    print("  VN AS Topology — Comprehensive Analysis")
    print("=" * 60)

    G = nx.read_gexf(gexf_path)

    # Separate AS nodes from IXP nodes for AS-specific analysis
    as_nodes = [n for n in G.nodes() if n.startswith('AS')]
    ixp_nodes = [n for n in G.nodes() if not n.startswith('AS')]

    # Also create an AS-only subgraph (excluding IXP nodes)
    G_as = G.subgraph(as_nodes).copy()

    report_lines = []

    def report(text):
        print(text)
        report_lines.append(text)

    # ──────────────────────────────────────────────────────────────
    # 1. Basic Statistics
    # ──────────────────────────────────────────────────────────────
    report("\n─── 1. Basic Statistics ───")
    report(f"  Total nodes:     {G.number_of_nodes()}")
    report(f"    AS nodes:      {len(as_nodes)}")
    report(f"    IXP nodes:     {len(ixp_nodes)}")
    report(f"  Total edges:     {G.number_of_edges()}")
    report(f"  AS-only edges:   {G_as.number_of_edges()}")

    # ──────────────────────────────────────────────────────────────
    # 2. Connected Components
    # ──────────────────────────────────────────────────────────────
    report("\n─── 2. Connected Components ───")
    components = list(nx.connected_components(G_as))
    components.sort(key=len, reverse=True)
    report(f"  Number of connected components: {len(components)}")
    for i, comp in enumerate(components[:5]):
        report(f"    Component {i+1}: {len(comp)} nodes")

    # Work with the largest connected component for path-based metrics
    if components:
        lcc = G_as.subgraph(components[0]).copy()
        report(f"  Largest component: {lcc.number_of_nodes()} nodes, {lcc.number_of_edges()} edges")
        lcc_coverage = lcc.number_of_nodes() / len(as_nodes) * 100 if as_nodes else 0
        report(f"  LCC coverage: {lcc_coverage:.1f}% of AS nodes")
    else:
        lcc = G_as

    # ──────────────────────────────────────────────────────────────
    # 3. Degree Analysis
    # ──────────────────────────────────────────────────────────────
    report("\n─── 3. Degree Analysis ───")
    degrees = dict(G_as.degree())

    if degrees:
        deg_values = list(degrees.values())
        report(f"  Min degree:  {min(deg_values)}")
        report(f"  Max degree:  {max(deg_values)}")
        report(f"  Mean degree: {np.mean(deg_values):.2f}")
        report(f"  Median degree: {np.median(deg_values):.1f}")

        # Top 15 hubs
        sorted_deg = sorted(degrees.items(), key=lambda x: x[1], reverse=True)
        report("\n  Top 15 Hubs by Degree:")
        for n, d in sorted_deg[:15]:
            org = G_as.nodes[n].get('org_name', '')
            label = f" ({org})" if org else ""
            report(f"    {n}{label}: degree {d}")

        # Stub vs Transit
        stubs = [n for n, d in degrees.items() if d <= 1]
        multihomed = [n for n, d in degrees.items() if d == 2]
        transits = [n for n, d in degrees.items() if d > 2]
        report(f"\n  Classification:")
        report(f"    Stub ASNs (degree ≤ 1):     {len(stubs)}")
        report(f"    Multi-homed (degree = 2):    {len(multihomed)}")
        report(f"    Transit ASNs (degree > 2):   {len(transits)}")

    # ──────────────────────────────────────────────────────────────
    # 4. Degree Distribution (log-log plot)
    # ──────────────────────────────────────────────────────────────
    report("\n─── 4. Degree Distribution ───")
    if deg_values:
        deg_count = Counter(deg_values)
        deg_x = sorted(deg_count.keys())
        deg_y = [deg_count[d] for d in deg_x]

        fig, axes = plt.subplots(1, 2, figsize=(14, 5))

        # Linear scale
        axes[0].bar(deg_x, deg_y, color='#3498DB', edgecolor='#2C3E50', alpha=0.8)
        axes[0].set_xlabel('Degree', fontsize=12)
        axes[0].set_ylabel('Frequency', fontsize=12)
        axes[0].set_title('Degree Distribution (Linear)', fontsize=14)

        # Log-log scale (CCDF for power-law check)
        deg_sorted = np.sort(deg_values)[::-1]
        ccdf_y = np.arange(1, len(deg_sorted) + 1) / len(deg_sorted)
        axes[1].loglog(deg_sorted, ccdf_y, 'o', markersize=4, color='#E74C3C', alpha=0.7)
        axes[1].set_xlabel('Degree (log)', fontsize=12)
        axes[1].set_ylabel('CCDF P(X ≥ x) (log)', fontsize=12)
        axes[1].set_title('Degree CCDF (log-log) — Power-Law Check', fontsize=14)
        axes[1].grid(True, alpha=0.3)

        plt.tight_layout()
        plot_path = os.path.join(graphs_dir, 'degree_distribution.png')
        plt.savefig(plot_path, dpi=150, bbox_inches='tight')
        plt.close()
        report(f"  Saved degree distribution plot to {plot_path}")

    # ──────────────────────────────────────────────────────────────
    # 5. Density
    # ──────────────────────────────────────────────────────────────
    report("\n─── 5. Network Density ───")
    density = nx.density(G_as)
    report(f"  Density: {density:.6f}")
    report(f"  (AS topologies are typically sparse, ~0.001-0.01)")

    # ──────────────────────────────────────────────────────────────
    # 6. Clustering Coefficient
    # ──────────────────────────────────────────────────────────────
    report("\n─── 6. Clustering Coefficient ───")
    avg_clustering = nx.average_clustering(G_as)
    report(f"  Average clustering coefficient: {avg_clustering:.4f}")

    # Top clustered nodes
    clustering = nx.clustering(G_as)
    sorted_clust = sorted(
        [(n, c) for n, c in clustering.items() if degrees.get(n, 0) >= 3],
        key=lambda x: x[1], reverse=True
    )
    if sorted_clust:
        report("  Top 10 most clustered ASNs (degree ≥ 3):")
        for n, c in sorted_clust[:10]:
            report(f"    {n}: clustering {c:.4f} (degree {degrees[n]})")

    # ──────────────────────────────────────────────────────────────
    # 7. Path-Based Metrics (on LCC)
    # ──────────────────────────────────────────────────────────────
    report("\n─── 7. Path-Based Metrics (Largest Component) ───")
    if lcc.number_of_nodes() > 1:
        if lcc.number_of_nodes() < 500:
            try:
                avg_path = nx.average_shortest_path_length(lcc)
                report(f"  Average shortest path length: {avg_path:.3f}")
            except Exception as e:
                report(f"  Average shortest path length: error ({e})")

            try:
                diameter = nx.diameter(lcc)
                report(f"  Diameter: {diameter}")
            except Exception as e:
                report(f"  Diameter: error ({e})")
        else:
            # For large graphs, sample
            report(f"  (LCC has {lcc.number_of_nodes()} nodes — sampling for path metrics)")
            import random
            sample = random.sample(list(lcc.nodes()), min(200, lcc.number_of_nodes()))
            path_lengths = []
            for src in sample[:50]:
                lengths = nx.single_source_shortest_path_length(lcc, src)
                path_lengths.extend(lengths.values())
            if path_lengths:
                report(f"  Estimated avg shortest path length (sampled): {np.mean(path_lengths):.3f}")
                report(f"  Estimated diameter (sampled): {max(path_lengths)}")
    else:
        report("  Skipped — LCC too small.")

    # ──────────────────────────────────────────────────────────────
    # 8. Betweenness Centrality
    # ──────────────────────────────────────────────────────────────
    report("\n─── 8. Betweenness Centrality ───")
    betweenness = nx.betweenness_centrality(G_as)
    sorted_bet = sorted(betweenness.items(), key=lambda x: x[1], reverse=True)
    report("  Top 15 Transit Backbones:")
    for n, b in sorted_bet[:15]:
        org = G_as.nodes[n].get('org_name', '')
        label = f" ({org})" if org else ""
        report(f"    {n}{label}: centrality {b:.4f}")

    # ──────────────────────────────────────────────────────────────
    # 9. Closeness Centrality
    # ──────────────────────────────────────────────────────────────
    report("\n─── 9. Closeness Centrality ───")
    if lcc.number_of_nodes() > 1:
        closeness = nx.closeness_centrality(lcc)
        sorted_close = sorted(closeness.items(), key=lambda x: x[1], reverse=True)
        report("  Top 10 Most Central ASNs (Closeness):")
        for n, c in sorted_close[:10]:
            org = lcc.nodes[n].get('org_name', '')
            label = f" ({org})" if org else ""
            report(f"    {n}{label}: closeness {c:.4f}")

    # ──────────────────────────────────────────────────────────────
    # 10. Assortativity
    # ──────────────────────────────────────────────────────────────
    report("\n─── 10. Assortativity ───")
    try:
        assort = nx.degree_assortativity_coefficient(G_as)
        report(f"  Degree assortativity: {assort:.4f}")
        if assort < 0:
            report("  (Negative = disassortative: high-degree nodes connect to low-degree nodes)")
            report("  (This is typical for AS topologies — hub-and-spoke structure)")
        else:
            report("  (Positive = assortative: high-degree nodes connect to other high-degree nodes)")
    except Exception as e:
        report(f"  Could not compute: {e}")

    # ──────────────────────────────────────────────────────────────
    # 11. k-Core Decomposition
    # ──────────────────────────────────────────────────────────────
    report("\n─── 11. k-Core Decomposition ───")
    core_numbers = nx.core_number(G_as)
    max_core = max(core_numbers.values()) if core_numbers else 0
    report(f"  Maximum k-core: {max_core}")

    core_dist = Counter(core_numbers.values())
    for k in sorted(core_dist.keys()):
        report(f"    {k}-core: {core_dist[k]} nodes")

    # Nodes in the innermost core
    if max_core >= 2:
        inner_core = [n for n, k in core_numbers.items() if k == max_core]
        report(f"\n  Innermost core ({max_core}-core) members:")
        for n in sorted(inner_core):
            org = G_as.nodes[n].get('org_name', '')
            label = f" ({org})" if org else ""
            report(f"    {n}{label}")

    # ──────────────────────────────────────────────────────────────
    # 12. Edge Relationship Breakdown (if classified)
    # ──────────────────────────────────────────────────────────────
    report("\n─── 12. Edge Relationship Breakdown ───")
    rel_counts = Counter()
    for u, v, data in G_as.edges(data=True):
        rel = data.get('relationship', 'unclassified')
        rel_counts[rel] += 1

    if rel_counts:
        for rtype, cnt in rel_counts.most_common():
            pct = cnt / G_as.number_of_edges() * 100 if G_as.number_of_edges() > 0 else 0
            report(f"  {rtype}: {cnt} ({pct:.1f}%)")
    else:
        report("  No edge relationship data found.")

    # ──────────────────────────────────────────────────────────────
    # Save report
    # ──────────────────────────────────────────────────────────────
    report_path = os.path.join(graphs_dir, 'analysis_report.txt')
    with open(report_path, 'w') as f:
        f.write('\n'.join(report_lines))
    print(f"\nFull report saved to {report_path}")


if __name__ == "__main__":
    analyze()
