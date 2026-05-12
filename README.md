# VN-AS-Topo

> **A research-grade pipeline for mapping Vietnam's national Autonomous System (AS) topology from BGP routing data.**

This project constructs a high-fidelity, annotated graph of all active Vietnamese ASNs and their peering relationships. It integrates data from APNIC, CAIDA, PeeringDB, VNIX, and RouteViews BGP RIB dumps, then applies rigorous AS-path sanitization before producing an enriched, interactive topology map.

---

## Table of Contents

- [Overview](#overview)
- [Key Results](#key-results)
- [Data Sources](#data-sources)
- [Project Structure](#project-structure)
- [Pipeline](#pipeline)
- [Setup & Installation](#setup--installation)
- [Running the Pipeline](#running-the-pipeline)
- [Output Files](#output-files)
- [Methodology Notes](#methodology-notes)

---

## Overview

The Vietnamese Internet is characterized by a **hub-and-spoke, disassortative topology** dominated by a small number of national ISPs. This project automates the full process of:

1. Collecting all VN-registered ASNs from APNIC
2. Fetching prefix, peering, and IXP membership data
3. Downloading ground-truth organizational and relationship data from CAIDA
4. Parsing MRT BGP RIB dumps from RouteViews (9 vantage points)
5. Sanitizing AS paths (removing IXP route servers, prepends, loops, and special ASNs)
6. Classifying edges as provider→customer (`p2c`) or peer↔peer (`p2p`)
7. Assembling a NetworkX graph enriched with org names and relationship types
8. Exporting interactive HTML (Pyvis) and GEXF (Gephi) visualizations
9. Computing comprehensive topological metrics

---

## Key Results

The final graph (as of January 2026 RIB snapshot) contains:

| Metric | Value |
|---|---|
| Active AS nodes | 525 |
| IXP nodes | 4 |
| Total edges | 981 |
| Largest connected component | 513 nodes (97.7% coverage) |
| Provider→Customer edges (p2c) | 857 (92.3%) |
| Peer↔Peer edges (p2p) | 13 (1.4%) |
| Network density | 0.0067 |
| Average clustering coefficient | 0.3235 |
| Estimated avg. shortest path | ~2.8 hops |
| Estimated diameter | 6 hops |
| Degree assortativity | −0.4611 (disassortative) |
| Maximum k-core | 5 |

### Top Transit Hubs (by Betweenness Centrality)

| Rank | ASN | Organization | Centrality |
|---|---|---|---|
| 1 | AS18403 | FPT Telecom | 0.5647 |
| 2 | AS45899 | VNPT | 0.1827 |
| 3 | AS38731 | Viettel (CHT) | 0.1693 |
| 4 | AS45903 | CMC Telecom | 0.1491 |
| 5 | AS7552 | Viettel Group | 0.1365 |

The **innermost 5-core** (20 ASNs) forms the absolute backbone of domestic Vietnamese Internet traffic, including national carriers, VTC, Vietnam Social Security, Vietnam Post, and financial infrastructure ASNs.

---

## Data Sources

| Source | Data | Script |
|---|---|---|
| [APNIC](https://ftp.apnic.net/stats/apnic/) | VN ASN registry | `01_fetch_vn_asns.py` |
| [APNIC](https://ftp.apnic.net/stats/apnic/) | VN IP prefix assignments | `02_fetch_vn_prefixes.py` |
| [CAIDA AS-Org](https://publicdata.caida.org/datasets/as-organizations/) | ASN → Organization mapping | `02b_fetch_caida_datasets.py` |
| [CAIDA AS-Rel](https://publicdata.caida.org/datasets/as-relationships/serial-2/) | p2c / p2p relationship classification | `02b_fetch_caida_datasets.py` |
| [CAIDA AS-Rank](https://api.asrank.caida.org/v2/graphql) | AS rank, customer cone size | `02b_fetch_caida_datasets.py` |
| [PeeringDB](https://www.peeringdb.com/) | IXP membership and peering fabric | `03_fetch_peeringdb.py` |
| [PeeringDB / PCH](https://www.peeringdb.com/) | Global IXP route-server ASNs | `03b_fetch_global_ixp_asns.py` |
| [IANA](https://www.iana.org/assignments/as-numbers/) | Reserved/special-purpose ASNs | `04_fetch_iana_special_asns.py` |
| [VNIX](https://vnix.vn/) | Vietnam National IXP members | `05_scrape_vnix.py` |
| [RouteViews](http://www.routeviews.org/) | BGP RIB dumps (9 collectors, Jan 2026) | `07_parse_routeviews.py` |

---

## Project Structure

```
VN-AS-Topo/
├── data/
│   ├── raw/                  # Downloaded raw files (CAIDA .gz/.bz2, IANA, etc.)
│   ├── processed/            # Cleaned CSVs (vn_asns.csv, vn_bgp_edges.csv, etc.)
│   ├── routeviews/           # MRT RIB dump files (.bz2)
│   └── graphs/               # Output graphs and analysis
│       ├── vn_topology_comprehensive.html   # Interactive browser visualization
│       ├── vn_topology_comprehensive.gexf  # Gephi-compatible graph file
│       ├── degree_distribution.png         # Degree distribution plot (linear + CCDF)
│       └── analysis_report.txt             # Full metric report
├── scripts/
│   ├── lib/
│   │   └── sanitize.py       # AS-path sanitization library
│   ├── 01_fetch_vn_asns.py
│   ├── 02_fetch_vn_prefixes.py
│   ├── 02b_fetch_caida_datasets.py
│   ├── 03_fetch_peeringdb.py
│   ├── 03b_fetch_global_ixp_asns.py
│   ├── 04_fetch_iana_special_asns.py
│   ├── 05_scrape_vnix.py
│   ├── 06_build_graph.py
│   ├── 06b_enrich_nodes.py
│   ├── 07_parse_routeviews.py
│   ├── 07c_apply_caida_as_rel.py
│   ├── 08_build_comprehensive_graph.py
│   └── 09_analyze_insights.py
├── docs/
│   └── walkthrough.md        # Detailed implementation walkthrough
├── notebooks/                # Jupyter notebooks for exploratory analysis
├── dataset.csv               # Consolidated dataset export
├── vn-as-topo.gephi          # Pre-built Gephi project file
└── requirements.txt
```

---

## Pipeline

The pipeline follows a 9-step research methodology. Each step produces intermediate outputs consumed by the next.

```
[01] Fetch VN ASNs (APNIC)
 └─▶ data/processed/vn_asns.csv

[02] Fetch VN IP Prefixes (APNIC)
 └─▶ data/processed/vn_prefixes.csv

[02b] Fetch CAIDA Datasets (AS-Org, AS-Rel, AS-Rank)
 └─▶ data/raw/20260101.as-org2info.txt.gz
 └─▶ data/raw/20260101.as-rel2.txt.bz2
 └─▶ data/raw/caida_asrank_vn.jsonl

[03] Fetch PeeringDB IXP links
 └─▶ data/processed/vn_peeringdb_links.csv
 └─▶ data/processed/vn_ixps.csv

[03b] Fetch Global IXP Route-Server ASNs
 └─▶ data/processed/ixp_asns_global.csv

[04] Fetch IANA Special ASNs
 └─▶ data/raw/iana_special_asns.txt

[05] Scrape VNIX Members
 └─▶ data/processed/vnix_members.csv

[06b] Enrich VN ASN nodes with CAIDA org data
 └─▶ data/processed/vn_asns_enriched.csv

[07] Parse RouteViews MRT RIB dumps → sanitized domestic edges
 └─▶ data/processed/vn_bgp_edges.csv

[07c] Apply CAIDA AS-Rel classification to edges
 └─▶ data/processed/vn_edges_classified.csv

[08] Build comprehensive graph (NetworkX + Pyvis + GEXF)
 └─▶ data/graphs/vn_topology_comprehensive.html
 └─▶ data/graphs/vn_topology_comprehensive.gexf

[09] Compute topological metrics and generate report
 └─▶ data/graphs/analysis_report.txt
 └─▶ data/graphs/degree_distribution.png
```

### AS Path Sanitization (`lib/sanitize.py`)

Before extracting edges from BGP paths, the pipeline applies the following sanitization steps:
- **Prepend removal** — collapses repeated consecutive ASNs
- **IXP route-server removal** — strips known IXP ASNs (from PeeringDB/PCH)
- **Special ASN removal** — strips IANA-reserved ASNs (private, documentation, etc.)
- **Loop removal** — discards paths with AS loops
- **Domestic filter** — only edges where *both* endpoints are VN ASNs are retained

---

## Setup & Installation

### Prerequisites

- Python 3.9+
- [`bgpdump`](https://github.com/RIPE-NCC/bgpdump) for MRT file parsing

```bash
# macOS
brew install bgpdump
```

### Python Dependencies

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

**`requirements.txt`**
```
requests
beautifulsoup4
networkx
pandas
mrtparse
pyvis
matplotlib
numpy
```

---

## Running the Pipeline

Run each script from within the `scripts/` directory, in order:

```bash
cd scripts/

python 01_fetch_vn_asns.py
python 02_fetch_vn_prefixes.py
python 02b_fetch_caida_datasets.py
python 03_fetch_peeringdb.py
python 03b_fetch_global_ixp_asns.py
python 04_fetch_iana_special_asns.py
python 05_scrape_vnix.py
python 06b_enrich_nodes.py

# Download MRT RIB dumps manually into data/routeviews/ first
python 07_parse_routeviews.py
python 07c_apply_caida_as_rel.py

python 08_build_comprehensive_graph.py
python 09_analyze_insights.py
```

> **Note on MRT dumps:** Download RIB files manually from [RouteViews](http://archive.routeviews.org/) and place them as `.bz2` files in `data/routeviews/`. The pipeline was built against January 2026 snapshots to align with the CAIDA dataset timestamps.

---

## Output Files

| File | Description |
|---|---|
| `data/graphs/vn_topology_comprehensive.html` | Interactive Pyvis graph — open in any browser, no server needed |
| `data/graphs/vn_topology_comprehensive.gexf` | Graph exchange format for [Gephi](https://gephi.org/) |
| `vn-as-topo.gephi` | Pre-built Gephi project with layout applied |
| `data/graphs/degree_distribution.png` | Linear + log-log CCDF degree distribution plot |
| `data/graphs/analysis_report.txt` | Full topological metric report |
| `dataset.csv` | Consolidated AS dataset export |

### Visualization Color Legend (HTML)

| Color | Meaning |
|---|---|
| 🔴 Red | Top-10 hub ASNs by degree |
| 🟣 Purple | Transit ASNs (degree > 1) |
| ⬜ Gray | Stub ASNs (degree ≤ 1) |
| 🟠 Orange diamond | IXP nodes |
| Red edge | Provider→Customer (p2c) |
| Blue edge | Peer↔Peer (p2p) |
| Gray edge | Unknown relationship |
| Gold edge | IXP connection |

---

## Methodology Notes

- **Edge classification accuracy:** 93.8% of BGP-observed edges were matched to a known CAIDA relationship. Of those, 92.3% are `p2c` (provider-customer), consistent with the hierarchical nature of the Vietnamese ISP landscape.
- **Node enrichment accuracy:** 99.1% of domestic ASNs were matched to a CAIDA organizational record.
- **Isolated ASN filtering:** 510 ASNs registered with APNIC but showing no active BGP footprint were excluded from the final graph.
- **Disassortativity (−0.4611):** Confirms a classic hub-and-spoke topology where customer ASNs connect upward to high-degree transit providers rather than laterally to one another — a hallmark of national AS topologies globally.
- **5-core backbone:** The 20 ASNs forming the innermost k-core are the de facto "center of the universe" for domestic traffic transit in Vietnam.
