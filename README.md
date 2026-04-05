# Global Maritime Trade Simulation Pipeline

A pipeline for generating and simulating synthetic global shipping traffic from real bilateral trade data. Vessels are placed into a maritime network, routed between ports, and their movement is tracked over a simulated year — enabling analysis of how chokepoint disruptions and trade policy changes affect shipping flows.

## Pipeline Overview

```
Part 1 (Trade Data)  →  Part 2 (Volume Conversion)  →  Part 3 (Network)
                                                              ↓
Part 6 (Network Analysis)    Part 5 (Visualization)  ←  Part 4 (Simulation)
```

| Part | Folder | Description |
|------|--------|-------------|
| 1 | `part_1_trade_data/` | Pull bilateral trade data from UN COMTRADE API |
| 2 | `part_2_trade_volume_conversion/` | Convert USD trade values → metric tons using BACI |
| 3 | `part_3_network_extraction/` | Build & calibrate global maritime network from AIS + IMF data |
| 4 | `part_4_new_simulation/` | Generate synthetic ships and run the simulation |
| 5 | `part_5_visualization/` | Static maps, animated GIFs, scenario comparisons |
| 6 | `part_6_network_analysis/` | Betweenness centrality, chokepoint impact analysis |

## Scenarios Simulated

| Scenario | Description |
|----------|-------------|
| `baseline` | Normal conditions |
| `suez_50pct_reduction` | Suez Canal at 50% daily capacity |
| `suez_closure_permanent` | Suez Canal permanently closed |
| `suez_evergiven` | Suez Canal blocked days 82–88 (Ever Given), reactive rerouting |
| `panama_closure_permanent` | Panama Canal permanently closed |
| `hormuz_closure_permanent` | Strait of Hormuz permanently closed |
| `suez_50pct_hormuz_closure` | Suez at 50% + Hormuz permanently closed |
| `suez_50pct_hormuz_temp` | Suez at 50% + Hormuz closed days 58–120 |
| `eu_trade_deals_and_tariffs` | EU–Mercosur & EU–India FTAs + US tariffs on EU goods |

## Scale

- ~228K synthetic ships, 13.4B metric tons, ~$19T cargo value
- 592 ports across 174 countries, 24 maritime chokepoints
- 8,760 hourly simulation timesteps (365 simulated days)
- Network: ~8,624 nodes, ~14,471 edges

## Quick Start

```bash
# 1. Set scenario and write config
cd part_4_new_simulation/
python3 create_scenario_config.py baseline

# 2. Run full pipeline (routes → ship generation → simulation)
python3 run_pipeline.py 2>&1 | tee pipeline_baseline.log

# To run a different scenario after code changes:
python3 create_scenario_config.py <scenario_name>
rm -rf simulation_output_data/<scenario_dir>/checkpoints/
python3 run_pipeline.py 2>&1 | tee pipeline_<scenario_name>.log
```

Available scenarios: `baseline`, `suez_50pct_reduction`, `suez_closure_permanent`, `suez_evergiven`, `panama_closure_permanent`, `hormuz_closure_permanent`, `suez_50pct_hormuz_closure`, `suez_50pct_hormuz_temp`, `eu_trade_deals_and_tariffs`

## Key Design

- **Port selection**: IMF trade-share and vessel-type weights combined with a distance penalty (`exp(−λ × max(0, ratio−1))`), calibrated via Jensen-Shannon divergence grid search (α=0.535, β=0.891, λ=0.094)
- **Canal queuing**: Transit-slot model (Suez=14h, Panama=22h transit) calibrated to target utilisation ρ=0.7; ships exceeding daily capacity pre-rerouted around canals
- **Panama DWT restriction**: Ships ≥ 120,000 DWT always rerouted around Panama Canal
- **Rerouting**: K-shortest-paths (k=3) with patience multiplier; proactive (advance-notice) or reactive (no-notice) modes per scenario
- **Checkpoints**: Full simulation state pickled every 30 days; last 3 kept for resume

## What Is Not in This Repo

Large files are excluded from git (see `.gitignore`):
- `data/` — raw trade matrices, port data, fleet data (external)
- `ship_locations.parquet` — ~1.2 GB per scenario
- `ships.parquet` — ~46 MB per scenario
- `port_pair_routes.pkl` — ~111 MB per scenario
- `checkpoints/` — simulation state snapshots (250–380 MB each)
- `compat/` — CSV copies of parquet outputs (107–242 MB each)

## Key Libraries

`pandas` · `numpy` · `networkx` · `geopandas` · `matplotlib` · `scipy` · `tqdm` · `pyarrow` · `comtradeapicall`
