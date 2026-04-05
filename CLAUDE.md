# Simulation Pipeline — Project Summary

Global maritime trade simulation pipeline. Generates synthetic shipping traffic from real trade data, simulates a year of vessel movement through a network, and visualizes the results.

## Pipeline Overview

```
Part 1 (Trade Data)  →  Part 2 (Volume Conversion)  →  Part 3 (Network)
                                                              ↓
Part 6 (Network Analysis)    Part 5 (Visualization)  ←  Part 4 (Simulation)
```

## Part 1: Trade Data Extraction (`part_1_trade_data/`)
- Notebook: `comtrade_data_pipeline.ipynb`
- Pulls bilateral trade data from UN COMTRADE API (key: `../../COMTRADE_API_KEY.json`)
- Config: `SELECTED_COUNTRIES`, `TRANSPORT_MODE`, `HS_CODE`, `PERIOD`
- Output: trade matrix CSVs (exporter × importer, values in USD)

## Part 2: Trade Volume Conversion (`part_2_trade_volume_conversion/`)
- Notebook: `trade_unit_conversion.ipynb`
- Converts trade values (USD) → weights (metric tons) using BACI reference data
- Produces `trade_volume_conversion_output/trade_volume_conversion_general.json` (conversion factors per HS chapter) and `trade_volume_conversion_bilateral_percentiles.json`
- Output: **194 trade matrix CSVs** in `../../data/all_trade_matrices/`:
  - `value_trade_matrix_all_transport_modes_HS{N}.csv` — USD values (97 files, HS1–97 excl. HS77)
  - `weight_trade_matrix_all_transport_modes_HS{N}.csv` — metric tons (97 files, incl. TOTAL)
  - Ship generation only uses the `weight_*` files
  - Format: rows = exporters, columns = importers

## Part 3: Network Extraction & Calibration (`part_3_network_extraction/`)

### 3A: `network_extraction.ipynb`
- Builds global maritime network from AIS density data + IMF port data
- **Port source:** `../../data/port_data_imf.csv` (IMF, ISO3 codes, joined to BACI via `BACI_country_codes.csv`)
- **Port selection:** `select_ports(df, PORT_COVERAGE_THRESHOLD=75)` — greedy per ISO3, adds ports until cumulative import AND export share both reach 75%; always ≥1 port per country → **592 ports, 174 countries**
- **Special handling:** `TWN` → `S19` (Taiwan BACI code)
- **Node ID format:** `f"port_{portid}"` (e.g. `port_1234`)
- **Node attributes:** `portid`, `portname`, `ISO3`, `country` (baci_name), `lat`, `lon`, `source='port'`
- **Choke points:** loaded from `../../data/maritime_chokepoints.csv`; merged into density network within `CHOKEPOINT_MERGE_RADIUS=1.5°`; node attrs: `source='choke_point'`, `name`
- **Checkpoint system** in `network_outputs/checkpoints/`:
  - `aggregated_data.parquet` — post k-means
  - `aggregated_data_filtered.parquet`
  - `G_density.gpickle`
  - `G_with_ports_pre_manual.gpickle`
  - `G_dp_pre_isolation.gpickle`
  - `large_ports.parquet`, `choke_ids.json`
  - `used_edges.pkl`, `used_nodes.pkl`
- **Raw output:** `network_outputs/network_dp.gpickle` + `.graphml` (~7,241 nodes, ~13,570 edges)

### 3B: `network_calibration.ipynb`
- Applies manual corrections and prunes orphaned nodes via A*
- Config (currently all empty, must re-populate after re-run with IMF data):
  - `EXCLUDED_PORTS_BY_PORTID = []`
  - `MANUAL_EDGES = []`  — `(portid_or_chokepoint_name, portid_or_chokepoint_name)` tuples
  - `PORTS_WITH_MANUAL_ONLY_CONNECTIONS = []`
- Step 0: exclude ports, Step 1: add manual edges, Step 2: isolate manual-only ports, Step 3: A* prune
- **Calibrated output:** `network_outputs/network_calibrated.gpickle` + `.graphml` (~6,916 nodes, ~11,534 edges)
- Also: `network_calibrated_v1.gpickle` — an earlier saved version

### Utility notebooks
- `imf_country_check.ipynb` — diagnostic checks on IMF port coverage vs BACI countries
- `wpi_country_check.ipynb` — WPI data cross-check

## Part 4: Simulation

### Legacy (`part_4_simulation/`) — OUTDATED, do not use
Old architecture. Ship data in `simulation_output_data/simulation_ship_data.csv`. Not maintained.

### Active (`part_4_new_simulation/`) — CURRENT ARCHITECTURE
All logic in `simulation_engine/` Python modules; notebooks are thin wrappers.

#### Workflow (run in order):
1. **`simulation_config.ipynb`** — set all params, run to write `simulation_config.json`
2. **`00_precompute_routes.ipynb`** — A* for all port-pair routes (~45–60 min for 592 ports); rerun only if network changes; routes are shared across scenarios
3. **`01_ship_generation.ipynb`** — generates ships and saves to Parquet
4. **`02_simulation.ipynb`** — runs simulation, writes Parquet outputs + optional CSV compat
5. **`02a_optimize_port_weights.ipynb`** — optional grid search to calibrate port selection weights (α, β, λ) via JS divergence

Also: **`create_scenario_config.py`** — CLI script to write scenario-specific `simulation_config.json` overrides.
Available scenarios (8 total): `baseline`, `suez_50pct_reduction`, `panama_closure_permanent`, `hormuz_closure_permanent`, `suez_evergiven`, `suez_closure_permanent`, `suez_50pct_hormuz_closure`, `suez_50pct_hormuz_temp`, `eu_trade_deals_and_tariffs`.

Also: **`run_pipeline.py`** — end-to-end CLI runner (steps 1–3). Skips route precomputation if cached files exist. Always uses `resume_from_checkpoint=True`. **Before re-running a scenario after code changes, delete `checkpoints/` in that scenario's output directory to avoid resuming from stale ship states.**

Also: **`run_pipeline.sh`** — shell script for multi-scenario batch runs. Waits for `network_dp.gpickle` to be produced by network extraction, then runs calibration, writes base config, and runs baseline + suez_50pct_reduction + hormuz_closure_permanent in sequence. Copies baseline port-pair routes to other scenario dirs to avoid recomputation.

Also: **`debug_preassign.py`** — debug/diagnostic script for the pre-assignment step.

Standard re-run workflow:
```bash
# from part_4_new_simulation/
python3 create_scenario_config.py <scenario_name>
rm -rf simulation_output_data/<scenario_output_dir>/checkpoints/
python3 run_pipeline.py 2>&1 | tee pipeline_<scenario_name>.log
```

#### Config Parameters (`simulation_config.ipynb` → `simulation_config.json`):
| Group | Key Parameters |
|-------|---------------|
| Temporal | `SIMULATION_DAYS=365`, `INTERVAL_SIZE=1/24` (1h), `RANDOM_SEED=42` |
| Paths | `NETWORK_FILE`, `DATA_DIR`, `TRADE_MATRICES_DIR`, `HS_CODES_MAPPING_FILE`, `CONVERSION_FACTORS_FILE`, `MERCHANT_FLEET_FILE`, `IMF_PORT_DATA_FILE`, `BACI_CODES_FILE`, `OUTPUT_DIR` |
| Ships | `SHIP_SPEEDS` (tanker=25, bulk=28, cargo=32 km/h); `PORT_LOADING_TIMES` (tanker=1.04d, bulk=2.13d, cargo=0.71d); `PORT_UNLOADING_TIMES` (same); `CAPACITY_QUANTILE=0.99`, `DIRICHLET_CONCENTRATION=1` |
| Port selection | `PORT_WEIGHT_ALPHA=0.5349` (size exponent, calibrated), `PORT_WEIGHT_BETA=0.8908` (type exponent, calibrated), `DISTANCE_PENALTY_SCALE=0.0942` (λ, calibrated via 02a optimizer) |
| Queuing | `TARGET_RHO=0.8`, `MIN_PORT_CAPACITY=1`, `CHOKE_POINT_THROUGHPUT={}` |
| Canals | `CANAL_CHOKEPOINTS` (Suez=14h, Panama=22h transit times), `CANAL_TARGET_RHO=0.7`, `CANAL_DAILY_RATES` (Suez=38, Panama=17 ships/day at baseline), `CANAL_DWT_RESTRICTIONS` (Panama: `exclude_above_dwt=120000`, ships ≥ 120K DWT always rerouted) |
| Rerouting | `K_ALTERNATIVE_ROUTES=3`, `REROUTE_PATIENCE_MULTIPLIER=1.0`, `PROACTIVE_REROUTING=True` |
| Events | `INTERRUPTION_EVENTS=[]`, `ECONOMIC_EVENTS=[]` |
| Output | `CHECKPOINT_INTERVAL_DAYS=30`, `SAVE_SHIP_LOCATIONS=True`, `LOCATION_SAMPLE_INTERVAL=1`, `BACKWARD_COMPAT_CSV=True` |

> **Note:** The current `simulation_config.json` on disk reflects the last-run scenario (`eu_trade_deals_and_tariffs`). Always run `create_scenario_config.py <scenario>` to reset it before running a different scenario.

#### Scenarios (defined in `create_scenario_config.py`):
| Scenario | Output Dir | Key Override |
|----------|-----------|--------------|
| `baseline` | `scenario_baseline/` | Suez=38/day, Panama=17/day, no events |
| `suez_50pct_reduction` | `scenario_suez_50pct_reduction/` | Suez=19/day (half rate) |
| `panama_closure_permanent` | `scenario_panama_closure_permanent/` | Panama closed day 0, permanent |
| `hormuz_closure_permanent` | `scenario_hormuz_closure_permanent/` | Hormuz closed day 0, cancel_if_no_alternative=True |
| `suez_closure_permanent` | `scenario_suez_closure_permanent/` | Suez closed day 0, permanent |
| `suez_50pct_hormuz_closure` | `scenario_suez_50pct_hormuz_closure/` | Suez=19/day + Hormuz closed day 0 |
| `suez_evergiven` | `scenario_suez_evergiven/` | Suez closed days 82–88 only, **PROACTIVE_REROUTING=False** |
| `suez_50pct_hormuz_temp` | `scenario_suez_50pct_hormuz_temp/` | Suez=19/day + Hormuz closed days 58–120 |
| `eu_trade_deals_and_tariffs` | `scenario_eu_trade_deals_and_tariffs/` | EU–Mercosur (+39%/+17%), EU–India (+65%/+87%), US tariffs on EU (−17%) via day-0 bilateral EconomicEvents |

All 8 scenario output directories currently exist in `simulation_output_data/`.

`CANAL_DAILY_RATES` must be set explicitly in every scenario (not reliably inherited from base config). All scenarios use `{'Suez Canal': 38, 'Panama Canal': 17}` except `suez_50pct_*` which use `{'Suez Canal': 19, 'Panama Canal': 17}`.

#### Key Design Decisions:
- Ships injected by sorted `injection_day` (deterministic, not Poisson)
- Economic events split sim into epochs; each epoch uses adjusted trade matrices (multiplicative; day-0 = baseline)
- Port selection scoring: `size_score^α × type_score^β × distance_penalty` where `distance_penalty = exp(−λ × max(0, ratio−1))`, `ratio = proposed_route_km / country_pair_optimal_km`; zero vessel counts → uniform type weights
- Port times use log-ratio scaling: `loading_time = Poisson(max(0.5, log(1 + dwt/avg_dwt) × scale_factor))` where scale_factor is calibrated so the mean matches `PORT_LOADING_TIMES`
- Destination failover: closed port → nearest open port in same country → else `LostShip`
- Choke throughput: `None` = passthrough; if multiplier<1 and base=None → fallback 5 ships/interval
- `CANAL_CHOKEPOINTS` get transit-slot queuing calibrated to `CANAL_TARGET_RHO`; other chokes use `CHOKE_POINT_THROUGHPUT`
- `CANAL_DWT_RESTRICTIONS`: per-canal DWT rules applied during `preassign_canal_capacities()`. Ships with DWT ≥ `exclude_above_dwt` are always rerouted. Per-band `daily_rate` caps can further restrict by DWT range.
- **Pre-assignment order** (runs before simulation loop):
  1. `preassign_canal_capacities()` — caps each canal's annual ship count to `floor(CANAL_TARGET_RHO × daily_rate × 365)`; processes canals sequentially, removing each processed node from G before next (prevents cascade overflow). Routes by (origin_node, dest_node) are cached for efficiency.
  2. `preassign_chokepoint_routes()` — reroutes ships around day-0 permanent closures (multiplier=0). Receives `blocked_nodes` = all capped canal nodes so rerouted ships cannot spill into already-full canals. For partial non-canal chokes: proportional random assignment.
- **`PROACTIVE_REROUTING`** (read at runtime in `simulation_runner.py`):
  - `True` (default): when a mid-sim choke closure fires, immediately reroutes all ships with that choke in their remaining path
  - `False` (evergiven): closure applied (capacity→0), NO rerouting — ships queue as `waiting_at_node`, drain through after restoration event fires
- Checkpoints: full pickle every 30 days; last 3 kept; RNG state saved for reproducibility

#### `simulation_engine/` Modules:
| Module | Responsibility |
|--------|---------------|
| `models.py` | `Ship`, `InterruptionEvent`, `EconomicEvent`, `LostShip` dataclasses; `SHIP_STATES` set |
| `config_loader.py` | Load/validate `simulation_config.json`; `resolve_paths()`; `get_interruption_events()`; `get_economic_events()` |
| `routing.py` | `build_port_node_map()`, `build_country_port_map()`, `build_choke_point_node_map()`, `compute_all_port_pair_routes()`, `derive_country_pair_optimal()`, `load_or_compute_port_pair_routes()`, `get_k_shortest_paths()`, `compute_path_travel_time_intervals()`, `evaluate_reroute()`, `find_nearest_open_port_in_country()`, `preassign_chokepoint_routes()`, `preassign_canal_capacities()` |
| `port_manager.py` | M/M/c berth queues + choke FIFO + canal transit slots; capacity multipliers (interruption-aware); `compute_base_capacities()`, `compute_canal_capacities()` static methods; `state_dict()`/`load_state_dict()` for checkpointing |
| `event_manager.py` | Min-heap priority queue; `schedule_all()`; `pop_events(up_to_day)`; auto-generates restoration event at `end_day`; `build_epoch_schedule()` for economic events |
| `io_manager.py` | Parquet I/O (`write_parquet`, `append_parquet`), `LocationBuffer` streaming, checkpoint save/load (keep last 3), CSV compat export (`export_compat_csvs`), `print_simulation_summary()` |
| `ship_generation.py` | `generate_all_ships()` entry point; `generate_ships_for_epoch()`; `build_port_selection_data()`; `calibrate_port_times()`; Gamma DWT distributions via `build_ship_distributions()` |
| `simulation_runner.py` | `run_simulation()` main loop; 8,760 hourly timesteps; handles checkpoint resume, pre-assignment, PortManager setup, event firing |
| `port_weight_optimizer.py` | Grid search optimizer for port selection weights (α, β, λ) via Jensen-Shannon divergence; objective = log(JS_export) + log(JS_import) + log(JS_type) |

#### Output Files (`simulation_output_data/<scenario>/`):
| File | Description |
|------|-------------|
| `port_pair_routes.pkl` | `{(portname_A, portname_B): {'path': [node,...], 'length': float}}` — from `00_precompute_routes` |
| `country_pair_optimal.pkl` | `{(cA, cB): {'origin_port', 'dest_port', 'optimal_length'}}` — from `00_precompute_routes` |
| `ships.parquet` | ship_id, origin/dest country+port, ship_type, injection_day, cargo_total_weight/value, cargo_hs{N}_weight/value, rerouted, reroute_count, was_lost |
| `edge_statistics.parquet` | node1, node2, edge_length_km, ship_count, total_time_hours, cargo_total_weight/value, cargo_hs{N}_* |
| `port_occupancy.parquet` | timestep, day, port_name, num_ships, capacity |
| `ship_locations.parquet` | timestep, day, ship_id, status, node1, node2, edge_length_km, progress_fraction, port_name |
| `lost_ships.parquet` | ship_id, day_lost, reason, origin/dest country+port, cargo |
| `port_cargo.parquet` | port_name, ship_count, cargo_total_weight/value, cargo_hs{N}_* |
| `choke_cargo.parquet` | choke_name, ship_count, cargo_total_weight/value, cargo_hs{N}_* |
| `common_countries.json` | list of countries in both network and trade matrices |
| `country_to_ports.json` | `{country: [portname, ...]}` |
| `port_name_to_node.pkl` | `{portname: node_id}` |
| `checkpoints/checkpoint_day_*.pkl` | full state pickle every 30 days; last 3 kept; includes RNG state |
| `compat/*.csv` | CSV copies of parquet outputs for Part 5 notebooks (if `BACKWARD_COMPAT_CSV=True`) |

`ship_locations.parquet` column notes:
- `waiting_at_node` ships: node1 = str(canal/choke node ID), node2 = next node
- `canal_transit` ships: port_name = canal name (e.g. `'Suez Canal'`)
- Canal choke node IDs looked up from graph G at runtime (not hardcoded)

#### Ship State Machine:
`waiting_to_load → loading → traveling → waiting_at_node → [canal_transit →] traveling → waiting_to_unload → unloading → completed`

## Part 5: Visualization (`part_5_visualization/`)
- `Simulation_Visualizations.ipynb` — static maps, charts, cargo analysis by HS code
- `Video_Simulation.ipynb` — animated GIF/MP4 (720 frames @ 20 fps); reads `ship_locations.parquet`
- `Scenario_Comparison.ipynb` — side-by-side comparison of multiple scenario outputs; queue/occupancy plots show `waiting_at_node` ships (not `canal_transit`); canal choke node IDs built dynamically from G
- `Trade_Flow_Visualization.ipynb` — visualizes trade flow data from Part 2 matrices
- Loads network from `../part_3_network_extraction/network_outputs/network_calibrated.gpickle`
- Reads from `compat/*.csv` or Parquet outputs

## Part 6: Network Analysis (`part_6_network_analysis/`)
- Notebook: `network_analysis.ipynb`
- Analyzes calibrated network structure: betweenness centrality, choke point removal impact,
  regional maps, port connectivity
- Input: `../part_3_network_extraction/network_outputs/network_calibrated.gpickle`

## Key Data Files (external, relative to `simulation_pipeline/`)
- `../../data/port_data_imf.csv` — IMF port data (portid, portname, ISO3, lat, lon, share_country_maritime_export, share_country_maritime_import, vessel_count_tanker, vessel_count_dry_bulk, vessel_count_container, vessel_count_general_cargo, vessel_count_RoRo, continent)
- `../../data/raw_trade_data/BACI_total_trade_volume_2024.csv` — trade flows (2024 data)
- `../../data/raw_trade_data/BACI_country_codes.csv` — 238 countries (country_iso3, country_name)
- `../../data/all_trade_matrices/` — 194 trade matrix CSVs (value + weight, HS1–97 excl. HS77)
- `../../data/hs_codes_mapping.json` — HS code names and ship_type assignments
- `../../data/merchant_fleet_data.csv` — fleet data for DWT distributions (Ship Type, Avg. dwt per ship, Max dwt per ship)
- `../../data/maritime_chokepoints.csv` — choke point coordinates

## Scale (current runs)
- **~228K ships**, 13.4B metric tons, ~$19T cargo per 365-day scenario
- **~592 ports**, 174 countries, 24 choke points, ~168 countries in trade simulation
- **~8,624 nodes, ~14,471 edges** in calibrated network
- 8,760 hourly timesteps (365 days × 24 intervals/day)
- 8 scenarios completed: baseline, suez_50pct_reduction, panama_closure_permanent, suez_closure_permanent, suez_50pct_hormuz_closure, suez_50pct_hormuz_temp, suez_evergiven, eu_trade_deals_and_tariffs

## Key Libraries
pandas, numpy, networkx, geopandas, osmnx, matplotlib, scipy, sklearn, pickle, tqdm, pyarrow, comtradeapicall
