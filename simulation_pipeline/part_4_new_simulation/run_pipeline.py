#!/usr/bin/env python3
"""
run_pipeline.py — Run the full Part 4 simulation pipeline end-to-end.

Steps (all outputs written to cfg['OUTPUT_DIR']):
  1. Pre-compute port-pair routes        (skipped if already cached in OUTPUT_DIR)
  2. Generate ships from trade matrices
  3. Run the simulation                  (resumes from checkpoint if one exists)

Usage:
    python run_pipeline.py

The scenario is controlled entirely by simulation_config.json.
To run a different scenario, update OUTPUT_DIR in simulation_config.ipynb
(or use create_scenario_config.py) before running this script.
"""

import json
import pickle
import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).parent))

from simulation_engine.config_loader import (
    load_config, resolve_paths, get_economic_events,
)
from simulation_engine.event_manager import build_epoch_schedule
from simulation_engine.routing import (
    build_port_node_map,
    build_country_port_map,
    compute_all_port_pair_routes,
    derive_country_pair_optimal,
)
from simulation_engine.ship_generation import generate_all_ships
from simulation_engine.io_manager import write_parquet
from simulation_engine.simulation_runner import run_simulation


# ---------------------------------------------------------------------------
# Step 1 — route pre-computation
# ---------------------------------------------------------------------------

def step_precompute_routes(cfg, G):
    output_dir = Path(cfg['OUTPUT_DIR'])
    output_dir.mkdir(parents=True, exist_ok=True)

    routes_path  = output_dir / 'port_pair_routes.pkl'
    optimal_path = output_dir / 'country_pair_optimal.pkl'

    port_name_to_node = build_port_node_map(G)
    country_to_ports  = build_country_port_map(G)

    if routes_path.exists() and optimal_path.exists():
        print(f'  Routes already cached — loading from {output_dir}')
        with open(routes_path, 'rb') as f:
            port_pair_routes = pickle.load(f)
        with open(optimal_path, 'rb') as f:
            country_pair_optimal = pickle.load(f)
        print(f'  {len(port_pair_routes):,} port-pair routes loaded.')
        return port_pair_routes, country_pair_optimal

    n_ports = len(port_name_to_node)
    print(f'  Ports: {n_ports}  |  Directed pairs: {n_ports*(n_ports-1):,}')
    print('  Computing routes (expect ~45-60 min)...')

    port_pair_routes = compute_all_port_pair_routes(G, port_name_to_node, show_progress=True)
    print(f'  Computed {len(port_pair_routes):,} routes.')

    with open(routes_path, 'wb') as f:
        pickle.dump(port_pair_routes, f)

    country_pair_optimal = derive_country_pair_optimal(port_pair_routes, country_to_ports)

    with open(optimal_path, 'wb') as f:
        pickle.dump(country_pair_optimal, f)

    print(f'  Saved to {output_dir}')
    return port_pair_routes, country_pair_optimal


# ---------------------------------------------------------------------------
# Step 2 — ship generation
# ---------------------------------------------------------------------------

def step_generate_ships(cfg, G, port_pair_routes, country_pair_optimal):
    output_dir = Path(cfg['OUTPUT_DIR'])
    output_dir.mkdir(parents=True, exist_ok=True)

    # Load IMF port data + BACI country codes
    imf_port_df  = pd.read_csv(cfg['IMF_PORT_DATA_FILE'])
    baci_codes   = pd.read_csv(cfg['BACI_CODES_FILE'])
    iso3_to_baci = dict(zip(baci_codes['country_iso3'], baci_codes['country_name']))
    iso3_to_baci['TWN'] = iso3_to_baci.get('S19', iso3_to_baci.get('TWN'))
    imf_port_df['baci_name'] = imf_port_df['ISO3'].map(iso3_to_baci)
    imf_port_df = imf_port_df[imf_port_df['baci_name'].notna()].copy()
    print(f'  IMF ports loaded: {len(imf_port_df):,}')

    seed = cfg.get('RANDOM_SEED')
    rng  = np.random.default_rng(seed)

    all_economic_events = get_economic_events(cfg)
    baseline_events = [e for e in all_economic_events if e.day == 0]
    mid_sim_events  = [e for e in all_economic_events if e.day > 0]
    epoch_schedule  = build_epoch_schedule(cfg['SIMULATION_DAYS'], mid_sim_events)

    all_ships, common_countries, country_to_ports, port_name_to_node = generate_all_ships(
        cfg=cfg,
        G=G,
        port_pair_routes=port_pair_routes,
        country_pair_optimal=country_pair_optimal,
        imf_port_df=imf_port_df,
        economic_events_baseline=baseline_events,
        epoch_schedule=epoch_schedule,
        rng=rng,
        show_progress=True,
    )
    print(f'  Generated {len(all_ships):,} ships.')

    # Save ships.parquet
    ships_df = pd.DataFrame([s.to_record() for s in all_ships])
    write_parquet(ships_df, str(output_dir / 'ships.parquet'), append=False)

    # Save auxiliary lookup files
    with open(output_dir / 'common_countries.json', 'w') as f:
        json.dump(common_countries, f)
    with open(output_dir / 'country_to_ports.json', 'w') as f:
        json.dump(country_to_ports, f)
    with open(output_dir / 'port_name_to_node.pkl', 'wb') as f:
        pickle.dump(port_name_to_node, f)

    print(f'  Saved ships.parquet and auxiliary files to {output_dir}')
    return all_ships, common_countries, country_to_ports, port_name_to_node


# ---------------------------------------------------------------------------
# Step 3 — simulation
# ---------------------------------------------------------------------------

def step_run_simulation(cfg, G, all_ships, common_countries, country_to_ports, port_name_to_node):
    run_simulation(
        cfg=cfg,
        G=G,
        all_ships=all_ships,
        common_countries=common_countries,
        country_to_ports=country_to_ports,
        port_name_to_node=port_name_to_node,
        resume_from_checkpoint=True,   # safe to resume if interrupted
    )


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    print('=' * 60)
    print('MARITIME SIMULATION PIPELINE')
    print('=' * 60)

    cfg = load_config()
    cfg = resolve_paths(cfg)
    print(f'Scenario output directory: {cfg["OUTPUT_DIR"]}')
    print()

    # Load network once — reused by all steps
    print('[Network] Loading...')
    with open(cfg['NETWORK_FILE'], 'rb') as f:
        G = pickle.load(f)
    print(f'  Nodes: {G.number_of_nodes()}  |  Edges: {G.number_of_edges()}')
    print()

    print('[Step 1/3] Pre-computing port-pair routes...')
    port_pair_routes, country_pair_optimal = step_precompute_routes(cfg, G)
    print()

    print('[Step 2/3] Generating ships...')
    all_ships, common_countries, country_to_ports, port_name_to_node = step_generate_ships(
        cfg, G, port_pair_routes, country_pair_optimal
    )
    print()

    print('[Step 3/3] Running simulation...')
    step_run_simulation(cfg, G, all_ships, common_countries, country_to_ports, port_name_to_node)
    print()

    print('Pipeline complete.')


if __name__ == '__main__':
    main()
