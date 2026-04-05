#!/usr/bin/env python3
"""
debug_preassign.py — Trace canal pre-assignment ship counts step-by-step.

Runs ship generation and all pre-simulation steps, printing counts at each
stage to diagnose discrepancies between the pre-assignment cap and the
number of ships the canal calibration model sees.

Usage (run from part_4_new_simulation/):
    python3 create_scenario_config.py <scenario_name>
    python3 debug_preassign.py
"""
import json
import math
import pickle
import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).parent))

from simulation_engine.config_loader import load_config, resolve_paths, get_interruption_events, get_economic_events
from simulation_engine.routing import (
    build_port_node_map,
    build_country_port_map,
    build_choke_point_node_map,
    preassign_canal_capacities,
    preassign_chokepoint_routes,
)
from simulation_engine.ship_generation import generate_all_ships
from simulation_engine.event_manager import build_epoch_schedule
from simulation_engine.port_manager import PortManager


def count_through(ships, node):
    return sum(1 for s in ships if node in s.path)


def section(title):
    print(f'\n{"=" * 60}')
    print(f'  {title}')
    print(f'{"=" * 60}')


def main():
    section('LOADING CONFIG')
    cfg = load_config()
    cfg = resolve_paths(cfg)

    output_dir = Path(cfg['OUTPUT_DIR'])
    simulation_days = cfg['SIMULATION_DAYS']
    interval_size   = cfg['INTERVAL_SIZE']
    n_intervals     = int(simulation_days / interval_size)
    canal_target_rho = cfg.get('CANAL_TARGET_RHO', 0.7)
    canal_config    = cfg.get('CANAL_CHOKEPOINTS', {})

    print(f'  OUTPUT_DIR:          {output_dir}')
    print(f'  CANAL_DAILY_RATES:   {cfg.get("CANAL_DAILY_RATES", {})}')
    print(f'  CANAL_TARGET_RHO:    {canal_target_rho}')
    print(f'  INTERRUPTION_EVENTS: {len(cfg.get("INTERRUPTION_EVENTS", []))} event(s)')
    print(f'  RANDOM_SEED:         {cfg.get("RANDOM_SEED")}')
    print(f'  n_intervals:         {n_intervals}')
    for name, rate in cfg.get('CANAL_DAILY_RATES', {}).items():
        cap = math.floor(canal_target_rho * rate * simulation_days)
        print(f'  Expected cap for {name}: floor({canal_target_rho} × {rate} × {simulation_days}) = {cap}')

    # -----------------------------------------------------------------------
    section('LOADING NETWORK')
    with open(cfg['NETWORK_FILE'], 'rb') as f:
        G = pickle.load(f)
    print(f'  Nodes: {G.number_of_nodes():,}  Edges: {G.number_of_edges():,}')

    port_name_to_node   = build_port_node_map(G)
    country_to_ports    = build_country_port_map(G)
    choke_name_to_node  = build_choke_point_node_map(G)
    choke_node_to_name  = {v: k for k, v in choke_name_to_node.items()}

    print('\n  Canal choke nodes:')
    for name in canal_config:
        node = choke_name_to_node.get(name)
        print(f'    {name!r}: node_id = {node!r}  (type: {type(node).__name__})')

    canal_names = set(canal_config.keys())

    # -----------------------------------------------------------------------
    section('LOADING ROUTES')
    routes_path  = output_dir / 'port_pair_routes.pkl'
    optimal_path = output_dir / 'country_pair_optimal.pkl'

    if not routes_path.exists():
        print(f'  ERROR: {routes_path} not found.')
        sys.exit(1)

    with open(routes_path, 'rb') as f:
        port_pair_routes = pickle.load(f)
    with open(optimal_path, 'rb') as f:
        country_pair_optimal = pickle.load(f)
    print(f'  Port-pair routes: {len(port_pair_routes):,}')

    # Verify that canal nodes actually appear in routes
    print('\n  Canal node presence in port_pair_routes:')
    for name in canal_config:
        node = choke_name_to_node.get(name)
        if node is None:
            print(f'    {name}: NODE NOT FOUND in choke map')
            continue
        routes_with_node = sum(1 for r in port_pair_routes.values() if node in r['path'])
        print(f'    {name} (node {node!r}): present in {routes_with_node:,} / {len(port_pair_routes):,} routes')

    # -----------------------------------------------------------------------
    section('GENERATING SHIPS')
    imf_port_df = pd.read_csv(cfg['IMF_PORT_DATA_FILE'])
    baci_codes  = pd.read_csv(cfg['BACI_CODES_FILE'])
    iso3_to_baci = dict(zip(baci_codes['country_iso3'], baci_codes['country_name']))
    iso3_to_baci['TWN'] = iso3_to_baci.get('S19', iso3_to_baci.get('TWN'))
    imf_port_df['baci_name'] = imf_port_df['ISO3'].map(iso3_to_baci)
    imf_port_df = imf_port_df[imf_port_df['baci_name'].notna()].copy()

    seed = cfg.get('RANDOM_SEED')
    rng  = np.random.default_rng(seed)

    all_economic_events  = get_economic_events(cfg)
    baseline_events      = [e for e in all_economic_events if e.day == 0]
    mid_sim_events       = [e for e in all_economic_events if e.day > 0]
    epoch_schedule       = build_epoch_schedule(simulation_days, mid_sim_events)

    all_ships, _, _, _ = generate_all_ships(
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
    print(f'\n  Total ships generated: {len(all_ships):,}')

    # -----------------------------------------------------------------------
    section('COUNTS BEFORE ANY PREASSIGNMENT')
    for name in canal_config:
        node = choke_name_to_node.get(name)
        if node is None:
            print(f'  {name}: node not found')
            continue
        count = count_through(all_ships, node)
        print(f'  {name}: {count:,} ships have the canal node in .path')

    # Sample a ship that goes through Suez to track it
    suez_node = choke_name_to_node.get('Suez Canal')
    tracked_ship = None
    if suez_node is not None:
        candidates = [s for s in all_ships if suez_node in s.path]
        if candidates:
            tracked_ship = candidates[0]
            idx = tracked_ship.path.index(suez_node)
            print(f'\n  Tracking ship id={tracked_ship.id}:')
            print(f'    .path id (object): {id(tracked_ship.path)}')
            print(f'    Suez node at index {idx} in path of length {len(tracked_ship.path)}')
            print(f'    Path slice around Suez: ...{tracked_ship.path[max(0,idx-2):idx+3]}...')

    # -----------------------------------------------------------------------
    section('STEP 1 — preassign_canal_capacities')
    _canal_daily_rates      = cfg.get('CANAL_DAILY_RATES', {})
    _canal_dwr_restrictions = cfg.get('CANAL_DWT_RESTRICTIONS', {})

    if not _canal_daily_rates:
        print('  No CANAL_DAILY_RATES configured — skipping.')
    else:
        n_canal_rerouted = preassign_canal_capacities(
            all_ships=all_ships,
            G=G,
            choke_name_to_node=choke_name_to_node,
            canal_daily_rates=_canal_daily_rates,
            canal_dwr_restrictions=_canal_dwr_restrictions,
            target_rho=canal_target_rho,
            simulation_days=simulation_days,
            rng=rng,
        )
        print(f'  Total rerouted: {n_canal_rerouted:,}')

    print('\n  Counts after preassign_canal_capacities:')
    for name in canal_config:
        node = choke_name_to_node.get(name)
        if node is not None:
            count = count_through(all_ships, node)
            rate  = _canal_daily_rates.get(name)
            cap   = math.floor(canal_target_rho * rate * simulation_days) if rate else '—'
            print(f'    {name}: {count:,} ships  (expected cap: {cap})')

    if tracked_ship is not None:
        still_in = suez_node in tracked_ship.path
        print(f'\n  Tracked ship {tracked_ship.id}:')
        print(f'    Suez still in .path: {still_in}')
        print(f'    Current .path id:    {id(tracked_ship.path)}')

    # Collect capped canal nodes to block in chokepoint rerouting
    capped_canal_nodes = set()
    for name in _canal_daily_rates:
        node = choke_name_to_node.get(name)
        if node is not None:
            capped_canal_nodes.add(node)

    # -----------------------------------------------------------------------
    section('STEP 2 — preassign_chokepoint_routes')
    _interruption_events = get_interruption_events(cfg)
    choke_base_throughputs = {k: None for k in choke_name_to_node}

    n_rerouted, pre_cancelled = preassign_chokepoint_routes(
        all_ships=all_ships,
        G=G,
        events=_interruption_events,
        choke_name_to_node=choke_name_to_node,
        choke_base_throughputs=choke_base_throughputs,
        canal_names=canal_names,
        n_intervals=n_intervals,
        rng=rng,
        blocked_nodes=capped_canal_nodes,
    )
    print(f'  Ships rerouted: {n_rerouted:,}')
    print(f'  Ships cancelled: {len(pre_cancelled)}')

    print('\n  Counts after preassign_chokepoint_routes:')
    for name in canal_config:
        node = choke_name_to_node.get(name)
        if node is not None:
            print(f'    {name}: {count_through(all_ships, node):,} ships')

    if tracked_ship is not None:
        still_in = suez_node in tracked_ship.path
        print(f'\n  Tracked ship {tracked_ship.id}:')
        print(f'    Suez still in .path: {still_in}')
        print(f'    .path id: {id(tracked_ship.path)}')

    # -----------------------------------------------------------------------
    section('STEP 3 — compute_canal_capacities (calibration)')
    canal_transit_intervals = {
        name: max(1, round(hours / (interval_size * 24)))
        for name, hours in canal_config.items()
    }

    canal_capacities = PortManager.compute_canal_capacities(
        all_ships=all_ships,
        canal_names=list(canal_config.keys()),
        canal_transit_intervals=canal_transit_intervals,
        choke_node_to_name=choke_node_to_name,
        n_intervals=n_intervals,
        target_rho=canal_target_rho,
    )

    print('  Canal calibration results:')
    for name, slots in canal_capacities.items():
        node  = choke_name_to_node.get(name)
        count = count_through(all_ships, node) if node else 0
        rate  = _canal_daily_rates.get(name)
        cap   = math.floor(canal_target_rho * rate * simulation_days) if rate else '—'
        print(f'    {name}:')
        print(f'      ships in .path right now: {count:,}  (cap should be: {cap})')
        print(f'      calibration counted:       {count:,}')
        print(f'      transit slots assigned:    {slots}')

    # -----------------------------------------------------------------------
    section('SUMMARY')
    print('  Pre-assignment chain complete. Simulation NOT run.')
    print()
    for name in canal_config:
        node = choke_name_to_node.get(name)
        if node is None:
            continue
        final_count = count_through(all_ships, node)
        rate = _canal_daily_rates.get(name)
        cap  = math.floor(canal_target_rho * rate * simulation_days) if rate else None
        ok   = '✓' if (cap is None or final_count <= cap) else '✗  OVER CAP'
        print(f'  {name}: {final_count:,} ships in path   cap={cap}   {ok}')


if __name__ == '__main__':
    main()
