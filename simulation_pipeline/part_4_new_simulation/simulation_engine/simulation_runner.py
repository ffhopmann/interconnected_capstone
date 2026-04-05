"""
simulation_runner.py — Main hourly simulation loop.

Key changes from the original Mediterranean_Model.ipynb:
  1. Ships injected deterministically by injection_day (sorted queue) rather
     than via Poisson arrivals — more reproducible across runs.
  2. Choke point throughput limits: ships queue at choke point nodes and
     evaluate the reroute decision each interval.
  3. Destination port failover: when a destination port is closed, try the
     nearest open port in the same country; otherwise log as lost cargo.
  4. Physical interruption events applied at their scheduled day.
  5. Parquet output streamed incrementally via LocationBuffer + checkpoints.
  6. RNG state saved in checkpoints for exact reproducibility on resume.
"""

from __future__ import annotations
import math
import pickle
from collections import deque
from pathlib import Path
from typing import Any, Deque, Dict, List, Optional, Set, Tuple

import networkx as nx
import numpy as np
import pandas as pd
from tqdm import tqdm

from .event_manager import EventManager, build_epoch_schedule
from .io_manager import (
    LocationBuffer,
    append_parquet,
    build_choke_cargo_df,
    build_edge_statistics_df,
    build_port_cargo_df,
    build_port_occupancy_df,
    export_compat_csvs,
    load_checkpoint,
    print_simulation_summary,
    save_checkpoint,
    write_parquet,
)
from .models import EconomicEvent, InterruptionEvent, LostShip, Ship
from .port_manager import PortManager
from .routing import (
    build_choke_point_node_map,
    compute_shortest_path,
    find_nearest_open_port_in_country,
    preassign_canal_capacities,
    preassign_chokepoint_routes,
)


# ---------------------------------------------------------------------------
# Edge traffic helpers
# ---------------------------------------------------------------------------

def _norm_edge(u: Any, v: Any) -> Tuple[Any, Any]:
    """Canonical undirected edge key."""
    try:
        return (u, v) if u < v else (v, u)
    except TypeError:
        return (u, v) if str(u) < str(v) else (v, u)


def _make_edge_entry(hs_codes: List[int]) -> Dict:
    entry: Dict = {
        'ship_count':         0,
        'cargo_total_weight': 0.0,
        'cargo_total_value':  0.0,
        'total_time_hours':   0.0,
    }
    for hs in hs_codes:
        entry[f'cargo_hs{hs}_weight'] = 0.0
        entry[f'cargo_hs{hs}_value']  = 0.0
    return entry


def _attribute_cargo_to_edge(
    edge_traffic: Dict,
    edge_key: Tuple,
    ship: Ship,
    hs_codes: List[int],
) -> None:
    """Add ship's cargo to an edge (called once per ship per edge)."""
    data = edge_traffic[edge_key]
    data['ship_count']         += 1
    data['cargo_total_weight'] += ship.cargo_total_weight
    data['cargo_total_value']  += ship.cargo_total_value
    for hs in hs_codes:
        cargo = ship.cargo_by_hs.get(hs, {})
        data[f'cargo_hs{hs}_weight'] += cargo.get('weight', 0.0)
        data[f'cargo_hs{hs}_value']  += cargo.get('value',  0.0)


def _make_node_cargo_entry(hs_codes: List[int]) -> Dict:
    entry: Dict = {
        'ship_count':         0,
        'cargo_total_weight': 0.0,
        'cargo_total_value':  0.0,
    }
    for hs in hs_codes:
        entry[f'cargo_hs{hs}_weight'] = 0.0
        entry[f'cargo_hs{hs}_value']  = 0.0
    return entry


def _attribute_cargo_to_node(
    node_cargo: Dict,
    name: str,
    ship: Ship,
    hs_codes: List[int],
) -> None:
    """Add ship's cargo to a port or choke point (called once per ship per node)."""
    data = node_cargo[name]
    data['ship_count']         += 1
    data['cargo_total_weight'] += ship.cargo_total_weight
    data['cargo_total_value']  += ship.cargo_total_value
    for hs in hs_codes:
        cargo = ship.cargo_by_hs.get(hs, {})
        data[f'cargo_hs{hs}_weight'] += cargo.get('weight', 0.0)
        data[f'cargo_hs{hs}_value']  += cargo.get('value',  0.0)


# ---------------------------------------------------------------------------
# Main simulation function
# ---------------------------------------------------------------------------

def run_simulation(
    cfg: Dict,
    G: nx.Graph,
    all_ships: List[Ship],
    common_countries: List[str],
    country_to_ports: Dict[str, List[str]],
    port_name_to_node: Dict[str, Any],
    resume_from_checkpoint: bool = False,
) -> None:
    """
    Run the full simulation and write outputs to cfg['OUTPUT_DIR'].

    Parameters
    ----------
    cfg                   : resolved config dict
    G                     : NetworkX shipping network
    all_ships             : ships sorted by injection_day (from ship_generation)
    common_countries      : countries in both network and trade data
    country_to_ports      : {country: [port_name, ...]}
    port_name_to_node     : {port_name: node_id}
    resume_from_checkpoint: if True, attempt to load latest checkpoint and
                            continue from there
    """
    # ------------------------------------------------------------------
    # Setup
    # ------------------------------------------------------------------
    output_dir    = Path(cfg['OUTPUT_DIR'])
    checkpoint_dir = output_dir / 'checkpoints'
    output_dir.mkdir(parents=True, exist_ok=True)
    checkpoint_dir.mkdir(parents=True, exist_ok=True)

    interval_size       = cfg['INTERVAL_SIZE']          # days
    simulation_days     = cfg['SIMULATION_DAYS']
    n_intervals         = int(simulation_days / interval_size)
    hours_per_interval  = interval_size * 24.0
    ship_speeds         = cfg['SHIP_SPEEDS']
    ckpt_interval       = cfg['CHECKPOINT_INTERVAL_DAYS']
    save_locations      = cfg['SAVE_SHIP_LOCATIONS']
    loc_sample          = cfg['LOCATION_SAMPLE_INTERVAL']
    hs_codes            = cfg['HS_CODES_LIST']
    proactive_rerouting = cfg.get('PROACTIVE_REROUTING', True)

    seed = cfg.get('RANDOM_SEED')
    rng  = np.random.default_rng(seed)

    # ------------------------------------------------------------------
    # Build choke point maps
    # ------------------------------------------------------------------
    choke_name_to_node: Dict[str, Any] = {}
    choke_node_to_name: Dict[Any, str] = {}
    for node in G.nodes():
        attrs = G.nodes[node]
        if attrs.get('source') == 'choke_point':
            name = attrs.get('name') or attrs.get('portName', '')
            if name:
                choke_name_to_node[name] = node
                choke_node_to_name[node] = name

    choke_base_throughputs = cfg.get('CHOKE_POINT_THROUGHPUT', {})

    # ------------------------------------------------------------------
    # Canal transit configuration
    # CANAL_CHOKEPOINTS: {canal_name: transit_time_hours}
    # CANAL_TARGET_RHO:  target utilisation for calibration
    # ------------------------------------------------------------------
    canal_config: Dict[str, float] = cfg.get('CANAL_CHOKEPOINTS', {})
    canal_target_rho: float = cfg.get('CANAL_TARGET_RHO', 0.7)

    # Convert transit times from hours to intervals
    hours_per_interval_val = interval_size * 24.0
    canal_transit_intervals: Dict[str, int] = {
        name: max(1, round(hours / hours_per_interval_val))
        for name, hours in canal_config.items()
    }

    # ------------------------------------------------------------------
    # Pre-assign canal routes based on explicit daily capacity caps.
    # Runs FIRST so that canal slots are filled by ships whose natural
    # routes use them.  Rerouted ships from closures (below) will then
    # be blocked from using already-full canals.
    # ------------------------------------------------------------------
    from .config_loader import get_interruption_events, get_economic_events
    _canal_daily_rates      = cfg.get('CANAL_DAILY_RATES', {})
    _canal_dwr_restrictions = cfg.get('CANAL_DWT_RESTRICTIONS', {})
    if _canal_daily_rates:
        _n_canal_rerouted = preassign_canal_capacities(
            all_ships=all_ships,
            G=G,
            choke_name_to_node=choke_name_to_node,
            canal_daily_rates=_canal_daily_rates,
            canal_dwr_restrictions=_canal_dwr_restrictions,
            target_rho=canal_target_rho,
            simulation_days=simulation_days,
            rng=rng,
        )
        print(f'  Canal capacity caps applied: {_n_canal_rerouted:,} ships rerouted')

    # Collect all canal nodes that are now at capacity so that ships
    # rerouted around closures cannot spill into them.
    _capped_canal_nodes: Set = set()
    for _cname in _canal_daily_rates:
        _cnode = choke_name_to_node.get(_cname)
        if _cnode is not None:
            _capped_canal_nodes.add(_cnode)

    # ------------------------------------------------------------------
    # Pre-assign routes for day-0 chokepoint closures / reductions.
    # Runs AFTER canal caps so that rerouted ships see the same blocked
    # canals as ships that were trimmed above.
    # ------------------------------------------------------------------
    _interruption_events = get_interruption_events(cfg)
    _n_preassigned, _pre_cancelled = preassign_chokepoint_routes(
        all_ships=all_ships,
        G=G,
        events=_interruption_events,
        choke_name_to_node=choke_name_to_node,
        choke_base_throughputs=choke_base_throughputs,
        canal_names=set(canal_config.keys()),
        n_intervals=n_intervals,
        rng=rng,
        blocked_nodes=_capped_canal_nodes,
    )
    if _n_preassigned > 0:
        print(f'  Pre-assigned routes: {_n_preassigned:,} ships rerouted around day-0 closures/reductions')
    if _pre_cancelled:
        print(f'  Pre-cancelled:       {len(_pre_cancelled):,} ships with no route around permanent closure')
        _cancelled_ids = {s.id for s in _pre_cancelled}
        all_ships = [s for s in all_ships if s.id not in _cancelled_ids]

    # ------------------------------------------------------------------
    # Initialise PortManager
    # ------------------------------------------------------------------
    port_names = [
        G.nodes[n].get('portname') or G.nodes[n].get('portName', '')
        for n in G.nodes()
        if G.nodes[n].get('source') == 'port'
           and (G.nodes[n].get('portname') or G.nodes[n].get('portName'))
    ]

    # Compute base berth capacities
    base_capacities = PortManager.compute_base_capacities(
        all_ships,
        port_names,
        interval_size,
        cfg['TARGET_RHO'],
        cfg['MIN_PORT_CAPACITY'],
    )

    port_mgr = PortManager(
        port_names=port_names,
        base_capacities=base_capacities,
        choke_point_names=list(choke_name_to_node.keys()),
        choke_base_throughputs=choke_base_throughputs,
    )

    # Calibrate and initialise canal transit slots
    if canal_config:
        canal_capacities = PortManager.compute_canal_capacities(
            all_ships=all_ships,
            canal_names=list(canal_config.keys()),
            canal_transit_intervals=canal_transit_intervals,
            choke_node_to_name=choke_node_to_name,
            n_intervals=n_intervals,
            target_rho=canal_target_rho,
        )
        port_mgr.setup_canals(canal_capacities)
        print('Canal calibration (target ρ={:.2f}):'.format(canal_target_rho))
        for name, slots in canal_capacities.items():
            t_h = canal_config[name]
            t_int = canal_transit_intervals[name]
            count = sum(
                1 for ship in all_ships
                for node in ship.path
                if choke_node_to_name.get(node) == name
            )
            print(f'  {name}: {count:,} ships, transit {t_h}h ({t_int} intervals), '
                  f'{slots} slot(s)')

    # ------------------------------------------------------------------
    # Initialise event manager
    # ------------------------------------------------------------------
    event_mgr = EventManager()
    event_mgr.schedule_all(
        _interruption_events,
        get_economic_events(cfg),
    )

    # ------------------------------------------------------------------
    # Initialise edge traffic dict (all edges pre-seeded to zero)
    # ------------------------------------------------------------------
    edge_traffic: Dict[Tuple, Dict] = {}
    for u, v in G.edges():
        key = _norm_edge(u, v)
        if key not in edge_traffic:
            edge_traffic[key] = _make_edge_entry(hs_codes)

    # ------------------------------------------------------------------
    # Initialise ship traversal tracking
    # ------------------------------------------------------------------
    ship_edge_history: Dict[int, Set[Tuple]] = {}  # ship_id → set of edge keys traversed
    ship_choke_history: Dict[int, Set[str]] = {}   # ship_id → set of choke names passed through

    # ------------------------------------------------------------------
    # Initialise port and choke cargo tracking
    # ------------------------------------------------------------------
    port_cargo: Dict[str, Dict] = {
        name: _make_node_cargo_entry(hs_codes) for name in port_names
    }
    choke_cargo: Dict[str, Dict] = {
        name: _make_node_cargo_entry(hs_codes) for name in choke_name_to_node
    }

    # ------------------------------------------------------------------
    # Ship injection queue (sorted by injection_day, already sorted)
    # ------------------------------------------------------------------
    ship_queue: Deque[Ship] = deque(all_ships)   # O(1) popleft
    active_ships: Dict[int, Ship] = {}           # keyed by ship.id for O(1) lookup/removal
    lost_ships_records: List[Dict] = []
    port_occupancy_records: List[Dict] = []

    # ------------------------------------------------------------------
    # Output buffers
    # ------------------------------------------------------------------
    loc_buffer = LocationBuffer(
        output_path=str(output_dir / 'ship_locations.parquet'),
        flush_every=50_000,
    )

    # ------------------------------------------------------------------
    # Resume from checkpoint (if requested)
    # ------------------------------------------------------------------
    start_interval = 0
    occ_append = False  # True after first checkpoint flush clears the list
    if resume_from_checkpoint:
        ckpt = load_checkpoint(str(checkpoint_dir))
        if ckpt:
            start_interval       = ckpt['interval']
            active_ships         = {s.id: s for s in ckpt['active_ships']}
            ship_queue           = deque(ckpt['ship_queue'])
            edge_traffic         = ckpt['edge_traffic']
            ship_edge_history    = ckpt['ship_edge_history']
            ship_choke_history   = ckpt.get('ship_choke_history', {})
            lost_ships_records   = ckpt['lost_ships_records']
            # port_occupancy_records already flushed to parquet at checkpoint;
            # start fresh and append going forward to avoid reloading millions of records
            port_occupancy_records = []
            occ_append = True
            port_cargo           = ckpt.get('port_cargo', {name: _make_node_cargo_entry(hs_codes) for name in port_names})
            choke_cargo          = ckpt.get('choke_cargo', {name: _make_node_cargo_entry(hs_codes) for name in choke_name_to_node})
            port_mgr.load_state_dict(ckpt['port_manager'])
            event_mgr.load_state_dict(ckpt['event_manager'])
            rng_state = ckpt.get('rng_state')
            if rng_state is not None:
                rng.bit_generator.state = rng_state
            print(f'Resumed from checkpoint at day {start_interval * interval_size:.1f}')
        else:
            print('No checkpoint found — starting from day 0.')

    # ------------------------------------------------------------------
    # Statistics trackers
    # ------------------------------------------------------------------
    total_loading_intervals   = 0
    total_unloading_intervals = 0
    n_completed               = 0
    cargo_weight_transported  = 0.0
    cargo_value_transported   = 0.0

    # ------------------------------------------------------------------
    # =====================  MAIN LOOP  ================================
    # ------------------------------------------------------------------
    print(f'Running simulation: {simulation_days} days, {n_intervals:,} intervals')
    print(f'  Ships in queue: {len(ship_queue):,}')

    for interval in tqdm(range(start_interval, n_intervals), desc='Simulating'):
        current_day = interval * interval_size

        # --------------------------------------------------------------
        # Fire scheduled events
        # --------------------------------------------------------------
        for tag, event in event_mgr.pop_events(current_day):
            if tag in ('interruption', 'restoration'):
                port_mgr.apply_interruption(event) if tag == 'interruption' \
                    else port_mgr.restore_capacity(event)

                if tag == 'interruption':
                    if event.event_type == 'port' and event.capacity_multiplier <= 0:
                        _handle_dest_port_closure(
                            event.target, active_ships, ship_queue,
                            G, country_to_ports, port_name_to_node,
                            port_mgr, current_day, lost_ships_records,
                        )
                    elif event.event_type == 'choke_point':
                        choke_node = choke_name_to_node.get(event.target)
                        if choke_node is not None and proactive_rerouting:
                            n_rt = _handle_choke_event(
                                event.target, choke_node,
                                event.capacity_multiplier,
                                active_ships, ship_queue, G, port_mgr,
                                current_day,
                            )
                            if n_rt > 0:
                                tqdm.write(
                                    f'  Day {current_day:.1f}: {n_rt:,} ships '
                                    f'rerouted around {event.target}'
                                )
            # EconomicEvents at day>0 don't affect ships already generated;
            # they were handled at ship generation time via epoch_schedule.

        # --------------------------------------------------------------
        # Inject ships whose injection_day has arrived
        # --------------------------------------------------------------
        while ship_queue and ship_queue[0].injection_day <= current_day:
            ship = ship_queue.popleft()
            ship_edge_history[ship.id] = set()
            ship_choke_history[ship.id] = set()
            active_ships[ship.id] = ship

        # --------------------------------------------------------------
        # Phase 1: Port operations
        # --------------------------------------------------------------
        ships_to_remove: List[Ship] = []

        for ship in list(active_ships.values()):
            if ship.state == 'waiting_to_load':
                if port_mgr.can_load(ship.origin_port):
                    port_mgr.start_loading(ship.origin_port, ship.id)
                    ship.state = 'loading'
                    ship.loading_remaining = ship.loading_time
                else:
                    ship.wait_intervals += 1

            elif ship.state == 'loading':
                ship.loading_remaining -= 1
                if ship.loading_remaining <= 0:
                    port_mgr.finish_loading(ship.origin_port, ship.id)
                    ship.state = 'traveling'
                    total_loading_intervals += ship.loading_time
                    if ship.origin_port in port_cargo:
                        _attribute_cargo_to_node(port_cargo, ship.origin_port, ship, hs_codes)

            elif ship.state == 'waiting_to_unload':
                if port_mgr.can_unload(ship.dest_port):
                    port_mgr.start_unloading(ship.dest_port, ship.id)
                    ship.state = 'unloading'
                    ship.unloading_remaining = ship.unloading_time
                else:
                    ship.wait_intervals += 1

            elif ship.state == 'unloading':
                ship.unloading_remaining -= 1
                if ship.unloading_remaining <= 0:
                    port_mgr.finish_unloading(ship.dest_port, ship.id)
                    ship.state = 'completed'
                    ship.completed = True
                    n_completed              += 1
                    cargo_weight_transported += ship.cargo_total_weight
                    cargo_value_transported  += ship.cargo_total_value
                    total_unloading_intervals += ship.unloading_time
                    if ship.dest_port in port_cargo:
                        _attribute_cargo_to_node(port_cargo, ship.dest_port, ship, hs_codes)
                    ships_to_remove.append(ship)

        # --------------------------------------------------------------
        # Phase 2: Move traveling ships + choke/canal logic
        # --------------------------------------------------------------
        for ship in list(active_ships.values()):
            if ship.completed:
                continue

            if ship.state == 'canal_transit':
                # Count down canal transit timer
                ship.canal_remaining -= 1
                if ship.canal_remaining <= 0:
                    canal_name = ship.current_canal
                    port_mgr.finish_canal_transit(canal_name, ship.id)
                    # Attribute cargo to choke node (first passage only)
                    if canal_name not in ship_choke_history.get(ship.id, set()):
                        ship_choke_history.setdefault(ship.id, set()).add(canal_name)
                        if canal_name in choke_cargo:
                            _attribute_cargo_to_node(choke_cargo, canal_name, ship, hs_codes)
                    ship.current_canal = None
                    ship.state = 'traveling'
                continue

            if ship.state not in ('traveling', 'waiting_at_node'):
                continue

            # ---- Choke/canal queue management ----
            if ship.state == 'waiting_at_node':
                _process_waiting_at_node(ship, port_mgr, choke_node_to_name)
                continue

            # ---- Advance traveling ship ----
            _advance_ship(
                ship, G, port_mgr, choke_node_to_name,
                interval_size, ship_speeds,
                edge_traffic, ship_edge_history,
                choke_cargo, ship_choke_history, hs_codes,
            )

        # --------------------------------------------------------------
        # Phase 3: Release ships from choke queues; admit ships to canals
        # --------------------------------------------------------------
        # Regular (non-canal) choke points — throughput-based release
        for choke_name in list(port_mgr._choke_names):
            if port_mgr.is_canal(choke_name):
                continue  # handled below
            throughput = port_mgr.effective_choke_throughput(choke_name)
            if throughput is None or throughput == 0:
                continue  # passthrough or fully closed (handled in waiting_at_node)
            released_ids = port_mgr.release_from_choke(choke_name, throughput)
            for sid in released_ids:
                ship = active_ships.get(sid)
                if ship is not None and ship.state == 'waiting_at_node':
                    ship.state = 'traveling'
                    if choke_name not in ship_choke_history.get(sid, set()):
                        ship_choke_history.setdefault(sid, set()).add(choke_name)
                        if choke_name in choke_cargo:
                            _attribute_cargo_to_node(choke_cargo, choke_name, ship, hs_codes)

        # Canal choke points — admit queued ships to transit when slots open
        for canal_name in list(port_mgr._canal_capacities.keys()):
            while True:
                next_id = port_mgr.try_start_canal_transit(canal_name)
                if next_id is None:
                    break
                ship = active_ships.get(next_id)
                if ship is None:
                    break
                # Ship already arrived at the canal node before queuing
                # (current_edge_idx points to the edge leaving the canal).
                # Just start the transit timer.
                ship.canal_remaining = canal_transit_intervals.get(canal_name, 1)
                ship.current_canal = canal_name
                ship.state = 'canal_transit'

        # --------------------------------------------------------------
        # Remove completed / lost ships
        # --------------------------------------------------------------
        for ship in ships_to_remove:
            active_ships.pop(ship.id, None)

        # --------------------------------------------------------------
        # Record port occupancy
        # --------------------------------------------------------------
        for port_name in port_names:
            occ = port_mgr.occupancy(port_name)
            if occ > 0:
                port_occupancy_records.append({
                    'timestep': interval,
                    'day':      float(current_day),
                    'port_name': port_name,
                    'num_ships': occ,
                    'capacity':  port_mgr.effective_capacity(port_name),
                })

        # --------------------------------------------------------------
        # Record ship locations
        # --------------------------------------------------------------
        if save_locations and (interval % loc_sample == 0):
            for ship in active_ships.values():
                loc = _ship_location_record(ship, G)
                loc_buffer.add(interval, current_day, ship.id, loc)

        # --------------------------------------------------------------
        # Checkpoint
        # --------------------------------------------------------------
        if (ckpt_interval > 0 and
                interval > 0 and
                (interval % int(ckpt_interval / interval_size)) == 0):
            loc_buffer.flush()
            _write_intermediate_parquets(
                output_dir, port_occupancy_records, lost_ships_records,
                edge_traffic, G, port_cargo, choke_cargo, hs_codes,
                append_occ=occ_append,
            )
            port_occupancy_records.clear()
            occ_append = True
            state = {
                'interval':              interval + 1,
                'active_ships':          list(active_ships.values()),
                'ship_queue':            list(ship_queue),
                'edge_traffic':          edge_traffic,
                'ship_edge_history':     ship_edge_history,
                'ship_choke_history':    ship_choke_history,
                'lost_ships_records':    lost_ships_records,
                'port_cargo':            port_cargo,
                'choke_cargo':           choke_cargo,
                'port_manager':          port_mgr.state_dict(),
                'event_manager':         event_mgr.state_dict(),
                'rng_state':             rng.bit_generator.state,
            }
            ckpt_path = save_checkpoint(state, str(checkpoint_dir), current_day)
            tqdm.write(f'  Checkpoint saved: {Path(ckpt_path).name}')

    # ------------------------------------------------------------------
    # Flush remaining location buffer
    # ------------------------------------------------------------------
    loc_buffer.flush()

    # ------------------------------------------------------------------
    # Write final outputs
    # ------------------------------------------------------------------
    print('\nWriting final outputs...')

    # Edge statistics
    edge_df = build_edge_statistics_df(edge_traffic, G, hs_codes)
    write_parquet(edge_df, str(output_dir / 'edge_statistics.parquet'))
    print(f'  edge_statistics.parquet  ({len(edge_df):,} edges)')

    # Port occupancy (append if records were cleared mid-run or on resume)
    occ_df = build_port_occupancy_df(port_occupancy_records)
    write_parquet(occ_df, str(output_dir / 'port_occupancy.parquet'), append=occ_append)
    print(f'  port_occupancy.parquet   ({len(occ_df):,} records)')

    # Port cargo
    port_cargo_df = build_port_cargo_df(port_cargo, hs_codes)
    write_parquet(port_cargo_df, str(output_dir / 'port_cargo.parquet'))
    n_ports_active = (port_cargo_df['ship_count'] > 0).sum()
    print(f'  port_cargo.parquet       ({n_ports_active:,}/{len(port_cargo_df):,} ports with traffic)')

    # Choke cargo
    choke_cargo_df = build_choke_cargo_df(choke_cargo, hs_codes)
    write_parquet(choke_cargo_df, str(output_dir / 'choke_cargo.parquet'))
    print(f'  choke_cargo.parquet      ({len(choke_cargo_df):,} choke points)')

    # Lost ships
    if lost_ships_records:
        lost_df = pd.DataFrame(lost_ships_records)
        write_parquet(lost_df, str(output_dir / 'lost_ships.parquet'))
        print(f'  lost_ships.parquet       ({len(lost_df):,} ships lost)')
    else:
        # Write empty file so downstream code can always load it
        empty_lost = pd.DataFrame(columns=[
            'ship_id', 'day_lost', 'reason', 'origin_country', 'dest_country',
            'origin_port', 'intended_dest_port', 'cargo_total_weight', 'cargo_total_value'
        ])
        write_parquet(empty_lost, str(output_dir / 'lost_ships.parquet'))
        print('  lost_ships.parquet       (0 ships lost)')

    # Update ships.parquet with final reroute stats
    ships_path = output_dir / 'ships.parquet'
    if ships_path.exists():
        ships_df = pd.read_parquet(ships_path)
        # Build a reroute lookup from active+completed ships
        reroute_lookup = {
            s.id: (len(s.reroute_history) > 0, len(s.reroute_history))
            for s in all_ships
        }
        if 'ship_id' in ships_df.columns:
            ships_df['rerouted']      = ships_df['ship_id'].map(lambda i: reroute_lookup.get(i, (False, 0))[0])
            ships_df['reroute_count'] = ships_df['ship_id'].map(lambda i: reroute_lookup.get(i, (False, 0))[1])
        write_parquet(ships_df, str(ships_path))

    # CSV backward-compat exports
    if cfg.get('BACKWARD_COMPAT_CSV', False):
        print('\nExporting CSV compatibility files...')
        export_compat_csvs(str(output_dir))

    # Summary
    print(f'\nCompleted ships: {n_completed:,}')
    print(f'Total weight transported: {cargo_weight_transported:,.0f} mt')
    print(f'Total value transported:  ${cargo_value_transported:,.0f}')
    print_simulation_summary(str(output_dir), hs_codes)


# ---------------------------------------------------------------------------
# Helper: advance a traveling ship one interval
# ---------------------------------------------------------------------------

def _advance_ship(
    ship: Ship,
    G: nx.Graph,
    port_mgr: PortManager,
    choke_node_to_name: Dict[Any, str],
    interval_size: float,
    ship_speeds: Dict[str, float],
    edge_traffic: Dict,
    ship_edge_history: Dict[int, Set],
    choke_cargo: Dict,
    ship_choke_history: Dict[int, Set],
    hs_codes: List[int],
) -> None:
    """Move ship along its path for one interval's worth of distance.

    Choke/canal detection happens AFTER the ship has physically traversed
    the edge and arrived at the node — not the moment the edge begins.
    This avoids the old behaviour where ships would queue at a choke
    immediately upon entering the preceding edge regardless of distance.
    """
    speed_kmh = ship_speeds.get(ship.ship_type, 25)
    km_remaining = speed_kmh * 24.0 * interval_size

    while km_remaining > 0 and ship.state == 'traveling':
        edge_idx = ship.current_edge_idx
        if edge_idx >= len(ship.path) - 1:
            ship.state = 'waiting_to_unload'
            return

        node1 = ship.path[edge_idx]
        node2 = ship.path[edge_idx + 1]

        # Get edge length before deciding whether to complete it
        if G.has_edge(node1, node2):
            edge_len = G[node1][node2].get('length', 0.0)
        elif G.has_edge(node2, node1):
            edge_len = G[node2][node1].get('length', 0.0)
        else:
            edge_len = 0.0

        km_left_in_edge = edge_len - ship.km_into_current_edge
        edge_key = _norm_edge(node1, node2)

        # Attribute cargo to edge on first traversal
        if edge_key not in ship_edge_history[ship.id]:
            ship_edge_history[ship.id].add(edge_key)
            _attribute_cargo_to_edge(edge_traffic, edge_key, ship, hs_codes)

        if km_remaining >= km_left_in_edge:
            # Ship completes this edge and arrives at node2
            hours_on_edge = (km_left_in_edge / speed_kmh) if speed_kmh > 0 else 0.0
            edge_traffic[edge_key]['total_time_hours'] += hours_on_edge
            km_remaining -= km_left_in_edge
            ship.current_edge_idx += 1
            ship.km_into_current_edge = 0.0

            # Ship has physically arrived at node2 — check if it's a choke/canal
            if node2 in choke_node_to_name:
                choke_name = choke_node_to_name[node2]
                if port_mgr.is_canal(choke_name):
                    # Always queue for canal transit (transit time applies)
                    port_mgr.enqueue_canal(choke_name, ship.id)
                    ship.state = 'waiting_at_node'
                    ship.current_canal = choke_name
                    return
                throughput = port_mgr.effective_choke_throughput(choke_name)
                if throughput is not None:
                    # Restricted or fully closed — join queue
                    port_mgr.enqueue_choke(choke_name, ship.id)
                    ship.state = 'waiting_at_node'
                    return
                # Passthrough: attribute cargo and continue
                if choke_name not in ship_choke_history.get(ship.id, set()):
                    ship_choke_history.setdefault(ship.id, set()).add(choke_name)
                    if choke_name in choke_cargo:
                        _attribute_cargo_to_node(choke_cargo, choke_name, ship, hs_codes)

            # Check if ship has reached its destination (last node in path)
            if ship.current_edge_idx >= len(ship.path) - 1:
                ship.state = 'waiting_to_unload'
                return

        else:
            # Partial traversal — ship does not reach node2 this interval
            hours_on_segment = (km_remaining / speed_kmh) if speed_kmh > 0 else 0.0
            edge_traffic[edge_key]['total_time_hours'] += hours_on_segment
            ship.km_into_current_edge += km_remaining
            km_remaining = 0


# ---------------------------------------------------------------------------
# Helper: process a ship that is waiting_at_node (choke point queue)
# ---------------------------------------------------------------------------

def _process_waiting_at_node(
    ship: Ship,
    port_mgr: PortManager,
    choke_node_to_name: Dict[Any, str],
) -> None:
    """
    Increment the wait counter for a ship queued at a choke point or canal.

    Ships remain queued until Phase 3 releases them (throughput > 0 chokes)
    or until the closure ends and capacity is restored (temporary closures).

    For non-canal chokes that revert to passthrough (throughput becomes None),
    the ship is released from the queue immediately.

    Note: ships blocked at a permanently closed choke with no day-0 pre-assignment
    (e.g. mid-simulation permanent closures) will queue indefinitely.
    """
    ship.wait_intervals += 1

    # Canal ships: Phase 3 (try_start_canal_transit) handles admission
    if ship.current_canal and port_mgr.is_canal(ship.current_canal):
        return

    # In the new model the ship is physically AT path[current_edge_idx]
    # (the choke node itself), not approaching it.
    edge_idx = ship.current_edge_idx
    if edge_idx >= len(ship.path) - 1:
        ship.state = 'waiting_to_unload'
        return

    blocked_node = ship.path[edge_idx]
    if blocked_node not in choke_node_to_name:
        ship.state = 'traveling'
        return

    choke_name = choke_node_to_name[blocked_node]
    if port_mgr.effective_choke_throughput(choke_name) is None:
        # Choke restored to passthrough — release immediately
        port_mgr.remove_from_choke_queue(choke_name, ship.id)
        ship.state = 'traveling'


# ---------------------------------------------------------------------------
# Helper: handle destination port closure for active ships
# ---------------------------------------------------------------------------

def _handle_dest_port_closure(
    closed_port: str,
    active_ships: Dict[int, Ship],
    ship_queue: Deque[Ship],
    G: nx.Graph,
    country_to_ports: Dict[str, List[str]],
    port_name_to_node: Dict[str, Any],
    port_mgr: PortManager,
    current_day: float,
    lost_ships_records: List[Dict],
) -> None:
    """
    When a port closes, all ships with that destination are redirected
    to the nearest open port in the same country, or logged as lost.

    Applies to both active_ships (en route) and ship_queue (not yet injected).
    """
    affected = [s for s in active_ships.values() if s.dest_port == closed_port and not s.completed]
    affected += [s for s in ship_queue if s.dest_port == closed_port]

    closed_ports = port_mgr.closed_ports

    for ship in affected:
        # Current position: last traversed node (or origin node if not yet traveling)
        if ship.state in ('waiting_to_load', 'loading') or ship.current_edge_idx == 0:
            from_node = port_name_to_node.get(ship.origin_port)
        else:
            from_node = ship.path[ship.current_edge_idx]

        if from_node is None:
            _log_lost_ship(ship, current_day, 'no_from_node', lost_ships_records)
            ship.completed = True
            ship.state = 'completed'
            continue

        result = find_nearest_open_port_in_country(
            country=ship.dest_country,
            country_to_ports=country_to_ports,
            port_name_to_node=port_name_to_node,
            closed_ports=closed_ports,
            G=G,
            from_node=from_node,
        )

        if result is None:
            # No open port in destination country
            _log_lost_ship(ship, current_day, 'dest_port_closed', lost_ships_records)
            ship.reroute_history.append({
                'day':    current_day,
                'reason': 'dest_port_closed_no_alternative',
                'old_dest_port': closed_port,
                'new_dest_port': None,
            })
            ship.completed = True
            ship.state = 'completed'
            active_ships.pop(ship.id, None)
        else:
            new_port, new_path, new_length = result
            ship.reroute_history.append({
                'day':           current_day,
                'reason':        'dest_port_closed',
                'old_dest_port': closed_port,
                'new_dest_port': new_port,
            })
            ship.dest_port         = new_port
            ship.path              = new_path
            ship.path_length       = new_length
            ship.current_edge_idx  = 0
            ship.km_into_current_edge = 0.0
            if ship.state not in ('waiting_to_load', 'loading'):
                ship.state = 'traveling'


# ---------------------------------------------------------------------------
# Helper: reroute ships when a choke point closes mid-simulation
# ---------------------------------------------------------------------------

def _handle_choke_event(
    choke_name: str,
    choke_node: Any,
    capacity_multiplier: float,
    active_ships: Dict[int, Ship],
    ship_queue: Deque,
    G: nx.Graph,
    port_mgr: PortManager,
    current_day: float,
) -> int:
    """
    When a choke point closes (or partially reduces) mid-simulation, reroute
    all ships whose future path passes through it.

    Only full closures (multiplier == 0) trigger immediate rerouting.
    Partial reductions are handled by the throughput model at runtime.

    Ships currently in canal_transit are allowed to finish their transit;
    new arrivals will encounter the closed choke and queue indefinitely
    unless rerouted here.

    In the updated movement model, a ship waiting_at_node is physically AT
    path[current_edge_idx] (the choke node itself), so we back up one step
    to the preceding node when computing the reroute origin.

    Returns the number of ships whose paths were changed.
    """
    if capacity_multiplier > 0:
        return 0  # Partial reduction — throughput model handles it

    G_modified = G.copy()
    G_modified.remove_node(choke_node)

    _route_cache: Dict[Tuple, Optional[Tuple]] = {}
    n_rerouted = 0

    # Process both in-flight ships and ships not yet injected
    all_candidates = list(active_ships.values()) + list(ship_queue)

    for ship in all_candidates:
        if ship.completed or ship.state == 'canal_transit':
            # Mid-transit ships continue; they will have exited before the
            # closure matters.  Pre-reroute ships not yet in canal.
            continue

        current_idx = ship.current_edge_idx

        # Skip if choke is not in the remaining path
        if choke_node not in ship.path[current_idx:]:
            continue

        # Determine the reroute origin node and path prefix length
        if ship.state == 'waiting_at_node':
            # Ship is AT the choke node (path[current_idx] == choke_node).
            # Back up one step so we can route in G_modified.
            back_idx = max(0, current_idx - 1)
            from_node = ship.path[back_idx]
            prefix_end = back_idx
            if port_mgr.is_canal(choke_name):
                port_mgr.remove_from_canal_queue(choke_name, ship.id)
            else:
                port_mgr.remove_from_choke_queue(choke_name, ship.id)
            ship.current_canal = None
        elif ship.state in ('waiting_to_load', 'loading') or current_idx == 0:
            from_node = ship.path[0]
            prefix_end = 0
        else:
            from_node = ship.path[current_idx]
            prefix_end = current_idx

        dest_node = ship.path[-1]
        cache_key = (from_node, dest_node)
        if cache_key not in _route_cache:
            _route_cache[cache_key] = compute_shortest_path(
                G_modified, from_node, dest_node
            )
        result = _route_cache[cache_key]

        if result is None:
            continue  # No alternative — ship keeps its route (will queue at closure)

        new_path, new_length = result
        old_length = ship.path_length

        ship.path = ship.path[:prefix_end] + new_path
        ship.path_length = new_length
        ship.current_edge_idx = prefix_end
        ship.km_into_current_edge = 0.0

        if ship.state == 'waiting_at_node':
            ship.state = 'traveling'

        ship.reroute_history.append({
            'day':          current_day,
            'reason':       'choke_closure_mid_sim',
            'blocked_node': choke_node,
            'old_path_len': old_length,
            'new_path_len': new_length,
        })
        n_rerouted += 1

    return n_rerouted


def _log_lost_ship(
    ship: Ship,
    day: float,
    reason: str,
    records: List[Dict],
) -> None:
    lost = LostShip(
        ship_id=ship.id,
        day_lost=day,
        reason=reason,
        origin_country=ship.origin_country,
        dest_country=ship.dest_country,
        origin_port=ship.origin_port,
        intended_dest_port=ship.dest_port,
        cargo_total_weight=ship.cargo_total_weight,
        cargo_total_value=ship.cargo_total_value,
        cargo_by_hs=ship.cargo_by_hs,
    )
    records.append(lost.to_record())


# ---------------------------------------------------------------------------
# Helper: ship location record for a single ship
# ---------------------------------------------------------------------------

def _ship_location_record(ship: Ship, G: nx.Graph) -> Dict:
    if ship.state in ('waiting_to_load', 'loading'):
        return {'status': 'loading', 'port': ship.origin_port}
    if ship.state in ('waiting_to_unload', 'unloading'):
        return {'status': 'unloading', 'port': ship.dest_port}
    if ship.state == 'canal_transit':
        # Ship is at the canal node (path[current_edge_idx - 1] is the inbound
        # node; current_edge_idx was already incremented when transit started).
        # Report as stationary at the canal choke point.
        return {'status': 'canal_transit', 'port': ship.current_canal or ''}
    if ship.state in ('waiting_at_node', 'traveling'):
        idx = ship.current_edge_idx
        if idx < len(ship.path) - 1:
            node1, node2 = ship.path[idx], ship.path[idx + 1]
            if G.has_edge(node1, node2):
                edge_len = G[node1][node2].get('length', 0.0)
            elif G.has_edge(node2, node1):
                edge_len = G[node2][node1].get('length', 0.0)
            else:
                edge_len = 0.0
            progress = (ship.km_into_current_edge / edge_len
                        if edge_len > 0 else 0.0)
            status = 'active' if ship.state == 'traveling' else 'waiting_at_node'
            return {
                'status': status,
                'edge': [str(node1), str(node2)],
                'edge_length_km': edge_len,
                'progress_fraction': progress,
            }
    return {'status': ship.state}


# ---------------------------------------------------------------------------
# Helper: write intermediate Parquet files at checkpoint time
# ---------------------------------------------------------------------------

def _write_intermediate_parquets(
    output_dir: Path,
    port_occupancy_records: List[Dict],
    lost_ships_records: List[Dict],
    edge_traffic: Dict,
    G: nx.Graph,
    port_cargo: Dict,
    choke_cargo: Dict,
    hs_codes: List[int],
    append_occ: bool = False,
) -> None:
    """Flush accumulated records to Parquet (used at checkpoint)."""
    if port_occupancy_records:
        occ_df = build_port_occupancy_df(port_occupancy_records)
        write_parquet(occ_df, str(output_dir / 'port_occupancy.parquet'), append=append_occ)

    if lost_ships_records:
        lost_df = pd.DataFrame(lost_ships_records)
        write_parquet(lost_df, str(output_dir / 'lost_ships.parquet'))

    edge_df = build_edge_statistics_df(edge_traffic, G, hs_codes)
    write_parquet(edge_df, str(output_dir / 'edge_statistics.parquet'))

    port_cargo_df = build_port_cargo_df(port_cargo, hs_codes)
    write_parquet(port_cargo_df, str(output_dir / 'port_cargo.parquet'))

    choke_cargo_df = build_choke_cargo_df(choke_cargo, hs_codes)
    write_parquet(choke_cargo_df, str(output_dir / 'choke_cargo.parquet'))
