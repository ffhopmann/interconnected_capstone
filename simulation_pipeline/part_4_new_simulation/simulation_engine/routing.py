"""
routing.py — Route computation, K-shortest paths, and rerouting decisions.

Key responsibilities:
  - Pre-compute shortest routes between all port pairs
  - Derive the best (shortest) port pair for each country pair
  - Find K shortest alternative paths (with optional node exclusions)
  - Evaluate the rerouting decision for a blocked ship
  - Destination port failover (find nearest open port in same country)
"""

from __future__ import annotations
import math
import pickle
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

import networkx as nx
from tqdm import tqdm


# ---------------------------------------------------------------------------
# Graph map builders (used at ship-generation time)
# ---------------------------------------------------------------------------

def build_port_node_map(G: nx.Graph) -> Dict[str, Any]:
    """
    Return a mapping  {port_name: node_id}  for all port nodes in the graph.
    Uses the 'portname' attribute (lower-case, from IMF data).
    """
    mapping = {}
    for node in G.nodes():
        attrs = G.nodes[node]
        if attrs.get('source') == 'port':
            name = attrs.get('portname')
            if name:
                mapping[name] = node
    return mapping


def build_country_port_map(G: nx.Graph) -> Dict[str, List[str]]:
    """
    Return a mapping  {country: [port_name, ...]}  from the graph.
    Uses the 'portname' attribute (lower-case, from IMF data).
    """
    result: Dict[str, List[str]] = {}
    for node in G.nodes():
        attrs = G.nodes[node]
        if attrs.get('source') == 'port':
            country = attrs.get('country')
            port_name = attrs.get('portname')
            if country and port_name:
                result.setdefault(country, []).append(port_name)
    return result


def build_choke_point_node_map(G: nx.Graph) -> Dict[str, Any]:
    """
    Return a mapping  {choke_point_name: node_id}  for all choke point nodes.
    """
    mapping = {}
    for node in G.nodes():
        attrs = G.nodes[node]
        if attrs.get('source') == 'choke_point':
            name = attrs.get('name') or attrs.get('portname')
            if name:
                mapping[name] = node
    return mapping


# ---------------------------------------------------------------------------
# Shortest path primitive
# ---------------------------------------------------------------------------

def compute_shortest_path(
    G: nx.Graph,
    origin_node: Any,
    dest_node: Any,
) -> Optional[Tuple[List[Any], float]]:
    """
    Find the shortest path (by 'length') between two nodes.

    Returns
    -------
    (path, length_km) or None if no path exists.
    """
    try:
        path = nx.shortest_path(G, origin_node, dest_node, weight='length')
        length = sum(
            G[path[i]][path[i + 1]].get('length', 0)
            for i in range(len(path) - 1)
        )
        return path, length
    except (nx.NetworkXNoPath, nx.NodeNotFound):
        return None


# ---------------------------------------------------------------------------
# Port-pair route pre-computation  (used by 00_precompute_routes.ipynb)
# ---------------------------------------------------------------------------

def compute_all_port_pair_routes(
    G: nx.Graph,
    port_name_to_node: Dict[str, Any],
    show_progress: bool = True,
) -> Dict[Tuple[str, str], Dict]:
    """
    Compute the shortest route for every directed port-pair combination.

    Parameters
    ----------
    G                 : NetworkX graph
    port_name_to_node : {port_name: node_id} (from build_port_node_map)
    show_progress     : show tqdm progress bar

    Returns
    -------
    {(portname_A, portname_B): {'path': [node, ...], 'length': float}}
    Only pairs with a valid path are included.
    """
    port_names = list(port_name_to_node.keys())
    pairs = [(p1, p2) for p1 in port_names for p2 in port_names if p1 != p2]

    routes: Dict[Tuple[str, str], Dict] = {}

    iterable = (
        tqdm(pairs, desc='Computing port-pair routes', unit='pair')
        if show_progress else pairs
    )

    for p1, p2 in iterable:
        o_node = port_name_to_node[p1]
        d_node = port_name_to_node[p2]
        result = compute_shortest_path(G, o_node, d_node)
        if result:
            routes[(p1, p2)] = {'path': result[0], 'length': result[1]}

    return routes


def derive_country_pair_optimal(
    port_pair_routes: Dict[Tuple[str, str], Dict],
    country_to_ports: Dict[str, List[str]],
) -> Dict[Tuple[str, str], Dict]:
    """
    For each (origin_country, dest_country) pair, find the port pair with
    the shortest route length.

    Parameters
    ----------
    port_pair_routes : output of compute_all_port_pair_routes
    country_to_ports : {country: [port_name, ...]}

    Returns
    -------
    {(country_A, country_B): {
        'origin_port':    str,
        'dest_port':      str,
        'optimal_length': float (km),
    }}
    Only pairs with at least one reachable port combination are included.
    """
    optimal: Dict[Tuple[str, str], Dict] = {}
    countries = list(country_to_ports.keys())

    for origin_country in countries:
        for dest_country in countries:
            if origin_country == dest_country:
                continue

            origin_ports = country_to_ports.get(origin_country, [])
            dest_ports   = country_to_ports.get(dest_country, [])

            best_length = math.inf
            best_o = None
            best_d = None

            for o_port in origin_ports:
                for d_port in dest_ports:
                    route = port_pair_routes.get((o_port, d_port))
                    if route and route['length'] < best_length:
                        best_length = route['length']
                        best_o = o_port
                        best_d = d_port

            if best_o is not None:
                optimal[(origin_country, dest_country)] = {
                    'origin_port':    best_o,
                    'dest_port':      best_d,
                    'optimal_length': best_length,
                }

    return optimal


def load_or_compute_port_pair_routes(
    G: nx.Graph,
    port_name_to_node: Dict[str, Any],
    cache_path: str,
    show_progress: bool = True,
) -> Dict[Tuple[str, str], Dict]:
    """
    Load pre-computed port-pair routes from a cache file, or compute and cache them.

    Parameters
    ----------
    G                 : NetworkX graph
    port_name_to_node : {port_name: node_id}
    cache_path        : path to the pickle cache file
    show_progress     : show tqdm bar during computation (ignored on cache hit)

    Returns
    -------
    dict keyed by (portname_A, portname_B) — same as compute_all_port_pair_routes.
    """
    cache = Path(cache_path)

    if cache.exists():
        if show_progress:
            print(f"Loading port-pair routes from cache: {cache_path}")
        with open(cache, 'rb') as f:
            return pickle.load(f)

    routes = compute_all_port_pair_routes(G, port_name_to_node, show_progress)

    cache.parent.mkdir(parents=True, exist_ok=True)
    with open(cache, 'wb') as f:
        pickle.dump(routes, f)

    if show_progress:
        print(f"Port-pair routes cached to: {cache_path}")

    return routes


# ---------------------------------------------------------------------------
# K-shortest paths (for rerouting)
# ---------------------------------------------------------------------------

def get_k_shortest_paths(
    G: nx.Graph,
    source: Any,
    target: Any,
    k: int,
    blocked_nodes: Optional[Set[Any]] = None,
    blocked_edges: Optional[Set[Tuple[Any, Any]]] = None,
    weight: str = 'length',
) -> List[Tuple[List[Any], float]]:
    """
    Find up to k shortest simple paths from source to target, optionally
    avoiding specified nodes and edges.

    Uses a modified Yen's algorithm via NetworkX's shortest_simple_paths.

    Parameters
    ----------
    G             : NetworkX graph
    source        : origin node
    target        : destination node
    k             : maximum number of paths to return
    blocked_nodes : set of nodes to exclude (e.g. a closed choke point)
    blocked_edges : set of (u, v) edge tuples to exclude
    weight        : edge attribute used as distance

    Returns
    -------
    List of (path, length_km) tuples, shortest first.
    Empty list if no path exists.
    """
    if blocked_nodes or blocked_edges:
        H = G.copy()
        if blocked_nodes:
            removable = blocked_nodes - {source, target}
            H.remove_nodes_from(removable)
        if blocked_edges:
            for u, v in blocked_edges:
                if H.has_edge(u, v):
                    H.remove_edge(u, v)
                if H.has_edge(v, u):
                    H.remove_edge(v, u)
    else:
        H = G

    results: List[Tuple[List[Any], float]] = []
    try:
        for path in nx.shortest_simple_paths(H, source, target, weight=weight):
            length = sum(
                H[path[i]][path[i + 1]].get(weight, 0)
                for i in range(len(path) - 1)
            )
            results.append((path, length))
            if len(results) >= k:
                break
    except (nx.NetworkXNoPath, nx.NodeNotFound, nx.NetworkXError):
        pass

    return results


# ---------------------------------------------------------------------------
# Path travel time
# ---------------------------------------------------------------------------

def compute_path_travel_time_intervals(
    path: List[Any],
    G: nx.Graph,
    ship_speed_kmh: float,
    interval_size_days: float,
    start_edge_idx: int = 0,
    km_into_start_edge: float = 0.0,
) -> float:
    """
    Compute the number of simulation intervals needed to travel the remaining
    portion of a path, starting at a given position.

    Parameters
    ----------
    path               : full node list
    G                  : network graph
    ship_speed_kmh     : ship's cruising speed in km/h
    interval_size_days : duration of one simulation interval in days
    start_edge_idx     : index of the edge the ship is currently on
    km_into_start_edge : how far (km) along that edge the ship already is

    Returns
    -------
    float — estimated intervals to complete the remaining route
    """
    if ship_speed_kmh <= 0 or len(path) < 2:
        return 0.0

    total_km = 0.0
    for i in range(start_edge_idx, len(path) - 1):
        u, v = path[i], path[i + 1]
        if G.has_edge(u, v):
            edge_len = G[u][v].get('length', 0.0)
        elif G.has_edge(v, u):
            edge_len = G[v][u].get('length', 0.0)
        else:
            edge_len = 0.0

        if i == start_edge_idx:
            edge_len = max(0.0, edge_len - km_into_start_edge)
        total_km += edge_len

    interval_km = ship_speed_kmh * 24.0 * interval_size_days
    return total_km / interval_km if interval_km > 0 else 0.0


# ---------------------------------------------------------------------------
# Rerouting decision
# ---------------------------------------------------------------------------

def evaluate_reroute(
    ship_path: List[Any],
    ship_current_edge_idx: int,
    ship_km_into_edge: float,
    ship_speed_kmh: float,
    interval_size_days: float,
    G: nx.Graph,
    blocked_node: Any,
    queue_position: int,
    effective_throughput: float,
    k_alternatives: int,
    patience_multiplier: float,
    dest_node: Any,
) -> Optional[Tuple[List[Any], float]]:
    """
    Decide whether a ship should reroute around a blocked node.

    Decision rule:
        expected_remaining_current = expected_wait + remaining_travel_current
        if any alternative_travel_time < expected_remaining_current × patience_multiplier:
            reroute to the fastest alternative

    Parameters
    ----------
    ship_path            : current full path
    ship_current_edge_idx: index of edge the ship is on
    ship_km_into_edge    : km traveled into current edge
    ship_speed_kmh       : cruising speed
    interval_size_days   : length of one interval in days
    G                    : network graph
    blocked_node         : the node where the ship is blocked
    queue_position       : ship's position in queue (0 = next to go)
    effective_throughput : ships per interval passing the blockage (0 = fully closed)
    k_alternatives       : number of alternative paths to consider
    patience_multiplier  : reroute if alt < remaining × this factor
    dest_node            : destination port node ID

    Returns
    -------
    (new_path, new_length_km) if rerouting is recommended, else None.
    """
    force_reroute = effective_throughput <= 0

    if not force_reroute:
        expected_wait = queue_position / effective_throughput
        remaining_travel = compute_path_travel_time_intervals(
            ship_path, G, ship_speed_kmh, interval_size_days,
            start_edge_idx=ship_current_edge_idx,
            km_into_start_edge=ship_km_into_edge,
        )
        expected_remaining_current = expected_wait + remaining_travel
    else:
        expected_remaining_current = math.inf

    current_node = ship_path[ship_current_edge_idx]

    alternatives = get_k_shortest_paths(
        G, current_node, dest_node,
        k=k_alternatives,
        blocked_nodes={blocked_node},
    )

    best_alt = None
    best_alt_intervals = math.inf

    for alt_path, alt_length_km in alternatives:
        interval_km = ship_speed_kmh * 24.0 * interval_size_days
        alt_intervals = alt_length_km / interval_km if interval_km > 0 else math.inf
        if alt_intervals < best_alt_intervals:
            best_alt_intervals = alt_intervals
            best_alt = (alt_path, alt_length_km)

    if best_alt is None:
        return None

    if best_alt_intervals < expected_remaining_current * patience_multiplier:
        return best_alt

    return None


# ---------------------------------------------------------------------------
# Destination port failover
# ---------------------------------------------------------------------------

def find_nearest_open_port_in_country(
    country: str,
    country_to_ports: Dict[str, List[str]],
    port_name_to_node: Dict[str, Any],
    closed_ports: Set[str],
    G: nx.Graph,
    from_node: Any,
) -> Optional[Tuple[str, List[Any], float]]:
    """
    Find the nearest open port in the given country, routing from `from_node`.

    Parameters
    ----------
    country           : destination country
    country_to_ports  : {country: [port_name, ...]}
    port_name_to_node : {port_name: node_id}
    closed_ports      : set of currently closed port names
    G                 : network graph
    from_node         : current ship position (last traversed node)

    Returns
    -------
    (port_name, path, length_km) for the nearest reachable open port,
    or None if no open port exists or none is reachable.
    """
    open_ports = [
        p for p in country_to_ports.get(country, [])
        if p not in closed_ports
    ]
    if not open_ports:
        return None

    best_port = None
    best_path = None
    best_length = math.inf

    for port_name in open_ports:
        dest_node = port_name_to_node.get(port_name)
        if dest_node is None:
            continue
        result = compute_shortest_path(G, from_node, dest_node)
        if result and result[1] < best_length:
            best_path, best_length = result
            best_port = port_name

    if best_port is None:
        return None
    return best_port, best_path, best_length


# ---------------------------------------------------------------------------
# Pre-simulation route assignment for day-0 chokepoint events
# ---------------------------------------------------------------------------

# Fallback throughput (ships/interval) when a passthrough choke has a
# capacity_multiplier < 1 but no explicit base throughput configured.
# Must match the value in port_manager.py.
_FALLBACK_CHOKE_THROUGHPUT = 5


def preassign_chokepoint_routes(
    all_ships: List,
    G: nx.Graph,
    events: List,
    choke_name_to_node: Dict[str, Any],
    choke_base_throughputs: Dict[str, Optional[int]],
    canal_names: Set[str],
    n_intervals: int,
    rng: Any,
) -> Tuple[int, List]:
    """
    Pre-assign alternative routes for ships affected by day-0 chokepoint events.

    Called once before the simulation loop. Modifies ship.path and
    ship.path_length in-place for every ship whose pre-computed route passes
    through an affected chokepoint node.

    Behaviour by event type
    -----------------------
    capacity_multiplier == 0 (full closure from day 0):
        Every ship whose path contains the chokepoint node is rerouted using
        a copy of the graph with that node removed.  If event.cancel_if_no_alternative
        is True, ships for which no alternative path exists are returned in the
        cancelled list rather than keeping their original route.

    0 < capacity_multiplier < 1 (partial reduction):
        For canal chokes: randomly selects floor(len(affected) * mult) ships
        to keep the direct route; the rest are rerouted around the canal.
        For non-canal chokes: computes annual throughput = effective_tp * n_intervals
        and randomly selects that many ships to keep the direct route.

    Parameters
    ----------
    all_ships             : full list of Ship objects (mutated in-place)
    G                     : NetworkX shipping network
    events                : list of InterruptionEvent objects
    choke_name_to_node    : {choke_name: node_id}
    choke_base_throughputs: {choke_name: ships_per_interval or None}
    canal_names           : set of choke names that use the canal transit model
    n_intervals           : total simulation intervals (for annual capacity calc)
    rng                   : numpy.random.Generator (for reproducible sampling)

    Returns
    -------
    (n_rerouted, pre_cancelled)
        n_rerouted    : number of ships whose paths were changed
        pre_cancelled : ships with no alternative route when cancel_if_no_alternative=True
    """
    n_rerouted = 0
    pre_cancelled: List = []

    for event in events:
        if event.day != 0 or event.event_type != 'choke_point':
            continue

        choke_name = event.target
        choke_node = choke_name_to_node.get(choke_name)
        if choke_node is None:
            continue

        mult = event.capacity_multiplier
        affected = [s for s in all_ships if choke_node in s.path]
        if not affected:
            continue

        if mult <= 0:
            # Full closure: reroute every ship that passes through this node
            ships_to_reroute = affected

        elif choke_name in canal_names:
            # Partial canal reduction: randomly assign ships to go around.
            # floor(len(affected) * mult) ships keep the direct route; rest reroute.
            n_can_pass = min(len(affected), math.floor(len(affected) * mult))
            if n_can_pass >= len(affected):
                continue  # Multiplier rounds up to full capacity
            keep_indices = set(
                rng.choice(len(affected), n_can_pass, replace=False).tolist()
            )
            ships_to_reroute = [
                s for i, s in enumerate(affected) if i not in keep_indices
            ]

        else:
            # Partial non-canal reduction: proportional pre-assignment
            base_tp = choke_base_throughputs.get(choke_name)
            if base_tp is None:
                effective_tp = max(0, math.floor(_FALLBACK_CHOKE_THROUGHPUT * mult))
            else:
                effective_tp = max(0, math.floor(base_tp * mult))

            if effective_tp == 0:
                ships_to_reroute = affected
            else:
                annual_capacity = effective_tp * n_intervals
                n_can_pass = min(len(affected), int(annual_capacity))
                if n_can_pass >= len(affected):
                    continue  # Enough capacity for all ships
                keep_indices = set(
                    rng.choice(len(affected), n_can_pass, replace=False).tolist()
                )
                ships_to_reroute = [
                    s for i, s in enumerate(affected) if i not in keep_indices
                ]

        if not ships_to_reroute:
            continue

        # Build a modified graph with the chokepoint node removed
        G_modified = G.copy()
        G_modified.remove_node(choke_node)

        for ship in ships_to_reroute:
            origin_node = ship.path[0]
            dest_node   = ship.path[-1]
            old_length  = ship.path_length

            result = compute_shortest_path(G_modified, origin_node, dest_node)
            if result is None:
                # No alternative route exists
                if event.cancel_if_no_alternative:
                    pre_cancelled.append(ship)
                # else: ship keeps its original path (will be blocked at runtime)
                continue

            new_path, new_length = result
            ship.path        = new_path
            ship.path_length = new_length
            ship.reroute_history.append({
                'day':          0.0,
                'reason':       'preassigned_chokepoint_closure',
                'blocked_node': choke_node,
                'old_path_len': old_length,
                'new_path_len': new_length,
            })
            n_rerouted += 1

    return n_rerouted, pre_cancelled
