"""
ship_generation.py — Epoch-aware synthetic ship generation.

Generates Ship objects from bilateral trade weight matrices.
Supports:
  - Multi-year runs (ships generated proportionally per epoch)
  - Baseline economic adjustments (day-0 EconomicEvents)
  - Mid-simulation economic events (generate ships per epoch with adjusted matrices)
  - IMF-data-driven port selection:
      origin port sampled by export share × vessel-type match
      dest port sampled by import share × vessel-type match × distance ratio penalty

The overall logic mirrors the original ship_generation.ipynb but is
refactored into functions callable from a notebook or script.
"""

from __future__ import annotations
import json
import math
import warnings
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union

import numpy as np
import pandas as pd
from scipy import stats
from scipy.optimize import fsolve
from tqdm import tqdm

from .models import EconomicEvent, Ship
from .routing import (
    build_choke_point_node_map,
    build_country_port_map,
    build_port_node_map,
)


# ---------------------------------------------------------------------------
# Trade matrix loading and adjustment
# ---------------------------------------------------------------------------

def load_trade_matrices(
    hs_codes: List[int],
    trade_matrices_dir: str,
    hs_codes_info: Dict[int, Dict],
) -> Dict[int, pd.DataFrame]:
    """
    Load weight-based trade matrices for each HS code.

    Returns
    -------
    {hs_code: DataFrame(exporter × importer, values in metric tons)}
    """
    matrices: Dict[int, pd.DataFrame] = {}
    for hs_code in hs_codes:
        path = Path(trade_matrices_dir) / f'weight_trade_matrix_all_transport_modes_HS{hs_code}.csv'
        if not path.exists():
            raise FileNotFoundError(f"Trade matrix not found: {path}")
        df = pd.read_csv(path, index_col=0)
        for label in ['World']:
            if label in df.index:
                df.drop(label, axis=0, inplace=True)
            if label in df.columns:
                df.drop(label, axis=1, inplace=True)
        matrices[hs_code] = df
    return matrices


def apply_economic_adjustments(
    trade_matrices: Dict[int, pd.DataFrame],
    adjustments: List[EconomicEvent],
    common_countries: List[str],
) -> Dict[int, pd.DataFrame]:
    """
    Apply a list of EconomicEvent adjustments to the trade matrices.

    Adjustments are applied multiplicatively.  Multiple adjustments for the
    same country/HS code compound (e.g. -10% then -10% = -19% overall).
    """
    if not adjustments:
        return trade_matrices

    adjusted = {hs: df.copy() for hs, df in trade_matrices.items()}

    for event in adjustments:
        country = event.country
        if country not in common_countries:
            continue

        factor = max(0.0, 1.0 + event.adjustment_pct / 100.0)
        hs_list = event.hs_codes if event.hs_codes else list(adjusted.keys())
        counterpart = event.counterpart_country

        for hs_code in hs_list:
            if hs_code not in adjusted:
                continue
            df = adjusted[hs_code]

            if counterpart:
                # Bilateral: adjust the single (country, counterpart) cell only
                if event.direction in ('export', 'both'):
                    if country in df.index and counterpart in df.columns:
                        df.loc[country, counterpart] *= factor
                if event.direction in ('import', 'both'):
                    if counterpart in df.index and country in df.columns:
                        df.loc[counterpart, country] *= factor
            else:
                # Unilateral: adjust whole row or column (original behaviour)
                if event.direction in ('export', 'both') and country in df.index:
                    df.loc[country, :] *= factor
                if event.direction in ('import', 'both') and country in df.columns:
                    df.loc[:, country] *= factor

    return adjusted


# ---------------------------------------------------------------------------
# Ship size (DWT) distributions
# ---------------------------------------------------------------------------

def build_ship_distributions(
    fleet_df: pd.DataFrame,
    capacity_quantile: float,
) -> Dict[str, Dict]:
    """
    Fit Gamma distributions for each ship type using mean DWT and max DWT
    from real merchant fleet data.
    """
    type_map = {
        'tanker':       'Oil Tanker',
        'bulk carrier': 'Bulk Carrier',
        'cargo ship':   'Container Ship',
    }

    distributions: Dict[str, Dict] = {}
    for our_type, fleet_type in type_map.items():
        row = fleet_df[fleet_df['Ship Type'] == fleet_type]
        if row.empty:
            raise ValueError(f"Ship type '{fleet_type}' not found in merchant fleet data.")
        row = row.iloc[0]
        mean_dwt = float(row['Avg. dwt per ship']) * 1000
        max_dwt  = float(row['Max dwt per ship'])  * 1000

        shape, scale = _fit_gamma(mean_dwt, max_dwt, capacity_quantile)
        distributions[our_type] = {
            'shape': shape,
            'scale': scale,
            'mean':  mean_dwt,
            'max':   max_dwt,
            'std':   math.sqrt(shape) * scale,
        }

    return distributions


def _fit_gamma(mean: float, max_val: float, quantile: float) -> Tuple[float, float]:
    """Fit Gamma(α, θ) such that mean = α·θ and P(X ≤ max) ≈ quantile."""

    def equations(params):
        alpha, theta = params
        return [
            alpha * theta - mean,
            stats.gamma.cdf(max_val, alpha, scale=theta) - quantile,
        ]

    try:
        with warnings.catch_warnings():
            warnings.simplefilter('ignore')
            solution = fsolve(equations, [10, mean / 10], full_output=True)
            alpha, theta = solution[0]
            if alpha > 0 and theta > 0:
                return float(alpha), float(theta)
    except Exception:
        pass

    cv = 0.3
    theta = (mean * cv) ** 2 / mean
    alpha = mean / theta
    return float(alpha), float(theta)


# ---------------------------------------------------------------------------
# Country-pair weight proportions (precomputed for efficiency)
# ---------------------------------------------------------------------------

def precompute_pair_proportions(
    weight_matrices: Dict[int, pd.DataFrame],
    hs_codes_info: Dict[int, Dict],
    ship_distributions: Dict[str, Dict],
    common_countries: List[str],
) -> Dict[Tuple[str, str], Dict]:
    """
    For each (origin, dest) country pair, precompute:
      - ship_type_probs:  {ship_type: probability of sampling this type}
      - hs_proportions:   {ship_type: {'hs_codes': [...], 'proportions': [...]}}
    """
    proportions: Dict[Tuple[str, str], Dict] = {}

    for origin in common_countries:
        for dest in common_countries:
            if origin == dest:
                continue

            weight_by_type: Dict[str, float] = {st: 0.0 for st in ['tanker', 'bulk carrier', 'cargo ship']}
            for hs_code, info in hs_codes_info.items():
                st = info['ship_type']
                w = weight_matrices[hs_code].loc[origin, dest] if (
                    origin in weight_matrices[hs_code].index and
                    dest in weight_matrices[hs_code].columns
                ) else 0.0
                weight_by_type[st] += w

            total_pair_weight = sum(weight_by_type.values())

            if total_pair_weight > 0:
                type_probs = {}
                for st, w in weight_by_type.items():
                    type_probs[st] = w / ship_distributions[st]['mean']
                total_prob = sum(type_probs.values())
                if total_prob > 0:
                    type_probs = {k: v / total_prob for k, v in type_probs.items()}
                else:
                    type_probs = {st: 1 / 3 for st in type_probs}
            else:
                type_probs = {st: 1 / 3 for st in weight_by_type}

            hs_props: Dict[str, Dict] = {}
            for ship_type in ['tanker', 'bulk carrier', 'cargo ship']:
                hs_for_type = [hs for hs, info in hs_codes_info.items() if info['ship_type'] == ship_type]
                hs_weights = []
                for hs in hs_for_type:
                    w = weight_matrices[hs].loc[origin, dest] if (
                        origin in weight_matrices[hs].index and
                        dest in weight_matrices[hs].columns
                    ) else 0.0
                    hs_weights.append(w)

                total_type_w = sum(hs_weights)
                if total_type_w > 0:
                    hs_probs = [w / total_type_w for w in hs_weights]
                else:
                    n = len(hs_for_type)
                    hs_probs = [1 / n if n > 0 else 0.0 for _ in hs_for_type]

                hs_props[ship_type] = {
                    'hs_codes':    hs_for_type,
                    'proportions': hs_probs,
                }

            proportions[(origin, dest)] = {
                'ship_type_probs': type_probs,
                'hs_proportions':  hs_props,
            }

    return proportions


# ---------------------------------------------------------------------------
# IMF port selection data (precomputed from IMF port dataset)
# ---------------------------------------------------------------------------

# IMF vessel count columns aggregated per ship type
_VESSEL_COLS: Dict[str, List[str]] = {
    'tanker':       ['vessel_count_tanker'],
    'bulk carrier': ['vessel_count_dry_bulk'],
    'cargo ship':   ['vessel_count_container', 'vessel_count_general_cargo', 'vessel_count_RoRo'],
}


def build_port_selection_data(
    country_to_ports: Dict[str, List[str]],
    imf_port_df: pd.DataFrame,
    alpha: float = 1.0,
    beta: float = 1.0,
) -> Dict[str, Dict]:
    """
    Precompute port selection weights from IMF data using a multiplicative scoring rule.

    For each country in the network:
      - export_scores[ship_type] : normalized probability array for origin port selection
      - import_scores[ship_type] : normalized probability array for dest port selection
                                   (before distance factor is applied)
      - ports                    : list of port names in the same order as the arrays

    Scores are the element-wise product of the size score and the vessel-type score:
      export_score(p) = export_share(p) x vessel_type_share(p, t)
      import_score(p) = import_share(p) x vessel_type_share(p, t)

    Fallback to uniform over all country ports when the product is zero for every
    port (i.e. no vessel counts of that type are recorded in any port of the country).

    Parameters
    ----------
    country_to_ports : {country: [portname, ...]} from build_country_port_map
    imf_port_df      : DataFrame with columns portname, share_country_maritime_export,
                       share_country_maritime_import, vessel_count_* — and a 'baci_name'
                       column whose values match the country keys in country_to_ports.

    Returns
    -------
    {country_baci_name: {
        'ports':         [portname, ...],
        'export_scores': {ship_type: np.ndarray},
        'import_scores': {ship_type: np.ndarray},
    }}
    """
    # Index IMF data by portname for fast lookup
    imf_indexed = imf_port_df.set_index('portname')

    port_data: Dict[str, Dict] = {}

    for country, ports in country_to_ports.items():
        # Only keep ports present in IMF data
        known_ports = [p for p in ports if p in imf_indexed.index]
        if not known_ports:
            continue

        df = imf_indexed.loc[known_ports]
        # Deduplicate: some portnames exist in multiple countries; keep first match per portname
        df = df[~df.index.duplicated(keep='first')]
        known_ports = list(df.index)
        n = len(known_ports)

        # --- Size scores ---
        export_shares = df['share_country_maritime_export'].fillna(0).values.astype(float)
        import_shares = df['share_country_maritime_import'].fillna(0).values.astype(float)

        export_size = export_shares / export_shares.sum() if export_shares.sum() > 0 else np.ones(n) / n
        import_size = import_shares / import_shares.sum() if import_shares.sum() > 0 else np.ones(n) / n

        # --- Type scores (per ship type) ---
        # Zero vector when no vessel counts recorded — causes product to be 0,
        # triggering uniform fallback below.
        type_scores: Dict[str, np.ndarray] = {}
        for ship_type, cols in _VESSEL_COLS.items():
            vc = np.zeros(n, dtype=float)
            for col in cols:
                if col in df.columns:
                    vc += df[col].fillna(0).values.astype(float)
            type_scores[ship_type] = vc / vc.sum() if vc.sum() > 0 else np.zeros(n)

        # --- Combined scores (multiplicative) ---
        # Fallback to uniform when all products are zero (no vessel type data).
        export_scores: Dict[str, np.ndarray] = {}
        import_scores: Dict[str, np.ndarray] = {}

        for ship_type in ['tanker', 'bulk carrier', 'cargo ship']:
            ts_raw = type_scores[ship_type]
            ts = np.power(ts_raw, beta) if ts_raw.sum() > 0 else np.ones(n)

            raw_exp = np.power(export_size, alpha) * ts
            export_scores[ship_type] = raw_exp / raw_exp.sum() if raw_exp.sum() > 0 else np.ones(n) / n

            raw_imp = np.power(import_size, alpha) * ts
            import_scores[ship_type] = raw_imp / raw_imp.sum() if raw_imp.sum() > 0 else np.ones(n) / n

        port_data[country] = {
            'ports':         known_ports,
            'export_scores': export_scores,
            'import_scores': import_scores,
        }

    return port_data


def _sample_origin_port(
    country: str,
    ship_type: str,
    port_selection_data: Dict[str, Dict],
    rng: np.random.Generator,
) -> Optional[str]:
    """
    Sample an origin port for a ship leaving the given country.

    Returns port name or None if no port data available.
    """
    pdata = port_selection_data.get(country)
    if pdata is None or not pdata['ports']:
        return None

    scores = pdata['export_scores'][ship_type]
    idx = rng.choice(len(pdata['ports']), p=scores)
    return pdata['ports'][idx]


def _sample_dest_port(
    country: str,
    ship_type: str,
    origin_port: str,
    country_optimal_length: float,
    port_selection_data: Dict[str, Dict],
    port_pair_routes: Dict[Tuple[str, str], Dict],
    distance_penalty_scale: float,
    rng: np.random.Generator,
) -> Optional[str]:
    """
    Sample a destination port weighted by import share, vessel type, and distance ratio.

    Distance factor:  exp(−λ × max(0, ratio − 1))
      where  ratio = port_pair_route_length / country_optimal_length
      and    λ     = distance_penalty_scale

    Ports with no reachable route from origin_port are excluded (weight = 0).
    Falls back to uniform over reachable ports if combined scores are all zero.

    Returns port name or None if no reachable destination port exists.
    """
    pdata = port_selection_data.get(country)
    if pdata is None or not pdata['ports']:
        return None

    base_scores = pdata['import_scores'][ship_type]
    ports = pdata['ports']
    n = len(ports)

    # Distance factors using actual routing distances
    dist_factors = np.zeros(n, dtype=float)
    for i, dest_port in enumerate(ports):
        route = port_pair_routes.get((origin_port, dest_port))
        if route is None:
            dist_factors[i] = 0.0  # no route → excluded
        elif country_optimal_length <= 0:
            dist_factors[i] = 1.0
        else:
            ratio = route['length'] / country_optimal_length
            dist_factors[i] = math.exp(-distance_penalty_scale * max(0.0, ratio - 1.0))

    scores = base_scores * dist_factors
    if scores.sum() == 0:
        # Fallback: uniform over reachable ports only
        scores = dist_factors.copy()
    if scores.sum() == 0:
        return None  # no reachable destination port

    scores = scores / scores.sum()
    idx = rng.choice(n, p=scores)
    return ports[idx]


# ---------------------------------------------------------------------------
# Core ship generation (single epoch)
# ---------------------------------------------------------------------------

def generate_ships_for_epoch(
    start_day: float,
    end_day: float,
    weight_matrices: Dict[int, pd.DataFrame],
    hs_codes_info: Dict[int, Dict],
    conversion_factors: Dict[int, float],
    ship_distributions: Dict[str, Dict],
    port_pair_routes: Dict[Tuple[str, str], Dict],
    country_pair_optimal: Dict[Tuple[str, str], Dict],
    port_selection_data: Dict[str, Dict],
    distance_penalty_scale: float,
    common_countries: List[str],
    pair_proportions: Dict,
    dirichlet_concentration: float,
    rng: np.random.Generator,
    show_progress: bool = True,
    ship_id_start: int = 0,
) -> List[Ship]:
    """
    Generate Ship objects for one simulation epoch.

    For each ship:
      1. Sample (origin_country, dest_country) by trade weight.
      2. Sample origin_port using combined export-share + vessel-type weights.
      3. Sample dest_port using combined import-share + vessel-type weights,
         discounted by the ratio of proposed/optimal route length.
      4. Look up the pre-computed route for the sampled port pair.
         Falls back to the country-pair optimal ports if the sampled pair
         has no route.

    N_ships for the epoch is proportional to the fraction of the year covered.

    Returns
    -------
    List of Ship objects (unsorted by injection_day).
    """
    epoch_fraction = (end_day - start_day) / 365.0

    total_weight = sum(df.sum().sum() for df in weight_matrices.values())
    weight_by_type: Dict[str, float] = {st: 0.0 for st in ['tanker', 'bulk carrier', 'cargo ship']}
    for hs_code, info in hs_codes_info.items():
        weight_by_type[info['ship_type']] += weight_matrices[hs_code].sum().sum()

    weighted_avg_cap = sum(
        weight_by_type[st] / total_weight * ship_distributions[st]['mean']
        for st in weight_by_type
        if total_weight > 0
    )
    n_ships = max(1, math.ceil(total_weight / weighted_avg_cap * epoch_fraction)) if weighted_avg_cap > 0 else 0

    n_countries = len(common_countries)
    total_weight_matrix = sum(weight_matrices.values())
    flat_probs = total_weight_matrix.values.flatten()
    flat_probs = np.maximum(flat_probs, 0)
    prob_sum = flat_probs.sum()
    if prob_sum == 0:
        return []
    flat_probs = flat_probs / prob_sum

    pair_indices = rng.choice(n_countries * n_countries, size=n_ships, p=flat_probs)

    ships: List[Ship] = []
    _ship_id = ship_id_start  # global sequential counter for this epoch

    desc = f"Generating ships (day {start_day:.0f}–{end_day:.0f})"
    iterable = tqdm(enumerate(pair_indices), total=n_ships, desc=desc) if show_progress else enumerate(pair_indices)

    for local_idx, pair_idx in iterable:
        origin_idx = pair_idx // n_countries
        dest_idx   = pair_idx  % n_countries

        origin_country = common_countries[origin_idx]
        dest_country   = common_countries[dest_idx]

        if origin_country == dest_country:
            continue

        pair_data = pair_proportions.get((origin_country, dest_country))
        if pair_data is None:
            continue

        optimal_info = country_pair_optimal.get((origin_country, dest_country))
        if optimal_info is None:
            continue

        # --- Sample ship type ---
        st_list  = list(pair_data['ship_type_probs'].keys())
        st_probs = np.array([pair_data['ship_type_probs'][st] for st in st_list])
        st_probs = np.maximum(st_probs, 0)
        if st_probs.sum() == 0:
            continue
        st_probs /= st_probs.sum()
        ship_type = rng.choice(st_list, p=st_probs)

        # --- Sample origin port ---
        origin_port = _sample_origin_port(origin_country, ship_type, port_selection_data, rng)
        if origin_port is None:
            origin_port = optimal_info['origin_port']  # fallback

        # --- Sample destination port with distance factor ---
        optimal_length = optimal_info['optimal_length']
        dest_port = _sample_dest_port(
            dest_country, ship_type, origin_port, optimal_length,
            port_selection_data, port_pair_routes, distance_penalty_scale, rng,
        )
        if dest_port is None:
            dest_port = optimal_info['dest_port']  # fallback

        # --- Look up route for the sampled port pair ---
        route_info = port_pair_routes.get((origin_port, dest_port))
        if route_info is None:
            # Sampled pair unreachable — use country-pair optimal
            fallback_o = optimal_info['origin_port']
            fallback_d = optimal_info['dest_port']
            route_info = port_pair_routes.get((fallback_o, fallback_d))
            if route_info is None:
                continue  # no route at all for this pair
            origin_port = fallback_o
            dest_port   = fallback_d

        # --- Sample DWT from Gamma ---
        dist = ship_distributions[ship_type]
        capacity = float(np.clip(
            rng.gamma(shape=dist['shape'], scale=dist['scale']),
            0, dist['max']
        ))

        # --- Sample cargo ---
        hs_data = pair_data['hs_proportions'][ship_type]
        hs_codes_for_type = hs_data['hs_codes']
        hs_probs = np.array(hs_data['proportions'], dtype=float)

        cargo_by_hs: Dict[int, Dict[str, float]] = {hs: {'weight': 0.0, 'value': 0.0}
                                                      for hs in hs_codes_info}

        if len(hs_codes_for_type) == 0:
            continue

        if ship_type in ('tanker', 'bulk carrier'):
            hs_probs = np.maximum(hs_probs, 0)
            if hs_probs.sum() == 0:
                hs_probs = np.ones_like(hs_probs)
            hs_probs /= hs_probs.sum()
            selected_hs = rng.choice(hs_codes_for_type, p=hs_probs)
            cargo_by_hs[selected_hs]['weight'] = capacity
            cf = conversion_factors.get(selected_hs, 0.0)
            cargo_by_hs[selected_hs]['value'] = capacity * cf if cf > 0 else 0.0
        else:
            alpha = np.maximum(hs_probs * dirichlet_concentration, 1e-10)
            cargo_fractions = rng.dirichlet(alpha)
            for i, hs_code in enumerate(hs_codes_for_type):
                w = capacity * cargo_fractions[i]
                cf = conversion_factors.get(hs_code, 0.0)
                cargo_by_hs[hs_code]['weight'] = w
                cargo_by_hs[hs_code]['value']  = w * cf if cf > 0 else 0.0

        total_value = sum(v['value'] for v in cargo_by_hs.values())

        injection_day = float(rng.uniform(start_day, end_day))

        ship = Ship(
            id=_ship_id,
            origin_country=origin_country,
            dest_country=dest_country,
            origin_port=origin_port,
            dest_port=dest_port,
            ship_type=ship_type,
            injection_day=injection_day,
            path=route_info['path'],
            path_length=route_info['length'],
            cargo_total_weight=capacity,
            cargo_total_value=total_value,
            cargo_by_hs=cargo_by_hs,
            loading_time=1,
            unloading_time=1,
            loading_remaining=1,
        )
        ships.append(ship)
        _ship_id += 1

    return ships


# ---------------------------------------------------------------------------
# Port processing time calibration
# ---------------------------------------------------------------------------

def calibrate_port_times(
    ships: List[Ship],
    port_loading_times: Dict[str, float],
    port_unloading_times: Dict[str, float],
    interval_size_days: float,
    rng: np.random.Generator,
) -> None:
    """
    Recompute loading_time and unloading_time for each ship using the
    log-ratio scaling approach from the original notebook.

    Modifies ships in-place.
    """
    if not ships:
        return

    avg_load = np.mean([s.cargo_total_weight for s in ships])

    scaling: Dict[str, Dict[str, float]] = {}
    for ship_type in ['tanker', 'bulk carrier', 'cargo ship']:
        type_ships = [s for s in ships if s.ship_type == ship_type]
        if not type_ships:
            scaling[ship_type] = {
                'loading':   port_loading_times[ship_type] / interval_size_days,
                'unloading': port_unloading_times[ship_type] / interval_size_days,
            }
            continue

        log_ratios = [math.log(1 + s.cargo_total_weight / avg_load) for s in type_ships]
        mean_lr = np.mean(log_ratios)

        target_l = port_loading_times[ship_type] / interval_size_days
        target_u = port_unloading_times[ship_type] / interval_size_days

        scaling[ship_type] = {
            'loading':   target_l / mean_lr if mean_lr > 0 else target_l,
            'unloading': target_u / mean_lr if mean_lr > 0 else target_u,
        }

    for ship in ships:
        lr = math.log(1 + ship.cargo_total_weight / avg_load)
        sf = scaling[ship.ship_type]

        loading_mean   = max(0.5, lr * sf['loading'])
        unloading_mean = max(0.5, lr * sf['unloading'])

        ship.loading_time   = max(1, int(rng.poisson(loading_mean)))
        ship.unloading_time = max(1, int(rng.poisson(unloading_mean)))
        ship.loading_remaining = ship.loading_time


# ---------------------------------------------------------------------------
# Top-level entry point
# ---------------------------------------------------------------------------

def generate_all_ships(
    cfg: Dict,
    G: Any,
    port_pair_routes: Dict[Tuple[str, str], Dict],
    country_pair_optimal: Dict[Tuple[str, str], Dict],
    imf_port_df: pd.DataFrame,
    economic_events_baseline: List[EconomicEvent],
    epoch_schedule: List[Dict],
    rng: np.random.Generator,
    show_progress: bool = True,
) -> Tuple[List[Ship], List[str], Dict[str, List[str]], Dict[str, Any]]:
    """
    Generate ships for all epochs and return them sorted by injection_day.

    Parameters
    ----------
    cfg                    : config dict from load_config()
    G                      : NetworkX graph (with IMF port node attributes)
    port_pair_routes       : {(portname_A, portname_B): {'path', 'length'}}
                             Pre-computed by 00_precompute_routes.ipynb
    country_pair_optimal   : {(country_A, country_B): {'origin_port', 'dest_port', 'optimal_length'}}
                             Pre-computed by 00_precompute_routes.ipynb
    imf_port_df            : IMF port DataFrame with columns:
                               portname, share_country_maritime_export,
                               share_country_maritime_import, vessel_count_*, baci_name
    economic_events_baseline : day-0 EconomicEvents (baseline adjustments)
    epoch_schedule           : from event_manager.build_epoch_schedule()
    rng                      : numpy random Generator
    show_progress            : show tqdm progress bars

    Returns
    -------
    (all_ships, common_countries, country_to_ports, port_name_to_node)
    """
    # ------ Load static data ------
    with open(cfg['HS_CODES_MAPPING_FILE'], 'r') as f:
        hs_mapping_full = json.load(f)

    hs_codes = cfg['HS_CODES_LIST']
    hs_codes_info: Dict[int, Dict] = {}
    for hs in hs_codes:
        hs_str = str(hs).zfill(2)
        if hs_str in hs_mapping_full:
            hs_codes_info[hs] = hs_mapping_full[hs_str]

    with open(cfg['CONVERSION_FACTORS_FILE'], 'r') as f:
        conv_raw = json.load(f)
    conversion_factors = {int(k): float(v['conversion_factor']) for k, v in conv_raw.items()}

    fleet_df = pd.read_csv(cfg['MERCHANT_FLEET_FILE'])
    ship_distributions = build_ship_distributions(fleet_df, cfg['CAPACITY_QUANTILE'])

    # ------ Network maps ------
    port_name_to_node = build_port_node_map(G)
    country_to_ports  = build_country_port_map(G)
    network_countries = set(country_to_ports.keys())

    # ------ Load and filter base trade matrices ------
    base_matrices = load_trade_matrices(hs_codes, cfg['TRADE_MATRICES_DIR'], hs_codes_info)
    first_df = next(iter(base_matrices.values()))
    trade_countries = set(first_df.index) & set(first_df.columns)
    common_countries = sorted(network_countries & trade_countries)

    if not common_countries:
        raise ValueError("No countries in common between the network and trade matrices.")

    base_matrices = {
        hs: df.loc[common_countries, common_countries].copy()
        for hs, df in base_matrices.items()
    }

    # Weight matrices are already in metric tons (no conversion needed).
    # Conversion factors are still used below to compute cargo_by_hs['value'] per ship.
    hs_codes_info = {hs: info for hs, info in hs_codes_info.items() if hs in base_matrices}

    if not base_matrices:
        raise ValueError("No weight trade matrices found. Check TRADE_MATRICES_DIR for weight_trade_matrix_all_transport_modes_HS*.csv files.")

    # ------ Apply baseline economic adjustments ------
    baseline_matrices = apply_economic_adjustments(
        base_matrices, economic_events_baseline, common_countries
    )

    # ------ Precompute IMF port selection weights ------
    distance_penalty_scale = cfg.get('DISTANCE_PENALTY_SCALE', 3.0)

    if show_progress:
        print("Building port selection weights from IMF data...")
    alpha = cfg.get('PORT_WEIGHT_ALPHA', 1.0)
    beta  = cfg.get('PORT_WEIGHT_BETA',  1.0)
    port_selection_data = build_port_selection_data(
        country_to_ports, imf_port_df, alpha=alpha, beta=beta,
    )
    if show_progress:
        countries_with_data = len(port_selection_data)
        print(f"  Port selection data built for {countries_with_data} countries.")

    # ------ Generate ships per epoch ------
    all_ships: List[Ship] = []
    ship_id_counter = 0  # monotonically increasing across all epochs

    epoch_iter = tqdm(epoch_schedule, desc='Epochs', unit='epoch') if show_progress else epoch_schedule
    for epoch in epoch_iter:
        start_day = epoch['start_day']
        end_day   = epoch['end_day']
        cumulative_adjustments = epoch['cumulative_adjustments']

        epoch_matrices = apply_economic_adjustments(
            baseline_matrices, cumulative_adjustments, common_countries
        )

        pair_props = precompute_pair_proportions(
            epoch_matrices, hs_codes_info, ship_distributions, common_countries
        )

        epoch_ships = generate_ships_for_epoch(
            start_day=start_day,
            end_day=end_day,
            weight_matrices=epoch_matrices,
            hs_codes_info=hs_codes_info,
            conversion_factors=conversion_factors,
            ship_distributions=ship_distributions,
            port_pair_routes=port_pair_routes,
            country_pair_optimal=country_pair_optimal,
            port_selection_data=port_selection_data,
            distance_penalty_scale=distance_penalty_scale,
            common_countries=common_countries,
            pair_proportions=pair_props,
            dirichlet_concentration=cfg['DIRICHLET_CONCENTRATION'],
            rng=rng,
            show_progress=show_progress,
            ship_id_start=ship_id_counter,
        )
        ship_id_counter += len(epoch_ships)
        all_ships.extend(epoch_ships)

    # ------ Calibrate port times using the full fleet ------
    calibrate_port_times(
        all_ships,
        port_loading_times=cfg['PORT_LOADING_TIMES'],
        port_unloading_times=cfg['PORT_UNLOADING_TIMES'],
        interval_size_days=cfg['INTERVAL_SIZE'],
        rng=rng,
    )

    all_ships.sort(key=lambda s: s.injection_day)

    return all_ships, common_countries, country_to_ports, port_name_to_node
