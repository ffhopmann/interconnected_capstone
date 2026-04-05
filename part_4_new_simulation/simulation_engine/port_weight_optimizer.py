"""
port_weight_optimizer.py — Grid search to calibrate port selection weights.

Finds optimal values of three parameters:
  alpha  : exponent on port size score    (0=ignore, 1=linear, >1=emphasise large ports)
  beta   : exponent on vessel-type score  (0=ignore, 1=linear, >1=emphasise type match)
  lam    : distance penalty scale in exp(-lam * max(0, ratio - 1))

Scoring formula (replaces hard-coded multiplication in ship_generation.py):
  export_score(p) = size_export(p)^alpha * type(p,t)^beta
  import_score(p) = size_import(p)^alpha * type(p,t)^beta * exp(-lam * excess(p))

Objective  (minimise):
  log(JS_export) + log(JS_import) + log(JS_type)

where each JS term is the mean Jensen-Shannon divergence across countries between
the simulated port distribution and the IMF empirical distribution.  Using the sum
of logs (= log of product = geometric mean criterion) prevents a near-perfect score
on two dimensions from masking a poor score on the third.

No ships are generated.  All distributions are computed analytically.
"""

from __future__ import annotations

import math
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
from tqdm import tqdm


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_SHIP_TYPES: List[str] = ['tanker', 'bulk carrier', 'cargo ship']

_VESSEL_COLS: Dict[str, List[str]] = {
    'tanker':       ['vessel_count_tanker'],
    'bulk carrier': ['vessel_count_dry_bulk'],
    'cargo ship':   ['vessel_count_container', 'vessel_count_general_cargo', 'vessel_count_RoRo'],
}

_EPS = 1e-10


# ---------------------------------------------------------------------------
# Step 1 — Build raw port data (run once, independent of parameters)
# ---------------------------------------------------------------------------

def build_raw_port_data(
    country_to_ports: Dict[str, List[str]],
    imf_port_df: pd.DataFrame,
) -> Dict[str, Dict]:
    """
    Extract normalised size and type arrays per country from IMF data.

    Does NOT apply alpha / beta — those are applied at evaluation time so
    the same raw data can be reused for every grid point.

    Returns
    -------
    {country: {
        'ports'       : [portname, ...],
        'export_size' : np.ndarray  — normalised export-share vector
        'import_size' : np.ndarray  — normalised import-share vector
        'type_scores' : {ship_type: np.ndarray}  — normalised vessel-type vectors
        'emp_type_mix': np.ndarray  shape (n_ports, 3)
                        empirical vessel-type distribution per port (rows sum to 1)
    }}
    """
    imf_indexed = imf_port_df.set_index('portname')
    port_data: Dict[str, Dict] = {}

    for country, ports in country_to_ports.items():
        known_ports = [p for p in ports if p in imf_indexed.index]
        if not known_ports:
            continue

        df = imf_indexed.loc[known_ports]
        df = df[~df.index.duplicated(keep='first')]
        known_ports = list(df.index)
        n = len(known_ports)

        # ── Size scores ──────────────────────────────────────────────────────
        exp_shares = df['share_country_maritime_export'].fillna(0).values.astype(float)
        imp_shares = df['share_country_maritime_import'].fillna(0).values.astype(float)
        export_size = exp_shares / exp_shares.sum() if exp_shares.sum() > 0 else np.ones(n) / n
        import_size = imp_shares / imp_shares.sum() if imp_shares.sum() > 0 else np.ones(n) / n

        # ── Type scores ───────────────────────────────────────────────────────
        type_scores: Dict[str, np.ndarray] = {}
        for ship_type, cols in _VESSEL_COLS.items():
            vc = np.zeros(n, dtype=float)
            for col in cols:
                if col in df.columns:
                    vc += df[col].fillna(0).values.astype(float)
            type_scores[ship_type] = vc / vc.sum() if vc.sum() > 0 else np.zeros(n)

        # ── Empirical type mix per port  (n × 3, rows normalised) ────────────
        emp_type_mix = np.zeros((n, 3), dtype=float)
        for j, ship_type in enumerate(_SHIP_TYPES):
            for col in _VESSEL_COLS[ship_type]:
                if col in df.columns:
                    emp_type_mix[:, j] += df[col].fillna(0).values.astype(float)
        row_sums = emp_type_mix.sum(axis=1, keepdims=True)
        valid = (row_sums[:, 0] > 0)
        emp_type_mix[valid]  = emp_type_mix[valid] / row_sums[valid]
        emp_type_mix[~valid] = 1.0 / 3.0   # uniform fallback for ports with no data

        # ── Vessel-count-based marginal port share  (independent of import_size) ─
        # Sums all vessel types per port then normalises across ports in this
        # country.  This is a genuinely different data source from import_size
        # (trade/BACI shares), making JS_size_vs_vc a non-circular comparison.
        total_vc = np.zeros(n, dtype=float)
        for cols in _VESSEL_COLS.values():
            for col in cols:
                if col in df.columns:
                    total_vc += df[col].fillna(0).values.astype(float)
        emp_vc_share = (
            total_vc / total_vc.sum() if total_vc.sum() > 0 else import_size.copy()
        )

        port_data[country] = {
            'ports':        known_ports,
            'export_size':  export_size,
            'import_size':  import_size,
            'type_scores':  type_scores,
            'emp_type_mix': emp_type_mix,
            'emp_vc_share': emp_vc_share,
        }

    return port_data


# ---------------------------------------------------------------------------
# Step 2 — Precompute average excess ratios (run once, independent of lam)
# ---------------------------------------------------------------------------

def precompute_avg_excess_ratios(
    raw_port_data: Dict[str, Dict],
    port_pair_routes: Dict[Tuple[str, str], Dict],
    country_pair_optimal: Dict[Tuple[str, str], Dict],
    country_to_ports: Dict[str, List[str]],
) -> Dict[str, np.ndarray]:
    """
    For each destination country D and each destination port p_d, compute the
    average excess ratio over all origin ports that have a route to p_d:

        avg_excess(p_d) = mean_{p_o} max(0,  route(p_o, p_d) / optimal(O, D)  − 1)

    Precomputing this once means the grid search only multiplies by lam:

        dist_factor(p_d) = exp(−lam × avg_excess(p_d))

    Returns
    -------
    {country: np.ndarray of shape (n_ports,)}
    """
    # Build port → country mapping
    port_to_country: Dict[str, str] = {
        p: c for c, ports in country_to_ports.items() for p in ports
    }

    # Build reverse lookup: dest_port → list of (orig_port, route_length)
    routes_to_dest: Dict[str, List[Tuple[str, float]]] = {}
    for (orig, dest), route in port_pair_routes.items():
        routes_to_dest.setdefault(dest, []).append((orig, route['length']))

    avg_excess: Dict[str, np.ndarray] = {}

    for dest_country, pdata in raw_port_data.items():
        dest_ports = pdata['ports']
        n = len(dest_ports)
        excess_sums   = np.zeros(n, dtype=float)
        excess_counts = np.zeros(n, dtype=float)

        for i, dest_port in enumerate(dest_ports):
            for orig_port, route_length in routes_to_dest.get(dest_port, []):
                orig_country = port_to_country.get(orig_port)
                if orig_country is None or orig_country == dest_country:
                    continue
                opt = country_pair_optimal.get((orig_country, dest_country))
                if opt is None or opt.get('optimal_length', 0) <= 0:
                    continue
                ratio = route_length / opt['optimal_length']
                excess_sums[i]   += max(0.0, ratio - 1.0)
                excess_counts[i] += 1

        result = np.zeros(n, dtype=float)
        valid = excess_counts > 0
        result[valid] = excess_sums[valid] / excess_counts[valid]
        avg_excess[dest_country] = result

    return avg_excess


# ---------------------------------------------------------------------------
# Step 3 — Compute distributions for a given (alpha, beta, lam)
# ---------------------------------------------------------------------------

def compute_distributions(
    raw_port_data: Dict[str, Dict],
    avg_excess_ratios: Dict[str, np.ndarray],
    alpha: float,
    beta: float,
    lam: float,
) -> Dict[str, Dict]:
    """
    Compute normalised simulated port distributions for all countries and ship
    types under the given parameters.

    Returns
    -------
    {country: {
        'export_dists': {ship_type: np.ndarray},
        'import_dists': {ship_type: np.ndarray},
    }}
    """
    distributions: Dict[str, Dict] = {}

    for country, pdata in raw_port_data.items():
        n           = len(pdata['ports'])
        export_size = pdata['export_size']
        import_size = pdata['import_size']
        type_scores = pdata['type_scores']

        # Distance factors using precomputed average excess ratios
        excess       = avg_excess_ratios.get(country, np.zeros(n))
        dist_factors = np.exp(-lam * excess)

        # Size arrays raised to alpha  (alpha=0 → uniform)
        exp_size_a = np.power(export_size, alpha)
        imp_size_a = np.power(import_size, alpha)

        export_dists: Dict[str, np.ndarray] = {}
        import_dists: Dict[str, np.ndarray] = {}

        for ship_type in _SHIP_TYPES:
            ts_raw = type_scores[ship_type]

            # Type vector raised to beta; uniform fallback when no vessel data
            if ts_raw.sum() > 0:
                ts = np.power(ts_raw, beta)
            else:
                ts = np.ones(n)   # ignore-type fallback

            raw_exp = exp_size_a * ts
            export_dists[ship_type] = (
                raw_exp / raw_exp.sum() if raw_exp.sum() > 0 else np.ones(n) / n
            )

            raw_imp = imp_size_a * ts * dist_factors
            if raw_imp.sum() > 0:
                import_dists[ship_type] = raw_imp / raw_imp.sum()
            elif dist_factors.sum() > 0:
                import_dists[ship_type] = dist_factors / dist_factors.sum()
            else:
                import_dists[ship_type] = np.ones(n) / n

        distributions[country] = {
            'export_dists': export_dists,
            'import_dists': import_dists,
        }

    return distributions


# ---------------------------------------------------------------------------
# Step 4 — Jensen-Shannon divergence and objective
# ---------------------------------------------------------------------------

def js_divergence(p: np.ndarray, q: np.ndarray, eps: float = _EPS) -> float:
    """
    Jensen-Shannon divergence between two discrete distributions.
    Both arrays are clipped and renormalised internally, so callers do not
    need to ensure they sum to exactly 1.
    """
    p = np.clip(p, eps, None);  p = p / p.sum()
    q = np.clip(q, eps, None);  q = q / q.sum()
    m = 0.5 * (p + q)
    return float(
        0.5 * np.sum(p * np.log(p / m)) + 0.5 * np.sum(q * np.log(q / m))
    )


def compute_js_type(
    raw_port_data: Dict[str, Dict],
    beta: float,
    eps: float = _EPS,
) -> float:
    """
    Mean JS divergence between simulated and empirical vessel-type composition.

    For each destination port i the simulated type fraction is:
        sim_type[i][t]  ∝  type_scores[t][i] ^ beta

    Crucially, alpha and lambda CANCEL in this per-port ratio — they affect
    the absolute traffic level at each port but not the mix of vessel types.
    This means beta is the only parameter that can be calibrated from IMF
    vessel-count data without creating a circular reference.

    Parameters
    ----------
    raw_port_data : dict   from build_raw_port_data()
    beta          : float  exponent on vessel-type score
    eps           : float  smoothing floor

    Returns
    -------
    float  (lower = better match to empirical type mix)
    """
    js_type_list: List[float] = []

    for country, pdata in raw_port_data.items():
        n = len(pdata['ports'])
        if n < 2:
            continue

        emp_type_mx = pdata['emp_type_mix']   # (n, 3)
        type_scores = pdata['type_scores']
        import_size = pdata['import_size']

        # Pure type fraction: proportional to type_scores[t][i]^beta.
        # When beta=0 this is uniform (1/3 each); beta=1 is linear in vessel counts.
        sim_type_stack = np.zeros((n, 3), dtype=float)
        for j, ship_type in enumerate(_SHIP_TYPES):
            ts = type_scores[ship_type]
            if ts.sum() > 0:
                sim_type_stack[:, j] = np.power(np.maximum(ts, eps), beta)
            else:
                sim_type_stack[:, j] = 1.0   # uniform fallback — no vessel data

        row_sums = sim_type_stack.sum(axis=1, keepdims=True)
        sim_type_norm = np.where(
            row_sums > eps,
            sim_type_stack / row_sums,
            1.0 / 3.0,
        )

        port_js = [
            js_divergence(sim_type_norm[i], emp_type_mx[i], eps)
            for i in range(n)
        ]
        # Weight by empirical import share so larger ports matter more
        js_type_list.append(float(np.average(port_js, weights=import_size)))

    return float(np.mean(js_type_list)) if js_type_list else 1.0


# ---------------------------------------------------------------------------
# Step 5 — 1-D optimisation over beta
# ---------------------------------------------------------------------------

def optimize_beta(
    raw_port_data: Dict[str, Dict],
    beta_bounds: Tuple[float, float] = (0.0, 6.0),
    tol: float = 1e-4,
) -> Dict:
    """
    Find the optimal beta (vessel-type exponent) via 1-D scalar minimisation.

    Uses scipy.optimize.minimize_scalar with Brent's bounded method — far more
    efficient than an exhaustive grid search for a 1-D smooth function.

    Why only beta?
    - alpha=1 is the theoretically motivated default (linear size → ships).
      Empirical port shares ARE the input, so comparing to themselves is circular;
      any value other than 1 adds bias without a principled target.
    - lambda (DISTANCE_PENALTY_SCALE) is a policy choice: the IMF import shares
      already embed real-world distance effects, so calibrating lambda against
      those shares is also circular.
    - beta changes the vessel-TYPE composition at each port — a non-circular
      comparison because vessel counts ≠ simulated type fractions.

    Parameters
    ----------
    raw_port_data : dict            from build_raw_port_data()
    beta_bounds   : (lo, hi)        search interval
    tol           : float           convergence tolerance on beta

    Returns
    -------
    dict with keys: best_beta, best_objective, success, n_evals
    """
    from scipy.optimize import minimize_scalar

    def _obj(beta: float) -> float:
        return compute_js_type(raw_port_data, beta)

    result = minimize_scalar(
        _obj,
        bounds=beta_bounds,
        method='bounded',
        options={'xatol': tol},
    )

    return {
        'best_beta':      float(result.x),
        'best_objective': float(result.fun),
        'success':        bool(result.success),
        'n_evals':        int(result.nfev),
    }


def beta_sweep(
    raw_port_data: Dict[str, Dict],
    beta_vals: np.ndarray,
    show_progress: bool = True,
) -> np.ndarray:
    """
    Evaluate compute_js_type at every value in beta_vals.

    Useful for plotting the JS-type landscape before or after optimisation.

    Returns
    -------
    np.ndarray  shape (len(beta_vals),)  — JS_type at each beta
    """
    iterator = tqdm(beta_vals, desc='Beta sweep') if show_progress else beta_vals
    return np.array([compute_js_type(raw_port_data, float(b)) for b in iterator])


# ---------------------------------------------------------------------------
# Step 6 — JS_size_vs_vc: calibrate alpha and lambda jointly
# ---------------------------------------------------------------------------

def compute_js_size_vs_vc(
    raw_port_data: Dict[str, Dict],
    avg_excess_ratios: Dict[str, np.ndarray],
    alpha: float,
    lam: float,
    eps: float = _EPS,
) -> float:
    """
    Mean JS divergence between the simulated marginal port distribution and
    the vessel-count-based empirical port share.

    Simulated marginal: P(port i) ∝ import_size[i]^alpha × exp(−lam × avg_excess[i])
    Empirical target  : emp_vc_share[i] = total_vessel_calls[i] / Σ_j calls[j]

    Non-circular: import_size comes from BACI/IMF trade shares; emp_vc_share
    comes from AIS/port-statistics vessel counts.  These are genuinely
    independent measurements of port activity.

    Parameters
    ----------
    raw_port_data     : from build_raw_port_data()
    avg_excess_ratios : from precompute_avg_excess_ratios()
    alpha             : exponent on port size score
    lam               : distance penalty scale
    """
    js_list: List[float] = []

    for country, pdata in raw_port_data.items():
        n = len(pdata['ports'])
        if n < 2:
            continue

        import_size  = pdata['import_size']
        emp_vc_share = pdata['emp_vc_share']

        excess       = avg_excess_ratios.get(country, np.zeros(n))
        dist_factors = np.exp(-lam * excess)

        sim_marg = np.power(np.maximum(import_size, eps), alpha) * dist_factors
        if sim_marg.sum() > 0:
            sim_marg = sim_marg / sim_marg.sum()
        else:
            sim_marg = np.ones(n) / n

        js_list.append(js_divergence(sim_marg, emp_vc_share, eps))

    return float(np.mean(js_list)) if js_list else 1.0


# ---------------------------------------------------------------------------
# Step 7 — Joint 3-D objective and optimiser
# ---------------------------------------------------------------------------

def compute_objective_3d(
    raw_port_data: Dict[str, Dict],
    avg_excess_ratios: Dict[str, np.ndarray],
    alpha: float,
    beta: float,
    lam: float,
) -> float:
    """
    Joint objective: log(JS_size_vs_vc) + log(JS_type)

    The two terms are separable:
      JS_size_vs_vc(alpha, lam) — calibrates port size + distance
      JS_type(beta)             — calibrates vessel-type composition

    Sum of logs (geometric mean criterion) prevents a good score on one
    dimension compensating for a poor score on the other.
    """
    js_size = compute_js_size_vs_vc(raw_port_data, avg_excess_ratios, alpha, lam)
    js_type = compute_js_type(raw_port_data, beta)
    return math.log(max(js_size, _EPS)) + math.log(max(js_type, _EPS))


def optimize_all(
    raw_port_data: Dict[str, Dict],
    avg_excess_ratios: Dict[str, np.ndarray],
    alpha_bounds: Tuple[float, float] = (0.0, 3.0),
    beta_bounds:  Tuple[float, float] = (0.0, 6.0),
    lam_bounds:   Tuple[float, float] = (0.0, 6.0),
    tol: float = 1e-4,
) -> Dict:
    """
    Jointly optimise (alpha, beta, lambda) exploiting the separable structure.

    Because JS_size_vs_vc only depends on (alpha, lam) and JS_type only depends
    on beta, the 3-D problem decomposes into two independent sub-problems:

      1. beta  : 1-D Brent scalar minimisation of JS_type
      2. (alpha, lam) : 2-D L-BFGS-B minimisation of JS_size_vs_vc,
                        restarted from multiple starting points for robustness

    Returns
    -------
    dict with keys: best_alpha, best_beta, best_lam, best_objective,
                    best_js_size, best_js_type, n_evals_beta, n_evals_size
    """
    from scipy.optimize import minimize_scalar, minimize

    # ── 1. Optimise beta (1-D) ────────────────────────────────────────────
    beta_result = minimize_scalar(
        lambda b: compute_js_type(raw_port_data, b),
        bounds=beta_bounds,
        method='bounded',
        options={'xatol': tol},
    )

    # ── 2. Optimise (alpha, lam) (2-D, multiple starts) ──────────────────
    def _obj_size(x: np.ndarray) -> float:
        a, l = float(x[0]), float(x[1])
        return compute_js_size_vs_vc(raw_port_data, avg_excess_ratios, a, l)

    starts = [(1.0, 1.0), (1.0, 3.0), (0.5, 1.0), (1.5, 2.0), (0.8, 0.5)]
    best_size_res = None
    best_size_val = float('inf')

    for a0, l0 in starts:
        res = minimize(
            _obj_size,
            x0=np.array([a0, l0]),
            method='L-BFGS-B',
            bounds=[alpha_bounds, lam_bounds],
            options={'ftol': tol, 'gtol': tol},
        )
        if res.fun < best_size_val:
            best_size_val = float(res.fun)
            best_size_res = res

    best_alpha = float(np.clip(best_size_res.x[0], *alpha_bounds))
    best_lam   = float(np.clip(best_size_res.x[1], *lam_bounds))
    best_beta  = float(beta_result.x)
    best_obj   = (
        math.log(max(best_size_val, _EPS))
        + math.log(max(float(beta_result.fun), _EPS))
    )

    return {
        'best_alpha':     best_alpha,
        'best_beta':      best_beta,
        'best_lam':       best_lam,
        'best_objective': best_obj,
        'best_js_size':   best_size_val,
        'best_js_type':   float(beta_result.fun),
        'n_evals_beta':   int(beta_result.nfev),
        'n_evals_size':   int(best_size_res.nfev),
    }
