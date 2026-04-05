"""
config_loader.py — Load and validate simulation_config.json.

The config JSON is written by simulation_config.ipynb.  All other notebooks
and modules import their parameters through this loader rather than reading
the JSON directly, so there is a single place to add validation or defaults.
"""

from __future__ import annotations
import json
import os
from pathlib import Path
from typing import Any, Dict, List, Optional

from .models import InterruptionEvent, EconomicEvent


# Default path (relative to part_4_new_simulation/)
_DEFAULT_CONFIG_PATH = Path(__file__).parent.parent / 'simulation_config.json'


def load_config(path: Optional[str] = None) -> Dict[str, Any]:
    """
    Load simulation_config.json and return the raw dict.

    Parameters
    ----------
    path : str or None
        Explicit path to the JSON file.  If None, looks for
        simulation_config.json in the part_4_new_simulation/ directory.

    Returns
    -------
    dict
        Full configuration dictionary.
    """
    config_path = Path(path) if path else _DEFAULT_CONFIG_PATH

    if not config_path.exists():
        raise FileNotFoundError(
            f"simulation_config.json not found at {config_path}.\n"
            "Run simulation_config.ipynb first to generate it."
        )

    with open(config_path, 'r') as f:
        cfg = json.load(f)

    _validate(cfg)
    return cfg


def _validate(cfg: Dict[str, Any]) -> None:
    """Raise ValueError if required keys are missing or values are invalid."""
    required = [
        'SIMULATION_DAYS', 'INTERVAL_SIZE', 'RANDOM_SEED',
        'NETWORK_FILE', 'DATA_DIR', 'TRADE_MATRICES_DIR',
        'HS_CODES_MAPPING_FILE', 'CONVERSION_FACTORS_FILE',
        'MERCHANT_FLEET_FILE', 'IMF_PORT_DATA_FILE', 'BACI_CODES_FILE',
        'HS_CODES_LIST',
        'SHIP_SPEEDS', 'PORT_LOADING_TIMES', 'PORT_UNLOADING_TIMES',
        'CAPACITY_QUANTILE', 'DIRICHLET_CONCENTRATION',
        'DISTANCE_PENALTY_SCALE',
        'TARGET_RHO', 'MIN_PORT_CAPACITY',
        'CHOKE_POINT_THROUGHPUT',
        'CANAL_CHOKEPOINTS', 'CANAL_TARGET_RHO',
        'K_ALTERNATIVE_ROUTES', 'REROUTE_PATIENCE_MULTIPLIER',
        'INTERRUPTION_EVENTS', 'ECONOMIC_EVENTS',
        'OUTPUT_DIR', 'CHECKPOINT_INTERVAL_DAYS',
        'SAVE_SHIP_LOCATIONS', 'LOCATION_SAMPLE_INTERVAL',
        'BACKWARD_COMPAT_CSV',
    ]
    missing = [k for k in required if k not in cfg]
    if missing:
        raise ValueError(
            f"simulation_config.json is missing required keys: {missing}\n"
            "Re-run simulation_config.ipynb to regenerate the config."
        )

    if cfg['SIMULATION_DAYS'] <= 0:
        raise ValueError("SIMULATION_DAYS must be positive.")
    if not (0 < cfg['INTERVAL_SIZE'] <= 1):
        raise ValueError("INTERVAL_SIZE must be in (0, 1] days.")
    if cfg['TARGET_RHO'] <= 0 or cfg['TARGET_RHO'] >= 1:
        raise ValueError("TARGET_RHO must be in (0, 1).")
    if cfg['CANAL_TARGET_RHO'] <= 0 or cfg['CANAL_TARGET_RHO'] >= 1:
        raise ValueError("CANAL_TARGET_RHO must be in (0, 1).")
    if not isinstance(cfg['CANAL_CHOKEPOINTS'], dict):
        raise ValueError("CANAL_CHOKEPOINTS must be a dict {canal_name: transit_time_hours}.")
    if cfg['K_ALTERNATIVE_ROUTES'] < 1:
        raise ValueError("K_ALTERNATIVE_ROUTES must be >= 1.")

    # PROACTIVE_REROUTING is optional (default True for backward compat)
    cfg.setdefault('PROACTIVE_REROUTING', True)
    # Port weight exponents — optional, default to current behaviour (linear)
    cfg.setdefault('PORT_WEIGHT_ALPHA', 1.0)
    cfg.setdefault('PORT_WEIGHT_BETA',  1.0)
    # Canal capacity caps (optional — empty dict = no pre-assignment)
    cfg.setdefault('CANAL_DAILY_RATES', {})
    cfg.setdefault('CANAL_DWT_RESTRICTIONS', {})


def get_interruption_events(cfg: Dict[str, Any]) -> List[InterruptionEvent]:
    """Parse INTERRUPTION_EVENTS list into InterruptionEvent dataclasses."""
    events = []
    for e in cfg.get('INTERRUPTION_EVENTS', []):
        events.append(InterruptionEvent(
            day=float(e['day']),
            end_day=float(e['end_day']) if e.get('end_day') is not None else None,
            event_type=e['type'],
            target=e['target'],
            capacity_multiplier=float(e['capacity_multiplier']),
            cancel_if_no_alternative=bool(e.get('cancel_if_no_alternative', False)),
        ))
    return events


def get_economic_events(cfg: Dict[str, Any]) -> List[EconomicEvent]:
    """Parse ECONOMIC_EVENTS list into EconomicEvent dataclasses."""
    events = []
    for e in cfg.get('ECONOMIC_EVENTS', []):
        events.append(EconomicEvent(
            day=float(e['day']),
            country=e['country'],
            direction=e['direction'],
            hs_codes=[int(h) for h in e.get('hs_codes', [])],
            adjustment_pct=float(e['adjustment_pct']),
            counterpart_country=e.get('counterpart_country'),
        ))
    return events


def resolve_paths(cfg: Dict[str, Any], base_dir: Optional[str] = None) -> Dict[str, Any]:
    """
    Resolve relative paths in the config against base_dir.

    Parameters
    ----------
    cfg : dict
        Config loaded by load_config().
    base_dir : str or None
        Directory to resolve paths against.  Defaults to the
        part_4_new_simulation/ directory.

    Returns
    -------
    dict
        Copy of cfg with all *_FILE and *_DIR values resolved to
        absolute paths.
    """
    if base_dir is None:
        base_dir = str(Path(__file__).parent.parent)

    path_keys = [
        'NETWORK_FILE', 'DATA_DIR', 'TRADE_MATRICES_DIR',
        'HS_CODES_MAPPING_FILE', 'CONVERSION_FACTORS_FILE',
        'MERCHANT_FLEET_FILE', 'IMF_PORT_DATA_FILE', 'BACI_CODES_FILE',
        'OUTPUT_DIR',
    ]

    resolved = dict(cfg)
    for key in path_keys:
        if key in resolved:
            p = Path(resolved[key])
            if not p.is_absolute():
                resolved[key] = str((Path(base_dir) / p).resolve())

    return resolved
