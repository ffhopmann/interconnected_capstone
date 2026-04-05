#!/usr/bin/env python3
"""
create_scenario_config.py — Write a scenario-specific simulation_config.json.

Usage:
    python3 create_scenario_config.py <scenario_name>

Available scenarios:
    baseline                  — normal conditions, proactive rerouting
    suez_50pct_reduction      — Suez Canal at 50% capacity: 50% of ships rerouted
                                around the canal (Cape of Good Hope), 50% transit normally
    panama_closure_permanent  — Panama Canal permanently closed from day 1
    hormuz_closure_permanent  — Strait of Hormuz permanently closed from day 1;
                                ships with no alternative route are cancelled
    suez_evergiven            — Suez Canal closed days 82-88 (Mar 23-29),
                                reactive rerouting only (ships have no advance warning)
    suez_50pct_hormuz_temp    — Suez Canal at 50% capacity + Strait of Hormuz closed
                                Feb 28 (day 58) through May 1 (day 120), proactive rerouting
    eu_trade_deals_and_tariffs — EU–Mercosur deal (+39% EU exports / +17% EU imports),
                                 EU–India deal (+65% EU exports / +87% EU imports),
                                 US tariffs on EU (−17% EU exports to US, imports stable)
"""

import json
import sys
from pathlib import Path

# ---------------------------------------------------------------------------
# Country groups for bilateral trade scenarios
# (BACI country_name strings, filtered to those present in the simulation)
# ---------------------------------------------------------------------------

_EU_COUNTRIES = [
    'Belgium', 'Bulgaria', 'Croatia', 'Cyprus', 'Denmark', 'Estonia',
    'Finland', 'France', 'Germany', 'Greece', 'Ireland', 'Italy',
    'Latvia', 'Lithuania', 'Malta', 'Netherlands', 'Poland', 'Portugal',
    'Romania', 'Slovenia', 'Spain', 'Sweden',
]
_MERCOSUR_COUNTRIES = ['Argentina', 'Brazil', 'Uruguay']


def _bilateral_events(
    origin_countries, dest_countries, direction, adjustment_pct, day=0
):
    """Generate one bilateral EconomicEvent dict per (origin, dest) pair."""
    events = []
    for country in origin_countries:
        for counterpart in dest_countries:
            events.append({
                'day':                  day,
                'country':              country,
                'direction':            direction,
                'hs_codes':             [],
                'adjustment_pct':       adjustment_pct,
                'counterpart_country':  counterpart,
            })
    return events

# ---------------------------------------------------------------------------
# Scenario definitions
# ---------------------------------------------------------------------------

SCENARIOS = {
    'baseline': {
        'OUTPUT_DIR':           'simulation_output_data/scenario_baseline/',
        'CANAL_DAILY_RATES':    {'Suez Canal': 38, 'Panama Canal': 17},
        'INTERRUPTION_EVENTS':  [],
        'PROACTIVE_REROUTING':  True,
    },
    'suez_50pct_reduction': {
        'OUTPUT_DIR':           'simulation_output_data/scenario_suez_50pct_reduction/',
        'CANAL_DAILY_RATES':    {'Suez Canal': 19, 'Panama Canal': 17},
        'INTERRUPTION_EVENTS':  [],
        'PROACTIVE_REROUTING':  True,
    },
    'panama_closure_permanent': {
        'OUTPUT_DIR':           'simulation_output_data/scenario_panama_closure_permanent/',
        'CANAL_DAILY_RATES':    {'Suez Canal': 38, 'Panama Canal': 17},
        'INTERRUPTION_EVENTS':  [
            {
                'day':                      0,
                'end_day':                  None,
                'type':                     'choke_point',
                'target':                   'Panama Canal',
                'capacity_multiplier':      0.0,
                'cancel_if_no_alternative': False,
            },
        ],
        'PROACTIVE_REROUTING':  True,
    },
    'hormuz_closure_permanent': {
        'OUTPUT_DIR':           'simulation_output_data/scenario_hormuz_closure_permanent/',
        'CANAL_DAILY_RATES':    {'Suez Canal': 38, 'Panama Canal': 17},
        'INTERRUPTION_EVENTS':  [
            {
                'day':                  0,
                'end_day':              None,
                'type':                 'choke_point',
                'target':               'Strait of Hormuz',
                'capacity_multiplier':  0.0,
                'cancel_if_no_alternative': True,
            },
        ],
        'PROACTIVE_REROUTING':  True,
    },
    'suez_50pct_hormuz_closure': {
        'OUTPUT_DIR':           'simulation_output_data/scenario_suez_50pct_hormuz_closure/',
        'CANAL_DAILY_RATES':    {'Suez Canal': 19, 'Panama Canal': 17},
        'INTERRUPTION_EVENTS':  [
            {
                'day':                      0,
                'end_day':                  None,
                'type':                     'choke_point',
                'target':                   'Strait of Hormuz',
                'capacity_multiplier':      0.0,
                'cancel_if_no_alternative': True,
            },
        ],
        'PROACTIVE_REROUTING':  True,
    },
    'suez_evergiven': {
        # Ever Given grounding: Suez Canal blocked March 23–29 (simulation days 82–88)
        # Reactive rerouting only: ships had no advance knowledge of the closure.
        'OUTPUT_DIR':           'simulation_output_data/scenario_suez_evergiven/',
        'CANAL_DAILY_RATES':    {'Suez Canal': 38, 'Panama Canal': 17},
        'INTERRUPTION_EVENTS':  [
            {
                'day':                  82,
                'end_day':              88,
                'type':                 'choke_point',
                'target':               'Suez Canal',
                'capacity_multiplier':  0.0,
            },
        ],
        'PROACTIVE_REROUTING':  False,
    },
    'suez_50pct_hormuz_temp': {
        # Suez Canal at 50% capacity (19/day) throughout the year.
        # Strait of Hormuz closed Feb 28 (day 58) through Apr 30, reopening May 1 (day 120).
        'OUTPUT_DIR':           'simulation_output_data/scenario_suez_50pct_hormuz_temp/',
        'CANAL_DAILY_RATES':    {'Suez Canal': 19, 'Panama Canal': 17},
        'INTERRUPTION_EVENTS':  [
            {
                'day':                      58,
                'end_day':                  120,
                'type':                     'choke_point',
                'target':                   'Strait of Hormuz',
                'capacity_multiplier':      0.0,
                'cancel_if_no_alternative': True,
            },
        ],
        'PROACTIVE_REROUTING':  True,
    },
    'eu_trade_deals_and_tariffs': {
        # EU–Mercosur FTA: +39 % EU exports to Mercosur, +17 % EU imports from Mercosur.
        # EU–India FTA   : +65 % EU exports to India,   +87 % EU imports from India.
        # US tariffs      : −17 % EU exports to US; EU imports from US unchanged.
        # All changes applied as baseline (day 0) bilateral adjustments.
        'OUTPUT_DIR':           'simulation_output_data/scenario_eu_trade_deals_and_tariffs/',
        'CANAL_DAILY_RATES':    {'Suez Canal': 38, 'Panama Canal': 17},
        'INTERRUPTION_EVENTS':  [],
        'ECONOMIC_EVENTS': (
            _bilateral_events(_EU_COUNTRIES, _MERCOSUR_COUNTRIES, 'export',  39) +
            _bilateral_events(_EU_COUNTRIES, _MERCOSUR_COUNTRIES, 'import',  17) +
            _bilateral_events(_EU_COUNTRIES, ['India'],            'export',  65) +
            _bilateral_events(_EU_COUNTRIES, ['India'],            'import',  87) +
            _bilateral_events(_EU_COUNTRIES, ['USA'],              'export', -17)
        ),
        'PROACTIVE_REROUTING':  True,
    },
    'suez_closure_permanent': {
        'OUTPUT_DIR':           'simulation_output_data/scenario_suez_closure_permanent/',
        'CANAL_DAILY_RATES':    {'Suez Canal': 38, 'Panama Canal': 17},
        'INTERRUPTION_EVENTS':  [
            {
                'day':                      0,
                'end_day':                  None,
                'type':                     'choke_point',
                'target':                   'Suez Canal',
                'capacity_multiplier':      0.0,
                'cancel_if_no_alternative': False,
            },
        ],
        'PROACTIVE_REROUTING':  True,
    },
}

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    if len(sys.argv) != 2 or sys.argv[1] not in SCENARIOS:
        print(__doc__)
        print(f"Available: {', '.join(SCENARIOS)}")
        sys.exit(1)

    scenario_name = sys.argv[1]
    overrides = SCENARIOS[scenario_name]

    base_path = Path(__file__).parent / 'simulation_config.json'
    if not base_path.exists():
        print(f"ERROR: {base_path} not found. Run simulation_config.ipynb first.")
        sys.exit(1)

    with open(base_path) as f:
        cfg = json.load(f)

    # Apply scenario-specific overrides
    cfg.update(overrides)

    # Ensure RANDOM_SEED is set for reproducibility across scenarios
    if cfg.get('RANDOM_SEED') is None:
        cfg['RANDOM_SEED'] = 42
        print("  Note: RANDOM_SEED was null — set to 42 for reproducibility.")

    out_path = base_path  # overwrite simulation_config.json in-place
    with open(out_path, 'w') as f:
        json.dump(cfg, f, indent=2)

    print(f"Config written for scenario '{scenario_name}':")
    print(f"  OUTPUT_DIR:          {cfg['OUTPUT_DIR']}")
    print(f"  PROACTIVE_REROUTING: {cfg['PROACTIVE_REROUTING']}")
    print(f"  INTERRUPTION_EVENTS: {len(cfg['INTERRUPTION_EVENTS'])}")
    for ev in cfg['INTERRUPTION_EVENTS']:
        end = ev.get('end_day')
        end_str = f"→ day {end}" if end is not None else "→ permanent"
        print(f"    [{ev['type']}] {ev['target']}: x{ev['capacity_multiplier']} "
              f"day {ev['day']} {end_str}")


if __name__ == '__main__':
    main()
