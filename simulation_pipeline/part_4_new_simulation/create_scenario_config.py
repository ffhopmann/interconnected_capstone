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
"""

import json
import sys
from pathlib import Path

# ---------------------------------------------------------------------------
# Scenario definitions
# ---------------------------------------------------------------------------

SCENARIOS = {
    'baseline': {
        'OUTPUT_DIR':           'simulation_output_data/scenario_baseline/',
        'INTERRUPTION_EVENTS':  [],
        'PROACTIVE_REROUTING':  True,
    },
    'suez_50pct_reduction': {
        'OUTPUT_DIR':           'simulation_output_data/scenario_suez_50pct_reduction/',
        'INTERRUPTION_EVENTS':  [
            {
                'day':                  0,
                'end_day':              None,
                'type':                 'choke_point',
                'target':               'Suez Canal',
                'capacity_multiplier':  0.5,
            },
        ],
        'PROACTIVE_REROUTING':  True,
    },
    'panama_closure_permanent': {
        'OUTPUT_DIR':           'simulation_output_data/scenario_panama_closure_permanent/',
        'INTERRUPTION_EVENTS':  [
            {
                'day':                  1,
                'end_day':              None,
                'type':                 'choke_point',
                'target':               'Panama Canal',
                'capacity_multiplier':  0.0,
            },
        ],
        'PROACTIVE_REROUTING':  True,
    },
    'hormuz_closure_permanent': {
        'OUTPUT_DIR':           'simulation_output_data/scenario_hormuz_closure_permanent/',
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
    'suez_evergiven': {
        # Ever Given grounding: Suez Canal blocked March 23–29 (simulation days 82–88)
        # Reactive rerouting only: ships had no advance knowledge of the closure.
        'OUTPUT_DIR':           'simulation_output_data/scenario_suez_evergiven/',
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
