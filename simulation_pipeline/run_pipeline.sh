#!/bin/bash
# run_pipeline.sh — Full pipeline: network (if needed) → simulation scenarios
#
# Scenarios run:
#   baseline                  Normal conditions
#   suez_50pct_reduction      Suez Canal at 50% capacity (50% of ships rerouted)
#   hormuz_closure_permanent  Strait of Hormuz permanently closed; ships with
#                             no alternative route are cancelled
#
# Network extraction is expected to be run manually in VS Code.
# This script polls network_dp.gpickle every 60s and proceeds automatically
# once it detects the file has been written/updated since the script started.
# It then runs network_calibration and the full simulation pipeline.
# If network_calibrated.gpickle already exists, all network steps are skipped.
# simulation_config.ipynb is run once to produce the base simulation_config.json;
# create_scenario_config.py then patches it for each scenario.
# Route pre-computation runs once (baseline) and is copied to other scenario dirs.

set -e
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PART3="$SCRIPT_DIR/part_3_network_extraction"
PART4="$SCRIPT_DIR/part_4_new_simulation"
CALIBRATED="$PART3/network_outputs/network_calibrated.gpickle"
NETWORK_DP="$PART3/network_outputs/network_dp.gpickle"
BASELINE_DIR="$PART4/simulation_output_data/scenario_baseline"

run_nb() {
    # run_nb <label> <notebook_path>
    echo ""
    echo "=== $1 ==="
    jupyter nbconvert --to notebook --execute --inplace \
        --ExecutePreprocessor.timeout=86400 \
        "$2"
    echo "$1 complete."
}

# Returns the Unix modification time of a file (macOS + Linux compatible)
file_mtime() {
    stat -f %m "$1" 2>/dev/null || stat -c %Y "$1" 2>/dev/null || echo 0
}

# ============================================================
# NETWORK (skip if calibrated network already exists)
# ============================================================
if [ -f "$CALIBRATED" ]; then
    echo ""
    echo "=== Network: skipping (network_calibrated.gpickle exists) ==="
else
    SCRIPT_START=$(date +%s)
    echo ""
    echo "=== Waiting for network_extraction.ipynb to finish in VS Code ==="
    echo "    Monitoring: $NETWORK_DP"
    echo "    Checking every 60s..."
    echo ""

    while true; do
        if [ -f "$NETWORK_DP" ]; then
            FILE_MTIME=$(file_mtime "$NETWORK_DP")
            if [ "$FILE_MTIME" -gt "$SCRIPT_START" ]; then
                echo ""
                echo "  [$(date +%H:%M:%S)] network_dp.gpickle updated — extraction complete."
                break
            fi
        fi
        printf "  [$(date +%H:%M:%S)] Still waiting for network_extraction to complete...\r"
        sleep 60
    done

    run_nb "Step 1b: Network Calibration" "$PART3/network_calibration.ipynb"
fi

# ============================================================
# SIMULATION CONFIG — run once to produce base simulation_config.json
# ============================================================
run_nb "Simulation Config" "$PART4/simulation_config.ipynb"

# ============================================================
# BASELINE (includes route pre-computation)
# ============================================================
echo ""
echo "========================================================"
echo "  SCENARIO: baseline"
echo "========================================================"
python3 "$PART4/create_scenario_config.py" baseline

run_nb "  Route Pre-computation (baseline)" "$PART4/00_precompute_routes.ipynb"
run_nb "  Ship Generation (baseline)"       "$PART4/01_ship_generation.ipynb"
run_nb "  Simulation (baseline)"            "$PART4/02_simulation.ipynb"

# ============================================================
# SUEZ 50% REDUCTION — copies routes from baseline
# ============================================================
echo ""
echo "========================================================"
echo "  SCENARIO: suez_50pct_reduction"
echo "========================================================"
python3 "$PART4/create_scenario_config.py" suez_50pct_reduction

SUEZ_DIR="$PART4/simulation_output_data/scenario_suez_50pct_reduction"
mkdir -p "$SUEZ_DIR/checkpoints"
echo "  Copying pre-computed routes from baseline..."
cp "$BASELINE_DIR/port_pair_routes.pkl"    "$SUEZ_DIR/port_pair_routes.pkl"
cp "$BASELINE_DIR/country_pair_optimal.pkl" "$SUEZ_DIR/country_pair_optimal.pkl"

run_nb "  Ship Generation (suez_50pct_reduction)" "$PART4/01_ship_generation.ipynb"
run_nb "  Simulation (suez_50pct_reduction)"      "$PART4/02_simulation.ipynb"

# ============================================================
# HORMUZ CLOSURE — copies routes from baseline
# ============================================================
echo ""
echo "========================================================"
echo "  SCENARIO: hormuz_closure_permanent"
echo "========================================================"
python3 "$PART4/create_scenario_config.py" hormuz_closure_permanent

HORMUZ_DIR="$PART4/simulation_output_data/scenario_hormuz_closure_permanent"
mkdir -p "$HORMUZ_DIR/checkpoints"
echo "  Copying pre-computed routes from baseline..."
cp "$BASELINE_DIR/port_pair_routes.pkl"    "$HORMUZ_DIR/port_pair_routes.pkl"
cp "$BASELINE_DIR/country_pair_optimal.pkl" "$HORMUZ_DIR/country_pair_optimal.pkl"

run_nb "  Ship Generation (hormuz_closure_permanent)" "$PART4/01_ship_generation.ipynb"
run_nb "  Simulation (hormuz_closure_permanent)"      "$PART4/02_simulation.ipynb"

# ============================================================
echo ""
echo "========================================================"
echo "  Pipeline finished successfully."
echo "  Outputs:"
echo "    $PART4/simulation_output_data/scenario_baseline/"
echo "    $PART4/simulation_output_data/scenario_suez_50pct_reduction/"
echo "    $PART4/simulation_output_data/scenario_hormuz_closure_permanent/"
echo "========================================================"
