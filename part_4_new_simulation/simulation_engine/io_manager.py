"""
io_manager.py — Parquet I/O, checkpoint save/load, and CSV compatibility exports.

Parquet files are written incrementally (append mode) to avoid keeping all
simulation data in memory for long runs.

Checkpoint files are pickle snapshots of the full simulation state, stored in
the checkpoints/ subdirectory.  Only the most recent MAX_CHECKPOINTS files are
kept to manage disk usage.
"""

from __future__ import annotations
import glob
import os
import pickle
import shutil
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd


MAX_CHECKPOINTS = 3   # Number of checkpoint files to retain


# ---------------------------------------------------------------------------
# Parquet helpers
# ---------------------------------------------------------------------------

def write_parquet(df: pd.DataFrame, path: str, append: bool = False) -> None:
    """
    Write a DataFrame to a Parquet file.

    Parameters
    ----------
    df     : DataFrame to write
    path   : destination file path
    append : if True and file exists, append rows; otherwise overwrite
    """
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)

    if append and p.exists():
        try:
            existing = pd.read_parquet(p)
            df = pd.concat([existing, df], ignore_index=True)
        except Exception:
            # File is corrupted (e.g. interrupted write); start fresh
            pass

    df.to_parquet(p, index=False, compression='snappy')


def append_parquet(df: pd.DataFrame, path: str) -> None:
    """Convenience wrapper: always append to the Parquet file."""
    write_parquet(df, path, append=True)


def read_parquet(path: str) -> pd.DataFrame:
    """Read a Parquet file, returning an empty DataFrame if it does not exist."""
    p = Path(path)
    if not p.exists():
        return pd.DataFrame()
    return pd.read_parquet(p)


# ---------------------------------------------------------------------------
# Ship location streaming
# ---------------------------------------------------------------------------

class LocationBuffer:
    """
    Accumulates ship location records and periodically flushes them to Parquet.

    Columns: timestep, day, ship_id, status, node1, node2,
             edge_length_km, progress_fraction, port_name
    """

    def __init__(self, output_path: str, flush_every: int = 8760):
        self._path = output_path
        self._flush_every = flush_every
        self._records: List[Dict] = []

    def add(self, timestep: int, day: float, ship_id: int, location: Dict) -> None:
        rec = {
            'timestep':         timestep,
            'day':              day,
            'ship_id':          ship_id,
            'status':           location.get('status', ''),
            'node1':            str(location.get('edge', [None, None])[0]) if 'edge' in location else '',
            'node2':            str(location.get('edge', [None, None])[1]) if 'edge' in location else '',
            'edge_length_km':   location.get('edge_length_km', 0.0),
            'progress_fraction':location.get('progress_fraction', 0.0),
            'port_name':        location.get('port', ''),
        }
        self._records.append(rec)

        if len(self._records) >= self._flush_every:
            self.flush()

    def flush(self) -> None:
        if not self._records:
            return
        df = pd.DataFrame(self._records)
        append_parquet(df, self._path)
        self._records = []

    def __len__(self) -> int:
        return len(self._records)


# ---------------------------------------------------------------------------
# Checkpoint management
# ---------------------------------------------------------------------------

def save_checkpoint(state: Dict[str, Any], checkpoint_dir: str, day: float) -> str:
    """
    Serialise the full simulation state to a pickle file.

    Parameters
    ----------
    state          : dictionary of all simulation state to persist
    checkpoint_dir : directory to write checkpoints into
    day            : current simulation day (used in the filename)

    Returns
    -------
    Path of the written checkpoint file.
    """
    ckpt_dir = Path(checkpoint_dir)
    ckpt_dir.mkdir(parents=True, exist_ok=True)

    filename = ckpt_dir / f'checkpoint_day_{day:.1f}.pkl'
    with open(filename, 'wb') as f:
        pickle.dump(state, f, protocol=pickle.HIGHEST_PROTOCOL)

    # Prune old checkpoints
    _prune_checkpoints(checkpoint_dir, MAX_CHECKPOINTS)

    return str(filename)


def load_checkpoint(checkpoint_dir: str) -> Optional[Dict[str, Any]]:
    """
    Load the most recent checkpoint from checkpoint_dir.

    Returns
    -------
    state dict, or None if no checkpoint exists.
    """
    ckpt_dir = Path(checkpoint_dir)
    files = sorted(ckpt_dir.glob('checkpoint_day_*.pkl'),
                   key=lambda p: float(p.stem.split('_day_')[1]))
    if not files:
        return None

    latest = files[-1]
    with open(latest, 'rb') as f:
        return pickle.load(f)


def _prune_checkpoints(checkpoint_dir: str, keep: int) -> None:
    files = sorted(Path(checkpoint_dir).glob('checkpoint_day_*.pkl'),
                   key=lambda p: float(p.stem.split('_day_')[1]))
    for old in files[:-keep]:
        try:
            old.unlink()
        except OSError:
            pass


# ---------------------------------------------------------------------------
# End-of-simulation aggregation and CSV exports
# ---------------------------------------------------------------------------

def build_edge_statistics_df(
    edge_traffic: Dict,
    G: Any,
    hs_codes: List[int],
) -> pd.DataFrame:
    """
    Convert the edge_traffic dict from the simulation runner into a
    DataFrame matching the original simulation_edge_statistics.csv schema.

    Parameters
    ----------
    edge_traffic : {(node1, node2): {'ship_count': int, 'cargo_total_weight': float,
                    'cargo_total_value': float, 'total_time_hours': float,
                    'cargo_hs{N}_weight': float, 'cargo_hs{N}_value': float, ...}}
    G            : NetworkX graph (for edge lengths)
    hs_codes     : list of HS code integers (e.g. range 1-97 minus 77)
    """
    rows = []
    for (node1, node2), data in edge_traffic.items():
        edge_length = 0.0
        if G.has_edge(node1, node2):
            edge_length = G[node1][node2].get('length', 0.0)
        elif G.has_edge(node2, node1):
            edge_length = G[node2][node1].get('length', 0.0)

        row = {
            'node1':              str(node1),
            'node2':              str(node2),
            'edge_length_km':     float(edge_length),
            'ship_count':         int(data.get('ship_count', 0)),
            'total_time_hours':   float(data.get('total_time_hours', 0.0)),
            'cargo_total_weight': float(data.get('cargo_total_weight', 0.0)),
            'cargo_total_value':  float(data.get('cargo_total_value', 0.0)),
        }
        for hs in hs_codes:
            row[f'cargo_hs{hs}_weight'] = float(data.get(f'cargo_hs{hs}_weight', 0.0))
            row[f'cargo_hs{hs}_value']  = float(data.get(f'cargo_hs{hs}_value',  0.0))
        rows.append(row)

    return pd.DataFrame(rows)


def build_port_occupancy_df(
    port_occupancy_records: List[Dict],
) -> pd.DataFrame:
    """
    Convert the list of port occupancy records into a DataFrame.

    Expected record keys: timestep, day, port_name, num_ships, capacity
    """
    return pd.DataFrame(port_occupancy_records)


def build_port_cargo_df(
    port_cargo: Dict,
    hs_codes: List[int],
) -> pd.DataFrame:
    """
    Convert the port_cargo dict into a DataFrame.

    One row per port. Columns: port_name, ship_count, cargo_total_weight,
    cargo_total_value, cargo_hs{N}_weight, cargo_hs{N}_value for each HS code.
    Ship count includes both ships that loaded (origin) and unloaded (destination)
    at the port.
    """
    rows = []
    for name, data in port_cargo.items():
        row: Dict = {
            'port_name':          name,
            'ship_count':         int(data.get('ship_count', 0)),
            'cargo_total_weight': float(data.get('cargo_total_weight', 0.0)),
            'cargo_total_value':  float(data.get('cargo_total_value',  0.0)),
        }
        for hs in hs_codes:
            row[f'cargo_hs{hs}_weight'] = float(data.get(f'cargo_hs{hs}_weight', 0.0))
            row[f'cargo_hs{hs}_value']  = float(data.get(f'cargo_hs{hs}_value',  0.0))
        rows.append(row)
    return pd.DataFrame(rows)


def build_choke_cargo_df(
    choke_cargo: Dict,
    hs_codes: List[int],
) -> pd.DataFrame:
    """
    Convert the choke_cargo dict into a DataFrame.

    One row per choke point. Columns: choke_name, ship_count, cargo_total_weight,
    cargo_total_value, cargo_hs{N}_weight, cargo_hs{N}_value for each HS code.
    """
    rows = []
    for name, data in choke_cargo.items():
        row: Dict = {
            'choke_name':         name,
            'ship_count':         int(data.get('ship_count', 0)),
            'cargo_total_weight': float(data.get('cargo_total_weight', 0.0)),
            'cargo_total_value':  float(data.get('cargo_total_value',  0.0)),
        }
        for hs in hs_codes:
            row[f'cargo_hs{hs}_weight'] = float(data.get(f'cargo_hs{hs}_weight', 0.0))
            row[f'cargo_hs{hs}_value']  = float(data.get(f'cargo_hs{hs}_value',  0.0))
        rows.append(row)
    return pd.DataFrame(rows)


def export_compat_csvs(output_dir: str) -> None:
    """
    Write CSV copies of key Parquet outputs into output_dir/compat/ so that
    part_5_visualization notebooks can read them without modification.

    Files written:
      compat/simulation_ship_data.csv
      compat/simulation_edge_statistics.csv
      compat/simulation_port_occupancy.csv
    """
    out = Path(output_dir)
    compat = out / 'compat'
    compat.mkdir(parents=True, exist_ok=True)

    _convert_parquet_to_csv(
        src=str(out / 'ships.parquet'),
        dst=str(compat / 'simulation_ship_data.csv'),
        rename={'ship_id': 'ship_id'},  # identity — no column rename needed
    )
    _convert_parquet_to_csv(
        src=str(out / 'edge_statistics.parquet'),
        dst=str(compat / 'simulation_edge_statistics.csv'),
    )
    _convert_parquet_to_csv(
        src=str(out / 'port_occupancy.parquet'),
        dst=str(compat / 'simulation_port_occupancy.csv'),
    )
    _convert_parquet_to_csv(
        src=str(out / 'port_cargo.parquet'),
        dst=str(compat / 'simulation_port_cargo.csv'),
    )
    _convert_parquet_to_csv(
        src=str(out / 'choke_cargo.parquet'),
        dst=str(compat / 'simulation_choke_cargo.csv'),
    )


def _convert_parquet_to_csv(
    src: str,
    dst: str,
    rename: Optional[Dict[str, str]] = None,
) -> None:
    src_path = Path(src)
    if not src_path.exists():
        print(f"  [compat] Skipping {src_path.name} (not found)")
        return
    df = pd.read_parquet(src_path)
    if rename:
        df = df.rename(columns=rename)
    df.to_csv(dst, index=False)
    print(f"  [compat] Wrote {Path(dst).name}  ({len(df):,} rows)")


# ---------------------------------------------------------------------------
# Summary helpers
# ---------------------------------------------------------------------------

def print_simulation_summary(output_dir: str, hs_codes: List[int]) -> None:
    """Print a brief post-simulation summary from the Parquet outputs."""
    out = Path(output_dir)
    print("=" * 70)
    print("SIMULATION OUTPUT SUMMARY")
    print("=" * 70)

    # Ships
    ships_path = out / 'ships.parquet'
    if ships_path.exists():
        ships_df = pd.read_parquet(ships_path)
        print(f"\nShips: {len(ships_df):,}")
        print(f"  Total weight: {ships_df['cargo_total_weight'].sum():,.0f} mt")
        print(f"  Total value:  ${ships_df['cargo_total_value'].sum():,.0f}")
        n_rerouted = ships_df['rerouted'].sum() if 'rerouted' in ships_df.columns else 'N/A'
        print(f"  Rerouted:     {n_rerouted}")

    # Lost ships
    lost_path = out / 'lost_ships.parquet'
    if lost_path.exists():
        lost_df = pd.read_parquet(lost_path)
        if len(lost_df) > 0:
            print(f"\nLost ships: {len(lost_df):,}")
            print(f"  Lost weight: {lost_df['cargo_total_weight'].sum():,.0f} mt")
            print(f"  Lost value:  ${lost_df['cargo_total_value'].sum():,.0f}")
            print(f"  Reasons: {lost_df['reason'].value_counts().to_dict()}")
        else:
            print("\nLost ships: 0")

    # Edge statistics
    edge_path = out / 'edge_statistics.parquet'
    if edge_path.exists():
        edge_df = pd.read_parquet(edge_path)
        n_edges_with_traffic = (edge_df['ship_count'] > 0).sum()
        print(f"\nEdges:  {len(edge_df):,} total, {n_edges_with_traffic:,} with traffic")

    # Port cargo
    port_cargo_path = out / 'port_cargo.parquet'
    if port_cargo_path.exists():
        port_cargo_df = pd.read_parquet(port_cargo_path)
        n_ports_with_traffic = (port_cargo_df['ship_count'] > 0).sum()
        print(f"\nPorts:  {len(port_cargo_df):,} total, {n_ports_with_traffic:,} with traffic")

    # Choke cargo
    choke_cargo_path = out / 'choke_cargo.parquet'
    if choke_cargo_path.exists():
        choke_cargo_df = pd.read_parquet(choke_cargo_path)
        print(f"\nChoke points: {len(choke_cargo_df):,}")
        for _, row in choke_cargo_df.iterrows():
            print(f"  {row['choke_name']}: {int(row['ship_count']):,} ships, "
                  f"{row['cargo_total_weight']:,.0f} mt, ${row['cargo_total_value']:,.0f}")

    print("=" * 70)
