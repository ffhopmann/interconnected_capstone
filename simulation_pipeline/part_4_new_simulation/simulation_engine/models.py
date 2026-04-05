"""
models.py — Core dataclasses for the simulation.

Provides:
    Ship               — A synthetic vessel with cargo, route, and mutable state
    InterruptionEvent  — Physical disruption to a port or choke point
    EconomicEvent      — Trade volume adjustment (baseline or mid-simulation)
    LostShip           — Record of a ship that could not complete its voyage
"""

from __future__ import annotations
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


# ---------------------------------------------------------------------------
# Ship states (ordered lifecycle)
# ---------------------------------------------------------------------------
# waiting_to_load  → ship has arrived at origin port, waiting for a berth
# loading          → ship is occupying a berth and being loaded
# traveling        → ship is underway along its route
# waiting_at_node  → ship is queued at a choke point or canal, waiting for access
# canal_transit    → ship is actively transiting a canal (timer counts down)
# waiting_to_unload→ ship has arrived at destination port, waiting for a berth
# unloading        → ship is occupying a berth and being unloaded
# completed        → ship has finished its voyage (removed from active set)

SHIP_STATES = {
    'waiting_to_load',
    'loading',
    'traveling',
    'waiting_at_node',
    'canal_transit',
    'waiting_to_unload',
    'unloading',
    'completed',
}


@dataclass
class Ship:
    """
    A synthetic vessel generated from trade data.

    Cargo is stored in two forms:
      - cargo_total_weight / cargo_total_value: aggregate totals
      - cargo_by_hs: per-HS-code breakdown, e.g.
            {27: {'weight': 45000.0, 'value': 22900000.0}, ...}

    Path is a list of NetworkX node IDs representing the route from
    origin port to destination port through the shipping network.

    Mutable state fields are updated in-place during the simulation loop.
    """

    # --- Identity ---
    id: int
    origin_country: str
    dest_country: str
    origin_port: str
    dest_port: str
    ship_type: str                        # 'tanker' | 'bulk carrier' | 'cargo ship'
    injection_day: float                  # Simulation day when this ship enters

    # --- Route ---
    path: List[Any]                       # Ordered list of network node IDs
    path_length: float                    # Total route length in km

    # --- Cargo ---
    cargo_total_weight: float             # Metric tons
    cargo_total_value: float              # USD
    cargo_by_hs: Dict[int, Dict[str, float]]  # {hs_code: {'weight': t, 'value': usd}}

    # --- Port timing (in simulation intervals, set at construction) ---
    loading_time: int
    unloading_time: int

    # --- Mutable simulation state ---
    state: str = 'waiting_to_load'
    current_edge_idx: int = 0             # Index into path[] for current edge
    km_into_current_edge: float = 0.0     # How far along the current edge (km)
    loading_remaining: int = 0            # Intervals left in loading
    unloading_remaining: int = 0          # Intervals left in unloading
    canal_remaining: int = 0              # Intervals left in canal transit
    current_canal: Optional[str] = None  # Canal name while in queue or transit
    wait_intervals: int = 0               # Total intervals spent waiting (all causes)
    completed: bool = False

    # --- Rerouting log ---
    reroute_history: List[Dict] = field(default_factory=list)
    # Each entry: {'day': float, 'reason': str, 'old_dest_port': str,
    #              'new_dest_port': str, 'blocked_node': any}

    def flat_cargo_dict(self) -> Dict[str, float]:
        """
        Return cargo as a flat dict for DataFrame construction:
            {'cargo_hs1_weight': ..., 'cargo_hs1_value': ..., ...}
        """
        out = {}
        for hs_code, vals in self.cargo_by_hs.items():
            out[f'cargo_hs{hs_code}_weight'] = vals.get('weight', 0.0)
            out[f'cargo_hs{hs_code}_value'] = vals.get('value', 0.0)
        return out

    def to_record(self) -> Dict:
        """Return a flat dict suitable for one row in ships.parquet."""
        rec = {
            'ship_id': self.id,
            'origin_country': self.origin_country,
            'dest_country': self.dest_country,
            'origin_port': self.origin_port,
            'dest_port': self.dest_port,
            'ship_type': self.ship_type,
            'injection_day': self.injection_day,
            'cargo_total_weight': self.cargo_total_weight,
            'cargo_total_value': self.cargo_total_value,
            'rerouted': len(self.reroute_history) > 0,
            'reroute_count': len(self.reroute_history),
            'was_lost': False,
        }
        rec.update(self.flat_cargo_dict())
        return rec


# ---------------------------------------------------------------------------
# Events
# ---------------------------------------------------------------------------

@dataclass(order=True)
class InterruptionEvent:
    """
    A physical disruption: capacity reduction or full closure of a port
    or choke point.

    capacity_multiplier:
        1.0  = no change (normal operation)
        0.5  = 50 % capacity (half the berths / throughput available)
        0.0  = full closure (no ships can enter / pass)

    end_day:
        None = permanent (no automatic restoration)
        float = the simulation day on which capacity is restored to 1.0
    """
    day: float                            # When this event fires
    end_day: Optional[float]             # When to restore (None = permanent)
    event_type: str                      # 'port' | 'choke_point'
    target: str                          # Port name or choke point name
    capacity_multiplier: float           # New capacity fraction [0.0, 1.0]
    cancel_if_no_alternative: bool = False  # If True, ships with no reroute are cancelled

    # Sort key uses day
    sort_index: float = field(init=False, repr=False)

    def __post_init__(self):
        object.__setattr__(self, 'sort_index', self.day)


@dataclass(order=True)
class EconomicEvent:
    """
    A trade volume adjustment that reshapes future ship injections.

    day == 0  → baseline modification applied before any ship generation
    day > 0   → mid-simulation event; affects ships generated for epochs
                starting on or after this day

    hs_codes:
        Empty list  = apply to ALL HS codes for the given country/direction
        Non-empty   = apply only to listed HS codes

    adjustment_pct:
        -10   = 10 % reduction in trade volume
        +20   = 20 % increase in trade volume
        -100  = zero out this trade flow entirely

    counterpart_country:
        None  = adjust the full row/column (all trading partners)
        str   = adjust only the single cell (country ↔ counterpart bilateral flow)
    """
    day: float
    country: str
    direction: str                        # 'export' | 'import' | 'both'
    hs_codes: List[int]                  # [] = all
    adjustment_pct: float                # percentage change
    counterpart_country: Optional[str] = None  # None = all partners; str = bilateral only

    sort_index: float = field(init=False, repr=False)

    def __post_init__(self):
        object.__setattr__(self, 'sort_index', self.day)


# ---------------------------------------------------------------------------
# Lost ship record
# ---------------------------------------------------------------------------

@dataclass
class LostShip:
    """
    Records a ship that was removed from the simulation before completing
    its voyage (e.g. destination port permanently closed with no alternative).
    """
    ship_id: int
    day_lost: float
    reason: str                           # e.g. 'dest_port_closed', 'no_route'
    origin_country: str
    dest_country: str
    origin_port: str
    intended_dest_port: str
    cargo_total_weight: float
    cargo_total_value: float
    cargo_by_hs: Dict[int, Dict[str, float]]

    def to_record(self) -> Dict:
        rec = {
            'ship_id': self.ship_id,
            'day_lost': self.day_lost,
            'reason': self.reason,
            'origin_country': self.origin_country,
            'dest_country': self.dest_country,
            'origin_port': self.origin_port,
            'intended_dest_port': self.intended_dest_port,
            'cargo_total_weight': self.cargo_total_weight,
            'cargo_total_value': self.cargo_total_value,
        }
        for hs_code, vals in self.cargo_by_hs.items():
            rec[f'cargo_hs{hs_code}_weight'] = vals.get('weight', 0.0)
            rec[f'cargo_hs{hs_code}_value'] = vals.get('value', 0.0)
        return rec
