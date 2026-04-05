"""
port_manager.py — Port, choke point, and canal queue management.

Responsibilities:
  - Track which ships are loading / unloading at each port
  - Track which ships are queued at each choke point or canal
  - Apply and restore capacity multipliers from InterruptionEvents
  - Expose the set of currently closed ports (for failover logic)

Design:
  Ports use M/M/c queuing: capacity = number of berths.
  Effective berths = floor(base_capacity × multiplier), minimum 0.

  Choke points use a throughput model: ships are released at a rate of
  floor(base_throughput × multiplier) per interval.
  If base_throughput is None at multiplier=1.0, ships pass freely (no queue).
  When multiplier drops below 1.0, an effective throughput is computed as:
      max(1, floor(FALLBACK_CHOKE_THROUGHPUT × multiplier))
  where FALLBACK_CHOKE_THROUGHPUT = 5 ships/interval (conservative default).

  Canals (e.g. Suez Canal, Panama Canal) use a transit-slot model:
  Each canal has a fixed number of simultaneous transit slots calibrated so
  that utilization ρ = CANAL_TARGET_RHO at baseline traffic. Ships queue
  (FIFO) until a slot is free, then enter canal_transit state for a fixed
  transit_time_intervals before continuing their voyage.
  Effective slots = floor(base_capacity × multiplier), minimum 0.
"""

from __future__ import annotations
import math
from collections import deque
from typing import Any, Dict, List, Optional, Set

from .models import InterruptionEvent

# Throughput used when a choke point has base_throughput=None but
# capacity_multiplier drops below 1.0 (i.e. an interruption is active).
_FALLBACK_CHOKE_THROUGHPUT = 5  # ships / interval


class PortManager:
    """
    Manages berth queues and capacity multipliers for all ports and choke points.
    """

    def __init__(
        self,
        port_names: List[str],
        base_capacities: Dict[str, int],
        choke_point_names: List[str],
        choke_base_throughputs: Dict[str, Optional[int]],
    ):
        """
        Parameters
        ----------
        port_names             : all port names in the network
        base_capacities        : {port_name: berths} from queuing theory
        choke_point_names      : all choke point names in the network
        choke_base_throughputs : {choke_name: ships_per_interval or None}
        """
        self._port_names = list(port_names)
        self._base_capacities: Dict[str, int] = dict(base_capacities)
        self._choke_names = list(choke_point_names)
        self._choke_base_throughputs: Dict[str, Optional[int]] = dict(choke_base_throughputs)

        # Current multipliers (1.0 = normal)
        self._port_multipliers: Dict[str, float] = {p: 1.0 for p in port_names}
        self._choke_multipliers: Dict[str, float] = {c: 1.0 for c in choke_point_names}

        # Active berth occupants: port → {ship_id, ...}
        self._loading: Dict[str, Set[int]] = {p: set() for p in port_names}
        self._unloading: Dict[str, Set[int]] = {p: set() for p in port_names}

        # Choke point FIFO queues: choke_name → deque of ship_ids
        self._choke_queues: Dict[str, deque] = {c: deque() for c in choke_point_names}

        # Canal transit slots (populated by setup_canals after calibration)
        self._canal_capacities: Dict[str, int] = {}
        self._canal_active: Dict[str, Set[int]] = {}
        self._canal_queues: Dict[str, deque] = {}
        self._canal_multipliers: Dict[str, float] = {}

    # ------------------------------------------------------------------
    # Port berth management
    # ------------------------------------------------------------------

    def effective_capacity(self, port_name: str) -> int:
        """Return current effective berths (0 = fully closed)."""
        base = self._base_capacities.get(port_name, 1)
        mult = self._port_multipliers.get(port_name, 1.0)
        return max(0, math.floor(base * mult))

    def loading_count(self, port_name: str) -> int:
        return len(self._loading.get(port_name, set()))

    def unloading_count(self, port_name: str) -> int:
        return len(self._unloading.get(port_name, set()))

    def occupancy(self, port_name: str) -> int:
        return self.loading_count(port_name) + self.unloading_count(port_name)

    def can_load(self, port_name: str) -> bool:
        cap = self.effective_capacity(port_name)
        return cap > 0 and self.occupancy(port_name) < cap

    def can_unload(self, port_name: str) -> bool:
        cap = self.effective_capacity(port_name)
        return cap > 0 and self.occupancy(port_name) < cap

    def start_loading(self, port_name: str, ship_id: int) -> None:
        self._loading[port_name].add(ship_id)

    def finish_loading(self, port_name: str, ship_id: int) -> None:
        self._loading[port_name].discard(ship_id)

    def start_unloading(self, port_name: str, ship_id: int) -> None:
        self._unloading[port_name].add(ship_id)

    def finish_unloading(self, port_name: str, ship_id: int) -> None:
        self._unloading[port_name].discard(ship_id)

    def is_port_closed(self, port_name: str) -> bool:
        return self._port_multipliers.get(port_name, 1.0) <= 0.0

    @property
    def closed_ports(self) -> Set[str]:
        return {p for p in self._port_names if self.is_port_closed(p)}

    # ------------------------------------------------------------------
    # Choke point queue management
    # ------------------------------------------------------------------

    def effective_choke_throughput(self, choke_name: str) -> Optional[int]:
        """
        Return the effective throughput (ships / interval) for a choke point.

        Returns None if the choke point is in passthrough mode (no queue).
        Returns 0 if fully closed.
        Returns a positive integer for limited throughput.
        """
        mult = self._choke_multipliers.get(choke_name, 1.0)
        base = self._choke_base_throughputs.get(choke_name)

        if base is None:
            # Passthrough mode
            if mult >= 1.0:
                return None  # No queuing
            else:
                # Interruption active: derive throughput from fallback
                return max(0, math.floor(_FALLBACK_CHOKE_THROUGHPUT * mult))
        else:
            return max(0, math.floor(base * mult))

    def is_choke_passthrough(self, choke_name: str) -> bool:
        """True when no queueing occurs (base=None and multiplier=1.0)."""
        return self.effective_choke_throughput(choke_name) is None

    def is_choke_closed(self, choke_name: str) -> bool:
        return self._choke_multipliers.get(choke_name, 1.0) <= 0.0

    def enqueue_choke(self, choke_name: str, ship_id: int) -> None:
        self._choke_queues[choke_name].append(ship_id)

    def choke_queue_position(self, choke_name: str, ship_id: int) -> int:
        """Return 0-based position of ship_id in the choke queue (-1 if absent)."""
        try:
            return list(self._choke_queues[choke_name]).index(ship_id)
        except ValueError:
            return -1

    def choke_queue_length(self, choke_name: str) -> int:
        return len(self._choke_queues[choke_name])

    def release_from_choke(self, choke_name: str, n: int) -> List[int]:
        """
        Release up to n ships from the front of the choke queue.
        Returns the list of released ship IDs.
        """
        released = []
        queue = self._choke_queues[choke_name]
        for _ in range(min(n, len(queue))):
            released.append(queue.popleft())
        return released

    def remove_from_choke_queue(self, choke_name: str, ship_id: int) -> None:
        """Remove a specific ship from a choke queue (e.g. when it reroutes)."""
        queue = self._choke_queues[choke_name]
        try:
            idx = list(queue).index(ship_id)
            del queue[idx]
        except ValueError:
            pass

    # ------------------------------------------------------------------
    # Canal transit slot management
    # ------------------------------------------------------------------

    def setup_canals(self, canal_capacities: Dict[str, int]) -> None:
        """
        Initialise canal transit-slot tracking.  Call once after calibration,
        before the simulation loop starts.
        """
        for name, cap in canal_capacities.items():
            self._canal_capacities[name] = cap
            self._canal_active[name] = set()
            self._canal_queues[name] = deque()
            self._canal_multipliers[name] = 1.0

    def is_canal(self, name: str) -> bool:
        """True if this choke point uses the canal transit-slot model."""
        return name in self._canal_capacities

    def canal_effective_capacity(self, canal_name: str) -> int:
        """Effective transit slots (0 = canal closed)."""
        base = self._canal_capacities.get(canal_name, 0)
        mult = self._canal_multipliers.get(canal_name, 1.0)
        return max(0, math.floor(base * mult))

    def canal_active_count(self, canal_name: str) -> int:
        return len(self._canal_active.get(canal_name, set()))

    def canal_queue_length(self, canal_name: str) -> int:
        return len(self._canal_queues.get(canal_name, deque()))

    def canal_queue_position(self, canal_name: str, ship_id: int) -> int:
        """0-based position in canal wait queue (-1 if not queued)."""
        try:
            return list(self._canal_queues[canal_name]).index(ship_id)
        except ValueError:
            return -1

    def enqueue_canal(self, canal_name: str, ship_id: int) -> None:
        """Add ship to the back of the canal wait queue."""
        self._canal_queues[canal_name].append(ship_id)

    def try_start_canal_transit(self, canal_name: str) -> Optional[int]:
        """
        If a transit slot is available and the queue is non-empty, dequeue the
        next ship, mark it as active, and return its id.  Returns None if the
        canal is at capacity or the queue is empty.
        """
        cap = self.canal_effective_capacity(canal_name)
        active = self._canal_active[canal_name]
        queue = self._canal_queues[canal_name]
        if len(active) < cap and queue:
            ship_id = queue.popleft()
            active.add(ship_id)
            return ship_id
        return None

    def finish_canal_transit(self, canal_name: str, ship_id: int) -> None:
        """Remove a ship from the active transit set (call when transit timer expires)."""
        self._canal_active[canal_name].discard(ship_id)

    def remove_from_canal_queue(self, canal_name: str, ship_id: int) -> None:
        """Remove a ship from the canal wait queue (e.g. when it reroutes away)."""
        queue = self._canal_queues.get(canal_name, deque())
        try:
            idx = list(queue).index(ship_id)
            del queue[idx]
        except ValueError:
            pass

    def is_canal_closed(self, canal_name: str) -> bool:
        return self._canal_multipliers.get(canal_name, 1.0) <= 0.0

    @staticmethod
    def compute_canal_capacities(
        all_ships: List[Any],
        canal_names: List[str],
        canal_transit_intervals: Dict[str, int],
        choke_node_to_name: Dict[Any, str],
        n_intervals: int,
        target_rho: float,
    ) -> Dict[str, int]:
        """
        Calibrate canal transit-slot counts so that the utilisation factor
        ρ = (arrival_rate × transit_time) / capacity equals target_rho at
        baseline traffic.

        Parameters
        ----------
        all_ships               : Ship objects with .path attributes
        canal_names             : canal names to calibrate
        canal_transit_intervals : {canal_name: transit_time_in_intervals}
        choke_node_to_name      : {network_node_id: choke_name}
        n_intervals             : total simulation intervals
        target_rho              : desired utilisation (e.g. 0.7)

        Returns
        -------
        {canal_name: transit_slots}
        """
        canal_set = set(canal_names)
        ship_counts: Dict[str, int] = {name: 0 for name in canal_names}

        for ship in all_ships:
            seen = set()
            for node in ship.path:
                name = choke_node_to_name.get(node)
                if name and name in canal_set and name not in seen:
                    ship_counts[name] += 1
                    seen.add(name)

        capacities: Dict[str, int] = {}
        for name in canal_names:
            count = ship_counts[name]
            t_intervals = canal_transit_intervals.get(name, 1)
            if count == 0:
                capacities[name] = 1
            else:
                arrival_rate = count / n_intervals        # ships per interval
                c_min = (arrival_rate * t_intervals) / target_rho
                capacities[name] = max(1, math.ceil(c_min))

        return capacities

    # ------------------------------------------------------------------
    # Interruption application / restoration
    # ------------------------------------------------------------------

    def apply_interruption(self, event: InterruptionEvent) -> None:
        """Apply a capacity multiplier from an InterruptionEvent."""
        if event.event_type == 'port':
            if event.target in self._port_multipliers:
                self._port_multipliers[event.target] = event.capacity_multiplier
        elif event.event_type == 'choke_point':
            # Canal choke points are tracked separately
            if event.target in self._canal_multipliers:
                self._canal_multipliers[event.target] = event.capacity_multiplier
            elif event.target in self._choke_multipliers:
                self._choke_multipliers[event.target] = event.capacity_multiplier

    def restore_capacity(self, event: InterruptionEvent) -> None:
        """Restore full capacity when an event's end_day is reached."""
        if event.event_type == 'port':
            if event.target in self._port_multipliers:
                self._port_multipliers[event.target] = 1.0
        elif event.event_type == 'choke_point':
            if event.target in self._canal_multipliers:
                self._canal_multipliers[event.target] = 1.0
            elif event.target in self._choke_multipliers:
                self._choke_multipliers[event.target] = 1.0

    # ------------------------------------------------------------------
    # Port capacity initialisation (from queuing theory)
    # ------------------------------------------------------------------

    @staticmethod
    def compute_base_capacities(
        ships: List[Any],
        port_names: List[str],
        interval_size_days: float,
        target_rho: float,
        min_capacity: int,
    ) -> Dict[str, int]:
        """
        Compute M/M/c berth counts for each port using the same queuing-theory
        approach as the original Mediterranean_Model.ipynb.

        Parameters
        ----------
        ships              : list of Ship objects (with origin_port, dest_port,
                             loading_time, unloading_time attributes)
        port_names         : all port names
        interval_size_days : days per interval
        target_rho         : target utilisation ρ < 1
        min_capacity       : minimum berths per port

        Returns
        -------
        {port_name: berths}
        """
        capacities: Dict[str, int] = {}

        for port_name in port_names:
            ships_from = [s for s in ships if s.origin_port == port_name]
            ships_to   = [s for s in ships if s.dest_port == port_name]
            total_ships = len(ships_from) + len(ships_to)

            if total_ships == 0:
                capacities[port_name] = min_capacity
                continue

            # Arrival rate λ (ships/day)
            # We distribute ships over SIMULATION_DAYS via injection_day;
            # use the min injection day span or fall back to 365 days.
            lambda_rate = total_ships / 365.0

            # Average service time weighted over loading + unloading
            total_service_time = sum(
                s.loading_time * interval_size_days for s in ships_from
            ) + sum(
                s.unloading_time * interval_size_days for s in ships_to
            )
            avg_service_time = total_service_time / total_ships
            mu_rate = 1.0 / avg_service_time if avg_service_time > 0 else 1.0

            # Minimum c to achieve ρ < target_rho
            c_min = lambda_rate / (mu_rate * target_rho)
            capacity = max(min_capacity, math.ceil(c_min))
            capacities[port_name] = capacity

        return capacities

    # ------------------------------------------------------------------
    # Snapshot (for checkpointing)
    # ------------------------------------------------------------------

    def state_dict(self) -> Dict:
        return {
            'port_multipliers': dict(self._port_multipliers),
            'choke_multipliers': dict(self._choke_multipliers),
            'loading': {p: set(s) for p, s in self._loading.items()},
            'unloading': {p: set(s) for p, s in self._unloading.items()},
            'choke_queues': {c: list(q) for c, q in self._choke_queues.items()},
            'canal_capacities': dict(self._canal_capacities),
            'canal_multipliers': dict(self._canal_multipliers),
            'canal_active': {c: set(s) for c, s in self._canal_active.items()},
            'canal_queues': {c: list(q) for c, q in self._canal_queues.items()},
        }

    def load_state_dict(self, state: Dict) -> None:
        self._port_multipliers.update(state.get('port_multipliers', {}))
        self._choke_multipliers.update(state.get('choke_multipliers', {}))
        for p, s in state.get('loading', {}).items():
            self._loading[p] = set(s)
        for p, s in state.get('unloading', {}).items():
            self._unloading[p] = set(s)
        for c, q in state.get('choke_queues', {}).items():
            self._choke_queues[c] = deque(q)
        # Canal state (only present if canals were configured)
        for c, cap in state.get('canal_capacities', {}).items():
            self._canal_capacities[c] = cap
        for c, mult in state.get('canal_multipliers', {}).items():
            self._canal_multipliers[c] = mult
        for c, s in state.get('canal_active', {}).items():
            self._canal_active[c] = set(s)
        for c, q in state.get('canal_queues', {}).items():
            self._canal_queues[c] = deque(q)
