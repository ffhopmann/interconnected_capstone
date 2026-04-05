"""
event_manager.py — Sorted event queue for physical and economic events.

All simulation events (InterruptionEvent, EconomicEvent, and auto-generated
restoration events) are stored in a single priority queue sorted by `day`.

The simulation loop calls `pop_events(current_day)` each interval to retrieve
and fire any events that have become due.
"""

from __future__ import annotations
import heapq
from typing import Any, Dict, Iterator, List, Optional, Tuple, Union

from .models import EconomicEvent, InterruptionEvent


# Internal tagged event tuple: (day, sequence_number, tag, event_object)
# The sequence number breaks ties deterministically; tag distinguishes type.
_TAG_INTERRUPTION = 'interruption'
_TAG_RESTORATION  = 'restoration'   # auto-generated end of an interruption
_TAG_ECONOMIC     = 'economic'

_EventTuple = Tuple[float, int, str, Any]


class EventManager:
    """
    Priority queue of all scheduled simulation events.

    Usage
    -----
    em = EventManager()
    em.schedule_all(interruption_events, economic_events)

    # Inside the simulation loop:
    for event in em.pop_events(current_day):
        handle(event)
    """

    def __init__(self):
        self._heap: List[_EventTuple] = []
        self._counter = 0  # sequence number for tie-breaking

    # ------------------------------------------------------------------
    # Scheduling
    # ------------------------------------------------------------------

    def _push(self, day: float, tag: str, event: Any) -> None:
        heapq.heappush(self._heap, (day, self._counter, tag, event))
        self._counter += 1

    def schedule_interruption(self, event: InterruptionEvent) -> None:
        """Schedule an interruption event and, if applicable, its restoration."""
        self._push(event.day, _TAG_INTERRUPTION, event)
        if event.end_day is not None:
            # Auto-generate a restoration event at end_day
            restoration = InterruptionEvent(
                day=event.end_day,
                end_day=None,
                event_type=event.event_type,
                target=event.target,
                capacity_multiplier=1.0,  # restore to full
            )
            self._push(event.end_day, _TAG_RESTORATION, restoration)

    def schedule_economic(self, event: EconomicEvent) -> None:
        self._push(event.day, _TAG_ECONOMIC, event)

    def schedule_all(
        self,
        interruption_events: List[InterruptionEvent],
        economic_events: List[EconomicEvent],
    ) -> None:
        """Bulk-schedule all events from the config."""
        for e in interruption_events:
            self.schedule_interruption(e)
        for e in economic_events:
            self.schedule_economic(e)

    # ------------------------------------------------------------------
    # Retrieval
    # ------------------------------------------------------------------

    def pop_events(self, up_to_day: float) -> Iterator[Tuple[str, Any]]:
        """
        Yield (tag, event) for all events with day <= up_to_day, in order.
        Events are removed from the queue as they are yielded.
        """
        while self._heap and self._heap[0][0] <= up_to_day:
            day, _, tag, event = heapq.heappop(self._heap)
            yield tag, event

    def peek_next_day(self) -> Optional[float]:
        """Return the day of the next scheduled event, or None if empty."""
        return self._heap[0][0] if self._heap else None

    def has_events(self) -> bool:
        return bool(self._heap)

    def __len__(self) -> int:
        return len(self._heap)

    # ------------------------------------------------------------------
    # Serialisation (for checkpointing)
    # ------------------------------------------------------------------

    def state_dict(self) -> Dict:
        """Return a serialisable snapshot of the queue."""
        return {
            'heap': list(self._heap),
            'counter': self._counter,
        }

    def load_state_dict(self, state: Dict) -> None:
        self._heap = list(state['heap'])
        heapq.heapify(self._heap)
        self._counter = state['counter']


# ---------------------------------------------------------------------------
# Epoch schedule builder  (used by ship_generation.py)
# ---------------------------------------------------------------------------

def build_epoch_schedule(
    simulation_days: float,
    economic_events: List[EconomicEvent],
) -> List[Dict]:
    """
    Split the simulation timeline into epochs at each day where a new
    economic event begins.  Day-0 events are treated as baseline adjustments
    and do NOT create epoch boundaries (they are applied before epoch 1).

    Returns
    -------
    List of epoch dicts:
        {
            'start_day': float,
            'end_day':   float,
            'cumulative_adjustments': [EconomicEvent, ...],
        }
    where cumulative_adjustments contains all EconomicEvents with day <= start_day
    (excluding day-0 events which are applied globally as a baseline).

    Example
    -------
    Economic events at day 0, day 90, day 180 over a 365-day sim:
        Epoch 1: day   0 –  90   (baseline only)
        Epoch 2: day  90 – 180   (baseline + day-90 event)
        Epoch 3: day 180 – 365   (baseline + day-90 + day-180 events)
    """
    # Mid-simulation events (day > 0) create epoch boundaries
    boundary_days = sorted(
        {float(e.day) for e in economic_events if e.day > 0}
    )
    # Always end at simulation_days
    boundaries = [0.0] + boundary_days + [float(simulation_days)]

    # Build cumulative adjustment list up to each epoch start
    mid_sim_events = sorted(
        [e for e in economic_events if e.day > 0],
        key=lambda e: e.day,
    )

    epochs = []
    for i in range(len(boundaries) - 1):
        start = boundaries[i]
        end = boundaries[i + 1]
        if start >= end:
            continue

        # All mid-sim events that have fired by the start of this epoch
        cumulative = [e for e in mid_sim_events if e.day <= start]

        epochs.append({
            'start_day': start,
            'end_day': end,
            'cumulative_adjustments': cumulative,
        })

    return epochs
