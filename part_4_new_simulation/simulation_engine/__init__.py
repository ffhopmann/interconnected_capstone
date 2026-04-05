"""
simulation_engine — Core logic for the Mediterranean (and global) maritime simulation.

Modules:
    models          — Dataclasses: Ship, InterruptionEvent, EconomicEvent
    config_loader   — Load simulation_config.json written by simulation_config.ipynb
    routing         — Route computation, K-shortest paths, rerouting decisions
    port_manager    — Port/choke-point queues and capacity multipliers
    event_manager   — Sorted event queue (physical + economic events)
    io_manager      — Parquet I/O and checkpoint save/load
    ship_generation — Epoch-aware synthetic ship generation
    simulation_runner — Main hourly simulation loop
"""

from .models import Ship, InterruptionEvent, EconomicEvent
from .config_loader import load_config
