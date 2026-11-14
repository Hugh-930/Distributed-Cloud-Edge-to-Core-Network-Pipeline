# Distributed Cloud Edge-to-Core Network Pipeline

A Python discrete-event simulation of an eight-node packet-processing pipeline. The default run simulates 10 seconds and prints a per-node report to the terminal.

## Run

Use Python 3 with pip. NumPy is the only third-party dependency. Run the following from a terminal:

```sh
git clone https://github.com/Hugh-930/Distributed-Cloud-Edge-to-Core-Network-Pipeline.git
cd Distributed-Cloud-Edge-to-Core-Network-Pipeline
python -m pip install numpy
python project_engine.py
```

Use `python3` in both commands if that is how your system exposes Python 3.

## Files

| File | What it does |
| --- | --- |
| [`project_engine.py`](project_engine.py) | Entry point. Instantiates the eight nodes and runs the simulation. `SimulationEngine` uses a min-heap to process events in timestamp order, starts service or queues arriving packets, routes packets after service, and prints queue and server statistics. |
| [`nodes.py`](nodes.py) | Defines the `Packet` and `Event` records, shared FIFO queue and server state in `BaseNode`, and the arrival, service-time, and routing rules for `Node1` through `Node8`. |
| `__pycache__/nodes.cpython-311.pyc` | Generated CPython 3.11 bytecode for `nodes.py`. Python can regenerate this cache from source. |

## Node routing

| Node | Role | Workers | Next node |
| --- | --- | ---: | --- |
| 1 | Ingress edge router. Generates arrivals with a time-varying Poisson rate. | 3 | 2 |
| 2 | Deep packet inspection and firewall | 2 | 3 for clean packets (90%), 6 for suspect packets (10%) |
| 3 | Central load balancer | 4 | 4 (60%) or 5 (40%) |
| 4 | Cluster A (CPU) | 5 | 7 |
| 5 | Cluster B (GPU) | 3 | 7 |
| 6 | Security quarantine and threat analysis | 1 | 7 |
| 7 | Database ingestion | 3 | 8 |
| 8 | Sink | 0 | Terminal |

## Change a run

Edit the settings in the `if __name__ == "__main__":` block at the bottom of [`project_engine.py`](project_engine.py).

| Setting | Default | Effect |
| --- | --- | --- |
| `sim_duration` | `10000.0` | Stops processing events after this many simulated milliseconds. Packets still in flight at the cutoff remain unfinished. |
| `max_packets` | `None` | Set a positive integer to cap the number of external arrivals scheduled at Node 1. |
| `max_nodes` | `len(nodes_dict)` (8) | Sets the node-ID cutoff for forwarding packets. Keep the default for the full pipeline. |

To change worker counts, service parameters, or routing probabilities, pass constructor arguments in `nodes_dict` near the top of `project_engine.py`. Change `SEED` in [`nodes.py`](nodes.py) to use a different random seed.

## Read the report

The terminal report shows completed packets, average queue wait and time spent **at each node** (in milliseconds), average queue and system occupancy, throughput (packets per millisecond), average active workers, and utilization (a fraction from 0 to 1). The time-weighted metrics and throughput use the last processed event time as the observation window.

- **Node 3 capacity.** The code sets capacity to 15 packets, counting both queued packets and packets in service. The upstream blocking handler is a stub, so the engine can admit more than 15.
- **Node 8 completions.** The sink has zero workers and queues incoming packets, so its completion count stays at zero. Use Node 7’s completed-packet count for the number of packets delivered to the sink. The report has no end-to-end latency metric.
