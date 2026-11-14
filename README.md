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


## what's currently missing in the code
| Requirement | Current code | What’s missing or needs fixing |
|---|---|---|
| **Node 3 finite capacity and backpressure** | Capacity is set to 15, but `update_blocked_servers()` contains `pass`. Arrivals are admitted even when full. | Hold completed packets at upstream workers when Node 3 is full, keep those workers blocked, and release them when space becomes available. Track blocking statistics. |
| **Node 7 has four workers** | `Node7.__init__()` defaults to **3**, despite its docstring saying 4. | Set the default to **4**. |
| **Node 8 acknowledges departures** | Its zero servers cause every arriving packet to enter its queue permanently. | Handle sink arrivals as completed network departures and record their completion times. |
| **Warm-up determination** | Statistics start at time zero. | Implement an algorithm such as Welch’s method, choose a warm-up duration, and reset measurement accumulators afterward while preserving network state. |
| **8 independent replications** | One simulation runs with fixed seeds. | Create fresh nodes and independent random streams for each of eight runs. Reusing the global `nodes_dict` retains state. |
| **48 seconds measured after warm-up** | Default duration is 10,000 ms. | Run each replication for the chosen warm-up plus **48,000 ms**, collecting results over the measurement interval. |
| **Network throughput** | Only per-node departure rates are printed. | Count completed network departures during the measurement interval and divide by its duration. |
| **Mean and 99th-percentile end-to-end latency** | `system_arrival_time` exists, but only per-node latency is recorded. | At the sink, calculate `completion_time - system_arrival_time`, then compute the mean and 99th percentile. |
| **95% confidence intervals** | Reports contain single-run averages. | Aggregate replication-level estimates and calculate confidence intervals for the required metrics. |
| **Utilization and Node 3 congestion** | Per-node utilization and average queue lengths are already calculated. | Measure them after warm-up, add confidence intervals, and report Node 3’s backpressure/blocking probability. |
| **Little’s Law verification** | The code calculates quantities corresponding to \(L\), \(\lambda\), and \(W\), but never compares them. | Check \(L \approx \lambda W\) for Nodes 1–7, with consistent measurement boundaries and blocking time accounted for. |
| **Bottleneck analysis** | No analysis workflow exists. | Compare utilization, delays, and upstream blocking to identify bottlenecks. |
| **Two-core upgrade experiments** | Worker counts can be changed manually. | Evaluate a proposed allocation of two additional workers against **three alternative allocations**, using 99th-percentile end-to-end latency as the objective. |
| **CRN and paired t-tests** | Separate RNGs exist for some processes, but there is no scenario-comparison runner. | Match random streams across designs within each replication, keep replications independent, and perform paired t-tests against the proposed allocation. |
