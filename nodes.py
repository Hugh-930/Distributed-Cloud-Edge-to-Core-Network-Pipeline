"""
Implementation of the base queueing node and the upstream subsystem for the Multi-Tier Edge-to-Cloud Processing Network Pipeline.
"""

from dataclasses import dataclass, field
from typing import List, Optional, Tuple
import numpy as np


@dataclass
class Packet:
    """
    Represents a single network packet flowing through the edge-to-cloud pipeline.
    Tracks arrival and service timestamps for end-to-end latency and Little's Law verification.
    """
    packet_id: int
    system_arrival_time: float
    current_node_arrival_time: float = 0.0
    service_start_time: float = 0.0
    is_suspect: bool = False


class BaseNode:
    """
    Base class for all network processing nodes.
    Handles FIFO queueing, server utilization tracking, and time-average area integrals.
    """

    def __init__(
        self,
        node_id: int,
        name: str,
        num_servers: int,
        rng: Optional[np.random.Generator] = None,
        capacity: float = float("inf"),
    ):
        self.node_id = node_id
        self.name = name
        self.num_servers = num_servers
        self.capacity = capacity  # Finite buffer limit K (inf for unbounded queues)
        self.rng = rng if rng is not None else np.random.default_rng()

        # Dynamic state variables
        self.busy_servers: int = 0
        self.blocked_servers: int = 0  # Used when downstream finite buffer is full
        self.queue: List[Packet] = []

        # Statistical accumulators (reset after warm-up period)
        self.area_num_in_queue: float = 0.0
        self.area_num_in_system: float = 0.0
        self.area_busy_servers: float = 0.0
        self.area_blocked_servers: float = 0.0

        self.total_queue_wait_time: float = 0.0
        self.total_system_time: float = 0.0
        self.arrived_packets: int = 0
        self.completed_packets: int = 0

    @property
    def num_in_system(self) -> int:
        return len(self.queue) + self.busy_servers

    def has_free_server(self) -> bool:
        return self.busy_servers < self.num_servers

    def is_full(self) -> bool:
        return self.num_in_system >= self.capacity

    def sample_service_time(self) -> float:
        # Generates a random service duration in milliseconds. Overridden by subclasses.
        raise NotImplementedError("Subclasses must implement sample_service_time().")

    def select_next_node(self, packet: Packet) -> int:
        # Determines the downstream node ID after service completion. Overridden by subclasses.
        raise NotImplementedError("Subclasses must implement select_next_node().")


class Node1IngressRouter(BaseNode):
    """
    Node 1: Ingress Edge Router (G/G/s Queue)
    - Arrivals: Non-Stationary / Non-Homogeneous Poisson Process (NSPP/NHPP) 
    - Servers: s1 = 3 hardware ingress interfaces.
    - Service Time: Log-Normal distribution (mean = 4.0 ms, std = 1.2 ms).
    - Routing: 100% of parsed packets proceed to Node 2 (DPI & Firewall).
    """

    def __init__(
        self,
        num_servers: int = 3,
        mean_ms: float = 4.0,
        std_ms: float = 1.2,
        rng: Optional[np.random.Generator] = None,
        arrival_rng: Optional[np.random.Generator] = None,
    ):
        super().__init__(node_id=1, name="Node 1: Ingress Edge Router", num_servers=num_servers, rng=rng)
        self.mean_ms = mean_ms
        self.std_ms = std_ms
        self.arrival_rng = arrival_rng if arrival_rng is not None else np.random.default_rng()

        # Convert arithmetic mean (4.0 ms) and std (1.2 ms) to underlying normal parameters (mu_N, sigma_N)
        variance = self.std_ms ** 2
        self.sigma_normal = float(np.sqrt(np.log(1.0 + variance / (self.mean_ms ** 2))))
        self.mu_normal = float(np.log(self.mean_ms) - 0.5 * (self.sigma_normal ** 2))

        
        self.base_arrival_rate = 0.65
        self.spike_amplitude = 0.15
        self.period_ms = 24000.0  # Sinusoidal cycle across simulation window
        self.lambda_max = self.base_arrival_rate + self.spike_amplitude

    def nhpp_rate(self, t_ms: float) -> float:
        """
        max_rate = base_rate + spike_amplitude
        min_rate = base_rate - spike_amplitude
        1 cycle = 24 seconds
        """
        return self.base_arrival_rate + self.spike_amplitude * float(
            np.sin(2.0 * np.pi * t_ms / self.period_ms)
        )

    def sample_next_arrival_time(self, current_time_ms: float) -> float:
        # Generates the absolute timestamp of the next external packet arrival using the Lewis-Shedler Thinning Algorithm
        
        t = current_time_ms
        while True:
            # Step 1: Generate candidate arrival using upper bound homogeneous rate lambda_max
            u1 = self.arrival_rng.uniform(0.0, 1.0)
            t += -np.log(1.0 - u1) / self.lambda_max

            # Step 2: Thinning / Acceptance-Rejection test with probability lambda(t) / lambda_max
            u2 = self.arrival_rng.uniform(0.0, 1.0)
            if u2 <= (self.nhpp_rate(t) / self.lambda_max):
                return t

    def sample_service_time(self) -> float:
        """Samples packet parsing time from Log-Normal(mean=4.0 ms, std=1.2 ms)."""
        return float(self.rng.normal(mean=self.mu_normal, sigma=self.sigma_normal))

    def select_next_node(self) -> int:
        """All packets from Node 1 are forwarded to Node 2 (Deep Packet Inspection)."""
        return 2


