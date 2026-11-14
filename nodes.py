"""
Implementation of the base queueing node and the upstream subsystem for the Multi-Tier Edge-to-Cloud Processing Network Pipeline.
"""

from dataclasses import dataclass, field
from typing import List, Optional, Tuple
import numpy as np

SEED = 3

@dataclass
class Packet:
    """
    Represents a single network packet flowing through the edge-to-cloud pipeline.
    Tracks arrival and service timestamps for end-to-end latency
    """
    packet_id: int
    current_node_id: int
    system_arrival_time: float # used for latency calculations from end to end
    node_arrival_time: float = 0.0 # used for latency calculations at each node
    node_service_start_time: float = 0.0 # used for queue waiting time calculations at each node
    is_suspect: bool = False  # Flag to indicate the packet failed inspection at Node 2 


@dataclass(order=True)
class Event:
    """
    Represents a discrete event in the simulation (either an arrival or departure).
    Events are ordered by timestamp and sequence_id for deterministic processing.
    """
    timestamp: float
    sequence_id: int
    event_type: str = field(compare=False)  # ARRIVAL or DEPARTURE
    packet: object = field(compare=False)

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
        completed_packets: int = 0,
        rng: Optional[np.random.Generator] = None,
        capacity: Optional[float] = float("inf"),
        upstream_nodes: Optional[List[int]] = None,
    ):
        self.node_id = node_id
        self.name = name
        self.num_servers = num_servers
        self.completed_packets = completed_packets
        self.rng = rng if rng is not None else np.random.default_rng(seed=SEED+node_id*100)
        self.capacity = capacity  # Finite buffer limit K (inf for unbounded queues)
        self.latencies = [] # List to store latencies for statistical analysis
        self.queue_wait_times = [] # List to store waiting times in queue for statistical analysis
        self.upstream_nodes = upstream_nodes if upstream_nodes is not None else [] # List of upstream node IDs for FEL updates when the buffer is full

        # Dynamic state variables
        self.busy_servers: int = 0
        self.blocked_servers: int = 0  # Used when downstream finite buffer is full
        self.queue: List[Packet] = []

        # Statistical accumulators (reset after warm-up period)
        self.area_num_in_queue: float = 0.0
        self.area_num_in_system: float = 0.0
        self.area_busy_servers: float = 0.0

    @property
    def num_in_system(self) -> int:
        return len(self.queue) + self.busy_servers

    def has_free_server(self) -> bool:
        return self.busy_servers < self.num_servers

    def is_full(self) -> bool:
        return self.num_in_system >= self.capacity

    def sample_service_time(self) -> float:
        """Generates a random service duration in milliseconds. Overridden by subclasses."""    
        raise NotImplementedError("Subclasses must implement sample_service_time().")

    def select_next_node(self, packet: Packet) -> int:
        """Determines the downstream node ID after service completion. Overridden by subclasses."""
        raise NotImplementedError("Subclasses must implement select_next_node().")


class Node1(BaseNode):
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
        self.arrival_rng = arrival_rng if arrival_rng is not None else np.random.default_rng(seed=SEED+1)

        # Convert log-normal mean (4.0 ms) and std (1.2 ms) to normal parameters (mu_N, sigma_N)
        variance = self.std_ms ** 2
        self.sigma_normal = float(np.sqrt(np.log(1.0 + variance / (self.mean_ms ** 2))))
        self.mu_normal = float(np.log(self.mean_ms) - 0.5 * (self.sigma_normal ** 2))

        
        self.base_arrival_rate = 0.65
        self.spike_amplitude = 0.15
        self.period_ms = 24000.0  # 1 cycle = 24 seconds
        self.lambda_max = self.base_arrival_rate + self.spike_amplitude

    def sample_arrival_time(self, current_time_ms: float) -> float:
        """
        calculate the arrival rate and generate the next arrival time using  
        Lewis-Shedler Thinning Algorithm for Non-Homogeneous Poisson Processes (NHPP).
        """
        t = current_time_ms
        while True:

            u1 = self.arrival_rng.uniform(0.0, 1.0)
            t += -np.log(1.0 - u1) / self.lambda_max

            rate = self.base_arrival_rate + self.spike_amplitude * float(
                np.sin(2.0 * np.pi * t / self.period_ms)
            )
            u2 = self.arrival_rng.uniform(0.0, 1.0)
            if u2 <= (rate / self.lambda_max):
                return t

    def sample_service_time(self) -> float:
        """Samples packet parsing time from Log-Normal(mean=4.0 ms, std=1.2 ms)."""
        service_time = self.rng.lognormal(mean=self.mu_normal, sigma=self.sigma_normal)
        return float(service_time)  

    def select_next_node(self, packet: Packet) -> int:
        """All packets from Node 1 are forwarded to Node 2 (Deep Packet Inspection)."""
        return 2


class Node2(BaseNode):
    """
    Node 2: Deep Packet Inspection (DPI) & Firewall (M/M/s Queue)
    - Servers: s2 = 2 dedicated inspection cores.
    - Service Time: Exponential distribution (lambda_service = 0.5 packets/ms per core -> mean = 2.0 ms).
    - Routing: 90% clean packets -> Node 3 (Central Load Balancer);
               10% suspect packets -> Node 6 (Security Quarantine).
    """

    def __init__(
        self,
        num_servers: int = 2,
        service_rate_per_ms: float = 0.5,
        clean_prob: float = 0.90,
        rng: Optional[np.random.Generator] = None,
        routing_rng: Optional[np.random.Generator] = None,
    ):
        super().__init__(node_id=2, name="Node 2: DPI & Firewall", num_servers=num_servers, rng=rng)
        self.service_rate_per_ms = service_rate_per_ms
        self.mean_service_ms = 1.0 / service_rate_per_ms
        self.clean_prob = clean_prob
        self.routing_rng = routing_rng if routing_rng is not None else np.random.default_rng(seed=SEED+2)

    def sample_service_time(self) -> float:
        u = self.rng.uniform(0.0, 1.0)
        return float(-self.mean_service_ms * np.log(1.0 - u))

    def select_next_node(self, packet: Packet) -> int:
        if self.routing_rng.uniform(0.0, 1.0) < self.clean_prob:
            packet.is_suspect = False
            return 3
        else:
            packet.is_suspect = True
            return 6


class Node3(BaseNode):
    """
    Node 3: Central Load Balancer (M/G/s/K Finite-Buffer Queue)
    - Buffer Capacity: K = 15 packets. Upstream nodes: 2,6
    - Servers: s3 = 4 distribution threads.
    - Service Time: Triangular distribution (min=1.0, mode=2.0, max=4.0 ms).
    - f(x):
        f(x) = 2/3(x - 1) for 1 <= x <= 2
        f(x) = 2/3(4 - x) for 2 < x
    - F(x):
        F(x) = 1/3(x - 1)^2 for 1 <= x <= 2
        F(x) = 1 - 1/3(4 - x)^2 for 2 < x <= 4
    - X = 1 + sqrt(3u) for 0 <= u <= 1/3
    - X = 4 - sqrt(3(1 - u)) for 1/3 < u <= 1
    - Routing: 60% -> Node 4 (Cluster A)
               40% -> Node 5 (Cluster B).
    """

    def __init__(
        self,
        num_servers: int = 4,
        capacity: int = 15,
        left_ms: float = 1.0,
        mode_ms: float = 2.0,
        right_ms: float = 4.0,
        cluster_a_prob: float = 0.60,
        rng: Optional[np.random.Generator] = None,
        routing_rng: Optional[np.random.Generator] = None,
        upstream_nodes: Optional[List[int]] = [2, 6]
    ):
        super().__init__(
            node_id=3,
            name="Node 3: Central Load Balancer",
            num_servers=num_servers,
            rng=rng,
            capacity=capacity,
            upstream_nodes=upstream_nodes
        )
        self.left_ms = left_ms
        self.mode_ms = mode_ms
        self.right_ms = right_ms
        self.cluster_a_prob = cluster_a_prob
        self.routing_rng = routing_rng if routing_rng is not None else np.random.default_rng(seed=SEED+3)

    def sample_service_time(self) -> float:
        a, c, b = self.left_ms, self.mode_ms, self.right_ms
        u = self.rng.uniform(0.0, 1.0)
        split_point = (c - a) / (b - a)
        if u <= split_point:
            return float(a + np.sqrt(u * (b - a) * (c - a)))
        else:
            return float(b - np.sqrt((1.0 - u) * (b - a) * (b - c)))

    def select_next_node(self, packet: Packet) -> int:
        if self.routing_rng.uniform(0.0, 1.0) < self.cluster_a_prob:
            return 4
        else:
            return 5

class Node4(BaseNode):
    """
    Node 4: Cluster A (G/G/s Queue)
    - Servers: s4 = 5  CPU worker threads.
    - Service Time: Weibull distribution (α = 1.5,β = 6.0 ms).
    - Routing: 100% -> Node 7 (Database Ingestion).;
    """

    def __init__(
        self,
        num_servers: int = 5,
        service_shape: float = 1.5,
        service_scale: float = 6.0,
        rng: Optional[np.random.Generator] = None,
        routing_rng: Optional[np.random.Generator] = None,
    ):
        super().__init__(node_id=4, name="Node 4: Cluster A", num_servers=num_servers, rng=rng)
        self.service_shape = service_shape
        self.service_scale = service_scale
        self.routing_rng = routing_rng if routing_rng is not None else np.random.default_rng(seed=SEED+4)

    def sample_service_time(self) -> float:
        u = self.rng.uniform(0.0, 1.0)
        return float(self.service_scale * (-np.log(1.0 - u)) ** (1.0 / self.service_shape))

    def select_next_node(self, packet: Packet) -> int:
        return 7 

class Node5(BaseNode):
    """
    Node 5: Cluster B (G/G/s Queue)
    - Servers: s5 = 3 GPU execution units
    - Service Time: Erlang-k distribution (k = 3,µ = 8.0 ms).
    - Routing: 100% -> Node 7 (Database Ingestion).
    """

    def __init__(
        self,
        num_servers: int = 3,
        service_shape: float = 3,
        service_mean: float = 8.0,
        rng: Optional[np.random.Generator] = None,
        routing_rng: Optional[np.random.Generator] = None,
    ):
        super().__init__(node_id=5, name="Node 5: Cluster B", num_servers=num_servers, rng=rng)
        self.service_shape = service_shape
        self.service_mean = service_mean
        self.routing_rng = routing_rng if routing_rng is not None else np.random.default_rng(seed=SEED+5)

    def sample_service_time(self) -> float:
        temp_sum = 0.0
        for _ in range(self.service_shape):
            u = self.rng.uniform(0.0, 1.0)
            temp_sum += np.log(1.0 - u)
        return float(-(self.service_mean / self.service_shape) * temp_sum)

    def select_next_node(self, packet: Packet) -> int:
        return 7

class Node6(BaseNode):
    """
    Node 6:  Security Quarantine & Threat Analysis (M/G/1 Queue)
    - Servers:  s6 = 1 specialized threat-sandbox thread
    - Service Time: Gamma distribution (α = 2.0,β = 5.0 ms)
    - Routing: 100% -> Node 7 (Database Ingestion).
    """

    def __init__(
        self,
        num_servers: int = 1,
        service_shape: float = 2.0,
        service_scale: float = 5.0,
        rng: Optional[np.random.Generator] = None,
        routing_rng: Optional[np.random.Generator] = None,
    ):
        super().__init__(node_id=5, name="Node 5: Cluster B", num_servers=num_servers, rng=rng)
        self.service_shape = service_shape
        self.service_scale = service_scale
        self.routing_rng = routing_rng if routing_rng is not None else np.random.default_rng(seed=SEED+5)

    def sample_service_time(self) -> float:
        u1 = self.rng.uniform(0.0, 1.0)
        u2 = self.rng.uniform(0.0, 1.0)
        
        return float(-self.service_scale*(np.log(1-u1)+np.log(1-u2)))

    def select_next_node(self, packet: Packet) -> int:
        return 7

class Node7(BaseNode):
    """
    Node 7: Database Ingestion Layer (G/G/s Queue)
    - Servers:  s7 = 4 I/O write workers
    - Service Time:  Uniform distribution (a = 2.0 ms,b = 5.0 ms)
    - Routing: 100% -> Node 8 (Sink)
    """

    def __init__(
        self,
        num_servers: int = 3,
        a: float = 2.0,
        b: float = 5.0,
        rng: Optional[np.random.Generator] = None,
        routing_rng: Optional[np.random.Generator] = None,
    ):
        super().__init__(node_id=5, name="Node 5: Cluster B", num_servers=num_servers, rng=rng)
        self.a = a
        self.b = b
        self.routing_rng = routing_rng if routing_rng is not None else np.random.default_rng(seed=SEED+5)

    def sample_service_time(self) -> float:
        return float(self.a + (self.b-self.a)*self.rng.uniform(0.0, 1.0))

    def select_next_node(self, packet: Packet) -> int:
        return 8