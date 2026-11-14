import heapq
from nodes import Packet, Event, Node1, Node2

nodes_dict = {
    1: Node1(),
    2: Node2(),
    # 3: Node3(),
    # 4: Node4(),
    # 5: Node5(),
    # 6: Node6(),
    # 7: Node7(),
    # 8: Node8(),
}

class SimulationEngine:
    def __init__(self, nodes_dict, sim_duration, max_packets):
        self.nodes = nodes_dict
        self.sim_duration = sim_duration if sim_duration is not None else float('inf')
        self.max_packets = max_packets if max_packets is not None else float('inf')
        self.event_list = []
        self.sim_time = 0.0
        self.packet_id_counter = 0
        self.event_sequence_counter = 0
        self.time_last_event = 0.0
        self.time_since_last_event = 0.0

    def schedule_event(self, timestamp: float, event_type: str, packet: Packet):
        """Schedules a new event in the simulation."""
        self.event_sequence_counter += 1
        ev = Event(
            timestamp=timestamp,
            sequence_id=self.event_sequence_counter,
            event_type=event_type,
            packet=packet,
        )
        # Use a heap to get the next event
        heapq.heappush(self.event_list, ev)

    def run(self):
        """Runs the simulation until the specified duration is reached."""
        first_arr = self.nodes[1].sample_arrival_time(self.sim_time)
        self.packet_id_counter += 1
        init_pkt = Packet(self.packet_id_counter, 1, first_arr, first_arr)
        self.schedule_event(first_arr, "ARRIVAL", init_pkt)

        while self.event_list:
            # Get the next event from the event list
            ev = heapq.heappop(self.event_list)
            
            if ev.timestamp > self.sim_duration:
                break

            self.sim_time = ev.timestamp
            self.update_stats()

            if ev.event_type == "ARRIVAL":
                self.handle_arrival(ev.packet)
            elif ev.event_type == "DEPARTURE":
                self.handle_departure(ev.packet)

        self.print_report()

    def handle_arrival(self, packet: Packet):
        """Handles an arrival event for a packet. 
        Schedules the next arrival and either starts service or queues the packet."""

        node_id = packet.current_node_id
        node = self.nodes[node_id]
        
        # schedule the next arrival only for Node 1
        if node_id == 1 and self.packet_id_counter < self.max_packets:
            self.packet_id_counter += 1
            next_arr = node.sample_arrival_time(self.sim_time)
            next_pkt = Packet(self.packet_id_counter, node_id, next_arr, next_arr)
            self.schedule_event(next_arr, "ARRIVAL", next_pkt)

        if node.has_free_server():
            node.busy_servers += 1
            serv_time = node.sample_service_time()
            self.schedule_event(self.sim_time + serv_time, "DEPARTURE", packet)
        else:
            node.queue.append(packet)

    def handle_departure(self, packet: Packet):
        node_id = packet.current_node_id
        node = self.nodes[node_id]
        node.completed_packets += 1

        latency = self.sim_time - packet.current_node_arrival_time
        node.latencies.append(latency)

        if node_id < 2:
            # Move the packet to the next node, update its info
            packet.current_node_arrival_time = self.sim_time
            packet.current_node_id += 1  # Move to the next node
            packet.current_node_arrival_time = self.sim_time
            self.schedule_event(self.sim_time, "ARRIVAL", packet)
        else:
            # Packet has completed processing through all nodes
            pass

        if node.queue:
            next_pkt = node.queue.pop(0)
            serv_time = node.sample_service_time()
            self.schedule_event(self.sim_time + serv_time, "DEPARTURE", next_pkt)
        else:
            node.busy_servers -= 1

    def update_stats(self):
        """Updates time-average statistics for the node based on the current simulation time."""

        self.time_since_last_event = self.sim_time - self.time_last_event
        self.time_last_event = self.sim_time

        if self.time_since_last_event <= 0:
            return  # No time has passed since the last event, so no need to update stats
        
        for node in self.nodes.values():
            # Update area under the number-in-queue variable
            num_in_queue = len(node.queue)
            node.area_num_in_queue += num_in_queue * self.time_since_last_event

            # Update area under the number-in-system variable
            num_in_system = node.num_in_system
            node.area_num_in_system += num_in_system * self.time_since_last_event

            # Update area under the busy servers variable
            node.area_busy_servers += node.busy_servers * self.time_since_last_event

    def print_report(self):
        """Prints a summary report of the simulation results."""
        print("\nSimulation Report:")
        for node_id, node in self.nodes.items():
            avg_queue_waiting_time = node.area_num_in_queue / node.completed_packets if node.completed_packets > 0 else 0.0 # W_q
            avg_time_in_system = node.area_num_in_system / node.completed_packets if node.completed_packets > 0 else 0.0 # W
            avg_num_in_queue = node.area_num_in_queue / self.sim_time # L_q
            avg_num_in_system = node.area_num_in_system / self.sim_time
            avg_busy_servers = node.area_busy_servers / self.sim_time
            server_utilization = avg_busy_servers / node.num_servers 

            print(f"\nNode {node_id} - {node.name}:")
            print(f"  Completed Packets: {node.completed_packets}")
            print(f"  Average Queue Waiting Time: {avg_queue_waiting_time:.4f}")
            print(f"  Average Time in System: {avg_time_in_system:.4f}")
            print(f"  Average Number in Queue: {avg_num_in_queue:.4f}")
            print(f"  Average Number in System: {avg_num_in_system:.4f}")
            print(f"  Average Active Servers: {avg_busy_servers:.4f}")
            print(f"  Server Utilization: {server_utilization:.4f}")

if __name__ == "__main__":
    sim_duration = 10000.0  # Simulation duration in milliseconds
    max_packets = 200  # Maximum number of packets to process
    sim_engine = SimulationEngine(nodes_dict, sim_duration = sim_duration, max_packets = max_packets)
    sim_engine.run()