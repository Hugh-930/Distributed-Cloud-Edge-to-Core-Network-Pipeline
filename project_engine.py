import heapq
from nodes import Packet
from nodes import Event
from nodes import Node1

nodes_dict = {
    1: Node1()
}
class SimulationEngine:
    def __init__(self, nodes_dict, sim_duration):
        self.nodes = nodes_dict
        self.sim_duration = sim_duration
        self.event_list = []
        self.sim_time = 0.0
        self.packet_id_counter = 0
        self.event_sequence_counter = 0

    def schedule_event(self, timestamp: float, event_type: str, packet: Packet):
            self.event_sequence_counter += 1
            ev = Event(
                timestamp=timestamp,
                sequence_id=self.event_sequence_counter,
                event_type=event_type,
                packet=packet,
            )
            heapq.heappush(self.event_list, ev)

    def run(self):
        first_arr = Node1.sample_arrival_time()
        self.packet_id_counter += 1
        init_pkt = Packet(self.packet_id_counter, 1, self.sim_time+first_arr)
        self.schedule_event(self.sim_time+first_arr, "ARRIVAL", init_pkt)

        while self.event_list:
            ev = heapq.heappop(self.event_list)
            
            if ev.timestamp > self.sim_duration:
                break
            
            self.sim_time = ev.timestamp

            if ev.event_type == "ARRIVAL":
                self.handle_arrival(ev.packet)
            elif ev.event_type == "DEPARTURE":
                self.handle_departure(ev.packet)

        print("Simulation completed. Total packets processed:", self.completed_packets)

def handle_arrival(self, packet: Packet):
    node_id = packet.current_node_id
    self.packet_id_counter += 1
    next_arr = self.sim_time + self.node_sample_arrival_time()
    next_pkt = Packet(self.packet_id_counter, node_id, self.sim_time + next_arr)
    self.schedule_event(next_arr, "ARRIVAL", next_pkt)

    if self.nodes[node_id].has_free_server():
        self.nodes[node_id].busy_servers += 1
        serv_time = self.nodes[node_id].sample_service_time()
        self.schedule_event(self.sim_time + serv_time, "DEPARTURE", packet)
    else:
        self.nodes[node_id].queue.append(packet)

def handle_departure(self, packet: Packet):
    node_id = packet.current_node_id
    self.nodes[node_id].completed_packets += 1

    latency = self.sim_time - packet.system_arrival_time
    self.nodes[node_id].latencies.append(latency)

    if self.nodes[node_id].queue:
        next_pkt = self.nodes[node_id].queue.pop(0)
        serv_time = self.nodes[node_id].sample_service_time()
        self.schedule_event(self.sim_time + serv_time, "DEPARTURE", next_pkt)
    else:
        self.nodes[node_id].busy_servers -= 1