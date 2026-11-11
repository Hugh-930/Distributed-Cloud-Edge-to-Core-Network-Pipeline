import heapq

class SimulationEngine:
    def __init__(self, nodes, warmup_time=5000.0, sim_duration=48000.0):
        self.nodes = nodes              # {1: Node1, 2: Node2, ..., 8: Node8} の辞書
        self.sim_time = 0.0             # 現在時刻 (ms)
        self.time_last_event = 0.0      # 直前のイベント時刻 (ms)
        self.warmup_time = warmup_time  # ウォームアップ期間 (ms)
        self.end_time = warmup_time + sim_duration # 終了時刻 (ms)
        self.warmup_done = False
        self.event_list = []            # heapqで管理するイベントリスト

    def schedule_event(self, event_time, event_type, node_id, packet):
        """イベントリストに予定を追加する"""
        heapq.heappush(self.event_list, (event_time, event_type, node_id, packet))

    def run(self):
        """課題の main() にあたるループ"""
        # 最初のパケット到着を予約
        self.schedule_first_arrival()

        while self.event_list:
            # 1. 課題の timing(): 一番早いイベントを取り出して時計を進める
            event_time, event_type, node_id, packet = heapq.heappop(self.event_list)
            if event_time > self.end_time:
                break
            self.sim_time = event_time

            # ウォームアップ終了時刻を過ぎたら統計カウンタをリセット
            if not self.warmup_done and self.sim_time >= self.warmup_time:
                for node in self.nodes.values():
                    node.reset_stats()
                self.warmup_done = True

            # 2. 課題の update_time_avg_stats(): 全ノードの面積統計を更新
            dt = self.sim_time - self.time_last_event
            self.time_last_event = self.sim_time
            for node in self.nodes.values():
                node.update_stats(dt)

            # 3. イベントに応じた処理を実行
            if event_type == "ARRIVAL":
                self.handle_arrival(node_id, packet)
            elif event_type == "DEPARTURE":
                self.handle_departure(node_id, packet)