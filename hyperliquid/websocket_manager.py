import json
import logging
import threading
from collections import defaultdict
import time
import websocket

from hyperliquid.utils.types import Any, Callable, Dict, List, NamedTuple, Optional, Subscription, Tuple, WsMsg

ActiveSubscription = NamedTuple("ActiveSubscription", [("callback", Callable[[Any], None]), ("subscription_id", int)])


def subscription_to_identifier(subscription: Subscription) -> str:
    if subscription["type"] == "allMids":
        return "allMids"
    elif subscription["type"] == "l2Book":
        return f'l2Book:{subscription["coin"].lower()}'
    elif subscription["type"] == "trades":
        return f'trades:{subscription["coin"].lower()}'
    elif subscription["type"] == "userEvents":
        return "userEvents"
    elif subscription["type"] == "userFills":
        return f'userFills:{subscription["user"].lower()}'
    elif subscription["type"] == "candle":
        return f'candle:{subscription["coin"].lower()},{subscription["interval"]}'
    elif subscription["type"] == "orderUpdates":
        return "orderUpdates"
    elif subscription["type"] == "userFundings":
        return f'userFundings:{subscription["user"].lower()}'
    elif subscription["type"] == "userNonFundingLedgerUpdates":
        return f'userNonFundingLedgerUpdates:{subscription["user"].lower()}'
    elif subscription["type"] == "webData2":
        return f'webData2:{subscription["user"].lower()}'


def ws_msg_to_identifier(ws_msg: WsMsg) -> Optional[str]:
    if ws_msg["channel"] == "pong":
        return "pong"
    elif ws_msg["channel"] == "allMids":
        return "allMids"
    elif ws_msg["channel"] == "l2Book":
        return f'l2Book:{ws_msg["data"]["coin"].lower()}'
    elif ws_msg["channel"] == "trades":
        trades = ws_msg["data"]
        if len(trades) == 0:
            return None
        else:
            return f'trades:{trades[0]["coin"].lower()}'
    elif ws_msg["channel"] == "user":
        return "userEvents"
    elif ws_msg["channel"] == "userFills":
        return f'userFills:{ws_msg["data"]["user"].lower()}'
    elif ws_msg["channel"] == "candle":
        return f'candle:{ws_msg["data"]["s"].lower()},{ws_msg["data"]["i"]}'
    elif ws_msg["channel"] == "orderUpdates":
        return "orderUpdates"
    elif ws_msg["channel"] == "userFundings":
        return f'userFundings:{ws_msg["data"]["user"].lower()}'
    elif ws_msg["channel"] == "userNonFundingLedgerUpdates":
        return f'userNonFundingLedgerUpdates:{ws_msg["data"]["user"].lower()}'
    elif ws_msg["channel"] == "webData2":
        return f'webData2:{ws_msg["data"]["user"].lower()}'


class WebsocketManager(threading.Thread):
    def __init__(self, base_url):
        super().__init__()
        self.subscription_id_counter = 0
        self.ws_ready = False
        self.queued_subscriptions: List[Tuple[Subscription, ActiveSubscription]] = []
        self.active_subscriptions: Dict[str, List[ActiveSubscription]] = defaultdict(list)
        ws_url = "ws" + base_url[len("http"):] + "/ws"
        self.ws = websocket.WebSocketApp(
            ws_url,
            on_message=self.on_message,
            on_open=self.on_open,
            on_error=self.on_error,  # 添加 on_error 回调
            on_close=self.on_close  # 添加 on_close 回调
        )
        self.ping_sender = threading.Thread(target=self.send_ping)
        self.stop_event = threading.Event()
        self.reconnect_attempts = 0
        self.max_reconnect_attempts = float('inf')  # 无限重连
        self.last_ping_time = time.time()

    def run(self):
        self.ping_sender.start()
        try:
            while not self.stop_event.is_set():
                self.ws.run_forever()
                if not self.stop_event.is_set():
                    self.reconnect()
        except KeyboardInterrupt:
            logging.info("KeyboardInterrupt received, shutting down WebSocket")
            self.stop()
        finally:
            if self.ping_sender.is_alive():
                self.ping_sender.join()
            logging.info("Websocket manager fully stopped")

    def send_ping(self):
        while not self.stop_event.wait(50):
            if not self.ws.keep_running:
                break
            current_time = time.time()
            if current_time - self.last_ping_time >= 50:  # 每 50 秒检查一次
                logging.debug("Websocket sending ping")
                self.ws.send(json.dumps({"method": "ping"}))
                self.last_ping_time = current_time
        logging.debug("Websocket ping sender stopped")

    def stop(self):
        self.stop_event.set()
        self.ws.close()
        if self.ping_sender.is_alive():
            self.ping_sender.join()

    def reconnect(self):
        self.reconnect_attempts += 1
        wait_time = min(2 ** min(self.reconnect_attempts, 10), 3600)  # 最大延迟 1 小时
        logging.warning(f"Attempting to reconnect ({self.reconnect_attempts}/∞) in {wait_time}s...")
        time.sleep(wait_time)
        self.ws.close()  # 关闭旧连接
        self.ws_ready = False
        # 保存所有活动订阅到队列
        for identifier, subs in self.active_subscriptions.items():
            for sub in subs:
                subscription = self.identifier_to_subscription(identifier)
                if subscription:
                    self.queued_subscriptions.append((subscription, sub))
        self.active_subscriptions.clear()  # 清空活动订阅，等待恢复
        self.ws = websocket.WebSocketApp(
            self.ws.url,
            on_message=self.on_message,
            on_open=self.on_open,
            on_error=self.on_error,
            on_close=self.on_close
        )
        logging.info("Reconnection initiated")

    def identifier_to_subscription(self, identifier: str) -> Optional[Subscription]:
        parts = identifier.split(":")
        if parts[0] == "allMids":
            return {"type": "allMids"}
        elif parts[0] in ["l2Book", "trades"]:
            return {"type": parts[0], "coin": parts[1].upper()}
        elif parts[0] == "userEvents":
            return {"type": "userEvents"}
        elif parts[0] == "orderUpdates":
            return {"type": "orderUpdates"}
        elif parts[0] in ["userFills", "userFundings", "userNonFundingLedgerUpdates", "webData2"]:
            return {"type": parts[0], "user": parts[1]}
        elif parts[0] == "candle":
            coin, interval = parts[1].split(",")
            return {"type": "candle", "coin": coin.upper(), "interval": interval}
        return None

    def on_message(self, _ws, message):
        if message == "Websocket connection established.":
            logging.debug(message)
            return
        logging.debug(f"on_message {message}")
        ws_msg: WsMsg = json.loads(message)
        identifier = ws_msg_to_identifier(ws_msg)
        if identifier == "pong":
            logging.debug("Websocket received pong")
            return
        if identifier is None:
            logging.debug("Websocket not handling empty message")
            return
        active_subscriptions = self.active_subscriptions[identifier]
        if len(active_subscriptions) == 0:
            print("Websocket message from an unexpected subscription:", message, identifier)
        else:
            for active_subscription in active_subscriptions:
                active_subscription.callback(ws_msg)

    def on_open(self, _ws):
        logging.debug("on_open")
        self.ws_ready = True
        self.reconnect_attempts = 0  # 重置重连计数
        self.last_ping_time = time.time()
        queued = self.queued_subscriptions.copy()
        self.queued_subscriptions.clear()
        for subscription, active_subscription in queued:
            self.subscribe(subscription, active_subscription.callback, active_subscription.subscription_id)
        logging.info("WebSocket connection established")

    def on_error(self, _ws, error):
        logging.error(f"WebSocket error: {error}")
        self.ws_ready = False
        # 可选：根据错误类型采取不同措施
        if isinstance(error, websocket.WebSocketTimeoutException):
            logging.warning("Timeout occurred, will attempt to reconnect")
        elif isinstance(error, ConnectionError):
            logging.warning("Connection error, will attempt to reconnect")

    def on_close(self, _ws, close_status_code, close_msg):
        logging.info(f"WebSocket closed with status {close_status_code}, reason: {close_msg}")
        self.ws_ready = False
        if not self.stop_event.is_set():
            logging.warning("Unexpected close, will attempt to reconnect")


    def subscribe(self, subscription: Subscription, callback: Callable[[Any], None], subscription_id: Optional[int] = None) -> int:
        if subscription_id is None:
            self.subscription_id_counter += 1
            subscription_id = self.subscription_id_counter
        if not self.ws_ready:
            logging.debug("enqueueing subscription")
            self.queued_subscriptions.append((subscription, ActiveSubscription(callback, subscription_id)))
        else:
            logging.debug("subscribing")
            identifier = subscription_to_identifier(subscription)
            if identifier == "userEvents" or identifier == "orderUpdates":
                # TODO: ideally the userEvent and orderUpdates messages would include the user so that we can multiplex
                if len(self.active_subscriptions[identifier]) != 0:
                    raise NotImplementedError(f"Cannot subscribe to {identifier} multiple times")
            self.active_subscriptions[identifier].append(ActiveSubscription(callback, subscription_id))
            self.ws.send(json.dumps({"method": "subscribe", "subscription": subscription}))
        return subscription_id

    def unsubscribe(self, subscription: Subscription, subscription_id: int) -> bool:
        if not self.ws_ready:
            raise NotImplementedError("Can't unsubscribe before websocket connected")
        identifier = subscription_to_identifier(subscription)
        active_subscriptions = self.active_subscriptions[identifier]
        new_active_subscriptions = [x for x in active_subscriptions if x.subscription_id != subscription_id]
        if len(new_active_subscriptions) == 0:
            self.ws.send(json.dumps({"method": "unsubscribe", "subscription": subscription}))
        self.active_subscriptions[identifier] = new_active_subscriptions
        return len(active_subscriptions) != len(new_active_subscriptions)

# 示例使用
if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    base_url = "https://api.hyperliquid-testnet.xyz"
    ws_manager = WebsocketManager(base_url)

    def callback(msg):
        print("Received:", msg)

    ws_manager.subscribe({"type": "l2Book", "coin": "BTC"}, callback)
    ws_manager.start()
    time.sleep(86400)  # 运行 一小时测试
    ws_manager.stop()