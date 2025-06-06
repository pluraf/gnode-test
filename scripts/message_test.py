import threading
import json
import queue
import requests
import base64
import signal

from collections import namedtuple
from abc import ABC, abstractmethod
from multiprocessing import Process, Queue, current_process
from queue import Empty
from time import sleep, time

import paho.mqtt.client as mqtt


Tester = namedtuple('Tester', ['name', 'p', 'q'])


class TestScenario:
    def __init__(self, name, quantity, duration, sender_class, receiver_class, sender_options, receiver_options):
        self._name = name
        self._quantity = quantity
        self._duration = duration
        self._sender_class = sender_class
        self._receiver_class = receiver_class
        self._sender_options = sender_options
        self._receiver_options = receiver_options

    @property
    def name(self):
        return self._name

    @property
    def quantity(self):
        return self._quantity

    @property
    def duration(self):
        return self._duration

    @property
    def sender_class(self):
        return self._sender_class

    @property
    def receiver_class(self):
        return self._receiver_class

    @property
    def sender_options(self):
        return self._sender_options

    @property
    def receiver_options(self):
        return self._receiver_options


class TestManager:
    def __init__(self):
        self._scenarios = []
        self._stop_earlier_flag = False
        signal.signal(signal.SIGINT, self.stop)

    def add(self, scenario):
        self._scenarios.append(scenario)

    def perform(self):
        testers = []
        max_duration = 0
        for scenario in self._scenarios:
            max_duration = max(max_duration, scenario.duration)
            for ix in range(scenario.quantity):
                command_queue = Queue()
                name = "%s-%s" % (scenario.name, ix)
                testers.append(Tester(
                    name = name,
                    p = Process(target=process,
                            args=(name,
                                command_queue,
                                scenario.sender_class,
                                scenario.receiver_class,
                                scenario.sender_options,
                                scenario.receiver_options
                            )
                    ),
                    q = command_queue
                ))

        for tester in testers:
            tester.p.start()

        print("Test scenarios run for", max_duration, "seconds")

        start_ts = time()
        while(start_ts + max_duration > time() and not self._stop_earlier_flag):
            sleep(1)

        for tester in testers:
            tester.q.put("stop")

        for tester in testers:
            tester.p.join()

        print("===== duration: %.1f" % round(time() - start_ts))
        for tester in testers:
            try:
                print(tester.q.get(block = False))
            except Empty:
                print(tester.name, "No report from tester!")

    def stop(self, *args):
        if current_process().name == "MainProcess":
            print("\nStopping earlier...")
            self._stop_earlier_flag = True


class SRTester:
    def __init__(self, name, sender_class, receiver_class, sender_options, receiver_options):
        self._name = name
        sender = sender_class(name, **self.prepare_options(sender_options))
        receiver = receiver_class(name, **self.prepare_options(receiver_options))
        self._sender = sender
        self._receiver = receiver
        self._lock = threading.Lock()
        self._database_unacknowledged = set()
        self._database_orphaned = set()

    def prepare_options(self, options_template):
        options = {}
        for opt_name in options_template:
            opt_value = options_template[opt_name]
            if isinstance(opt_value, str):
                options[opt_name] = opt_value.format(name = self._name)
            else:
                options[opt_name] = opt_value
        return options

    def run(self, bi_queue):
        s_thread = threading.Thread(target=self.send)
        r_thread = threading.Thread(target=self.receive)

        self._active = True
        r_thread.start()
        sleep(1)  # Make sure receiving thread is running when sending thread starts to send
        s_thread.start()

        while True:
            try:
                cmd = bi_queue.get(block=False)
                if cmd == "stop":
                    self._active = False
                    break
            except Empty:
                sleep(1)
        s_thread.join()
        r_thread.join()

        self._sender.stop()
        self._receiver.stop()

        self._database_unacknowledged.difference_update(self._database_orphaned)

        report = "%s: sent messages: %d; received messages: %d; lost messages: %d%%" % (
            self._sender.name,
            self._sender.count,
            self._receiver.count,
            round((len(self._database_unacknowledged) / self._sender.count) * 100)
        )
        bi_queue.put(report)

    def send(self):
        while self._active:
            msg_id = self._sender.send()
            with self._lock:
                self._database_unacknowledged.add(msg_id)
            self._sender.sleep()

    def receive(self):
        while True:
            try:
                mqtt_msg = self._receiver.receive(1)
                msg = json.loads(mqtt_msg.payload)
                msg_id = msg["msg_id"]
                with self._lock:
                    try:
                        self._database_unacknowledged.remove(msg_id)
                    except KeyError:
                        self._database_orphaned.add(msg_id)
            except Empty:
                if not self._active:
                    break


class MessageSender(ABC):
    def __init__(self, tester_name, server, pause):
        self._name = "%s-%s" % (tester_name, self.__class__.__name__)
        self._server = server
        self._pause = pause
        self._msg_counter = 0

    @property
    def name(self):
        return self._name

    @property
    def count(self):
        return self._msg_counter

    @abstractmethod
    def send(self): pass

    def sleep(self):
        sleep(self._pause)

    def stop(self):
        pass


class MessageReceiver(ABC):
    def __init__(self, tester_name, server):
        self._name = "%s-%s" % (tester_name, self.__class__.__name__)
        self._server = server
        self._msg_counter = 0

    @property
    def name(self):
        return self._name

    @property
    def count(self):
        return self._msg_counter

    @abstractmethod
    def receive(self): pass

    def stop(self):
        pass


class MQTTSender(MessageSender):
    def __init__(self, tester_name, server, pause, topic, qos, data_length):
        super().__init__(tester_name, server, pause)
        self._qos = qos
        self._topic = topic
        self._pause = pause
        self._data_length = data_length
        self._mqttc = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
        self._mqttc.connect(server)
        self._mqttc.loop_start()

    def send(self):
        self._msg_counter += 1
        payload = {
            "msg_id": "{}-{}".format(self._name, self._msg_counter)
        }
        if self._data_length > 0:
            payload["data"] = "A" * self._data_length
        msg_info = self._mqttc.publish(self._topic, json.dumps(payload), qos=self._qos)
        msg_info.wait_for_publish()
        return payload["msg_id"]

    def stop(self):
        self._mqttc.loop_stop()
        self._mqttc.disconnect()


class MQTTReceiver(MessageReceiver):
    def __init__(self, tester_name, server, topic, qos):
        super().__init__(tester_name, server)
        self._qos = qos
        self._topic = topic
        self._mgs_queue = queue.Queue()
        self._mqttc = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
        self._mqttc.user_data_set(self)
        self._mqttc.on_connect = self._on_connect
        self._mqttc.on_message = self._on_message
        self._mqttc.on_subscribe = self._on_subscribe
        self._mqttc.on_unsubscribe = self._on_unsubscribe
        self._mqttc.connect(server)
        self._mqttc.loop_start()

    @staticmethod
    def _on_subscribe(client, self, mid, reason_code_list, properties):
        if reason_code_list[0].is_failure:
            print(f"Broker rejected you subscription: {reason_code_list[0]}")

    @staticmethod
    def _on_unsubscribe(client, self, mid, reason_code_list, properties):
        if len(reason_code_list) == 0 or not reason_code_list[0].is_failure:
            print("unsubscribe succeeded (if SUBACK is received in MQTTv3 it success)")
        else:
            print(f"Broker replied with failure: {reason_code_list[0]}")
        client.disconnect()

    @staticmethod
    def _on_message(client, self, message):
        self._msg_counter += 1
        self._mgs_queue.put(message)

    @staticmethod
    def _on_connect(client, self, flags, reason_code, properties):
        if reason_code.is_failure:
            print(f"Failed to connect: {reason_code}. loop_forever() will retry connection")
        else:
            client.subscribe(self._topic, qos = self._qos)

    def receive(self, timeout = None):
        return self._mgs_queue.get(timeout = timeout)

    def stop(self):
        self._mqttc.loop_stop()
        self._mqttc.disconnect()


class HTTPSender(MessageSender):
    GET = "GET"
    POST = "POST"

    def __init__(self, ix, server, pause, path, method, data_length):
        super().__init__(ix, server, pause)
        self._url = "%s/%s" % (server, path)
        self._method = method
        self._data_length = data_length

    def send(self):
        self._msg_counter += 1
        payload = {
            "msg_id": "{}-{}".format(self._name, self._msg_counter)
        }
        if self._data_length > 0:
            payload["data"] = "A" * self._data_length

        if self._method is HTTPSender.GET:
            data = base64.urlsafe_b64encode(json.dumps(payload).encode()).rstrip(b"=")
            resp = requests.get(self._url, (("data", data),))
        elif self._method is HTTPSender.POST:
            resp = requests.post(self._url, json = payload)

        if resp.status_code != 200:
            print(f"Failure: {resp.status_code}")

        return payload["msg_id"]


def process(name, command_queue, sender_class, receiver_class, sender_options, receiver_options):
    tester = SRTester(name, sender_class, receiver_class, sender_options, receiver_options)
    tester.run(command_queue)


def main():
    tester = TestManager()
    server = "gnode-2.local"
    # server = "127.0.0.1"

    mqtt_sender_options = dict(
        server = "%s" % server,
        topic = "/{name}/test/topic",
        qos = 0,
        data_length = 100,
        pause = 0
    )
    http_sender_options = dict(
        server = "http://%s" % server,
        path = "/channel/http/http_channel",
        method = HTTPSender.POST,
        data_length = 100,
        pause = 0
    )
    mqtt_receiver_options = dict(
        server = "%s" % server,
        topic = "/{name}/test/topic",
        qos = 0
    )
    http_receiver_options = dict(
        server = "%s" % server,
        topic = "/from_http",
        qos = 0
    )

    http_sender_options2 = dict(
        server = "http://%s" % server,
        path = "/channel/http/http_channel_2",
        method = HTTPSender.POST,
        data_length = 100,
        pause = 0
    )

    DURATION = 10
    tester.add(TestScenario("S1", 1, DURATION, MQTTSender, MQTTReceiver, mqtt_sender_options, mqtt_receiver_options))
    tester.add(TestScenario("S2", 1, DURATION, HTTPSender, MQTTReceiver, http_sender_options, http_receiver_options))
    tester.add(TestScenario("S3", 1, DURATION, HTTPSender, MQTTReceiver, http_sender_options2, http_receiver_options))

    tester.perform()

    #tester.perform(10, 2, HTTPSender, MQTTReceiver, http_sender_options, receiver_options)


if __name__ == "__main__":
    main()