import os
import threading
import json
import queue
import requests
import base64
import signal
import copy

from collections import namedtuple
from abc import ABC, abstractmethod
from multiprocessing import Process, Queue, current_process
from queue import Empty
from time import sleep, time

import paho.mqtt.client as mqtt


Tester = namedtuple('Tester', ['name', 'p', 'q'])


class TestScenario:
    def __init__(self, name, quantity, duration, provisioner_class, sender_class, receiver_class, provisioner_options, sender_options, receiver_options):
        self._name = name
        self._quantity = quantity
        self._duration = duration
        self._provisioner_class = provisioner_class
        self._sender_class = sender_class
        self._receiver_class = receiver_class
        self._provisioner_options = provisioner_options
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
    def provisioner_class(self):
        return self._provisioner_class

    @property
    def sender_class(self):
        return self._sender_class

    @property
    def receiver_class(self):
        return self._receiver_class

    @property
    def provisioner_options(self):
        return self._provisioner_options

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

    def perform_t(self):
        testers = []
        max_duration = 0
        for scenario in self._scenarios:
            max_duration = max(max_duration, scenario.duration)
            for ix in range(scenario.quantity):
                command_queue = queue.Queue()
                name = "%s-%s" % (scenario.name,  ix)
                testers.append(Tester(
                    name = name,
                    p = threading.Thread(target=process,
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
            if self._stop_earlier_flag:
                break
            print("Tester %s started" % tester.name)
            tester.p.start()

        print("Test scenarios estimated run time:", max_duration, "seconds")

        start_ts = time()
        while(start_ts + max_duration > time() and not self._stop_earlier_flag):
            sleep(1)

        for tester in testers:
            tester.q.put("stop")

        for tester in testers:
            if tester.p.is_alive():
                tester.p.join()

        print("===== duration: %.1f" % round(time() - start_ts))
        for tester in testers:
            try:
                print(tester.q.get(block = False))
            except Empty:
                print(tester.name, "No report from tester!")

    def perform_p(self):
        testers = []
        max_duration = 0
        pid = os.getpid()
        for scenario in self._scenarios:
            max_duration = max(max_duration, scenario.duration)
            for ix in range(scenario.quantity):
                command_queue = Queue()
                name = "%s-%d-%d" % (scenario.name, pid, ix)
                testers.append(Tester(
                    name = name,
                    p = Process(target=process,
                            args=(name,
                                command_queue,
                                scenario.provisioner_class,
                                scenario.sender_class,
                                scenario.receiver_class,
                                scenario.provisioner_options,
                                scenario.sender_options,
                                scenario.receiver_options
                            )
                    ),
                    q = command_queue
                ))

        for tester in testers:
            if self._stop_earlier_flag:
                break
            print("Tester %s started" % tester.name)
            tester.p.start()
            sleep(0.01)

        print("Test scenarios estimated run time:", max_duration, "seconds")

        start_ts = time()
        while(start_ts + max_duration > time() and not self._stop_earlier_flag):
            sleep(1)

        for tester in testers:
            tester.q.put("stop")

        for tester in testers:
            if tester.p.is_alive():
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
    def __init__(self, name, provisioner_class, sender_class, receiver_class, provisioner_options, sender_options, receiver_options):
        self._name = name
        self._provisioner = provisioner_class(name, **self.prepare_options(provisioner_options))
        self._sender = sender_class(name, **self.prepare_options(sender_options))
        self._receiver = receiver_class(name, **self.prepare_options(receiver_options))
        self._lock = threading.Lock()
        self._database_unacknowledged = set()
        self._database_orphaned = set()

    @staticmethod
    def substitute(template, env):
        if isinstance(template, str):
            return template.format(**env)
        if isinstance(template, dict):
            acceptor = dict()
            for key in template:
                acceptor[key] = SRTester.substitute(template[key], env)
        elif isinstance(template, list):
            acceptor = list()
            for ix in range(len(template)):
                acceptor.append(SRTester.substitute(template[ix], env))
        else:
            acceptor = copy.copy(template)
        return acceptor

    def prepare_options(self, options_template):
        return SRTester.substitute(options_template, {"name": self._name})

    def run(self, bi_queue):
        s_thread = threading.Thread(target=self.send)
        r_thread = threading.Thread(target=self.receive)

        self._provisioner.provide()

        self._active = True
        r_thread.start()
        sleep(1)  # Make sure receiving thread is running when sending thread starts to send
        s_thread.start()
        start_ts = time()

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

        duration = time() - start_ts
        report = "%s: run time: %d sec; sent messages: %d; received messages: %d; lost messages: %d%%; rate: %0.2f msg/sec" % (
            self._sender.name,
            duration,
            self._sender.count,
            self._receiver.count,
            round((len(self._database_unacknowledged) / self._sender.count) * 100),
            (self._sender.count - len(self._database_unacknowledged)) / duration
        )
        bi_queue.put(report)

    def send(self):
        while self._active:
            try:
                msg_id = self._sender.send()
            except Exception as e:
                print(e)
                sleep(1)
            else:
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
        if self._pause > 0:
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


class Provisioner:
    def __init__(self, tester_name, channel, pipeline, pipeline_url, channel_url, pipeline_start_url):
        self._channel = channel
        self._pipeline = pipeline
        self._pipeline_url = pipeline_url
        self._channel_url = channel_url
        self._pipeline_start_url = pipeline_start_url
        self._name = "%s-%s" % (tester_name, self.__class__.__name__)

    def provide(self):
        import urllib3
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        headers = {
            "Authorization": "Bearer RtPwI7APdmhmkoFoOGxGIyUq65RGftBrIXnrD96ouc0CXFKBvW"
        }
        r = requests.post(self._channel_url, json = self._channel, verify = False, headers = headers)
        r.raise_for_status()
        r = requests.post(self._pipeline_url, json = self._pipeline, verify = False, headers = headers)
        r.raise_for_status()
        r = requests.put(self._pipeline_start_url, verify = False, headers = headers)
        r.raise_for_status()
        sleep(2)


def process(name, command_queue, provisioner_class, sender_class, receiver_class, provisioner_options, sender_options, receiver_options):
    tester = SRTester(name, provisioner_class, sender_class, receiver_class, provisioner_options, sender_options, receiver_options)
    tester.run(command_queue)


def main():
    tester = TestManager()
    # server = "gnode-2.local"
    # server = "127.0.0.1"
    # server = "mqtt.iotplan.io"
    server = "192.168.1.100"
    # server = "94.16.123.241"
    # server = "192.168.0.71"

    mqtt_sender_options = dict(
        server = "%s" % server,
        topic = "/{name}/test/topic",
        qos = 0,
        data_length = 1024 * 1024 * 1,
        pause = 0
    )
    http_sender_options = dict(
        server = "http://%s" % server,
        path = "/channel/http/{name}",
        method = HTTPSender.POST,
        data_length = 1,
        pause = 0.01
    )
    mqtt_receiver_options = dict(
        server = "%s" % server,
        topic = "/{name}/test/topic",
        qos = 0
    )
    http_receiver_options = dict(
        server = "%s" % server,
        topic = "queue{name}",
        qos = 0
    )

    http_sender_options2 = dict(
        server = "http://%s" % server,
        path = "/channel/http/http_channel_2",
        method = HTTPSender.POST,
        data_length = 100,
        pause = 0
    )

    provisioner_options = dict(
        channel = {
            "type": "http",
            "authtype": "none",
            "enabled": True,
            "queue_name": "queue{name}"
        },
        pipeline = {
            "connector_in": {
                "type": "queue",
                "name": "queue{name}"

            },
            "connector_out": {
                "type": "mqtt",
                "topic": "queue{name}"
            }
        },
        pipeline_url = "https://%s/api/pipeline/config/{name}" % server,
        channel_url = "https://%s/api/channel/{name}" % server,
        pipeline_start_url = "https://%s/api/pipeline/control/start/{name}" % server,
    )

    DURATION = 20
    #tester.add(TestScenario("S1", 100, DURATION, MQTTSender, MQTTReceiver, mqtt_sender_options, mqtt_receiver_options))
    tester.add(TestScenario("S2", 500, DURATION, Provisioner, HTTPSender, MQTTReceiver, provisioner_options, http_sender_options, http_receiver_options))
    #tester.add(TestScenario("S3", 1, DURATION, HTTPSender, MQTTReceiver, http_sender_options2, http_receiver_options))

    tester.perform_p()

    #tester.perform(10, 2, HTTPSender, MQTTReceiver, http_sender_options, receiver_options)


if __name__ == "__main__":
    main()