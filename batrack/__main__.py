import argparse
import configparser
import copy
import csv
import datetime
import json
import inspect
import logging
import os
import signal
import socket
import sys
import threading
import time
from distutils.util import strtobool
from typing import List, Union, Literal, Dict
import paho.mqtt.client as mqtt

import schedule

from batrack.sensors import AbstractAnalysisUnit
from batrack.audio import AudioAnalysisUnit
from batrack.vhf import VHFAnalysisUnit
from batrack.video import CameraAnalysisUnit

logger = logging.getLogger(__name__)


class BatRack(threading.Thread):
    def __init__(
        self,
        config,
        name: str = "default",
        data_path: str = "data",
        duty_cycle_s: int = 10,
        use_vhf: Union[bool, Literal[0, 1], str] = True,
        use_audio: Union[bool, Literal[0, 1], str] = True,
        use_camera: Union[bool, Literal[0, 1], str] = True,
        use_timed_camera: Union[bool, Literal[0, 1], str] = True,
        use_trigger_vhf: Union[bool, Literal[0, 1], str] = True,
        use_trigger_audio: Union[bool, Literal[0, 1], str] = True,
        use_trigger_camera: Union[bool, Literal[0, 1], str] = True,
        always_on: Union[bool, Literal[0, 1], str] = False,
        mqtt_host: str = "localhost",
        mqtt_port: int = 1883,
        mqtt_keepalive: int = 60,
        **kwargs,
    ):
        super().__init__()
        self.name: str = str(name)

        # add hostname and  data path
        self.data_path: str = os.path.join(data_path, socket.gethostname(), "batrack")
        os.makedirs(self.data_path, exist_ok=True)
        logger.debug("Data path: %s", self.data_path)
        start_time_str = datetime.datetime.now().strftime("%Y-%m-%dT%H_%M_%S")
        self.csvfile = open(os.path.join(self.data_path, f"{socket.gethostname()}_{start_time_str}_{self.name}.csv"), "w", encoding="utf-8")
        self.csv = csv.writer(self.csvfile)

        # create instance variables
        self.duty_cycle_s: int = int(duty_cycle_s)
        self._units: List[AbstractAnalysisUnit] = []

        # convert boolean config variables
        use_vhf = strtobool(use_vhf) if isinstance(use_vhf, str) else bool(use_vhf)
        use_audio = strtobool(use_audio) if isinstance(use_audio, str) else bool(use_audio)
        use_camera = strtobool(use_camera) if isinstance(use_camera, str) else bool(use_camera)
        use_timed_camera = strtobool(use_timed_camera) if isinstance(use_timed_camera, str) else bool(use_timed_camera)

        use_trigger_vhf = strtobool(use_trigger_vhf) if isinstance(use_trigger_vhf, str) else bool(use_trigger_vhf)
        use_trigger_audio = strtobool(use_trigger_audio) if isinstance(use_trigger_audio, str) else bool(use_trigger_audio)
        use_trigger_camera = strtobool(use_trigger_camera) if isinstance(use_trigger_camera, str) else bool(use_trigger_camera)

        self.always_on: bool = bool(strtobool(always_on)) if isinstance(always_on, str) else bool(always_on)

        self.mqtt_host = str(mqtt_host)
        self.mqtt_port = int(mqtt_port)
        self.mqtt_keepalive = int(mqtt_keepalive)
        self.mqtt_client = mqtt.Client(client_id=f"{socket.gethostname()}-batrack", clean_session=False, userdata=self)
        self.mqtt_client.connect(self.mqtt_host, port=self.mqtt_port)
        self.mqtt_client.loop_start()
        self.topic_prefix = f"{socket.gethostname()}/batrack"

        # setup vhf
        self.vhf: VHFAnalysisUnit
        if use_vhf:
            self.vhf = VHFAnalysisUnit(
                **config["VHFAnalysisUnit"],
                use_trigger=use_trigger_vhf,
                trigger_callback=self.evaluate_triggers,
                data_path=self.data_path,
            )
            self._units.append(self.vhf)

        # setup audio
        self.audio: AudioAnalysisUnit
        if use_audio:
            self.audio = AudioAnalysisUnit(
                **config["AudioAnalysisUnit"],
                use_trigger=use_trigger_audio,
                trigger_callback=self.evaluate_triggers,
                data_path=self.data_path,
            )
            self._units.append(self.audio)

        # setup camera
        self.camera: CameraAnalysisUnit
        if use_camera:
            self.camera = CameraAnalysisUnit(
                **config["CameraAnalysisUnit"],
                use_trigger=use_trigger_camera,
                trigger_callback=self.evaluate_triggers,
                data_path=self.data_path,
            )
            self._units.append(self.camera)

        self._running: bool = False
        self._trigger: bool = False

    def evaluate_triggers(self, callback_trigger: bool, message: Dict) -> bool:
        calling_class = inspect.stack()[1][0].f_locals["self"].__class__.__name__
        msg_str = json.dumps(message)

        # publish trigger event
        mqtt_topic = f"{self.topic_prefix}/{calling_class}/{callback_trigger}"
        logger.debug("mqtt publish %s: %s", mqtt_topic, msg_str)
        self.mqtt_client.publish(mqtt_topic, msg_str)

        # write trigger event in csv
        self.csv.writerow([datetime.datetime.now(), calling_class, callback_trigger, msg_str])
        self.csvfile.flush()

        # if always on OR any of the used triggers fires, the system trigger is set
        trigger = self.always_on or any([unit.use_trigger and unit.trigger for unit in self._units])
        logger.debug("trigger evaluation, current state: %s", trigger)

        # start / stop recordings if the system trigger changed
        if trigger != self._trigger:
            self._trigger = trigger
            if trigger:
                logger.info("System triggered, starting recordings")
                [unit.start_recording() for unit in self._units]
            else:
                logger.info("System un-triggered, stopping recordings")
                [unit.stop_recording() for unit in self._units]

        return trigger

    def run(self):
        self._running = True

        # start units
        [unit.start() for unit in self._units if unit]

        # do an initial trigger evaluation, also starts recordings when no trigger is used at all
        self.evaluate_triggers(False, {})

        # print status reports
        while self._running:
            for unit in self._units:
                status_str = ", ".join([f"{k}: {'1' if v else '0'}" for k, v in unit.get_status().items()])
                logger.info("%s: %s", unit.__class__.__name__, status_str)
                if unit._running and not unit.is_alive():
                    logger.warning("%s is not active, but should run; self-terminating", unit.__class__.__name__)
                    os.kill(os.getpid(), signal.SIGINT)

            time.sleep(self.duty_cycle_s)

        self.mqtt_client.disconnect()

        logger.info("BatRack [%s] finished", self.name)

    def stop(self):
        """
        stops all streams, clean up state and set the gpio to low
        :return:
        """
        logger.info("Stopping [%s] and respective sensor instances", self.name)
        self._running = False

        [unit.stop() for unit in self._units]
        logger.info("Finished cleaning [%s] sensors", self.name)

        self.join()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Evaluate sensors for active bats and trigger recordings.")
    parser.add_argument(
        "configfile",
        nargs="?",
        default="etc/BatRack.conf",
    )

    args = parser.parse_args()

    # read config file
    config = configparser.ConfigParser()
    config.read(args.configfile)

    # configure logging
    logging_level = config["BatRack"].get("logging_level", "INFO")
    logging.basicConfig(level=logging_level)
    logger.debug("logging level set to %s", logging_level)

    lock = threading.Lock()
    instance = None

    def create_and_run(config, k, run_config):
        logger.info("[%s] waiting for remaining instance", k)
        lock.acquire()

        logger.info("[%s] creating instance", k)
        global instance
        instance = BatRack(config, name=k, **run_config)
        instance.start()
        logger.info("[%s] started", k)

    def stop_and_remove():
        global instance
        if instance:
            instance.stop()
            instance = None
            lock.release()

    config_has_runs = 0
    now = datetime.datetime.now()

    # iterate through runs an enter schedulings
    for k, v in config.items():
        if not k.startswith("run"):
            continue

        run_config = copy.deepcopy(config["BatRack"])
        run_config.update(config[k])

        try:
            start_s = schedule.every().day.at(run_config["start"])
            stop_s = schedule.every().day.at(run_config["stop"])

            logger.info("[%s] running from %s to %s", k, run_config['start'], run_config['stop'])

            start_s.do(create_and_run, config, k, run_config)
            stop_s.do(stop_and_remove)

            if now.time() > start_s.at_time:
                if now.time() < stop_s.at_time:
                    logger.info("[%s] starting run now (in interval)", k)
                    create_and_run(config, k, run_config)

            config_has_runs += 1

        except KeyError as e:
            logger.error("[%s] is missing a %s time, please check the configuration file (%s).", k, e, args.configfile)
            sys.exit(1)
        except schedule.ScheduleValueError as e:
            logger.error("[%s] %s, please check the configuration file (%s).", k, e, args.configfile)
            sys.exit(1)

    running = True

    # create a signal handler to terminate cleanly
    def signal_handler(signal_value=None, frame=None):
        sig = signal.Signals(signal_value)
        global running
        if running:
            logger.info("Caught %s, terminating execution...", sig.name)
            running = False

            stop_and_remove()

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    # start the run scheduling or run continuously
    if config_has_runs:
        logger.info("starting run scheduling")
        while running:
            schedule.run_pending()
            time.sleep(1)
    else:
        logger.info("No valid runs have been defined, running continuously.")
        create_and_run(config, "continuous", config["BatRack"])
