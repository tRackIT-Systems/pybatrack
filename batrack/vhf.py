import datetime
import time
import json
import numpy as np
import logging
import socket
import cbor2 as cbor
from typing import List, Tuple, Dict, Optional

import paho.mqtt.client as mqtt
from radiotracking import MatchedSignal
from radiotracking.consume import uncborify

from batrack.sensors import AbstractAnalysisUnit

logger = logging.getLogger(__name__)


class VHFAnalysisUnit(AbstractAnalysisUnit):
    def __init__(
        self,
        freq_bw_hz: int,
        sig_freqs_mhz: List[float],
        sig_threshold_dbw: float,
        sig_duration_threshold_s: float,
        freq_active_window_s: float,
        freq_active_var: float,
        freq_active_count: int,
        untrigger_duration_s: float,
        mqtt_host: str = "localhost",
        mqtt_port: int = 1883,
        mqtt_keepalive: int = 60,
        **kwargs,
    ):
        """[summary]

        Args:
            freq_bw_hz (int): bandwidth used by a sender, required to match received signals to defined frequencies
            sig_freqs_mhz (List[float]): list of frequencies to monitor
            sig_threshold_dbw (float): power threshold for received signals
            sig_duration_threshold_s (float): duration threshold for received signals
            freq_active_window_s (float): duration of window used for active / passive freq classification
            freq_active_var (float): threshold, after which a frequency is classified active
            freq_active_count (int): required number of signals in a frequencyy for classifaciton
            untrigger_duration_s (float): duration for which a trigger will stay active

        Raises:
            ValueError: format of an argument is not valid.
        """
        super().__init__(**kwargs)

        # base system values
        self.freq_bw_hz: int = int(freq_bw_hz)

        # signal-specific configuration and thresholds
        if isinstance(sig_freqs_mhz, list):
            sig_freqs_mhz = [float(f) for f in sig_freqs_mhz]
        elif isinstance(sig_freqs_mhz, str):
            sig_freqs_mhz = [float(f) for f in json.loads(sig_freqs_mhz)]
        else:
            raise ValueError(f"invalid format for frequencies, {type(sig_freqs_mhz)}:'{sig_freqs_mhz}'")

        # freqs_bins to contain old signal values for variance calc
        self._freqs_bins: Dict[float, Tuple[float, float, List[Tuple[datetime.datetime, float]]]] = {}
        for freq_mhz in sig_freqs_mhz:
            freq_rel = int(freq_mhz * 1000 * 1000)
            lower = freq_rel - (self.freq_bw_hz / 2)
            upper = freq_rel + (self.freq_bw_hz / 2)

            self._freqs_bins[freq_mhz] = (lower, upper, [])

        self.sig_threshold_dbw = float(sig_threshold_dbw)
        # TODO: Signal duration threshold is not yet used
        self.sig_duration_threshold_s = float(sig_duration_threshold_s)

        self.freq_active_window_s = float(freq_active_window_s)
        self.freq_active_var = float(freq_active_var)
        self.freq_active_count = int(freq_active_count)

        self.untrigger_duration_s = float(untrigger_duration_s)

        # create client object and set callback methods
        self.mqtt_host = str(mqtt_host)
        self.mqtt_port = int(mqtt_port)
        self.mqtt_keepalive = int(mqtt_keepalive)
        self.mqttc = mqtt.Client(client_id=f"{socket.gethostname()}-batrack-client", clean_session=False, userdata=self)

        self.untrigger_ts = time.time()

    def start_recording(self):
        # the vhf sensor is recording continuously
        pass

    def stop_recording(self):
        # the vhf sensor is recording continuously
        pass

    @staticmethod
    def on_matched_cbor(client: mqtt.Client, self, message):
        # extract payload and meta data
        matched_list = cbor.loads(message.payload, tag_hook=uncborify)
        station, _, _, _ = message.topic.split("/")

        msig = MatchedSignal(["0"], *matched_list)
        logger.debug("Received %s", msig)

        # helper method to retrieve the signal list
        def get_freqs_list(freq: int) -> Tuple[Optional[float], List[Tuple[datetime.datetime, float]]]:
            for mhz, (lower, upper, sigs) in self._freqs_bins.items():
                if freq > lower and freq < upper:
                    return (mhz, sigs)

            return (None, [])

        previous_absent: bool = False
        frequency_mhz, sigs = get_freqs_list(msig.frequency)

        if not frequency_mhz:
            logger.debug(f"signal {msig.frequency/1000.0/1000.0:.3f} MHz: not in sig_freqs_mhz list, discarding")
            return

        # append current signal to the signal list of this freq
        sigs.append((msig.ts, msig._avgs[0]))

        # discard signals below threshold
        if msig._avgs[0] < self.sig_threshold_dbw:
            logger.debug(f"signal {frequency_mhz:.3f} MHz, {msig._avgs[0]:.3f} dBW: too weak, discarding")
            return

        # cleanup current signal list (discard older signals)
        sig_start = msig.ts - datetime.timedelta(seconds=self.freq_active_window_s)
        sigs[:] = [sig for sig in sigs if sig[0] > sig_start]

        # check if bats was absent before
        count = len(sigs)
        if count < self.freq_active_count:
            previous_absent = True
            logger.debug(f"signal {frequency_mhz:.3f} MHz, {msig._avgs[0]:.3f} dBW: one of the first signals => match")

        # check if bat is active
        if not previous_absent:
            var = np.std([sig[1] for sig in sigs])
            if var < self.freq_active_var:
                logger.debug(f"signal {frequency_mhz:.3f} MHz, {msig._avgs[0]:.3f} dBW: frequency variance low ({var}), discarding")
                return
            else:
                logger.debug(f"signal: {frequency_mhz:.3f} MHz, {msig._avgs[0]:.3f} dBW: met all conditions (sig_count: {count}, sig_var: {var:.3f})")

        # set untrigger time if all criterions are met
        # TODO: set this from db_ts, instead of local time
        # this could lead to decreasing of untrigger_ts, which could be avoided by calling max(untrigger_ts_old, ..._new)
        # if this is correct the 'sigs[:] = [sig for sig in sigs if sig[0] > sig_start]' statement should also be incorrect in some cases
        self.untrigger_ts = time.time() + self.untrigger_duration_s
        self._set_trigger(True, {"VHF Frequency": msig.frequency, "VHF Power (dBW)": msig._avgs[0], "VHF Signals": count})

    @staticmethod
    def on_connect(mqttc: mqtt.Client, self, flags, rc):
        logger.info("MQTT connection established (%s)", rc)

        # subscribe to match signal cbor messages
        topic_matched_cbor = "+/radiotracking/matched/cbor"
        mqttc.subscribe(topic_matched_cbor)
        mqttc.message_callback_add(topic_matched_cbor, self.on_matched_cbor)
        logger.info("Subscribed to %s", topic_matched_cbor)

    def run(self):
        self._running = True
        self.mqttc.on_connect = self.on_connect

        ret = self.mqttc.connect(self.mqtt_host, self.mqtt_port, self.mqtt_keepalive)
        if ret != mqtt.MQTT_ERR_SUCCESS:
            logger.critical("MQTT connetion failed: %s", ret)

        while self._running:
            self.mqttc.loop(0.1)
            if self.untrigger_ts < time.time():
                if self._trigger:
                    self._set_trigger(False, {})

        self.mqttc.disconnect()
