import gpiozero
import logging
import threading
import time

from batrack.sensors import AbstractAnalysisUnit

logger = logging.getLogger(__name__)


class CameraAnalysisUnit(AbstractAnalysisUnit):
    def __init__(self, light_pin: int, **kwargs):
        """Camera and light sensor, currently only supporting recording.

        Args:
            light_pin (int): GPIO pin to be used to controll the light.
        """
        super().__init__(**kwargs)
        self.number_of_lines_to_observe = 5

        # initialize GPIO communication
        self.light: gpiozero.LED = gpiozero.LED(light_pin, active_high=True)

    def run(self):
        self._running = True

        # camera software is running in a system process and does
        # not require any active computations here
        while self._running:
            logger.debug("sensor running")
            time.sleep(1)

        self.light.close()

    def start_recording(self):
        if not self._recording:
            logger.info("Powering light on")
            self.light.on()

            logger.info("Starting camera recording")
            with open("/var/www/html/FIFO1", "w", encoding="ascii") as f:
                f.write("1")

            timer = threading.Timer(1.0, self.observe_camera_started)
            timer.start()

            self._recording = True
        else:
            logger.info("Starting camera recording: ignored, camera already recording")

    def stop_recording(self):
        if self._recording:
            logger.info("Stopping camera recording")
            with open("/var/www/html/FIFO1", "w", encoding="ascii") as f:
                f.write("0")

            logger.info("Powering light off")
            self.light.off()

            timer = threading.Timer(1.0, self.observe_camera_stopped)
            timer.start()

            self._recording = False
        else:
            logger.debug("Stopping camera recording: ignored, camera not recording")

    def observe_camera_stopped(self):
        tail = self.schedule_log_tail()
        pattern_found = any(["Capturing stopped" in line for line in tail])

        if pattern_found:
            logger.info("Confirmed capturing stopped.")
        else:
            logger.warning("Capturing stopped NOT confirmed, ignoring.")

    def observe_camera_started(self):
        tail = self.schedule_log_tail()
        pattern_found = any(["Capturing started" in line for line in tail])

        if pattern_found:
            logger.info("Confirmed capturing started.")
        else:
            logger.warning("Capturing started NOT confirmed, terminating.")
            exit(1)

    def schedule_log_tail(self):
        with open("/var/www/html/scheduleLog.txt", "r", encoding="ascii") as f:
            return f.readlines()[-self.number_of_lines_to_observe:]
