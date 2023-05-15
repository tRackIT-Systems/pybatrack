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

    def start_recording(self):
        logger.info("Powering light on")
        self.light.on()

        logger.info("Starting camera recording")
        with open("/var/www/html/FIFO1", "w", encoding="ascii") as f:
            f.write("1")

        timer = threading.Timer(1.0, self.observe_camera_started)
        timer.start()

        self._recording = True

    def stop_recording(self):
        logger.info("Stopping camera recording")
        with open("/var/www/html/FIFO1", "w", encoding="ascii") as f:
            f.write("0")

        logger.info("Powering light off")
        self.light.off()

        timer = threading.Timer(1.0, self.observe_camera_stopped)
        timer.start()

        self._recording = False

    def observe_camera_stopped(self):
        self.observe_camera("Capturing stopped")

    def observe_camera_started(self):
        self.observe_camera("Capturing started")

    def observe_camera(self, pattern):
        every_thing_is_fine = False
        with open("/var/www/html/scheduleLog.txt", "r", encoding="ascii") as f:
            last_three_lines = f.readlines()[-self.number_of_lines_to_observe:]
            for line in last_three_lines:
                logger.debug("checked line %s for pattern: %s", line, pattern)
                if pattern in line:
                    every_thing_is_fine = True
        if not every_thing_is_fine:
            logger.warning("camera seems broken, Terminating.")
            exit(1)
