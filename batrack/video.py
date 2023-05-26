import gpiozero
import logging
import threading
import time
import os
import datetime
import socket
import glob

from batrack.sensors import AbstractAnalysisUnit

logger = logging.getLogger(__name__)


class CameraAnalysisUnit(AbstractAnalysisUnit):
    def __init__(self,
                 light_pin: int,
                 html_folder: str = "/var/www/html/",
                 video_boxing_timeout_s: int = 60,
                 **kwargs):
        """Camera and light sensor, currently only supporting recording.

        Args:
            light_pin (int): GPIO pin to be used to controll the light.
            video_boxing_timeout_s (int): Timeout in seconds to wait for boxing finished messages.
        """
        super().__init__(**kwargs)
        self.number_of_lines_to_observe = 5
        self.html_folder: str = html_folder
        self.video_boxing_timeout_s: int = int(video_boxing_timeout_s)

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
            with open(os.path.join(self.html_folder, "FIFO1"), "w", encoding="ascii") as f:
                f.write("1")

            timer = threading.Timer(1.0, self.observe_camera_started)
            timer.start()

            self._recording = True
        else:
            logger.info("Starting camera recording: ignored, camera already recording")

    def stop_recording(self):
        if self._recording:
            logger.info("Stopping camera recording")
            with open(os.path.join(self.html_folder, "FIFO1"), "w", encoding="ascii") as f:
                f.write("0")

            logger.info("Powering light off")
            self.light.off()

            threading.Thread(target=self.observe_camera_stopped).start()
            # timer = threading.Timer(1.0, self.observe_camera_stopped)
            # timer.start()

            self._recording = False
        else:
            logger.debug("Stopping camera recording: ignored, camera not recording")

    def observe_camera_stopped(self):
        timeout = time.time() + self.video_boxing_timeout_s
        with open(os.path.join(self.html_folder, "scheduleLog.txt"), "r", encoding="ascii") as f:
            f.seek(0, 2)
            while timeout > time.time():
                line = f.readline()[:-1]
                if not line:
                    time.sleep(0.1)
                    continue

                logger.debug(line)

                if "Capturing stopped" in line:
                    logger.info("Confirmed capturing stopped.")

                if "Finished boxing" in line:
                    video_path = line.split()[4]

                    file_name, file_ext = os.path.splitext(os.path.split(video_path)[1])

                    # ex: vi_0281_20230515_151643
                    video_time_str = "_".join(file_name.split("_")[2:4])
                    video_time = datetime.datetime.strptime(video_time_str, "%Y%m%d_%H%M%S")
                    # video_time = datetime.datetime.fromtimestamp(os.path.getmtime(video_path))

                    time_str = video_time.strftime("%Y-%m-%dT%H_%M_%S")
                    target_path = os.path.join(self.data_path, f"{socket.gethostname()}_{time_str}{file_ext}")

                    logger.info("Moving video from %s to %s", video_path, target_path)
                    os.rename(video_path, target_path)

                    thumbnail_glob = f"{video_path}.*.th.jpg"
                    thumbnail_paths = glob.glob(thumbnail_glob)
                    logger.info("Removing thumbnails %s", thumbnail_paths)
                    for th_path in thumbnail_paths:
                        os.remove(th_path)

                    return

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
        with open(os.path.join(self.html_folder, "scheduleLog.txt"), "r", encoding="ascii") as f:
            return f.readlines()[-self.number_of_lines_to_observe:]
