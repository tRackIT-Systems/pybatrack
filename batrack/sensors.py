import logging
import threading
from distutils.util import strtobool
from typing import Callable, Dict, Union, Literal


logger = logging.getLogger(__name__)


class AbstractAnalysisUnit(threading.Thread):
    def __init__(
        self,
        use_trigger: Union[str, bool, Literal[0, 1]],
        trigger_callback: Callable,
        data_path: str = ".",
        **kwargs,
    ):
        super().__init__()

        self.use_trigger: Union[bool, Literal[0, 1]] = strtobool(use_trigger) if isinstance(use_trigger, str) else bool(use_trigger)
        self.data_path: str = str(data_path)

        self._trigger_callback: Callable = trigger_callback

        self._running: bool = False
        self._trigger: bool = False
        self._recording: bool = False

        if kwargs:
            logger.debug("unused configuration parameters: %s", kwargs)

    @property
    def recording(self) -> bool:
        """Return, wether the sensors values are recorded."""
        return self._recording

    @property
    def trigger(self) -> bool:
        """Return trigger state based on this sensor."""
        return self._trigger

    def _set_trigger(self, trigger: bool, message: Dict):
        if self._trigger != trigger:
            logger.info("setting %s trigger %s: %s", self.__class__.__name__,  trigger, message)
            self._trigger = trigger
            self._trigger_callback(trigger, message)

    def stop(self):
        """Stop and join the running threaded sensor."""
        self.stop_recording()
        self._running = False
        self.join()

    def start_recording(self):
        """Start recording of the sensor.

        The sensor instance requires to run.
        """
        logger.warning("%s.start_recording() is not implemented.", self.__class__.__name__)

    def stop_recording(self):
        """Stop recording of the sensor.

        The sensor instance requires to run and a recording needs to be
        running.
        """
        logger.warning("%s.stop_recording() is not implemented.", self.__class__.__name__)

    def get_status(self) -> Dict:
        return {
            "running": self._running,
            "alive": self.is_alive(),
            "recording": self._recording,
            "use_trigger": self.use_trigger,
            "trigger": self._trigger,
        }
