import time
import logging
import numpy as np
import pyaudio
import subprocess
import socket
import threading
import datetime
import wave
import os
from queue import Empty, Queue
from typing import Optional, Tuple


from batrack.sensors import AbstractAnalysisUnit

logger = logging.getLogger(__name__)


class AudioAnalysisUnit(AbstractAnalysisUnit):
    def __init__(
        self,
        threshold_dbfs: int,
        highpass_hz: int,
        wave_export_len_s: float,
        quiet_threshold_s: float,
        noise_threshold_s: float,
        sampling_rate: int = 250000,
        lowpass_hz: int = 42000,
        input_block_duration: float = 0.05,
        **kwargs,
    ):
        """Bat call audio sensor.

        Args:
            threshold_dbfs (int): Loudness threshold for a noisy block.
            highpass_hz (int): Frequency for highpass filter.
            lowpass_hz (int): Frequency for lowpass filter.
            wave_export_len_s (float): Maximum duration of an exported wave.
            quiet_threshold_s (float): Silence duration for trigger unset.
            noise_threshold_s (float): Noise duration, to set trigger.
            sampling_rate (int, optional): Sampling rate of the microphone.
            input_block_duration (float, optional): Length of input blocks.
        """
        super().__init__(**kwargs)

        # user-configuration values
        self.threshold_dbfs: int = int(threshold_dbfs)
        self.highpass_hz: int = int(highpass_hz)
        self.lowpass_hz: int = int(lowpass_hz)

        self.sampling_rate: int = int(sampling_rate)
        self.input_block_duration: float = float(input_block_duration)
        self.input_frames_per_block: int = int(self.sampling_rate * input_block_duration)

        self.wave_export_len: float = float(wave_export_len_s) * self.sampling_rate

        self.quiet_blocks_max: float = float(quiet_threshold_s) / input_block_duration
        self.noise_blocks_max: float = float(noise_threshold_s) / input_block_duration

        self.freq_bins_hz = np.arange((self.input_frames_per_block / 2) + 1) / (
            self.input_frames_per_block / float(self.sampling_rate))

        self.frame_count = 0

        # set pyaudio config
        self.pa: pyaudio.PyAudio = pyaudio.PyAudio()

        self.__pings: int = 0

        self.__noise_blocks: int = 0
        self.__quiet_blocks: int = 0
        self.__wavewriter: Optional[WaveWriter] = None

    def run(self):
        self._running = True

        # open input stream
        device_index = self.__find_input_device()

        def callback(in_data, frame_count, time_info, status):
            self.frame_count += 1
            self.__analyse_frame(in_data)

            # if a wave file is opened, write the frame to this file
            if self.__wavewriter:
                self.__wavewriter.q.put(in_data)

            return (in_data, pyaudio.paContinue)

        stream = self.pa.open(
            input_device_index=device_index,
            format=pyaudio.paInt16,
            channels=1,
            rate=self.sampling_rate,
            input=True,
            frames_per_buffer=self.input_frames_per_block,
            stream_callback=callback,
        )

        stream.start_stream()

        while stream.is_active() and self._running:
            time.sleep(2)
            logger.debug("received %s frames", self.frame_count)
            if self.frame_count == 0:
                logger.warning("received no frames, power cycling usb ")
                subprocess.check_output(["sudo uhubctl -a cycle -p 3 -l 1-1"], shell=True)
                break
            self.frame_count = 0

        # left while-loop, clean up
        if self.__wavewriter:
            self.__wavewriter.stop()

        stream.stop_stream()
        stream.close()
        self.pa.terminate()

        logger.info("termination finished")

    def start_recording(self):
        if not self.wave_export_len:
            logger.info("Wave export length is zero, not creating wave file.")
            return

        if self.__wavewriter:
            logger.warning("another wave is opened, not creating new file.")
            return

        logger.info("Starting audio recording")
        self.__wavewriter = WaveWriter(self)
        self.__wavewriter.start()
        self._recording = True

    def stop_recording(self):
        # TODO: isn't it enough to set self._reconging = False? In run()
        # __wave_finalize() is also called.
        if self.__wavewriter:
            logger.info("Stopping audio recording")
            self.__wavewriter.stop()
            self.__wavewriter = None
        self._recording = False

    def __find_input_device(self) -> Optional[int]:
        """
        searches for a microphone and returns the device number
        :return: the device id
        """
        for device_index in range(self.pa.get_device_count()):
            dev_info = self.pa.get_device_info_by_index(device_index)
            logger.debug("Device %s: %s", device_index, dev_info['name'])

            for keyword in ["mic", "input"]:
                if keyword in dev_info["name"].lower():
                    logger.info("Found an input: device %s - %s", device_index, dev_info['name'])
                    return device_index

        logger.info("No preferred input found; using default input device.")
        return None

    def __analyse_frame(self, frame: str):
        """checks for the given frame if a trigger is present

        Args:
            frame (str): the recorded audio frame to be analysed
        """

        spectrum = self.__exec_fft(frame)
        peak_db, peak_frequency_hz = self.__get_peak_db(spectrum)

        # noisy block
        if peak_db > self.threshold_dbfs:
            self.__quiet_blocks = 0
            self.__noise_blocks += 1

        # quiet block
        else:
            # ping detection; a ping has to be a noisy sequence which is not
            # longer than self.noise_blocks_max
            if 1 <= self.__noise_blocks <= self.noise_blocks_max:
                logger.info("detected ping %s", self.__pings)
                self.__pings += 1

            # set trigger and callback
            # it's the second ping because of the *click* of the relays which
            # is the first ping every time
            # in the moment we done have a relay anymore we can delete the
            # lower boundary
            if 1 <= self.__pings and not self._trigger:
                self._set_trigger(True, {"Pings": self.__pings, "Ping Frequency": peak_frequency_hz})

            # stop audio if thresbold of quiet blocks is met
            if self.__quiet_blocks > self.quiet_blocks_max and self._trigger:
                self._set_trigger(False, {"Quiet Blocks": self.__quiet_blocks})
                self.__pings = 0

            self.__noise_blocks = 0
            self.__quiet_blocks += 1

    def __exec_fft(self, signal) -> np.fft.rfft:
        """execute a fft on given samples and apply highpass filter

        Args:
            signal ([type]): the input samples

        Returns:
            np.fft.rfft: highpass-filtered spectrum
        """
        # do the fft
        data_int16 = np.frombuffer(signal, dtype=np.int16)
        spectrum = np.fft.rfft(data_int16)

        # apply the highpass
        spectrum[self.freq_bins_hz < self.highpass_hz] = 0.000000001
        spectrum[self.freq_bins_hz > self.lowpass_hz] = 0.000000001

        return spectrum

    def __get_peak_db(self, spectrum: np.fft) -> Tuple[float, int]:
        """extract the maximal volume of a given spectrum

        Args:
            spectrum (np.fft): spectrum to analyze

        Returns:
            float: the retrieved maximum
        """

        window_function_dbfs_max = np.sum(self.input_frames_per_block) / 2.0
        dbfs_spectrum = 20 * np.log10(np.abs(spectrum) / max([window_function_dbfs_max, 1]))
        bin_peak_index = dbfs_spectrum.argmax()
        peak_db = dbfs_spectrum[bin_peak_index]
        peak_frequency_hz = bin_peak_index * self.sampling_rate / self.input_frames_per_block
        logger.debug("Peak freq hz: %s dBFS: %s", peak_frequency_hz, peak_db)
        return peak_db, peak_frequency_hz


class WaveWriter(threading.Thread):
    def __init__(self, aau: AudioAnalysisUnit):
        super().__init__()
        self.aau: AudioAnalysisUnit = aau

        start_time_str = datetime.datetime.now().strftime("%Y-%m-%dT%H_%M_%S")
        file_path = os.path.join(aau.data_path, socket.gethostname() + "_" + start_time_str + ".wav")

        logger.info("creating wav file '%s'", file_path)
        self.__wave = wave.open(file_path, "wb")
        self.__wave.setnchannels(1)
        self.__wave.setsampwidth(aau.pa.get_sample_size(pyaudio.paInt16))
        self.__wave.setframerate(aau.sampling_rate)

        self._running = False
        self.q: Queue = Queue()

    def stop(self):
        self._running = False
        self.join()

    def run(self):
        self._running = True

        while self._running:
            try:
                frame = self.q.get(block=True, timeout=1)
                self.__wave_write(frame)
            except Empty:
                break

        self.__wave_finalize()

    def __wave_write(self, frame):
        remaining_length = int(self.aau.wave_export_len - self.__wave._nframeswritten)

        if len(frame) > remaining_length:
            logger.info("wave reached maximum, starting new file...")
            self.__wave_finalize()
            self.start()

        logger.debug("writing frame, len: %s", len(frame))
        self.__wave.writeframes(frame)

    def __wave_finalize(self):
        if not self.__wave:
            logger.warning("no wave is opened, skipping finalization request")
            return

        self.__wave.close()
        self.__wave = None
