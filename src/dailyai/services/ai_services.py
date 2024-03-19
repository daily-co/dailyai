import asyncio
import io
import logging
import time
import wave
from dailyai.pipeline.frame_processor import FrameProcessor

from dailyai.pipeline.frames import (
    AudioFrame,
    EndFrame,
    EndPipeFrame,
    ImageFrame,
    LLMMessagesQueueFrame,
    LLMResponseEndFrame,
    LLMResponseStartFrame,
    LLMFunctionStartFrame,
    LLMFunctionCallFrame,
    Frame,
    TextFrame,
    TranscriptionQueueFrame,
    VisionFrame
)

from abc import abstractmethod
from typing import AsyncGenerator, BinaryIO


class AIService(FrameProcessor):
    def __init__(self):
        self.logger = logging.getLogger("dailyai")


class LLMService(AIService):
    """This class is a no-op but serves as a base class for LLM services."""

    def __init__(self):
        super().__init__()


class TTSService(AIService):
    def __init__(self, aggregate_sentences=True):
        super().__init__()
        self.aggregate_sentences: bool = aggregate_sentences
        self.current_sentence: str = ""

    # Some TTS services require a specific sample rate. We default to 16k
    def get_mic_sample_rate(self):
        return 16000

    # Converts the text to audio. Yields a list of audio frames that can
    # be sent to the microphone device
    @abstractmethod
    async def run_tts(self, text) -> AsyncGenerator[bytes, None]:
        # yield empty bytes here, so linting can infer what this method does
        yield bytes()

    async def process_frame(self, frame: Frame) -> AsyncGenerator[Frame, None]:
        if isinstance(frame, EndFrame) or isinstance(frame, EndPipeFrame):
            if self.current_sentence:
                async for audio_chunk in self.run_tts(self.current_sentence):
                    yield AudioFrame(audio_chunk)
                yield TextFrame(self.current_sentence)

        if not isinstance(frame, TextFrame):
            yield frame
            return

        text: str | None = None
        if not self.aggregate_sentences:
            text = frame.text
        else:
            self.current_sentence += frame.text
            if self.current_sentence.strip().endswith((".", "?", "!")):
                text = self.current_sentence
                self.current_sentence = ""

        if text:
            async for audio_chunk in self.run_tts(text):
                yield AudioFrame(audio_chunk)

            # note we pass along the text frame *after* the audio, so the text
            # frame is completed after the audio is processed.
            yield TextFrame(text)


class ImageGenService(AIService):
    def __init__(self, image_size, **kwargs):
        super().__init__(**kwargs)
        self.image_size = image_size

    # Renders the image. Returns an Image object.
    @abstractmethod
    async def run_image_gen(self, sentence: str) -> tuple[str, bytes]:
        pass

    async def process_frame(self, frame: Frame) -> AsyncGenerator[Frame, None]:
        if not isinstance(frame, TextFrame):
            yield frame
            return

        (url, image_data) = await self.run_image_gen(frame.text)
        yield ImageFrame(url, image_data)


class STTService(AIService):
    """STTService is a base class for speech-to-text services."""

    _frame_rate: int

    def __init__(self, frame_rate: int = 16000, **kwargs):
        super().__init__(**kwargs)
        self._frame_rate = frame_rate

    @abstractmethod
    async def run_stt(self, audio: BinaryIO) -> str:
        """Returns transcript as a string"""
        pass

    async def process_frame(self, frame: Frame) -> AsyncGenerator[Frame, None]:
        """Processes a frame of audio data, either buffering or transcribing it."""
        if not isinstance(frame, AudioFrame):
            return

        data = frame.data
        content = io.BufferedRandom(io.BytesIO())
        ww = wave.open(self._content, "wb")
        ww.setnchannels(1)
        ww.setsampwidth(2)
        ww.setframerate(self._frame_rate)
        ww.writeframesraw(data)
        ww.close()
        content.seek(0)
        text = await self.run_stt(content)
        yield TranscriptionQueueFrame(text, "", str(time.time()))


class VisionService(AIService):
    def __init__(self):
        super().__init__()

    # Renders the image. Returns an Image object.
    # TODO-CB: return type
    @abstractmethod
    async def run_vision(self, prompt: str, image: bytes):
        pass

    async def process_frame(self, frame: Frame) -> AsyncGenerator[Frame, None]:
        if isinstance(frame, VisionFrame):
            async for frame in self.run_vision(frame.prompt, frame.image):
                yield frame
            yield LLMResponseEndFrame()
        else:
            yield frame


class FrameLogger(AIService):
    def __init__(self, prefix="Frame", **kwargs):
        super().__init__(**kwargs)
        self.prefix = prefix

    async def process_frame(self, frame: Frame) -> AsyncGenerator[Frame, None]:
        if isinstance(frame, (AudioFrame, ImageFrame)):
            self.logger.info(f"{self.prefix}: {type(frame)}")
        else:
            print(f"{self.prefix}: {frame}")

        yield frame
