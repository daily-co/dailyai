#
# Copyright (c) 2024, Daily

#
# SPDX-License-Identifier: BSD 2-Clause License
#

import asyncio
import itertools

from PIL import Image
from typing import List

from pipecat.processors.frame_processor import FrameDirection, FrameProcessor
from pipecat.frames.frames import (
    AudioRawFrame,
    BotSpeakingFrame,
    CancelFrame,
    MetricsFrame,
    SpriteFrame,
    StartFrame,
    EndFrame,
    Frame,
    ImageRawFrame,
    StartInterruptionFrame,
    StopInterruptionFrame,
    SystemFrame,
    TransportMessageFrame)
from pipecat.transports.base_transport import TransportParams

from loguru import logger


class BaseOutputTransport(FrameProcessor):

    def __init__(self, params: TransportParams, **kwargs):
        super().__init__(**kwargs)

        self._params = params

        # These are the images that we should send to the camera at our desired
        # framerate.
        self._camera_images = None

        # We will write 20ms audio at a time. If we receive long audio frames we
        # will chunk them. This will help with interruption handling.
        audio_bytes_10ms = int(self._params.audio_out_sample_rate / 100) * \
            self._params.audio_out_channels * 2
        self._audio_chunk_size = audio_bytes_10ms * 2

        self._stopped_event = asyncio.Event()

        # Create sink frame task. This is the task that will actually write
        # audio or video frames. We write audio/video in a task so we can keep
        # generating frames upstream while, for example, the audio is playing.
        self._create_sink_task()

        # Create push frame task. This is the task that will push frames in
        # order. We also guarantee that all frames are pushed in the same task.
        self._create_push_task()

    async def start(self, frame: StartFrame):
        # Create media threads queues.
        if self._params.camera_out_enabled:
            self._camera_out_queue = asyncio.Queue()
            self._camera_out_task = self.get_event_loop().create_task(self._camera_out_task_handler())

    async def stop(self):
        # Wait on the threads to finish.
        if self._params.camera_out_enabled:
            self._camera_out_task.cancel()
            await self._camera_out_task

        self._stopped_event.set()

    async def send_message(self, frame: TransportMessageFrame):
        pass

    async def send_metrics(self, frame: MetricsFrame):
        pass

    async def write_frame_to_camera(self, frame: ImageRawFrame):
        pass

    async def write_raw_audio_frames(self, frames: bytes):
        pass

    #
    # Frame processor
    #

    async def cleanup(self):
        if self._sink_task:
            self._sink_task.cancel()
            await self._sink_task

        self._push_frame_task.cancel()
        await self._push_frame_task

    async def process_frame(self, frame: Frame, direction: FrameDirection):
        await super().process_frame(frame, direction)

        #
        # Out-of-band frames like (CancelFrame or StartInterruptionFrame) are
        # pushed immediately. Other frames require order so they are put in the
        # sink queue.
        #
        if isinstance(frame, StartFrame):
            await self.start(frame)
            await self.push_frame(frame, direction)
        # EndFrame is managed in the sink queue handler.
        elif isinstance(frame, CancelFrame):
            await self.stop()
            await self.push_frame(frame, direction)
        elif isinstance(frame, StartInterruptionFrame) or isinstance(frame, StopInterruptionFrame):
            await self._handle_interruptions(frame)
            await self.push_frame(frame, direction)
        elif isinstance(frame, MetricsFrame):
            await self.send_metrics(frame)
            await self.push_frame(frame, direction)
        elif isinstance(frame, SystemFrame):
            await self.push_frame(frame, direction)
        elif isinstance(frame, AudioRawFrame):
            await self._handle_audio(frame)
        else:
            await self._sink_queue.put(frame)

        # If we are finishing, wait here until we have stopped, otherwise we might
        # close things too early upstream. We need this event because we don't
        # know when the internal threads will finish.
        if isinstance(frame, CancelFrame) or isinstance(frame, EndFrame):
            await self._stopped_event.wait()

    async def _handle_interruptions(self, frame: Frame):
        if not self.interruptions_allowed:
            return

        if isinstance(frame, StartInterruptionFrame):
            # Stop sink task.
            self._sink_task.cancel()
            await self._sink_task
            self._create_sink_task()
            # Stop push task.
            self._push_frame_task.cancel()
            await self._push_frame_task
            self._create_push_task()

    async def _handle_audio(self, frame: AudioRawFrame):
        audio = frame.audio
        for i in range(0, len(audio), self._audio_chunk_size):
            chunk = AudioRawFrame(audio[i: i + self._audio_chunk_size],
                                  sample_rate=frame.sample_rate, num_channels=frame.num_channels)
            await self._sink_queue.put(chunk)

    def _create_sink_task(self):
        loop = self.get_event_loop()
        self._sink_queue = asyncio.Queue()
        self._sink_task = loop.create_task(self._sink_task_handler())

    async def _sink_task_handler(self):
        # Audio accumlation buffer
        buffer = bytearray()
        while True:
            try:
                frame = await self._sink_queue.get()
                if isinstance(frame, AudioRawFrame) and self._params.audio_out_enabled:
                    buffer.extend(frame.audio)
                    buffer = await self._maybe_send_audio(buffer)
                elif isinstance(frame, ImageRawFrame) and self._params.camera_out_enabled:
                    await self._set_camera_image(frame)
                elif isinstance(frame, SpriteFrame) and self._params.camera_out_enabled:
                    await self._set_camera_images(frame.images)
                elif isinstance(frame, TransportMessageFrame):
                    await self.send_message(frame)
                else:
                    await self._internal_push_frame(frame)

                if isinstance(frame, EndFrame):
                    await self.stop()

                self._sink_queue.task_done()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.exception(f"{self} error processing sink queue: {e}")

    #
    # Push frames task
    #

    def _create_push_task(self):
        loop = self.get_event_loop()
        self._push_queue = asyncio.Queue()
        self._push_frame_task = loop.create_task(self._push_frame_task_handler())

    async def _internal_push_frame(
            self,
            frame: Frame | None,
            direction: FrameDirection | None = FrameDirection.DOWNSTREAM):
        await self._push_queue.put((frame, direction))

    async def _push_frame_task_handler(self):
        while True:
            try:
                (frame, direction) = await self._push_queue.get()
                await self.push_frame(frame, direction)
            except asyncio.CancelledError:
                break

    #
    # Camera out
    #

    async def send_image(self, frame: ImageRawFrame | SpriteFrame):
        await self.process_frame(frame, FrameDirection.DOWNSTREAM)

    async def _draw_image(self, frame: ImageRawFrame):
        desired_size = (self._params.camera_out_width, self._params.camera_out_height)

        if frame.size != desired_size:
            image = Image.frombytes(frame.format, frame.size, frame.image)
            resized_image = image.resize(desired_size)
            logger.warning(
                f"{frame} does not have the expected size {desired_size}, resizing")
            frame = ImageRawFrame(resized_image.tobytes(), resized_image.size, resized_image.format)

        await self.write_frame_to_camera(frame)

    async def _set_camera_image(self, image: ImageRawFrame):
        if self._params.camera_out_is_live:
            await self._camera_out_queue.put(image)
        else:
            self._camera_images = itertools.cycle([image])

    async def _set_camera_images(self, images: List[ImageRawFrame]):
        self._camera_images = itertools.cycle(images)

    async def _camera_out_task_handler(self):
        while True:
            try:
                if self._params.camera_out_is_live:
                    image = await self._camera_out_queue.get()
                    await self._draw_image(image)
                    self._camera_out_queue.task_done()
                elif self._camera_images:
                    image = next(self._camera_images)
                    await self._draw_image(image)
                    await asyncio.sleep(1.0 / self._params.camera_out_framerate)
                else:
                    await asyncio.sleep(1.0 / self._params.camera_out_framerate)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.exception(f"{self} error writing to camera: {e}")

    #
    # Audio out
    #

    async def send_audio(self, frame: AudioRawFrame):
        await self.process_frame(frame, FrameDirection.DOWNSTREAM)

    async def _maybe_send_audio(self, buffer: bytearray) -> bytearray:
        if len(buffer) >= self._audio_chunk_size:
            await self.write_raw_audio_frames(bytes(buffer[:self._audio_chunk_size]))
            buffer = buffer[self._audio_chunk_size:]
            await self.push_frame(BotSpeakingFrame(), FrameDirection.UPSTREAM)
        return buffer
