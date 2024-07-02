#
# Copyright (c) 2024, Daily
#
# SPDX-License-Identifier: BSD 2-Clause License
#

import asyncio

from typing import Awaitable, Callable

from pipecat.frames.frames import BotSpeakingFrame, Frame, StartInterruptionFrame, StopInterruptionFrame, SystemFrame
from pipecat.processors.async_frame_processor import AsyncFrameProcessor
from pipecat.processors.frame_processor import FrameDirection


class UserIdleProcessor(AsyncFrameProcessor):
    """This class is useful to check if the user is interacting with the bot
    within a given timeout. If the timeout is reached before any interaction
    occurred the provided callback will be called.

    The callback can then be used to push frames downstream by using
    `queue_frame()` (or `push_frame()` for system frames).

    """

    def __init__(
            self,
            *,
            callback: Callable[["UserIdleProcessor"], Awaitable[None]],
            timeout: float,
            **kwargs):
        super().__init__(**kwargs)

        self._callback = callback
        self._timeout = timeout

        self._interrupted = False

        self._create_idle_task()

    async def process_frame(self, frame: Frame, direction: FrameDirection):
        await super().process_frame(frame, direction)

        if isinstance(frame, SystemFrame):
            await self.push_frame(frame, direction)
        else:
            await self.queue_frame(frame, direction)

        # We shouldn't call the idle callback if the user or the bot are speaking.
        if isinstance(frame, StartInterruptionFrame):
            self._interrupted = True
            self._idle_event.set()
        elif isinstance(frame, StopInterruptionFrame):
            self._interrupted = False
            self._idle_event.set()
        elif isinstance(frame, BotSpeakingFrame):
            self._idle_event.set()

    async def cleanup(self):
        self._idle_task.cancel()
        await self._idle_task

    def _create_idle_task(self):
        self._idle_event = asyncio.Event()
        self._idle_task = self.get_event_loop().create_task(self._idle_task_handler())

    async def _idle_task_handler(self):
        while True:
            try:
                await asyncio.wait_for(self._idle_event.wait(), timeout=self._timeout)
            except asyncio.TimeoutError:
                if not self._interrupted:
                    await self._callback(self)
            except asyncio.CancelledError:
                break
            finally:
                self._idle_event.clear()
