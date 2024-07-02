#
# Copyright (c) 2024, Daily
#
# SPDX-License-Identifier: BSD 2-Clause License
#

import asyncio

from typing import Awaitable, Callable, List

from pipecat.frames.frames import Frame, SystemFrame
from pipecat.processors.async_frame_processor import AsyncFrameProcessor
from pipecat.processors.frame_processor import FrameDirection


class IdleFrameProcessor(AsyncFrameProcessor):
    """This class waits to receive any frame or list of desired frames within a
    given timeout. If the timeout is reached before receiving any of those
    frames the provided callback will be called.

    The callback can then be used to push frames downstream by using
    `queue_frame()` (or `push_frame()` for system frames).

    """

    def __init__(
            self,
            *,
            callback: Callable[["IdleFrameProcessor"], Awaitable[None]],
            timeout: float,
            types: List[type] = [],
            **kwargs):
        super().__init__(**kwargs)

        self._callback = callback
        self._timeout = timeout
        self._types = types

        self._create_idle_task()

    async def process_frame(self, frame: Frame, direction: FrameDirection):
        await super().process_frame(frame, direction)

        if isinstance(frame, SystemFrame):
            await self.push_frame(frame, direction)
        else:
            await self.queue_frame(frame, direction)

        # If we are not waiting for any specific frame set the event, otherwise
        # check if we have received one of the desired frames.
        if not self._types:
            self._idle_event.set()
        else:
            for t in self._types:
                if isinstance(frame, t):
                    self._idle_event.set()

        # If we are not waiting for any specific frame set the event, otherwise
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
                await self._callback(self)
            except asyncio.CancelledError:
                break
            finally:
                self._idle_event.clear()
