import asyncio
from typing import AsyncGenerator, AsyncIterable, Iterable, List
from dailyai.pipeline.frame_processor import FrameProcessor

from dailyai.pipeline.frames import EndPipeFrame, EndFrame, Frame, ControlFrame


class Pipeline:
    """
    This class manages a pipe of FrameProcessors, and runs them in sequence. The "source"
    and "sink" queues are managed by the caller. You can use this class stand-alone to
    perform specialized processing, or you can use the Transport's run_pipeline method to
    instantiate and run a pipeline with the Transport's sink and source queues.
    """

    def __init__(
        self,
        processors: List[FrameProcessor],
        source: asyncio.Queue | None = None,
        sink: asyncio.Queue[Frame] | None = None
    ):
        """Create a new pipeline. By default we create the sink and source queues
        if they're not provided, but these can be overridden to point to other
        queues. If this pipeline is run by a transport, its sink and source queues
        will be overridden.
        """
        self._processors: List[FrameProcessor] = processors

        self.source: asyncio.Queue[Frame] = source or asyncio.Queue()
        self.sink: asyncio.Queue[Frame] = sink or asyncio.Queue()

    def set_source(self, source: asyncio.Queue[Frame]):
        """Set the source queue for this pipeline. Frames from this queue
        will be processed by each frame_processor in the pipeline, or order
        from first to last."""
        self.source = source

    def set_sink(self, sink: asyncio.Queue[Frame]):
        """Set the sink queue for this pipeline. After the last frame_processor
        has processed a frame, its output will be placed on this queue."""
        self.sink = sink

    def add_processor(self, processor: FrameProcessor):
        self._processors.append(processor)

    async def get_next_source_frame(self) -> AsyncGenerator[Frame, None]:
        """Convenience function to get the next frame from the source queue. This
        lets us consistently have an AsyncGenerator yield frames, from either the
        source queue or a frame_processor."""

        yield await self.source.get()

    async def queue_frames(
        self,
        frames: Iterable[Frame] | AsyncIterable[Frame],
    ) -> None:
        """Insert frames directly into a pipeline. This is typically used inside a transport
        participant_joined callback to prompt a bot to start a conversation, for example."""

        if isinstance(frames, AsyncIterable):
            async for frame in frames:
                await self.source.put(frame)
        elif isinstance(frames, Iterable):
            for frame in frames:
                await self.source.put(frame)
        else:
            raise Exception("Frames must be an iterable or async iterable")

    async def run_pipeline(self):
        """Run the pipeline. Take each frame from the source queue, pass it to
        the first frame_processor, pass the output of that frame_processor to the
        next in the list, etc. until the last frame_processor has processed the
        resulting frames, then place those frames in the sink queue.

        The source and sink queues must be set before calling this method.

        This method will exit when an EndStreamQueueFrame is placed on the sink queue.
        No more frames will be placed on the sink queue after an EndStreamQueueFrame, even
        if it's not the last frame yielded by the last frame_processor in the pipeline..
        """

        try:
            while True:
                initial_frame = await self.source.get()
                async for frame in self._run_pipeline_recursively(
                    initial_frame, self._processors
                ):
                    await self.sink.put(frame)

                if isinstance(initial_frame, EndFrame) or isinstance(
                    initial_frame, EndPipeFrame
                ):
                    break
        except asyncio.CancelledError:
            # this means there's been an interruption, do any cleanup necessary
            # here.
            for processor in self._processors:
                await processor.interrupted()
            pass

    async def _run_pipeline_recursively(
        self, initial_frame: Frame, processors: List[FrameProcessor]
    ) -> AsyncGenerator[Frame, None]:
        """Internal function to add frames to the pipeline as they're yielded
        by each processor."""
        if processors:
            async for frame in processors[0].process_frame(initial_frame):
                async for final_frame in self._run_pipeline_recursively(
                    frame, processors[1:]
                ):
                    yield final_frame
        else:
            yield initial_frame


class CustomPipeline(Pipeline):
    """Used for services that don't neatly map to the frame processor model
    of standard pipelines. For example, LMNT's full-duplex TTS streaming."""
    # TODO-CB: Maybe this is a BasePipeline?

    def __init__(self,
                 source: asyncio.Queue | None = None,
                 sink: asyncio.Queue[Frame] | None = None):
        super().__init__(processors=[], source=source, sink=sink)

    def run_pipeline(self):
        """Right now, we'll override this in a subclass and
        access the source and sink queues directly, but I
        might need to clean that up later."""
        pass
