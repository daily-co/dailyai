import asyncio
from asyncio import QueueEmpty

from typing import AsyncGenerator
from lmnt.api import Speech

from dailyai.services.ai_services import AIService, TTSService
from dailyai.pipeline.frames import (
    Frame,
    TextFrame,
    AudioFrame,
    LLMResponseEndFrame
)
from dailyai.pipeline.pipeline import CustomPipeline


# Since LMNT offers "full duplex" streaming, we have to implement a different
# kind of process_frame function, so we don't subclass TTSService. But this
# class can be used in place of any other TTS service
class LmntStreamingTTSPipeline(CustomPipeline):

    def __init__(
        self,
        *,
        api_key,
        voice_id="lily",
        source,
        sink
    ):
        super().__init__(source=source, sink=sink)

        self._api_key = api_key
        self._voice_id = voice_id
        self._connection = None
        # self._run_thread = asyncio.to_thread(self.run_connection)

    async def run_pipeline(self):
        print(f"@@@ !!! starting run_connection, hopefully in a thread")
        async with Speech() as speech:
            connection = await speech.synthesize_streaming(self._voice_id, format="raw", sample_rate=16000)
            print(f"@@@ connection created")
            t1 = asyncio.create_task(self.in_task(connection))
            t2 = asyncio.create_task(self.out_task(connection))
            await asyncio.gather(t1, t2)
            print(f"@@@ !!! run_connection is past the asyncio.gather")
            connection.finish()
            print(f"@@@ !!! run_connection is past finish()")

        print(f"@@@ !!! run_ connection is past the async with block, and is presumably done")

    async def in_task(self, connection):
        while True:
            frame = await self.source.get()
            print(f"@@@ GOT FRAME IN IN TASK: {frame}")
            if isinstance(frame, LLMResponseEndFrame):
                print(f"@@@ got an LLM response end; flushing, er, finishing")
                await connection.flush()
                print(f"@@@ PAST FLUSH")
            elif isinstance(frame, TextFrame):
                # Then it must be a TextFrame
                await connection.append_text(frame.text)
            else:
                await self.sink.put(frame)

    async def out_task(self, connection):
        async for message in connection:
            print(f"### OUT_TASK GOT A MESSAGE")
            frame = AudioFrame(message['audio'])
            print(f"@@@ out_task got some audio data! {frame}")
            await self.sink.put(frame)
        print(f"@@@ out_task seems to be done with the async for")


class LmntTTSService(TTSService):

    def __init__(
        self,
        *,
        api_key,
        voice_id="lily"
    ):
        super().__init__()

        self._api_key = api_key
        self._voice_id = voice_id

    async def run_tts(self, sentence) -> AsyncGenerator[bytes, None]:
        async with Speech() as speech:
            print(f"@@@ Sentence is {sentence}, voice is {self._voice_id}")
            synthesis = await speech.synthesize('Hello, world.', 'lily')
            yield AudioFrame[synthesis['audio']]
