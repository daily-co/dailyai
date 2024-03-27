import asyncio
import os
import logging

import aiohttp

from dailyai.pipeline.frames import EndFrame, TextFrame, LLMMessagesQueueFrame, Frame
from dailyai.pipeline.pipeline import Pipeline
from dailyai.services.daily_transport_service import DailyTransportService
from dailyai.services.elevenlabs_ai_service import ElevenLabsTTSService
from dailyai.services.open_ai_services import OpenAILLMService
from dailyai.services.lmnt_ai_services import LmntTTSService, LmntStreamingTTSPipeline
from dailyai.services.ai_services import FrameLogger

from runner import configure

from dotenv import load_dotenv
load_dotenv()

logging.basicConfig(format=f"%(levelno)s %(asctime)s %(message)s")
logger = logging.getLogger("dailyai")
logger.setLevel(logging.DEBUG)


async def main(room_url):
    async with aiohttp.ClientSession() as session:
        transport = DailyTransportService(
            room_url,
            None,
            "Say One Thing From an LLM",
            mic_enabled=True,
        )

        # tts = ElevenLabsTTSService(
        #     aiohttp_session=session,
        #     api_key=os.getenv("ELEVENLABS_API_KEY"),
        #     voice_id=os.getenv("ELEVENLABS_VOICE_ID"),
        # )

        tts = LmntTTSService(
            api_key=os.getenv("LMNT_API_KEY")
        )

        llm = OpenAILLMService(
            api_key=os.getenv("OPENAI_CHATGPT_API_KEY"),
            model="gpt-4-turbo-preview")
        q = asyncio.Queue()
        messages = [
            {
                "role": "system",
                "content": "You are an LLM in a WebRTC session, and this is a 'hello world' demo. Say hello to the world.",
            }]
        messages = [
            {
                "role": "system",
                "content": "Tell me a really long story about dogs.",
            }]
        fl = FrameLogger("%%% Before TTS")
        pipeline = Pipeline([llm, fl], source=transport.receive_queue, sink=q)
        tts_pipeline = LmntStreamingTTSPipeline(
            api_key=os.getenv("LMNT_API_KEY"), source=q, sink=transport.send_queue
        )

        @transport.event_handler("on_first_other_participant_joined")
        async def on_first_other_participant_joined(transport):
            print(f"### queueing frames for pipeline")
            await pipeline.queue_frames([LLMMessagesQueueFrame(messages), Frame(), Frame(), Frame()])

        await asyncio.gather(transport.run(), pipeline.run_pipeline(), tts_pipeline.run_pipeline())


if __name__ == "__main__":
    (url, token) = configure()
    asyncio.run(main(url))
