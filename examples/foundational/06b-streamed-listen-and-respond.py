import asyncio
import aiohttp
import logging
import os
from dailyai.pipeline.frames import LLMMessagesQueueFrame
from dailyai.pipeline.pipeline import Pipeline

from dailyai.services.daily_transport_service import DailyTransportService
from dailyai.services.elevenlabs_ai_service import ElevenLabsTTSService
from dailyai.services.open_ai_services import OpenAILLMService
from dailyai.services.ai_services import FrameLogger
from dailyai.pipeline.aggregators import (
    LLMAssistantContextAggregator,
    LLMUserContextAggregator,
)
from runner import configure

from dotenv import load_dotenv
load_dotenv()

logging.basicConfig(format=f"%(levelno)s %(asctime)s %(message)s")
logger = logging.getLogger("dailyai")
logger.setLevel(logging.DEBUG)


async def main(room_url: str, token):
    async with aiohttp.ClientSession() as session:
        transport = DailyTransportService(
            room_url,
            token,
            "Respond bot",
            duration_minutes=5,
            start_transcription=True,
            mic_enabled=True,
            mic_sample_rate=16000,
            camera_enabled=False,
            vad_enabled=True,
        )

        tts = ElevenLabsTTSService(
            aiohttp_session=session,
            api_key=os.getenv("ELEVENLABS_API_KEY"),
            voice_id=os.getenv("ELEVENLABS_VOICE_ID"),
        )

        llm = OpenAILLMService(
            api_key=os.getenv("OPENAI_API_KEY"),
            model="gpt-4-turbo-preview")
        fl = FrameLogger("Inner")
        fl2 = FrameLogger("Outer")
        messages = [
            {
                "role": "system",
                "content": "You are a helpful LLM in a WebRTC call. Your goal is to demonstrate your capabilities in a succinct way. Your output will be converted to audio. Respond to what the user said in a creative and helpful way.",
            },
        ]

        tma_in = LLMUserContextAggregator(
            messages, transport._my_participant_id)
        tma_out = LLMAssistantContextAggregator(
            messages, transport._my_participant_id
        )
        pipeline = Pipeline(
            processors=[
                fl,
                tma_in,
                llm,
                fl2,
                tts,
                tma_out,
            ],
        )

        @transport.event_handler("on_first_other_participant_joined")
        async def on_first_other_participant_joined(transport):
            # Kick off the conversation.
            messages.append(
                {"role": "system", "content": "Please introduce yourself to the user."})
            await pipeline.queue_frames([LLMMessagesQueueFrame(messages)])

        transport.transcription_settings["extra"]["endpointing"] = True
        transport.transcription_settings["extra"]["punctuate"] = True
        await transport.run(pipeline)


if __name__ == "__main__":
    (url, token) = configure()
    asyncio.run(main(url, token))
