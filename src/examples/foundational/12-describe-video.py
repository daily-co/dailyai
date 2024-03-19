import asyncio
import aiohttp
import logging
import os
from typing import AsyncGenerator

from dailyai.pipeline.frames import Frame, LLMMessagesQueueFrame
from dailyai.pipeline.pipeline import Pipeline
from dailyai.pipeline.frame_processor import FrameProcessor
from dailyai.services.daily_transport_service import DailyTransportService
from dailyai.services.elevenlabs_ai_service import ElevenLabsTTSService
from dailyai.services.open_ai_services import OpenAILLMService, OpenAIVisionService
from dailyai.services.ai_services import FrameLogger
from dailyai.pipeline.aggregators import (
    LLMAssistantContextAggregator,
    LLMUserContextAggregator,
)
from dailyai.pipeline.frames import VideoImageFrame, VisionFrame
from examples.support.runner import configure

logging.basicConfig(format=f"%(levelno)s %(asctime)s %(message)s")
logger = logging.getLogger("dailyai")
logger.setLevel(logging.DEBUG)


class VideoImageFrameProcessor(FrameProcessor):
    def __init__(self):
        pass

    async def process_frame(self, frame: Frame) -> AsyncGenerator[Frame, None]:
        if isinstance(frame, VideoImageFrame):
            yield VisionFrame("What is in this image?", frame.image)
        else:
            yield frame


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
            receive_video=True,
            receive_video_fps=1/10.0
        )

        tts = ElevenLabsTTSService(
            aiohttp_session=session,
            api_key=os.getenv("ELEVENLABS_API_KEY"),
            voice_id=os.getenv("ELEVENLABS_VOICE_ID"),
        )

        llm = OpenAILLMService(
            api_key=os.getenv("OPENAI_CHATGPT_API_KEY"),
            model="gpt-4-turbo-preview")
        fl = FrameLogger("!!! before VIFP")
        fl2 = FrameLogger("Outer")
        fl3 = FrameLogger("### Before VS")
        fl4 = FrameLogger("$$$ After VS")
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
        vs = OpenAIVisionService(api_key=os.getenv("OPENAI_CHATGPT_API_KEY"))

        vifp = VideoImageFrameProcessor()
        pipeline = Pipeline(
            processors=[
                fl,
                vifp,
                fl3,
                vs,
                fl4,
                llm,
                fl2,
                tts,
                tma_out,
            ],
        )

        transport.transcription_settings["extra"]["endpointing"] = True
        transport.transcription_settings["extra"]["punctuate"] = True
        await transport.run(pipeline)


if __name__ == "__main__":
    (url, token) = configure()
    asyncio.run(main(url, token))
