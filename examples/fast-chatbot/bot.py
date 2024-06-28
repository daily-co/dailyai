#
# Copyright (c) 2024, Daily
#
# SPDX-License-Identifier: BSD 2-Clause License
#

from loguru import logger
import argparse
import asyncio
import aiohttp
import os
import sys
import time
from typing import Optional

from pydantic import BaseModel, ValidationError

from pipecat.vad.vad_analyzer import VADParams
from pipecat.vad.silero import SileroVADAnalyzer
from pipecat.transports.services.daily import DailyParams, DailyTransport
from pipecat.services.openai import OpenAILLMService
from pipecat.services.deepgram import DeepgramSTTService
from pipecat.pipeline.task import PipelineParams, PipelineTask
from pipecat.pipeline.runner import PipelineRunner
from pipecat.pipeline.pipeline import Pipeline
from pipecat.frames.frames import LLMMessagesFrame, EndFrame

from pipecat.processors.aggregators.llm_response import (
    LLMAssistantResponseAggregator, LLMUserResponseAggregator
)

from helpers import (
    ClearableDeepgramTTSService,
    AudioVolumeTimer,
    TranscriptionTimingLogger
)


from dotenv import load_dotenv
load_dotenv(override=True)

logger.remove(0)
logger.add(sys.stderr, level=os.getenv("LOG_LEVEL", "DEBUG"))


class BotSettings(BaseModel):
    room_url: str
    room_token: str
    bot_name: str = "Pipecat"
    prompt: Optional[str] = "You are a helpful assistant."
    deepgram_api_key: Optional[str] = os.getenv("DEEPGRAM_API_KEY", None)
    deepgram_voice: Optional[str] = os.getenv("DEEPGRAM_VOICE", "aura-asteria-en")
    deepgram_tts_base_url: Optional[str] = os.getenv(
        "DEEPGRAM_TTS_BASE_URL", "https://api.deepgram.com/v1/speak")
    deepgram_stt_base_url: Optional[str] = os.getenv(
        "DEEPGRAM_STT_BASE_URL", "https://api.deepgram.com/v1/speak")
    openai_api_key: Optional[str] = os.getenv("OPENAI_API_KEY", None),
    openai_model: Optional[str] = os.getenv("OPENAI_MODEL", None),
    openai_base_url: Optional[str] = os.getenv("OPENAI_BASE_URL", None)
    vad_stop_secs: Optional[float] = os.getenv("VAD_STOP_SECS", 0.200)


async def main(settings: BotSettings):
    async with aiohttp.ClientSession() as session:
        transport = DailyTransport(
            settings.room_url,
            settings.room_token,
            settings.bot_name,
            DailyParams(
                audio_out_enabled=True,
                transcription_enabled=False,
                vad_enabled=True,
                vad_analyzer=SileroVADAnalyzer(params=VADParams(
                    stop_secs=settings.vad_stop_secs
                )),
                vad_audio_passthrough=True
            )
        )

        stt = DeepgramSTTService(
            name="STT",
            api_key=settings.deepgram_api_key,
            url=settings.deepgram_stt_base_url
        )

        tts = ClearableDeepgramTTSService(
            name="Voice",
            aiohttp_session=session,
            api_key=settings.deepgram_api_key,
            voice=settings.deepgram_voice,
            **({'base_url': url} if (url := settings.deepgram_tts_base_url) else {})
        )

        llm = OpenAILLMService(
            name="LLM",
            api_key=settings.openai_api_key,
            model=settings.openai_model,
            base_url=settings.openai_base_url,
        )

        messages = [
            {
                "role": "system",
                "content": settings.prompt,
            },
        ]

        avt = AudioVolumeTimer()
        tl = TranscriptionTimingLogger(avt)

        tma_in = LLMUserResponseAggregator(messages)
        tma_out = LLMAssistantResponseAggregator(messages)

        pipeline = Pipeline([
            transport.input(),   # Transport user input
            avt,                 # Audio volume timer
            stt,                 # Speech-to-text
            tl,                  # Transcription timing logger
            tma_in,              # User responses
            llm,                 # LLM
            tts,                 # TTS
            transport.output(),  # Transport bot output
            tma_out,             # Assistant spoken responses
        ])

        task = PipelineTask(
            pipeline,
            PipelineParams(
                allow_interruptions=True,
                enable_metrics=True,
                report_only_initial_ttfb=True
            ))

        # When the participant leaves, we exit the bot.
        @transport.event_handler("on_participant_left")
        async def on_participant_left(transport, participant, reason):
            await task.queue_frame(EndFrame())

        # When the first participant joins, the bot should introduce itself.
        @transport.event_handler("on_first_participant_joined")
        async def on_first_participant_joined(transport, participant):
            # Provide some air whilst tracks subscribe
            time.sleep(2)
            messages.append(
                {
                    "role": "system",
                    "content": "Introduce yourself by saying 'hello, I'm FastBot, how can I help you today?'"})
            await task.queue_frames([LLMMessagesFrame(messages)])

        runner = PipelineRunner()
        await runner.run(task)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Pipecat Bot")
    parser.add_argument("-s", "--settings", type=str, required=True, help="Pipecat bot settings")

    args, unknown = parser.parse_known_args()

    try:
        settings = BotSettings.model_validate_json(args.settings)
        asyncio.run(main(settings))
    except ValidationError as e:
        print(e)
