#
# Copyright (c) 2024, Daily
#
# SPDX-License-Identifier: BSD 2-Clause License
#

from loguru import logger
from runner import configure
import argparse
import asyncio
import aiohttp
import os
import sys
from typing import List, Optional

from pydantic import BaseModel, ValidationError

from pipecat.vad.vad_analyzer import VADParams
from pipecat.vad.silero import SileroVADAnalyzer
from pipecat.transports.services.daily import DailyParams, DailyTransport, DailyTransportMessageFrame
from pipecat.services.openai import OpenAILLMService, OpenAILLMContext
from pipecat.services.deepgram import DeepgramSTTService
from pipecat.pipeline.task import PipelineParams, PipelineTask
from pipecat.pipeline.runner import PipelineRunner
from pipecat.pipeline.pipeline import Pipeline
from pipecat.processors.logger import FrameLogger
from pipecat.frames.frames import LLMMessagesFrame, EndFrame

from pipecat.processors.aggregators.llm_response import (
    LLMAssistantResponseAggregator, LLMUserResponseAggregator
)


from fastbothelpers import (
    GreedyLLMAggregator,
    ClearableDeepgramTTSService,
    VADGate,
    AudioVolumeTimer,
    TranscriptionTimingLogger
)


from dotenv import load_dotenv
load_dotenv(override=True)

logger.remove(0)
logger.add(sys.stderr, level="DEBUG")


class BotSettings(BaseModel):
    room_url: str
    room_token: str
    bot_name: str = "Pipecat"
    prompt: Optional[str] = "You are a helpful LLM in a WebRTC call. Your goal is to demonstrate your capabilities in a succinct way. Respond to what the user said in a creative and helpful way in a few short sentences."
    deepgram_api_key: Optional[str] = None
    deepgram_voice: Optional[str] = "aura-asteria-en"
    deepgram_base_url: Optional[str] = "https://api.deepgram.com/v1/speak"
    openai_api_key: Optional[str] = None
    openai_model: Optional[str] = "gpt-4o"
    openai_base_url: Optional[str] = None


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
                vad_analyzer=SileroVADAnalyzer(params=VADParams(stop_secs=0.200)),
                vad_audio_passthrough=True
            )
        )

        stt = DeepgramSTTService(
            api_key=os.getenv("DEEPGRAM_API_KEY"),
            **({'url': url} if (url := os.getenv("DEEPGRAM_STT_URL")) else {})

        )

        tts = ClearableDeepgramTTSService(
            name="Voice",
            aiohttp_session=session,
            api_key=os.getenv("DEEPGRAM_API_KEY"),
            voice="aura-asteria-en",
            **({'base_url': url} if (url := os.getenv("DEEPGRAM_TTS_BASE_URL")) else {})
        )

        llm = OpenAILLMService(
            name="LLM",
            # To use OpenAI
            api_key=os.getenv("OPENAI_API_KEY"),
            model=os.getenv("OPENAI_MODEL"),
            base_url=os.getenv("OPENAI_BASE_URL")
        )

        messages = [
            {
                "role": "system",
                "content": """You are a helpful assistant in an audio conversation.

Your goal is to demonstrate your capabilities in a succinct way. Your output will be converted to audio so don't include special characters in your answers.

Respond to what the user said in a creative and helpful way. Be concise in your answers to basic questions. If you are asked to elaborate or tell a story, provide a longer response.
""",
            },
        ]

        avt = AudioVolumeTimer()
        tl = TranscriptionTimingLogger(avt)

        tma_in = LLMUserResponseAggregator(messages)
        tma_out = LLMAssistantResponseAggregator(messages)

        pipeline = Pipeline([
            transport.input(),   # Transport user input
            avt,
            stt,
            tl,
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

        # When a participant joins, start transcription for that participant so the
        # bot can "hear" and respond to them.
        # @ transport.event_handler("on_participant_joined")
        # async def on_participant_joined(transport, participant):
        #    transport.capture_participant_transcription(participant["id"])

        # When the participant leaves, we exit the bot.
        @transport.event_handler("on_participant_left")
        async def on_participant_left(transport, participant, reason):
            await task.queue_frame(EndFrame())
            
        # When the first participant joins, the bot should introduce itself.
        @ transport.event_handler("on_first_participant_joined")
        async def on_first_participant_joined(transport, participant):
            messages.append(
                {"role": "system", "content": "Please introduce yourself to the user."})
            await task.queue_frames([LLMMessagesFrame(messages)])

        # Handle "latency-ping" messages. The client will send app messages that look like
        # this:
        #   { "latency-ping": { ts: <client-side timestamp> }}
        #
        # We want to send an immediate pong back to the client from this handler function.
        # Also, we will push a frame into the top of the pipeline and send it after the
        #
        @ transport.event_handler("on_app_message")
        async def on_app_message(transport, message, sender):
            try:
                if "latency-ping" in message:
                    logger.debug(f"Received latency ping app message: {message}")
                    ts = message["latency-ping"]["ts"]
                    # Send immediately
                    transport.output().send_message(DailyTransportMessageFrame(
                        message={"latency-pong-msg-handler": {"ts": ts}},
                        participant_id=sender))
                    # And push to the pipeline for the Daily transport.output to send
                    await tma_in.push_frame(
                        DailyTransportMessageFrame(
                            message={"latency-pong-pipeline-delivery": {"ts": ts}},
                            participant_id=sender))
            except Exception as e:
                logger.debug(f"message handling error: {e} - {message}")

        runner = PipelineRunner()
        await runner.run(task)


# if __name__ == "__main__":
#     (url, token) = configure()
#     asyncio.run(main(url, token))

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Pipecat Bot")
    parser.add_argument("-s", "--settings", type=str, required=True, help="Pipecat bot settings")

    args, unknown = parser.parse_known_args()

    try:
        settings = BotSettings.model_validate_json(args.settings)
        asyncio.run(main(settings))
    except ValidationError as e:
        print(e)