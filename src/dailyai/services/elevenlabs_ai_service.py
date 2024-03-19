import aiohttp
import os
import requests
import time

from typing import AsyncGenerator

from dailyai.services.ai_services import TTSService


class ElevenLabsTTSService(TTSService):

    def __init__(
        self,
        *,
        aiohttp_session: aiohttp.ClientSession,
        api_key,
        voice_id,
        model="eleven_turbo_v2",
    ):
        super().__init__()

        self._api_key = api_key
        self._voice_id = voice_id
        self._aiohttp_session = aiohttp_session
        self._model = model

    async def run_tts(self, sentence) -> AsyncGenerator[bytes, None]:
        url = f"https://api.elevenlabs.io/v1/text-to-speech/{self._voice_id}/stream"
        payload = {"text": sentence, "model_id": self._model}
        querystring = {
            "output_format": "pcm_16000",
            "optimize_streaming_latency": 2}
        headers = {
            "xi-api-key": self._api_key,
            "Content-Type": "application/json",
        }

        async with self._aiohttp_session.post(
            url, json=payload, headers=headers, params=querystring
        ) as r:
            if r.status != 200:
                self.logger.error(
                    f"audio fetch status code: {r.status}, error: {r.text}"
                )
                return

            async for chunk in r.content:
                if chunk:
                    yield chunk
