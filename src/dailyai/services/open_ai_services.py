import aiohttp
from PIL import Image
import io
import time
import base64
from openai import AsyncOpenAI, AsyncStream

import json
from collections.abc import AsyncGenerator

from openai.types.chat import (
    ChatCompletion,
    ChatCompletionChunk,
    ChatCompletionMessageParam,
)

from dailyai.services.ai_services import LLMService, ImageGenService, VisionService
from dailyai.services.openai_api_llm_service import BaseOpenAILLMService
from dailyai.pipeline.frames import TextFrame


class OpenAILLMService(BaseOpenAILLMService):

    def __init__(self, model="gpt-4", * args, **kwargs):
        super().__init__(model, *args, **kwargs)


class OpenAIImageGenService(ImageGenService):

    def __init__(
        self,
        *,
        image_size: str,
        aiohttp_session: aiohttp.ClientSession,
        api_key,
        model="dall-e-3",
    ):
        super().__init__(image_size=image_size)
        self._model = model
        self._client = AsyncOpenAI(api_key=api_key)
        self._aiohttp_session = aiohttp_session

    async def run_image_gen(self, sentence) -> tuple[str, bytes]:
        self.logger.info("Generating OpenAI image", sentence)

        image = await self._client.images.generate(
            prompt=sentence,
            model=self._model,
            n=1,
            size=self.image_size
        )
        image_url = image.data[0].url
        if not image_url:
            raise Exception("No image provided in response", image)

        # Load the image from the url
        async with self._aiohttp_session.get(image_url) as response:
            image_stream = io.BytesIO(await response.content.read())
            image = Image.open(image_stream)
            return (image_url, image.tobytes())


class OpenAIVisionService(VisionService):
    def __init__(
        self,
        *,
        model="gpt-4-vision-preview",
        api_key,
    ):
        self._model = model
        self._client = AsyncOpenAI(api_key=api_key)

    async def run_vision(self, prompt: str, image: bytes):
        base64_image = base64.b64encode(image).decode('utf-8')
        messages = [
            {
                "role": "user",
                "content": [
                    {"type": "text", "text": prompt},
                    {
                        "type": "image_url",
                        "image_url": {
                            "url": f"data:image/jpeg;base64,{base64_image}"
                        },
                    },
                ],
            }
        ]
        chunks: AsyncStream[ChatCompletionChunk] = (
            await self._client.chat.completions.create(
                model=self._model,
                stream=True,
                messages=messages,
            )
        )
        async for chunk in chunks:
            print(f"!!! chunk: {chunk}")
            yield TextFrame(chunk)
