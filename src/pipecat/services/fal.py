#
# Copyright (c) 2024, Daily
#
# SPDX-License-Identifier: BSD 2-Clause License
#

import aiohttp
import io
import os

from PIL import Image
from pydantic import BaseModel
from typing import AsyncGenerator, Optional, Union, Dict

from pipecat.frames.frames import ErrorFrame, Frame, URLImageRawFrame
from pipecat.services.ai_services import ImageGenService

from loguru import logger

try:
    import fal_client
except ModuleNotFoundError as e:
    logger.error(f"Exception: {e}")
    logger.error(
        "In order to use Fal, you need to `pip install pipecat-ai[fal]`. Also, set `FAL_KEY` environment variable.")
    raise Exception(f"Missing module: {e}")


class FalImageGenService(ImageGenService):
    class InputParams(BaseModel):
        seed: Optional[int] = None
        num_inference_steps: int = 8
        num_images: int = 1
        image_size: Union[str, Dict[str, int]] = "square_hd"
        expand_prompt: bool = False
        enable_safety_checker: bool = True
        format: str = "png"

    def __init__(
        self,
        *,
        aiohttp_session: aiohttp.ClientSession,
        params: InputParams,
        model: str = "fal-ai/fast-sdxl",
        key: str | None = None,
    ):
        super().__init__()
        self._model = model
        self._params = params
        self._aiohttp_session = aiohttp_session
        if key:
            os.environ["FAL_KEY"] = key

    async def run_image_gen(self, prompt: str) -> AsyncGenerator[Frame, None]:
        logger.debug(f"Generating image from prompt: {prompt}")

        response = await fal_client.run_async(
            self._model,
            arguments={"prompt": prompt, **self._params.model_dump(exclude_none=True)}
        )

        image_url = response["images"][0]["url"] if response else None

        if not image_url:
            logger.error(f"{self} error: image generation failed")
            yield ErrorFrame("Image generation failed")
            return

        logger.debug(f"Image generated at: {image_url}")

        # Load the image from the url
        logger.debug(f"Downloading image {image_url} ...")
        async with self._aiohttp_session.get(image_url) as response:
            logger.debug(f"Downloaded image {image_url}")
            image_stream = io.BytesIO(await response.content.read())
            image = Image.open(image_stream)

            frame = URLImageRawFrame(
                url=image_url,
                image=image.tobytes(),
                size=image.size,
                format=image.format)
            yield frame
