from dataclasses import dataclass
from typing import Any, List

from dailyai.services.openai_llm_context import OpenAILLMContext


class Frame:
    def __str__(self):
        return f"{self.__class__.__name__}"


class ControlFrame(Frame):
    # Control frames should contain no instance data, so
    # equality is based solely on the class.
    def __eq__(self, other):
        return isinstance(other, self.__class__)


class StartFrame(ControlFrame):
    """Used (but not required) to start a pipeline, and is also used to
    indicate that an interruption has ended and the transport should start
    processing frames again."""
    pass


class EndFrame(ControlFrame):
    """Indicates that a pipeline has ended and frame processors and pipelines
    should be shut down. If the transport receives this frame, it will stop
    sending frames to its output channel(s) and close all its threads."""
    pass


class EndPipeFrame(ControlFrame):
    """Indicates that a pipeline has ended but that the transport should
    continue processing. This frame is used in parallel pipelines and other
    sub-pipelines."""
    pass


class PipelineStartedFrame(ControlFrame):
    """
    Used by the transport to indicate that execution of a pipeline is starting
    (or restarting). It should be the first frame your app receives when it
    starts, or when an interruptible pipeline has been interrupted.
    """

    pass


class LLMResponseStartFrame(ControlFrame):
    """Used to indicate the beginning of an LLM response. Following TextFrames
    are part of the LLM response until an LLMResponseEndFrame"""
    pass


class LLMResponseEndFrame(ControlFrame):
    """Indicates the end of an LLM response."""
    pass


@dataclass()
class AudioFrame(Frame):
    """A chunk of audio. Will be played by the transport if the transport's mic
    has been enabled."""
    data: bytes

    def __str__(self):
        return f"{self.__class__.__name__}, size: {len(self.data)} B"


@dataclass()
class ImageFrame(Frame):
    """An image. Will be shown by the transport if the transport's camera is
    enabled."""
    url: str | None
    image: bytes

    def __str__(self):
        return f"{self.__class__.__name__}, url: {self.url}, image size: {len(self.image)} B"


@dataclass()
class SpriteFrame(Frame):
    """An animated sprite. Will be shown by the transport if the transport's
    camera is enabled. Will play at the framerate specified in the transport's
    `fps` constructor parameter."""
    images: list[bytes]

    def __str__(self):
        return f"{self.__class__.__name__}, list size: {len(self.images)}"


@dataclass()
class TextFrame(Frame):
    """A chunk of text. Emitted by LLM services, consumed by TTS services, can
    be used to send text through pipelines."""
    text: str

    def __str__(self):
        return f'{self.__class__.__name__}: "{self.text}"'


@dataclass()
class TranscriptionQueueFrame(TextFrame):
    """A text frame with transcription-specific data. Will be placed in the
    transport's receive queue when a participant speaks."""
    participantId: str
    timestamp: str


@dataclass()
class LLMMessagesQueueFrame(Frame):
    """A frame containing a list of LLM messages. Used to signal that an LLM
    service should run a chat completion and emit an LLMStartFrames, TextFrames
    and an LLMEndFrame.
    Note that the messages property on this class is mutable, and will be
    be updated by various ResponseAggregator frame processors."""
    messages: List[dict]


@dataclass()
class OpenAILLMContextFrame(Frame):
    """Like an LLMMessagesQueueFrame, but with extra context specific to the
    OpenAI API. The context in this message is also mutable, and will be
    changed by the OpenAIContextAggregator frame processor."""
    context: OpenAILLMContext


@dataclass()
class ReceivedAppMessageFrame(Frame):
    message: Any
    sender: str

    def __str__(self):
        return f"ReceivedAppMessageFrame: sender: {self.sender}, message: {self.message}"


@dataclass()
class SendAppMessageFrame(Frame):
    message: Any
    participantId: str | None

    def __str__(self):
        return f"SendAppMessageFrame: participantId: {self.participantId}, message: {self.message}"


class UserStartedSpeakingFrame(Frame):
    """Emitted by VAD to indicate that a participant has started speaking.
    This can be used for interruptions or other times when detecting that
    someone is speaking is more important than knowing what they're saying
    (as you will with a TranscriptionFrame)"""
    pass


class UserStoppedSpeakingFrame(Frame):
    """Emitted by the VAD to indicate that a user stopped speaking."""
    pass


class BotStartedSpeakingFrame(Frame):
    pass


class BotStoppedSpeakingFrame(Frame):
    pass


@dataclass()
class LLMFunctionStartFrame(Frame):
    """Emitted when the LLM receives the beginning of a function call
    completion. A frame processor can use this frame to indicate that it should
    start preparing to make a function call, if it can do so in the absence of
    any arguments."""
    function_name: str


@dataclass()
class LLMFunctionCallFrame(Frame):
    """Emitted when the LLM has received an entire function call completion."""
    function_name: str
    arguments: str


@dataclass()
class VideoImageFrame(Frame):
    """Contains a still image from a partcipant's video stream."""
    participantId: str
    image: bytes

    # def __str__(self):
    #     return f"{self.__class__.__name__}, participantId: {self.participantId}, image size: {len(self.image)} B"


@dataclass()
class VisionFrame(Frame):
    prompt: str
    image: bytes

    # def __str__(self):
    #     return f"{self.__class__.__name__}, prompt: {self.prompt}, image size: {len(self.image)} B"


@dataclass()
class RequestVideoImageFrame(Frame):
    """Send to the transport to request a new video image from a specific participant. Leave participantId
    empty to request a frame from all participants."""
    participantId: str | None
