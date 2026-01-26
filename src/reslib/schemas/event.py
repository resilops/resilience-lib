from typing import Optional

from pydantic import BaseModel, ConfigDict, Field

from reslib.constants import ReslibEventEnum
from reslib.runtime.phases import ExecutionPhase


class ResLibEventPayload(BaseModel):
    """Base event payload that allows arbitrary additional fields."""

    model_config = ConfigDict(extra="allow")

    event_name: ReslibEventEnum = Field(..., description="Name of the event.")
    phase: ExecutionPhase = Field(
        ..., description="Name of the phase this event belongs to."
    )
    is_error: bool = Field(default=False, description="Is this event error related")
    error_msg: Optional[str] = Field(default=None, description="Error message")
