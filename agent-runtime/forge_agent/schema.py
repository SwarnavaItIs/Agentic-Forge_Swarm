import json
from typing import Annotated, Any, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, field_validator


class Strict(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)


class Empty(Strict):
    pass


class Read(Strict):
    path: Annotated[str, Field(min_length=1, max_length=200)]
    start: Annotated[int, Field(ge=1)]
    lines: Annotated[int, Field(ge=1, le=80)]


class Search(Strict):
    text: Annotated[str, Field(min_length=1, max_length=128)]


class Patch(Strict):
    path: Literal["sum.go"]
    before: Annotated[str, Field(min_length=1, max_length=2048)]
    after: Annotated[str, Field(max_length=2048)]
    workspace_sha256: Annotated[str, Field(pattern=r"^[0-9a-f]{64}$")]


class Test(Strict):
    profile: Literal["sum-v1"]


class InspectAction(Strict):
    schema_version: Literal[1]
    id: str
    tool: Literal["list_tree", "status", "diff", "final"]
    args: Empty


class ReadAction(Strict):
    schema_version: Literal[1]
    id: str
    tool: Literal["read_file"]
    args: Read


class SearchAction(Strict):
    schema_version: Literal[1]
    id: str
    tool: Literal["search_text"]
    args: Search


class PatchAction(Strict):
    schema_version: Literal[1]
    id: str
    tool: Literal["apply_patch"]
    args: Patch


class TestAction(Strict):
    schema_version: Literal[1]
    id: str
    tool: Literal["run_test_profile"]
    args: Test


Action = Annotated[
    InspectAction | ReadAction | SearchAction | PatchAction | TestAction,
    Field(discriminator="tool"),
]


class Proposal(Strict):
    action: Action

    @field_validator("action")
    @classmethod
    def valid_id(cls, value: Action) -> Action:
        if UUID(value.id).int == 0:
            raise ValueError("nonzero action identity required")
        return value


class Next(Strict):
    schema_version: Literal[1]
    attempt_id: str
    generation: Annotated[int, Field(ge=1)]
    request_id: str
    turn: Annotated[int, Field(ge=0, lt=12)]
    task: Annotated[str, Field(min_length=1, max_length=2000)]
    context: Annotated[str, Field(max_length=12288)]
    workspace_sha256: Annotated[str, Field(pattern=r"^[0-9a-f]{64}$")]
    max_output_tokens: Annotated[int, Field(ge=1, le=1024)]

    @field_validator("context")
    @classmethod
    def byte_bound(cls, value: str) -> str:
        if len(value.encode()) > 12288:
            raise ValueError("context byte ceiling")
        return value

    @field_validator("attempt_id", "request_id")
    @classmethod
    def identity(cls, value: str) -> str:
        if UUID(value).int == 0:
            raise ValueError("nonzero identity required")
        return value


class Reply(Strict):
    status: Literal[
        "ok",
        "disabled",
        "invalid_response",
        "timeout",
        "rate_limited",
        "provider_error",
    ]
    action: Action | None = None
    provider: str
    model: str
    request_id: str = ""
    input_tokens: Annotated[int, Field(ge=0)] | None = None
    output_tokens: Annotated[int, Field(ge=0)] | None = None
    latency_ms: Annotated[int, Field(ge=0)] = 0
    retry_after_ms: Annotated[int, Field(ge=0, le=1000)] = 0


def unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate JSON key")
        result[key] = value
    return result


def parse_proposal(raw: str) -> Proposal:
    if len(raw.encode()) > 16384:
        raise ValueError("response ceiling")
    return Proposal.model_validate(json.loads(raw, object_pairs_hook=unique_object))
