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


class RoleSpec(Strict):
    schema_version: Literal[1]
    name: Literal["planner", "developer", "integration", "reviewer"]
    version: Annotated[int, Field(ge=1, le=3)]
    task_key: Annotated[str, Field(pattern=r"^[a-z][a-z0-9_-]{0,31}$")]
    write_scope: Annotated[
        list[Literal["add.go", "double.go", "score.go"]], Field(max_length=3)
    ]
    test_profile: Literal["add-v1", "double-v1", "score-v1", "math-v1"]
    max_calls: Annotated[int, Field(ge=1, le=8)]


class PlanTask(Strict):
    key: Annotated[str, Field(pattern=r"^[a-z][a-z0-9_-]{0,31}$")]
    description: Annotated[str, Field(min_length=1, max_length=500)]
    acceptance_criteria: Annotated[
        list[Annotated[str, Field(min_length=1, max_length=200)]],
        Field(min_length=1, max_length=3),
    ]
    depends_on: Annotated[list[str], Field(max_length=4)]
    required_capabilities: list[Literal["agent_go"]]
    write_scope: Annotated[
        list[Literal["add.go", "double.go", "score.go"]],
        Field(min_length=1, max_length=3),
    ]
    test_profile: Literal["add-v1", "double-v1", "score-v1", "math-v1"]
    max_calls: Annotated[int, Field(ge=1, le=8)]


class Plan(Strict):
    schema_version: Literal[1]
    repository_id: Annotated[str, Field(pattern=r"^[0-9a-f]{64}$")]
    base_revision: Annotated[str, Field(pattern=r"^([0-9a-f]{40}|[0-9a-f]{64})$")]
    policy: Literal["collaborative-go-v1"]
    tasks: Annotated[list[PlanTask], Field(min_length=1, max_length=4)]
    integration_profile: Literal["math-v1"]
    review_required: Literal[True]


class Finding(Strict):
    severity: Literal["info", "warning", "error"]
    file: Literal["add.go", "double.go", "score.go"]
    line: Annotated[int, Field(ge=1, le=200)]
    rationale: Annotated[str, Field(min_length=1, max_length=500)]


class Review(Strict):
    schema_version: Literal[1]
    candidate_sha256: Annotated[str, Field(pattern=r"^[0-9a-f]{64}$")]
    decision: Literal["pass", "reject"]
    findings: Annotated[list[Finding], Field(max_length=8)]


class MathPatch(Patch):
    path: Literal["add.go", "double.go", "score.go"]  # type: ignore[assignment]


class MathTest(Strict):
    profile: Literal["add-v1", "double-v1", "score-v1", "math-v1"]


class MathPatchAction(Strict):
    schema_version: Literal[1]
    id: str
    tool: Literal["apply_patch"]
    args: MathPatch


class MathTestAction(Strict):
    schema_version: Literal[1]
    id: str
    tool: Literal["run_test_profile"]
    args: MathTest


class PlanAction(Strict):
    schema_version: Literal[1]
    id: str
    tool: Literal["final_plan"]
    args: Plan


class ReviewAction(Strict):
    schema_version: Literal[1]
    id: str
    tool: Literal["final_review"]
    args: Review


CollabAction = Annotated[
    InspectAction
    | ReadAction
    | SearchAction
    | MathPatchAction
    | MathTestAction
    | PlanAction
    | ReviewAction,
    Field(discriminator="tool"),
]


class CollabProposal(Strict):
    action: CollabAction

    @field_validator("action")
    @classmethod
    def valid_id(cls, value: CollabAction) -> CollabAction:
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


class RoleNext(Strict):
    next: Next
    role: RoleSpec
    repository_id: Annotated[str, Field(pattern=r"^[0-9a-f]{64}$")]
    base_revision: Annotated[str, Field(pattern=r"^([0-9a-f]{40}|[0-9a-f]{64})$")]


class Reply(Strict):
    status: Literal[
        "ok",
        "disabled",
        "invalid_response",
        "timeout",
        "rate_limited",
        "provider_error",
    ]
    action: Action | CollabAction | None = None
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


def parse_collaboration(raw: str) -> CollabProposal:
    if len(raw.encode()) > 16384:
        raise ValueError("response ceiling")
    return CollabProposal.model_validate(
        json.loads(raw, object_pairs_hook=unique_object)
    )
