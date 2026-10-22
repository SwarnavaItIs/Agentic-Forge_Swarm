import asyncio
import json
from uuid import uuid4

import pytest
import httpx
from pydantic import ValidationError

from forge_agent.provider import Budget, OpenAI, ScriptedDemo, response_schema
from forge_agent.schema import CollabProposal, RoleNext, parse_collaboration


def request(role: str, turn: int = 0) -> RoleNext:
    return RoleNext.model_validate(
        {
            "next": {
                "schema_version": 1,
                "attempt_id": str(uuid4()),
                "generation": 1,
                "request_id": str(uuid4()),
                "turn": turn,
                "task": "controlled TEST/DEMO",
                "context": "untrusted source",
                "workspace_sha256": "a" * 64,
                "max_output_tokens": 1024,
            },
            "role": {
                "schema_version": 1,
                "name": role,
                "version": 1,
                "task_key": "plan" if role == "planner" else "review",
                "write_scope": [],
                "test_profile": "math-v1",
                "max_calls": 4,
            },
            "repository_id": "b" * 64,
            "base_revision": "c" * 40,
        }
    )


def test_scripted_multirole_strict_plan_and_review() -> None:
    for role, tool in [("planner", "final_plan"), ("reviewer", "final_review")]:
        reply = asyncio.run(ScriptedDemo().generate(request(role, 1), Budget(1024, 1)))
        assert reply.provider == "scripted_TEST_DEMO"
        assert reply.action is not None and reply.action.tool == tool
        CollabProposal.model_validate({"action": reply.action.model_dump()})
    schema = json.dumps(response_schema(True))
    assert '"final_plan"' in schema and '"final_review"' in schema
    assert "discriminator" not in schema and "oneOf" not in schema


@pytest.mark.parametrize(
    "raw",
    ['{"action":{"tool":"shell"}}', '{"action":null}', '{"action":{},"action":{}}'],
)
def test_role_response_rejects_unbounded_or_ambiguous_actions(raw: str) -> None:
    with pytest.raises((ValueError, ValidationError)):
        parse_collaboration(raw)


def test_collaboration_does_not_enlarge_legacy_output_schema() -> None:
    assert '"final_plan"' not in json.dumps(response_schema())
    assert len(json.dumps(response_schema()).encode()) + 1024 < 8192
    assert len(json.dumps(response_schema(True)).encode()) + 2048 < 10240


def test_openai_collaboration_uses_real_adapter_and_strict_role_schema() -> None:
    value = request("planner", 1)
    scripted = asyncio.run(ScriptedDemo().generate(value, Budget(1024, 1)))
    assert scripted.action is not None
    proposed = json.dumps({"action": scripted.action.model_dump()})
    observed: list[dict[str, object]] = []

    def handle(call: httpx.Request) -> httpx.Response:
        body = json.loads(call.content)
        observed.append(body)
        assert str(call.url) == "https://api.openai.com/v1/responses"
        assert body["store"] is False and body["max_output_tokens"] == 1024
        assert json.loads(body["input"])["role"]["name"] == "planner"
        assert body["text"]["format"]["strict"] is True
        assert "final_plan" in json.dumps(body["text"]["format"]["schema"])
        return httpx.Response(
            200,
            headers={"x-request-id": "role-contract"},
            json={
                "status": "completed",
                "usage": {"input_tokens": 30, "output_tokens": 50},
                "output": [
                    {
                        "type": "message",
                        "content": [{"type": "output_text", "text": proposed}],
                    }
                ],
            },
        )

    adapter = OpenAI(
        "contract-test-key-not-a-real-secret",
        "configured-model",
        httpx.MockTransport(handle),
    )
    reply = asyncio.run(adapter.generate(value, Budget(1024, 1)))
    assert reply.status == "ok" and reply.provider == "openai"
    assert reply.action is not None and reply.action.tool == "final_plan"
    assert reply.request_id == "role-contract" and reply.input_tokens == 30
    assert len(observed) == 1
