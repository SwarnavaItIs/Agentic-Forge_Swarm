import asyncio
import json
from uuid import uuid4

import httpx
import pytest

from forge_agent.provider import Budget, EVAL_SYSTEM, OpenAI, response_schema
from forge_agent.schema import EvalProposal, Next


def request() -> Next:
    return Next(
        schema_version=1,
        attempt_id=str(uuid4()),
        generation=1,
        request_id=str(uuid4()),
        turn=0,
        task="Evaluation task add: fix arithmetic",
        context='{"files":{"solution.go":"untrusted source"}}',
        workspace_sha256="a" * 64,
        max_output_tokens=1024,
        evaluation_role="developer",
    )


def test_evaluation_strict_schema_and_prompt_ceiling() -> None:
    schema = response_schema(evaluation=True)
    assert len(json.dumps(schema).encode()) + len(EVAL_SYSTEM.encode()) + 1024 <= 8192
    with pytest.raises(ValueError):
        EvalProposal.model_validate(
            {
                "action": {
                    "schema_version": 1,
                    "id": str(uuid4()),
                    "tool": "shell",
                    "args": {"command": "publish"},
                }
            }
        )


def test_evaluation_uses_same_pinned_provider_usage_and_output_cap() -> None:
    def respond(value: httpx.Request) -> httpx.Response:
        payload = json.loads(value.content)
        assert payload["model"] == "pinned-test-model"
        assert payload["max_output_tokens"] == 1024
        assert payload["store"] is False
        assert payload["instructions"] == EVAL_SYSTEM
        assert "protected_test.go" not in payload["input"]
        action = {
            "action": {
                "schema_version": 1,
                "id": str(uuid4()),
                "tool": "final",
                "args": {"decision": "pass", "notes": "proposal only"},
            }
        }
        return httpx.Response(
            200,
            json={
                "status": "completed",
                "usage": {"input_tokens": 123, "output_tokens": 45},
                "output": [
                    {
                        "type": "message",
                        "content": [
                            {"type": "output_text", "text": json.dumps(action)}
                        ],
                    }
                ],
            },
        )

    provider = OpenAI(
        "test-key-never-exported-12345",
        "pinned-test-model",
        httpx.MockTransport(respond),
    )
    reply = asyncio.run(provider.generate(request(), Budget(1024, 1)))
    assert (
        reply.status == "ok" and reply.input_tokens == 123 and reply.output_tokens == 45
    )
    assert reply.action is not None and reply.action.tool == "final"
