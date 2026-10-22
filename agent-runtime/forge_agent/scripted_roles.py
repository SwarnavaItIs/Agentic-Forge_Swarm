"""Deterministic multi-role TEST/DEMO fixture. No live capability claim."""

from typing import Any
from uuid import NAMESPACE_URL, uuid5

from forge_agent.schema import CollabProposal, Reply, RoleNext


def scripted_role(request: RoleNext) -> Reply:
    n, role = request.next, request.role
    tool: str
    args: dict[str, Any]
    if role.name == "planner":
        tool, args = "list_tree", {}
        if n.turn > 0:
            tasks = []
            for name, description, deps in [
                ("add", "Make Add return a+b.", []),
                ("double", "Make Double return 2*a.", []),
                (
                    "score",
                    "Read predecessor source and make Score return 3*a+b.",
                    ["add", "double"],
                ),
            ]:
                tasks.append(
                    {
                        "key": name,
                        "description": description,
                        "acceptance_criteria": [description],
                        "depends_on": deps,
                        "required_capabilities": ["agent_go"],
                        "write_scope": [name + ".go"],
                        "test_profile": name + "-v1",
                        "max_calls": 4,
                    }
                )
            if "TEST_DEMO_OVERLAP" in n.task:
                tasks = [
                    {
                        "key": key,
                        "description": "Fix Add in an isolated source snapshot.",
                        "acceptance_criteria": ["Add returns the sum"],
                        "depends_on": [],
                        "required_capabilities": ["agent_go"],
                        "write_scope": ["add.go"],
                        "test_profile": "add-v1",
                        "max_calls": 4,
                    }
                    for key in ["add_first", "add_second"]
                ]
            elif "TEST_DEMO_SEMANTIC_CONFLICT" in n.task:
                tasks = tasks[:2]
            tool, args = (
                "final_plan",
                {
                    "schema_version": 1,
                    "repository_id": request.repository_id,
                    "base_revision": request.base_revision,
                    "policy": "collaborative-go-v1",
                    "tasks": tasks,
                    "integration_profile": "math-v1",
                    "review_required": True,
                },
            )
    elif role.name == "developer":
        filename = role.write_scope[0]
        before, after = {
            "add.go": ("return a - b", "return a + b"),
            "double.go": ("return a * 3", "return a * 2"),
            "score.go": ("return a - b", "return 3*a + b"),
        }[filename]
        steps: list[tuple[str, dict[str, Any]]] = [
            ("read_file", {"path": filename, "start": 1, "lines": 80}),
            (
                "apply_patch",
                {
                    "path": filename,
                    "before": before,
                    "after": after,
                    "workspace_sha256": n.workspace_sha256,
                },
            ),
            ("run_test_profile", {"profile": role.test_profile}),
            ("final", {}),
        ]
        tool, args = steps[min(n.turn, 3)]
        if n.turn == 1 and before not in n.context.split("LAST_OBSERVATION", 1)[0]:
            tool, args = "status", {}
    else:
        tool, args = "diff", {}
        if n.turn > 0:
            reject = "TEST_DEMO_REJECT_ALWAYS" in n.task or (
                "TEST_DEMO_REJECT_ONCE" in n.task and role.version == 1
            )
            findings = (
                [
                    {
                        "severity": "warning",
                        "file": "score.go",
                        "line": 2,
                        "rationale": "Explicit scripted TEST/DEMO rejection scenario.",
                    }
                ]
                if reject
                else []
            )
            tool, args = (
                "final_review",
                {
                    "schema_version": 1,
                    "candidate_sha256": n.workspace_sha256,
                    "decision": "reject" if reject else "pass",
                    "findings": findings,
                },
            )
            if "TEST_DEMO_STALE_REVIEW" in n.task:
                args["candidate_sha256"] = request.repository_id
    action = CollabProposal.model_validate(
        {
            "action": {
                "schema_version": 1,
                "id": str(uuid5(NAMESPACE_URL, n.request_id)),
                "tool": tool,
                "args": args,
            }
        }
    ).action
    return Reply(
        status="ok",
        action=action,
        provider="scripted_TEST_DEMO",
        model="multi-role-fixture-v1",
        request_id=n.request_id,
        input_tokens=len(n.context.encode()) + len(n.task.encode()),
        output_tokens=768,
    )
