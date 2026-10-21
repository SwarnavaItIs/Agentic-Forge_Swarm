import asyncio
import hmac
import json
import os
from typing import Any

import grpc

from forge_agent.provider import Budget, Disabled, OpenAI, Provider, ScriptedDemo
from forge_agent.schema import Next, unique_object
from generated import agent_pb2, agent_pb2_grpc


class Runtime(agent_pb2_grpc.RuntimeServicer):
    def __init__(self, provider: Provider, token: str) -> None:
        if not 32 <= len(token) <= 256:
            raise ValueError("runtime authentication required")
        self.provider, self.token = provider, token
        self.slots = asyncio.Semaphore(2)

    async def NextAction(self, request: Any, context: Any) -> Any:
        values = [v for k, v in context.invocation_metadata() if k == "authorization"]
        if len(values) != 1 or not hmac.compare_digest(
            values[0], "Bearer " + self.token
        ):
            await context.abort(
                grpc.StatusCode.UNAUTHENTICATED, "runtime authentication"
            )
        remaining = context.time_remaining()
        if remaining is None or not 0 < remaining <= 11:
            await context.abort(
                grpc.StatusCode.INVALID_ARGUMENT, "bounded deadline required"
            )
        try:
            if len(request.context_json) > 32768:
                raise ValueError("payload ceiling")
            value = Next.model_validate(
                json.loads(request.context_json, object_pairs_hook=unique_object)
            )
        except (ValueError, RecursionError):
            await context.abort(
                grpc.StatusCode.INVALID_ARGUMENT, "invalid next-action input"
            )
        async with self.slots:
            async with asyncio.timeout(max(0.001, context.time_remaining())):
                reply = await self.provider.generate(
                    value,
                    Budget(value.max_output_tokens, min(9.5, context.time_remaining())),
                )
                wire = reply.model_dump_json().encode()
                if len(wire) > 32768:
                    await context.abort(
                        grpc.StatusCode.RESOURCE_EXHAUSTED, "reply ceiling"
                    )
                return agent_pb2.NextReply(response_json=wire)


async def serve() -> None:
    token = os.environ.get("AGENT_RUNTIME_TOKEN", "")
    selected = os.environ.get("AGENT_PROVIDER", "disabled")
    provider: Provider = Disabled()
    if selected == "scripted_TEST_DEMO":
        if os.environ.get("AGENT_TEST_DEMO") != "true":
            raise ValueError("scripted provider requires explicit TEST/DEMO mode")
        provider = ScriptedDemo()
    elif selected == "openai":
        if os.environ.get("AGENT_LIVE_ENABLED") != "true":
            raise ValueError(
                "live provider disabled; configure authorized spending first"
            )
        # Config is installed by the operator, never supplied by a job/model.
        with open("pricing.json", encoding="utf-8") as source:
            config = json.load(source)
        provider = OpenAI(os.environ.get("OPENAI_API_KEY", ""), config["model"])
    elif selected != "disabled":
        raise ValueError("unknown provider; no fallback")
    server = grpc.aio.server(
        options=[
            ("grpc.max_receive_message_length", 32768),
            ("grpc.max_send_message_length", 32768),
        ],
        maximum_concurrent_rpcs=2,
    )
    # grpcio-tools 1.76 generates an unannotated registration helper.
    agent_pb2_grpc.add_RuntimeServicer_to_server(Runtime(provider, token), server)  # type: ignore[no-untyped-call]
    # The packaged container has no host mounts or control-plane credentials.
    # Plaintext is restricted to explicit local mode and a loopback host publish.
    if os.environ.get("AGENT_INSECURE_LOCAL") != "true":
        raise ValueError("local runtime requires explicit loopback transport mode")
    server.add_insecure_port(os.environ.get("AGENT_LISTEN", "127.0.0.1:9091"))
    await server.start()
    await server.wait_for_termination()


if __name__ == "__main__":
    asyncio.run(serve())
