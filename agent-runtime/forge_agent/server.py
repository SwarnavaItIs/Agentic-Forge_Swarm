import asyncio
import hmac
import json
import os
from typing import Any

import grpc

from forge_agent.provider import Budget, Disabled, OpenAI, Provider, ScriptedDemo
from forge_agent.schema import Next, RoleNext, unique_object
from forge_agent.telemetry import provider_span, setup as setup_telemetry
from generated import agent_pb2, agent_pb2_grpc


class Runtime(agent_pb2_grpc.RuntimeServicer):
    def __init__(self, provider: Provider, token: str) -> None:
        if not 32 <= len(token) <= 256:
            raise ValueError("runtime authentication required")
        self.provider, self.token = provider, token
        self.slots = asyncio.Semaphore(2)

    async def NextAction(self, request: Any, context: Any) -> Any:
        return await self._next(request, context, False)

    async def Collaborate(self, request: Any, context: Any) -> Any:
        return await self._next(request, context, True)

    async def _next(self, request: Any, context: Any, collaboration: bool) -> Any:
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
            value = (RoleNext if collaboration else Next).model_validate(
                json.loads(request.context_json, object_pairs_hook=unique_object)
            )
            if (
                isinstance(value, Next)
                and value.evaluation_role is not None
                and os.environ.get("AGENT_EVAL_ENABLED") != "true"
            ):
                raise ValueError("evaluation runtime explicitly disabled")
        except (ValueError, RecursionError):
            await context.abort(
                grpc.StatusCode.INVALID_ARGUMENT, "invalid next-action input"
            )
        async with self.slots:
            async with asyncio.timeout(max(0.001, context.time_remaining())):
                with provider_span(context.invocation_metadata()) as span:
                    reply = await self.provider.generate(
                        value,
                        Budget(
                            value.next.max_output_tokens
                            if isinstance(value, RoleNext)
                            else value.max_output_tokens,
                            min(9.5, context.time_remaining()),
                        ),
                    )
                    span.set_attribute("outcome", reply.status)
                wire = reply.model_dump_json().encode()
                if len(wire) > 32768:
                    await context.abort(
                        grpc.StatusCode.RESOURCE_EXHAUSTED, "reply ceiling"
                    )
                return agent_pb2.NextReply(response_json=wire)


def runtime_server(provider: Provider, token: str) -> Any:
    # grpcio can still count a completed RPC while its client has already sent
    # the next request. Keep bounded admission headroom for two clients; the
    # Runtime semaphore independently limits actual provider work to two calls.
    server = grpc.aio.server(
        options=[
            ("grpc.max_receive_message_length", 32768),
            ("grpc.max_send_message_length", 32768),
        ],
        maximum_concurrent_rpcs=4,
    )
    agent_pb2_grpc.add_RuntimeServicer_to_server(Runtime(provider, token), server)  # type: ignore[no-untyped-call]
    return server


async def serve() -> None:
    setup_telemetry()
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
    server = runtime_server(provider, token)
    # The packaged container has no host mounts or control-plane credentials.
    # Plaintext is restricted to explicit local mode and a loopback host publish.
    if os.environ.get("AGENT_INSECURE_LOCAL") != "true":
        raise ValueError("local runtime requires explicit loopback transport mode")
    server.add_insecure_port(os.environ.get("AGENT_LISTEN", "127.0.0.1:9091"))
    await server.start()
    await server.wait_for_termination()


if __name__ == "__main__":
    asyncio.run(serve())
