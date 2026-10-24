import asyncio

import grpc
import pytest

from forge_agent.provider import Budget, ScriptedDemo
from forge_agent.schema import Next, Reply, RoleNext
from forge_agent.server import runtime_server
from generated import agent_pb2, agent_pb2_grpc
from test_roles import request


TOKEN = "controlled-local-transport-fixture-credential"


def test_two_clients_repeated_calls_have_admission_headroom() -> None:
    async def exercise() -> None:
        server = runtime_server(ScriptedDemo(), TOKEN)
        port = server.add_insecure_port("127.0.0.1:0")
        await server.start()
        try:
            async with grpc.aio.insecure_channel(f"127.0.0.1:{port}") as channel:
                stub = agent_pb2_grpc.RuntimeStub(channel)

                async def client() -> None:
                    for _ in range(100):
                        result = await stub.Collaborate(
                            agent_pb2.NextRequest(
                                context_json=request("planner")
                                .model_dump_json()
                                .encode()
                            ),
                            timeout=5,
                            metadata=(("authorization", "Bearer " + TOKEN),),
                        )
                        assert (
                            Reply.model_validate_json(result.response_json).status
                            == "ok"
                        )

                await asyncio.gather(client(), client())
        finally:
            await server.stop(0)

    asyncio.run(exercise())


def test_admission_and_provider_work_are_separately_bounded() -> None:
    class BlockingProvider:
        def __init__(self) -> None:
            self.started = asyncio.Event()
            self.release = asyncio.Event()
            self.active = self.peak = 0

        async def generate(self, value: Next | RoleNext, budget: Budget) -> Reply:
            self.active += 1
            self.peak = max(self.peak, self.active)
            if self.active == 2:
                self.started.set()
            try:
                await self.release.wait()
                return Reply(status="disabled", provider="TEST", model="bounded")
            finally:
                self.active -= 1

    async def exercise() -> None:
        provider = BlockingProvider()
        server = runtime_server(provider, TOKEN)
        port = server.add_insecure_port("127.0.0.1:0")
        await server.start()
        calls = []
        try:
            async with grpc.aio.insecure_channel(f"127.0.0.1:{port}") as channel:
                stub = agent_pb2_grpc.RuntimeStub(channel)

                async def call() -> object:
                    return await stub.Collaborate(
                        agent_pb2.NextRequest(
                            context_json=request("planner").model_dump_json().encode()
                        ),
                        timeout=5,
                        metadata=(("authorization", "Bearer " + TOKEN),),
                    )

                calls = [asyncio.create_task(call()) for _ in range(4)]
                await asyncio.wait_for(provider.started.wait(), 2)
                # Allow both queued requests to reach the actual gRPC server.
                await asyncio.sleep(0.1)
                with pytest.raises(grpc.aio.AioRpcError) as error:
                    await call()
                assert error.value.code() == grpc.StatusCode.RESOURCE_EXHAUSTED
                assert provider.active == provider.peak == 2
                provider.release.set()
                await asyncio.gather(*calls)
                assert provider.active == 0 and provider.peak == 2
        finally:
            provider.release.set()
            await asyncio.gather(*calls, return_exceptions=True)
            await server.stop(0)

    asyncio.run(exercise())
