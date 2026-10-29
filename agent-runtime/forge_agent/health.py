"""Authenticated schema probe: invalid input is rejected before provider work."""

import sys

import grpc

from forge_agent.server import runtime_token
from generated import agent_pb2, agent_pb2_grpc


def main() -> int:
    try:
        with grpc.insecure_channel("127.0.0.1:9091") as channel:
            # grpcio-tools emits an untyped constructor (same boundary as the
            # server registration); request/response messages remain generated.
            stub = agent_pb2_grpc.RuntimeStub(channel)  # type: ignore[no-untyped-call]
            stub.NextAction(
                agent_pb2.NextRequest(context_json=b"{}"),
                timeout=1,
                metadata=(("authorization", "Bearer " + runtime_token()),),
            )
    except grpc.RpcError as error:
        return 0 if error.code() == grpc.StatusCode.INVALID_ARGUMENT else 1
    except (OSError, ValueError):
        return 1
    return 1


if __name__ == "__main__":
    sys.exit(main())
