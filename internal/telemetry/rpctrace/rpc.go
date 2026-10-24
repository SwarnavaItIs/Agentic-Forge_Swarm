package rpctrace

import (
	"forgeswarm/api/agent/v1"
	"forgeswarm/api/worker/v1"
	"forgeswarm/internal/telemetry"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc/stats"
)

// Only generated method names are eligible. An arbitrary unknown RPC path may
// not allocate a new metric label or cause arbitrary metadata to be exported.
func allowed(info *stats.RPCTagInfo) bool {
	for _, description := range []struct {
		name    string
		methods []string
	}{
		{workerv1.Coordinator_ServiceDesc.ServiceName, workerMethods()},
		{agentv1.Runtime_ServiceDesc.ServiceName, []string{"NextAction", "Collaborate"}},
	} {
		for _, method := range description.methods {
			if info.FullMethodName == "/"+description.name+"/"+method {
				return true
			}
		}
	}
	return false
}
func workerMethods() []string {
	v := make([]string, 0, len(workerv1.Coordinator_ServiceDesc.Methods))
	for _, m := range workerv1.Coordinator_ServiceDesc.Methods {
		v = append(v, m.MethodName)
	}
	return v
}
func Client() stats.Handler {
	return otelgrpc.NewClientHandler(otelgrpc.WithPropagators(telemetry.Propagator{}), otelgrpc.WithFilter(allowed))
}
func Server() stats.Handler {
	return otelgrpc.NewServerHandler(otelgrpc.WithPropagators(telemetry.Propagator{}), otelgrpc.WithFilter(allowed))
}
