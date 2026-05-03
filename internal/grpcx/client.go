package grpcx

import (
	"context"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/dynamicpb"
)

// UnaryCall performs a blocking unary RPC using dynamic request/response messages.
func UnaryCall(ctx context.Context, cc *grpc.ClientConn, fullMethod string, req *dynamicpb.Message, resp *dynamicpb.Message, opts ...grpc.CallOption) error {
	return cc.Invoke(ctx, fullMethod, req, resp, opts...)
}
