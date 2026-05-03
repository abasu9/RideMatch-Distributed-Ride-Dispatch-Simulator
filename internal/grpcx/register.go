package grpcx

import (
	"context"
	"fmt"
	"reflect"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/dynamicpb"
)

// DynUnary is a protobuf-generic unary handler backed by dynamic messages.
type DynUnary func(ctx context.Context, req *dynamicpb.Message, resp *dynamicpb.Message) error

// RegisterUnaryService wires a protobuf service descriptor to dynamic handlers.
//
// impl must be a non-nil pointer (the concrete server object). grpc uses reflect.TypeOf(impl)
// only for sanity checks against the HandlerType embedded in grpc.ServiceDesc.
func RegisterUnaryService(s *grpc.Server, fd protoreflect.FileDescriptor, serviceName string, impl any, handlers map[string]DynUnary) {
	if impl == nil || reflect.ValueOf(impl).Kind() != reflect.Ptr {
		panic("grpcx: impl must be a non-nil pointer to the service implementation")
	}

	svc := fd.Services().ByName(protoreflect.Name(serviceName))
	if svc == nil {
		panic(fmt.Sprintf("grpcx: missing service %s in %s", serviceName, fd.Path()))
	}

	desc := grpc.ServiceDesc{
		ServiceName: string(svc.FullName()),
		HandlerType: reflect.TypeOf(impl),
	}

	servicePath := "/" + string(svc.FullName()) + "/"

	for i := 0; i < svc.Methods().Len(); i++ {
		md := svc.Methods().Get(i)
		methodName := string(md.Name())
		h, ok := handlers[methodName]
		if !ok {
			panic(fmt.Sprintf("grpcx: missing handler for %s", md.FullName()))
		}

		fullMethod := servicePath + methodName
		desc.Methods = append(desc.Methods, grpc.MethodDesc{
			MethodName: methodName,
			Handler:    newDynUnaryHandler(md, fullMethod, h),
		})
	}

	s.RegisterService(&desc, impl)
}

func newDynUnaryHandler(md protoreflect.MethodDescriptor, fullMethod string, fn DynUnary) func(srv any, ctx context.Context, dec func(any) error, interceptor grpc.UnaryServerInterceptor) (any, error) {
	return func(srv any, ctx context.Context, dec func(any) error, interceptor grpc.UnaryServerInterceptor) (any, error) {
		in := dynamicpb.NewMessage(md.Input())
		if err := dec(in); err != nil {
			return nil, err
		}

		handler := func(ctx context.Context, req any) (any, error) {
			out := dynamicpb.NewMessage(md.Output())
			if err := fn(ctx, req.(*dynamicpb.Message), out); err != nil {
				return nil, err
			}
			return out, nil
		}

		if interceptor == nil {
			return handler(ctx, in)
		}

		info := &grpc.UnaryServerInfo{FullMethod: fullMethod}
		return interceptor(ctx, in, info, handler)
	}
}
