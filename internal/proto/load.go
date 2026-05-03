package proto

import (
	"context"
	"fmt"
	"strings"

	"github.com/bufbuild/protocompile"
	"github.com/bufbuild/protocompile/linker"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/dynamicpb"
)

// Schema exposes linked protobuf descriptors compiled from embedded protobuf sources.
type Schema struct {
	Files              *protoregistry.Files
	DriverDescriptor   protoreflect.FileDescriptor
	RiderDescriptor    protoreflect.FileDescriptor
	MatchingDescriptor protoreflect.FileDescriptor

	MatchingFindNearestGRPCPath string
	RiderRequestRideGRPCPath    string
	RiderGetRideGRPCPath        string
}

func MustLoadCompiled() *Schema {
	s, err := LoadCompiled(context.Background())
	if err != nil {
		panic(err)
	}
	return s
}

func LoadCompiled(ctx context.Context) (*Schema, error) {
	embedded := mustReadSchemas()

	cmp := protocompile.Compiler{
		Resolver: protocompile.WithStandardImports(
			protocompile.ResolverFunc(func(path string) (protocompile.SearchResult, error) {
				src, ok := embedded[path]
				if !ok {
					return protocompile.SearchResult{}, protoregistry.NotFound
				}
				return protocompile.SearchResult{Source: strings.NewReader(src)}, nil
			}),
		),
		SourceInfoMode: protocompile.SourceInfoNone,
	}

	compiled, err := cmp.Compile(ctx, "driver.proto", "matching.proto", "rider.proto")
	if err != nil {
		return nil, err
	}

	filesReg := new(protoregistry.Files)

	for _, f := range compiled {
		res := f.(linker.Result)

		if _, err := filesReg.FindFileByPath(res.Path()); err == nil {
			continue
		}

		fd, err := protodesc.NewFile(res.FileDescriptorProto(), filesReg)
		if err != nil {
			return nil, fmt.Errorf("link descriptor for %s: %w", res.Path(), err)
		}

		if err := filesReg.RegisterFile(fd); err != nil {
			return nil, err
		}
	}

	driverFile, err := filesReg.FindFileByPath("driver.proto")
	if err != nil {
		return nil, err
	}
	matchFile, err := filesReg.FindFileByPath("matching.proto")
	if err != nil {
		return nil, err
	}
	riderFile, err := filesReg.FindFileByPath("rider.proto")
	if err != nil {
		return nil, err
	}

	return &Schema{
		Files:                       filesReg,
		DriverDescriptor:            driverFile,
		MatchingDescriptor:          matchFile,
		RiderDescriptor:             riderFile,
		MatchingFindNearestGRPCPath: grpcFullMethod(matchFile, "MatchingService", "FindNearestDriver"),
		RiderRequestRideGRPCPath:    grpcFullMethod(riderFile, "RiderService", "RequestRide"),
		RiderGetRideGRPCPath:        grpcFullMethod(riderFile, "RiderService", "GetRideStatus"),
	}, nil
}

func grpcFullMethod(fd protoreflect.FileDescriptor, serviceName, methodName string) string {
	svc := fd.Services().ByName(protoreflect.Name(serviceName))
	if svc == nil {
		panic(fmt.Sprintf("missing service %s in %s", serviceName, fd.Path()))
	}
	m := svc.Methods().ByName(protoreflect.Name(methodName))
	if m == nil {
		panic(fmt.Sprintf("missing method %s in %s", methodName, fd.Path()))
	}
	return "/" + string(svc.FullName()) + "/" + string(m.Name())
}

func (s *Schema) Service(fd protoreflect.FileDescriptor, name string) protoreflect.ServiceDescriptor {
	svc := fd.Services().ByName(protoreflect.Name(name))
	if svc == nil {
		panic(fmt.Sprintf("missing service %s in %s", name, fd.Path()))
	}
	return svc
}

func (s *Schema) Method(svc protoreflect.ServiceDescriptor, name string) protoreflect.MethodDescriptor {
	m := svc.Methods().ByName(protoreflect.Name(name))
	if m == nil {
		panic(fmt.Sprintf("missing method %s in service %s", name, svc.FullName()))
	}
	return m
}

func (s *Schema) NewMessage(md protoreflect.MessageDescriptor) *dynamicpb.Message {
	return dynamicpb.NewMessage(md)
}
