package dynfield

import (
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/dynamicpb"
)

func GetString(msg *dynamicpb.Message, field protoreflect.Name) string {
	fd := msg.Descriptor().Fields().ByName(field)
	if fd == nil {
		return ""
	}
	return msg.Get(fd).String()
}

func GetDouble(msg *dynamicpb.Message, field protoreflect.Name) float64 {
	fd := msg.Descriptor().Fields().ByName(field)
	if fd == nil {
		return 0
	}
	return msg.Get(fd).Float()
}

func GetBool(msg *dynamicpb.Message, field protoreflect.Name) bool {
	fd := msg.Descriptor().Fields().ByName(field)
	if fd == nil {
		return false
	}
	return msg.Get(fd).Bool()
}

func GetInt64(msg *dynamicpb.Message, field protoreflect.Name) int64 {
	fd := msg.Descriptor().Fields().ByName(field)
	if fd == nil {
		return 0
	}
	return msg.Get(fd).Int()
}

func SetString(msg *dynamicpb.Message, field protoreflect.Name, v string) {
	fd := msg.Descriptor().Fields().ByName(field)
	if fd == nil {
		return
	}
	msg.Set(fd, protoreflect.ValueOfString(v))
}

func SetDouble(msg *dynamicpb.Message, field protoreflect.Name, v float64) {
	fd := msg.Descriptor().Fields().ByName(field)
	if fd == nil {
		return
	}
	msg.Set(fd, protoreflect.ValueOfFloat64(v))
}

func SetBool(msg *dynamicpb.Message, field protoreflect.Name, v bool) {
	fd := msg.Descriptor().Fields().ByName(field)
	if fd == nil {
		return
	}
	msg.Set(fd, protoreflect.ValueOfBool(v))
}

func SetInt64(msg *dynamicpb.Message, field protoreflect.Name, v int64) {
	fd := msg.Descriptor().Fields().ByName(field)
	if fd == nil {
		return
	}
	msg.Set(fd, protoreflect.ValueOfInt64(v))
}

func SetEnumNumber(msg *dynamicpb.Message, field protoreflect.Name, num protoreflect.EnumNumber) {
	fd := msg.Descriptor().Fields().ByName(field)
	if fd == nil || fd.Kind() != protoreflect.EnumKind {
		return
	}
	if ev := fd.Enum().Values().ByNumber(num); ev != nil {
		msg.Set(fd, protoreflect.ValueOfEnum(ev.Number()))
		return
	}
	msg.Set(fd, protoreflect.ValueOfEnum(num))
}
