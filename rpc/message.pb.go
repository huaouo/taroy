// Copyright 2019 Zhenhua Yang. All rights reserved.
// Licensed under the MIT License that can be
// found in the LICENSE file in the root directory.

// Code generated by protoc-gen-go. DO NOT EDIT.
// source: message.proto

package rpc

import (
	context "context"
	fmt "fmt"
	proto "github.com/golang/protobuf/proto"
	grpc "google.golang.org/grpc"
	codes "google.golang.org/grpc/codes"
	status "google.golang.org/grpc/status"
	math "math"
)

// Reference imports to suppress errors if they are not otherwise used.
var _ = proto.Marshal
var _ = fmt.Errorf
var _ = math.Inf

// This is a compile-time assertion to ensure that this generated file
// is compatible with the proto package it is being compiled against.
// A compilation error at this line likely means your copy of the
// proto package needs to be updated.
const _ = proto.ProtoPackageIsVersion3 // please upgrade the proto package

type RawSQL struct {
	Sql                  string   `protobuf:"bytes,1,opt,name=sql,proto3" json:"sql,omitempty"`
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *RawSQL) Reset()         { *m = RawSQL{} }
func (m *RawSQL) String() string { return proto.CompactTextString(m) }
func (*RawSQL) ProtoMessage()    {}
func (*RawSQL) Descriptor() ([]byte, []int) {
	return fileDescriptor_33c57e4bae7b9afd, []int{0}
}

func (m *RawSQL) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_RawSQL.Unmarshal(m, b)
}
func (m *RawSQL) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_RawSQL.Marshal(b, m, deterministic)
}
func (m *RawSQL) XXX_Merge(src proto.Message) {
	xxx_messageInfo_RawSQL.Merge(m, src)
}
func (m *RawSQL) XXX_Size() int {
	return xxx_messageInfo_RawSQL.Size(m)
}
func (m *RawSQL) XXX_DiscardUnknown() {
	xxx_messageInfo_RawSQL.DiscardUnknown(m)
}

var xxx_messageInfo_RawSQL proto.InternalMessageInfo

func (m *RawSQL) GetSql() string {
	if m != nil {
		return m.Sql
	}
	return ""
}

type Value struct {
	// Types that are valid to be assigned to Val:
	//	*Value_IntVal
	//	*Value_StrVal
	Val                  isValue_Val `protobuf_oneof:"val"`
	XXX_NoUnkeyedLiteral struct{}    `json:"-"`
	XXX_unrecognized     []byte      `json:"-"`
	XXX_sizecache        int32       `json:"-"`
}

func (m *Value) Reset()         { *m = Value{} }
func (m *Value) String() string { return proto.CompactTextString(m) }
func (*Value) ProtoMessage()    {}
func (*Value) Descriptor() ([]byte, []int) {
	return fileDescriptor_33c57e4bae7b9afd, []int{1}
}

func (m *Value) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_Value.Unmarshal(m, b)
}
func (m *Value) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_Value.Marshal(b, m, deterministic)
}
func (m *Value) XXX_Merge(src proto.Message) {
	xxx_messageInfo_Value.Merge(m, src)
}
func (m *Value) XXX_Size() int {
	return xxx_messageInfo_Value.Size(m)
}
func (m *Value) XXX_DiscardUnknown() {
	xxx_messageInfo_Value.DiscardUnknown(m)
}

var xxx_messageInfo_Value proto.InternalMessageInfo

type isValue_Val interface {
	isValue_Val()
}

type Value_IntVal struct {
	IntVal int64 `protobuf:"varint,1,opt,name=int_val,json=intVal,proto3,oneof"`
}

type Value_StrVal struct {
	StrVal string `protobuf:"bytes,2,opt,name=str_val,json=strVal,proto3,oneof"`
}

func (*Value_IntVal) isValue_Val() {}

func (*Value_StrVal) isValue_Val() {}

func (m *Value) GetVal() isValue_Val {
	if m != nil {
		return m.Val
	}
	return nil
}

func (m *Value) GetIntVal() int64 {
	if x, ok := m.GetVal().(*Value_IntVal); ok {
		return x.IntVal
	}
	return 0
}

func (m *Value) GetStrVal() string {
	if x, ok := m.GetVal().(*Value_StrVal); ok {
		return x.StrVal
	}
	return ""
}

// XXX_OneofWrappers is for the internal use of the proto package.
func (*Value) XXX_OneofWrappers() []interface{} {
	return []interface{}{
		(*Value_IntVal)(nil),
		(*Value_StrVal)(nil),
	}
}

type Row struct {
	Fields               []*Value `protobuf:"bytes,1,rep,name=fields,proto3" json:"fields,omitempty"`
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *Row) Reset()         { *m = Row{} }
func (m *Row) String() string { return proto.CompactTextString(m) }
func (*Row) ProtoMessage()    {}
func (*Row) Descriptor() ([]byte, []int) {
	return fileDescriptor_33c57e4bae7b9afd, []int{2}
}

func (m *Row) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_Row.Unmarshal(m, b)
}
func (m *Row) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_Row.Marshal(b, m, deterministic)
}
func (m *Row) XXX_Merge(src proto.Message) {
	xxx_messageInfo_Row.Merge(m, src)
}
func (m *Row) XXX_Size() int {
	return xxx_messageInfo_Row.Size(m)
}
func (m *Row) XXX_DiscardUnknown() {
	xxx_messageInfo_Row.DiscardUnknown(m)
}

var xxx_messageInfo_Row proto.InternalMessageInfo

func (m *Row) GetFields() []*Value {
	if m != nil {
		return m.Fields
	}
	return nil
}

type ResultSet struct {
	Table                []*Row   `protobuf:"bytes,1,rep,name=table,proto3" json:"table,omitempty"`
	Message              string   `protobuf:"bytes,2,opt,name=message,proto3" json:"message,omitempty"`
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *ResultSet) Reset()         { *m = ResultSet{} }
func (m *ResultSet) String() string { return proto.CompactTextString(m) }
func (*ResultSet) ProtoMessage()    {}
func (*ResultSet) Descriptor() ([]byte, []int) {
	return fileDescriptor_33c57e4bae7b9afd, []int{3}
}

func (m *ResultSet) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_ResultSet.Unmarshal(m, b)
}
func (m *ResultSet) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_ResultSet.Marshal(b, m, deterministic)
}
func (m *ResultSet) XXX_Merge(src proto.Message) {
	xxx_messageInfo_ResultSet.Merge(m, src)
}
func (m *ResultSet) XXX_Size() int {
	return xxx_messageInfo_ResultSet.Size(m)
}
func (m *ResultSet) XXX_DiscardUnknown() {
	xxx_messageInfo_ResultSet.DiscardUnknown(m)
}

var xxx_messageInfo_ResultSet proto.InternalMessageInfo

func (m *ResultSet) GetTable() []*Row {
	if m != nil {
		return m.Table
	}
	return nil
}

func (m *ResultSet) GetMessage() string {
	if m != nil {
		return m.Message
	}
	return ""
}

func init() {
	proto.RegisterType((*RawSQL)(nil), "rpc.RawSQL")
	proto.RegisterType((*Value)(nil), "rpc.Value")
	proto.RegisterType((*Row)(nil), "rpc.Row")
	proto.RegisterType((*ResultSet)(nil), "rpc.ResultSet")
}

func init() { proto.RegisterFile("message.proto", fileDescriptor_33c57e4bae7b9afd) }

var fileDescriptor_33c57e4bae7b9afd = []byte{
	// 234 bytes of a gzipped FileDescriptorProto
	0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0xff, 0x34, 0x90, 0x31, 0x4f, 0xc3, 0x30,
	0x14, 0x84, 0x1b, 0x4c, 0x12, 0xfa, 0x2a, 0x10, 0xf2, 0x14, 0x3a, 0xa0, 0xca, 0x53, 0x58, 0x22,
	0x54, 0xfe, 0x41, 0xd5, 0x4a, 0x0c, 0x30, 0xf0, 0x22, 0x65, 0x45, 0x6e, 0x78, 0xa0, 0x48, 0xa6,
	0x09, 0xf6, 0x4b, 0xc3, 0xcf, 0xaf, 0x62, 0x27, 0x9b, 0xcf, 0xdf, 0x9d, 0x4f, 0x67, 0xb8, 0xfd,
	0x25, 0xe7, 0xf4, 0x0f, 0x15, 0x9d, 0x6d, 0xb9, 0x95, 0xc2, 0x76, 0xb5, 0x5a, 0x43, 0x82, 0x7a,
	0x28, 0x3f, 0xde, 0xe4, 0x3d, 0x08, 0xf7, 0x67, 0xb2, 0x68, 0x13, 0xe5, 0x4b, 0x1c, 0x8f, 0x6a,
	0x0f, 0x71, 0xa5, 0x4d, 0x4f, 0xf2, 0x01, 0xd2, 0xe6, 0xc4, 0x9f, 0x67, 0x1d, 0xb0, 0x78, 0x5d,
	0x60, 0xd2, 0x9c, 0xb8, 0xd2, 0x66, 0x44, 0x8e, 0xad, 0x47, 0x57, 0x63, 0x72, 0x44, 0x8e, 0x6d,
	0xa5, 0xcd, 0x2e, 0x06, 0x71, 0xd6, 0x46, 0x3d, 0x81, 0xc0, 0x76, 0x90, 0x0a, 0x92, 0xef, 0x86,
	0xcc, 0x97, 0xcb, 0xa2, 0x8d, 0xc8, 0x57, 0x5b, 0x28, 0x6c, 0x57, 0x17, 0xfe, 0x7d, 0x9c, 0x88,
	0x3a, 0xc0, 0x12, 0xc9, 0xf5, 0x86, 0x4b, 0x62, 0xf9, 0x08, 0x31, 0xeb, 0xa3, 0xa1, 0xc9, 0x7f,
	0xe3, 0xfd, 0xd8, 0x0e, 0x18, 0xae, 0x65, 0x06, 0xe9, 0xb4, 0x27, 0x34, 0xe3, 0x2c, 0xb7, 0xcf,
	0x70, 0xbd, 0xdf, 0xbd, 0x97, 0x32, 0x87, 0xf4, 0xf0, 0x4f, 0x75, 0xcf, 0x24, 0x57, 0x21, 0xed,
	0x97, 0xae, 0xef, 0x82, 0x98, 0x9b, 0xd4, 0xe2, 0x98, 0xf8, 0x1f, 0x79, 0xb9, 0x04, 0x00, 0x00,
	0xff, 0xff, 0x5e, 0xba, 0x65, 0x71, 0x22, 0x01, 0x00, 0x00,
}

// Reference imports to suppress errors if they are not otherwise used.
var _ context.Context
var _ grpc.ClientConn

// This is a compile-time assertion to ensure that this generated file
// is compatible with the grpc package it is being compiled against.
const _ = grpc.SupportPackageIsVersion4

// DBMSClient is the client API for DBMS service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://godoc.org/google.golang.org/grpc#ClientConn.NewStream.
type DBMSClient interface {
	Execute(ctx context.Context, in *RawSQL, opts ...grpc.CallOption) (*ResultSet, error)
}

type dBMSClient struct {
	cc *grpc.ClientConn
}

func NewDBMSClient(cc *grpc.ClientConn) DBMSClient {
	return &dBMSClient{cc}
}

func (c *dBMSClient) Execute(ctx context.Context, in *RawSQL, opts ...grpc.CallOption) (*ResultSet, error) {
	out := new(ResultSet)
	err := c.cc.Invoke(ctx, "/rpc.DBMS/Execute", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// DBMSServer is the server API for DBMS service.
type DBMSServer interface {
	Execute(context.Context, *RawSQL) (*ResultSet, error)
}

// UnimplementedDBMSServer can be embedded to have forward compatible implementations.
type UnimplementedDBMSServer struct {
}

func (*UnimplementedDBMSServer) Execute(ctx context.Context, req *RawSQL) (*ResultSet, error) {
	return nil, status.Errorf(codes.Unimplemented, "method Execute not implemented")
}

func RegisterDBMSServer(s *grpc.Server, srv DBMSServer) {
	s.RegisterService(&_DBMS_serviceDesc, srv)
}

func _DBMS_Execute_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(RawSQL)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(DBMSServer).Execute(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/rpc.DBMS/Execute",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(DBMSServer).Execute(ctx, req.(*RawSQL))
	}
	return interceptor(ctx, in, info, handler)
}

var _DBMS_serviceDesc = grpc.ServiceDesc{
	ServiceName: "rpc.DBMS",
	HandlerType: (*DBMSServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "Execute",
			Handler:    _DBMS_Execute_Handler,
		},
	},
	Streams:  []grpc.StreamDesc{},
	Metadata: "message.proto",
}
