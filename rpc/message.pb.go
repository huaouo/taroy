// Copyright 2019 Zhenhua Yang. All rights reserved.
// Licensed under the MIT License that can be
// found in the LICENSE file in the root directory.

// Code generated by protoc-gen-go. DO NOT EDIT.
// source: message.proto

package rpc

import (
	"fmt"
	"github.com/golang/protobuf/proto"
	"math"
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
	// 235 bytes of a gzipped FileDescriptorProto
	0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0xff, 0x34, 0x90, 0x31, 0x4f, 0xc3, 0x30,
	0x14, 0x84, 0x1b, 0xac, 0x24, 0xe4, 0x55, 0x20, 0xe4, 0x29, 0x74, 0x40, 0x95, 0xa7, 0xb0, 0x64,
	0x28, 0xe2, 0x0f, 0x20, 0x90, 0x18, 0xba, 0xf4, 0x59, 0xca, 0x8a, 0xdc, 0xf6, 0x81, 0x22, 0x99,
	0x38, 0xd8, 0x6e, 0xf2, 0xf7, 0x91, 0xed, 0x74, 0xf3, 0xf9, 0x3b, 0xdf, 0xe9, 0x0c, 0x77, 0xbf,
	0xe4, 0x9c, 0xfa, 0xa1, 0x76, 0xb4, 0xc6, 0x1b, 0xce, 0xec, 0x78, 0x12, 0x1b, 0x28, 0x50, 0xcd,
	0xf2, 0xb0, 0xe7, 0x0f, 0xc0, 0xdc, 0x9f, 0xae, 0xb3, 0x6d, 0xd6, 0x54, 0x18, 0x8e, 0xe2, 0x1d,
	0xf2, 0x4e, 0xe9, 0x0b, 0xf1, 0x47, 0x28, 0xfb, 0xc1, 0x7f, 0x4d, 0x2a, 0x61, 0xf6, 0xb9, 0xc2,
	0xa2, 0x1f, 0x7c, 0xa7, 0x74, 0x40, 0xce, 0xdb, 0x88, 0x6e, 0xc2, 0xcb, 0x80, 0x9c, 0xb7, 0x9d,
	0xd2, 0x6f, 0x39, 0xb0, 0x49, 0x69, 0xf1, 0x0c, 0x0c, 0xcd, 0xcc, 0x05, 0x14, 0xdf, 0x3d, 0xe9,
	0xb3, 0xab, 0xb3, 0x2d, 0x6b, 0xd6, 0x3b, 0x68, 0xed, 0x78, 0x6a, 0x63, 0x3e, 0x2e, 0x44, 0x7c,
	0x40, 0x85, 0xe4, 0x2e, 0xda, 0x4b, 0xf2, 0xfc, 0x09, 0x72, 0xaf, 0x8e, 0x9a, 0x16, 0xff, 0x6d,
	0xf4, 0xa3, 0x99, 0x31, 0x5d, 0xf3, 0x1a, 0xca, 0x65, 0x4f, 0x6a, 0xc6, 0xab, 0xdc, 0xbd, 0x42,
	0x25, 0x0f, 0x7b, 0x49, 0x76, 0x22, 0xcb, 0x1b, 0x28, 0x25, 0x0d, 0xe7, 0xb0, 0x70, 0x9d, 0x22,
	0xe2, 0xdc, 0xcd, 0x7d, 0x12, 0xd7, 0x3a, 0xb1, 0x3a, 0x16, 0xf1, 0x5b, 0x5e, 0xfe, 0x03, 0x00,
	0x00, 0xff, 0xff, 0x4e, 0x8c, 0x69, 0xd6, 0x27, 0x01, 0x00, 0x00,
}
