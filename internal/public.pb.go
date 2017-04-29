// Copyright 2017 Pilosa Corp.
//
// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions
// are met:
//
// 1. Redistributions of source code must retain the above copyright
// notice, this list of conditions and the following disclaimer.
//
// 2. Redistributions in binary form must reproduce the above copyright
// notice, this list of conditions and the following disclaimer in the
// documentation and/or other materials provided with the distribution.
//
// 3. Neither the name of the copyright holder nor the names of its
// contributors may be used to endorse or promote products derived
// from this software without specific prior written permission.
//
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND
// CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES,
// INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF
// MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
// DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR
// CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
// SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
// BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
// SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
// INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
// WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
// NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
// OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH
// DAMAGE.

/*
Package internal is a generated protocol buffer package.

It is generated from these files:
	internal/public.proto

It has these top-level messages:
	Bitmap
	Pair
	Bit
	ColumnAttrSet
	Attr
	AttrMap
	QueryRequest
	QueryResponse
	QueryResult
	ImportRequest
*/
package internal

import proto "github.com/golang/protobuf/proto"
import fmt "fmt"
import math "math"

// Reference imports to suppress errors if they are not otherwise used.
var _ = proto.Marshal
var _ = fmt.Errorf
var _ = math.Inf

// This is a compile-time assertion to ensure that this generated file
// is compatible with the proto package it is being compiled against.
// A compilation error at this line likely means your copy of the
// proto package needs to be updated.
const _ = proto.ProtoPackageIsVersion2 // please upgrade the proto package

type Bitmap struct {
	Bits  []uint64 `protobuf:"varint,1,rep,packed,name=Bits" json:"Bits,omitempty"`
	Attrs []*Attr  `protobuf:"bytes,2,rep,name=Attrs" json:"Attrs,omitempty"`
}

func (m *Bitmap) Reset()                    { *m = Bitmap{} }
func (m *Bitmap) String() string            { return proto.CompactTextString(m) }
func (*Bitmap) ProtoMessage()               {}
func (*Bitmap) Descriptor() ([]byte, []int) { return fileDescriptor0, []int{0} }

func (m *Bitmap) GetBits() []uint64 {
	if m != nil {
		return m.Bits
	}
	return nil
}

func (m *Bitmap) GetAttrs() []*Attr {
	if m != nil {
		return m.Attrs
	}
	return nil
}

type Pair struct {
	Key   uint64 `protobuf:"varint,1,opt,name=Key" json:"Key,omitempty"`
	Count uint64 `protobuf:"varint,2,opt,name=Count" json:"Count,omitempty"`
}

func (m *Pair) Reset()                    { *m = Pair{} }
func (m *Pair) String() string            { return proto.CompactTextString(m) }
func (*Pair) ProtoMessage()               {}
func (*Pair) Descriptor() ([]byte, []int) { return fileDescriptor0, []int{1} }

func (m *Pair) GetKey() uint64 {
	if m != nil {
		return m.Key
	}
	return 0
}

func (m *Pair) GetCount() uint64 {
	if m != nil {
		return m.Count
	}
	return 0
}

type Bit struct {
	RowID     uint64 `protobuf:"varint,1,opt,name=RowID" json:"RowID,omitempty"`
	ColumnID  uint64 `protobuf:"varint,2,opt,name=ColumnID" json:"ColumnID,omitempty"`
	Timestamp int64  `protobuf:"varint,3,opt,name=Timestamp" json:"Timestamp,omitempty"`
}

func (m *Bit) Reset()                    { *m = Bit{} }
func (m *Bit) String() string            { return proto.CompactTextString(m) }
func (*Bit) ProtoMessage()               {}
func (*Bit) Descriptor() ([]byte, []int) { return fileDescriptor0, []int{2} }

func (m *Bit) GetRowID() uint64 {
	if m != nil {
		return m.RowID
	}
	return 0
}

func (m *Bit) GetColumnID() uint64 {
	if m != nil {
		return m.ColumnID
	}
	return 0
}

func (m *Bit) GetTimestamp() int64 {
	if m != nil {
		return m.Timestamp
	}
	return 0
}

type ColumnAttrSet struct {
	ID    uint64  `protobuf:"varint,1,opt,name=ID" json:"ID,omitempty"`
	Attrs []*Attr `protobuf:"bytes,2,rep,name=Attrs" json:"Attrs,omitempty"`
}

func (m *ColumnAttrSet) Reset()                    { *m = ColumnAttrSet{} }
func (m *ColumnAttrSet) String() string            { return proto.CompactTextString(m) }
func (*ColumnAttrSet) ProtoMessage()               {}
func (*ColumnAttrSet) Descriptor() ([]byte, []int) { return fileDescriptor0, []int{3} }

func (m *ColumnAttrSet) GetID() uint64 {
	if m != nil {
		return m.ID
	}
	return 0
}

func (m *ColumnAttrSet) GetAttrs() []*Attr {
	if m != nil {
		return m.Attrs
	}
	return nil
}

type Attr struct {
	Key         string  `protobuf:"bytes,1,opt,name=Key" json:"Key,omitempty"`
	Type        uint64  `protobuf:"varint,2,opt,name=Type" json:"Type,omitempty"`
	StringValue string  `protobuf:"bytes,3,opt,name=StringValue" json:"StringValue,omitempty"`
	IntValue    int64   `protobuf:"varint,4,opt,name=IntValue" json:"IntValue,omitempty"`
	BoolValue   bool    `protobuf:"varint,5,opt,name=BoolValue" json:"BoolValue,omitempty"`
	FloatValue  float64 `protobuf:"fixed64,6,opt,name=FloatValue" json:"FloatValue,omitempty"`
}

func (m *Attr) Reset()                    { *m = Attr{} }
func (m *Attr) String() string            { return proto.CompactTextString(m) }
func (*Attr) ProtoMessage()               {}
func (*Attr) Descriptor() ([]byte, []int) { return fileDescriptor0, []int{4} }

func (m *Attr) GetKey() string {
	if m != nil {
		return m.Key
	}
	return ""
}

func (m *Attr) GetType() uint64 {
	if m != nil {
		return m.Type
	}
	return 0
}

func (m *Attr) GetStringValue() string {
	if m != nil {
		return m.StringValue
	}
	return ""
}

func (m *Attr) GetIntValue() int64 {
	if m != nil {
		return m.IntValue
	}
	return 0
}

func (m *Attr) GetBoolValue() bool {
	if m != nil {
		return m.BoolValue
	}
	return false
}

func (m *Attr) GetFloatValue() float64 {
	if m != nil {
		return m.FloatValue
	}
	return 0
}

type AttrMap struct {
	Attrs []*Attr `protobuf:"bytes,1,rep,name=Attrs" json:"Attrs,omitempty"`
}

func (m *AttrMap) Reset()                    { *m = AttrMap{} }
func (m *AttrMap) String() string            { return proto.CompactTextString(m) }
func (*AttrMap) ProtoMessage()               {}
func (*AttrMap) Descriptor() ([]byte, []int) { return fileDescriptor0, []int{5} }

func (m *AttrMap) GetAttrs() []*Attr {
	if m != nil {
		return m.Attrs
	}
	return nil
}

type QueryRequest struct {
	Query       string   `protobuf:"bytes,1,opt,name=Query" json:"Query,omitempty"`
	Slices      []uint64 `protobuf:"varint,2,rep,packed,name=Slices" json:"Slices,omitempty"`
	ColumnAttrs bool     `protobuf:"varint,3,opt,name=ColumnAttrs" json:"ColumnAttrs,omitempty"`
	Quantum     string   `protobuf:"bytes,4,opt,name=Quantum" json:"Quantum,omitempty"`
	Remote      bool     `protobuf:"varint,5,opt,name=Remote" json:"Remote,omitempty"`
}

func (m *QueryRequest) Reset()                    { *m = QueryRequest{} }
func (m *QueryRequest) String() string            { return proto.CompactTextString(m) }
func (*QueryRequest) ProtoMessage()               {}
func (*QueryRequest) Descriptor() ([]byte, []int) { return fileDescriptor0, []int{6} }

func (m *QueryRequest) GetQuery() string {
	if m != nil {
		return m.Query
	}
	return ""
}

func (m *QueryRequest) GetSlices() []uint64 {
	if m != nil {
		return m.Slices
	}
	return nil
}

func (m *QueryRequest) GetColumnAttrs() bool {
	if m != nil {
		return m.ColumnAttrs
	}
	return false
}

func (m *QueryRequest) GetQuantum() string {
	if m != nil {
		return m.Quantum
	}
	return ""
}

func (m *QueryRequest) GetRemote() bool {
	if m != nil {
		return m.Remote
	}
	return false
}

type QueryResponse struct {
	Err            string           `protobuf:"bytes,1,opt,name=Err" json:"Err,omitempty"`
	Results        []*QueryResult   `protobuf:"bytes,2,rep,name=Results" json:"Results,omitempty"`
	ColumnAttrSets []*ColumnAttrSet `protobuf:"bytes,3,rep,name=ColumnAttrSets" json:"ColumnAttrSets,omitempty"`
}

func (m *QueryResponse) Reset()                    { *m = QueryResponse{} }
func (m *QueryResponse) String() string            { return proto.CompactTextString(m) }
func (*QueryResponse) ProtoMessage()               {}
func (*QueryResponse) Descriptor() ([]byte, []int) { return fileDescriptor0, []int{7} }

func (m *QueryResponse) GetErr() string {
	if m != nil {
		return m.Err
	}
	return ""
}

func (m *QueryResponse) GetResults() []*QueryResult {
	if m != nil {
		return m.Results
	}
	return nil
}

func (m *QueryResponse) GetColumnAttrSets() []*ColumnAttrSet {
	if m != nil {
		return m.ColumnAttrSets
	}
	return nil
}

type QueryResult struct {
	Bitmap  *Bitmap `protobuf:"bytes,1,opt,name=Bitmap" json:"Bitmap,omitempty"`
	N       uint64  `protobuf:"varint,2,opt,name=N" json:"N,omitempty"`
	Pairs   []*Pair `protobuf:"bytes,3,rep,name=Pairs" json:"Pairs,omitempty"`
	Changed bool    `protobuf:"varint,4,opt,name=Changed" json:"Changed,omitempty"`
}

func (m *QueryResult) Reset()                    { *m = QueryResult{} }
func (m *QueryResult) String() string            { return proto.CompactTextString(m) }
func (*QueryResult) ProtoMessage()               {}
func (*QueryResult) Descriptor() ([]byte, []int) { return fileDescriptor0, []int{8} }

func (m *QueryResult) GetBitmap() *Bitmap {
	if m != nil {
		return m.Bitmap
	}
	return nil
}

func (m *QueryResult) GetN() uint64 {
	if m != nil {
		return m.N
	}
	return 0
}

func (m *QueryResult) GetPairs() []*Pair {
	if m != nil {
		return m.Pairs
	}
	return nil
}

func (m *QueryResult) GetChanged() bool {
	if m != nil {
		return m.Changed
	}
	return false
}

type ImportRequest struct {
	DB         string   `protobuf:"bytes,1,opt,name=DB" json:"DB,omitempty"`
	Frame      string   `protobuf:"bytes,2,opt,name=Frame" json:"Frame,omitempty"`
	Slice      uint64   `protobuf:"varint,3,opt,name=Slice" json:"Slice,omitempty"`
	RowIDs     []uint64 `protobuf:"varint,4,rep,packed,name=RowIDs" json:"RowIDs,omitempty"`
	ColumnIDs  []uint64 `protobuf:"varint,5,rep,packed,name=ColumnIDs" json:"ColumnIDs,omitempty"`
	Timestamps []int64  `protobuf:"varint,6,rep,packed,name=Timestamps" json:"Timestamps,omitempty"`
}

func (m *ImportRequest) Reset()                    { *m = ImportRequest{} }
func (m *ImportRequest) String() string            { return proto.CompactTextString(m) }
func (*ImportRequest) ProtoMessage()               {}
func (*ImportRequest) Descriptor() ([]byte, []int) { return fileDescriptor0, []int{9} }

func (m *ImportRequest) GetDB() string {
	if m != nil {
		return m.DB
	}
	return ""
}

func (m *ImportRequest) GetFrame() string {
	if m != nil {
		return m.Frame
	}
	return ""
}

func (m *ImportRequest) GetSlice() uint64 {
	if m != nil {
		return m.Slice
	}
	return 0
}

func (m *ImportRequest) GetRowIDs() []uint64 {
	if m != nil {
		return m.RowIDs
	}
	return nil
}

func (m *ImportRequest) GetColumnIDs() []uint64 {
	if m != nil {
		return m.ColumnIDs
	}
	return nil
}

func (m *ImportRequest) GetTimestamps() []int64 {
	if m != nil {
		return m.Timestamps
	}
	return nil
}

func init() {
	proto.RegisterType((*Bitmap)(nil), "internal.Bitmap")
	proto.RegisterType((*Pair)(nil), "internal.Pair")
	proto.RegisterType((*Bit)(nil), "internal.Bit")
	proto.RegisterType((*ColumnAttrSet)(nil), "internal.ColumnAttrSet")
	proto.RegisterType((*Attr)(nil), "internal.Attr")
	proto.RegisterType((*AttrMap)(nil), "internal.AttrMap")
	proto.RegisterType((*QueryRequest)(nil), "internal.QueryRequest")
	proto.RegisterType((*QueryResponse)(nil), "internal.QueryResponse")
	proto.RegisterType((*QueryResult)(nil), "internal.QueryResult")
	proto.RegisterType((*ImportRequest)(nil), "internal.ImportRequest")
}

func init() { proto.RegisterFile("internal/public.proto", fileDescriptor0) }

var fileDescriptor0 = []byte{
	// 556 bytes of a gzipped FileDescriptorProto
	0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0xff, 0x8c, 0x54, 0xcd, 0x8a, 0xd4, 0x40,
	0x10, 0xa6, 0x93, 0xcc, 0x5f, 0xcd, 0xce, 0xb0, 0x34, 0xae, 0x06, 0x11, 0x09, 0xc1, 0x43, 0x4e,
	0x33, 0xb0, 0x3e, 0x80, 0x98, 0x99, 0x59, 0x18, 0xc4, 0xc5, 0xad, 0x59, 0xbd, 0x67, 0xd7, 0x66,
	0x0d, 0xe4, 0xcf, 0xee, 0x0e, 0x32, 0x0f, 0xe0, 0x5d, 0xf0, 0x09, 0xbc, 0xf9, 0x98, 0x52, 0xdd,
	0xe9, 0x49, 0xd6, 0x83, 0x78, 0xeb, 0xef, 0xab, 0xae, 0x4e, 0x7d, 0xf5, 0x55, 0x05, 0x2e, 0xf2,
	0x4a, 0x0b, 0x59, 0x65, 0xc5, 0xba, 0x69, 0xef, 0x8a, 0xfc, 0x7e, 0xd5, 0xc8, 0x5a, 0xd7, 0x7c,
	0xea, 0xe8, 0x38, 0x85, 0x71, 0x9a, 0xeb, 0x32, 0x6b, 0x38, 0x87, 0x20, 0xcd, 0xb5, 0x0a, 0x59,
	0xe4, 0x27, 0x01, 0x9a, 0x33, 0x7f, 0x05, 0xa3, 0xb7, 0x5a, 0x4b, 0x15, 0x7a, 0x91, 0x9f, 0xcc,
	0x2f, 0x97, 0x2b, 0x97, 0xb7, 0x22, 0x1a, 0x6d, 0x30, 0x5e, 0x41, 0xf0, 0x21, 0xcb, 0x25, 0x3f,
	0x07, 0xff, 0x9d, 0x38, 0x86, 0x2c, 0x62, 0x49, 0x80, 0x74, 0xe4, 0x4f, 0x60, 0xb4, 0xa9, 0xdb,
	0x4a, 0x87, 0x9e, 0xe1, 0x2c, 0x88, 0x3f, 0x82, 0x9f, 0xe6, 0x9a, 0x82, 0x58, 0x7f, 0xdb, 0x6f,
	0xbb, 0x04, 0x0b, 0xf8, 0x73, 0x98, 0x6e, 0xea, 0xa2, 0x2d, 0xab, 0xfd, 0xb6, 0xcb, 0x3a, 0x61,
	0xfe, 0x02, 0x66, 0xb7, 0x79, 0x29, 0x94, 0xce, 0xca, 0x26, 0xf4, 0x23, 0x96, 0xf8, 0xd8, 0x13,
	0xf1, 0x0e, 0x16, 0xf6, 0x26, 0x55, 0x75, 0x10, 0x9a, 0x2f, 0xc1, 0x3b, 0xbd, 0xee, 0xed, 0xb7,
	0xff, 0xa9, 0xe6, 0x37, 0x83, 0x80, 0x4e, 0x43, 0x39, 0x33, 0x2b, 0x87, 0x43, 0x70, 0x7b, 0x6c,
	0x44, 0x57, 0x97, 0x39, 0xf3, 0x08, 0xe6, 0x07, 0x2d, 0xf3, 0xea, 0xe1, 0x53, 0x56, 0xb4, 0xc2,
	0x54, 0x35, 0xc3, 0x21, 0x45, 0x8a, 0xf6, 0x95, 0xb6, 0xe1, 0xc0, 0x14, 0x7d, 0xc2, 0xa4, 0x28,
	0xad, 0xeb, 0xc2, 0x06, 0x47, 0x11, 0x4b, 0xa6, 0xd8, 0x13, 0xfc, 0x25, 0xc0, 0x55, 0x51, 0x67,
	0x5d, 0xee, 0x38, 0x62, 0x09, 0xc3, 0x01, 0x13, 0xaf, 0x61, 0x42, 0x95, 0xbe, 0xcf, 0x9a, 0x5e,
	0x1b, 0xfb, 0x97, 0xb6, 0x1f, 0x0c, 0xce, 0x6e, 0x5a, 0x21, 0x8f, 0x28, 0xbe, 0xb6, 0x42, 0x19,
	0x0f, 0x0c, 0xee, 0x54, 0x5a, 0xc0, 0x9f, 0xc2, 0xf8, 0x50, 0xe4, 0xf7, 0xc2, 0x76, 0x2a, 0xc0,
	0x0e, 0x91, 0xd6, 0xbe, 0xc3, 0xca, 0x68, 0x9d, 0xe2, 0x90, 0xe2, 0x21, 0x4c, 0x6e, 0xda, 0xac,
	0xd2, 0x6d, 0x69, 0xa4, 0xce, 0xd0, 0x41, 0x7a, 0x13, 0x45, 0x59, 0x6b, 0x27, 0xb3, 0x43, 0xf1,
	0x4f, 0x06, 0x8b, 0xae, 0x24, 0xd5, 0xd4, 0x95, 0x12, 0xd4, 0xf7, 0x9d, 0x94, 0xae, 0xef, 0x3b,
	0x29, 0xf9, 0x1a, 0x26, 0x28, 0x54, 0x5b, 0x68, 0x67, 0xdd, 0x45, 0x2f, 0xcf, 0xe5, 0xb6, 0x85,
	0x46, 0x77, 0x8b, 0xbf, 0x81, 0xe5, 0xa3, 0x51, 0xa0, 0x5a, 0x29, 0xef, 0x59, 0x9f, 0xf7, 0x28,
	0x8e, 0x7f, 0x5d, 0x8f, 0xbf, 0x33, 0x98, 0x0f, 0x5e, 0xe6, 0x89, 0x5b, 0x13, 0x53, 0xd6, 0xfc,
	0xf2, 0xbc, 0x7f, 0xc8, 0xf2, 0xe8, 0xd6, 0xe8, 0x0c, 0xd8, 0x75, 0x37, 0x20, 0xec, 0x9a, 0x6c,
	0xa1, 0xd5, 0x70, 0xdf, 0x1f, 0xd8, 0x42, 0x34, 0xda, 0x20, 0x75, 0x6d, 0xf3, 0x25, 0xab, 0x1e,
	0xc4, 0x67, 0xd3, 0xb5, 0x29, 0x3a, 0x18, 0xff, 0x62, 0xb0, 0xd8, 0x97, 0x4d, 0x2d, 0xb5, 0x73,
	0x6c, 0x09, 0xde, 0x36, 0xed, 0x9a, 0xe3, 0x6d, 0x53, 0x72, 0xf0, 0x4a, 0x66, 0xa5, 0x1d, 0xca,
	0x19, 0x5a, 0x40, 0xac, 0xf1, 0xcc, 0x78, 0x14, 0xa0, 0x05, 0xc6, 0x03, 0x5a, 0x32, 0x15, 0x06,
	0xd6, 0x57, 0x8b, 0x68, 0x0a, 0xdd, 0x8e, 0xa9, 0x70, 0x64, 0x42, 0x3d, 0x41, 0x53, 0x78, 0x5a,
	0x32, 0x15, 0x8e, 0x23, 0x3f, 0xf1, 0x71, 0xc0, 0xdc, 0x8d, 0xcd, 0x3f, 0xe5, 0xf5, 0x9f, 0x00,
	0x00, 0x00, 0xff, 0xff, 0x5d, 0x87, 0x06, 0x79, 0x6c, 0x04, 0x00, 0x00,
}
