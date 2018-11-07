// Code generated by protoc-gen-go. DO NOT EDIT.
// source: gopilosa_pbuf/public.proto

package gopilosa_pbuf

import (
	fmt "fmt"
	proto "github.com/golang/protobuf/proto"
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
const _ = proto.ProtoPackageIsVersion2 // please upgrade the proto package

type Row struct {
	Columns              []uint64 `protobuf:"varint,1,rep,packed,name=Columns,proto3" json:"Columns,omitempty"`
	Keys                 []string `protobuf:"bytes,3,rep,name=Keys,proto3" json:"Keys,omitempty"`
	Attrs                []*Attr  `protobuf:"bytes,2,rep,name=Attrs,proto3" json:"Attrs,omitempty"`
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *Row) Reset()         { *m = Row{} }
func (m *Row) String() string { return proto.CompactTextString(m) }
func (*Row) ProtoMessage()    {}
func (*Row) Descriptor() ([]byte, []int) {
	return fileDescriptor_a66c591da5e2e6d0, []int{0}
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

func (m *Row) GetColumns() []uint64 {
	if m != nil {
		return m.Columns
	}
	return nil
}

func (m *Row) GetKeys() []string {
	if m != nil {
		return m.Keys
	}
	return nil
}

func (m *Row) GetAttrs() []*Attr {
	if m != nil {
		return m.Attrs
	}
	return nil
}

type Pair struct {
	ID                   uint64   `protobuf:"varint,1,opt,name=ID,proto3" json:"ID,omitempty"`
	Key                  string   `protobuf:"bytes,3,opt,name=Key,proto3" json:"Key,omitempty"`
	Count                uint64   `protobuf:"varint,2,opt,name=Count,proto3" json:"Count,omitempty"`
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *Pair) Reset()         { *m = Pair{} }
func (m *Pair) String() string { return proto.CompactTextString(m) }
func (*Pair) ProtoMessage()    {}
func (*Pair) Descriptor() ([]byte, []int) {
	return fileDescriptor_a66c591da5e2e6d0, []int{1}
}

func (m *Pair) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_Pair.Unmarshal(m, b)
}
func (m *Pair) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_Pair.Marshal(b, m, deterministic)
}
func (m *Pair) XXX_Merge(src proto.Message) {
	xxx_messageInfo_Pair.Merge(m, src)
}
func (m *Pair) XXX_Size() int {
	return xxx_messageInfo_Pair.Size(m)
}
func (m *Pair) XXX_DiscardUnknown() {
	xxx_messageInfo_Pair.DiscardUnknown(m)
}

var xxx_messageInfo_Pair proto.InternalMessageInfo

func (m *Pair) GetID() uint64 {
	if m != nil {
		return m.ID
	}
	return 0
}

func (m *Pair) GetKey() string {
	if m != nil {
		return m.Key
	}
	return ""
}

func (m *Pair) GetCount() uint64 {
	if m != nil {
		return m.Count
	}
	return 0
}

type ValCount struct {
	Val                  int64    `protobuf:"varint,1,opt,name=Val,proto3" json:"Val,omitempty"`
	Count                int64    `protobuf:"varint,2,opt,name=Count,proto3" json:"Count,omitempty"`
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *ValCount) Reset()         { *m = ValCount{} }
func (m *ValCount) String() string { return proto.CompactTextString(m) }
func (*ValCount) ProtoMessage()    {}
func (*ValCount) Descriptor() ([]byte, []int) {
	return fileDescriptor_a66c591da5e2e6d0, []int{2}
}

func (m *ValCount) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_ValCount.Unmarshal(m, b)
}
func (m *ValCount) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_ValCount.Marshal(b, m, deterministic)
}
func (m *ValCount) XXX_Merge(src proto.Message) {
	xxx_messageInfo_ValCount.Merge(m, src)
}
func (m *ValCount) XXX_Size() int {
	return xxx_messageInfo_ValCount.Size(m)
}
func (m *ValCount) XXX_DiscardUnknown() {
	xxx_messageInfo_ValCount.DiscardUnknown(m)
}

var xxx_messageInfo_ValCount proto.InternalMessageInfo

func (m *ValCount) GetVal() int64 {
	if m != nil {
		return m.Val
	}
	return 0
}

func (m *ValCount) GetCount() int64 {
	if m != nil {
		return m.Count
	}
	return 0
}

type Bit struct {
	RowID                uint64   `protobuf:"varint,1,opt,name=RowID,proto3" json:"RowID,omitempty"`
	ColumnID             uint64   `protobuf:"varint,2,opt,name=ColumnID,proto3" json:"ColumnID,omitempty"`
	Timestamp            int64    `protobuf:"varint,3,opt,name=Timestamp,proto3" json:"Timestamp,omitempty"`
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *Bit) Reset()         { *m = Bit{} }
func (m *Bit) String() string { return proto.CompactTextString(m) }
func (*Bit) ProtoMessage()    {}
func (*Bit) Descriptor() ([]byte, []int) {
	return fileDescriptor_a66c591da5e2e6d0, []int{3}
}

func (m *Bit) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_Bit.Unmarshal(m, b)
}
func (m *Bit) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_Bit.Marshal(b, m, deterministic)
}
func (m *Bit) XXX_Merge(src proto.Message) {
	xxx_messageInfo_Bit.Merge(m, src)
}
func (m *Bit) XXX_Size() int {
	return xxx_messageInfo_Bit.Size(m)
}
func (m *Bit) XXX_DiscardUnknown() {
	xxx_messageInfo_Bit.DiscardUnknown(m)
}

var xxx_messageInfo_Bit proto.InternalMessageInfo

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

type BulkColumnAttrRequest struct {
	ColAttrSets          []*ColumnAttrSet `protobuf:"bytes,1,rep,name=ColAttrSets,proto3" json:"ColAttrSets,omitempty"`
	XXX_NoUnkeyedLiteral struct{}         `json:"-"`
	XXX_unrecognized     []byte           `json:"-"`
	XXX_sizecache        int32            `json:"-"`
}

func (m *BulkColumnAttrRequest) Reset()         { *m = BulkColumnAttrRequest{} }
func (m *BulkColumnAttrRequest) String() string { return proto.CompactTextString(m) }
func (*BulkColumnAttrRequest) ProtoMessage()    {}
func (*BulkColumnAttrRequest) Descriptor() ([]byte, []int) {
	return fileDescriptor_a66c591da5e2e6d0, []int{4}
}

func (m *BulkColumnAttrRequest) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_BulkColumnAttrRequest.Unmarshal(m, b)
}
func (m *BulkColumnAttrRequest) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_BulkColumnAttrRequest.Marshal(b, m, deterministic)
}
func (m *BulkColumnAttrRequest) XXX_Merge(src proto.Message) {
	xxx_messageInfo_BulkColumnAttrRequest.Merge(m, src)
}
func (m *BulkColumnAttrRequest) XXX_Size() int {
	return xxx_messageInfo_BulkColumnAttrRequest.Size(m)
}
func (m *BulkColumnAttrRequest) XXX_DiscardUnknown() {
	xxx_messageInfo_BulkColumnAttrRequest.DiscardUnknown(m)
}

var xxx_messageInfo_BulkColumnAttrRequest proto.InternalMessageInfo

func (m *BulkColumnAttrRequest) GetColAttrSets() []*ColumnAttrSet {
	if m != nil {
		return m.ColAttrSets
	}
	return nil
}

type ColumnAttrSet struct {
	ID                   uint64   `protobuf:"varint,1,opt,name=ID,proto3" json:"ID,omitempty"`
	Key                  string   `protobuf:"bytes,3,opt,name=Key,proto3" json:"Key,omitempty"`
	Attrs                []*Attr  `protobuf:"bytes,2,rep,name=Attrs,proto3" json:"Attrs,omitempty"`
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *ColumnAttrSet) Reset()         { *m = ColumnAttrSet{} }
func (m *ColumnAttrSet) String() string { return proto.CompactTextString(m) }
func (*ColumnAttrSet) ProtoMessage()    {}
func (*ColumnAttrSet) Descriptor() ([]byte, []int) {
	return fileDescriptor_a66c591da5e2e6d0, []int{5}
}

func (m *ColumnAttrSet) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_ColumnAttrSet.Unmarshal(m, b)
}
func (m *ColumnAttrSet) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_ColumnAttrSet.Marshal(b, m, deterministic)
}
func (m *ColumnAttrSet) XXX_Merge(src proto.Message) {
	xxx_messageInfo_ColumnAttrSet.Merge(m, src)
}
func (m *ColumnAttrSet) XXX_Size() int {
	return xxx_messageInfo_ColumnAttrSet.Size(m)
}
func (m *ColumnAttrSet) XXX_DiscardUnknown() {
	xxx_messageInfo_ColumnAttrSet.DiscardUnknown(m)
}

var xxx_messageInfo_ColumnAttrSet proto.InternalMessageInfo

func (m *ColumnAttrSet) GetID() uint64 {
	if m != nil {
		return m.ID
	}
	return 0
}

func (m *ColumnAttrSet) GetKey() string {
	if m != nil {
		return m.Key
	}
	return ""
}

func (m *ColumnAttrSet) GetAttrs() []*Attr {
	if m != nil {
		return m.Attrs
	}
	return nil
}

type Attr struct {
	Key                  string   `protobuf:"bytes,1,opt,name=Key,proto3" json:"Key,omitempty"`
	Type                 uint64   `protobuf:"varint,2,opt,name=Type,proto3" json:"Type,omitempty"`
	StringValue          string   `protobuf:"bytes,3,opt,name=StringValue,proto3" json:"StringValue,omitempty"`
	IntValue             int64    `protobuf:"varint,4,opt,name=IntValue,proto3" json:"IntValue,omitempty"`
	BoolValue            bool     `protobuf:"varint,5,opt,name=BoolValue,proto3" json:"BoolValue,omitempty"`
	FloatValue           float64  `protobuf:"fixed64,6,opt,name=FloatValue,proto3" json:"FloatValue,omitempty"`
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *Attr) Reset()         { *m = Attr{} }
func (m *Attr) String() string { return proto.CompactTextString(m) }
func (*Attr) ProtoMessage()    {}
func (*Attr) Descriptor() ([]byte, []int) {
	return fileDescriptor_a66c591da5e2e6d0, []int{6}
}

func (m *Attr) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_Attr.Unmarshal(m, b)
}
func (m *Attr) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_Attr.Marshal(b, m, deterministic)
}
func (m *Attr) XXX_Merge(src proto.Message) {
	xxx_messageInfo_Attr.Merge(m, src)
}
func (m *Attr) XXX_Size() int {
	return xxx_messageInfo_Attr.Size(m)
}
func (m *Attr) XXX_DiscardUnknown() {
	xxx_messageInfo_Attr.DiscardUnknown(m)
}

var xxx_messageInfo_Attr proto.InternalMessageInfo

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
	Attrs                []*Attr  `protobuf:"bytes,1,rep,name=Attrs,proto3" json:"Attrs,omitempty"`
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *AttrMap) Reset()         { *m = AttrMap{} }
func (m *AttrMap) String() string { return proto.CompactTextString(m) }
func (*AttrMap) ProtoMessage()    {}
func (*AttrMap) Descriptor() ([]byte, []int) {
	return fileDescriptor_a66c591da5e2e6d0, []int{7}
}

func (m *AttrMap) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_AttrMap.Unmarshal(m, b)
}
func (m *AttrMap) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_AttrMap.Marshal(b, m, deterministic)
}
func (m *AttrMap) XXX_Merge(src proto.Message) {
	xxx_messageInfo_AttrMap.Merge(m, src)
}
func (m *AttrMap) XXX_Size() int {
	return xxx_messageInfo_AttrMap.Size(m)
}
func (m *AttrMap) XXX_DiscardUnknown() {
	xxx_messageInfo_AttrMap.DiscardUnknown(m)
}

var xxx_messageInfo_AttrMap proto.InternalMessageInfo

func (m *AttrMap) GetAttrs() []*Attr {
	if m != nil {
		return m.Attrs
	}
	return nil
}

type QueryRequest struct {
	Query                string   `protobuf:"bytes,1,opt,name=Query,proto3" json:"Query,omitempty"`
	Shards               []uint64 `protobuf:"varint,2,rep,packed,name=Shards,proto3" json:"Shards,omitempty"`
	ColumnAttrs          bool     `protobuf:"varint,3,opt,name=ColumnAttrs,proto3" json:"ColumnAttrs,omitempty"`
	Remote               bool     `protobuf:"varint,5,opt,name=Remote,proto3" json:"Remote,omitempty"`
	ExcludeRowAttrs      bool     `protobuf:"varint,6,opt,name=ExcludeRowAttrs,proto3" json:"ExcludeRowAttrs,omitempty"`
	ExcludeColumns       bool     `protobuf:"varint,7,opt,name=ExcludeColumns,proto3" json:"ExcludeColumns,omitempty"`
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *QueryRequest) Reset()         { *m = QueryRequest{} }
func (m *QueryRequest) String() string { return proto.CompactTextString(m) }
func (*QueryRequest) ProtoMessage()    {}
func (*QueryRequest) Descriptor() ([]byte, []int) {
	return fileDescriptor_a66c591da5e2e6d0, []int{8}
}

func (m *QueryRequest) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_QueryRequest.Unmarshal(m, b)
}
func (m *QueryRequest) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_QueryRequest.Marshal(b, m, deterministic)
}
func (m *QueryRequest) XXX_Merge(src proto.Message) {
	xxx_messageInfo_QueryRequest.Merge(m, src)
}
func (m *QueryRequest) XXX_Size() int {
	return xxx_messageInfo_QueryRequest.Size(m)
}
func (m *QueryRequest) XXX_DiscardUnknown() {
	xxx_messageInfo_QueryRequest.DiscardUnknown(m)
}

var xxx_messageInfo_QueryRequest proto.InternalMessageInfo

func (m *QueryRequest) GetQuery() string {
	if m != nil {
		return m.Query
	}
	return ""
}

func (m *QueryRequest) GetShards() []uint64 {
	if m != nil {
		return m.Shards
	}
	return nil
}

func (m *QueryRequest) GetColumnAttrs() bool {
	if m != nil {
		return m.ColumnAttrs
	}
	return false
}

func (m *QueryRequest) GetRemote() bool {
	if m != nil {
		return m.Remote
	}
	return false
}

func (m *QueryRequest) GetExcludeRowAttrs() bool {
	if m != nil {
		return m.ExcludeRowAttrs
	}
	return false
}

func (m *QueryRequest) GetExcludeColumns() bool {
	if m != nil {
		return m.ExcludeColumns
	}
	return false
}

type QueryResponse struct {
	Err                  string           `protobuf:"bytes,1,opt,name=Err,proto3" json:"Err,omitempty"`
	Results              []*QueryResult   `protobuf:"bytes,2,rep,name=Results,proto3" json:"Results,omitempty"`
	ColumnAttrSets       []*ColumnAttrSet `protobuf:"bytes,3,rep,name=ColumnAttrSets,proto3" json:"ColumnAttrSets,omitempty"`
	XXX_NoUnkeyedLiteral struct{}         `json:"-"`
	XXX_unrecognized     []byte           `json:"-"`
	XXX_sizecache        int32            `json:"-"`
}

func (m *QueryResponse) Reset()         { *m = QueryResponse{} }
func (m *QueryResponse) String() string { return proto.CompactTextString(m) }
func (*QueryResponse) ProtoMessage()    {}
func (*QueryResponse) Descriptor() ([]byte, []int) {
	return fileDescriptor_a66c591da5e2e6d0, []int{9}
}

func (m *QueryResponse) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_QueryResponse.Unmarshal(m, b)
}
func (m *QueryResponse) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_QueryResponse.Marshal(b, m, deterministic)
}
func (m *QueryResponse) XXX_Merge(src proto.Message) {
	xxx_messageInfo_QueryResponse.Merge(m, src)
}
func (m *QueryResponse) XXX_Size() int {
	return xxx_messageInfo_QueryResponse.Size(m)
}
func (m *QueryResponse) XXX_DiscardUnknown() {
	xxx_messageInfo_QueryResponse.DiscardUnknown(m)
}

var xxx_messageInfo_QueryResponse proto.InternalMessageInfo

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
	Type                 uint32    `protobuf:"varint,6,opt,name=Type,proto3" json:"Type,omitempty"`
	Row                  *Row      `protobuf:"bytes,1,opt,name=Row,proto3" json:"Row,omitempty"`
	N                    uint64    `protobuf:"varint,2,opt,name=N,proto3" json:"N,omitempty"`
	Pairs                []*Pair   `protobuf:"bytes,3,rep,name=Pairs,proto3" json:"Pairs,omitempty"`
	ValCount             *ValCount `protobuf:"bytes,5,opt,name=ValCount,proto3" json:"ValCount,omitempty"`
	Changed              bool      `protobuf:"varint,4,opt,name=Changed,proto3" json:"Changed,omitempty"`
	XXX_NoUnkeyedLiteral struct{}  `json:"-"`
	XXX_unrecognized     []byte    `json:"-"`
	XXX_sizecache        int32     `json:"-"`
}

func (m *QueryResult) Reset()         { *m = QueryResult{} }
func (m *QueryResult) String() string { return proto.CompactTextString(m) }
func (*QueryResult) ProtoMessage()    {}
func (*QueryResult) Descriptor() ([]byte, []int) {
	return fileDescriptor_a66c591da5e2e6d0, []int{10}
}

func (m *QueryResult) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_QueryResult.Unmarshal(m, b)
}
func (m *QueryResult) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_QueryResult.Marshal(b, m, deterministic)
}
func (m *QueryResult) XXX_Merge(src proto.Message) {
	xxx_messageInfo_QueryResult.Merge(m, src)
}
func (m *QueryResult) XXX_Size() int {
	return xxx_messageInfo_QueryResult.Size(m)
}
func (m *QueryResult) XXX_DiscardUnknown() {
	xxx_messageInfo_QueryResult.DiscardUnknown(m)
}

var xxx_messageInfo_QueryResult proto.InternalMessageInfo

func (m *QueryResult) GetType() uint32 {
	if m != nil {
		return m.Type
	}
	return 0
}

func (m *QueryResult) GetRow() *Row {
	if m != nil {
		return m.Row
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

func (m *QueryResult) GetValCount() *ValCount {
	if m != nil {
		return m.ValCount
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
	Index                string   `protobuf:"bytes,1,opt,name=Index,proto3" json:"Index,omitempty"`
	Field                string   `protobuf:"bytes,2,opt,name=Field,proto3" json:"Field,omitempty"`
	Shard                uint64   `protobuf:"varint,3,opt,name=Shard,proto3" json:"Shard,omitempty"`
	RowIDs               []uint64 `protobuf:"varint,4,rep,packed,name=RowIDs,proto3" json:"RowIDs,omitempty"`
	ColumnIDs            []uint64 `protobuf:"varint,5,rep,packed,name=ColumnIDs,proto3" json:"ColumnIDs,omitempty"`
	RowKeys              []string `protobuf:"bytes,7,rep,name=RowKeys,proto3" json:"RowKeys,omitempty"`
	ColumnKeys           []string `protobuf:"bytes,8,rep,name=ColumnKeys,proto3" json:"ColumnKeys,omitempty"`
	Timestamps           []int64  `protobuf:"varint,6,rep,packed,name=Timestamps,proto3" json:"Timestamps,omitempty"`
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *ImportRequest) Reset()         { *m = ImportRequest{} }
func (m *ImportRequest) String() string { return proto.CompactTextString(m) }
func (*ImportRequest) ProtoMessage()    {}
func (*ImportRequest) Descriptor() ([]byte, []int) {
	return fileDescriptor_a66c591da5e2e6d0, []int{11}
}

func (m *ImportRequest) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_ImportRequest.Unmarshal(m, b)
}
func (m *ImportRequest) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_ImportRequest.Marshal(b, m, deterministic)
}
func (m *ImportRequest) XXX_Merge(src proto.Message) {
	xxx_messageInfo_ImportRequest.Merge(m, src)
}
func (m *ImportRequest) XXX_Size() int {
	return xxx_messageInfo_ImportRequest.Size(m)
}
func (m *ImportRequest) XXX_DiscardUnknown() {
	xxx_messageInfo_ImportRequest.DiscardUnknown(m)
}

var xxx_messageInfo_ImportRequest proto.InternalMessageInfo

func (m *ImportRequest) GetIndex() string {
	if m != nil {
		return m.Index
	}
	return ""
}

func (m *ImportRequest) GetField() string {
	if m != nil {
		return m.Field
	}
	return ""
}

func (m *ImportRequest) GetShard() uint64 {
	if m != nil {
		return m.Shard
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

func (m *ImportRequest) GetRowKeys() []string {
	if m != nil {
		return m.RowKeys
	}
	return nil
}

func (m *ImportRequest) GetColumnKeys() []string {
	if m != nil {
		return m.ColumnKeys
	}
	return nil
}

func (m *ImportRequest) GetTimestamps() []int64 {
	if m != nil {
		return m.Timestamps
	}
	return nil
}

type ImportValueRequest struct {
	Index                string   `protobuf:"bytes,1,opt,name=Index,proto3" json:"Index,omitempty"`
	Field                string   `protobuf:"bytes,2,opt,name=Field,proto3" json:"Field,omitempty"`
	Shard                uint64   `protobuf:"varint,3,opt,name=Shard,proto3" json:"Shard,omitempty"`
	ColumnIDs            []uint64 `protobuf:"varint,5,rep,packed,name=ColumnIDs,proto3" json:"ColumnIDs,omitempty"`
	ColumnKeys           []string `protobuf:"bytes,7,rep,name=ColumnKeys,proto3" json:"ColumnKeys,omitempty"`
	Values               []int64  `protobuf:"varint,6,rep,packed,name=Values,proto3" json:"Values,omitempty"`
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *ImportValueRequest) Reset()         { *m = ImportValueRequest{} }
func (m *ImportValueRequest) String() string { return proto.CompactTextString(m) }
func (*ImportValueRequest) ProtoMessage()    {}
func (*ImportValueRequest) Descriptor() ([]byte, []int) {
	return fileDescriptor_a66c591da5e2e6d0, []int{12}
}

func (m *ImportValueRequest) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_ImportValueRequest.Unmarshal(m, b)
}
func (m *ImportValueRequest) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_ImportValueRequest.Marshal(b, m, deterministic)
}
func (m *ImportValueRequest) XXX_Merge(src proto.Message) {
	xxx_messageInfo_ImportValueRequest.Merge(m, src)
}
func (m *ImportValueRequest) XXX_Size() int {
	return xxx_messageInfo_ImportValueRequest.Size(m)
}
func (m *ImportValueRequest) XXX_DiscardUnknown() {
	xxx_messageInfo_ImportValueRequest.DiscardUnknown(m)
}

var xxx_messageInfo_ImportValueRequest proto.InternalMessageInfo

func (m *ImportValueRequest) GetIndex() string {
	if m != nil {
		return m.Index
	}
	return ""
}

func (m *ImportValueRequest) GetField() string {
	if m != nil {
		return m.Field
	}
	return ""
}

func (m *ImportValueRequest) GetShard() uint64 {
	if m != nil {
		return m.Shard
	}
	return 0
}

func (m *ImportValueRequest) GetColumnIDs() []uint64 {
	if m != nil {
		return m.ColumnIDs
	}
	return nil
}

func (m *ImportValueRequest) GetColumnKeys() []string {
	if m != nil {
		return m.ColumnKeys
	}
	return nil
}

func (m *ImportValueRequest) GetValues() []int64 {
	if m != nil {
		return m.Values
	}
	return nil
}

func init() {
	proto.RegisterType((*Row)(nil), "gopilosa_pbuf.Row")
	proto.RegisterType((*Pair)(nil), "gopilosa_pbuf.Pair")
	proto.RegisterType((*ValCount)(nil), "gopilosa_pbuf.ValCount")
	proto.RegisterType((*Bit)(nil), "gopilosa_pbuf.Bit")
	proto.RegisterType((*BulkColumnAttrRequest)(nil), "gopilosa_pbuf.BulkColumnAttrRequest")
	proto.RegisterType((*ColumnAttrSet)(nil), "gopilosa_pbuf.ColumnAttrSet")
	proto.RegisterType((*Attr)(nil), "gopilosa_pbuf.Attr")
	proto.RegisterType((*AttrMap)(nil), "gopilosa_pbuf.AttrMap")
	proto.RegisterType((*QueryRequest)(nil), "gopilosa_pbuf.QueryRequest")
	proto.RegisterType((*QueryResponse)(nil), "gopilosa_pbuf.QueryResponse")
	proto.RegisterType((*QueryResult)(nil), "gopilosa_pbuf.QueryResult")
	proto.RegisterType((*ImportRequest)(nil), "gopilosa_pbuf.ImportRequest")
	proto.RegisterType((*ImportValueRequest)(nil), "gopilosa_pbuf.ImportValueRequest")
}

func init() { proto.RegisterFile("gopilosa_pbuf/public.proto", fileDescriptor_a66c591da5e2e6d0) }

var fileDescriptor_a66c591da5e2e6d0 = []byte{
	// 721 bytes of a gzipped FileDescriptorProto
	0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0xff, 0xac, 0x55, 0xcd, 0x6e, 0xd3, 0x4a,
	0x14, 0xd6, 0xc4, 0xce, 0xdf, 0x49, 0xd3, 0x7b, 0x35, 0xb7, 0xb7, 0x58, 0x55, 0x85, 0x2c, 0x0b,
	0x21, 0xb3, 0x29, 0x52, 0xda, 0x75, 0x25, 0xda, 0xb4, 0x52, 0x54, 0x51, 0xc1, 0xa4, 0x94, 0x0d,
	0x02, 0xb9, 0xcd, 0xd0, 0x5a, 0x4c, 0x3c, 0xc6, 0x1e, 0x2b, 0xcd, 0xc3, 0xb0, 0x67, 0xc1, 0x83,
	0xb0, 0xe3, 0x19, 0x78, 0x13, 0x34, 0x67, 0x3c, 0xb1, 0x63, 0x24, 0x0a, 0x12, 0xbb, 0xf9, 0xbe,
	0x73, 0xce, 0xf8, 0xfc, 0x7c, 0x73, 0x0c, 0x3b, 0x37, 0x32, 0x8d, 0x85, 0xcc, 0xa3, 0x77, 0xe9,
	0x55, 0xf1, 0xfe, 0x69, 0x5a, 0x5c, 0x89, 0xf8, 0x7a, 0x2f, 0xcd, 0xa4, 0x92, 0x74, 0xb8, 0x66,
	0x0b, 0xde, 0x82, 0xc3, 0xe4, 0x82, 0x7a, 0xd0, 0x3d, 0x96, 0xa2, 0x98, 0x27, 0xb9, 0x47, 0x7c,
	0x27, 0x74, 0x99, 0x85, 0x94, 0x82, 0x7b, 0xc6, 0x97, 0xb9, 0xe7, 0xf8, 0x4e, 0xd8, 0x67, 0x78,
	0xa6, 0x4f, 0xa0, 0xfd, 0x4c, 0xa9, 0x2c, 0xf7, 0x5a, 0xbe, 0x13, 0x0e, 0x46, 0xff, 0xed, 0xad,
	0xdd, 0xb9, 0xa7, 0x6d, 0xcc, 0x78, 0x04, 0x87, 0xe0, 0xbe, 0x88, 0xe2, 0x8c, 0x6e, 0x42, 0x6b,
	0x32, 0xf6, 0x88, 0x4f, 0x42, 0x97, 0xb5, 0x26, 0x63, 0xfa, 0x2f, 0x38, 0x67, 0x7c, 0xe9, 0x39,
	0x3e, 0x09, 0xfb, 0x4c, 0x1f, 0xe9, 0x16, 0xb4, 0x8f, 0x65, 0x91, 0x28, 0xaf, 0x85, 0x4e, 0x06,
	0x04, 0x23, 0xe8, 0x5d, 0x46, 0x02, 0xcf, 0x3a, 0xe6, 0x32, 0x12, 0x78, 0x89, 0xc3, 0xf4, 0x71,
	0x3d, 0xc6, 0xb1, 0x31, 0xaf, 0xc0, 0x39, 0x8a, 0x95, 0x36, 0x32, 0xb9, 0x58, 0x7d, 0xd5, 0x00,
	0xba, 0x03, 0x3d, 0x53, 0xda, 0x64, 0x5c, 0x7e, 0x69, 0x85, 0xe9, 0x2e, 0xf4, 0x2f, 0xe2, 0x39,
	0xcf, 0x55, 0x34, 0x4f, 0x31, 0x35, 0x87, 0x55, 0x44, 0xf0, 0x1a, 0xfe, 0x3f, 0x2a, 0xc4, 0x07,
	0xe3, 0x8d, 0x35, 0xf2, 0x8f, 0x05, 0xcf, 0x15, 0x3d, 0x84, 0xc1, 0xb1, 0x14, 0x9a, 0x99, 0x72,
	0x65, 0x1a, 0x38, 0x18, 0xed, 0x36, 0x9a, 0x52, 0x85, 0x4d, 0xb9, 0x62, 0xf5, 0x80, 0xe0, 0x0d,
	0x0c, 0xd7, 0xac, 0xbf, 0xd1, 0xac, 0x3f, 0x98, 0xc0, 0x67, 0x02, 0xae, 0x3e, 0xd9, 0x5b, 0x48,
	0x75, 0x0b, 0x05, 0xf7, 0x62, 0x99, 0xf2, 0xb2, 0x0f, 0x78, 0xa6, 0x3e, 0x0c, 0xa6, 0x2a, 0x8b,
	0x93, 0x9b, 0xcb, 0x48, 0x14, 0xbc, 0xfc, 0x66, 0x9d, 0xd2, 0x1d, 0x9c, 0x24, 0xca, 0x98, 0x5d,
	0x6c, 0xd2, 0x0a, 0xeb, 0x0e, 0x1e, 0x49, 0x29, 0x8c, 0xb1, 0xed, 0x93, 0xb0, 0xc7, 0x2a, 0x82,
	0x3e, 0x04, 0x38, 0x15, 0x32, 0x2a, 0x63, 0x3b, 0x3e, 0x09, 0x09, 0xab, 0x31, 0xc1, 0x01, 0x74,
	0x75, 0xa6, 0xcf, 0xa3, 0xb4, 0x2a, 0x90, 0xdc, 0x5b, 0xe0, 0x57, 0x02, 0x1b, 0x2f, 0x0b, 0x9e,
	0x2d, 0xed, 0x3c, 0xb6, 0xa0, 0x8d, 0xb8, 0x2c, 0xd5, 0x00, 0xba, 0x0d, 0x9d, 0xe9, 0x6d, 0x94,
	0xcd, 0x4c, 0xcf, 0x5c, 0x56, 0x22, 0x5d, 0x70, 0xd5, 0xfd, 0x1c, 0x0b, 0xee, 0xb1, 0x3a, 0xa5,
	0x23, 0x19, 0x9f, 0x4b, 0x65, 0x2b, 0x2a, 0x11, 0x0d, 0xe1, 0x9f, 0x93, 0xbb, 0x6b, 0x51, 0xcc,
	0x38, 0x93, 0x0b, 0x13, 0xdd, 0x41, 0x87, 0x26, 0x4d, 0x1f, 0xc3, 0x66, 0x49, 0xd9, 0x57, 0xd6,
	0x45, 0xc7, 0x06, 0x1b, 0x7c, 0x22, 0x30, 0x2c, 0x4b, 0xc9, 0x53, 0x99, 0xe4, 0x5c, 0x0f, 0xed,
	0x24, 0xcb, 0xec, 0xd0, 0x4e, 0xb2, 0x8c, 0x1e, 0x40, 0x97, 0xf1, 0xbc, 0x10, 0xca, 0x0e, 0x7f,
	0xa7, 0xd1, 0x1b, 0x7b, 0x41, 0x21, 0x14, 0xb3, 0xae, 0x74, 0x0c, 0x9b, 0x6b, 0x1a, 0x33, 0x0f,
	0xfa, 0x3e, 0x99, 0x36, 0x62, 0x82, 0x6f, 0x04, 0x06, 0xb5, 0xeb, 0x57, 0x02, 0xd2, 0x65, 0x0f,
	0x4b, 0x01, 0x3d, 0xc2, 0x8d, 0x82, 0x19, 0x0f, 0x46, 0xb4, 0x71, 0x3d, 0x93, 0x0b, 0x86, 0x0b,
	0x67, 0x03, 0xc8, 0x79, 0xa9, 0x3b, 0x72, 0xae, 0xa7, 0xad, 0xb7, 0x84, 0x4d, 0xaa, 0x39, 0x6d,
	0x6d, 0x63, 0xc6, 0x83, 0xee, 0x57, 0x0b, 0x01, 0xc7, 0x31, 0x18, 0x3d, 0x68, 0x78, 0x5b, 0x33,
	0xab, 0x36, 0x87, 0x5e, 0x6f, 0xb7, 0x51, 0x72, 0xc3, 0x67, 0xa8, 0xd8, 0x1e, 0xb3, 0x30, 0xf8,
	0x4e, 0x60, 0x38, 0x99, 0xa7, 0x32, 0x53, 0x35, 0xf5, 0x4c, 0x92, 0x19, 0xbf, 0xb3, 0xea, 0x41,
	0xa0, 0xd9, 0xd3, 0x98, 0x8b, 0x19, 0xe6, 0xdc, 0x67, 0x06, 0x68, 0x16, 0x55, 0x84, 0xaa, 0x71,
	0x99, 0x01, 0xa8, 0x17, 0xbd, 0x6b, 0x72, 0xcf, 0x35, 0x4a, 0x33, 0x48, 0x3f, 0x0e, 0xbb, 0x6a,
	0x72, 0xaf, 0x8d, 0xa6, 0x8a, 0xd0, 0x39, 0x32, 0xb9, 0xc0, 0x5d, 0xdb, 0xc5, 0x5d, 0x6b, 0xa1,
	0x7e, 0x36, 0xc6, 0x0d, 0x8d, 0x3d, 0x34, 0xd6, 0x18, 0x6d, 0x5f, 0x6d, 0x29, 0x2d, 0x41, 0x27,
	0x74, 0x58, 0x8d, 0x09, 0xbe, 0x10, 0xa0, 0xa6, 0x46, 0x7c, 0x66, 0x7f, 0xaf, 0xd0, 0x5f, 0x17,
	0xb4, 0x9e, 0x76, 0xf7, 0xa7, 0xb4, 0xb7, 0xa1, 0x83, 0xf9, 0xd8, 0x94, 0x4b, 0x74, 0xd5, 0xc1,
	0x1f, 0xd5, 0xfe, 0x8f, 0x00, 0x00, 0x00, 0xff, 0xff, 0xf2, 0xf9, 0x80, 0x3f, 0xc6, 0x06, 0x00,
	0x00,
}
