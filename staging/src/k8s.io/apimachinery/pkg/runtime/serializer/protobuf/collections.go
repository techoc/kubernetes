/*
Copyright 2025 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package protobuf

import (
	"encoding/binary"
	"errors"
	"io"
	"math/bits"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/conversion"
	"k8s.io/apimachinery/pkg/runtime"
)

// 预定义错误：校验 List 类型布局失败时返回，调用方据此回退到通用编码器。
// 错误信息本身就是对期望布局的说明。
var (
	errFieldCount          = errors.New("expected ListType to have 3 fields")
	errTypeMetaField       = errors.New("expected TypeMeta field to have TypeMeta type")
	errTypeMetaProtobufTag = errors.New(`expected TypeMeta protobuf field tag to be ""`)
	errListMetaField       = errors.New("expected ListMeta field to have ListMeta type")
	errListMetaProtobufTag = errors.New(`expected ListMeta protobuf field tag to be "bytes,1,opt,name=metadata"`)
	errItemsProtobufTag    = errors.New(`expected Items protobuf field tag to be "bytes,2,rep,name=items"`)
	errItemsSizer          = errors.New(`expected Items elements to implement proto.Sizer`)
)

// getStreamingListData 实现 protobuf 流式序列化时的列表提取逻辑。
//
// 不复用 meta 包 accessor 的原因：需要严格校验 protobuf tag 与字段布局，
// 并保留 nil/空 Items 的区别（ListMetaAccessor 无法区分二者）；
// TypeAccessor 重新解析 apiVersion 并以 "{group}/{version}" 形式序列化，
// 也会改变编码结果。
func getStreamingListData(list runtime.Object) (data streamingListData, err error) {
	listValue, err := conversion.EnforcePtr(list)
	if err != nil {
		return data, err
	}
	listType := listValue.Type()
	if listType.NumField() != 3 {
		return data, errFieldCount
	}
	// TypeMeta 在 protobuf 中不序列化（GVK 由外层 Unknown 承载），这里只校验。
	_, ok := listValue.Field(0).Interface().(metav1.TypeMeta)
	if !ok {
		return data, errTypeMetaField
	}
	if listType.Field(0).Tag.Get("protobuf") != "" {
		return data, errTypeMetaProtobufTag
	}
	// ListMeta
	listMeta, ok := listValue.Field(1).Interface().(metav1.ListMeta)
	if !ok {
		return data, errListMetaField
	}
	// 字段号 1 对应 wire tag 0xa；若放宽 protobuf tag 校验，写入 ListMeta 时必须同步修改下方硬编码。
	if listType.Field(1).Tag.Get("protobuf") != "bytes,1,opt,name=metadata" {
		return data, errListMetaProtobufTag
	}
	data.listMeta = listMeta
	// Items；字段号 2 对应 wire tag 0x12；若放宽 protobuf tag 校验，写入 Items 时必须同步修改下方硬编码。
	if listType.Field(2).Tag.Get("protobuf") != "bytes,2,rep,name=items" {
		return data, errItemsProtobufTag
	}
	items, err := meta.ExtractList(list)
	if err != nil {
		return data, err
	}
	data.items = items
	// 预先计算各段大小，供一次性分配与 Unknown 头部使用。
	data.totalSize, data.listMetaSize, data.itemsSizes, err = listSize(listMeta, items)
	return data, err
}

// streamingListData 缓存流式编码 List 所需的元数据与尺寸，避免重复 Size() 调用。
type streamingListData struct {
	// totalSize 是 List 序列化后的总大小，包含 proto 头部的长度字节。
	totalSize int

	// listMetaSize 缓存 listMeta.Size() 的结果，不含头部字节（字段标识与长度）。
	listMetaSize int
	listMeta     metav1.ListMeta

	// itemsSizes 缓存各 item 的 Size() 结果，不含头部字节（字段标识与长度）。
	itemsSizes []int
	items      []runtime.Object
}

// sizer 是 generated.pb.go 中 proto.Sizer 的本地最小接口。
type sizer interface {
	Size() int
}

// listSize 返回 ListMeta 与各元素的大小，供后续预分配使用。
// listMetaSize 与 itemSizes 不含头部字节（字段标识、长度）；总大小则为
// 每个元素加上 1 字节 tag 与 varint 长度前缀，得到精确的 wire 大小。
func listSize(listMeta metav1.ListMeta, items []runtime.Object) (totalSize, listMetaSize int, itemSizes []int, err error) {
	// ListMeta
	listMetaSize = listMeta.Size()
	totalSize += 1 + sovGenerated(uint64(listMetaSize)) + listMetaSize
	// Items
	itemSizes = make([]int, len(items))
	for i, item := range items {
		sizer, ok := item.(sizer)
		if !ok {
			// 无法预知大小则拒绝流式编码，由调用方回退。
			return totalSize, listMetaSize, nil, errItemsSizer
		}
		n := sizer.Size()
		itemSizes[i] = n
		totalSize += 1 + sovGenerated(uint64(n)) + n
	}
	return totalSize, listMetaSize, itemSizes, nil
}

// streamingEncodeUnknownList 写出 runtime 前缀，并把 List 内容作为
// runtime.Unknown 的 Raw 字段流式写入。
func streamingEncodeUnknownList(w io.Writer, unk runtime.Unknown, listData streamingListData, memAlloc runtime.MemoryAllocator) error {
	_, err := w.Write(protoEncodingPrefix)
	if err != nil {
		return err
	}
	// encodeList 负责把 List 编码进 unknown 的 Raw 字段。
	encodeList := func(writer io.Writer) (int, error) {
		return streamingEncodeList(writer, listData, memAlloc)
	}
	_, err = unk.MarshalToWriter(w, listData.totalSize, encodeList)
	return err
}

// streamingEncodeList 按 protobuf wire 格式写出 ListMeta 与 Items，返回写入字节数。
func streamingEncodeList(w io.Writer, listData streamingListData, memAlloc runtime.MemoryAllocator) (size int, err error) {
	// 头部缓冲会被 w.Write 立即消费，因此每次调用分配一次即可，避免逐项分配。
	headerScratch := make([]byte, 1+binary.MaxVarintLen64)
	// ListMeta；0xa = (1 << 3) | 2；字段号：1，类型：2（LEN）。 https://protobuf.dev/programming-guides/encoding/#structure
	n, err := doEncodeWithHeader(&listData.listMeta, w, 0xa, listData.listMetaSize, headerScratch, memAlloc)
	size += n
	if err != nil {
		return size, err
	}
	// Items；0x12 = (2 << 3) | 2；字段号：2，类型：2（LEN）。 https://protobuf.dev/programming-guides/encoding/#structure
	for i, item := range listData.items {
		n, err := doEncodeWithHeader(item, w, 0x12, listData.itemsSizes[i], headerScratch, memAlloc)
		size += n
		if err != nil {
			return size, err
		}
	}
	return size, nil
}

// sovGenerated 复制自 `generated.pb.go`，返回 varint 的字节数。
//
// 与生成代码保持一致，保证尺寸计算与真实编码完全匹配。
func sovGenerated(v uint64) int {
	return (bits.Len64(v|1) + 6) / 7
}

// encodeVarintGenerated 复制自 `generated.pb.go`，编码 varint。
//
// 先按 varint 长度回退偏移，再正向写入字节；与生成代码逐字节兼容。
func encodeVarintGenerated(dAtA []byte, offset int, v uint64) int {
	offset -= sovGenerated(v)
	base := offset
	for v >= 1<<7 {
		dAtA[offset] = uint8(v&0x7f | 0x80)
		v >>= 7
		offset++
	}
	dAtA[offset] = uint8(v)
	return base
}
