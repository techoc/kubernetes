/*
Copyright 2024 The Kubernetes Authors.

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

package etcd3

import (
	"context"
	"fmt"
	"time"

	"k8s.io/apimachinery/pkg/conversion"
	"k8s.io/apimachinery/pkg/runtime"
	endpointsrequest "k8s.io/apiserver/pkg/endpoints/request"
	"k8s.io/apiserver/pkg/storage"

	"k8s.io/klog/v2"
)

// NewDefaultDecoder 返回 etcd3 store 使用的默认解码器。
func NewDefaultDecoder(codec runtime.Codec, versioner storage.Versioner) Decoder {
	return &defaultDecoder{
		codec:     codec,
		versioner: versioner,
	}
}

// Decoder 被 etcd 存储实现用来把存储中的转换后数据解码为对象。
// 把存储中的转换后数据解码为对象。
type Decoder interface {
	// Decode 把字节值解码为对象，并把对象的 resource version 设为 rev。
	// 成功时 objPtr 会被设置为解码出的对象。
	// 成功时 objPtr 被设置为该对象。
	Decode(value []byte, objPtr runtime.Object, rev int64) error

	// DecodeListItem 把数组中的字节值解码为对象。
	DecodeListItem(ctx context.Context, data []byte, rev uint64, newItemFunc func() runtime.Object) (runtime.Object, error)
}

var _ Decoder = &defaultDecoder{}

type defaultDecoder struct {
	codec     runtime.Codec
	versioner storage.Versioner
}

// decode 把字节值解码为对象，并把对象的 resource version 设为 rev。成功时 objPtr 会被设置。
// 成功时 objPtr 被设置为该对象。
func (d *defaultDecoder) Decode(value []byte, objPtr runtime.Object, rev int64) error {
	// 校验传入的是指针：codec 要求目标必须是可写的指针，否则无法填充对象。
	if _, err := conversion.EnforcePtr(objPtr); err != nil {
		// nolint:errorlint // this code was moved from store.go as is
		return fmt.Errorf("unable to convert output object to pointer: %v", err)
	}
	_, _, err := d.codec.Decode(value, nil, objPtr)
	if err != nil {
		return err
	}
	// 无法设置版本并不妨碍对象被提取出来
	//
	if err := d.versioner.UpdateObject(objPtr, uint64(rev)); err != nil {
		klog.Errorf("failed to update object version: %v", err)
	}
	return nil
}

// decodeListItem 把数组中的字节值解码为对象。
//
// 列表条目解码会在每次迭代中被调用，因此这里统计逐条目解码延迟，帮助定位
// 大规模 List 请求的耗时瓶颈。
func (d *defaultDecoder) DecodeListItem(ctx context.Context, data []byte, rev uint64, newItemFunc func() runtime.Object) (runtime.Object, error) {
	startedAt := time.Now()
	defer func() {
		endpointsrequest.TrackDecodeLatency(ctx, time.Since(startedAt))
	}()

	obj, _, err := d.codec.Decode(data, nil, newItemFunc())
	if err != nil {
		return nil, err
	}

	if err := d.versioner.UpdateObject(obj, rev); err != nil {
		klog.Errorf("failed to update object version: %v", err)
	}

	return obj, nil
}
