/*
Copyright 2014 The Kubernetes Authors.

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

package watch

import (
	"fmt"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/serializer/streaming"
	"k8s.io/apimachinery/pkg/watch"
)

// Decoder 为 io.ReadCloser 实现 watch.Decoder 接口。
//
// 流内容是一连串 metav1.WatchEvent，由 streaming 解码器（decoder）逐条解出；
// 每条事件里的对象是 RawExtension，再由 embeddedDecoder 做第二层解码，
// 最终得到具体 API 类型的 runtime.Object。两层解码是必要的：外层只认识
// 通用的 WatchEvent 信封，内层才知道具体资源类型。
type Decoder struct {
	decoder         streaming.Decoder
	embeddedDecoder runtime.Decoder
}

// NewDecoder 根据给定的流式解码器与内嵌解码器创建 Decoder。
func NewDecoder(decoder streaming.Decoder, embeddedDecoder runtime.Decoder) *Decoder {
	return &Decoder{
		decoder:         decoder,
		embeddedDecoder: embeddedDecoder,
	}
}

// Decode 阻塞直到能返回读取器中的下一个对象。
// 读取器关闭或对象无法解码时返回错误。
func (d *Decoder) Decode() (watch.EventType, runtime.Object, error) {
	var got metav1.WatchEvent
	res, _, err := d.decoder.Decode(nil, &got)
	if err != nil {
		return "", nil, err
	}
	// 解码器可能把内容解成别的类型；只有解成 WatchEvent 才继续。
	if res != &got {
		return "", nil, fmt.Errorf("unable to decode to metav1.WatchEvent")
	}
	// 校验事件类型，防止服务端返回无法识别的类型时被静默透传。
	switch got.Type {
	case string(watch.Added), string(watch.Modified), string(watch.Deleted), string(watch.Error), string(watch.Bookmark):
	default:
		return "", nil, fmt.Errorf("got invalid watch event type: %v", got.Type)
	}

	obj, err := runtime.Decode(d.embeddedDecoder, got.Object.Raw)
	if err != nil {
		return "", nil, fmt.Errorf("unable to decode watch event: %v", err)
	}
	return watch.EventType(got.Type), obj, nil
}

// Close 关闭底层读取器。
func (d *Decoder) Close() {
	d.decoder.Close()
}
