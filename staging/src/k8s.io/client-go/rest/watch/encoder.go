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
	"encoding/json"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/serializer/streaming"
	"k8s.io/apimachinery/pkg/watch"
)

// Encoder 把 watch.Event 序列化到 io.Writer。
//
// 内外两层分别编码：先由 embeddedEncoder 把事件对象编成字节，
// 再由 encoder 把包着这些字节的 WatchEvent 信封写出。
// TODO: this type is only used by tests
type Encoder struct {
	encoder         streaming.Encoder
	embeddedEncoder runtime.Encoder
}

// NewEncoder 根据给定的流式编码器与内嵌编码器创建 Encoder。
func NewEncoder(encoder streaming.Encoder, embeddedEncoder runtime.Encoder) *Encoder {
	return &Encoder{
		encoder:         encoder,
		embeddedEncoder: embeddedEncoder,
	}
}

// Encode 把事件写入 writer。writer 已关闭或对象无法编码时返回错误。
func (e *Encoder) Encode(event *watch.Event) error {
	data, err := runtime.Encode(e.embeddedEncoder, event.Object)
	if err != nil {
		return err
	}
	// FIXME: get rid of json.RawMessage.
	return e.encoder.Encode(&metav1.WatchEvent{
		Type:   string(event.Type),
		Object: runtime.RawExtension{Raw: json.RawMessage(data)},
	})
}
