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
	"bytes"
	"io"
	"testing"

	v1 "k8s.io/api/core/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	runtimejson "k8s.io/apimachinery/pkg/runtime/serializer/json"
	"k8s.io/apimachinery/pkg/runtime/serializer/streaming"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes/scheme"
)

// getEncoder 模仿 k8s.io/client-go/rest.createSerializers 构造 encoder 的方式，
// 复现 rest 客户端的序列化器构造——JSON serializer + 不做转换的 codec。
// 使用同样的构造可确保测试覆盖真实的线上编码行为。
func getEncoder() runtime.Encoder {
	jsonSerializer := runtimejson.NewSerializerWithOptions(runtimejson.DefaultMetaFactory, scheme.Scheme, scheme.Scheme, runtimejson.SerializerOptions{})
	directCodecFactory := scheme.Codecs.WithoutConversion()
	return directCodecFactory.EncoderForVersion(jsonSerializer, v1.SchemeGroupVersion)
}

// TestEncodeDecodeRoundTrip 验证 watch 事件编码后再解码能还原类型与对象内容。
// 覆盖 Added/Modified/Deleted/Bookmark 四种事件类型。Bookmark 事件的对象
// 只携带 resourceVersion，但这里仍用完整 Pod 确保编解码路径一致。比较使用
// DeepDerivative，忽略解码后补零的字段（例如 TypeMeta 未设置时）。
func TestEncodeDecodeRoundTrip(t *testing.T) {
	testCases := []struct {
		Type   watch.EventType
		Object runtime.Object
	}{
		{
			watch.Added,
			&v1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "foo"}},
		},
		{
			watch.Modified,
			&v1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "foo"}},
		},
		{
			watch.Deleted,
			&v1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "foo"}},
		},
		{
			watch.Bookmark,
			&v1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "foo"}},
		},
	}
	for i, testCase := range testCases {
		buf := &bytes.Buffer{}

		// 中文：encoder 需要同时提供 streaming 层与单对象编解码器，与 rest watch 的构造一致。
		encoder := NewEncoder(streaming.NewEncoder(buf, getEncoder()), getEncoder())
		if err := encoder.Encode(&watch.Event{Type: testCase.Type, Object: testCase.Object}); err != nil {
			t.Errorf("%d: unexpected error: %v", i, err)
			continue
		}

		rc := io.NopCloser(buf)
		decoder := NewDecoder(streaming.NewDecoder(rc, getDecoder()), getDecoder())
		event, obj, err := decoder.Decode()
		if err != nil {
			t.Errorf("%d: unexpected error: %v", i, err)
			continue
		}
		if !apiequality.Semantic.DeepDerivative(testCase.Object, obj) {
			t.Errorf("%d: expected %#v, got %#v", i, testCase.Object, obj)
		}
		// 中文：事件类型经由 JSON 的 type 字段往返，必须保持不变。
		if event != testCase.Type {
			t.Errorf("%d: unexpected type: %#v", i, event)
		}
	}
}
