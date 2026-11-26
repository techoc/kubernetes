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
	"fmt"
	"io"
	"testing"
	"time"

	v1 "k8s.io/api/core/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	runtimejson "k8s.io/apimachinery/pkg/runtime/serializer/json"
	"k8s.io/apimachinery/pkg/runtime/serializer/streaming"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes/scheme"
)

// getDecoder 模仿 k8s.io/client-go/rest.createSerializers 创建解码器的方式。
func getDecoder() runtime.Decoder {
	jsonSerializer := runtimejson.NewSerializerWithOptions(runtimejson.DefaultMetaFactory, scheme.Scheme, scheme.Scheme, runtimejson.SerializerOptions{})
	directCodecFactory := scheme.Codecs.WithoutConversion()
	return directCodecFactory.DecoderToVersion(jsonSerializer, v1.SchemeGroupVersion)
}

// TestDecoder 对每种 watch 事件类型验证“编码 -> 流式解码”的往返一致性，
// 并确认流关闭后继续 Decode 会返回错误而不是阻塞。
func TestDecoder(t *testing.T) {
	table := []watch.EventType{watch.Added, watch.Deleted, watch.Modified, watch.Error, watch.Bookmark}

	for _, eventType := range table {
		out, in := io.Pipe()

		decoder := NewDecoder(streaming.NewDecoder(out, getDecoder()), getDecoder())
		expect := &v1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "foo"}}
		encoder := json.NewEncoder(in)
		eType := eventType
		errc := make(chan error)

		go func() {
			data, err := runtime.Encode(scheme.Codecs.LegacyCodec(v1.SchemeGroupVersion), expect)
			if err != nil {
				errc <- fmt.Errorf("Unexpected error %v", err)
				return
			}
			event := metav1.WatchEvent{
				Type:   string(eType),
				Object: runtime.RawExtension{Raw: json.RawMessage(data)},
			}
			if err := encoder.Encode(&event); err != nil {
				t.Errorf("Unexpected error %v", err)
			}
			in.Close()
		}()

		done := make(chan struct{})
		go func() {
			action, got, err := decoder.Decode()
			if err != nil {
				errc <- fmt.Errorf("Unexpected error %v", err)
				return
			}
			if e, a := eType, action; e != a {
				t.Errorf("Expected %v, got %v", e, a)
			}
			if e, a := expect, got; !apiequality.Semantic.DeepDerivative(e, a) {
				t.Errorf("Expected %v, got %v", e, a)
			}
			t.Logf("Exited read")
			close(done)
		}()
		select {
		case err := <-errc:
			t.Fatal(err)
		case <-done:
		}

		done = make(chan struct{})
		go func() {
			_, _, err := decoder.Decode()
			if err == nil {
				t.Errorf("Unexpected nil error")
			}
			close(done)
		}()
		<-done

		decoder.Close()
	}
}

// TestDecoder_SourceClose 验证底层 pipe 被关闭时 Decode 能及时返回错误，
// 而不是永久阻塞（watch 连接断开是常见场景）。
func TestDecoder_SourceClose(t *testing.T) {
	out, in := io.Pipe()
	decoder := NewDecoder(streaming.NewDecoder(out, getDecoder()), getDecoder())

	done := make(chan struct{})

	go func() {
		_, _, err := decoder.Decode()
		if err == nil {
			t.Errorf("Unexpected nil error")
		}
		close(done)
	}()

	in.Close()

	select {
	case <-done:
		break
	case <-time.After(wait.ForeverTestTimeout):
		t.Error("Timeout")
	}
}
