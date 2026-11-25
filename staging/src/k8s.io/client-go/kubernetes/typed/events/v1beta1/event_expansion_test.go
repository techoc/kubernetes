/*
Copyright 2021 The Kubernetes Authors.

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

package v1beta1

import (
	"io"
	"net/http"
	"strings"
	"testing"

	"k8s.io/api/events/v1beta1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/scheme"
	restfake "k8s.io/client-go/rest/fake"
)

// TestCreateWithEventNamespace 验证 CreateWithEventNamespace 会用事件自身的
// namespace 发起请求，且拒绝与客户端 namespace 不一致的事件。
// fake RESTClient 对任何请求都返回 {}，因此断言重点在错误路径——即
// 客户端在真正发请求前就应发现 namespace 冲突。该能力用于把事件写到事件所属
// namespace，而不是客户端创建时绑定的 namespace。
func TestCreateWithEventNamespace(t *testing.T) {
	event := &v1beta1.Event{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test",
			Namespace: "default",
		},
	}
	cli := &restfake.RESTClient{
		Client: restfake.CreateHTTPClient(func(request *http.Request) (*http.Response, error) {
			resp := &http.Response{
				StatusCode: http.StatusOK,
				Body:       io.NopCloser(strings.NewReader("{}")),
			}
			return resp, nil
		}),
		NegotiatedSerializer: scheme.Codecs.WithoutConversion(),
	}

	tests := []struct {
		name    string
		ns      string
		event   *v1beta1.Event
		wantErr bool
	}{
		{
			// 中文：客户端与事件命名空间一致，应成功。
			name:  "create event",
			ns:    "default",
			event: event,
		},
		{
			// 中文：事件在 default，客户端在 other，应返回错误且不发请求。
			name:    "create event with different namespace",
			ns:      "other",
			event:   event,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := newEvents(New(cli), tt.ns)
			_, err := e.CreateWithEventNamespace(tt.event)
			if (err != nil) != tt.wantErr {
				t.Errorf("CreateWithEventNamespace() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
		})
	}
}

// TestPatchWithEventNamespace 验证 PatchWithEventNamespace 的 namespace 校验逻辑。
// 数据为空 JSON patch（[]byte{}），成功路径只验证请求能被构造并通过 fake
// 客户端；重点同样是 namespace 不匹配时必须提前失败。
func TestPatchWithEventNamespace(t *testing.T) {
	event := &v1beta1.Event{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test",
			Namespace: "default",
		},
	}
	cli := &restfake.RESTClient{
		Client: restfake.CreateHTTPClient(func(request *http.Request) (*http.Response, error) {
			resp := &http.Response{
				StatusCode: http.StatusOK,
				Body:       io.NopCloser(strings.NewReader("{}")),
			}
			return resp, nil
		}),
		NegotiatedSerializer: scheme.Codecs.WithoutConversion(),
	}

	tests := []struct {
		name    string
		ns      string
		event   *v1beta1.Event
		data    []byte
		wantErr bool
	}{
		{
			name:  "patch event",
			ns:    "default",
			event: event,
			data:  []byte{},
		},
		{
			name:    "patch event with different namespace",
			ns:      "other",
			event:   event,
			data:    []byte{},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := newEvents(New(cli), tt.ns)
			_, err := e.PatchWithEventNamespace(tt.event, tt.data)
			if (err != nil) != tt.wantErr {
				t.Errorf("PatchWithEventNamespace() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
		})
	}
}

// TestUpdateWithEventNamespace 验证 UpdateWithEventNamespace 的 namespace 校验逻辑。
//
// 中文：子测试名称沿用 “patch event”，是复制自上一个测试的历史命名，保留以免
// 影响依赖测试名的外部工具。
func TestUpdateWithEventNamespace(t *testing.T) {
	event := &v1beta1.Event{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test",
			Namespace: "default",
		},
	}
	cli := &restfake.RESTClient{
		Client: restfake.CreateHTTPClient(func(request *http.Request) (*http.Response, error) {
			resp := &http.Response{
				StatusCode: http.StatusOK,
				Body:       io.NopCloser(strings.NewReader("{}")),
			}
			return resp, nil
		}),
		NegotiatedSerializer: scheme.Codecs.WithoutConversion(),
	}

	tests := []struct {
		name    string
		ns      string
		event   *v1beta1.Event
		wantErr bool
	}{
		{
			name:  "patch event",
			ns:    "default",
			event: event,
		},
		{
			name:    "patch event with different namespace",
			ns:      "other",
			event:   event,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := newEvents(New(cli), tt.ns)
			_, err := e.UpdateWithEventNamespace(tt.event)
			if (err != nil) != tt.wantErr {
				t.Errorf("UpdateWithEventNamespace() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
		})
	}
}
