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

package v1

import (
	"io"
	"net/http"
	"strings"
	"testing"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/scheme"
	restfake "k8s.io/client-go/rest/fake"
)

// TestCreateWithEventNamespace 验证事件命名空间校验：事件对象必须与
// Events 客户端绑定的命名空间一致，否则在发请求前就返回错误。
func TestCreateWithEventNamespace(t *testing.T) {
	// 固定的事件对象属于 default 命名空间。
	event := &v1.Event{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test",
			Namespace: "default",
		},
	}
	// fake REST client 对任意请求都返回空 JSON，校验逻辑不依赖响应体。
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
		event   *v1.Event
		wantErr bool
	}{
		{
			// 事件命名空间与客户端一致，请求应正常发出。
			name:  "create event",
			ns:    "default",
			event: event,
		},
		{
			// 命名空间不一致时必须在客户端侧提前失败。
			name:    "create event with different namespace",
			ns:      "other",
			event:   event,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// 用 tt.ns 构造 events 实例，模拟客户端绑定到不同命名空间。
			e := newEvents(New(cli), tt.ns)
			_, err := e.CreateWithEventNamespace(tt.event)
			if (err != nil) != tt.wantErr {
				t.Errorf("CreateWithEventNamespace() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
		})
	}
}

// TestPatchWithEventNamespace 验证 Patch 路径同样拒绝与客户端命名空间不一致的事件。
func TestPatchWithEventNamespace(t *testing.T) {
	event := &v1.Event{
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
		event   *v1.Event
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

// TestUpdateWithEventNamespace 验证 Update 路径的命名空间校验，行为与 Create/Patch 一致。
func TestUpdateWithEventNamespace(t *testing.T) {
	event := &v1.Event{
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
		event   *v1.Event
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
