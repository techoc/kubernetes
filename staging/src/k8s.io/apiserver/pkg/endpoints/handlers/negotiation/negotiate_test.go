/*
Copyright 2015 The Kubernetes Authors.

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

package negotiation

import (
	"mime"
	"net/http"
	"net/url"
	"strings"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

// statusError 是可以转换为 metav1.Status 的对象
type statusError interface {
	Status() metav1.Status
}

// fakeNegotiater 是 Negotiate 所需接口的最小实现，只回放预设的媒体类型列表。
type fakeNegotiater struct {
	serializer, streamSerializer runtime.Serializer
	framer                       runtime.Framer
	types, streamTypes           []string
}

// SupportedMediaTypes 由 types/streamTypes 构造 SerializerInfo；
// 同一媒体类型若出现在 streamTypes 中则附带 StreamSerializer。
func (n *fakeNegotiater) SupportedMediaTypes() []runtime.SerializerInfo {
	var out []runtime.SerializerInfo
	for _, s := range n.types {
		mediaType, _, err := mime.ParseMediaType(s)
		if err != nil {
			panic(err)
		}
		parts := strings.SplitN(mediaType, "/", 2)
		if len(parts) == 1 {
			// 这是服务端错误
			parts = append(parts, "")
		}

		info := runtime.SerializerInfo{
			Serializer:       n.serializer,
			MediaType:        s,
			MediaTypeType:    parts[0],
			MediaTypeSubType: parts[1],
			EncodesAsText:    true,
		}
		for _, t := range n.streamTypes {
			if t == s {
				info.StreamSerializer = &runtime.StreamSerializerInfo{
					EncodesAsText: true,
					Framer:        n.framer,
					Serializer:    n.streamSerializer,
				}
			}
		}
		out = append(out, info)
	}
	return out
}

func (n *fakeNegotiater) EncoderForVersion(serializer runtime.Encoder, gv runtime.GroupVersioner) runtime.Encoder {
	return n.serializer
}

func (n *fakeNegotiater) DecoderToVersion(serializer runtime.Decoder, gv runtime.GroupVersioner) runtime.Decoder {
	return n.serializer
}

var fakeCodec = runtime.NewCodec(runtime.NoopEncoder{}, runtime.NoopDecoder{})

// TestNegotiate 覆盖内容协商的主要分支：Accept 通配符、具体类型、pretty 参数
// 的三种来源（Accept 参数、查询参数、User-Agent），以及无可用媒体类型时
// 必须返回 406 statusError。
func TestNegotiate(t *testing.T) {
	testCases := []struct {
		accept      string
		req         *http.Request
		ns          *fakeNegotiater
		serializer  runtime.Serializer
		contentType string
		params      map[string]string
		errFn       func(error) bool
	}{
		// 未指定 Accept 时选默认类型
		{
			req:         &http.Request{},
			contentType: "application/json",
			ns:          &fakeNegotiater{serializer: fakeCodec, types: []string{"application/json"}},
			serializer:  fakeCodec,
		},
		{
			accept:      "",
			contentType: "application/json",
			ns:          &fakeNegotiater{serializer: fakeCodec, types: []string{"application/json"}},
			serializer:  fakeCodec,
		},
		{
			accept:      "*/*",
			contentType: "application/json",
			ns:          &fakeNegotiater{serializer: fakeCodec, types: []string{"application/json"}},
			serializer:  fakeCodec,
		},
		{
			accept:      "application/*",
			contentType: "application/json",
			ns:          &fakeNegotiater{serializer: fakeCodec, types: []string{"application/json"}},
			serializer:  fakeCodec,
		},
		{
			accept:      "application/json",
			contentType: "application/json",
			ns:          &fakeNegotiater{serializer: fakeCodec, types: []string{"application/json"}},
			serializer:  fakeCodec,
		},
		{
			accept:      "application/json",
			contentType: "application/json",
			ns:          &fakeNegotiater{serializer: fakeCodec, types: []string{"application/json", "application/protobuf"}},
			serializer:  fakeCodec,
		},
		{
			accept:      "application/protobuf",
			contentType: "application/protobuf",
			ns:          &fakeNegotiater{serializer: fakeCodec, types: []string{"application/json", "application/protobuf"}},
			serializer:  fakeCodec,
		},
		{
			accept:      "application/json; pretty=1",
			contentType: "application/json",
			ns:          &fakeNegotiater{serializer: fakeCodec, types: []string{"application/json"}},
			serializer:  fakeCodec,
			params:      map[string]string{"pretty": "1"},
		},
		{
			accept:      "unrecognized/stuff,application/json; pretty=1",
			contentType: "application/json",
			ns:          &fakeNegotiater{serializer: fakeCodec, types: []string{"application/json"}},
			serializer:  fakeCodec,
			params:      map[string]string{"pretty": "1"},
		},

		// 查询参数触发 pretty
		{
			req: &http.Request{
				Header: http.Header{"Accept": []string{"application/json"}},
				URL:    &url.URL{RawQuery: "pretty=1"},
			},
			contentType: "application/json",
			ns:          &fakeNegotiater{serializer: fakeCodec, types: []string{"application/json"}},
			serializer:  fakeCodec,
			params:      map[string]string{"pretty": "1"},
		},

		// 特定 user agent 触发 pretty
		{
			req: &http.Request{
				Header: http.Header{
					"Accept":     []string{"application/json"},
					"User-Agent": []string{"curl"},
				},
			},
			contentType: "application/json",
			ns:          &fakeNegotiater{serializer: fakeCodec, types: []string{"application/json"}},
			serializer:  fakeCodec,
			params:      map[string]string{"pretty": "1"},
		},
		{
			req: &http.Request{
				Header: http.Header{
					"Accept":     []string{"application/json"},
					"User-Agent": []string{"Wget"},
				},
			},
			contentType: "application/json",
			ns:          &fakeNegotiater{serializer: fakeCodec, types: []string{"application/json"}},
			serializer:  fakeCodec,
			params:      map[string]string{"pretty": "1"},
		},
		{
			req: &http.Request{
				Header: http.Header{
					"Accept":     []string{"application/json"},
					"User-Agent": []string{"Mozilla/5.0"},
				},
			},
			contentType: "application/json",
			ns:          &fakeNegotiater{serializer: fakeCodec, types: []string{"application/json"}},
			serializer:  fakeCodec,
			params:      map[string]string{"pretty": "1"},
		},
		{
			req: &http.Request{
				Header: http.Header{
					"Accept": []string{"application/json;as=BOGUS;v=v1beta1;g=meta.k8s.io, application/json"},
				},
			},
			contentType: "application/json",
			ns:          &fakeNegotiater{serializer: fakeCodec, types: []string{"application/json"}},
			serializer:  fakeCodec,
		},
		{
			req: &http.Request{
				Header: http.Header{
					"Accept": []string{"application/BOGUS, application/json"},
				},
			},
			contentType: "application/json",
			ns:          &fakeNegotiater{serializer: fakeCodec, types: []string{"application/json"}},
			serializer:  fakeCodec,
		},
		// "application" 不是合法的媒体类型，server 会在协商阶段拒绝响应
		// （server 错误地指定了非法媒体类型）
		{
			accept: "application",
			ns:     &fakeNegotiater{serializer: fakeCodec, types: []string{"application"}},
			errFn: func(err error) bool {
				return err.Error() == "only the following media types are accepted: application"
			},
		},
		{
			ns: &fakeNegotiater{},
			errFn: func(err error) bool {
				return err.Error() == "only the following media types are accepted: "
			},
		},
		{
			accept: "*/*",
			ns:     &fakeNegotiater{},
			errFn: func(err error) bool {
				return err.Error() == "only the following media types are accepted: "
			},
		},
	}

	// 逐个用例执行；用下标而非名字定位失败，便于与表项对应。
	for i, test := range testCases {
		req := test.req
		if req == nil {
			req = &http.Request{Header: http.Header{}}
			req.Header.Set("Accept", test.accept)
		}
		_, s, err := NegotiateOutputMediaType(req, test.ns, DefaultEndpointRestrictions)
		switch {
		case err == nil && test.errFn != nil:
			t.Errorf("%d: failed: expected error", i)
			continue
		case err != nil && test.errFn == nil:
			t.Errorf("%d: failed: %v", i, err)
			continue
		case err != nil:
			if !test.errFn(err) {
				t.Errorf("%d: failed: %v", i, err)
			}
			status, ok := err.(statusError)
			if !ok {
				t.Errorf("%d: failed, error should be statusError: %v", i, err)
				continue
			}
			if status.Status().Status != metav1.StatusFailure || status.Status().Code != http.StatusNotAcceptable {
				t.Errorf("%d: failed: %v", i, err)
				continue
			}
			continue
		}
		if test.contentType != s.MediaType {
			t.Errorf("%d: unexpected %s %s", i, test.contentType, s.MediaType)
		}
		if s.Serializer != test.serializer {
			t.Errorf("%d: unexpected %s %s", i, test.serializer, s.Serializer)
		}
	}
}

// fakeSerializerInfoSlice 返回 JSON 与 protobuf 两种媒体类型，用于基准测试。
func fakeSerializerInfoSlice() []runtime.SerializerInfo {
	result := make([]runtime.SerializerInfo, 2)
	result[0] = runtime.SerializerInfo{
		MediaType:        "application/json",
		MediaTypeType:    "application",
		MediaTypeSubType: "json",
	}
	result[1] = runtime.SerializerInfo{
		MediaType:        "application/vnd.kubernetes.protobuf",
		MediaTypeType:    "application",
		MediaTypeSubType: "vnd.kubernetes.protobuf",
	}
	return result
}

// BenchmarkNegotiateMediaTypeOptions 衡量一次协商的分配与耗时；
// 该函数位于每个请求的热路径上，因此有基准测试守护。
func BenchmarkNegotiateMediaTypeOptions(b *testing.B) {
	accepted := fakeSerializerInfoSlice()
	header := "application/vnd.kubernetes.protobuf,*/*"

	for i := 0; i < b.N; i++ {
		options, _ := NegotiateMediaTypeOptions(header, accepted, DefaultEndpointRestrictions)
		if options.Accepted != accepted[1] {
			b.Errorf("Unexpected result")
		}
	}
}
