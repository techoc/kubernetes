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

package rest

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apiserver/pkg/registry/rest"
)

// CounterMetric 是流式转发中用于计数的极小接口；只暴露 Inc，便于注入 prometheus 计数器或测试替身。
type CounterMetric interface {
	Inc()
}

// LocationStreamer 是从指定 location URL 流式传输内容
// 的资源。
//
// 用于代理型接口（如日志、exec、attach）：apiserver 不产生内容，只把后端 URL 的
// 响应透传给客户端。Transport 与 RedirectChecker 可注入，是因为后端可能是
// 不可信的 kubelet，需要限制重定向并做 TLS 校验。
type LocationStreamer struct {
	Location        *url.URL
	Transport       http.RoundTripper
	ContentType     string
	Flush           bool
	ResponseChecker HttpResponseChecker
	RedirectChecker func(req *http.Request, via []*http.Request) error
	// TLSVerificationErrorCounter 是可选计数器，每次遇到 TLS 错误时递增。
	// 可把同一个 prometheus counter 实例接入此处以统计总量。
	TLSVerificationErrorCounter CounterMetric
}

// LocationStreamer 必须实现 rest.ResourceStreamer
var _ rest.ResourceStreamer = &LocationStreamer{}

// GetObjectKind 返回空 kind：该对象从不参与序列化，仅作为流式接口的载体。
func (obj *LocationStreamer) GetObjectKind() schema.ObjectKind {
	return schema.EmptyObjectKind
}

// DeepCopyObject 显式 panic：流式对象不可复制，若被误当作普通 API 对象处理应立即暴露。
func (obj *LocationStreamer) DeepCopyObject() runtime.Object {
	panic("rest.LocationStreamer does not implement DeepCopyObject")
}

// InputStream 返回包含 URL 位置内容的流。若未提供 location，
// 则返回空流。
func (s *LocationStreamer) InputStream(ctx context.Context, apiVersion, acceptHeader string) (stream io.ReadCloser, flush bool, contentType string, err error) {
	if s.Location == nil {
		// 未提供 location 时，返回空流
		return nil, false, "", nil
	}
	transport := s.Transport
	if transport == nil {
		transport = http.DefaultTransport
	}

	client := &http.Client{
		Transport:     transport,
		CheckRedirect: s.RedirectChecker,
	}
	req, err := http.NewRequest("GET", s.Location.String(), nil)
	if err != nil {
		return nil, false, "", fmt.Errorf("failed to construct request for %s, got %v", s.Location.String(), err)
	}
	// 把父 context 传递给请求，确保资源
	// 被正确释放。
	// 客户端断开时父上下文被取消，后端连接随之关闭，避免 goroutine 与连接泄漏。
	req = req.WithContext(ctx)

	resp, err := client.Do(req)
	if err != nil {
		// TODO 更可靠地区分 TLS 错误，但这里确实需要递增计数
		// 用字符串匹配识别 TLS 错误是权宜之计：便于快速统计证书问题，但依赖错误文案。
		if strings.Contains(err.Error(), "x509:") && s.TLSVerificationErrorCounter != nil {
			s.TLSVerificationErrorCounter.Inc()
		}
		return nil, false, "", err
	}

	if s.ResponseChecker != nil {
		if err = s.ResponseChecker.Check(resp); err != nil {
			return nil, false, "", err
		}
	}

	contentType = s.ContentType
	if len(contentType) == 0 {
		// 未显式指定时透传后端 Content-Type，并去掉 charset 等参数，只保留 MIME 类型。
		contentType = resp.Header.Get("Content-Type")
		if len(contentType) > 0 {
			contentType = strings.TrimSpace(strings.SplitN(contentType, ";", 2)[0])
		}
	}
	flush = s.Flush
	stream = resp.Body
	return
}

// PreventRedirects 是阻止客户端跟随重定向的检查器。
//
// 默认拒绝重定向：代理场景下跟随重定向可能把请求带到非预期主机，形成 SSRF 面。
func PreventRedirects(_ *http.Request, _ []*http.Request) error {
	return errors.New("redirects forbidden")
}
