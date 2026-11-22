/*
Copyright 2018 The Kubernetes Authors.

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

// Package testing 提供 webhook 准入测试所需的假服务端、认证信息解析器与辅助工具。
package testing

import (
	"sync/atomic"

	"k8s.io/apiserver/pkg/admission/plugin/webhook/testcerts"
	"k8s.io/apiserver/pkg/util/webhook"
	"k8s.io/client-go/rest"
)

// Wrapper turns an AuthenticationInfoResolver into a AuthenticationInfoResolverWrapper that unconditionally
// returns the given AuthenticationInfoResolver.
//
// 用于在测试里跳过真实的 service/URL 解析，直接注入预置的 TLS 配置。
func Wrapper(r webhook.AuthenticationInfoResolver) func(webhook.AuthenticationInfoResolver) webhook.AuthenticationInfoResolver {
	return func(webhook.AuthenticationInfoResolver) webhook.AuthenticationInfoResolver {
		return r
	}
}

// NewAuthenticationInfoResolver creates a fake AuthenticationInfoResolver that counts cache misses on
// every call to its methods.
//
// cacheMisses 由调用方传入并在每次解析时原子递增，用于断言缓存命中情况；
// 返回的 rest.Config 固定使用 testcerts 中的 CA 与客户端证书。
func NewAuthenticationInfoResolver(cacheMisses *int32) webhook.AuthenticationInfoResolver {
	return &authenticationInfoResolver{
		restConfig: &rest.Config{
			TLSClientConfig: rest.TLSClientConfig{
				CAData:   testcerts.CACert,
				CertData: testcerts.ClientCert,
				KeyData:  testcerts.ClientKey,
			},
		},
		cacheMisses: cacheMisses,
	}
}

// authenticationInfoResolver 是计数用的假实现，两个入口都返回同一份配置。
type authenticationInfoResolver struct {
	restConfig  *rest.Config
	cacheMisses *int32
}

// ClientConfigFor 按主机名解析配置；真实实现会做 DNS/证书发现，这里只计数。
func (a *authenticationInfoResolver) ClientConfigFor(hostPort string) (*rest.Config, error) {
	atomic.AddInt32(a.cacheMisses, 1)
	return a.restConfig, nil
}

// ClientConfigForService 按 Service 解析配置，同样只计数并返回预置配置。
func (a *authenticationInfoResolver) ClientConfigForService(serviceName, serviceNamespace string, servicePort int) (*rest.Config, error) {
	atomic.AddInt32(a.cacheMisses, 1)
	return a.restConfig, nil
}

// NewPanickingAuthenticationInfoResolver creates a fake AuthenticationInfoResolver that panics
//
// 用于验证调用方在解析器 panic 时的恢复行为（应转为请求错误而不是进程崩溃）。
func NewPanickingAuthenticationInfoResolver(panicMessage string) webhook.AuthenticationInfoResolver {
	return &panickingAuthenticationInfoResolver{
		panicMessage: panicMessage,
	}
}

// panickingAuthenticationInfoResolver 在被调用时总是 panic。
type panickingAuthenticationInfoResolver struct {
	panicMessage string
}

// ClientConfigFor 触发 panic，模拟解析器内部缺陷。
func (a *panickingAuthenticationInfoResolver) ClientConfigFor(hostPort string) (*rest.Config, error) {
	panic(a.panicMessage)
}

// ClientConfigForService 触发 panic，模拟解析器内部缺陷。
func (a *panickingAuthenticationInfoResolver) ClientConfigForService(serviceName, serviceNamespace string, servicePort int) (*rest.Config, error) {
	panic(a.panicMessage)
}
