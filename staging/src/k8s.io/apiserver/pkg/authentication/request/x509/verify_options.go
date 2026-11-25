/*
Copyright 2019 The Kubernetes Authors.

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

package x509

import (
	"crypto/x509"
	"fmt"

	"k8s.io/client-go/util/cert"
)

// StaticVerifierFn 是一个总是返回同一个值的 VerifyOptionFunc，用于构造不会变化的校验选项。
// 第二个返回值为 false 时表示“无可用校验配置”，调用方据此决定是否跳过校验。
func StaticVerifierFn(opts x509.VerifyOptions) VerifyOptionFunc {
	return func() (x509.VerifyOptions, bool) {
		return opts, true
	}
}

// NewStaticVerifierFromFile creates a new verification func from a file.  It reads the content and then fails.
// It will return a nil function if you pass an empty CA file.
//
// 一次性读取 CA 文件并构造证书池，之后不再重读；需要热加载的场景应使用动态证书实现。
func NewStaticVerifierFromFile(clientCA string) (VerifyOptionFunc, error) {
	// 空路径表示未配置客户端 CA，返回 nil 让调用方按“不校验”处理。
	if len(clientCA) == 0 {
		return nil, nil
	}

	// Wrap with an x509 verifier
	var err error
	opts := DefaultVerifyOptions()
	opts.Roots, err = cert.NewPool(clientCA)
	if err != nil {
		return nil, fmt.Errorf("error loading certs from  %s: %v", clientCA, err)
	}

	return StaticVerifierFn(opts), nil
}

// StringSliceProvider 是获取字符串切片值的抽象，认证头等场景大量使用它。
// 抽象成接口是为了支持动态值（如轮转的请求头/证书），同时保持调用方无锁读取。
type StringSliceProvider interface {
	// Value returns the current string slice.  Callers should never mutate the returned value.
	Value() []string
}

// StringSliceProviderFunc is a function that matches the StringSliceProvider interface
type StringSliceProviderFunc func() []string

// Value returns the current string slice.  Callers should never mutate the returned value.
func (d StringSliceProviderFunc) Value() []string {
	return d()
}

// StaticStringSlice a StringSliceProvider that returns a fixed value
type StaticStringSlice []string

// Value returns the current string slice.  Callers should never mutate the returned value.
func (s StaticStringSlice) Value() []string {
	return s
}
