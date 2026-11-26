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

// NewStaticVerifierFromFile 从文件创建校验函数：读取内容后即固化。
// 若传入空的 CA 文件路径，则返回 nil 函数。
//
// 一次性读取 CA 文件并构造证书池，之后不再重读；需要热加载的场景应使用动态证书实现。
func NewStaticVerifierFromFile(clientCA string) (VerifyOptionFunc, error) {
	// 空路径表示未配置客户端 CA，返回 nil 让调用方按“不校验”处理。
	if len(clientCA) == 0 {
		return nil, nil
	}

	// 用 x509 校验器包装。
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
	// Value 返回当前的字符串切片。调用方绝不能修改返回值。
	Value() []string
}

// StringSliceProviderFunc 是匹配 StringSliceProvider 接口的函数类型。
type StringSliceProviderFunc func() []string

// Value 返回当前的字符串切片。调用方绝不能修改返回值。
func (d StringSliceProviderFunc) Value() []string {
	return d()
}

// StaticStringSlice 是返回固定值的 StringSliceProvider。
type StaticStringSlice []string

// Value 返回当前的字符串切片。调用方绝不能修改返回值。
func (s StaticStringSlice) Value() []string {
	return s
}
