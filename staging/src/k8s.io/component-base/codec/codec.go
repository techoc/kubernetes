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

// Package codec 提供按需构造宽松编解码器（禁用 strict decoding）的辅助函数，
// 供组件在特定 API 版本上跳过未知字段校验。
package codec

import (
	"fmt"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/serializer"
)

// NewLenientSchemeAndCodecs 构造一个禁用 strict decoding 的 CodecFactory，
// 其中只注册通过 AddToScheme 函数传入的类型。
// 典型用途是仅对某个特定 API 版本跳过 strict decoding，而不影响其他版本。
func NewLenientSchemeAndCodecs(addToSchemeFns ...func(s *runtime.Scheme) error) (*runtime.Scheme, *serializer.CodecFactory, error) {
	// 使用独立 scheme，避免污染进程内共享的全局 scheme。
	lenientScheme := runtime.NewScheme()
	for _, s := range addToSchemeFns {
		if err := s(lenientScheme); err != nil {
			return nil, nil, fmt.Errorf("unable to add API to lenient scheme: %v", err)
		}
	}
	lenientCodecs := serializer.NewCodecFactory(lenientScheme, serializer.DisableStrict)
	return lenientScheme, &lenientCodecs, nil
}
