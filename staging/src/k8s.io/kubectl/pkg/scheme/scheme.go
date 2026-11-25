/*
Copyright 2017 The Kubernetes Authors.

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

package scheme

import (
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/serializer"
)

// 所有 kubectl 代码最终都应该改用这个 Registry 和 Scheme，而不是全局的那些。

// Scheme 是 runtime.Scheme 的默认实例，Kubernetes API 中的类型已经注册到其中。
var Scheme = runtime.NewScheme()

// Codecs 提供对该 scheme 进行编码和解码的访问入口
var Codecs = serializer.NewCodecFactory(Scheme)

// ParameterCodec 处理被转换为查询参数的对象的版本控制。
var ParameterCodec = runtime.NewParameterCodec(Scheme)

// DefaultJSONEncoder 返回我们这个 scheme 的默认编码器
func DefaultJSONEncoder() runtime.Encoder {
	return unstructured.NewJSONFallbackEncoder(Codecs.LegacyCodec(Scheme.PrioritizedVersionsAllGroups()...))
}
