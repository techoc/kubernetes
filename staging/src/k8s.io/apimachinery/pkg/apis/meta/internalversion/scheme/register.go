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
	"k8s.io/apimachinery/pkg/apis/meta/internalversion"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
)

// Scheme 是遵循 meta API 规范的类型的注册表。
var Scheme = runtime.NewScheme()

// Codecs 提供基于 Scheme 的编码与解码能力。
var Codecs = serializer.NewCodecFactory(Scheme)

// ParameterCodec 负责在对象与查询参数之间做版本化转换。
var ParameterCodec = runtime.NewParameterCodec(Scheme)

// 与其他 API 组不同，meta internal 知晓所有 meta 外部版本，但将转换逻辑保持私有。
// 因此在这里完成注册，使调用方无需感知 internal 与各外部版本之间的映射细节。
func init() {
	utilruntime.Must(internalversion.AddToScheme(Scheme))
}
