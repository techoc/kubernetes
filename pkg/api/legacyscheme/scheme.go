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

package legacyscheme

import (
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/serializer"
)

var (
	// Scheme 是 runtime.Scheme 的默认实例，Kubernetes API 的所有类型都已注册到它上面。
	// 注意：如果你想复制这个文件来新建一个 API group，请先停下——应该复制 extensions group 的做法。
	// 这个 Scheme 是特殊的，只应出现在 core api group 中，除非你非常清楚自己在做什么。
	// TODO(lavalamp): 从机制上杜绝上述误用。
	Scheme = runtime.NewScheme()

	// Codecs 基于上面的 Scheme 构造，提供编码与解码能力。
	Codecs = serializer.NewCodecFactory(Scheme)

	// ParameterCodec 处理对象与查询参数之间的版本化转换（例如 list/watch 的 labelSelector 等参数）。
	ParameterCodec = runtime.NewParameterCodec(Scheme)
)
