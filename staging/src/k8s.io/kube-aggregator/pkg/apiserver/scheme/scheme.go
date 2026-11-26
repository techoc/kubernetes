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

package scheme

import (
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/serializer"

	"k8s.io/kube-aggregator/pkg/apis/apiregistration/install"
)

// 全局 scheme 让 apiserver、REST 存储和客户端共用同一份类型注册表；
// 序列化、反序列化和版本转换都依赖它，分散注册会导致各处可用的类型不一致。
var (
	// Scheme 定义 API 对象的序列化与反序列化方法。
	Scheme = runtime.NewScheme()
	// Codecs 按指定版本与内容类型提供 codec 与 serializer。
	Codecs = serializer.NewCodecFactory(Scheme)
)

// 放在 init 而不是由每个调用方显式注册，避免某个调用路径漏注册后
// 只在反序列化特定版本时才出错；代价是这个包一旦被导入就完成注册。
func init() {
	install.Install(Scheme)
}
