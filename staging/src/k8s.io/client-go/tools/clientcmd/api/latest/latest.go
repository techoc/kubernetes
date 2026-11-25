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

package latest

import (
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/runtime/serializer/json"
	"k8s.io/apimachinery/pkg/runtime/serializer/versioning"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/client-go/tools/clientcmd/api"
	"k8s.io/client-go/tools/clientcmd/api/v1"
)

// Version 是表示当前默认外部版本的字符串。
const Version = "v1"

// ExternalVersion 是当前对外版本的 GroupVersion（core 组，无 group 前缀）。
var ExternalVersion = schema.GroupVersion{Group: "", Version: "v1"}

// OldestVersion 是所支持的最旧服务端版本的字符串，
// 供希望硬编码最低公共版本的客户端代码使用。
const OldestVersion = "v1"

// Versions 是代码中识别的版本列表。列表顺序可视为从功能最少到功能最全，
// 当需要在多个版本中选择时，客户端可能会偏好列表靠后的版本。
var Versions = []string{"v1"}

var (
	// Codec 用于在内部 kubeconfig 表示与 v1 之间编解码（默认 YAML）。
	Codec runtime.Codec
	// Scheme 注册了内部类型与 v1 类型，供转换与编解码使用。
	Scheme *runtime.Scheme
)

// init 构建全局 Scheme 与 Codec。Codec 配置为“先默认化再转换到 v1”，
// 这样反序列化旧版 kubeconfig 时会自动补齐默认值。
func init() {
	Scheme = runtime.NewScheme()
	utilruntime.Must(api.AddToScheme(Scheme))
	utilruntime.Must(v1.AddToScheme(Scheme))
	yamlSerializer := json.NewSerializerWithOptions(json.DefaultMetaFactory, Scheme, Scheme, json.SerializerOptions{Yaml: true})
	Codec = versioning.NewDefaultingCodecForScheme(
		Scheme,
		yamlSerializer,
		yamlSerializer,
		schema.GroupVersion{Version: Version},
		runtime.InternalGroupVersioner,
	)
}
