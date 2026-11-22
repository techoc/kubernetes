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

// Version is the string that represents the current external default version.
const Version = "v1"

// ExternalVersion 是当前对外版本的 GroupVersion（core 组，无 group 前缀）。
var ExternalVersion = schema.GroupVersion{Group: "", Version: "v1"}

// OldestVersion is the string that represents the oldest server version supported,
// for client code that wants to hardcode the lowest common denominator.
const OldestVersion = "v1"

// Versions is the list of versions that are recognized in code. The order provided
// may be assumed to be least feature rich to most feature rich, and clients may
// choose to prefer the latter items in the list over the former items when presented
// with a set of versions to choose.
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
