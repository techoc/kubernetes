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

// TODO: Delete this file if we generate a clientset.

package audit

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	auditinternal "k8s.io/apiserver/pkg/apis/audit"
	"k8s.io/apiserver/pkg/apis/audit/v1"
)

// Scheme 是审计包专用的 runtime.Scheme，注册 audit.k8s.io 的外部版本 v1
// 与内部版本。审计后端用它来编解码 Event/Policy，与 apiserver 主 scheme 隔离，
// 避免审计类型污染核心 scheme。
var Scheme = runtime.NewScheme()

// Codecs 是基于 Scheme 的编解码器工厂，供审计日志与策略文件使用。
var Codecs = serializer.NewCodecFactory(Scheme)

func init() {
	// 注册 metav1 类型，使 Event/Policy 中的 TypeMeta 与 ListMeta 可被正确解码。
	metav1.AddToGroupVersion(Scheme, schema.GroupVersion{Version: "v1"})
	utilruntime.Must(v1.AddToScheme(Scheme))
	utilruntime.Must(auditinternal.AddToScheme(Scheme))
	// 优先使用 v1 作为默认序列化版本。
	utilruntime.Must(Scheme.SetVersionPriority(v1.SchemeGroupVersion))
}
