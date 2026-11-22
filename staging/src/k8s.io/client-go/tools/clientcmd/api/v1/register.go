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

package v1

import (
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// SchemeGroupVersion is group version used to register these objects
// TODO this should be in the "kubeconfig" group
//
// 中文：kubeconfig 使用核心组（空 group）+ v1，而非独立的 "kubeconfig" 组，属于历史遗留。
var SchemeGroupVersion = schema.GroupVersion{Group: "", Version: "v1"}

var (
	// TODO: move SchemeBuilder with zz_generated.deepcopy.go to k8s.io/api.
	// localSchemeBuilder and AddToScheme will stay in k8s.io/kubernetes.
	// 中文：builder 目前留在 client-go；将来若迁往 k8s.io/api，仅 AddToScheme 保留在此。
	SchemeBuilder      runtime.SchemeBuilder
	localSchemeBuilder = &SchemeBuilder
	// AddToScheme 把 kubeconfig v1 类型与默认值函数注册到目标 scheme。
	AddToScheme = localSchemeBuilder.AddToScheme
)

func init() {
	// We only register manually written functions here. The registration of the
	// generated functions takes place in the generated files. The separation
	// makes the code compile even when the generated files are missing.
	// 中文：手写注册与生成注册分离，缺少生成文件时仍可编译。
	localSchemeBuilder.Register(addKnownTypes, addDefaultingFuncs)
}

// addKnownTypes 把 Config 注册为 v1 的已知类型。
func addKnownTypes(scheme *runtime.Scheme) error {
	scheme.AddKnownTypes(SchemeGroupVersion,
		&Config{},
	)
	return nil
}

// GetObjectKind 实现 runtime.Object；Config 的类型信息直接存放在自身字段中。
func (obj *Config) GetObjectKind() schema.ObjectKind { return obj }

// SetGroupVersionKind 把 GVK 写回 APIVersion 与 Kind 字段。
func (obj *Config) SetGroupVersionKind(gvk schema.GroupVersionKind) {
	obj.APIVersion, obj.Kind = gvk.ToAPIVersionAndKind()
}

// GroupVersionKind 从 APIVersion 与 Kind 字段还原 GVK。
func (obj *Config) GroupVersionKind() schema.GroupVersionKind {
	return schema.FromAPIVersionAndKind(obj.APIVersion, obj.Kind)
}
