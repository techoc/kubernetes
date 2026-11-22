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

package api

import (
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// SchemeGroupVersion is group version used to register these objects
// TODO this should be in the "kubeconfig" group
// 使用内部版本（APIVersionInternal）注册，表示该类型仅用于内部表示，不直接序列化到磁盘。
var SchemeGroupVersion = schema.GroupVersion{Group: "", Version: runtime.APIVersionInternal}

var (
	// SchemeBuilder 负责把 Config 注册进 scheme。
	SchemeBuilder = runtime.NewSchemeBuilder(addKnownTypes)
	AddToScheme   = SchemeBuilder.AddToScheme
)

// addKnownTypes 注册 kubeconfig 的内部 Config 类型。
func addKnownTypes(scheme *runtime.Scheme) error {
	scheme.AddKnownTypes(SchemeGroupVersion,
		&Config{},
	)
	return nil
}

// GetObjectKind 返回对象自身的 Kind，使 Config 满足 runtime.Object 接口。
func (obj *Config) GetObjectKind() schema.ObjectKind { return obj }
func (obj *Config) SetGroupVersionKind(gvk schema.GroupVersionKind) {
	obj.APIVersion, obj.Kind = gvk.ToAPIVersionAndKind()
}
func (obj *Config) GroupVersionKind() schema.GroupVersionKind {
	return schema.FromAPIVersionAndKind(obj.APIVersion, obj.Kind)
}
