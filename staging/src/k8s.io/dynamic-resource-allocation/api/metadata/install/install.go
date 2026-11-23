/*
Copyright The Kubernetes Authors.

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

// Package install registers the metadata internal and versioned types with a
// runtime.Scheme. Consumers should call NewScheme() to get a scheme that can
// decode any supported metadata version into the internal types.
//
// install 包负责把元数据的内部类型和各个版本化类型注册到 runtime.Scheme 中。
// 消费者应当调用 NewScheme() 获取一个 scheme，它能把任何受支持的元数据版本解码为内部类型。
package install

import (
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/dynamic-resource-allocation/api/metadata"
	"k8s.io/dynamic-resource-allocation/api/metadata/v1alpha1"
	"k8s.io/dynamic-resource-allocation/api/metadata/v1beta1"
)

// Install registers the internal, v1alpha1, and v1beta1 metadata types with the given scheme.
//
// Install 把内部版本、v1alpha1 和 v1beta1 的元数据类型注册到给定的 scheme 中。
// 每次 AddToScheme 调用都用 utilruntime.Must 包裹，注册失败时直接 panic（注册本不应失败）。
func Install(scheme *runtime.Scheme) {
	// 依次注册内部类型、v1alpha1 版本类型、v1beta1 版本类型，覆盖全部受支持的版本。
	utilruntime.Must(metadata.AddToScheme(scheme))
	utilruntime.Must(v1alpha1.AddToScheme(scheme))
	utilruntime.Must(v1beta1.AddToScheme(scheme))
}

// NewScheme returns a new runtime.Scheme with all metadata versions registered.
// The returned scheme can decode any supported metadata version into the
// internal metadata.DeviceMetadata type.
//
// NewScheme 返回一个注册了所有元数据版本的全新 runtime.Scheme。
// 返回的 scheme 可以把任何受支持的元数据版本解码为内部的 metadata.DeviceMetadata 类型。
func NewScheme() *runtime.Scheme {
	// 先创建空 scheme，再通过 Install 注册全部版本后返回。
	scheme := runtime.NewScheme()
	Install(scheme)
	return scheme
}
