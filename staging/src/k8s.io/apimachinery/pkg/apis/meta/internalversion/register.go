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

package internalversion

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	metav1beta1 "k8s.io/apimachinery/pkg/apis/meta/v1beta1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// GroupName 是该 API 的组名。
const GroupName = "meta.k8s.io"

var (
	// TODO: move SchemeBuilder with zz_generated.deepcopy.go to k8s.io/api.
	// localSchemeBuilder and AddToScheme will stay in k8s.io/kubernetes.
	// SchemeBuilder 收集需要注册到 Scheme 的函数；AddToScheme 是对它的便捷封装，
	// 让调用方无需关心内部是否使用局部变量。
	SchemeBuilder      runtime.SchemeBuilder
	localSchemeBuilder = &SchemeBuilder
	AddToScheme        = localSchemeBuilder.AddToScheme
)

// SchemeGroupVersion 是注册这些对象所用的组版本。
// 使用 APIVersionInternal 表明这是仅存在于进程内部的版本，不对外暴露。
var SchemeGroupVersion = schema.GroupVersion{Group: GroupName, Version: runtime.APIVersionInternal}

// Kind 接收不带组限定的 kind，返回带组限定的 GroupKind。
func Kind(kind string) schema.GroupKind {
	return SchemeGroupVersion.WithKind(kind).GroupKind()
}

// addToGroupVersion 把通用 meta 类型注册到 scheme 中。
// 由于 meta 内部版本是"汇聚点"，这里同时注册了 metav1 与 metav1beta1 的类型，
// 使内部版本可以直接与各外部版本互转。
func addToGroupVersion(scheme *runtime.Scheme) error {
	// TypeMeta 只有 TypeMeta -> TypeMeta 这一种转换，显式忽略可避免转换器生成器报错。
	if err := scheme.AddIgnoredConversionType(&metav1.TypeMeta{}, &metav1.TypeMeta{}); err != nil {
		return err
	}
	// ListOptions 是唯一需要转换的 options 结构（它为了方便把 labels/fields 暴露为 selector）；
	// 其他 options 类型目前只有一种表示形式，注册即可。
	// 注意：ListOptions 的转换函数需要手工实现（见 conversion.go），因为它涉及 selector 的拆解。
	scheme.AddKnownTypes(SchemeGroupVersion,
		&ListOptions{},
		&metav1.GetOptions{},
		&metav1.DeleteOptions{},
		&metav1.CreateOptions{},
		&metav1.UpdateOptions{},
	)
	scheme.AddKnownTypes(SchemeGroupVersion,
		&metav1.Table{},
		&metav1.TableOptions{},
		&metav1beta1.PartialObjectMetadata{},
		&metav1beta1.PartialObjectMetadataList{},
	)
	// 先注册 beta1，再注册 v1，使后者在存在同名类型时获得优先权。
	if err := metav1beta1.AddMetaToScheme(scheme); err != nil {
		return err
	}
	if err := metav1.AddMetaToScheme(scheme); err != nil {
		return err
	}
	// 把 options 注册为 unversioned 类型，使其在所有版本下都能被解码
	// （这样客户端无论使用哪个版本发送请求，服务端都能识别这些 options）。
	scheme.AddUnversionedTypes(SchemeGroupVersion,
		&metav1.DeleteOptions{},
		&metav1.CreateOptions{},
		&metav1.UpdateOptions{})

	metav1.AddToGroupVersion(scheme, metav1.SchemeGroupVersion)
	if err := metav1beta1.RegisterConversions(scheme); err != nil {
		return err
	}
	return nil
}

// 与其他 API 组不同，meta 内部版本了解所有 meta 外部版本，但把转换逻辑保持为私有。
// 通过 init 注册而非导出，避免外部代码依赖内部转换细节。
func init() {
	localSchemeBuilder.Register(addToGroupVersion)
}
