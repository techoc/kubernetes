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

package v1beta1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// GroupName holds the API group name.
// 示例组名用 example.com 后缀，避免和真实 API 组冲突。
const GroupName = "wardle.example.com"

// SchemeGroupVersion is group version used to register these objects
// 同一组下多版本共存时，注册和序列化都以它为身份标识。
var SchemeGroupVersion = schema.GroupVersion{Group: GroupName, Version: "v1beta1"}

var (
	// SchemeBuilder allows to add this group to a scheme.
	// TODO: move SchemeBuilder with zz_generated.deepcopy.go to k8s.io/api.
	// localSchemeBuilder and AddToScheme will stay in k8s.io/kubernetes.
	// 包变量而非 init 里直接注册，调用方可以在多个 Scheme 上重复安装。
	SchemeBuilder      runtime.SchemeBuilder
	localSchemeBuilder = &SchemeBuilder

	// AddToScheme adds this group to a scheme.
	// 暴露函数值而不是包级函数，保持与生成代码一致的注册入口。
	AddToScheme = localSchemeBuilder.AddToScheme
)

func init() {
	// We only register manually written functions here. The registration of the
	// generated functions takes place in the generated files. The separation
	// makes the code compile even when the generated files are missing.
	// 手写和生成注册分开，缺生成文件时仍能编译，方便排查生成链问题。
	localSchemeBuilder.Register(addKnownTypes)
}

// Adds the list of known types to the given scheme.
// 每新增一个该版本的类型都要加进这里，否则序列化时会报 unregistered type。
func addKnownTypes(scheme *runtime.Scheme) error {
	scheme.AddKnownTypes(SchemeGroupVersion,
		&Flunder{},
		&FlunderList{},
	)
	metav1.AddToGroupVersion(scheme, SchemeGroupVersion)
	return nil
}

// Resource takes an unqualified resource and returns a Group qualified GroupResource
// 让存储层和校验错误都带上完整组名，避免不同组的同名资源混淆。
func Resource(resource string) schema.GroupResource {
	return SchemeGroupVersion.WithResource(resource).GroupResource()
}
