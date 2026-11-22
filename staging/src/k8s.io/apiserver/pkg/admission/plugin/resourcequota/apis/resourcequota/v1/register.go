/*
Copyright 2019 The Kubernetes Authors.

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

// GroupName is the group name use in this package
// 配置 API 的组名；与其余 apiserver 配置组共用 apiserver.config.k8s.io 前缀。
const GroupName = "apiserver.config.k8s.io"

// SchemeGroupVersion is group version used to register these objects
// 该组版本用于在本包内注册类型；版本号与目录名保持一致。
var SchemeGroupVersion = schema.GroupVersion{Group: GroupName, Version: "v1"}

var (
	// TODO: move SchemeBuilder with zz_generated.deepcopy.go to k8s.io/api.
	// localSchemeBuilder and AddToScheme will stay in k8s.io/kubernetes.

	// SchemeBuilder is a pointer used to call AddToScheme
	SchemeBuilder      runtime.SchemeBuilder
	localSchemeBuilder = &SchemeBuilder
	// AddToScheme is used to register the types to API encoding/decoding machinery
	AddToScheme = localSchemeBuilder.AddToScheme
)

func init() {
	// We only register manually written functions here. The registration of the
	// generated functions takes place in the generated files. The separation
	// makes the code compile even when the generated files are missing.
	// 手写注册与生成代码分离：即使生成文件缺失，本包仍可编译。
	localSchemeBuilder.Register(addKnownTypes, addDefaultingFuncs)
}

// addKnownTypes 注册本组版本对外暴露的配置类型。
func addKnownTypes(scheme *runtime.Scheme) error {
	scheme.AddKnownTypeWithName(SchemeGroupVersion.WithKind("ResourceQuotaConfiguration"), &Configuration{})
	return nil
}
