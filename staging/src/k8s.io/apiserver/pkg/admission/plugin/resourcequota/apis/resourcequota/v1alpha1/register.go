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

package v1alpha1

import (
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// GroupName is the group name use in this package
const GroupName = "resourcequota.admission.k8s.io"

// SchemeGroupVersion is group version used to register these objects
//
// 该组版本只用于解析 ResourceQuota 准入插件的 Configuration 配置对象，
// 不对外暴露 REST API；修改组名会破坏现有配置文件的反序列化。
var SchemeGroupVersion = schema.GroupVersion{Group: GroupName, Version: "v1alpha1"}

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
	//
	// 手写注册与生成代码分离：生成文件缺失时包仍可编译，只是缺少默认值等生成逻辑。
	localSchemeBuilder.Register(addKnownTypes, addDefaultingFuncs)
}

// addKnownTypes 将本组版本的 Configuration 注册进 scheme，供解码配置时识别类型。
func addKnownTypes(scheme *runtime.Scheme) error {
	scheme.AddKnownTypes(SchemeGroupVersion,
		&Configuration{},
	)
	return nil
}
