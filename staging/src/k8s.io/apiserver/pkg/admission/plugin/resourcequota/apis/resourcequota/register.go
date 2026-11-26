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

package resourcequota

import (
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

var (
	// 用于调用 AddToScheme 的 SchemeBuilder 指针
	SchemeBuilder = runtime.NewSchemeBuilder(addKnownTypes)
	// 将本包类型注册到 API 编解码体系
	AddToScheme = SchemeBuilder.AddToScheme
)

// LegacyGroupName 是本包保留的旧组名
//
// 旧的 resourcequota.admission.k8s.io 组名必须继续注册：已存在的配置对象可能仍以该组名
// 序列化，删除会导致升级后配置无法解码。
const LegacyGroupName = "resourcequota.admission.k8s.io"

// LegacySchemeGroupVersion 是注册这些对象时使用的旧组版本
var LegacySchemeGroupVersion = schema.GroupVersion{Group: LegacyGroupName, Version: runtime.APIVersionInternal}

// GroupName 是本包使用的组名
const GroupName = "apiserver.config.k8s.io"

// SchemeGroupVersion 是注册这些对象时使用的组版本
var SchemeGroupVersion = schema.GroupVersion{Group: GroupName, Version: runtime.APIVersionInternal}

// addKnownTypes 同时注册新旧两个组名指向同一个内部 Configuration 类型，
// 使旧配置文件在迁移期仍能被识别。
func addKnownTypes(scheme *runtime.Scheme) error {
	scheme.AddKnownTypes(LegacySchemeGroupVersion,
		&Configuration{},
	)
	// 新组名下的 kind 名为 ResourceQuotaConfiguration，避免与其他 apiserver 配置重名。
	scheme.AddKnownTypeWithName(SchemeGroupVersion.WithKind("ResourceQuotaConfiguration"),
		&Configuration{},
	)
	return nil
}
