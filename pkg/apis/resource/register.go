/*
Copyright 2022 The Kubernetes Authors.

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

package resource

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// GroupName 是本包对应的 API 组名。
const GroupName = "resource.k8s.io"

// SchemeGroupVersion 是注册这些对象时使用的组版本。
var SchemeGroupVersion = schema.GroupVersion{Group: GroupName, Version: runtime.APIVersionInternal}

// Kind 接收不带组名的 kind，返回带组名的 GroupKind。
func Kind(kind string) schema.GroupKind {
	return SchemeGroupVersion.WithKind(kind).GroupKind()
}

// Resource 接收不带组名的 resource，返回带组名的 GroupResource。
func Resource(resource string) schema.GroupResource {
	return SchemeGroupVersion.WithResource(resource).GroupResource()
}

var (
	// SchemeBuilder object to register various known types
	SchemeBuilder = runtime.NewSchemeBuilder(addKnownTypes)

	// AddToScheme 是一个函数，用于把所有已注册的
	// funcs in a scheme
	AddToScheme = SchemeBuilder.AddToScheme
)

func addKnownTypes(scheme *runtime.Scheme) error {
	if err := scheme.AddIgnoredConversionType(&metav1.TypeMeta{}, &metav1.TypeMeta{}); err != nil {
		return err
	}
	scheme.AddKnownTypes(SchemeGroupVersion,
		&DeviceClass{},
		&DeviceClassList{},
		&DeviceTaintRule{},
		&DeviceTaintRuleList{},
		&ResourceClaim{},
		&ResourceClaimList{},
		&ResourceClaimTemplate{},
		&ResourceClaimTemplateList{},
		&ResourceSlice{},
		&ResourceSliceList{},
		&ResourcePoolStatusRequest{},
		&ResourcePoolStatusRequestList{},
	)

	return nil
}
