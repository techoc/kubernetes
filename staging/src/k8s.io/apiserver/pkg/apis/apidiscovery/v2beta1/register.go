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

package v2beta1

import (
	apidiscoveryv2beta1 "k8s.io/api/apidiscovery/v2beta1"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// GroupName is the group name use in this package
const GroupName = "apidiscovery.k8s.io"

// SchemeGroupVersion 是注册这些对象所使用的组版本。本包只做类型注册，不定义新结构，
// 实际类型来自 k8s.io/api/apidiscovery/v2beta1，以避免 apiserver 与 api 仓库各维护一份定义。
var SchemeGroupVersion = schema.GroupVersion{Group: GroupName, Version: "v2beta1"}

// Resource takes an unqualified resource and returns a Group qualified GroupResource
func Resource(resource string) schema.GroupResource {
	return SchemeGroupVersion.WithResource(resource).GroupResource()
}

var (
	localSchemeBuilder = &apidiscoveryv2beta1.SchemeBuilder
	// AddToScheme adds api to a scheme
	AddToScheme = localSchemeBuilder.AddToScheme
)
