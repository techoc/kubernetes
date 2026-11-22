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

package config

import (
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// GroupName 是本包使用的 API 组名。
const GroupName = "kubescheduler.config.k8s.io"

// SchemeGroupVersion 是注册这些对象时使用的组版本。
// 这里用内部版本：外部版本的转换由各自的版本化包负责。
var SchemeGroupVersion = schema.GroupVersion{Group: GroupName, Version: runtime.APIVersionInternal}

var (
	// SchemeBuilder 聚合本 API 包需要在 scheme 上执行的初始化函数。
	SchemeBuilder = runtime.NewSchemeBuilder(addKnownTypes)
	// AddToScheme 是把本 API 组和版本注册到指定 scheme 的全局函数。
	AddToScheme = SchemeBuilder.AddToScheme
)

// addKnownTypes 把已知类型注册到给定的 scheme。
func addKnownTypes(scheme *runtime.Scheme) error {
	scheme.AddKnownTypes(SchemeGroupVersion,
		&KubeSchedulerConfiguration{},
		&DefaultPreemptionArgs{},
		&InterPodAffinityArgs{},
		&NodeResourcesFitArgs{},
		&PodTopologySpreadArgs{},
		&VolumeBindingArgs{},
		&NodeResourcesBalancedAllocationArgs{},
		&NodeAffinityArgs{},
		&DynamicResourcesArgs{},
	)
	return nil
}
