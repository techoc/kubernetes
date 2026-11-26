/*
Copyright 2015 The Kubernetes Authors.

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

import metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

// +genclient
// +genclient:method=GetClusterTestType,verb=get
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// TestType 是顶层类型，会为其生成 client。
//
// 生成指令说明：+genclient:method 生成额外的 GetClusterTestType 方法，验证自定义 verb
// 在 namespaced 类型上的展开。
type TestType struct {
	metav1.TypeMeta `json:""`
	// +optional
	metav1.ObjectMeta `json:"metadata,omitempty"`
	// +optional
	Status TestTypeStatus `json:"status,omitempty"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// TestTypeList 是顶层列表类型，其 client 方法自动生成。
// 不应为这个类型单独生成 client。
type TestTypeList struct {
	metav1.TypeMeta `json:""`
	// +optional
	metav1.ListMeta `json:"metadata,omitempty"`

	Items []TestType `json:"items"`
}

// TestTypeStatus 是 TestType 的状态子结构。
type TestTypeStatus struct {
	Blah string `json:"blah"`
}

// +genclient:nonNamespaced
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// ClusterTestTypeList 是 cluster-scoped 资源的列表类型。
type ClusterTestTypeList struct {
	metav1.TypeMeta `json:""`
	// +optional
	metav1.ListMeta `json:"metadata,omitempty"`

	Items []ClusterTestType `json:"items"`
}

// +genclient
// +genclient:nonNamespaced
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +genclient:method=GetScale,verb=get,subresource=scale,result=k8s.io/api/autoscaling/v1.Scale
// +genclient:method=UpdateScale,verb=update,subresource=scale,input=k8s.io/api/autoscaling/v1.Scale,result=k8s.io/api/autoscaling/v1.Scale
//
// ClusterTestType 演示 cluster-scoped 类型上的 scale 子资源方法。
type ClusterTestType struct {
	metav1.TypeMeta `json:""`
	// +optional
	metav1.ObjectMeta `json:"metadata,omitempty"`
	// +optional
	Status ClusterTestTypeStatus `json:"status,omitempty"`
}

// 结构体级注释
//
// 结构体级与字段级注释用于验证文档注释在生成代码中的保留。
type ClusterTestTypeStatus struct {
	// 字段级注释
	Blah string `json:"blah"`
}
