/*
Copyright 2024 The Kubernetes Authors.

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
// +genclient:method=GetExtended,verb=get
// +genclient:method=ListExtended,verb=list
// +genclient:method=CreateExtended,verb=create
// +genclient:method=UpdateExtended,verb=update
// +genclient:method=PatchExtended,verb=patch
// +genclient:method=ApplyExtended,verb=apply
// +genclient:method=GetSubresource,verb=get,subresource=testsubresource,result=k8s.io/code-generator/examples/crd/apis/extensions/v1.TestSubresource
// +genclient:method=CreateSubresource,verb=create,subresource=testsubresource,input=k8s.io/code-generator/examples/crd/apis/extensions/v1.TestSubresource,result=k8s.io/code-generator/examples/crd/apis/extensions/v1.TestSubresource
// +genclient:method=UpdateSubresource,verb=update,subresource=subresource,input=k8s.io/code-generator/examples/crd/apis/extensions/v1.TestSubresource,result=k8s.io/code-generator/examples/crd/apis/extensions/v1.TestSubresource
// +genclient:method=ApplySubresource,verb=apply,subresource=subresource,input=k8s.io/code-generator/examples/crd/apis/extensions/v1.TestSubresource,result=k8s.io/code-generator/examples/crd/apis/extensions/v1.TestSubresource
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// TestType 是顶层类型，会为其生成 client。
//
// 上面一组 +genclient:method 让 client-gen 为它生成额外的方法：
// Get/List/Create/Update/Patch/Apply 的扩展形式，以及带子资源的
// Get/Create/Update/Apply 方法，用于覆盖 client-gen 的子资源代码生成。
type TestType struct {
	metav1.TypeMeta `json:""`
	// +optional
	metav1.ObjectMeta `json:"metadata,omitempty"`
	// +optional
	Status TestTypeStatus `json:"status,omitempty"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// TestSubresource 是子资源方法的输入/输出类型。
type TestSubresource struct {
	metav1.TypeMeta `json:""`

	Name string `json:"name"`
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

// TestTypeStatus 是 TestType 的 status 子结构。
type TestTypeStatus struct {
	Blah string `json:"blah"`
}
