/*
Copyright 2023 The Kubernetes Authors.

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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// +genclient
// +genclient:nonNamespaced
// +k8s:prerelease-lifecycle-gen:introduced=1.26
// +k8s:prerelease-lifecycle-gen:deprecated=1.34
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// ClusterTrustBundle 是集群作用域的容器，用于存放 X.509 信任锚（根证书）。
//
// 集群中任何已认证用户都被视为可以读取 ClusterTrustBundle 对象，因为它们可以通过
// `clusterTrustBundle` 投影挂载到 Pod 中。所有服务账户默认具有对 ClusterTrustBundle 的读权限。
// 只有命名空间级别权限的用户可以通过模拟其有权访问的服务账户来读取 ClusterTrustBundle。
//
// 它可以选择与特定签名者关联，此时包含该签名者的一组有效信任锚。一个签名者可以关联多个
// ClusterTrustBundle，每个都是该签名者的独立信任锚集合。准入控制用于确保只有对签名者拥有
// 权限的用户才能创建或修改相应的 bundle。
type ClusterTrustBundle struct {
	metav1.TypeMeta `json:""`

	// metadata 包含对象元数据。
	// +optional
	metav1.ObjectMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// spec 包含签名者（如有）和信任锚。
	Spec ClusterTrustBundleSpec `json:"spec" protobuf:"bytes,2,opt,name=spec"`
}

// ClusterTrustBundleSpec 包含签名者和信任锚。
type ClusterTrustBundleSpec struct {
	// signerName 表示关联的签名者（如有）。
	//
	// 要创建或更新设置了 signerName 的 ClusterTrustBundle，必须具有如下集群作用域权限：
	// group=certificates.k8s.io resource=signers resourceName=<签名者名称>
	// verb=attest。
	//
	// 若 signerName 非空，ClusterTrustBundle 对象的名称必须以签名者名称为前缀（将斜杠转为冒号）。
	// 例如签名者名 `example.com/foo`，有效的对象名包括 `example.com:foo:abc` 和
	// `example.com:foo:v1`。
	//
	// 若 signerName 为空，则 ClusterTrustBundle 对象的名称不得带有该前缀。
	//
	// 对 ClusterTrustBundle 的 list/watch 请求可以使用 `spec.signerName=NAME` 字段选择器
	// 对此字段进行过滤。
	//
	// +optional
	// +k8s:alpha(since:"1.37")=+k8s:optional
	// +k8s:alpha(since:"1.37")=+k8s:immutable
	SignerName string `json:"signerName,omitempty" protobuf:"bytes,1,opt,name=signerName"`

	// trustBundle 包含此 bundle 中各个 X.509 信任锚，形式为 PEM 封装的 DER 格式 X.509
	// 证书组成的 PEM bundle。
	//
	// 数据只能由可解析为有效 X.509 证书的 PEM 证书块组成。每个证书必须包含设置了 CA 位的
	// basic constraints 扩展。API 服务器会拒绝包含重复证书或使用 PEM 块头的对象。
	//
	// ClusterTrustBundle 的使用方（包括 Kubelet）可以按自己的逻辑对此文件中的证书块重新排序、
	// 去重，也可以丢弃 PEM 块头和块间数据。
	TrustBundle string `json:"trustBundle" protobuf:"bytes,2,opt,name=trustBundle"`
}

// +k8s:prerelease-lifecycle-gen:introduced=1.26
// +k8s:prerelease-lifecycle-gen:deprecated=1.34
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// ClusterTrustBundleList 是 ClusterTrustBundle 对象的集合。
type ClusterTrustBundleList struct {
	metav1.TypeMeta `json:""`

	// metadata 包含列表元数据。
	//
	// +optional
	metav1.ListMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// items 是 ClusterTrustBundle 对象的集合。
	Items []ClusterTrustBundle `json:"items" protobuf:"bytes,2,rep,name=items"`
}
