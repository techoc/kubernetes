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

// 本文件界定 kubelet 可以自行写入 Node 对象的 label 范围：
// 允许列表收紧是为了防止节点上的组件给自己“封官”，篡改调度相关的任意标签。

package apis

import (
	"strings"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/sets"
)

const (
	// LabelOS is a label to indicate the operating system of the node.
	// The OS labels are promoted to GA in 1.14. kubelet applies GA labels and stop applying the beta OS labels in Kubernetes 1.19.
	//
	// beta 名称仍留在允许列表里，是为了兼容升级期间还未清理旧标签的节点。
	LabelOS = "beta.kubernetes.io/os"
	// LabelArch is a label to indicate the architecture of the node.
	// The Arch labels are promoted to GA in 1.14. kubelet applies GA labels and stop applying the beta Arch labels in Kubernetes 1.19.
	//
	// 与 LabelOS 同理，保留 beta key 只是为旧标签留出升级过渡期。
	LabelArch = "beta.kubernetes.io/arch"
)

var kubeletLabels = sets.NewString(
	v1.LabelHostname,
	v1.LabelTopologyZone,
	v1.LabelTopologyRegion,
	v1.LabelFailureDomainBetaZone,
	v1.LabelFailureDomainBetaRegion,
	v1.LabelInstanceType,
	v1.LabelInstanceTypeStable,
	v1.LabelOSStable,
	v1.LabelArchStable,

	LabelOS,
	LabelArch,
)

// 同时列出 beta 与 GA 标签名，覆盖升级窗口里节点上可能残留的两种写法。
var kubeletLabelNamespaces = sets.NewString(
	v1.LabelNamespaceSuffixKubelet,
	v1.LabelNamespaceSuffixNode,
)

// KubeletLabels returns the list of label keys kubelets are allowed to set on their own Node objects
//
// 返回切片副本，调用方改动不会污染包内集合。
func KubeletLabels() []string {
	return kubeletLabels.List()
}

// KubeletLabelNamespaces returns the list of label key namespaces kubelets are allowed to set on their own Node objects
//
// 按 namespace 授权而不是逐个键列举，第三方插件才能用自己的前缀扩展标签。
func KubeletLabelNamespaces() []string {
	return kubeletLabelNamespaces.List()
}

// IsKubeletLabel returns true if the label key is one that kubelets are allowed to set on their own Node object.
// This checks if the key is in the KubeletLabels() list, or has a namespace in the KubeletLabelNamespaces() list.
//
// 后缀匹配（".kubelet.k8s.io" 之类）覆盖子域，便于按组织或插件划分 label 前缀。
func IsKubeletLabel(key string) bool {
	if kubeletLabels.Has(key) {
		return true
	}

	namespace := getLabelNamespace(key)
	for allowedNamespace := range kubeletLabelNamespaces {
		if namespace == allowedNamespace || strings.HasSuffix(namespace, "."+allowedNamespace) {
			return true
		}
	}

	return false
}

// getLabelNamespace 取 "/" 前的部分作为命名空间；无斜杠的 key 不属于任何命名空间。
func getLabelNamespace(key string) string {
	if parts := strings.SplitN(key, "/", 2); len(parts) == 2 {
		return parts[0]
	}
	return ""
}
