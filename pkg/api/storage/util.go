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

package storage

import (
	"fmt"

	"k8s.io/apimachinery/pkg/util/validation/field"
	nodeapi "k8s.io/kubernetes/pkg/api/node"
	"k8s.io/kubernetes/pkg/apis/storage"
)

// GetWarningsForStorageClass 检查 StorageClass 的 allowedTopologies 是否引用了弃用的节点标签。
func GetWarningsForStorageClass(sc *storage.StorageClass) []string {
	var warnings []string

	if sc != nil && sc.AllowedTopologies != nil {
		// allowedTopologies[i].matchLabelExpressions[j].key 可能使用了弃用的节点标签。
		for i, topo := range sc.AllowedTopologies {
			for j, expression := range topo.MatchLabelExpressions {
				if msg, deprecated := nodeapi.GetNodeLabelDeprecatedMessage(expression.Key); deprecated {
					warnings = append(warnings, fmt.Sprintf("%s: %s", field.NewPath("allowedTopologies").Index(i).Child("matchLabelExpressions").Index(j).Child("key"), msg))
				}
			}
		}
	}

	return warnings
}

// GetWarningsForCSIStorageCapacity 检查 CSIStorageCapacity 的 nodeTopology 节点选择器，
// 复用 node 包的通用检查逻辑。
func GetWarningsForCSIStorageCapacity(csc *storage.CSIStorageCapacity) []string {
	if csc != nil {
		return nodeapi.GetWarningsForNodeSelector(csc.NodeTopology, field.NewPath("nodeTopology"))
	}
	return nil
}
