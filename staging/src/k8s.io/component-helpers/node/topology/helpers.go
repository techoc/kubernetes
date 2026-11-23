/*
Copyright 2020 The Kubernetes Authors.

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

// Package topology 提供节点拓扑相关的辅助函数，例如从节点标签中解析可用区标识。
package topology

import (
	"k8s.io/api/core/v1"
)

// GetZoneKey 根据节点标签构造一个在单个可用区内唯一的字符串标识；
// 节点没有可用区信息时返回空字符串。
//
// 历史上存在两个表示可用区的标签键：
//   - "failure-domain.beta.kubernetes.io/zone"
//   - "topology.kubernetes.io/zone"
//
// GetZoneKey 优先读取旧的 beta 标签，只有它不存在时才回退到新的 GA 标签；region 同理。
func GetZoneKey(node *v1.Node) string {
	// 只读取标签，不修改节点；nil map 的读取是安全的，但要单独处理。
	labels := node.Labels
	if labels == nil {
		return ""
	}

	// TODO: "failure-domain.beta..." 系列标签名虽然已经废弃，但由于存量对象（如 PV）仍在使用，
	// 还会长期存在。也许有一天可以不再考虑它们（见 #88493）。
	// 旧标签优先于新标签：同一节点若两边都有值，以旧标签为准，
	// 这样历史对象（例如已静态绑定的 PV）与节点在升级过程中始终落在同一个 zone key 上。
	zone, ok := labels[v1.LabelFailureDomainBetaZone]
	if !ok {
		zone, _ = labels[v1.LabelTopologyZone]
	}

	region, ok := labels[v1.LabelFailureDomainBetaRegion]
	if !ok {
		region, _ = labels[v1.LabelTopologyRegion]
	}

	if region == "" && zone == "" {
		return ""
	}

	// 用空字符做分隔符，是为了防止 region 或 failureDomain 的取值里出现冒号
	// （这里假定 region 或 failureDomain 中不含空字符）。
	// 附带好处是 fmt.Print 或 glog 不会打印空字符，便于日志阅读。
	// region 为空但 zone 非空时仍保留分隔符，使 (空 region, zone) 与 (region, 空 zone)
	// 不会拼出同一个字符串；这对仅按 zone 分区的集群尤其重要。
	return region + ":\x00:" + zone
}
