/*
Copyright 2021 The Kubernetes Authors.

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

package v1beta1

import (
	corev1 "k8s.io/api/core/v1"
	"k8s.io/api/discovery/v1beta1"
	"k8s.io/apimachinery/pkg/conversion"
	"k8s.io/kubernetes/pkg/apis/discovery"
)

// Convert_v1beta1_Endpoint_To_discovery_Endpoint 把外部版本 v1beta1 的 Endpoint 转换为内部版本 discovery.Endpoint。
// 自动转换完成后，主要处理 topology 相关字段的语义差异：v1beta1 用统一的 topology map 表达拓扑信息，
// 而内部版本把 zone 单独提升为 Zone 字段，并用 DeprecatedTopology 保存其余拓扑键。
// 关键场景：
//   - topology 中含 well-known 的 zone 标签时，把它从 map 移到 Zone 字段，避免 v1 中同一信息出现两份。
//   - topology[hostname] 与 NodeName 值相同时删除 hostname，保留两者取值不同时并存的兼容行为
//     （v1beta1 允许这种略显奇怪的组合，简单删除会丢失信息）。
//   - zone/hostname 被移走后若 DeprecatedTopology 为空，则置为 nil，避免留下多余的空 map。
func Convert_v1beta1_Endpoint_To_discovery_Endpoint(in *v1beta1.Endpoint, out *discovery.Endpoint, s conversion.Scope) error {
	if err := autoConvert_v1beta1_Endpoint_To_discovery_Endpoint(in, out, s); err != nil {
		return err
	}
	if in.Topology != nil {
		// 先把完整的 topology 拷到 DeprecatedTopology，后续再按需删除已提升的字段，
		// 这样可保留 v1beta1 中无法用新字段表达的其它拓扑键。
		out.DeprecatedTopology = make(map[string]string, len(in.Topology))
		for k, v := range in.Topology {
			out.DeprecatedTopology[k] = v
		}

		// 把 zone 从 topology map 提升到独立的 Zone 字段，
		// 同时从 DeprecatedTopology 中删除，防止新版本中同一信息出现两份。
		if zone, ok := in.Topology[corev1.LabelTopologyZone]; ok {
			out.Zone = &zone
			delete(out.DeprecatedTopology, corev1.LabelTopologyZone)
		}

		// 只有当 topology[hostname] 与 nodeName 值相同时才删除 hostname：
		// v1beta1 允许两者取不同的值，无差别删除会丢失信息；
		// 只在相等时删除，则既避免了 v1 中出现重复值，又保留了这种兼容行为。
		if node, ok := in.Topology[corev1.LabelHostname]; ok {
			if out.NodeName != nil && node == *out.NodeName {
				delete(out.DeprecatedTopology, corev1.LabelHostname)
			}
		}

		// 如果 zone 和 hostname 是 map 中仅有的字段（或原本 topology 为空），
		// 删除后 DeprecatedTopology 已无内容，置为 nil 以免留下无意义的空 map。
		if len(out.DeprecatedTopology) == 0 {
			out.DeprecatedTopology = nil
		}
	}

	return nil
}

// Convert_discovery_Endpoint_To_v1beta1_Endpoint 把内部版本 discovery.Endpoint 转换回外部版本 v1beta1.Endpoint。
// 与正向转换对称：把内部版本的 Zone 字段和 NodeName 回填到统一的 topology map。
// 关键场景：
//   - 没有 DeprecatedTopology、Zone 和 NodeName 时无需任何处理，直接返回，避免构造出空 map。
//   - Zone 非空时写入 topology 的 well-known zone 标签。
//   - 仅当 topology 中尚无 hostname 时才用 NodeName 补上，保留 v1beta1 中
//     topology[hostname] 与 nodename 可以取值不同的兼容行为，同时不覆盖显式设置的 hostname。
func Convert_discovery_Endpoint_To_v1beta1_Endpoint(in *discovery.Endpoint, out *v1beta1.Endpoint, s conversion.Scope) error {
	if err := autoConvert_discovery_Endpoint_To_v1beta1_Endpoint(in, out, s); err != nil {
		return err
	}

	// 三个来源都没有数据时提前返回；否则即使没有拓扑信息也会凭空构造出空 map，
	// 改变对象的序列化结果和语义。
	if in.DeprecatedTopology == nil && in.Zone == nil && in.NodeName == nil {
		return nil
	}

	// 先把 DeprecatedTopology 中的旧拓扑键拷回 Topology，再叠加字段形式的 zone/hostname。
	out.Topology = make(map[string]string, len(in.DeprecatedTopology))
	for k, v := range in.DeprecatedTopology {
		out.Topology[k] = v
	}

	// 把独立的 Zone 字段写回 topology 的 well-known zone 标签，与正向转换保持对称。
	if in.Zone != nil {
		out.Topology[corev1.LabelTopologyZone] = *in.Zone
	}

	// 只有 topology 中还没有 hostname 时才用 NodeName 补上：
	// 这样既兼容 v1beta1 中 topology[hostname] 与 nodename 可以不同（奇怪但被允许）的行为，
	// 又不会覆盖已显式设置的 hostname 值。
	if in.NodeName != nil && out.Topology[corev1.LabelHostname] == "" {
		out.Topology[corev1.LabelHostname] = *in.NodeName
	}

	return nil
}
