/*
Copyright 2019 The Kubernetes Authors.

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

package helpers

import (
	"fmt"
	"hash/fnv"
	"math/rand"
	"strconv"
	"strings"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/sets"
	cloudvolume "k8s.io/cloud-provider/volume"
	"k8s.io/klog/v2"
)

// LabelZonesToSet 把 PV 标签值（包含分隔符分隔的 zone 列表的字符串）转换为集合。
// PV 多可用区标签使用 cloudvolume.LabelMultiZoneDelimiter 分隔，而不是逗号。
func LabelZonesToSet(labelZonesValue string) (sets.String, error) {
	return stringToSet(labelZonesValue, cloudvolume.LabelMultiZoneDelimiter)
}

// ZonesSetToLabelValue 将 zone 集合转换为标签值。
// 顺序不重要（集合语义），因此使用 UnsortedList 避免额外排序开销。
func ZonesSetToLabelValue(strSet sets.String) string {
	return strings.Join(strSet.UnsortedList(), cloudvolume.LabelMultiZoneDelimiter)
}

// ZonesToSet 将逗号分隔的 zone 列表字符串转换为集合。
// 用于 StorageClass 的 zones 参数解析；与 PV 标签使用不同的分隔符。
func ZonesToSet(zonesString string) (sets.String, error) {
	zones, err := stringToSet(zonesString, ",")
	if err != nil {
		return nil, fmt.Errorf("error parsing zones %s, must be strings separated by commas: %v", zonesString, err)
	}
	return zones, nil
}

// StringToSet 把由指定分隔符分隔的列表字符串转换为集合。
// 去空白后不允许空项：空 zone 会使后续拓扑选择无法收敛。
func stringToSet(str, delimiter string) (sets.String, error) {
	zonesSlice := strings.Split(str, delimiter)
	zonesSet := make(sets.String)
	for _, zone := range zonesSlice {
		trimmedZone := strings.TrimSpace(zone)
		if trimmedZone == "" {
			return make(sets.String), fmt.Errorf(
				"%q separated list (%q) must not contain an empty string",
				delimiter,
				str)
		}
		zonesSet.Insert(trimmedZone)
	}
	return zonesSet, nil
}

// LabelZonesToList 把 PV 标签值（包含分隔符分隔的 zone 列表的字符串）转换为列表。
// 与 LabelZonesToSet 不同，这里保留输入顺序（例如主 zone 在前），供需要确定顺序的调用方使用。
func LabelZonesToList(labelZonesValue string) ([]string, error) {
	return stringToList(labelZonesValue, cloudvolume.LabelMultiZoneDelimiter)
}

// StringToList 把由指定分隔符分隔的列表字符串转换为列表。
// 与 stringToSet 一样拒绝空项，但保留顺序与重复项。
func stringToList(str, delimiter string) ([]string, error) {
	zonesSlice := make([]string, 0)
	for _, zone := range strings.Split(str, delimiter) {
		trimmedZone := strings.TrimSpace(zone)
		if trimmedZone == "" {
			return nil, fmt.Errorf(
				"%q separated list (%q) must not contain an empty string",
				delimiter,
				str)
		}
		zonesSlice = append(zonesSlice, trimmedZone)
	}
	return zonesSlice, nil
}

// SelectZoneForVolume 是对 SelectZonesForVolume 的包装，
// 根据参数为卷挑选单个 zone。
// 单副本场景的便捷入口；PopAny 对单元素集合等价于取唯一值。
func SelectZoneForVolume(zoneParameterPresent, zonesParameterPresent bool, zoneParameter string, zonesParameter, zonesWithNodes sets.String, node *v1.Node, allowedTopologies []v1.TopologySelectorTerm, pvcName string) (string, error) {
	zones, err := SelectZonesForVolume(zoneParameterPresent, zonesParameterPresent, zoneParameter, zonesParameter, zonesWithNodes, node, allowedTopologies, pvcName, 1)
	if err != nil {
		return "", err
	}
	zone, ok := zones.PopAny()
	if !ok {
		return "", fmt.Errorf("could not determine a zone to provision volume in")
	}
	return zone, nil
}

// SelectZonesForVolume 基于多个因素为卷选择 zone：
// node.zone、allowedTopologies、StorageClass 中的 zone/zones 参数、
// 集群中有活动节点的 zone；所选 zone 的数量等于副本数（replicas）。
// 优先级：node 的 zone（WaitForFirstConsumer 场景）> allowedTopologies > zone/zones 参数
// > 集群中有节点的 zone。zone 与 zones 参数互斥，同时出现直接报错。
func SelectZonesForVolume(zoneParameterPresent, zonesParameterPresent bool, zoneParameter string, zonesParameter, zonesWithNodes sets.String, node *v1.Node, allowedTopologies []v1.TopologySelectorTerm, pvcName string, numReplicas uint32) (sets.String, error) {
	if zoneParameterPresent && zonesParameterPresent {
		return nil, fmt.Errorf("both zone and zones StorageClass parameters must not be used at the same time")
	}

	var zoneFromNode string
	// 有 node 时从节点取 zone。
	if node != nil {
		// node 非 nil 意味着隐式启用了 VolumeScheduling。
		// 有 node 说明处于 WaitForFirstConsumer：StorageClass 中再指定 zone 会与调度结果冲突。
		if zoneParameterPresent || zonesParameterPresent {
			return nil, fmt.Errorf("zone[s] cannot be specified in StorageClass if VolumeBindingMode is set to WaitForFirstConsumer. Please specify allowedTopologies in StorageClass for constraining zones")
		}

		// 把 node 所在 zone 分配给其中一个副本。
		// 优先新标签，回退到已弃用的 failure-domain 标签，兼容未升级的节点。
		var ok bool
		zoneFromNode, ok = node.ObjectMeta.Labels[v1.LabelTopologyZone]
		if !ok {
			zoneFromNode, ok = node.ObjectMeta.Labels[v1.LabelFailureDomainBetaZone]
			if !ok {
				return nil, fmt.Errorf("Either %s or %s Label for node missing", v1.LabelTopologyZone, v1.LabelFailureDomainBetaZone)
			}
		}
		// 若为单副本卷且找到了带 zone 的 node，立即返回。
		// 单副本必须与 Pod 同 zone，无需再考虑其他候选。
		if numReplicas == 1 {
			return sets.NewString(zoneFromNode), nil
		}
	}

	// 指定 allowedTopologies 时从允许的 zone 中选。
	allowedZones, err := ZonesFromAllowedTopologies(allowedTopologies)
	if err != nil {
		return nil, err
	}

	// 指定了 allowedTopologies 却没有 zone 项，说明用户配置有误，不能静默忽略约束。
	if (len(allowedTopologies) > 0) && (allowedZones.Len() == 0) {
		return nil, fmt.Errorf("no matchLabelExpressions with %s key found in allowedTopologies. Please specify matchLabelExpressions with %s key", v1.LabelTopologyZone, v1.LabelTopologyZone)
	}

	if allowedZones.Len() > 0 {
		// allowedZones 存在即隐含启用了 VolumeScheduling。
		if zoneParameterPresent || zonesParameterPresent {
			return nil, fmt.Errorf("zone[s] cannot be specified in StorageClass if allowedTopologies specified")
		}
		// 调度器保证节点存在时其 zone 属于 allowedZones，
		// 所以 zoneFromNode 非空即可放心当作其中一员。
		zones, err := chooseZonesForVolumeIncludingZone(allowedZones, pvcName, zoneFromNode, numReplicas)
		if err != nil {
			return nil, fmt.Errorf("cannot process zones in allowedTopologies: %v", err)
		}
		return zones, nil
	}

	// 未指定其他来源时从 StorageClass 参数取 zone。
	if zoneParameterPresent {
		// 单 zone 无法满足多副本，要求用户改用 zones 或 allowedTopologies。
		if numReplicas > 1 {
			return nil, fmt.Errorf("zone cannot be specified if desired number of replicas for pv is greather than 1. Please specify zones or allowedTopologies to specify desired zones")
		}
		return sets.NewString(zoneParameter), nil
	}

	if zonesParameterPresent {
		if uint32(zonesParameter.Len()) < numReplicas {
			return nil, fmt.Errorf("not enough zones found in zones parameter to provision a volume with %d replicas. Found %d zones, need %d zones", numReplicas, zonesParameter.Len(), numReplicas)
		}
		// 直接从 zones 参数选取；节点的 zone 已在前面处理过。
		return ChooseZonesForVolume(zonesParameter, pvcName, numReplicas), nil
	}

	// 最后从集群中有节点的 zone 中选。
	if zonesWithNodes.Len() > 0 {
		// 节点存在时其 zone 必然在 zonesWithNodes 中。
		zones, err := chooseZonesForVolumeIncludingZone(zonesWithNodes, pvcName, zoneFromNode, numReplicas)
		if err != nil {
			return nil, fmt.Errorf("cannot process zones where nodes exist in the cluster: %v", err)
		}
		return zones, nil
	}
	return nil, fmt.Errorf("cannot determine zones to provision volume in")
}

// ZonesFromAllowedTopologies 返回 allowedTopologies 中指定的 zone 列表。
// 只接受 zone 相关 key；出现其他 topology key 时报错，避免误把 region 等约束当作 zone。
func ZonesFromAllowedTopologies(allowedTopologies []v1.TopologySelectorTerm) (sets.String, error) {
	zones := make(sets.String)
	for _, term := range allowedTopologies {
		for _, exp := range term.MatchLabelExpressions {
			if exp.Key == v1.LabelTopologyZone || exp.Key == v1.LabelFailureDomainBetaZone {
				for _, value := range exp.Values {
					zones.Insert(value)
				}
			} else {
				return nil, fmt.Errorf("unsupported key found in matchLabelExpressions: %s", exp.Key)
			}
		}
	}
	return zones, nil
}

// chooseZonesForVolumeIncludingZone 是对 ChooseZonesForVolume 的包装，确保 zoneToInclude 一定会被选中。
// zoneToInclude 可以为空（此时被忽略）；若非空，则它应当是 zones 的成员；
// numReplicas 预期满足 > 0 且 <= zones.Len()。
// 先扣除必选 zone 再补选剩余副本，保证结果仍由 PVC 名称的哈希决定（可重现）。
func chooseZonesForVolumeIncludingZone(zones sets.String, pvcName, zoneToInclude string, numReplicas uint32) (sets.String, error) {
	if numReplicas == 0 {
		return nil, fmt.Errorf("invalid number of replicas passed")
	}
	if uint32(zones.Len()) < numReplicas {
		return nil, fmt.Errorf("not enough zones found to provision a volume with %d replicas. Need at least %d distinct zones for a volume with %d replicas", numReplicas, numReplicas, numReplicas)
	}
	if zoneToInclude != "" && !zones.Has(zoneToInclude) {
		return nil, fmt.Errorf("zone to be included: %s needs to be member of set: %v", zoneToInclude, zones)
	}
	// 候选数与副本数相等时无需选择，直接全取，避免对集合做可变删除。
	if uint32(zones.Len()) == numReplicas {
		return zones, nil
	}
	if zoneToInclude != "" {
		// 删除会修改调用方的集合，因此仅在此分支（已确认不直接返回 zones）执行。
		zones.Delete(zoneToInclude)
		numReplicas = numReplicas - 1
	}
	zonesChosen := ChooseZonesForVolume(zones, pvcName, numReplicas)
	if zoneToInclude != "" {
		zonesChosen.Insert(zoneToInclude)
	}
	return zonesChosen, nil
}

// ChooseZonesForVolume 与 ChooseZoneForVolume 相同，但为多 zone 盘选择多个 zone。
// 选择结果只取决于 pvcName 与 zone 列表：同一 PVC 会稳定落在相同 zone，
// 使 StatefulSet 的卷分布在集群 zone 集合不变时保持可预测。
func ChooseZonesForVolume(zones sets.String, pvcName string, numZones uint32) sets.String {
	// 没有可用 zone 时返回空集合。
	replicaZones := sets.NewString()
	if zones.Len() == 0 {
		return replicaZones
	}

	// 按 PVC 名字的哈希决定卷落在哪个 zone；调度器上线后会与可用 zone 的选择协调。
	hash, index := getPVCNameHashAndIndexOffset(pvcName)

	// Zones.List 以一致（排序后）的顺序返回 zone。
	// 存在一个潜在的失败场景：若 zone 集合在 StatefulSet 创建卷的过程中发生变化，
	// 卷的分布可能不均匀；不过这基本不会发生，因为我们期望集群的 zone 集合几乎静态。
	// 如果未来把 PVC 放置完整接入调度器，也许能解决这个问题
	// （届时还能避免把卷放到过载或不健康的 zone）。
	// 排序保证哈希取模结果跨进程稳定；zone 集合变化会改变映射，这是已知限制。
	zoneSlice := zones.List()

	startingIndex := index * numZones
	for index = startingIndex; index < startingIndex+numZones; index++ {
		zone := zoneSlice[(hash+index)%uint32(len(zoneSlice))]
		replicaZones.Insert(zone)
	}

	klog.V(2).Infof("Creating volume for replicated PVC %q; chosen zones=%q from zones=%q",
		pvcName, replicaZones.UnsortedList(), zoneSlice)
	return replicaZones
}

// getPVCNameHashAndIndexOffset 解析 PVC 名称中的 StatefulSet 序号，
// 返回用于散列的字符串哈希与起始偏移。序号让同一 StatefulSet 的不同副本
// 在 zone 间轮转，而哈希前缀让同一 Pod 的多个 claim 落在同一 zone。
func getPVCNameHashAndIndexOffset(pvcName string) (hash uint32, index uint32) {
	if pvcName == "" {
		// 正常情况下总会传入名字，这里不该发生。
		klog.Warningf("No name defined during volume create; choosing random zone")

		hash = rand.Uint32()
	} else {
		hashString := pvcName

		// 用启发式规则让 StatefulSet 的卷分散在不同 zone。
		// StatefulSet PVC 当前命名为 ClaimName-StatefulSetName-Id，Id 是整数序号。
		// 但同一 Pod 的多个 claim 必须落在同一 zone，否则无法同时挂载两个卷而不可调度。
		// 因此形如 `ClaimName-StatefulSetName-Id` 时只对 StatefulSetName 部分做哈希。
		// 形如 `Name-Id` 的卷名仍按序号轮转，方便未使用 StatefulSet 的用户实现类似能力。
		lastDash := strings.LastIndexByte(pvcName, '-')
		if lastDash != -1 {
			statefulsetIDString := pvcName[lastDash+1:]
			statefulsetID, err := strconv.ParseUint(statefulsetIDString, 10, 32)
			if err == nil {
				// 按 StatefulSet 序号偏移，让各副本轮转分布到不同 zone。
				index = uint32(statefulsetID)
				// 哈希仍用卷名，但只取前缀部分。
				hashString = pvcName[:lastDash]

				// 形如 `ClaimName-StatefulSetName-Id` 时仅哈希 StatefulSetName，
				// 使同一 StatefulSet 成员上的多个 claim 落在同一 zone。
				// StatefulSetName 与 ClaimName 自身也可能含连字符，这里取最后一段，
				// 对结果影响有限，只是分布稍不理想。
				lastDash := strings.LastIndexByte(hashString, '-')
				if lastDash != -1 {
					hashString = hashString[lastDash+1:]
				}

				klog.V(2).Infof("Detected StatefulSet-style volume name %q; index=%d", pvcName, index)
			}
		}

		// 对卷名基础部分做哈希，避免总是偏向前 N 个 zone。
		h := fnv.New32()
		h.Write([]byte(hashString))
		hash = h.Sum32()
	}

	return hash, index
}
