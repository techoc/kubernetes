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

// LabelZonesToSet converts a PV label value from string containing a delimited list of zones to set
// PV 多可用区标签使用 cloudvolume.LabelMultiZoneDelimiter 分隔，而不是逗号。
func LabelZonesToSet(labelZonesValue string) (sets.String, error) {
	return stringToSet(labelZonesValue, cloudvolume.LabelMultiZoneDelimiter)
}

// ZonesSetToLabelValue converts zones set to label value
// 顺序不重要（集合语义），因此使用 UnsortedList 避免额外排序开销。
func ZonesSetToLabelValue(strSet sets.String) string {
	return strings.Join(strSet.UnsortedList(), cloudvolume.LabelMultiZoneDelimiter)
}

// ZonesToSet converts a string containing a comma separated list of zones to set
// 用于 StorageClass 的 zones 参数解析；与 PV 标签使用不同的分隔符。
func ZonesToSet(zonesString string) (sets.String, error) {
	zones, err := stringToSet(zonesString, ",")
	if err != nil {
		return nil, fmt.Errorf("error parsing zones %s, must be strings separated by commas: %v", zonesString, err)
	}
	return zones, nil
}

// StringToSet converts a string containing list separated by specified delimiter to a set
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

// LabelZonesToList converts a PV label value from string containing a delimited list of zones to list
// 与 LabelZonesToSet 不同，这里保留输入顺序（例如主 zone 在前），供需要确定顺序的调用方使用。
func LabelZonesToList(labelZonesValue string) ([]string, error) {
	return stringToList(labelZonesValue, cloudvolume.LabelMultiZoneDelimiter)
}

// StringToList converts a string containing list separated by specified delimiter to a list
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

// SelectZoneForVolume is a wrapper around SelectZonesForVolume
// to select a single zone for a volume based on parameters
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

// SelectZonesForVolume selects zones for a volume based on several factors:
// node.zone, allowedTopologies, zone/zones parameters from storageclass,
// zones with active nodes from the cluster. The number of zones = replicas.
// 优先级：node 的 zone（WaitForFirstConsumer 场景）> allowedTopologies > zone/zones 参数
// > 集群中有节点的 zone。zone 与 zones 参数互斥，同时出现直接报错。
func SelectZonesForVolume(zoneParameterPresent, zonesParameterPresent bool, zoneParameter string, zonesParameter, zonesWithNodes sets.String, node *v1.Node, allowedTopologies []v1.TopologySelectorTerm, pvcName string, numReplicas uint32) (sets.String, error) {
	if zoneParameterPresent && zonesParameterPresent {
		return nil, fmt.Errorf("both zone and zones StorageClass parameters must not be used at the same time")
	}

	var zoneFromNode string
	// pick one zone from node if present
	if node != nil {
		// VolumeScheduling implicit since node is not nil
		// 有 node 说明处于 WaitForFirstConsumer：StorageClass 中再指定 zone 会与调度结果冲突。
		if zoneParameterPresent || zonesParameterPresent {
			return nil, fmt.Errorf("zone[s] cannot be specified in StorageClass if VolumeBindingMode is set to WaitForFirstConsumer. Please specify allowedTopologies in StorageClass for constraining zones")
		}

		// pick node's zone for one of the replicas
		// 优先新标签，回退到已弃用的 failure-domain 标签，兼容未升级的节点。
		var ok bool
		zoneFromNode, ok = node.ObjectMeta.Labels[v1.LabelTopologyZone]
		if !ok {
			zoneFromNode, ok = node.ObjectMeta.Labels[v1.LabelFailureDomainBetaZone]
			if !ok {
				return nil, fmt.Errorf("Either %s or %s Label for node missing", v1.LabelTopologyZone, v1.LabelFailureDomainBetaZone)
			}
		}
		// if single replica volume and node with zone found, return immediately
		// 单副本必须与 Pod 同 zone，无需再考虑其他候选。
		if numReplicas == 1 {
			return sets.NewString(zoneFromNode), nil
		}
	}

	// pick zone from allowedZones if specified
	allowedZones, err := ZonesFromAllowedTopologies(allowedTopologies)
	if err != nil {
		return nil, err
	}

	// 指定了 allowedTopologies 却没有 zone 项，说明用户配置有误，不能静默忽略约束。
	if (len(allowedTopologies) > 0) && (allowedZones.Len() == 0) {
		return nil, fmt.Errorf("no matchLabelExpressions with %s key found in allowedTopologies. Please specify matchLabelExpressions with %s key", v1.LabelTopologyZone, v1.LabelTopologyZone)
	}

	if allowedZones.Len() > 0 {
		// VolumeScheduling implicit since allowedZones present
		if zoneParameterPresent || zonesParameterPresent {
			return nil, fmt.Errorf("zone[s] cannot be specified in StorageClass if allowedTopologies specified")
		}
		// scheduler will guarantee if node != null above, zoneFromNode is member of allowedZones.
		// so if zoneFromNode != "", we can safely assume it is part of allowedZones.
		zones, err := chooseZonesForVolumeIncludingZone(allowedZones, pvcName, zoneFromNode, numReplicas)
		if err != nil {
			return nil, fmt.Errorf("cannot process zones in allowedTopologies: %v", err)
		}
		return zones, nil
	}

	// pick zone from parameters if present
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
		// directly choose from zones parameter; no zone from node need to be considered
		return ChooseZonesForVolume(zonesParameter, pvcName, numReplicas), nil
	}

	// pick zone from zones with nodes
	if zonesWithNodes.Len() > 0 {
		// If node != null (and thus zoneFromNode != ""), zoneFromNode will be member of zonesWithNodes
		zones, err := chooseZonesForVolumeIncludingZone(zonesWithNodes, pvcName, zoneFromNode, numReplicas)
		if err != nil {
			return nil, fmt.Errorf("cannot process zones where nodes exist in the cluster: %v", err)
		}
		return zones, nil
	}
	return nil, fmt.Errorf("cannot determine zones to provision volume in")
}

// ZonesFromAllowedTopologies returns a list of zones specified in allowedTopologies
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

// chooseZonesForVolumeIncludingZone is a wrapper around ChooseZonesForVolume that ensures zoneToInclude is chosen
// zoneToInclude can either be empty in which case it is ignored. If non-empty, zoneToInclude is expected to be member of zones.
// numReplicas is expected to be > 0 and <= zones.Len()
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

// ChooseZonesForVolume is identical to ChooseZoneForVolume, but selects a multiple zones, for multi-zone disks.
// 选择结果只取决于 pvcName 与 zone 列表：同一 PVC 会稳定落在相同 zone，
// 使 StatefulSet 的卷分布在集群 zone 集合不变时保持可预测。
func ChooseZonesForVolume(zones sets.String, pvcName string, numZones uint32) sets.String {
	// No zones available, return empty set.
	replicaZones := sets.NewString()
	if zones.Len() == 0 {
		return replicaZones
	}

	// We create the volume in a zone determined by the name
	// Eventually the scheduler will coordinate placement into an available zone
	hash, index := getPVCNameHashAndIndexOffset(pvcName)

	// Zones.List returns zones in a consistent order (sorted)
	// We do have a potential failure case where volumes will not be properly spread,
	// if the set of zones changes during StatefulSet volume creation.  However, this is
	// probably relatively unlikely because we expect the set of zones to be essentially
	// static for clusters.
	// Hopefully we can address this problem if/when we do full scheduler integration of
	// PVC placement (which could also e.g. avoid putting volumes in overloaded or
	// unhealthy zones)
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
		// We should always be called with a name; this shouldn't happen
		klog.Warningf("No name defined during volume create; choosing random zone")

		hash = rand.Uint32()
	} else {
		hashString := pvcName

		// Heuristic to make sure that volumes in a StatefulSet are spread across zones
		// StatefulSet PVCs are (currently) named ClaimName-StatefulSetName-Id,
		// where Id is an integer index.
		// Note though that if a StatefulSet pod has multiple claims, we need them to be
		// in the same zone, because otherwise the pod will be unable to mount both volumes,
		// and will be unschedulable.  So we hash _only_ the "StatefulSetName" portion when
		// it looks like `ClaimName-StatefulSetName-Id`.
		// We continue to round-robin volume names that look like `Name-Id` also; this is a useful
		// feature for users that are creating statefulset-like functionality without using statefulsets.
		lastDash := strings.LastIndexByte(pvcName, '-')
		if lastDash != -1 {
			statefulsetIDString := pvcName[lastDash+1:]
			statefulsetID, err := strconv.ParseUint(statefulsetIDString, 10, 32)
			if err == nil {
				// Offset by the statefulsetID, so we round-robin across zones
				index = uint32(statefulsetID)
				// We still hash the volume name, but only the prefix
				hashString = pvcName[:lastDash]

				// In the special case where it looks like `ClaimName-StatefulSetName-Id`,
				// hash only the StatefulSetName, so that different claims on the same StatefulSet
				// member end up in the same zone.
				// Note that StatefulSetName (and ClaimName) might themselves both have dashes.
				// We actually just take the portion after the final - of ClaimName-StatefulSetName.
				// For our purposes it doesn't much matter (just suboptimal spreading).
				lastDash := strings.LastIndexByte(hashString, '-')
				if lastDash != -1 {
					hashString = hashString[lastDash+1:]
				}

				klog.V(2).Infof("Detected StatefulSet-style volume name %q; index=%d", pvcName, index)
			}
		}

		// We hash the (base) volume name, so we don't bias towards the first N zones
		h := fnv.New32()
		h.Write([]byte(hashString))
		hash = h.Sum32()
	}

	return hash, index
}
