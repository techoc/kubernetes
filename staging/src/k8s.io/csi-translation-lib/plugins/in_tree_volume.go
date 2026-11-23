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

package plugins

import (
	"errors"
	"fmt"
	"sort"
	"strings"

	v1 "k8s.io/api/core/v1"
	storage "k8s.io/api/storage/v1"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/klog/v2"
)

// InTreePlugin handles translations between CSI and in-tree sources in a PV
// InTreePlugin 定义了 in-tree 卷插件与 CSI 之间双向翻译所需实现的契约。
// 每个 in-tree 插件实现该接口后，既能把自身的 VolumeSource、StorageClass 转换为 CSI 形式，
// 也能把 CSI 形式的 PV 还原为 in-tree 形式，从而支撑卷的 CSI Migration（迁移）。
type InTreePlugin interface {

	// TranslateInTreeStorageClassToCSI takes in-tree volume options
	// and translates them to a volume options consumable by CSI plugin
	// TranslateInTreeStorageClassToCSI 把 in-tree StorageClass 的参数翻译为 CSI 驱动可消费的参数。
	// 例如把已废弃的 fstype、zone 等参数改写成 CSI 约定使用的键。
	TranslateInTreeStorageClassToCSI(logger klog.Logger, sc *storage.StorageClass) (*storage.StorageClass, error)

	// TranslateInTreeInlineVolumeToCSI takes a inline volume and will translate
	// the in-tree inline volume source to a CSIPersistentVolumeSource
	// A PV object containing the CSIPersistentVolumeSource in it's spec is returned
	// podNamespace is only needed for azurefile to fetch secret namespace, no need to be set for other plugins.
	// TranslateInTreeInlineVolumeToCSI 把 Pod 中内联（inline）的 in-tree 卷转换为 CSI 形式。
	// 返回值是一个「虚拟」PV，其 spec 中携带翻译好的 CSIPersistentVolumeSource。
	// podNamespace 仅 azurefile 需要，用来定位 Secret 所在的命名空间，其他插件无需传入。
	TranslateInTreeInlineVolumeToCSI(logger klog.Logger, volume *v1.Volume, podNamespace string) (*v1.PersistentVolume, error)

	// TranslateInTreePVToCSI takes a persistent volume and will translate
	// the in-tree pv source to a CSI Source. The input persistent volume can be modified
	// TranslateInTreePVToCSI 把 in-tree PV 的卷源翻译为 CSI PersistentVolumeSource。
	// 入参 pv 允许被原地修改（例如补写拓扑、注解相关信息）。
	TranslateInTreePVToCSI(logger klog.Logger, pv *v1.PersistentVolume) (*v1.PersistentVolume, error)

	// TranslateCSIPVToInTree takes a PV with a CSI PersistentVolume Source and will translate
	// it to a in-tree Persistent Volume Source for the in-tree volume
	// by the `Driver` field in the CSI Source. The input PV object can be modified
	// TranslateCSIPVToInTree 是反向翻译：依据 CSI Source 的 Driver 字段，
	// 把 CSI PV 还原成对应的 in-tree PersistentVolumeSource。
	// 入参 pv 同样允许被原地修改。
	TranslateCSIPVToInTree(pv *v1.PersistentVolume) (*v1.PersistentVolume, error)

	// CanSupport tests whether the plugin supports a given persistent volume
	// specification from the API.
	// CanSupport 判断本插件能否处理给定的 PV 规格，用于翻译前的分派与识别。
	CanSupport(pv *v1.PersistentVolume) bool

	// CanSupportInline tests whether the plugin supports a given inline volume
	// specification from the API.
	// CanSupportInline 判断本插件能否处理给定的 inline Volume 规格。
	CanSupportInline(vol *v1.Volume) bool

	// GetInTreePluginName returns the in-tree plugin name this migrates
	// GetInTreePluginName 返回本插件所替代的 in-tree 插件名称（如 kubernetes.io/gce-pd）。
	GetInTreePluginName() string

	// GetCSIPluginName returns the name of the CSI plugin that supersedes the in-tree plugin
	// GetCSIPluginName 返回接管该 in-tree 插件的 CSI 驱动名称（如 pd.csi.storage.gke.io）。
	GetCSIPluginName() string

	// RepairVolumeHandle generates a correct volume handle based on node ID information.
	// RepairVolumeHandle 依据节点 ID 信息重新生成正确的 volume handle，
	// 用于修复历史遗留的、格式不规范的 CSI volume handle。
	RepairVolumeHandle(volumeHandle, nodeID string) (string, error)
}

const (
	// fsTypeKey is the deprecated storage class parameter key for fstype
	// fsTypeKey 是 in-tree StorageClass 中已废弃的 fstype 参数键。
	fsTypeKey = "fstype"
	// csiFsTypeKey is the storage class parameter key for CSI fstype
	// csiFsTypeKey 是 CSI 驱动约定使用的 fstype 参数键，翻译时会把 fsTypeKey 改写成它。
	csiFsTypeKey = "csi.storage.k8s.io/fstype"
	// zoneKey is the deprecated storage class parameter key for zone
	// zoneKey 是已废弃的单可用区参数键。
	zoneKey = "zone"
	// zonesKey is the deprecated storage class parameter key for zones
	// zonesKey 是已废弃的多可用区参数键。
	zonesKey = "zones"
)

// replaceTopology overwrites an existing key in NodeAffinity by a new one.
// If there are any newKey already exist in an expression of a term, we will
// not combine the replaced key Values with the existing ones.
// So there might be duplication if there is any newKey expression
// already in the terms.
// replaceTopology 把 PV NodeAffinity 中所有匹配 oldKey 的表达式的 Key 改写为 newKey。
// 注意它只改 Key，不合并 Values；若某个 term 里原本已存在 newKey 表达式，
// 改写后可能出现重复的 Key，这是刻意保留的行为（调用方另有去重逻辑）。
func replaceTopology(pv *v1.PersistentVolume, oldKey, newKey string) error {
	// Make sure the necessary fields exist
	// 先确保从 PV 到 NodeSelectorTerms 这条路径上的每一层都存在；
	// 缺少任何一层都说明没有可替换的拓扑，直接返回成功即可。
	if pv == nil || pv.Spec.NodeAffinity == nil || pv.Spec.NodeAffinity.Required == nil ||
		pv.Spec.NodeAffinity.Required.NodeSelectorTerms == nil || len(pv.Spec.NodeAffinity.Required.NodeSelectorTerms) == 0 {
		return nil
	}
	// 遍历每个 term、每个表达式，逐条改写 Key 本身，Values 与 Operator 保持不变。
	for i := range pv.Spec.NodeAffinity.Required.NodeSelectorTerms {
		for j, r := range pv.Spec.NodeAffinity.Required.NodeSelectorTerms[i].MatchExpressions {
			if r.Key == oldKey {
				pv.Spec.NodeAffinity.Required.NodeSelectorTerms[i].MatchExpressions[j].Key = newKey
			}
		}
	}

	return nil
}

// getTopologyValues returns all unique topology values with the given key found in
// the PV NodeAffinity. Sort by alphabetical order.
// This function collapses multiple zones into a list that is ORed. This assumes that
// the plugin does not support a constraint like key in "zone1" AND "zone2"
// getTopologyValues 收集 PV NodeAffinity 中指定 key 的所有取值，去重后按字典序返回。
// 它把多个 zone 折叠成一个「或」关系的列表，前提是插件不支持「key in zone1 AND zone2」这类与约束。
func getTopologyValues(pv *v1.PersistentVolume, key string) []string {
	// NodeAffinity 或其 Required 为空时没有拓扑可取，返回 nil。
	if pv.Spec.NodeAffinity == nil ||
		pv.Spec.NodeAffinity.Required == nil ||
		len(pv.Spec.NodeAffinity.Required.NodeSelectorTerms) < 1 {
		return nil
	}

	// 用 map 作为集合来去重，避免同一拓扑值在多个 term 中重复出现。
	values := make(map[string]bool)
	for i := range pv.Spec.NodeAffinity.Required.NodeSelectorTerms {
		for _, r := range pv.Spec.NodeAffinity.Required.NodeSelectorTerms[i].MatchExpressions {
			if r.Key == key {
				for _, v := range r.Values {
					values[v] = true
				}
			}
		}
	}
	// remove duplication and sort them in order for better usage
	// 把集合展开为切片并排序，使返回结果稳定、便于后续拼接和比较。
	var re []string
	for k := range values {
		re = append(re, k)
	}
	sort.Strings(re)
	return re
}

// addTopology appends the topology to the given PV to all Terms.
// addTopology 把一组 zone 作为 In 操作符的拓扑约束追加到 PV 的每一个 NodeSelectorTerm 上。
// 入参 zones 会先去除空白项和重复项；若过滤后为空则返回错误，避免写入无意义的拓扑。
func addTopology(pv *v1.PersistentVolume, topologyKey string, zones []string) error {
	// Make sure there are no duplicate or empty strings
	// 先清洗 zone 列表：去掉首尾空白并丢弃空字符串，同时利用集合去重。
	filteredZones := sets.String{}
	for i := range zones {
		zone := strings.TrimSpace(zones[i])
		if len(zone) > 0 {
			filteredZones.Insert(zone)
		}
	}

	// sets.String.List() 会返回有序列表，保证写入的 Values 顺序稳定。
	zones = filteredZones.List()
	if len(zones) < 1 {
		return errors.New("there are no valid zones to add to pv")
	}

	// Make sure the necessary fields exist
	// 逐级补齐 NodeAffinity 相关结构；缺哪一层就创建哪一层，
	// 这样调用方无需预先构造完整的 NodeAffinity 也能添加拓扑。
	if pv.Spec.NodeAffinity == nil {
		pv.Spec.NodeAffinity = new(v1.VolumeNodeAffinity)
	}

	if pv.Spec.NodeAffinity.Required == nil {
		pv.Spec.NodeAffinity.Required = new(v1.NodeSelector)
	}

	if len(pv.Spec.NodeAffinity.Required.NodeSelectorTerms) == 0 {
		pv.Spec.NodeAffinity.Required.NodeSelectorTerms = make([]v1.NodeSelectorTerm, 1)
	}

	// 构造一条 OpIn 的拓扑表达式，Values 为清洗后的 zone 集合。
	topology := v1.NodeSelectorRequirement{
		Key:      topologyKey,
		Operator: v1.NodeSelectorOpIn,
		Values:   zones,
	}

	// add the CSI topology to each term
	// 同一个拓扑必须加到每个 term 上，否则OR关系的term之间会出现拓扑约束不一致。
	for i := range pv.Spec.NodeAffinity.Required.NodeSelectorTerms {
		pv.Spec.NodeAffinity.Required.NodeSelectorTerms[i].MatchExpressions = append(
			pv.Spec.NodeAffinity.Required.NodeSelectorTerms[i].MatchExpressions,
			topology,
		)
	}

	return nil
}

// translateTopologyFromInTreeToCSI converts existing zone labels or in-tree topology to CSI topology.
// In-tree topology has precedence over zone labels. When both in-tree topology and zone labels exist
// for a particular CSI topology, in-tree topology will be used.
// This function will remove the Beta version Kubernetes topology label in case the node upgrade to a
// newer version where it does not have any Beta topology label anymore
// translateTopologyFromInTreeToCSI 把 in-tree 的 zone 标签或 NodeAffinity 拓扑转换为 CSI 拓扑。
// 优先级：NodeAffinity 中的 in-tree 拓扑高于 PV 上的 zone 标签；两者同时存在时以前者为准。
// 同时会把 Beta 版的 region 标签升级为 GA 版，避免节点升级到只有 GA 标签的新版本后无法调度。
func translateTopologyFromInTreeToCSI(pv *v1.PersistentVolume, csiTopologyKey string) error {

	// 先确定该 PV 实际使用的 zone/region 标签版本（GA 还是 Beta）。
	zoneLabel, regionLabel := getTopologyLabel(pv)

	// If Zone kubernetes topology exist, replace it to use csiTopologyKey
	// 优先使用 NodeAffinity 中已有的 Kubernetes zone 拓扑，直接把 Key 换成 CSI 拓扑 Key。
	zones := getTopologyValues(pv, zoneLabel)
	if len(zones) > 0 {
		replaceTopology(pv, zoneLabel, csiTopologyKey)
	} else {
		// if nothing is in the NodeAffinity, try to fetch the topology from PV labels
		// NodeAffinity 里没有可用拓扑时，退而使用 PV 标签中的 zone 信息：
		// 多 zone 以 labelMultiZoneDelimiter（"__"）分隔，拆开后作为 CSI 拓扑追加进去。
		if label, ok := pv.Labels[zoneLabel]; ok {
			zones = strings.Split(label, labelMultiZoneDelimiter)
			if len(zones) > 0 {
				addTopology(pv, csiTopologyKey, zones)
			}
		}
	}

	// if the in-tree PV has beta region label, replace it with GA label to ensure
	// the scheduler is able to schedule it on new nodes with only GA kubernetes label
	// No need to check it for zone label because it has already been replaced if exist
	// zone 标签无需额外处理，上面的分支已经完成替换；这里只负责把 Beta region 升级为 GA region。
	if regionLabel == v1.LabelFailureDomainBetaRegion {
		regions := getTopologyValues(pv, regionLabel)
		if len(regions) > 0 {
			replaceTopology(pv, regionLabel, v1.LabelTopologyRegion)
		}
	}

	return nil
}

// getTopologyLabel checks if the kubernetes topology label used in this
// PV is GA and return the zone/region label used.
// The version checking follows the following orders
//  1. Check NodeAffinity
//     1.1 Check if zoneGA exists, if yes return GA labels
//     1.2 Check if zoneBeta exists, if yes return Beta labels
//  2. Check PV labels
//     2.1 Check if zoneGA exists, if yes return GA labels
//     2.2 Check if zoneBeta exists, if yes return Beta labels
//
// getTopologyLabel 探测 PV 使用的是 GA 还是 Beta 版 Kubernetes 拓扑标签，
// 并成对返回对应的 zone/region 标签键。
// 检测顺序：先看 NodeAffinity（GA 优先于 Beta），再看 PV Labels（GA 优先于 Beta）。
func getTopologyLabel(pv *v1.PersistentVolume) (zoneLabel string, regionLabel string) {

	// NodeAffinity 中存在 GA zone 键，直接判定为 GA 版本。
	if zoneGA := TopologyKeyExist(v1.LabelTopologyZone, pv.Spec.NodeAffinity); zoneGA {
		return v1.LabelTopologyZone, v1.LabelTopologyRegion
	}
	// NodeAffinity 中只有 Beta zone 键，判定为 Beta 版本。
	if zoneBeta := TopologyKeyExist(v1.LabelFailureDomainBetaZone, pv.Spec.NodeAffinity); zoneBeta {
		return v1.LabelFailureDomainBetaZone, v1.LabelFailureDomainBetaRegion
	}
	// NodeAffinity 没有线索时，继续查看 PV 标签，同样 GA 优先。
	if _, zoneGA := pv.Labels[v1.LabelTopologyZone]; zoneGA {
		return v1.LabelTopologyZone, v1.LabelTopologyRegion
	}
	if _, zoneBeta := pv.Labels[v1.LabelFailureDomainBetaZone]; zoneBeta {
		return v1.LabelFailureDomainBetaZone, v1.LabelFailureDomainBetaRegion
	}
	// No labels or NodeAffinity exist, default to GA version
	// 都无法判断时默认使用 GA 标签，因为新版本集群只识别 GA 标签。
	return v1.LabelTopologyZone, v1.LabelTopologyRegion
}

// TopologyKeyExist checks if a certain key exists in a VolumeNodeAffinity
// TopologyKeyExist 判断给定的拓扑 Key 是否出现在 VolumeNodeAffinity 的任意表达式中。
func TopologyKeyExist(key string, vna *v1.VolumeNodeAffinity) bool {
	// 层级缺失或没有任何 term，自然不可能存在该 Key。
	if vna == nil || vna.Required == nil || vna.Required.NodeSelectorTerms == nil || len(vna.Required.NodeSelectorTerms) == 0 {
		return false
	}

	// 遍历所有 term 的所有表达式，找到即返回，无需继续。
	for _, nodeSelectorTerms := range vna.Required.NodeSelectorTerms {
		nsrequirements := nodeSelectorTerms.MatchExpressions
		for _, nodeSelectorRequirement := range nsrequirements {
			if nodeSelectorRequirement.Key == key {
				return true
			}
		}
	}
	return false
}

// regionParserFn 定义一个地域解析器：输入一组 zone 名称，推导出所属 region；
// 无法解析时返回错误。由各云插件按自身拓扑规则提供具体实现。
type regionParserFn func([]string) (string, error)

// translateTopologyFromCSIToInTree translate a CSI topology to
// Kubernetes topology and add topology labels to it. Note that this function
// will only work for plugin with a single topologyKey that translates to
// Kubernetes zone(and region if regionParser is passed in).
// If a plugin has more than one topologyKey, it will need to be processed
// separately by the plugin.
// If regionParser is nil, no region NodeAffinity will be added. If not nil,
// it'll be passed to regionTopologyHandler, which will add region topology NodeAffinity
// and labels for the given PV. It assumes the Zone NodeAffinity already exists.
// In short this function will,
// 1. Replace all CSI topology to Kubernetes Zone topology label
// 2. Process and generate region topology if a regionParser is passed
// 3. Add Kubernetes Topology labels(zone) if they do not exist
// translateTopologyFromCSIToInTree 是反向翻译：把 CSI 拓扑 Key 换回 Kubernetes zone 标签，
// 并在必要时补齐 region 拓扑和 zone 标签。
// 限制：只适用于「单个 CSI topologyKey 对应一个 Kubernetes zone」的插件；
// 多 topologyKey 的插件需要自行处理。regionParser 为 nil 时不生成 region 信息。
func translateTopologyFromCSIToInTree(pv *v1.PersistentVolume, csiTopologyKey string, regionParser regionParserFn) error {

	// 复用 getTopologyLabel 判定目标标签版本（这里只用到 zoneLabel）。
	zoneLabel, _ := getTopologyLabel(pv)

	// 1. Replace all CSI topology to Kubernetes Zone label
	// 第一步：把所有 CSI 拓扑 Key 改写为 Kubernetes zone 标签键。
	err := replaceTopology(pv, csiTopologyKey, zoneLabel)
	if err != nil {
		return fmt.Errorf("Failed to replace CSI topology to Kubernetes topology, error: %v", err)
	}

	// 2. Take care of region topology if a regionParser is passed
	if regionParser != nil {
		// let's make less strict on this one. Even if there is an error in the region processing, just ignore it
		// 第二步：若提供了解析器，则基于刚得到的 zone 拓扑推导并写入 region 拓扑。
		// 注释提到这里尽量宽松处理，但当前实现仍会把错误向上返回。
		err = regionTopologyHandler(pv, regionParser)
		if err != nil {
			return fmt.Errorf("Failed to handle region topology. error: %v", err)
		}
	}

	// 3. Add labels about Kubernetes Topology
	// 第三步：若 PV 尚无 zone 标签，则把 NodeAffinity 中的 zone 值用 "__" 连接后写入标签。
	zoneVals := getTopologyValues(pv, zoneLabel)
	if len(zoneVals) > 0 {
		if pv.Labels == nil {
			pv.Labels = make(map[string]string)
		}
		_, zoneOK := pv.Labels[zoneLabel]
		if !zoneOK {
			zoneValStr := strings.Join(zoneVals, labelMultiZoneDelimiter)
			pv.Labels[zoneLabel] = zoneValStr
		}
	}

	return nil
}

// translateAllowedTopologies translates allowed topologies within storage class or PV
// from legacy failure domain to given CSI topology key
// translateAllowedTopologies 把 StorageClass 或 PV 中 allowedTopologies 的旧版 zone 标签键
// （Beta 或 GA 的 failure-domain zone）替换为指定的 CSI topologyKey。
// 其他拓扑键原样透传，不做任何改动。
func translateAllowedTopologies(terms []v1.TopologySelectorTerm, key string) ([]v1.TopologySelectorTerm, error) {
	if terms == nil {
		return nil, nil
	}

	// 逐 term、逐表达式构造新的切片，避免原地修改入参。
	newTopologies := []v1.TopologySelectorTerm{}
	for _, term := range terms {
		newTerm := v1.TopologySelectorTerm{}
		for _, exp := range term.MatchLabelExpressions {
			var newExp v1.TopologySelectorLabelRequirement
			// 只有 zone 类的旧标签需要换算成 CSI key，Values 保持不变。
			if exp.Key == v1.LabelFailureDomainBetaZone || exp.Key == v1.LabelTopologyZone {
				newExp = v1.TopologySelectorLabelRequirement{
					Key:    key,
					Values: exp.Values,
				}
			} else {
				// Other topologies are passed through unchanged.
				// 非 zone 的拓扑键与 CSI 无关，直接沿用原表达式。
				newExp = exp
			}
			newTerm.MatchLabelExpressions = append(newTerm.MatchLabelExpressions, newExp)
		}
		newTopologies = append(newTopologies, newTerm)
	}
	return newTopologies, nil
}

// regionTopologyHandler will process the PV and add region
// kubernetes topology label to its NodeAffinity and labels
// It assumes the Zone NodeAffinity already exists
// Each provider is responsible for providing their own regionParser
// regionTopologyHandler 为每个 NodeSelectorTerm 补充 region 拓扑约束，并把 region 值写入 PV 标签。
// 前提：zone 拓扑已经存在；region 的具体推导逻辑由各云插件通过 regionParser 提供。
func regionTopologyHandler(pv *v1.PersistentVolume, regionParser regionParserFn) error {

	// Make sure the necessary fields exist
	// 没有 NodeAffinity/term 时无处理对象，直接返回。
	if pv == nil || pv.Spec.NodeAffinity == nil || pv.Spec.NodeAffinity.Required == nil ||
		pv.Spec.NodeAffinity.Required.NodeSelectorTerms == nil || len(pv.Spec.NodeAffinity.Required.NodeSelectorTerms) == 0 {
		return nil
	}

	// 取得与当前 PV 匹配的 zone/region 标签版本（GA 或 Beta）。
	zoneLabel, regionLabel := getTopologyLabel(pv)

	// process each term
	// 每个 term 独立处理，因为不同 term 可能包含不同 zone 集合。
	for index, nodeSelectorTerm := range pv.Spec.NodeAffinity.Required.NodeSelectorTerms {
		// In the first loop, see if regionLabel already exist
		regionExist := false
		var zoneVals []string
		// 先扫描该 term：若已有 region 约束则跳过；同时收集其中的 zone 值供解析器使用。
		for _, nsRequirement := range nodeSelectorTerm.MatchExpressions {
			if nsRequirement.Key == regionLabel {
				regionExist = true
				break
			} else if nsRequirement.Key == zoneLabel {
				zoneVals = append(zoneVals, nsRequirement.Values...)
			}
		}
		if regionExist {
			// Regionlabel already exist in this term, skip it
			continue
		}
		// If no regionLabel found, generate region label from the zoneLabel we collect from this term
		// 用该 term 的 zone 集合解析出所属 region；解析失败则整体失败返回。
		regionVal, err := regionParser(zoneVals)
		if err != nil {
			return err
		}
		// Add the regionVal to this term
		// 把推导出的 region 作为一条 OpIn 表达式追加到当前 term。
		pv.Spec.NodeAffinity.Required.NodeSelectorTerms[index].MatchExpressions =
			append(pv.Spec.NodeAffinity.Required.NodeSelectorTerms[index].MatchExpressions, v1.NodeSelectorRequirement{
				Key:      regionLabel,
				Operator: v1.NodeSelectorOpIn,
				Values:   []string{regionVal},
			})

	}

	// Add region label
	// 只有 region 值恰好唯一时才写入 PV 标签，避免跨 region 的 PV 被错误标记。
	regionVals := getTopologyValues(pv, regionLabel)
	if len(regionVals) == 1 {
		// We should only have exactly 1 region value
		if pv.Labels == nil {
			pv.Labels = make(map[string]string)
		}
		_, regionOK := pv.Labels[regionLabel]
		if !regionOK {
			pv.Labels[regionLabel] = regionVals[0]
		}
	}

	return nil
}
