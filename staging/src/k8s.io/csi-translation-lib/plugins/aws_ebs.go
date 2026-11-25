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
	"fmt"
	"net/url"
	"regexp"
	"strconv"
	"strings"

	v1 "k8s.io/api/core/v1"
	storage "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/klog/v2"
)

const (
	// AWSEBSDriverName 是 EBS CSI 驱动的名称。
	AWSEBSDriverName = "ebs.csi.aws.com"
	// AWSEBSInTreePluginName 是 in-tree EBS 插件的名称。
	AWSEBSInTreePluginName = "kubernetes.io/aws-ebs"
	// AWSEBSTopologyKey 是 AWS EBS CSI 驱动用于表示可用区（zone）的拓扑键。
	AWSEBSTopologyKey = "topology." + AWSEBSDriverName + "/zone"
	// iopsPerGBKey 是 StorageClass 中指定“每 GB 对应 IOPS”的参数名。
	iopsPerGBKey = "iopspergb"
	// allowIncreaseIOPSKey 是允许 CSI 驱动在给定卷大小下，按“卷大小 × 每 GB IOPS”
	// 计算出的总 IOPS 低于 AWS 支持的最小值时，自动提升到该最小值的参数名，
	// 用于保持 in-tree 插件的原有行为。
	allowIncreaseIOPSKey = "allowautoiopspergbincrease"
)

// 编译期断言：确保 awsElasticBlockStoreCSITranslator 实现了 InTreePlugin 接口。
var _ InTreePlugin = &awsElasticBlockStoreCSITranslator{}

// awsElasticBlockStoreCSITranslator 负责 in-tree EBS 与 CSI EBS 之间 PV spec 的双向翻译。
type awsElasticBlockStoreCSITranslator struct{}

// NewAWSElasticBlockStoreCSITranslator 返回一个新的 AWS EBS 翻译器实例。
func NewAWSElasticBlockStoreCSITranslator() InTreePlugin {
	return &awsElasticBlockStoreCSITranslator{}
}

// TranslateInTreeStorageClassToCSI 把 in-tree EBS StorageClass 的参数翻译为 CSI 参数：
// 重命名 fstype、补充 IOPS 参数，并把 zone/zones 转换为 AllowedTopologies。
func (t *awsElasticBlockStoreCSITranslator) TranslateInTreeStorageClassToCSI(logger klog.Logger, sc *storage.StorageClass) (*storage.StorageClass, error) {
	var (
		generatedTopologies []v1.TopologySelectorTerm
		params              = map[string]string{}
	)
	// 逐项遍历原参数，按小写参数名匹配后写入新的 params 映射。
	for k, v := range sc.Parameters {
		switch strings.ToLower(k) {
		case fsTypeKey:
			// in-tree 的 fstype 对应 CSI 的标准参数 csi.storage.k8s.io/fstype。
			params[csiFsTypeKey] = v
		case zoneKey:
			// 单个 zone 生成一个拓扑选择器，使用 EBS CSI 的区拓扑键。
			generatedTopologies = generateToplogySelectors(AWSEBSTopologyKey, []string{v})
		case zonesKey:
			// 逗号分隔的多个 zone 生成一个含多个匹配值的拓扑选择器。
			generatedTopologies = generateToplogySelectors(AWSEBSTopologyKey, strings.Split(v, ","))
		case iopsPerGBKey:
			// 保留 iopsPerGB 参数本身。
			params[k] = v
			// 额外开启自动提升 IOPS：给定卷大小下，卷大小 × 每 GB IOPS 低于 AWS
			// 支持的最小值时提升到最小值，复现 in-tree 插件的行为。
			params[allowIncreaseIOPSKey] = "true"
		default:
			// 其他参数原样透传给 CSI 驱动。
			params[k] = v
		}
	}

	// zone/zones 与 AllowedTopologies 语义重叠，二者不能同时设置；
	// 随后按来源把拓扑选择器写入或翻译为 CSI 拓扑键。
	if len(generatedTopologies) > 0 && len(sc.AllowedTopologies) > 0 {
		return nil, fmt.Errorf("cannot simultaneously set allowed topologies and zone/zones parameters")
	} else if len(generatedTopologies) > 0 {
		sc.AllowedTopologies = generatedTopologies
	} else if len(sc.AllowedTopologies) > 0 {
		// 已有拓扑使用 in-tree 键，需要翻译成 CSI 键。
		newTopologies, err := translateAllowedTopologies(sc.AllowedTopologies, AWSEBSTopologyKey)
		if err != nil {
			return nil, fmt.Errorf("failed translating allowed topologies: %v", err)
		}
		sc.AllowedTopologies = newTopologies
	}

	// 用转换后的参数集整体替换原参数。
	sc.Parameters = params

	return sc, nil
}

// TranslateInTreeInlineVolumeToCSI 把 Pod 中 inline 的 in-tree AWS EBS 卷转换为一个独立的 CSI PV。
func (t *awsElasticBlockStoreCSITranslator) TranslateInTreeInlineVolumeToCSI(logger klog.Logger, volume *v1.Volume, podNamespace string) (*v1.PersistentVolume, error) {
	// 入参必须是带 AWSElasticBlockStore 的 Volume，否则没有可翻译的来源。
	if volume == nil || volume.AWSElasticBlockStore == nil {
		return nil, fmt.Errorf("volume is nil or AWS EBS not defined on volume")
	}
	ebsSource := volume.AWSElasticBlockStore
	// 先把 Kubernetes 风格的卷 ID（可能含 aws:// 前缀）转换为纯 EBS 卷 ID，
	// 因为 CSI 的 volumeHandle 只接受 vol-xxx 形式。
	volumeHandle, err := KubernetesVolumeIDToEBSVolumeID(ebsSource.VolumeID)
	if err != nil {
		return nil, fmt.Errorf("failed to translate Kubernetes ID to EBS Volume ID %v", err)
	}
	pv := &v1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{
			// PV 名称按磁盘唯一，因为它会作为 staging 路径的唯一组成部分。
			Name: fmt.Sprintf("%s-%s", AWSEBSDriverName, volumeHandle),
		},
		Spec: v1.PersistentVolumeSpec{
			PersistentVolumeSource: v1.PersistentVolumeSource{
				CSI: &v1.CSIPersistentVolumeSource{
					Driver:       AWSEBSDriverName,
					VolumeHandle: volumeHandle,
					ReadOnly:     ebsSource.ReadOnly,
					FSType:       ebsSource.FSType,
					// 分区号在 CSI 源中没有对应字段，只能作为 VolumeAttributes 传递。
					VolumeAttributes: map[string]string{
						"partition": strconv.FormatInt(int64(ebsSource.Partition), 10),
					},
				},
			},
			// inline 卷没有原始访问模式，按 AWS EBS 的能力固定为 ReadWriteOnce。
			AccessModes: []v1.PersistentVolumeAccessMode{v1.ReadWriteOnce},
		},
	}
	return pv, nil
}

// TranslateInTreePVToCSI 把带 in-tree AWSElasticBlockStore 源的 PV 原地转换为 CSI PV。
func (t *awsElasticBlockStoreCSITranslator) TranslateInTreePVToCSI(logger klog.Logger, pv *v1.PersistentVolume) (*v1.PersistentVolume, error) {
	// 校验 PV 非空且确实带有 in-tree EBS 源。
	if pv == nil || pv.Spec.AWSElasticBlockStore == nil {
		return nil, fmt.Errorf("pv is nil or AWS EBS not defined on pv")
	}

	ebsSource := pv.Spec.AWSElasticBlockStore

	// 转换卷 ID，CSI 只接受不带 aws:// URL 前缀的 vol-xxx 形式。
	volumeHandle, err := KubernetesVolumeIDToEBSVolumeID(ebsSource.VolumeID)
	if err != nil {
		return nil, fmt.Errorf("failed to translate Kubernetes ID to EBS Volume ID %v", err)
	}

	// 逐字段映射：驱动名、卷句柄、只读标志、文件系统类型，以及放在
	// VolumeAttributes 中的分区号。
	csiSource := &v1.CSIPersistentVolumeSource{
		Driver:       AWSEBSDriverName,
		VolumeHandle: volumeHandle,
		ReadOnly:     ebsSource.ReadOnly,
		FSType:       ebsSource.FSType,
		VolumeAttributes: map[string]string{
			"partition": strconv.FormatInt(int64(ebsSource.Partition), 10),
		},
	}

	// PV 上的拓扑标签也需要从 in-tree 键翻译成 CSI 键。
	if err := translateTopologyFromInTreeToCSI(pv, AWSEBSTopologyKey); err != nil {
		return nil, fmt.Errorf("failed to translate topology: %v", err)
	}

	// 先清空旧源再挂上新源，避免在对象上同时保留两种 volume source。
	pv.Spec.AWSElasticBlockStore = nil
	pv.Spec.CSI = csiSource
	return pv, nil
}

// TranslateCSIPVToInTree 是反向翻译：把 EBS CSI 源还原为 in-tree AWSElasticBlockStore 源。
func (t *awsElasticBlockStoreCSITranslator) TranslateCSIPVToInTree(pv *v1.PersistentVolume) (*v1.PersistentVolume, error) {
	// 校验 PV 非空且确实带有 CSI 源。
	if pv == nil || pv.Spec.CSI == nil {
		return nil, fmt.Errorf("pv is nil or CSI source not defined on pv")
	}

	csiSource := pv.Spec.CSI

	// volumeHandle、FSType、ReadOnly 都有直接对应的 in-tree 字段。
	ebsSource := &v1.AWSElasticBlockStoreVolumeSource{
		VolumeID: csiSource.VolumeHandle,
		FSType:   csiSource.FSType,
		ReadOnly: csiSource.ReadOnly,
	}

	// 分区号存放在 VolumeAttributes 中，需要解析回 int32；格式非法时直接报错。
	if partition, ok := csiSource.VolumeAttributes["partition"]; ok {
		partValue, err := strconv.Atoi(partition)
		if err != nil {
			return nil, fmt.Errorf("failed to convert partition %v to integer: %v", partition, err)
		}
		ebsSource.Partition = int32(partValue)
	}

	// 反向翻译拓扑，并把 zone 推导出的 region 交给回调函数，以兼容回滚场景。
	if err := translateTopologyFromCSIToInTree(pv, AWSEBSTopologyKey, getAwsRegionFromZones); err != nil {
		return nil, fmt.Errorf("failed to translate topology. PV:%+v. Error:%v", *pv, err)
	}

	pv.Spec.CSI = nil
	pv.Spec.AWSElasticBlockStore = ebsSource
	return pv, nil
}

// CanSupport 判断该插件能否处理给定的 PV：即 PV 是否为 in-tree AWS EBS 源。
// 传入对象的 spec 应视为常量，不得修改。
func (t *awsElasticBlockStoreCSITranslator) CanSupport(pv *v1.PersistentVolume) bool {
	return pv != nil && pv.Spec.AWSElasticBlockStore != nil
}

// CanSupportInline 判断该插件能否处理给定的 inline Volume：即是否带 AWS EBS 源。
// 传入对象的 spec 应视为常量，不得修改。
func (t *awsElasticBlockStoreCSITranslator) CanSupportInline(volume *v1.Volume) bool {
	return volume != nil && volume.AWSElasticBlockStore != nil
}

// GetInTreePluginName 返回 in-tree 插件的名称。
func (t *awsElasticBlockStoreCSITranslator) GetInTreePluginName() string {
	return AWSEBSInTreePluginName
}

// GetCSIPluginName 返回对应 CSI 插件的名称。
func (t *awsElasticBlockStoreCSITranslator) GetCSIPluginName() string {
	return AWSEBSDriverName
}

// RepairVolumeHandle 对旧的 volumeHandle 做修复；AWS EBS 无需修复，原样返回。
func (t *awsElasticBlockStoreCSITranslator) RepairVolumeHandle(volumeHandle, nodeID string) (string, error) {
	return volumeHandle, nil
}

// awsVolumeRegMatch 用于校验 EBS 卷 ID 是否符合 vol-xxx（且不含路径分隔符）的格式。
var awsVolumeRegMatch = regexp.MustCompile("^vol-[^/]*$")

// KubernetesVolumeIDToEBSVolumeID 把 Kubernetes 卷 ID 转换为纯 EBS 卷 ID，
// 兼容三种输入：裸 vol-id、aws:///vol-id 以及 aws://zone/vol-id。
// 长期看这种转换应不再必要：
// https://github.com/kubernetes/kubernetes/issues/73730
// URL 形式最初把可用区放在 host 中，以便无需查询 API 即可取得；现在 PV 标签
// 已保存可用区，因此不再需要这一层。若未来支持不使用 PV 的多可用区卷感知，
// 可能仍需从 host 获取可用区。
func KubernetesVolumeIDToEBSVolumeID(kubernetesID string) (string, error) {

	// 没有 aws:// 前缀时视为裸卷 ID，直接返回。
	if !strings.HasPrefix(kubernetesID, "aws://") {
		return kubernetesID, nil
	}
	// 带 URL 前缀时按 URL 解析，并校验 scheme 是否为 aws。
	url, err := url.Parse(kubernetesID)
	if err != nil {
		// TODO: 考虑让 Volume 函数直接接收解析后的 URL。
		return "", fmt.Errorf("Invalid disk name (%s): %v", kubernetesID, err)
	}
	if url.Scheme != "aws" {
		return "", fmt.Errorf("Invalid scheme for AWS volume (%s)", kubernetesID)
	}

	// 取 URL 路径部分并去掉首尾斜杠，得到候选卷 ID。
	awsID := url.Path
	awsID = strings.Trim(awsID, "/")

	// 用正则做合法性校验，避免把畸形 ID 传给 CSI 驱动。
	if !awsVolumeRegMatch.MatchString(awsID) {
		return "", fmt.Errorf("Invalid format for AWS volume (%s)", kubernetesID)
	}

	return awsID, nil
}

// getAwsRegionFromZones 从一组可用区名称中推导出唯一的 AWS region；
// 供拓扑反向翻译时把 zone 映射回 region 使用。
// 标准可用区名称假定以单个字母结尾；形如 us-west-10 的名称会解析失败。
func getAwsRegionFromZones(zones []string) (string, error) {
	regions := sets.String{}
	if len(zones) < 1 {
		return "", fmt.Errorf("no zones specified")
	}

	// 依次解析每个 zone，利用字符串分段数区分标准区、GovCloud、local zone 与 wavelength 区。
	for _, zone := range zones {
		splitZone := strings.Split(zone, "-")
		if (len(splitZone) == 3 || len(splitZone) == 4) && len(splitZone[len(splitZone)-1]) == 2 {
			// 标准可用区：去掉末尾表示可用区的单个字母，剩下即为 region。
			splitZone[len(splitZone)-1] = splitZone[len(splitZone)-1][:1]
			regions.Insert(strings.Join(splitZone, "-"))
		} else if len(splitZone) == 5 || len(splitZone) == 7 {
			// local zone 或 wavelength 区：取前三段作为 region。
			regions.Insert(strings.Join(splitZone[:3], "-"))
		} else {
			return "", fmt.Errorf("Unexpected zone format: %v is not a valid AWS zone", zone)
		}
	}
	// 只有当所有 zone 都指向同一个 region 时才有唯一结果。
	if regions.Len() != 1 {
		return "", fmt.Errorf("multiple or no regions gotten from zones, got: %v", regions)
	}
	return regions.UnsortedList()[0], nil
}
