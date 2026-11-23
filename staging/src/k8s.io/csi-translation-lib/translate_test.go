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

package csitranslation

import (
	"fmt"
	"reflect"
	"testing"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/uuid"
	"k8s.io/csi-translation-lib/plugins"
	"k8s.io/klog/v2/ktesting"
	_ "k8s.io/klog/v2/ktesting/init"
)

var (
	// kubernetesBetaTopologyLabels 是 Beta 版本的 Kubernetes 拓扑标签（failure-domain.beta.kubernetes.io/*）。
	kubernetesBetaTopologyLabels = map[string]string{
		v1.LabelFailureDomainBetaZone:   "us-east-1a",
		v1.LabelFailureDomainBetaRegion: "us-east-1",
	}
	// kubernetesGATopologyLabels 是 GA 版本的 Kubernetes 拓扑标签（topology.kubernetes.io/*）。
	kubernetesGATopologyLabels = map[string]string{
		v1.LabelTopologyZone:   "us-east-1a",
		v1.LabelTopologyRegion: "us-east-1",
	}
	// regionalBetaPDLabels 用 Beta 标签表示跨可用区的区域级 PD（zone 值含两个可用区）。
	regionalBetaPDLabels = map[string]string{
		v1.LabelFailureDomainBetaZone: "europe-west1-b__europe-west1-c",
	}
	// regionalGAPDLabels 用 GA 标签表示跨可用区的区域级 PD。
	regionalGAPDLabels = map[string]string{
		v1.LabelTopologyZone: "europe-west1-b__europe-west1-c",
	}
)

// TestTranslationStability 验证翻译的"往返稳定性"：
// 树内 PV -> CSI PV -> 树内 PV 之后，得到的对象应与原始对象深度相等。
// 用例覆盖 GCE PD 与 AWS EBS 两种插件。
func TestTranslationStability(t *testing.T) {
	logger, _ := ktesting.NewTestContext(t)
	testCases := []struct {
		name string
		pv   *v1.PersistentVolume
	}{

		{
			name: "GCE PD PV Source",
			pv: &v1.PersistentVolume{
				Spec: v1.PersistentVolumeSpec{
					PersistentVolumeSource: v1.PersistentVolumeSource{
						GCEPersistentDisk: &v1.GCEPersistentDiskVolumeSource{
							PDName:    "test-disk",
							FSType:    "ext4",
							Partition: 0,
							ReadOnly:  false,
						},
					},
				},
			},
		},
		{
			name: "AWS EBS PV Source",
			pv: &v1.PersistentVolume{
				Spec: v1.PersistentVolumeSpec{
					PersistentVolumeSource: v1.PersistentVolumeSource{
						AWSElasticBlockStore: &v1.AWSElasticBlockStoreVolumeSource{
							VolumeID:  "vol01",
							FSType:    "ext3",
							Partition: 1,
							ReadOnly:  true,
						},
					},
				},
			},
		},
	}
	for _, test := range testCases {
		ctl := New()
		t.Logf("Testing %v", test.name)
		// 第一步：树内 PV 翻译为 CSI PV。
		csiSource, err := ctl.TranslateInTreePVToCSI(logger, test.pv)
		if err != nil {
			t.Errorf("Error when translating to CSI: %v", err)
		}
		// 第二步：再翻译回树内 PV。
		newPV, err := ctl.TranslateCSIPVToInTree(csiSource)
		if err != nil {
			t.Errorf("Error when translating CSI Source to in tree volume: %v", err)
		}
		// 第三步：深度比较，要求往返后与原对象完全一致。
		if !reflect.DeepEqual(newPV, test.pv) {
			t.Errorf("Volumes after translation and back not equal:\n\nOriginal Volume: %#v\n\nRound-trip Volume: %#v", test.pv, newPV)
		}
	}
}

// TestTopologyTranslation 验证拓扑信息在各插件翻译过程中的处理是否正确。
//
// 覆盖场景：
//   - Beta 与 GA 两种 Kubernetes 拓扑标签；
//   - PV 已有拓扑（beta key 或 CSI key）时的保留行为；
//   - 标签与已有拓扑同时存在时，以已有拓扑为准；
//   - 区域级 PD（多个 zone）与多 NodeSelectorTerm 场景；
//   - AWS EBS、OpenStack Cinder 主要验证拓扑 key 的替换。
//
// 翻译为 CSI 后应得到期望的 NodeAffinity；再翻译回树内时，
// CSI 特有的拓扑 key 应被移除，同时保留 Kubernetes 自身的拓扑标签。
func TestTopologyTranslation(t *testing.T) {
	logger, _ := ktesting.NewTestContext(t)
	// 用例结构：name 用例名；key 需要被清除的 CSI 拓扑 key（为空表示走另一分支）；
	// pv 输入；expectedNodeAffinity 翻译为 CSI 后期望的亲和性。
	testCases := []struct {
		name                 string
		key                  string
		pv                   *v1.PersistentVolume
		expectedNodeAffinity *v1.VolumeNodeAffinity
	}{
		{
			name:                 "GCE PD with beta zone labels",
			key:                  plugins.GCEPDTopologyKey,
			pv:                   makeGCEPDPV(kubernetesBetaTopologyLabels, nil /*topology*/),
			expectedNodeAffinity: makeNodeAffinity(false /*multiTerms*/, plugins.GCEPDTopologyKey, "us-east-1a"),
		},
		{
			name:                 "GCE PD with GA kubernetes zone labels",
			key:                  plugins.GCEPDTopologyKey,
			pv:                   makeGCEPDPV(kubernetesGATopologyLabels, nil /*topology*/),
			expectedNodeAffinity: makeNodeAffinity(false /*multiTerms*/, plugins.GCEPDTopologyKey, "us-east-1a"),
		},
		{
			name:                 "GCE PD with existing topology (beta keys)",
			pv:                   makeGCEPDPV(nil /*labels*/, makeTopology(v1.LabelFailureDomainBetaZone, "us-east-2a")),
			expectedNodeAffinity: makeNodeAffinity(false /*multiTerms*/, plugins.GCEPDTopologyKey, "us-east-2a"),
		},
		{
			name:                 "GCE PD with existing topology (CSI keys)",
			key:                  plugins.GCEPDTopologyKey,
			pv:                   makeGCEPDPV(nil /*labels*/, makeTopology(plugins.GCEPDTopologyKey, "us-east-2a")),
			expectedNodeAffinity: makeNodeAffinity(false /*multiTerms*/, plugins.GCEPDTopologyKey, "us-east-2a"),
		},
		{
			name:                 "GCE PD with zone labels and topology",
			pv:                   makeGCEPDPV(kubernetesBetaTopologyLabels, makeTopology(v1.LabelFailureDomainBetaZone, "us-east-2a")),
			expectedNodeAffinity: makeNodeAffinity(false /*multiTerms*/, plugins.GCEPDTopologyKey, "us-east-2a"),
		},
		{
			name:                 "GCE PD with regional zones",
			key:                  plugins.GCEPDTopologyKey,
			pv:                   makeGCEPDPV(regionalBetaPDLabels, nil /*topology*/),
			expectedNodeAffinity: makeNodeAffinity(false /*multiTerms*/, plugins.GCEPDTopologyKey, "europe-west1-b", "europe-west1-c"),
		},
		{
			name:                 "GCE PD with regional topology",
			key:                  plugins.GCEPDTopologyKey,
			pv:                   makeGCEPDPV(nil /*labels*/, makeTopology(v1.LabelTopologyZone, "europe-west1-b", "europe-west1-c")),
			expectedNodeAffinity: makeNodeAffinity(false /*multiTerms*/, plugins.GCEPDTopologyKey, "europe-west1-b", "europe-west1-c"),
		},
		{
			name:                 "GCE PD with Beta regional zone and topology",
			key:                  plugins.GCEPDTopologyKey,
			pv:                   makeGCEPDPV(regionalBetaPDLabels, makeTopology(v1.LabelFailureDomainBetaZone, "europe-west1-f", "europe-west1-g")),
			expectedNodeAffinity: makeNodeAffinity(false /*multiTerms*/, plugins.GCEPDTopologyKey, "europe-west1-f", "europe-west1-g"),
		},
		{
			name:                 "GCE PD with GA regional zone and topology",
			key:                  plugins.GCEPDTopologyKey,
			pv:                   makeGCEPDPV(regionalGAPDLabels, makeTopology(v1.LabelTopologyZone, "europe-west1-f", "europe-west1-g")),
			expectedNodeAffinity: makeNodeAffinity(false /*multiTerms*/, plugins.GCEPDTopologyKey, "europe-west1-f", "europe-west1-g"),
		},
		{
			name: "GCE PD with multiple node selector terms",
			key:  plugins.GCEPDTopologyKey,
			pv: makeGCEPDPVMultTerms(
				nil, /*labels*/
				makeTopology(v1.LabelTopologyZone, "europe-west1-f"),
				makeTopology(v1.LabelTopologyZone, "europe-west1-g")),
			expectedNodeAffinity: makeNodeAffinity(
				true, /*multiTerms*/
				plugins.GCEPDTopologyKey, "europe-west1-f", "europe-west1-g"),
		},
		// EBS test cases: test mostly topology key, i.e., don't repeat testing done with GCE
		// EBS 用例：主要验证拓扑 key，不重复 GCE 已覆盖的通用测试。
		{
			name:                 "AWS EBS with beta zone labels",
			pv:                   makeAWSEBSPV(kubernetesBetaTopologyLabels, nil /*topology*/),
			expectedNodeAffinity: makeNodeAffinity(false /*multiTerms*/, plugins.AWSEBSTopologyKey, "us-east-1a"),
		},
		{
			name:                 "AWS EBS with beta zone labels and topology",
			pv:                   makeAWSEBSPV(kubernetesBetaTopologyLabels, makeTopology(v1.LabelFailureDomainBetaZone, "us-east-2a")),
			expectedNodeAffinity: makeNodeAffinity(false /*multiTerms*/, plugins.AWSEBSTopologyKey, "us-east-2a"),
		},
		{
			name:                 "AWS EBS with GA zone labels",
			pv:                   makeAWSEBSPV(kubernetesGATopologyLabels, nil /*topology*/),
			expectedNodeAffinity: makeNodeAffinity(false /*multiTerms*/, plugins.AWSEBSTopologyKey, "us-east-1a"),
		},
		{
			name:                 "AWS EBS with GA zone labels and topology",
			pv:                   makeAWSEBSPV(kubernetesGATopologyLabels, makeTopology(v1.LabelTopologyZone, "us-east-2a")),
			expectedNodeAffinity: makeNodeAffinity(false /*multiTerms*/, plugins.AWSEBSTopologyKey, "us-east-2a"),
		},
		// Cinder test cases: test mosty topology key, i.e., don't repeat testing done with GCE
		// Cinder 用例：同样只验证拓扑 key。
		{
			name:                 "OpenStack Cinder with zone labels",
			pv:                   makeCinderPV(kubernetesBetaTopologyLabels, nil /*topology*/),
			expectedNodeAffinity: makeNodeAffinity(false /*multiTerms*/, plugins.CinderTopologyKey, "us-east-1a"),
		},
		{
			name:                 "OpenStack Cinder with zone labels and topology",
			pv:                   makeCinderPV(kubernetesBetaTopologyLabels, makeTopology(v1.LabelFailureDomainBetaZone, "us-east-2a")),
			expectedNodeAffinity: makeNodeAffinity(false /*multiTerms*/, plugins.CinderTopologyKey, "us-east-2a"),
		},
	}

	for _, test := range testCases {
		ctl := New()
		t.Logf("Testing %v", test.name)

		// Translate to CSI PV and check translated node affinity
		// 第一步：翻译为 CSI PV，并校验节点亲和性符合期望。
		newCSIPV, err := ctl.TranslateInTreePVToCSI(logger, test.pv)
		if err != nil {
			t.Errorf("Error when translating to CSI: %v", err)
		}

		nodeAffinity := newCSIPV.Spec.NodeAffinity
		if !reflect.DeepEqual(nodeAffinity, test.expectedNodeAffinity) {
			t.Errorf("Expected node affinity %v, got %v", *test.expectedNodeAffinity, *nodeAffinity)
		}

		// Translate back to in-tree and make sure node affinity has been removed
		// 第二步：翻译回树内 PV。
		newInTreePV, err := ctl.TranslateCSIPVToInTree(newCSIPV)
		if err != nil {
			t.Errorf("Error when translating to in-tree: %v", err)
		}

		// For now, non-pd cloud should stay the old behavior which is still have the CSI topology.
		// 第三步：按用例类型分别断言。
		// 对于指定 key 的用例（GCE/EBS/Cinder），CSI 拓扑 key 应被移除，
		// 同时 Kubernetes 自身的 beta 或 GA 拓扑标签应保留。
		if test.key != "" {
			nodeAffinity = newInTreePV.Spec.NodeAffinity
			if plugins.TopologyKeyExist(test.key, nodeAffinity) {
				t.Errorf("Expected node affinity key %v being removed, got %v", test.key, *nodeAffinity)
			}
			// verify that either beta or GA kubernetes topology key should exist
			if !(plugins.TopologyKeyExist(v1.LabelFailureDomainBetaZone, nodeAffinity) || plugins.TopologyKeyExist(v1.LabelTopologyZone, nodeAffinity)) {
				t.Errorf("Expected node affinity kubernetes topology label exist, got %v", *nodeAffinity)
			}
		} else {
			// 未指定 key 的用例（如已有 CSI 拓扑），保持原行为：仍保留 CSI 拓扑。
			nodeAffinity := newCSIPV.Spec.NodeAffinity
			if !reflect.DeepEqual(nodeAffinity, test.expectedNodeAffinity) {
				t.Errorf("Expected node affinity %v, got %v", *test.expectedNodeAffinity, *nodeAffinity)
			}
		}
	}
}

// makePV 构造一个基础 PV：设置给定的标签，并在 topology 非空时把它写入 NodeAffinity。
// 其余字段留空，由更具体的 make* 辅助函数补充卷源。
func makePV(labels map[string]string, topology *v1.NodeSelectorRequirement) *v1.PersistentVolume {
	pv := &v1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{
			Labels: labels,
		},
		Spec: v1.PersistentVolumeSpec{},
	}

	if topology != nil {
		pv.Spec.NodeAffinity = &v1.VolumeNodeAffinity{
			Required: &v1.NodeSelector{
				NodeSelectorTerms: []v1.NodeSelectorTerm{
					{MatchExpressions: []v1.NodeSelectorRequirement{*topology}},
				},
			},
		}
	}

	return pv
}

// makeGCEPDPV 在 makePV 的基础上填入 GCE PD 卷源，构造一个树内 GCE PD PV。
func makeGCEPDPV(labels map[string]string, topology *v1.NodeSelectorRequirement) *v1.PersistentVolume {
	pv := makePV(labels, topology)
	pv.Spec.PersistentVolumeSource = v1.PersistentVolumeSource{
		GCEPersistentDisk: &v1.GCEPersistentDiskVolumeSource{
			PDName:    "test-disk",
			FSType:    "ext4",
			Partition: 0,
			ReadOnly:  false,
		},
	}
	return pv
}

// makeGCEPDPVMultTerms 构造具有多个 NodeSelectorTerm 的 GCE PD PV。
// 第一个 topology 由 makeGCEPDPV 写入，其余作为额外的 term 追加。
func makeGCEPDPVMultTerms(labels map[string]string, topologies ...*v1.NodeSelectorRequirement) *v1.PersistentVolume {
	pv := makeGCEPDPV(labels, topologies[0])
	for _, topology := range topologies[1:] {
		pv.Spec.NodeAffinity.Required.NodeSelectorTerms = append(
			pv.Spec.NodeAffinity.Required.NodeSelectorTerms,
			v1.NodeSelectorTerm{
				MatchExpressions: []v1.NodeSelectorRequirement{*topology},
			},
		)
	}
	return pv
}

// makeAWSEBSPV 构造一个树内 AWS EBS PV，卷源包含 VolumeID、FSType、Partition、ReadOnly。
func makeAWSEBSPV(labels map[string]string, topology *v1.NodeSelectorRequirement) *v1.PersistentVolume {
	pv := makePV(labels, topology)
	pv.Spec.PersistentVolumeSource = v1.PersistentVolumeSource{
		AWSElasticBlockStore: &v1.AWSElasticBlockStoreVolumeSource{
			VolumeID:  "vol01",
			FSType:    "ext3",
			Partition: 1,
			ReadOnly:  true,
		},
	}
	return pv
}

// makeCinderPV 构造一个树内 OpenStack Cinder PV。
func makeCinderPV(labels map[string]string, topology *v1.NodeSelectorRequirement) *v1.PersistentVolume {
	pv := makePV(labels, topology)
	pv.Spec.PersistentVolumeSource = v1.PersistentVolumeSource{
		Cinder: &v1.CinderPersistentVolumeSource{
			VolumeID: "vol1",
			FSType:   "ext4",
			ReadOnly: false,
		},
	}
	return pv
}

// makeNodeAffinity 构造期望的 NodeAffinity，用于与翻译结果比对。
// multiTerms 为 false 时，所有 value 放在单个 term 的同一个 MatchExpression 中；
// 为 true 时，每个 value 各自占用一个独立的 term。
func makeNodeAffinity(multiTerms bool, key string, values ...string) *v1.VolumeNodeAffinity {
	nodeAffinity := &v1.VolumeNodeAffinity{
		Required: &v1.NodeSelector{
			NodeSelectorTerms: []v1.NodeSelectorTerm{
				{
					MatchExpressions: []v1.NodeSelectorRequirement{
						{
							Key:      key,
							Operator: v1.NodeSelectorOpIn,
							Values:   values,
						},
					},
				},
			},
		},
	}

	// If multiple terms is NOT requested, return a single term with all values
	if !multiTerms {
		return nodeAffinity
	}

	// Otherwise return multiple terms, each one with a single value
	nodeAffinity.Required.NodeSelectorTerms[0].MatchExpressions[0].Values = values[:1] // If values=[1,2,3], overwrite with [1]
	for _, value := range values[1:] {
		term := v1.NodeSelectorTerm{
			MatchExpressions: []v1.NodeSelectorRequirement{
				{
					Key:      key,
					Operator: v1.NodeSelectorOpIn,
					Values:   []string{value},
				},
			},
		}
		nodeAffinity.Required.NodeSelectorTerms = append(nodeAffinity.Required.NodeSelectorTerms, term)
	}

	return nodeAffinity
}

// makeTopology 用给定的 key 与 values 构造一个 Operator 为 In 的 NodeSelectorRequirement，
// 用于构造 PV 上已有的拓扑约束。
func makeTopology(key string, values ...string) *v1.NodeSelectorRequirement {
	return &v1.NodeSelectorRequirement{
		Key:      key,
		Operator: v1.NodeSelectorOpIn,
		Values:   values,
	}
}

// TestTranslateInTreeInlineVolumeToCSINameUniqueness 验证内联卷翻译时自动生成的 PV 名称足够唯一。
// 对每个已注册驱动，构造两个不同的随机卷源并翻译，要求得到的 PV 名称不重复，
// 否则在 Pod 中同时使用多个内联卷时会发生命名冲突。
func TestTranslateInTreeInlineVolumeToCSINameUniqueness(t *testing.T) {
	for driverName := range inTreePlugins {
		t.Run(driverName, func(t *testing.T) {
			logger, _ := ktesting.NewTestContext(t)
			ctl := New()
			vs1, err := generateUniqueVolumeSource(driverName)
			if err != nil {
				t.Fatalf("Couldn't generate random source: %v", err)
			}
			pv1, err := ctl.TranslateInTreeInlineVolumeToCSI(logger, &v1.Volume{
				VolumeSource: vs1,
			}, "")
			if err != nil {
				t.Fatalf("Error when translating to CSI: %v", err)
			}
			vs2, err := generateUniqueVolumeSource(driverName)
			if err != nil {
				t.Fatalf("Couldn't generate random source: %v", err)
			}
			pv2, err := ctl.TranslateInTreeInlineVolumeToCSI(logger, &v1.Volume{
				VolumeSource: vs2,
			}, "")
			if err != nil {
				t.Fatalf("Error when translating to CSI: %v", err)
			}
			if pv1 == nil || pv2 == nil {
				t.Fatalf("Did not expect either pv1: %v or pv2: %v to be nil", pv1, pv2)
			}
			if pv1.Name == pv2.Name {
				t.Errorf("PV name %s not sufficiently unique for different volumes", pv1.Name)
			}
		})

	}
}

// generateUniqueVolumeSource 根据 CSI 驱动名生成一个随机且唯一的 VolumeSource，
// 用于内联卷唯一性测试。随机值使用 UUID，确保每次调用得到的卷源不同。
// 未注册的驱动返回错误。
func generateUniqueVolumeSource(driverName string) (v1.VolumeSource, error) {
	switch driverName {
	case plugins.GCEPDDriverName:
		return v1.VolumeSource{
			GCEPersistentDisk: &v1.GCEPersistentDiskVolumeSource{
				PDName: string(uuid.NewUUID()),
			},
		}, nil
	case plugins.AWSEBSDriverName:
		return v1.VolumeSource{
			AWSElasticBlockStore: &v1.AWSElasticBlockStoreVolumeSource{
				VolumeID: string(uuid.NewUUID()),
			},
		}, nil

	case plugins.CinderDriverName:
		return v1.VolumeSource{
			Cinder: &v1.CinderVolumeSource{
				VolumeID: string(uuid.NewUUID()),
			},
		}, nil
	case plugins.AzureDiskDriverName:
		return v1.VolumeSource{
			AzureDisk: &v1.AzureDiskVolumeSource{
				DiskName:    string(uuid.NewUUID()),
				DataDiskURI: string(uuid.NewUUID()),
			},
		}, nil
	case plugins.AzureFileDriverName:
		return v1.VolumeSource{
			AzureFile: &v1.AzureFileVolumeSource{
				SecretName: string(uuid.NewUUID()),
				ShareName:  string(uuid.NewUUID()),
			},
		}, nil
	case plugins.VSphereDriverName:
		return v1.VolumeSource{
			VsphereVolume: &v1.VsphereVirtualDiskVolumeSource{
				VolumePath: " [vsanDatastore] 6785a85e-268e-6352-a2e8-02008b7afadd/kubernetes-dynamic-pvc-" + string(uuid.NewUUID()+".vmdk"),
				FSType:     "ext4",
			},
		}, nil
	case plugins.PortworxDriverName:
		return v1.VolumeSource{
			PortworxVolume: &v1.PortworxVolumeSource{
				VolumeID: string(uuid.NewUUID()),
			},
		}, nil
	default:
		return v1.VolumeSource{}, fmt.Errorf("couldn't find logic for driver: %v", driverName)
	}
}

// TestPluginNameMappings 验证树内插件名与 CSI 驱动名之间的双向映射一致且自洽：
// 从树内名查到 CSI 名后，反向查询应能得到原来的树内名，
// 且两个方向的"可迁移/已被取代"判定都应返回 true。
func TestPluginNameMappings(t *testing.T) {
	testCases := []struct {
		name             string
		inTreePluginName string
		csiPluginName    string
	}{
		{
			name:             "GCE PD plugin name",
			inTreePluginName: "kubernetes.io/gce-pd",
			csiPluginName:    "pd.csi.storage.gke.io",
		},
		{
			name:             "AWS EBS plugin name",
			inTreePluginName: "kubernetes.io/aws-ebs",
			csiPluginName:    "ebs.csi.aws.com",
		},
	}
	for _, test := range testCases {
		t.Logf("Testing %v", test.name)
		ctl := New()
		csiPluginName, err := ctl.GetCSINameFromInTreeName(test.inTreePluginName)
		if err != nil {
			t.Errorf("Error when mapping In-tree plugin name to CSI plugin name %s", err)
		}
		if !ctl.IsMigratedCSIDriverByName(csiPluginName) {
			t.Errorf("%s expected to supersede an In-tree plugin", csiPluginName)
		}
		inTreePluginName, err := ctl.GetInTreeNameFromCSIName(csiPluginName)
		if err != nil {
			t.Errorf("Error when mapping CSI plugin name to In-tree plugin name %s", err)
		}
		if !ctl.IsMigratableIntreePluginByName(inTreePluginName) {
			t.Errorf("%s expected to be migratable to a CSI name", inTreePluginName)
		}
		if inTreePluginName != test.inTreePluginName || csiPluginName != test.csiPluginName {
			t.Errorf("CSI plugin name and In-tree plugin name do not map to each other: [%s => %s], [%s => %s]", test.csiPluginName, inTreePluginName, test.inTreePluginName, csiPluginName)
		}
	}
}

// TODO: test for not modifying the original PV.
