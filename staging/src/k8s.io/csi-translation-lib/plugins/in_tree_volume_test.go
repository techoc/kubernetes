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
	"reflect"
	"testing"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

var (
	// useast1aGALabels 是一组 GA 版 Kubernetes 拓扑标签：us-east1 地域的 us-east1-a 可用区。
	useast1aGALabels = map[string]string{
		v1.LabelTopologyZone:   "us-east1-a",
		v1.LabelTopologyRegion: "us-east1",
	}
	// useast1aGANodeSelectorTermZoneFirst 是与上面标签等价的 GA NodeSelectorTerm，
	// 表达式顺序为 zone 在前、region 在后。
	useast1aGANodeSelectorTermZoneFirst = []v1.NodeSelectorTerm{
		{
			MatchExpressions: []v1.NodeSelectorRequirement{
				{
					Key:      v1.LabelTopologyZone,
					Operator: v1.NodeSelectorOpIn,
					Values:   []string{"us-east1-a"},
				},
				{
					Key:      v1.LabelTopologyRegion,
					Operator: v1.NodeSelectorOpIn,
					Values:   []string{"us-east1"},
				},
			},
		},
	}

	// useast1aGANodeSelectorTermRegionFirst 与上一变量取值相同，只是 region 表达式在前，
	// 用于验证翻译函数不会重排已有表达式的顺序。
	useast1aGANodeSelectorTermRegionFirst = []v1.NodeSelectorTerm{
		{
			MatchExpressions: []v1.NodeSelectorRequirement{
				{
					Key:      v1.LabelTopologyRegion,
					Operator: v1.NodeSelectorOpIn,
					Values:   []string{"us-east1"},
				},
				{
					Key:      v1.LabelTopologyZone,
					Operator: v1.NodeSelectorOpIn,
					Values:   []string{"us-east1-a"},
				},
			},
		},
	}

	// uswest2bBetaLabels 是一组 Beta 版 Kubernetes 拓扑标签：us-west2 地域的 us-west2-b 可用区。
	uswest2bBetaLabels = map[string]string{
		v1.LabelFailureDomainBetaZone:   "us-west2-b",
		v1.LabelFailureDomainBetaRegion: "us-west2",
	}

	// uswest2bBetaNodeSelectorTermZoneFirst 是 Beta 版拓扑对应的 NodeSelectorTerm，zone 在前。
	uswest2bBetaNodeSelectorTermZoneFirst = []v1.NodeSelectorTerm{
		{
			MatchExpressions: []v1.NodeSelectorRequirement{
				{
					Key:      v1.LabelFailureDomainBetaZone,
					Operator: v1.NodeSelectorOpIn,
					Values:   []string{"us-west2-b"},
				},
				{
					Key:      v1.LabelFailureDomainBetaRegion,
					Operator: v1.NodeSelectorOpIn,
					Values:   []string{"us-west2"},
				},
			},
		},
	}

	// uswest2bBetaNodeSelectorTermRegionFirst 是 Beta 版的等价 term，region 在前，
	// 同样用于校验表达式顺序保持不变。
	uswest2bBetaNodeSelectorTermRegionFirst = []v1.NodeSelectorTerm{
		{
			MatchExpressions: []v1.NodeSelectorRequirement{
				{
					Key:      v1.LabelFailureDomainBetaRegion,
					Operator: v1.NodeSelectorOpIn,
					Values:   []string{"us-west2"},
				},
				{
					Key:      v1.LabelFailureDomainBetaZone,
					Operator: v1.NodeSelectorOpIn,
					Values:   []string{"us-west2-b"},
				},
			},
		},
	}
)

// TestTranslateTopologyFromCSIToInTree 测试 CSI → in-tree 的拓扑回退翻译。
// 覆盖点包括：CSI Key 替换、GA/Beta 标签保留、从标签或 NodeAffinity 取拓扑、
// region 解析器的有无、多 term 处理以及 cinder 插件场景。
func TestTranslateTopologyFromCSIToInTree(t *testing.T) {
	// 表驱动用例：key 为待替换的 CSI 拓扑键，regionParser 可选，
	// expectedNodeSelectorTerms 与 expectedLabels 分别是期望的 NodeAffinity 与标签。
	testCases := []struct {
		name                      string
		key                       string
		expErr                    bool
		regionParser              regionParserFn
		pv                        *v1.PersistentVolume
		expectedNodeSelectorTerms []v1.NodeSelectorTerm
		expectedLabels            map[string]string
	}{
		// 用例1：PV 已有 GA 版 zone/region 拓扑且带 CSI Key。
		// 输入：region 表达式在前、CSI zone Key 在后，标签为 GA 版。
		// 断言：CSI Key 被换成 GA zone Key，region 与标签均保持不变。
		{
			name:         "Remove CSI Topology Key and do not change existing GA Kubernetes topology",
			key:          GCEPDTopologyKey,
			expErr:       false,
			regionParser: gceGetRegionFromZones,
			pv: &v1.PersistentVolume{
				ObjectMeta: metav1.ObjectMeta{
					Name: "gcepd", Namespace: "myns",
					Labels: useast1aGALabels,
				},
				Spec: v1.PersistentVolumeSpec{
					NodeAffinity: &v1.VolumeNodeAffinity{
						Required: &v1.NodeSelector{
							NodeSelectorTerms: []v1.NodeSelectorTerm{
								{
									MatchExpressions: []v1.NodeSelectorRequirement{
										{
											Key:      v1.LabelTopologyRegion,
											Operator: v1.NodeSelectorOpIn,
											Values:   []string{"us-east1"},
										},
										{
											Key:      GCEPDTopologyKey,
											Operator: v1.NodeSelectorOpIn,
											Values:   []string{"us-east1-a"},
										},
									},
								},
							},
						},
					},
				},
			},
			expectedNodeSelectorTerms: useast1aGANodeSelectorTermRegionFirst,
			expectedLabels:            useast1aGALabels,
		},
		// 用例2：与用例1相同，但现有拓扑是 Beta 版。
		// 断言：CSI Key 被换成 Beta zone Key，Beta region 与 Beta 标签原样保留。
		{
			name:         "Remove CSI Topology Key and do not change existing Beta Kubernetes topology",
			key:          GCEPDTopologyKey,
			expErr:       false,
			regionParser: gceGetRegionFromZones,
			pv: &v1.PersistentVolume{
				ObjectMeta: metav1.ObjectMeta{
					Name: "gcepd", Namespace: "myns",
					Labels: uswest2bBetaLabels,
				},
				Spec: v1.PersistentVolumeSpec{
					NodeAffinity: &v1.VolumeNodeAffinity{
						Required: &v1.NodeSelector{
							NodeSelectorTerms: []v1.NodeSelectorTerm{
								{
									MatchExpressions: []v1.NodeSelectorRequirement{
										{
											Key:      v1.LabelFailureDomainBetaRegion,
											Operator: v1.NodeSelectorOpIn,
											Values:   []string{"us-west2"},
										},
										{
											Key:      GCEPDTopologyKey,
											Operator: v1.NodeSelectorOpIn,
											Values:   []string{"us-west2-b"},
										},
									},
								},
							},
						},
					},
				},
			},
			expectedNodeSelectorTerms: uswest2bBetaNodeSelectorTermRegionFirst,
			expectedLabels:            uswest2bBetaLabels,
		},
		// 用例3：NodeAffinity 里只有 CSI Key，但 PV 标签上已有 zone/region。
		// 断言：以 NodeAffinity 为准生成 GA 拓扑（zone + 由解析器推导的 region），
		// 标签中已有的 zone/region 值不被覆盖。
		{
			name:         "Remove CSI Topology Key and add Kubernetes topology from NodeAffinity, ignore labels",
			key:          GCEPDTopologyKey,
			expErr:       false,
			regionParser: gceGetRegionFromZones,
			pv: &v1.PersistentVolume{
				ObjectMeta: metav1.ObjectMeta{
					Name: "gcepd", Namespace: "myns",
					Labels: map[string]string{
						v1.LabelTopologyZone:   "existingZone",
						v1.LabelTopologyRegion: "existingRegion",
					},
				},
				Spec: v1.PersistentVolumeSpec{
					NodeAffinity: &v1.VolumeNodeAffinity{
						Required: &v1.NodeSelector{
							NodeSelectorTerms: []v1.NodeSelectorTerm{
								{
									MatchExpressions: []v1.NodeSelectorRequirement{
										{
											Key:      GCEPDTopologyKey,
											Operator: v1.NodeSelectorOpIn,
											Values:   []string{"us-east1-a"},
										},
									},
								},
							},
						},
					},
				},
			},
			expectedNodeSelectorTerms: useast1aGANodeSelectorTermZoneFirst,
			expectedLabels: map[string]string{
				v1.LabelTopologyRegion: "existingRegion",
				v1.LabelTopologyZone:   "existingZone",
			},
		},
		// 用例4：NodeAffinity 中没有任何表达式，仅 PV 标签带有旧拓扑。
		// 断言：没有 CSI Key 可替换时，NodeAffinity 与标签都保持原样。
		{
			name:         "No CSI topology label exists and no change to the NodeAffinity",
			key:          GCEPDTopologyKey,
			expErr:       false,
			regionParser: gceGetRegionFromZones,
			pv: &v1.PersistentVolume{
				ObjectMeta: metav1.ObjectMeta{
					Name: "gcepd", Namespace: "myns",
					Labels: map[string]string{
						v1.LabelTopologyZone:   "existingZone",
						v1.LabelTopologyRegion: "existingRegion",
					},
				},
				Spec: v1.PersistentVolumeSpec{
					NodeAffinity: &v1.VolumeNodeAffinity{
						Required: &v1.NodeSelector{
							NodeSelectorTerms: []v1.NodeSelectorTerm{},
						},
					},
				},
			},
			expectedNodeSelectorTerms: []v1.NodeSelectorTerm{},
			expectedLabels: map[string]string{
				v1.LabelTopologyZone:   "existingZone",
				v1.LabelTopologyRegion: "existingRegion",
			},
		},
		// 用例5：PV 既无标签也无 Kubernetes 拓扑，只有 CSI zone Key。
		// 断言：从 CSI 拓扑生成完整的 GA 表达式，并补齐 GA zone/region 标签。
		{
			name:         "Generate GA labels and kubernetes topology only from CSI topology",
			key:          GCEPDTopologyKey,
			expErr:       false,
			regionParser: gceGetRegionFromZones,
			pv: &v1.PersistentVolume{
				ObjectMeta: metav1.ObjectMeta{
					Name: "gcepd", Namespace: "myns",
				},
				Spec: v1.PersistentVolumeSpec{
					NodeAffinity: &v1.VolumeNodeAffinity{
						Required: &v1.NodeSelector{
							NodeSelectorTerms: []v1.NodeSelectorTerm{
								{
									MatchExpressions: []v1.NodeSelectorRequirement{
										{
											Key:      GCEPDTopologyKey,
											Operator: v1.NodeSelectorOpIn,
											Values:   []string{"us-east1-a"},
										},
									},
								},
							},
						},
					},
				},
			},
			expectedNodeSelectorTerms: useast1aGANodeSelectorTermZoneFirst,
			expectedLabels:            useast1aGALabels,
		},
		// 用例6：NodeAffinity 中已有 Beta zone 拓扑，但没有 CSI Key。
		// 断言：保留 Beta 表达式，并据此推导出 Beta zone/region 标签。
		{
			name:         "Generate Beta labels and kubernetes topology from Beta NodeAffinity",
			key:          GCEPDTopologyKey,
			expErr:       false,
			regionParser: gceGetRegionFromZones,
			pv: &v1.PersistentVolume{
				ObjectMeta: metav1.ObjectMeta{
					Name: "gcepd", Namespace: "myns",
				},
				Spec: v1.PersistentVolumeSpec{
					NodeAffinity: &v1.VolumeNodeAffinity{
						Required: &v1.NodeSelector{
							NodeSelectorTerms: []v1.NodeSelectorTerm{
								{
									MatchExpressions: []v1.NodeSelectorRequirement{
										{
											Key:      v1.LabelFailureDomainBetaZone,
											Operator: v1.NodeSelectorOpIn,
											Values:   []string{"us-west2-b"},
										},
									},
								},
							},
						},
					},
				},
			},
			expectedNodeSelectorTerms: uswest2bBetaNodeSelectorTermZoneFirst,
			expectedLabels:            uswest2bBetaLabels,
		},
		// 用例7：不传 regionParser。
		// 断言：CSI Key 仍会换成 zone 键并生成 zone 标签，但不会推导 region 表达式或标签。
		{
			name:   "regionParser is missing and only zone labels get generated",
			key:    GCEPDTopologyKey,
			expErr: false,
			pv: &v1.PersistentVolume{
				ObjectMeta: metav1.ObjectMeta{
					Name: "gcepd", Namespace: "myns",
				},
				Spec: v1.PersistentVolumeSpec{
					NodeAffinity: &v1.VolumeNodeAffinity{
						Required: &v1.NodeSelector{
							NodeSelectorTerms: []v1.NodeSelectorTerm{
								{
									MatchExpressions: []v1.NodeSelectorRequirement{
										{
											Key:      GCEPDTopologyKey,
											Operator: v1.NodeSelectorOpIn,
											Values:   []string{"us-east1-a"},
										},
									},
								},
							},
						},
					},
				},
			},
			expectedNodeSelectorTerms: []v1.NodeSelectorTerm{
				{
					MatchExpressions: []v1.NodeSelectorRequirement{
						{
							Key:      v1.LabelTopologyZone,
							Operator: v1.NodeSelectorOpIn,
							Values:   []string{"us-east1-a"},
						},
					},
				},
			},
			expectedLabels: map[string]string{
				v1.LabelTopologyZone: "us-east1-a",
			},
		},
		// 用例8：NodeAffinity 含两个 term，各自带不同 zone 的 CSI Key。
		// 断言：每个 term 都完成 Key 替换与 region 推导；zone 标签用 "__" 拼接多个值。
		{
			name:         "Replace multi-term CSI Topology Key and add Region Kubernetes topology for both",
			key:          GCEPDTopologyKey,
			expErr:       false,
			regionParser: gceGetRegionFromZones,
			pv: &v1.PersistentVolume{
				ObjectMeta: metav1.ObjectMeta{
					Name: "gcepd", Namespace: "myns",
				},
				Spec: v1.PersistentVolumeSpec{
					NodeAffinity: &v1.VolumeNodeAffinity{
						Required: &v1.NodeSelector{
							NodeSelectorTerms: []v1.NodeSelectorTerm{
								{
									MatchExpressions: []v1.NodeSelectorRequirement{
										{
											Key:      GCEPDTopologyKey,
											Operator: v1.NodeSelectorOpIn,
											Values:   []string{"us-east1-a"},
										},
									},
								},
								{
									MatchExpressions: []v1.NodeSelectorRequirement{
										{
											Key:      GCEPDTopologyKey,
											Operator: v1.NodeSelectorOpIn,
											Values:   []string{"us-east1-c"},
										},
									},
								},
							},
						},
					},
				},
			},
			expectedNodeSelectorTerms: []v1.NodeSelectorTerm{
				{
					MatchExpressions: []v1.NodeSelectorRequirement{
						{
							Key:      v1.LabelTopologyZone,
							Operator: v1.NodeSelectorOpIn,
							Values:   []string{"us-east1-a"},
						},
						{
							Key:      v1.LabelTopologyRegion,
							Operator: v1.NodeSelectorOpIn,
							Values:   []string{"us-east1"},
						},
					},
				},
				{
					MatchExpressions: []v1.NodeSelectorRequirement{
						{
							Key:      v1.LabelTopologyZone,
							Operator: v1.NodeSelectorOpIn,
							Values:   []string{"us-east1-c"},
						},
						{
							Key:      v1.LabelTopologyRegion,
							Operator: v1.NodeSelectorOpIn,
							Values:   []string{"us-east1"},
						},
					},
				},
			},
			expectedLabels: map[string]string{
				v1.LabelTopologyZone:   "us-east1-a__us-east1-c",
				v1.LabelTopologyRegion: "us-east1",
			},
		},
		// 用例9：cinder 插件，拓扑键为 CinderTopologyKey 且不提供 regionParser。
		// 断言：CSI 键被换成 GA zone 键，只生成 zone 标签，不生成 region。
		{
			name:         "cinder translation",
			key:          CinderTopologyKey,
			expErr:       false,
			regionParser: nil,
			pv: &v1.PersistentVolume{
				ObjectMeta: metav1.ObjectMeta{
					Name: "cinder", Namespace: "myns",
				},
				Spec: v1.PersistentVolumeSpec{
					NodeAffinity: &v1.VolumeNodeAffinity{
						Required: &v1.NodeSelector{
							NodeSelectorTerms: []v1.NodeSelectorTerm{
								{
									MatchExpressions: []v1.NodeSelectorRequirement{
										{
											Key:      CinderTopologyKey,
											Operator: v1.NodeSelectorOpIn,
											Values:   []string{"nova"},
										},
									},
								},
							},
						},
					},
				},
			},
			expectedNodeSelectorTerms: []v1.NodeSelectorTerm{
				{
					MatchExpressions: []v1.NodeSelectorRequirement{
						{
							Key:      v1.LabelTopologyZone,
							Operator: v1.NodeSelectorOpIn,
							Values:   []string{"nova"},
						},
					},
				},
			},
			expectedLabels: map[string]string{
				v1.LabelTopologyZone: "nova",
			},
		},
	}

	// 逐用例执行翻译，并分别校验错误预期、NodeSelectorTerms 与 PV 标签。
	for _, tc := range testCases {
		t.Logf("Running test: %v", tc.name)
		err := translateTopologyFromCSIToInTree(tc.pv, tc.key, tc.regionParser)
		// 错误预期与实际结果双向比对：多出错误或缺少错误都算失败。
		if err != nil && !tc.expErr {
			t.Errorf("Did not expect an error, got: %v", err)
		}
		if err == nil && tc.expErr {
			t.Errorf("Expected an error but did not get one")
		}

		// 深比较变换后的 NodeSelectorTerms，验证 Key 替换、region 推导和表达式顺序。
		if !reflect.DeepEqual(tc.pv.Spec.NodeAffinity.Required.NodeSelectorTerms, tc.expectedNodeSelectorTerms) {
			t.Errorf("Expected topology: %v, but got: %v", tc.expectedNodeSelectorTerms, tc.pv.Spec.NodeAffinity.Required.NodeSelectorTerms)
		}
		if !reflect.DeepEqual(tc.pv.Labels, tc.expectedLabels) {
			t.Errorf("Expected labels: %v, but got: %v", tc.expectedLabels, tc.pv.Labels)
		}
	}
}

// TestTranslateTopologyFromInTreeToCSI 测试 in-tree → CSI 的拓扑翻译。
// 输入为带 GA 或 Beta 拓扑的 PV，断言 zone 键被替换为 CSI 拓扑键，
// 并且 Beta 版 region 标签会升级为 GA 版。
func TestTranslateTopologyFromInTreeToCSI(t *testing.T) {
	// 表驱动用例：key 为目标 CSI 拓扑键，expectedNodeSelectorTerms 为期望的最终表达式。
	testCases := []struct {
		name                      string
		key                       string
		expErr                    bool
		pv                        *v1.PersistentVolume
		expectedNodeSelectorTerms []v1.NodeSelectorTerm
	}{
		// 用例1：GA 版 zone 在前的拓扑。
		// 断言：zone 键变为 GCEPDTopologyKey，GA region 表达式保持不动。
		{
			name:   "Replace GA Kubernetes Zone Topology to GCE CSI Topology",
			key:    GCEPDTopologyKey,
			expErr: false,
			pv: &v1.PersistentVolume{
				ObjectMeta: metav1.ObjectMeta{
					Name: "gcepd", Namespace: "myns",
					Labels: useast1aGALabels,
				},
				Spec: v1.PersistentVolumeSpec{
					NodeAffinity: &v1.VolumeNodeAffinity{
						Required: &v1.NodeSelector{
							NodeSelectorTerms: useast1aGANodeSelectorTermZoneFirst,
						},
					},
				},
			},
			expectedNodeSelectorTerms: []v1.NodeSelectorTerm{
				{
					MatchExpressions: []v1.NodeSelectorRequirement{
						{
							Key:      GCEPDTopologyKey,
							Operator: v1.NodeSelectorOpIn,
							Values:   []string{"us-east1-a"},
						},
						{
							Key:      v1.LabelTopologyRegion,
							Operator: v1.NodeSelectorOpIn,
							Values:   []string{"us-east1"},
						},
					},
				},
			},
		},
		// 用例2：Beta 版 zone/region 拓扑。
		// 断言：zone 键变为 CSI 拓扑键，同时 Beta region 键升级为 GA 的 LabelTopologyRegion。
		{
			name:   "Replace Beta Kubernetes Topology to GCE CSI Topology and upgrade region label",
			key:    GCEPDTopologyKey,
			expErr: false,
			pv: &v1.PersistentVolume{
				ObjectMeta: metav1.ObjectMeta{
					Name: "gcepd", Namespace: "myns",
					Labels: useast1aGALabels,
				},
				Spec: v1.PersistentVolumeSpec{
					NodeAffinity: &v1.VolumeNodeAffinity{
						Required: &v1.NodeSelector{
							NodeSelectorTerms: uswest2bBetaNodeSelectorTermZoneFirst,
						},
					},
				},
			},
			expectedNodeSelectorTerms: []v1.NodeSelectorTerm{
				{
					MatchExpressions: []v1.NodeSelectorRequirement{
						{
							Key:      GCEPDTopologyKey,
							Operator: v1.NodeSelectorOpIn,
							Values:   []string{"us-west2-b"},
						},
						{
							Key:      v1.LabelTopologyRegion,
							Operator: v1.NodeSelectorOpIn,
							Values:   []string{"us-west2"},
						},
					},
				},
			},
		},
	}
	// 逐用例执行翻译，核对错误预期与最终的 NodeSelectorTerms。
	for _, tc := range testCases {
		t.Logf("Running test: %v", tc.name)
		err := translateTopologyFromInTreeToCSI(tc.pv, tc.key)
		// 预期不报错却报错、或预期报错却成功，都视为用例失败。
		if err != nil && !tc.expErr {
			t.Errorf("Did not expect an error, got: %v", err)
		}
		if err == nil && tc.expErr {
			t.Errorf("Expected an error but did not get one")
		}

		if !reflect.DeepEqual(tc.pv.Spec.NodeAffinity.Required.NodeSelectorTerms, tc.expectedNodeSelectorTerms) {
			t.Errorf("Expected topology: %v, but got: %v", tc.expectedNodeSelectorTerms, tc.pv.Spec.NodeAffinity.Required.NodeSelectorTerms)
		}
	}
}

// TestTranslateAllowedTopologies 测试 allowedTopologies 中旧 zone 键到 CSI 拓扑键的翻译。
// 分别验证：已是 CSI 键时不改动、Beta 键被替换、同一 term 内混合键各自处理、无关键原样透传。
func TestTranslateAllowedTopologies(t *testing.T) {
	// 表驱动用例：topology 为输入选择器，expectedToplogy 为期望输出。
	testCases := []struct {
		name            string
		topology        []v1.TopologySelectorTerm
		expectedToplogy []v1.TopologySelectorTerm
	}{
		// 用例1：输入已是 CSI 拓扑键，不应发生任何变化。
		{
			name:     "no translation",
			topology: generateToplogySelectors(GCEPDTopologyKey, []string{"foo", "bar"}),
			expectedToplogy: []v1.TopologySelectorTerm{
				{
					MatchLabelExpressions: []v1.TopologySelectorLabelRequirement{
						{
							Key:    GCEPDTopologyKey,
							Values: []string{"foo", "bar"},
						},
					},
				},
			},
		},
		// 用例2：输入为 Beta failure-domain zone 键。
		// 断言：键被替换为 GCEPDTopologyKey，Values 原样保留。
		{
			name: "translate",
			topology: []v1.TopologySelectorTerm{
				{
					MatchLabelExpressions: []v1.TopologySelectorLabelRequirement{
						{
							Key:    "failure-domain.beta.kubernetes.io/zone",
							Values: []string{"foo", "bar"},
						},
					},
				},
			},
			expectedToplogy: []v1.TopologySelectorTerm{
				{
					MatchLabelExpressions: []v1.TopologySelectorLabelRequirement{
						{
							Key:    GCEPDTopologyKey,
							Values: []string{"foo", "bar"},
						},
					},
				},
			},
		},
		// 用例3：同一 term 内同时含 Beta zone 键与 CSI 键。
		// 断言：只替换 Beta 键，已有的 CSI 键保持不变，两个表达式都保留。
		{
			name: "combo",
			topology: []v1.TopologySelectorTerm{
				{
					MatchLabelExpressions: []v1.TopologySelectorLabelRequirement{
						{
							Key:    "failure-domain.beta.kubernetes.io/zone",
							Values: []string{"foo", "bar"},
						},
						{
							Key:    GCEPDTopologyKey,
							Values: []string{"boo", "baz"},
						},
					},
				},
			},
			expectedToplogy: []v1.TopologySelectorTerm{
				{
					MatchLabelExpressions: []v1.TopologySelectorLabelRequirement{
						{
							Key:    GCEPDTopologyKey,
							Values: []string{"foo", "bar"},
						},
						{
							Key:    GCEPDTopologyKey,
							Values: []string{"boo", "baz"},
						},
					},
				},
			},
		},
		// 用例4：输入为与拓扑无关的普通键 "test"。
		// 断言：非 zone 键不属于翻译范围，原样透传。
		{
			name: "some other key",
			topology: []v1.TopologySelectorTerm{
				{
					MatchLabelExpressions: []v1.TopologySelectorLabelRequirement{
						{
							Key:    "test",
							Values: []string{"foo", "bar"},
						},
					},
				},
			},
			expectedToplogy: []v1.TopologySelectorTerm{
				{
					MatchLabelExpressions: []v1.TopologySelectorLabelRequirement{
						{
							Key:    "test",
							Values: []string{"foo", "bar"},
						},
					},
				},
			},
		},
	}

	// 逐用例执行翻译，先确认无错误，再深比较输出的选择器。
	for _, tc := range testCases {
		t.Logf("Running test: %v", tc.name)
		gotTop, err := translateAllowedTopologies(tc.topology, GCEPDTopologyKey)
		// 该函数当前实现不会返回错误，出现错误即为异常。
		if err != nil {
			t.Errorf("Unexpected error: %v", err)
		}

		if !reflect.DeepEqual(gotTop, tc.expectedToplogy) {
			t.Errorf("Expected topology: %v, but got: %v", tc.expectedToplogy, gotTop)
		}
	}
}

// TestAddTopology 测试向空 PV 追加 CSI 拓扑的行为。
// 覆盖：空 zones、全空白 zones 应报错；含空白项时应过滤；
// 以及无序输入应被排序后写入 NodeAffinity。
func TestAddTopology(t *testing.T) {
	// 表驱动用例：expErr 表示期望 addTopology 返回错误，expectedAffinity 为成功时的期望结果。
	testCases := []struct {
		name             string
		topologyKey      string
		zones            []string
		expErr           bool
		expectedAffinity *v1.VolumeNodeAffinity
	}{
		// 用例1：zones 为 nil，清洗后没有任何有效值，期望返回错误。
		{
			name:        "empty zones",
			topologyKey: GCEPDTopologyKey,
			zones:       nil,
			expErr:      true,
		},
		// 用例2：zones 全部是空白字符（空格、换行、制表符），TrimSpace 后为空，期望报错。
		{
			name:        "only whitespace-named zones",
			topologyKey: GCEPDTopologyKey,
			zones:       []string{" ", "\n", "\t", "  "},
			expErr:      true,
		},
		// 用例3：zones 同时含空白项与有效 zone。
		// 断言：空白项被丢弃，只把有效 zone 写入 NodeAffinity，不返回错误。
		{
			name:        "including whitespace-named zones",
			topologyKey: GCEPDTopologyKey,
			zones:       []string{" ", "us-central1-a"},
			expErr:      false,
			expectedAffinity: &v1.VolumeNodeAffinity{
				Required: &v1.NodeSelector{
					NodeSelectorTerms: []v1.NodeSelectorTerm{
						{
							MatchExpressions: []v1.NodeSelectorRequirement{
								{
									Key:      GCEPDTopologyKey,
									Operator: v1.NodeSelectorOpIn,
									Values:   []string{"us-central1-a"},
								},
							},
						},
					},
				},
			},
		},
		// 用例4：zones 乱序输入。
		// 断言：写入前按字典序排序，保证 NodeAffinity 内容稳定可比较。
		{
			name:        "unsorted zones",
			topologyKey: GCEPDTopologyKey,
			zones:       []string{"us-central1-f", "us-central1-a", "us-central1-c", "us-central1-b"},
			expErr:      false,
			expectedAffinity: &v1.VolumeNodeAffinity{
				Required: &v1.NodeSelector{
					NodeSelectorTerms: []v1.NodeSelectorTerm{
						{
							MatchExpressions: []v1.NodeSelectorRequirement{
								{
									Key:      GCEPDTopologyKey,
									Operator: v1.NodeSelectorOpIn,
									// Values are expected to be ordered
									Values: []string{"us-central1-a", "us-central1-b", "us-central1-c", "us-central1-f"},
								},
							},
						},
					},
				},
			},
		},
	}

	// 每个用例都从一个没有任何 NodeAffinity 的空 PV 开始，验证 addTopology 能按需补齐结构。
	for _, tc := range testCases {
		t.Logf("Running test: %v", tc.name)
		pv := &v1.PersistentVolume{
			Spec: v1.PersistentVolumeSpec{},
		}
		err := addTopology(pv, tc.topologyKey, tc.zones)
		// 先校验错误预期；只有成功时才进一步比较生成的 NodeAffinity。
		if err != nil && !tc.expErr {
			t.Errorf("Did not expect an error, got: %v", err)
		}
		if err == nil && tc.expErr {
			t.Errorf("Expected an error but did not get one")
		}
		if err == nil && !reflect.DeepEqual(pv.Spec.NodeAffinity, tc.expectedAffinity) {
			t.Errorf("Expected affinity: %v, but got: %v", tc.expectedAffinity, pv.Spec.NodeAffinity)
		}
	}
}

// TestReplaceTopology 测试 replaceTopology 仅改写 Key 的行为。
// 覆盖：单个 term 替换、未找到 Key 时不改动、多 term 全部替换，
// 以及同一 term 中已存在 newKey 时不做值合并（允许 Key 重复）。
func TestReplaceTopology(t *testing.T) {
	// 表驱动用例：oldKey/newKey 为替换前后的键，expOk 表示是否期望发生替换。
	testCases := []struct {
		name             string
		oldKey           string
		newKey           string
		pv               *v1.PersistentVolume
		expOk            bool
		expectedAffinity *v1.VolumeNodeAffinity
	}{
		// 用例1：单个 term 中只有一个 CSI Key。
		// 断言：Key 被替换为 GA zone 键，Values 与 Operator 不变。
		{
			name:   "Replace single csi topology from PV",
			oldKey: GCEPDTopologyKey,
			newKey: v1.LabelTopologyZone,
			pv: makePVWithNodeSelectorTerms([]v1.NodeSelectorTerm{
				{
					MatchExpressions: []v1.NodeSelectorRequirement{
						{
							Key:      GCEPDTopologyKey,
							Operator: v1.NodeSelectorOpIn,
							Values:   []string{"us-east1-a"},
						},
					},
				},
			}),
			expOk: true,
			expectedAffinity: &v1.VolumeNodeAffinity{
				Required: &v1.NodeSelector{
					NodeSelectorTerms: []v1.NodeSelectorTerm{
						{
							MatchExpressions: []v1.NodeSelectorRequirement{
								{
									Key:      v1.LabelTopologyZone,
									Operator: v1.NodeSelectorOpIn,
									Values:   []string{"us-east1-a"},
								},
							},
						},
					},
				},
			},
		},
		// 用例2：term 中只有 GA zone 键，没有待替换的 CSI Key。
		// 断言：不做任何改动，NodeAffinity 与输入完全一致。
		{
			name:   "Not found the topology key so do nothing",
			oldKey: GCEPDTopologyKey,
			newKey: v1.LabelTopologyZone,
			pv: makePVWithNodeSelectorTerms([]v1.NodeSelectorTerm{
				{
					MatchExpressions: []v1.NodeSelectorRequirement{
						{
							Key:      v1.LabelTopologyZone,
							Operator: v1.NodeSelectorOpIn,
							Values:   []string{"us-east1-a"},
						},
					},
				},
			}),
			expOk: false,
			expectedAffinity: &v1.VolumeNodeAffinity{
				Required: &v1.NodeSelector{
					NodeSelectorTerms: []v1.NodeSelectorTerm{
						{
							MatchExpressions: []v1.NodeSelectorRequirement{
								{
									Key:      v1.LabelTopologyZone,
									Operator: v1.NodeSelectorOpIn,
									Values:   []string{"us-east1-a"},
								},
							},
						},
					},
				},
			},
		},
		// 用例3：两个 term 各带一个 CSI Key。
		// 断言：两个 term 中的 Key 都被替换，各自 Values 保持独立。
		{
			name:   "Replace the topology key from multiple terms",
			oldKey: GCEPDTopologyKey,
			newKey: v1.LabelTopologyZone,
			pv: makePVWithNodeSelectorTerms([]v1.NodeSelectorTerm{
				{
					MatchExpressions: []v1.NodeSelectorRequirement{
						{
							Key:      GCEPDTopologyKey,
							Operator: v1.NodeSelectorOpIn,
							Values:   []string{"us-east1-a"},
						},
					},
				},
				{
					MatchExpressions: []v1.NodeSelectorRequirement{
						{
							Key:      GCEPDTopologyKey,
							Operator: v1.NodeSelectorOpIn,
							Values:   []string{"us-east1-c"},
						},
					},
				},
			}),
			expOk: true,
			expectedAffinity: &v1.VolumeNodeAffinity{
				Required: &v1.NodeSelector{
					NodeSelectorTerms: []v1.NodeSelectorTerm{
						{
							MatchExpressions: []v1.NodeSelectorRequirement{
								{
									Key:      v1.LabelTopologyZone,
									Operator: v1.NodeSelectorOpIn,
									Values:   []string{"us-east1-a"},
								},
							},
						},
						{
							MatchExpressions: []v1.NodeSelectorRequirement{
								{
									Key:      v1.LabelTopologyZone,
									Operator: v1.NodeSelectorOpIn,
									Values:   []string{"us-east1-c"},
								},
							},
						},
					},
				},
			},
		},
		// 用例4：同一 term 中同时存在 CSI Key 与 newKey（GA zone），二者 Values 不同。
		// 断言：只改 Key 不合并 Values，结果中出现两条同 Key 的表达式，印证文档所述的重复行为。
		{
			name:   "Replace the topology key from single term and not combine topology key",
			oldKey: GCEPDTopologyKey,
			newKey: v1.LabelTopologyZone,
			pv: makePVWithNodeSelectorTerms([]v1.NodeSelectorTerm{
				{
					MatchExpressions: []v1.NodeSelectorRequirement{
						{
							Key:      GCEPDTopologyKey,
							Operator: v1.NodeSelectorOpIn,
							Values:   []string{"us-east1-a"},
						},
						{
							Key:      v1.LabelTopologyZone,
							Operator: v1.NodeSelectorOpIn,
							Values:   []string{"us-east1-c"},
						},
					},
				},
			}),
			expOk: true,
			expectedAffinity: &v1.VolumeNodeAffinity{
				Required: &v1.NodeSelector{
					NodeSelectorTerms: []v1.NodeSelectorTerm{
						{
							MatchExpressions: []v1.NodeSelectorRequirement{
								{
									Key:      v1.LabelTopologyZone,
									Operator: v1.NodeSelectorOpIn,
									Values:   []string{"us-east1-a"},
								},
								{
									Key:      v1.LabelTopologyZone,
									Operator: v1.NodeSelectorOpIn,
									Values:   []string{"us-east1-c"},
								},
							},
						},
					},
				},
			},
		},
	}

	// 逐用例执行替换；该函数当前总是返回 nil，expOk 主要用于表达用例意图。
	for _, tc := range testCases {
		t.Logf("Running test: %v", tc.name)
		err := replaceTopology(tc.pv, tc.oldKey, tc.newKey)
		// 断言没有意外错误，再深比较整个 NodeAffinity。
		if err != nil && tc.expOk {
			t.Errorf("Expected no err: %v, but got err: %v", tc.expOk, err)
		}
		if !reflect.DeepEqual(tc.pv.Spec.NodeAffinity, tc.expectedAffinity) {
			t.Errorf("Expected affinity: %v, but got: %v", tc.expectedAffinity, tc.pv.Spec.NodeAffinity)
		}
	}
}

// makePVWithNodeSelectorTerms 构造一个仅填好 NodeAffinity.Required.NodeSelectorTerms 的 PV，
// 供 replaceTopology 相关用例快速准备输入，避免每个用例重复书写嵌套结构。
func makePVWithNodeSelectorTerms(nodeSelectorTerms []v1.NodeSelectorTerm) *v1.PersistentVolume {
	return &v1.PersistentVolume{
		Spec: v1.PersistentVolumeSpec{
			NodeAffinity: &v1.VolumeNodeAffinity{
				Required: &v1.NodeSelector{
					NodeSelectorTerms: nodeSelectorTerms,
				},
			},
		},
	}

}
