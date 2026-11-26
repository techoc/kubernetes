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
	"hash/fnv"
	"testing"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/sets"
)

// TestZonesToSet 覆盖 zone 列表字符串解析：空串、多余逗号、首尾空格都必须报错，
// 合法输入应得到去空格后的集合。
func TestZonesToSet(t *testing.T) {
	functionUnderTest := "ZonesToSet"
	// 第一部分：期望报错
	sliceOfZones := []string{"", ",", "us-east-1a, , us-east-1d", ", us-west-1b", "us-west-2b,"}
	for _, zones := range sliceOfZones {
		if got, err := ZonesToSet(zones); err == nil {
			t.Errorf("%v(%v) returned (%v), want (%v)", functionUnderTest, zones, got, "an error")
		}
	}

	// 第二部分：期望成功
	tests := []struct {
		zones string
		want  sets.String
	}{
		{
			zones: "us-east-1a",
			want:  sets.String{"us-east-1a": sets.Empty{}},
		},
		{
			zones: "us-east-1a, us-west-2a",
			want: sets.String{
				"us-east-1a": sets.Empty{},
				"us-west-2a": sets.Empty{},
			},
		},
	}
	for _, tt := range tests {
		if got, err := ZonesToSet(tt.zones); err != nil || !got.Equal(tt.want) {
			t.Errorf("%v(%v) returned (%v), want (%v)", functionUnderTest, tt.zones, got, tt.want)
		}
	}
}

// TestChooseZonesForVolume 验证按卷名散列选择 zone 的确定性：
//   - 同名卷总是选中同一组 zone；
//   - 带 StatefulSet 序号时，序号参与偏移，使副本分散到不同 zone；
//   - 卷名以 '-' 结尾或缺省序号时回退到整体散列。
//
// 用例中的注释记录了期望值背后的散列推导，便于人工核对偏移规则。
func TestChooseZonesForVolume(t *testing.T) {
	checkFnv32(t, "henley", 1180403676)
	// 1180403676 mod 3 == 0，"henley" 的偏移为 0，便于人工核对

	// 其他几个哈希值
	checkFnv32(t, "henley-", 2652299129)
	checkFnv32(t, "henley-a", 1459735322)
	checkFnv32(t, "", 2166136261)

	tests := []struct {
		Zones      sets.String
		VolumeName string
		NumZones   uint32
		Expected   sets.String
	}{
		// 不含连字符的 PVC 名
		{
			Zones:      sets.NewString("a", "b", "c"),
			VolumeName: "henley",
			NumZones:   1,
			Expected:   sets.NewString("a" /* hash("henley") == 0 */),
		},
		{
			Zones:      sets.NewString("a", "b", "c"),
			VolumeName: "henley",
			NumZones:   2,
			Expected:   sets.NewString("a" /* hash("henley") == 0 */, "b"),
		},
		// 以 -数字 结尾、但并非 StatefulSet PVC 的名字
		{
			Zones:      sets.NewString("a", "b", "c"),
			VolumeName: "henley-0",
			NumZones:   1,
			Expected:   sets.NewString("a" /* hash("henley") == 0 */),
		},
		{
			Zones:      sets.NewString("a", "b", "c"),
			VolumeName: "henley-0",
			NumZones:   2,
			Expected:   sets.NewString("a" /* hash("henley") == 0 */, "b"),
		},
		{
			Zones:      sets.NewString("a", "b", "c"),
			VolumeName: "henley-1",
			NumZones:   1,
			Expected:   sets.NewString("b" /* hash("henley") + 1 == 1 */),
		},
		{
			Zones:      sets.NewString("a", "b", "c"),
			VolumeName: "henley-1",
			NumZones:   2,
			Expected:   sets.NewString("c" /* hash("henley") + 1 + 1(startingIndex) == 2 */, "a"),
		},
		{
			Zones:      sets.NewString("a", "b", "c"),
			VolumeName: "henley-2",
			NumZones:   1,
			Expected:   sets.NewString("c" /* hash("henley") + 2 == 2 */),
		},
		{
			Zones:      sets.NewString("a", "b", "c"),
			VolumeName: "henley-2",
			NumZones:   2,
			Expected:   sets.NewString("b" /* hash("henley") + 2 + 2(startingIndex) == 4 */, "c"),
		},
		{
			Zones:      sets.NewString("a", "b", "c"),
			VolumeName: "henley-3",
			NumZones:   1,
			Expected:   sets.NewString("a" /* hash("henley") + 3 == 3 === 0 mod 3 */),
		},
		{
			Zones:      sets.NewString("a", "b", "c"),
			VolumeName: "henley-3",
			NumZones:   2,
			Expected:   sets.NewString("a" /* hash("henley") + 3 + 3(startingIndex) == 6 */, "b"),
		},
		{
			Zones:      sets.NewString("a", "b", "c"),
			VolumeName: "henley-4",
			NumZones:   1,
			Expected:   sets.NewString("b" /* hash("henley") + 4 == 4 === 1 mod 3 */),
		},
		{
			Zones:      sets.NewString("a", "b", "c"),
			VolumeName: "henley-4",
			NumZones:   2,
			Expected:   sets.NewString("c" /* hash("henley") + 4 + 4(startingIndex) == 8 */, "a"),
		},
		// 边界情况的 PVC 名
		{
			Zones:      sets.NewString("a", "b", "c"),
			VolumeName: "henley-",
			NumZones:   1,
			Expected:   sets.NewString("c" /* hash("henley-") = 2652299129 === 2 mod 3 */),
		},
		{
			Zones:      sets.NewString("a", "b", "c"),
			VolumeName: "henley-",
			NumZones:   2,
			Expected:   sets.NewString("c" /* hash("henley-") = 2652299129 === 2 mod 3 = 2 */, "a"),
		},
		{
			Zones:      sets.NewString("a", "b", "c"),
			VolumeName: "henley-a",
			NumZones:   1,
			Expected:   sets.NewString("c" /* hash("henley-a") = 1459735322 === 2 mod 3 */),
		},
		{
			Zones:      sets.NewString("a", "b", "c"),
			VolumeName: "henley-a",
			NumZones:   2,
			Expected:   sets.NewString("c" /* hash("henley-a") = 1459735322 === 2 mod 3 = 2 */, "a"),
		},
		{
			Zones:      sets.NewString("a", "b", "c"),
			VolumeName: "medium--1",
			NumZones:   1,
			Expected:   sets.NewString("c" /* hash("") + 1 == 2166136261 + 1 === 2 mod 3 */),
		},
		{
			Zones:      sets.NewString("a", "b", "c"),
			VolumeName: "medium--1",
			NumZones:   2,
			Expected:   sets.NewString("a" /* hash("") + 1 + 1(startingIndex) == 2166136261 + 1 + 1 === 3 mod 3 = 0 */, "b"),
		},
		// 简单 StatefulSet 场景的 PVC 名
		{
			Zones:      sets.NewString("a", "b", "c"),
			VolumeName: "medium-henley-1",
			NumZones:   1,
			Expected:   sets.NewString("b" /* hash("henley") + 1 == 1 */),
		},
		// 简单 StatefulSet 场景的 PVC 名
		{
			Zones:      sets.NewString("a", "b", "c"),
			VolumeName: "medium-henley-1",
			NumZones:   2,
			Expected:   sets.NewString("c" /* hash("henley") + 1 + 1(startingIndex) == 2 */, "a"),
		},
		{
			Zones:      sets.NewString("a", "b", "c"),
			VolumeName: "loud-henley-1",
			NumZones:   1,
			Expected:   sets.NewString("b" /* hash("henley") + 1 == 1 */),
		},
		{
			Zones:      sets.NewString("a", "b", "c"),
			VolumeName: "loud-henley-1",
			NumZones:   2,
			Expected:   sets.NewString("c" /* hash("henley") + 1 + 1(startingIndex) == 2 */, "a"),
		},
		{
			Zones:      sets.NewString("a", "b", "c"),
			VolumeName: "quiet-henley-2",
			NumZones:   1,
			Expected:   sets.NewString("c" /* hash("henley") + 2 == 2 */),
		},
		{
			Zones:      sets.NewString("a", "b", "c"),
			VolumeName: "quiet-henley-2",
			NumZones:   2,
			Expected:   sets.NewString("b" /* hash("henley") + 2 + 2(startingIndex) == 4 */, "c"),
		},
		{
			Zones:      sets.NewString("a", "b", "c"),
			VolumeName: "medium-henley-2",
			NumZones:   1,
			Expected:   sets.NewString("c" /* hash("henley") + 2 == 2 */),
		},
		{
			Zones:      sets.NewString("a", "b", "c"),
			VolumeName: "medium-henley-2",
			NumZones:   2,
			Expected:   sets.NewString("b" /* hash("henley") + 2 + 2(startingIndex) == 4 */, "c"),
		},
		{
			Zones:      sets.NewString("a", "b", "c"),
			VolumeName: "medium-henley-3",
			NumZones:   1,
			Expected:   sets.NewString("a" /* hash("henley") + 3 == 3 === 0 mod 3 */),
		},
		{
			Zones:      sets.NewString("a", "b", "c"),
			VolumeName: "medium-henley-3",
			NumZones:   2,
			Expected:   sets.NewString("a" /* hash("henley") + 3 + 3(startingIndex) == 6 === 6 mod 3 = 0 */, "b"),
		},
		{
			Zones:      sets.NewString("a", "b", "c"),
			VolumeName: "medium-henley-4",
			NumZones:   1,
			Expected:   sets.NewString("b" /* hash("henley") + 4 == 4 === 1 mod 3 */),
		},
		{
			Zones:      sets.NewString("a", "b", "c"),
			VolumeName: "medium-henley-4",
			NumZones:   2,
			Expected:   sets.NewString("c" /* hash("henley") + 4 + 4(startingIndex) == 8 === 2 mod 3 */, "a"),
		},
		// 名字里带连字符的 StatefulSet（或 claim）
		{
			Zones:      sets.NewString("a", "b", "c"),
			VolumeName: "medium-alpha-henley-2",
			NumZones:   1,
			Expected:   sets.NewString("c" /* hash("henley") + 2 == 2 */),
		},
		{
			Zones:      sets.NewString("a", "b", "c"),
			VolumeName: "medium-alpha-henley-2",
			NumZones:   2,
			Expected:   sets.NewString("b" /* hash("henley") + 2 + 2(startingIndex) == 4 */, "c"),
		},
		{
			Zones:      sets.NewString("a", "b", "c"),
			VolumeName: "medium-beta-henley-3",
			NumZones:   1,
			Expected:   sets.NewString("a" /* hash("henley") + 3 == 3 === 0 mod 3 */),
		},
		{
			Zones:      sets.NewString("a", "b", "c"),
			VolumeName: "medium-beta-henley-3",
			NumZones:   2,
			Expected:   sets.NewString("a" /* hash("henley") + 3 + 3(startingIndex) == 6 === 0 mod 3 */, "b"),
		},
		{
			Zones:      sets.NewString("a", "b", "c"),
			VolumeName: "medium-gamma-henley-4",
			NumZones:   1,
			Expected:   sets.NewString("b" /* hash("henley") + 4 == 4 === 1 mod 3 */),
		},
		{
			Zones:      sets.NewString("a", "b", "c"),
			VolumeName: "medium-gamma-henley-4",
			NumZones:   2,
			Expected:   sets.NewString("c" /* hash("henley") + 4 + 4(startingIndex) == 8 === 2 mod 3 */, "a"),
		},
		// 以连字符结尾的 StatefulSet 名
		{
			Zones:      sets.NewString("a", "b", "c"),
			VolumeName: "medium-henley--2",
			NumZones:   1,
			Expected:   sets.NewString("a" /* hash("") + 2 == 0 mod 3 */),
		},
		{
			Zones:      sets.NewString("a", "b", "c"),
			VolumeName: "medium-henley--2",
			NumZones:   2,
			Expected:   sets.NewString("c" /* hash("") + 2 + 2(startingIndex) == 2 mod 3 */, "a"),
		},
		{
			Zones:      sets.NewString("a", "b", "c"),
			VolumeName: "medium-henley--3",
			NumZones:   1,
			Expected:   sets.NewString("b" /* hash("") + 3 == 1 mod 3 */),
		},
		{
			Zones:      sets.NewString("a", "b", "c"),
			VolumeName: "medium-henley--3",
			NumZones:   2,
			Expected:   sets.NewString("b" /* hash("") + 3 + 3(startingIndex) == 1 mod 3 */, "c"),
		},
		{
			Zones:      sets.NewString("a", "b", "c"),
			VolumeName: "medium-henley--4",
			NumZones:   1,
			Expected:   sets.NewString("c" /* hash("") + 4 == 2 mod 3 */),
		},
		{
			Zones:      sets.NewString("a", "b", "c"),
			VolumeName: "medium-henley--4",
			NumZones:   2,
			Expected:   sets.NewString("a" /* hash("") + 4 + 4(startingIndex) == 0 mod 3 */, "b"),
		},
		{
			Zones:      sets.NewString("a", "b", "c"),
			VolumeName: "medium-henley--4",
			NumZones:   3,
			Expected:   sets.NewString("c" /* hash("") + 4 == 2 mod 3 */, "a", "b"),
		},
		{
			Zones:      sets.NewString("a", "b", "c"),
			VolumeName: "medium-henley--4",
			NumZones:   4,
			Expected:   sets.NewString("c" /* hash("") + 4 + 9(startingIndex) == 2 mod 3 */, "a", "b", "c"),
		},
		{
			Zones:      sets.NewString("a", "b", "c", "d", "e", "f", "g", "h", "i"),
			VolumeName: "henley-0",
			NumZones:   2,
			Expected:   sets.NewString("a" /* hash("henley") == 0 */, "b"),
		},
		{
			Zones:      sets.NewString("a", "b", "c", "d", "e", "f", "g", "h", "i"),
			VolumeName: "henley-1",
			NumZones:   2,
			Expected:   sets.NewString("c" /* hash("henley") == 0 + 2 */, "d"),
		},
		{
			Zones:      sets.NewString("a", "b", "c", "d", "e", "f", "g", "h", "i"),
			VolumeName: "henley-2",
			NumZones:   2,
			Expected:   sets.NewString("e" /* hash("henley") == 0 + 2 + 2(startingIndex) */, "f"),
		},
		{
			Zones:      sets.NewString("a", "b", "c", "d", "e", "f", "g", "h", "i"),
			VolumeName: "henley-3",
			NumZones:   2,
			Expected:   sets.NewString("g" /* hash("henley") == 0 + 2 + 4(startingIndex) */, "h"),
		},
		{
			Zones:      sets.NewString("a", "b", "c", "d", "e", "f", "g", "h", "i"),
			VolumeName: "henley-0",
			NumZones:   3,
			Expected:   sets.NewString("a" /* hash("henley") == 0 */, "b", "c"),
		},
		{
			Zones:      sets.NewString("a", "b", "c", "d", "e", "f", "g", "h", "i"),
			VolumeName: "henley-1",
			NumZones:   3,
			Expected:   sets.NewString("d" /* hash("henley") == 0 + 1 + 2(startingIndex) */, "e", "f"),
		},
		{
			Zones:      sets.NewString("a", "b", "c", "d", "e", "f", "g", "h", "i"),
			VolumeName: "henley-2",
			NumZones:   3,
			Expected:   sets.NewString("g" /* hash("henley") == 0 + 2 + 4(startingIndex) */, "h", "i"),
		},
		{
			Zones:      sets.NewString("a", "b", "c", "d", "e", "f", "g", "h", "i"),
			VolumeName: "henley-3",
			NumZones:   3,
			Expected:   sets.NewString("a" /* hash("henley") == 0 + 3 + 6(startingIndex) */, "b", "c"),
		},
		// 无可用 zone 的情况
		{
			Zones:      sets.NewString(),
			VolumeName: "henley-1",
			Expected:   sets.NewString(),
		},
		{
			Zones:      nil,
			VolumeName: "henley-2",
			Expected:   sets.NewString(),
		},
	}

	for _, test := range tests {
		actual := ChooseZonesForVolume(test.Zones, test.VolumeName, test.NumZones)

		if !actual.Equal(test.Expected) {
			t.Errorf("Test %v failed, expected zone %#v, actual %#v", test, test.Expected, actual)
		}
	}
}

// TestSelectZoneForVolume 覆盖单副本 PVC 的 zone 选择优先级与互斥校验：
// node 上的 zone 标签优先，其次 zone/zones 参数、AllowedTopologies、活跃 zone 集合；
// 同时给出 zone 与 zones、或给 node 又给 zone 参数等冲突组合必须拒绝。
func TestSelectZoneForVolume(t *testing.T) {

	nodeWithGATopologyLabels := &v1.Node{}
	nodeWithGATopologyLabels.Labels = map[string]string{v1.LabelTopologyZone: "zoneX"}

	nodeWithBetaTopologyLabels := &v1.Node{}
	nodeWithBetaTopologyLabels.Labels = map[string]string{v1.LabelFailureDomainBetaZone: "zoneY"}

	nodeWithNoLabels := &v1.Node{}

	tests := []struct {
		// 传给 SelectZoneForVolume 的参数
		Name              string
		ZonePresent       bool
		Zone              string
		ZonesPresent      bool
		Zones             string
		ZonesWithNodes    string
		Node              *v1.Node
		AllowedTopologies []v1.TopologySelectorTerm
		// 对 SelectZoneForVolume 返回 zone 的期望
		Reject             bool   // expect error due to validation failing
		ExpectSpecificZone bool   // expect returned zone to specifically match a single zone (rather than one from a set)
		ExpectedZone       string // single zone that should perfectly match returned zone (requires ExpectSpecificZone to be true)
		ExpectedZones      string // set of zones one of whose members should match returned zone (requires ExpectSpecificZone to be false)
	}{
		// 反面用例

		// Zone 与 Zones 同时指定 [Fail]
		// [1] Node 无关
		// [2] 同时给出 Zone 与 Zones 参数
		// [3] AllowedTopologies 无关
		{
			Name:         "Nil_Node_with_Zone_Zones_parameters_present",
			ZonePresent:  true,
			Zone:         "zoneX",
			ZonesPresent: true,
			Zones:        "zoneX,zoneY",
			Reject:       true,
		},

		// Node 没有 zone 标签 [Fail]
		// [1] Node 没有 zone 标签
		// [2] Zone/Zones 参数无关
		// [3] AllowedTopologies 无关
		{
			Name:   "Node_with_no_Zone_labels",
			Node:   nodeWithNoLabels,
			Reject: true,
		},

		// Node 有 zone 标签且指定了 Zone 参数 [Fail]
		// [1] Node 带 zone 标签
		// [2] 指定了 Zone 参数
		// [3] AllowedTopologies 无关
		{
			Name:        "Node_with_Zone_labels_and_Zone_parameter_present",
			Node:        nodeWithGATopologyLabels,
			ZonePresent: true,
			Zone:        "zoneX",
			Reject:      true,
		},

		// Node 有 zone 标签且指定了 Zones 参数 [Fail]
		// [1] Node 带 zone 标签
		// [2] 指定了 Zones 参数
		// [3] AllowedTopologies 无关
		{
			Name:         "Node_with_Zone_labels_and_Zones_parameter_present",
			Node:         nodeWithGATopologyLabels,
			ZonesPresent: true,
			Zones:        "zoneX,zoneY",
			Reject:       true,
		},

		// Zone 参数与 AllowedTopologies 同时指定 [Fail]
		// [1] Node 为 nil
		// [2] 指定了 Zone 参数
		// [3] 指定了 AllowedTopologies
		{
			Name:        "Nil_Node_and_Zone_parameter_and_Allowed_Topology_term",
			Node:        nil,
			ZonePresent: true,
			Zone:        "zoneX",
			AllowedTopologies: []v1.TopologySelectorTerm{
				{
					MatchLabelExpressions: []v1.TopologySelectorLabelRequirement{
						{
							Key:    v1.LabelTopologyZone,
							Values: []string{"zoneX"},
						},
					},
				},
			},
			Reject: true,
		},

		// Zones 参数与 AllowedTopologies 同时指定 [Fail]
		// [1] Node 为 nil
		// [2] 指定了 Zones 参数
		// [3] 指定了 AllowedTopologies
		{
			Name:         "Nil_Node_and_Zones_parameter_and_Allowed_Topology_term",
			Node:         nil,
			ZonesPresent: true,
			Zones:        "zoneX,zoneY",
			AllowedTopologies: []v1.TopologySelectorTerm{
				{
					MatchLabelExpressions: []v1.TopologySelectorLabelRequirement{
						{
							Key:    v1.LabelTopologyZone,
							Values: []string{"zoneX"},
						},
					},
				},
			},
			Reject: true,
		},

		// AllowedTopologies 中的 key 不是 LabelTopologyZone [Fail]
		// [1] Node 为 nil
		// [2] 未指定 Zone/Zones 参数
		// [3] AllowedTopologies 中使用了非法 key
		{
			Name: "Nil_Node_and_Invalid_Allowed_Topology_Key",
			Node: nil,
			AllowedTopologies: []v1.TopologySelectorTerm{
				{
					MatchLabelExpressions: []v1.TopologySelectorLabelRequirement{
						{
							Key:    "invalid_key",
							Values: []string{"zoneX"},
						},
						{
							Key:    v1.LabelTopologyZone,
							Values: []string{"zoneY"},
						},
					},
				},
			},
			Reject: true,
		},

		// AllowedTopologies 未包含 LabelTopologyZone key [Fail]
		// [1] Node 为 nil
		// [2] 未指定 Zone/Zones 参数
		// [3] AllowedTopologies 非法
		{
			Name: "Nil_Node_and_Invalid_AllowedTopologies",
			Node: nil,
			AllowedTopologies: []v1.TopologySelectorTerm{
				{
					MatchLabelExpressions: []v1.TopologySelectorLabelRequirement{},
				},
			},
			Reject: true,
		},

		// 启用 VolumeScheduling 时的正面用例

		// 从活跃 zone 中选择 [Pass]
		// [1] Node 为 nil
		// [2] 未指定 Zone 参数
		// [3] 未指定 AllowedTopologies
		{
			Name:           "Nil_Node_and_No_Zone_Zones_parameter_and_no_Allowed_topologies_and_VolumeScheduling_enabled",
			Node:           nil,
			ZonesWithNodes: "zoneX,zoneY",
			Reject:         false,
			ExpectedZones:  "zoneX,zoneY",
		},

		// 从单个 zone 参数中选择 [Pass]
		// [1] Node 为 nil
		// [2] 指定了 Zone 参数
		// [3] 未指定 AllowedTopology
		{
			Name:               "Nil_Node_and_Zone_parameter_present_and_VolumeScheduling_enabled",
			ZonePresent:        true,
			Zone:               "zoneX",
			Node:               nil,
			Reject:             false,
			ExpectSpecificZone: true,
			ExpectedZone:       "zoneX",
		},

		// 从 zones 参数中选择 [Pass]
		// [1] Node 为 nil
		// [2] 指定了 Zones 参数
		// [3] 未指定 AllowedTopology
		{
			Name:          "Nil_Node_and_Zones_parameter_present_and_VolumeScheduling_enabled",
			ZonesPresent:  true,
			Zones:         "zoneX,zoneY",
			Node:          nil,
			Reject:        false,
			ExpectedZones: "zoneX,zoneY",
		},

		// 从节点 GA 标签选择 [Pass]
		// [1] Node 带 zone 标签
		// [2] 未指定 zone/zones 参数
		// [3] 未指定 AllowedTopology
		{
			Name:               "Node_with_GA_Zone_labels_and_VolumeScheduling_enabled",
			Node:               nodeWithGATopologyLabels,
			Reject:             false,
			ExpectSpecificZone: true,
			ExpectedZone:       "zoneX",
		},

		// 从节点 beta 标签选择 [Pass]
		// [1] Node 带 zone 标签
		// [2] 未指定 zone/zones 参数
		// [3] 未指定 AllowedTopology
		{
			Name:               "Node_with_Beta_Zone_labels_and_VolumeScheduling_enabled",
			Node:               nodeWithBetaTopologyLabels,
			Reject:             false,
			ExpectSpecificZone: true,
			ExpectedZone:       "zoneY",
		},

		// 从节点标签选择 [Pass]
		// [1] Node 带 zone 标签
		// [2] 未指定 Zone/Zones 参数
		// [3] AllowedTopology 单个 term 多值（忽略）
		{
			Name: "Node_with_Zone_labels_and_Multiple_Allowed_Topology_values_and_VolumeScheduling_enabled",
			Node: nodeWithGATopologyLabels,
			AllowedTopologies: []v1.TopologySelectorTerm{
				{
					MatchLabelExpressions: []v1.TopologySelectorLabelRequirement{
						{
							Key:    v1.LabelTopologyZone,
							Values: []string{"zoneZ", "zoneY"},
						},
					},
				},
			},
			Reject:             false,
			ExpectSpecificZone: true,
			ExpectedZone:       "zoneX",
		},

		// 从 AllowedTopologies 选择 zone [Pass]
		// [1] Node 为 nil
		// [2] 未指定 Zone/Zones 参数
		// [3] AllowedTopologies 单个 term 多值
		{
			Name: "Nil_Node_with_Multiple_Allowed_Topology_values_and_VolumeScheduling_enabled",
			Node: nil,
			AllowedTopologies: []v1.TopologySelectorTerm{
				{
					MatchLabelExpressions: []v1.TopologySelectorLabelRequirement{
						{
							Key:    v1.LabelTopologyZone,
							Values: []string{"zoneX", "zoneY"},
						},
					},
				},
			},
			Reject:        false,
			ExpectedZones: "zoneX,zoneY",
		},

		// 用 beta 标签从 AllowedTopologies 选择 zone [Pass]
		// [1] Node 为 nil
		// [2] 未指定 Zone/Zones 参数
		// [3] AllowedTopologies 单个 term 多值
		{
			Name: "Nil_Node_with_Multiple_Allowed_Topology_values_Beta_label_and_VolumeScheduling_enabled",
			Node: nil,
			AllowedTopologies: []v1.TopologySelectorTerm{
				{
					MatchLabelExpressions: []v1.TopologySelectorLabelRequirement{
						{
							Key:    v1.LabelFailureDomainBetaZone,
							Values: []string{"zoneX", "zoneY"},
						},
					},
				},
			},
			Reject:        false,
			ExpectedZones: "zoneX,zoneY",
		},

		// 从 AllowedTopologies 选择 zone [Pass]
		// [1] Node 为 nil
		// [2] 未指定 Zone/Zones 参数
		// [3] AllowedTopologies 多个 term
		{
			Name: "Nil_Node_and_Multiple_Allowed_Topology_terms_and_VolumeScheduling_enabled",
			Node: nil,
			AllowedTopologies: []v1.TopologySelectorTerm{
				{
					MatchLabelExpressions: []v1.TopologySelectorLabelRequirement{
						{
							Key:    v1.LabelTopologyZone,
							Values: []string{"zoneX"},
						},
					},
				},
				{
					MatchLabelExpressions: []v1.TopologySelectorLabelRequirement{
						{
							Key:    v1.LabelTopologyZone,
							Values: []string{"zoneY"},
						},
					},
				},
			},
			Reject:        false,
			ExpectedZones: "zoneX,zoneY",
		},

		// 从 AllowedTopologies 选择 zone [Pass]
		// 注意：双副本配同一 AllowedTopologies 会失败，见用例 Nil_Node_and_Single_Allowed_Topology_term_value_and_Dual_replicas
		// [1] Node 为 nil
		// [2] 未指定 Zone/Zones 参数
		// [3] AllowedTopologies 单个 term 与值
		{
			Name: "Nil_Node_and_Single_Allowed_Topology_term_value_and_VolumeScheduling_enabled",
			Node: nil,
			AllowedTopologies: []v1.TopologySelectorTerm{
				{
					MatchLabelExpressions: []v1.TopologySelectorLabelRequirement{
						{
							Key:    v1.LabelTopologyZone,
							Values: []string{"zoneX"},
						},
					},
				},
			},
			Reject:             false,
			ExpectSpecificZone: true,
			ExpectedZone:       "zoneX",
		},
	}

	for _, test := range tests {
		t.Run(test.Name, func(t *testing.T) {
			var zonesParameter, zonesWithNodes sets.String
			var err error

			if test.Zones != "" {
				zonesParameter, err = ZonesToSet(test.Zones)
				if err != nil {
					t.Fatalf("Could not convert Zones to a set: %s. This is a test error %s", test.Zones, test.Name)
				}
			}

			if test.ZonesWithNodes != "" {
				zonesWithNodes, err = ZonesToSet(test.ZonesWithNodes)
				if err != nil {
					t.Fatalf("Could not convert specified ZonesWithNodes to a set: %s. This is a test error %s", test.ZonesWithNodes, test.Name)
				}
			}

			zone, err := SelectZoneForVolume(test.ZonePresent, test.ZonesPresent, test.Zone, zonesParameter, zonesWithNodes, test.Node, test.AllowedTopologies, test.Name)

			if test.Reject && err == nil {
				t.Errorf("Unexpected zone from SelectZoneForVolume for %s", zone)
			}

			if !test.Reject {
				if err != nil {
					t.Errorf("Unexpected error from SelectZoneForVolume for %s; Error: %v", test.Name, err)
				}

				if test.ExpectSpecificZone {
					if zone != test.ExpectedZone {
						t.Errorf("Expected zone %v does not match obtained zone %v for %s", test.ExpectedZone, zone, test.Name)
					}
				}

				if test.ExpectedZones != "" {
					expectedZones, err := ZonesToSet(test.ExpectedZones)
					if err != nil {
						t.Fatalf("Could not convert ExpectedZones to a set: %s. This is a test error", test.ExpectedZones)
					}
					if !expectedZones.Has(zone) {
						t.Errorf("Obtained zone %s not member of expectedZones %s", zone, expectedZones)
					}
				}
			}
		})
	}
}

// TestSelectZonesForVolume 覆盖多副本（如 StatefulSet 的 volumeClaimTemplates）
// 的 zone 选择：返回的 zone 数量必须等于 ReplicaCount，且尽量分散；
// 同时校验 zone/zones、node 标签与 AllowedTopologies 之间的互斥规则。
func TestSelectZonesForVolume(t *testing.T) {

	nodeWithZoneLabels := &v1.Node{}
	nodeWithZoneLabels.Labels = map[string]string{v1.LabelTopologyZone: "zoneX"}

	nodeWithNoLabels := &v1.Node{}

	tests := []struct {
		// 传给 SelectZonesForVolume 的参数
		Name              string
		ReplicaCount      uint32
		ZonePresent       bool
		Zone              string
		ZonesPresent      bool
		Zones             string
		ZonesWithNodes    string
		Node              *v1.Node
		AllowedTopologies []v1.TopologySelectorTerm
		// 对 SelectZonesForVolume 返回 zone 集合的期望
		Reject              bool   // expect error due to validation failing
		ExpectSpecificZones bool   // expect set of returned zones to be equal to ExpectedZones (rather than subset of ExpectedZones)
		ExpectedZones       string // set of zones that is a superset of returned zones or equal to returned zones (if ExpectSpecificZones is set)
		ExpectSpecificZone  bool   // expect set of returned zones to include ExpectedZone as a member
		ExpectedZone        string // zone that should be a member of returned zones
	}{
		// 反面用例

		// Zone 与 Zones 同时指定 [Fail]
		// [1] Node 无关
		// [2] 同时给出 Zone 与 Zones 参数
		// [3] AllowedTopologies 无关
		// [4] ReplicaCount 无关
		// [5] ZonesWithNodes 无关
		{
			Name:         "Nil_Node_with_Zone_Zones_parameters_present",
			ZonePresent:  true,
			Zone:         "zoneX",
			ZonesPresent: true,
			Zones:        "zoneX,zoneY",
			Reject:       true,
		},

		// Node 没有 zone 标签 [Fail]
		// [1] Node 没有 zone 标签
		// [2] Zone/Zones 参数无关
		// [3] AllowedTopologies 无关
		// [4] ReplicaCount 无关
		// [5] ZonesWithNodes 无关
		{
			Name:   "Node_with_no_Zone_labels",
			Node:   nodeWithNoLabels,
			Reject: true,
		},

		// Node 有 zone 标签且指定了 Zone 参数 [Fail]
		// [1] Node 带 zone 标签
		// [2] 指定了 Zone 参数
		// [3] AllowedTopologies 无关
		// [4] ReplicaCount 无关
		// [5] ZonesWithNodes 无关
		{
			Name:        "Node_with_Zone_labels_and_Zone_parameter_present",
			Node:        nodeWithZoneLabels,
			ZonePresent: true,
			Zone:        "zoneX",
			Reject:      true,
		},

		// Node 有 zone 标签且指定了 Zones 参数 [Fail]
		// [1] Node 带 zone 标签
		// [2] 指定了 Zones 参数
		// [3] AllowedTopologies 无关
		// [4] ReplicaCount 无关
		// [5] ZonesWithNodes 无关
		{
			Name:         "Node_with_Zone_labels_and_Zones_parameter_present",
			Node:         nodeWithZoneLabels,
			ZonesPresent: true,
			Zones:        "zoneX,zoneY",
			Reject:       true,
		},

		// Zone 参数与 AllowedTopologies 同时指定 [Fail]
		// [1] Node 为 nil
		// [2] 指定了 Zone 参数
		// [3] 指定了 AllowedTopologies
		// [4] ReplicaCount 无关
		// [5] ZonesWithNodes 无关
		{
			Name:        "Nil_Node_and_Zone_parameter_and_Allowed_Topology_term",
			Node:        nil,
			ZonePresent: true,
			Zone:        "zoneX",
			AllowedTopologies: []v1.TopologySelectorTerm{
				{
					MatchLabelExpressions: []v1.TopologySelectorLabelRequirement{
						{
							Key:    v1.LabelTopologyZone,
							Values: []string{"zoneX"},
						},
					},
				},
			},
			Reject: true,
		},

		// Zones 参数与 AllowedTopologies 同时指定 [Fail]
		// [1] Node 为 nil
		// [2] 指定了 Zones 参数
		// [3] 指定了 AllowedTopologies
		// [4] ReplicaCount 无关
		// [5] ZonesWithNodes 无关
		{
			Name:         "Nil_Node_and_Zones_parameter_and_Allowed_Topology_term",
			Node:         nil,
			ZonesPresent: true,
			Zones:        "zoneX,zoneY",
			AllowedTopologies: []v1.TopologySelectorTerm{
				{
					MatchLabelExpressions: []v1.TopologySelectorLabelRequirement{
						{
							Key:    v1.LabelTopologyZone,
							Values: []string{"zoneX"},
						},
					},
				},
			},
			Reject: true,
		},

		// AllowedTopologies 中的 key 不是 LabelTopologyZone [Fail]
		// [1] Node 为 nil
		// [2] 未指定 Zone/Zones 参数
		// [3] AllowedTopologies 中使用了非法 key
		// [4] ReplicaCount 无关
		// [5] ZonesWithNodes 无关
		{
			Name: "Nil_Node_and_Invalid_Allowed_Topology_Key",
			Node: nil,
			AllowedTopologies: []v1.TopologySelectorTerm{
				{
					MatchLabelExpressions: []v1.TopologySelectorLabelRequirement{
						{
							Key:    "invalid_key",
							Values: []string{"zoneX"},
						},
						{
							Key:    v1.LabelTopologyZone,
							Values: []string{"zoneY"},
						},
					},
				},
			},
			Reject: true,
		},

		// AllowedTopologies 未包含 LabelTopologyZone key [Fail]
		// [1] Node 为 nil
		// [2] 未指定 Zone/Zones 参数
		// [3] AllowedTopologies 非法
		// [4] ReplicaCount 无关
		// [5] ZonesWithNodes 无关
		{
			Name: "Nil_Node_and_Invalid_AllowedTopologies",
			Node: nil,
			AllowedTopologies: []v1.TopologySelectorTerm{
				{
					MatchLabelExpressions: []v1.TopologySelectorLabelRequirement{},
				},
			},
			Reject: true,
		},

		// ReplicaCount > 1 时指定 Zone [Fail]
		// [1] Node 为 nil
		// [2] 指定了 Zone/Zones 参数
		// [3] AllowedTopologies 无关
		// [4] ReplicaCount > 1
		// [5] ZonesWithNodes 无关
		{
			Name:         "Zone_and_Replicas_mismatched",
			Node:         nil,
			ZonePresent:  true,
			Zone:         "zoneX",
			ReplicaCount: 2,
			Reject:       true,
		},

		// Zones 参数中的 zone 不足以满足 ReplicaCount [Fail]
		// [1] Node 为 nil
		// [2] 指定了 Zone/Zones 参数
		// [3] AllowedTopologies 无关
		// [4] ReplicaCount 超过 zone 数
		// [5] ZonesWithNodes 无关
		{
			Name:         "Zones_and_Replicas_mismatched",
			Node:         nil,
			ZonesPresent: true,
			Zones:        "zoneX",
			ReplicaCount: 2,
			Reject:       true,
		},

		// ZonesWithNodes 中的 zone 不足以满足 ReplicaCount [Fail]
		// [1] Node 为 nil
		// [2] 未指定 Zone/Zones 参数
		// [3] AllowedTopologies 无关
		// [4] ReplicaCount 超过 ZonesWithNodes 数
		// [5] 指定了 ZonesWithNodes 但数量不足
		{
			Name:           "Zones_and_Replicas_mismatched",
			Node:           nil,
			ZonesWithNodes: "zoneX",
			ReplicaCount:   2,
			Reject:         true,
		},

		// AllowedTopologies 中的 zone 不足以满足 ReplicaCount [Fail]
		// [1] Node 为 nil
		// [2] 指定了 Zone/Zones 参数
		// [3] AllowedTopologies 非法
		// [4] ReplicaCount 超过 zone 数
		// [5] ZonesWithNodes 无关
		{
			Name:         "AllowedTopologies_and_Replicas_mismatched",
			Node:         nil,
			ReplicaCount: 2,
			AllowedTopologies: []v1.TopologySelectorTerm{
				{
					MatchLabelExpressions: []v1.TopologySelectorLabelRequirement{
						{
							Key:    v1.LabelTopologyZone,
							Values: []string{"zoneX"},
						},
					},
				},
			},
			Reject: true,
		},

		// 正面用例

		// 从 zones 参数中选择多个 zone [Pass]
		// [1] Node 为 nil（与 Node 无关）
		// [2] 指定了 Zones 参数
		// [3] 未指定 AllowedTopologies
		// [4] 指定了 ReplicaCount
		// [5] ZonesWithNodes 无关
		{
			Name:          "Zones_parameter_Dual_replica_Superset",
			ZonesPresent:  true,
			Zones:         "zoneW,zoneX,zoneY,zoneZ",
			ReplicaCount:  2,
			Reject:        false,
			ExpectedZones: "zoneW,zoneX,zoneY,zoneZ",
		},

		// 从 zones 参数中选择多个 zone [Pass]
		// [1] Node 为 nil（与 Node 无关）
		// [2] 指定了 Zones 参数
		// [3] 未指定 AllowedTopologies
		// [4] 指定了 ReplicaCount
		// [5] ZonesWithNodes 无关
		{
			Name:                "Zones_parameter_Dual_replica_Match",
			ZonesPresent:        true,
			Zones:               "zoneX,zoneY",
			ReplicaCount:        2,
			Reject:              false,
			ExpectSpecificZones: true,
			ExpectedZones:       "zoneX,zoneY",
		},

		// 从集群中有节点的活跃 zone 中选择 [Pass]
		// [1] Node 为 nil（与 Node 无关）
		// [2] 未指定 Zone 参数
		// [3] 未指定 AllowedTopologies
		// [4] 指定了 ReplicaCount
		// [5] 指定了 ZonesWithNodes
		{
			Name:           "Active_zones_Nil_node_Dual_replica_Superset",
			ZonesWithNodes: "zoneW,zoneX,zoneY,zoneZ",
			ReplicaCount:   2,
			Reject:         false,
			ExpectedZones:  "zoneW,zoneX,zoneY,zoneZ",
		},

		// 从活跃 zone 中选择多个 [Pass]
		// [1] Node 为 nil（与 Node 无关）
		// [2] 未指定 Zone[s] 参数
		// [3] 未指定 AllowedTopologies
		// [4] 指定了 ReplicaCount
		// [5] 指定了 ZonesWithNodes
		{
			Name:                "Active_zones_Nil_node_Dual_replica_Match",
			ZonesWithNodes:      "zoneW,zoneX",
			ReplicaCount:        2,
			Reject:              false,
			ExpectSpecificZone:  true,
			ExpectedZone:        "zoneX",
			ExpectSpecificZones: true,
			ExpectedZones:       "zoneW,zoneX",
		},

		// 结合节点标签与活跃 zone 选择 [Pass]
		// [1] Node 带 zone 标签
		// [2] 未指定 Zone 参数
		// [3] 未指定 AllowedTopologies
		// [4] 指定了 ReplicaCount
		// [5] ZonesWithNodes 无关
		{
			Name:               "Active_zones_Node_with_Zone_labels_Dual_replica_Superset",
			Node:               nodeWithZoneLabels,
			ZonesWithNodes:     "zoneW,zoneX,zoneY,zoneZ",
			ReplicaCount:       2,
			Reject:             false,
			ExpectSpecificZone: true,
			ExpectedZone:       "zoneX",
			ExpectedZones:      "zoneW,zoneX,zoneY,zoneZ",
		},

		// 结合节点标签与活跃 zone 选择 [Pass]
		// [1] Node 带 zone 标签
		// [2] 未指定 Zone[s] 参数
		// [3] 未指定 AllowedTopologies
		// [4] 指定了 ReplicaCount
		// [5] ZonesWithNodes 无关
		{
			Name:                "Active_zones_Node_with_Zone_labels_Dual_replica_Match",
			Node:                nodeWithZoneLabels,
			ZonesWithNodes:      "zoneW,zoneX",
			ReplicaCount:        2,
			Reject:              false,
			ExpectSpecificZone:  true,
			ExpectedZone:        "zoneX",
			ExpectSpecificZones: true,
			ExpectedZones:       "zoneW,zoneX",
		},

		// 结合节点标签与 AllowedTopologies 选择 [Pass]
		// [1] Node 带 zone 标签
		// [2] 未指定 Zone/Zones 参数
		// [3] AllowedTopologies 单个 term 多值
		// [4] 指定了 ReplicaCount
		// [5] ZonesWithNodes 无关
		// 注意：用例 Name 的后缀被当作 pvcname，会影响 ChooseZonesForVolume，
		// 用于验证 zoneFromNode 设置错误或缺失时不会误选 AllowedTopologies 里的 zoneX。
		{
			Name:         "Node_with_Zone_labels_and_Multiple_Allowed_Topology_values_Superset-1",
			Node:         nodeWithZoneLabels,
			ReplicaCount: 2,
			AllowedTopologies: []v1.TopologySelectorTerm{
				{
					MatchLabelExpressions: []v1.TopologySelectorLabelRequirement{
						{
							Key:    v1.LabelTopologyZone,
							Values: []string{"zoneV", "zoneW", "zoneX", "zoneY", "zoneZ"},
						},
					},
				},
			},
			Reject:             false,
			ExpectSpecificZone: true,
			ExpectedZone:       "zoneX",
			ExpectedZones:      "zoneV,zoneW,zoneX,zoneY,zoneZ",
		},

		// 结合节点标签与 AllowedTopologies 选择 [Pass]
		// [1] Node 带 zone 标签
		// [2] 未指定 Zone/Zones 参数
		// [3] AllowedTopologies 单个 term 多值
		// [4] 指定了 ReplicaCount
		// [5] ZonesWithNodes 无关
		{
			Name:         "Node_with_Zone_labels_and_Multiple_Allowed_Topology_values_Match",
			Node:         nodeWithZoneLabels,
			ReplicaCount: 2,
			AllowedTopologies: []v1.TopologySelectorTerm{
				{
					MatchLabelExpressions: []v1.TopologySelectorLabelRequirement{
						{
							Key:    v1.LabelTopologyZone,
							Values: []string{"zoneX", "zoneY"},
						},
					},
				},
			},
			Reject:              false,
			ExpectSpecificZone:  true,
			ExpectedZone:        "zoneX",
			ExpectSpecificZones: true,
			ExpectedZones:       "zoneX,zoneY",
		},

		// 从 AllowedTopologies 选择多个 zone [Pass]
		// [1] Node 为 nil
		// [2] 未指定 Zone/Zones 参数
		// [3] AllowedTopologies 单个 term 多值
		// [4] 指定了 ReplicaCount
		// [5] ZonesWithNodes 无关
		{
			Name:         "Nil_Node_with_Multiple_Allowed_Topology_values_Superset",
			Node:         nil,
			ReplicaCount: 2,
			AllowedTopologies: []v1.TopologySelectorTerm{
				{
					MatchLabelExpressions: []v1.TopologySelectorLabelRequirement{
						{
							Key:    v1.LabelTopologyZone,
							Values: []string{"zoneX", "zoneY", "zoneZ"},
						},
					},
				},
			},
			Reject:        false,
			ExpectedZones: "zoneX,zoneY,zoneZ",
		},

		// 从 AllowedTopologies 选择多个 zone [Pass]
		// [1] Node 为 nil
		// [2] 未指定 Zone/Zones 参数
		// [3] AllowedTopologies 单个 term 多值
		// [4] 指定了 ReplicaCount
		// [5] ZonesWithNodes 无关
		{
			Name:         "Nil_Node_with_Multiple_Allowed_Topology_values_Match",
			Node:         nil,
			ReplicaCount: 2,
			AllowedTopologies: []v1.TopologySelectorTerm{
				{
					MatchLabelExpressions: []v1.TopologySelectorLabelRequirement{
						{
							Key:    v1.LabelTopologyZone,
							Values: []string{"zoneX", "zoneY"},
						},
					},
				},
			},
			Reject:              false,
			ExpectSpecificZones: true,
			ExpectedZones:       "zoneX,zoneY",
		},

		// 结合节点标签与 AllowedTopologies 选择 [Pass]
		// [1] Node 带 zone 标签
		// [2] 未指定 Zone/Zones 参数
		// [3] AllowedTopologies 多个 term
		// [4] 指定了 ReplicaCount
		// [5] ZonesWithNodes 无关
		// 注意：用例 Name 被当作 pvcname，其哈希会影响 ChooseZonesForVolume，
		// 用于验证 zoneFromNode（zoneX）设置错误时不会误选含 5 个 zone 的
		// AllowedTopologies 中的 zoneX；缺失该字段时同样能被发现。
		{
			Name:         "Node_with_Zone_labels_and_Multiple_Allowed_Topology_terms_Superset-2",
			Node:         nodeWithZoneLabels,
			ReplicaCount: 2,
			AllowedTopologies: []v1.TopologySelectorTerm{
				{
					MatchLabelExpressions: []v1.TopologySelectorLabelRequirement{
						{
							Key:    v1.LabelTopologyZone,
							Values: []string{"zoneV"},
						},
					},
				},
				{
					MatchLabelExpressions: []v1.TopologySelectorLabelRequirement{
						{
							Key:    v1.LabelTopologyZone,
							Values: []string{"zoneW"},
						},
					},
				},
				{
					MatchLabelExpressions: []v1.TopologySelectorLabelRequirement{
						{
							Key:    v1.LabelTopologyZone,
							Values: []string{"zoneX"},
						},
					},
				},
				{
					MatchLabelExpressions: []v1.TopologySelectorLabelRequirement{
						{
							Key:    v1.LabelTopologyZone,
							Values: []string{"zoneY"},
						},
					},
				},
				{
					MatchLabelExpressions: []v1.TopologySelectorLabelRequirement{
						{
							Key:    v1.LabelTopologyZone,
							Values: []string{"zoneZ"},
						},
					},
				},
			},
			Reject:             false,
			ExpectSpecificZone: true,
			ExpectedZone:       "zoneX",
			ExpectedZones:      "zoneV,zoneW,zoneX,zoneY,zoneZ",
		},

		// 结合节点标签与 AllowedTopologies 选择 [Pass]
		// [1] Node 带 zone 标签
		// [2] 未指定 Zone/Zones 参数
		// [3] AllowedTopologies 多个 term
		// [4] 指定了 ReplicaCount
		// [5] ZonesWithNodes 无关
		{
			Name:         "Node_with_Zone_labels_and_Multiple_Allowed_Topology_terms_Match",
			Node:         nodeWithZoneLabels,
			ReplicaCount: 2,
			AllowedTopologies: []v1.TopologySelectorTerm{
				{
					MatchLabelExpressions: []v1.TopologySelectorLabelRequirement{
						{
							Key:    v1.LabelTopologyZone,
							Values: []string{"zoneX"},
						},
					},
				},
				{
					MatchLabelExpressions: []v1.TopologySelectorLabelRequirement{
						{
							Key:    v1.LabelTopologyZone,
							Values: []string{"zoneY"},
						},
					},
				},
			},
			Reject:              false,
			ExpectSpecificZone:  true,
			ExpectedZone:        "zoneX",
			ExpectSpecificZones: true,
			ExpectedZones:       "zoneX,zoneY",
		},

		// 从 AllowedTopologies 选择 zone [Pass]
		// [1] Node 为 nil
		// [2] 未指定 Zone/Zones 参数
		// [3] AllowedTopologies 多个 term
		// [4] 指定了 ReplicaCount
		// [5] ZonesWithNodes 无关
		{
			Name:         "Nil_Node_and_Multiple_Allowed_Topology_terms_Superset",
			Node:         nil,
			ReplicaCount: 2,
			AllowedTopologies: []v1.TopologySelectorTerm{
				{
					MatchLabelExpressions: []v1.TopologySelectorLabelRequirement{
						{
							Key:    v1.LabelTopologyZone,
							Values: []string{"zoneX"},
						},
					},
				},
				{
					MatchLabelExpressions: []v1.TopologySelectorLabelRequirement{
						{
							Key:    v1.LabelTopologyZone,
							Values: []string{"zoneY"},
						},
					},
				},
				{
					MatchLabelExpressions: []v1.TopologySelectorLabelRequirement{
						{
							Key:    v1.LabelTopologyZone,
							Values: []string{"zoneZ"},
						},
					},
				},
			},
			Reject:        false,
			ExpectedZones: "zoneX,zoneY,zoneZ",
		},

		// 从 AllowedTopologies 选择 zone [Pass]
		// [1] Node 为 nil
		// [2] 未指定 Zone/Zones 参数
		// [3] AllowedTopologies 多个 term
		// [4] 指定了 ReplicaCount
		// [5] ZonesWithNodes 无关
		{
			Name:         "Nil_Node_and_Multiple_Allowed_Topology_terms_Match",
			Node:         nil,
			ReplicaCount: 2,
			AllowedTopologies: []v1.TopologySelectorTerm{
				{
					MatchLabelExpressions: []v1.TopologySelectorLabelRequirement{
						{
							Key:    v1.LabelTopologyZone,
							Values: []string{"zoneX"},
						},
					},
				},
				{
					MatchLabelExpressions: []v1.TopologySelectorLabelRequirement{
						{
							Key:    v1.LabelTopologyZone,
							Values: []string{"zoneY"},
						},
					},
				},
			},
			Reject:              false,
			ExpectSpecificZones: true,
			ExpectedZones:       "zoneX,zoneY",
		},
	}

	for _, test := range tests {
		var zonesParameter, zonesWithNodes sets.String
		var err error

		if test.Zones != "" {
			zonesParameter, err = ZonesToSet(test.Zones)
			if err != nil {
				t.Errorf("Could not convert Zones to a set: %s. This is a test error %s", test.Zones, test.Name)
				continue
			}
		}

		if test.ZonesWithNodes != "" {
			zonesWithNodes, err = ZonesToSet(test.ZonesWithNodes)
			if err != nil {
				t.Errorf("Could not convert specified ZonesWithNodes to a set: %s. This is a test error %s", test.ZonesWithNodes, test.Name)
				continue
			}
		}

		zones, err := SelectZonesForVolume(test.ZonePresent, test.ZonesPresent, test.Zone, zonesParameter, zonesWithNodes, test.Node, test.AllowedTopologies, test.Name, test.ReplicaCount)

		if test.Reject && err == nil {
			t.Errorf("Unexpected zones from SelectZonesForVolume in %s. Zones: %v", test.Name, zones)
			continue
		}

		if !test.Reject {
			if err != nil {
				t.Errorf("Unexpected error from SelectZonesForVolume in %s; Error: %v", test.Name, err)
				continue
			}

			if uint32(zones.Len()) != test.ReplicaCount {
				t.Errorf("Number of elements in returned zones %v does not equal replica count %d in %s", zones, test.ReplicaCount, test.Name)
				continue
			}

			expectedZones, err := ZonesToSet(test.ExpectedZones)
			if err != nil {
				t.Errorf("Could not convert ExpectedZones to a set: %v. This is a test error in %s", test.ExpectedZones, test.Name)
				continue
			}

			if test.ExpectSpecificZones && !zones.Equal(expectedZones) {
				t.Errorf("Expected zones %v does not match obtained zones %v in %s", expectedZones, zones, test.Name)
				continue
			}

			if test.ExpectSpecificZone && !zones.Has(test.ExpectedZone) {
				t.Errorf("Expected zone %s not found in obtained zones %v in %s", test.ExpectedZone, zones, test.Name)
				continue
			}

			if !expectedZones.IsSuperset(zones) {
				t.Errorf("Obtained zones %v not subset of expectedZones %v in %s", zones, expectedZones, test.Name)
			}
		}
	}
}

// TestChooseZonesForVolumeIncludingZone 验证“必须包含指定 zone”时的选择逻辑：
// ZoneToInclude 必须在候选集合内，副本数为 0 或候选 zone 不足都报错；
// 选中结果必须包含该 zone，其余 zone 仍按散列挑选。
func TestChooseZonesForVolumeIncludingZone(t *testing.T) {
	tests := []struct {
		// 传给 chooseZonesForVolumeIncludingZone 的参数
		Name          string
		ReplicaCount  uint32
		Zones         string
		ZoneToInclude string
		// 对 chooseZonesForVolumeIncludingZone 返回 zone 集合的期望
		Reject              bool   // expect error due to validation failing
		ExpectSpecificZones bool   // expect set of returned zones to be equal to ExpectedZones (rather than subset of ExpectedZones)
		ExpectedZones       string // set of zones that is a superset of returned zones or equal to returned zones (if ExpectSpecificZones is set)
		ExpectSpecificZone  bool   // expect set of returned zones to include ExpectedZone as a member
		ExpectedZone        string // zone that should be a member of returned zones
	}{
		// 反面用例

		// 指定的 zone 不足以满足 ReplicaCount [Fail]
		{
			Name:         "Too_Few_Zones",
			ReplicaCount: 2,
			Zones:        "zoneX",
			Reject:       true,
		},

		// ReplicaCount 非法 [Fail]
		{
			Name:         "Zero_Replica_Count",
			ReplicaCount: 0,
			Zones:        "zoneY,zoneZ",
			Reject:       true,
		},

		// ZoneToInclude 非法 [Fail]
		{
			Name:          "ZoneToInclude_Not_In_Zones_Single_replica_Dual_zones",
			ReplicaCount:  1,
			ZoneToInclude: "zoneX",
			Zones:         "zoneY,zoneZ",
			Reject:        true,
		},

		// ZoneToInclude 非法 [Fail]
		{
			Name:          "ZoneToInclude_Not_In_Zones_Single_replica_Single_zone",
			ReplicaCount:  1,
			ZoneToInclude: "zoneX",
			Zones:         "zoneY",
			Reject:        true,
		},

		// ZoneToInclude 非法 [Fail]
		{
			Name:          "ZoneToInclude_Not_In_Zones_Dual_replica_Multiple_zones",
			ReplicaCount:  2,
			ZoneToInclude: "zoneX",
			Zones:         "zoneY,zoneZ,zoneW",
			Reject:        true,
		},

		// 正面用例

		// 从 Zones 中任选一个 zone
		{
			Name:          "No_zones_to_include_and_Single_replica_and_Superset",
			ReplicaCount:  1,
			Zones:         "zoneX,zoneY,zoneZ",
			Reject:        false,
			ExpectedZones: "zoneX,zoneY,zoneZ",
		},

		// 从 Zones 中任选两个 zone
		{
			Name:          "No_zones_to_include_and_Dual_replicas_and_Superset",
			ReplicaCount:  2,
			Zones:         "zoneW,zoneX,zoneY,zoneZ",
			Reject:        false,
			ExpectedZones: "zoneW,zoneX,zoneY,zoneZ",
		},

		// 选出 Zones 中的两个 zone
		{
			Name:                "No_zones_to_include_and_Dual_replicas_and_Match",
			ReplicaCount:        2,
			Zones:               "zoneX,zoneY",
			Reject:              false,
			ExpectSpecificZones: true,
			ExpectedZones:       "zoneX,zoneY",
		},

		// 只选 ZoneToInclude 中的 zone（忽略其他）
		{
			Name:                "Include_zone_and_Single_replica_and_Match",
			ReplicaCount:        1,
			Zones:               "zoneW,zoneX,zoneY,zoneZ",
			ZoneToInclude:       "zoneX",
			Reject:              false,
			ExpectSpecificZone:  true,
			ExpectedZone:        "zoneX",
			ExpectSpecificZones: true,
			ExpectedZones:       "zoneX",
		},

		// 只选 ZoneToInclude 中的 zone（忽略其他）
		{
			Name:                "Include_zone_and_single_replica_and_Match",
			ReplicaCount:        1,
			Zones:               "zoneX",
			ZoneToInclude:       "zoneX",
			Reject:              false,
			ExpectSpecificZone:  true,
			ExpectedZone:        "zoneX",
			ExpectSpecificZones: true,
			ExpectedZones:       "zoneX",
		},

		// 一个来自 Zones，另一个来自 ZoneToInclude
		{
			Name:               "Include_zone_and_dual_replicas_and_Superset",
			ReplicaCount:       2,
			Zones:              "zoneW,zoneX,zoneY,zoneZ",
			ZoneToInclude:      "zoneX",
			Reject:             false,
			ExpectSpecificZone: true,
			ExpectedZone:       "zoneX",
			ExpectedZones:      "zoneW,zoneX,zoneY,zoneZ",
		},

		// 一个来自 Zones，另一个来自 ZoneToInclude
		{
			Name:                "Include_zone_and_dual_replicas_and_Match",
			ReplicaCount:        2,
			Zones:               "zoneX,zoneY",
			ZoneToInclude:       "zoneX",
			Reject:              false,
			ExpectSpecificZone:  true,
			ExpectedZone:        "zoneX",
			ExpectSpecificZones: true,
			ExpectedZones:       "zoneX,zoneY",
		},
	}
	for _, test := range tests {
		zonesParameter, err := ZonesToSet(test.Zones)
		if err != nil {
			t.Errorf("Could not convert Zones to a set: %s. This is a test error %s", test.Zones, test.Name)
			continue
		}

		zones, err := chooseZonesForVolumeIncludingZone(zonesParameter, test.Name, test.ZoneToInclude, test.ReplicaCount)
		if test.Reject && err == nil {
			t.Errorf("Unexpected zones from chooseZonesForVolumeIncludingZone in %s. Zones: %v", test.Name, zones)
			continue
		}
		if !test.Reject {
			if err != nil {
				t.Errorf("Unexpected error from chooseZonesForVolumeIncludingZone in %s; Error: %v", test.Name, err)
				continue
			}

			if uint32(zones.Len()) != test.ReplicaCount {
				t.Errorf("Number of elements in returned zones %v does not equal replica count %d in %s", zones, test.ReplicaCount, test.Name)
				continue
			}

			expectedZones, err := ZonesToSet(test.ExpectedZones)
			if err != nil {
				t.Errorf("Could not convert ExpectedZones to a set: %v. This is a test error in %s", test.ExpectedZones, test.Name)
				continue
			}

			if test.ExpectSpecificZones && !zones.Equal(expectedZones) {
				t.Errorf("Expected zones %v does not match obtained zones %v in %s", expectedZones, zones, test.Name)
				continue
			}

			if test.ExpectSpecificZone && !zones.Has(test.ExpectedZone) {
				t.Errorf("Expected zone %s not found in obtained zones %v in %s", test.ExpectedZone, zones, test.Name)
				continue
			}

			if !expectedZones.IsSuperset(zones) {
				t.Errorf("Obtained zones %v not subset of expectedZones %v in %s", zones, expectedZones, test.Name)
			}
		}
	}
}

// checkFnv32 固定 FNV-32 散列值。用例依赖该散列来推导 zone 偏移，
// 若散列实现或输入编码改变，这里会第一时间失败。
func checkFnv32(t *testing.T, s string, expected uint32) {
	h := fnv.New32()
	h.Write([]byte(s))
	h.Sum32()

	if h.Sum32() != expected {
		t.Fatalf("hash of %q was %v, expected %v", s, h.Sum32(), expected)
	}
}
