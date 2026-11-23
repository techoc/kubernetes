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

package topology

import (
	"testing"

	"k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// Test_GetZoneKey 覆盖 GetZoneKey 的两组关键行为：旧 beta 标签与新 GA 标签共存时的
// 优先级，以及用空字符拼接以避开标签值中可能包含的冒号。期望值里直接写入 "\x00"，
// 是为了把“分隔符不会与内容混淆”这一约定固化成可执行的断言。
func Test_GetZoneKey(t *testing.T) {
	tests := []struct {
		// name 是用例描述，会作为子测试名。
		name string
		// node 是待解析的节点对象。
		node *v1.Node
		// zone 是期望得到的 zone key。
		zone string
	}{
		{
			// 没有任何标签时必须返回空串，而不是拼出 ":\x00:" 这样的占位符。
			name: "has no zone or region keys",
			node: &v1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{},
				},
			},
			zone: "",
		},
		{
			// 只有旧标签时仍要能用：存量对象上的 beta 标签会长期存在。
			name: "has beta zone and region keys",
			node: &v1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						v1.LabelFailureDomainBetaZone:   "zone1",
						v1.LabelFailureDomainBetaRegion: "region1",
					},
				},
			},
			zone: "region1:\x00:zone1",
		},
		{
			// 只有新 GA 标签时也要能解析，保证升级后的节点能正常计算 zone key。
			name: "has GA zone and region keys",
			node: &v1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						v1.LabelTopologyZone:   "zone1",
						v1.LabelTopologyRegion: "region1",
					},
				},
			},
			zone: "region1:\x00:zone1",
		},
		{
			// 两套标签值一致时结果不变；同时验证重复配置不会让解析逻辑出错。
			name: "has both beta and GA zone and region keys",
			node: &v1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						v1.LabelTopologyZone:            "zone1",
						v1.LabelTopologyRegion:          "region1",
						v1.LabelFailureDomainBetaZone:   "zone1",
						v1.LabelFailureDomainBetaRegion: "region1",
					},
				},
			},
			zone: "region1:\x00:zone1",
		},
		{
			// 两套标签值不一致时，beta 标签优先。把这条钉死是为了让依赖旧标签的存量对象
			// 不会被新标签悄悄改变所属可用区，升级过程中保持身份稳定。
			name: "has both beta and GA zone and region keys, beta labels take precedent",
			node: &v1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						v1.LabelTopologyZone:            "zone1",
						v1.LabelTopologyRegion:          "region1",
						v1.LabelFailureDomainBetaZone:   "zone2",
						v1.LabelFailureDomainBetaRegion: "region2",
					},
				},
			},
			zone: "region2:\x00:zone2",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			zone := GetZoneKey(test.node)
			// 失败时同时打印实际值与期望值，避免只报“不一致”却看不出差异在哪。
			if zone != test.zone {
				t.Logf("actual zone key: %q", zone)
				t.Logf("expected zone key: %q", test.zone)
				t.Errorf("unexpected zone key")
			}
		})
	}
}
