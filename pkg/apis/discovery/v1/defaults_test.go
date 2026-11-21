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

package v1_test

// 导入依赖：testing 提供测试框架，go-cmp 用于输出结构化的差异报告。
import (
	"testing"

	// go-cmp：生成人可读的 expected/actual 差异，便于定位默认化结果哪里出错。
	"github.com/google/go-cmp/cmp"
	// core/v1：复用 ProtocolTCP/ProtocolUDP 常量，与 API 定义保持一致。
	v1 "k8s.io/api/core/v1"
	// discovery/v1：被测对象 EndpointSlice/EndpointPort 的定义包。
	discovery "k8s.io/api/discovery/v1"
	// equality.Semantic.DeepEqual：按语义比较对象，忽略 map/slice 的顺序差异。
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	// legacyscheme：提供全局 Scheme，Default 会按注册的顺序执行默认化函数。
	"k8s.io/kubernetes/pkg/api/legacyscheme"
	// 空白导入 install 包以触发 init() 注册 discovery 各版本的默认化函数。
	_ "k8s.io/kubernetes/pkg/apis/discovery/install"
	// ptr.To：便捷地构造指向基本类型值的指针，用于表达“字段已显式设置”。
	"k8s.io/utils/ptr"
)

// TestSetDefaultEndpointPort 验证 EndpointSlice 中端口的默认化行为。
// 核心断言：nil 的 Name/Protocol 会被补齐默认值，已设置的字段不被覆盖。
func TestSetDefaultEndpointPort(t *testing.T) {
	// 模拟字段被显式设置为空串的场景，用于区分“未设置(nil)”和“设置为空值”。
	emptyStr := ""
	// 已设置的端口名，验证默认化不会覆盖用户显式提供的值。
	fooStr := "foo"
	// 指针变量便于在测试用例中直接取地址，表达 TCP 协议已显式指定。
	protoTCP := v1.ProtocolTCP
	// UDP 协议，用于验证非默认协议会被原样保留。
	protoUDP := v1.ProtocolUDP

	// 表驱动测试：每个命名用例包含输入对象和期望的默认化结果。
	tests := map[string]struct {
		// original：待默认化的输入对象，会被就地修改。
		original *discovery.EndpointSlice
		// expected：默认化完成后应当得到的完整对象结构。
		// 注释放在字段列表上方，避免打断 gofmt 对相邻字段的对齐。
		expected *discovery.EndpointSlice
	}{
		// 场景一：只设置端口号，Name 和 Protocol 缺省，期望被补为默认值。
		"should set appropriate defaults": {
			// 输入仅含 Port=80，Name/Protocol 均为 nil，触发默认化逻辑。
			original: &discovery.EndpointSlice{Ports: []discovery.EndpointPort{{
				// ptr.To[int32](80)：显式指定端口号，不影响其他字段的默认化。
				Port: ptr.To[int32](80),
			}}},
			// 期望结果：Name 补为空串(默认端口名)、Protocol 补为 TCP(默认协议)，Port 保持 80。
			// 注释统一放在字段上方，保持字段行相邻以便 gofmt 对齐。
			expected: &discovery.EndpointSlice{
				Ports: []discovery.EndpointPort{{
					Name:     &emptyStr,
					Protocol: &protoTCP,
					Port:     ptr.To[int32](80),
				}},
			},
		},
		// 场景二：Name 与 Protocol 均已设置，默认化不应覆盖已有值。
		"should not overwrite values with defaults when set": {
			// 输入显式指定 Name=foo、Protocol=UDP，Port 缺省（保持 nil）。
			original: &discovery.EndpointSlice{
				// 端口字段均显式设置：Name=foo、Protocol=UDP；若默认化误覆盖，比较会失败。
				Ports: []discovery.EndpointPort{{
					Name:     &fooStr,
					Protocol: &protoUDP,
				}},
			},
			// 期望结果与输入完全一致，证明默认化对已设置字段是幂等的。
			// 注释放在字段上方，避免打断字段对齐。
			expected: &discovery.EndpointSlice{
				Ports: []discovery.EndpointPort{{
					Name:     &fooStr,
					Protocol: &protoUDP,
				}},
			},
		},
	}

	// 遍历所有用例；map 顺序随机，但各用例互不依赖，顺序无关紧要。
	for _, test := range tests {
		// 直接拿着 original 的指针做默认化，Scheme.Default 会就地修改它。
		actual := test.original
		// 期望值单独取出来，避免与被测对象共享同一份数据。
		expected := test.expected
		// 触发默认化：Scheme 会按注册顺序调用 EndpointPort 的默认化函数。
		legacyscheme.Scheme.Default(actual)
		// 语义深比较：失败时用 cmp.Diff 打印详细差异，方便定位未补默认值或误覆盖。
		if !apiequality.Semantic.DeepEqual(actual, expected) {
			// 输出 expected 与 actual 的差异，left 为期望值、right 为实际值。
			t.Error(cmp.Diff(expected, actual))
			// 每个用例结束后都会比较，断言失败不中断，便于一次看到所有失败用例。
		}
	}
}
