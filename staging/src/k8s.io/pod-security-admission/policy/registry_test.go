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

package policy

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/pod-security-admission/api"
)

// TestCheckRegistry 用伪造的检查函数验证版本展开与覆盖逻辑：
// 每个检查按 "<ID>:<版本>" 回报，断言结果列表即可看出各版本实际生效的是哪条规则。
func TestCheckRegistry(t *testing.T) {
	checks := []Check{
		generateCheck("a", api.LevelBaseline, []string{"v1.0"}),
		generateCheck("b", api.LevelBaseline, []string{"v1.10"}),
		generateCheck("c", api.LevelBaseline, []string{"v1.0", "v1.5", "v1.10"}),
		generateCheck("d", api.LevelBaseline, []string{"v1.11", "v1.15", "v1.20"}),
		generateCheck("e", api.LevelRestricted, []string{"v1.0"}),
		generateCheck("f", api.LevelRestricted, []string{"v1.12", "v1.16", "v1.21"}),
		withOverrides(generateCheck("g", api.LevelRestricted, []string{"v1.10"}), []CheckID{"a"}),
		withOverrides(generateCheck("h", api.LevelRestricted, []string{"v1.0"}), []CheckID{"b"}),
	}
	// 同一个检查在不同版本覆盖不同 baseline 检查，验证覆盖关系按版本生效而非全局生效。
	multiOverride := generateCheck("i", api.LevelRestricted, []string{"v1.10", "v1.21"})
	multiOverride.Versions[0].OverrideCheckIDs = []CheckID{"c"}
	multiOverride.Versions[1].OverrideCheckIDs = []CheckID{"d"}
	checks = append(checks, multiOverride)

	reg, err := NewEvaluator(checks, nil)
	require.NoError(t, err)

	levelCases := []registryTestCase{
		{api.LevelPrivileged, "v1.0", nil},
		{api.LevelPrivileged, "latest", nil},
		{api.LevelBaseline, "v1.0", []string{"a:v1.0", "c:v1.0"}},
		{api.LevelBaseline, "v1.4", []string{"a:v1.0", "c:v1.0"}},
		{api.LevelBaseline, "v1.5", []string{"a:v1.0", "c:v1.5"}},
		{api.LevelBaseline, "v1.10", []string{"a:v1.0", "b:v1.10", "c:v1.10"}},
		{api.LevelBaseline, "v1.11", []string{"a:v1.0", "b:v1.10", "c:v1.10", "d:v1.11"}},
		{api.LevelBaseline, "latest", []string{"a:v1.0", "b:v1.10", "c:v1.10", "d:v1.20"}},
		{api.LevelRestricted, "v1.0", []string{"a:v1.0", "c:v1.0", "e:v1.0", "h:v1.0"}},
		{api.LevelRestricted, "v1.4", []string{"a:v1.0", "c:v1.0", "e:v1.0", "h:v1.0"}},
		{api.LevelRestricted, "v1.5", []string{"a:v1.0", "c:v1.5", "e:v1.0", "h:v1.0"}},
		{api.LevelRestricted, "v1.10", []string{"e:v1.0", "g:v1.10", "h:v1.0", "i:v1.10"}},
		{api.LevelRestricted, "v1.11", []string{"d:v1.11", "e:v1.0", "g:v1.10", "h:v1.0", "i:v1.10"}},
		{api.LevelRestricted, "latest", []string{"c:v1.10", "e:v1.0", "f:v1.21", "g:v1.10", "h:v1.0", "i:v1.21"}},
		{api.LevelRestricted, "v1.10000", []string{"c:v1.10", "e:v1.0", "f:v1.21", "g:v1.10", "h:v1.0", "i:v1.21"}},
	}
	// 远超缓存上限的版本应回落到已知最新结果，而不是查表落空。
	for _, test := range levelCases {
		test.Run(t, reg)
	}
}

// TestCheckRegistry_NoBaseline 只注册 restricted 检查，仍允许它声明对 baseline ID 的覆盖。
// 这样插件式场景里不存在的 baseline 检查不会被误当成校验错误。
func TestCheckRegistry_NoBaseline(t *testing.T) {
	checks := []Check{
		generateCheck("e", api.LevelRestricted, []string{"v1.0"}),
		generateCheck("f", api.LevelRestricted, []string{"v1.12", "v1.16", "v1.21"}),
		withOverrides(generateCheck("g", api.LevelRestricted, []string{"v1.10"}), []CheckID{"a"}),
		withOverrides(generateCheck("h", api.LevelRestricted, []string{"v1.0"}), []CheckID{"b"}),
	}

	reg, err := NewEvaluator(checks, nil)
	require.NoError(t, err)

	levelCases := []registryTestCase{
		{api.LevelPrivileged, "v1.0", nil},
		{api.LevelPrivileged, "latest", nil},
		{api.LevelBaseline, "v1.0", nil},
		{api.LevelBaseline, "v1.10", nil},
		{api.LevelBaseline, "latest", nil},
		{api.LevelRestricted, "v1.0", []string{"e:v1.0", "h:v1.0"}},
		{api.LevelRestricted, "v1.10", []string{"e:v1.0", "g:v1.10", "h:v1.0"}},
		{api.LevelRestricted, "latest", []string{"e:v1.0", "f:v1.21", "g:v1.10", "h:v1.0"}},
		{api.LevelRestricted, "v1.10000", []string{"e:v1.0", "f:v1.21", "g:v1.10", "h:v1.0"}},
	}
	for _, test := range levelCases {
		test.Run(t, reg)
	}
}

// TestCheckRegistry_NoRestricted 没有 restricted 检查时，restricted 档应与 baseline 完全一致。
// 这是 restricted 作为 baseline 超集但不强制要求新增检查的语义保证。
func TestCheckRegistry_NoRestricted(t *testing.T) {
	checks := []Check{
		generateCheck("a", api.LevelBaseline, []string{"v1.0"}),
		generateCheck("b", api.LevelBaseline, []string{"v1.10"}),
		generateCheck("c", api.LevelBaseline, []string{"v1.0", "v1.5", "v1.10"}),
		generateCheck("d", api.LevelBaseline, []string{"v1.11", "v1.15", "v1.20"}),
	}

	reg, err := NewEvaluator(checks, nil)
	require.NoError(t, err)

	levelCases := []registryTestCase{
		{api.LevelBaseline, "v1.0", []string{"a:v1.0", "c:v1.0"}},
		{api.LevelBaseline, "v1.4", []string{"a:v1.0", "c:v1.0"}},
		{api.LevelBaseline, "v1.5", []string{"a:v1.0", "c:v1.5"}},
		{api.LevelBaseline, "v1.10", []string{"a:v1.0", "b:v1.10", "c:v1.10"}},
		{api.LevelBaseline, "v1.11", []string{"a:v1.0", "b:v1.10", "c:v1.10", "d:v1.11"}},
		{api.LevelBaseline, "latest", []string{"a:v1.0", "b:v1.10", "c:v1.10", "d:v1.20"}},
	}
	for _, test := range levelCases {
		test.Run(t, reg)
		// restricted 的结果应与 baseline 完全一致。
		restrictedTest := test
		restrictedTest.level = api.LevelRestricted
		restrictedTest.Run(t, reg)
	}
}

// TestCheckRegistry_Empty 空检查表也必须能构造出可用的 Evaluator，
// 避免完全禁用策略检查时准入控制器启动失败。
func TestCheckRegistry_Empty(t *testing.T) {
	reg, err := NewEvaluator(nil, nil)
	require.NoError(t, err)

	levelCases := []registryTestCase{
		{api.LevelPrivileged, "latest", nil},
		{api.LevelBaseline, "latest", nil},
		{api.LevelRestricted, "latest", nil},
	}
	for _, test := range levelCases {
		test.Run(t, reg)
		// restricted 的结果应与 baseline 完全一致。
		restrictedTest := test
		restrictedTest.level = api.LevelRestricted
		restrictedTest.Run(t, reg)
	}
}

// registryTestCase 描述一次评估请求的输入与期望命中的检查列表。
type registryTestCase struct {
	level           api.Level
	version         string
	expectedReasons []string
}

// Run 把期望列表与实际检查结果的 ForbiddenReason 逐一比对，
// 用伪造检查回报的 "<ID>:<版本>" 字符串即可定位展开或覆盖是否出错。
func (tc *registryTestCase) Run(t *testing.T, registry Evaluator) {
	t.Run(fmt.Sprintf("%s:%s", tc.level, tc.version), func(t *testing.T) {
		results := registry.EvaluatePod(api.LevelVersion{Level: tc.level, Version: versionOrPanic(tc.version)}, nil, nil)

		// set 从结果中提取 ForbiddenReasons，便于表格断言。
		var actualReasons []string
		for _, result := range results {
			actualReasons = append(actualReasons, result.ForbiddenReason)
		}
		assert.Equal(t, tc.expectedReasons, actualReasons)
	})
}

// generateCheck 造出一批检查，每个版本的检查回报 "<ID>:<版本>"。
// 这样测试结果能直接显示是哪个检查的哪个实现被启用。
func generateCheck(id CheckID, level api.Level, versions []string) Check {
	c := Check{
		ID:    id,
		Level: level,
	}
	for _, ver := range versions {
		v := versionOrPanic(ver) // 复制版本值，避免闭包捕获循环变量。
		c.Versions = append(c.Versions, VersionedCheck{
			MinimumVersion: v,
			CheckPod: func(_ *metav1.ObjectMeta, _ *corev1.PodSpec) CheckResult {
				return CheckResult{
					ForbiddenReason: fmt.Sprintf("%s:%s", id, v),
				}
			},
		})
	}
	return c
}

// withOverrides 给检查的所有版本统一加覆盖列表，方便构造简单的覆盖用例。
func withOverrides(c Check, overrides []CheckID) Check {
	for i := range c.Versions {
		c.Versions[i].OverrideCheckIDs = overrides
	}
	return c
}

// versionOrPanic 只用于测试：版本串写错属于用例自身错误，直接 panic 便于定位。
func versionOrPanic(v string) api.Version {
	ver, err := api.ParseVersion(v)
	if err != nil {
		panic(err)
	}
	return ver
}
