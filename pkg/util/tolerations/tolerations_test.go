/*
Copyright 2017 The Kubernetes Authors.

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

package tolerations

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/util/validation/field"
	api "k8s.io/kubernetes/pkg/apis/core"
	"k8s.io/kubernetes/pkg/apis/core/validation"
	"k8s.io/utils/ptr"
)

var (
	tolerations = map[string]api.Toleration{
		"all": {Operator: api.TolerationOpExists},
		"all-nosched": {
			Operator: api.TolerationOpExists,
			Effect:   api.TaintEffectNoSchedule,
		},
		"all-noexec": {
			Operator: api.TolerationOpExists,
			Effect:   api.TaintEffectNoExecute,
		},
		"foo": {
			Key:      "foo",
			Operator: api.TolerationOpExists,
		},
		"foo-bar": {
			Key:      "foo",
			Operator: api.TolerationOpEqual,
			Value:    "bar",
		},
		"foo-nosched": {
			Key:      "foo",
			Operator: api.TolerationOpExists,
			Effect:   api.TaintEffectNoSchedule,
		},
		"foo-bar-nosched": {
			Key:      "foo",
			Operator: api.TolerationOpEqual,
			Value:    "bar",
			Effect:   api.TaintEffectNoSchedule,
		},
		"foo-baz-nosched": {
			Key:      "foo",
			Operator: api.TolerationOpEqual,
			Value:    "baz",
			Effect:   api.TaintEffectNoSchedule,
		},
		"faz-nosched": {
			Key:      "faz",
			Operator: api.TolerationOpExists,
			Effect:   api.TaintEffectNoSchedule,
		},
		"faz-baz-nosched": {
			Key:      "faz",
			Operator: api.TolerationOpEqual,
			Value:    "baz",
			Effect:   api.TaintEffectNoSchedule,
		},
		"foo-prefnosched": {
			Key:      "foo",
			Operator: api.TolerationOpExists,
			Effect:   api.TaintEffectPreferNoSchedule,
		},
		"foo-noexec": {
			Key:      "foo",
			Operator: api.TolerationOpExists,
			Effect:   api.TaintEffectNoExecute,
		},
		"foo-bar-noexec": {
			Key:      "foo",
			Operator: api.TolerationOpEqual,
			Value:    "bar",
			Effect:   api.TaintEffectNoExecute,
		},
		"foo-noexec-10": {
			Key:               "foo",
			Operator:          api.TolerationOpExists,
			Effect:            api.TaintEffectNoExecute,
			TolerationSeconds: ptr.To[int64](10),
		},
		"foo-noexec-0": {
			Key:               "foo",
			Operator:          api.TolerationOpExists,
			Effect:            api.TaintEffectNoExecute,
			TolerationSeconds: ptr.To[int64](0),
		},
		"foo-bar-noexec-10": {
			Key:               "foo",
			Operator:          api.TolerationOpEqual,
			Value:             "bar",
			Effect:            api.TaintEffectNoExecute,
			TolerationSeconds: ptr.To[int64](10),
		},
	}
)

// TestIsSuperset 验证 isSuperset 的超集判定：逐一检查每种容忍与其他容忍的包含关系。
func TestIsSuperset(t *testing.T) {
	tests := []struct {
		toleration string
		ss         []string // 该容忍应为这些容忍的超集
	}{{
		"all",
		[]string{"all-nosched", "all-noexec", "foo", "foo-bar", "foo-nosched", "foo-bar-nosched", "foo-baz-nosched", "faz-nosched", "faz-baz-nosched", "foo-prefnosched", "foo-noexec", "foo-bar-noexec", "foo-noexec-10", "foo-noexec-0", "foo-bar-noexec-10"},
	}, {
		"all-nosched",
		[]string{"foo-nosched", "foo-bar-nosched", "foo-baz-nosched", "faz-nosched", "faz-baz-nosched"},
	}, {
		"all-noexec",
		[]string{"foo-noexec", "foo-bar-noexec", "foo-noexec-10", "foo-noexec-0", "foo-bar-noexec-10"},
	}, {
		"foo",
		[]string{"foo-bar", "foo-nosched", "foo-bar-nosched", "foo-baz-nosched", "foo-prefnosched", "foo-noexec", "foo-bar-noexec", "foo-noexec-10", "foo-noexec-0", "foo-bar-noexec-10"},
	}, {
		"foo-bar",
		[]string{"foo-bar-nosched", "foo-bar-noexec", "foo-bar-noexec-10"},
	}, {
		"foo-nosched",
		[]string{"foo-bar-nosched", "foo-baz-nosched"},
	}, {
		"foo-bar-nosched",
		[]string{},
	}, {
		"faz-nosched",
		[]string{"faz-baz-nosched"},
	}, {
		"faz-baz-nosched",
		[]string{},
	}, {
		"foo-prenosched",
		[]string{},
	}, {
		"foo-noexec",
		[]string{"foo-noexec", "foo-bar-noexec", "foo-noexec-10", "foo-noexec-0", "foo-bar-noexec-10"},
	}, {
		"foo-bar-noexec",
		[]string{"foo-bar-noexec-10"},
	}, {
		"foo-noexec-10",
		[]string{"foo-noexec-0", "foo-bar-noexec-10"},
	}, {
		"foo-noexec-0",
		[]string{},
	}, {
		"foo-bar-noexec-10",
		[]string{},
	}}

	assertSuperset := func(t *testing.T, super, sub string) {
		assert.True(t, isSuperset(tolerations[super], tolerations[sub]),
			"%s should be a superset of %s", super, sub)
	}
	assertNotSuperset := func(t *testing.T, super, sub string) {
		assert.False(t, isSuperset(tolerations[super], tolerations[sub]),
			"%s should NOT be a superset of %s", super, sub)
	}

	for _, test := range tests {
		t.Run(test.toleration, func(t *testing.T) {
			for name := range tolerations {
				if name == test.toleration || slices.Contains(test.ss, name) {
					assertSuperset(t, test.toleration, name)
				} else {
					assertNotSuperset(t, test.toleration, name)
				}
			}
		})
	}
}

// TestVerifyAgainstWhitelist 验证白名单校验：输入容忍须被白名单覆盖，空输入或空白名单均视为通过。
func TestVerifyAgainstWhitelist(t *testing.T) {
	tests := []struct {
		testName  string
		input     []string
		whitelist []string
		expected  bool
	}{
		{
			testName:  "equal input and whitelist",
			input:     []string{"foo-bar-nosched", "foo-baz-nosched"},
			whitelist: []string{"foo-bar-nosched", "foo-baz-nosched"},
			expected:  true,
		},
		{
			testName:  "duplicate input allowed",
			input:     []string{"foo-bar-nosched", "foo-bar-nosched"},
			whitelist: []string{"foo-bar-nosched", "foo-baz-nosched"},
			expected:  true,
		},
		{
			testName:  "allow all",
			input:     []string{"foo-bar-nosched", "foo-bar-nosched"},
			whitelist: []string{"all"},
			expected:  true,
		},
		{
			testName:  "duplicate input forbidden",
			input:     []string{"foo-bar-nosched", "foo-bar-nosched"},
			whitelist: []string{"foo-baz-nosched"},
			expected:  false,
		},
		{
			testName:  "value mismatch",
			input:     []string{"foo-bar-nosched", "foo-baz-nosched"},
			whitelist: []string{"foo-baz-nosched"},
			expected:  false,
		},
		{
			testName:  "input does not exist in whitelist",
			input:     []string{"foo-bar-nosched"},
			whitelist: []string{"foo-baz-nosched"},
			expected:  false,
		},
		{
			testName:  "disjoint sets",
			input:     []string{"foo-bar"},
			whitelist: []string{"foo-nosched"},
			expected:  false,
		},
		{
			testName:  "empty whitelist",
			input:     []string{"foo-bar"},
			whitelist: []string{},
			expected:  true,
		},
		{
			testName:  "empty input",
			input:     []string{},
			whitelist: []string{"foo-bar"},
			expected:  true,
		},
	}

	for _, c := range tests {
		t.Run(c.testName, func(t *testing.T) {
			actual := VerifyAgainstWhitelist(getTolerations(c.input), getTolerations(c.whitelist))
			assert.Equal(t, c.expected, actual)
		})
	}
}

// TestMergeTolerations 验证合并结果：去除冗余、保留并集，结果按序比较。
func TestMergeTolerations(t *testing.T) {
	tests := []struct {
		name     string
		a, b     []string
		expected []string
	}{{
		name:     "disjoint",
		a:        []string{"foo-bar-nosched", "faz-baz-nosched", "foo-noexec-10"},
		b:        []string{"foo-prefnosched", "foo-baz-nosched"},
		expected: []string{"foo-bar-nosched", "faz-baz-nosched", "foo-noexec-10", "foo-prefnosched", "foo-baz-nosched"},
	}, {
		name:     "duplicate",
		a:        []string{"foo-bar-nosched", "faz-baz-nosched", "foo-noexec-10"},
		b:        []string{"foo-bar-nosched", "faz-baz-nosched", "foo-noexec-10"},
		expected: []string{"foo-bar-nosched", "faz-baz-nosched", "foo-noexec-10"},
	}, {
		name:     "merge redundant",
		a:        []string{"foo-bar-nosched", "foo-baz-nosched"},
		b:        []string{"foo-nosched", "faz-baz-nosched"},
		expected: []string{"foo-nosched", "faz-baz-nosched"},
	}, {
		name:     "merge all",
		a:        []string{"foo-bar-nosched", "foo-baz-nosched", "foo-noexec-10"},
		b:        []string{"all"},
		expected: []string{"all"},
	}, {
		name:     "merge into all",
		a:        []string{"all"},
		b:        []string{"foo-bar-nosched", "foo-baz-nosched", "foo-noexec-10"},
		expected: []string{"all"},
	}}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			actual := MergeTolerations(getTolerations(test.a), getTolerations(test.b))
			require.Len(t, actual, len(test.expected))
			for i, expect := range getTolerations(test.expected) {
				assert.Equal(t, expect, actual[i], "expected[%d] = %s", i, test.expected[i])
			}
		})
	}
}

// TestFuzzed 用固定随机种子生成大量合法容忍，验证白名单校验与合并的包含关系性质。
func TestFuzzed(t *testing.T) {
	r := rand.New(rand.NewSource(1234)) // 固定种子，避免测试偶发失败。

	const (
		allProbability               = 0.01 // 生成"容忍所有"的概率
		existsProbability            = 0.3
		tolerationSecondsProbability = 0.5
	)
	effects := []api.TaintEffect{"", api.TaintEffectNoExecute, api.TaintEffectNoSchedule, api.TaintEffectPreferNoSchedule}
	genToleration := func() api.Toleration {
		gen := api.Toleration{
			Effect: effects[r.Intn(len(effects))],
		}
		if r.Float32() < allProbability {
			gen = tolerations["all"]
			return gen
		}
		// 缩小 key/value 取值空间，增加碰撞概率。
		gen.Key = strings.Repeat("a", r.Intn(6)+1)
		if r.Float32() < existsProbability {
			gen.Operator = api.TolerationOpExists
		} else {
			gen.Operator = api.TolerationOpEqual
			gen.Value = strings.Repeat("b", r.Intn(6)+1)
		}
		if gen.Effect == api.TaintEffectNoExecute && r.Float32() < tolerationSecondsProbability {
			gen.TolerationSeconds = ptr.To[int64](r.Int63n(10))
		}
		// 确保只生成合法的容忍，避免把非法输入当作测试失败。
		require.NoError(t, validation.ValidateTolerations([]api.Toleration{gen}, field.NewPath(""), validation.PodValidationOptions{}).ToAggregate(), "%#v", gen)
		return gen
	}
	genTolerations := func() []api.Toleration {
		result := []api.Toleration{}
		for i := 0; i < r.Intn(10); i++ {
			result = append(result, genToleration())
		}
		return result
	}

	// 检查容忍是否为集合中某个容忍的子集（即被其覆盖）。
	isContained := func(toleration api.Toleration, set []api.Toleration) bool {
		for _, ss := range set {
			if isSuperset(ss, toleration) {
				return true
			}
		}
		return false
	}

	const iterations = 1000

	debugMsg := func(tolerations ...[]api.Toleration) string {
		str, err := json.Marshal(tolerations)
		if err != nil {
			return fmt.Sprintf("[ERR: %v] %v", err, tolerations)
		}
		return string(str)
	}
	t.Run("VerifyAgainstWhitelist", func(t *testing.T) {
		for i := 0; i < iterations; i++ {
			input := genTolerations()
			whitelist := append(genTolerations(), genToleration()) // 保证非空
			if VerifyAgainstWhitelist(input, whitelist) {
				for _, tol := range input {
					require.True(t, isContained(tol, whitelist), debugMsg(input, whitelist))
				}
			} else {
				uncontained := false
				for _, tol := range input {
					if !isContained(tol, whitelist) {
						uncontained = true
						break
					}
				}
				require.True(t, uncontained, debugMsg(input, whitelist))
			}
		}
	})

	t.Run("MergeTolerations", func(t *testing.T) {
		for i := 0; i < iterations; i++ {
			a := genTolerations()
			b := genTolerations()
			result := MergeTolerations(a, b)
			for _, tol := range append(a, b...) {
				require.True(t, isContained(tol, result), debugMsg(a, b, result))
			}
		}
	})
}

// getTolerations 按名称列表从预设表中取出对应的容忍。
func getTolerations(names []string) []api.Toleration {
	result := []api.Toleration{}
	for _, name := range names {
		result = append(result, tolerations[name])
	}
	return result
}
