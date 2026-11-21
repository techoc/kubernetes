/*
Copyright 2018 The Kubernetes Authors.

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

package validation

import (
	"testing"

	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/kubernetes/pkg/apis/core"
	"k8s.io/kubernetes/pkg/apis/node"
	"k8s.io/utils/ptr"

	"github.com/stretchr/testify/assert"
)

// TestValidateRuntimeClass 验证创建时对不同 name/handler 组合的校验结果：
// 名称与 handler 都必须合法，且 handler 不能为空。
func TestValidateRuntimeClass(t *testing.T) {
	// 表驱动用例：expectError 表示期望校验返回错误。
	tests := []struct {
		name        string
		rc          node.RuntimeClass
		expectError bool
	}{{
		// 对象名包含非法字符，应被元数据校验拒绝。
		name:        "invalid name",
		expectError: true,
		rc: node.RuntimeClass{
			ObjectMeta: metav1.ObjectMeta{Name: "&!@#"},
			Handler:    "foo",
		},
	}, {
		// handler 名称包含非法字符，应被 DNS Label 校验拒绝。
		name:        "invalid Handler name",
		expectError: true,
		rc: node.RuntimeClass{
			ObjectMeta: metav1.ObjectMeta{Name: "foo"},
			Handler:    "&@#$",
		},
	}, {
		// handler 为空：必填字段缺失必须报错。
		name:        "invalid empty RuntimeClass",
		expectError: true,
		rc: node.RuntimeClass{
			ObjectMeta: metav1.ObjectMeta{Name: "empty"},
		},
	}, {
		// 合法的对象名与 handler：连字符在 DNS Label 中是允许的。
		name:        "valid Handler",
		expectError: false,
		rc: node.RuntimeClass{
			ObjectMeta: metav1.ObjectMeta{Name: "foo"},
			Handler:    "bar-baz",
		},
	}}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			errs := ValidateRuntimeClass(&test.rc)
			if test.expectError {
				assert.NotEmpty(t, errs)
			} else {
				assert.Empty(t, errs)
			}
		})
	}
}

// TestValidateRuntimeUpdate 验证更新时的不变性约束：
// 元数据可改（除命名空间等不可变字段），但 handler 一旦设定便不可修改。
func TestValidateRuntimeUpdate(t *testing.T) {
	// old 是各用例共用的基线对象。
	old := node.RuntimeClass{
		ObjectMeta: metav1.ObjectMeta{Name: "foo"},
		Handler:    "bar",
	}
	tests := []struct {
		name        string
		expectError bool
		old, new    node.RuntimeClass
	}{{
		// 仅修改标签属于合法的元数据更新。
		name: "valid metadata update",
		old:  old,
		new: node.RuntimeClass{
			ObjectMeta: metav1.ObjectMeta{
				Name:   "foo",
				Labels: map[string]string{"foo": "bar"},
			},
			Handler: "bar",
		},
	}, {
		// 修改 namespace 属于非法的元数据更新，namespace 是不可变字段。
		name:        "invalid metadata update",
		expectError: true,
		old:         old,
		new: node.RuntimeClass{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "empty",
				Namespace: "somethingelse", // immutable
			},
			Handler: "bar",
		},
	}, {
		// 修改 handler 必须被拒绝，因为运行时句柄不可变更。
		name:        "invalid Handler update",
		expectError: true,
		old:         old,
		new: node.RuntimeClass{
			ObjectMeta: metav1.ObjectMeta{Name: "foo"},
			Handler:    "somethingelse",
		},
	}}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			// 统一设置 ResourceVersion，避免每个用例重复书写，
			// 让用例聚焦于被测试的字段变化本身。
			test.old.ObjectMeta.ResourceVersion = "1"
			test.new.ObjectMeta.ResourceVersion = "1"

			errs := ValidateRuntimeClassUpdate(&test.new, &test.old)
			if test.expectError {
				assert.NotEmpty(t, errs)
			} else {
				assert.Empty(t, errs)
			}
		})
	}
}

// TestValidateOverhead 验证 overhead 的资源校验：
// 标准 CPU/内存资源应通过，自定义域名的非法资源名应被拒绝。
func TestValidateOverhead(t *testing.T) {
	// 合法开销用例：标准资源名与数量均应通过校验。
	successCase := []struct {
		Name     string
		overhead *node.Overhead
	}{{
		Name: "Overhead with valid cpu and memory resources",
		overhead: &node.Overhead{
			PodFixed: core.ResourceList{
				core.ResourceName(core.ResourceCPU):    resource.MustParse("10"),
				core.ResourceName(core.ResourceMemory): resource.MustParse("10G"),
			},
		},
	}}

	for _, tc := range successCase {
		rc := &node.RuntimeClass{
			ObjectMeta: metav1.ObjectMeta{Name: "foo"},
			Handler:    "bar",
			Overhead:   tc.overhead,
		}
		if errs := ValidateRuntimeClass(rc); len(errs) != 0 {
			t.Errorf("%q unexpected error: %v", tc.Name, errs)
		}
	}

	// 非法开销用例：非标准的资源名（my.org）不是合法的资源名称，应被拒绝。
	errorCase := []struct {
		Name     string
		overhead *node.Overhead
	}{{
		Name: "Invalid Resources",
		overhead: &node.Overhead{
			PodFixed: core.ResourceList{
				core.ResourceName("my.org"): resource.MustParse("10m"),
			},
		},
	}}
	for _, tc := range errorCase {
		rc := &node.RuntimeClass{
			ObjectMeta: metav1.ObjectMeta{Name: "foo"},
			Handler:    "bar",
			Overhead:   tc.overhead,
		}
		if errs := ValidateRuntimeClass(rc); len(errs) == 0 {
			t.Errorf("%q expected error", tc.Name)
		}
	}
}

// TestValidateScheduling 验证调度约束的校验，覆盖合法配置、空配置、
// nodeSelector 非法、toleration 非法与重复、以及多处错误同时出现的场景。
func TestValidateScheduling(t *testing.T) {
	tests := []struct {
		name       string
		scheduling *node.Scheduling
		expectErrs int
	}{{
		// 合法的 selector 与 toleration 组合，不应产生错误。
		name: "valid scheduling",
		scheduling: &node.Scheduling{
			NodeSelector: map[string]string{"valid": "yes"},
			Tolerations: []core.Toleration{{
				Key:      "valid",
				Operator: core.TolerationOpExists,
				Effect:   core.TaintEffectNoSchedule,
			}},
		},
	}, {
		// 空的 Scheduling 对象也是合法的，不应产生错误。
		name:       "empty scheduling",
		scheduling: &node.Scheduling{},
	}, {
		// nodeSelector 的 key 含非法字符，应恰好产生一个错误。
		name: "invalid nodeSelector",
		scheduling: &node.Scheduling{
			NodeSelector: map[string]string{"not a valid key!!!": "nope"},
		},
		expectErrs: 1,
	}, {
		// 第二个 toleration 的 key 非法。
		// 只应报告非法项这一个错误，第一个合法 toleration 不应受影响。
		name: "invalid toleration",
		scheduling: &node.Scheduling{
			Tolerations: []core.Toleration{{
				Key:      "valid",
				Operator: core.TolerationOpExists,
				Effect:   core.TaintEffectNoSchedule,
			}, {
				Key:      "not a valid key!!!",
				Operator: core.TolerationOpExists,
				Effect:   core.TaintEffectNoSchedule,
			}},
		},
		expectErrs: 1,
	}, {
		// 两条 toleration 的列表键相同，仅 TolerationSeconds 不同：
		// 时长不是列表键，因此仍算重复，应报告一个重复错误。
		name: "duplicate tolerations",
		scheduling: &node.Scheduling{
			Tolerations: []core.Toleration{{
				Key:               "valid",
				Operator:          core.TolerationOpExists,
				Effect:            core.TaintEffectNoExecute,
				TolerationSeconds: ptr.To[int64](5),
			}, {
				Key:               "valid",
				Operator:          core.TolerationOpExists,
				Effect:            core.TaintEffectNoExecute,
				TolerationSeconds: ptr.To[int64](10),
			}},
		},
		expectErrs: 1,
	}, {
		// selector 与 toleration 同时非法，应累计两个错误，
		// 验证校验器不会在遇到第一个错误后提前返回。
		name: "invalid scheduling",
		scheduling: &node.Scheduling{
			NodeSelector: map[string]string{"not a valid key!!!": "nope"},
			Tolerations: []core.Toleration{{
				Key:      "valid",
				Operator: core.TolerationOpExists,
				Effect:   core.TaintEffectNoSchedule,
			}, {
				Key:      "not a valid toleration key!!!",
				Operator: core.TolerationOpExists,
				Effect:   core.TaintEffectNoSchedule,
			}},
		},
		expectErrs: 2,
	}}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			rc := &node.RuntimeClass{
				ObjectMeta: metav1.ObjectMeta{Name: "foo"},
				Handler:    "bar",
				Scheduling: test.scheduling,
			}
			assert.Len(t, ValidateRuntimeClass(rc), test.expectErrs)
		})
	}
}
