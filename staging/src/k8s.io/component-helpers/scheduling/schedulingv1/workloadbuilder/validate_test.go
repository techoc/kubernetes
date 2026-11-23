/*
Copyright The Kubernetes Authors.

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

package workloadbuilder

import (
	"testing"

	schedulingv1alpha3 "k8s.io/api/scheduling/v1alpha3"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"k8s.io/klog/v2/ktesting"
)

// schedulingPath 模拟控制器 API 中调度配置块的实际字段位置（spec.scheduling）。
// 注入 Path 后，声明式校验的错误路径会带上这段前缀；未注入时错误路径只由 PathElements 组成。
var (
	schedulingPath = field.NewPath("spec", "scheduling")
)

// TestBuilderValidate 验证 Builder.Validate 的两个层次：声明式校验（building block 的结构约束）
// 和复杂校验（策略白名单、跨字段规则、输入 union、更新时的 ratchet 行为）。
// 每条用例断言错误的数量、类型和字段路径，因此不光检查“是否报错”，还检查错误是否精确落回
// 控制器记录的输入路径——这是这套校验对 API 用户真正有用的地方。
// 用例先由 NewBuilder 构造 builder，再调用 Validate；OldRoot 为 nil 表示创建，非 nil 表示更新。
func TestBuilderValidate(t *testing.T) {
	// ktesting 提供带测试日志的 context，供生成代码的校验函数使用。
	_, ctx := ktesting.NewTestContext(t)
	// 默认白名单同时放开两种策略和两种中断模式，减少用例里的重复配置。
	allPolicies := []SchedulingPolicyOption{BasicPolicy, GangPolicy}
	allModes := []DisruptionModeOption{SingleMode, AllMode}
	// 以下闭包是构造常见策略输入的语法糖：gang/basic 为叶子构建块，
	// compositeGang/compositeBasic 为复合构建块；计数取合法的 2。
	gang := func() PolicyInput {
		return PolicyInput{PodGroupData: gangData(2)}
	}
	basic := func() PolicyInput {
		return PolicyInput{PodGroupData: &schedulingv1alpha3.WorkloadPodGroupSchedulingPolicy{Basic: &schedulingv1alpha3.WorkloadPodGroupBasicSchedulingPolicy{}}}
	}
	compositeGang := func() PolicyInput {
		return PolicyInput{CompositePodGroupData: compositeGangData(2)}
	}
	compositeBasic := func() PolicyInput {
		return PolicyInput{CompositePodGroupData: &schedulingv1alpha3.WorkloadCompositePodGroupSchedulingPolicy{Basic: &schedulingv1alpha3.WorkloadCompositePodGroupBasicSchedulingPolicy{}}}
	}

	// table-driven 用例字段：name 为用例名；root/oldRoot 为新旧 WorkloadItem 树；
	// allowPolicies/allowModes 是控制器白名单；disableDV 控制是否关闭声明式校验；
	// wantErrs/wantType/wantErrFieldPath 断言错误条数、首个错误的类型和字段路径（空串表示不检查后两项）。
	tests := []struct {
		name             string
		root             *WorkloadItem
		oldRoot          *WorkloadItem
		allowPolicies    []SchedulingPolicyOption
		allowModes       []DisruptionModeOption
		disableDV        bool
		wantErrs         int
		wantType         field.ErrorType
		wantErrFieldPath string
	}{
		{
			// root 为 nil：builder 缺少根节点，Validate 应返回一条 Invalid 错误。
			name:          "nil tree fails to compile",
			root:          nil,
			allowPolicies: allPolicies,
			allowModes:    allModes,
			wantErrs:      1,
			wantType:      field.ErrorTypeInvalid,
		},
		{
			// Basic 策略在白名单内时，合法的叶子节点不应产生任何错误。
			name:          "basic allowed",
			root:          &WorkloadItem{Name: "job", Input: WorkloadInput{Policy: basic()}},
			allowPolicies: allPolicies,
			allowModes:    allModes,
		},
		{
			// 节点名为空：违反构造契约，返回 InternalError，且错误路径落在注入的 spec.scheduling。
			name:             "empty item name is rejected",
			root:             &WorkloadItem{Name: "", Path: schedulingPath, Input: WorkloadInput{Policy: basic()}},
			allowPolicies:    allPolicies,
			allowModes:       allModes,
			wantErrs:         1,
			wantType:         field.ErrorTypeInternal,
			wantErrFieldPath: "spec.scheduling",
		},
		{
			// Basic 不在白名单内：策略白名单检查返回 Forbidden，即使声明式校验通过。
			name:          "basic forbidden when not allow-listed",
			root:          &WorkloadItem{Name: "job", Input: WorkloadInput{Policy: basic()}},
			allowPolicies: []SchedulingPolicyOption{GangPolicy},
			allowModes:    allModes,
			wantErrs:      1,
			wantType:      field.ErrorTypeForbidden,
		},
		{
			// Gang 策略在白名单内时，合法的叶子节点不应报错。
			name:          "gang allowed",
			root:          &WorkloadItem{Name: "job", Input: WorkloadInput{Policy: gang()}},
			allowPolicies: []SchedulingPolicyOption{GangPolicy},
			allowModes:    allModes,
		},
		{
			// 白名单只放开 Basic 时，Gang 策略应被 Forbidden 拒绝。
			name:          "gang forbidden when not allow-listed",
			root:          &WorkloadItem{Name: "job", Input: WorkloadInput{Policy: gang()}},
			allowPolicies: []SchedulingPolicyOption{BasicPolicy},
			allowModes:    allModes,
			wantErrs:      1,
			wantType:      field.ErrorTypeForbidden,
		},
		{
			// Single 模式在白名单内时合法，不产生错误。
			name:          "single disruption allowed",
			root:          &WorkloadItem{Name: "job", Input: WorkloadInput{Policy: gang(), DisruptionMode: singleDisruption()}},
			allowPolicies: allPolicies,
			allowModes:    []DisruptionModeOption{SingleMode},
		},
		{
			// All 模式不在白名单内：返回 Forbidden。
			name:          "all disruption forbidden when not allow-listed",
			root:          &WorkloadItem{Name: "job", Input: WorkloadInput{Policy: gang(), DisruptionMode: allDisruption()}},
			allowPolicies: allPolicies,
			allowModes:    []DisruptionModeOption{SingleMode},
			wantErrs:      1,
			wantType:      field.ErrorTypeForbidden,
		},
		{
			// 即使两种模式都在白名单内，Basic + All 仍因跨字段互斥规则返回 Invalid。
			name:          "basic with all disruption is rejected by cross-field rule",
			root:          &WorkloadItem{Name: "job", Input: WorkloadInput{Policy: basic(), DisruptionMode: allDisruption()}},
			allowPolicies: allPolicies,
			allowModes:    allModes,
			wantErrs:      1,
			wantType:      field.ErrorTypeInvalid,
		},
		{
			// Gang 策略下 All 模式合法，验证跨字段规则只针对 Basic。
			name:          "gang with all disruption is valid",
			root:          &WorkloadItem{Name: "job", Input: WorkloadInput{Policy: gang(), DisruptionMode: allDisruption()}},
			allowPolicies: allPolicies,
			allowModes:    allModes,
		},
		{
			// minCount=0 违反声明式校验的下界：注入的 Path 与 PathElements 让错误精确落在
			// spec.scheduling.schedulingPolicy.gang.minCount。
			name:             "gang minCount below minimum fails declarative validation at injected path",
			root:             newWorkloadItem("job", withPath(schedulingPath), withGang(0)),
			allowPolicies:    allPolicies,
			allowModes:       allModes,
			wantErrs:         1,
			wantType:         field.ErrorTypeInvalid,
			wantErrFieldPath: "spec.scheduling.schedulingPolicy.gang.minCount",
		},
		{
			// 节点没有 Path 时，错误路径只由 PathElements 组成，不带 spec.scheduling 前缀。
			name:             "nil item Path reports building-block errors without a root prefix",
			root:             newWorkloadItem("job", withGang(0)),
			allowPolicies:    allPolicies,
			allowModes:       allModes,
			wantErrs:         1,
			wantType:         field.ErrorTypeInvalid,
			wantErrFieldPath: "schedulingPolicy.gang.minCount",
		},
		{
			// disableDV=true 时，同样的非法 minCount 也不报错，验证该开关确实跳过了声明式校验。
			name:          "declarative validation is skipped when disabled",
			root:          newWorkloadItem("job", withPath(schedulingPath), withGang(0)),
			allowPolicies: allPolicies,
			allowModes:    allModes,
			disableDV:     true,
		},
		{
			// Single 与 All 同时设置违反 union 规则，错误落在 disruptionMode 块路径上。
			name: "disruptionMode with single and all fails union declarative validation",
			root: &WorkloadItem{Name: "job", Path: schedulingPath, Input: WorkloadInput{
				Policy: gang(),
				DisruptionMode: DisruptionModeInput{
					PathElements: []string{"disruptionMode"},
					PodGroupData: &schedulingv1alpha3.WorkloadPodGroupDisruptionMode{
						Single: &schedulingv1alpha3.WorkloadPodGroupSingleDisruptionMode{},
						All:    &schedulingv1alpha3.WorkloadPodGroupAllDisruptionMode{},
					},
				},
			}},
			allowPolicies:    allPolicies,
			allowModes:       allModes,
			wantErrs:         1,
			wantType:         field.ErrorTypeInvalid,
			wantErrFieldPath: "spec.scheduling.disruptionMode",
		},
		{
			// 两条拓扑约束超过上限，返回 TooMany，并定位到 topology 字段。
			name: "too many topology constraints fails declarative validation",
			root: &WorkloadItem{Name: "job", Path: schedulingPath, Input: WorkloadInput{
				Policy: gang(),
				Constraints: ConstraintsInput{
					PathElements: []string{"schedulingConstraints"},
					PodGroupData: &schedulingv1alpha3.WorkloadPodGroupSchedulingConstraints{
						Topology: []schedulingv1alpha3.TopologyConstraint{{Key: "zone"}, {Key: "rack"}},
					},
				},
			}},
			allowPolicies:    allPolicies,
			allowModes:       allModes,
			wantErrs:         1,
			wantType:         field.ErrorTypeTooMany,
			wantErrFieldPath: "spec.scheduling.schedulingConstraints.topology",
		},
		{
			// 资源声明缺少来源违反 union 规则；path.Index(0) 让错误落到 resourceClaims[0]。
			name: "resourceClaim with no source fails union declarative validation at indexed path",
			root: &WorkloadItem{Name: "job", Path: schedulingPath, Input: WorkloadInput{
				Policy: gang(),
				ResourceClaims: ResourceClaimsInput{
					PathElements: []string{"resourceClaims"},
					PodGroupData: []schedulingv1alpha3.WorkloadPodGroupResourceClaim{{Name: "claim0"}},
				},
			}},
			allowPolicies:    allPolicies,
			allowModes:       allModes,
			wantErrs:         1,
			wantType:         field.ErrorTypeInvalid,
			wantErrFieldPath: "spec.scheduling.resourceClaims[0]",
		},
		{
			// 复合节点的 Gang 策略在白名单内时合法。
			name:          "composite gang allowed",
			root:          compositeRoot(WorkloadInput{Policy: compositeGang()}),
			allowPolicies: []SchedulingPolicyOption{GangPolicy},
			allowModes:    allModes,
		},
		{
			// 复合 Basic 不在白名单内：返回 Forbidden。
			name:          "composite basic forbidden when not allow-listed",
			root:          compositeRoot(WorkloadInput{Policy: compositeBasic()}),
			allowPolicies: []SchedulingPolicyOption{GangPolicy},
			allowModes:    allModes,
			wantErrs:      1,
			wantType:      field.ErrorTypeForbidden,
		},
		{
			// 复合节点的 minGroupCount=0 触发声明式校验，错误路径含注入的 spec.scheduling 前缀。
			name: "composite gang minGroupCount below minimum fails declarative validation at injected path",
			root: compositeRoot(WorkloadInput{Policy: PolicyInput{
				PathElements:          []string{"schedulingPolicy"},
				CompositePodGroupData: compositeGangData(0),
			}}),
			allowPolicies:    allPolicies,
			allowModes:       allModes,
			wantErrs:         1,
			wantType:         field.ErrorTypeInvalid,
			wantErrFieldPath: "spec.scheduling.schedulingPolicy.gang.minGroupCount",
		},
		{
			// 复合策略同时设置 Basic 与 Gang 违反 union 规则，错误落在策略块路径。
			name: "composite policy with basic and gang fails union declarative validation",
			root: compositeRoot(WorkloadInput{Policy: PolicyInput{
				PathElements: []string{"schedulingPolicy"},
				CompositePodGroupData: &schedulingv1alpha3.WorkloadCompositePodGroupSchedulingPolicy{
					Basic: &schedulingv1alpha3.WorkloadCompositePodGroupBasicSchedulingPolicy{},
					Gang:  &schedulingv1alpha3.WorkloadCompositePodGroupGangSchedulingPolicy{MinGroupCount: new(int32(2))},
				},
			}}),
			allowPolicies:    allPolicies,
			allowModes:       allModes,
			wantErrs:         1,
			wantType:         field.ErrorTypeInvalid,
			wantErrFieldPath: "spec.scheduling.schedulingPolicy",
		},
		{
			// 复合节点同时设置叶子 policy：违反输入 union，属于结构错误 InternalError，定位到根路径。
			name: "composite policy set with a leaf policy fails the input union",
			root: compositeRoot(WorkloadInput{Policy: PolicyInput{
				PathElements:          []string{"schedulingPolicy"},
				CompositePodGroupData: compositeGangData(2),
				PodGroupData:          gangData(2),
			}}),
			allowPolicies:    allPolicies,
			allowModes:       allModes,
			wantErrs:         1,
			wantType:         field.ErrorTypeInternal,
			wantErrFieldPath: "spec.scheduling",
		},
		{
			// 复合节点设置叶子 disruptionMode：同样违反输入 union。
			name: "composite policy set with a leaf disruptionMode fails the input union",
			root: compositeRoot(WorkloadInput{
				Policy: PolicyInput{
					PathElements:          []string{"schedulingPolicy"},
					CompositePodGroupData: compositeGangData(2),
				},
				DisruptionMode: singleDisruption(),
			}),
			allowPolicies:    allPolicies,
			allowModes:       allModes,
			wantErrs:         1,
			wantType:         field.ErrorTypeInternal,
			wantErrFieldPath: "spec.scheduling",
		},
		{
			// 输入 union 属于结构约束，即使关闭声明式校验也仍然生效。
			name: "input union is enforced even when declarative validation is disabled",
			root: compositeRoot(WorkloadInput{Policy: PolicyInput{
				PathElements:          []string{"schedulingPolicy"},
				CompositePodGroupData: compositeGangData(2),
				PodGroupData:          gangData(2),
			}}),
			allowPolicies:    allPolicies,
			allowModes:       allModes,
			disableDV:        true,
			wantErrs:         1,
			wantType:         field.ErrorTypeInternal,
			wantErrFieldPath: "spec.scheduling",
		},
		{
			// 复合树中的非法叶子子节点会被递归校验，错误路径指向子节点的 minCount。
			name: "invalid leaf child in a composite tree is validated recursively",
			root: newWorkloadItem("root", withPath(schedulingPath), withInput(WorkloadInput{Policy: compositeBasic()}),
				withChildren(newWorkloadItem("child", withPath(schedulingPath), withGang(0)))),
			allowPolicies:    allPolicies,
			allowModes:       allModes,
			wantErrs:         1,
			wantType:         field.ErrorTypeInvalid,
			wantErrFieldPath: "spec.scheduling.schedulingPolicy.gang.minCount",
		},
		{
			// 更新时新旧 root 名字一致且内容合法，应无错误。
			name:          "update with matching root name is accepted",
			root:          &WorkloadItem{Name: "job", Input: WorkloadInput{Policy: gang()}},
			oldRoot:       &WorkloadItem{Name: "job", Input: WorkloadInput{Policy: gang()}},
			allowPolicies: allPolicies,
			allowModes:    allModes,
		},
		{
			// 新旧 root 名字不一致视为调用契约错误，返回 InternalError，定位到 spec.scheduling。
			name:             "update with mismatched root name is an internal error",
			root:             &WorkloadItem{Name: "job", Path: schedulingPath, Input: WorkloadInput{Policy: gang()}},
			oldRoot:          &WorkloadItem{Name: "stale-job", Input: WorkloadInput{Policy: gang()}},
			allowPolicies:    allPolicies,
			allowModes:       allModes,
			wantErrs:         1,
			wantType:         field.ErrorTypeInternal,
			wantErrFieldPath: "spec.scheduling",
		},
		{
			// 新 root 清空了 policy（nil）：该字段块的声明式校验被跳过，不会拿旧值校验。
			name:          "update clearing policy skips declarative validation for the new nil policy",
			root:          &WorkloadItem{Name: "job", Input: WorkloadInput{}},
			oldRoot:       &WorkloadItem{Name: "job", Input: WorkloadInput{Policy: gang()}},
			allowPolicies: allPolicies,
			allowModes:    allModes,
		},
		{
			// 新 root 清空了 constraints：同理跳过该字段块的声明式校验。
			name: "update clearing constraints skips declarative validation for the new nil constraints",
			root: &WorkloadItem{Name: "job", Input: WorkloadInput{Policy: gang()}},
			oldRoot: &WorkloadItem{Name: "job", Input: WorkloadInput{
				Policy: gang(),
				Constraints: ConstraintsInput{
					PodGroupData: &schedulingv1alpha3.WorkloadPodGroupSchedulingConstraints{
						Topology: []schedulingv1alpha3.TopologyConstraint{{Key: "zone"}},
					},
				},
			}},
			allowPolicies: allPolicies,
			allowModes:    allModes,
		},
		{
			// 新 root 清空了 disruptionMode：同理跳过该字段块的声明式校验。
			name: "update clearing disruptionMode skips declarative validation for the new nil disruptionMode",
			root: &WorkloadItem{Name: "job", Input: WorkloadInput{Policy: gang()}},
			oldRoot: &WorkloadItem{Name: "job", Input: WorkloadInput{
				Policy:         gang(),
				DisruptionMode: singleDisruption(),
			}},
			allowPolicies: allPolicies,
			allowModes:    allModes,
		},
		{
			// 子节点名字和位置都没变，能与旧节点关联；其非法的 minCount 被 ratchet 保留，
			// 更新时不再重新校验，因此无错误。
			name:          "update of an unchanged child at the same position is ratcheted",
			root:          newWorkloadItem("root", withChildren(newWorkloadItem("leaf", withGang(0)))),
			oldRoot:       newWorkloadItem("root", withChildren(newWorkloadItem("leaf", withGang(0)))),
			allowPolicies: allPolicies,
			allowModes:    allModes,
		},
		{
			// 同名 leaf 上移了一层（root>mid>leaf 变成 root>leaf），在 root 层级找不到对应旧节点，
			// 因而按“新增”处理，暴露其非法 minCount。
			name:             "update of a child that changed position is validated as an addition",
			root:             newWorkloadItem("root", withPath(schedulingPath), withChildren(newWorkloadItem("leaf", withPath(schedulingPath), withGang(0)))),
			oldRoot:          newWorkloadItem("root", withChildren(newWorkloadItem("mid", withChildren(newWorkloadItem("leaf", withGang(0)))))),
			allowPolicies:    allPolicies,
			allowModes:       allModes,
			wantErrs:         1,
			wantType:         field.ErrorTypeInvalid,
			wantErrFieldPath: "spec.scheduling.schedulingPolicy.gang.minCount",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := NewBuilder(tt.root, BuildOptions{
				Owner:                        jobOwner(),
				AllowedPolicies:              tt.allowPolicies,
				AllowedDisruptionModes:       tt.allowModes,
				DisableDeclarativeValidation: tt.disableDV,
			})
			errs := b.Validate(ctx, ValidationInput{OldRoot: tt.oldRoot})
			// 先断言错误条数；不符则 Fatalf 终止当前子测试，避免下面访问 errs[0] 越界。
			if len(errs) != tt.wantErrs {
				t.Fatalf("expected %d error(s), got %d: %v", tt.wantErrs, len(errs), errs)
			}
			// 断言首个错误的类型，仅在用例声明了期望类型时检查。
			if tt.wantErrs > 0 && tt.wantType != "" && errs[0].Type != tt.wantType {
				t.Errorf("expected error type %s, got %s", tt.wantType, errs[0].Type)
			}
			// 断言首个错误的字段路径，确认错误能精确映射回控制器 API 字段。
			if tt.wantErrFieldPath != "" && errs[0].Field != tt.wantErrFieldPath {
				t.Errorf("expected error field %q, got %q", tt.wantErrFieldPath, errs[0].Field)
			}
		})
	}
}

// itemOption 用函数式选项构造用例中的 WorkloadItem，避免为每种字段组合定义一个构造函数。
type itemOption func(*WorkloadItem)

// newWorkloadItem 用名字和若干 itemOption 构造节点，便于在用例中声明式地组装 WorkloadItem 树。
func newWorkloadItem(name string, opts ...itemOption) *WorkloadItem {
	item := &WorkloadItem{Name: name}
	for _, opt := range opts {
		opt(item)
	}
	return item
}

// withPath 设置节点的 Path，用于验证错误路径的前缀拼接。
func withPath(path *field.Path) itemOption {
	return func(i *WorkloadItem) {
		i.Path = path
	}
}

// withInput 一次性替换节点的整个 Input。
func withInput(in WorkloadInput) itemOption {
	return func(i *WorkloadItem) {
		i.Input = in
	}
}

// withGang 把节点的 policy 设为 Gang（minCount 由参数指定），并注入 schedulingPolicy 路径元素。
func withGang(minCount int32) itemOption {
	return func(i *WorkloadItem) {
		i.Input.Policy = PolicyInput{
			PathElements: []string{"schedulingPolicy"},
			PodGroupData: gangData(minCount),
		}
	}
}

// withChildren 设置节点的子节点列表，用于构造树结构并验证递归校验。
func withChildren(children ...*WorkloadItem) itemOption {
	return func(i *WorkloadItem) {
		i.Children = children
	}
}

// compositeRoot 构造复合根节点：自带注入路径和给定 Input，并挂一个合法的叶子子节点。
func compositeRoot(in WorkloadInput) *WorkloadItem {
	return newWorkloadItem("root", withPath(schedulingPath), withInput(in),
		withChildren(newWorkloadItem("child", withPath(schedulingPath), withGang(2))))
}

// singleDisruption 构造只含 Single 的中断模式叶子输入。
func singleDisruption() DisruptionModeInput {
	return DisruptionModeInput{
		PodGroupData: &schedulingv1alpha3.WorkloadPodGroupDisruptionMode{
			Single: &schedulingv1alpha3.WorkloadPodGroupSingleDisruptionMode{},
		},
	}
}

// allDisruption 构造只含 All 的中断模式叶子输入。
func allDisruption() DisruptionModeInput {
	return DisruptionModeInput{
		PodGroupData: &schedulingv1alpha3.WorkloadPodGroupDisruptionMode{
			All: &schedulingv1alpha3.WorkloadPodGroupAllDisruptionMode{},
		},
	}
}

// gangData 构造指定 minCount 的 Gang 策略叶子数据。
func gangData(minCount int32) *schedulingv1alpha3.WorkloadPodGroupSchedulingPolicy {
	return &schedulingv1alpha3.WorkloadPodGroupSchedulingPolicy{
		Gang: &schedulingv1alpha3.WorkloadPodGroupGangSchedulingPolicy{
			MinCount: new(minCount),
		},
	}
}

// compositeGangData 构造指定 minGroupCount 的 Gang 策略复合数据。
func compositeGangData(minGroupCount int32) *schedulingv1alpha3.WorkloadCompositePodGroupSchedulingPolicy {
	return &schedulingv1alpha3.WorkloadCompositePodGroupSchedulingPolicy{
		Gang: &schedulingv1alpha3.WorkloadCompositePodGroupGangSchedulingPolicy{
			MinGroupCount: new(minGroupCount),
		},
	}
}
