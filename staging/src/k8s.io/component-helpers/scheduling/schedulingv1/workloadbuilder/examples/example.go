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

// Package examples 提供 workloadbuilder 的可执行示例，展示从构造 WorkloadItem、
// 校验、编译 Workload 到物化 PodGroup 的完整调用顺序，以及被拒绝时的错误形态。
package examples

import (
	"context"
	"fmt"

	schedulingv1alpha3 "k8s.io/api/scheduling/v1alpha3"
	schedulingv1beta1 "k8s.io/api/scheduling/v1beta1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/validation/field"
	workloadbuilder "k8s.io/component-helpers/scheduling/schedulingv1/workloadbuilder"
)

// ExampleOutOfTreeControllerValidation 演示 out-of-tree 控制器如何在准入阶段校验自己的
// 调度 API：它把每个 building block 及其在控制器 API 中的字段路径记录到 WorkloadItem
// 的 Input 中，然后调用 Builder.Validate——后者先对 building block 运行声明式校验，
// 再执行 DV 无法表达的复杂校验。in-tree 控制器由 apiserver 提供 DV，
// 因此改为设置 BuildOptions.DisableDeclarativeValidation。
// 把这些步骤写成一个可运行的 Example，而不只是文档，是因为 builder 的调用顺序
// （先记录字段路径，再构造 item，最后 Validate）很容易被误用；示例的输出断言
// 会在测试里直接暴露路径拼错或校验被跳过的情况。
func ExampleOutOfTreeControllerValidation() {
	ctx := context.Background()
	schedulingPath := field.NewPath("spec", "scheduling")

	policy := &schedulingv1alpha3.WorkloadPodGroupSchedulingPolicy{
		Basic: &schedulingv1alpha3.WorkloadPodGroupBasicSchedulingPolicy{},
	}
	mode := &schedulingv1alpha3.WorkloadPodGroupDisruptionMode{
		All: &schedulingv1alpha3.WorkloadPodGroupAllDisruptionMode{},
	}

	item := &workloadbuilder.WorkloadItem{
		Name: "worker",
		Path: schedulingPath,
		DefaultConfig: &workloadbuilder.SchedulingConfig{
			Policy: &workloadbuilder.SchedulingPolicy{Basic: &workloadbuilder.BasicSchedulingPolicy{}},
		},
		Input: workloadbuilder.WorkloadInput{
			Policy:         workloadbuilder.PolicyInput{PodGroupData: policy, PathElements: []string{"schedulingPolicy"}},
			DisruptionMode: workloadbuilder.DisruptionModeInput{PodGroupData: mode, PathElements: []string{"disruptionMode"}},
		},
	}
	// 零值 ValidationInput 表示校验一次创建（create）。更新（update）时，控制器传入
	// ValidationInput{OldRoot: oldItem}，使 DV 针对之前持久化的 item 运行更新期校验
	// （即不可变性检查）；操作类型由 OldRoot 是否为 nil 推断。
	// 这里传入 Owner 只是为了满足 BuildOptions；校验阶段只关心字段合法性，
	// 身份信息的构造是 Build 阶段的事。
	allErrs := workloadbuilder.NewBuilder(item, workloadbuilder.BuildOptions{
		Owner:                  &metav1.OwnerReference{APIVersion: "batch/v1", Kind: "Job", Name: "worker", UID: "job-uid"},
		AllowedPolicies:        []workloadbuilder.SchedulingPolicyOption{workloadbuilder.BasicPolicy, workloadbuilder.GangPolicy},
		AllowedDisruptionModes: []workloadbuilder.DisruptionModeOption{workloadbuilder.SingleMode, workloadbuilder.AllMode},
	}).Validate(ctx, workloadbuilder.ValidationInput{})

	if len(allErrs) > 0 {
		fmt.Printf("rejected: %v\n", allErrs.ToAggregate())
		return
	}
	fmt.Println("accepted")

	// Output:
	// rejected: spec.scheduling.disruptionMode: Invalid value: "": the disruptionMode `all` is not supported with the Basic scheduling policy
}

// ExampleJobControllerE2E 演示控制器把面向用户的调度 API 转换为调度器侧 Workload
// 和运行时 PodGroup 的端到端流程。它复刻了内置 Job 控制器处理单个 Job 的做法：
// 映射 spec.scheduling、叠加控制器默认值、把 gang 的 minCount 默认成 Job 的 parallelism、
// 编译 Workload，再从编译出的模板物化 PodGroup。
// 整套流程拆成“记录输入 -> 叠加默认值 -> 构造 builder -> Validate -> Build -> 物化”六步，
// 是为了对应控制器里真实的处理阶段；任何一步被跳过，后续对象就会缺字段或路径。
func ExampleJobControllerE2E() {
	// Job 的 parallelism，用于在 gang 未设置 minCount 时提供默认值。
	parallelism := int32(4)

	// 1. 记录用户的 spec.scheduling（这里是一个没有 minCount 的 gang，
	//    并按拓扑固定到单个节点），作为版本化 building block，
	//    并配上它们在控制器 API 中的字段路径。子字段为 nil 时保持 nil，
	//    以便按字段粒度回退到控制器默认值。
	userInput := workloadbuilder.WorkloadInput{
		Policy: workloadbuilder.PolicyInput{PodGroupData: &schedulingv1alpha3.WorkloadPodGroupSchedulingPolicy{
			Gang: &schedulingv1alpha3.WorkloadPodGroupGangSchedulingPolicy{}, // minCount 在下面的回调中补默认值
		}, PathElements: []string{"schedulingPolicy"}},
		Constraints: workloadbuilder.ConstraintsInput{PodGroupData: &schedulingv1alpha3.WorkloadPodGroupSchedulingConstraints{
			Topology: []schedulingv1alpha3.TopologyConstraint{{Key: "kubernetes.io/hostname"}},
		}, PathElements: []string{"schedulingConstraints"}},
		// DisruptionMode 与 ResourceClaims 不设置：使用默认值。
	}

	// 2. 组装单节点 workload 树：控制器默认值（Basic）、叠加其上的用户意图，
	//    以及一个在用户未设置时把 gang minCount 默认成 Job parallelism 的回调。
	//    回调只修改解析后的配置，绝不动调用方传入的输入。
	// 默认配置做成独立字段而不是写死在 builder 里，是为了让不同控制器
	// 按各自策略提供基线；回调只在最后作用于解析后的配置，
	// 因此调用方传入的 versioned 对象不会被偷偷改写。
	item := &workloadbuilder.WorkloadItem{
		Name:          "trainer-pgt-0",
		Path:          field.NewPath("spec", "scheduling"),
		DefaultConfig: &workloadbuilder.SchedulingConfig{Policy: &workloadbuilder.SchedulingPolicy{Basic: &workloadbuilder.BasicSchedulingPolicy{}}},
		Input:         userInput,
		Callbacks: []workloadbuilder.SchedulingConfigFunc{
			func(cfg *workloadbuilder.SchedulingConfig) {
				if g := cfg.Policy.Gang; g != nil && g.MinCount == nil {
					minCount := parallelism
					g.MinCount = &minCount
				}
			},
		},
	}

	// 3. 用 Workload 的身份信息、控制器的 ownerRef（它会成为 spec.controllerRef，
	//    用于发现 Workload 并驱动垃圾回收）以及控制器的调度白名单构造 builder。
	owner := &metav1.OwnerReference{APIVersion: "batch/v1", Kind: "Job", Name: "trainer", UID: "job-uid"}
	builder := workloadbuilder.NewBuilder(item, workloadbuilder.BuildOptions{
		Name:                   "trainer-wl",
		Namespace:              "ml",
		Owner:                  owner,
		AllowedPolicies:        []workloadbuilder.SchedulingPolicyOption{workloadbuilder.BasicPolicy, workloadbuilder.GangPolicy},
		AllowedDisruptionModes: []workloadbuilder.DisruptionModeOption{workloadbuilder.SingleMode, workloadbuilder.AllMode},
		// Job 的 apiserver 已经对 building block 跑过声明式校验，
		// 因此这个 in-tree 控制器选择不在这里重复运行，Validate 只执行复杂的控制器策略检查。
		// 不关掉的话同一套规则会在 apiserver 和控制器里各跑一遍，
		// 既浪费时间还可能因两处版本不同给出互相矛盾的错误。
		DisableDeclarativeValidation: true,
	})

	// 4. 执行复杂的控制器策略检查。building block 的结构校验已在 apiserver 跑过，
	//    因此在上面通过 DisableDeclarativeValidation 关闭。
	if errs := builder.Validate(context.Background(), workloadbuilder.ValidationInput{}); len(errs) > 0 {
		fmt.Printf("validation failed: %v\n", errs.ToAggregate())
		return
	}

	// 5. 编译 Workload。结果会被缓存，后续物化 PodGroup 无需重新编译。
	// 编译结果缓存在 builder 里，是因为一个 Workload 可能派生出多个 PodGroup，
	// 每次都重新编译既费 CPU，也可能让不同 PodGroup 拿到不一致的模板。
	workload, err := builder.BuildWorkload()
	if err != nil {
		fmt.Printf("build failed: %v\n", err)
		return
	}

	// 6. 从编译出的模板物化运行时 PodGroup，其 owner 取自 builder 配置的 owner。
	podGroup, err := builder.NewPodGroup("trainer-pg", item.Name)
	if err != nil {
		fmt.Printf("podgroup failed: %v\n", err)
		return
	}

	tmpl := workload.Spec.PodGroupTemplates[0]
	fmt.Printf("Workload %s/%s controllerRef=%s/%s/%s\n",
		workload.Namespace, workload.Name,
		workload.Spec.ControllerRef.APIGroup, workload.Spec.ControllerRef.Kind, workload.Spec.ControllerRef.Name)
	fmt.Printf("  template %q: gang minCount=%d, topology=%s\n",
		tmpl.Name, tmpl.SchedulingPolicy.Gang.MinCount, tmpl.SchedulingConstraints.Topology[0].Key)
	fmt.Printf("PodGroup %s/%s -> workload=%s template=%s gang minCount=%d\n",
		podGroup.Namespace, podGroup.Name,
		podGroup.Spec.WorkloadRef.WorkloadName, podGroup.Spec.WorkloadRef.TemplateName,
		podGroup.Spec.SchedulingPolicy.Gang.MinCount)

	// Output:
	// Workload ml/trainer-wl controllerRef=batch/Job/trainer
	//   template "trainer-pgt-0": gang minCount=4, topology=kubernetes.io/hostname
	// PodGroup ml/trainer-pg -> workload=trainer-wl template=trainer-pgt-0 gang minCount=4
}

// ExampleDelegatedPodGroupFromExistingWorkload 演示控制器如何从自己没有编译过的
// Workload 物化运行时 PodGroup。Workload 由父控制器（或手写模板）拥有并已持久化，
// 它就是事实来源：NewBuilderFromExistingWorkload 直接接收该 Workload，子控制器不提供
// 任何自己的调度输入。Validate 会报告 InternalError，因为没有 WorkloadItem 树可校验，
// 也不应把 apiserver 已完成的校验误当成空操作；BuildWorkload 同样会被拒绝，
// 确保该对象不会被重新编译覆盖。这对应 Job 控制器的委托式 PodGroup 模式。
// 这个例子存在的意义是区分两种所有权：Workload 属于父控制器，PodGroup 属于子控制器。
// 子控制器只有“按现有模板物化”的权限，不能反过来重建父控制器的对象。
func ExampleDelegatedPodGroupFromExistingWorkload() {
	// 父控制器已经编译并持久化了这个 Workload。
	parentWorkload := &schedulingv1beta1.Workload{
		ObjectMeta: metav1.ObjectMeta{Name: "trainer-wl", Namespace: "ml", UID: "wl-uid"},
		Spec: schedulingv1beta1.WorkloadSpec{
			PodGroupTemplates: []schedulingv1beta1.PodGroupTemplate{{
				Name:              "trainer-pgt-0",
				PriorityClassName: "high-priority",
				SchedulingPolicy:  schedulingv1beta1.PodGroupSchedulingPolicy{Gang: &schedulingv1beta1.GangSchedulingPolicy{MinCount: 4}},
			}},
		},
	}

	// 子控制器只拥有 PodGroup，因此传入自己的 ownerRef。
	owner := &metav1.OwnerReference{APIVersion: "batch/v1", Kind: "Job", Name: "trainer-worker", UID: "job-uid"}
	builder := workloadbuilder.NewBuilderFromExistingWorkload(parentWorkload, workloadbuilder.BuildOptions{Owner: owner})

	// 对从既有 Workload 构造的 Builder 调用 Validate 会返回 InternalError；
	// 该模式只用于物化，不应重新校验父控制器已经持久化的对象。
	if errs := builder.Validate(context.Background(), workloadbuilder.ValidationInput{}); len(errs) > 0 {
		fmt.Printf("unexpected validation errors: %v\n", errs.ToAggregate())
		return
	}

	// BuildWorkload 会被拒绝，因此父控制器编写的 Workload 永远不会被重新编译。
	// 这里是刻意调用一个注定失败的方法，用示例输出证明“拒绝重编译”确实生效，
	// 而不是靠文档承诺。
	if _, err := builder.BuildWorkload(); err != nil {
		fmt.Println("BuildWorkload refused for existing Workload")
	}

	// 只有 NewPodGroup 是有意义的：它把持久化模板的字段
	// （这里是 priorityClassName 和 gang minCount）拷贝到运行时 PodGroup 中。
	podGroup, err := builder.NewPodGroup("trainer-pg", "trainer-pgt-0")
	if err != nil {
		fmt.Printf("podgroup failed: %v\n", err)
		return
	}

	fmt.Printf("PodGroup %s/%s -> workload=%s template=%s priorityClass=%s gang minCount=%d ownedBy=%s\n",
		podGroup.Namespace, podGroup.Name,
		podGroup.Spec.WorkloadRef.WorkloadName, podGroup.Spec.WorkloadRef.TemplateName,
		podGroup.Spec.PriorityClassName,
		podGroup.Spec.SchedulingPolicy.Gang.MinCount,
		podGroup.OwnerReferences[0].Name)

	// Output:
	// BuildWorkload refused for existing Workload
	// PodGroup ml/trainer-pg -> workload=trainer-wl template=trainer-pgt-0 priorityClass=high-priority gang minCount=4 ownedBy=trainer-worker
}
