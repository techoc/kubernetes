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

// Package workloadbuilder 是一个共享的翻译库，负责把控制器的调度意图转换成
// 调度器侧可识别的 Workload，统一处理默认值、校验与 PodGroupTemplate 编译，
// 避免每个控制器各自重复实现。
//
// 控制器把自己 API 中描述调度意图的 building block（以及它们所在的字段路径）
// 记录到 WorkloadItem 的 Input 中，将其叠加到 DefaultConfig 之上，组装出
// WorkloadItem 树，再通过 Builder 驱动整个流程：
//
//	builder := workloadbuilder.NewBuilder(item, workloadbuilder.BuildOptions{
//		Name:                   "trainer-wl",
//		Namespace:              "ml",
//		Owner:                  ownerRef,
//		AllowedPolicies:        []workloadbuilder.SchedulingPolicyOption{workloadbuilder.BasicPolicy, workloadbuilder.GangPolicy},
//		AllowedDisruptionModes: []workloadbuilder.DisruptionModeOption{workloadbuilder.SingleMode, workloadbuilder.AllMode},
//	})
//	if errs := builder.Validate(ctx, workloadbuilder.ValidationInput{}); len(errs) > 0 { /* reject */ }
//	workload, err := builder.BuildWorkload()
//	podGroup, err := builder.NewPodGroup("trainer-pg", "trainer-pgt-0")
//
// BuildWorkload 会编译并缓存 Workload，使同一份编译结果能物化出多个 PodGroup。
// 如果控制器管理的 PodGroup 来自一个并非自己编译的 Workload，则应改用
// NewBuilderFromExistingWorkload：此时 Builder 从该 Workload 物化 PodGroup，
// 并拒绝 BuildWorkload，确保传入的对象不会被重新编译覆盖。
//
// 之所以把“翻译 + 默认值 + 校验 + 编译 + PodGroup 生成”放在同一个 Builder 里，
// 是因为这些步骤共享同一棵 WorkloadItem 树和同一份编译结果；拆给各 controller
// 各自实现会让字段路径、默认值和交叉字段规则逐渐漂移，最终同一个 Workload
// 在不同 controller 下得到不同的调度语义。
//
// # 校验
//
// Builder.Validate 针对 WorkloadItem 树分两层报告错误：
//
//   - 声明式校验（DV）：对每个 Input 中记录的 building block 做结构约束检查。
//     Validate 会运行生成的 Validate_* 校验器，并在 Input 的 PathElements
//     所记录的字段路径上报告错误。
//
//   - 复杂校验：包括控制器白名单（AllowedPolicies、AllowedDisruptionModes），
//     以及 DV 无法表达的跨字段规则，例如 Basic 策略不能与 All 中断模式组合。
//
// out-of-tree 控制器保留 DV，因此一次 Builder.Validate 调用即可覆盖两层。
// 它们把每个 building block 连同其在控制器 API 中的字段路径一起记录，然后调用一次 Validate：
//
//	item := &workloadbuilder.WorkloadItem{
//		Name: "worker",
//		Path: fldPath,
//		Input: workloadbuilder.WorkloadInput{
//			Policy:         workloadbuilder.PolicyInput{PodGroupData: policy, PathElements: []string{"schedulingPolicy"}},
//			DisruptionMode: workloadbuilder.DisruptionModeInput{PodGroupData: mode, PathElements: []string{"disruptionMode"}},
//		},
//	}
//	builder := workloadbuilder.NewBuilder(item, opts)
//	allErrs = append(allErrs, builder.Validate(ctx, workloadbuilder.ValidationInput{})...)
//
// 零值 ValidationInput 表示校验一次创建（create）。更新（update）时控制器设置 OldRoot，
// 使 DV 针对之前持久化的 item 运行更新期校验（即不可变性检查）；操作类型由
// OldRoot 是否为 nil 推断：
//
//	allErrs = append(allErrs, builder.Validate(ctx, workloadbuilder.ValidationInput{
//		OldRoot: oldItem,
//	})...)
//
// 可运行的版本参见 ExampleOutOfTreeControllerValidation。in-tree 控制器的 apiserver
// 已经对嵌入的 building block 跑过 DV，因此可以设 BuildOptions.DisableDeclarativeValidation
// 跳过第一层，只运行复杂校验；无论什么操作都传零值 ValidationInput，因为只有 DV
// 才会用到 OldRoot。参见 ExampleJobControllerE2E。
//
// 两层校验分开是刻意的：DV 只认结构规则，表达不了“哪些策略被 controller 允许”
// 或“Basic 策略不能配 All 中断模式”这类跨字段约束；而 in-tree controller 的
// apiserver 已经对嵌入字段跑过 DV，重复一遍既浪费又会因字段路径不同而报出
// 两套错误信息，所以提供了关闭第一层的开关。
//
// +k8s:conversion-gen=k8s.io/api/scheduling/v1alpha3
// +k8s:conversion-gen-external-types=k8s.io/api/scheduling/v1beta1
package workloadbuilder
