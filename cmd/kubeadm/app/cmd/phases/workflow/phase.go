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

package workflow

import (
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

// Phase 提供工作流阶段的实现，只需实例化此类型的变量即可创建新阶段。
type Phase struct {
	// Name 是阶段名称。
	// 同级阶段（属于同一工作流或同一父阶段）的名称必须唯一。
	Name string

	// Aliases 是阶段的别名列表。
	Aliases []string

	// Short 是阶段简短描述。
	Short string

	// Long 是阶段的详细描述。
	Long string

	// Example 是阶段用法示例。
	Example string

	// Hidden 定义阶段是否在工作流帮助中隐藏。
	// 例如 kubeadm init 工作流中的 PrintFilesIfDryRunning 阶段可考虑对用户隐藏。
	Hidden bool

	// Phases 定义有序的子阶段序列。
	Phases []Phase

	// RunAllSiblings 允许将运行全部同级阶段的责任交给某阶段。
	// 注意：标记为 RunAllSiblings 的阶段不能定义 Run 或 RunIf。
	RunAllSiblings bool

	// Run 定义实现阶段动作的函数。
	// 建议实现类型断言，例如使用 Go 类型 switch 校验 RunData 类型。
	Run func(data RunData) error

	// RunIf 定义在执行阶段动作前检查的条件函数。
	// 返回 true 时执行，返回 false 时跳过；错误会中止工作流。未设置时始终执行。
	RunIf func(data RunData) (bool, error)

	// InheritFlags 定义为此阶段生成的 cobra 命令应从父命令本地参数
	// 或阶段运行器额外参数中继承的参数列表。
	// 未设置或为空时，不会向命令分配参数。
	// 注意：全局参数会自动由嵌套 cobra 命令继承。
	InheritFlags []string

	// LocalFlags 定义应分配给为此阶段生成的 cobra 命令的参数列表。
	// 注意：若两个或多个阶段拥有相同本地参数，请考虑在父命令中定义本地参数，
	// 或在阶段运行器中定义额外参数。
	LocalFlags *pflag.FlagSet

	// ArgsValidator 定义用于校验此阶段位置参数的函数。
	// 叶阶段未设置时沿用顶层命令规则；非叶阶段始终不接受参数。
	ArgsValidator cobra.PositionalArgs

	// Dependencies 是特定阶段所依赖的阶段列表。
	Dependencies []string
}

// AppendPhase 将给定阶段添加到有序的子阶段序列。
func (t *Phase) AppendPhase(phase Phase) {
	t.Phases = append(t.Phases, phase)
}
