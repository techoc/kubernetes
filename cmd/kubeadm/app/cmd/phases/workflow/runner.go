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
	"fmt"
	"strings"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"

	cmdutil "k8s.io/kubernetes/cmd/kubeadm/app/cmd/util"
	"k8s.io/kubernetes/cmd/kubeadm/app/util/errors"
)

// phaseSeparator 定义拼接嵌套阶段名称时使用的分隔符。
const phaseSeparator = "/"

// RunnerOptions 定义执行 kubeadm 可组合工作流时支持的选项。
type RunnerOptions struct {
	// FilterPhases 定义要执行的阶段列表；为空时执行全部阶段。
	FilterPhases []string

	// SkipPhases 定义不执行的阶段列表；为空时不跳过任何阶段。
	SkipPhases []string
}

// RunData 定义工作流中所有阶段共享的数据，可以是任意类型。
type RunData = interface{}

// Runner 实现 kubeadm 可组合工作流的管理。
type Runner struct {
	// Options 控制运行器行为。
	Options RunnerOptions

	// Phases 是构成待管理工作流的有序阶段列表。
	Phases []Phase

	// runDataInitializer 定义创建运行时数据的函数，数据由工作流包含的所有阶段共享。
	runDataInitializer func(*cobra.Command, []string) (RunData, error)

	// runData 是运行器内部状态的一部分，用于在 InitData 方法中实现单例，
	// 从而避免多次初始化数据。
	runData RunData

	// runCmd 是运行器内部状态的一部分，用于跟踪触发运行器的命令
	// （仅在运行器执行 BindToCommand 时）。
	runCmd *cobra.Command

	// cmdAdditionalFlags 保存可添加到阶段生成子命令的额外共享参数。
	// 这些参数可从父命令继承，也可直接添加到每个阶段。
	cmdAdditionalFlags *pflag.FlagSet

	// phaseRunners 是运行器内部状态的一部分，提供构成工作流的阶段包装器列表，
	// 包装器带有支持阶段执行的上下文信息。
	phaseRunners []*phaseRunner
}

// phaseRunner 在 Phase 包装器之外补充一组由 Runner 管理的工作流衍生的上下文信息。
// TODO: 若未来需要更复杂的实现，可将此类型替换为定义良好的 DAG 或树库。
type phaseRunner struct {
	// Phase 提供对阶段实现的访问。
	Phase

	// 提供对 Runner 管理的工作流中父阶段的访问。
	parent *phaseRunner

	// level 定义此阶段在 Runner 管理的工作流中的嵌套层级。
	level int

	// selfPath 包含标识 Runner 管理的工作流中此阶段的全部路径元素。
	selfPath []string

	// generatedName 是阶段完整名称，对应 Runner 管理的工作流中阶段的绝对路径。
	generatedName string

	// use 是工作流帮助中打印的阶段用法字符串，
	// 对应 Runner 管理的工作流中阶段的相对路径。
	use string
}

// NewRunner 返回用于 kubeadm 可组合工作流的新运行器。
func NewRunner() *Runner {
	return &Runner{
		Phases: []Phase{},
	}
}

// AppendPhase 将给定阶段添加到运行器管理的有序阶段序列。
func (e *Runner) AppendPhase(t Phase) {
	e.Phases = append(e.Phases, t)
}

// computePhaseRunFlags 返回定义各阶段是否应运行的映射。
// PhaseRunFlags 根据 RunnerOptions 计算。
func (e *Runner) computePhaseRunFlags() (map[string]bool, error) {
	// 初始化辅助数据结构。
	phaseRunFlags := map[string]bool{}
	phaseHierarchy := map[string][]string{}
	e.visitAll(func(p *phaseRunner) error {
		// 假定所有阶段都应运行，初始化 phaseRunFlags。
		phaseRunFlags[p.generatedName] = true

		// 为当前阶段初始化 phaseHierarchy，即依赖当前阶段的阶段列表。
		phaseHierarchy[p.generatedName] = []string{}

		// 将当前阶段注册到其自身的父阶段层级中。
		parent := p.parent
		for parent != nil {
			phaseHierarchy[parent.generatedName] = append(phaseHierarchy[parent.generatedName], p.generatedName)
			parent = parent.parent
		}
		return nil
	})

	// 若指定过滤选项，将所有 phaseRunFlags 设为 false，
	// 但过滤集合中的阶段及其嵌套阶段层级除外。
	if len(e.Options.FilterPhases) > 0 {
		for i := range phaseRunFlags {
			phaseRunFlags[i] = false
		}
		for _, f := range e.Options.FilterPhases {
			if _, ok := phaseRunFlags[f]; !ok {
				return phaseRunFlags, errors.Errorf("invalid phase name: %s", f)
			}
			phaseRunFlags[f] = true
			for _, c := range phaseHierarchy[f] {
				phaseRunFlags[c] = true
			}
		}
	}

	// 若指定阶段跳过选项，将对应 phaseRunFlags 设为 false，
	// 并对下层阶段层级应用相同更改。
	for _, f := range e.Options.SkipPhases {
		if _, ok := phaseRunFlags[f]; !ok {
			return phaseRunFlags, errors.Errorf("invalid phase name: %s", f)
		}
		phaseRunFlags[f] = false
		for _, c := range phaseHierarchy[f] {
			phaseRunFlags[c] = false
		}
	}

	return phaseRunFlags, nil
}

// SetDataInitializer 设置用于初始化运行时数据的函数，数据由工作流包含的所有阶段共享。
// 该方法接收触发 Runner 的命令作为输入（仅在运行器执行 BindToCommand 时）。
func (e *Runner) SetDataInitializer(builder func(cmd *cobra.Command, args []string) (RunData, error)) {
	e.runDataInitializer = builder
}

// InitData 触发创建工作流中所有阶段共享的运行时数据。
// 若需要在执行 Run 前获取 RunData，可显式调用此操作；
// 调用 Run 时也会隐式执行。
func (e *Runner) InitData(args []string) (RunData, error) {
	if e.runData == nil && e.runDataInitializer != nil {
		var err error
		if e.runData, err = e.runDataInitializer(e.runCmd, args); err != nil {
			return nil, err
		}
	}

	return e.runData, nil
}

// 运行 kubeadm 可组合工作流。
func (e *Runner) Run(args []string) error {
	e.prepareForExecution()

	// 根据 RunnerOptions 确定应运行哪些阶段。
	phaseRunFlags, err := e.computePhaseRunFlags()
	if err != nil {
		return err
	}

	// 实际执行前预检阶段依赖。
	missedDeps := make(map[string][]string)
	visited := make(map[string]struct{})
	for _, p := range e.phaseRunners {
		if run, ok := phaseRunFlags[p.generatedName]; !run || !ok {
			continue
		}
		for _, dep := range p.Phase.Dependencies {
			if _, ok := visited[dep]; !ok {
				missedDeps[p.Phase.Name] = append(missedDeps[p.Phase.Name], dep)
			}
		}
		visited[p.Phase.Name] = struct{}{}
	}
	if len(missedDeps) > 0 {
		var msg strings.Builder
		msg.WriteString("unresolved dependencies:")
		for phase, missedPhases := range missedDeps {
			msg.WriteString(fmt.Sprintf("\n\tmissing %v phase(s) needed by %q phase", missedPhases, phase))
		}
		return errors.New(msg.String())
	}

	// 若运行时数据尚未初始化，则构建运行器数据。
	data := e.runData
	if data == nil {
		if data, err = e.InitData(args); err != nil {
			return err
		}
	}

	err = e.visitAll(func(p *phaseRunner) error {
		// 若阶段不应运行，则跳过该阶段。
		if run, ok := phaseRunFlags[p.generatedName]; !run || !ok {
			return nil
		}

		// 若仅用于创建特殊子命令的阶段错误地绑定了 Run 方法，则返回错误。
		if p.RunAllSiblings && (p.RunIf != nil || p.Run != nil) {
			return errors.Errorf("phase marked as RunAllSiblings can not have Run functions %s", p.generatedName)
		}

		// 若阶段定义了执行动作前应检查的条件，
		if p.RunIf != nil {
			// 检查条件；条件不满足或检查失败时返回。
			ok, err := p.RunIf(data)
			if err != nil {
				return errors.Wrapf(err, "error execution run condition for phase %s", p.generatedName)
			}

			if !ok {
				return nil
			}
		}

		// 执行阶段动作（若已定义）。
		if p.Run != nil {
			if err := p.Run(data); err != nil {
				return errors.Wrapf(err, "error execution phase %s", p.generatedName)
			}
		}

		return nil
	})

	return err
}

// Help 返回包含工作流全部阶段的文本。
func (e *Runner) Help(cmdUse string) string {
	e.prepareForExecution()

	// 计算每个阶段用法行的最大长度。
	maxLength := 0
	e.visitAll(func(p *phaseRunner) error {
		if !p.Hidden && !p.RunAllSiblings {
			length := len(p.use)
			if maxLength < length {
				maxLength = length
			}
		}
		return nil
	})

	// 按层级缩进阶段列表，并使用最大长度格式化。
	// 列表置于 Markdown 代码块中，以提高公开网站上的可读性。
	line := fmt.Sprintf("The %q command executes the following phases:\n", cmdUse)
	line += "```\n"
	offset := 2
	e.visitAll(func(p *phaseRunner) error {
		if !p.Hidden && !p.RunAllSiblings {
			padding := maxLength - len(p.use) + offset
			line += strings.Repeat(" ", offset*p.level) // 缩进。
			line += p.use                               // 名称和别名。
			line += strings.Repeat(" ", padding)        // 右侧填充至最大长度（另加间距偏移）。
			line += p.Short                             // 阶段简短描述。
			line += "\n"
		}

		return nil
	})
	line += "```"
	return line
}

// SetAdditionalFlags 定义要添加到每个阶段生成子命令的参数
// （父命令中不存在的参数）。
// 注意：必须在 BindToCommand 之前完成此操作。
// 注意：若参数仅由一个阶段使用，请考虑使用阶段的 LocalFlags。
func (e *Runner) SetAdditionalFlags(fn func(*pflag.FlagSet)) {
	// 创建新的 NewFlagSet。
	e.cmdAdditionalFlags = pflag.NewFlagSet("phaseAdditionalFlags", pflag.ContinueOnError)
	// 调用设置额外参数的函数。
	fn(e.cmdAdditionalFlags)
}

// BindToCommand 将 Runner 绑定到 cobra 命令：修改帮助信息、
// 添加阶段相关参数并添加阶段子命令。
// 注意：必须在全部阶段添加到 Runner 之后执行且仅执行一次此操作。
func (e *Runner) BindToCommand(cmd *cobra.Command) {
	// 跟踪触发运行器的命令。
	e.runCmd = cmd

	// 若未添加阶段，则提前返回。
	if len(e.Phases) == 0 {
		return
	}

	e.prepareForExecution()

	// 添加阶段子命令。
	phaseCommand := &cobra.Command{
		Use:   "phase",
		Short: fmt.Sprintf("Use this command to invoke single phase of the %q workflow", cmd.Name()),
	}
	cmdutil.RequireSubcommand(phaseCommand)

	cmd.AddCommand(phaseCommand)

	// 生成用于调用单个阶段的全部嵌套子命令。
	subcommands := map[string]*cobra.Command{}
	e.visitAll(func(p *phaseRunner) error {
		// 跳过隐藏阶段。
		if p.Hidden {
			return nil
		}

		// 初始化阶段选择器。
		phaseSelector := p.generatedName

		// 若请求运行全部同级阶段，则设置当前阶段。
		if p.RunAllSiblings {
			phaseSelector = p.parent.generatedName
		}

		// 创建阶段子命令。
		phaseCmd := &cobra.Command{
			Use:     strings.ToLower(p.Name),
			Short:   p.Short,
			Long:    p.Long,
			Example: p.Example,
			Aliases: p.Aliases,
			RunE: func(cmd *cobra.Command, args []string) error {
				// 若阶段有子阶段，则打印帮助并退出。
				if len(p.Phases) > 0 {
					_ = cmd.Help()
					return cmdutil.ErrorSubcommandRequired
				}

				// 使用 phaseCmd 覆盖触发 Runner 的命令。
				e.runCmd = cmd
				e.Options.FilterPhases = []string{phaseSelector}
				return e.Run(args)
			},
		}

		// 使新命令继承父命令的本地参数。
		// 注意：全局参数会自动继承。
		inheritsFlags(cmd.Flags(), phaseCmd.Flags(), p.InheritFlags)

		// 使新命令继承阶段的额外参数。
		if e.cmdAdditionalFlags != nil {
			inheritsFlags(e.cmdAdditionalFlags, phaseCmd.Flags(), p.InheritFlags)
		}

		// 若已定义，则添加阶段本地参数。
		if p.LocalFlags != nil {
			p.LocalFlags.VisitAll(func(f *pflag.Flag) {
				phaseCmd.Flags().AddFlag(f)
			})
		}

		// 若此阶段有子阶段（非叶阶段），则不接受任何参数。
		if len(p.Phases) > 0 {
			phaseCmd.Args = cobra.NoArgs
		} else {
			if p.ArgsValidator == nil {
				phaseCmd.Args = cmd.Args
			} else {
				phaseCmd.Args = p.ArgsValidator
			}
		}

		// 将命令添加到父命令。
		if p.level == 0 {
			phaseCommand.AddCommand(phaseCmd)
		} else {
			subcommands[p.parent.generatedName].AddCommand(phaseCmd)
		}

		subcommands[p.generatedName] = phaseCmd
		return nil
	})

	// 修改命令描述以显示可用阶段。
	if cmd.Long != "" {
		cmd.Long = fmt.Sprintf("%s\n\n%s\n", cmd.Long, e.Help(cmd.Use))
	} else {
		cmd.Long = fmt.Sprintf("%s\n\n%s\n", cmd.Short, e.Help(cmd.Use))
	}

	// 向主命令添加阶段相关参数。
	cmd.Flags().StringSliceVar(&e.Options.SkipPhases, "skip-phases", nil, "List of phases to be skipped")
}

func inheritsFlags(sourceFlags, targetFlags *pflag.FlagSet, cmdFlags []string) {
	// 若未定义要从父命令继承的参数列表，则不添加参数。
	if cmdFlags == nil {
		return
	}

	// 将所有待继承参数添加到目标 flagSet。
	sourceFlags.VisitAll(func(f *pflag.Flag) {
		for _, c := range cmdFlags {
			if f.Name == c {
				targetFlags.AddFlag(f)
			}
		}
	})
}

// visitAll 提供遍历工作流全部阶段的工具方法，按执行顺序对每个阶段执行函数。
// 子阶段在其父阶段之后立即访问。
func (e *Runner) visitAll(fn func(*phaseRunner) error) error {
	for _, currentRunner := range e.phaseRunners {
		if err := fn(currentRunner); err != nil {
			return err
		}
	}
	return nil
}

// prepareForExecution 初始化 Runner 的内部状态（phaseRunner 列表）。
func (e *Runner) prepareForExecution() {
	e.phaseRunners = []*phaseRunner{}
	var parentRunner *phaseRunner
	for _, phase := range e.Phases {
		// 跳过仅用于创建特殊子命令的阶段。
		if phase.RunAllSiblings {
			continue
		}

		// 将阶段添加到执行列表。
		addPhaseRunner(e, parentRunner, phase)
	}
}

// addPhaseRunner 将给定阶段的 phaseRunner 添加到 phaseRunners 列表。
func addPhaseRunner(e *Runner, parentRunner *phaseRunner, phase Phase) {
	// 计算由 Runner 管理的工作流衍生的上下文信息。
	use := cleanName(phase.Name)
	generatedName := use
	selfPath := []string{generatedName}

	if parentRunner != nil {
		generatedName = strings.Join([]string{parentRunner.generatedName, generatedName}, phaseSeparator)
		use = fmt.Sprintf("%s%s", phaseSeparator, use)
		selfPath = append(parentRunner.selfPath, selfPath...)
	}

	// 创建 phaseRunner。
	currentRunner := &phaseRunner{
		Phase:         phase,
		parent:        parentRunner,
		level:         len(selfPath) - 1,
		selfPath:      selfPath,
		generatedName: generatedName,
		use:           use,
	}

	// 添加到 phaseRunners 列表。
	e.phaseRunners = append(e.phaseRunners, currentRunner)

	// 遍历有序的嵌套阶段列表，按预期执行顺序存储阶段；
	// 子阶段存储在其父阶段之后。
	for _, childPhase := range phase.Phases {
		addPhaseRunner(e, currentRunner, childPhase)
	}
}

// cleanName 将阶段名称转换为适合运行器帮助的形式：
// 转为小写并移除参数描述符（若存在）。
func cleanName(name string) string {
	ret := strings.ToLower(name)
	if pos := strings.Index(ret, " "); pos != -1 {
		ret = ret[:pos]
	}
	return ret
}
