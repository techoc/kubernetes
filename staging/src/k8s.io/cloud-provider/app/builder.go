/*
Copyright 2022 The Kubernetes Authors.

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

package app

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/cloud-provider/names"
	"k8s.io/cloud-provider/options"
	cliflag "k8s.io/component-base/cli/flag"
	"k8s.io/component-base/cli/globalflag"
	"k8s.io/component-base/term"
	"k8s.io/component-base/version/verflag"
)

// CommandBuilder 收集构建 cloud-controller-manager cobra 命令所需的全部可变部分：
// 控制器与 webhook 注册表、别名、附加 flag、选项、云初始化函数和停机通道。
// 上游（如各云厂商的 CCM）通过链式 Set/Register 调用定制命令，
// 最终由 BuildCommand 补齐默认值并生成命令，避免各厂商重复实现启动骨架。
type CommandBuilder struct {
	// webhookConfigs 以 webhook 名称索引其构造配置；名称同时决定启用/禁用开关。
	webhookConfigs map[string]WebhookConfig
	// controllerInitFuncConstructors 以控制器名称索引其构造函数；带 "-controllers" 的 flag 依赖该名称。
	controllerInitFuncConstructors map[string]ControllerInitFuncConstructor
	// controllerAliases 把用户可见的旧/别名控制器名映射到规范名称，保证既有启动参数继续有效。
	controllerAliases map[string]string
	// additionalFlags 是厂商私有 flag；与通用 flag 合并后一起注册到命令。
	additionalFlags cliflag.NamedFlagSets
	// options 为空时 setdefaults 会按 defaults 新建，因此未显式设置也能得到完整选项。
	options *options.CloudControllerManagerOptions
	// cloudInitializer 在配置完成后构造云实现；它是厂商代码接入 Kubernetes 的唯一入口。
	cloudInitializer InitCloudFunc
	// stopCh 驱动所有控制器退出；未设置时使用永不关闭的通道。
	stopCh <-chan struct{}
	// cmdName 是命令名，影响 usage 文本与全局 flag 的显示。
	cmdName string
	// long 是命令的详细说明文本，默认描述 CCM 的职责。
	long string
	// defaults 提供各选项在 flag 未设置时的业务默认值。
	defaults *options.ProviderDefaults
}

// NewBuilder 创建一个已初始化内部 map 的 CommandBuilder，
// 使调用方可以立即 RegisterController/RegisterWebhook 而无需判空。
func NewBuilder() *CommandBuilder {
	cb := CommandBuilder{}
	cb.webhookConfigs = make(map[string]WebhookConfig)
	cb.controllerInitFuncConstructors = make(map[string]ControllerInitFuncConstructor)
	cb.controllerAliases = make(map[string]string)
	return &cb
}

// SetOptions 注入现成的选项对象，通常用于测试或需要完全掌控 flag 默认值的场景。
func (cb *CommandBuilder) SetOptions(options *options.CloudControllerManagerOptions) {
	cb.options = options
}

// AddFlags 注册厂商额外提供的 flag；它们会出现在 help 与 usage 的独立分组中。
func (cb *CommandBuilder) AddFlags(additionalFlags cliflag.NamedFlagSets) {
	cb.additionalFlags = additionalFlags
}

// RegisterController 注册一个控制器构造器。aliases 中只有值等于 name 的条目会被采纳，
// 这样调用方传入的全局别名表不会被无关映射污染。
func (cb *CommandBuilder) RegisterController(name string, constructor ControllerInitFuncConstructor, aliases map[string]string) {
	cb.controllerInitFuncConstructors[name] = constructor
	for key, val := range aliases {
		if name == val {
			cb.controllerAliases[key] = val
		}
	}
}

// RegisterDefaultControllers 一次性装载内置的 cloud-node、cloud-node-lifecycle、
// service 与 route 控制器及其标准别名，供不使用厂商定制控制器的场景直接复用。
func (cb *CommandBuilder) RegisterDefaultControllers() {
	for key, val := range DefaultInitFuncConstructors {
		cb.controllerInitFuncConstructors[key] = val
	}
	for key, val := range names.CCMControllerAliases() {
		cb.controllerAliases[key] = val
	}
}

// RegisterWebhook 注册一个 webhook 处理器配置；名称用于 webhook 的启用开关与指标标签。
func (cb *CommandBuilder) RegisterWebhook(name string, config WebhookConfig) {
	cb.webhookConfigs[name] = config
}

// SetCloudInitializer 设置根据最终配置构造云实现的函数。为 nil 会导致启动时 panic，
// 因此厂商实现必须在 BuildCommand 之前调用本方法。
func (cb *CommandBuilder) SetCloudInitializer(cloudInitializer InitCloudFunc) {
	cb.cloudInitializer = cloudInitializer
}

// SetStopChannel 设置控制器运行的停机通道；未设置时使用永不关闭的通道。
func (cb *CommandBuilder) SetStopChannel(stopCh <-chan struct{}) {
	cb.stopCh = stopCh
}

// SetCmdName 覆盖默认命令名 "cloud-controller-manager"。
func (cb *CommandBuilder) SetCmdName(name string) {
	cb.cmdName = name
}

// SetLongDesc 覆盖默认的命令长描述。
func (cb *CommandBuilder) SetLongDesc(long string) {
	cb.long = long
}

// SetProviderDefaults can be called to change the default values for some
// options when a flag is not set
// 这些默认值只在 flag 未显式设置时生效；显式传入的 flag 始终优先。
func (cb *CommandBuilder) SetProviderDefaults(defaults options.ProviderDefaults) {
	cb.defaults = &defaults
}

// setdefaults 为未配置字段填入可工作的默认值，使 BuildCommand 不依赖调用顺序。
// 其中 options 为空时才根据 defaults 新建，允许调用方提前 SetOptions 以完全替换。
func (cb *CommandBuilder) setdefaults() {
	if cb.stopCh == nil {
		cb.stopCh = wait.NeverStop
	}

	if cb.cmdName == "" {
		cb.cmdName = "cloud-controller-manager"
	}

	if cb.long == "" {
		cb.long = `The Cloud controller manager is a daemon that embeds the cloud specific control loops shipped with Kubernetes.`
	}

	if cb.defaults == nil {
		cb.defaults = &options.ProviderDefaults{}
	}

	if cb.options == nil {
		opts, err := options.NewCloudControllerManagerOptionsWithProviderDefaults(*cb.defaults)
		if err != nil {
			fmt.Fprintf(os.Stderr, "unable to initialize command options: %v\n", err)
			os.Exit(1)
		}
		cb.options = opts
	}
}

// BuildCommand 生成可执行的 cobra 命令。RunE 中的顺序很重要：
// 先解析 flag 得到配置，再 Complete 补齐客户端等运行时依赖，
// 然后用 cloudInitializer 构造云实现，最后才启动控制器与 webhook。
func (cb *CommandBuilder) BuildCommand() *cobra.Command {
	cb.setdefaults()
	cmd := &cobra.Command{
		Use:  cb.cmdName,
		Long: cb.long,
		RunE: func(cmd *cobra.Command, args []string) error {
			verflag.PrintAndExitIfRequested()
			cliflag.PrintFlags(cmd.Flags())

			// 控制器/webhook 的启用集合必须来自注册表而非硬编码，才能反映厂商定制。
			config, err := cb.options.Config(ControllerNames(cb.controllerInitFuncConstructors), ControllersDisabledByDefault.List(),
				cb.controllerAliases, WebhookNames(cb.webhookConfigs), WebhooksDisabledByDefault.List())
			if err != nil {
				fmt.Fprintf(os.Stderr, "%v\n", err)
				return err
			}
			completedConfig := config.Complete()
			cloud := cb.cloudInitializer(completedConfig)
			controllerInitializers := ConstructControllerInitializers(cb.controllerInitFuncConstructors, completedConfig, cloud)
			webhooks := NewWebhookHandlers(cb.webhookConfigs, completedConfig, cloud)

			if err := Run(completedConfig, cloud, controllerInitializers, webhooks, cb.stopCh); err != nil {
				fmt.Fprintf(os.Stderr, "%v\n", err)
				return err
			}
			return nil
		},
		Args: func(cmd *cobra.Command, args []string) error {
			// CCM 只接受 flag，收到位置参数视为启动配置错误。
			for _, arg := range args {
				if len(arg) > 0 {
					return fmt.Errorf("%q does not take any arguments, got %q", cmd.CommandPath(), args)
				}
			}
			return nil
		},
	}

	fs := cmd.Flags()
	namedFlagSets := cb.options.Flags(ControllerNames(cb.controllerInitFuncConstructors), ControllersDisabledByDefault.List(), cb.controllerAliases,
		WebhookNames(cb.webhookConfigs), WebhooksDisabledByDefault.List())
	verflag.AddFlags(namedFlagSets.FlagSet("global"))
	globalflag.AddGlobalFlags(namedFlagSets.FlagSet("global"), cmd.Name())

	// 先加通用 flag，再加厂商附加 flag；分组信息同时保留给 usage/help 渲染。
	for _, f := range namedFlagSets.FlagSets {
		fs.AddFlagSet(f)
	}
	for _, f := range cb.additionalFlags.FlagSets {
		fs.AddFlagSet(f)
	}

	usageFmt := "Usage:\n  %s\n"
	cols, _, _ := term.TerminalSize(cmd.OutOrStdout())
	cmd.SetUsageFunc(func(cmd *cobra.Command) error {
		fmt.Fprintf(cmd.OutOrStderr(), usageFmt, cmd.UseLine())
		cliflag.PrintSections(cmd.OutOrStderr(), namedFlagSets, cols)
		return nil
	})
	cmd.SetHelpFunc(func(cmd *cobra.Command, args []string) {
		fmt.Fprintf(cmd.OutOrStdout(), "%s\n\n"+usageFmt, cmd.Long, cmd.UseLine())
		cliflag.PrintSections(cmd.OutOrStdout(), namedFlagSets, cols)
	})

	return cmd
}
