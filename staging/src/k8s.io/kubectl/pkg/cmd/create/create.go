/*
Copyright 2014 The Kubernetes Authors.

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

package create

import (
	"fmt"
	"net/url"
	"runtime"
	"strings"

	"github.com/spf13/cobra"

	kruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/cli-runtime/pkg/genericclioptions"
	"k8s.io/cli-runtime/pkg/genericiooptions"
	"k8s.io/cli-runtime/pkg/resource"
	"k8s.io/klog/v2"
	cmdutil "k8s.io/kubectl/pkg/cmd/util"
	"k8s.io/kubectl/pkg/cmd/util/editor"
	"k8s.io/kubectl/pkg/rawhttp"
	"k8s.io/kubectl/pkg/scheme"
	"k8s.io/kubectl/pkg/util"
	"k8s.io/kubectl/pkg/util/i18n"
	"k8s.io/kubectl/pkg/util/templates"
)

// CreateOptions 是 'create' 子命令的命令行选项。
// 生命周期：NewCreateOptions（构造默认值）→ Complete（从 cmd 填充）→ Validate（校验组合）→ RunCreate（消费）
type CreateOptions struct {
	PrintFlags  *genericclioptions.PrintFlags  // -o 输出格式等打印选项
	RecordFlags *genericclioptions.RecordFlags // --record，已废弃但保留解析兼容旧脚本

	DryRunStrategy cmdutil.DryRunStrategy // none / client / server

	ValidationDirective string // --validate=strict|warn|ignore
	CreateAnnotation    bool   // --save-config

	fieldManager string // --field-manager，SSA 字段归属标识，未导出：只在本命令内使用

	FilenameOptions  resource.FilenameOptions // -f 文件/目录/URL/stdin 及 --recursive
	Selector         string                   // --selector，过滤清单中的资源
	EditBeforeCreate bool                     // --edit，创建前先打开编辑器修改
	Raw              string                   // --raw，直接 POST 原始内容到指定 URI

	Recorder    genericclioptions.Recorder      // --record 的执行体；未启用时是 NoopRecorder
	PrintObj    func(obj kruntime.Object) error // Complete 里由 PrintFlags 装配的输出闭包
	editOptions *editor.EditOptions             // --edit 时复用 kubectl edit 的完整实现

	genericiooptions.IOStreams // stdin/stdout/stderr，注入编辑器与 printer
}

var (
	createLong = templates.LongDesc(i18n.T(`
		Create a resource from a file or from stdin.

		JSON and YAML formats are accepted.`))

	createExample = templates.Examples(i18n.T(`
		# Create a pod using the data in pod.json
		kubectl create -f ./pod.json

		# Create a pod based on the JSON passed into stdin
		cat pod.json | kubectl create -f -

		# Edit the data in registry.yaml in JSON then create the resource using the edited data
		kubectl create -f registry.yaml --edit -o json`))
)

// NewCreateOptions 只填充编译期即可确定的默认值（默认输出后缀 "created"）；
// 其余字段留零值，等 Complete 阶段从命令行解析后再填。
func NewCreateOptions(ioStreams genericiooptions.IOStreams) *CreateOptions {
	return &CreateOptions{
		// 打印前缀 "created"：输出形如 "pod/foo created"
		PrintFlags:  genericclioptions.NewPrintFlags("created").WithTypeSetter(scheme.Scheme),
		RecordFlags: genericclioptions.NewRecordFlags(),

		// Complete 里若未启用 --record 会被覆盖为真实 recorder；这里先给 no-op 占位
		Recorder: genericclioptions.NoopRecorder{},

		IOStreams: ioStreams,
	}
}

// NewCmdCreate 返回 `kubectl create` 命令的新实例。
// 它本身不直接创建任何具体资源，而是一个"命令聚合器"：
//   - 无参数时依赖 -f FILENAME 从文件/stdin 读取资源清单并 POST 到 apiserver；
//   - 末尾注册的各 NewCmdCreateXxx 才是创建具体资源（namespace、secret、deployment 等）的快捷方式。
func NewCmdCreate(f cmdutil.Factory, ioStreams genericiooptions.IOStreams) *cobra.Command {
	// 用默认值构造 options，持有本命令的全部可配置状态（文件选项、编辑开关、selector 等）
	o := NewCreateOptions(ioStreams)

	cmd := &cobra.Command{
		Use:                   "create -f FILENAME",
		DisableFlagsInUseLine: true, // 用法行已含 "-f FILENAME"，禁止 cobra 自动追加 "[flags]"
		Short:                 i18n.T("Create a resource from a file or from stdin"),
		Long:                  createLong,
		Example:               createExample,
		Run: func(cmd *cobra.Command, args []string) {
			// 经典三段式：Complete（把 flag/args 解析进 options）→ Validate（校验组合合法性）→ RunCreate（真正发请求）
			cmdutil.CheckErr(o.Complete(f, cmd, args))
			cmdutil.CheckErr(o.Validate())
			cmdutil.CheckErr(o.RunCreate(f))
		},
	}

	// 绑定各 flag 结构体：--record 已废弃但仍保留解析，避免老脚本报 "unknown flag"
	o.RecordFlags.AddFlags(cmd)

	usage := "to use to create the resource"
	cmdutil.AddFilenameOptionFlags(cmd, &o.FilenameOptions, usage)
	cmdutil.AddValidateFlags(cmd)
	cmd.Flags().BoolVar(&o.EditBeforeCreate, "edit", o.EditBeforeCreate, "Edit the API resource before creating")
	// 编辑器换行符风格跟随平台：Windows 用 CRLF，其余用 LF，避免编辑后 diff 出现整文件变更
	cmd.Flags().Bool("windows-line-endings", runtime.GOOS == "windows",
		"Only relevant if --edit=true. Defaults to the line ending native to your platform.")
	cmdutil.AddApplyAnnotationFlags(cmd) // --save-config：把创建时的选项写进 last-applied annotation，供后续 kubectl apply diff
	cmdutil.AddDryRunFlag(cmd)
	cmdutil.AddLabelSelectorFlagVar(cmd, &o.Selector) // --selector：只处理清单中匹配 label 的资源
	cmd.Flags().StringVar(&o.Raw, "raw", o.Raw, "Raw URI to POST to the server.  Uses the transport specified by the kubeconfig file.")
	cmdutil.AddFieldManagerFlagVar(cmd, &o.fieldManager, "kubectl-create") // --field-manager：SSA 冲突检测时标识本命令的字段归属

	o.PrintFlags.AddFlags(cmd) // -o name 等输出格式；create 默认打印 "xxx/yyy created"，不需要完整对象

	// 注册 create 子命令：聚合所有 `create <type>` 快捷子命令，它们共享同一个 factory 和 ioStreams
	cmd.AddCommand(NewCmdCreateNamespace(f, ioStreams))
	cmd.AddCommand(NewCmdCreateQuota(f, ioStreams))
	cmd.AddCommand(NewCmdCreateSecret(f, ioStreams))
	cmd.AddCommand(NewCmdCreateConfigMap(f, ioStreams))
	cmd.AddCommand(NewCmdCreateServiceAccount(f, ioStreams))
	cmd.AddCommand(NewCmdCreateService(f, ioStreams))
	cmd.AddCommand(NewCmdCreateDeployment(f, ioStreams))
	cmd.AddCommand(NewCmdCreateClusterRole(f, ioStreams))
	cmd.AddCommand(NewCmdCreateClusterRoleBinding(f, ioStreams))
	cmd.AddCommand(NewCmdCreateRole(f, ioStreams))
	cmd.AddCommand(NewCmdCreateRoleBinding(f, ioStreams))
	cmd.AddCommand(NewCmdCreatePodDisruptionBudget(f, ioStreams))
	cmd.AddCommand(NewCmdCreatePriorityClass(f, ioStreams))
	cmd.AddCommand(NewCmdCreateJob(f, ioStreams))
	cmd.AddCommand(NewCmdCreateCronJob(f, ioStreams))
	cmd.AddCommand(NewCmdCreateIngress(f, ioStreams))
	cmd.AddCommand(NewCmdCreateToken(f, ioStreams))
	return cmd
}

// Validate 确保命令选项之间没有冲突。校验 flag 组合的互斥性；仅在 Validate 阶段报错可以在
// 触碰 apiserver 之前就拦下非法组合（如 --raw 与 --edit 不能并存）。
func (o *CreateOptions) Validate() error {
	// -f 文件、目录、URL 或 kustomize.yaml 至少要给一个
	if err := o.FilenameOptions.RequireFilenameOrKustomize(); err != nil {
		return err
	}

	// --raw 模式直接 POST 原始内容到指定 URI，绕过资源构建流程，
	// 因此和所有依赖清单解析/编辑/输出的 flag 都互斥
	if len(o.Raw) > 0 {
		if o.EditBeforeCreate {
			return fmt.Errorf("--raw and --edit are mutually exclusive")
		}
		// raw 只接受一个数据源，多对象 POST 到任意 URI 语义不明确
		if len(o.FilenameOptions.Filenames) != 1 {
			return fmt.Errorf("--raw can only use a single local file or stdin")
		}
		if strings.Index(o.FilenameOptions.Filenames[0], "http://") == 0 || strings.Index(o.FilenameOptions.Filenames[0], "https://") == 0 {
			return fmt.Errorf("--raw cannot read from a url")
		}
		if o.FilenameOptions.Recursive {
			return fmt.Errorf("--raw and --recursive are mutually exclusive")
		}
		if len(o.Selector) > 0 {
			return fmt.Errorf("--raw and --selector (-l) are mutually exclusive")
		}
		if o.PrintFlags.OutputFormat != nil && len(*o.PrintFlags.OutputFormat) > 0 {
			return fmt.Errorf("--raw and --output are mutually exclusive")
		}
		// 只允许路径形式（如 /api/v1/namespaces/test/pods），拒绝整 URL —— 目标主机由 kubeconfig 决定
		if _, err := url.ParseRequestURI(o.Raw); err != nil {
			return fmt.Errorf("--raw must be a valid URL path: %v", err)
		}
	}

	return nil
}

// Complete 把命令行 flag/args 解析成可执行状态：解析 --record、--dry-run、--validate、
// --save-config，并在 --edit 时提前初始化编辑器 options，最后装配 PrintObj 输出函数。
// 调用顺序在 Run 回调里位于 Validate 之前——先"收集"，再"校验"，最后"执行"。
func (o *CreateOptions) Complete(f cmdutil.Factory, cmd *cobra.Command, args []string) error {
	// create 的资源都来自 -f 文件/stdin，不接受位置参数
	if len(args) != 0 {
		return cmdutil.UsageErrorf(cmd, "Unexpected args: %v", args)
	}
	var err error
	// --record 生命周期：flags.Complete 解析 → ToRecorder 产出写入 change-cause 的记录器
	o.RecordFlags.Complete(cmd)
	o.Recorder, err = o.RecordFlags.ToRecorder()
	if err != nil {
		return err
	}

	// client / server / none 三种 dry-run 模式；server 模式要传给 apiserver 但不落库
	o.DryRunStrategy, err = cmdutil.GetDryRunStrategy(cmd)
	if err != nil {
		return err
	}
	// server dry-run 下部分输出格式（如 -o yaml 展示 status）需与真实 create 区分，故联动调整
	cmdutil.PrintFlagsWithDryRunStrategy(o.PrintFlags, o.DryRunStrategy)

	// --validate=strict/warn/ignore，传给 f.Validator 决定 schema 校验行为
	o.ValidationDirective, err = cmdutil.GetValidationDirective(cmd)
	if err != nil {
		return err
	}

	// --save-config：把当前配置写入 last-applied-configuration 注解，供 apply 做三方 diff
	o.CreateAnnotation = cmdutil.GetFlagBool(cmd, cmdutil.ApplyAnnotationsFlag)

	if o.EditBeforeCreate {
		// --edit 复用 kubectl edit 的整套实现：打开编辑器改完 YAML 后再走创建流程
		editOptions := editor.NewEditOptions(editor.EditBeforeCreateMode, o.IOStreams)
		// 编辑期间文件、校验、输出、注解等行为都与 create 保持一致
		editOptions.FilenameOptions = o.FilenameOptions
		editOptions.ValidateOptions = cmdutil.ValidateOptions{
			ValidationDirective: o.ValidationDirective,
		}
		editOptions.PrintFlags = o.PrintFlags
		editOptions.ApplyAnnotation = o.CreateAnnotation
		editOptions.RecordFlags = o.RecordFlags
		editOptions.FieldManager = "kubectl-create"
		if err := editOptions.Complete(f, []string{}, cmd); err != nil {
			return err
		}
		o.editOptions = editOptions
	}

	printer, err := o.PrintFlags.ToPrinter()
	if err != nil {
		return err
	}

	// 闭包缓存 printer，RunCreate 循环里直接回调即可，无需重复解析输出格式
	o.PrintObj = func(obj kruntime.Object) error {
		return printer.PrintObj(obj, o.Out)
	}

	return nil
}

// RunCreate 是创建的执行入口，按优先级分三条路径：
//  1. --edit：交给 edit 流程（打开编辑器 → 保存后创建）；
//  2. --raw：绕过资源构建，直接把文件内容 POST 到指定 URI；
//  3. 默认：用 Builder 读入清单 → 逐个资源 POST → 打印结果。
func (o *CreateOptions) RunCreate(f cmdutil.Factory) error {
	if o.EditBeforeCreate {
		// Complete() 未执行完就到这里说明 Run 回调顺序被破坏，防御性检查
		if o.editOptions == nil {
			return fmt.Errorf("EditBeforeCreate requires edit options to be initialized via Complete()")
		}
		return o.editOptions.Run()
	}

	// --raw 只对单文件资源有意义（多个对象大概率不是你想要的），validator 已强制这一约束。
	// --raw 路径：RESTClient 的 transport（TLS/认证）由 kubeconfig 提供，Body 来自唯一的数据源文件
	if len(o.Raw) > 0 {
		restClient, err := f.RESTClient()
		if err != nil {
			return err
		}
		return rawhttp.RawPost(restClient, o.IOStreams, o.Raw, o.FilenameOptions.Filenames[0])
	}

	// 按前面的 ValidationDirective 取 schema 校验器（OpenAPI/CELSchema），Builder 读入时会调用
	schema, err := f.Validator(o.ValidationDirective)
	if err != nil {
		return err
	}

	// 从 kubeconfig 解析目标 namespace；enforceNamespace 表示清单里是否允许省略/覆盖 namespace
	cmdNamespace, enforceNamespace, err := f.ToRawKubeConfigLoader().Namespace()
	if err != nil {
		return err
	}

	// Builder 把 -f/--selector 指定的输入统一转成可访问的 resource.Info 列表：
	// Unstructured：不经 Go 类型解码，未知类型也能创建（否则 create 无从谈起）；Schema：读入即做校验；Flatten：展开 List
	r := f.NewBuilder().
		Unstructured().
		Schema(schema).
		ContinueOnError().
		NamespaceParam(cmdNamespace).DefaultNamespace().
		FilenameParam(enforceNamespace, &o.FilenameOptions).
		LabelSelectorParam(o.Selector).
		Flatten().
		Do()
	err = r.Err()
	if err != nil {
		return err
	}

	count := 0
	// Visit 逐个处理解析出的资源；出错时携带来源信息，便于定位是哪个文件出的问题
	err = r.Visit(func(info *resource.Info, err error) error {
		if err != nil {
			return err
		}
		// --save-config 时写入 last-applied-configuration，供后续 apply 做三方合并
		if err := util.CreateOrUpdateAnnotation(o.CreateAnnotation, info.Object, scheme.DefaultJSONEncoder()); err != nil {
			return cmdutil.AddSourceToErr("creating", info.Source, err)
		}

		// --record 写 change-cause；失败只降级记日志，不阻断创建
		if err := o.Recorder.Record(info.Object); err != nil {
			klog.V(4).Infof("error recording current command: %v", err)
		}

		if o.DryRunStrategy != cmdutil.DryRunClient {
			// resource.Helper 封装 POST：DryRun(server) 时加 ?dryRun=server 参数，
			// fieldManager/fieldValidation 对应 --field-manager 与 --validate
			obj, err := resource.
				NewHelper(info.Client, info.Mapping).
				DryRun(o.DryRunStrategy == cmdutil.DryRunServer).
				WithFieldManager(o.fieldManager).
				WithFieldValidation(o.ValidationDirective).
				Create(info.Namespace, true, info.Object)
			if err != nil {
				return cmdutil.AddSourceToErr("creating", info.Source, err)
			}
			// 用 apiserver 返回的对象刷新 info（补全 uid/resourceVersion 等服务端字段）
			info.Refresh(obj, true)
		}

		count++

		// client dry-run 时 info.Object 保持本地状态，只打印不落库
		return o.PrintObj(info.Object)
	})
	if err != nil {
		return err
	}
	if count == 0 {
		return fmt.Errorf("no objects passed to create")
	}
	return nil
}

// 假定首个参数为资源名的命令可用这个工具函数取名。
func NameFromCommandArgs(cmd *cobra.Command, args []string) (string, error) {
	argsLen := cmd.ArgsLenAtDash()
	// 未指定 -- 时 ArgsLenAtDash 返回 -1。
	if argsLen == -1 {
		argsLen = len(args)
	}
	if argsLen != 1 {
		return "", cmdutil.UsageErrorf(cmd, "exactly one NAME is required, got %d", argsLen)
	}
	return args[0], nil
}
