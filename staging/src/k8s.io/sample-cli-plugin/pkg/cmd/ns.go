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

// 本包实现 kubectl-ns 插件的命令逻辑：读取 KUBECONFIG 中的 context 信息，
// 展示或切换当前命名空间，并把最终配置写回用户的 kubeconfig 文件。
// 用普通包而非 main 包承载逻辑，便于入口文件之外的测试直接构造选项并驱动各阶段。

package cmd

import (
	"fmt"
	"strings"

	"github.com/spf13/cobra"

	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/tools/clientcmd/api"

	"k8s.io/cli-runtime/pkg/genericclioptions"
	"k8s.io/cli-runtime/pkg/genericiooptions"
)

var (
	// 示例使用 %[1]s 占位命令名，这样 command 可以根据显示需要传入 "kubectl" 或其他前缀，
	// 而不用为每种调用方式维护一份字符串。
	namespaceExample = `
	# view the current namespace in your KUBECONFIG
	%[1]s ns

	# view all of the namespaces in use by contexts in your KUBECONFIG
	%[1]s ns --list

	# switch your current-context to one that contains the desired namespace
	%[1]s ns foo
`

	// 把“没有当前 context”的错误集中成一个变量：Complete 和 Validate 都可能触发它，
	// 复用同一实例可以避免两处的提示文案日后各自漂移。
	errNoContext = fmt.Errorf("no context is currently set, use %q to select a new one", "kubectl config use-context <context>")
)

// NamespaceOptions 保存更新用户 KUBECONFIG 当前 context 所需的全部状态。
// Complete、Validate、Run 各阶段只读写自己关心的字段，测试也可绕过 cobra 直接驱动。
type NamespaceOptions struct {
	// 复用 cli-runtime 的 ConfigFlags，让本命令自动支持 --kubeconfig、--context 等标准 kubectl 参数，
	// 而不必自己解析这些标识。
	configFlags *genericclioptions.ConfigFlags

	// resultingContext 与 resultingContextName 描述“准备写入”的目标 context，
	// 只有在用户显式给出命名空间时才会生成，纯查询分支保持为空。
	resultingContext     *api.Context
	resultingContextName string

	// 以下字段记录用户真正显式指定的值，用来区分“未指定”与“指定为空”。
	// 显式值优先于当前 context 的继承值，是覆盖规则的依据。
	userSpecifiedCluster   string
	userSpecifiedContext   string
	userSpecifiedAuthInfo  string
	userSpecifiedNamespace string

	// rawConfig 保存完整的 kubeconfig 快照，避免命令执行期间多次读取文件；
	// listNamespaces 控制是列出全部命名空间还是只报告当前命名空间；args 留给 Validate 校验位置参数个数。
	rawConfig      api.Config
	listNamespaces bool
	args           []string

	// 内嵌 IOStreams 使输出目标可注入，测试里可以换成缓冲区而不是写死 os.Stdout。
	genericiooptions.IOStreams
}

// NewNamespaceOptions 创建带默认值的 NamespaceOptions。
// ConfigFlags(true) 让默认值继承用户 kubeconfig 的当前 context，而非构造空配置。
func NewNamespaceOptions(streams genericiooptions.IOStreams) *NamespaceOptions {
	return &NamespaceOptions{
		configFlags: genericclioptions.NewConfigFlags(true),

		IOStreams: streams,
	}
}

// NewCmdNamespace 创建封装 NamespaceOptions 的 cobra 命令。
// SilenceUsage 避免在 RunE 已给出具体错误时再打印整段用法；
// DisplayName 注解确保帮助与提示显示 "kubectl ns"，而非二进制名 kubectl-ns。
func NewCmdNamespace(streams genericiooptions.IOStreams) *cobra.Command {
	o := NewNamespaceOptions(streams)

	cmd := &cobra.Command{
		Use:          "ns [new-namespace] [flags]",
		Short:        "View or set the current namespace",
		Example:      fmt.Sprintf(namespaceExample, "kubectl"),
		SilenceUsage: true,
		Annotations: map[string]string{
			cobra.CommandDisplayNameAnnotation: "kubectl ns",
		},
		RunE: func(c *cobra.Command, args []string) error {
			// 严格按 Complete -> Validate -> Run 的顺序执行：
			// 先把标志和 kubeconfig 汇总成选项状态，再做廉价的参数校验，最后才执行有副作用的写操作。
			if err := o.Complete(c, args); err != nil {
				return err
			}
			if err := o.Validate(); err != nil {
				return err
			}
			if err := o.Run(); err != nil {
				return err
			}

			return nil
		},
	}

	// --list 直接绑定到选项字段，避免命令层再维护一份标志状态需要同步。
	cmd.Flags().BoolVar(&o.listNamespaces, "list", o.listNamespaces, "if true, print the list of all namespaces in the current KUBECONFIG")
	// 把 ConfigFlags 挂到命令上，--context、--cluster、--user、--namespace 等标志由它统一定义和解析。
	o.configFlags.AddFlags(cmd.Flags())

	return cmd
}

// Complete 汇总更新当前 context 所需信息，但不写回 kubeconfig。
// 先读取配置快照，再解析目标命名空间及 context、cluster、user 覆盖值，供后续阶段使用一致视图。
func (o *NamespaceOptions) Complete(cmd *cobra.Command, args []string) error {
	o.args = args

	var err error
	o.rawConfig, err = o.configFlags.ToRawKubeConfigLoader().RawConfig()
	if err != nil {
		return err
	}

	o.userSpecifiedNamespace, err = cmd.Flags().GetString("namespace")
	if err != nil {
		return err
	}
	if len(args) > 0 {
		if len(o.userSpecifiedNamespace) > 0 {
			return fmt.Errorf("cannot specify both a --namespace value and a new namespace argument")
		}

		o.userSpecifiedNamespace = args[0]
	}

	// 未指定命名空间时不生成目标 context；集群与用户覆盖只对写操作有意义。
	// 提前返回也让尚未设置 context 的纯 --list 查询能继续工作。
	if len(o.userSpecifiedNamespace) == 0 {
		return nil
	}

	o.userSpecifiedContext, err = cmd.Flags().GetString("context")
	if err != nil {
		return err
	}

	o.userSpecifiedCluster, err = cmd.Flags().GetString("cluster")
	if err != nil {
		return err
	}

	o.userSpecifiedAuthInfo, err = cmd.Flags().GetString("user")
	if err != nil {
		return err
	}

	currentContext, exists := o.rawConfig.Contexts[o.rawConfig.CurrentContext]
	if !exists {
		return errNoContext
	}

	o.resultingContext = api.NewContext()
	o.resultingContext.Cluster = currentContext.Cluster
	o.resultingContext.AuthInfo = currentContext.AuthInfo

	// 默认以当前 context 为基底；用户指定目标 context 时改用其深拷贝。
	// 必须深拷贝，否则修改命名空间会污染 rawConfig 中的原对象。
	if len(o.userSpecifiedContext) > 0 {
		o.resultingContextName = o.userSpecifiedContext
		if userCtx, exists := o.rawConfig.Contexts[o.userSpecifiedContext]; exists {
			o.resultingContext = userCtx.DeepCopy()
		}
	}

	// 命名空间始终取位置参数或 --namespace；cluster 与 user 仅在显式给出时覆盖。
	o.resultingContext.Namespace = o.userSpecifiedNamespace

	if len(o.userSpecifiedCluster) > 0 {
		o.resultingContext.Cluster = o.userSpecifiedCluster
	}
	if len(o.userSpecifiedAuthInfo) > 0 {
		o.resultingContext.AuthInfo = o.userSpecifiedAuthInfo
	}

	// 未指定 context 名称时按内容生成确定性名字，使相同命名空间、集群和用户复用同一条目。
	if len(o.userSpecifiedContext) == 0 {
		o.resultingContextName = generateContextName(o.resultingContext)
	}

	return nil
}

// generateContextName 把 context 的三个关键字段拼成一个确定性的名字。
// 之所以对 AuthInfo 取第一段，是因为它常是 "user/cluster" 这类复合形式，
// 直接拼进去既让名字过长，也会把本已由 cluster 字段表达的信息再重复一遍。
func generateContextName(fromContext *api.Context) string {
	name := fromContext.Namespace
	if len(fromContext.Cluster) > 0 {
		name = fmt.Sprintf("%s/%s", name, fromContext.Cluster)
	}
	if len(fromContext.AuthInfo) > 0 {
		cleanAuthInfo := strings.Split(fromContext.AuthInfo, "/")[0]
		name = fmt.Sprintf("%s/%s", name, cleanAuthInfo)
	}

	return name
}

// Validate 检查执行命令前必须满足的参数前提：存在当前 context，且最多只有一个位置参数。
// --namespace 与位置参数互斥等规则已在 Complete 中处理。
func (o *NamespaceOptions) Validate() error {
	if len(o.rawConfig.CurrentContext) == 0 {
		return errNoContext
	}
	if len(o.args) > 1 {
		return fmt.Errorf("either one or no arguments are allowed")
	}

	return nil
}

// Run 列出 KUBECONFIG 中的命名空间，或按指定命名空间更新当前 context。
// 有目标命名空间时通过 setNamespace 写回；否则只读 rawConfig 输出当前或全部命名空间。
func (o *NamespaceOptions) Run() error {
	if len(o.userSpecifiedNamespace) > 0 && o.resultingContext != nil {
		return o.setNamespace(o.resultingContext, o.resultingContextName)
	}

	namespaces := map[string]bool{}

	for name, c := range o.rawConfig.Contexts {
		if !o.listNamespaces && name == o.rawConfig.CurrentContext {
			if len(c.Namespace) == 0 {
				return fmt.Errorf("no namespace is set for your current context: %q", name)
			}

			fmt.Fprintf(o.Out, "%s\n", c.Namespace)
			return nil
		}

		// 跳过空命名空间，并用 map 去重，确保 --list 不因多个 context 共享命名空间而重复输出。
		if len(c.Namespace) == 0 {
			continue
		}
		if namespaces[c.Namespace] {
			continue
		}

		namespaces[c.Namespace] = true
	}

	if !o.listNamespaces {
		return fmt.Errorf("unable to find information for the current namespace in your configuration")
	}

	for n := range namespaces {
		fmt.Fprintf(o.Out, "%s\n", n)
	}

	return nil
}

// isContextEqual 只比较 context 中会影响命名空间切换结果的三个字段。
// 故意不比较 LocationOfOrigin 等元数据：同一逻辑 context 的来源标记不同不应阻止复用。
func isContextEqual(ctxA, ctxB *api.Context) bool {
	if ctxA == nil || ctxB == nil {
		return false
	}
	if ctxA.Cluster != ctxB.Cluster {
		return false
	}
	if ctxA.Namespace != ctxB.Namespace {
		return false
	}
	if ctxA.AuthInfo != ctxB.AuthInfo {
		return false
	}

	return true
}

// setNamespace 保存目标 context 并将 CurrentContext 切换为指定名称。
// 仅在同名 context 不存在或内容不等价时覆盖；ModifyConfig 统一处理文件写入与备份。
func (o *NamespaceOptions) setNamespace(fromContext *api.Context, withContextName string) error {
	if len(fromContext.Namespace) == 0 {
		return fmt.Errorf("a non-empty namespace must be provided")
	}

	configAccess := clientcmd.NewDefaultPathOptions()

	// 内容一致时保留原条目，避免无意义改写；不等价时才替换同名 context。
	if existingResultingCtx, exists := o.rawConfig.Contexts[withContextName]; !exists || !isContextEqual(fromContext, existingResultingCtx) {
		o.rawConfig.Contexts[withContextName] = fromContext
	}
	o.rawConfig.CurrentContext = withContextName

	fmt.Fprintf(o.Out, "namespace changed to %q\n", fromContext.Namespace)
	return clientcmd.ModifyConfig(configAccess, o.rawConfig, true)
}
