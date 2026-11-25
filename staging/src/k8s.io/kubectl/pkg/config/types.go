/*
Copyright 2024 The Kubernetes Authors.

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

package config

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// Preference 存储 KubeRC 配置文件中的各个元素。
type Preference struct {
	metav1.TypeMeta

	// Defaults 允许修改命令的默认选项值。
	// 当用户不想每次都显式设置选项时，
	// 这一点尤其有用。
	// +optional
	Defaults []CommandDefaults

	// Aliases 允许为现有的 kubectl 命令定义别名，并可附带默认的选项值。
	// 如果别名与内置命令同名，内置命令始终优先。
	// defaults 部分中定义的选项覆盖不适用于同一命令的别名。
	// kubectl [ALIAS NAME] [USER_OPTIONS] [USER_EXPLICIT_ARGS] expands to
	// kubectl [COMMAND] # built-in command alias points to
	//         [KUBERC_PREPEND_ARGS]
	//         [USER_OPTIONS]
	//         [KUBERC_OPTIONS] # rest of the options that are not passed by user in [USER_OPTIONS]
	//         [USER_EXPLICIT_ARGS]
	//         [KUBERC_APPEND_ARGS]
	// e.g.
	// - name: runx
	//   command: run
	//   options:
	//   - name: image
	//     default: nginx
	//   appendArgs:
	//   - --
	//   - custom-arg1
	// 例如，如果用户调用 "kubectl runx test-pod" 命令，
	// 该命令将展开为 "kubectl run --image=nginx test-pod -- custom-arg1"
	// - name: getn
	//   command: get
	//   options:
	//   - name: output
	//     default: wide
	//   prependArgs:
	//   - node
	// "kubectl getn control-plane-1" expands to "kubectl get node control-plane-1 --output=wide"
	// "kubectl getn control-plane-1 --output=json" expands to "kubectl get node --output=json control-plane-1"
	// +optional
	Aliases []AliasOverride

	// credentialPluginPolicy 指定管理哪些 client-go 凭据插件
	// 可以执行（如果有）的策略。它必须是以下值之一：
	// { "", "AllowAll", "DenyAll", "Allowlist" }。
	// 如果策略为 ""，则回退到 "AllowAll"（这是保持向后兼容所必需的）。
	// 如果策略为 DenyAll，则不允许运行任何凭据插件。
	// 如果策略为 Allowlist，则只有满足 `credentialPluginAllowlist`
	// 字段中所指定条件的插件才可以运行。
	// +optional
	CredentialPluginPolicy CredentialPluginPolicy

	// Allowlist 是由允许列表条目组成的切片。如果其中任何
	// 一个条目匹配，则相应的可执行文件可以运行。也就是说，
	// 结果是允许列表中所有条目的逻辑或。如果策略不是
	// "Allowlist"，则绝不能提供此列表。
	//
	// e.g.
	// credentialPluginAllowlist:
	// - name: cloud-provider-plugin
	// - name: /usr/local/bin/my-plugin
	// 在上面的示例中，用户允许使用凭据插件
	// `cloud-provider-plugin`（在 PATH 中的某处找到），以及位于显式路径
	// `/usr/local/bin/my-plugin` 的插件。
	// +optional
	CredentialPluginAllowlist []AllowlistEntry
}

// CredentialPluginPolicy 指定管理哪些 client-go 凭据插件
// 可以执行（如果有）的策略。它必须是以下值之一：
// { "", "AllowAll", "DenyAll", "Allowlist" }。
// 如果策略为 ""，则回退到 "AllowAll"（这是保持向后兼容所必需的）。
// 如果策略为 DenyAll，则不允许运行任何凭据插件。
// 如果策略为 Allowlist，则只有满足 `credentialPluginAllowlist`
// 字段中所指定条件的插件才可以运行。如果策略不是 `Allowlist`
// 却提供了该字段，则视为配置错误。
type CredentialPluginPolicy string

const (
	PluginPolicyAllowAll  CredentialPluginPolicy = "AllowAll"
	PluginPolicyDenyAll   CredentialPluginPolicy = "DenyAll"
	PluginPolicyAllowlist CredentialPluginPolicy = "Allowlist"
)

// AllowlistEntry 是允许列表中的一个条目。
// 对于每个允许列表项，至少要有一个字段非空。
// 所有字段都为空的结构体被视为配置错误。
// 每个字段都是一项执行条件。如果指定了多个字段，
// 则必须满足所有指定字段的条件。
// 也就是说，单个条目的结果是该条目内所有
// 指定字段对应检查的逻辑与。
type AllowlistEntry struct {
	// 命令匹配时，首先使用 `exec.LookPath` 解析插件
	// 和允许列表条目中名称的绝对路径。该调用会分别
	// 作用于两者，所得字符串必须相等。如果任意一次
	// `exec.LookPath` 调用出错，`Command` 检查将被视为
	// 失败。
	Command string
}

// AliasOverride 存储别名定义。
type AliasOverride struct {
	// Name 是别名的名称，只能包含字母字符。
	// 如果别名与内置命令冲突，
	// 将使用内置命令。
	Name string
	// Command 是要执行的单条或一组命令，例如 "set env" 或 "create"
	Command string
	// PrependArgs 存储资源名称等参数。
	// 这些参数会插入到别名名称之后。
	PrependArgs []string
	// AppendArgs 存储资源名称等参数。
	// 这些参数会追加到 USER_ARGS 之后。
	AppendArgs []string
	// Options 用于存放别名的选项定义。
	// Options 只修改选项的默认值，如果用户
	// 显式传递了值，则使用显式值。
	Options []CommandOptionDefault
}

// CommandDefaults 存储命令及其关联选项的
// 默认值。
type CommandDefaults struct {
	// Command 指其标志默认值被修改的命令。
	Command string
	// Options 是存储不同默认值的选项列表。
	Options []CommandOptionDefault
}

// CommandOptionDefault 存储某个选项的名称
// 和指定的默认值。
type CommandOptionDefault struct {
	// 选项名称（完整形式，不带短横线）。
	Name string

	// 默认值的字符串形式。kubectl 会将其解析为
	// 与该选项兼容的值。
	Default string
}
