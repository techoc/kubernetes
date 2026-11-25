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

package v1alpha1

import metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// Preference 存储 KubeRC 配置文件中的各个元素。
type Preference struct {
	metav1.TypeMeta `json:""`

	// overrides 允许修改命令的默认标志值。
	// 当用户不想每次都显式
	// 设置标志时，这一点尤其有用。
	// +listType=atomic
	Defaults []CommandDefaults `json:"overrides"`

	// aliases 允许为现有的 kubectl 命令定义别名，并可附带默认的标志值。
	// 如果别名与内置命令同名，内置命令始终优先。
	// overrides 部分中定义的标志覆盖不适用于同一命令的别名。
	// kubectl [ALIAS NAME] [USER_FLAGS] [USER_EXPLICIT_ARGS] expands to
	// kubectl [COMMAND] # built-in command alias points to
	//         [KUBERC_PREPEND_ARGS]
	//         [USER_FLAGS]
	//         [KUBERC_FLAGS] # rest of the flags that are not passed by user in [USER_FLAGS]
	//         [USER_EXPLICIT_ARGS]
	//         [KUBERC_APPEND_ARGS]
	// e.g.
	// - name: runx
	//   command: run
	//   flags:
	//   - name: image
	//     default: nginx
	//   appendArgs:
	//   - --
	//   - custom-arg1
	// 例如，如果用户调用 "kubectl runx test-pod" 命令，
	// 该命令将展开为 "kubectl run --image=nginx test-pod -- custom-arg1"
	// - name: getn
	//   command: get
	//   flags:
	//   - name: output
	//     default: wide
	//   prependArgs:
	//   - node
	// "kubectl getn control-plane-1" expands to "kubectl get node control-plane-1 --output=wide"
	// "kubectl getn control-plane-1 --output=json" expands to "kubectl get node --output=json control-plane-1"
	// +listType=atomic
	Aliases []AliasOverride `json:"aliases"`
}

// AliasOverride 存储别名定义。
type AliasOverride struct {
	// name 是别名的名称，只能包含字母字符。
	// 如果别名与内置命令冲突，
	// 将使用内置命令。
	Name string `json:"name"`
	// command 是要执行的单条或一组命令，例如 "set env" 或 "create"
	Command string `json:"command"`
	// prependArgs 存储资源名称等参数。
	// 这些参数会插入到别名名称之后。
	// +listType=atomic
	PrependArgs []string `json:"prependArgs,omitempty"`
	// appendArgs 存储资源名称等参数。
	// 这些参数会追加到 USER_ARGS 之后。
	// +listType=atomic
	AppendArgs []string `json:"appendArgs,omitempty"`
	// flags 用于存放别名的标志定义。
	// flags 只修改标志的默认值，如果用户
	// 显式传递了值，则使用显式值。
	// +listType=atomic
	Options []CommandOptionDefault `json:"flags,omitempty"`
}

// CommandDefaults 存储命令及其关联
// 选项的默认值。
type CommandDefaults struct {
	// command 指其标志默认值被修改的命令。
	Command string `json:"command"`
	// flags 是存储不同默认值的标志列表。
	// +listType=atomic
	Options []CommandOptionDefault `json:"flags"`
}

// CommandOptionDefault 存储某个选项的名称
// 和指定的默认值。
type CommandOptionDefault struct {
	// 标志名称（完整形式，不带短横线）。
	Name string `json:"name"`

	// 默认值的字符串形式。kubectl 会将其解析为
	// 与该标志兼容的值。
	Default string `json:"default"`
}
