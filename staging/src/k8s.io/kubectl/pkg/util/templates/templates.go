/*
Copyright 2016 The Kubernetes Authors.

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

package templates

import (
	"strings"
	"unicode"
)

const (
	// help 模板中声明模板变量的段落。
	SectionVars = `{{$isRootCmd := isRootCmd .}}` +
		`{{$rootCmd := rootCmd .}}` +
		`{{$visibleFlags := visibleFlags (flagsNotIntersected .LocalFlags .PersistentFlags)}}` +
		`{{$explicitlyExposedFlags := exposed .}}` +
		`{{$optionsCmdFor := optionsCmdFor .}}` +
		`{{$usageLine := usageLine .}}` +
		`{{$reverseParentsNames := reverseParentsNames .}}`

	// 显示命令别名的段落。
	SectionAliases = `{{if gt .Aliases 0}}Aliases:
{{.NameAndAliases}}

{{end}}`

	// 显示命令示例的段落。
	SectionExamples = `{{if .HasExample}}Examples:
{{trimRight .Example}}

{{end}}`

	// 显示子命令的段落。
	SectionSubcommands = `{{if .HasAvailableSubCommands}}{{cmdGroupsString .}}

{{end}}`

	// 显示命令标志的段落。
	SectionFlags = `{{ if or $visibleFlags.HasFlags $explicitlyExposedFlags.HasFlags}}Options:
{{ if $visibleFlags.HasFlags}}{{trimRight (flagsUsages $visibleFlags)}}{{end}}{{ if $explicitlyExposedFlags.HasFlags}}{{ if $visibleFlags.HasFlags}}
{{end}}{{trimRight (flagsUsages $explicitlyExposedFlags)}}{{end}}

{{end}}`

	// 显示命令用法的段落。
	SectionUsage = `{{if and .Runnable (ne .UseLine "") (ne .UseLine $rootCmd)}}Usage:
  {{$usageLine}}

{{end}}`

	// 显示 '--help' 提示的段落。
	SectionTipsHelp = `{{if .HasSubCommands}}Use "{{range $reverseParentsNames}}{{.}} {{end}}<command> --help" for more information about a given command.
{{end}}`

	// 显示 'options' 提示以查看全局标志的段落。
	SectionTipsGlobalOptions = `{{if $optionsCmdFor}}Use "{{$optionsCmdFor}}" for a list of global command-line options (applies to all commands).
{{end}}`
)

// 大多数命令使用的 help 模板。
func MainHelpTemplate() string {
	return `{{with or .Long .Short }}{{. | trim}}{{end}}{{if or .Runnable .HasSubCommands}}{{.UsageString}}{{end}}`
}

// 大多数命令使用的 usage 模板。
func MainUsageTemplate() string {
	sections := []string{
		"\n\n",
		SectionVars,
		SectionAliases,
		SectionExamples,
		SectionSubcommands,
		SectionFlags,
		SectionUsage,
		SectionTipsHelp,
		SectionTipsGlobalOptions,
	}
	return strings.TrimRightFunc(strings.Join(sections, ""), unicode.IsSpace)
}

// 'options' 命令使用的 help 模板。
func OptionsHelpTemplate() string {
	return ""
}

// 'options' 命令使用的 usage 模板。
func OptionsUsageTemplate() string {
	return `{{ if .HasInheritedFlags}}The following options can be passed to any command:

{{flagsUsages .InheritedFlags}}{{end}}`
}
