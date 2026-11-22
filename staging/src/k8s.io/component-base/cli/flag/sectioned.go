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

package flag

import (
	"bytes"
	"fmt"
	"io"
	"strings"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

const (
	usageFmt = "Usage:\n  %s\n"
)

// NamedFlagSets 按调用 FlagSet 的先后顺序保存命名的 flag set。
// 组件把 flag 按用途分组（global、logging、etcd 等），以便帮助信息分节展示，
// 同时在只注册部分 flag 的场景下仍能保持稳定输出。
type NamedFlagSets struct {
	// Order 是按注册顺序排列的 flag set 名称，保证帮助信息分节顺序稳定。
	Order []string
	// FlagSets 按名称保存 flag set。
	FlagSets map[string]*pflag.FlagSet
	// NormalizeNameFunc 用于初始化由 NamedFlagSets 创建的 FlagSet 的名称归一化函数。
	NormalizeNameFunc func(f *pflag.FlagSet, name string) pflag.NormalizedName
}

// FlagSet 返回指定名称的 flag set；若尚未创建，则新建并追加到 Order。
// 这里惰性创建是为了让调用方按需注册 flag，而不必预先声明分组。
func (nfs *NamedFlagSets) FlagSet(name string) *pflag.FlagSet {
	if nfs.FlagSets == nil {
		nfs.FlagSets = map[string]*pflag.FlagSet{}
	}
	if _, ok := nfs.FlagSets[name]; !ok {
		flagSet := pflag.NewFlagSet(name, pflag.ExitOnError)
		// 默认继承全局归一化函数，保证与主 flag set 的下划线/连字符规则一致。
		flagSet.SetNormalizeFunc(pflag.CommandLine.GetNormalizeFunc())
		// 显式配置的归一化函数优先于全局设置。
		if nfs.NormalizeNameFunc != nil {
			flagSet.SetNormalizeFunc(nfs.NormalizeNameFunc)
		}
		nfs.FlagSets[name] = flagSet
		nfs.Order = append(nfs.Order, name)
	}
	return nfs.FlagSets[name]
}

// PrintSections 按分组打印 flag，按 cols 指定的最大列宽换行；
// cols 为 0 时不换行。
func PrintSections(w io.Writer, fss NamedFlagSets, cols int) {
	for _, name := range fss.Order {
		fs := fss.FlagSets[name]
		// 空分组不输出标题，避免帮助信息里出现无内容的节。
		if !fs.HasFlags() {
			continue
		}

		// 复制到临时 flag set 再渲染，避免修改调用方持有的原始集合。
		wideFS := pflag.NewFlagSet("", pflag.ExitOnError)
		wideFS.AddFlagSet(fs)

		// pflag 的 FlagUsagesWrapped 会强制按宽度换行，还会截断 usage 尾部。
		// 注册一个超长名字的占位 flag，并以其输出位置作为“正文结束”的标记，
		// 从而保留 usage 的完整内容。
		var zzz string
		if cols > 24 {
			zzz = strings.Repeat("z", cols-24)
			wideFS.Int(zzz, 0, strings.Repeat("z", cols-24))
		}

		var buf bytes.Buffer
		fmt.Fprintf(&buf, "\n%s flags:\n\n%s", strings.ToUpper(name[:1])+name[1:], wideFS.FlagUsagesWrapped(cols))

		if cols > 24 {
			// 定位占位 flag 的起始位置，丢弃其后的内容（占位行及其后的换行）。
			i := strings.Index(buf.String(), zzz)
			lines := strings.Split(buf.String()[:i], "\n")
			fmt.Fprint(w, strings.Join(lines[:len(lines)-1], "\n"))
			fmt.Fprintln(w)
		} else {
			fmt.Fprint(w, buf.String())
		}
	}
}

// SetUsageAndHelpFunc 同时设置 cobra 命令的 usage 与 help 函数，
// 使输出只包含组件注册的 NamedFlagSets，而不是继承来的全部 flag。
func SetUsageAndHelpFunc(cmd *cobra.Command, fss NamedFlagSets, cols int) {
	cmd.SetUsageFunc(func(cmd *cobra.Command) error {
		fmt.Fprintf(cmd.OutOrStderr(), usageFmt, cmd.UseLine())
		PrintSections(cmd.OutOrStderr(), fss, cols)
		return nil
	})
	cmd.SetHelpFunc(func(cmd *cobra.Command, args []string) {
		// help 输出到 stdout，便于用户管道处理。
		fmt.Fprintf(cmd.OutOrStdout(), "%s\n\n"+usageFmt, cmd.Long, cmd.UseLine())
		PrintSections(cmd.OutOrStdout(), fss, cols)
	})
}
