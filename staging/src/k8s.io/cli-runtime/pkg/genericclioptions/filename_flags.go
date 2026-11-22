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

package genericclioptions

import (
	"strings"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"

	"k8s.io/cli-runtime/pkg/resource"
)

// FileNameFlags are flags for processing files.
// Usage of this struct by itself is discouraged.
// These flags are composed by ResourceBuilderFlags
// which should be used instead.
//
// FileNameFlags 描述输入文件来源相关的 flag；字段同样为指针，
// 为 nil 表示该 flag 不注册。通常应通过 ResourceBuilderFlags 组合使用。
type FileNameFlags struct {
	// Usage 作为 -f 的帮助文本，由使用方自定义以说明文件用途。
	Usage string

	Filenames *[]string // -f
	Kustomize *string   // -k
	Recursive *bool     // -R
}

// ToOptions creates a new FileNameOptions struct and sets FilenameOptions based on FileNameflags
//
// 接收者为 nil 时返回零值 options，调用方无需先判空。
func (o *FileNameFlags) ToOptions() resource.FilenameOptions {
	options := resource.FilenameOptions{}

	if o == nil {
		return options
	}

	if o.Recursive != nil {
		options.Recursive = *o.Recursive
	}
	if o.Filenames != nil {
		options.Filenames = *o.Filenames
	}
	if o.Kustomize != nil {
		options.Kustomize = *o.Kustomize
	}

	return options
}

// AddFlags binds file name flags to a given flagset
func (o *FileNameFlags) AddFlags(flags *pflag.FlagSet) {
	if o == nil {
		return
	}

	if o.Recursive != nil {
		flags.BoolVarP(o.Recursive, "recursive", "R", *o.Recursive, "Process the directory used in -f, --filename recursively. Useful when you want to manage related manifests organized within the same directory.")
	}
	if o.Filenames != nil {
		flags.StringSliceVarP(o.Filenames, "filename", "f", *o.Filenames, o.Usage)
		// 为 shell 补全注册可接受的文件后缀；去掉前导点以匹配补全框架的格式。
		annotations := make([]string, 0, len(resource.FileExtensions))
		for _, ext := range resource.FileExtensions {
			annotations = append(annotations, strings.TrimLeft(ext, "."))
		}
		flags.SetAnnotation("filename", cobra.BashCompFilenameExt, annotations)
	}
	// -k 与 -f/-R 互斥的约束由命令层的校验保证，这里无法在 flag 定义中表达。
	if o.Kustomize != nil {
		flags.StringVarP(o.Kustomize, "kustomize", "k", *o.Kustomize,
			"Process a kustomization directory. This flag can't be used together with -f or -R.")
	}
}
