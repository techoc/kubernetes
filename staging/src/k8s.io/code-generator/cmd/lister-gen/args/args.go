/*
Copyright 2017 The Kubernetes Authors.

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

// Package args 定义 lister-gen 的命令行参数。
// 输出目录与输出 import 路径必须成对提供，否则生成结果无法确定
// 文件落点与包引用。
package args

import (
	"fmt"

	"github.com/spf13/pflag"

	"k8s.io/code-generator/pkg/apidefinitions"
)

// Args is used by the gengo framework to pass args specific to this generator.
type Args struct {
	OutputDir    string // must be a directory path
	OutputPkg    string // must be a Go import-path
	GoHeaderFile string

	// PluralExceptions specify list of exceptions used when pluralizing certain types.
	// For example 'Endpoints:Endpoints', otherwise the pluralizer will generate 'Endpointes'.
	//
	// 复数例外同时影响生成的 lister 方法名，必须与 client-gen 传入的一致，
	// 否则 lister 与 client 的资源名会对不上。
	PluralExceptions []string

	apidefinitions.LintArgs
}

// New returns default arguments for the generator.
func New() *Args {
	return &Args{}
}

// AddFlags add the generator flags to the flag set.
func (args *Args) AddFlags(fs *pflag.FlagSet) {
	fs.StringVar(&args.OutputDir, "output-dir", "",
		"the base directory under which to generate results")
	fs.StringVar(&args.OutputPkg, "output-pkg", "",
		"the base Go import-path under which to generate results")
	fs.StringSliceVar(&args.PluralExceptions, "plural-exceptions", args.PluralExceptions,
		"list of comma separated plural exception definitions in Type:PluralizedType format")
	fs.StringVar(&args.GoHeaderFile, "go-header-file", "",
		"the path to a file containing boilerplate header text; the string \"YEAR\" will be replaced with the current 4-digit year")
	apidefinitions.AddFlags(&args.LintArgs, fs)
}

// Validate checks the given arguments.
//
// 校验失败直接返回错误而不是 panic，便于 main 统一打印可操作的提示。
func (args *Args) Validate() error {
	if len(args.OutputDir) == 0 {
		return fmt.Errorf("--output-dir must be specified")
	}
	if len(args.OutputPkg) == 0 {
		return fmt.Errorf("--output-pkg must be specified")
	}
	if err := apidefinitions.ValidateFlags(args.LintRules); err != nil {
		return err
	}
	return nil
}
