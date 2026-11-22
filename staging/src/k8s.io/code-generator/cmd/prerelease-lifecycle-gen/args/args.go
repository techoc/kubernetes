/*
Copyright 2020 The Kubernetes Authors.

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

// Package args 定义 prerelease-lifecycle-gen 的命令行参数。
package args

import (
	"fmt"

	"github.com/spf13/pflag"

	"k8s.io/code-generator/pkg/apidefinitions"
)

// Args 汇总生成器参数。嵌入 LintArgs 以与其他 API 定义生成器共享
// lint 规则开关。
type Args struct {
	OutputFile   string
	GoHeaderFile string

	apidefinitions.LintArgs
}

// New returns default arguments for the generator.
// 默认输出文件名固定，保证重复运行覆盖同一文件而非堆积。
func New() *Args {
	return &Args{}
}

// AddFlags add the generator flags to the flag set.
func (args *Args) AddFlags(fs *pflag.FlagSet) {
	fs.StringVar(&args.OutputFile, "output-file", "generated.prerelease_lifecycle.go",
		"the name of the file to be generated")
	fs.StringVar(&args.GoHeaderFile, "go-header-file", "",
		"the path to a file containing boilerplate header text; the string \"YEAR\" will be replaced with the current 4-digit year")
	apidefinitions.AddFlags(&args.LintArgs, fs)
}

// Validate checks the given arguments.
// output-file 为空会导致无法确定写文件位置，因此必须显式或使用默认值。
func (args *Args) Validate() error {
	if len(args.OutputFile) == 0 {
		return fmt.Errorf("--output-file must be specified")
	}
	if err := apidefinitions.ValidateFlags(args.LintRules); err != nil {
		return err
	}
	return nil
}
