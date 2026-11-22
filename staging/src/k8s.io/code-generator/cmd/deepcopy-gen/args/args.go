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

// Package args defines the command-line arguments shared by the deepcopy-gen
// entry point and its tests.
//
// 与其它生成器一致，参数校验放在 Validate 中、生成开始前完成，避免生成到一半才失败。
package args

import (
	"fmt"

	"github.com/spf13/pflag"

	"k8s.io/code-generator/pkg/apidefinitions"
)

// Args 汇总 deepcopy-gen 的全部参数。
// LintArgs 内嵌了 lint 相关开关，使其与生成参数共用同一 flag 集合与校验流程。
type Args struct {
	OutputFile   string // 生成文件的路径；默认写在当前包下
	GoHeaderFile string // boilerplate 头文件路径；空表示不加头

	apidefinitions.LintArgs
}

// New returns default arguments for the generator.
func New() *Args {
	return &Args{}
}

// AddFlags add the generator flags to the flag set.
// 默认输出文件名与 kube 仓库的约定一致（generated.deepcopy.go），
// 这样 make update 无需显式传参。
func (args *Args) AddFlags(fs *pflag.FlagSet) {
	fs.StringVar(&args.OutputFile, "output-file", "generated.deepcopy.go",
		"the name of the file to be generated")
	fs.StringVar(&args.GoHeaderFile, "go-header-file", "",
		"the path to a file containing boilerplate header text; the string \"YEAR\" will be replaced with the current 4-digit year")
	apidefinitions.AddFlags(&args.LintArgs, fs)
}

// Validate checks the given arguments.
// 输出文件是唯一的硬性要求；lint 参数交由共享包统一校验。
func (args *Args) Validate() error {
	if len(args.OutputFile) == 0 {
		return fmt.Errorf("--output-file must be specified")
	}
	if err := apidefinitions.ValidateFlags(args.LintRules); err != nil {
		return err
	}
	return nil
}
