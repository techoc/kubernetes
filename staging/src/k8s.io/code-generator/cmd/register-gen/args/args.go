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

// Package args 定义 register-gen 的命令行参数。
//
// register-gen 为 API 包生成 scheme 注册代码（register.go），因此参数很少，
// 主要只需输出文件名与 boilerplate 模板。
package args

import (
	"fmt"

	"github.com/spf13/pflag"
	"k8s.io/code-generator/pkg/apidefinitions"
)

// Args 汇集 register-gen 的输入。
type Args struct {
	// 生成的文件名；默认 generated.register.go。
	OutputFile   string
	GoHeaderFile string

	// 内嵌 lint 参数，用于校验生成代码。
	apidefinitions.LintArgs
}

func New() *Args {
	return &Args{}
}

func (args *Args) AddFlags(fs *pflag.FlagSet) {
	fs.StringVar(&args.OutputFile, "output-file", "generated.register.go",
		"the name of the file to be generated")
	fs.StringVar(&args.GoHeaderFile, "go-header-file", "",
		"the path to a file containing boilerplate header text; the string \"YEAR\" will be replaced with the current 4-digit year")
	apidefinitions.AddFlags(&args.LintArgs, fs)
}

func (args *Args) Validate() error {
	if len(args.OutputFile) == 0 {
		return fmt.Errorf("output file base name cannot be empty")
	}
	if err := apidefinitions.ValidateFlags(args.LintRules); err != nil {
		return err
	}

	return nil
}
