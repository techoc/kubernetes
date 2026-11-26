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

// Package args 定义 informer-gen 的命令行参数、默认值与校验逻辑。
//
// informer-gen 需要同时知道 internal/versioned clientset 与 listers 包的
// import path，才能生成 shared informer factory 及其按 group/version 的访问器。
package args

import (
	"fmt"

	"github.com/spf13/pflag"

	"k8s.io/code-generator/pkg/apidefinitions"
)

// Args 是传给 gengo 框架的生成器专用参数。
type Args struct {
	OutputDir                 string // 必须是目录路径
	OutputPkg                 string // 必须是 Go import-path
	GoHeaderFile              string
	VersionedClientSetPackage string // 必须是 Go import-path
	InternalClientSetPackage  string // 必须是 Go import-path
	ListersPackage            string // 必须是 Go import-path
	SingleDirectory           bool

	// PluralExceptions 定义复数化例外，格式为 Type:PluralType。
	// 默认列表为 "Endpoints:Endpoints"。
	PluralExceptions []string

	// 内嵌的 lint 参数，用于校验生成代码的 API 规范。
	apidefinitions.LintArgs
}

// 默认不启用单目录布局（即保留 internalversion/externalversions 子目录）。
func New() *Args {
	return &Args{
		SingleDirectory: false,
	}
}

func (args *Args) AddFlags(fs *pflag.FlagSet) {
	fs.StringVar(&args.OutputDir, "output-dir", "",
		"the base directory under which to generate results")
	fs.StringVar(&args.OutputPkg, "output-pkg", args.OutputPkg,
		"the Go import-path of the generated results")
	fs.StringVar(&args.GoHeaderFile, "go-header-file", "",
		"the path to a file containing boilerplate header text; the string \"YEAR\" will be replaced with the current 4-digit year")
	// internal/versioned 两个 clientset 包分别对应 inner 与外部版本的 informer。
	fs.StringVar(&args.InternalClientSetPackage, "internal-clientset-package", args.InternalClientSetPackage,
		"the Go import-path of the internal clientset to use")
	fs.StringVar(&args.VersionedClientSetPackage, "versioned-clientset-package", args.VersionedClientSetPackage,
		"the Go import-path of the versioned clientset to use")
	fs.StringVar(&args.ListersPackage, "listers-package", args.ListersPackage,
		"the Go import-path of the listers to use")
	fs.BoolVar(&args.SingleDirectory, "single-directory", args.SingleDirectory,
		"if true, omit the intermediate \"internalversion\" and \"externalversions\" subdirectories")
	fs.StringSliceVar(&args.PluralExceptions, "plural-exceptions", args.PluralExceptions,
		"list of comma separated plural exception definitions in Type:PluralizedType format")
	apidefinitions.AddFlags(&args.LintArgs, fs)
}

func (args *Args) Validate() error {
	if len(args.OutputDir) == 0 {
		return fmt.Errorf("--output-dir must be specified")
	}
	if len(args.OutputPkg) == 0 {
		return fmt.Errorf("--output-pkg must be specified")
	}
	if len(args.VersionedClientSetPackage) == 0 {
		return fmt.Errorf("--versioned-clientset-package must be specified")
	}
	if len(args.ListersPackage) == 0 {
		return fmt.Errorf("--listers-package must be specified")
	}
	if err := apidefinitions.ValidateFlags(args.LintRules); err != nil {
		return err
	}
	return nil
}
