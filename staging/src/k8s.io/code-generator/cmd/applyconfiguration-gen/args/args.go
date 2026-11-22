/*
Copyright 2021 The Kubernetes Authors.

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

// Package args 定义 applyconfiguration-gen 的命令行参数及其校验规则。
//
// applyconfiguration-gen 为 API 类型生成 Server-Side Apply 使用的
// ApplyConfiguration 结构体（即带 WithXxx 链式构造器的 *XxxApplyConfiguration），
// 这些类型最终由 client-go 的 applyconfigurations 包对外提供。
package args

import (
	"fmt"

	"github.com/spf13/pflag"
	"k8s.io/code-generator/pkg/apidefinitions"
	"k8s.io/gengo/v2/types"
)

// Args 封装 applyconfiguration-gen 的全部参数。
type Args struct {
	OutputDir string // 必须是目录路径
	OutputPkg string // 必须是 Go import-path

	GoHeaderFile string

	// ExternalApplyConfigurations 给出输入 go 结构体所引用类型的外部生成
	// apply configuration 的位置。位置以逗号分隔的
	// <package>.<typeName>:<applyconfiguration-package> 条目列表提供。
	//
	// 例如若某类型引用了 appsv1.Deployment，则需要提供其 apply configuration 的位置：
	//   k8s.io/api/apps/v1.Deployment:k8s.io/client-go/applyconfigurations/apps/v1
	//
	// meta/v1 类型（TypeMeta 与 ObjectMeta）总是被包含，无需传入。
	ExternalApplyConfigurations map[types.Name]string

	OpenAPISchemaFilePath string

	apidefinitions.LintArgs
}

// New 返回生成器的默认参数。
func New() *Args {
	return &Args{
		ExternalApplyConfigurations: map[types.Name]string{
			// 始终包含 client-go 中已生成的 applyconfigurations：
			// 对绝大多数场景它们已经足够，避免每个调用方重复生成同一批 meta/v1 类型。
			{Package: "k8s.io/apimachinery/pkg/apis/meta/v1", Name: "Condition"}:                "k8s.io/client-go/applyconfigurations/meta/v1",
			{Package: "k8s.io/apimachinery/pkg/apis/meta/v1", Name: "DeleteOptions"}:            "k8s.io/client-go/applyconfigurations/meta/v1",
			{Package: "k8s.io/apimachinery/pkg/apis/meta/v1", Name: "LabelSelector"}:            "k8s.io/client-go/applyconfigurations/meta/v1",
			{Package: "k8s.io/apimachinery/pkg/apis/meta/v1", Name: "LabelSelectorRequirement"}: "k8s.io/client-go/applyconfigurations/meta/v1",
			{Package: "k8s.io/apimachinery/pkg/apis/meta/v1", Name: "ManagedFieldsEntry"}:       "k8s.io/client-go/applyconfigurations/meta/v1",
			{Package: "k8s.io/apimachinery/pkg/apis/meta/v1", Name: "ObjectMeta"}:               "k8s.io/client-go/applyconfigurations/meta/v1",
			{Package: "k8s.io/apimachinery/pkg/apis/meta/v1", Name: "OwnerReference"}:           "k8s.io/client-go/applyconfigurations/meta/v1",
			{Package: "k8s.io/apimachinery/pkg/apis/meta/v1", Name: "TypeMeta"}:                 "k8s.io/client-go/applyconfigurations/meta/v1",
		},
	}
}

// AddFlags 把参数注册到 pflag 集合上。
// inputBase 为历史遗留参数，当前未使用。
func (args *Args) AddFlags(fs *pflag.FlagSet, inputBase string) {
	fs.StringVar(&args.OutputDir, "output-dir", "",
		"the base directory under which to generate results")
	fs.StringVar(&args.OutputPkg, "output-pkg", args.OutputPkg,
		"the Go import-path of the generated results")
	fs.StringVar(&args.GoHeaderFile, "go-header-file", "",
		"the path to a file containing boilerplate header text; the string \"YEAR\" will be replaced with the current 4-digit year")
	fs.Var(NewExternalApplyConfigurationValue(&args.ExternalApplyConfigurations, nil), "external-applyconfigurations",
		"list of comma separated external apply configurations locations in <type-package>.<type-name>:<applyconfiguration-package> form."+
			"For example: k8s.io/api/apps/v1.Deployment:k8s.io/client-go/applyconfigurations/apps/v1")
	fs.StringVar(&args.OpenAPISchemaFilePath, "openapi-schema", "",
		"path to the openapi schema containing all the types that apply configurations will be generated for")
	apidefinitions.AddFlags(&args.LintArgs, fs)
}

// Validate 校验参数。
// 输出目录与输出 import-path 都是必填项：生成器无法从其他参数推断目标位置，
// 缺失会导致生成文件写到不可预期的路径。
func (args *Args) Validate() error {
	if len(args.OutputDir) == 0 {
		return fmt.Errorf("--output-dir must be specified")
	}
	if len(args.OutputPkg) == 0 {
		return fmt.Errorf("--output-pkg must be specified")
	}
	// LintRules 决定生成代码中 APIDefinition 的检查等级，非法取值应尽早失败。
	if err := apidefinitions.ValidateFlags(args.LintRules); err != nil {
		return err
	}
	return nil
}
