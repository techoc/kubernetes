/*
Copyright 2015 The Kubernetes Authors.

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

// Package args 定义 client-gen 的命令行参数、默认值以及参数校验。
//
// 这里的参数最终会喂给 client-gen/generators：Groups 决定为哪些 group/version
// 生成 typed client，OutputDir/OutputPkg 决定生成文件落在哪里，而
// ClientsetOnly/FakeClient 等开关决定生成哪些产物。
package args

import (
	"fmt"

	"github.com/spf13/pflag"

	"k8s.io/code-generator/cmd/client-gen/types"
	"k8s.io/code-generator/pkg/apidefinitions"
)

// Args 汇集 client-gen 运行所需的全部输入。
// 大部分字段直接由命令行 flag 填充，New 负责给出默认值。
type Args struct {
	// 生成结果的输出目录。
	OutputDir string

	// 生成结果的 Go import-path，用于生成代码里的包导入与包名推导。
	OutputPkg string

	// Go 文件使用的 license boilerplate 模板路径。
	GoHeaderFile string

	// 待生成的有序 group/version 列表。每个 version 对应的源码包路径记录在
	// 各自的 PackageVersion.Package 中；顺序影响生成代码的排列顺序，需保持稳定。
	Groups []types.GroupVersions

	// 覆盖“哪些类型进入 client”的默认判定（默认依据 types.go 中的 genclient 标记）。
	// 仅在显式指定某个 groupVersion 时生效。
	IncludedTypesOverrides map[types.GroupVersion][]string

	// ClientsetName 是生成的 clientset 包名，来自命令行参数。
	ClientsetName string
	// ClientsetAPIPath 是生成 client 使用的默认 API HTTP 路径前缀。
	ClientsetAPIPath string
	// ClientsetOnly 为真时只生成 clientset 外壳，不生成各 group/version 的 typed client。
	// 该字段来自命令行参数。
	ClientsetOnly bool
	// FakeClient 决定 client-gen 是否生成供测试使用的 fake clientset。
	FakeClient bool
	// PluralExceptions 是复数化例外表，用于避免自动复数化得到错误结果。
	// 例如 'Endpoints:Endpoints'，否则复数化器会生成 'Endpointes'。
	PluralExceptions []string

	// ApplyConfigurationPackage 是 applyconfiguration-gen 生成的 apply builder 包。
	// 非空时为每个类型生成 Apply 函数并引用这些 builder；为空则不生成 Apply 函数。
	ApplyConfigurationPackage string

	// PrefersProtobuf 决定生成的 clientset 是否对 API 请求使用 protobuf。
	PrefersProtobuf bool

	// 内嵌的 lint 规则参数，用于校验代码生成产物的 API 规范。
	apidefinitions.LintArgs
}

// New 返回带默认值的 Args。
// 默认生成 fake client（便于测试），且不生成 Apply 函数（需显式传入包路径）。
func New() *Args {
	return &Args{
		ClientsetName:             "internalclientset",
		ClientsetAPIPath:          "/apis",
		ClientsetOnly:             false,
		FakeClient:                true,
		ApplyConfigurationPackage: "",
	}
}

// AddFlags 把 Args 的各个字段绑定到给定的 FlagSet。
// inputBase 是查找 API group 的基础路径，会交给 --input-base 使用。
func (args *Args) AddFlags(fs *pflag.FlagSet, inputBase string) {
	// gvsBuilder 负责把 --input 解析出的 group/version 逐步累积到 args.Groups。
	gvsBuilder := NewGroupVersionsBuilder(&args.Groups)
	fs.StringVar(&args.OutputDir, "output-dir", "",
		"the base directory under which to generate results")
	fs.StringVar(&args.OutputPkg, "output-pkg", args.OutputPkg,
		"the Go import-path of the generated results")
	fs.StringVar(&args.GoHeaderFile, "go-header-file", "",
		"the path to a file containing boilerplate header text; the string \"YEAR\" will be replaced with the current 4-digit year")
	// --input 与 --input-base 共享同一个 builder：前者给出 group/version 列表，
	// 后者补全其源码包路径。两者都注册为 pflag.Value，解析后直接改写 args.Groups。
	fs.Var(NewGVPackagesValue(gvsBuilder, nil), "input",
		`group/versions that client-gen will generate clients for. At most one version per group is allowed. Specified in the format "group1/version1,group2/version2...".`)
	fs.Var(NewGVTypesValue(&args.IncludedTypesOverrides, []string{}), "included-types-overrides",
		"list of group/version/type for which client should be generated. By default, client is generated for all types which have genclient in types.go. This overrides that. For each groupVersion in this list, only the types mentioned here will be included. The default check of genclient will be used for other group versions.")
	fs.Var(NewInputBasePathValue(gvsBuilder, inputBase), "input-base",
		"base path to look for the api group.")
	// -n 是 --clientset-name 的短选项；默认 "internalclientset" 对应 internal 版本的 clientset。
	fs.StringVarP(&args.ClientsetName, "clientset-name", "n", args.ClientsetName,
		"the name of the generated clientset package.")
	fs.StringVarP(&args.ClientsetAPIPath, "clientset-api-path", "", args.ClientsetAPIPath,
		"the value of default API HTTP path, starting with / and without trailing /.")
	fs.BoolVar(&args.ClientsetOnly, "clientset-only", args.ClientsetOnly,
		"when set, client-gen only generates the clientset shell, without generating the individual typed clients")
	fs.BoolVar(&args.FakeClient, "fake-clientset", args.FakeClient,
		"when set, client-gen will generate the fake clientset that can be used in tests")
	fs.StringSliceVar(&args.PluralExceptions, "plural-exceptions", args.PluralExceptions,
		"list of comma separated plural exception definitions in Type:PluralizedType form")
	fs.StringVar(&args.ApplyConfigurationPackage, "apply-configuration-package", args.ApplyConfigurationPackage,
		"optional package of apply configurations, generated by applyconfiguration-gen, that are required to generate Apply functions for each type in the clientset. By default Apply functions are not generated.")
	fs.BoolVar(&args.PrefersProtobuf, "prefers-protobuf", args.PrefersProtobuf,
		"when set, client-gen will generate a clientset that uses protobuf for API requests")
	apidefinitions.AddFlags(&args.LintArgs, fs)

	// 兼容旧 flag：历史上叫 clientset-path，现更名为 output-pkg。
	fs.SetNormalizeFunc(mapFlagName("clientset-path", "output-pkg", fs.GetNormalizeFunc()))
}

// Validate 检查必填参数是否齐全，并校验 lint 规则是否合法。
func (args *Args) Validate() error {
	if len(args.OutputDir) == 0 {
		return fmt.Errorf("--output-dir must be specified")
	}
	if len(args.OutputPkg) == 0 {
		return fmt.Errorf("--output-pkg must be specified")
	}
	if len(args.ClientsetName) == 0 {
		return fmt.Errorf("--clientset-name must be specified")
	}
	if len(args.ClientsetAPIPath) == 0 {
		return fmt.Errorf("--clientset-api-path cannot be empty")
	}
	if err := apidefinitions.ValidateFlags(args.LintRules); err != nil {
		return err
	}

	return nil
}

// GroupVersionPackages 返回 GroupVersion 到其 types.go 所在包路径的映射。
// 生成器用它定位每个 group/version 的类型定义源包。
func (args *Args) GroupVersionPackages() map[types.GroupVersion]string {
	res := map[types.GroupVersion]string{}
	for _, pkg := range args.Groups {
		for _, v := range pkg.Versions {
			res[types.GroupVersion{Group: pkg.Group, Version: v.Version}] = v.Package
		}
	}
	return res
}

// mapFlagName 在原有归一化函数之前插入一层重命名：把 from 映射为 to，
// 从而让旧 flag 名在 pflag 解析前被改写为新名字。
func mapFlagName(from, to string, old func(fs *pflag.FlagSet, name string) pflag.NormalizedName) func(fs *pflag.FlagSet, name string) pflag.NormalizedName {
	return func(fs *pflag.FlagSet, name string) pflag.NormalizedName {
		if name == from {
			name = to
		}
		return old(fs, name)
	}
}
