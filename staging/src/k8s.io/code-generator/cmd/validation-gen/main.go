/*
Copyright 2024 The Kubernetes Authors.

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

// validation-gen 自动生成 Validation 函数。
//
// 该工具把类型与字段上的声明式标记（+k8s:required、+k8s:minLength 等）翻译成
// Go 校验函数，并生成注册代码。生成器只依赖标记，不解析手写校验逻辑，
// 因此同一份 API 定义可以在不同版本、不同仓库中重复生成出等价代码。
package main

import (
	"bytes"
	"cmp"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"slices"

	"github.com/spf13/pflag"

	"k8s.io/code-generator/cmd/validation-gen/validators"
	"k8s.io/code-generator/pkg/apidefinitions"
	"k8s.io/gengo/v2"
	"k8s.io/gengo/v2/generator"
	"k8s.io/gengo/v2/namer"
	"k8s.io/gengo/v2/types"
	"k8s.io/klog/v2"
)

func main() {
	klog.InitFlags(nil)
	args := &Args{}

	args.AddFlags(pflag.CommandLine)
	if err := flag.Set("logtostderr", "true"); err != nil {
		klog.Fatalf("Error: %v", err)
	}
	pflag.CommandLine.AddGoFlagSet(flag.CommandLine)
	pflag.Parse()

	if err := args.Validate(); err != nil {
		klog.Fatalf("Error: %v", err)
	}

	// --docs 只打印支持的标记文档后退出，不读取任何输入包。
	if args.PrintDocs {
		printDocs()
		os.Exit(0)
	}

	myTargets := func(context *generator.Context) []generator.Target {
		return GetTargets(context, args)
	}

	// 执行生成；输入包取自位置参数。
	if err := gengo.Execute(
		NameSystems(),
		DefaultNameSystem(),
		myTargets,
		gengo.StdBuildTag,
		pflag.Args(),
	); err != nil {
		klog.Fatalf("Error: %v", err)
	}
	klog.V(2).Info("Completed successfully.")
}

// Args 是 validation-gen 的命令行参数。
type Args struct {
	OutputFile   string
	ReadOnlyPkgs []string // 始终作为校验函数的兜底来源考虑。
	GoHeaderFile string
	PrintDocs    bool
	// TestOutputRoot 非空时启用覆盖率测试夹具生成：为每个声明了规则的 Kind
	// 在 <TestOutputRoot>/<short-group>/<lowercase(kind)>/ 下生成每个版本一个
	// <TestOutputFilePrefix><version>_test.go，外加共享的
	// <TestOutputFilePrefix>main_test.go。
	TestOutputRoot string

	// TestOutputFilePrefix 会加到每个测试夹具文件名前面。默认为空；
	// 通过 linguist-generated gitattributes 模式（如 "zz_generated."）标记
	// 生成文件的调用方应把它设为该前缀。
	TestOutputFilePrefix string

	// TestAllowlist 非空时是 YAML 文件的路径，内容为按规则级别的过滤项，
	// 用于从夹具生成中排除某些规则。每项含 apiVersion、kind、path、
	// errorType、origin 字段（用 "*" 通配 kind/path/errorType/origin），
	// 以及必需的 reason。
	TestAllowlist string

	apidefinitions.LintArgs
}

// AddFlags 把生成器参数注册到 flag 集合上。
func (args *Args) AddFlags(fs *pflag.FlagSet) {
	fs.StringVar(&args.OutputFile, "output-file", "generated.validations.go",
		"the name of the file to be generated")
	fs.StringSliceVar(&args.ReadOnlyPkgs, "readonly-pkg", args.ReadOnlyPkgs,
		"the import path of a package whose validation can be used by generated code, but is not being generated for")
	fs.StringVar(&args.GoHeaderFile, "go-header-file", "",
		"the path to a file containing boilerplate header text; the string \"YEAR\" will be replaced with the current 4-digit year")
	fs.BoolVar(&args.PrintDocs, "docs", false,
		"print documentation for supported declarative validations, and then exit")
	fs.StringVar(&args.TestOutputRoot, "test-output-root", "",
		"if non-empty, also emit declarative-validation coverage test fixtures under this path, organized as <root>/<group>/<lowercase(kind)>/<file-prefix>{<version>,main}_test.go")
	fs.StringVar(&args.TestOutputFilePrefix, "test-output-file-prefix", "",
		"prefix prepended to every emitted test fixture filename; useful for marking files via a linguist-generated gitattributes pattern (e.g. \"zz_generated.\")")
	fs.StringVar(&args.TestAllowlist, "test-allowlist", "",
		"path to a YAML config file of rule-level filters to exclude from coverage fixture generation; only meaningful with --test-output-root")
	apidefinitions.AddFlags(&args.LintArgs, fs)
}

// Validate 校验参数。
// 测试夹具相关参数只在 --test-output-root 生效时才有意义，
// 单独指定多半是命令行拼写错误，因此直接报错而不是静默忽略。
func (args *Args) Validate() error {
	if len(args.OutputFile) == 0 {
		return fmt.Errorf("--output-file must be specified")
	}
	if args.TestAllowlist != "" && args.TestOutputRoot == "" {
		return fmt.Errorf("--test-allowlist is only meaningful with --test-output-root")
	}
	if args.TestOutputFilePrefix != "" && args.TestOutputRoot == "" {
		return fmt.Errorf("--test-output-file-prefix is only meaningful with --test-output-root")
	}

	if err := apidefinitions.ValidateFlags(args.LintRules); err != nil {
		return err
	}
	return nil
}

// printDocs 以 JSON 形式输出所有已注册声明式校验标记的文档。
func printDocs() {
	// 需要一个假 context 来初始化校验器插件。
	c := &generator.Context{
		Namers:    namer.NameSystems{},
		Universe:  types.Universe{},
		FileTypes: map[string]generator.FileType{},
	}

	// 初始化所有已注册的校验器。
	validator := validators.InitGlobalValidator(c, nil)

	docs := validator.Docs()
	for i := range docs {
		d := &docs[i]
		slices.Sort(d.Scopes)
		if d.Usage == "" {
			// 未提供用法字符串时尝试自动生成。
			usage := d.Tag
			if len(d.Args) > 0 {
				usage += "("
				for i := range d.Args {
					if i > 0 {
						usage += ", "
					}
					usage += d.Args[i].Description
				}
				usage += ")"
			}
			if len(d.Payloads) > 0 {
				usage += "="
				if len(d.Payloads) == 1 {
					usage += d.Payloads[0].Description
				} else {
					usage += "<payload>"
				}
			}
			d.Usage = usage
		}
	}
	// 按标记名排序，保证文档输出稳定、可 diff。
	slices.SortFunc(docs, func(a, b validators.TagDoc) int {
		return cmp.Compare(a.Tag, b.Tag)
	})

	var buf bytes.Buffer
	encoder := json.NewEncoder(&buf)
	encoder.SetEscapeHTML(false)
	encoder.SetIndent("", "    ")
	if err := encoder.Encode(docs); err != nil {
		klog.Fatalf("failed to marshal docs: %v", err)
	}

	fmt.Println(buf.String())
}
