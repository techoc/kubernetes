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

// go-to-protobuf 从 Go 结构体生成 Protobuf IDL，并保留已有的 IDL 标签。
//
// 中文概述：go-to-protobuf 从 Go 结构体生成 Protobuf IDL，再调用 protoc 生成
// 序列化代码，并把生成的 .pb.go 中的类型声明改写为仅保留序列化器。
package protobuf

import (
	"bytes"
	"fmt"
	"log"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"

	flag "github.com/spf13/pflag"

	"k8s.io/gengo/v2"
	"k8s.io/gengo/v2/generator"
	"k8s.io/gengo/v2/namer"
	"k8s.io/gengo/v2/parser"
	"k8s.io/gengo/v2/types"
)

// Generator 汇总 go-to-protobuf 的配置项。
type Generator struct {
	GoHeaderFile         string
	APIMachineryPackages string
	Packages             string
	OutputDir            string
	ProtoImport          []string
	Conditional          string
	Clean                bool
	OnlyIDL              bool
	KeepGogoproto        bool
	DropGogoGo           bool
	SkipGeneratedRewrite bool
	DropEmbeddedFields   string
}

// New 返回带默认值的配置：默认输出到当前目录，并预置一组 apimachinery 输入包。
// 以 '+' 开头的包只为其显式带 IDL 标签的类型生成 IDL。
func New() *Generator {
	defaultSourceTree := "."
	return &Generator{
		OutputDir: defaultSourceTree,
		APIMachineryPackages: strings.Join([]string{
			`+k8s.io/apimachinery/pkg/util/intstr`,
			`+k8s.io/apimachinery/pkg/api/resource`,
			`+k8s.io/apimachinery/pkg/runtime/schema`,
			`+k8s.io/apimachinery/pkg/runtime`,
			`k8s.io/apimachinery/pkg/apis/meta/v1`,
			`k8s.io/apimachinery/pkg/apis/meta/v1beta1`,
			`k8s.io/apimachinery/pkg/apis/testapigroup/v1`,
		}, ","),
		Packages:           "",
		DropEmbeddedFields: "k8s.io/apimachinery/pkg/apis/meta/v1.TypeMeta",
		DropGogoGo:         true,
	}
}

// BindFlags 把配置项注册到 flag set。
func (g *Generator) BindFlags(flag *flag.FlagSet) {
	flag.StringVarP(&g.GoHeaderFile, "go-header-file", "h", "", "File containing boilerplate header text. The string YEAR will be replaced with the current 4-digit year.")
	flag.StringVarP(&g.Packages, "packages", "p", g.Packages, "comma-separated list of directories to get input types from. Directories prefixed with '-' are not generated, directories prefixed with '+' only create types with explicit IDL instructions.")
	flag.StringVar(&g.APIMachineryPackages, "apimachinery-packages", g.APIMachineryPackages, "comma-separated list of directories to get apimachinery input types from which are needed by any API. Directories prefixed with '-' are not generated, directories prefixed with '+' only create types with explicit IDL instructions.")
	flag.StringVar(&g.OutputDir, "output-dir", g.OutputDir, "The base directory under which to generate results.")
	flag.StringSliceVar(&g.ProtoImport, "proto-import", g.ProtoImport, "A search path for imported protobufs (may be repeated).")
	flag.StringVar(&g.Conditional, "conditional", g.Conditional, "An optional Golang build tag condition to add to the generated Go code")
	flag.BoolVar(&g.Clean, "clean", g.Clean, "If true, remove all generated files for the specified Packages.")
	flag.BoolVar(&g.OnlyIDL, "only-idl", g.OnlyIDL, "If true, only generate the IDL for each package.")
	flag.BoolVar(&g.KeepGogoproto, "keep-gogoproto", g.KeepGogoproto, "If true, the generated IDL will contain gogoprotobuf extensions which are normally removed")
	flag.BoolVar(&g.SkipGeneratedRewrite, "skip-generated-rewrite", g.SkipGeneratedRewrite, "If true, skip fixing up the generated.pb.go file (debugging only).")
	flag.BoolVar(&g.DropGogoGo, "drop-gogo-go", g.DropGogoGo, "Drop all references to gogo packages in generated code")
	flag.StringVar(&g.DropEmbeddedFields, "drop-embedded-fields", g.DropEmbeddedFields, "Comma-delimited list of embedded Go types to omit from generated protobufs")
}

// Run 是命令行工具的入口：解析输入包、生成 IDL、调用 protoc 并改写产物。
// 注释中的 "roughly models" 表示这里复刻了 gengo/v2 的执行流程，而非直接复用。
// 这里大致复刻 gengo/v2.Execute 的流程。
func Run(g *Generator) {
	// 大致对应 gengo/v2.newBuilder。

	// 用 "proto" build tag 解析，确保只看到参与 protobuf 生成的代码分支。
	p := parser.NewWithOptions(parser.Options{BuildTags: []string{"proto"}})

	var allInputs []string
	if len(g.APIMachineryPackages) != 0 {
		allInputs = append(allInputs, strings.Split(g.APIMachineryPackages, ",")...)
	}
	if len(g.Packages) != 0 {
		allInputs = append(allInputs, strings.Split(g.Packages, ",")...)
	}
	if len(allInputs) == 0 {
		log.Fatalf("Both apimachinery-packages and packages are empty. At least one package must be specified.")
	}

	// 改写生成文件与丢弃 gogo 引用二者互斥：若不改写，就无从删除 gogo 引用。
	if g.DropGogoGo && g.SkipGeneratedRewrite {
		log.Fatalf("--drop-gogo-go=true and --skip-generated-rewrite=true are mutually exclusive")
	}

	// 构建待加载包列表并记录每个输入的特殊修饰符。注意：不支持 pkg/... 语法。
	type modifier struct {
		allTypes bool
		output   bool
		name     string
	}
	inputModifiers := map[string]modifier{}
	packages := make([]string, 0, len(allInputs))

	for _, d := range allInputs {
		modifier := modifier{allTypes: true, output: true}

		// '+' 表示只生成显式声明 IDL 的类型；'-' 表示仅作为依赖、不输出。
		switch {
		case strings.HasPrefix(d, "+"):
			d = d[1:]
			modifier.allTypes = false
		case strings.HasPrefix(d, "-"):
			d = d[1:]
			modifier.output = false
		}
		// 允许用 "path=proto.name" 覆盖生成的 protobuf 包名。
		name := protoSafePackage(d)
		parts := strings.SplitN(d, "=", 2)
		if len(parts) > 1 {
			d = parts[0]
			name = parts[1]
		}
		modifier.name = name

		packages = append(packages, d)
		inputModifiers[d] = modifier
	}

	// 所有包一次性加载，减少解析开销。
	if err := p.LoadPackages(packages...); err != nil {
		log.Fatalf("Unable to load packages: %v", err)
	}

	// 仅注册 public 命名系统，protobuf 命名由后面的 "proto" 覆盖。
	c, err := generator.NewContext(
		p,
		namer.NameSystems{
			"public": namer.NewPublicNamer(3),
		},
		"public",
	)
	if err != nil {
		log.Fatalf("Failed making a context: %v", err)
	}

	c.FileTypes["protoidl"] = NewProtoFile()

	// 大致对应 gengo/v2.Execute 调用工具提供的 Targets() 回调。

	boilerplate, err := gengo.GoBoilerplate(g.GoHeaderFile, "", "")
	if err != nil {
		log.Fatalf("Failed loading boilerplate (consider using the go-header-file flag): %v", err)
	}

	// 把 --drop-embedded-fields 解析为 types.Name 集合，供生成时跳过内嵌字段。
	omitTypes := map[types.Name]struct{}{}
	for _, t := range strings.Split(g.DropEmbeddedFields, ",") {
		name := types.Name{}
		if i := strings.LastIndex(t, "."); i != -1 {
			name.Package, name.Name = t[:i], t[i+1:]
		} else {
			name.Name = t
		}
		if len(name.Name) == 0 {
			log.Fatalf("--drop-embedded-types requires names in the form of [GOPACKAGE.]TYPENAME: %v", t)
		}
		omitTypes[name] = struct{}{}
	}

	protobufNames := NewProtobufNamer()
	outputPackages := []generator.Target{}
	nonOutputPackages := map[string]struct{}{}

	for _, input := range c.Inputs {
		mod, found := inputModifiers[input]
		if !found {
			log.Fatalf("BUG: can't find input modifiers for %q", input)
		}
		pkg := c.Universe[input]
		protopkg := newProtobufPackage(pkg.Path, pkg.Dir, mod.name, mod.allTypes, omitTypes)
		// boilerplate 与 protobuf 头部注释合并，保证每个生成文件都带许可声明。
		header := append([]byte{}, boilerplate...)
		header = append(header, protopkg.HeaderComment...)
		protopkg.HeaderComment = header
		protobufNames.Add(protopkg)
		if mod.output {
			outputPackages = append(outputPackages, protopkg)
		} else {
			nonOutputPackages[mod.name] = struct{}{}
		}
	}
	c.Namers["proto"] = protobufNames

	for _, p := range outputPackages {
		if err := p.(*protobufPackage).Clean(); err != nil {
			log.Fatalf("Unable to clean package %s: %v", p.Name(), err)
		}
	}

	if g.Clean {
		return
	}

	// 按导入关系排序包，被导入的包排在前面：按包依赖做拓扑排序，保证引用它的 IDL 已存在。
	deps := deps(c, protobufNames.packages)
	order, err := importOrder(deps)
	if err != nil {
		log.Fatalf("Failed to order packages by imports: %v", err)
	}
	topologicalPos := map[string]int{}
	for i, p := range order {
		topologicalPos[p] = i
	}
	sort.Sort(positionOrder{topologicalPos, protobufNames.packages})

	var localOutputPackages []generator.Target
	for _, p := range protobufNames.packages {
		if _, ok := nonOutputPackages[p.Name()]; ok {
			// 不输出的包不加入任何包列表。
			continue
		}
		localOutputPackages = append(localOutputPackages, p)
	}

	// 必须先确定 Common 类型归属，再执行生成，否则跨包引用会落到错误包。
	if err := protobufNames.AssignTypesToPackages(c); err != nil {
		log.Fatalf("Failed to identify Common types: %v", err)
	}

	if err := c.ExecuteTargets(localOutputPackages); err != nil {
		log.Fatalf("Failed executing local generator: %v", err)
	}

	// --only-idl 到此为止，不调用 protoc。
	if g.OnlyIDL {
		return
	}

	if _, err := exec.LookPath("protoc"); err != nil {
		log.Fatalf("Unable to find 'protoc': %v", err)
	}

	searchArgs := []string{"-I", ".", "-I", g.OutputDir}
	if len(g.ProtoImport) != 0 {
		for _, s := range g.ProtoImport {
			searchArgs = append(searchArgs, "-I", s)
		}
	}
	// 文档声称 `--gogo_out=paths=source_relative:.` 会把 .pb.go 输出到 .proto 同目录，实际并非如此：
	// 比如 -I 路径下的 example.com/foo/bar.proto 会生成
	// $output_base/example.com/foo/example.com/foo/bar.pb.go，基本不可用。
	// 因此 output-dir 应设为能覆盖所有目标包的上级目录（如 kubernetes 中的 staging/src）。
	// 也可以生成到临时目录再搬回输入目录，但那样在其他方面更脆弱。
	args := searchArgs
	args = append(args, fmt.Sprintf("--gogo_out=%s", g.OutputDir))

	// 预生成文件头（可选 build tag + boilerplate），供改写阶段插入。
	buf := &bytes.Buffer{}
	if len(g.Conditional) > 0 {
		fmt.Fprintf(buf, "// +build %s\n\n", g.Conditional)
	}
	buf.Write(boilerplate)

	for _, outputPackage := range outputPackages {
		p := outputPackage.(*protobufPackage)

		// 输出路径与导入路径分开：IDL 使用源目录路径，.pb.go 使用包内输出路径。
		path := filepath.Join(g.OutputDir, p.ImportPath())
		outputPath := filepath.Join(g.OutputDir, p.OutputPath())

		// 生成 gogoprotobuf 的 protoc 产物。
		cmd := exec.Command("protoc", append(args, path)...)
		out, err := cmd.CombinedOutput()
		if err != nil {
			log.Println(strings.Join(cmd.Args, " "))
			log.Println(string(out))
			log.Fatalf("Unable to run protoc on %s: %v", p.Name(), err)
		}

		if g.SkipGeneratedRewrite {
			continue
		}

		// 改写生成的 protobuf 文件：删除生成的类型（保留序列化器），并把 package 语句改为目标包名。
		// 原因：pkg 目录下的 Go 类型已存在，protoc 生成的定义会冲突；
		// 因此只保留序列化相关代码并改写包名。
		if err := RewriteGeneratedGogoProtobufFile(outputPath, p.ExtractGeneratedType, p.OptionalTypeName, buf.Bytes(), g.DropGogoGo); err != nil {
			log.Fatalf("Unable to rewrite generated %s: %v", outputPath, err)
		}

		outputPaths := []string{outputPath}

		// 排序导入：改写后导入必然有缺失或冗余，需要 goimports 修正。
		cmd = exec.Command("goimports", append([]string{"-w"}, outputPaths...)...)
		out, err = cmd.CombinedOutput()
		if len(out) > 0 {
			log.Print(string(out))
		}
		if err != nil {
			log.Println(strings.Join(cmd.Args, " "))
			log.Fatalf("Unable to rewrite imports for %s: %v", p.Name(), err)
		}

		// 格式化并简化生成文件。
		cmd = exec.Command("gofmt", append([]string{"-s", "-w"}, outputPaths...)...)
		out, err = cmd.CombinedOutput()
		if len(out) > 0 {
			log.Print(string(out))
		}
		if err != nil {
			log.Println(strings.Join(cmd.Args, " "))
			log.Fatalf("Unable to apply gofmt for %s: %v", p.Name(), err)
		}
	}

	if g.SkipGeneratedRewrite {
		return
	}

	if !g.KeepGogoproto {
		// 重新生成，但不带 gogoprotobuf 扩展：再跑一遍生成器并设置 OmitGogo，用不带 gogo 扩展的版本覆盖之前的结果。
		for _, outputPackage := range outputPackages {
			p := outputPackage.(*protobufPackage)
			p.OmitGogo = true
		}
		if err := c.ExecuteTargets(localOutputPackages); err != nil {
			log.Fatalf("Failed executing local generator: %v", err)
		}
	}

	// 最后把 protobuf struct tag 写回源文件，使 Go 结构体与 IDL 字段编号保持一致。
	for _, outputPackage := range outputPackages {
		p := outputPackage.(*protobufPackage)

		if len(p.StructTags) == 0 {
			continue
		}

		pattern := filepath.Join(g.OutputDir, p.Path(), "*.go")
		files, err := filepath.Glob(pattern)
		if err != nil {
			log.Fatalf("Can't glob pattern %q: %v", pattern, err)
		}

		for _, s := range files {
			// 测试文件不需要 protobuf tag。
			if strings.HasSuffix(s, "_test.go") {
				continue
			}
			if err := RewriteTypesWithProtobufStructTags(s, p.StructTags); err != nil {
				log.Fatalf("Unable to rewrite with struct tags %s: %v", s, err)
			}
		}
	}
}

// deps 返回包路径到其依赖包路径列表的映射，供拓扑排序使用。
func deps(c *generator.Context, pkgs []*protobufPackage) map[string][]string {
	ret := map[string][]string{}
	for _, p := range pkgs {
		pkg, ok := c.Universe[p.Path()]
		if !ok {
			log.Fatalf("Unrecognized package: %s", p.Path())
		}

		for _, d := range pkg.Imports {
			ret[p.Path()] = append(ret[p.Path()], d.Path)
		}
	}
	return ret
}

// importOrder 给定 pkg->[]deps 的映射，返回一个确保所有依赖都先于使用者的处理顺序。
// 实现为 Kahn 拓扑排序；若剩余节点非空说明依赖图有环。
func importOrder(deps map[string][]string) ([]string, error) {
	// 加入所有节点与边。
	var remainingNodes = map[string]struct{}{}
	var graph = map[edge]struct{}{}
	for to, froms := range deps {
		remainingNodes[to] = struct{}{}
		for _, from := range froms {
			remainingNodes[from] = struct{}{}
			graph[edge{from: from, to: to}] = struct{}{}
		}
	}

	// 找出没有任何依赖的初始节点。
	sorted := findAndRemoveNodesWithoutDependencies(remainingNodes, graph)
	for i := 0; i < len(sorted); i++ {
		node := sorted[i]
		// 每取出一个节点就删除它以它为起点的边，使更多节点成为新的依赖根。
		removeEdgesFrom(node, graph)
		sorted = append(sorted, findAndRemoveNodesWithoutDependencies(remainingNodes, graph)...)
	}
	if len(remainingNodes) > 0 {
		return nil, fmt.Errorf("cycle: remaining nodes: %#v, remaining edges: %#v", remainingNodes, graph)
	}
	// for _, n := range sorted {
	// 	 fmt.Println("topological order", n)
	// }
	return sorted, nil
}

// edge 描述图中一条 from->to 的依赖边（from 依赖 to）。
// edge 描述图中一条 from->to 的依赖边（from 依赖 to）。
type edge struct {
	from string
	to   string
}

// findAndRemoveNodesWithoutDependencies 找出图中没有任何入边指向的节点，
// 将它们从节点集合中移除并按字典序返回。
// findAndRemoveNodesWithoutDependencies 找出图中没有任何入边指向的节点，
// 将它们从节点集合中移除并按字典序返回。
func findAndRemoveNodesWithoutDependencies(nodes map[string]struct{}, graph map[edge]struct{}) []string {
	roots := []string{}
	// 遍历所有节点，寻找潜在的 "to" 节点。
	for node := range nodes {
		incoming := false
		// 遍历剩余所有的边。
		for edge := range graph {
			// 只要存在指向该节点的边，它就不是根节点。
			if edge.to == node {
				incoming = true
				break
			}
		}
		// 没有入边则从剩余集合移除并加入结果。
		if !incoming {
			delete(nodes, node)
			roots = append(roots, node)
		}
	}
	// 排序保证输出顺序稳定，使生成结果可复现。
	sort.Strings(roots)
	return roots
}

// removeEdgesFrom 删除图中所有以 node 为起点的边。
// removeEdgesFrom 删除图中所有以 node 为起点的边。
func removeEdgesFrom(node string, graph map[edge]struct{}) {
	for edge := range graph {
		if edge.from == node {
			delete(graph, edge)
		}
	}
}

// positionOrder 按外部给定的拓扑位置对 protobufPackage 切片排序。
type positionOrder struct {
	pos      map[string]int
	elements []*protobufPackage
}

// Len 实现 sort.Interface。
func (o positionOrder) Len() int {
	return len(o.elements)
}

// Less 比较两个包在拓扑序中的位置。
func (o positionOrder) Less(i, j int) bool {
	return o.pos[o.elements[i].Path()] < o.pos[o.elements[j].Path()]
}

// Swap 实现 sort.Interface。
func (o positionOrder) Swap(i, j int) {
	o.elements[i], o.elements[j] = o.elements[j], o.elements[i]
}
