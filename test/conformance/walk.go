/*
Copyright 2019 The Kubernetes Authors.

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

package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io"
	"log"
	"os"
	"regexp"
	"sort"
	"strings"
	"text/template"

	"github.com/onsi/ginkgo/v2/types"

	yaml "go.yaml.in/yaml/v2"
)

// ConformanceData 描述 conformance.yaml 文件的结构
type ConformanceData struct {
	// 指向 kube 源码仓库中该测试对应代码行的 URL。为避免暴露行号，不从 YAML 中输出。
	URL string `yaml:"-"`
	// 从测试前的 "Testname:" 注释中提取
	TestName string
	// CodeName 取自实际的 ginkgo 描述，例如 `[sig-apps] Foo should bar [Conformance]`
	CodeName string
	// 从测试前的 "Description:" 注释中提取
	Description string
	// 添加或修改该测试时的版本，例如 v1.12、v1.13
	Release string
	// File 是定义测试的文件名。我们有意不在此保存行号，以避免无意义的变更。
	File string
}

var (
	baseURL = flag.String("url", "https://github.com/kubernetes/kubernetes/tree/master/", "location of the current source")
	k8sPath = flag.String("source", "", "location of the current source on the current machine")
	confDoc = flag.Bool("docs", false, "write a conformance document")
	version = flag.String("version", "v1.9", "version of this conformance document")

	// 如果测试名称包含以下任一标签，则该测试没有资格晋升为一致性测试
	regexIneligibleTags = regexp.MustCompile(`\[(Alpha|Feature:[^\]]+|Flaky)\]`)

	// 一致性注释与该调用本身的距离应在此行数范围内。
	// 允许不止一行，以防其下方还有一两条多余注释。
	conformanceCommentsLineWindow = 5

	seenLines map[string]struct{}
)

type frame struct {
	// File 和 Line 是该帧对应位置的
	// 文件名和行号。对于非叶子帧，这是调用所在的位置。
	// 若位置未知，File 和 Line 分别为空字符串和 0。
	// 这两个字段用于定位对应的源码位置。
	File string
	Line int
}

func main() {
	flag.Parse()

	if len(flag.Args()) < 1 {
		log.Fatalln("Requires the name of the test details file as first and only argument.")
	}
	testDetailsFile := flag.Args()[0]
	f, err := os.Open(testDetailsFile)
	if err != nil {
		log.Fatalf("Failed to open file %v: %v", testDetailsFile, err)
	}
	defer f.Close()

	seenLines = map[string]struct{}{}
	dec := json.NewDecoder(f)
	testInfos := []*ConformanceData{}
	for {
		var spec *types.SpecReport
		if err := dec.Decode(&spec); err == io.EOF {
			break
		} else if err != nil {
			log.Fatal(err)
		}

		if isConformance(spec) {
			testInfo := getTestInfo(spec)
			if testInfo != nil {
				testInfos = append(testInfos, testInfo)
				if err := validateTestName(testInfo.CodeName); err != nil {
					log.Fatal(err)
				}
			}
		}
	}

	sort.Slice(testInfos, func(i, j int) bool { return testInfos[i].CodeName < testInfos[j].CodeName })
	saveAllTestInfo(testInfos)
}

func isConformance(spec *types.SpecReport) bool {
	return strings.Contains(getTestName(spec), "[Conformance]")
}

func getTestInfo(spec *types.SpecReport) *ConformanceData {
	var c *ConformanceData
	var err error
	// 这样做的关键在于无需解析每个文件
	// 或遍历每个 types.CodeLocation。
	// LeafNodeLocation 就是 file:line，它附着在我们想要的注释上。
	leafNodeLocation := spec.LeafNodeLocation
	frame := frame{
		File: leafNodeLocation.FileName,
		Line: leafNodeLocation.LineNumber,
	}
	c, err = getConformanceData(frame)
	if err != nil {
		log.Printf("Error looking for conformance data: %v", err)
	}
	if c == nil {
		log.Printf("Did not find test info for spec: %#v\n", getTestName(spec))
		return nil
	}
	c.CodeName = getTestName(spec)
	return c
}

func getTestName(spec *types.SpecReport) string {
	return strings.Join(spec.ContainerHierarchyTexts[0:], " ") + " " + spec.LeafNodeText
}

func saveAllTestInfo(dataSet []*ConformanceData) {
	if *confDoc {
		// 注意：这里假设从 kube 源码仓库的根目录运行
		templ, err := template.ParseFiles("./test/conformance/cf_header.md")
		if err != nil {
			fmt.Printf("Error reading the Header file information: %s\n\n", err)
		}
		data := struct {
			Version string
		}{
			Version: *version,
		}
		templ.Execute(os.Stdout, data)

		for _, data := range dataSet {
			fmt.Printf("## [%s](%s)\n\n", data.TestName, data.URL)
			fmt.Printf("- Added to conformance in release %s\n", data.Release)
			fmt.Printf("- Defined in code as: %s\n\n", data.CodeName)
			fmt.Printf("%s\n\n", data.Description)
		}
		return
	}

	// 将整个列表序列化。通常用于生成 conformance.txt，以记录测试集合。
	b, err := yaml.Marshal(dataSet)
	if err != nil {
		log.Printf("Error marshalling data into YAML: %v", err)
	}
	fmt.Println(string(b))
}

func getConformanceData(targetFrame frame) (*ConformanceData, error) {
	root := *k8sPath
	if !strings.HasSuffix(root, string(os.PathSeparator)) {
		root += string(os.PathSeparator)
	}

	trimmedFile := strings.TrimPrefix(targetFrame.File, root)
	targetFrame.File = trimmedFile

	freader, err := os.Open(targetFrame.File)
	if err != nil {
		return nil, err
	}
	defer freader.Close()

	cd, err := scanFileForFrame(targetFrame.File, freader, targetFrame)
	if err != nil {
		return nil, err
	}
	if cd != nil {
		return cd, nil
	}

	return nil, nil
}

// scanFileForFrame 扫描目标，查找附着在目标帧所描述函数上的一致性注释。
// 如果找不到该注释，则返回 nil, nil。
func scanFileForFrame(filename string, src interface{}, targetFrame frame) (*ConformanceData, error) {
	fset := token.NewFileSet() // 位置相对于 fset
	f, err := parser.ParseFile(fset, filename, src, parser.ParseComments)
	if err != nil {
		return nil, err
	}

	cmap := ast.NewCommentMap(fset, f, f.Comments)
	for _, cs := range cmap {
		for _, c := range cs {
			if cd := tryCommentGroupAndFrame(fset, c, targetFrame); cd != nil {
				return cd, nil
			}
		}
	}
	return nil, nil
}

func validateTestName(s string) error {
	matches := regexIneligibleTags.FindAllString(s, -1)
	if matches != nil {
		return fmt.Errorf("'%s' cannot have invalid tags %v", s, strings.Join(matches, ","))
	}
	return nil
}

func tryCommentGroupAndFrame(fset *token.FileSet, cg *ast.CommentGroup, f frame) *ConformanceData {
	if !shouldProcessCommentGroup(fset, cg, f) {
		return nil
	}

	// 每个 file/line 要么是某个辅助函数（而非一致性注释），要么只对应单个测试。不要重复访问。
	if seenLines != nil {
		seenLines[fmt.Sprintf("%v:%v", f.File, f.Line)] = struct{}{}
	}
	cd := commentToConformanceData(cg.Text())
	if cd == nil {
		return nil
	}

	cd.URL = fmt.Sprintf("%s%s#L%d", *baseURL, f.File, f.Line)
	cd.File = f.File
	return cd
}

func shouldProcessCommentGroup(fset *token.FileSet, cg *ast.CommentGroup, f frame) bool {
	lineDiff := f.Line - fset.Position(cg.End()).Line
	return lineDiff > 0 && lineDiff <= conformanceCommentsLineWindow
}

func commentToConformanceData(comment string) *ConformanceData {
	lines := strings.Split(comment, "\n")
	descLines := []string{}
	cd := &ConformanceData{}
	var curLine string
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if len(line) == 0 {
			continue
		}
		if sline := regexp.MustCompile("^Testname\\s*:\\s*").Split(line, -1); len(sline) == 2 {
			curLine = "Testname"
			cd.TestName = sline[1]
			continue
		}
		if sline := regexp.MustCompile("^Release\\s*:\\s*").Split(line, -1); len(sline) == 2 {
			curLine = "Release"
			cd.Release = sline[1]
			continue
		}
		if sline := regexp.MustCompile("^Description\\s*:\\s*").Split(line, -1); len(sline) == 2 {
			curLine = "Description"
			descLines = append(descLines, sline[1])
			continue
		}

		// 该行没有头部标记
		if curLine == "Description" {
			descLines = append(descLines, line)
		}
	}
	if cd.TestName == "" {
		return nil
	}

	cd.Description = strings.Join(descLines, " ")
	return cd
}
