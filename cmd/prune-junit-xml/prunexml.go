/*
Copyright 2022 The Kubernetes Authors.

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
	"bufio"
	"bytes"
	"encoding/xml"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"

	"k8s.io/kubernetes/cmd/prune-junit-xml/logparse"
	"k8s.io/kubernetes/third_party/forked/gotestsum/junitxml"
	"sigs.k8s.io/yaml"
)

func main() {
	maxTextSize := flag.Int("max-text-size", 1, "maximum size of attribute or text (in MB)")
	pruneTests := flag.Bool("prune-tests", true,
		"prune's xml files to display only top level tests and failed sub-tests")
	addOwners := flag.Bool("add-owners", true,
		"when pruning tests, also look for OWNERs files of the packages and prefix the names with [sig-...] if found")
	flag.Parse()

	pkgs := newPackageOwners(*addOwners)

	for _, path := range flag.Args() {
		fmt.Printf("processing junit xml file : %s\n", path)
		xmlReader, err := os.Open(path)
		if err != nil {
			panic(err)
		}
		defer xmlReader.Close()
		suites, err := fetchXML(xmlReader) // 大致把 MB 换算成字节
		if err != nil {
			panic(err)
		}

		pruneXML(suites, *maxTextSize*1e6) // 大致把 MB 换算成字节
		if *pruneTests {
			pruneTESTS(suites, pkgs)
		}

		xmlWriter, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0666)
		if err != nil {
			panic(err)
		}
		defer xmlWriter.Close()
		err = streamXML(xmlWriter, suites)
		if err != nil {
			panic(err)
		}
		fmt.Println("done.")
	}
}

func pruneXML(suites *junitxml.JUnitTestSuites, maxBytes int) {
	for _, suite := range suites.Suites {
		for i := range suite.TestCases {
			// 必要时直接修改 TestCases 切片。
			testcase := &suite.TestCases[i]
			if testcase.SkipMessage != nil {
				pruneStringIfNeeded(&testcase.SkipMessage.Message, maxBytes, "clipping skip message in test case : %s\n", testcase.Name)
			}
			if testcase.Failure != nil {
				// 在 Go 单元测试中，由于 `go
				// test` 不记录测试失败的具体原因，
				// 整个测试输出都会成为失败消息，这会让
				// 失败消息变得非常大。
				//
				// 由于 Kubernetes 对测试失败的格式没有约定，
				// 我们在这里同样无法识别真正的失败信息；
				// 能做的只是识别 klog 添加的日志输出。
				//
				// 因此这里把完整文本移到测试输出中，
				// 失败消息里只保留那些并非来自 klog 的行。
				if testcase.SystemOut == "" {
					var buf strings.Builder
					// 遍历所有日志条目，决定哪些内容保留为失败消息。
					for entry := range logparse.All(strings.NewReader(testcase.Failure.Contents)) {
						if _, ok := entry.(*logparse.KlogEntry); ok {
							continue
						}
						_, _ = buf.WriteString(entry.LogData())
					}
					if buf.Len() < len(testcase.Failure.Contents) {
						// 两个字符串现在不同了，因此都要更新。
						testcase.SystemOut = testcase.Failure.Contents
						pruneStringIfNeeded(&testcase.SystemOut, maxBytes, "clipping log output in test case: %s\n", testcase.Name)
						testcase.Failure.Contents = buf.String()
					}
				}
				pruneStringIfNeeded(&testcase.Failure.Contents, maxBytes, "clipping failure message in test case : %s\n", testcase.Name)
			}
		}
	}
}

func pruneStringIfNeeded(str *string, maxBytes int, msg string, args ...any) {
	if len(*str) <= maxBytes {
		return
	}
	fmt.Printf(msg, args...)
	head := (*str)[:maxBytes/2]
	tail := (*str)[len(*str)-maxBytes/2:]
	*str = head + "[...clipped...]" + tail
}

// 该函数压缩 junit xml，使包名成为顶层标识，
// 其余内容嵌套在其下。
func pruneTESTS(suites *junitxml.JUnitTestSuites, pkgs *packageOwners) {
	var updatedTestsuites []junitxml.JUnitTestSuite

	for _, suite := range suites.Suites {
		var updatedTestcases []junitxml.JUnitTestCase
		var updatedTestcase junitxml.JUnitTestCase
		var updatedTestcaseFailure junitxml.JUnitFailure
		failflag := false
		name := suite.Name

		// 如果可能，注入所属 SIG 前缀。
		// 这一步必须在我们手上仍保有很可能是完整包名的时候完成。
		name = pkgs.addOwner(name)

		regex := regexp.MustCompile(`^(.*?)/([^/]+)/?$`)
		match := regex.FindStringSubmatch(name)
		baseName := match[1]
		leafName := match[2]

		// testgrid 使用 suite.Name。
		// Spyglass/Prow 使用 testcase.Classname。
		// 因此两者都需要更新。
		suite.Name = baseName
		updatedTestcase.Classname = baseName
		updatedTestcase.Name = leafName
		updatedTestcase.Time = suite.Time
		updatedSystemOut := ""
		updatedSystemErr := ""
		for _, testcase := range suite.TestCases {
			// JUnit xml 文件中顶层的 testcase 元素不含 / 字符。
			if testcase.Failure != nil {
				failflag = true
				updatedTestcaseFailure.Message = joinTexts(updatedTestcaseFailure.Message, testcase.Failure.Message)
				updatedTestcaseFailure.Contents = joinTexts(updatedTestcaseFailure.Contents, testcase.Failure.Contents)
				updatedTestcaseFailure.Type = joinTexts(updatedTestcaseFailure.Type, testcase.Failure.Type)
				updatedSystemOut = joinTexts(updatedSystemOut, testcase.SystemOut)
				updatedSystemErr = joinTexts(updatedSystemErr, testcase.SystemErr)
			}
		}
		if failflag {
			updatedTestcase.Failure = &updatedTestcaseFailure
			updatedTestcase.SystemOut = updatedSystemOut
			updatedTestcase.SystemErr = updatedSystemErr
		}
		suite.TestCases = append(updatedTestcases, updatedTestcase)
		updatedTestsuites = append(updatedTestsuites, suite)
	}
	suites.Suites = updatedTestsuites
}

// 当 a 和 b 都非空时，joinTexts 返回 "<a><empty line><b>"；
// 否则返回其中非空的那个字符串（如果有的话）。
//
// 如果 <b> 完全包含在 <a> 中，则返回 <a>，因为重复完全相同的字符串
// 不会带来任何新信息。这种情况通常出现在拼接失败消息时：无论测试
// 记录了什么，失败消息都是固定的字符串 "Failed"。而测试日志输出
// 通常各不相同，因为它们包含 "=== RUN
// <test name>"，因此不会被丢弃。
func joinTexts(a, b string) string {
	if a == "" {
		return b
	}
	if b == "" {
		return a
	}
	if strings.Contains(a, b) {
		return a
	}
	sep := "\n"
	if !strings.HasSuffix(a, "\n") {
		sep = "\n\n"
	}
	return a + sep + b
}

func fetchXML(xmlReader io.Reader) (*junitxml.JUnitTestSuites, error) {
	decoder := xml.NewDecoder(xmlReader)
	var suites junitxml.JUnitTestSuites
	err := decoder.Decode(&suites)
	if err != nil {
		return nil, err
	}
	return &suites, nil
}

func streamXML(writer io.Writer, in *junitxml.JUnitTestSuites) error {
	_, err := writer.Write([]byte("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"))
	if err != nil {
		return err
	}
	encoder := xml.NewEncoder(writer)
	encoder.Indent("", "\t")
	err = encoder.Encode(in)
	if err != nil {
		return err
	}
	return encoder.Flush()
}

type packageOwners struct {
	// pkgs 记录从 import 路径到目录的映射。
	pkgs map[string]string
}

func newPackageOwners(enabled bool) *packageOwners {
	if !enabled {
		return nil
	}

	return &packageOwners{
		pkgs: make(map[string]string),
	}
}

// addOwner 接受包名（即 import 路径，如 k8s.io/client-go），
// 尝试查找该包的源代码，然后逐级向上查找，
// 直到找到带有 sig 标签的 OWNERS 文件，并使用找到的第一个 sig 标签。
//
// 如果成功，就把对应的 SIG 加到名称前面；
// 否则名称保持不变。
func (p *packageOwners) addOwner(name string) string {
	if p == nil {
		return name
	}

	dir := p.pkgs[name]
	if dir == "" {
		// 通过 `go list` 查找。为减少调用次数，
		// 我们同时查询子包并缓存结果；错误可以忽略。
		//
		// 这与 golang.org/x/tools/go/packages 的做法大致相同，
		// 但这里不能用它，因为会给 k/k 引入新的依赖。
		cmd := exec.Command("go", "list", "-f", "{{.ImportPath}}:{{.Dir}}", name+"/...")
		out, _ := cmd.Output()
		scanner := bufio.NewScanner(bytes.NewReader(out))
		for scanner.Scan() {
			line := scanner.Text()
			parts := strings.SplitN(line, ":", 2)
			if len(parts) != 2 {
				continue
			}
			p.pkgs[parts[0]] = parts[1]
		}
		dir = p.pkgs[name]
	}

	// 从绝对路径开始逐级向上，直到到达文件系统根目录。
	// 在任何根目录（/、C:\、\\server\share）下 filepath.Dir 都会返回 dir 本身，
	// 因此用 filepath.Dir(dir) != dir 判断可统一处理所有路径风格。
	for ; dir != "" && filepath.Dir(dir) != dir; dir = filepath.Dir(dir) {
		data, err := os.ReadFile(filepath.Join(dir, "OWNERS"))
		if err != nil {
			continue
		}
		var owners owners
		if err := yaml.Unmarshal(data, &owners); err != nil {
			continue
		}
		for _, label := range owners.Labels {
			if strings.HasPrefix(label, "sig/") {
				// 找到了！
				return fmt.Sprintf("[sig-%s] %s", label[4:], name)
			}
		}
	}

	return name
}

// owners 只包含 https://go.k8s.io/owners 中我们关心的字段。
type owners struct {
	Labels []string `json:"labels"`
}
