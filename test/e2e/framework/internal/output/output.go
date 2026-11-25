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

package output

import (
	"encoding/xml"
	"os"
	"path"
	"regexp"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/ginkgo/v2/reporters"
	"github.com/onsi/gomega"
	"github.com/stretchr/testify/require"

	"k8s.io/kubernetes/test/e2e/framework"
	"k8s.io/kubernetes/test/e2e/framework/internal/junit"
)

// TestGinkgoOutput 运行当前 suite 并验证生成的 JUnit 文件与预期结果一致。
//
// 控制台上的 Ginkgo 输出（即测试套件日志）不在检查范围内。它对 CI 通常没那么重要，
// 在交互式使用测试套件时更有意义。想查看 Ginkgo 输出的样貌，
// 可用 "go test -v" 运行测试。
func TestGinkgoOutput(t *testing.T, expected TestResult, runSpecsArgs ...interface{}) {
	tmpdir := t.TempDir()
	junitFile := path.Join(tmpdir, "junit.xml")
	gomega.RegisterFailHandler(framework.Fail)
	ginkgo.ReportAfterSuite("write JUnit file", func(report ginkgo.Report) {
		junit.WriteJUnitReport(report, junitFile)
	})
	fakeT := &testing.T{}
	ginkgo.RunSpecs(fakeT, "Logging Suite", runSpecsArgs...)

	var actual reporters.JUnitTestSuites
	data, err := os.ReadFile(junitFile)
	require.NoError(t, err)
	err = xml.Unmarshal(data, &actual)
	require.NoError(t, err)

	if len(actual.TestSuites) != 1 {
		t.Fatalf("expected one test suite, got %d, JUnit content:\n%s", len(actual.TestSuites), string(data))
	}
	diff := cmp.Diff(expected.Suite, actual.TestSuites[0],
		// Time 每次都在变。
		// Name 和 Classname 固定为 "Logging Suite"。
		// Package 包含变化的路径，无需比较。
		// Properties 也过于复杂，不便比较。
		cmpopts.IgnoreFields(reporters.JUnitTestSuite{}, "Time", "Timestamp", "Name", "Package", "Properties"),
		cmpopts.IgnoreFields(reporters.JUnitTestCase{}, "Time", "Classname"),
		cmpopts.SortSlices(func(tc1, tc2 reporters.JUnitTestCase) bool {
			return tc1.Name < tc2.Name
		}),
		cmpopts.AcyclicTransformer("simplify", func(in string) any {
			out := simplify(in, expected)
			// 字符串过长时 cmp.Diff 有时不会完整打印。
			// 取消这里的注释有助于排查差异。
			// if len(out) > 100 {
			// 	t.Logf("%s\n---------------------------------------\n%s\n", in, out)
			// }

			// 思路与
			// https://github.com/google/go-cmp/issues/192#issuecomment-605346277:
			// 相同：强制 cmp.Diff 逐行比较字符串，
			// 即使它通常不会这么做。代价是输出更难还原成
			// 预期的参考字符串。
			// if len(out) > 50 {
			// 	return strings.Split(out, "\n")
			// }

			return out
		}),
	)
	if diff != "" {
		t.Fatalf("Simplified JUnit report not as expected (-want, +got):\n%s\n\nFull XML:\n%s", diff, string(data))
	}
}

// TestResult 是测试套件的预期结果，附带决定相等性的额外参数。
type TestResult struct {
	// 非 nil 时，在比较前调用以规范化所有输出字符串。
	NormalizeOutput func(string) string

	// 所有测试用例及整个 suite 的结果。
	Suite reporters.JUnitTestSuite
}

func simplify(in string, expected TestResult) string {
	out := normalizeLocation(in)
	out = stripTimes(out)
	out = stripAddresses(out)
	out = normalizeInitFunctions(out)
	if expected.NormalizeOutput != nil {
		out = expected.NormalizeOutput(out)
	}
	return out
}

// timePrefix 匹配每行开头的 "Jul 17 08:08:25.950: "。
var timePrefix = regexp.MustCompile(`(?m)^[[:alpha:]]{3} +[[:digit:]]{1,2} +[[:digit:]]{2}:[[:digit:]]{2}:[[:digit:]]{2}.[[:digit:]]{3}: `)

// elapsedSuffix 匹配 "Elapsed: 16.189µs"
var elapsedSuffix = regexp.MustCompile(`Elapsed: [[:digit:]]+(\.[[:digit:]]+)?(µs|ns|ms|s|m)`)

// afterSuffix 匹配 "after 5.001s."
var afterSuffix = regexp.MustCompile(`after [[:digit:]]+(\.[[:digit:]]+)?(µs|ns|ms|s|m).`)

// timeSuffix 匹配 Ginkgo v2 为日志输出打印的 "@ 09/06/22 15:36:43.44 (5.001s)"，其中时长部分可选。
var timeSuffix = regexp.MustCompile(`(?m)@[[:space:]][[:digit:]]{2}/[[:digit:]]{2}/[[:digit:]]{2} [[:digit:]]{2}:[[:digit:]]{2}:[[:digit:]]{2}(\.[[:digit:]]{1,3})?( \([[:digit:]]+(\.[[:digit:]]+)?(µs|ns|ms|s|m)\))?$`)

func stripTimes(in string) string {
	out := timePrefix.ReplaceAllString(in, "")
	out = elapsedSuffix.ReplaceAllString(out, "Elapsed: <elapsed>")
	out = timeSuffix.ReplaceAllString(out, "<time>")
	out = afterSuffix.ReplaceAllString(out, "after <after>.")
	return out
}

// instanceAddr 匹配 " | 0xc0003dec60>"
var instanceAddr = regexp.MustCompile(` \| 0x[0-9a-fA-F]+>`)

func stripAddresses(in string) string {
	return instanceAddr.ReplaceAllString(in, ">")
}

// stackLocation 匹配斜杠之后的 "<some path>/<file>.go:75 +0x1f1"（本地构建）
// 或少数几个相对路径之一（Kubernetes CI 构建）。
var stackLocation = regexp.MustCompile(`(?:/|vendor/|test/|GOROOT/).*/([[:^space:]]+.go:[[:digit:]]+)( \+0x[0-9a-fA-F]+)?`)

// functionArgs 匹配 "<function name>(...)"，其中 <function name> 可能是匿名函数（例如 "pod_test.glob..func1.1"）。
var functionArgs = regexp.MustCompile(`([[:alpha:][:digit:].]+)\(.*\)`)

// klogPrefix 匹配 "I0822 16:10:39.343790  989127 "
var klogPrefix = regexp.MustCompile(`(?m)^[IEF][[:digit:]]{4} [[:digit:]]{2}:[[:digit:]]{2}:[[:digit:]]{2}\.[[:digit:]]{6}[[:space:]]+[[:digit:]]+ `)

// testFailureOutput 匹配 TestFailureOutput() 及其源码位置，后跟其他堆栈条目：
//
// k8s.io/kubernetes/test/e2e/framework/pod/pod_test.TestFailureOutput(0xc000558800)
//
//	/nvme/gopath/src/k8s.io/kubernetes/test/e2e/framework/pod/wait_test.go:73 +0x1c9
//
// testing.tRunner(0xc000558800, 0x1af2848)
//
//	/nvme/gopath/go/src/testing/testing.go:865 +0xc0
//
// 由 testing.(*T).Run 创建
//
//	/nvme/gopath/go/src/testing/testing.go:916 +0x35a
var testFailureOutput = regexp.MustCompile(`(?m)^k8s.io/kubernetes/test/e2e/framework/internal/output\.TestGinkgoOutput\(.*\n\t.*(\n.*\n\t.*)*`)

// normalizeLocation 移除路径前缀、函数参数以及我们不关心的某些堆栈条目。
func normalizeLocation(in string) string {
	out := in
	out = stackLocation.ReplaceAllString(out, "$1")
	out = functionArgs.ReplaceAllString(out, "$1()")
	out = testFailureOutput.ReplaceAllString(out, "")
	out = klogPrefix.ReplaceAllString(out, "<klog> ")
	return out
}

var initFunc = regexp.MustCompile(`(init\.+func|glob\.+func)`)

// normalizeInitFunctions 将 init.func（Go >= 1.22 使用）和
// glob..func（Go < 1.22 使用）都映射为 <init.func>。
func normalizeInitFunctions(in string) string {
	out := initFunc.ReplaceAllString(in, "<init.func>")
	return out
}
