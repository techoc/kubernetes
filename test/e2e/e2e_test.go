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

package e2e

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/onsi/ginkgo/v2"
	yaml "go.yaml.in/yaml/v2"

	// 永远不要删掉含 "/ginkgo" 的这一行。少了它，
	// ginkgo test runner 就无法识别这个目录包含 Ginkgo 测试套件。
	// 见 https://github.com/kubernetes/kubernetes/issues/74827
	// "github.com/onsi/ginkgo/v2"

	"k8s.io/component-base/version"
	"k8s.io/klog/v2"
	conformancetestdata "k8s.io/kubernetes/test/conformance/testdata"
	"k8s.io/kubernetes/test/e2e/framework"
	"k8s.io/kubernetes/test/e2e/framework/config"
	"k8s.io/kubernetes/test/e2e/framework/testfiles"
	e2etestingmanifests "k8s.io/kubernetes/test/e2e/testing-manifests"
	testfixtures "k8s.io/kubernetes/test/fixtures"

	// 定义并冻结常量
	_ "k8s.io/kubernetes/test/e2e/feature"

	// 测试源码
	_ "k8s.io/kubernetes/test/e2e/apimachinery"
	_ "k8s.io/kubernetes/test/e2e/apps"
	_ "k8s.io/kubernetes/test/e2e/architecture"
	_ "k8s.io/kubernetes/test/e2e/auth"
	_ "k8s.io/kubernetes/test/e2e/autoscaling"
	_ "k8s.io/kubernetes/test/e2e/common"
	_ "k8s.io/kubernetes/test/e2e/dra"
	_ "k8s.io/kubernetes/test/e2e/instrumentation"
	_ "k8s.io/kubernetes/test/e2e/invariants"
	"k8s.io/kubernetes/test/e2e/invariants/logcheck"
	_ "k8s.io/kubernetes/test/e2e/kubectl"
	_ "k8s.io/kubernetes/test/e2e/network"
	_ "k8s.io/kubernetes/test/e2e/node"
	_ "k8s.io/kubernetes/test/e2e/scheduling"
	_ "k8s.io/kubernetes/test/e2e/storage"
	_ "k8s.io/kubernetes/test/e2e/storage/csimock"
	_ "k8s.io/kubernetes/test/e2e/storage/external"
	_ "k8s.io/kubernetes/test/e2e/windows"

	// 重新配置 framework
	_ "k8s.io/kubernetes/test/e2e/framework/debug/init"
	_ "k8s.io/kubernetes/test/e2e/framework/metrics/init"
	_ "k8s.io/kubernetes/test/e2e/framework/node/init"
	_ "k8s.io/kubernetes/test/utils/ktesting/format"
)

// handleFlags 注册所有 flag 并解析命令行。
func handleFlags() {
	config.CopyFlags(config.Flags, flag.CommandLine)
	framework.RegisterCommonFlags(flag.CommandLine)
	framework.RegisterClusterFlags(flag.CommandLine)
	logcheck.RegisterFlags(flag.CommandLine)
	flag.Parse()
}

func TestMain(m *testing.M) {
	var versionFlag bool
	flag.CommandLine.BoolVar(&versionFlag, "version", false, "Displays version information.")
	listConformanceTests := flag.CommandLine.Bool("list-conformance-tests", false, "If true, will show list of conformance tests.")

	// 先注册测试 flag，再解析。
	handleFlags()

	if versionFlag {
		fmt.Printf("%s\n", version.Get())
		os.Exit(0)
	}

	if flag.CommandLine.NArg() > 0 {
		fmt.Fprintf(os.Stderr, "unknown additional command line arguments: %s", flag.CommandLine.Args())
		os.Exit(1)
	}

	// 启用嵌入式 FS 作为文件查找的后备来源
	testfiles.AddFileSource(e2etestingmanifests.GetE2ETestingManifestsFS())
	testfiles.AddFileSource(testfixtures.GetTestFixturesFS())
	testfiles.AddFileSource(conformancetestdata.GetConformanceTestdataFS())

	if *listConformanceTests {
		var tests []struct {
			Testname    string `yaml:"testname"`
			Codename    string `yaml:"codename"`
			Description string `yaml:"description"`
			Release     string `yaml:"release"`
			File        string `yaml:"file"`
		}

		data, err := testfiles.Read("test/conformance/testdata/conformance.yaml")
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		if err := yaml.Unmarshal(data, &tests); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		if err := yaml.NewEncoder(os.Stdout).Encode(tests); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		os.Exit(0)
	}

	framework.AfterReadingAllFlags(&framework.TestContext)

	// TODO: 逐步弃用 repo-root，改为只用 gobindata_util.go，见 #23987。
	// 目前仍需要它，例如：
	// test/e2e/framework/ingress/ingress_utils.go
	// 用它提供可选的 secret.yaml 文件；
	// test/e2e/framework/util.go 用它做 cluster/log-dump。
	if framework.TestContext.RepoRoot != "" {
		testfiles.AddFileSource(testfiles.RootFileSource{Root: framework.TestContext.RepoRoot})
	}

	os.Exit(m.Run())
}

func TestE2E(t *testing.T) {
	RunE2ETests(t)
}

var _ = ginkgo.ReportAfterEach(func(report ginkgo.SpecReport) {
	progressReporter.ProcessSpecReport(report)
})

var _ = ginkgo.ReportBeforeSuite(func(report ginkgo.Report) {
	progressReporter.SetTestsTotal(report.PreRunStats.SpecsThatWillRun)
})

var _ = ginkgo.ReportAfterSuite("Kubernetes e2e suite report", func(report ginkgo.Report) {
	var err error
	// DetailsRepoerter 会输出每个测试的详细信息（名称、文件、行号等），
	// 便于为测试编写文档。
	if len(framework.TestContext.SpecSummaryOutput) <= 0 {
		return
	}
	absPath, err := filepath.Abs(framework.TestContext.SpecSummaryOutput)
	if err != nil {
		klog.Errorf("%#v\n", err)
		panic(err)
	}
	f, err := os.Create(absPath)
	if err != nil {
		klog.Errorf("%#v\n", err)
		panic(err)
	}

	defer f.Close()

	for _, specReport := range report.SpecReports {
		b, err := specReport.MarshalJSON()
		if err != nil {
			klog.Errorf("Error in detail reporter: %v", err)
			return
		}
		_, err = f.Write(b)
		if err != nil {
			klog.Errorf("Error saving test details in detail reporter: %v", err)
			return
		}
		// 记录之间打印换行，方便各种工具查看。
		_, err = fmt.Fprintln(f, "")
		if err != nil {
			klog.Errorf("Error saving test details in detail reporter: %v", err)
			return
		}
	}
})
