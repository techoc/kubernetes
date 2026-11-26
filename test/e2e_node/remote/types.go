/*
Copyright 2016 The Kubernetes Authors.

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

// Package remote 包含 TestSuite 接口的实现，用于指定
// 如何远程运行各种 node 测试套件。
package remote

import (
	"fmt"
	"time"
)

// TestSuite 是测试套件的接口，例如 node e2e、node conformance、
// node soaking、cri validation 等。
type TestSuite interface {
	// SetupTestPackage 在给定目录中准备测试包。TestSuite
	// 应把所有必要的二进制和依赖放到该路径下。调用方
	// 会：
	// * 把该目录打成 tarball。
	// * 把 tarball 部署到测试主机。
	// * 在测试主机的工作区解压 tarball。
	SetupTestPackage(path, systemSpecName string) error
	// RunTest 在给定工作区的节点上运行测试，返回测试输出
	// 和测试错误（如果有）。
	// * host 是运行测试的目标节点。
	// * workspace 是测试主机上运行测试的工作目录。注意
	//   运行测试前，测试包已解压到该工作区。
	// * results 是测试应把结果写入的目录。所有日志应
	//   保存为 *.log，所有 junit 文件应以 junit* 开头。
	// * imageDesc 是运行测试所用镜像的描述。
	//   仅用于日志。
	// * junitFilePrefix 是输出 junit 文件的前缀。
	// * testArgs 是传给测试的参数。
	// * ginkgoArgs 是传给 ginkgo 的参数。
	// * systemSpecName 是校验测试所用镜像时使用的
	//   system spec 名称。
	// * extraEnvs 是 node e2e 测试需要的额外环境变量。
	// * runtimeConfig 是 node e2e 测试使用的 API runtime 配置。
	// * timeout 是测试超时时间。
	RunTest(host, workspace, results, imageDesc, junitFilePrefix, testArgs, ginkgoArgs, systemSpecName, extraEnvs, runtimeConfig string, timeout time.Duration) (string, error)
}

var testSuites = make(map[string]TestSuite)

func RegisterTestSuite(name string, suite TestSuite) {
	testSuites[name] = suite
}

func GetTestSuiteKeys() []string {
	var keys []string
	for key := range testSuites {
		keys = append(keys, key)
	}
	return keys
}

func GetTestSuite(name string) (TestSuite, error) {
	suite, ok := testSuites[name]
	if ok {
		return suite, nil
	}
	return nil, fmt.Errorf("unable to find testsuite for %s", name)
}

type NewRunner func(Config) Runner

var runners = make(map[string]NewRunner)

func RegisterRunner(name string, runner NewRunner) {
	runners[name] = runner
}

func GetRunner(name string) (NewRunner, error) {
	runner, ok := runners[name]
	if ok {
		return runner, nil
	}
	return nil, fmt.Errorf("unable to runner for %s", name)
}
