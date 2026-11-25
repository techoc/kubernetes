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

const (
	// resultsTarballName 是我们打包所有结果所用的 tarball 名称。
	resultsTarballName = "e2e.tar.gz"

	// doneFileName 是用于通知 Sonobuoy worker 我们已完成的信号文件名称。
	// 该文件应包含结果文件的路径。
	doneFileName = "done"

	// resultsDirEnvKey 是存放 donefile 和结果的目录对应的环境变量。
	// 它是插件与 Sonobuoy 之间共享的挂载卷。
	resultsDirEnvKey = "RESULTS_DIR"

	// logFileName 是 stdout 通过 tee 写入的文件名称。
	logFileName = "e2e.log"

	// 在 go runner 之前就已明确支持的其他环境变量。
	dryRunEnvKey     = "E2E_DRYRUN"
	parallelEnvKey   = "E2E_PARALLEL"
	focusEnvKey      = "E2E_FOCUS"
	skipEnvKey       = "E2E_SKIP"
	providerEnvKey   = "E2E_PROVIDER"
	kubeconfigEnvKey = "KUBECONFIG"
	ginkgoEnvKey     = "GINKGO_BIN"
	testBinEnvKey    = "TEST_BIN"

	// extraGinkgoArgsEnvKey 若设置，是要传给 ginkgo 的其他参数列表。
	// 这些参数位于测试二进制文件之前，例如 `--afterSuiteHook`。
	extraGinkgoArgsEnvKey = "E2E_EXTRA_GINKGO_ARGS"

	// extraArgsEnvKey 若设置，是要传给测试的其他参数列表。
	// 这些参数位于 `--` 之后，例如 `--provider`。
	extraArgsEnvKey = "E2E_EXTRA_ARGS"

	// extraArgsSeparaterEnvKey 指定如何拆分额外参数值。若未设置，
	// 默认按空格拆分。
	extraArgsSeparaterEnvKey = "E2E_EXTRA_ARGS_SEP"

	defaultSkip         = ""
	defaultFocus        = "\\[Conformance\\]"
	defaultProvider     = "local"
	defaultParallel     = "1"
	defaultResultsDir   = "/tmp/results"
	defaultGinkgoBinary = "/usr/local/bin/ginkgo"
	defaultTestBinary   = "/usr/local/bin/e2e.test"

	// serialTestsRegexp 是并行运行时的默认 skip 值，不会覆盖
	// 显式设置的 E2E_SKIP 值。
	serialTestsRegexp = "\\[Serial\\]"
)
