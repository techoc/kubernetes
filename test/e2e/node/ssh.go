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

package node

import (
	"context"
	"fmt"
	"strings"

	"k8s.io/kubernetes/test/e2e/framework"
	e2eskipper "k8s.io/kubernetes/test/e2e/framework/skipper"
	e2essh "k8s.io/kubernetes/test/e2e/framework/ssh"
	admissionapi "k8s.io/pod-security-admission/api"

	"github.com/onsi/ginkgo/v2"
)

const maxNodes = 100

var _ = SIGDescribe("SSH", framework.WithProvider(framework.ProvidersWithSSH...) /* When adding more providers here, also implement their functionality in e2essh.GetSigner(...). */, func() {

	f := framework.NewDefaultFramework("ssh")
	f.NamespacePodSecurityLevel = admissionapi.LevelPrivileged

	ginkgo.BeforeEach(func() {
		// 这个测试要 SSH 到 node，需要 $HOME/.ssh/id_rsa 密钥存在。因此
		// 如果环境没有该密钥就跳过（不是所有 CI 系统都支持这个用例）
		e2eskipper.SkipUnlessSSHKeyPresent()
	})

	ginkgo.It("should SSH to all nodes and run commands", func(ctx context.Context) {
		// 获取所有节点的外部 IP。
		ginkgo.By("Getting all nodes' SSH-able IP addresses")
		hosts, err := e2essh.NodeSSHHosts(ctx, f.ClientSet)
		if err != nil {
			framework.Failf("Error getting node hostnames: %v", err)
		}
		ginkgo.By(fmt.Sprintf("Found %d SSH'able hosts", len(hosts)))

		testCases := []struct {
			cmd            string
			checkStdout    bool
			expectedStdout string
			expectedStderr string
			expectedCode   int
			expectedError  error
		}{
			// 把这个测试放在最前面 —— 这个变体在所有节点上运行。
			{`echo "Hello from $(whoami)@$(hostname)"`, false, "", "", 0, nil},
			{`echo "foo" | grep "bar"`, true, "", "", 1, nil},
			{`echo "stdout" && echo "stderr" >&2 && exit 7`, true, "stdout", "stderr", 7, nil},
		}

		for i, testCase := range testCases {
			// 第一个测试最多对 100 个节点运行。其余的
			// 只对找到的第一个节点运行，因为它们本质上
			// 测试的是 SSH 语义（不需要
			// 对集群中的每个主机都做一遍）。
			nodes := len(hosts)
			if i > 0 {
				nodes = 1
			} else if nodes > maxNodes {
				nodes = maxNodes
			}
			testhosts := hosts[:nodes]
			ginkgo.By(fmt.Sprintf("SSH'ing to %d nodes and running %s", len(testhosts), testCase.cmd))

			for _, host := range testhosts {
				ginkgo.By(fmt.Sprintf("SSH'ing host %s", host))

				result, err := e2essh.SSH(ctx, testCase.cmd, host, framework.TestContext.Provider)
				stdout, stderr := strings.TrimSpace(result.Stdout), strings.TrimSpace(result.Stderr)
				if err != testCase.expectedError {
					framework.Failf("Ran %s on %s, got error %v, expected %v", testCase.cmd, host, err, testCase.expectedError)
				}
				if testCase.checkStdout && stdout != testCase.expectedStdout {
					framework.Failf("Ran %s on %s, got stdout '%s', expected '%s'", testCase.cmd, host, stdout, testCase.expectedStdout)
				}
				if stderr != testCase.expectedStderr {
					framework.Failf("Ran %s on %s, got stderr '%s', expected '%s'", testCase.cmd, host, stderr, testCase.expectedStderr)
				}
				if result.Code != testCase.expectedCode {
					framework.Failf("Ran %s on %s, got exit code %d, expected %d", testCase.cmd, host, result.Code, testCase.expectedCode)
				}
				// 为记录日志，显示 stdout 和 stderr。
				if len(stdout) > 0 {
					framework.Logf("Got stdout from %s: %s", host, strings.TrimSpace(stdout))
				}
				if len(stderr) > 0 {
					framework.Logf("Got stderr from %s: %s", host, strings.TrimSpace(stderr))
				}
			}
		}

		// 快速验证 SSH 自身能正确报错。
		ginkgo.By("SSH'ing to a nonexistent host")
		if _, err = e2essh.SSH(ctx, `echo "hello"`, "i.do.not.exist", framework.TestContext.Provider); err == nil {
			framework.Failf("Expected error trying to SSH to nonexistent host.")
		}
	})
})
