/*
Copyright 2023 The Kubernetes Authors.

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

package verflag

import (
	"bytes"
	"fmt"
	"strings"
	"testing"

	"github.com/spf13/pflag"

	"k8s.io/component-base/version"
)

// TestVersionFlag 覆盖 --version 的全部解析形态：无值、布尔值、raw、
// 版本覆盖，以及非法覆盖版本时的错误路径。
// 测试通过替换 output/exit 变量避免真正退出进程。
func TestVersionFlag(t *testing.T) {
	initialFlagValue := string(*versionFlag)
	initialVersion := version.Get()

	testcases := []struct {
		name               string
		flags              []string
		expectError        string
		expectExit         bool
		expectPrintVersion string
		expectGitVersion   string
	}{
		{
			// 不传标志时既不退出也不改变版本。
			name:             "no flag",
			flags:            []string{},
			expectGitVersion: initialVersion.GitVersion,
		},
		{
			name:             "false",
			flags:            []string{"--version=false"},
			expectGitVersion: initialVersion.GitVersion,
		},

		{
			// 无值形式应等价于 --version=true。
			name:               "valueless",
			flags:              []string{"--version"},
			expectGitVersion:   initialVersion.GitVersion,
			expectExit:         true,
			expectPrintVersion: "Kubernetes " + initialVersion.GitVersion,
		},
		{
			name:               "true",
			flags:              []string{"--version=true"},
			expectGitVersion:   initialVersion.GitVersion,
			expectExit:         true,
			expectPrintVersion: "Kubernetes " + initialVersion.GitVersion,
		},
		{
			// raw 打印完整结构体，供脚本取全部字段。
			name:               "raw",
			flags:              []string{"--version=raw"},
			expectGitVersion:   initialVersion.GitVersion,
			expectExit:         true,
			expectPrintVersion: fmt.Sprintf("%#v", initialVersion),
		},
		{
			// ParseBool 接受 T/1 等简写，与 Go 标准库语义一致。
			name:               "truthy",
			flags:              []string{"--version=T"},
			expectGitVersion:   initialVersion.GitVersion,
			expectExit:         true,
			expectPrintVersion: "Kubernetes " + initialVersion.GitVersion,
		},

		{
			// vX.Y.Z 形式只覆盖 GitVersion，不退出。
			name:             "override",
			flags:            []string{"--version=v0.0.0-custom"},
			expectGitVersion: "v0.0.0-custom",
		},
		{
			name:        "invalid override semver",
			flags:       []string{"--version=vX"},
			expectError: `could not parse "vX"`,
		},
		{
			// 覆盖版本的主/次/补丁必须与构建版本一致，否则拒绝。
			name:        "invalid override major",
			flags:       []string{"--version=v1.0.0"},
			expectError: `must match major/minor/patch`,
		},
		{
			name:        "invalid override minor",
			flags:       []string{"--version=v0.1.0"},
			expectError: `must match major/minor/patch`,
		},
		{
			name:        "invalid override patch",
			flags:       []string{"--version=v0.0.1"},
			expectError: `must match major/minor/patch`,
		},

		{
			// 先覆盖再要求打印时，输出的是覆盖后的版本。
			name:               "override and exit",
			flags:              []string{"--version=v0.0.0-custom", "--version"},
			expectGitVersion:   "v0.0.0-custom",
			expectExit:         true,
			expectPrintVersion: "Kubernetes v0.0.0-custom",
		},
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {

			originalOutput := output
			originalExit := exit

			// 替换可写输出与退出函数，避免测试进程真的退出。
			outputBuffer := &bytes.Buffer{}
			output = outputBuffer
			exitCalled := false
			exit = func(code int) { exitCalled = true }

			t.Cleanup(func() {
				// 恢复全局状态：标志值、输出、退出函数以及动态版本。
				output = originalOutput
				exit = originalExit
				*versionFlag = versionValue(initialFlagValue)
				err := version.SetDynamicVersion(initialVersion.GitVersion)
				if err != nil {
					t.Fatal(err)
				}
			})

			fs := pflag.NewFlagSet("test", pflag.ContinueOnError)
			AddFlags(fs)
			err := fs.Parse(tc.flags)
			if tc.expectError != "" {
				if err == nil {
					t.Fatal("expected error, got none")
				}
				if !strings.Contains(err.Error(), tc.expectError) {
					t.Fatalf("expected error containing %q, got %q", tc.expectError, err.Error())
				}
				return
			} else if err != nil {
				t.Fatalf("unexpected parse error: %v", err)
			}

			if e, a := tc.expectGitVersion, version.Get().GitVersion; e != a {
				t.Fatalf("gitversion: expected %v, got %v", e, a)
			}

			PrintAndExitIfRequested()
			if e, a := tc.expectExit, exitCalled; e != a {
				t.Fatalf("exit(): expected %v, got %v", e, a)
			}
			if e, a := tc.expectPrintVersion, strings.TrimSpace(outputBuffer.String()); e != a {
				t.Fatalf("print version: expected %v, got %v", e, a)
			}
		})
	}
}
