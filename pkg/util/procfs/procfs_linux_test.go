//go:build linux

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

package procfs

import (
	"os"
	"os/signal"
	"path/filepath"
	"regexp"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// verifyContainerName 校验 containerNameFromProcCgroup 的解析结果：
// 按 expectedErr 决定应返回错误还是应得到 expectedName。
func verifyContainerName(procCgroupText, expectedName string, expectedErr bool, t *testing.T) {
	name, err := containerNameFromProcCgroup(procCgroupText)
	if expectedErr && err == nil {
		t.Errorf("Expected error but did not get error in verifyContainerName")
		return
	} else if !expectedErr && err != nil {
		t.Errorf("Expected no error, but got error %+v in verifyContainerName", err)
		return
	} else if expectedErr {
		return
	}
	if name != expectedName {
		t.Errorf("Expected container name %s but got name %s", expectedName, name)
	}
}

// TestContainerNameFromProcCgroup 覆盖 cgroup 内容中 devices 行存在、缺失，
// 以及真实示例文件和格式非法等情况。
func TestContainerNameFromProcCgroup(t *testing.T) {
	procCgroupValid := "2:devices:docker/kubelet"
	verifyContainerName(procCgroupValid, "docker/kubelet", false, t)

	procCgroupEmpty := ""
	verifyContainerName(procCgroupEmpty, "", true, t)

	content, err := os.ReadFile("example_proc_cgroup")
	if err != nil {
		t.Errorf("Could not read example /proc cgroup file")
	}
	verifyContainerName(string(content), "/user/1000.user/c1.session", false, t)

	procCgroupNoDevice := "2:freezer:docker/kubelet\n5:cpuacct:pkg/kubectl"
	verifyContainerName(procCgroupNoDevice, "", true, t)

	procCgroupInvalid := "devices:docker/kubelet\ncpuacct:pkg/kubectl"
	verifyContainerName(procCgroupInvalid, "", true, t)
}

// TestPidOf 验证 PidOf 能按当前测试进程名找到自身 PID。
func TestPidOf(t *testing.T) {
	pids, err := PidOf(filepath.Base(os.Args[0]))
	assert.Empty(t, err)
	assert.NotZero(t, pids)
	assert.Contains(t, pids, os.Getpid())
}

// TestPKill 用 SIGCONT 向自身进程发送信号，验证 PKill 能命中目标进程。
// 选择 SIGCONT 是为了不打断测试进程。
func TestPKill(t *testing.T) {
	sig := syscall.SIGCONT
	c := make(chan os.Signal, 1)
	signal.Notify(c, sig)
	defer signal.Stop(c)
	PKill(os.Args[0], sig)
	select {
	case s := <-c:
		if s != sig {
			t.Fatalf("signal was %v, want %v", s, sig)
		}
	case <-time.After(1 * time.Second):
		t.Fatalf("timeout waiting for %v", sig)
	}
}

// BenchmarkGetPids 测量 getPids 遍历 /proc 的开销；断言操作在计时之外进行。
func BenchmarkGetPids(b *testing.B) {
	re, err := regexp.Compile("(^|/)" + filepath.Base(os.Args[0]) + "$")
	assert.Empty(b, err)

	for i := 0; i < b.N; i++ {
		pids := getPids(re)

		b.StopTimer()
		assert.NotZero(b, pids)
		assert.Contains(b, pids, os.Getpid())
		b.StartTimer()
	}
}
