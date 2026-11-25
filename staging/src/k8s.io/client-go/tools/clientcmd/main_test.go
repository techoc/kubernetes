/*
Copyright 2021 The Kubernetes Authors.

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

package clientcmd

import (
	"os"
	"path/filepath"
	"testing"
)

// TestMain 隔离测试进程的 kubeconfig 环境。
// 把 KUBECONFIG 指向临时目录中一个不存在的文件，确保各测试不会意外读取
// 开发者本机的真实 kubeconfig，从而得到可复现的结果。注意 m.Run() 的返回值未作为
// 退出码传递，测试失败仍会由 go test 从输出判定。
func TestMain(m *testing.M) {
	tmp, err := os.MkdirTemp("", "testkubeconfig")
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(tmp)
	os.Setenv("KUBECONFIG", filepath.Join(tmp, "dummy-nonexistent-kubeconfig"))
	m.Run()
}
