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

package app

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"k8s.io/kubernetes/test/utils/ktesting"
)

const fakeKubeconfig = `
apiVersion: v1
kind: Config
clusters:
- cluster:
    insecure-skip-tls-verify: true
    server: %s
  name: default
contexts:
- context:
    cluster: default
    user: default
  name: default
current-context: default
users:
- name: default
  user:
    username: config
`

// TestHollowNode 是一个简单粗暴的测试：尝试启动 hollow node，并检查其没有崩溃。
// 这类测试足以发现诸如 pkg/kubemark/hollow_kubelet.go 中遗漏 kubelet 依赖的问题。
func TestHollowNode(t *testing.T) {
	// 临时目录
	tmpDir, err := os.MkdirTemp("", "hollow-node")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	// HTTPS 服务器
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		w.WriteHeader(200)
		w.Write([]byte(`ok`))
	}))
	defer server.Close()

	kubeconfigPath := filepath.Join(tmpDir, "config.kubeconfig")
	if err := os.WriteFile(kubeconfigPath, []byte(fmt.Sprintf(fakeKubeconfig, server.URL)), os.FileMode(0600)); err != nil {
		t.Fatal(err)
	}

	for morph := range knownMorphs {
		t.Run(morph, func(t *testing.T) {
			s := &hollowNodeConfig{
				KubeconfigPath: kubeconfigPath,
				Morph:          morph,
			}
			errCh := make(chan error)
			go func() {
				data, err := os.ReadFile(kubeconfigPath)
				t.Logf("read %d, err=%v\n", len(data), err)
				ctx := ktesting.Init(t)
				errCh <- run(ctx, s)
			}()

			select {
			case err := <-errCh:
				t.Fatalf("Run finished unexpectedly with error: %v", err)
			case <-time.After(3 * time.Second):
				t.Logf("Morph %q hasn't crashed for 3s. Calling success.", morph)
			}
		})
	}
}
