/*
Copyright 2025 The Kubernetes Authors.

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

package apiserver

import (
	"context"
	"strings"
	"testing"

	kubernetes "k8s.io/client-go/kubernetes"
	"k8s.io/kubernetes/test/utils/ktesting"
)

// TestHealthHandler 测试带有 etcd 覆盖服务器的 /health?verbose。
func TestHealthHandler(t *testing.T) {
	tCtx := ktesting.Init(t)
	// 搭建使用 `--etcd-servers-overrides` 的多 etcd 服务器
	c, closeFn := multiEtcdSetup(tCtx, t)
	defer closeFn()

	// 测试 /healthz
	raw := readinessCheck(t, c, "/healthz", "")
	// 断言 raw 包含 `[+]etcd-override-0 ok`
	if !strings.Contains(string(raw), "[+]etcd-override-0 ok") {
		t.Errorf("Health check result should contain etcd-override-0 ok. Raw: %v", string(raw))
	}

	// 测试 /healthz?exclude=etcd 的分组排除
	raw = readinessCheck(t, c, "/healthz", "etcd")
	// 断言 raw 不包含 `[+]etcd-override-0 ok`
	if strings.Contains(string(raw), "[+]etcd-override-0 ok") {
		t.Errorf("Health check result should not contain etcd-override-0 ok. Raw: %v", string(raw))
	}
	if strings.Contains(string(raw), "[+]etcd ok") {
		t.Errorf("Health check result should not contain etcd ok. Raw: %v", string(raw))
	}

	// 测试 /healthz?exclude=etcd-override-0 的分组排除
	raw = readinessCheck(t, c, "/healthz", "etcd-override-0")
	if strings.Contains(string(raw), "[+]etcd-override-0 ok") {
		t.Errorf("Health check result should not contain etcd-override-0 ok. Raw: %v", string(raw))
	}
	if !strings.Contains(string(raw), "[+]etcd ok") {
		t.Errorf("Health check result should contain etcd ok. Raw: %v", string(raw))
	}
}

func TestHealthHandlerLoopbackServingCertificate(t *testing.T) {
	_, c, _, teardownFn := setup(t)
	defer teardownFn()

	paths := []string{
		"/healthz",
		"/livez",
	}

	for _, path := range paths {
		raw := readinessCheck(t, c, path, "")
		if !strings.Contains(string(raw), "[+]loopback-serving-certificate ok") {
			t.Errorf("%s result should contain loopback-serving-certificate ok. Raw: %v", path, string(raw))
		}
	}

	raw := readinessCheck(t, c, "/readyz", "")
	if strings.Contains(string(raw), "[+]loopback-serving-certificate ok") {
		t.Errorf("/readyz result should not contain loopback-serving-certificate ok. Raw: %v", string(raw))
	}
}

func readinessCheck(t *testing.T, c kubernetes.Interface, path string, exclude string) []byte {
	var statusCode int
	req := c.CoreV1().RESTClient().Get().AbsPath(path)
	req.Param("verbose", "true")
	if exclude != "" {
		req.Param("exclude", exclude)
	}
	result := req.Do(context.TODO())
	result.StatusCode(&statusCode)
	if statusCode == 200 {
		t.Logf("Health check passed")
	} else {
		t.Errorf("Health check failed with status code: %d", statusCode)
	}
	raw, err := result.Raw()
	if err != nil {
		t.Errorf("Failed to get health check result: %v", err)
	}
	t.Logf("Health check result: %v", string(raw))

	return raw
}
