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

package exec_test // separate package to prevent circular import

import (
	"context"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	utilnet "k8s.io/apimachinery/pkg/util/net"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"
)

// TestExecTLSCache asserts the semantics of the TLS cache when exec auth is used.
//
// In particular, when:
//   - multiple identical rest configs exist as distinct objects, and
//   - these rest configs use exec auth, and
//   - these rest configs are used to create distinct clientsets, then
//
// the underlying TLS config is shared between those clientsets.
//
// 缓存键由 exec 插件命令与参数等配置内容派生，而非 *rest.Config 指针：
// 因此配置内容相同的客户端复用同一个 TLS 配置（共享证书与刷新状态），
// 参数不同的客户端则必须各自独立，避免把 A 的凭据用于 B。
func TestExecTLSCache(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)

	config1 := &rest.Config{
		Host: "https://localhost",
		ExecProvider: &clientcmdapi.ExecConfig{
			Command:         "./testdata/test-plugin.sh",
			APIVersion:      "client.authentication.k8s.io/v1",
			InteractiveMode: clientcmdapi.IfAvailableExecInteractiveMode,
		},
	}
	client1 := clientset.NewForConfigOrDie(config1)

	// config2 与 config1 内容完全相同，但指针不同；预期共享 TLS 配置。
	config2 := &rest.Config{
		Host: "https://localhost",
		ExecProvider: &clientcmdapi.ExecConfig{
			Command:         "./testdata/test-plugin.sh",
			APIVersion:      "client.authentication.k8s.io/v1",
			InteractiveMode: clientcmdapi.IfAvailableExecInteractiveMode,
		},
	}
	client2 := clientset.NewForConfigOrDie(config2)

	// config3 的 Args 不同，缓存键应不同，不得复用 config1/2 的 TLS 配置。
	config3 := &rest.Config{
		Host: "https://localhost",
		ExecProvider: &clientcmdapi.ExecConfig{
			Command:         "./testdata/test-plugin.sh",
			Args:            []string{"make this exec auth different"},
			APIVersion:      "client.authentication.k8s.io/v1",
			InteractiveMode: clientcmdapi.IfAvailableExecInteractiveMode,
		},
	}
	client3 := clientset.NewForConfigOrDie(config3)

	// 分别发起一次请求，迫使各客户端完成 TLS 配置的惰性构建。
	_, _ = client1.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
	_, _ = client2.CoreV1().Namespaces().List(ctx, metav1.ListOptions{})
	_, _ = client3.CoreV1().PersistentVolumes().List(ctx, metav1.ListOptions{})

	rt1 := client1.RESTClient().(*rest.RESTClient).Client.Transport
	rt2 := client2.RESTClient().(*rest.RESTClient).Client.Transport
	rt3 := client3.RESTClient().(*rest.RESTClient).Client.Transport

	tlsConfig1, err := utilnet.TLSClientConfig(rt1)
	if err != nil {
		t.Fatal(err)
	}
	tlsConfig2, err := utilnet.TLSClientConfig(rt2)
	if err != nil {
		t.Fatal(err)
	}
	tlsConfig3, err := utilnet.TLSClientConfig(rt3)
	if err != nil {
		t.Fatal(err)
	}

	if tlsConfig1 == nil || tlsConfig2 == nil || tlsConfig3 == nil {
		t.Fatal("expected non-nil TLS configs")
	}

	if tlsConfig1 != tlsConfig2 {
		t.Fatal("expected the same TLS config for matching exec config via rest config")
	}

	// 指针比较即可：缓存命中时返回的是同一个 *tls.Config 实例。
	if tlsConfig1 == tlsConfig3 {
		t.Fatal("expected different TLS config for non-matching exec config via rest config")
	}
}
