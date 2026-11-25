/*
Copyright 2014 The Kubernetes Authors.

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

package client

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"slices"
	"strconv"
	"testing"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	utilnet "k8s.io/apimachinery/pkg/util/net"
	"k8s.io/apiserver/pkg/server/egressselector"
	"k8s.io/kubernetes/test/utils/client-go/ktesting"
)

// kubeletTestCertHelper 构造用于测试的 KubeletTLSConfig。
// valid 为 true 时使用有效证书，否则使用无效证书，用于覆盖不同的证书校验路径。
func kubeletTestCertHelper(valid bool) KubeletTLSConfig {
	if valid {
		return KubeletTLSConfig{
			CertFile: "testdata/mycertvalid.cer",
			KeyFile:  "testdata/mycertvalid.key",
			CAFile:   "testdata/myCA.cer",
		}
	}
	return KubeletTLSConfig{
		CertFile: "testdata/mycertinvalid.cer",
		KeyFile:  "testdata/mycertinvalid.key",
		CAFile:   "testdata/myCA.cer",
	}
}

// kubeletTestRoundTripHelper 通过给定的 RoundTripper 向 addr 发起一次 GET 请求，
// 校验请求成功且响应状态码为 200，否则令测试失败。
func kubeletTestRoundTripHelper(t *testing.T, rt http.RoundTripper, addr string) {
	req, err := http.NewRequest(http.MethodGet, addr, nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := rt.RoundTrip(req)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK {
		dump, err := httputil.DumpResponse(resp, true)
		if err != nil {
			t.Fatal(err)
		}
		t.Fatal(string(dump))
	}
}

// TestMakeTransportInvalid 验证提供无效证书路径时 MakeTransport 返回错误，
// 且不返回可用的 RoundTripper。
func TestMakeTransportInvalid(t *testing.T) {
	config := &KubeletClientConfig{
		// 无效的证书与密钥路径
		TLSClientConfig: kubeletTestCertHelper(false),
	}
	rt, err := MakeTransport(config)
	if err == nil {
		t.Errorf("Expected an error")
	}
	if rt != nil {
		t.Error("rt should be nil as we provided invalid cert file")
	}
}

// TestMakeTransportValid 验证提供有效配置时 MakeTransport 成功创建非空的 RoundTripper。
func TestMakeTransportValid(t *testing.T) {
	config := &KubeletClientConfig{
		Port:            1234,
		TLSClientConfig: kubeletTestCertHelper(true),
	}

	rt, err := MakeTransport(config)
	if err != nil {
		t.Errorf("Not expecting an error %#v", err)
	}
	if rt == nil {
		t.Error("rt should not be nil")
	}
}

// TestMakeTransportWithLookUp 覆盖 Lookup 闭包正常与报错两种情况，
// 验证 MakeInsecureTransport 在对应场景下分别成功或失败。
func TestMakeTransportWithLookUp(t *testing.T) {
	dialFunc := func(ctx context.Context, network, addr string) (net.Conn, error) {
		return net.Dial(network, addr)
	}
	tests := []struct {
		name        string
		config      *KubeletClientConfig
		expectError bool
	}{
		{
			"test makeTransport with Lookup closure initialized",
			&KubeletClientConfig{
				Port:            1234,
				TLSClientConfig: kubeletTestCertHelper(true),
				Lookup: func(_ egressselector.NetworkContext) (utilnet.DialFunc, error) {
					return dialFunc, nil
				},
			},
			false,
		},
		{
			"test makeTransport with Lookup closure returning error",
			&KubeletClientConfig{
				Port:            1234,
				TLSClientConfig: kubeletTestCertHelper(true),
				Lookup: func(_ egressselector.NetworkContext) (utilnet.DialFunc, error) {
					return nil, errors.New("mock error")
				},
			},
			true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			rt, err := MakeInsecureTransport(tt.config)
			if tt.expectError {
				if err == nil {
					t.Fatalf("expected error but got none: Lookup func is invalid")
				}
				return
			}
			if rt == nil {
				t.Fatalf("rt should not be nil")
			}
			// 启动 TLS 测试服务器，验证传输层能完成握手并返回 200。
			testServer := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				w.WriteHeader(http.StatusOK)
			}))
			defer testServer.Close()
			kubeletTestRoundTripHelper(t, rt, testServer.URL)
		})
	}
}

// TestMakeInsecureTransport 验证 MakeInsecureTransport 基于有效配置创建传输，
// 并可对测试服务器完成一次成功的 RoundTrip。
func TestMakeInsecureTransport(t *testing.T) {
	testServer := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer testServer.Close()

	testURL, err := url.Parse(testServer.URL)
	if err != nil {
		t.Fatal(err)
	}
	// 从测试服务器 URL 中提取端口，确保配置端口与测试服务器一致。
	_, portStr, err := net.SplitHostPort(testURL.Host)
	if err != nil {
		t.Fatal(err)
	}
	port, err := strconv.ParseUint(portStr, 10, 32)
	if err != nil {
		t.Fatal(err)
	}

	config := &KubeletClientConfig{
		Port:            uint(port),
		TLSClientConfig: kubeletTestCertHelper(true),
	}

	rt, err := MakeInsecureTransport(config)
	if err != nil {
		t.Errorf("Not expecting an error #%v", err)
	}
	if rt == nil {
		t.Error("rt should not be nil")
	}
	kubeletTestRoundTripHelper(t, rt, testServer.URL)
}

// mockNodeGetter 是测试用的 NodeGetter 模拟实现，返回预设的节点或错误。
type mockNodeGetter struct {
	node *v1.Node
	err  error
}

// Get 直接返回模拟实现中预设的节点与错误，不执行任何真实查询。
func (m *mockNodeGetter) Get(ctx context.Context, name string, options metav1.GetOptions) (*v1.Node, error) {
	return m.node, m.err
}

// TestNewNodeConnectionInfoGetter 验证 NewNodeConnectionInfoGetter 在有效配置下
// 能正确初始化 NodeConnectionInfoGetter，在证书无效时返回错误。
func TestNewNodeConnectionInfoGetter(t *testing.T) {
	tests := []struct {
		name        string
		nodes       NodeGetter
		config      KubeletClientConfig
		expectError bool
	}{
		{
			name:  "valid config",
			nodes: &mockNodeGetter{},
			config: KubeletClientConfig{
				Port:                  10250,
				PreferredAddressTypes: []string{"InternalIP", "ExternalIP"},
				TLSClientConfig:       kubeletTestCertHelper(true),
			},
			expectError: false,
		},
		{
			name:  "invalid cert file",
			nodes: &mockNodeGetter{},
			config: KubeletClientConfig{
				Port:                  10250,
				PreferredAddressTypes: []string{"InternalIP"},
				TLSClientConfig:       kubeletTestCertHelper(false),
			},
			expectError: true,
		},
		{
			name:  "empty preferred address types",
			nodes: &mockNodeGetter{},
			config: KubeletClientConfig{
				Port:                  10250,
				PreferredAddressTypes: []string{},
				TLSClientConfig:       kubeletTestCertHelper(true),
			},
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			getter, err := NewNodeConnectionInfoGetter(tt.nodes, tt.config)

			if tt.expectError {
				if err == nil {
					t.Fatalf("expected error but got none")
				}
				return
			}

			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if getter == nil {
				t.Fatalf("expected non-nil getter")
			}

			// 校验创建出的 getter 各属性是否符合预期
			ncig, ok := getter.(*NodeConnectionInfoGetter)
			if !ok {
				t.Fatalf("expected *NodeConnectionInfoGetter, got %T", getter)
			}

			if ncig.nodes != tt.nodes {
				t.Errorf("expected nodes to be set correctly")
			}

			if ncig.scheme != "https" {
				t.Errorf("expected scheme to be 'https', got %s", ncig.scheme)
			}

			if ncig.defaultPort != int(tt.config.Port) {
				t.Errorf("expected defaultPort to be %d, got %d", tt.config.Port, ncig.defaultPort)
			}

			if len(ncig.preferredAddressTypes) != len(tt.config.PreferredAddressTypes) {
				t.Errorf("expected %d preferred address types, got %d",
					len(tt.config.PreferredAddressTypes), len(ncig.preferredAddressTypes))
			}
		})
	}
}

// TestGetConnectionInfo 验证 GetConnectionInfo 能根据节点地址、端口与声明的特性
// 返回正确的连接信息，并覆盖节点不存在、无地址等错误场景。
func TestGetConnectionInfo(t *testing.T) {
	tests := []struct {
		name                 string
		nodeGetter           NodeGetter
		nodeName             types.NodeName
		expectedHost         string
		expectedPort         string
		expectedNodeFeatures []string
		expectError          bool
	}{
		{
			name: "valid node with kubelet endpoint port",
			nodeGetter: &mockNodeGetter{
				node: &v1.Node{
					ObjectMeta: metav1.ObjectMeta{Name: "test-node"},
					Status: v1.NodeStatus{
						Addresses: []v1.NodeAddress{
							{Type: v1.NodeInternalIP, Address: "192.168.1.10"},
							{Type: v1.NodeExternalIP, Address: "203.0.113.10"},
						},
						DaemonEndpoints: v1.NodeDaemonEndpoints{
							KubeletEndpoint: v1.DaemonEndpoint{Port: 10250},
						},
					},
				},
			},
			nodeName:             "test-node",
			expectedHost:         "203.0.113.10",
			expectedPort:         "10250",
			expectedNodeFeatures: nil,
			expectError:          false,
		},
		{
			name: "valid node without kubelet endpoint port (uses default)",
			nodeGetter: &mockNodeGetter{
				node: &v1.Node{
					ObjectMeta: metav1.ObjectMeta{Name: "test-node"},
					Status: v1.NodeStatus{
						Addresses: []v1.NodeAddress{
							{Type: v1.NodeInternalIP, Address: "192.168.1.10"},
						},
						DaemonEndpoints: v1.NodeDaemonEndpoints{
							KubeletEndpoint: v1.DaemonEndpoint{Port: 0},
						},
					},
				},
			},
			nodeName:             "test-node",
			expectedHost:         "192.168.1.10",
			expectedPort:         "10250", // 默认端口
			expectedNodeFeatures: nil,
			expectError:          false,
		},
		{
			name: "node with external IP preferred",
			nodeGetter: &mockNodeGetter{
				node: &v1.Node{
					ObjectMeta: metav1.ObjectMeta{Name: "test-node"},
					Status: v1.NodeStatus{
						Addresses: []v1.NodeAddress{
							{Type: v1.NodeInternalIP, Address: "192.168.1.10"},
							{Type: v1.NodeExternalIP, Address: "203.0.113.10"},
						},
						DaemonEndpoints: v1.NodeDaemonEndpoints{
							KubeletEndpoint: v1.DaemonEndpoint{Port: 10250},
						},
					},
				},
			},
			nodeName:             "test-node",
			expectedHost:         "203.0.113.10", // 可用时应优先选择 External IP
			expectedPort:         "10250",
			expectedNodeFeatures: nil,
			expectError:          false,
		},
		{
			name: "node not found",
			nodeGetter: &mockNodeGetter{
				err: errors.New("node not found"),
			},
			nodeName:    "nonexistent-node",
			expectError: true,
		},
		{
			name: "node with no addresses",
			nodeGetter: &mockNodeGetter{
				node: &v1.Node{
					ObjectMeta: metav1.ObjectMeta{Name: "test-node"},
					Status: v1.NodeStatus{
						Addresses: []v1.NodeAddress{},
						DaemonEndpoints: v1.NodeDaemonEndpoints{
							KubeletEndpoint: v1.DaemonEndpoint{Port: 10250},
						},
					},
				},
			},
			nodeName:    "test-node",
			expectError: true,
		},
		{
			name: "node with one declared feature",
			nodeGetter: &mockNodeGetter{
				node: &v1.Node{
					ObjectMeta: metav1.ObjectMeta{Name: "test-node"},
					Status: v1.NodeStatus{
						Addresses: []v1.NodeAddress{
							{Type: v1.NodeInternalIP, Address: "192.168.1.10"},
						},
						DaemonEndpoints: v1.NodeDaemonEndpoints{
							KubeletEndpoint: v1.DaemonEndpoint{Port: 10250},
						},
						DeclaredFeatures: []string{"ExtendWebSocketsToKubelet"},
					},
				},
			},
			nodeName:             "test-node",
			expectedHost:         "192.168.1.10",
			expectedPort:         "10250",
			expectedNodeFeatures: []string{"ExtendWebSocketsToKubelet"},
			expectError:          false,
		},
		{
			name: "node with multiple declared features",
			nodeGetter: &mockNodeGetter{
				node: &v1.Node{
					ObjectMeta: metav1.ObjectMeta{Name: "test-node"},
					Status: v1.NodeStatus{
						Addresses: []v1.NodeAddress{
							{Type: v1.NodeInternalIP, Address: "192.168.1.10"},
						},
						DaemonEndpoints: v1.NodeDaemonEndpoints{
							KubeletEndpoint: v1.DaemonEndpoint{Port: 10250},
						},
						DeclaredFeatures: []string{"ExtendWebSocketsToKubelet", "SomeOtherFeature"},
					},
				},
			},
			nodeName:             "test-node",
			expectedHost:         "192.168.1.10",
			expectedPort:         "10250",
			expectedNodeFeatures: []string{"ExtendWebSocketsToKubelet", "SomeOtherFeature"},
			expectError:          false,
		},
		{
			name: "node with empty declared features",
			nodeGetter: &mockNodeGetter{
				node: &v1.Node{
					ObjectMeta: metav1.ObjectMeta{Name: "test-node"},
					Status: v1.NodeStatus{
						Addresses: []v1.NodeAddress{
							{Type: v1.NodeInternalIP, Address: "192.168.1.10"},
						},
						DaemonEndpoints: v1.NodeDaemonEndpoints{
							KubeletEndpoint: v1.DaemonEndpoint{Port: 10250},
						},
						DeclaredFeatures: []string{},
					},
				},
			},
			nodeName:             "test-node",
			expectedHost:         "192.168.1.10",
			expectedPort:         "10250",
			expectedNodeFeatures: []string{},
			expectError:          false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			// 使用 ExternalIP 优先的配置，便于校验地址选择逻辑。
			config := KubeletClientConfig{
				Port:                  10250,
				PreferredAddressTypes: []string{"ExternalIP", "InternalIP"},
				TLSClientConfig:       kubeletTestCertHelper(true),
			}
			getter, err := NewNodeConnectionInfoGetter(tt.nodeGetter, config)
			if err != nil {
				t.Fatalf("failed to create NodeConnectionInfoGetter: %v", err)
			}

			tCtx := ktesting.Init(t)
			connInfo, err := getter.GetConnectionInfo(tCtx, tt.nodeName)

			if tt.expectError {
				if err == nil {
					t.Fatalf("expected error but got none")
				}
				return
			}

			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if connInfo == nil {
				t.Fatalf("expected non-nil connection info")
			}

			if connInfo.Scheme != "https" {
				t.Errorf("expected scheme 'https', got %s", connInfo.Scheme)
			}

			if connInfo.Hostname != tt.expectedHost {
				t.Errorf("expected hostname %s, got %s", tt.expectedHost, connInfo.Hostname)
			}

			if connInfo.Port != tt.expectedPort {
				t.Errorf("expected port %s, got %s", tt.expectedPort, connInfo.Port)
			}

			if connInfo.Transport == nil {
				t.Errorf("expected non-nil Transport")
			}

			if connInfo.InsecureSkipTLSVerifyTransport == nil {
				t.Errorf("expected non-nil InsecureSkipTLSVerifyTransport")
			}

			if !slices.Equal(connInfo.NodeFeatures, tt.expectedNodeFeatures) {
				t.Errorf("expected NodeFeatures %v, got %v", tt.expectedNodeFeatures, connInfo.NodeFeatures)
			}
		})
	}
}
