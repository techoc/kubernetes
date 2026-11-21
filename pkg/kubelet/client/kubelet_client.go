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
	"fmt"
	"net/http"
	"strconv"
	"time"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apiserver/pkg/server/egressselector"
	"k8s.io/client-go/transport"
	nodeutil "k8s.io/kubernetes/pkg/util/node"
)

// KubeletClientConfig 定义 kubelet 客户端的配置参数。
type KubeletClientConfig struct {
	// Port 指定默认端口：当 Node.NodeStatus.DaemonEndpoints 中没有 kubelet 端口信息时使用。
	Port uint

	// ReadOnlyPort 指定只读通信使用的端口。
	ReadOnlyPort uint

	// PreferredAddressTypes 用于从 Node.NodeStatus.Addresses 中选择地址。
	PreferredAddressTypes []string

	// TLSClientConfig 包含启用传输层安全所需的配置。
	TLSClientConfig KubeletTLSConfig

	// HTTPTimeout 是客户端访问 kubelet 的 HTTP 请求超时时间。
	HTTPTimeout time.Duration

	// 配置了 egress selector 时，Lookup 会返回对应的拨号器。
	Lookup egressselector.Lookup
}

// KubeletTLSConfig 保存访问 kubelet 所需的 TLS 客户端配置。
type KubeletTLSConfig struct {
	// CertFile 是客户端证书文件，服务端要求 TLS 客户端认证时使用。
	CertFile string
	// KeyFile 是客户端私钥文件，与 CertFile 配对使用。
	KeyFile string
	// CAFile 是用于校验服务端的受信任根证书。
	CAFile string
}

// ConnectionInfo 提供连接 kubelet 所需的信息。
type ConnectionInfo struct {
	Scheme                         string
	Hostname                       string
	Port                           string
	Transport                      http.RoundTripper
	InsecureSkipTLSVerifyTransport http.RoundTripper
	NodeFeatures                   []string
}

// ConnectionInfoGetter 为运行在指定节点上的 kubelet 提供 ConnectionInfo。
type ConnectionInfoGetter interface {
	GetConnectionInfo(ctx context.Context, nodeName types.NodeName) (*ConnectionInfo, error)
}

// MakeTransport 创建安全的 HTTP RoundTripper。
func MakeTransport(config *KubeletClientConfig) (http.RoundTripper, error) {
	return makeTransport(config, false)
}

// MakeInsecureTransport 创建跳过 TLS 校验的 HTTP RoundTripper。
func MakeInsecureTransport(config *KubeletClientConfig) (http.RoundTripper, error) {
	return makeTransport(config, true)
}

// makeTransport 创建用于 HTTP 传输的 RoundTripper。
func makeTransport(config *KubeletClientConfig, insecureSkipTLSVerify bool) (http.RoundTripper, error) {
	// 在获取可能被缓存的连接之前，先对 transport 配置设置 insecureSkipTLSVerify；
	// transportConfig 每次都会返回新的结构体指针。
	transportConfig := config.transportConfig()
	if insecureSkipTLSVerify {
		// 忽略证书校验时清空 CA 文件，避免同时存在两种校验配置。
		transportConfig.TLS.Insecure = true
		transportConfig.TLS.CAFile = "" // we are only using files so we can ignore CAData
	}

	// 配置了 egress selector 时通过它获取拨号器（如 Konnectivity）。
	if config.Lookup != nil {
		// 假定使用 EgressSelector（而非 SSHTunnel）。
		// egress selector 未启用时不会得到拨号器。
		networkContext := egressselector.Cluster.AsNetworkContext()
		dialer, err := config.Lookup(networkContext)
		if err != nil {
			return nil, fmt.Errorf("failed to get context dialer for 'cluster': got %v", err)
		}
		if dialer != nil {
			transportConfig.DialHolder = &transport.DialHolder{Dial: dialer}
		}
	}
	return transport.New(transportConfig)
}

// transportConfig 把客户端配置转换为合适的 transport 配置。
func (c *KubeletClientConfig) transportConfig() *transport.Config {
	cfg := &transport.Config{
		TLS: transport.TLSConfig{
			CAFile:   c.TLSClientConfig.CAFile,
			CertFile: c.TLSClientConfig.CertFile,
			KeyFile:  c.TLSClientConfig.KeyFile,
			// transport.loadTLSFiles 会因为只使用文件而把它设为 true；
			// 这里显式设置更清晰，也提醒读者存在这一行为。
			ReloadTLSFiles: true,
		},
	}
	// 没有配置 CA 时只能跳过服务端证书校验。
	if !cfg.HasCA() {
		cfg.TLS.Insecure = true
	}
	return cfg
}

// NodeGetter 定义按名称查询 Node 的接口。
type NodeGetter interface {
	Get(ctx context.Context, name string, options metav1.GetOptions) (*v1.Node, error)
}

// NodeGetterFunc 允许用函数实现 NodeGetter。
type NodeGetterFunc func(ctx context.Context, name string, options metav1.GetOptions) (*v1.Node, error)

// Get 通过 NodeGetterFunc 获取信息。
func (f NodeGetterFunc) Get(ctx context.Context, name string, options metav1.GetOptions) (*v1.Node, error) {
	return f(ctx, name, options)
}

// NodeConnectionInfoGetter 从 Node API 对象的状态中获取连接信息。
type NodeConnectionInfoGetter struct {
	// nodes 用于查询 Node 对象。
	nodes NodeGetter
	// scheme 是连接所有 kubelet 使用的协议。
	scheme string
	// defaultPort 是节点状态中未记录 kubelet 端口时使用的默认端口。
	defaultPort int
	// transport 是向所有 kubelet 发送请求时使用的传输层。
	transport http.RoundTripper
	// insecureSkipTLSVerifyTransport 是 kube-apiserver 需要跳过校验 kubelet TLS 证书时使用的传输层。
	insecureSkipTLSVerifyTransport http.RoundTripper
	// preferredAddressTypes 指定查找节点地址时使用的优先顺序。
	preferredAddressTypes []v1.NodeAddressType
}

// NewNodeConnectionInfoGetter 创建新的 NodeConnectionInfoGetter。
func NewNodeConnectionInfoGetter(nodes NodeGetter, config KubeletClientConfig) (ConnectionInfoGetter, error) {
	transport, err := MakeTransport(&config)
	if err != nil {
		return nil, err
	}
	// 同时创建安全与跳过校验两种 transport，供调用方按需选择。
	insecureSkipTLSVerifyTransport, err := MakeInsecureTransport(&config)
	if err != nil {
		return nil, err
	}

	// 把字符串形式的地址类型转换为 API 类型。
	types := []v1.NodeAddressType{}
	for _, t := range config.PreferredAddressTypes {
		types = append(types, v1.NodeAddressType(t))
	}

	return &NodeConnectionInfoGetter{
		nodes:                          nodes,
		scheme:                         "https",
		defaultPort:                    int(config.Port),
		transport:                      transport,
		insecureSkipTLSVerifyTransport: insecureSkipTLSVerifyTransport,

		preferredAddressTypes: types,
	}, nil
}

// GetConnectionInfo 从 Node API 对象的状态中获取连接信息。
func (k *NodeConnectionInfoGetter) GetConnectionInfo(ctx context.Context, nodeName types.NodeName) (*ConnectionInfo, error) {
	node, err := k.nodes.Get(ctx, string(nodeName), metav1.GetOptions{})
	if err != nil {
		return nil, err
	}

	// 按首选地址类型查找 kubelet 上报的地址。
	host, err := nodeutil.GetPreferredNodeAddress(node, k.preferredAddressTypes)
	if err != nil {
		return nil, err
	}

	// 优先使用 kubelet 上报的端口；未上报时回退到默认端口。
	port := int(node.Status.DaemonEndpoints.KubeletEndpoint.Port)
	if port <= 0 {
		port = k.defaultPort
	}

	return &ConnectionInfo{
		Scheme:                         k.scheme,
		Hostname:                       host,
		Port:                           strconv.Itoa(port),
		Transport:                      k.transport,
		InsecureSkipTLSVerifyTransport: k.insecureSkipTLSVerifyTransport,
		NodeFeatures:                   node.Status.DeclaredFeatures,
	}, nil
}
