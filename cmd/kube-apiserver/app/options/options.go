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

// Package options 定义初始化 API 服务器所需的命令行参数和配置。
package options

import (
	"net"
	"strings"
	"time"

	v1 "k8s.io/api/core/v1"
	utilnet "k8s.io/apimachinery/pkg/util/net"
	"k8s.io/client-go/tools/cache"
	cliflag "k8s.io/component-base/cli/flag"

	api "k8s.io/kubernetes/pkg/apis/core"
	"k8s.io/kubernetes/pkg/cluster/ports"
	controlplaneapiserver "k8s.io/kubernetes/pkg/controlplane/apiserver/options"
	"k8s.io/kubernetes/pkg/controlplane/reconcilers"
	_ "k8s.io/kubernetes/pkg/features" // 注册 Kubernetes 特性开关
	kubeoptions "k8s.io/kubernetes/pkg/kubeapiserver/options"
	kubeletclient "k8s.io/kubernetes/pkg/kubelet/client"
)

// ServerRunOptions 保存运行 Kubernetes API 服务器所需的参数。
type ServerRunOptions struct {
	*controlplaneapiserver.Options // 嵌入以减少现有调用方的配置噪音

	Extra
}

// Extra 保存 API 服务器特有的配置项。
type Extra struct {
	// AllowPrivileged 控制是否允许特权容器。
	AllowPrivileged bool
	// KubeletConfig 保存访问 kubelet 的连接参数。
	KubeletConfig kubeletclient.KubeletClientConfig
	// KubernetesServiceNodePort 指定 Kubernetes 默认 Service 的 NodePort；零值表示使用 ClusterIP。
	KubernetesServiceNodePort int
	// ServiceClusterIPRanges 保存用户输入的服务网段列表。
	ServiceClusterIPRanges string
	// PrimaryServiceClusterIPRange 和 SecondaryServiceClusterIPRange 是解析用户输入后得到的主、备网段。
	PrimaryServiceClusterIPRange net.IPNet
	// SecondaryServiceClusterIPRange 仅在双栈部署中提供第二地址族，不能与主网段属于同一地址族。
	SecondaryServiceClusterIPRange net.IPNet
	// APIServerServiceIP 是主服务网段中的首个有效 IP。
	APIServerServiceIP net.IP

	// ServiceNodePortRange 是预留给 NodePort Service 的端口范围。
	ServiceNodePortRange utilnet.PortRange

	// EndpointReconcilerType 选择默认 Service Endpoint 的协调器实现。
	EndpointReconcilerType string

	// MasterCount 是旧版端点协调器使用的 API 服务器数量。
	MasterCount int

	// InformerName 标识共享 Informer 的来源。
	InformerName *cache.InformerName
}

// NewServerRunOptions 创建带有默认值的 API 服务器启动参数。
func NewServerRunOptions() *ServerRunOptions {
	s := ServerRunOptions{
		Options: controlplaneapiserver.NewOptions(),

		Extra: Extra{
			EndpointReconcilerType: string(reconcilers.LeaseEndpointReconcilerType),
			KubeletConfig: kubeletclient.KubeletClientConfig{
				Port:         ports.KubeletPort,
				ReadOnlyPort: ports.KubeletReadOnlyPort,
				PreferredAddressTypes: []string{
					// --override-hostname
					string(api.NodeHostName),

					// 内部地址优先使用 DNS，其次使用 IP。
					string(api.NodeInternalDNS),
					string(api.NodeInternalIP),

					// 外部地址优先使用 DNS，其次使用 IP。
					string(api.NodeExternalDNS),
					string(api.NodeExternalIP),
				},
				HTTPTimeout: time.Duration(5) * time.Second,
			},
			ServiceNodePortRange: kubeoptions.DefaultServiceNodePortRange,
			MasterCount:          1,
		},
	}

	s.Options.SystemNamespaces = append(s.Options.SystemNamespaces, v1.NamespaceNodeLease)

	return &s
}

// Flags 按分组名称注册 API 服务器特有的命令行参数。
func (s *ServerRunOptions) Flags() (fss cliflag.NamedFlagSets) {
	s.Options.AddFlags(&fss)

	// 下面的 ""+ 用于让 gofmt 保持帮助文本块的拼接格式。
	fs := fss.FlagSet("misc")

	fs.BoolVar(&s.AllowPrivileged, "allow-privileged", s.AllowPrivileged,
		"If true, allow privileged containers. [default=false]")

	fs.StringVar(&s.EndpointReconcilerType, "endpoint-reconciler-type", s.EndpointReconcilerType,
		"Use an endpoint reconciler ("+strings.Join(reconcilers.AllTypes.Names(), ", ")+") master-count is deprecated, and will be removed in a future version.")

	// 测试方法见 #14282。
	// TODO: 在 CI 中覆盖该参数后删除本说明。
	fs.IntVar(&s.KubernetesServiceNodePort, "kubernetes-service-node-port", s.KubernetesServiceNodePort, ""+
		"If non-zero, the Kubernetes master service (which apiserver creates/maintains) will be "+
		"of type NodePort, using this as the value of the port. If zero, the Kubernetes master "+
		"service will be of type ClusterIP.")

	fs.StringVar(&s.ServiceClusterIPRanges, "service-cluster-ip-range", s.ServiceClusterIPRanges, ""+
		"A CIDR notation IP range from which to assign service cluster IPs. This must not "+
		"overlap with any IP ranges assigned to nodes or pods. Max of two dual-stack CIDRs is allowed.")

	fs.Var(&s.ServiceNodePortRange, "service-node-port-range", ""+
		"A port range to reserve for services with NodePort visibility.  This must not overlap with the ephemeral port range on nodes.  "+
		"Example: '30000-32767'. Inclusive at both ends of the range.")

	// kubelet 相关参数：
	fs.StringSliceVar(&s.KubeletConfig.PreferredAddressTypes, "kubelet-preferred-address-types", s.KubeletConfig.PreferredAddressTypes,
		"List of the preferred NodeAddressTypes to use for kubelet connections.")

	fs.UintVar(&s.KubeletConfig.Port, "kubelet-port", s.KubeletConfig.Port,
		"DEPRECATED: kubelet port.")
	fs.MarkDeprecated("kubelet-port", "kubelet-port is deprecated and will be removed.")

	fs.UintVar(&s.KubeletConfig.ReadOnlyPort, "kubelet-read-only-port", s.KubeletConfig.ReadOnlyPort,
		"DEPRECATED: kubelet read only port.")
	fs.MarkDeprecated("kubelet-read-only-port", "kubelet-read-only-port is deprecated and will be removed.")

	fs.DurationVar(&s.KubeletConfig.HTTPTimeout, "kubelet-timeout", s.KubeletConfig.HTTPTimeout,
		"Timeout for kubelet operations.")

	fs.StringVar(&s.KubeletConfig.TLSClientConfig.CertFile, "kubelet-client-certificate", s.KubeletConfig.TLSClientConfig.CertFile,
		"Path to a client cert file for TLS.")

	fs.StringVar(&s.KubeletConfig.TLSClientConfig.KeyFile, "kubelet-client-key", s.KubeletConfig.TLSClientConfig.KeyFile,
		"Path to a client key file for TLS.")

	fs.StringVar(&s.KubeletConfig.TLSClientConfig.CAFile, "kubelet-certificate-authority", s.KubeletConfig.TLSClientConfig.CAFile,
		"Path to a cert file for the certificate authority.")

	fs.IntVar(&s.MasterCount, "apiserver-count", s.MasterCount,
		"The number of apiservers running in the cluster, must be a positive number. (In use when --endpoint-reconciler-type=master-count is enabled.)")
	fs.MarkDeprecated("apiserver-count", "apiserver-count is deprecated and will be removed in a future version.")

	return fss
}
