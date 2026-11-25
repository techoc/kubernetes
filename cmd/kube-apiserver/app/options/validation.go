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

package options

import (
	"errors"
	"fmt"
	"net"
	"strings"

	genericoptions "k8s.io/apiserver/pkg/server/options"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	netutils "k8s.io/utils/net"

	controlplaneapiserver "k8s.io/kubernetes/pkg/controlplane/apiserver/options"
	"k8s.io/kubernetes/pkg/controlplane/reconcilers"
	"k8s.io/kubernetes/pkg/features"
)

// TODO: 长远来看应从配置存储读取，而不是依赖命令行参数。
// validateClusterIPFlags 必须在 Complete 之后调用。
func validateClusterIPFlags(options Extra) []error {
	var errs []error
	// 限制集群服务 IP 网段的主机位数量。
	maxCIDRBits := 20

	// 主网段必须已由用户输入或默认值填充。
	if options.PrimaryServiceClusterIPRange.IP == nil {
		errs = append(errs, errors.New("--service-cluster-ip-range must contain at least one valid cidr"))
	}

	serviceClusterIPRangeList := strings.Split(options.ServiceClusterIPRanges, ",")
	if len(serviceClusterIPRangeList) > 2 {
		errs = append(errs, errors.New("--service-cluster-ip-range must not contain more than two entries"))
	}

	// Complete 应已填充 Primary* 和 Secondary 字段。
	if !utilfeature.DefaultFeatureGate.Enabled(features.MultiCIDRServiceAllocator) ||
		!utilfeature.DefaultFeatureGate.Enabled(features.DisableAllocatorDualWrite) {
		// 校验主网段。
		if err := validateMaxCIDRRange(options.PrimaryServiceClusterIPRange, maxCIDRBits, "--service-cluster-ip-range"); err != nil {
			errs = append(errs, err)
		}
	}

	secondaryServiceClusterIPRangeUsed := (options.SecondaryServiceClusterIPRange.IP != nil)
	// 即使 Pod 使用双栈，集群入口仍可只使用 --service-cluster-ip-range 指定的一种协议族。
	// 若用户配置双栈 Service，备网段必须与主网段属于不同协议族。
	if secondaryServiceClusterIPRangeUsed {
		// 双栈要求 IPFamily(PrimaryServiceClusterIPRange) != IPFamily(SecondaryServiceClusterIPRange)。
		dualstack, err := netutils.IsDualStackCIDRs([]*net.IPNet{&options.PrimaryServiceClusterIPRange, &options.SecondaryServiceClusterIPRange})
		if err != nil {
			errs = append(errs, fmt.Errorf("error attempting to validate dualstack for --service-cluster-ip-range value error:%v", err))
		}

		if !dualstack {
			errs = append(errs, errors.New("--service-cluster-ip-range[0] and --service-cluster-ip-range[1] must be of different IP family"))
		}
		if !utilfeature.DefaultFeatureGate.Enabled(features.MultiCIDRServiceAllocator) ||
			!utilfeature.DefaultFeatureGate.Enabled(features.DisableAllocatorDualWrite) {
			if err := validateMaxCIDRRange(options.SecondaryServiceClusterIPRange, maxCIDRBits, "--service-cluster-ip-range[1]"); err != nil {
				errs = append(errs, err)
			}
		}
	}

	return errs
}

func validateMaxCIDRRange(cidr net.IPNet, maxCIDRBits int, cidrFlag string) error {
	// 网段会持久化到 etcd；过大尤其是 IPv6 默认网段会显著增加快照耗时。
	var ones, bits = cidr.Mask.Size()
	if bits-ones > maxCIDRBits {
		return fmt.Errorf("specified %s is too large; for %d-bit addresses, the mask must be >= %d", cidrFlag, bits, bits-maxCIDRBits)
	}

	return nil
}

func validateServiceNodePort(options Extra) []error {
	var errs []error

	if options.KubernetesServiceNodePort < 0 || options.KubernetesServiceNodePort > 65535 {
		errs = append(errs, fmt.Errorf("--kubernetes-service-node-port %v must be between 0 and 65535, inclusive. If 0, the Kubernetes master service will be of type ClusterIP", options.KubernetesServiceNodePort))
	}

	if options.KubernetesServiceNodePort > 0 && !options.ServiceNodePortRange.Contains(options.KubernetesServiceNodePort) {
		errs = append(errs, fmt.Errorf("kubernetes service node port range %v doesn't contain %v", options.ServiceNodePortRange, options.KubernetesServiceNodePort))
	}
	return errs
}

func validatePublicIPServiceClusterIPRangeIPFamilies(extra Extra, generic genericoptions.ServerRunOptions) []error {
	// "kubernetes.default" Service 按配置的 ServiceIPRange 使用单栈。
	// 引导控制器协调该 Service 与 Endpoints 时，必须保证 ClusterIPRepairConfig 和
	// Endpoints 使用相同协议族，否则客户端会因协议族不匹配而无法访问。
	// TODO: 双栈支持重新评估 https://github.com/kubernetes/enhancements/issues/2438
	if reconcilers.Type(extra.EndpointReconcilerType) != reconcilers.NoneEndpointReconcilerType {
		serviceIPRange, _, err := controlplaneapiserver.ServiceIPRange(extra.PrimaryServiceClusterIPRange)
		if err != nil {
			return []error{fmt.Errorf("error determining service IP ranges: %w", err)}
		}
		if netutils.IsIPv4CIDR(&serviceIPRange) != netutils.IsIPv4(generic.AdvertiseAddress) {
			return []error{fmt.Errorf("service IP family %q must match public address family %q", extra.PrimaryServiceClusterIPRange.String(), generic.AdvertiseAddress.String())}
		}
	}

	return nil
}

// Validate 校验已完成的启动参数，并返回全部错误。
func (s CompletedOptions) Validate() []error {
	var errs []error

	errs = append(errs, s.CompletedOptions.Validate()...)
	errs = append(errs, validateClusterIPFlags(s.Extra)...)
	errs = append(errs, validateServiceNodePort(s.Extra)...)
	errs = append(errs, validatePublicIPServiceClusterIPRangeIPFamilies(s.Extra, *s.GenericServerRunOptions)...)

	if s.MasterCount <= 0 {
		errs = append(errs, fmt.Errorf("--apiserver-count should be a positive number, but value '%d' provided", s.MasterCount))
	}

	return errs
}
