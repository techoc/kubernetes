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
	"context"
	"fmt"
	"net"
	"strings"

	apiserveroptions "k8s.io/apiserver/pkg/server/options"
	_ "k8s.io/component-base/metrics/prometheus/workqueue"
	netutils "k8s.io/utils/net"

	cp "k8s.io/kubernetes/pkg/controlplane/apiserver/options"
	"k8s.io/kubernetes/pkg/kubeapiserver"
)

// completedOptions 通过私有封装确保运行前必须先调用 Complete。
type completedOptions struct {
	cp.CompletedOptions

	Extra
}

// CompletedOptions 是已完成默认值填充、可交给运行阶段使用的启动参数。
type CompletedOptions struct {
	// 嵌入私有指针，避免包外直接构造完成态。
	*completedOptions
}

// Complete 为 ServerRunOptions 填充默认值，应在 kube-apiserver 参数解析完成后调用。
func (s *ServerRunOptions) Complete(ctx context.Context) (CompletedOptions, error) {
	if s == nil {
		return CompletedOptions{completedOptions: &completedOptions{}}, nil
	}

	// 将用户输入的服务网段拆为主、备网段；未提供备网段时保持零值。
	apiServerServiceIP, primaryServiceIPRange, secondaryServiceIPRange, err := getServiceIPAndRanges(s.ServiceClusterIPRanges)
	if err != nil {
		return CompletedOptions{}, err
	}
	controlplane, err := s.Options.Complete(ctx, []string{"kubernetes.default.svc", "kubernetes.default", "kubernetes"}, []net.IP{apiServerServiceIP})
	if err != nil {
		return CompletedOptions{}, err
	}

	completed := completedOptions{
		CompletedOptions: controlplane,

		Extra: s.Extra,
	}

	completed.PrimaryServiceClusterIPRange = primaryServiceIPRange
	completed.SecondaryServiceClusterIPRange = secondaryServiceIPRange
	completed.APIServerServiceIP = apiServerServiceIP

	if completed.Etcd != nil && completed.Etcd.EnableWatchCache {
		sizes := kubeapiserver.DefaultWatchCacheSizes()
		// 先解析用户覆盖值，再与默认缓存大小合并，避免格式错误被静默忽略。
		userSpecified, err := apiserveroptions.ParseWatchCacheSizes(completed.Etcd.WatchCacheSizes)
		if err != nil {
			return CompletedOptions{}, err
		}
		for resource, size := range userSpecified {
			sizes[resource] = size
		}
		completed.Etcd.WatchCacheSizes, err = apiserveroptions.WriteWatchCacheSizes(sizes)
		if err != nil {
			return CompletedOptions{}, err
		}
	}

	return CompletedOptions{
		completedOptions: &completed,
	}, nil
}

func getServiceIPAndRanges(serviceClusterIPRanges string) (net.IP, net.IPNet, net.IPNet, error) {
	serviceClusterIPRangeList := []string{}
	if serviceClusterIPRanges != "" {
		serviceClusterIPRangeList = strings.Split(serviceClusterIPRanges, ",")
	}

	var apiServerServiceIP net.IP
	var primaryServiceIPRange net.IPNet
	var secondaryServiceIPRange net.IPNet
	var err error
	// 用户未指定时使用默认主网段，不生成备网段。
	if len(serviceClusterIPRangeList) == 0 {
		var primaryServiceClusterCIDR net.IPNet
		primaryServiceIPRange, apiServerServiceIP, err = cp.ServiceIPRange(primaryServiceClusterCIDR)
		if err != nil {
			return net.IP{}, net.IPNet{}, net.IPNet{}, fmt.Errorf("error determining service IP ranges: %v", err)
		}
		return apiServerServiceIP, primaryServiceIPRange, net.IPNet{}, nil
	}

	_, primaryServiceClusterCIDR, err := netutils.ParseCIDRSloppy(serviceClusterIPRangeList[0])
	if err != nil {
		return net.IP{}, net.IPNet{}, net.IPNet{}, fmt.Errorf("service-cluster-ip-range[0] is not a valid cidr")
	}

	primaryServiceIPRange, apiServerServiceIP, err = cp.ServiceIPRange(*primaryServiceClusterCIDR)
	if err != nil {
		return net.IP{}, net.IPNet{}, net.IPNet{}, fmt.Errorf("error determining service IP ranges for primary service cidr: %v", err)
	}

	// 仅在用户提供第二项时解析备网段；校验阶段限制最多两个双栈网段。
	if len(serviceClusterIPRangeList) > 1 {
		_, secondaryServiceClusterCIDR, err := netutils.ParseCIDRSloppy(serviceClusterIPRangeList[1])
		if err != nil {
			return net.IP{}, net.IPNet{}, net.IPNet{}, fmt.Errorf("service-cluster-ip-range[1] is not an ip net")
		}
		secondaryServiceIPRange = *secondaryServiceClusterCIDR
	}
	return apiServerServiceIP, primaryServiceIPRange, secondaryServiceIPRange, nil
}
