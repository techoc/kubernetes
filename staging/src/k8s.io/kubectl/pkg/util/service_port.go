/*
Copyright 2018 The Kubernetes Authors.

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

package util

import (
	"fmt"

	"k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
)

// 解析 Service 端口对应的容器命名端口；
// clusterIP=None 时忽略 targetPort。
// 命名端口无匹配时返回错误（-1），
// Service 未声明该端口时也返回错误（返回输入端口号）。
func LookupContainerPortNumberByServicePort(svc v1.Service, pod v1.Pod, port int32) (int32, error) {
	for _, svcportspec := range svc.Spec.Ports {
		if svcportspec.Port != port {
			continue
		}
		if svc.Spec.ClusterIP == v1.ClusterIPNone {
			return port, nil
		}
		if svcportspec.TargetPort.Type == intstr.Int {
			if svcportspec.TargetPort.IntValue() == 0 {
				// 未指定 targetPort 时 IntValue() 为零。
				return svcportspec.Port, nil
			}
			return int32(svcportspec.TargetPort.IntValue()), nil
		}
		return LookupContainerPortNumberByName(pod, svcportspec.TargetPort.String())
	}
	return port, fmt.Errorf("Service %s does not have a service port %d", svc.Name, port)
}

// 按命名端口名查找 Service 端口号。
func LookupServicePortNumberByName(svc v1.Service, name string) (int32, error) {
	for _, svcportspec := range svc.Spec.Ports {
		if svcportspec.Name == name {
			return svcportspec.Port, nil
		}
	}

	return int32(-1), fmt.Errorf("Service '%s' does not have a named port '%s'", svc.Name, name)
}
