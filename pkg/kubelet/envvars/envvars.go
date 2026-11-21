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

package envvars

import (
	"fmt"
	"net"
	"strconv"
	"strings"

	"k8s.io/api/core/v1"
	v1helper "k8s.io/kubernetes/pkg/apis/core/v1/helper"
)

// FromServices 构造容器启动时使用的环境变量，
// 告知容器如何找到参数中传入的各个 Service。
func FromServices(services []*v1.Service) []v1.EnvVar {
	var result []v1.EnvVar
	// 为每个 Service 生成环境变量。
	for i := range services {
		service := services[i]

		// 忽略 ClusterIP 为 "None" 或空的 Service。
		// 传入的 Service 应已预过滤，这里只处理设置了 ClusterIP 的 Service。
		if !v1helper.IsServiceIPSet(service) {
			continue
		}

		// 生成 Service 的 ClusterIP 环境变量。
		name := makeEnvVariableName(service.Name) + "_SERVICE_HOST"
		result = append(result, v1.EnvVar{Name: name, Value: service.Spec.ClusterIP})
		// 第一个端口使用向后兼容的名称。
		name = makeEnvVariableName(service.Name) + "_SERVICE_PORT"
		result = append(result, v1.EnvVar{Name: name, Value: strconv.Itoa(int(service.Spec.Ports[0].Port))})
		// 为所有命名端口生成变量（只有第一个端口可以无名，校验阶段已检查）。
		for i := range service.Spec.Ports {
			sp := &service.Spec.Ports[i]
			if sp.Name != "" {
				pn := name + "_" + makeEnvVariableName(sp.Name)
				result = append(result, v1.EnvVar{Name: pn, Value: strconv.Itoa(int(sp.Port))})
			}
		}
		// 生成与 Docker 兼容的变量。
		result = append(result, makeLinkVariables(service)...)
	}
	return result
}

// makeEnvVariableName 把名称转换为环境变量形式：转大写并将 - 替换为 _。
func makeEnvVariableName(str string) string {
	// TODO: If we simplify to "all names are DNS1123Subdomains" this
	// will need two tweaks:
	//   1) Handle leading digits
	//   2) Handle dots
	return strings.ToUpper(strings.Replace(str, "-", "_", -1))
}

// makeLinkVariables 生成 Docker link 风格的服务端口环境变量。
func makeLinkVariables(service *v1.Service) []v1.EnvVar {
	prefix := makeEnvVariableName(service.Name)
	all := []v1.EnvVar{}
	for i := range service.Spec.Ports {
		sp := &service.Spec.Ports[i]

		protocol := string(v1.ProtocolTCP)
		if sp.Protocol != "" {
			protocol = string(sp.Protocol)
		}

		hostPort := net.JoinHostPort(service.Spec.ClusterIP, strconv.Itoa(int(sp.Port)))

		if i == 0 {
			// Docker 对第一个端口有特殊处理，生成不带端口号的变量。
			all = append(all, v1.EnvVar{
				Name:  prefix + "_PORT",
				Value: fmt.Sprintf("%s://%s", strings.ToLower(protocol), hostPort),
			})
		}
		portPrefix := fmt.Sprintf("%s_PORT_%d_%s", prefix, sp.Port, strings.ToUpper(protocol))
		all = append(all, []v1.EnvVar{
			{
				Name:  portPrefix,
				Value: fmt.Sprintf("%s://%s", strings.ToLower(protocol), hostPort),
			},
			{
				Name:  portPrefix + "_PROTO",
				Value: strings.ToLower(protocol),
			},
			{
				Name:  portPrefix + "_PORT",
				Value: strconv.Itoa(int(sp.Port)),
			},
			{
				Name:  portPrefix + "_ADDR",
				Value: service.Spec.ClusterIP,
			},
		}...)
	}
	return all
}
