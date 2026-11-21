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

package probe

import (
	"fmt"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
)

// ResolveContainerPort 将 IntOrString 类型的端口参数解析为具体的端口号。
// HTTP/TCP 探针的 Port 字段可以是数字或命名端口（named port）。
// 如果是数字，直接返回；如果是字符串，则在容器的 Ports 列表中查找对应的命名端口。
// 最终端口号必须在 1-65535 范围内。
func ResolveContainerPort(param intstr.IntOrString, container *v1.Container) (int, error) {
	port := -1
	var err error
	switch param.Type {
	case intstr.Int:
		// 直接取整数值
		port = param.IntValue()
	case intstr.String:
		// 通过端口名在容器的 Ports 列表中查找
		if port, err = findPortByName(container, param.StrVal); err != nil {
			return port, err
		}
	default:
		return port, fmt.Errorf("intOrString had no kind: %+v", param)
	}
	// 验证端口号范围：1-65535
	if port > 0 && port < 65536 {
		return port, nil
	}
	return port, fmt.Errorf("invalid port number: %v", port)
}

// findPortByName 在容器的 Ports 列表中按名称查找端口。
// 如果容器未定义该名称的端口，返回错误。
func findPortByName(container *v1.Container, portName string) (int, error) {
	for _, port := range container.Ports {
		if port.Name == portName {
			return int(port.ContainerPort), nil
		}
	}
	return 0, fmt.Errorf("port %q not found", portName)
}
