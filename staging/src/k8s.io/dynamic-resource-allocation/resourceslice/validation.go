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

package resourceslice

import (
	"fmt"

	resourceapi "k8s.io/api/resource/v1"
	"k8s.io/apimachinery/pkg/util/sets"
)

// validateDriverResources 校验服务端校验无法捕获、且会阻碍已发布 ResourceSlice 使用的交叉引用等问题，以便尽早发现。
// 分配器在分配设备前也会执行同样的检查。
func validateDriverResources(resources *DriverResources) error {
	// 逐个 pool 进行校验，遇到第一个错误立即返回。
	for poolName, pool := range resources.Pools {
		if err := validatePool(poolName, pool); err != nil {
			return err
		}
	}
	return nil
}

// validatePool 检查单个 pool 内无法由 API 服务器逐 ResourceSlice 校验发现的整体性问题。
// 其逻辑与分配器汇总 pool 时相近，后续可考虑合并。
func validatePool(name string, pool Pool) error {
	// 收集 pool 内所有共享计数器集合，用于检测重复与引用有效性。
	counterSets := make(map[string]resourceapi.CounterSet)
	// 遍历所有 slice 中的计数器集合。
	for _, slice := range pool.Slices {
		for _, counterSet := range slice.SharedCounters {
			// 同名计数器集合重复出现则报错。
			if _, found := counterSets[counterSet.Name]; found {
				return fmt.Errorf("found duplicate counter set %q in pool %q", counterSet.Name, name)
			}
			counterSets[counterSet.Name] = counterSet
		}
	}

	// 已见过的设备名集合，用于检测重复设备。
	devices := sets.New[string]()
	for _, slice := range pool.Slices {
		// 遍历所有 slice 中的设备。
		for _, device := range slice.Devices {
			// 设备名重复则报错。
			if _, found := devices[device.Name]; found {
				return fmt.Errorf("found duplicate device %q in pool %q", device.Name, name)
			}
			// 记录设备名，并检查其计数器引用。
			devices.Insert(device.Name)

			// 遍历设备消耗的计数器。
			for _, dcc := range device.ConsumesCounters {
				// 引用的计数器集合必须存在。
				counterSet, found := counterSets[dcc.CounterSet]
				if !found {
					return fmt.Errorf("counter set %q referenced by device %q not found", dcc.CounterSet, device.Name)
				}
				// 引用的每个计数器都必须存在于该集合中。
				for counterName := range dcc.Counters {
					if _, found := counterSet.Counters[counterName]; !found {
						return fmt.Errorf("counter %q referenced by device %q not found in counter set %q", counterName, device.Name, counterSet.Name)
					}
				}
			}
		}
	}
	return nil
}
