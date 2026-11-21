/*
Copyright 2019 The Kubernetes Authors.

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

package v1

import (
	"fmt"

	"k8s.io/api/scheduling/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/kubernetes/pkg/apis/scheduling"
)

// systemPriorityClasses 定义集群引导阶段自动创建的系统优先级类。
// API 校验逻辑确保任何带系统前缀或值高于 HighestUserDefinablePriority 的优先级类，
// 都必须与这里定义的某个系统优先级类完全一致，防止伪装或冲突。
var systemPriorityClasses = []*v1.PriorityClass{
	{
		ObjectMeta: metav1.ObjectMeta{
			Name: scheduling.SystemNodeCritical,
		},
		Value:       scheduling.SystemCriticalPriority + 1000,
		Description: "Used for system critical pods that must not be moved from their current node.",
	},
	{
		ObjectMeta: metav1.ObjectMeta{
			Name: scheduling.SystemClusterCritical,
		},
		Value:       scheduling.SystemCriticalPriority,
		Description: "Used for system critical pods that must run in the cluster, but can be moved to another node if necessary.",
	},
}

// SystemPriorityClasses 返回系统优先级类列表的深拷贝。
// 使用深拷贝避免调用方修改返回值污染包内的共享数据；
// 如果只需要名称，请使用 SystemPriorityClassNames()。
func SystemPriorityClasses() []*v1.PriorityClass {
	retval := make([]*v1.PriorityClass, 0, len(systemPriorityClasses))
	for _, c := range systemPriorityClasses {
		retval = append(retval, c.DeepCopy())
	}
	return retval
}

// SystemPriorityClassNames 返回所有系统优先级类的名称。
func SystemPriorityClassNames() []string {
	retval := make([]string, 0, len(systemPriorityClasses))
	for _, c := range systemPriorityClasses {
		retval = append(retval, c.Name)
	}
	return retval
}

// IsKnownSystemPriorityClass 判断是否存在名称、值、globalDefault 完全匹配的系统优先级类。
// 若名称匹配但值或 globalDefault 不符，返回带有具体差异的错误信息。
func IsKnownSystemPriorityClass(name string, value int32, globalDefault bool) (bool, error) {
	for _, spc := range systemPriorityClasses {
		if spc.Name == name {
			if spc.Value != value {
				return false, fmt.Errorf("value of %v PriorityClass must be %v", spc.Name, spc.Value)
			}
			if spc.GlobalDefault != globalDefault {
				return false, fmt.Errorf("globalDefault of %v PriorityClass must be %v", spc.Name, spc.GlobalDefault)
			}
			return true, nil
		}
	}
	return false, fmt.Errorf("%v is not a known system priority class", name)
}
