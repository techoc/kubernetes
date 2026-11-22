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

package helper

import v1 "k8s.io/api/core/v1"

// DoNotScheduleTaintsFilterFunc 返回一个过滤函数，
// 用于筛出会阻止 Pod 调度到节点上的污点（taint）。
func DoNotScheduleTaintsFilterFunc() func(t *v1.Taint) bool {
	return func(t *v1.Taint) bool {
		// PodToleratesNodeTaints 只关心 NoSchedule 和 NoExecute 两种效果，
		// PreferNoSchedule 只是软性偏好，不构成调度阻碍。
		return t.Effect == v1.TaintEffectNoSchedule || t.Effect == v1.TaintEffectNoExecute
	}
}
