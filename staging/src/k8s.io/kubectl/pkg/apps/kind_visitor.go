/*
Copyright 2017 The Kubernetes Authors.

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

package apps

import (
	"fmt"

	"k8s.io/apimachinery/pkg/runtime/schema"
)

// KindVisitor 与 GroupKindElement 配合使用，根据 schema.GroupKind 的
// Kind 调用相应的函数
type KindVisitor interface {
	VisitDaemonSet(kind GroupKindElement)
	VisitDeployment(kind GroupKindElement)
	VisitJob(kind GroupKindElement)
	VisitPod(kind GroupKindElement)
	VisitReplicaSet(kind GroupKindElement)
	VisitReplicationController(kind GroupKindElement)
	VisitStatefulSet(kind GroupKindElement)
	VisitCronJob(kind GroupKindElement)
}

// GroupKindElement 定义一个 Kubernetes API group 元素
type GroupKindElement schema.GroupKind

// Accept 调用 visitor 上与 elem 的 Kind 对应的 Visit 方法
func (elem GroupKindElement) Accept(visitor KindVisitor) error {
	switch {
	case elem.GroupMatch("apps", "extensions") && elem.Kind == "DaemonSet":
		visitor.VisitDaemonSet(elem)
	case elem.GroupMatch("apps", "extensions") && elem.Kind == "Deployment":
		visitor.VisitDeployment(elem)
	case elem.GroupMatch("batch") && elem.Kind == "Job":
		visitor.VisitJob(elem)
	case elem.GroupMatch("", "core") && elem.Kind == "Pod":
		visitor.VisitPod(elem)
	case elem.GroupMatch("apps", "extensions") && elem.Kind == "ReplicaSet":
		visitor.VisitReplicaSet(elem)
	case elem.GroupMatch("", "core") && elem.Kind == "ReplicationController":
		visitor.VisitReplicationController(elem)
	case elem.GroupMatch("apps") && elem.Kind == "StatefulSet":
		visitor.VisitStatefulSet(elem)
	case elem.GroupMatch("batch") && elem.Kind == "CronJob":
		visitor.VisitCronJob(elem)
	default:
		return fmt.Errorf("no visitor method exists for %v", elem)
	}
	return nil
}

// 当且仅当 elem 的 group 与传入的某个 group 参数匹配时，
// GroupMatch 返回 true
func (elem GroupKindElement) GroupMatch(groups ...string) bool {
	for _, g := range groups {
		if elem.Group == g {
			return true
		}
	}
	return false
}
