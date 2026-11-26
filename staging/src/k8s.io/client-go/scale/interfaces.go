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

package scale

import (
	"context"

	autoscalingapi "k8s.io/api/autoscaling/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
)

// ScalesGetter 用于创建 ScaleInterface。
//
// 分层的设计让调用方先绑定命名空间，再以 GroupResource 定位具体资源，
// 避免每次调用都重复拼接 URL。
type ScalesGetter interface {
	// Scales 为指定命名空间创建 ScaleInterface；集群级资源传入空字符串。
	Scales(namespace string) ScaleInterface
}

// ScaleInterface 用于获取和更新指定命名空间内实现了 scale 子资源的资源的 Scale。
//
// 它面向的是实现了 scale 子资源的任意资源（Deployment、StatefulSet、ReplicaSet 等），
// 因此方法都以 GroupResource 而非具体类型为参数，Get/Patch 使用 autoscaling/v1 的 Scale 对象
// 作为统一的线上表示。
type ScaleInterface interface {
	// Get 获取给定可伸缩资源的 Scale。
	Get(ctx context.Context, resource schema.GroupResource, name string, opts metav1.GetOptions) (*autoscalingapi.Scale, error)

	// Update 更新给定可伸缩资源的 Scale。
	Update(ctx context.Context, resource schema.GroupResource, scale *autoscalingapi.Scale, opts metav1.UpdateOptions) (*autoscalingapi.Scale, error)

	// Patch 修补给定可伸缩资源的 Scale。
	Patch(ctx context.Context, gvr schema.GroupVersionResource, name string, pt types.PatchType, data []byte, opts metav1.PatchOptions) (*autoscalingapi.Scale, error)
}
