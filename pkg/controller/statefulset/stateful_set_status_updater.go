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

package statefulset

import (
	"context"

	apps "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientset "k8s.io/client-go/kubernetes"
	appslisters "k8s.io/client-go/listers/apps/v1"
	"k8s.io/client-go/util/retry"
	"k8s.io/klog/v2"
	consistencyutil "k8s.io/kubernetes/pkg/controller/util/consistency"
)

// StatefulSetStatusUpdaterInterface 用于更新 StatefulSet 关联的 StatefulSetStatus。
// 除测试外的场景，调用方都应使用 NewRealStatefulSetStatusUpdater 创建实例。
type StatefulSetStatusUpdaterInterface interface {
	// UpdateStatefulSetStatus 将该 StatefulSet 的 Status 设为 status。
	// 实现方必须在发生冲突时重试，但遇到其他错误应直接失败。
	// 若返回 nil，说明状态已成功写入。
	UpdateStatefulSetStatus(ctx context.Context, set *apps.StatefulSet, status *apps.StatefulSetStatus) error
}

// NewRealStatefulSetStatusUpdater 返回一个 StatefulSetStatusUpdaterInterface，
// 使用给定的客户端与 setLister 更新 StatefulSet 的 Status。
func NewRealStatefulSetStatusUpdater(
	client clientset.Interface,
	setLister appslisters.StatefulSetLister,
	consistencyStore consistencyutil.ConsistencyStore) StatefulSetStatusUpdaterInterface {
	return &realStatefulSetStatusUpdater{client, setLister, consistencyStore}
}

type realStatefulSetStatusUpdater struct {
	client           clientset.Interface
	setLister        appslisters.StatefulSetLister
	consistencyStore consistencyutil.ConsistencyStore
}

func (ssu *realStatefulSetStatusUpdater) UpdateStatefulSetStatus(
	ctx context.Context,
	set *apps.StatefulSet,
	status *apps.StatefulSetStatus) error {
	logger := klog.FromContext(ctx)
	// 由于客户端数量有限，这里在冲突时不额外等待，但仍沿用默认的退避重试策略。
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		set.Status = *status
		// TODO: 待提供 RetryOnConflictWithContext 后，这里的 context.TODO 应换成真实 context。
		updatedSet, updateErr := ssu.client.AppsV1().StatefulSets(set.Namespace).UpdateStatus(context.TODO(), set, metav1.UpdateOptions{})
		if updateErr == nil {
			ssu.consistencyStore.WroteAt(
				types.NamespacedName{Namespace: updatedSet.Namespace, Name: updatedSet.Name},
				updatedSet.UID,
				statefulSetGroupResource,
				updatedSet.ResourceVersion,
			)
			return nil
		}

		if updated, err := ssu.setLister.StatefulSets(set.Namespace).Get(set.Name); err == nil {
			// 必须深拷贝，避免修改 lister 背后的共享缓存对象。
			set = updated.DeepCopy()
		} else {
			utilruntime.HandleErrorWithLogger(logger, nil, "Error getting updated StatefulSet from lister", "StatefulSet", klog.KObj(set))
		}

		return updateErr
	})
}

var _ StatefulSetStatusUpdaterInterface = &realStatefulSetStatusUpdater{}
