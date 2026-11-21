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

package reconciler

import (
	"context"

	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/klog/v2"
)

// Run 先重建卷状态，然后按固定周期执行 reconcile。
func (rc *reconciler) Run(ctx context.Context, stopCh <-chan struct{}) {
	logger := klog.FromContext(ctx)
	rc.reconstructVolumes(logger)
	logger.Info("Reconciler: start to sync state")
	wait.Until(func() { rc.reconcile(ctx) }, rc.loopSleepDuration, stopCh)
}

// reconcile 执行一轮卷对账：卸载、挂载/挂接、设备卸载与孤儿卷清理。
func (rc *reconciler) reconcile(ctx context.Context) {
	logger := klog.FromContext(ctx)
	readyToUnmount := rc.readyToUnmount()
	if readyToUnmount {
		// 先卸载再挂载：若某卷原本被已删除的 Pod 引用、现在改由新 Pod 引用，
		// 需要先把它从旧 Pod 卸载，再挂载给新 Pod。
		rc.unmountVolumes(logger)
	}

	// 接下来挂载所需卷；若 kubelet 负责挂接卷，此函数也可能触发 attach。
	// 若底层 PVC 在使用中被扩容，这里也会处理卷扩容。
	rc.mountOrAttachVolumes(logger)

	// 只有 DSW 与 ASW 都完整填充后才卸载卷，避免卸载仍被需要、但尚未进入 DSW 的卷。
	if readyToUnmount {
		// 确保应卸载/解挂的设备都已完成操作。
		rc.unmountDetachDevices(logger)

		// 清理重建失败后遗留的孤儿卷。
		rc.cleanOrphanVolumes(logger)
	}

	if len(rc.volumesNeedUpdateFromNodeStatus) != 0 {
		rc.updateReconstructedFromNodeStatus(ctx)
	}
	if len(rc.volumesNeedUpdateFromNodeStatus) == 0 {
		// 只有 devicePaths 与不确定的卷挂接能力都从 apiserver 重建完成后，ASW 才算完整；
		// 此时才开始对账 node.status.volumesInUse。
		rc.updateLastSyncTime()
	}
}
