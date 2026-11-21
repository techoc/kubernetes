/*
Copyright 2016 The Kubernetes Authors.

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

package deployment

import (
	"context"
	"fmt"
	"reflect"
	"time"

	apps "k8s.io/api/apps/v1"
	"k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/controller/deployment/util"
)

// syncRolloutStatus 在滚动更新期间更新 Deployment 的 status。
// 有些调用场景（例如扩容后的重新同步）无法被扩缩容检测拦截，此时不应估算进度，
// 否则会把一次普通的状态重算误判为新的发布进展。
func (dc *DeploymentController) syncRolloutStatus(ctx context.Context, allRSs []*apps.ReplicaSet, newRS *apps.ReplicaSet, d *apps.Deployment) error {
	newStatus := calculateStatus(allRSs, newRS, d)

	// 未配置 progressDeadlineSeconds 时进度估算无从谈起，
	// 因此清掉可能残留的 Progressing 条件，避免误导用户。
	if !util.HasProgressDeadline(d) {
		util.RemoveDeploymentCondition(&newStatus, apps.DeploymentProgressing)
	}

	// 只有一个 ReplicaSet 处于活跃状态说明当前没有新发布，只是一次 resync；
	// 这种情况下无须估算进度，避免把存量状态当作新进展。
	currentCond := util.GetDeploymentCondition(d.Status, apps.DeploymentProgressing)
	isCompleteDeployment := newStatus.Replicas == newStatus.UpdatedReplicas && currentCond != nil && currentCond.Reason == util.NewRSAvailableReason
	// 仅在设置了进度截止时间且最新一轮发布尚未完成时检查进度。
	if util.HasProgressDeadline(d) && !isCompleteDeployment {
		switch {
		case util.DeploymentComplete(d, &newStatus):
			// 用新 ReplicaSet 已成功部署的消息更新条件；若条件已存在则忽略该更新。
			msg := fmt.Sprintf("Deployment %q has successfully progressed.", d.Name)
			if newRS != nil {
				msg = fmt.Sprintf("ReplicaSet %q has successfully progressed.", newRS.Name)
			}
			condition := util.NewDeploymentCondition(apps.DeploymentProgressing, v1.ConditionTrue, util.NewRSAvailableReason, msg)
			util.SetDeploymentCondition(&newStatus, *condition)

		case util.DeploymentProgressing(d, &newStatus):
			// 只要检测到进展就跳过失败判定，保持与 rolling updater 的 progressDeadline 检查一致：
			// 有进展意味着截止时间应重新计时，此时判超时会误报。
			msg := fmt.Sprintf("Deployment %q is progressing.", d.Name)
			if newRS != nil {
				msg = fmt.Sprintf("ReplicaSet %q is progressing.", newRS.Name)
			}
			condition := util.NewDeploymentCondition(apps.DeploymentProgressing, v1.ConditionTrue, util.ReplicaSetUpdatedReason, msg)
			// 这里需要手动处理已有的 Progressing 条件：SetDeploymentCondition 在 reason 相同时
			// 不会更新条件，但 Progressing 恰恰要求在 reason 不变时也能刷新 lastUpdateTime
			// 以记录“有进展”，同时保留 lastTransitionTime 以反映状态真正在何时翻转。
			if currentCond != nil {
				if currentCond.Status == v1.ConditionTrue {
					condition.LastTransitionTime = currentCond.LastTransitionTime
				}
				util.RemoveDeploymentCondition(&newStatus, apps.DeploymentProgressing)
			}
			util.SetDeploymentCondition(&newStatus, *condition)

		case util.DeploymentTimedOut(ctx, d, &newStatus):
			// 发布超时则写入超时条件；若条件已存在则忽略该更新。
			msg := fmt.Sprintf("Deployment %q has timed out progressing.", d.Name)
			if newRS != nil {
				msg = fmt.Sprintf("ReplicaSet %q has timed out progressing.", newRS.Name)
			}
			condition := util.NewDeploymentCondition(apps.DeploymentProgressing, v1.ConditionFalse, util.TimedOutReason, msg)
			util.SetDeploymentCondition(&newStatus, *condition)
		}
	}

	// 把 ReplicaSet 的失败条件汇总到 Deployment 上；目前 getReplicaFailures
	// 只会返回一个失败条件，因此只需设置第一条。
	if replicaFailureCond := dc.getReplicaFailures(allRSs, newRS); len(replicaFailureCond) > 0 {
		// ReplicaSet 上只会有一个 ReplicaFailure 条件。
		util.SetDeploymentCondition(&newStatus, replicaFailureCond[0])
	} else {
		// 没有失败条件时要清理旧条件，避免已恢复的 Deployment 长期显示失败状态。
		util.RemoveDeploymentCondition(&newStatus, apps.DeploymentReplicaFailure)
	}

	// 状态没有变化就不发起无意义的 API 写操作；但如果发布可能卡住，
	// 仍需安排一次带延迟的重入队来在截止时间点上检查超时。
	if reflect.DeepEqual(d.Status, newStatus) {
		// 即便本次不更新状态，也要按需重入队，否则超时判定只能依赖控制器的全量 resync。
		dc.requeueStuckDeployment(ctx, d, newStatus)
		return nil
	}

	newDeployment := d
	newDeployment.Status = newStatus
	_, err := dc.client.AppsV1().Deployments(newDeployment.Namespace).UpdateStatus(ctx, newDeployment, metav1.UpdateOptions{})
	return err
}

// getReplicaFailures 把 ReplicaSet 的失败条件转换为 Deployment 条件。
// 新 ReplicaSet 的失败条件优先于旧 ReplicaSet：当前发布的问题比历史遗留问题更值得展示。
func (dc *DeploymentController) getReplicaFailures(allRSs []*apps.ReplicaSet, newRS *apps.ReplicaSet) []apps.DeploymentCondition {
	var conditions []apps.DeploymentCondition
	if newRS != nil {
		for _, c := range newRS.Status.Conditions {
			if c.Type != apps.ReplicaSetReplicaFailure {
				continue
			}
			conditions = append(conditions, util.ReplicaSetToDeploymentCondition(c))
		}
	}

	// 新 ReplicaSet 已有失败条件时直接返回，忽略旧 ReplicaSet 的失败，
	// 保证用户看到的是与当前发布最相关的信息。
	if len(conditions) > 0 {
		return conditions
	}

	for i := range allRSs {
		rs := allRSs[i]
		if rs == nil {
			continue
		}

		for _, c := range rs.Status.Conditions {
			if c.Type != apps.ReplicaSetReplicaFailure {
				continue
			}
			conditions = append(conditions, util.ReplicaSetToDeploymentCondition(c))
		}
	}
	return conditions
}

// nowFn 用于单元测试中替换时间源，使超时相关断言不依赖真实时钟。
var nowFn = time.Now

// requeueStuckDeployment 判断是否需要为进度检查重新入队该 Deployment。
// 返回距离下次检查的时间；返回 0 表示立即入队，返回 -1 表示无须入队。
func (dc *DeploymentController) requeueStuckDeployment(ctx context.Context, d *apps.Deployment, newStatus apps.DeploymentStatus) time.Duration {
	logger := klog.FromContext(ctx)
	currentCond := util.GetDeploymentCondition(d.Status, apps.DeploymentProgressing)
	// 没有截止时间或没有 Progressing 条件时无法计算剩余时间，只能放弃估算。
	if !util.HasProgressDeadline(d) || currentCond == nil {
		return time.Duration(-1)
	}
	// 发布已完成或已超时时再入队也不会有新的判定结果，直接跳过。
	if util.DeploymentComplete(d, &newStatus) || currentCond.Reason == util.TimedOutReason {
		return time.Duration(-1)
	}
	// 若当前没有进展迹象，Deployment 很可能已卡住，需要在未来某个时间点[1]
	// 重新同步并检查是否超时。必须主动安排，否则只能依赖控制器的全量 resync 周期，
	// 超时判定会被延迟到不可预期的时间。见 https://github.com/kubernetes/kubernetes/issues/34458。
	//
	// [1] ProgressingCondition.LastUpdatedTime + progressDeadlineSeconds - time.Now()
	//
	// 例如某 Deployment 在 3 分钟前更新了 Progressing 条件，截止时间为 10 分钟，
	// 则应在 7 分钟后重新同步做进度检查。
	//
	// lastUpdated: 			00:00:00
	// now: 					00:03:00
	// progressDeadlineSeconds: 600 (10 minutes)
	//
	// lastUpdated + progressDeadlineSeconds - now => 00:00:00 + 00:10:00 - 00:03:00 => 07:00
	after := currentCond.LastUpdateTime.Time.Add(time.Duration(*d.Spec.ProgressDeadlineSeconds) * time.Second).Sub(nowFn())
	// 剩余时间不足一秒时立即入队，并使用限速队列防止在截止点附近反复刷新；
	// 最终该 Deployment 会收敛到 Complete 或 TimedOut 条件之一。
	if after < time.Second {
		logger.V(4).Info("Queueing up deployment for a progress check now", "deployment", klog.KObj(d))
		dc.enqueueRateLimited(d)
		return time.Duration(0)
	}
	logger.V(4).Info("Queueing up deployment for a progress check", "deployment", klog.KObj(d), "queueAfter", int(after.Seconds()))
	// 额外加一秒，避免 AddAfter 的毫秒级时间偏差导致检查早于截止时间执行。
	// 见 https://github.com/kubernetes/kubernetes/issues/39785#issuecomment-279959133。
	dc.enqueueAfter(d, after+time.Second)
	return after
}
