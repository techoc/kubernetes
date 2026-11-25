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
	"strconv"

	apps "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/klog/v2"
	deploymentutil "k8s.io/kubernetes/pkg/controller/deployment/util"
)

// rollback 把 Deployment 回滚到指定 revision；无论成功与否，最后都会清理 rollbackTo 规格，
// 避免同一请求在后续 requeue 中被反复处理。
func (dc *DeploymentController) rollback(ctx context.Context, d *apps.Deployment, rsList []*apps.ReplicaSet) error {
	logger := klog.FromContext(ctx)
	newRS, allOldRSs, err := dc.getAllReplicaSetsAndSyncRevision(ctx, d, rsList, true)
	if err != nil {
		return err
	}

	allRSs := append(allOldRSs, newRS)
	rollbackRevision := *getRollbackTo(d)
	// revision 为 0 表示用户未指定版本，回退到最近一个历史版本
	if rollbackRevision == 0 {
		if rollbackRevision = deploymentutil.LastRevision(logger, allRSs); rollbackRevision == 0 {
			// 找不到可回退的历史版本，放弃回滚并清理标记
			dc.emitRollbackWarningEvent(d, deploymentutil.RollbackRevisionNotFound, "Unable to find last revision.")
			// 放弃回滚
			return dc.updateDeploymentAndClearRollbackTo(ctx, d)
		}
	}
	for _, rs := range allRSs {
		v, err := deploymentutil.Revision(rs)
		if err != nil {
			logger.V(4).Info("Unable to extract revision from deployment's replica set", "replicaSet", klog.KObj(rs), "err", err)
			continue
		}
		if v == rollbackRevision {
			logger.V(4).Info("Found replica set with desired revision", "replicaSet", klog.KObj(rs), "revision", v)
			// 通过复制目标 ReplicaSet 的 podTemplate.Spec 完成回滚；
			// 即使模板相同也继续清理 rollbackTo，revision 号会在下次同步时递增。
			// 模板未变化时 rollbackToTemplate 为空操作。
			performedRollback, err := dc.rollbackToTemplate(ctx, d, rs)
			if performedRollback && err == nil {
				dc.emitRollbackNormalEvent(d, fmt.Sprintf("Rolled back deployment %q to revision %d", d.Name, rollbackRevision))
			}
			return err
		}
	}
	dc.emitRollbackWarningEvent(d, deploymentutil.RollbackRevisionNotFound, "Unable to find the revision to rollback to.")
	// 放弃回滚
	return dc.updateDeploymentAndClearRollbackTo(ctx, d)
}

// rollbackToTemplate 比较 Deployment 与 ReplicaSet 的模板，不同则用 ReplicaSet 的模板更新 Deployment，
// 并清理 rollbackTo 规格，防止后续 requeue 再次进入回滚流程。
func (dc *DeploymentController) rollbackToTemplate(ctx context.Context, d *apps.Deployment, rs *apps.ReplicaSet) (bool, error) {
	logger := klog.FromContext(ctx)
	performedRollback := false
	if !deploymentutil.EqualIgnoreHash(&d.Spec.Template, &rs.Spec.Template) {
		logger.V(4).Info("Rolling back deployment to old template spec", "deployment", klog.KObj(d), "templateSpec", rs.Spec.Template.Spec)
		deploymentutil.SetFromReplicaSetTemplate(d, rs.Spec.Template)
		// 必须把目标旧 RS 的注解同步回 Deployment：回滚后新 RS 会从 Deployment 继承注解，
		// 若不同步，change-cause 等历史信息会被当前版本覆盖。
		//
		// 例如旧 RS1 带 {change-cause:create}，新 RS2 带 {change-cause:edit}，Deployment 当前也是 edit。
		// 回滚到 RS1 时应把模板和注解一并复制回来：Deployment 变为 create，新 RS1 为 create，旧 RS2 仍为 edit。
		// 否则 Deployment 停留在 edit，新 RS1 也会继承 edit，导致变更历史错误。
		deploymentutil.SetDeploymentAnnotationsTo(d, rs)
		performedRollback = true
	} else {
		logger.V(4).Info("Rolling back to a revision that contains the same template as current deployment, skipping rollback...", "deployment", klog.KObj(d))
		eventMsg := fmt.Sprintf("The rollback revision contains the same template as current deployment %q", d.Name)
		dc.emitRollbackWarningEvent(d, deploymentutil.RollbackTemplateUnchanged, eventMsg)
	}

	return performedRollback, dc.updateDeploymentAndClearRollbackTo(ctx, d)
}

// emitRollbackWarningEvent 记录回滚失败的 Warning 事件。
func (dc *DeploymentController) emitRollbackWarningEvent(d *apps.Deployment, reason, message string) {
	dc.eventRecorder.Eventf(d, v1.EventTypeWarning, reason, "%s", message)
}

// emitRollbackNormalEvent 记录回滚成功的 Normal 事件。
func (dc *DeploymentController) emitRollbackNormalEvent(d *apps.Deployment, message string) {
	dc.eventRecorder.Eventf(d, v1.EventTypeNormal, deploymentutil.RollbackDone, "%s", message)
}

// updateDeploymentAndClearRollbackTo 清空 .spec.rollbackTo 并更新 Deployment。
// 调用方应已在需要回滚时更新好模板；本函数也用于回滚失败后的清理。
func (dc *DeploymentController) updateDeploymentAndClearRollbackTo(ctx context.Context, d *apps.Deployment) error {
	logger := klog.FromContext(ctx)
	logger.V(4).Info("Cleans up rollbackTo of deployment", "deployment", klog.KObj(d))
	setRollbackTo(d, nil)
	_, err := dc.client.AppsV1().Deployments(d.Namespace).Update(ctx, d, metav1.UpdateOptions{})
	return err
}

// getRollbackTo 从已废弃的 rollback 注解中读取目标 revision；注解缺失或格式非法时返回 nil。
func getRollbackTo(d *apps.Deployment) *int64 {
	// 提取用于往返保存已废弃 RollbackTo 字段的注解。
	revision := d.Annotations[apps.DeprecatedRollbackTo]
	if revision == "" {
		return nil
	}
	revision64, err := strconv.ParseInt(revision, 10, 64)
	if err != nil {
		// 注解内容非法时视为未指定，不阻断同步流程
		return nil
	}
	return &revision64
}

// setRollbackTo 设置已废弃的 rollback 注解；revision 为 nil 时删除该注解。
func setRollbackTo(d *apps.Deployment, revision *int64) {
	if revision == nil {
		delete(d.Annotations, apps.DeprecatedRollbackTo)
		return
	}
	if d.Annotations == nil {
		d.Annotations = make(map[string]string)
	}
	d.Annotations[apps.DeprecatedRollbackTo] = strconv.FormatInt(*revision, 10)
}
