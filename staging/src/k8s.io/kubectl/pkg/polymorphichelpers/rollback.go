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

package polymorphichelpers

import (
	"bytes"
	"context"
	"fmt"
	"sort"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/json"
	"k8s.io/apimachinery/pkg/util/strategicpatch"
	"k8s.io/client-go/kubernetes"
	"k8s.io/kubectl/pkg/apps"
	cmdutil "k8s.io/kubectl/pkg/cmd/util"
	"k8s.io/kubectl/pkg/scheme"
	deploymentutil "k8s.io/kubectl/pkg/util/deployment"
)

const (
	rollbackSuccess = "rolled back"
	rollbackSkipped = "skipped rollback"
)

// Rollbacker 为支持回滚的资源提供统一接口。
type Rollbacker interface {
	Rollback(obj runtime.Object, updatedAnnotations map[string]string, toRevision int64, dryRunStrategy cmdutil.DryRunStrategy) (string, error)
}

type RollbackVisitor struct {
	clientset kubernetes.Interface
	result    Rollbacker
}

func (v *RollbackVisitor) VisitDeployment(elem apps.GroupKindElement) {
	v.result = &DeploymentRollbacker{v.clientset}
}

func (v *RollbackVisitor) VisitStatefulSet(kind apps.GroupKindElement) {
	v.result = &StatefulSetRollbacker{v.clientset}
}

func (v *RollbackVisitor) VisitDaemonSet(kind apps.GroupKindElement) {
	v.result = &DaemonSetRollbacker{v.clientset}
}

func (v *RollbackVisitor) VisitJob(kind apps.GroupKindElement)                   {}
func (v *RollbackVisitor) VisitPod(kind apps.GroupKindElement)                   {}
func (v *RollbackVisitor) VisitReplicaSet(kind apps.GroupKindElement)            {}
func (v *RollbackVisitor) VisitReplicationController(kind apps.GroupKindElement) {}
func (v *RollbackVisitor) VisitCronJob(kind apps.GroupKindElement)               {}

// 按给定 schema kind 返回 Rollbacker 实现。
func RollbackerFor(kind schema.GroupKind, c kubernetes.Interface) (Rollbacker, error) {
	elem := apps.GroupKindElement(kind)
	visitor := &RollbackVisitor{
		clientset: c,
	}

	err := elem.Accept(visitor)

	if err != nil {
		return nil, fmt.Errorf("error retrieving rollbacker for %q, %v", kind.String(), err)
	}

	if visitor.result == nil {
		return nil, fmt.Errorf("no rollbacker has been implemented for %q", kind)
	}

	return visitor.result, nil
}

type DeploymentRollbacker struct {
	c kubernetes.Interface
}

func (r *DeploymentRollbacker) Rollback(obj runtime.Object, updatedAnnotations map[string]string, toRevision int64, dryRunStrategy cmdutil.DryRunStrategy) (string, error) {
	if toRevision < 0 {
		return "", revisionNotFoundErr(toRevision)
	}
	accessor, err := meta.Accessor(obj)
	if err != nil {
		return "", fmt.Errorf("failed to create accessor for kind %v: %w", obj.GetObjectKind(), err)
	}
	name := accessor.GetName()
	namespace := accessor.GetNamespace()

	// TODO: kubectl 从核心仓库移出后再修正。runtime.Object 无法在不经过内部版本往返的情况下
	// 转换为外部 appsv1 Deployment。目前正逐步移除所有资源的内部版本，
	// 因此这里直接指定 appsv1 版本，
	// 与 DaemonSet 和 StatefulSet 的处理方式一致。
	deployment, err := r.c.AppsV1().Deployments(namespace).Get(context.TODO(), name, metav1.GetOptions{})
	if err != nil {
		return "", fmt.Errorf("failed to retrieve Deployment %s: %v", name, err)
	}

	rsForRevision, err := deploymentRevision(deployment, r.c, toRevision)
	if err != nil {
		return "", err
	}
	if dryRunStrategy == cmdutil.DryRunClient {
		return printTemplate(&rsForRevision.Spec.Template)
	}
	if deployment.Spec.Paused {
		return "", fmt.Errorf("you cannot rollback a paused deployment; resume it first with 'kubectl rollout resume' and try again")
	}

	// 修订与当前 Deployment 相同则跳过。
	if equalIgnoreHash(&rsForRevision.Spec.Template, &deployment.Spec.Template) {
		return fmt.Sprintf("%s (current template already matches revision %d)", rollbackSkipped, toRevision), nil
	}

	// 回填到 Deployment 前先去掉 hash 标签。
	delete(rsForRevision.Spec.Template.Labels, appsv1.DefaultDeploymentUniqueLabelKey)

	// 计算 Deployment 注解。
	annotations := map[string]string{}
	for k := range annotationsToSkip {
		if v, ok := deployment.Annotations[k]; ok {
			annotations[k] = v
		}
	}
	for k, v := range rsForRevision.Annotations {
		if !annotationsToSkip[k] {
			annotations[k] = v
		}
	}

	// 生成用于恢复的 patch。
	patchType, patch, err := getDeploymentPatch(&rsForRevision.Spec.Template, annotations)
	if err != nil {
		return "", fmt.Errorf("failed restoring revision %d: %v", toRevision, err)
	}

	patchOptions := metav1.PatchOptions{}
	if dryRunStrategy == cmdutil.DryRunServer {
		patchOptions.DryRun = []string{metav1.DryRunAll}
	}
	// 恢复指定修订。
	if _, err = r.c.AppsV1().Deployments(namespace).Patch(context.TODO(), name, patchType, patch, patchOptions); err != nil {
		return "", fmt.Errorf("failed restoring revision %d: %v", toRevision, err)
	}
	return rollbackSuccess, nil
}

// 忽略 Labels[pod-template-hash] 的差异后，判断两个 PodTemplateSpec 是否相等。
// 忽略 pod-template-hash 是因为：
//  1. PodTemplateSpec 的 API 变化会改变哈希值
//     （例如新增字段就会改变哈希码）；
//  2. Deployment 模板本身没有这些 hash 标签。
func equalIgnoreHash(template1, template2 *corev1.PodTemplateSpec) bool {
	t1Copy := template1.DeepCopy()
	t2Copy := template2.DeepCopy()
	// 比较前先从 template.Labels 中删除 hash 标签。
	delete(t1Copy.Labels, appsv1.DefaultDeploymentUniqueLabelKey)
	delete(t2Copy.Labels, appsv1.DefaultDeploymentUniqueLabelKey)
	return apiequality.Semantic.DeepEqual(t1Copy, t2Copy)
}

// 回滚时保留 Deployment 自身的这些注解，
// 不从 ReplicaSet 复制。
var annotationsToSkip = map[string]bool{
	corev1.LastAppliedConfigAnnotation:       true,
	deploymentutil.RevisionAnnotation:        true,
	deploymentutil.RevisionHistoryAnnotation: true,
	deploymentutil.DesiredReplicasAnnotation: true,
	deploymentutil.MaxReplicasAnnotation:     true,
	appsv1.DeprecatedRollbackTo:              true,
}

// 返回可把 Deployment 恢复到旧版本的 patch；
// 错误为 nil 时 patch 有效。
func getDeploymentPatch(podTemplate *corev1.PodTemplateSpec, annotations map[string]string) (types.PatchType, []byte, error) {
	// 生成替换 spec.template 的 Deployment patch。
	patch, err := json.Marshal([]interface{}{
		map[string]interface{}{
			"op":    "replace",
			"path":  "/spec/template",
			"value": podTemplate,
		},
		map[string]interface{}{
			"op":    "replace",
			"path":  "/metadata/annotations",
			"value": annotations,
		},
	})
	return types.JSONPatchType, patch, err
}

func deploymentRevision(deployment *appsv1.Deployment, c kubernetes.Interface, toRevision int64) (revision *appsv1.ReplicaSet, err error) {

	_, allOldRSs, newRS, err := deploymentutil.GetAllReplicaSets(deployment, c.AppsV1())
	if err != nil {
		return nil, fmt.Errorf("failed to retrieve replica sets from deployment %s: %v", deployment.Name, err)
	}
	allRSs := allOldRSs
	if newRS != nil {
		allRSs = append(allRSs, newRS)
	}

	var (
		latestReplicaSet   *appsv1.ReplicaSet
		latestRevision     = int64(-1)
		previousReplicaSet *appsv1.ReplicaSet
		previousRevision   = int64(-1)
	)
	for _, rs := range allRSs {
		if v, err := deploymentutil.Revision(rs); err == nil {
			if toRevision == 0 {
				if latestRevision < v {
					// 目前见过的最新一个。
					previousRevision = latestRevision
					previousReplicaSet = latestReplicaSet
					latestRevision = v
					latestReplicaSet = rs
				} else if previousRevision < v {
					// 目前见过的次新一个。
					previousRevision = v
					previousReplicaSet = rs
				}
			} else if toRevision == v {
				return rs, nil
			}
		}
	}

	if toRevision > 0 {
		return nil, revisionNotFoundErr(toRevision)
	}

	if previousReplicaSet == nil {
		return nil, fmt.Errorf("no rollout history found for deployment %q", deployment.Name)
	}
	return previousReplicaSet, nil
}

type DaemonSetRollbacker struct {
	c kubernetes.Interface
}

func (r *DaemonSetRollbacker) Rollback(obj runtime.Object, updatedAnnotations map[string]string, toRevision int64, dryRunStrategy cmdutil.DryRunStrategy) (string, error) {
	if toRevision < 0 {
		return "", revisionNotFoundErr(toRevision)
	}
	accessor, err := meta.Accessor(obj)
	if err != nil {
		return "", fmt.Errorf("failed to create accessor for kind %v: %w", obj.GetObjectKind(), err)
	}
	ds, history, err := daemonSetHistory(r.c.AppsV1(), accessor.GetNamespace(), accessor.GetName())
	if err != nil {
		return "", err
	}
	if toRevision == 0 && len(history) <= 1 {
		return "", fmt.Errorf("no last revision to roll back to")
	}

	toHistory := findHistory(toRevision, history)
	if toHistory == nil {
		return "", revisionNotFoundErr(toRevision)
	}

	if dryRunStrategy == cmdutil.DryRunClient {
		appliedDS, err := applyDaemonSetHistory(ds, toHistory)
		if err != nil {
			return "", err
		}
		return printPodTemplate(&appliedDS.Spec.Template)
	}

	// 修订与当前 DaemonSet 相同则跳过。
	done, err := daemonSetMatch(ds, toHistory)
	if err != nil {
		return "", err
	}
	if done {
		return fmt.Sprintf("%s (current template already matches revision %d)", rollbackSkipped, toRevision), nil
	}

	patchOptions := metav1.PatchOptions{}
	if dryRunStrategy == cmdutil.DryRunServer {
		patchOptions.DryRun = []string{metav1.DryRunAll}
	}
	// 恢复指定修订。
	if _, err = r.c.AppsV1().DaemonSets(accessor.GetNamespace()).Patch(context.TODO(), accessor.GetName(), types.StrategicMergePatchType, toHistory.Data.Raw, patchOptions); err != nil {
		return "", fmt.Errorf("failed restoring revision %d: %v", toRevision, err)
	}

	return rollbackSuccess, nil
}

// 检查给定 DaemonSet 的模板是否与历史中保存的模板一致。
func daemonSetMatch(ds *appsv1.DaemonSet, history *appsv1.ControllerRevision) (bool, error) {
	patch, err := getDaemonSetPatch(ds)
	if err != nil {
		return false, err
	}
	return bytes.Equal(patch, history.Data.Raw), nil
}

// 返回可把 DaemonSet 恢复到旧版本的 strategic merge patch；
// 错误为 nil 时 patch 有效。当前只保存 PodSpecTemplate，
// 以后可调整保存的状态范围，同时保持与已有 patch 兼容。
func getDaemonSetPatch(ds *appsv1.DaemonSet) ([]byte, error) {
	dsBytes, err := json.Marshal(ds)
	if err != nil {
		return nil, err
	}
	var raw map[string]interface{}
	err = json.Unmarshal(dsBytes, &raw)
	if err != nil {
		return nil, err
	}
	objCopy := make(map[string]interface{})
	specCopy := make(map[string]interface{})

	// 生成替换 spec.template 的 DaemonSet patch。
	spec := raw["spec"].(map[string]interface{})
	template := spec["template"].(map[string]interface{})
	specCopy["template"] = template
	template["$patch"] = "replace"
	objCopy["spec"] = specCopy
	patch, err := json.Marshal(objCopy)
	return patch, err
}

type StatefulSetRollbacker struct {
	c kubernetes.Interface
}

// toRevision 为非负整数，0 保留表示回滚到上一个配置。
func (r *StatefulSetRollbacker) Rollback(obj runtime.Object, updatedAnnotations map[string]string, toRevision int64, dryRunStrategy cmdutil.DryRunStrategy) (string, error) {
	if toRevision < 0 {
		return "", revisionNotFoundErr(toRevision)
	}
	accessor, err := meta.Accessor(obj)
	if err != nil {
		return "", fmt.Errorf("failed to create accessor for kind %v: %w", obj.GetObjectKind(), err)
	}
	sts, history, err := statefulSetHistory(r.c.AppsV1(), accessor.GetNamespace(), accessor.GetName())
	if err != nil {
		return "", err
	}
	if toRevision == 0 && len(history) <= 1 {
		return "", fmt.Errorf("no last revision to roll back to")
	}

	toHistory := findHistory(toRevision, history)
	if toHistory == nil {
		return "", revisionNotFoundErr(toRevision)
	}

	if dryRunStrategy == cmdutil.DryRunClient {
		appliedSS, err := applyRevision(sts, toHistory)
		if err != nil {
			return "", err
		}
		return printPodTemplate(&appliedSS.Spec.Template)
	}

	// 修订与当前 StatefulSet 相同则跳过。
	done, err := statefulsetMatch(sts, toHistory)
	if err != nil {
		return "", err
	}
	if done {
		return fmt.Sprintf("%s (current template already matches revision %d)", rollbackSkipped, toRevision), nil
	}

	patchOptions := metav1.PatchOptions{}
	if dryRunStrategy == cmdutil.DryRunServer {
		patchOptions.DryRun = []string{metav1.DryRunAll}
	}
	// 恢复指定修订。
	if _, err = r.c.AppsV1().StatefulSets(sts.Namespace).Patch(context.TODO(), sts.Name, types.StrategicMergePatchType, toHistory.Data.Raw, patchOptions); err != nil {
		return "", fmt.Errorf("failed restoring revision %d: %v", toRevision, err)
	}

	return rollbackSuccess, nil
}

var appsCodec = scheme.Codecs.LegacyCodec(appsv1.SchemeGroupVersion)

// 把指定修订的状态还原到新的 StatefulSet；
// 错误为 nil 时返回的 StatefulSet 有效。
func applyRevision(set *appsv1.StatefulSet, revision *appsv1.ControllerRevision) (*appsv1.StatefulSet, error) {
	patched, err := strategicpatch.StrategicMergePatch([]byte(runtime.EncodeOrDie(appsCodec, set)), revision.Data.Raw, set)
	if err != nil {
		return nil, err
	}
	result := &appsv1.StatefulSet{}
	err = json.Unmarshal(patched, result)
	if err != nil {
		return nil, err
	}
	return result, nil
}

// 检查给定 StatefulSet 的模板是否与历史中保存的模板一致。
func statefulsetMatch(ss *appsv1.StatefulSet, history *appsv1.ControllerRevision) (bool, error) {
	patch, err := getStatefulSetPatch(ss)
	if err != nil {
		return false, err
	}
	return bytes.Equal(patch, history.Data.Raw), nil
}

// 返回可把 StatefulSet 恢复到旧版本的 strategic merge patch；
// 错误为 nil 时 patch 有效。当前只保存 PodSpecTemplate，
// 以后可调整保存的状态范围，同时保持与已有 patch 兼容。
func getStatefulSetPatch(set *appsv1.StatefulSet) ([]byte, error) {
	str, err := runtime.Encode(appsCodec, set)
	if err != nil {
		return nil, err
	}
	var raw map[string]interface{}
	if err := json.Unmarshal([]byte(str), &raw); err != nil {
		return nil, err
	}
	objCopy := make(map[string]interface{})
	specCopy := make(map[string]interface{})
	spec := raw["spec"].(map[string]interface{})
	template := spec["template"].(map[string]interface{})
	specCopy["template"] = template
	template["$patch"] = "replace"
	objCopy["spec"] = specCopy
	patch, err := json.Marshal(objCopy)
	return patch, err
}

// 从给定 ControllerRevision 列表中返回指定修订的那一个；
// 不存在时返回 nil。
// toRevision 为 0 时返回上一次使用的历史。
func findHistory(toRevision int64, allHistory []*appsv1.ControllerRevision) *appsv1.ControllerRevision {
	if toRevision == 0 && len(allHistory) <= 1 {
		return nil
	}

	// 找出要回滚到的历史。
	var toHistory *appsv1.ControllerRevision
	if toRevision == 0 {
		// toRevision 为 0 时取次新修订。
		sort.Sort(historiesByRevision(allHistory))
		toHistory = allHistory[len(allHistory)-2]
	} else {
		for _, h := range allHistory {
			if h.Revision == toRevision {
				// toRevision 非 0 时取修订号匹配的历史。
				return h
			}
		}
	}

	return toHistory
}

// 把 Pod 模板转换为易读字符串。
func printPodTemplate(specTemplate *corev1.PodTemplateSpec) (string, error) {
	podSpec, err := printTemplate(specTemplate)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("will roll back to %s", podSpec), nil
}

func revisionNotFoundErr(r int64) error {
	return fmt.Errorf("unable to find specified revision %v in history", r)
}

// TODO: 从 daemon 控制器复制而来，应抽取为公共库。
type historiesByRevision []*appsv1.ControllerRevision

func (h historiesByRevision) Len() int      { return len(h) }
func (h historiesByRevision) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h historiesByRevision) Less(i, j int) bool {
	return h[i].Revision < h[j].Revision
}
