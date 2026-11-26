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
	"sort"
	"strconv"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	intstrutil "k8s.io/apimachinery/pkg/util/intstr"
	runtimeresource "k8s.io/cli-runtime/pkg/resource"
	appsclient "k8s.io/client-go/kubernetes/typed/apps/v1"
)

const (
	// 记录 Deployment 各 ReplicaSet  rollout 顺序的修订注解。
	RevisionAnnotation = "deployment.kubernetes.io/revision"
	// 记录 ReplicaSet 曾为 Deployment 提供的所有旧修订。
	RevisionHistoryAnnotation = "deployment.kubernetes.io/revision-history"
	// 记录在 ReplicaSet 上的期望副本数注解，
	// 用于区分扩缩容与 rollout，并判断新 ReplicaSet
	// 是否已达到预期副本数。
	DesiredReplicasAnnotation = "deployment.kubernetes.io/desired-replicas"
	// 记录某一时刻 Deployment 的最大副本数，即
	// deployment.spec.replicas + maxSurge；底层 ReplicaSet 用它估算
	// 存在超出副本时的占比。
	MaxReplicasAnnotation = "deployment.kubernetes.io/max-replicas"
	// 回滚时找不到修订的事件原因。
	RollbackRevisionNotFound = "DeploymentRollbackRevisionNotFound"
	// 回滚时模板未变更的事件原因。
	RollbackTemplateUnchanged = "DeploymentRollbackTemplateUnchanged"
	// 回滚完成的事件原因。
	RollbackDone = "DeploymentRollback"
	// 最新 ReplicaSet 在 progressDeadlineSeconds 内没有进展时，
	// 给 Deployment 加上该原因。
	TimedOutReason = "ProgressDeadlineExceeded"
)

// 返回指定类型的 Deployment Condition。
func GetDeploymentCondition(status appsv1.DeploymentStatus, condType appsv1.DeploymentConditionType) *appsv1.DeploymentCondition {
	for i := range status.Conditions {
		c := status.Conditions[i]
		if c.Type == condType {
			return &c
		}
	}
	return nil
}

// 返回输入对象的修订号。
func Revision(obj runtime.Object) (int64, error) {
	acc, err := meta.Accessor(obj)
	if err != nil {
		return 0, err
	}
	v, ok := acc.GetAnnotations()[RevisionAnnotation]
	if !ok {
		return 0, nil
	}
	return strconv.ParseInt(v, 10, 64)
}

// 返回给定 Deployment 关联的新旧 ReplicaSet。从客户端取 PodList 和
// ReplicaSetList。第一批旧 ReplicaSet 不含无 Pod 的，
// 第二批则包含全部旧 ReplicaSet。第三个返回值
// 是新 ReplicaSet，尚未创建时为 nil。
func GetAllReplicaSets(deployment *appsv1.Deployment, c appsclient.AppsV1Interface) ([]*appsv1.ReplicaSet, []*appsv1.ReplicaSet, *appsv1.ReplicaSet, error) {
	rsList, err := listReplicaSets(deployment, rsListFromClient(c), nil)
	if err != nil {
		return nil, nil, nil, err
	}
	newRS := findNewReplicaSet(deployment, rsList)
	oldRSes, allOldRSes := findOldReplicaSets(deployment, rsList, newRS)
	return oldRSes, allOldRSes, newRS, nil
}

// 与 GetAllReplicaSets 相同，但接受分块大小参数。
// 返回给定 Deployment 关联的新旧 ReplicaSet。从客户端取 PodList 和
// ReplicaSetList。第一批旧 ReplicaSet 不含无 Pod 的，
// 第二批则包含全部旧 ReplicaSet。第三个返回值
// 是新 ReplicaSet，尚未创建时为 nil。
func GetAllReplicaSetsInChunks(deployment *appsv1.Deployment, c appsclient.AppsV1Interface, chunkSize int64) ([]*appsv1.ReplicaSet, []*appsv1.ReplicaSet, *appsv1.ReplicaSet, error) {
	rsList, err := listReplicaSets(deployment, rsListFromClient(c), &chunkSize)
	if err != nil {
		return nil, nil, nil, err
	}
	newRS := findNewReplicaSet(deployment, rsList)
	oldRSes, allOldRSes := findOldReplicaSets(deployment, rsList, newRS)
	return oldRSes, allOldRSes, newRS, nil
}

// 返回包装给定客户端的 rsListFunc。
func rsListFromClient(c appsclient.AppsV1Interface) rsListFunc {
	return func(namespace string, initialOpts metav1.ListOptions) ([]*appsv1.ReplicaSet, error) {
		rsList := &appsv1.ReplicaSetList{}
		err := runtimeresource.FollowContinue(&initialOpts,
			func(opts metav1.ListOptions) (runtime.Object, error) {
				newRs, err := c.ReplicaSets(namespace).List(context.TODO(), opts)
				if err != nil {
					return nil, runtimeresource.EnhanceListError(err, opts, "replicasets")
				}
				rsList.Items = append(rsList.Items, newRs.Items...)
				return newRs, nil
			})
		if err != nil {
			return nil, err
		}
		var ret []*appsv1.ReplicaSet
		for i := range rsList.Items {
			ret = append(ret, &rsList.Items[i])
		}
		return ret, err
	}
}

// TODO: 改用完整的 namespacer。
type rsListFunc func(string, metav1.ListOptions) ([]*appsv1.ReplicaSet, error)

// 返回给定 Deployment 关联的 ReplicaSet。
// 这里不会调和 ControllerRef（adopt/orphan），
// 那是控制器自身的职责。
// 但会过滤掉 ControllerRef 不匹配的对象。
func listReplicaSets(deployment *appsv1.Deployment, getRSList rsListFunc, chunkSize *int64) ([]*appsv1.ReplicaSet, error) {
	// TODO: 目前按标签列举 ReplicaSet，应按其 selector 列举。
	//       should be a superset of the deployment's selector, see https://github.com/kubernetes/kubernetes/issues/19830.
	namespace := deployment.Namespace
	selector, err := metav1.LabelSelectorAsSelector(deployment.Spec.Selector)
	if err != nil {
		return nil, err
	}
	options := metav1.ListOptions{LabelSelector: selector.String()}
	if chunkSize != nil {
		options.Limit = *chunkSize
	}
	all, err := getRSList(namespace, options)
	if err != nil {
		return nil, err
	}
	// 只保留 ControllerRef 指向该 Deployment 的。
	owned := make([]*appsv1.ReplicaSet, 0, len(all))
	for _, rs := range all {
		if metav1.IsControlledBy(rs, deployment) {
			owned = append(owned, rs)
		}
	}
	return owned, nil
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

// 返回与给定 Deployment 模板相同的新 ReplicaSet。
func findNewReplicaSet(deployment *appsv1.Deployment, rsList []*appsv1.ReplicaSet) *appsv1.ReplicaSet {
	sort.Sort(replicaSetsByCreationTimestamp(rsList))
	for i := range rsList {
		if equalIgnoreHash(&rsList[i].Spec.Template, &deployment.Spec.Template) {
			// 少数情况下（如集群升级后），Deployment 可能出现
			// 多个模板相同的新 ReplicaSet；
			// see https://github.com/kubernetes/kubernetes/issues/40415
			// 此时确定性地选择最早创建的那个。
			return rsList[i]
		}
	}
	// 新 ReplicaSet 不存在。
	return nil
}

// 按创建时间排序 ReplicaSet，时间相同则以名称打破平局。
type replicaSetsByCreationTimestamp []*appsv1.ReplicaSet

func (o replicaSetsByCreationTimestamp) Len() int      { return len(o) }
func (o replicaSetsByCreationTimestamp) Swap(i, j int) { o[i], o[j] = o[j], o[i] }
func (o replicaSetsByCreationTimestamp) Less(i, j int) bool {
	if o[i].CreationTimestamp.Equal(&o[j].CreationTimestamp) {
		return o[i].Name < o[j].Name
	}
	return o[i].CreationTimestamp.Before(&o[j].CreationTimestamp)
}

// 从给定 RS 列表中筛出 Deployment 关联的旧 ReplicaSet。
// 第一批旧 ReplicaSet 不含无 Pod 的，第二批则包含全部。
func findOldReplicaSets(deployment *appsv1.Deployment, rsList []*appsv1.ReplicaSet, newRS *appsv1.ReplicaSet) ([]*appsv1.ReplicaSet, []*appsv1.ReplicaSet) {
	var requiredRSs []*appsv1.ReplicaSet
	var allRSs []*appsv1.ReplicaSet
	for _, rs := range rsList {
		// 过滤掉新 ReplicaSet。
		if newRS != nil && rs.UID == newRS.UID {
			continue
		}
		allRSs = append(allRSs, rs)
		if *(rs.Spec.Replicas) != 0 {
			requiredRSs = append(requiredRSs, rs)
		}
	}
	return requiredRSs, allRSs
}

// 同时解析 maxSurge 和 maxUnavailable，必须一次性
// 完成。例如：
//
// 期望 2、maxUnavailable 1%、surge 0%：旧(-1)、新(+1)、旧(-1)、新(+1)
// 期望 1、maxUnavailable 1%、surge 0%：旧(-1)、新(+1)
// 期望 2、maxUnavailable 25%、surge 1%：新(+1)、旧(-1)、新(+1)、旧(-1)
// 期望 1、maxUnavailable 25%、surge 1%：新(+1)、旧(-1)
// 期望 2、maxUnavailable 0%、surge 1%：新(+1)、旧(-1)、新(+1)、旧(-1)
// 期望 1、maxUnavailable 0%、surge 1%：新(+1)、旧(-1)
func ResolveFenceposts(maxSurge, maxUnavailable *intstrutil.IntOrString, desired int32) (int32, int32, error) {
	surge, err := intstrutil.GetScaledValueFromIntOrPercent(intstrutil.ValueOrDefault(maxSurge, intstrutil.FromInt32(0)), int(desired), true)
	if err != nil {
		return 0, 0, err
	}
	unavailable, err := intstrutil.GetScaledValueFromIntOrPercent(intstrutil.ValueOrDefault(maxUnavailable, intstrutil.FromInt32(0)), int(desired), false)
	if err != nil {
		return 0, 0, err
	}

	if surge == 0 && unavailable == 0 {
		// 校验应禁止用户把 maxSurge 和 maxUnavailable
		// 同时显式设为 0；但向下取整可能让 maxUnavailable 变成 0。
		// 两者都为 0 时把 maxUnavailable 设为 1，
		// 因为配额可能导致 surge 无法生效。
		unavailable = 1
	}

	return int32(surge), int32(unavailable), nil
}
