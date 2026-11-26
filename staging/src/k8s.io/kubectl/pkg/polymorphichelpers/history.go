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
	"io"
	"slices"
	"text/tabwriter"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/json"
	"k8s.io/apimachinery/pkg/util/strategicpatch"
	"k8s.io/client-go/kubernetes"
	clientappsv1 "k8s.io/client-go/kubernetes/typed/apps/v1"
	"k8s.io/klog/v2"
	"k8s.io/kubectl/pkg/apps"
	"k8s.io/kubectl/pkg/describe"
	deploymentutil "k8s.io/kubectl/pkg/util/deployment"
)

const (
	ChangeCauseAnnotation = "kubernetes.io/change-cause"
)

// HistoryViewer 为含历史信息的资源提供查看接口。
type HistoryViewer interface {
	ViewHistory(namespace, name string, revision int64) (string, error)
	GetHistory(namespace, name string) (map[int64]runtime.Object, error)
}

type HistoryVisitor struct {
	clientset kubernetes.Interface
	result    HistoryViewer
}

func (v *HistoryVisitor) VisitDeployment(elem apps.GroupKindElement) {
	v.result = &DeploymentHistoryViewer{v.clientset}
}

func (v *HistoryVisitor) VisitStatefulSet(kind apps.GroupKindElement) {
	v.result = &StatefulSetHistoryViewer{v.clientset}
}

func (v *HistoryVisitor) VisitDaemonSet(kind apps.GroupKindElement) {
	v.result = &DaemonSetHistoryViewer{v.clientset}
}

func (v *HistoryVisitor) VisitJob(kind apps.GroupKindElement)                   {}
func (v *HistoryVisitor) VisitPod(kind apps.GroupKindElement)                   {}
func (v *HistoryVisitor) VisitReplicaSet(kind apps.GroupKindElement)            {}
func (v *HistoryVisitor) VisitReplicationController(kind apps.GroupKindElement) {}
func (v *HistoryVisitor) VisitCronJob(kind apps.GroupKindElement)               {}

// 按给定 schema kind 返回 HistoryViewer 实现。
func HistoryViewerFor(kind schema.GroupKind, c kubernetes.Interface) (HistoryViewer, error) {
	elem := apps.GroupKindElement(kind)
	visitor := &HistoryVisitor{
		clientset: c,
	}

	// 先确定这里需要哪种 HistoryViewer。
	err := elem.Accept(visitor)

	if err != nil {
		return nil, fmt.Errorf("error retrieving history for %q, %v", kind.String(), err)
	}

	if visitor.result == nil {
		return nil, fmt.Errorf("no history viewer has been implemented for %q", kind.String())
	}

	return visitor.result, nil
}

type DeploymentHistoryViewer struct {
	c kubernetes.Interface
}

// 以 revision 到 ReplicaSet 的映射返回 Deployment 的修订历史。
// TODO: 这里应改用 describer。
func (h *DeploymentHistoryViewer) ViewHistory(namespace, name string, revision int64) (string, error) {
	allRSs, err := getDeploymentReplicaSets(h.c.AppsV1(), namespace, name)
	if err != nil {
		return "", err
	}

	historyInfo := make(map[int64]*corev1.PodTemplateSpec)
	for _, rs := range allRSs {
		v, err := deploymentutil.Revision(rs)
		if err != nil {
			klog.Warningf("unable to get revision from replicaset %s for deployment %s in namespace %s: %v", rs.Name, name, namespace, err)
			continue
		}
		historyInfo[v] = &rs.Spec.Template
		changeCause := getChangeCause(rs)
		if historyInfo[v].Annotations == nil {
			historyInfo[v].Annotations = make(map[string]string)
		}
		if len(changeCause) > 0 {
			historyInfo[v].Annotations[ChangeCauseAnnotation] = changeCause
		}
	}

	if len(historyInfo) == 0 {
		return "No rollout history found.", nil
	}

	if revision > 0 {
		// 输出指定修订的详情。
		template, ok := historyInfo[revision]
		if !ok {
			return "", fmt.Errorf("unable to find the specified revision")
		}
		return printTemplate(template)
	}

	// 按修订号排序 revisionToChangeCause。
	revisions := make([]int64, 0, len(historyInfo))
	for r := range historyInfo {
		revisions = append(revisions, r)
	}
	slices.Sort(revisions)

	return tabbedString(func(out io.Writer) error {
		fmt.Fprintf(out, "REVISION\tCHANGE-CAUSE\n")
		for _, r := range revisions {
			// 找出修订 r 的变更原因。
			changeCause := historyInfo[r].Annotations[ChangeCauseAnnotation]
			if len(changeCause) == 0 {
				changeCause = "<none>"
			}
			fmt.Fprintf(out, "%d\t%s\n", r, changeCause)
		}
		return nil
	})
}

// 返回 Deployment 关联的 ReplicaSet 修订。
func (h *DeploymentHistoryViewer) GetHistory(namespace, name string) (map[int64]runtime.Object, error) {
	allRSs, err := getDeploymentReplicaSets(h.c.AppsV1(), namespace, name)
	if err != nil {
		return nil, err
	}

	result := make(map[int64]runtime.Object)
	for _, rs := range allRSs {
		v, err := deploymentutil.Revision(rs)
		if err != nil {
			klog.Warningf("unable to get revision from replicaset %s for deployment %s in namespace %s: %v", rs.Name, name, namespace, err)
			continue
		}
		result[v] = rs
	}

	return result, nil
}

func printTemplate(template *corev1.PodTemplateSpec) (string, error) {
	buf := bytes.NewBuffer([]byte{})
	w := describe.NewPrefixWriter(buf)
	describe.DescribePodTemplate(template, w)
	return buf.String(), nil
}

type DaemonSetHistoryViewer struct {
	c kubernetes.Interface
}

// 以 revision 到历史的映射返回 Deployment 的修订历史。
// TODO: 这里应改用 describer。
func (h *DaemonSetHistoryViewer) ViewHistory(namespace, name string, revision int64) (string, error) {
	ds, history, err := daemonSetHistory(h.c.AppsV1(), namespace, name)
	if err != nil {
		return "", err
	}
	return printHistory(history, revision, func(history *appsv1.ControllerRevision) (*corev1.PodTemplateSpec, error) {
		dsOfHistory, err := applyDaemonSetHistory(ds, history)
		if err != nil {
			return nil, err
		}
		return &dsOfHistory.Spec.Template, err
	})
}

// 返回 DaemonSet 关联的修订。
func (h *DaemonSetHistoryViewer) GetHistory(namespace, name string) (map[int64]runtime.Object, error) {
	ds, history, err := daemonSetHistory(h.c.AppsV1(), namespace, name)
	if err != nil {
		return nil, err
	}

	result := make(map[int64]runtime.Object)
	for _, h := range history {
		applied, err := applyDaemonSetHistory(ds, h)
		if err != nil {
			return nil, err
		}
		result[h.Revision] = applied
	}

	return result, nil
}

// printHistory 在修订号非零时输出该修订的 podTemplate，
// 否则输出全部修订概览。
func printHistory(history []*appsv1.ControllerRevision, revision int64, getPodTemplate func(history *appsv1.ControllerRevision) (*corev1.PodTemplateSpec, error)) (string, error) {
	historyInfo := make(map[int64]*appsv1.ControllerRevision)
	for _, history := range history {
		// TODO: 目前假定修订互不重叠，后续可能需要处理重叠。
		historyInfo[history.Revision] = history
	}
	if len(historyInfo) == 0 {
		return "No rollout history found.", nil
	}

	// 输出指定修订的详情。
	if revision > 0 {
		history, ok := historyInfo[revision]
		if !ok {
			return "", fmt.Errorf("unable to find the specified revision")
		}
		podTemplate, err := getPodTemplate(history)
		if err != nil {
			return "", fmt.Errorf("unable to parse history %s", history.Name)
		}
		return printTemplate(podTemplate)
	}

	// 输出所有修订的概览。
	// 按修订号排序 revisionToChangeCause。
	revisions := make([]int64, 0, len(historyInfo))
	for r := range historyInfo {
		revisions = append(revisions, r)
	}
	slices.Sort(revisions)

	return tabbedString(func(out io.Writer) error {
		fmt.Fprintf(out, "REVISION\tCHANGE-CAUSE\n")
		for _, r := range revisions {
			// 找出修订 r 的变更原因。
			changeCause := historyInfo[r].Annotations[ChangeCauseAnnotation]
			if len(changeCause) == 0 {
				changeCause = "<none>"
			}
			fmt.Fprintf(out, "%d\t%s\n", r, changeCause)
		}
		return nil
	})
}

type StatefulSetHistoryViewer struct {
	c kubernetes.Interface
}

// 返回 StatefulSet 的修订历史列表。
// TODO: 这里应改用 describer。
func (h *StatefulSetHistoryViewer) ViewHistory(namespace, name string, revision int64) (string, error) {
	sts, history, err := statefulSetHistory(h.c.AppsV1(), namespace, name)
	if err != nil {
		return "", err
	}
	return printHistory(history, revision, func(history *appsv1.ControllerRevision) (*corev1.PodTemplateSpec, error) {
		stsOfHistory, err := applyStatefulSetHistory(sts, history)
		if err != nil {
			return nil, err
		}
		return &stsOfHistory.Spec.Template, err
	})
}

// 返回 StatefulSet 关联的修订。
func (h *StatefulSetHistoryViewer) GetHistory(namespace, name string) (map[int64]runtime.Object, error) {
	sts, history, err := statefulSetHistory(h.c.AppsV1(), namespace, name)
	if err != nil {
		return nil, err
	}

	result := make(map[int64]runtime.Object)
	for _, h := range history {
		applied, err := applyStatefulSetHistory(sts, h)
		if err != nil {
			return nil, err
		}
		result[h.Revision] = applied
	}

	return result, nil
}

func getDeploymentReplicaSets(apps clientappsv1.AppsV1Interface, namespace, name string) ([]*appsv1.ReplicaSet, error) {
	deployment, err := apps.Deployments(namespace).Get(context.TODO(), name, metav1.GetOptions{})
	if err != nil {
		return nil, fmt.Errorf("failed to retrieve deployment %s: %v", name, err)
	}

	_, oldRSs, newRS, err := deploymentutil.GetAllReplicaSets(deployment, apps)
	if err != nil {
		return nil, fmt.Errorf("failed to retrieve replica sets from deployment %s: %v", name, err)
	}

	if newRS == nil {
		return oldRSs, nil
	}
	return append(oldRSs, newRS), nil
}

// 返回指定 namespace 中由选择器筛出且归属给定对象的全部 ControllerRevision。
// TODO: 其他控制器升级后将其改名为 controllerHistory。
func controlledHistoryV1(
	apps clientappsv1.AppsV1Interface,
	namespace string,
	selector labels.Selector,
	accessor metav1.Object) ([]*appsv1.ControllerRevision, error) {
	var result []*appsv1.ControllerRevision
	historyList, err := apps.ControllerRevisions(namespace).List(context.TODO(), metav1.ListOptions{LabelSelector: selector.String()})
	if err != nil {
		return nil, err
	}
	for i := range historyList.Items {
		history := historyList.Items[i]
		// 只加入属于该 API 对象的历史。
		if metav1.IsControlledBy(&history, accessor) {
			result = append(result, &history)
		}
	}
	return result, nil
}

// 返回指定 namespace 中由选择器筛出且归属给定对象的全部 ControllerRevision。
func controlledHistory(
	apps clientappsv1.AppsV1Interface,
	namespace string,
	selector labels.Selector,
	accessor metav1.Object) ([]*appsv1.ControllerRevision, error) {
	var result []*appsv1.ControllerRevision
	historyList, err := apps.ControllerRevisions(namespace).List(context.TODO(), metav1.ListOptions{LabelSelector: selector.String()})
	if err != nil {
		return nil, err
	}
	for i := range historyList.Items {
		history := historyList.Items[i]
		// 只加入属于该 API 对象的历史。
		if metav1.IsControlledBy(&history, accessor) {
			result = append(result, &history)
		}
	}
	return result, nil
}

// 返回指定 namespace 中的 DaemonSet 及其历史中的全部 ControllerRevision。
func daemonSetHistory(
	apps clientappsv1.AppsV1Interface,
	namespace, name string) (*appsv1.DaemonSet, []*appsv1.ControllerRevision, error) {
	ds, err := apps.DaemonSets(namespace).Get(context.TODO(), name, metav1.GetOptions{})
	if err != nil {
		return nil, nil, fmt.Errorf("failed to retrieve DaemonSet %s: %v", name, err)
	}
	selector, err := metav1.LabelSelectorAsSelector(ds.Spec.Selector)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create selector for DaemonSet %s: %v", ds.Name, err)
	}
	accessor, err := meta.Accessor(ds)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create accessor for DaemonSet %s: %v", ds.Name, err)
	}
	history, err := controlledHistory(apps, ds.Namespace, selector, accessor)
	if err != nil {
		return nil, nil, fmt.Errorf("unable to find history controlled by DaemonSet %s: %v", ds.Name, err)
	}
	return ds, history, nil
}

// 返回指定 namespace 中的 StatefulSet 及其历史中的全部 ControllerRevision。
func statefulSetHistory(
	apps clientappsv1.AppsV1Interface,
	namespace, name string) (*appsv1.StatefulSet, []*appsv1.ControllerRevision, error) {
	sts, err := apps.StatefulSets(namespace).Get(context.TODO(), name, metav1.GetOptions{})
	if err != nil {
		return nil, nil, fmt.Errorf("failed to retrieve StatefulSet %s: %w", name, err)
	}
	selector, err := metav1.LabelSelectorAsSelector(sts.Spec.Selector)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create selector for StatefulSet %s: %w", name, err)
	}
	accessor, err := meta.Accessor(sts)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to obtain accessor for StatefulSet %s: %w", name, err)
	}
	history, err := controlledHistoryV1(apps, namespace, selector, accessor)
	if err != nil {
		return nil, nil, fmt.Errorf("unable to find history controlled by StatefulSet %s: %v", name, err)
	}
	return sts, history, nil
}

// 把给定历史应用到 DaemonSet 的副本，还原指定修订。
func applyDaemonSetHistory(ds *appsv1.DaemonSet, history *appsv1.ControllerRevision) (*appsv1.DaemonSet, error) {
	dsBytes, err := json.Marshal(ds)
	if err != nil {
		return nil, err
	}
	patched, err := strategicpatch.StrategicMergePatch(dsBytes, history.Data.Raw, ds)
	if err != nil {
		return nil, err
	}
	result := &appsv1.DaemonSet{}
	err = json.Unmarshal(patched, result)
	if err != nil {
		return nil, err
	}
	return result, nil
}

// 把给定历史应用到 StatefulSet 的副本，还原指定修订。
func applyStatefulSetHistory(sts *appsv1.StatefulSet, history *appsv1.ControllerRevision) (*appsv1.StatefulSet, error) {
	stsBytes, err := json.Marshal(sts)
	if err != nil {
		return nil, err
	}
	patched, err := strategicpatch.StrategicMergePatch(stsBytes, history.Data.Raw, sts)
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

// TODO: 此处先复制代码，待改用 describer 后移除。
func tabbedString(f func(io.Writer) error) (string, error) {
	out := new(tabwriter.Writer)
	buf := &bytes.Buffer{}
	out.Init(buf, 0, 8, 2, ' ', 0)

	err := f(out)
	if err != nil {
		return "", err
	}

	out.Flush()
	return buf.String(), nil
}

// 返回输入对象的 change-cause 注解。
func getChangeCause(obj runtime.Object) string {
	accessor, err := meta.Accessor(obj)
	if err != nil {
		return ""
	}
	return accessor.GetAnnotations()[ChangeCauseAnnotation]
}
