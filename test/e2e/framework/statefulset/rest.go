/*
Copyright 2019 The Kubernetes Authors.

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
	"fmt"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/wait"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/util/retry"
	"k8s.io/kubectl/pkg/util/podutils"
	"k8s.io/kubernetes/test/e2e/framework"
	e2emanifest "k8s.io/kubernetes/test/e2e/framework/manifest"
	e2epodoutput "k8s.io/kubernetes/test/e2e/framework/pod/output"
)

// CreateStatefulSet 用 kubectl create 从 ns namespace 中 manifestPath 的 manifest 创建 StatefulSet。
func CreateStatefulSet(ctx context.Context, c clientset.Interface, manifestPath, ns string) *appsv1.StatefulSet {
	mkpath := func(file string) string {
		return filepath.Join(manifestPath, file)
	}

	framework.Logf("Parsing statefulset from %v", mkpath("statefulset.yaml"))
	ss, err := e2emanifest.StatefulSetFromManifest(mkpath("statefulset.yaml"), ns)
	framework.ExpectNoError(err)
	framework.Logf("Parsing service from %v", mkpath("service.yaml"))
	svc, err := e2emanifest.SvcFromManifest(mkpath("service.yaml"))
	framework.ExpectNoError(err)

	framework.Logf("creating %s service", ss.Name)
	_, err = c.CoreV1().Services(ns).Create(ctx, svc, metav1.CreateOptions{})
	framework.ExpectNoError(err)

	framework.Logf("creating statefulset %v/%v with %d replicas and selector %+v", ss.Namespace, ss.Name, *(ss.Spec.Replicas), ss.Spec.Selector)
	_, err = c.AppsV1().StatefulSets(ns).Create(ctx, ss, metav1.CreateOptions{})
	framework.ExpectNoError(err)
	WaitForRunningAndReady(ctx, c, *ss.Spec.Replicas, ss)
	return ss
}

// GetPodList 获取 ss 中当前的 Pod。
func GetPodList(ctx context.Context, c clientset.Interface, ss *appsv1.StatefulSet) (*v1.PodList, error) {
	selector, err := metav1.LabelSelectorAsSelector(ss.Spec.Selector)
	if err != nil {
		return nil, err
	}

	return c.CoreV1().Pods(ss.Namespace).List(ctx, metav1.ListOptions{LabelSelector: selector.String()})
}

// DeleteAllStatefulSets 删除 namespace ns 中所有 StatefulSet API 对象。
func DeleteAllStatefulSets(ctx context.Context, c clientset.Interface, ns string) {
	ssList, err := c.AppsV1().StatefulSets(ns).List(ctx, metav1.ListOptions{LabelSelector: labels.Everything().String()})
	framework.ExpectNoError(err)

	// 先缩小每个 statefulset，再彻底删除。
	// 不这样做就删除 pvc 会泄漏 volume，#25101。
	errList := []string{}
	for i := range ssList.Items {
		ss := &ssList.Items[i]
		var err error
		if ss, err = Scale(ctx, c, ss, 0); err != nil {
			errList = append(errList, fmt.Sprintf("%v", err))
		}
		WaitForStatusReplicas(ctx, c, ss, 0)
		framework.Logf("Deleting statefulset %v", ss.Name)
		// 用 OrphanDependents=false 让它同步删除。
		// 我们已经通过 Scale() 确保 Pod 都消失了。
		if err := c.AppsV1().StatefulSets(ss.Namespace).Delete(ctx, ss.Name, metav1.DeleteOptions{OrphanDependents: new(bool)}); err != nil {
			errList = append(errList, fmt.Sprintf("%v", err))
		}
	}

	// pv 是全局的，所以要等待与 statefulset pvc 绑定的那些。
	pvNames := sets.NewString()
	// TODO: 不要假设 ns 中所有 pvc 都属于 statefulset
	pvcPollErr := wait.PollUntilContextTimeout(ctx, StatefulSetPoll, StatefulSetTimeout, true, func(ctx context.Context) (bool, error) {
		pvcList, err := c.CoreV1().PersistentVolumeClaims(ns).List(ctx, metav1.ListOptions{LabelSelector: labels.Everything().String()})
		if err != nil {
			framework.Logf("WARNING: Failed to list pvcs, retrying %v", err)
			return false, nil
		}
		for _, pvc := range pvcList.Items {
			pvNames.Insert(pvc.Spec.VolumeName)
			// TODO: 再确认没有 pod 引用该 pvc
			framework.Logf("Deleting pvc: %v with volume %v", pvc.Name, pvc.Spec.VolumeName)
			if err := c.CoreV1().PersistentVolumeClaims(ns).Delete(ctx, pvc.Name, metav1.DeleteOptions{}); err != nil {
				return false, nil
			}
		}
		return true, nil
	})
	if pvcPollErr != nil {
		errList = append(errList, "Timeout waiting for pvc deletion.")
	}

	pollErr := wait.PollUntilContextTimeout(ctx, StatefulSetPoll, StatefulSetTimeout, true, func(ctx context.Context) (bool, error) {
		pvList, err := c.CoreV1().PersistentVolumes().List(ctx, metav1.ListOptions{LabelSelector: labels.Everything().String()})
		if err != nil {
			framework.Logf("WARNING: Failed to list pvs, retrying %v", err)
			return false, nil
		}
		waitingFor := []string{}
		for _, pv := range pvList.Items {
			if pvNames.Has(pv.Name) {
				waitingFor = append(waitingFor, fmt.Sprintf("%v: %+v", pv.Name, pv.Status))
			}
		}
		if len(waitingFor) == 0 {
			return true, nil
		}
		framework.Logf("Still waiting for pvs of statefulset to disappear:\n%v", strings.Join(waitingFor, "\n"))
		return false, nil
	})
	if pollErr != nil {
		errList = append(errList, "Timeout waiting for pv provisioner to delete pvs, this might mean the test leaked pvs.")
	}
	if len(errList) != 0 {
		framework.ExpectNoError(fmt.Errorf("%v", strings.Join(errList, "\n")))
	}
}

// Scale 把 ss 扩缩容到 count 个副本。
func Scale(ctx context.Context, c clientset.Interface, ss *appsv1.StatefulSet, count int32) (*appsv1.StatefulSet, error) {
	name := ss.Name
	ns := ss.Namespace

	framework.Logf("Scaling statefulset %s to %d", name, count)
	ss = update(ctx, c, ns, name, count)

	var statefulPodList *v1.PodList
	pollErr := wait.PollUntilContextTimeout(ctx, StatefulSetPoll, StatefulSetTimeout, true, func(ctx context.Context) (bool, error) {
		statefulPodList, err := GetPodList(ctx, c, ss)
		if err != nil {
			return false, err
		}

		if int32(len(statefulPodList.Items)) == count {
			return true, nil
		}
		return false, nil
	})
	if pollErr != nil {
		unhealthy := []string{}
		for _, statefulPod := range statefulPodList.Items {
			delTs, phase, readiness := statefulPod.DeletionTimestamp, statefulPod.Status.Phase, podutils.IsPodReady(&statefulPod)
			if delTs != nil || phase != v1.PodRunning || !readiness {
				unhealthy = append(unhealthy, fmt.Sprintf("%v: deletion %v, phase %v, readiness %v", statefulPod.Name, delTs, phase, readiness))
			}
		}
		return ss, fmt.Errorf("Failed to scale statefulset to %d in %v. Remaining pods:\n%v", count, StatefulSetTimeout, unhealthy)
	}
	return ss, nil
}

// UpdateReplicas 把 ss 的副本数更新为 count。
func UpdateReplicas(ctx context.Context, c clientset.Interface, ss *appsv1.StatefulSet, count int32) {
	update(ctx, c, ss.Namespace, ss.Name, count)
}

// Restart 把 ss 缩到 0 再恢复到之前的副本数。
func Restart(ctx context.Context, c clientset.Interface, ss *appsv1.StatefulSet) {
	oldReplicas := *(ss.Spec.Replicas)
	ss, err := Scale(ctx, c, ss, 0)
	framework.ExpectNoError(err)
	// 等待 controller 上报期望数量的 Pod。
	// 这样我们知道在扩回去之前
	// controller 已观察到所有 Pod 删除。
	WaitForStatusReplicas(ctx, c, ss, 0)
	update(ctx, c, ss.Namespace, ss.Name, oldReplicas)
}

// CheckHostname 验证 ss 中所有 Pod 都有正确的 Hostname。返回错误非 nil 表示验证失败。
func CheckHostname(ctx context.Context, c clientset.Interface, ss *appsv1.StatefulSet) error {
	cmd := "printf $(hostname)"
	podList, err := GetPodList(ctx, c, ss)
	if err != nil {
		return err
	}

	for _, statefulPod := range podList.Items {
		hostname, err := e2epodoutput.RunHostCmdWithRetries(statefulPod.Namespace, statefulPod.Name, cmd, StatefulSetPoll, StatefulPodTimeout)
		if err != nil {
			return err
		}
		if hostname != statefulPod.Name {
			return fmt.Errorf("unexpected hostname (%s) and stateful pod name (%s) not equal", hostname, statefulPod.Name)
		}
	}
	return nil
}

// CheckMount 检查 ss 中所有 Pod 在 mountPath 的挂载有效。
func CheckMount(ctx context.Context, c clientset.Interface, ss *appsv1.StatefulSet, mountPath string) error {
	for _, cmd := range []string{
		// 打印 inode、大小等
		fmt.Sprintf("ls -idlh %v", mountPath),
		// 打印子目录
		fmt.Sprintf("find %v", mountPath),
		// 尝试写入
		fmt.Sprintf("touch %v", filepath.Join(mountPath, fmt.Sprintf("%v", time.Now().UnixNano()))),
	} {
		if err := ExecInStatefulPods(ctx, c, ss, cmd); err != nil {
			return fmt.Errorf("failed to execute %v, error: %w", cmd, err)
		}
	}
	return nil
}

// CheckServiceName 断言 ss 的 ServiceName 等于 expectedServiceName。
func CheckServiceName(ss *appsv1.StatefulSet, expectedServiceName string) error {
	framework.Logf("Checking if statefulset spec.serviceName is %s", expectedServiceName)

	if expectedServiceName != ss.Spec.ServiceName {
		return fmt.Errorf("wrong service name governing statefulset. Expected %s got %s",
			expectedServiceName, ss.Spec.ServiceName)
	}

	return nil
}

// CheckPodIndexLabel 断言 ss 的 pod 具有预期的 index label 和值。
func CheckPodIndexLabel(ctx context.Context, c clientset.Interface, ss *appsv1.StatefulSet) error {
	pods, err := GetPodList(ctx, c, ss)
	if err != nil {
		return err
	}

	labelIndices := sets.NewInt()
	for _, pod := range pods.Items {
		ix, err := strconv.Atoi(pod.Labels[appsv1.PodIndexLabel])
		if err != nil {
			return err
		}
		labelIndices.Insert(ix)
	}
	wantIndexes := []int{0, 1, 2}
	gotIndexes := labelIndices.List()

	if !reflect.DeepEqual(gotIndexes, wantIndexes) {
		return fmt.Errorf("pod index labels are not as expected, got: %v, want: %v", gotIndexes, wantIndexes)
	}
	return nil
}

// ExecInStatefulPods 在 ss 的所有 Pod 中执行命令。出错则返回，后续 Pod 不再执行。
func ExecInStatefulPods(ctx context.Context, c clientset.Interface, ss *appsv1.StatefulSet, cmd string) error {
	podList, err := GetPodList(ctx, c, ss)
	if err != nil {
		return err
	}

	for _, statefulPod := range podList.Items {
		stdout, err := e2epodoutput.RunHostCmdWithRetries(statefulPod.Namespace, statefulPod.Name, cmd, StatefulSetPoll, StatefulPodTimeout)
		framework.Logf("stdout of %v on %v: %v", cmd, statefulPod.Name, stdout)
		if err != nil {
			return err
		}
	}
	return nil
}

// update 更新 statefulset，仅在 rest.go 内部使用
func update(ctx context.Context, c clientset.Interface, ns, name string, replicas int32) (ss *appsv1.StatefulSet) {
	err := retry.RetryOnConflict(retry.DefaultBackoff, func() error {
		var err error
		ss, err = c.AppsV1().StatefulSets(ns).Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			framework.Failf("failed to get statefulset %q: %v", name, err)
		}
		if *(ss.Spec.Replicas) == replicas {
			return nil
		}
		*(ss.Spec.Replicas) = replicas
		ss, err = c.AppsV1().StatefulSets(ns).Update(ctx, ss, metav1.UpdateOptions{})
		return err
	})
	if err == nil {
		return ss
	}
	framework.Failf("failed to update statefulset  %q: %v", name, err)
	return nil
}
