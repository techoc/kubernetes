/*
Copyright 2015 The Kubernetes Authors.

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

package replicationcontroller

import (
	"context"
	"fmt"
	"reflect"
	"testing"
	"time"

	v1 "k8s.io/api/core/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/util/uuid"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/informers"
	clientset "k8s.io/client-go/kubernetes"
	typedv1 "k8s.io/client-go/kubernetes/typed/core/v1"
	restclient "k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/retry"
	kubeapiservertesting "k8s.io/kubernetes/cmd/kube-apiserver/app/testing"
	podutil "k8s.io/kubernetes/pkg/api/v1/pod"
	"k8s.io/kubernetes/pkg/controller/replication"
	"k8s.io/kubernetes/test/integration/framework"
	"k8s.io/kubernetes/test/utils/ktesting"
	"k8s.io/utils/ptr"
)

const (
	interval = 100 * time.Millisecond
	timeout  = 60 * time.Second
)

func labelMap() map[string]string {
	return map[string]string{"foo": "bar"}
}

func newRC(name, namespace string, replicas int) *v1.ReplicationController {
	replicasCopy := int32(replicas)
	return &v1.ReplicationController{
		TypeMeta: metav1.TypeMeta{
			Kind:       "ReplicationController",
			APIVersion: "v1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Namespace: namespace,
			Name:      name,
		},
		Spec: v1.ReplicationControllerSpec{
			Selector: labelMap(),
			Replicas: &replicasCopy,
			Template: &v1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: labelMap(),
				},
				Spec: v1.PodSpec{
					Containers: []v1.Container{
						{
							Name:  "fake-name",
							Image: "fakeimage",
						},
					},
				},
			},
		},
	}
}

func newMatchingPod(podName, namespace string) *v1.Pod {
	return &v1.Pod{
		TypeMeta: metav1.TypeMeta{
			Kind:       "Pod",
			APIVersion: "v1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      podName,
			Namespace: namespace,
			Labels:    labelMap(),
		},
		Spec: v1.PodSpec{
			Containers: []v1.Container{
				{
					Name:  "fake-name",
					Image: "fakeimage",
				},
			},
		},
		Status: v1.PodStatus{
			Phase: v1.PodRunning,
		},
	}
}

func rmSetup(t *testing.T) (context.Context, kubeapiservertesting.TearDownFunc, *replication.ReplicationManager, informers.SharedInformerFactory, clientset.Interface) {
	tCtx := ktesting.Init(t)
	// 禁用 ServiceAccount admission 插件，因为没有运行 serviceaccount controller。
	server := kubeapiservertesting.StartTestServerOrDie(t, nil, framework.DefaultTestServerFlags(), framework.SharedEtcd())

	config := restclient.CopyConfig(server.ClientConfig)
	clientSet, err := clientset.NewForConfig(config)
	if err != nil {
		t.Fatalf("Error in create clientset: %v", err)
	}
	resyncPeriod := 12 * time.Hour
	informers := informers.NewSharedInformerFactory(clientset.NewForConfigOrDie(restclient.AddUserAgent(config, "rc-informers")), resyncPeriod)

	rm := replication.NewReplicationManager(
		tCtx,
		informers.Core().V1().Pods(),
		informers.Core().V1().ReplicationControllers(),
		clientset.NewForConfigOrDie(restclient.AddUserAgent(config, "replication-controller")),
		replication.BurstReplicas,
	)
	newTeardown := func() {
		tCtx.Cancel("tearing down controller")
		server.TearDownFn()
	}

	return tCtx, newTeardown, rm, informers, clientSet
}

// 启动 RC controller 和 informer
func runControllerAndInformers(t *testing.T, rm *replication.ReplicationManager, informers informers.SharedInformerFactory, podNum int) func() {
	ctx, cancelFn := context.WithCancel(context.Background())
	informers.Start(ctx.Done())
	waitToObservePods(t, informers.Core().V1().Pods().Informer(), podNum)
	go rm.Run(ctx, 5)
	return cancelFn
}

// 等待 podInformer 观测到 Pod。在启动 RC controller 之前调用此函数，
// 以避免 rc manager 创建新的 Pod，而不是接管已有的 Pod。
func waitToObservePods(t *testing.T, podInformer cache.SharedIndexInformer, podNum int) {
	if err := wait.PollImmediate(interval, timeout, func() (bool, error) {
		objects := podInformer.GetIndexer().List()
		return len(objects) == podNum, nil
	}); err != nil {
		t.Fatalf("Error encountered when waiting for podInformer to observe the pods: %v", err)
	}
}

func createRCsPods(t *testing.T, clientSet clientset.Interface, rcs []*v1.ReplicationController, pods []*v1.Pod) ([]*v1.ReplicationController, []*v1.Pod) {
	var createdRCs []*v1.ReplicationController
	var createdPods []*v1.Pod
	for _, rc := range rcs {
		createdRC, err := clientSet.CoreV1().ReplicationControllers(rc.Namespace).Create(context.TODO(), rc, metav1.CreateOptions{})
		if err != nil {
			t.Fatalf("Failed to create replication controller %s: %v", rc.Name, err)
		}
		createdRCs = append(createdRCs, createdRC)
	}
	for _, pod := range pods {
		createdPod, err := clientSet.CoreV1().Pods(pod.Namespace).Create(context.TODO(), pod, metav1.CreateOptions{})
		if err != nil {
			t.Fatalf("Failed to create pod %s: %v", pod.Name, err)
		}
		createdPods = append(createdPods, createdPod)
	}

	return createdRCs, createdPods
}

// 验证 .Status.Replicas 等于 .Spec.Replicas
func waitRCStable(t *testing.T, clientSet clientset.Interface, rc *v1.ReplicationController) {
	rcClient := clientSet.CoreV1().ReplicationControllers(rc.Namespace)
	if err := wait.PollImmediate(interval, timeout, func() (bool, error) {
		newRC, err := rcClient.Get(context.TODO(), rc.Name, metav1.GetOptions{})
		if err != nil {
			return false, err
		}
		return newRC.Status.Replicas == *rc.Spec.Replicas, nil
	}); err != nil {
		t.Fatalf("Failed to verify .Status.Replicas is equal to .Spec.Replicas for rc %s: %v", rc.Name, err)
	}
}

// 将 .Spec.Replicas 更新为 replicas，并验证 .Status.Replicas 相应变化
func scaleRC(t *testing.T, c clientset.Interface, rc *v1.ReplicationController, replicas int32) {
	rcClient := c.CoreV1().ReplicationControllers(rc.Namespace)
	rc = updateRC(t, rcClient, rc.Name, func(rc *v1.ReplicationController) {
		*rc.Spec.Replicas = replicas
	})
	waitRCStable(t, c, rc)
}

func updatePod(t *testing.T, podClient typedv1.PodInterface, podName string, updateFunc func(*v1.Pod)) *v1.Pod {
	var pod *v1.Pod
	if err := retry.RetryOnConflict(retry.DefaultBackoff, func() error {
		newPod, err := podClient.Get(context.TODO(), podName, metav1.GetOptions{})
		if err != nil {
			return err
		}
		updateFunc(newPod)
		pod, err = podClient.Update(context.TODO(), newPod, metav1.UpdateOptions{})
		return err
	}); err != nil {
		t.Fatalf("Failed to update pod %s: %v", podName, err)
	}
	return pod
}

func updatePodStatus(t *testing.T, podClient typedv1.PodInterface, pod *v1.Pod, updateStatusFunc func(*v1.Pod)) {
	if err := retry.RetryOnConflict(retry.DefaultBackoff, func() error {
		newPod, err := podClient.Get(context.TODO(), pod.Name, metav1.GetOptions{})
		if err != nil {
			return err
		}
		updateStatusFunc(newPod)
		_, err = podClient.UpdateStatus(context.TODO(), newPod, metav1.UpdateOptions{})
		return err
	}); err != nil {
		t.Fatalf("Failed to update status of pod %s: %v", pod.Name, err)
	}
}

func getPods(t *testing.T, podClient typedv1.PodInterface, labelMap map[string]string) *v1.PodList {
	podSelector := labels.Set(labelMap).AsSelector()
	options := metav1.ListOptions{LabelSelector: podSelector.String()}
	pods, err := podClient.List(context.TODO(), options)
	if err != nil {
		t.Fatalf("Failed obtaining a list of pods that match the pod labels %v: %v", labelMap, err)
	}
	return pods
}

func updateRC(t *testing.T, rcClient typedv1.ReplicationControllerInterface, rcName string, updateFunc func(*v1.ReplicationController)) *v1.ReplicationController {
	var rc *v1.ReplicationController
	if err := retry.RetryOnConflict(retry.DefaultBackoff, func() error {
		newRC, err := rcClient.Get(context.TODO(), rcName, metav1.GetOptions{})
		if err != nil {
			return err
		}
		updateFunc(newRC)
		rc, err = rcClient.Update(context.TODO(), newRC, metav1.UpdateOptions{})
		return err
	}); err != nil {
		t.Fatalf("Failed to update rc %s: %v", rcName, err)
	}
	return rc
}

// 验证属性不正确的 RC Pod 的 ControllerRef 会被 RC 自动修补
func testPodControllerRefPatch(t *testing.T, c clientset.Interface, pod *v1.Pod, ownerReference *metav1.OwnerReference, rc *v1.ReplicationController, expectedOwnerReferenceNum int) {
	ns := rc.Namespace
	podClient := c.CoreV1().Pods(ns)
	updatePod(t, podClient, pod.Name, func(pod *v1.Pod) {
		pod.OwnerReferences = []metav1.OwnerReference{*ownerReference}
	})

	if err := wait.PollImmediate(interval, timeout, func() (bool, error) {
		newPod, err := podClient.Get(context.TODO(), pod.Name, metav1.GetOptions{})
		if err != nil {
			return false, err
		}
		return metav1.GetControllerOf(newPod) != nil, nil
	}); err != nil {
		t.Fatalf("Failed to verify ControllerRef for the pod %s is not nil: %v", pod.Name, err)
	}

	newPod, err := podClient.Get(context.TODO(), pod.Name, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("Failed to obtain pod %s: %v", pod.Name, err)
	}
	controllerRef := metav1.GetControllerOf(newPod)
	if controllerRef.UID != rc.UID {
		t.Fatalf("RC owner of the pod %s has a different UID: Expected %v, got %v", newPod.Name, rc.UID, controllerRef.UID)
	}
	ownerReferenceNum := len(newPod.GetOwnerReferences())
	if ownerReferenceNum != expectedOwnerReferenceNum {
		t.Fatalf("Unexpected number of owner references for pod %s: Expected %d, got %d", newPod.Name, expectedOwnerReferenceNum, ownerReferenceNum)
	}
}

func setPodsReadyCondition(t *testing.T, clientSet clientset.Interface, pods *v1.PodList, conditionStatus v1.ConditionStatus, lastTransitionTime time.Time) {
	replicas := int32(len(pods.Items))
	var readyPods int32
	err := wait.PollImmediate(interval, timeout, func() (bool, error) {
		readyPods = 0
		for i := range pods.Items {
			pod := &pods.Items[i]
			if podutil.IsPodReady(pod) {
				readyPods++
				continue
			}
			pod.Status.Phase = v1.PodRunning
			_, condition := podutil.GetPodCondition(&pod.Status, v1.PodReady)
			if condition != nil {
				condition.Status = conditionStatus
				condition.LastTransitionTime = metav1.Time{Time: lastTransitionTime}
			} else {
				condition = &v1.PodCondition{
					Type:               v1.PodReady,
					Status:             conditionStatus,
					LastTransitionTime: metav1.Time{Time: lastTransitionTime},
				}
				pod.Status.Conditions = append(pod.Status.Conditions, *condition)
			}
			_, err := clientSet.CoreV1().Pods(pod.Namespace).UpdateStatus(context.TODO(), pod, metav1.UpdateOptions{})
			if err != nil {
				// 状态更新失败时，继续处理下一个 Pod
				continue
			}
			readyPods++
		}
		return readyPods >= replicas, nil
	})
	if err != nil {
		t.Fatalf("failed to mark all ReplicationController pods to ready: %v", err)
	}
}

func testScalingUsingScaleSubresource(t *testing.T, c clientset.Interface, rc *v1.ReplicationController, replicas int32) {
	ns := rc.Namespace
	rcClient := c.CoreV1().ReplicationControllers(ns)
	newRC, err := rcClient.Get(context.TODO(), rc.Name, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("Failed to obtain rc %s: %v", rc.Name, err)
	}
	scale, err := c.CoreV1().ReplicationControllers(ns).GetScale(context.TODO(), rc.Name, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("Failed to obtain scale subresource for rc %s: %v", rc.Name, err)
	}
	if scale.Spec.Replicas != *newRC.Spec.Replicas {
		t.Fatalf("Scale subresource for rc %s does not match .Spec.Replicas: expected %d, got %d", rc.Name, *newRC.Spec.Replicas, scale.Spec.Replicas)
	}

	if err := retry.RetryOnConflict(retry.DefaultBackoff, func() error {
		scale, err := c.CoreV1().ReplicationControllers(ns).GetScale(context.TODO(), rc.Name, metav1.GetOptions{})
		if err != nil {
			return err
		}
		scale.Spec.Replicas = replicas
		_, err = c.CoreV1().ReplicationControllers(ns).UpdateScale(context.TODO(), rc.Name, scale, metav1.UpdateOptions{})
		return err
	}); err != nil {
		t.Fatalf("Failed to set .Spec.Replicas of scale subresource for rc %s: %v", rc.Name, err)
	}

	newRC, err = rcClient.Get(context.TODO(), rc.Name, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("Failed to obtain rc %s: %v", rc.Name, err)
	}
	if *newRC.Spec.Replicas != replicas {
		t.Fatalf(".Spec.Replicas of rc %s does not match its scale subresource: expected %d, got %d", rc.Name, replicas, *newRC.Spec.Replicas)
	}
}

func TestAdoption(t *testing.T) {
	testCases := []struct {
		name                    string
		existingOwnerReferences func(rc *v1.ReplicationController) []metav1.OwnerReference
		expectedOwnerReferences func(rc *v1.ReplicationController) []metav1.OwnerReference
	}{
		{
			"pod refers rc as an owner, not a controller",
			func(rc *v1.ReplicationController) []metav1.OwnerReference {
				return []metav1.OwnerReference{{UID: rc.UID, Name: rc.Name, APIVersion: "v1", Kind: "ReplicationController"}}
			},
			func(rc *v1.ReplicationController) []metav1.OwnerReference {
				return []metav1.OwnerReference{{UID: rc.UID, Name: rc.Name, APIVersion: "v1", Kind: "ReplicationController", Controller: ptr.To(true), BlockOwnerDeletion: ptr.To(true)}}
			},
		},
		{
			"pod doesn't have owner references",
			func(rc *v1.ReplicationController) []metav1.OwnerReference {
				return []metav1.OwnerReference{}
			},
			func(rc *v1.ReplicationController) []metav1.OwnerReference {
				return []metav1.OwnerReference{{UID: rc.UID, Name: rc.Name, APIVersion: "v1", Kind: "ReplicationController", Controller: ptr.To(true), BlockOwnerDeletion: ptr.To(true)}}
			},
		},
		{
			"pod refers rc as a controller",
			func(rc *v1.ReplicationController) []metav1.OwnerReference {
				return []metav1.OwnerReference{{UID: rc.UID, Name: rc.Name, APIVersion: "v1", Kind: "ReplicationController", Controller: ptr.To(true)}}
			},
			func(rc *v1.ReplicationController) []metav1.OwnerReference {
				return []metav1.OwnerReference{{UID: rc.UID, Name: rc.Name, APIVersion: "v1", Kind: "ReplicationController", Controller: ptr.To(true)}}
			},
		},
		{
			"pod refers other rc as the controller, refers the rc as an owner",
			func(rc *v1.ReplicationController) []metav1.OwnerReference {
				return []metav1.OwnerReference{
					{UID: "1", Name: "anotherRC", APIVersion: "v1", Kind: "ReplicationController", Controller: ptr.To(true)},
					{UID: rc.UID, Name: rc.Name, APIVersion: "v1", Kind: "ReplicationController"},
				}
			},
			func(rc *v1.ReplicationController) []metav1.OwnerReference {
				return []metav1.OwnerReference{
					{UID: "1", Name: "anotherRC", APIVersion: "v1", Kind: "ReplicationController", Controller: ptr.To(true)},
					{UID: rc.UID, Name: rc.Name, APIVersion: "v1", Kind: "ReplicationController"},
				}
			},
		},
	}
	for i, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			tCtx, closeFn, rm, informers, clientSet := rmSetup(t)
			defer closeFn()
			ns := framework.CreateNamespaceOrDie(clientSet, fmt.Sprintf("rc-adoption-%d", i), t)
			defer framework.DeleteNamespaceOrDie(clientSet, ns, t)

			rcClient := clientSet.CoreV1().ReplicationControllers(ns.Name)
			podClient := clientSet.CoreV1().Pods(ns.Name)
			const rcName = "rc"
			rc, err := rcClient.Create(tCtx, newRC(rcName, ns.Name, 1), metav1.CreateOptions{})
			if err != nil {
				t.Fatalf("Failed to create replication controllers: %v", err)
			}
			podName := fmt.Sprintf("pod%d", i)
			pod := newMatchingPod(podName, ns.Name)
			pod.OwnerReferences = tc.existingOwnerReferences(rc)
			_, err = podClient.Create(tCtx, pod, metav1.CreateOptions{})
			if err != nil {
				t.Fatalf("Failed to create Pod: %v", err)
			}

			stopControllers := runControllerAndInformers(t, rm, informers, 1)
			defer stopControllers()
			if err := wait.PollImmediate(interval, timeout, func() (bool, error) {
				updatedPod, err := podClient.Get(tCtx, pod.Name, metav1.GetOptions{})
				if err != nil {
					return false, err
				}

				e, a := tc.expectedOwnerReferences(rc), updatedPod.OwnerReferences
				if reflect.DeepEqual(e, a) {
					return true, nil
				}

				t.Logf("ownerReferences don't match, expect %v, got %v", e, a)
				return false, nil
			}); err != nil {
				t.Fatalf("test %q failed: %v", tc.name, err)
			}
		})
	}
}

func TestSpecReplicasChange(t *testing.T) {
	tCtx, closeFn, rm, informers, c := rmSetup(t)
	defer closeFn()
	ns := framework.CreateNamespaceOrDie(c, "test-spec-replicas-change", t)
	defer framework.DeleteNamespaceOrDie(c, ns, t)
	stopControllers := runControllerAndInformers(t, rm, informers, 0)
	defer stopControllers()

	rc := newRC("rc", ns.Name, 2)
	rcs, _ := createRCsPods(t, c, []*v1.ReplicationController{rc}, []*v1.Pod{})
	rc = rcs[0]
	waitRCStable(t, c, rc)

	// 将 .Spec.Replicas 更新为 replicas，并验证 .Status.Replicas 相应变化
	scaleRC(t, c, rc, 3)
	scaleRC(t, c, rc, 0)
	scaleRC(t, c, rc, 2)

	// 修改 template 上的注解，以验证 RC 的 status 会更新
	// 且 .Spec.Replicas 不变
	rcClient := c.CoreV1().ReplicationControllers(ns.Name)
	var oldGeneration int64
	newRC := updateRC(t, rcClient, rc.Name, func(rc *v1.ReplicationController) {
		oldGeneration = rc.Generation
		rc.Spec.Template.Annotations = map[string]string{"test": "annotation"}
	})
	savedGeneration := newRC.Generation
	if savedGeneration == oldGeneration {
		t.Fatalf("Failed to verify .Generation has incremented for rc %s", rc.Name)
	}

	if err := wait.PollImmediate(interval, timeout, func() (bool, error) {
		newRC, err := rcClient.Get(tCtx, rc.Name, metav1.GetOptions{})
		if err != nil {
			return false, err
		}
		return newRC.Status.ObservedGeneration >= savedGeneration, nil
	}); err != nil {
		t.Fatalf("Failed to verify .Status.ObservedGeneration has incremented for rc %s: %v", rc.Name, err)
	}
}

func TestLogarithmicScaleDown(t *testing.T) {
	tCtx, closeFn, rm, informers, c := rmSetup(t)
	defer closeFn()
	ns := framework.CreateNamespaceOrDie(c, "test-spec-replicas-change", t)
	defer framework.DeleteNamespaceOrDie(c, ns, t)
	stopControllers := runControllerAndInformers(t, rm, informers, 0)
	defer stopControllers()

	rc := newRC("rc", ns.Name, 2)
	rcs, _ := createRCsPods(t, c, []*v1.ReplicationController{rc}, []*v1.Pod{})
	rc = rcs[0]
	waitRCStable(t, c, rc)

	// 获取集群中的 Pod 列表
	pods, err := c.CoreV1().Pods(ns.Name).List(tCtx, metav1.ListOptions{})
	if err != nil {
		t.Fatalf("failed to get pods in namespace %s: %+v", ns.Name, err)
	}

	// 等待 10 秒后扩容；新 Pod 的 logarithmic rank 应与前两个不同
	time.Sleep(10 * time.Second)
	scaleRC(t, c, rc, 3)

	// 再缩容回去，并确认命名空间中剩下的仍是原来的 Pod，
	// 即第三个已被删除
	scaleRC(t, c, rc, 2)

	newPods, err := c.CoreV1().Pods(ns.Name).List(tCtx, metav1.ListOptions{})
	if err != nil {
		t.Fatalf("failed to get pods in namespace %s: %+v", ns.Name, err)
	}

	if !apiequality.Semantic.DeepEqual(pods.Items, newPods.Items) {
		t.Fatalf("expected pods %+v, got %+v", pods.Items, newPods.Items)
	}
}

func TestDeletingAndFailedPods(t *testing.T) {
	tCtx, closeFn, rm, informers, c := rmSetup(t)
	defer closeFn()
	ns := framework.CreateNamespaceOrDie(c, "test-deleting-and-failed-pods", t)
	defer framework.DeleteNamespaceOrDie(c, ns, t)
	stopControllers := runControllerAndInformers(t, rm, informers, 0)
	defer stopControllers()

	rc := newRC("rc", ns.Name, 2)
	rcs, _ := createRCsPods(t, c, []*v1.ReplicationController{rc}, []*v1.Pod{})
	rc = rcs[0]
	waitRCStable(t, c, rc)

	// 验证 RC 创建了 2 个 Pod
	podClient := c.CoreV1().Pods(ns.Name)
	pods := getPods(t, podClient, labelMap())
	if len(pods.Items) != 2 {
		t.Fatalf("len(pods) = %d, want 2", len(pods.Items))
	}

	// 将第一个 Pod 设为待删除的 Pod
	// 为该 Pod 设置 finalizer，以模拟删除中的状态
	deletingPod := &pods.Items[0]
	updatePod(t, podClient, deletingPod.Name, func(pod *v1.Pod) {
		pod.Finalizers = []string{"fake.example.com/blockDeletion"}
	})
	if err := c.CoreV1().Pods(ns.Name).Delete(tCtx, deletingPod.Name, metav1.DeleteOptions{}); err != nil {
		t.Fatalf("Error deleting pod %s: %v", deletingPod.Name, err)
	}

	// 将第二个 Pod 设为失败 Pod
	failedPod := &pods.Items[1]
	updatePodStatus(t, podClient, failedPod, func(pod *v1.Pod) {
		pod.Status.Phase = v1.PodFailed
	})

	// 轮询等待创建 2 个新 Pod，以替换待删除和失败的 Pod
	if err := wait.PollImmediate(interval, timeout, func() (bool, error) {
		pods = getPods(t, podClient, labelMap())
		return len(pods.Items) == 4, nil
	}); err != nil {
		t.Fatalf("Failed to verify 2 new pods have been created (expected 4 pods): %v", err)
	}

	// 验证这四个 Pod 中包含待删除和失败的 Pod
	foundDeletingPod := false
	foundFailedPod := false
	for _, pod := range pods.Items {
		if pod.UID == deletingPod.UID {
			foundDeletingPod = true
		}
		if pod.UID == failedPod.UID {
			foundFailedPod = true
		}
	}
	// 验证待删除的 Pod 存在
	if !foundDeletingPod {
		t.Fatalf("expected deleting pod %s exists, but it is not found", deletingPod.Name)
	}
	// 验证失败的 Pod 存在
	if !foundFailedPod {
		t.Fatalf("expected failed pod %s exists, but it is not found", failedPod.Name)
	}
}

func TestOverlappingRCs(t *testing.T) {
	tCtx, closeFn, rm, informers, c := rmSetup(t)
	defer closeFn()
	ns := framework.CreateNamespaceOrDie(c, "test-overlapping-rcs", t)
	defer framework.DeleteNamespaceOrDie(c, ns, t)
	stopControllers := runControllerAndInformers(t, rm, informers, 0)
	defer stopControllers()

	// 创建 2 个 selector 完全相同的 RC
	for i := range 2 {
		// 一个 RC 有 1 个副本，另一个有 2 个
		rc := newRC(fmt.Sprintf("rc-%d", i+1), ns.Name, i+1)
		rcs, _ := createRCsPods(t, c, []*v1.ReplicationController{rc}, []*v1.Pod{})
		waitRCStable(t, c, rcs[0])
	}

	// 预期共创建 3 个 Pod
	podClient := c.CoreV1().Pods(ns.Name)
	pods := getPods(t, podClient, labelMap())
	if len(pods.Items) != 3 {
		t.Errorf("len(pods) = %d, want 3", len(pods.Items))
	}

	// 预期两个 RC 的 .status.replicas 均等于 .spec.replicas
	for i := range 2 {
		newRC, err := c.CoreV1().ReplicationControllers(ns.Name).Get(tCtx, fmt.Sprintf("rc-%d", i+1), metav1.GetOptions{})
		if err != nil {
			t.Fatalf("failed to obtain rc rc-%d: %v", i+1, err)
		}
		if newRC.Status.Replicas != *newRC.Spec.Replicas {
			t.Fatalf(".Status.Replicas %d is not equal to .Spec.Replicas %d", newRC.Status.Replicas, *newRC.Spec.Replicas)
		}
	}
}

func TestPodOrphaningAndAdoptionWhenLabelsChange(t *testing.T) {
	tCtx, closeFn, rm, informers, c := rmSetup(t)
	defer closeFn()
	ns := framework.CreateNamespaceOrDie(c, "test-pod-orphaning-and-adoption-when-labels-change", t)
	defer framework.DeleteNamespaceOrDie(c, ns, t)
	stopControllers := runControllerAndInformers(t, rm, informers, 0)
	defer stopControllers()

	rc := newRC("rc", ns.Name, 1)
	rcs, _ := createRCsPods(t, c, []*v1.ReplicationController{rc}, []*v1.Pod{})
	rc = rcs[0]
	waitRCStable(t, c, rc)

	// 孤立：当 Pod 的标签不再匹配 RC 的 selector 时，RC 应移除其 OwnerReference
	podClient := c.CoreV1().Pods(ns.Name)
	pods := getPods(t, podClient, labelMap())
	if len(pods.Items) != 1 {
		t.Fatalf("len(pods) = %d, want 1", len(pods.Items))
	}
	pod := &pods.Items[0]

	// 先验证该 Pod 的 ControllerRef 非空
	if metav1.GetControllerOf(pod) == nil {
		t.Fatalf("ControllerRef of pod %s is nil", pod.Name)
	}
	newLabelMap := map[string]string{"new-foo": "new-bar"}
	updatePod(t, podClient, pod.Name, func(pod *v1.Pod) {
		pod.Labels = newLabelMap
	})
	if err := wait.PollImmediate(interval, timeout, func() (bool, error) {
		newPod, err := podClient.Get(tCtx, pod.Name, metav1.GetOptions{})
		if err != nil {
			return false, err
		}
		pod = newPod
		return metav1.GetControllerOf(newPod) == nil, nil
	}); err != nil {
		t.Fatalf("Failed to verify ControllerRef for the pod %s is nil: %v", pod.Name, err)
	}

	// 接管：当 Pod 的标签变得匹配 RC 的 selector 时，RC 应为其添加 ControllerRef
	updatePod(t, podClient, pod.Name, func(pod *v1.Pod) {
		pod.Labels = labelMap()
	})
	if err := wait.PollImmediate(interval, timeout, func() (bool, error) {
		newPod, err := podClient.Get(tCtx, pod.Name, metav1.GetOptions{})
		if err != nil {
			// 如果找不到该 Pod，说明 RC 选中它进行删除（它是多余的）
			// 验证命名空间中只剩一个 Pod，且其 ControllerRef 指向该 RC
			if !apierrors.IsNotFound(err) {
				return false, err
			}

			pods := getPods(t, podClient, labelMap())
			if len(pods.Items) != 1 {
				return false, fmt.Errorf("Expected 1 pod in current namespace, got %d", len(pods.Items))
			}
			// 相应地修改该 Pod
			pod = &pods.Items[0]
			return true, nil
		}
		// 始终更新 Pod，这样后面可以少调用一次 API server 的 GET
		pod = newPod
		// 如果找到了该 Pod，验证其带有 ControllerRef
		return metav1.GetControllerOf(newPod) != nil, nil
	}); err != nil {
		t.Fatalf("Failed to verify ControllerRef for pod %s is not nil: %v", pod.Name, err)
	}
	// 验证该 Pod 的 ControllerRef 指向该 RC
	// 若 Pod 为 nil（即已被选中删除），则不做任何操作
	if pod != nil {
		controllerRef := metav1.GetControllerOf(pod)
		if controllerRef.UID != rc.UID {
			t.Fatalf("RC owner of the pod %s has a different UID: Expected %v, got %v", pod.Name, rc.UID, controllerRef.UID)
		}
	}
}

func TestGeneralPodAdoption(t *testing.T) {
	_, closeFn, rm, informers, c := rmSetup(t)
	defer closeFn()
	ns := framework.CreateNamespaceOrDie(c, "test-general-pod-adoption", t)
	defer framework.DeleteNamespaceOrDie(c, ns, t)
	stopControllers := runControllerAndInformers(t, rm, informers, 0)
	defer stopControllers()

	rc := newRC("rc", ns.Name, 1)
	rcs, _ := createRCsPods(t, c, []*v1.ReplicationController{rc}, []*v1.Pod{})
	rc = rcs[0]
	waitRCStable(t, c, rc)

	podClient := c.CoreV1().Pods(ns.Name)
	pods := getPods(t, podClient, labelMap())
	if len(pods.Items) != 1 {
		t.Fatalf("len(pods) = %d, want 1", len(pods.Items))
	}
	pod := &pods.Items[0]
	var falseVar = false

	// 当 Pod 唯一的 OwnerReference 指向其他类型的 API 对象（如 statefulset）
	// 且 Controller=false 时，RC 应添加第二个指向自身的 OwnerReference（ControllerRef）
	// 且 Controller=true。
	ownerReference := metav1.OwnerReference{UID: uuid.NewUUID(), APIVersion: "apps/v1beta1", Kind: "StatefulSet", Name: rc.Name, Controller: &falseVar}
	testPodControllerRefPatch(t, c, pod, &ownerReference, rc, 2)

	// 当 Pod 唯一的 OwnerReference 指向 RC，但 Controller=false 时
	ownerReference = metav1.OwnerReference{UID: rc.UID, APIVersion: "v1", Kind: "ReplicationController", Name: rc.Name, Controller: &falseVar}
	testPodControllerRefPatch(t, c, pod, &ownerReference, rc, 1)
}

func TestReadyAndAvailableReplicas(t *testing.T) {
	tCtx, closeFn, rm, informers, c := rmSetup(t)
	defer closeFn()
	ns := framework.CreateNamespaceOrDie(c, "test-ready-and-available-replicas", t)
	defer framework.DeleteNamespaceOrDie(c, ns, t)
	stopControllers := runControllerAndInformers(t, rm, informers, 0)
	defer stopControllers()

	rc := newRC("rc", ns.Name, 3)
	rc.Spec.MinReadySeconds = 3600
	rcs, _ := createRCsPods(t, c, []*v1.ReplicationController{rc}, []*v1.Pod{})
	rc = rcs[0]
	waitRCStable(t, c, rc)

	// 先验证没有 Pod 处于 Available 状态
	if rc.Status.AvailableReplicas != 0 {
		t.Fatalf("Unexpected .Status.AvailableReplicas: Expected 0, saw %d", rc.Status.AvailableReplicas)
	}

	podClient := c.CoreV1().Pods(ns.Name)
	pods := getPods(t, podClient, labelMap())
	if len(pods.Items) != 3 {
		t.Fatalf("len(pods) = %d, want 3", len(pods.Items))
	}

	// 将 3 个 Pod 分成各自的列表
	firstPodList := &v1.PodList{Items: pods.Items[:1]}
	secondPodList := &v1.PodList{Items: pods.Items[1:2]}
	thirdPodList := &v1.PodList{Items: pods.Items[2:]}
	// 第一个 Pod：Running，但未 Ready
	// 将 Ready condition 设为 false，LastTransitionTime 设为当前时间
	setPodsReadyCondition(t, c, firstPodList, v1.ConditionFalse, time.Now())
	// 第二个 Pod：Running 且 Ready，但并非 Available
	// 将 LastTransitionTime 设为当前时间
	setPodsReadyCondition(t, c, secondPodList, v1.ConditionTrue, time.Now())
	// 第三个 Pod：Running、Ready 且 Available
	// 将 LastTransitionTime 设为 3600 秒之前
	setPodsReadyCondition(t, c, thirdPodList, v1.ConditionTrue, time.Now().Add(-120*time.Minute))

	rcClient := c.CoreV1().ReplicationControllers(ns.Name)
	if err := wait.PollImmediate(interval, timeout, func() (bool, error) {
		newRC, err := rcClient.Get(tCtx, rc.Name, metav1.GetOptions{})
		if err != nil {
			return false, err
		}
		// 验证存在 3 个 Pod：2 个 Ready，1 个 Available
		return newRC.Status.Replicas == 3 && newRC.Status.ReadyReplicas == 2 && newRC.Status.AvailableReplicas == 1, nil
	}); err != nil {
		t.Fatalf("Failed to verify number of Replicas, ReadyReplicas and AvailableReplicas of rc %s to be as expected: %v", rc.Name, err)
	}
}

func TestRCScaleSubresource(t *testing.T) {
	_, closeFn, rm, informers, c := rmSetup(t)
	defer closeFn()
	ns := framework.CreateNamespaceOrDie(c, "test-rc-scale-subresource", t)
	defer framework.DeleteNamespaceOrDie(c, ns, t)
	stopControllers := runControllerAndInformers(t, rm, informers, 0)
	defer stopControllers()

	rc := newRC("rc", ns.Name, 1)
	rcs, _ := createRCsPods(t, c, []*v1.ReplicationController{rc}, []*v1.Pod{})
	rc = rcs[0]
	waitRCStable(t, c, rc)

	// 使用 scale 子资源将 .Spec.Replicas 扩容到 3
	testScalingUsingScaleSubresource(t, c, rc, 3)
	// 使用 scale 子资源将 .Spec.Replicas 缩容到 0
	testScalingUsingScaleSubresource(t, c, rc, 0)
}

func TestExtraPodsAdoptionAndDeletion(t *testing.T) {
	_, closeFn, rm, informers, c := rmSetup(t)
	defer closeFn()
	ns := framework.CreateNamespaceOrDie(c, "test-extra-pods-adoption-and-deletion", t)
	defer framework.DeleteNamespaceOrDie(c, ns, t)

	rc := newRC("rc", ns.Name, 2)
	// 创建 3 个 Pod，RC 应只接管其中 2 个
	podList := []*v1.Pod{}
	for i := range 3 {
		pod := newMatchingPod(fmt.Sprintf("pod-%d", i+1), ns.Name)
		pod.Labels = labelMap()
		podList = append(podList, pod)
	}
	rcs, _ := createRCsPods(t, c, []*v1.ReplicationController{rc}, podList)
	rc = rcs[0]
	stopControllers := runControllerAndInformers(t, rm, informers, 3)
	defer stopControllers()
	waitRCStable(t, c, rc)

	// 通过判断命名空间内 Pod 总数是否等于 RC 的 .spec.replicas（本例为 2），
	// 验证多余的 Pod 最终被删除
	podClient := c.CoreV1().Pods(ns.Name)
	if err := wait.PollImmediate(interval, timeout, func() (bool, error) {
		// 所有 Pod 的标签均为 labelMap
		pods := getPods(t, podClient, labelMap())
		return int32(len(pods.Items)) == *rc.Spec.Replicas, nil
	}); err != nil {
		t.Fatalf("Failed to verify number of all pods within current namespace matches .spec.replicas of rc %s: %v", rc.Name, err)
	}
}

func TestFullyLabeledReplicas(t *testing.T) {
	tCtx, closeFn, rm, informers, c := rmSetup(t)
	defer closeFn()
	ns := framework.CreateNamespaceOrDie(c, "test-fully-labeled-replicas", t)
	defer framework.DeleteNamespaceOrDie(c, ns, t)
	stopControllers := runControllerAndInformers(t, rm, informers, 0)
	defer stopControllers()

	extraLabelMap := map[string]string{"foo": "bar", "extraKey": "extraValue"}
	rc := newRC("rc", ns.Name, 2)
	rcs, _ := createRCsPods(t, c, []*v1.ReplicationController{rc}, []*v1.Pod{})
	rc = rcs[0]
	waitRCStable(t, c, rc)

	// 给 RC 的 template 增加额外标签，但不改其 selector
	rcClient := c.CoreV1().ReplicationControllers(ns.Name)
	updateRC(t, rcClient, rc.Name, func(rc *v1.ReplicationController) {
		rc.Spec.Template.Labels = extraLabelMap
	})

	// 给其中一个 Pod 设置额外标签
	podClient := c.CoreV1().Pods(ns.Name)
	pods := getPods(t, podClient, labelMap())
	if len(pods.Items) != 2 {
		t.Fatalf("len(pods) = %d, want 2", len(pods.Items))
	}
	fullyLabeledPod := &pods.Items[0]
	updatePod(t, podClient, fullyLabeledPod.Name, func(pod *v1.Pod) {
		pod.Labels = extraLabelMap
	})

	// 验证只有一个 Pod 带有完整标签
	if err := wait.PollImmediate(interval, timeout, func() (bool, error) {
		newRC, err := rcClient.Get(tCtx, rc.Name, metav1.GetOptions{})
		if err != nil {
			return false, err
		}
		return (newRC.Status.Replicas == 2 && newRC.Status.FullyLabeledReplicas == 1), nil
	}); err != nil {
		t.Fatalf("Failed to verify only one pod is fully labeled: %v", err)
	}
}
