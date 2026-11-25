/*
Copyright 2018 The Kubernetes Authors.

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
	"strconv"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	appsv1 "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/json"
	"k8s.io/apimachinery/pkg/util/wait"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/informers"
	clientset "k8s.io/client-go/kubernetes"
	restclient "k8s.io/client-go/rest"
	featuregatetesting "k8s.io/component-base/featuregate/testing"
	apiservertesting "k8s.io/kubernetes/cmd/kube-apiserver/app/testing"
	podutil "k8s.io/kubernetes/pkg/api/v1/pod"
	"k8s.io/kubernetes/pkg/controller/statefulset"
	"k8s.io/kubernetes/pkg/controlplane"
	"k8s.io/kubernetes/pkg/features"
	"k8s.io/kubernetes/test/integration/framework"
	"k8s.io/kubernetes/test/utils/ktesting"
	"k8s.io/utils/ptr"
)

const (
	interval = 100 * time.Millisecond
	timeout  = 60 * time.Second
)

// TestVolumeTemplateNoopUpdate 验证带 PVC 模板的 StatefulSet 用原 spec 更新时应被当作 no-op 通过校验
func TestVolumeTemplateNoopUpdate(t *testing.T) {
	// 用默认存储配置启动 server
	server := apiservertesting.StartTestServerOrDie(t, nil, framework.DefaultTestServerFlags(), framework.SharedEtcd())
	defer server.TearDownFn()

	c, err := dynamic.NewForConfig(server.ClientConfig)
	if err != nil {
		t.Fatal(err)
	}

	// 用 unstructured client 原样提交 JSON，避免序列化改动内嵌 PVC 模板的字节
	sts := &unstructured.Unstructured{}
	err = json.Unmarshal([]byte(`{
		"apiVersion": "apps/v1",
		"kind": "StatefulSet",
		"metadata": {"name": "web"},
		"spec": {
		  "selector": {"matchLabels": {"app": "nginx"}},
		  "serviceName": "nginx",
		  "replicas": 3,
		  "template": {
			"metadata": {"labels": {"app": "nginx"}},
			"spec": {
			  "terminationGracePeriodSeconds": 10,
			  "containers": [{
				  "name": "nginx",
				  "image": "registry.k8s.io/nginx-slim:0.8",
				  "ports": [{"containerPort": 80,"name": "web"}],
				  "volumeMounts": [{"name": "www","mountPath": "/usr/share/nginx/html"}]
			  }]
			}
		  },
		  "volumeClaimTemplates": [{
			  "apiVersion": "v1",
			  "kind": "PersistentVolumeClaim",
			  "metadata": {"name": "www"},
			  "spec": {
				"accessModes": ["ReadWriteOnce"],
				"storageClassName": "my-storage-class",
				"resources": {"requests": {"storage": "1Gi"}}
			  }
			}
		  ]
		}
	  }`), &sts.Object)
	if err != nil {
		t.Fatal(err)
	}

	stsClient := c.Resource(appsv1.SchemeGroupVersion.WithResource("statefulsets")).Namespace("default")

	// 创建 StatefulSet
	persistedSTS, err := stsClient.Create(context.TODO(), sts, metav1.CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}

	// 用原始 spec 更新：默认值填充结果应一致，校验将此视为 no-op
	originalSpec, ok, err := unstructured.NestedFieldCopy(sts.Object, "spec")
	if err != nil || !ok {
		t.Fatal(err, ok)
	}
	err = unstructured.SetNestedField(persistedSTS.Object, originalSpec, "spec")
	if err != nil {
		t.Fatal(err)
	}
	_, err = stsClient.Update(context.TODO(), persistedSTS, metav1.UpdateOptions{})
	if err != nil {
		t.Fatal(err)
	}
}

func TestSpecReplicasChange(t *testing.T) {
	tCtx, closeFn, rm, informers, c := scSetup(t)
	defer closeFn()
	ns := framework.CreateNamespaceOrDie(c, "test-spec-replicas-change", t)
	defer framework.DeleteNamespaceOrDie(c, ns, t)
	cancel := runControllerAndInformers(tCtx, rm, informers)
	defer cancel()

	createHeadlessService(t, c, newHeadlessService(ns.Name))
	sts := newSTS("sts", ns.Name, 2)
	stss, _ := createSTSsPods(t, c, []*appsv1.StatefulSet{sts}, []*v1.Pod{})
	sts = stss[0]
	waitSTSStable(t, c, sts)

	// 修改 .Spec.Replicas，验证 .Status.Replicas 随之更新
	scaleSTS(t, c, sts, 3)
	scaleSTS(t, c, sts, 0)
	scaleSTS(t, c, sts, 2)

	// 改动模板 annotation，验证即使 .Spec.Replicas 不变，status 也会更新（ObservedGeneration 推进）
	stsClient := c.AppsV1().StatefulSets(ns.Name)
	var oldGeneration int64
	newSTS := updateSTS(t, stsClient, sts.Name, func(sts *appsv1.StatefulSet) {
		oldGeneration = sts.Generation
		sts.Spec.Template.Annotations = map[string]string{"test": "annotation"}
	})
	savedGeneration := newSTS.Generation
	if savedGeneration == oldGeneration {
		t.Fatalf("failed to verify .Generation has incremented for sts %s", sts.Name)
	}

	if err := wait.PollImmediate(pollInterval, pollTimeout, func() (bool, error) {
		newSTS, err := stsClient.Get(context.TODO(), sts.Name, metav1.GetOptions{})
		if err != nil {
			return false, err
		}
		return newSTS.Status.ObservedGeneration >= savedGeneration, nil
	}); err != nil {
		t.Fatalf("failed to verify .Status.ObservedGeneration has incremented for sts %s: %v", sts.Name, err)
	}
}

func TestDeletingAndTerminatingPods(t *testing.T) {
	tCtx, closeFn, rm, informers, c := scSetup(t)
	defer closeFn()
	ns := framework.CreateNamespaceOrDie(c, "test-deleting-and-failed-pods", t)
	defer framework.DeleteNamespaceOrDie(c, ns, t)
	cancel := runControllerAndInformers(tCtx, rm, informers)
	defer cancel()

	podCount := 3

	labelMap := labelMap()
	sts := newSTS("sts", ns.Name, podCount)
	stss, _ := createSTSsPods(t, c, []*appsv1.StatefulSet{sts}, []*v1.Pod{})
	sts = stss[0]
	waitSTSStable(t, c, sts)

	// 验证 STS 创建了 3 个 Pod
	podClient := c.CoreV1().Pods(ns.Name)
	pods := getPods(t, podClient, labelMap)
	if len(pods.Items) != podCount {
		t.Fatalf("len(pods) = %d, want %d", len(pods.Items), podCount)
	}

	// 给第一个 Pod 加 finalizer，模拟正在删除但尚未消失的状态
	deletingPod := &pods.Items[0]
	updatePod(t, podClient, deletingPod.Name, func(pod *v1.Pod) {
		pod.Finalizers = []string{"fake.example.com/blockDeletion"}
	})
	if err := c.CoreV1().Pods(ns.Name).Delete(context.TODO(), deletingPod.Name, metav1.DeleteOptions{}); err != nil {
		t.Fatalf("error deleting pod %s: %v", deletingPod.Name, err)
	}

	// 将第二个 Pod 置为 Failed
	failedPod := &pods.Items[1]
	updatePodStatus(t, podClient, failedPod.Name, func(pod *v1.Pod) {
		pod.Status.Phase = v1.PodFailed
	})

	// 将第三个 Pod 置为 Succeeded
	succeededPod := &pods.Items[2]
	updatePodStatus(t, podClient, succeededPod.Name, func(pod *v1.Pod) {
		pod.Status.Phase = v1.PodSucceeded
	})

	exists := func(pods []v1.Pod, uid types.UID) bool {
		for _, pod := range pods {
			if pod.UID == uid {
				return true
			}
		}
		return false
	}

	if err := wait.PollImmediate(pollInterval, pollTimeout, func() (bool, error) {
		// 应始终有 3 个 Pod：删除中的 Pod 仍在，Failed/Succeeded 的已被替换
		pods = getPods(t, podClient, labelMap)
		if len(pods.Items) != podCount {
			return false, nil
		}

		// 删除中的 Pod 必须先于替换它的新 Pod 存在，否则直接判失败
		if !exists(pods.Items, deletingPod.UID) {
			return false, fmt.Errorf("expected deleting pod %s still exists, but it is not found", deletingPod.Name)
		}
		// Failed Pod 应已被替换
		if exists(pods.Items, failedPod.UID) {
			return false, nil
		}
		// Succeeded Pod 应已被替换
		if exists(pods.Items, succeededPod.UID) {
			return false, nil
		}
		// 替换后的 Pod 都应处于非终态
		for _, pod := range pods.Items {
			if pod.Status.Phase == v1.PodFailed || pod.Status.Phase == v1.PodSucceeded {
				return false, nil
			}
		}
		return true, nil
	}); err != nil {
		t.Fatalf("failed to verify failed pod %s has been replaced with a new non-failed pod, and deleting pod %s survives: %v", failedPod.Name, deletingPod.Name, err)
	}

	// 移除 finalizer，让删除真正完成
	updatePod(t, podClient, deletingPod.Name, func(pod *v1.Pod) {
		pod.Finalizers = []string{}
	})

	if err := wait.PollImmediate(pollInterval, pollTimeout, func() (bool, error) {
		// 删除完成后控制器应再建一个新 Pod 补齐数量
		pods = getPods(t, podClient, labelMap)
		if len(pods.Items) != podCount {
			return false, nil
		}
		// 确认原删除中的 Pod 已被替换
		return !exists(pods.Items, deletingPod.UID), nil
	}); err != nil {
		t.Fatalf("failed to verify deleting pod %s has been replaced with a new non-deleting pod: %v", deletingPod.Name, err)
	}
}

func TestStatefulSetAvailable(t *testing.T) {
	tests := []struct {
		name           string
		totalReplicas  int32
		readyReplicas  int32
		activeReplicas int32
	}{
		{
			name:           "only certain replicas would become active",
			totalReplicas:  4,
			readyReplicas:  3,
			activeReplicas: 2,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			tCtx, closeFn, rm, informers, c := scSetup(t)
			defer closeFn()
			ns := framework.CreateNamespaceOrDie(c, "test-available-pods", t)
			defer framework.DeleteNamespaceOrDie(c, ns, t)
			cancel := runControllerAndInformers(tCtx, rm, informers)
			defer cancel()

			labelMap := labelMap()
			sts := newSTS("sts", ns.Name, 4)
			sts.Spec.MinReadySeconds = int32(3600)
			stss, _ := createSTSsPods(t, c, []*appsv1.StatefulSet{sts}, []*v1.Pod{})
			sts = stss[0]
			waitSTSStable(t, c, sts)

			// 验证 STS 创建了 4 个 Pod
			podClient := c.CoreV1().Pods(ns.Name)
			pods := getPods(t, podClient, labelMap)
			if len(pods.Items) != 4 {
				t.Fatalf("len(pods) = %d, want 4", len(pods.Items))
			}

			// 将 Pod 分成三组，用 LastTransitionTime 区分 Ready/Available 状态
			firstPodList := &v1.PodList{Items: pods.Items[:1]}
			secondPodList := &v1.PodList{Items: pods.Items[1:2]}
			thirdPodList := &v1.PodList{Items: pods.Items[2:]}
			// 第一个 Running 但未 Ready：Ready 条件为 false
			setPodsReadyCondition(t, c, firstPodList, v1.ConditionFalse, time.Now())
			// 第二个 Ready 但未 Available：LastTransitionTime 设为当前
			setPodsReadyCondition(t, c, secondPodList, v1.ConditionTrue, time.Now())
			// 第三个 Ready 且 Available：LastTransitionTime 早于 MinReadySeconds
			setPodsReadyCondition(t, c, thirdPodList, v1.ConditionTrue, time.Now().Add(-120*time.Minute))

			stsClient := c.AppsV1().StatefulSets(ns.Name)
			if err := wait.PollImmediate(interval, timeout, func() (bool, error) {
				newSts, err := stsClient.Get(context.TODO(), sts.Name, metav1.GetOptions{})
				if err != nil {
					return false, err
				}
				// 期望 4 个 Pod，其中 3 个 Ready、2 个 Available
				return newSts.Status.Replicas == test.totalReplicas && newSts.Status.ReadyReplicas == test.readyReplicas && newSts.Status.AvailableReplicas == test.activeReplicas, nil
			}); err != nil {
				t.Fatalf("Failed to verify number of Replicas, ReadyReplicas and AvailableReplicas of rs %s to be as expected: %v", sts.Name, err)
			}
		})
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
				// 某个 Pod 状态更新失败不影响其余 Pod，继续处理下一个
				continue
			}
			readyPods++
		}
		return readyPods >= replicas, nil
	})
	if err != nil {
		t.Fatalf("failed to mark all StatefulSet pods to ready: %v", err)
	}
}

// 为 https://github.com/kubernetes/kubernetes/issues/108837 补充的回归用例
func TestStatefulSetStatusWithPodFail(t *testing.T) {
	tCtx := ktesting.Init(t)
	limitedPodNumber := 2
	c, config, closeFn := framework.StartTestServer(tCtx, t, framework.TestServerSetup{
		ModifyServerConfig: func(config *controlplane.Config) {
			config.ControlPlane.Generic.AdmissionControl = &fakePodFailAdmission{
				limitedPodNumber: limitedPodNumber,
			}
		},
	})
	defer closeFn()
	defer tCtx.Cancel("test has completed")
	resyncPeriod := 12 * time.Hour
	informers := informers.NewSharedInformerFactory(clientset.NewForConfigOrDie(restclient.AddUserAgent(config, "statefulset-informers")), resyncPeriod)
	ssc := statefulset.NewStatefulSetController(
		tCtx,
		informers.Core().V1().Pods(),
		informers.Apps().V1().StatefulSets(),
		informers.Core().V1().PersistentVolumeClaims(),
		informers.Apps().V1().ControllerRevisions(),
		clientset.NewForConfigOrDie(restclient.AddUserAgent(config, "statefulset-controller")),
	)

	ns := framework.CreateNamespaceOrDie(c, "test-pod-fail", t)
	defer framework.DeleteNamespaceOrDie(c, ns, t)

	informers.Start(tCtx.Done())
	go ssc.Run(tCtx, 5)

	sts := newSTS("sts", ns.Name, 4)
	_, err := c.AppsV1().StatefulSets(sts.Namespace).Create(tCtx, sts, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("Could not create statefulSet %s: %v", sts.Name, err)
	}

	wantReplicas := limitedPodNumber
	var gotReplicas int32
	if err := wait.PollImmediate(pollInterval, pollTimeout, func() (bool, error) {
		newSTS, err := c.AppsV1().StatefulSets(sts.Namespace).Get(tCtx, sts.Name, metav1.GetOptions{})
		if err != nil {
			return false, err
		}
		gotReplicas = newSTS.Status.Replicas
		return gotReplicas == int32(wantReplicas), nil
	}); err != nil {
		t.Fatalf("StatefulSet %s status has %d replicas, want replicas %d: %v", sts.Name, gotReplicas, wantReplicas, err)
	}
}

func TestAutodeleteOwnerRefs(t *testing.T) {
	tests := []struct {
		namespace         string
		policy            appsv1.StatefulSetPersistentVolumeClaimRetentionPolicy
		expectPodOwnerRef bool
		expectSetOwnerRef bool
	}{
		{
			namespace: "always-retain",
			policy: appsv1.StatefulSetPersistentVolumeClaimRetentionPolicy{
				WhenDeleted: appsv1.RetainPersistentVolumeClaimRetentionPolicyType,
				WhenScaled:  appsv1.RetainPersistentVolumeClaimRetentionPolicyType,
			},
			expectPodOwnerRef: false,
			expectSetOwnerRef: false,
		},
		{
			namespace: "delete-on-scaledown-only",
			policy: appsv1.StatefulSetPersistentVolumeClaimRetentionPolicy{
				WhenDeleted: appsv1.RetainPersistentVolumeClaimRetentionPolicyType,
				WhenScaled:  appsv1.DeletePersistentVolumeClaimRetentionPolicyType,
			},
			expectPodOwnerRef: true,
			expectSetOwnerRef: false,
		},
		{
			namespace: "delete-with-set-only",
			policy: appsv1.StatefulSetPersistentVolumeClaimRetentionPolicy{
				WhenDeleted: appsv1.DeletePersistentVolumeClaimRetentionPolicyType,
				WhenScaled:  appsv1.RetainPersistentVolumeClaimRetentionPolicyType,
			},
			expectPodOwnerRef: false,
			expectSetOwnerRef: true,
		},
		{
			namespace: "always-delete",
			policy: appsv1.StatefulSetPersistentVolumeClaimRetentionPolicy{
				WhenDeleted: appsv1.DeletePersistentVolumeClaimRetentionPolicyType,
				WhenScaled:  appsv1.DeletePersistentVolumeClaimRetentionPolicyType,
			},
			expectPodOwnerRef: true,
			expectSetOwnerRef: true,
		},
	}

	tCtx, closeFn, rm, informers, c := scSetup(t)
	defer closeFn()
	cancel := runControllerAndInformers(tCtx, rm, informers)
	defer cancel()

	for _, test := range tests {
		t.Run(test.namespace, func(t *testing.T) {
			ns := framework.CreateNamespaceOrDie(c, test.namespace, t)
			defer framework.DeleteNamespaceOrDie(c, ns, t)

			sts := newSTS("sts", ns.Name, 3)
			sts.Spec.PersistentVolumeClaimRetentionPolicy = &test.policy
			stss, _ := createSTSsPods(t, c, []*appsv1.StatefulSet{sts}, []*v1.Pod{})
			sts = stss[0]
			waitSTSStable(t, c, sts)

			// 验证 StatefulSet ownerRef 按 retention policy 正确添加
			pvcClient := c.CoreV1().PersistentVolumeClaims(ns.Name)
			pvcs := getStatefulSetPVCs(t, pvcClient, sts)
			for _, pvc := range pvcs {
				verifyOwnerRef(t, pvc, "StatefulSet", test.expectSetOwnerRef)
				verifyOwnerRef(t, pvc, "Pod", false)
			}

			// 缩容到 1 个 Pod，验证被缩掉副本的 PVC 的 Pod ownerRef
			one := int32(1)
			sts.Spec.Replicas = &one
			waitSTSStable(t, c, sts)

			pvcs = getStatefulSetPVCs(t, pvcClient, sts)
			for i, pvc := range pvcs {
				verifyOwnerRef(t, pvc, "StatefulSet", test.expectSetOwnerRef)
				if i == 0 {
					verifyOwnerRef(t, pvc, "Pod", false)
				} else {
					verifyOwnerRef(t, pvc, "Pod", test.expectPodOwnerRef)
				}
			}
		})
	}
}

func TestDeletingPodForRollingUpdatePartition(t *testing.T) {
	tCtx, closeFn, rm, informers, c := scSetup(t)
	defer closeFn()
	ns := framework.CreateNamespaceOrDie(c, "test-deleting-pod-for-rolling-update-partition", t)
	defer framework.DeleteNamespaceOrDie(c, ns, t)
	cancel := runControllerAndInformers(tCtx, rm, informers)
	defer cancel()

	labelMap := labelMap()
	sts := newSTS("sts", ns.Name, 2)
	sts.Spec.UpdateStrategy = appsv1.StatefulSetUpdateStrategy{
		Type: appsv1.RollingUpdateStatefulSetStrategyType,
		RollingUpdate: func() *appsv1.RollingUpdateStatefulSetStrategy {
			return &appsv1.RollingUpdateStatefulSetStrategy{
				Partition: ptr.To[int32](1),
			}
		}(),
	}
	stss, _ := createSTSsPods(t, c, []*appsv1.StatefulSet{sts}, []*v1.Pod{})
	sts = stss[0]
	waitSTSStable(t, c, sts)

	// 验证 STS 创建了 2 个 Pod
	podClient := c.CoreV1().Pods(ns.Name)
	pods := getPods(t, podClient, labelMap)
	if len(pods.Items) != 2 {
		t.Fatalf("len(pods) = %d, want 2", len(pods.Items))
	}
	// 将两个 Pod 都置为 Running、Ready 且 Available
	setPodsReadyCondition(t, c, &v1.PodList{Items: pods.Items}, v1.ConditionTrue, time.Now())

	// 1. 发布新镜像
	oldImage := sts.Spec.Template.Spec.Containers[0].Image
	newImage := "new-image"
	if oldImage == newImage {
		t.Fatalf("bad test setup, statefulSet %s roll out with the same image", sts.Name)
	}
	// 给 pod-0 加 finalizer，让 UpdateRevision 推进但其重建被卡住
	pod0 := &pods.Items[0]
	updatePod(t, podClient, pod0.Name, func(pod *v1.Pod) {
		pod.Finalizers = []string{"fake.example.com/blockDeletion"}
	})

	stsClient := c.AppsV1().StatefulSets(ns.Name)
	_ = updateSTS(t, stsClient, sts.Name, func(sts *appsv1.StatefulSet) {
		sts.Spec.Template.Spec.Containers[0].Image = newImage
	})

	// 等待 pod-1 被重建，同时 pod-0 仍在运行
	if err := wait.PollUntilContextTimeout(tCtx, pollInterval, pollTimeout, false, func(ctx context.Context) (bool, error) {
		ss, err := stsClient.Get(ctx, sts.Name, metav1.GetOptions{})
		if err != nil {
			return false, err
		}
		pods := getPods(t, podClient, labelMap)
		recreatedPods := v1.PodList{}
		for _, pod := range pods.Items {
			if pod.Status.Phase == v1.PodPending {
				recreatedPods.Items = append(recreatedPods.Items, pod)
			}
		}
		setPodsReadyCondition(t, c, &v1.PodList{Items: recreatedPods.Items}, v1.ConditionTrue, time.Now())
		return ss.Status.UpdatedReplicas == *ss.Spec.Replicas-*sts.Spec.UpdateStrategy.RollingUpdate.Partition && ss.Status.Replicas == *ss.Spec.Replicas && ss.Status.ReadyReplicas == *ss.Spec.Replicas, nil
	}); err != nil {
		t.Fatalf("failed to await for pod-1 to be recreated by sts %s: %v", sts.Name, err)
	}

	// 将 pod-0 标记为 Failed 且未就绪
	updatePodStatus(t, podClient, pod0.Name, func(pod *v1.Pod) {
		pod.Status.Phase = v1.PodFailed
	})

	// 删除 pod-0，使其带上删除时间戳等待重建
	if err := c.CoreV1().Pods(ns.Name).Delete(context.TODO(), pod0.Name, metav1.DeleteOptions{}); err != nil {
		t.Fatalf("error deleting pod %s: %v", pod0.Name, err)
	}

	// 等待 status 中 ReadyReplicas 减 1
	if err := wait.PollUntilContextTimeout(tCtx, pollInterval, pollTimeout, false, func(ctx context.Context) (bool, error) {
		ss, err := stsClient.Get(ctx, sts.Name, metav1.GetOptions{})
		if err != nil {
			return false, err
		}
		return ss.Status.ReadyReplicas == *ss.Spec.Replicas-1, nil
	}); err != nil {
		t.Fatalf("failed to await for pod-0 to be not counted as ready in status of sts %s: %v", sts.Name, err)
	}

	// 移除 finalizer，允许 pod-0 被真正删除并重建
	updatePod(t, podClient, pod0.Name, func(pod *v1.Pod) {
		pod.Finalizers = []string{}
	})

	// 等待 pod-0 重建并就绪
	if err := wait.PollUntilContextTimeout(tCtx, pollInterval, pollTimeout, false, func(ctx context.Context) (bool, error) {
		pods := getPods(t, podClient, labelMap)
		recreatedPods := v1.PodList{}
		for _, pod := range pods.Items {
			if pod.Status.Phase == v1.PodPending {
				recreatedPods.Items = append(recreatedPods.Items, pod)
			}
		}
		setPodsReadyCondition(t, c, &v1.PodList{Items: recreatedPods.Items}, v1.ConditionTrue, time.Now().Add(-120*time.Minute))
		return len(recreatedPods.Items) > 0, nil
	}); err != nil {
		t.Fatalf("failed to await for pod-0 to be recreated by sts %s: %v", sts.Name, err)
	}

	// 等待 StatefulSet status 记录所有副本均已就绪
	if err := wait.PollUntilContextTimeout(tCtx, pollInterval, pollTimeout, false, func(ctx context.Context) (bool, error) {
		ss, err := stsClient.Get(ctx, sts.Name, metav1.GetOptions{})
		if err != nil {
			return false, err
		}
		return ss.Status.ReadyReplicas == *ss.Spec.Replicas, nil
	}); err != nil {
		t.Fatalf("failed to verify .Spec.Template.Spec.Containers[0].Image is updated for sts %s: %v", sts.Name, err)
	}

	// 验证 Pod 数量符合预期
	pods = getPods(t, podClient, labelMap)
	if len(pods.Items) != int(*sts.Spec.Replicas) {
		t.Fatalf("Unexpected number of pods")
	}

	// 验证镜像按 partition 分成新旧两组
	for i := range pods.Items {
		if i < int(*sts.Spec.UpdateStrategy.RollingUpdate.Partition) {
			if pods.Items[i].Spec.Containers[0].Image != oldImage {
				t.Fatalf("Pod %s has image %s not equal to old image %s", pods.Items[i].Name, pods.Items[i].Spec.Containers[0].Image, oldImage)
			}
		} else {
			if pods.Items[i].Spec.Containers[0].Image != newImage {
				t.Fatalf("Pod %s has image %s not equal to new image %s", pods.Items[i].Name, pods.Items[i].Spec.Containers[0].Image, newImage)
			}
		}
	}
}

func TestStatefulSetStartOrdinal(t *testing.T) {
	tests := []struct {
		ordinals           *appsv1.StatefulSetOrdinals
		name               string
		namespace          string
		replicas           int
		expectedPodIndexes []int
	}{
		{
			name:               "default start ordinal, no ordinals set",
			namespace:          "no-ordinals",
			replicas:           3,
			expectedPodIndexes: []int{0, 1, 2},
		},
		{
			name:               "default start ordinal",
			namespace:          "no-start-ordinals",
			ordinals:           &appsv1.StatefulSetOrdinals{},
			replicas:           3,
			expectedPodIndexes: []int{0, 1, 2},
		},
		{
			name:      "start ordinal 4",
			namespace: "start-ordinal-4",
			ordinals: &appsv1.StatefulSetOrdinals{
				Start: 4,
			},
			replicas:           4,
			expectedPodIndexes: []int{4, 5, 6, 7},
		},
		{
			name:      "start ordinal 5",
			namespace: "start-ordinal-5",
			ordinals: &appsv1.StatefulSetOrdinals{
				Start: 2,
			},
			replicas:           7,
			expectedPodIndexes: []int{2, 3, 4, 5, 6, 7, 8},
		},
	}

	tCtx, closeFn, rm, informers, c := scSetup(t)
	defer closeFn()
	cancel := runControllerAndInformers(tCtx, rm, informers)
	defer cancel()

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ns := framework.CreateNamespaceOrDie(c, test.namespace, t)
			defer framework.DeleteNamespaceOrDie(c, ns, t)

			// 使用与 newSTS() 一致的 Pod label
			labelMap := labelMap()
			sts := newSTS("sts", ns.Name, test.replicas)
			sts.Spec.Ordinals = test.ordinals
			stss := createSTSs(t, c, []*appsv1.StatefulSet{sts})
			sts = stss[0]
			waitSTSStable(t, c, sts)

			podClient := c.CoreV1().Pods(ns.Name)
			pods := getPods(t, podClient, labelMap)
			if len(pods.Items) != test.replicas {
				t.Errorf("len(pods) = %v, want %v", len(pods.Items), test.replicas)
			}

			var podNames []string
			var podLabelIndexes []int
			for _, pod := range pods.Items {
				podNames = append(podNames, pod.Name)
				if idx, ok := pod.Labels[appsv1.PodIndexLabel]; !ok {
					t.Errorf("Expected pod index label with key: %s", appsv1.PodIndexLabel)
				} else {
					idxInt, err := strconv.Atoi(idx)
					if err != nil {
						t.Errorf("Unable to convert pod index to int, unexpected pod index: %s", idx)
					}
					podLabelIndexes = append(podLabelIndexes, idxInt)
				}
			}
			ignoreOrder := cmpopts.SortSlices(func(a, b string) bool {
				return a < b
			})
			ignoreOrderForOrdinals := cmpopts.SortSlices(func(a, b int) bool {
				return a < b
			})

			expectedNames := []string{}
			for _, ord := range test.expectedPodIndexes {
				expectedNames = append(expectedNames, fmt.Sprintf("sts-%d", ord))
			}

			// 验证所有预期 Pod 均已创建
			if diff := cmp.Diff(expectedNames, podNames, ignoreOrder); diff != "" {
				t.Errorf("Unexpected pod names: (-want +got): %v", diff)
			}
			// 验证 Pod index label 从指定的起始序号开始
			if diff := cmp.Diff(test.expectedPodIndexes, podLabelIndexes, ignoreOrderForOrdinals); diff != "" {
				t.Errorf("Unexpected pod indices: (-want +got): %v", diff)
			}

			// 缩容到 1 个 Pod，应保留起始序号最小的那个
			scaleSTS(t, c, sts, 1)
			waitSTSStable(t, c, sts)

			pods = getPods(t, podClient, labelMap)
			if len(pods.Items) != 1 {
				t.Errorf("len(pods) = %v, want %v", len(pods.Items), 1)
			}
			if pods.Items[0].Name != expectedNames[0] {
				t.Errorf("Unexpected singleton pod name: got = %v, want %v", pods.Items[0].Name, expectedNames[0])
			}
		})
	}
}
func TestStatefulSetPodSubdomain(t *testing.T) {
	tCtx, closeFn, rm, informers, c := scSetup(t)
	defer closeFn()
	ns := framework.CreateNamespaceOrDie(c, "test-pod-subdomain", t)
	defer framework.DeleteNamespaceOrDie(c, ns, t)
	cancel := runControllerAndInformers(tCtx, rm, informers)
	defer cancel()

	// 创建 headless Service
	serviceName := "test-service"
	service := newHeadlessService(ns.Name)
	service.Name = serviceName
	createHeadlessService(t, c, service)

	// 创建引用该 Service 的 StatefulSet
	sts := newSTS("sts", ns.Name, 3)
	sts.Spec.ServiceName = serviceName
	stss, _ := createSTSsPods(t, c, []*appsv1.StatefulSet{sts}, []*v1.Pod{})
	sts = stss[0]
	waitSTSStable(t, c, sts)

	// 验证 Pod 的 subdomain 指向该 Service
	labelMap := labelMap()
	podClient := c.CoreV1().Pods(ns.Name)
	pods := getPods(t, podClient, labelMap)

	for _, pod := range pods.Items {
		if pod.Spec.Subdomain != serviceName {
			t.Errorf("Pod %s has incorrect subdomain: got %s, want %s", pod.Name, pod.Spec.Subdomain, serviceName)
		}
	}
}

func TestRecreateStatefulSetUpdate(t *testing.T) {
	featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.StatefulSetRecreateStrategy, true)
	tCtx, closeFn, rm, informers, c := scSetup(t)
	defer closeFn()
	ns := framework.CreateNamespaceOrDie(c, "test-recreate-update", t)
	defer framework.DeleteNamespaceOrDie(c, ns, t)
	cancel := runControllerAndInformers(tCtx, rm, informers)
	defer cancel()

	createHeadlessService(t, c, newHeadlessService(ns.Name))
	labelMap := labelMap()
	sts := newSTS("sts", ns.Name, 3)
	sts.Spec.UpdateStrategy = appsv1.StatefulSetUpdateStrategy{
		Type: appsv1.RecreateStatefulSetStrategyType,
	}
	stss, _ := createSTSsPods(t, c, []*appsv1.StatefulSet{sts}, []*v1.Pod{})
	sts = stss[0]
	waitSTSStable(t, c, sts)

	podClient := c.CoreV1().Pods(ns.Name)
	pods := getPods(t, podClient, labelMap)
	if len(pods.Items) != 3 {
		t.Fatalf("len(pods) = %d, want 3", len(pods.Items))
	}
	setPodsReadyCondition(t, c, pods, v1.ConditionTrue, time.Now())

	stsClient := c.AppsV1().StatefulSets(ns.Name)
	sts, err := stsClient.Get(tCtx, sts.Name, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("Failed to get StatefulSet: %v", err)
	}
	oldCurrentRevision := sts.Status.CurrentRevision

	// 记录旧 Pod UID，用于确认重建时 UID 都发生变化
	pods = getPods(t, podClient, labelMap)
	oldPodUIDs := make(map[string]types.UID)
	for _, pod := range pods.Items {
		oldPodUIDs[pod.Name] = pod.UID
	}

	// 记录旧 PVC UID，用于确认重建后 PVC 被保留
	pvcClient := c.CoreV1().PersistentVolumeClaims(ns.Name)
	oldPVCs := getStatefulSetPVCs(t, pvcClient, sts)
	oldPVCUIDs := make(map[string]types.UID)
	for _, pvc := range oldPVCs {
		oldPVCUIDs[pvc.Name] = pvc.UID
	}

	newImage := "new-image"
	updateSTS(t, stsClient, sts.Name, func(sts *appsv1.StatefulSet) {
		sts.Spec.Template.Spec.Containers[0].Image = newImage
	})

	if err := wait.PollUntilContextTimeout(tCtx, pollInterval, pollTimeout, false, func(ctx context.Context) (bool, error) {
		ss, err := stsClient.Get(ctx, sts.Name, metav1.GetOptions{})
		if err != nil {
			return false, err
		}
		currentPods := getPods(t, podClient, labelMap)

		// Recreate 策略下任意时刻都不应出现新旧 revision 混用
		revisions := make(map[string]bool)
		for _, pod := range currentPods.Items {
			rev := pod.Labels[appsv1.ControllerRevisionHashLabelKey]
			if rev != "" {
				revisions[rev] = true
			}
		}
		if len(revisions) > 1 {
			return false, fmt.Errorf("found mixed revisions among pods: %v", revisions)
		}

		pendingPods := v1.PodList{}
		for _, pod := range currentPods.Items {
			if pod.Status.Phase == v1.PodPending {
				pendingPods.Items = append(pendingPods.Items, pod)
			}
		}
		if len(pendingPods.Items) > 0 {
			setPodsReadyCondition(t, c, &pendingPods, v1.ConditionTrue, time.Now())
		}

		return ss.Status.ReadyReplicas == *ss.Spec.Replicas &&
			ss.Status.CurrentRevision == ss.Status.UpdateRevision, nil
	}); err != nil {
		t.Fatalf("Timed out waiting for recreate update to complete: %v", err)
	}

	sts, err = stsClient.Get(tCtx, sts.Name, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("Failed to get StatefulSet after update: %v", err)
	}

	if sts.Status.CurrentRevision == oldCurrentRevision {
		t.Fatalf("CurrentRevision should have changed from %s after recreate", oldCurrentRevision)
	}

	// 验证所有 Pod 均已重建且使用新镜像
	pods = getPods(t, podClient, labelMap)
	if len(pods.Items) != int(*sts.Spec.Replicas) {
		t.Fatalf("Expected %d pods, got %d", *sts.Spec.Replicas, len(pods.Items))
	}
	for _, pod := range pods.Items {
		if oldUID, ok := oldPodUIDs[pod.Name]; ok && pod.UID == oldUID {
			t.Fatalf("Pod %s was not recreated", pod.Name)
		}
		if pod.Spec.Containers[0].Image != newImage {
			t.Fatalf("Pod %s has image %s, want %s", pod.Name, pod.Spec.Containers[0].Image, newImage)
		}
	}

	// 验证重建后 PVC 保持不变（UID 不变）
	newPVCs := getStatefulSetPVCs(t, pvcClient, sts)
	if len(newPVCs) != len(oldPVCs) {
		t.Fatalf("PVC count changed: before=%d, after=%d", len(oldPVCs), len(newPVCs))
	}
	for _, pvc := range newPVCs {
		oldUID, ok := oldPVCUIDs[pvc.Name]
		if !ok {
			t.Fatalf("Unexpected PVC %s appeared after recreate", pvc.Name)
		}
		if pvc.UID != oldUID {
			t.Fatalf("PVC %s was recreated (UID changed from %s to %s), should be preserved",
				pvc.Name, oldUID, pvc.UID)
		}
	}
}
