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

package apps

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	autoscalingv1 "k8s.io/api/autoscaling/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/resourceversion"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/tools/cache"
	watchtools "k8s.io/client-go/tools/watch"
	"k8s.io/client-go/util/retry"

	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/rand"
	"k8s.io/apimachinery/pkg/util/uuid"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/kubernetes/pkg/controller/replicaset"
	apimachineryutils "k8s.io/kubernetes/test/e2e/common/apimachinery"
	"k8s.io/kubernetes/test/e2e/framework"
	e2epod "k8s.io/kubernetes/test/e2e/framework/pod"
	e2ereplicaset "k8s.io/kubernetes/test/e2e/framework/replicaset"
	imageutils "k8s.io/kubernetes/test/utils/image"
	admissionapi "k8s.io/pod-security-admission/api"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
)

const (
	rsRetryTimeout = 2 * time.Minute
)

func newRS(rsName string, replicas int32, rsPodLabels map[string]string, imageName string, image string, args []string) *appsv1.ReplicaSet {
	zero := int64(0)
	return &appsv1.ReplicaSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:   rsName,
			Labels: rsPodLabels,
		},
		Spec: appsv1.ReplicaSetSpec{
			Selector: &metav1.LabelSelector{
				MatchLabels: rsPodLabels,
			},
			Replicas: &replicas,
			Template: v1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: rsPodLabels,
				},
				Spec: v1.PodSpec{
					TerminationGracePeriodSeconds: &zero,
					Containers: []v1.Container{
						{
							Name:  imageName,
							Image: image,
							Args:  args,
						},
					},
				},
			},
		},
	}
}

func newPodQuota(name, number string) *v1.ResourceQuota {
	return &v1.ResourceQuota{
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
		},
		Spec: v1.ResourceQuotaSpec{
			Hard: v1.ResourceList{
				v1.ResourcePods: resource.MustParse(number),
			},
		},
	}
}

var _ = SIGDescribe("ReplicaSet", func() {
	f := framework.NewDefaultFramework("replicaset")
	f.NamespacePodSecurityLevel = admissionapi.LevelBaseline

	/*
		Release: v1.9
		Testname: Replica Set, run basic image
		Description: 创建一个带一个 Pod 和单个容器的 ReplicaSet。确保 Pod 正在运行。查询时 Pod SHOULD 返回有效响应。
	*/
	framework.ConformanceIt("should serve a basic image on each replica with a public image", func(ctx context.Context) {
		testReplicaSetServeImageOrFail(ctx, f, "basic", imageutils.GetE2EImage(imageutils.Agnhost))
	})

	f.It("should serve a basic image on each replica with a private image", f.WithProvider("gce") /* 需要私有镜像 */, func(ctx context.Context) {
		privateimage := imageutils.GetConfig(imageutils.AgnhostPrivate)
		testReplicaSetServeImageOrFail(ctx, f, "private", privateimage.GetE2EImage())
	})

	ginkgo.It("should surface a failure condition on a common issue like exceeded quota", func(ctx context.Context) {
		testReplicaSetConditionCheck(ctx, f)
	})

	/*
		Release: v1.13
		Testname: Replica Set, adopt matching pods and release non matching pods
		Description: 先创建一个 Pod，再创建一个标签选择器与该 Pod 匹配的 Replica Set（RS）。RS MUST 收养该 Pod，或删除并用新 Pod 替换。当 RS 拥有的某个 Pod 的标签不再匹配 RS 的标签选择器时，RS MUST 释放该 Pod 并更新其 owner reference。
	*/
	framework.ConformanceIt("should adopt matching pods on creation and release no longer matching pods", func(ctx context.Context) {
		testRSAdoptMatchingAndReleaseNotMatching(ctx, f)
	})

	/*
		Release: v1.21
		Testname: ReplicaSet, completes the scaling of a ReplicaSet subresource
		Description: 创建一个只有一个 Pod 的 ReplicaSet（RS）。必须验证 Pod
		正在运行。RS 必须获取并验证 scale 子资源的计数。
		RS 必须更新并验证 scale 子资源。RS 必须给 scale 子资源打补丁并验证。
	*/
	framework.ConformanceIt("Replicaset should have a working scale subresource", func(ctx context.Context) {
		testRSScaleSubresources(ctx, f)
	})

	/*
		Release: v1.21
		Testname: ReplicaSet, is created, Replaced and Patched
		Description: 创建一个只有一个 Pod 的 ReplicaSet（RS）。必须验证 Pod
		正在运行。RS 必须缩容/扩容到两个副本并验证副本数。
		RS 必须被打补丁并验证补丁成功。
	*/
	framework.ConformanceIt("Replace and Patch tests", func(ctx context.Context) {
		testRSLifeCycle(ctx, f)
	})

	/*
		Release: v1.22
		Testname: ReplicaSet, list and delete a collection of ReplicaSets
		Description: 创建 ReplicaSet 必须成功。通过标签选择器列出
		ReplicaSet 必须成功。通过 deleteCollection 删除
		ReplicaSet 必须成功。
	*/
	framework.ConformanceIt("should list and delete a collection of ReplicaSets", func(ctx context.Context) {
		listRSDeleteCollection(ctx, f)

	})

	/*	Release: v1.22
		Testname: ReplicaSet, status sub-resource
		Description: 创建 ReplicaSet 资源必须成功。
		尝试读取、更新和打补丁其 status 子资源；所有
		修改子资源的操作必须能被后续读取看到。
	*/
	framework.ConformanceIt("should validate Replicaset Status endpoints", func(ctx context.Context) {
		testRSStatus(ctx, f)
	})
})

// 使用 ReplicaSet 检查镜像部署的基本测试。
// 镜像提供其 hostname，每个副本都会检查。
func testReplicaSetServeImageOrFail(ctx context.Context, f *framework.Framework, test string, image string) {
	name := "my-hostname-" + test + "-" + string(uuid.NewUUID())
	rsLabels := map[string]string{"name": name}
	replicas := int32(1)

	// 为一个提供 hostname 的服务创建 ReplicaSet。
	// 检查新 RC 的 Pod 是否已创建。
	// TODO: 也许可以改为只检查 owner reference。
	framework.Logf("Creating ReplicaSet %s", name)
	newRS := newRS(name, replicas, rsLabels, name, image, []string{"serve-hostname"})
	newRS.Spec.Template.Spec.Containers[0].Ports = []v1.ContainerPort{{ContainerPort: 9376}}
	_, err := f.ClientSet.AppsV1().ReplicaSets(f.Namespace.Name).Create(ctx, newRS, metav1.CreateOptions{})
	framework.ExpectNoError(err)

	// 检查新 RS 的 Pod 是否已创建。
	// 未运行的 Pod 会导致此测试超时。
	pods, err := e2epod.PodsCreatedByLabel(ctx, f.ClientSet, f.Namespace.Name, name, replicas, labels.SelectorFromSet(rsLabels))
	framework.ExpectNoError(err)

	// 等待 Pod 进入运行态。循环等待直到 Pod 运行，
	// 验证有服务在监听。
	framework.Logf("Ensuring a pod for ReplicaSet %q is running", name)
	running := int32(0)
	for _, pod := range pods.Items {
		if pod.DeletionTimestamp != nil {
			continue
		}
		err = e2epod.WaitForPodNameRunningInNamespace(ctx, f.ClientSet, pod.Name, f.Namespace.Name)
		if err != nil {
			updatePod, getErr := f.ClientSet.CoreV1().Pods(f.Namespace.Name).Get(ctx, pod.Name, metav1.GetOptions{})
			if getErr == nil {
				err = fmt.Errorf("pod %q never run (phase: %s, conditions: %+v): %w", updatePod.Name, updatePod.Status.Phase, updatePod.Status.Conditions, err)
			} else {
				err = fmt.Errorf("pod %q never run: %w", pod.Name, err)
			}
		}
		framework.ExpectNoError(err)
		framework.Logf("Pod %q is running (conditions: %+v)", pod.Name, pod.Status.Conditions)
		running++
	}

	// 1. 创建一个配额，将当前命名空间中的 Pod 数限制为 2。
	gomega.Expect(running).To(gomega.Equal(replicas), "unexpected number of running pods: %+v", pods.Items)

	// 2. 创建一个想运行 3 个 Pod 的 replication controller。
	framework.Logf("Trying to dial the pod")
	framework.ExpectNoError(e2epod.WaitForPodsResponding(ctx, f.ClientSet, f.Namespace.Name, name, labels.SelectorFromSet(rsLabels), true, 2*time.Minute, pods))
}

// 3. 检查 replication controller 的 conditions 中是否有 ReplicaFailure。
// 2. 创建一个想运行 3 个 Pod 的 replica set。
// 3. 检查 replica set 的 conditions 中是否有 ReplicaFailure。
// 4. 缩容 replica set 并观察 condition 消失。
func testReplicaSetConditionCheck(ctx context.Context, f *framework.Framework) {
	c := f.ClientSet
	namespace := f.Namespace.Name
	name := "condition-test"

	ginkgo.By(fmt.Sprintf("Creating quota %q that allows only two pods to run in the current namespace", name))
	quota := newPodQuota(name, "2")
	_, err := c.CoreV1().ResourceQuotas(namespace).Create(ctx, quota, metav1.CreateOptions{})
	framework.ExpectNoError(err)

	err = wait.PollImmediate(1*time.Second, 1*time.Minute, func() (bool, error) {
		quota, err = c.CoreV1().ResourceQuotas(namespace).Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			return false, err
		}
		quantity := resource.MustParse("2")
		podQuota := quota.Status.Hard[v1.ResourcePods]
		return (&podQuota).Cmp(quantity) == 0, nil
	})
	if wait.Interrupted(err) {
		err = fmt.Errorf("resource quota %q never synced", name)
	}
	framework.ExpectNoError(err)

	ginkgo.By(fmt.Sprintf("Creating replica set %q that asks for more than the allowed pod quota", name))
	rs := newRS(name, 3, map[string]string{"name": name}, AgnhostImageName, AgnhostImage, nil)
	rs, err = c.AppsV1().ReplicaSets(namespace).Create(ctx, rs, metav1.CreateOptions{})
	framework.ExpectNoError(err)

	ginkgo.By(fmt.Sprintf("Checking replica set %q has the desired failure condition set", name))
	generation := rs.Generation
	conditions := rs.Status.Conditions
	err = wait.PollImmediate(1*time.Second, 1*time.Minute, func() (bool, error) {
		rs, err = c.AppsV1().ReplicaSets(namespace).Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			return false, err
		}

		if generation > rs.Status.ObservedGeneration {
			return false, nil
		}
		conditions = rs.Status.Conditions

		cond := replicaset.GetCondition(rs.Status, appsv1.ReplicaSetReplicaFailure)
		return cond != nil, nil

	})
	if wait.Interrupted(err) {
		err = fmt.Errorf("rs controller never added the failure condition for replica set %q: %#v", name, conditions)
	}
	framework.ExpectNoError(err)

	ginkgo.By(fmt.Sprintf("Scaling down replica set %q to satisfy pod quota", name))
	rs, err = e2ereplicaset.UpdateReplicaSetWithRetries(c, namespace, name, func(update *appsv1.ReplicaSet) {
		x := int32(2)
		update.Spec.Replicas = &x
	})
	framework.ExpectNoError(err)

	ginkgo.By(fmt.Sprintf("Checking replica set %q has no failure condition set", name))
	generation = rs.Generation
	conditions = rs.Status.Conditions
	err = wait.PollImmediate(1*time.Second, 1*time.Minute, func() (bool, error) {
		rs, err = c.AppsV1().ReplicaSets(namespace).Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			return false, err
		}

		if generation > rs.Status.ObservedGeneration {
			return false, nil
		}
		conditions = rs.Status.Conditions

		cond := replicaset.GetCondition(rs.Status, appsv1.ReplicaSetReplicaFailure)
		return cond == nil, nil
	})
	if wait.Interrupted(err) {
		err = fmt.Errorf("rs controller never removed the failure condition for rs %q: %#v", name, conditions)
	}
	framework.ExpectNoError(err)
}

func testRSAdoptMatchingAndReleaseNotMatching(ctx context.Context, f *framework.Framework) {
	name := "pod-adoption-release"
	rsLabels := map[string]string{"name": name}
	ginkgo.By(fmt.Sprintf("Given a Pod with a 'name' label %s is created", name))
	p := e2epod.NewPodClient(f).CreateSync(ctx, &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:   name,
			Labels: rsLabels,
		},
		Spec: v1.PodSpec{
			Containers: []v1.Container{
				{
					Name:  name,
					Image: AgnhostImage,
				},
			},
		},
	})

	ginkgo.By("When a replicaset with a matching selector is created")
	replicas := int32(1)
	rsSt := newRS(name, replicas, rsLabels, name, AgnhostImage, nil)
	rsSt.Spec.Selector = &metav1.LabelSelector{MatchLabels: rsLabels}
	rs, err := f.ClientSet.AppsV1().ReplicaSets(f.Namespace.Name).Create(ctx, rsSt, metav1.CreateOptions{})
	framework.ExpectNoError(err)

	ginkgo.By("Then the orphan pod is adopted")
	err = wait.PollImmediate(1*time.Second, 1*time.Minute, func() (bool, error) {
		p2, err := f.ClientSet.CoreV1().Pods(f.Namespace.Name).Get(ctx, p.Name, metav1.GetOptions{})
		// Pod p 应被 ReplicaSet 收养或删除
		if apierrors.IsNotFound(err) {
			return true, nil
		}
		framework.ExpectNoError(err)
		for _, owner := range p2.OwnerReferences {
			if *owner.Controller && owner.UID == rs.UID {
				// pod 仍属于 replication controller
				return true, nil
			}
		}
		// pod 已释放
		return false, nil
	})
	framework.ExpectNoError(err)

	ginkgo.By("When the matched label of one of its pods change")
	pods, err := e2epod.PodsCreatedByLabel(ctx, f.ClientSet, f.Namespace.Name, rs.Name, replicas, labels.SelectorFromSet(rsLabels))
	framework.ExpectNoError(err)

	p = &pods.Items[0]
	err = wait.PollImmediate(1*time.Second, 1*time.Minute, func() (bool, error) {
		pod, err := f.ClientSet.CoreV1().Pods(f.Namespace.Name).Get(ctx, p.Name, metav1.GetOptions{})
		framework.ExpectNoError(err)

		pod.Labels = map[string]string{"name": "not-matching-name"}
		_, err = f.ClientSet.CoreV1().Pods(f.Namespace.Name).Update(ctx, pod, metav1.UpdateOptions{})
		if err != nil && apierrors.IsConflict(err) {
			return false, nil
		}
		if err != nil {
			return false, err
		}
		return true, nil
	})
	framework.ExpectNoError(err)

	ginkgo.By("Then the pod is released")
	err = wait.PollImmediate(1*time.Second, 1*time.Minute, func() (bool, error) {
		p2, err := f.ClientSet.CoreV1().Pods(f.Namespace.Name).Get(ctx, p.Name, metav1.GetOptions{})
		framework.ExpectNoError(err)
		for _, owner := range p2.OwnerReferences {
			if *owner.Controller && owner.UID == rs.UID {
				// pod 仍属于 replicaset
				return false, nil
			}
		}
		// 1. 获取最新资源
		return true, nil
	})
	framework.ExpectNoError(err)
}

func testRSScaleSubresources(ctx context.Context, f *framework.Framework) {
	ns := f.Namespace.Name
	c := f.ClientSet

	// 创建 webserver Pod。
	podName := "sample-pod"
	rsPodLabels := map[string]string{
		"name": podName,
		"pod":  AgnhostImageName,
	}

	rsName := "test-rs"
	replicas := int32(1)
	ginkgo.By(fmt.Sprintf("Creating replica set %q that asks for more than the allowed pod quota", rsName))
	rs := newRS(rsName, replicas, rsPodLabels, AgnhostImageName, AgnhostImage, nil)
	createdRS, err := c.AppsV1().ReplicaSets(ns).Create(ctx, rs, metav1.CreateOptions{})
	framework.ExpectNoError(err)

	// 验证所需的 Pod 都已启动。
	err = e2ereplicaset.WaitForReplicaSetTargetAvailableReplicas(ctx, c, createdRS, replicas)
	framework.ExpectNoError(err, "error in waiting for pods to come up: %s", err)

	ginkgo.By("getting scale subresource")
	scale, err := c.AppsV1().ReplicaSets(ns).GetScale(ctx, rsName, metav1.GetOptions{})
	if err != nil {
		framework.Failf("Failed to get scale subresource: %v", err)
	}
	gomega.Expect(scale.Spec.Replicas).To(gomega.Equal(int32(1)))
	gomega.Expect(scale.Status.Replicas).To(gomega.Equal(int32(1)))

	ginkgo.By("updating a scale subresource")
	scale.ResourceVersion = "" // 表示 scale 更新应无条件执行
	scale.Spec.Replicas = 2
	scaleResult, err := c.AppsV1().ReplicaSets(ns).UpdateScale(ctx, rsName, scale, metav1.UpdateOptions{})
	if err != nil {
		framework.Failf("Failed to put scale subresource: %v", err)
	}
	gomega.Expect(scaleResult.Spec.Replicas).To(gomega.Equal(int32(2)))

	ginkgo.By("verifying the replicaset Spec.Replicas was modified")
	rs, err = c.AppsV1().ReplicaSets(ns).Get(ctx, rsName, metav1.GetOptions{})
	if err != nil {
		framework.Failf("Failed to get statefulset resource: %v", err)
	}
	gomega.Expect(*(rs.Spec.Replicas)).To(gomega.Equal(int32(2)))

	ginkgo.By("Patch a scale subresource")
	scale.ResourceVersion = "" // 表示 scale 更新应无条件执行
	scale.Spec.Replicas = 4    // "UpdateScale" 操作后应为 2，现在 Patch 为 4
	rsScalePatchPayload, err := json.Marshal(autoscalingv1.Scale{
		Spec: autoscalingv1.ScaleSpec{
			Replicas: scale.Spec.Replicas,
		},
	})
	framework.ExpectNoError(err, "Could not Marshal JSON for patch payload")

	_, err = c.AppsV1().ReplicaSets(ns).Patch(ctx, rsName, types.StrategicMergePatchType, []byte(rsScalePatchPayload), metav1.PatchOptions{}, "scale")
	framework.ExpectNoError(err, "Failed to patch replicaset: %v", err)

	rs, err = c.AppsV1().ReplicaSets(ns).Get(ctx, rsName, metav1.GetOptions{})
	framework.ExpectNoError(err, "Failed to get replicaset resource: %v", err)
	gomega.Expect(*(rs.Spec.Replicas)).To(gomega.Equal(int32(4)), "replicaset should have 4 replicas")
}

// ReplicaSet Replace 和 Patch 测试
func testRSLifeCycle(ctx context.Context, f *framework.Framework) {
	ns := f.Namespace.Name
	c := f.ClientSet
	zero := int64(0)

	// 创建 webserver Pod。
	podName := "sample-pod"
	rsPodLabels := map[string]string{
		"name": podName,
		"pod":  AgnhostImageName,
	}

	rsName := "test-rs"
	label := "test-rs=patched"
	labelMap := map[string]string{"test-rs": "patched"}
	replicas := int32(1)
	rsPatchReplicas := int32(3)
	rsPatchImage := imageutils.GetE2EImage(imageutils.Pause)

	w := &cache.ListWatch{
		WatchFunc: func(options metav1.ListOptions) (watch.Interface, error) {
			options.LabelSelector = label
			return f.ClientSet.AppsV1().ReplicaSets(ns).Watch(ctx, options)
		},
	}
	rsList, err := f.ClientSet.AppsV1().ReplicaSets("").List(ctx, metav1.ListOptions{LabelSelector: label})
	framework.ExpectNoError(err, "failed to list rsList")
	// Create a ReplicaSet
	rs := newRS(rsName, replicas, rsPodLabels, AgnhostImageName, AgnhostImage, nil)
	createdRS, err := c.AppsV1().ReplicaSets(ns).Create(ctx, rs, metav1.CreateOptions{})
	framework.ExpectNoError(err)
	gomega.Expect(createdRS).To(apimachineryutils.HaveValidResourceVersion())

	// 验证所需的 Pod 都已启动。
	err = e2epod.VerifyPodsRunning(ctx, c, ns, podName, labels.SelectorFromSet(map[string]string{"name": podName}), false, replicas)
	framework.ExpectNoError(err, "Failed to create pods: %s", err)

	// 扩缩容 ReplicaSet
	ginkgo.By(fmt.Sprintf("Scaling up %q replicaset", rsName))
	_, err = e2ereplicaset.UpdateReplicaSetWithRetries(c, ns, rsName, func(update *appsv1.ReplicaSet) {
		x := int32(2)
		update.Spec.Replicas = &x
	})
	framework.ExpectNoError(err, "ReplicaSet fail to scale to %q replicasets")

	// 给 PeplicaSet 打补丁
	ginkgo.By("patching the ReplicaSet")
	rsPatch, err := json.Marshal(map[string]interface{}{
		"metadata": map[string]interface{}{
			"labels": labelMap,
		},
		"spec": map[string]interface{}{
			"replicas": rsPatchReplicas,
			"template": map[string]interface{}{
				"spec": map[string]interface{}{
					"terminationGracePeriodSeconds": &zero,
					"containers": [1]map[string]interface{}{{
						"name":  rsName,
						"image": rsPatchImage,
					}},
				},
			},
		},
	})
	framework.ExpectNoError(err, "failed to Marshal ReplicaSet JSON patch")
	patchedRS, err := f.ClientSet.AppsV1().ReplicaSets(ns).Patch(ctx, rsName, types.StrategicMergePatchType, []byte(rsPatch), metav1.PatchOptions{})
	framework.ExpectNoError(err, "failed to patch ReplicaSet")
	gomega.Expect(resourceversion.CompareResourceVersion(createdRS.ResourceVersion, patchedRS.ResourceVersion)).To(gomega.BeNumerically("==", -1), "patched object should have a larger resource version")

	ctxUntil, cancel := context.WithTimeout(ctx, f.Timeouts.PodStart)
	defer cancel()
	_, err = watchtools.Until(ctxUntil, rsList.ResourceVersion, w, func(event watch.Event) (bool, error) {
		if rset, ok := event.Object.(*appsv1.ReplicaSet); ok {
			found := rset.ObjectMeta.Name == rsName &&
				rset.ObjectMeta.Labels["test-rs"] == "patched" &&
				rset.Status.ReadyReplicas == rsPatchReplicas &&
				rset.Status.AvailableReplicas == rsPatchReplicas &&
				rset.Spec.Template.Spec.Containers[0].Image == rsPatchImage &&
				*rset.Spec.Template.Spec.TerminationGracePeriodSeconds == zero
			if !found {
				framework.Logf("observed ReplicaSet %v in namespace %v with ReadyReplicas %v, AvailableReplicas %v", rset.ObjectMeta.Name, rset.ObjectMeta.Namespace, rset.Status.ReadyReplicas,
					rset.Status.AvailableReplicas)
			} else {
				framework.Logf("observed Replicaset %v in namespace %v with ReadyReplicas %v found %v", rset.ObjectMeta.Name, rset.ObjectMeta.Namespace, rset.Status.ReadyReplicas, found)
			}
			return found, nil
		} else {
			framework.Logf("observed event type %#v", event)
		}
		return false, nil
	})

	framework.ExpectNoError(err, "failed to see replicas of %v in namespace %v scale to requested amount of %v", rs.Name, ns, rsPatchReplicas)
}

// List 和 DeleteCollection 操作
func listRSDeleteCollection(ctx context.Context, f *framework.Framework) {

	ns := f.Namespace.Name
	c := f.ClientSet
	rsClient := f.ClientSet.AppsV1().ReplicaSets(ns)
	one := int64(1)
	rsName := "test-rs"
	replicas := int32(3)
	e2eValue := rand.String(5)

	// Define ReplicaSet Labels
	podName := "sample-pod"
	rsPodLabels := map[string]string{
		"name": podName,
		"pod":  AgnhostImageName,
		"e2e":  e2eValue,
	}

	ginkgo.By("Create a ReplicaSet")
	rs := newRS(rsName, replicas, rsPodLabels, AgnhostImageName, AgnhostImage, nil)
	_, err := rsClient.Create(ctx, rs, metav1.CreateOptions{})
	framework.ExpectNoError(err)

	ginkgo.By("Verify that the required pods have come up")
	err = e2epod.VerifyPodsRunning(ctx, c, ns, podName, labels.SelectorFromSet(map[string]string{"name": podName}), false, replicas)
	framework.ExpectNoError(err, "Failed to create pods: %s", err)
	r, err := rsClient.Get(ctx, rsName, metav1.GetOptions{})
	framework.ExpectNoError(err, "failed to get ReplicaSets")
	framework.Logf("Replica Status: %+v", r.Status)

	ginkgo.By("Listing all ReplicaSets")
	rsList, err := c.AppsV1().ReplicaSets("").List(ctx, metav1.ListOptions{LabelSelector: "e2e=" + e2eValue})
	framework.ExpectNoError(err, "failed to list ReplicaSets")
	gomega.Expect(rsList.Items).To(gomega.HaveLen(1), "filtered list wasn't found")

	ginkgo.By("DeleteCollection of the ReplicaSets")
	err = rsClient.DeleteCollection(ctx, metav1.DeleteOptions{GracePeriodSeconds: &one}, metav1.ListOptions{LabelSelector: "e2e=" + e2eValue})
	framework.ExpectNoError(err, "failed to delete ReplicaSets")

	ginkgo.By("After DeleteCollection verify that ReplicaSets have been deleted")
	rsList, err = c.AppsV1().ReplicaSets("").List(ctx, metav1.ListOptions{LabelSelector: "e2e=" + e2eValue})
	framework.ExpectNoError(err, "failed to list ReplicaSets")
	gomega.Expect(rsList.Items).To(gomega.BeEmpty(), "filtered list should have no replicas")
}

func testRSStatus(ctx context.Context, f *framework.Framework) {
	ns := f.Namespace.Name
	c := f.ClientSet
	rsClient := c.AppsV1().ReplicaSets(ns)

	// Define ReplicaSet Labels
	podName := "sample-pod"
	rsPodLabels := map[string]string{
		"name": podName,
		"pod":  AgnhostImageName,
	}
	labelSelector := labels.SelectorFromSet(rsPodLabels).String()

	rsName := "test-rs"
	replicas := int32(1)

	w := &cache.ListWatch{
		WatchFunc: func(options metav1.ListOptions) (watch.Interface, error) {
			options.LabelSelector = labelSelector
			return rsClient.Watch(ctx, options)
		},
	}
	rsList, err := c.AppsV1().ReplicaSets("").List(ctx, metav1.ListOptions{LabelSelector: labelSelector})
	framework.ExpectNoError(err, "failed to list Replicasets")

	ginkgo.By("Create a Replicaset")
	rs := newRS(rsName, replicas, rsPodLabels, AgnhostImageName, AgnhostImage, nil)
	testReplicaSet, err := c.AppsV1().ReplicaSets(ns).Create(ctx, rs, metav1.CreateOptions{})
	framework.ExpectNoError(err)

	ginkgo.By("Verify that the required pods have come up.")
	err = e2epod.VerifyPodsRunning(ctx, c, ns, podName, labels.SelectorFromSet(map[string]string{"name": podName}), false, replicas)
	framework.ExpectNoError(err, "Failed to create pods: %s", err)

	ginkgo.By("Getting /status")
	rsResource := schema.GroupVersionResource{Group: "apps", Version: "v1", Resource: "replicasets"}
	rsStatusUnstructured, err := f.DynamicClient.Resource(rsResource).Namespace(ns).Get(ctx, rsName, metav1.GetOptions{}, "status")
	framework.ExpectNoError(err, "Failed to fetch the status of replicaset %s in namespace %s", rsName, ns)
	rsStatusBytes, err := json.Marshal(rsStatusUnstructured)
	framework.ExpectNoError(err, "Failed to marshal unstructured response. %v", err)

	var rsStatus appsv1.ReplicaSet
	err = json.Unmarshal(rsStatusBytes, &rsStatus)
	framework.ExpectNoError(err, "Failed to unmarshal JSON bytes to a replicaset object type")
	framework.Logf("Replicaset %s has Conditions: %v", rsName, rsStatus.Status.Conditions)

	ginkgo.By("updating the Replicaset Status")
	var statusToUpdate, updatedStatus *appsv1.ReplicaSet

	err = retry.RetryOnConflict(retry.DefaultRetry, func() error {
		statusToUpdate, err = rsClient.Get(ctx, rsName, metav1.GetOptions{})
		framework.ExpectNoError(err, "Unable to retrieve replicaset %s", rsName)

		statusToUpdate.Status.Conditions = append(statusToUpdate.Status.Conditions, appsv1.ReplicaSetCondition{
			Type:    "StatusUpdate",
			Status:  "True",
			Reason:  "E2E",
			Message: "Set from e2e test",
		})

		updatedStatus, err = rsClient.UpdateStatus(ctx, statusToUpdate, metav1.UpdateOptions{})
		return err
	})
	framework.ExpectNoError(err, "Failed to update status. %v", err)
	framework.Logf("updatedStatus.Conditions: %#v", updatedStatus.Status.Conditions)

	ginkgo.By("watching for the ReplicaSet status to be updated")
	ctxUntil, cancel := context.WithTimeout(ctx, rsRetryTimeout)
	defer cancel()
	_, err = watchtools.Until(ctxUntil, rsList.ResourceVersion, w, func(event watch.Event) (bool, error) {
		if rs, ok := event.Object.(*appsv1.ReplicaSet); ok {
			found := rs.ObjectMeta.Name == testReplicaSet.ObjectMeta.Name &&
				rs.ObjectMeta.Namespace == testReplicaSet.ObjectMeta.Namespace &&
				rs.ObjectMeta.Labels["name"] == testReplicaSet.ObjectMeta.Labels["name"] &&
				rs.ObjectMeta.Labels["pod"] == testReplicaSet.ObjectMeta.Labels["pod"]
			if !found {
				framework.Logf("Observed replicaset %v in namespace %v with annotations: %v & Conditions: %v", rs.ObjectMeta.Name, rs.ObjectMeta.Namespace, rs.Annotations, rs.Status.Conditions)
				return false, nil
			}
			for _, cond := range rs.Status.Conditions {
				if cond.Type == "StatusUpdate" &&
					cond.Reason == "E2E" &&
					cond.Message == "Set from e2e test" {
					framework.Logf("Found replicaset %v in namespace %v with labels: %v annotations: %v & Conditions: %v", rs.ObjectMeta.Name, rs.ObjectMeta.Namespace, rs.ObjectMeta.Labels, rs.Annotations, rs.Status.Conditions)
					return found, nil
				}
				framework.Logf("Observed replicaset %v in namespace %v with annotations: %v & Conditions: %v", rs.ObjectMeta.Name, rs.ObjectMeta.Namespace, rs.Annotations, rs.Status.Conditions)
			}
		}
		object := strings.Split(fmt.Sprintf("%v", event.Object), "{")[0]
		framework.Logf("Observed %v event: %+v", object, event.Type)
		return false, nil
	})
	framework.ExpectNoError(err, "failed to locate replicaset %v in namespace %v", testReplicaSet.ObjectMeta.Name, ns)
	framework.Logf("Replicaset %s has an updated status", rsName)

	ginkgo.By("patching the Replicaset Status")
	payload := []byte(`{"status":{"conditions":[{"type":"StatusPatched","status":"True"}]}}`)
	framework.Logf("Patch payload: %v", string(payload))

	patchedReplicaSet, err := rsClient.Patch(ctx, rsName, types.MergePatchType, payload, metav1.PatchOptions{}, "status")
	framework.ExpectNoError(err, "Failed to patch status. %v", err)
	framework.Logf("Patched status conditions: %#v", patchedReplicaSet.Status.Conditions)

	ginkgo.By("watching for the Replicaset status to be patched")
	ctxUntil, cancel = context.WithTimeout(ctx, rsRetryTimeout)
	defer cancel()
	_, err = watchtools.Until(ctxUntil, rsList.ResourceVersion, w, func(event watch.Event) (bool, error) {
		if rs, ok := event.Object.(*appsv1.ReplicaSet); ok {
			found := rs.ObjectMeta.Name == testReplicaSet.ObjectMeta.Name &&
				rs.ObjectMeta.Namespace == testReplicaSet.ObjectMeta.Namespace &&
				rs.ObjectMeta.Labels["name"] == testReplicaSet.ObjectMeta.Labels["name"] &&
				rs.ObjectMeta.Labels["pod"] == testReplicaSet.ObjectMeta.Labels["pod"]
			if !found {
				framework.Logf("Observed replicaset %v in namespace %v with annotations: %v & Conditions: %v", rs.ObjectMeta.Name, rs.ObjectMeta.Namespace, rs.Annotations, rs.Status.Conditions)
				return false, nil
			}
			for _, cond := range rs.Status.Conditions {
				if cond.Type == "StatusPatched" {
					framework.Logf("Found replicaset %v in namespace %v with labels: %v annotations: %v & Conditions: %v", rs.ObjectMeta.Name, rs.ObjectMeta.Namespace, rs.ObjectMeta.Labels, rs.Annotations, cond)
					return found, nil
				}
				framework.Logf("Observed replicaset %v in namespace %v with annotations: %v & Conditions: %v", rs.ObjectMeta.Name, rs.ObjectMeta.Namespace, rs.Annotations, cond)
			}
		}
		object := strings.Split(fmt.Sprintf("%v", event.Object), "{")[0]
		framework.Logf("Observed %v event: %+v", object, event.Type)
		return false, nil
	})
	framework.ExpectNoError(err, "failed to locate replicaset %v in namespace %v", testReplicaSet.ObjectMeta.Name, ns)
	framework.Logf("Replicaset %s has a patched status", rsName)
}
