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

package scheduling

import (
	"context"
	"fmt"
	"math"
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/uuid"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/kubernetes/test/e2e/framework"
	e2enode "k8s.io/kubernetes/test/e2e/framework/node"
	e2epod "k8s.io/kubernetes/test/e2e/framework/pod"
	e2erc "k8s.io/kubernetes/test/e2e/framework/rc"
	e2eskipper "k8s.io/kubernetes/test/e2e/framework/skipper"
	testutils "k8s.io/kubernetes/test/utils"
	imageutils "k8s.io/kubernetes/test/utils/image"
	admissionapi "k8s.io/pod-security-admission/api"
)

var _ = SIGDescribe("Multi-AZ Clusters", func() {
	f := framework.NewDefaultFramework("multi-az")
	f.NamespacePodSecurityLevel = admissionapi.LevelBaseline
	var zoneCount int
	var err error
	var zoneNames sets.Set[string]
	ginkgo.BeforeEach(func(ctx context.Context) {
		cs := f.ClientSet

		if zoneCount <= 0 {
			zoneNames, err = e2enode.GetSchedulableClusterZones(ctx, cs)
			framework.ExpectNoError(err)
			zoneCount = len(zoneNames)
		}
		ginkgo.By(fmt.Sprintf("Checking for multi-zone cluster. Schedulable zone count = %d", zoneCount))
		msg := fmt.Sprintf("Schedulable zone count is %d, only run for multi-zone clusters, skipping test", zoneCount)
		e2eskipper.SkipUnlessAtLeast(zoneCount, 2, msg)
		// TODO: SkipUnlessDefaultScheduler() // 非默认调度器可能无法均匀分布 Pod

		e2enode.WaitForTotalHealthy(ctx, cs, time.Minute)
		nodeList, err := e2enode.GetReadySchedulableNodes(ctx, cs)
		framework.ExpectNoError(err)

		// 让各节点的 CPU、内存使用量保持均衡
		err = createBalancedPodForNodes(ctx, f, cs, f.Namespace.Name, nodeList.Items, podRequestedResource, 0.0)
		framework.ExpectNoError(err)
	})
	f.It("should spread the pods of a service across zones", f.WithSerial(), func(ctx context.Context) {
		SpreadServiceOrFail(ctx, f, 5*zoneCount, zoneNames, imageutils.GetPauseImageName())
	})

	f.It("should spread the pods of a replication controller across zones", f.WithSerial(), func(ctx context.Context) {
		SpreadRCOrFail(ctx, f, int32(5*zoneCount), zoneNames, imageutils.GetE2EImage(imageutils.Agnhost), []string{"serve-hostname"})
	})
})

// SpreadServiceOrFail 检查构成 Service 的 Pod 是否均匀分布在各可用 zone 上
func SpreadServiceOrFail(ctx context.Context, f *framework.Framework, replicaCount int, zoneNames sets.Set[string], image string) {
	// 先创建 Service
	serviceName := "test-service"
	serviceSpec := &v1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      serviceName,
			Namespace: f.Namespace.Name,
		},
		Spec: v1.ServiceSpec{
			Selector: map[string]string{
				"service": serviceName,
			},
			Ports: []v1.ServicePort{{
				Port:       80,
				TargetPort: intstr.FromInt32(80),
			}},
		},
	}
	_, err := f.ClientSet.CoreV1().Services(f.Namespace.Name).Create(ctx, serviceSpec, metav1.CreateOptions{})
	framework.ExpectNoError(err)

	// 再创建 Service 后端的 Pod
	podSpec := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:   serviceName,
			Labels: map[string]string{"service": serviceName},
		},
		Spec: v1.PodSpec{
			Containers: []v1.Container{
				{
					Name:  "test",
					Image: image,
				},
			},
		},
	}

	// 注意：StartPods 要求至少一个 Pod 可供复制。根据调用方，replicas 始终是正数：
	// zoneCount >= 0 意味着 (2*zoneCount)+1 > 0，因此无需额外检查。
	// 一旦前置条件变为允许零副本，就必须检查 replicaCount > 0，否则 StartPods 会 panic。
	framework.ExpectNoError(testutils.StartPods(f.ClientSet, replicaCount, f.Namespace.Name, serviceName, *podSpec, false, framework.Logf))

	// 等待所有 Pod 完成调度
	selector := labels.SelectorFromSet(labels.Set(map[string]string{"service": serviceName}))
	pods, err := e2epod.WaitForPodsWithLabelScheduled(ctx, f.ClientSet, f.Namespace.Name, selector)
	framework.ExpectNoError(err)

	// 然后确认 Pod 分布在多个 zone 上
	checkZoneSpreading(ctx, f.ClientSet, pods, sets.List(zoneNames))
}

// getZoneNameForNode 返回 Node 所在 zone 的名称
func getZoneNameForNode(node v1.Node) (string, error) {
	if z, ok := node.Labels[v1.LabelFailureDomainBetaZone]; ok {
		return z, nil
	} else if z, ok := node.Labels[v1.LabelTopologyZone]; ok {
		return z, nil
	}
	return "", fmt.Errorf("node %s doesn't have zone label %s or %s",
		node.Name, v1.LabelFailureDomainBetaZone, v1.LabelTopologyZone)
}

// getZoneNameForPod 返回 Pod 被调度到的 zone 的名称
func getZoneNameForPod(ctx context.Context, c clientset.Interface, pod v1.Pod) (string, error) {
	ginkgo.By(fmt.Sprintf("Getting zone name for pod %s, on node %s", pod.Name, pod.Spec.NodeName))
	node, err := c.CoreV1().Nodes().Get(ctx, pod.Spec.NodeName, metav1.GetOptions{})
	framework.ExpectNoError(err)
	return getZoneNameForNode(*node)
}

// checkZoneSpreading 判断一组 Pod 是否大致均匀地分布在指定 zone 上
func checkZoneSpreading(ctx context.Context, c clientset.Interface, pods *v1.PodList, zoneNames []string) {
	podsPerZone := make(map[string]int)
	for _, zoneName := range zoneNames {
		podsPerZone[zoneName] = 0
	}
	for _, pod := range pods.Items {
		if pod.DeletionTimestamp != nil {
			continue
		}
		zoneName, err := getZoneNameForPod(ctx, c, pod)
		framework.ExpectNoError(err)
		podsPerZone[zoneName] = podsPerZone[zoneName] + 1
	}
	minPodsPerZone := math.MaxInt32
	maxPodsPerZone := 0
	for _, podCount := range podsPerZone {
		if podCount < minPodsPerZone {
			minPodsPerZone = podCount
		}
		if podCount > maxPodsPerZone {
			maxPodsPerZone = podCount
		}
	}
	gomega.Expect(maxPodsPerZone-minPodsPerZone).To(gomega.BeNumerically("~", 0, 2),
		"Pods were not evenly spread across zones.  %d in one zone and %d in another zone",
		minPodsPerZone, maxPodsPerZone)
}

// SpreadRCOrFail 检查构成 ReplicationController 的 Pod 是否均匀分布在各可用 zone 上
func SpreadRCOrFail(ctx context.Context, f *framework.Framework, replicaCount int32, zoneNames sets.Set[string], image string, args []string) {
	name := "ubelite-spread-rc-" + string(uuid.NewUUID())
	rcLabels := map[string]string{"name": name}

	ginkgo.By(fmt.Sprintf("Creating replication controller %s", name))
	controller, err := f.ClientSet.CoreV1().ReplicationControllers(f.Namespace.Name).Create(ctx, &v1.ReplicationController{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: f.Namespace.Name,
			Name:      name,
		},
		Spec: v1.ReplicationControllerSpec{
			Replicas: &replicaCount,
			Selector: rcLabels,
			Template: &v1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: rcLabels,
				},
				Spec: v1.PodSpec{
					Containers: []v1.Container{
						{
							Name:  name,
							Image: image,
							Args:  args,
							Ports: []v1.ContainerPort{{ContainerPort: 9376}},
						},
					},
				},
			},
		},
	}, metav1.CreateOptions{})
	framework.ExpectNoError(err)
	// 测试结束后清理 ReplicationController。
	defer func() {
		// 将 ReplicationController 缩容到零，以清除 Pod。
		if err := e2erc.DeleteRCAndWaitForGC(ctx, f.ClientSet, f.Namespace.Name, controller.Name); err != nil {
			framework.Logf("Failed to cleanup replication controller %v: %v.", controller.Name, err)
		}
	}()
	// 列出 Pod，确认观察到所有副本。
	selector := labels.SelectorFromSet(rcLabels)
	_, err = e2epod.PodsCreatedByLabel(ctx, f.ClientSet, f.Namespace.Name, name, replicaCount, selector)
	framework.ExpectNoError(err)

	// 等待所有 Pod 完成调度
	ginkgo.By(fmt.Sprintf("Waiting for %d replicas of %s to be scheduled.  Selector: %v", replicaCount, name, selector))
	pods, err := e2epod.WaitForPodsWithLabelScheduled(ctx, f.ClientSet, f.Namespace.Name, selector)
	framework.ExpectNoError(err)

	// 然后确认 Pod 分布在多个 zone 上
	checkZoneSpreading(ctx, f.ClientSet, pods, sets.List(zoneNames))
}
