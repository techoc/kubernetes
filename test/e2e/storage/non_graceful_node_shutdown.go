/*
Copyright 2022 The Kubernetes Authors.

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

package storage

import (
	"context"
	"fmt"

	"github.com/onsi/ginkgo/v2"
	appsv1 "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/labels"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/kubernetes/test/e2e/framework"
	e2enode "k8s.io/kubernetes/test/e2e/framework/node"
	e2epod "k8s.io/kubernetes/test/e2e/framework/pod"
	e2epv "k8s.io/kubernetes/test/e2e/framework/pv"
	e2eskipper "k8s.io/kubernetes/test/e2e/framework/skipper"
	"k8s.io/kubernetes/test/e2e/storage/drivers"
	storageframework "k8s.io/kubernetes/test/e2e/storage/framework"
	"k8s.io/kubernetes/test/e2e/storage/utils"
	imageutils "k8s.io/kubernetes/test/utils/image"
	admissionapi "k8s.io/pod-security-admission/api"
)

/*
测试前提：
- 基础设施为 GCP。

测试步骤：
- 部署 gce-pd csi 驱动
- 创建 gce-pd csi storage class
- 用该 storage class 创建 PVC
- 创建副本数为 1 的应用 Deployment，并使用该 PVC 作为卷
- 关闭应用 Pod 所在节点的 kubelet。由于 kubelet 默认优雅关闭宽限期为 0，此处属于非优雅关闭。
- 给已关闭的节点添加 `out-of-service` 污点
- 验证 Pod 立即被调度到其他节点并进入 Running 和 Ready 状态
- 重新启动 kubelet
- 移除节点上的 `out-of-service` 污点
*/

var _ = utils.SIGDescribe(framework.WithDisruptive(), framework.WithProvider("gce"), "[LinuxOnly] NonGracefulNodeShutdown", func() {
	var (
		c  clientset.Interface
		ns string
	)
	f := framework.NewDefaultFramework("non-graceful-shutdown")
	f.NamespacePodSecurityLevel = admissionapi.LevelPrivileged

	ginkgo.BeforeEach(func(ctx context.Context) {
		c = f.ClientSet
		ns = f.Namespace.Name
		nodeList, err := e2enode.GetReadySchedulableNodes(ctx, c)
		if err != nil {
			framework.Logf("Failed to list node: %v", err)
		}
		if len(nodeList.Items) < 2 {
			ginkgo.Skip("At least 2 nodes are required to run the test")
		}
	})

	ginkgo.Describe("[NonGracefulNodeShutdown] pod that uses a persistent volume via gce pd driver", func() {
		ginkgo.It("should get immediately rescheduled to a different node after non graceful node shutdown", func(ctx context.Context) {
			// 安装 gce-pd CSI 驱动
			ginkgo.By("deploying csi gce-pd driver")
			driver := drivers.InitGcePDCSIDriver()
			config := driver.PrepareTest(ctx, f)
			dDriver, ok := driver.(storageframework.DynamicPVTestDriver)
			if !ok {
				e2eskipper.Skipf("csi driver expected DynamicPVTestDriver but got %v", driver)
			}
			ginkgo.By("Creating a gce-pd storage class")
			sc := dDriver.GetDynamicProvisionStorageClass(ctx, config, "")
			_, err := c.StorageV1().StorageClasses().Create(ctx, sc, metav1.CreateOptions{})
			framework.ExpectNoError(err, "failed to create a storageclass")
			scName := &sc.Name

			deploymentName := "sts-pod-gcepd"
			podLabels := map[string]string{"app": deploymentName}
			pod := createAndVerifyStatefulDeployment(ctx, scName, deploymentName, ns, podLabels, c)
			oldNodeName := pod.Spec.NodeName

			ginkgo.By("Stopping the kubelet non gracefully for pod" + pod.Name)
			utils.KubeletCommand(ctx, utils.KStop, c, pod)
			ginkgo.DeferCleanup(utils.KubeletCommand, utils.KStart, c, pod)

			ginkgo.By("Adding out of service taint on node " + oldNodeName)
			// 给该节点打上 out-of-service 污点
			taint := v1.Taint{
				Key:    v1.TaintNodeOutOfService,
				Effect: v1.TaintEffectNoExecute,
			}
			e2enode.AddOrUpdateTaintOnNode(ctx, c, oldNodeName, taint)
			ginkgo.DeferCleanup(e2enode.RemoveTaintOffNode, c, oldNodeName, taint)

			ginkgo.By(fmt.Sprintf("Checking if the pod %s got rescheduled to a new node", pod.Name))
			labelSelectorStr := labels.SelectorFromSet(podLabels).String()
			podListOpts := metav1.ListOptions{
				LabelSelector: labelSelectorStr,
				FieldSelector: fields.OneTermNotEqualSelector("spec.nodeName", oldNodeName).String(),
			}
			_, err = e2epod.WaitForPods(ctx, c, ns, podListOpts, e2epod.Range{MinMatching: 1}, framework.PodStartTimeout, "be running and ready", e2epod.RunningReady)
			framework.ExpectNoError(err)

			// 让节点重新上线并移除污点
			utils.KubeletCommand(ctx, utils.KStart, c, pod)
			e2enode.RemoveTaintOffNode(ctx, c, oldNodeName, taint)

			// 验证 Pod 被调度到之前非优雅终止、现已重新上线的旧节点
			newDeploymentName := "sts-pod-gcepd-new"
			newPodLabels := map[string]string{"app": newDeploymentName}
			createAndVerifyStatefulDeployment(ctx, scName, newDeploymentName, ns, newPodLabels, c)
		})
	})
})

// createAndVerifyStatefulDeployment 依次：
// i) 用给定的 storage class 创建 PVC
// ii) 用该 PVC 创建副本数为 1 的 Deployment
// iii) 最后验证 Pod 处于 Running 且 Ready，并返回该 Pod 对象
func createAndVerifyStatefulDeployment(ctx context.Context, scName *string, name, ns string, podLabels map[string]string,
	c clientset.Interface) *v1.Pod {
	ginkgo.By("Creating a pvc using the storage class " + *scName)
	pvc := e2epv.MakePersistentVolumeClaim(e2epv.PersistentVolumeClaimConfig{
		StorageClassName: scName,
	}, ns)
	gotPVC, err := c.CoreV1().PersistentVolumeClaims(ns).Create(ctx, pvc, metav1.CreateOptions{})
	framework.ExpectNoError(err, "failed to create a persistent volume claim")

	ginkgo.By("Creating a deployment using the pvc " + pvc.Name)
	dep := makeDeployment(ns, name, gotPVC.Name, podLabels)
	_, err = c.AppsV1().Deployments(ns).Create(ctx, dep, metav1.CreateOptions{})
	framework.ExpectNoError(err, "failed to created the deployment")

	ginkgo.By(fmt.Sprintf("Ensuring that the pod of deployment %s is running and ready", dep.Name))
	labelSelector := labels.SelectorFromSet(labels.Set(podLabels))
	podList, err := e2epod.WaitForPodsWithLabelRunningReady(ctx, c, ns, labelSelector, 1, framework.PodStartTimeout)
	framework.ExpectNoError(err)
	pod := &podList.Items[0]
	return pod
}

func makeDeployment(ns, name, pvcName string, labels map[string]string) *appsv1.Deployment {
	ssReplicas := int32(1)
	return &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: ns,
		},
		Spec: appsv1.DeploymentSpec{
			Replicas: &ssReplicas,
			Selector: &metav1.LabelSelector{
				MatchLabels: labels,
			},
			Template: v1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: labels,
				},
				Spec: v1.PodSpec{
					Containers: []v1.Container{
						{
							Name:  "sts-pod-nginx",
							Image: imageutils.GetE2EImage(imageutils.Agnhost),
							Command: []string{
								"/bin/sh",
								"-c",
								"while true; do echo $(date) >> /mnt/managed/outfile; sleep 1; done",
							},
							VolumeMounts: []v1.VolumeMount{
								{
									Name:      "managed",
									MountPath: "/mnt/managed",
								},
							},
						},
					},
					Volumes: []v1.Volume{
						{
							Name: "managed",
							VolumeSource: v1.VolumeSource{
								PersistentVolumeClaim: &v1.PersistentVolumeClaimVolumeSource{
									ClaimName: pvcName,
								},
							},
						},
					},
				},
			},
		},
	}
}
