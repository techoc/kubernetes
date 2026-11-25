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

package storage

import (
	"context"
	"fmt"
	"strconv"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/apimachinery/pkg/util/uuid"
	"k8s.io/kubernetes/pkg/features"
	"k8s.io/kubernetes/test/e2e/framework"
	e2enode "k8s.io/kubernetes/test/e2e/framework/node"
	e2epod "k8s.io/kubernetes/test/e2e/framework/pod"
	e2erc "k8s.io/kubernetes/test/e2e/framework/rc"
	"k8s.io/kubernetes/test/e2e/storage/utils"
	imageutils "k8s.io/kubernetes/test/utils/image"
	admissionapi "k8s.io/pod-security-admission/api"

	"github.com/onsi/ginkgo/v2"
)

const (
	// 这些数值是通过经验得出的。
	// 如果设置得太低，当竞争 bug 再现时，
	// 测试会变得不稳定而非明确失败。
	// 如果 volume 数或 Pod 数设置得太高，
	// 测试可能失败，因为挂载 configmap/git_repo
	// volume 速度不快，测试可能在等待 Pod 进入 Running 时
	// 超时。
	// 当然，数值越高，
	// 测试越慢。
	wrappedVolumeRaceConfigMapVolumeCount    = 50
	wrappedVolumeRaceConfigMapPodCount       = 5
	wrappedVolumeRaceConfigMapIterationCount = 3
	wrappedVolumeRaceGitRepoVolumeCount      = 50
	wrappedVolumeRaceGitRepoPodCount         = 5
	wrappedVolumeRaceGitRepoIterationCount   = 3
	wrappedVolumeRaceRCNamePrefix            = "wrapped-volume-race-"
)

var _ = utils.SIGDescribe("EmptyDir wrapper volumes", func() {
	f := framework.NewDefaultFramework("emptydir-wrapper")
	f.NamespacePodSecurityLevel = admissionapi.LevelBaseline

	/*
		Release: v1.13
		Testname: EmptyDir Wrapper Volume, Secret and ConfigMap volumes, no conflict
		Description: 创建带数据的 Secret volume 和 ConfigMap volume。Pod MUST 能在挂载 Secret 和 ConfigMap volume 到容器的情况下启动。
	*/
	framework.ConformanceIt("should not conflict", func(ctx context.Context) {
		name := "emptydir-wrapper-test-" + string(uuid.NewUUID())
		volumeName := "secret-volume"
		volumeMountPath := "/etc/secret-volume"

		secret := &v1.Secret{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: f.Namespace.Name,
				Name:      name,
			},
			Data: map[string][]byte{
				"data-1": []byte("value-1\n"),
			},
		}

		var err error
		if secret, err = f.ClientSet.CoreV1().Secrets(f.Namespace.Name).Create(ctx, secret, metav1.CreateOptions{}); err != nil {
			framework.Failf("unable to create test secret %s: %v", secret.Name, err)
		}

		configMapVolumeName := "configmap-volume"
		configMapVolumeMountPath := "/etc/configmap-volume"

		configMap := &v1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: f.Namespace.Name,
				Name:      name,
			},
			BinaryData: map[string][]byte{
				"data-1": []byte("value-1\n"),
			},
		}

		if configMap, err = f.ClientSet.CoreV1().ConfigMaps(f.Namespace.Name).Create(ctx, configMap, metav1.CreateOptions{}); err != nil {
			framework.Failf("unable to create test configMap %s: %v", configMap.Name, err)
		}

		pod := &v1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name: "pod-secrets-" + string(uuid.NewUUID()),
			},
			Spec: v1.PodSpec{
				Volumes: []v1.Volume{
					{
						Name: volumeName,
						VolumeSource: v1.VolumeSource{
							Secret: &v1.SecretVolumeSource{
								SecretName: name,
							},
						},
					},
					{
						Name: configMapVolumeName,
						VolumeSource: v1.VolumeSource{
							ConfigMap: &v1.ConfigMapVolumeSource{
								LocalObjectReference: v1.LocalObjectReference{
									Name: name,
								},
							},
						},
					},
				},
				Containers: []v1.Container{
					{
						Name:  "secret-test",
						Image: imageutils.GetE2EImage(imageutils.Agnhost),
						Args:  []string{"test-webserver"},
						VolumeMounts: []v1.VolumeMount{
							{
								Name:      volumeName,
								MountPath: volumeMountPath,
								ReadOnly:  true,
							},
							{
								Name:      configMapVolumeName,
								MountPath: configMapVolumeMountPath,
							},
						},
					},
				},
			},
		}
		pod = e2epod.NewPodClient(f).CreateSync(ctx, pod)
		ginkgo.DeferCleanup(func(ctx context.Context) {
			ginkgo.By("Cleaning up the secret")
			if err := f.ClientSet.CoreV1().Secrets(f.Namespace.Name).Delete(ctx, secret.Name, metav1.DeleteOptions{}); err != nil {
				framework.Failf("unable to delete secret %v: %v", secret.Name, err)
			}
			ginkgo.By("Cleaning up the configmap")
			if err := f.ClientSet.CoreV1().ConfigMaps(f.Namespace.Name).Delete(ctx, configMap.Name, metav1.DeleteOptions{}); err != nil {
				framework.Failf("unable to delete configmap %v: %v", configMap.Name, err)
			}
			ginkgo.By("Cleaning up the pod")
			if err = f.ClientSet.CoreV1().Pods(f.Namespace.Name).Delete(ctx, pod.Name, *metav1.NewDeleteOptions(0)); err != nil {
				framework.Failf("unable to delete pod %v: %v", pod.Name, err)
			}
		})
	})

	// 以下两个测试检查 #29641 修复的问题。
	// 要复现该问题，需要撤销修复，例如：
	// git revert -n df1e925143daf34199b55ffb91d0598244888cce
	// or
	// curl -sL https://github.com/kubernetes/kubernetes/pull/29641.patch | patch -p1 -R
	//
	// 之后这些测试将失败，因为它们创建的部分 Pod
	// 永远不会进入 Running 状态。
	//
	// 它们需要标记为 [Serial] 和 [Slow]，因为它们试图通过创建
	// 带大量 volume 和容器挂载的 Pod 来诱发竞争，
	// 这需要相当长的时间，且可能干扰其他测试。
	//
	// 或许也应为 secrets 和 downwardapi 编写测试，
	// 但这些情况更难，因为基于 tmpfs 的 emptyDir
	// 似乎较少受该竞争问题影响。

	/*
		Release: v1.13
		Testname: EmptyDir Wrapper Volume, ConfigMap volumes, no race
		Description: 创建 50 个 ConfigMap volume 和 5 个挂载这些 ConfigMap volume 的 Pod 副本。Pod MUST NOT 因等待 volume 而失败。
	*/
	framework.ConformanceIt("should not cause race condition when used for configmaps", f.WithSerial(), func(ctx context.Context) {
		configMapNames := createConfigmapsForRace(ctx, f)
		ginkgo.DeferCleanup(deleteConfigMaps, f, configMapNames)
		volumes, volumeMounts := makeConfigMapVolumes(configMapNames)
		for range wrappedVolumeRaceConfigMapIterationCount {
			testNoWrappedVolumeRace(ctx, f, volumes, volumeMounts, wrappedVolumeRaceConfigMapPodCount)
		}
	})

	// 设计上较慢[约 150 秒]。
	// 此测试使用已弃用的 GitRepo VolumeSource，因此 MUST NOT 被提升为 Conformance 测试。
	// 要为容器提供 git 仓库，可将 EmptyDir 挂载到用 git 克隆仓库的 InitContainer，然后再将该 EmptyDir 挂载到 Pod 的容器中。
	// 这种 projected volume 映射方法也可用于 secrets 和 downwardapi VolumeSource 的测试，但它们较少受竞争问题影响。
	f.It("should not cause race condition when used for git_repo", f.WithFeatureGate(features.GitRepoVolumeDriver), f.WithSerial(), f.WithSlow(), func(ctx context.Context) {
		gitURL, gitRepo, cleanup := createGitServer(ctx, f)
		defer cleanup()
		volumes, volumeMounts := makeGitRepoVolumes(gitURL, gitRepo)
		for range wrappedVolumeRaceGitRepoIterationCount {
			testNoWrappedVolumeRace(ctx, f, volumes, volumeMounts, wrappedVolumeRaceGitRepoPodCount)
		}
	})
})

func createGitServer(ctx context.Context, f *framework.Framework) (gitURL string, gitRepo string, cleanup func()) {
	var err error
	gitServerPodName := "git-server-" + string(uuid.NewUUID())
	containerPort := int32(8000)

	labels := map[string]string{"name": gitServerPodName}

	gitServerPod := e2epod.NewAgnhostPod(f.Namespace.Name, gitServerPodName, nil, nil, []v1.ContainerPort{{ContainerPort: int32(containerPort)}}, "fake-gitserver")
	gitServerPod.ObjectMeta.Labels = labels
	e2epod.NewPodClient(f).CreateSync(ctx, gitServerPod)

	// Portal IP 和端口
	httpPort := 2345

	gitServerSvc := &v1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name: "git-server-svc",
		},
		Spec: v1.ServiceSpec{
			Selector: labels,
			Ports: []v1.ServicePort{
				{
					Name:       "http-portal",
					Port:       int32(httpPort),
					TargetPort: intstr.FromInt32(containerPort),
				},
			},
		},
	}

	if gitServerSvc, err = f.ClientSet.CoreV1().Services(f.Namespace.Name).Create(ctx, gitServerSvc, metav1.CreateOptions{}); err != nil {
		framework.Failf("unable to create test git server service %s: %v", gitServerSvc.Name, err)
	}

	return "http://" + gitServerSvc.Spec.ClusterIP + ":" + strconv.Itoa(httpPort), "test", func() {
		ginkgo.By("Cleaning up the git server pod")
		if err := f.ClientSet.CoreV1().Pods(f.Namespace.Name).Delete(ctx, gitServerPod.Name, *metav1.NewDeleteOptions(0)); err != nil {
			framework.Failf("unable to delete git server pod %v: %v", gitServerPod.Name, err)
		}
		ginkgo.By("Cleaning up the git server svc")
		if err := f.ClientSet.CoreV1().Services(f.Namespace.Name).Delete(ctx, gitServerSvc.Name, metav1.DeleteOptions{}); err != nil {
			framework.Failf("unable to delete git server svc %v: %v", gitServerSvc.Name, err)
		}
	}
}

func makeGitRepoVolumes(gitURL, gitRepo string) (volumes []v1.Volume, volumeMounts []v1.VolumeMount) {
	for i := range wrappedVolumeRaceGitRepoVolumeCount {
		volumeName := fmt.Sprintf("racey-git-repo-%d", i)
		volumes = append(volumes, v1.Volume{
			Name: volumeName,
			VolumeSource: v1.VolumeSource{
				GitRepo: &v1.GitRepoVolumeSource{
					Repository: gitURL,
					Directory:  gitRepo,
				},
			},
		})
		volumeMounts = append(volumeMounts, v1.VolumeMount{
			Name:      volumeName,
			MountPath: fmt.Sprintf("/etc/git-volume-%d", i),
		})
	}
	return
}

func createConfigmapsForRace(ctx context.Context, f *framework.Framework) (configMapNames []string) {
	ginkgo.By(fmt.Sprintf("Creating %d configmaps", wrappedVolumeRaceConfigMapVolumeCount))
	for i := range wrappedVolumeRaceConfigMapVolumeCount {
		configMapName := fmt.Sprintf("racey-configmap-%d", i)
		configMapNames = append(configMapNames, configMapName)
		configMap := &v1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: f.Namespace.Name,
				Name:      configMapName,
			},
			Data: map[string]string{
				"data-1": "value-1",
			},
		}
		_, err := f.ClientSet.CoreV1().ConfigMaps(f.Namespace.Name).Create(ctx, configMap, metav1.CreateOptions{})
		framework.ExpectNoError(err)
	}
	return
}

func deleteConfigMaps(ctx context.Context, f *framework.Framework, configMapNames []string) {
	ginkgo.By("Cleaning up the configMaps")
	for _, configMapName := range configMapNames {
		err := f.ClientSet.CoreV1().ConfigMaps(f.Namespace.Name).Delete(ctx, configMapName, metav1.DeleteOptions{})
		framework.ExpectNoError(err, "unable to delete configMap %v", configMapName)
	}
}

func makeConfigMapVolumes(configMapNames []string) (volumes []v1.Volume, volumeMounts []v1.VolumeMount) {
	for i, configMapName := range configMapNames {
		volumeName := fmt.Sprintf("racey-configmap-%d", i)
		volumes = append(volumes, v1.Volume{
			Name: volumeName,
			VolumeSource: v1.VolumeSource{
				ConfigMap: &v1.ConfigMapVolumeSource{
					LocalObjectReference: v1.LocalObjectReference{
						Name: configMapName,
					},
					Items: []v1.KeyToPath{
						{
							Key:  "data-1",
							Path: "data-1",
						},
					},
				},
			},
		})
		volumeMounts = append(volumeMounts, v1.VolumeMount{
			Name:      volumeName,
			MountPath: fmt.Sprintf("/etc/config-%d", i),
		})
	}
	return
}

func testNoWrappedVolumeRace(ctx context.Context, f *framework.Framework, volumes []v1.Volume, volumeMounts []v1.VolumeMount, podCount int32) {
	const nodeHostnameLabelKey = "kubernetes.io/hostname"

	rcName := wrappedVolumeRaceRCNamePrefix + string(uuid.NewUUID())
	rcLabels := map[string]string{
		"name": rcName,
	}
	targetNode, err := e2enode.GetRandomReadySchedulableNode(ctx, f.ClientSet)
	framework.ExpectNoError(err)

	ginkgo.By("Creating RC which spawns configmap-volume pods")
	affinity := &v1.Affinity{
		NodeAffinity: &v1.NodeAffinity{
			RequiredDuringSchedulingIgnoredDuringExecution: &v1.NodeSelector{
				NodeSelectorTerms: []v1.NodeSelectorTerm{
					{
						MatchExpressions: []v1.NodeSelectorRequirement{
							{
								Key:      nodeHostnameLabelKey,
								Operator: v1.NodeSelectorOpIn,
								Values:   []string{targetNode.Labels[nodeHostnameLabelKey]},
							},
						},
					},
				},
			},
		},
	}

	rc := &v1.ReplicationController{
		ObjectMeta: metav1.ObjectMeta{
			Name: rcName,
		},
		Spec: v1.ReplicationControllerSpec{
			Replicas: &podCount,
			Selector: rcLabels,
			Template: &v1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: rcLabels,
				},
				Spec: v1.PodSpec{
					Containers: []v1.Container{
						{
							Name:         "test-container",
							Image:        imageutils.GetE2EImage(imageutils.Pause),
							VolumeMounts: volumeMounts,
						},
					},
					Affinity:  affinity,
					DNSPolicy: v1.DNSDefault,
					Volumes:   volumes,
				},
			},
		},
	}
	_, err = f.ClientSet.CoreV1().ReplicationControllers(f.Namespace.Name).Create(ctx, rc, metav1.CreateOptions{})
	framework.ExpectNoError(err, "error creating replication controller")

	ginkgo.DeferCleanup(e2erc.DeleteRCAndWaitForGC, f.ClientSet, f.Namespace.Name, rcName)

	pods, err := e2epod.PodsCreatedByLabel(ctx, f.ClientSet, f.Namespace.Name, rcName, podCount, labels.SelectorFromSet(rcLabels))
	framework.ExpectNoError(err, "error creating pods")

	ginkgo.By("Ensuring each pod is running")

	// 等待 Pod 进入运行态。循环等待直到 Pod 运行，
	// 验证有服务在监听。
	for _, pod := range pods.Items {
		if pod.DeletionTimestamp != nil {
			continue
		}
		err = e2epod.WaitForPodNameRunningInNamespace(ctx, f.ClientSet, pod.Name, f.Namespace.Name)
		framework.ExpectNoError(err, "Failed waiting for pod %s to enter running state", pod.Name)
	}
}
