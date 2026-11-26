/*
Copyright 2014 The Kubernetes Authors.

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
	"time"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/uuid"
	"k8s.io/kubernetes/pkg/features"
	"k8s.io/kubernetes/test/e2e/framework"
	e2epod "k8s.io/kubernetes/test/e2e/framework/pod"
	e2epodoutput "k8s.io/kubernetes/test/e2e/framework/pod/output"
	e2eskipper "k8s.io/kubernetes/test/e2e/framework/skipper"
	imageutils "k8s.io/kubernetes/test/utils/image"
	admissionapi "k8s.io/pod-security-admission/api"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
)

var _ = SIGDescribe("Projected downwardAPI", func() {
	f := framework.NewDefaultFramework("projected")
	f.NamespacePodSecurityLevel = admissionapi.LevelBaseline

	// 等待 log pod 显示的超时时间
	const podLogTimeout = 2 * time.Minute
	var podClient *e2epod.PodClient
	ginkgo.BeforeEach(func() {
		podClient = e2epod.NewPodClient(f)
	})

	/*
	   发布版本：v1.9
	   测试名称：Projected Volume, DownwardAPI, pod name
	   描述：创建带 downwardAPI projected volume source 的 Pod，包含 pod name、cpu 和 memory limits 以及 requests。Pod 必须能从挂载的 DownwardAPIVolumeFiles 中读取 pod name。
	*/
	framework.ConformanceIt("should provide podname only", f.WithNodeConformance(), func(ctx context.Context) {
		podName := "downwardapi-volume-" + string(uuid.NewUUID())
		pod := downwardAPIVolumePodForSimpleTest(podName, "/etc/podinfo/podname")

		e2epodoutput.TestContainerOutput(ctx, f, "downward API volume plugin", pod, 0, []string{
			fmt.Sprintf("%s\n", podName),
		})
	})

	/*
	   发布版本：v1.9
	   测试名称：Projected Volume, DownwardAPI, volume mode 0400
	   描述：创建带 downwardAPI projected volume source 的 Pod，包含 pod name、cpu 和 memory limits 以及 requests。volume mount 的默认模式设为 0400。Pod 必须能从挂载的 DownwardAPIVolumeFiles 中读取 pod name，volume 模式必须为 -r--------。
	   此测试标记为 LinuxOnly，因为 Windows 不支持设置特定文件权限。
	*/
	framework.ConformanceIt("should set DefaultMode on files [LinuxOnly]", f.WithNodeConformance(), func(ctx context.Context) {
		podName := "downwardapi-volume-" + string(uuid.NewUUID())
		defaultMode := int32(0400)
		pod := projectedDownwardAPIVolumePodForModeTest(podName, "/etc/podinfo/podname", nil, &defaultMode)

		e2epodoutput.TestContainerOutput(ctx, f, "downward API volume plugin", pod, 0, []string{
			"mode of file \"/etc/podinfo/podname\": -r--------",
		})
	})

	f.It("should set DefaultUser on files [LinuxOnly]", f.WithFeatureGate(features.AtomicWriteVolumeUserFields), func(ctx context.Context) {
		podName := "downwardapi-volume-" + string(uuid.NewUUID())
		defaultUser := int64(1000)
		pod := projectedDownwardAPIVolumePodForUserTest(podName, "/etc/podinfo/podname", nil, &defaultUser, &defaultUser)

		e2epodoutput.TestContainerOutput(ctx, f, "downward API volume plugin", pod, 0, []string{
			"owner UID of \"/etc/podinfo/podname\": 1000",
		})
	})

	/*
	   发布版本：v1.9
	   测试名称：Projected Volume, DownwardAPI, volume mode 0400
	   描述：创建带 downwardAPI projected volume source 的 Pod，包含 pod name、cpu 和 memory limits 以及 requests。volume mount 的默认模式设为 0400。Pod 必须能从挂载的 DownwardAPIVolumeFiles 中读取 pod name，volume 模式必须为 -r--------。
	   此测试标记为 LinuxOnly，因为 Windows 不支持设置特定文件权限。
	*/
	framework.ConformanceIt("should set mode on item file [LinuxOnly]", f.WithNodeConformance(), func(ctx context.Context) {
		podName := "downwardapi-volume-" + string(uuid.NewUUID())
		mode := int32(0400)
		pod := projectedDownwardAPIVolumePodForModeTest(podName, "/etc/podinfo/podname", &mode, nil)

		e2epodoutput.TestContainerOutput(ctx, f, "downward API volume plugin", pod, 0, []string{
			"mode of file \"/etc/podinfo/podname\": -r--------",
		})
	})

	f.It("should set user on item file [LinuxOnly]", f.WithFeatureGate(features.AtomicWriteVolumeUserFields), func(ctx context.Context) {
		podName := "downwardapi-volume-" + string(uuid.NewUUID())
		user := int64(1000)
		pod := projectedDownwardAPIVolumePodForUserTest(podName, "/etc/podinfo/podname", &user, nil, &user)

		e2epodoutput.TestContainerOutput(ctx, f, "downward API volume plugin", pod, 0, []string{
			"owner UID of \"/etc/podinfo/podname\": 1000",
		})
	})

	f.It("should provide podname as non-root with fsgroup [LinuxOnly]", f.WithNodeConformance(), func(ctx context.Context) {
		// Windows 不支持 RunAsUser / FSGroup SecurityContext 选项。
		e2eskipper.SkipIfNodeOSDistroIs("windows")
		podName := "metadata-volume-" + string(uuid.NewUUID())
		gid := int64(1234)
		pod := downwardAPIVolumePodForSimpleTest(podName, "/etc/podinfo/podname")
		pod.Spec.SecurityContext = &v1.PodSecurityContext{
			FSGroup: &gid,
		}
		setPodNonRootUser(pod)
		e2epodoutput.TestContainerOutput(ctx, f, "downward API volume plugin", pod, 0, []string{
			fmt.Sprintf("%s\n", podName),
		})
	})

	f.It("should provide podname as non-root with fsgroup and defaultMode [LinuxOnly]", f.WithNodeConformance(), func(ctx context.Context) {
		// Windows 不支持 RunAsUser / FSGroup SecurityContext 选项，也不支持设置文件权限。
		e2eskipper.SkipIfNodeOSDistroIs("windows")
		podName := "metadata-volume-" + string(uuid.NewUUID())
		gid := int64(1234)
		mode := int32(0440) /* setting fsGroup sets mode to at least 440 */
		pod := projectedDownwardAPIVolumePodForModeTest(podName, "/etc/podinfo/podname", &mode, nil)
		pod.Spec.SecurityContext = &v1.PodSecurityContext{
			FSGroup: &gid,
		}
		setPodNonRootUser(pod)
		e2epodoutput.TestContainerOutput(ctx, f, "downward API volume plugin", pod, 0, []string{
			"mode of file \"/etc/podinfo/podname\": -r--r-----",
		})
	})

	/*
	   发布版本：v1.9
	   测试名称：Projected Volume, DownwardAPI, update labels
	   描述：创建带 downwardAPI projected volume source 的 Pod，包含 pod name、cpu 和 memory limits、requests 以及 label items。Pod 必须能从挂载的 DownwardAPIVolumeFiles 中读取标签。随后更新标签，Pod 必须能读到更新后的值。
	*/
	framework.ConformanceIt("should update labels on modification", f.WithNodeConformance(), func(ctx context.Context) {
		labels := map[string]string{}
		labels["key1"] = "value1"
		labels["key2"] = "value2"

		podName := "labelsupdate" + string(uuid.NewUUID())
		pod := projectedDownwardAPIVolumePodForUpdateTest(podName, labels, map[string]string{}, "/etc/podinfo/labels")
		containerName := "client-container"
		ginkgo.By("Creating the pod")
		podClient.CreateSync(ctx, pod)

		gomega.Eventually(ctx, func() (string, error) {
			return e2epod.GetPodLogs(ctx, f.ClientSet, f.Namespace.Name, podName, containerName)
		},
			podLogTimeout, framework.Poll).Should(gomega.ContainSubstring("key1=\"value1\"\n"))

		//修改标签
		podClient.Update(ctx, podName, func(pod *v1.Pod) {
			pod.Labels["key3"] = "value3"
		})

		gomega.Eventually(ctx, func() (string, error) {
			return e2epod.GetPodLogs(ctx, f.ClientSet, f.Namespace.Name, pod.Name, containerName)
		},
			podLogTimeout, framework.Poll).Should(gomega.ContainSubstring("key3=\"value3\"\n"))
	})

	/*
	   发布版本：v1.9
	   测试名称：Projected Volume, DownwardAPI, update annotation
	   描述：创建带 downwardAPI projected volume source 的 Pod，包含 pod name、cpu 和 memory limits、requests 以及 annotation items。Pod 必须能从挂载的 DownwardAPIVolumeFiles 中读取注解。随后更新注解，Pod 必须能读到更新后的值。
	*/
	framework.ConformanceIt("should update annotations on modification", f.WithNodeConformance(), func(ctx context.Context) {
		annotations := map[string]string{}
		annotations["builder"] = "bar"
		podName := "annotationupdate" + string(uuid.NewUUID())
		pod := projectedDownwardAPIVolumePodForUpdateTest(podName, map[string]string{}, annotations, "/etc/podinfo/annotations")

		containerName := "client-container"
		ginkgo.By("Creating the pod")
		pod = podClient.CreateSync(ctx, pod)

		gomega.Eventually(ctx, func() (string, error) {
			return e2epod.GetPodLogs(ctx, f.ClientSet, f.Namespace.Name, pod.Name, containerName)
		},
			podLogTimeout, framework.Poll).Should(gomega.ContainSubstring("builder=\"bar\"\n"))

		//修改注解
		podClient.Update(ctx, podName, func(pod *v1.Pod) {
			pod.Annotations["builder"] = "foo"
		})

		gomega.Eventually(ctx, func() (string, error) {
			return e2epod.GetPodLogs(ctx, f.ClientSet, f.Namespace.Name, pod.Name, containerName)
		},
			podLogTimeout, framework.Poll).Should(gomega.ContainSubstring("builder=\"foo\"\n"))
	})

	/*
	   发布版本：v1.9
	   测试名称：Projected Volume, DownwardAPI, CPU limits
	   描述：创建带 downwardAPI projected volume source 的 Pod，包含 pod name、cpu 和 memory limits 以及 requests。Pod 必须能从挂载的 DownwardAPIVolumeFiles 中读取 cpu limits。
	*/
	framework.ConformanceIt("should provide container's cpu limit", f.WithNodeConformance(), func(ctx context.Context) {
		podName := "downwardapi-volume-" + string(uuid.NewUUID())
		pod := downwardAPIVolumeForContainerResources(podName, "/etc/podinfo/cpu_limit")

		e2epodoutput.TestContainerOutput(ctx, f, "downward API volume plugin", pod, 0, []string{
			fmt.Sprintf("2\n"),
		})
	})

	/*
	   发布版本：v1.9
	   测试名称：Projected Volume, DownwardAPI, memory limits
	   描述：创建带 downwardAPI projected volume source 的 Pod，包含 pod name、cpu 和 memory limits 以及 requests。Pod 必须能从挂载的 DownwardAPIVolumeFiles 中读取 memory limits。
	*/
	framework.ConformanceIt("should provide container's memory limit", f.WithNodeConformance(), func(ctx context.Context) {
		podName := "downwardapi-volume-" + string(uuid.NewUUID())
		pod := downwardAPIVolumeForContainerResources(podName, "/etc/podinfo/memory_limit")

		e2epodoutput.TestContainerOutput(ctx, f, "downward API volume plugin", pod, 0, []string{
			"134217728\n",
		})
	})

	/*
	   发布版本：v1.9
	   测试名称：Projected Volume, DownwardAPI, CPU request
	   描述：创建带 downwardAPI projected volume source 的 Pod，包含 pod name、cpu 和 memory limits 以及 requests。Pod 必须能从挂载的 DownwardAPIVolumeFiles 中读取 cpu request。
	*/
	framework.ConformanceIt("should provide container's cpu request", f.WithNodeConformance(), func(ctx context.Context) {
		podName := "downwardapi-volume-" + string(uuid.NewUUID())
		pod := downwardAPIVolumeForContainerResources(podName, "/etc/podinfo/cpu_request")

		e2epodoutput.TestContainerOutput(ctx, f, "downward API volume plugin", pod, 0, []string{
			fmt.Sprintf("1\n"),
		})
	})

	/*
	   发布版本：v1.9
	   测试名称：Projected Volume, DownwardAPI, memory request
	   描述：创建带 downwardAPI projected volume source 的 Pod，包含 pod name、cpu 和 memory limits 以及 requests。Pod 必须能从挂载的 DownwardAPIVolumeFiles 中读取 memory request。
	*/
	framework.ConformanceIt("should provide container's memory request", f.WithNodeConformance(), func(ctx context.Context) {
		podName := "downwardapi-volume-" + string(uuid.NewUUID())
		pod := downwardAPIVolumeForContainerResources(podName, "/etc/podinfo/memory_request")

		e2epodoutput.TestContainerOutput(ctx, f, "downward API volume plugin", pod, 0, []string{
			fmt.Sprintf("33554432\n"),
		})
	})

	/*
	   发布版本：v1.9
	   测试名称：Projected Volume, DownwardAPI, CPU limit, node allocatable
	   描述：创建带 downwardAPI projected volume source 的 Pod，包含 pod name、cpu 和 memory limits 以及 requests。容器没有指定 requests 和 limits 的 CPU 与内存资源。Pod 必须能从挂载的 DownwardAPIVolumeFiles 中读取默认 cpu limits。
	*/
	framework.ConformanceIt("should provide node allocatable (cpu) as default cpu limit if the limit is not set", f.WithNodeConformance(), func(ctx context.Context) {
		podName := "downwardapi-volume-" + string(uuid.NewUUID())
		pod := downwardAPIVolumeForDefaultContainerResources(podName, "/etc/podinfo/cpu_limit")

		e2epodoutput.TestContainerOutputRegexp(ctx, f, "downward API volume plugin", pod, 0, []string{"[1-9]"})
	})

	/*
	   发布版本：v1.9
	   测试名称：Projected Volume, DownwardAPI, memory limit, node allocatable
	   描述：创建带 downwardAPI projected volume source 的 Pod，包含 pod name、cpu 和 memory limits 以及 requests。容器没有指定 requests 和 limits 的 CPU 与内存资源。Pod 必须能从挂载的 DownwardAPIVolumeFiles 中读取默认 memory limits。
	*/
	framework.ConformanceIt("should provide node allocatable (memory) as default memory limit if the limit is not set", f.WithNodeConformance(), func(ctx context.Context) {
		podName := "downwardapi-volume-" + string(uuid.NewUUID())
		pod := downwardAPIVolumeForDefaultContainerResources(podName, "/etc/podinfo/memory_limit")

		e2epodoutput.TestContainerOutputRegexp(ctx, f, "downward API volume plugin", pod, 0, []string{"[1-9]"})
	})
})

func projectedDownwardAPIVolumePodForModeTest(name, filePath string, itemMode, defaultMode *int32) *v1.Pod {
	pod := projectedDownwardAPIVolumeBasePod(name, nil, nil)

	pod.Spec.Containers = []v1.Container{
		{
			Name:  "client-container",
			Image: imageutils.GetE2EImage(imageutils.Agnhost),
			Args:  []string{"mounttest", "--file_mode=" + filePath},
			VolumeMounts: []v1.VolumeMount{
				{
					Name:      "podinfo",
					MountPath: "/etc/podinfo",
				},
			},
		},
	}
	if itemMode != nil {
		pod.Spec.Volumes[0].VolumeSource.Projected.Sources[0].DownwardAPI.Items[0].Mode = itemMode
	}
	if defaultMode != nil {
		pod.Spec.Volumes[0].VolumeSource.Projected.DefaultMode = defaultMode
	}

	return pod
}

func projectedDownwardAPIVolumePodForUserTest(name, filePath string, itemUser, defaultUser, runAsUser *int64) *v1.Pod {
	pod := projectedDownwardAPIVolumeBasePod(name, nil, nil)

	pod.Spec.Containers = []v1.Container{
		{
			Name:  "client-container",
			Image: imageutils.GetE2EImage(imageutils.Agnhost),
			Args:  []string{"mounttest", "--file_owner=" + filePath},
			VolumeMounts: []v1.VolumeMount{
				{
					Name:      "podinfo",
					MountPath: "/etc/podinfo",
				},
			},
		},
	}
	if runAsUser != nil {
		pod.Spec.SecurityContext = &v1.PodSecurityContext{
			RunAsUser: runAsUser,
		}
	}
	if itemUser != nil {
		pod.Spec.Volumes[0].VolumeSource.Projected.Sources[0].DownwardAPI.Items[0].User = itemUser
	}
	if defaultUser != nil {
		pod.Spec.Volumes[0].VolumeSource.Projected.DefaultUser = defaultUser
	}

	return pod
}

func projectedDownwardAPIVolumePodForUpdateTest(name string, labels, annotations map[string]string, filePath string) *v1.Pod {
	pod := projectedDownwardAPIVolumeBasePod(name, labels, annotations)

	pod.Spec.Containers = []v1.Container{
		{
			Name:  "client-container",
			Image: imageutils.GetE2EImage(imageutils.Agnhost),
			Args:  []string{"mounttest", "--break_on_expected_content=false", "--retry_time=1200", "--file_content_in_loop=" + filePath},
			VolumeMounts: []v1.VolumeMount{
				{
					Name:      "podinfo",
					MountPath: "/etc/podinfo",
					ReadOnly:  false,
				},
			},
		},
	}

	applyLabelsAndAnnotationsToProjectedDownwardAPIPod(labels, annotations, pod)
	return pod
}

func projectedDownwardAPIVolumeBasePod(name string, labels, annotations map[string]string) *v1.Pod {
	pod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:        name,
			Labels:      labels,
			Annotations: annotations,
		},
		Spec: v1.PodSpec{
			Volumes: []v1.Volume{
				{
					Name: "podinfo",
					VolumeSource: v1.VolumeSource{
						Projected: &v1.ProjectedVolumeSource{
							Sources: []v1.VolumeProjection{
								{
									DownwardAPI: &v1.DownwardAPIProjection{
										Items: []v1.DownwardAPIVolumeFile{
											{
												Path: "podname",
												FieldRef: &v1.ObjectFieldSelector{
													APIVersion: "v1",
													FieldPath:  "metadata.name",
												},
											},
											{
												Path: "cpu_limit",
												ResourceFieldRef: &v1.ResourceFieldSelector{
													ContainerName: "client-container",
													Resource:      "limits.cpu",
												},
											},
											{
												Path: "cpu_request",
												ResourceFieldRef: &v1.ResourceFieldSelector{
													ContainerName: "client-container",
													Resource:      "requests.cpu",
												},
											},
											{
												Path: "memory_limit",
												ResourceFieldRef: &v1.ResourceFieldSelector{
													ContainerName: "client-container",
													Resource:      "limits.memory",
												},
											},
											{
												Path: "memory_request",
												ResourceFieldRef: &v1.ResourceFieldSelector{
													ContainerName: "client-container",
													Resource:      "requests.memory",
												},
											},
										},
									},
								},
							},
						},
					},
				},
			},
			RestartPolicy: v1.RestartPolicyNever,
		},
	}

	return pod
}

func applyLabelsAndAnnotationsToProjectedDownwardAPIPod(labels, annotations map[string]string, pod *v1.Pod) {
	if len(labels) > 0 {
		pod.Spec.Volumes[0].VolumeSource.Projected.Sources[0].DownwardAPI.Items = append(pod.Spec.Volumes[0].VolumeSource.Projected.Sources[0].DownwardAPI.Items, v1.DownwardAPIVolumeFile{
			Path: "labels",
			FieldRef: &v1.ObjectFieldSelector{
				APIVersion: "v1",
				FieldPath:  "metadata.labels",
			},
		})
	}

	if len(annotations) > 0 {
		pod.Spec.Volumes[0].VolumeSource.Projected.Sources[0].DownwardAPI.Items = append(pod.Spec.Volumes[0].VolumeSource.Projected.Sources[0].DownwardAPI.Items, v1.DownwardAPIVolumeFile{
			Path: "annotations",
			FieldRef: &v1.ObjectFieldSelector{
				APIVersion: "v1",
				FieldPath:  "metadata.annotations",
			},
		})
	}
}
