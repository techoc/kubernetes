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

package node

import (
	"context"
	"fmt"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/uuid"
	"k8s.io/kubernetes/pkg/features"
	"k8s.io/kubernetes/test/e2e/feature"
	"k8s.io/kubernetes/test/e2e/framework"
	e2enetwork "k8s.io/kubernetes/test/e2e/framework/network"
	e2epod "k8s.io/kubernetes/test/e2e/framework/pod"
	e2epodoutput "k8s.io/kubernetes/test/e2e/framework/pod/output"
	imageutils "k8s.io/kubernetes/test/utils/image"
	admissionapi "k8s.io/pod-security-admission/api"
)

var _ = SIGDescribe("Downward API", func() {
	f := framework.NewDefaultFramework("downward-api")
	f.NamespacePodSecurityLevel = admissionapi.LevelPrivileged

	/*
	   Release: v1.9
	   Testname: DownwardAPI, environment for name, namespace and ip
	   Description: Downward API MUST 将 Pod 和 Container 的字段作为环境变量暴露。在 Pod Spec 中指定 Pod 名称、命名空间和 IP 作为环境变量，它们在容器运行时可见。
	*/
	framework.ConformanceIt("should provide pod name, namespace and IP address as env vars", f.WithNodeConformance(), func(ctx context.Context) {
		podName := "downward-api-" + string(uuid.NewUUID())
		env := []v1.EnvVar{
			{
				Name: "POD_NAME",
				ValueFrom: &v1.EnvVarSource{
					FieldRef: &v1.ObjectFieldSelector{
						APIVersion: "v1",
						FieldPath:  "metadata.name",
					},
				},
			},
			{
				Name: "POD_NAMESPACE",
				ValueFrom: &v1.EnvVarSource{
					FieldRef: &v1.ObjectFieldSelector{
						APIVersion: "v1",
						FieldPath:  "metadata.namespace",
					},
				},
			},
			{
				Name: "POD_IP",
				ValueFrom: &v1.EnvVarSource{
					FieldRef: &v1.ObjectFieldSelector{
						APIVersion: "v1",
						FieldPath:  "status.podIP",
					},
				},
			},
		}

		expectations := []string{
			fmt.Sprintf("POD_NAME=%v", podName),
			fmt.Sprintf("POD_NAMESPACE=%v", f.Namespace.Name),
			fmt.Sprintf("POD_IP=%v|%v", e2enetwork.RegexIPv4, e2enetwork.RegexIPv6),
		}

		testDownwardAPI(ctx, f, podName, env, expectations)
	})

	/*
	   Release: v1.9
	   Testname: DownwardAPI, environment for host ip
	   Description: Downward API MUST 将 Pod 和 Container 的字段作为环境变量暴露。在 Pod Spec 中指定 host IP 作为环境变量，它在容器运行时可见。
	*/
	framework.ConformanceIt("should provide host IP as an env var", f.WithNodeConformance(), func(ctx context.Context) {
		podName := "downward-api-" + string(uuid.NewUUID())
		env := []v1.EnvVar{
			{
				Name: "HOST_IP",
				ValueFrom: &v1.EnvVarSource{
					FieldRef: &v1.ObjectFieldSelector{
						APIVersion: "v1",
						FieldPath:  "status.hostIP",
					},
				},
			},
		}

		expectations := []string{
			fmt.Sprintf("HOST_IP=%v|%v", e2enetwork.RegexIPv4, e2enetwork.RegexIPv6),
		}

		testDownwardAPI(ctx, f, podName, env, expectations)
	})

	/*
	   Release: v1.32
	   Testname: DownwardAPI, environment for hostIPs
	   Description: Downward API MUST 将 Pod 和 Container 的字段作为环境变量暴露。在 Pod Spec 中指定 hostIPs 作为环境变量，它们在容器运行时可见。
	*/
	framework.ConformanceIt("should provide hostIPs as an env var", f.WithNodeConformance(), func(ctx context.Context) {
		podName := "downward-api-" + string(uuid.NewUUID())
		env := []v1.EnvVar{
			{
				Name: "HOST_IP",
				ValueFrom: &v1.EnvVarSource{
					FieldRef: &v1.ObjectFieldSelector{
						APIVersion: "v1",
						FieldPath:  "status.hostIP",
					},
				},
			},
		}

		expectations := []string{
			fmt.Sprintf("HOST_IP=%v|%v", e2enetwork.RegexIPv4, e2enetwork.RegexIPv6),
		}

		testDownwardAPI(ctx, f, podName, env, expectations)
	})

	f.It("should provide host IP and pod IP as an env var if pod uses host network [LinuxOnly]", f.WithNodeConformance(), func(ctx context.Context) {
		podName := "downward-api-" + string(uuid.NewUUID())
		env := []v1.EnvVar{
			{
				Name: "HOST_IP",
				ValueFrom: &v1.EnvVarSource{
					FieldRef: &v1.ObjectFieldSelector{
						APIVersion: "v1",
						FieldPath:  "status.hostIP",
					},
				},
			},
			{
				Name: "POD_IP",
				ValueFrom: &v1.EnvVarSource{
					FieldRef: &v1.ObjectFieldSelector{
						APIVersion: "v1",
						FieldPath:  "status.podIP",
					},
				},
			},
		}

		expectations := []string{
			"OK",
		}

		pod := &v1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:   podName,
				Labels: map[string]string{"name": podName},
			},
			Spec: v1.PodSpec{
				Containers: []v1.Container{
					{
						Name:    "dapi-container",
						Image:   imageutils.GetE2EImage(imageutils.BusyBox),
						Command: []string{"sh", "-c", `[[ "${HOST_IP:?}" == "${POD_IP:?}" ]] && echo 'OK' || echo "HOST_IP: '${HOST_IP}' != POD_IP: '${POD_IP}'"`},
						Env:     env,
					},
				},
				HostNetwork:   true,
				RestartPolicy: v1.RestartPolicyNever,
			},
		}

		testDownwardAPIUsingPod(ctx, f, pod, env, expectations)

	})

	/*
	   Release: v1.9
	   Testname: DownwardAPI, environment for CPU and memory limits and requests
	   Description: Downward API MUST 通过环境变量在容器运行时暴露设置的 CPU request 和 Memory request。
	*/
	framework.ConformanceIt("should provide container's limits.cpu/memory and requests.cpu/memory as env vars", f.WithNodeConformance(), func(ctx context.Context) {
		podName := "downward-api-" + string(uuid.NewUUID())
		env := []v1.EnvVar{
			{
				Name: "CPU_LIMIT",
				ValueFrom: &v1.EnvVarSource{
					ResourceFieldRef: &v1.ResourceFieldSelector{
						Resource: "limits.cpu",
					},
				},
			},
			{
				Name: "MEMORY_LIMIT",
				ValueFrom: &v1.EnvVarSource{
					ResourceFieldRef: &v1.ResourceFieldSelector{
						Resource: "limits.memory",
					},
				},
			},
			{
				Name: "CPU_REQUEST",
				ValueFrom: &v1.EnvVarSource{
					ResourceFieldRef: &v1.ResourceFieldSelector{
						Resource: "requests.cpu",
					},
				},
			},
			{
				Name: "MEMORY_REQUEST",
				ValueFrom: &v1.EnvVarSource{
					ResourceFieldRef: &v1.ResourceFieldSelector{
						Resource: "requests.memory",
					},
				},
			},
		}
		expectations := []string{
			"CPU_LIMIT=2",
			"MEMORY_LIMIT=67108864",
			"CPU_REQUEST=1",
			"MEMORY_REQUEST=33554432",
		}

		testDownwardAPI(ctx, f, podName, env, expectations)
	})

	/*
	   Release: v1.9
	   Testname: DownwardAPI, environment for default CPU and memory limits and requests
	   Description: Downward API MUST 通过环境变量在容器运行时暴露设置的 CPU request 和 Memory limit。
	*/
	framework.ConformanceIt("should provide default limits.cpu/memory from node allocatable", f.WithNodeConformance(), func(ctx context.Context) {
		podName := "downward-api-" + string(uuid.NewUUID())
		env := []v1.EnvVar{
			{
				Name: "CPU_LIMIT",
				ValueFrom: &v1.EnvVarSource{
					ResourceFieldRef: &v1.ResourceFieldSelector{
						Resource: "limits.cpu",
					},
				},
			},
			{
				Name: "MEMORY_LIMIT",
				ValueFrom: &v1.EnvVarSource{
					ResourceFieldRef: &v1.ResourceFieldSelector{
						Resource: "limits.memory",
					},
				},
			},
		}
		expectations := []string{
			"CPU_LIMIT=[1-9]",
			"MEMORY_LIMIT=[1-9]",
		}
		pod := &v1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:   podName,
				Labels: map[string]string{"name": podName},
			},
			Spec: v1.PodSpec{
				Containers: []v1.Container{
					{
						Name:    "dapi-container",
						Image:   imageutils.GetE2EImage(imageutils.BusyBox),
						Command: []string{"sh", "-c", "env"},
						Env:     env,
					},
				},
				RestartPolicy: v1.RestartPolicyNever,
			},
		}

		testDownwardAPIUsingPod(ctx, f, pod, env, expectations)
	})

	/*
	   Release: v1.9
	   Testname: DownwardAPI, environment for Pod UID
	   Description: Downward API MUST 通过环境变量在容器运行时暴露设置的 Pod UID。
	*/
	framework.ConformanceIt("should provide pod UID as env vars", f.WithNodeConformance(), func(ctx context.Context) {
		podName := "downward-api-" + string(uuid.NewUUID())
		env := []v1.EnvVar{
			{
				Name: "POD_UID",
				ValueFrom: &v1.EnvVarSource{
					FieldRef: &v1.ObjectFieldSelector{
						APIVersion: "v1",
						FieldPath:  "metadata.uid",
					},
				},
			},
		}

		expectations := []string{
			"POD_UID=[a-z0-9]{8}-[a-z0-9]{4}-[a-z0-9]{4}-[a-z0-9]{4}-[a-z0-9]{12}",
		}

		testDownwardAPI(ctx, f, podName, env, expectations)
	})
})

var _ = SIGDescribe("Downward API", framework.WithSerial(), framework.WithDisruptive(), feature.DownwardAPIHugePages, func() {
	f := framework.NewDefaultFramework("downward-api")
	f.NamespacePodSecurityLevel = admissionapi.LevelPrivileged

	ginkgo.Context("Downward API tests for hugepages", func() {
		ginkgo.It("should provide container's limits.hugepages-<pagesize> and requests.hugepages-<pagesize> as env vars", func(ctx context.Context) {
			podName := "downward-api-" + string(uuid.NewUUID())
			env := []v1.EnvVar{
				{
					Name: "HUGEPAGES_LIMIT",
					ValueFrom: &v1.EnvVarSource{
						ResourceFieldRef: &v1.ResourceFieldSelector{
							Resource: "limits.hugepages-2Mi",
						},
					},
				},
				{
					Name: "HUGEPAGES_REQUEST",
					ValueFrom: &v1.EnvVarSource{
						ResourceFieldRef: &v1.ResourceFieldSelector{
							Resource: "requests.hugepages-2Mi",
						},
					},
				},
			}

			// 重要：我们允许 0，以便测试在未分配 hugepage 的环境中通过。
			expectations := []string{
				fmt.Sprintf("HUGEPAGES_LIMIT=%d", 0),
				fmt.Sprintf("HUGEPAGES_REQUEST=%d", 0),
			}
			pod := &v1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:   podName,
					Labels: map[string]string{"name": podName},
				},
				Spec: v1.PodSpec{
					Containers: []v1.Container{
						{
							Name:    "dapi-container",
							Image:   imageutils.GetE2EImage(imageutils.BusyBox),
							Command: []string{"sh", "-c", "env"},
							Resources: v1.ResourceRequirements{
								Requests: v1.ResourceList{
									"cpu":           resource.MustParse("10m"),
									"hugepages-2Mi": resource.MustParse("0Mi"),
								},
								Limits: v1.ResourceList{
									"hugepages-2Mi": resource.MustParse("0Mi"),
								},
							},
							Env: env,
						},
					},
					RestartPolicy: v1.RestartPolicyNever,
				},
			}
			testDownwardAPIUsingPod(ctx, f, pod, env, expectations)
		})

		ginkgo.It("should provide default limits.hugepages-<pagesize> from node allocatable", func(ctx context.Context) {
			podName := "downward-api-" + string(uuid.NewUUID())
			env := []v1.EnvVar{
				{
					Name: "HUGEPAGES_LIMIT",
					ValueFrom: &v1.EnvVarSource{
						ResourceFieldRef: &v1.ResourceFieldSelector{
							Resource: "limits.hugepages-2Mi",
						},
					},
				},
			}
			// 尽管 Pod Level Resources 的 CPU limit 设为 1250m（即 1.25），
			expectations := []string{
				"HUGEPAGES_LIMIT=((0)|([1-9][0-9]*))\n",
			}
			pod := &v1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:   podName,
					Labels: map[string]string{"name": podName},
				},
				Spec: v1.PodSpec{
					Containers: []v1.Container{
						{
							Name:    "dapi-container",
							Image:   imageutils.GetE2EImage(imageutils.BusyBox),
							Command: []string{"sh", "-c", "env"},
							Env:     env,
						},
					},
					RestartPolicy: v1.RestartPolicyNever,
				},
			}

			testDownwardAPIUsingPod(ctx, f, pod, env, expectations)
		})
	})

})

var _ = SIGDescribe("Downward API", feature.PodLevelResources, framework.WithFeatureGate(features.PodLevelResources), func() {
	f := framework.NewDefaultFramework("downward-api")
	f.NamespacePodSecurityLevel = admissionapi.LevelPrivileged

	ginkgo.Context("Downward API tests for pod level resources", func() {
		ginkgo.It("should provide default limits.cpu/memory from pod level resources", func(ctx context.Context) {
			podName := "downward-api-" + string(uuid.NewUUID())
			env := []v1.EnvVar{
				{
					Name: "CPU_LIMIT",
					ValueFrom: &v1.EnvVarSource{
						ResourceFieldRef: &v1.ResourceFieldSelector{
							Resource: "limits.cpu",
						},
					},
				},
				{
					Name: "MEMORY_LIMIT",
					ValueFrom: &v1.EnvVarSource{
						ResourceFieldRef: &v1.ResourceFieldSelector{
							Resource: "limits.memory",
						},
					},
				},
			}

			expectations := []string{
				// Downward API 使用 convertResourceCPUToString 将值转换为字符串，
				// 该函数会向上取整小数，因此这里得到 2。
				// 在引入 Pod Level Resources 特性之前，如果未设置容器级
				// https://github.com/kubernetes/kubernetes/blob/49cd87182cac80a4f3d29e2e65e80c8f88e890da/pkg/api/v1/resource/helpers.go#L148-L153
				"CPU_LIMIT=2",
				"MEMORY_LIMIT=67108864",
			}
			pod := &v1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:   podName,
					Labels: map[string]string{"name": podName},
				},
				Spec: v1.PodSpec{
					Resources: &v1.ResourceRequirements{
						Requests: v1.ResourceList{
							v1.ResourceCPU:    resource.MustParse("250m"),
							v1.ResourceMemory: resource.MustParse("32Mi"),
						},
						Limits: v1.ResourceList{
							v1.ResourceCPU:    resource.MustParse("1250m"),
							v1.ResourceMemory: resource.MustParse("64Mi"),
						},
					},
					Containers: []v1.Container{
						{
							Name:    "dapi-container",
							Image:   imageutils.GetE2EImage(imageutils.BusyBox),
							Command: []string{"sh", "-c", "env"},
							Env:     env,
						},
					},
					RestartPolicy: v1.RestartPolicyNever,
				},
			}
			testDownwardAPIUsingPod(ctx, f, pod, env, expectations)
		})

		ginkgo.It("should provide default limits.cpu/memory from pod level resources or node allocatable", func(ctx context.Context) {
			podName := "downward-api-" + string(uuid.NewUUID())
			cName := "dapi-container"
			env := []v1.EnvVar{
				{
					Name: "CPU_LIMIT",
					ValueFrom: &v1.EnvVarSource{
						ResourceFieldRef: &v1.ResourceFieldSelector{
							Resource: "limits.cpu",
						},
					},
				},
				{
					Name: "MEMORY_LIMIT",
					ValueFrom: &v1.EnvVarSource{
						ResourceFieldRef: &v1.ResourceFieldSelector{
							Resource: "limits.memory",
						},
					},
				},
			}

			pod := &v1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:   podName,
					Labels: map[string]string{"name": podName},
				},
				Spec: v1.PodSpec{
					Resources: &v1.ResourceRequirements{
						Requests: v1.ResourceList{
							v1.ResourceCPU:    resource.MustParse("250m"),
							v1.ResourceMemory: resource.MustParse("32Mi"),
						},
						Limits: v1.ResourceList{
							v1.ResourceCPU: resource.MustParse("1250m"),
						},
					},
					Containers: []v1.Container{
						{
							Name:    cName,
							Image:   imageutils.GetE2EImage(imageutils.BusyBox),
							Command: []string{"sh", "-c", "env"},
							Env:     env,
						},
					},
					RestartPolicy: v1.RestartPolicyNever,
				},
			}

			// resource limit，Downward API 会返回节点的
			// allocatable 资源而不是 0。因此只需检查
			// 环境变量中设置的值非零即可。
			// 然而，引入 Pod Level Resources 特性后，当未指定 Pod 级资源限制时，
			// 节点的 allocatable 资源会被用作回退值。因此现在需要验证
			// 环境变量中的值与节点的 allocatable 资源一致。
			// 由于需要动态获取节点的 allocatable 资源，
			// 这里不能直接使用 e2epodoutput.TestContainerOutputRegexp。
			// 等待客户端 Pod 完成。
			// 取日志。先获取 host。
			podClient := e2epod.NewPodClient(f)
			createdPod := podClient.Create(ctx, pod)
			ginkgo.DeferCleanup(func(ctx context.Context) {
				ginkgo.By("delete the pods")
				podClient.DeleteSync(ctx, createdPod.Name, metav1.DeleteOptions{}, f.Timeouts.PodDelete)
			})

			// 获取节点 allocatable 资源
			err := e2epod.WaitForPodSuccessInNamespaceTimeout(ctx, f.ClientSet, createdPod.Name, f.Namespace.Name, f.Timeouts.PodStart)
			framework.ExpectNoError(err)

			// 基于节点 allocatable 资源准备期望值
			podStatus, err := podClient.Get(ctx, createdPod.Name, metav1.GetOptions{})
			framework.ExpectNoError(err)

			// 每个容器
			nodeName := podStatus.Spec.NodeName
			node, err := f.ClientSet.CoreV1().Nodes().Get(ctx, nodeName, metav1.GetOptions{})
			framework.ExpectNoError(err)
			// 基于节点 allocatable 资源准备期望值
			expectations := []string{
				// Downward API 使用 convertResourceCPUToString 将值转换为字符串，
				// 该函数会向上取整小数，因此这里得到 2。
				// 在引入 Pod Level Resources 特性之前，如果未设置容器级
				// https://github.com/kubernetes/kubernetes/blob/49cd87182cac80a4f3d29e2e65e80c8f88e890da/pkg/api/v1/resource/helpers.go#L148-L153
				"CPU_LIMIT=2",
				fmt.Sprintf("MEMORY_LIMIT=%d", node.Status.Allocatable.Memory().Value()),
			}

			logs, err := e2epod.GetPodLogs(ctx, f.ClientSet, f.Namespace.Name, podStatus.Name, cName)
			framework.ExpectNoError(err)

			ginkgo.By(fmt.Sprintf("Checking logs from node %s pod %s container %s", nodeName, podStatus.Name, cName))
			for _, expected := range expectations {
				m := gomega.MatchRegexp(expected)
				matches, err := m.Match(logs)
				framework.ExpectNoError(err)
				gomega.Expect(matches).To(gomega.BeTrueBecause("expected %q in container output: %s", expected, logs))
			}
		})
	})
})

func testDownwardAPI(ctx context.Context, f *framework.Framework, podName string, env []v1.EnvVar, expectations []string) {
	pod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:   podName,
			Labels: map[string]string{"name": podName},
		},
		Spec: v1.PodSpec{
			Containers: []v1.Container{
				{
					Name:    "dapi-container",
					Image:   imageutils.GetE2EImage(imageutils.BusyBox),
					Command: []string{"sh", "-c", "env"},
					Resources: v1.ResourceRequirements{
						Requests: v1.ResourceList{
							v1.ResourceCPU:    resource.MustParse("250m"),
							v1.ResourceMemory: resource.MustParse("32Mi"),
						},
						Limits: v1.ResourceList{
							v1.ResourceCPU:    resource.MustParse("1250m"),
							v1.ResourceMemory: resource.MustParse("64Mi"),
						},
					},
					Env: env,
				},
			},
			RestartPolicy: v1.RestartPolicyNever,
		},
	}

	testDownwardAPIUsingPod(ctx, f, pod, env, expectations)
}

func testDownwardAPIUsingPod(ctx context.Context, f *framework.Framework, pod *v1.Pod, env []v1.EnvVar, expectations []string) {
	e2epodoutput.TestContainerOutputRegexp(ctx, f, "downward api env vars", pod, 0, expectations)
}
