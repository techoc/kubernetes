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

package storage

import (
	"context"
	"fmt"
	"path"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/uuid"
	"k8s.io/kubernetes/pkg/features"
	"k8s.io/kubernetes/test/e2e/framework"
	e2epod "k8s.io/kubernetes/test/e2e/framework/pod"
	e2epodoutput "k8s.io/kubernetes/test/e2e/framework/pod/output"
	e2eskipper "k8s.io/kubernetes/test/e2e/framework/skipper"
	imageutils "k8s.io/kubernetes/test/utils/image"
	admissionapi "k8s.io/pod-security-admission/api"
)

var _ = SIGDescribe("ConfigMap", func() {
	f := framework.NewDefaultFramework("configmap")
	f.NamespacePodSecurityLevel = admissionapi.LevelBaseline

	/*
		Release: v1.9
		Testname: ConfigMap Volume, without mapping
		Description: 创建 ConfigMap，创建一个挂载 volume 并用 ConfigMap 中数据填充该 volume 的 Pod。创建的 ConfigMap MUST 能被新 Pod 通过 volume mount 读取。文件内容 MUST 可读并验证，文件 mode MUST 默认为 0x644。
	*/
	framework.ConformanceIt("should be consumable from pods in volume", f.WithNodeConformance(), func(ctx context.Context) {
		doConfigMapE2EWithoutMappings(ctx, f, false, 0, nil, nil)
	})

	/*
		Release: v1.9
		Testname: ConfigMap Volume, without mapping, volume mode set
		Description: 创建 ConfigMap，创建一个挂载 volume 并用 ConfigMap 中数据填充该 volume 的 Pod。文件 mode 改为自定义值 '0x400'。创建的 ConfigMap MUST 能被新创建的 Pod 通过 volume mount 读取。文件内容 MUST 可读并经过验证，文件 mode MUST 设置为自定义值 '0x400'
		由于 Windows 不支持设置特定文件权限，该测试标记为 LinuxOnly。
	*/
	framework.ConformanceIt("should be consumable from pods in volume with defaultMode set [LinuxOnly]", f.WithNodeConformance(), func(ctx context.Context) {
		defaultMode := int32(0400)
		doConfigMapE2EWithoutMappings(ctx, f, false, 0, &defaultMode, nil)
	})

	f.It("should be consumable from pods in volume as non-root with defaultMode and fsGroup set [LinuxOnly]", f.WithNodeConformance(), func(ctx context.Context) {
		// Windows 不支持 RunAsUser / FSGroup SecurityContext 选项，也不支持设置文件权限。
		e2eskipper.SkipIfNodeOSDistroIs("windows")
		defaultMode := int32(0440) /* 设置 fsGroup 会将 mode 设置为至少 440 */
		doConfigMapE2EWithoutMappings(ctx, f, true, 1001, &defaultMode, nil)
	})

	f.It("should be consumable from pods in volume as non-root with defaultUser set [LinuxOnly]", f.WithFeatureGate(features.AtomicWriteVolumeUserFields), func(ctx context.Context) {
		// Windows 不支持为虚拟化容器账户设置文件属主。
		e2eskipper.SkipIfNodeOSDistroIs("windows")
		defaultUser := int64(1000)
		doConfigMapE2EWithoutMappings(ctx, f, true, 1001, nil, &defaultUser)
	})

	/*
		Release: v1.9
		Testname: ConfigMap Volume, without mapping, non-root user
		Description: 创建 ConfigMap，创建一个挂载 volume 并用 ConfigMap 中数据填充该 volume 的 Pod。Pod 以 uid=1000 的非 root 用户运行。创建的 ConfigMap MUST 能被新 Pod 通过 volume mount 读取。volume 上的文件 mode MUST 设为默认值 0x644。
	*/
	framework.ConformanceIt("should be consumable from pods in volume as non-root", f.WithNodeConformance(), func(ctx context.Context) {
		doConfigMapE2EWithoutMappings(ctx, f, true, 0, nil, nil)
	})

	f.It("should be consumable from pods in volume as non-root with FSGroup [LinuxOnly]", f.WithNodeConformance(), func(ctx context.Context) {
		// Windows 不支持 RunAsUser / FSGroup SecurityContext 选项。
		e2eskipper.SkipIfNodeOSDistroIs("windows")
		doConfigMapE2EWithoutMappings(ctx, f, true, 1001, nil, nil)
	})

	/*
		Release: v1.9
		Testname: ConfigMap Volume, with mapping
		Description: 创建 ConfigMap，创建一个挂载 volume 并用 ConfigMap 中数据填充该 volume 的 Pod。文件被映射到 volume 中的路径。创建的 ConfigMap MUST 能被新 Pod 通过 volume mount 读取。文件内容 MUST 可读并验证，文件 mode MUST 默认为 0x644。
	*/
	framework.ConformanceIt("should be consumable from pods in volume with mappings", f.WithNodeConformance(), func(ctx context.Context) {
		doConfigMapE2EWithMappings(ctx, f, false, 0, nil, nil)
	})

	/*
		Release: v1.9
		Testname: ConfigMap Volume, with mapping, volume mode set
		Description: 创建 ConfigMap，创建一个挂载 volume 并用 ConfigMap 中数据填充该 volume 的 Pod。文件被映射到 volume 中的某个路径。文件 mode 改为自定义值 '0x400'。创建的 ConfigMap MUST 能被新创建的 Pod 通过 volume mount 读取。文件内容 MUST 可读并经过验证，文件 mode MUST 设置为自定义值 '0x400'
		由于 Windows 不支持设置特定文件权限，该测试标记为 LinuxOnly。
	*/
	framework.ConformanceIt("should be consumable from pods in volume with mappings and Item mode set [LinuxOnly]", f.WithNodeConformance(), func(ctx context.Context) {
		mode := int32(0400)
		doConfigMapE2EWithMappings(ctx, f, false, 0, &mode, nil)
	})

	f.It("should be consumable from pods in volume with mappings and Item user set [LinuxOnly]", f.WithFeatureGate(features.AtomicWriteVolumeUserFields), func(ctx context.Context) {
		// Windows 不支持为虚拟化容器账户设置文件属主。
		e2eskipper.SkipIfNodeOSDistroIs("windows")
		user := int64(1000)
		doConfigMapE2EWithMappings(ctx, f, true, 1001, nil, &user)
	})

	/*
		Release: v1.9
		Testname: ConfigMap Volume, with mapping, non-root user
		Description: 创建 ConfigMap，创建一个挂载 volume 并用 ConfigMap 中数据填充该 volume 的 Pod。文件被映射到 volume 中的路径。Pod 以 uid=1000 的非 root 用户运行。创建的 ConfigMap MUST 能被新 Pod 通过 volume mount 读取。volume 上的文件 mode MUST 设为默认值 0x644。
	*/
	framework.ConformanceIt("should be consumable from pods in volume with mappings as non-root", f.WithNodeConformance(), func(ctx context.Context) {
		doConfigMapE2EWithMappings(ctx, f, true, 0, nil, nil)
	})

	f.It("should be consumable from pods in volume with mappings as non-root with FSGroup [LinuxOnly]", f.WithNodeConformance(), func(ctx context.Context) {
		// Windows 不支持 RunAsUser / FSGroup SecurityContext 选项。
		e2eskipper.SkipIfNodeOSDistroIs("windows")
		doConfigMapE2EWithMappings(ctx, f, true, 1001, nil, nil)
	})

	/*
		Release: v1.9
		Testname: ConfigMap Volume, update
		Description: 创建的 ConfigMap MUST 能被新 Pod 通过映射到 Pod 中自定义路径的 volume mount 读取。ConfigMap 更新后，MUST 通过读取 Pod 中挂载文件的内容来验证更改。
	*/
	framework.ConformanceIt("updates should be reflected in volume", f.WithNodeConformance(), func(ctx context.Context) {
		podLogTimeout := e2epod.GetPodSecretUpdateTimeout(ctx, f.ClientSet)
		containerTimeoutArg := fmt.Sprintf("--retry_time=%v", int(podLogTimeout.Seconds()))

		name := "configmap-test-upd-" + string(uuid.NewUUID())
		volumeName := "configmap-volume"
		volumeMountPath := "/etc/configmap-volume"

		configMap := &v1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: f.Namespace.Name,
				Name:      name,
			},
			Data: map[string]string{
				"data-1": "value-1",
			},
		}

		ginkgo.By(fmt.Sprintf("Creating configMap with name %s", configMap.Name))
		var err error
		if configMap, err = f.ClientSet.CoreV1().ConfigMaps(f.Namespace.Name).Create(ctx, configMap, metav1.CreateOptions{}); err != nil {
			framework.Failf("unable to create test configMap %s: %v", configMap.Name, err)
		}

		pod := createConfigMapVolumeMounttestPod(f.Namespace.Name, volumeName, name, volumeMountPath,
			"--break_on_expected_content=false", containerTimeoutArg, "--file_content_in_loop=/etc/configmap-volume/data-1")

		ginkgo.By("Creating the pod")
		e2epod.NewPodClient(f).CreateSync(ctx, pod)

		pollLogs := func() (string, error) {
			return e2epod.GetPodLogs(ctx, f.ClientSet, f.Namespace.Name, pod.Name, pod.Spec.Containers[0].Name)
		}

		gomega.Eventually(ctx, pollLogs, podLogTimeout, framework.Poll).Should(gomega.ContainSubstring("value-1"))

		ginkgo.By(fmt.Sprintf("Updating configmap %v", configMap.Name))
		configMap.ResourceVersion = "" // 强制更新
		configMap.Data["data-1"] = "value-2"
		_, err = f.ClientSet.CoreV1().ConfigMaps(f.Namespace.Name).Update(ctx, configMap, metav1.UpdateOptions{})
		framework.ExpectNoError(err, "Failed to update configmap %q in namespace %q", configMap.Name, f.Namespace.Name)

		ginkgo.By("waiting to observe update in volume")
		gomega.Eventually(ctx, pollLogs, podLogTimeout, framework.Poll).Should(gomega.ContainSubstring("value-2"))
	})

	/*
		Release: v1.12
		Testname: ConfigMap Volume, text data, binary data
		Description: 用文本数据和二进制数据创建的 ConfigMap MUST 能被新 Pod 通过映射到自定义路径的 volume mount 读取。ConfigMap 的文本和二进制数据 MUST 通过读取 Pod 中挂载文件的内容来验证。
	*/
	framework.ConformanceIt("binary data should be reflected in volume", f.WithNodeConformance(), func(ctx context.Context) {
		podLogTimeout := e2epod.GetPodSecretUpdateTimeout(ctx, f.ClientSet)
		containerTimeoutArg := fmt.Sprintf("--retry_time=%v", int(podLogTimeout.Seconds()))

		name := "configmap-test-upd-" + string(uuid.NewUUID())
		volumeName := "configmap-volume"
		volumeMountPath := "/etc/configmap-volume"
		containerName := "configmap-volume-binary-test"

		configMap := &v1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: f.Namespace.Name,
				Name:      name,
			},
			Data: map[string]string{
				"data-1": "value-1",
			},
			BinaryData: map[string][]byte{
				"dump.bin": {0xde, 0xca, 0xfe, 0xba, 0xd0, 0xfe, 0xff},
			},
		}

		ginkgo.By(fmt.Sprintf("Creating configMap with name %s", configMap.Name))
		var err error
		if configMap, err = f.ClientSet.CoreV1().ConfigMaps(f.Namespace.Name).Create(ctx, configMap, metav1.CreateOptions{}); err != nil {
			framework.Failf("unable to create test configMap %s: %v", configMap.Name, err)
		}

		pod := createConfigMapVolumeMounttestPod(f.Namespace.Name, volumeName, name, volumeMountPath,
			"--break_on_expected_content=false", containerTimeoutArg, "--file_content_in_loop=/etc/configmap-volume/data-1")
		pod.Spec.Containers = append(pod.Spec.Containers, v1.Container{
			Name:    containerName,
			Image:   imageutils.GetE2EImage(imageutils.BusyBox),
			Command: []string{"hexdump", "-C", "/etc/configmap-volume/dump.bin"},
			VolumeMounts: []v1.VolumeMount{
				{
					Name:      volumeName,
					MountPath: volumeMountPath,
					ReadOnly:  true,
				},
			},
		})

		ginkgo.By("Creating the pod")
		e2epod.NewPodClient(f).Create(ctx, pod)
		framework.ExpectNoError(e2epod.WaitForPodNameRunningInNamespace(ctx, f.ClientSet, pod.Name, f.Namespace.Name))

		pollLogs1 := func() (string, error) {
			return e2epod.GetPodLogs(ctx, f.ClientSet, f.Namespace.Name, pod.Name, pod.Spec.Containers[0].Name)
		}
		pollLogs2 := func() (string, error) {
			return e2epod.GetPodLogs(ctx, f.ClientSet, f.Namespace.Name, pod.Name, pod.Spec.Containers[1].Name)
		}

		ginkgo.By("Waiting for pod with text data")
		gomega.Eventually(ctx, pollLogs1, podLogTimeout, framework.Poll).Should(gomega.ContainSubstring("value-1"))
		ginkgo.By("Waiting for pod with binary data")
		gomega.Eventually(ctx, pollLogs2, podLogTimeout, framework.Poll).Should(gomega.ContainSubstring("de ca fe ba d0 fe ff"))
	})

	/*
		Release: v1.9
		Testname: ConfigMap Volume, create, update and delete
		Description: 创建的 ConfigMap MUST 能被新 Pod 通过映射到自定义路径的 volume mount 读取。ConfigMap 更新后 MUST 通过读取 Pod 中挂载文件来验证更改。从 map 中删除 item（文件）后，读取该文件 MUST 报错。
	*/
	framework.ConformanceIt("optional updates should be reflected in volume", f.WithNodeConformance(), func(ctx context.Context) {
		podLogTimeout := e2epod.GetPodSecretUpdateTimeout(ctx, f.ClientSet)
		containerTimeoutArg := fmt.Sprintf("--retry_time=%v", int(podLogTimeout.Seconds()))
		trueVal := true
		volumeMountPath := "/etc/configmap-volumes"

		deleteName := "cm-test-opt-del-" + string(uuid.NewUUID())
		deleteContainerName := "delcm-volume-test"
		deleteVolumeName := "deletecm-volume"
		deleteConfigMap := &v1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: f.Namespace.Name,
				Name:      deleteName,
			},
			Data: map[string]string{
				"data-1": "value-1",
			},
		}

		updateName := "cm-test-opt-upd-" + string(uuid.NewUUID())
		updateContainerName := "updcm-volume-test"
		updateVolumeName := "updatecm-volume"
		updateConfigMap := &v1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: f.Namespace.Name,
				Name:      updateName,
			},
			Data: map[string]string{
				"data-1": "value-1",
			},
		}

		createName := "cm-test-opt-create-" + string(uuid.NewUUID())
		createContainerName := "createcm-volume-test"
		createVolumeName := "createcm-volume"
		createConfigMap := &v1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: f.Namespace.Name,
				Name:      createName,
			},
			Data: map[string]string{
				"data-1": "value-1",
			},
		}

		ginkgo.By(fmt.Sprintf("Creating configMap with name %s", deleteConfigMap.Name))
		var err error
		if deleteConfigMap, err = f.ClientSet.CoreV1().ConfigMaps(f.Namespace.Name).Create(ctx, deleteConfigMap, metav1.CreateOptions{}); err != nil {
			framework.Failf("unable to create test configMap %s: %v", deleteConfigMap.Name, err)
		}

		ginkgo.By(fmt.Sprintf("Creating configMap with name %s", updateConfigMap.Name))
		if updateConfigMap, err = f.ClientSet.CoreV1().ConfigMaps(f.Namespace.Name).Create(ctx, updateConfigMap, metav1.CreateOptions{}); err != nil {
			framework.Failf("unable to create test configMap %s: %v", updateConfigMap.Name, err)
		}

		pod := &v1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name: "pod-configmaps-" + string(uuid.NewUUID()),
			},
			Spec: v1.PodSpec{
				Volumes: []v1.Volume{
					{
						Name: deleteVolumeName,
						VolumeSource: v1.VolumeSource{
							ConfigMap: &v1.ConfigMapVolumeSource{
								LocalObjectReference: v1.LocalObjectReference{
									Name: deleteName,
								},
								Optional: &trueVal,
							},
						},
					},
					{
						Name: updateVolumeName,
						VolumeSource: v1.VolumeSource{
							ConfigMap: &v1.ConfigMapVolumeSource{
								LocalObjectReference: v1.LocalObjectReference{
									Name: updateName,
								},
								Optional: &trueVal,
							},
						},
					},
					{
						Name: createVolumeName,
						VolumeSource: v1.VolumeSource{
							ConfigMap: &v1.ConfigMapVolumeSource{
								LocalObjectReference: v1.LocalObjectReference{
									Name: createName,
								},
								Optional: &trueVal,
							},
						},
					},
				},
				Containers: []v1.Container{
					{
						Name:  deleteContainerName,
						Image: imageutils.GetE2EImage(imageutils.Agnhost),
						Args:  []string{"mounttest", "--break_on_expected_content=false", containerTimeoutArg, "--file_content_in_loop=/etc/configmap-volumes/delete/data-1"},
						VolumeMounts: []v1.VolumeMount{
							{
								Name:      deleteVolumeName,
								MountPath: path.Join(volumeMountPath, "delete"),
								ReadOnly:  true,
							},
						},
					},
					{
						Name:  updateContainerName,
						Image: imageutils.GetE2EImage(imageutils.Agnhost),
						Args:  []string{"mounttest", "--break_on_expected_content=false", containerTimeoutArg, "--file_content_in_loop=/etc/configmap-volumes/update/data-3"},
						VolumeMounts: []v1.VolumeMount{
							{
								Name:      updateVolumeName,
								MountPath: path.Join(volumeMountPath, "update"),
								ReadOnly:  true,
							},
						},
					},
					{
						Name:  createContainerName,
						Image: imageutils.GetE2EImage(imageutils.Agnhost),
						Args:  []string{"mounttest", "--break_on_expected_content=false", containerTimeoutArg, "--file_content_in_loop=/etc/configmap-volumes/create/data-1"},
						VolumeMounts: []v1.VolumeMount{
							{
								Name:      createVolumeName,
								MountPath: path.Join(volumeMountPath, "create"),
								ReadOnly:  true,
							},
						},
					},
				},
				RestartPolicy: v1.RestartPolicyNever,
			},
		}
		ginkgo.By("Creating the pod")
		e2epod.NewPodClient(f).CreateSync(ctx, pod)

		pollCreateLogs := func() (string, error) {
			return e2epod.GetPodLogs(ctx, f.ClientSet, f.Namespace.Name, pod.Name, createContainerName)
		}
		gomega.Eventually(ctx, pollCreateLogs, podLogTimeout, framework.Poll).Should(gomega.ContainSubstring("Error reading file /etc/configmap-volumes/create/data-1"))

		pollUpdateLogs := func() (string, error) {
			return e2epod.GetPodLogs(ctx, f.ClientSet, f.Namespace.Name, pod.Name, updateContainerName)
		}
		gomega.Eventually(ctx, pollUpdateLogs, podLogTimeout, framework.Poll).Should(gomega.ContainSubstring("Error reading file /etc/configmap-volumes/update/data-3"))

		pollDeleteLogs := func() (string, error) {
			return e2epod.GetPodLogs(ctx, f.ClientSet, f.Namespace.Name, pod.Name, deleteContainerName)
		}
		gomega.Eventually(ctx, pollDeleteLogs, podLogTimeout, framework.Poll).Should(gomega.ContainSubstring("value-1"))

		ginkgo.By(fmt.Sprintf("Deleting configmap %v", deleteConfigMap.Name))
		err = f.ClientSet.CoreV1().ConfigMaps(f.Namespace.Name).Delete(ctx, deleteConfigMap.Name, metav1.DeleteOptions{})
		framework.ExpectNoError(err, "Failed to delete configmap %q in namespace %q", deleteConfigMap.Name, f.Namespace.Name)

		ginkgo.By(fmt.Sprintf("Updating configmap %v", updateConfigMap.Name))
		updateConfigMap.ResourceVersion = "" // 强制更新
		delete(updateConfigMap.Data, "data-1")
		updateConfigMap.Data["data-3"] = "value-3"
		_, err = f.ClientSet.CoreV1().ConfigMaps(f.Namespace.Name).Update(ctx, updateConfigMap, metav1.UpdateOptions{})
		framework.ExpectNoError(err, "Failed to update configmap %q in namespace %q", updateConfigMap.Name, f.Namespace.Name)

		ginkgo.By(fmt.Sprintf("Creating configMap with name %s", createConfigMap.Name))
		if createConfigMap, err = f.ClientSet.CoreV1().ConfigMaps(f.Namespace.Name).Create(ctx, createConfigMap, metav1.CreateOptions{}); err != nil {
			framework.Failf("unable to create test configMap %s: %v", createConfigMap.Name, err)
		}

		ginkgo.By("waiting to observe update in volume")

		gomega.Eventually(ctx, pollCreateLogs, podLogTimeout, framework.Poll).Should(gomega.ContainSubstring("value-1"))
		gomega.Eventually(ctx, pollUpdateLogs, podLogTimeout, framework.Poll).Should(gomega.ContainSubstring("value-3"))
		gomega.Eventually(ctx, pollDeleteLogs, podLogTimeout, framework.Poll).Should(gomega.ContainSubstring("Error reading file /etc/configmap-volumes/delete/data-1"))
	})

	/*
		Release: v1.9
		Testname: ConfigMap Volume, multiple volume maps
		Description: 创建的 ConfigMap MUST 能被新 Pod 通过映射到多个路径的 volume mount 读取。所有映射的 volume mount MUST 都能访问其内容。
	*/
	framework.ConformanceIt("should be consumable in multiple volumes in the same pod", f.WithNodeConformance(), func(ctx context.Context) {
		var (
			name             = "configmap-test-volume-" + string(uuid.NewUUID())
			volumeName       = "configmap-volume"
			volumeMountPath  = "/etc/configmap-volume"
			volumeName2      = "configmap-volume-2"
			volumeMountPath2 = "/etc/configmap-volume-2"
			configMap        = newConfigMap(f, name)
		)

		ginkgo.By(fmt.Sprintf("Creating configMap with name %s", configMap.Name))
		var err error
		if configMap, err = f.ClientSet.CoreV1().ConfigMaps(f.Namespace.Name).Create(ctx, configMap, metav1.CreateOptions{}); err != nil {
			framework.Failf("unable to create test configMap %s: %v", configMap.Name, err)
		}

		pod := &v1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name: "pod-configmaps-" + string(uuid.NewUUID()),
			},
			Spec: v1.PodSpec{
				Volumes: []v1.Volume{
					{
						Name: volumeName,
						VolumeSource: v1.VolumeSource{
							ConfigMap: &v1.ConfigMapVolumeSource{
								LocalObjectReference: v1.LocalObjectReference{
									Name: name,
								},
							},
						},
					},
					{
						Name: volumeName2,
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
						Name:  "configmap-volume-test",
						Image: imageutils.GetE2EImage(imageutils.Agnhost),
						Args:  []string{"mounttest", "--file_content=/etc/configmap-volume/data-1"},
						VolumeMounts: []v1.VolumeMount{
							{
								Name:      volumeName,
								MountPath: volumeMountPath,
								ReadOnly:  true,
							},
							{
								Name:      volumeName2,
								MountPath: volumeMountPath2,
								ReadOnly:  true,
							},
						},
					},
				},
				RestartPolicy: v1.RestartPolicyNever,
			},
		}

		e2epodoutput.TestContainerOutput(ctx, f, "consume configMaps", pod, 0, []string{
			"content of file \"/etc/configmap-volume/data-1\": value-1",
		})

	})

	/*
		Release: v1.21
		Testname: ConfigMap Volume, immutability
		Description: 创建 ConfigMap。更新其 data 字段，更新 MUST 成功。
			将 ConfigMap 标记为 immutable，更新 MUST 成功。尝试更新其 data，更新 MUST 失败。
			尝试将 ConfigMap 改回非 immutable，更新 MUST 失败。
			尝试更新 ConfigMap 的 metadata（labels），更新必须成功。
			尝试删除 ConfigMap，删除必须成功。
	*/
	framework.ConformanceIt("should be immutable if `immutable` field is set", func(ctx context.Context) {
		name := "immutable"
		configMap := newConfigMap(f, name)

		currentConfigMap, err := f.ClientSet.CoreV1().ConfigMaps(f.Namespace.Name).Create(ctx, configMap, metav1.CreateOptions{})
		framework.ExpectNoError(err, "Failed to create config map %q in namespace %q", configMap.Name, configMap.Namespace)

		currentConfigMap.Data["data-4"] = "value-4"
		currentConfigMap, err = f.ClientSet.CoreV1().ConfigMaps(f.Namespace.Name).Update(ctx, currentConfigMap, metav1.UpdateOptions{})
		framework.ExpectNoError(err, "Failed to update config map %q in namespace %q", configMap.Name, configMap.Namespace)

		// 将 config map 标记为 immutable。
		trueVal := true
		currentConfigMap.Immutable = &trueVal
		currentConfigMap, err = f.ClientSet.CoreV1().ConfigMaps(f.Namespace.Name).Update(ctx, currentConfigMap, metav1.UpdateOptions{})
		framework.ExpectNoError(err, "Failed to mark config map %q in namespace %q as immutable", configMap.Name, configMap.Namespace)

		// 确保现在无法更改 data。
		currentConfigMap.Data["data-5"] = "value-5"
		_, err = f.ClientSet.CoreV1().ConfigMaps(f.Namespace.Name).Update(ctx, currentConfigMap, metav1.UpdateOptions{})
		if !apierrors.IsInvalid(err) {
			framework.Failf("expected 'invalid' as error, got instead: %v", err)
		}

		// 确保 config map 无法从 immutable 切回 mutable。
		currentConfigMap, err = f.ClientSet.CoreV1().ConfigMaps(f.Namespace.Name).Get(ctx, name, metav1.GetOptions{})
		framework.ExpectNoError(err, "Failed to get config map %q in namespace %q", configMap.Name, configMap.Namespace)
		if !*currentConfigMap.Immutable {
			framework.Failf("currentConfigMap %s can be switched from immutable to mutable", currentConfigMap.Name)
		}

		falseVal := false
		currentConfigMap.Immutable = &falseVal
		_, err = f.ClientSet.CoreV1().ConfigMaps(f.Namespace.Name).Update(ctx, currentConfigMap, metav1.UpdateOptions{})
		if !apierrors.IsInvalid(err) {
			framework.Failf("expected 'invalid' as error, got instead: %v", err)
		}

		// 确保 metadata 可以更改。
		currentConfigMap, err = f.ClientSet.CoreV1().ConfigMaps(f.Namespace.Name).Get(ctx, name, metav1.GetOptions{})
		framework.ExpectNoError(err, "Failed to get config map %q in namespace %q", configMap.Name, configMap.Namespace)
		currentConfigMap.Labels = map[string]string{"label1": "value1"}
		_, err = f.ClientSet.CoreV1().ConfigMaps(f.Namespace.Name).Update(ctx, currentConfigMap, metav1.UpdateOptions{})
		framework.ExpectNoError(err, "Failed to update config map %q in namespace %q", configMap.Name, configMap.Namespace)

		// 确保 immutable 的 config map 可以删除。
		err = f.ClientSet.CoreV1().ConfigMaps(f.Namespace.Name).Delete(ctx, name, metav1.DeleteOptions{})
		framework.ExpectNoError(err, "Failed to delete config map %q in namespace %q", configMap.Name, configMap.Namespace)
	})

	// 在 volume 创建期间 Pod 处于 pending，直到 configMap 对象可用
	// 或挂载 configMap volume 超时。该 Pod 没有定义 configMap 对象，因此除非标记为 optional，否则应返回超时异常。
	// 慢（约 5 分钟）
	f.It("Should fail non-optional pod creation due to configMap object does not exist", f.WithSlow(), f.WithNodeConformance(), func(ctx context.Context) {
		volumeMountPath := "/etc/configmap-volumes"
		pod := createNonOptionalConfigMapPod(ctx, f, volumeMountPath)
		getPod := e2epod.Get(f.ClientSet, pod)
		gomega.Consistently(ctx, getPod).WithTimeout(f.Timeouts.PodStart).Should(e2epod.BeInPhase(v1.PodPending))
	})

	// 为 Pod 定义了 ConfigMap 对象，如果指定的 key 在 ConfigMap 中不存在，
	// 则在 Pod 创建期间 volume 设置将报错，除非标记为 optional。
	// 慢（约 5 分钟）
	f.It("Should fail non-optional pod creation due to the key in the configMap object does not exist", f.WithSlow(), f.WithNodeConformance(), func(ctx context.Context) {
		volumeMountPath := "/etc/configmap-volumes"
		pod := createNonOptionalConfigMapPodWithConfig(ctx, f, volumeMountPath)
		getPod := e2epod.Get(f.ClientSet, pod)
		gomega.Consistently(ctx, getPod).WithTimeout(f.Timeouts.PodStart).Should(e2epod.BeInPhase(v1.PodPending))
	})
})

func newConfigMap(f *framework.Framework, name string) *v1.ConfigMap {
	return &v1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: f.Namespace.Name,
			Name:      name,
		},
		Data: map[string]string{
			"data-1": "value-1",
			"data-2": "value-2",
			"data-3": "value-3",
		},
	}
}

func doConfigMapE2EWithoutMappings(ctx context.Context, f *framework.Framework, asUser bool, fsGroup int64, defaultMode *int32, defaultUser *int64) {
	var (
		name            = "configmap-test-volume-" + string(uuid.NewUUID())
		volumeName      = "configmap-volume"
		volumeMountPath = "/etc/configmap-volume"
		configMap       = newConfigMap(f, name)
	)

	ginkgo.By(fmt.Sprintf("Creating configMap with name %s", configMap.Name))
	var err error
	if configMap, err = f.ClientSet.CoreV1().ConfigMaps(f.Namespace.Name).Create(ctx, configMap, metav1.CreateOptions{}); err != nil {
		framework.Failf("unable to create test configMap %s: %v", configMap.Name, err)
	}

	pod := createConfigMapVolumeMounttestPod(f.Namespace.Name, volumeName, name, volumeMountPath,
		"--file_content=/etc/configmap-volume/data-1",
		"--file_mode=/etc/configmap-volume/data-1",
		"--file_owner=/etc/configmap-volume/data-1")
	one := int64(1)
	pod.Spec.TerminationGracePeriodSeconds = &one

	if asUser {
		setPodNonRootUser(pod)
	}

	if fsGroup != 0 {
		pod.Spec.SecurityContext.FSGroup = &fsGroup
	}

	if defaultMode != nil {
		pod.Spec.Volumes[0].VolumeSource.ConfigMap.DefaultMode = defaultMode
	}
	if defaultUser != nil {
		pod.Spec.Volumes[0].VolumeSource.ConfigMap.DefaultUser = defaultUser
	}

	fileModeRegexp := getFileModeRegex("/etc/configmap-volume/data-1", defaultMode)
	output := []string{
		"content of file \"/etc/configmap-volume/data-1\": value-1",
		fileModeRegexp,
	}
	if defaultUser != nil {
		output = append(output, fmt.Sprintf("owner UID of \"/etc/configmap-volume/data-1\": %d", *defaultUser))
	}
	e2epodoutput.TestContainerOutputRegexp(ctx, f, "consume configMaps", pod, 0, output)
}

func doConfigMapE2EWithMappings(ctx context.Context, f *framework.Framework, asUser bool, fsGroup int64, itemMode *int32, itemUser *int64) {
	var (
		name            = "configmap-test-volume-map-" + string(uuid.NewUUID())
		volumeName      = "configmap-volume"
		volumeMountPath = "/etc/configmap-volume"
		configMap       = newConfigMap(f, name)
	)

	ginkgo.By(fmt.Sprintf("Creating configMap with name %s", configMap.Name))

	var err error
	if configMap, err = f.ClientSet.CoreV1().ConfigMaps(f.Namespace.Name).Create(ctx, configMap, metav1.CreateOptions{}); err != nil {
		framework.Failf("unable to create test configMap %s: %v", configMap.Name, err)
	}

	pod := createConfigMapVolumeMounttestPod(f.Namespace.Name, volumeName, name, volumeMountPath,
		"--file_content=/etc/configmap-volume/path/to/data-2",
		"--file_mode=/etc/configmap-volume/path/to/data-2",
		"--file_owner=/etc/configmap-volume/path/to/data-2")
	one := int64(1)
	pod.Spec.TerminationGracePeriodSeconds = &one
	pod.Spec.Volumes[0].VolumeSource.ConfigMap.Items = []v1.KeyToPath{
		{
			Key:  "data-2",
			Path: "path/to/data-2",
		},
	}

	if asUser {
		setPodNonRootUser(pod)
	}

	if fsGroup != 0 {
		pod.Spec.SecurityContext.FSGroup = &fsGroup
	}

	if itemMode != nil {
		pod.Spec.Volumes[0].VolumeSource.ConfigMap.Items[0].Mode = itemMode
	}
	if itemUser != nil {
		pod.Spec.Volumes[0].VolumeSource.ConfigMap.Items[0].User = itemUser
	}

	// 如果未设置 fsGroup，只检查文件 mode。如果设置了 fsGroup，
	// 最终 mode 会被调整，我们不测试该情况。
	output := []string{
		"content of file \"/etc/configmap-volume/path/to/data-2\": value-2",
	}
	if fsGroup == 0 {
		fileModeRegexp := getFileModeRegex("/etc/configmap-volume/path/to/data-2", itemMode)
		output = append(output, fileModeRegexp)
	}
	if itemUser != nil {
		output = append(output, fmt.Sprintf("owner UID of \"/etc/configmap-volume/path/to/data-2\": %d", *itemUser))
	}
	e2epodoutput.TestContainerOutputRegexp(ctx, f, "consume configMaps", pod, 0, output)
}

func createNonOptionalConfigMapPod(ctx context.Context, f *framework.Framework, volumeMountPath string) *v1.Pod {
	podLogTimeout := e2epod.GetPodSecretUpdateTimeout(ctx, f.ClientSet)
	containerTimeoutArg := fmt.Sprintf("--retry_time=%v", int(podLogTimeout.Seconds()))
	falseValue := false

	createName := "cm-test-opt-create-" + string(uuid.NewUUID())
	createVolumeName := "createcm-volume"

	// 创建 Pod 时不创建 configMap 对象，只提及 configMap volume source 的 local reference name
	pod := createConfigMapVolumeMounttestPod(f.Namespace.Name, createVolumeName, createName, path.Join(volumeMountPath, "create"),
		"--break_on_expected_content=false", containerTimeoutArg, "--file_content_in_loop=/etc/configmap-volumes/create/data-1")
	pod.Spec.Volumes[0].VolumeSource.ConfigMap.Optional = &falseValue

	ginkgo.By("Creating the pod")
	pod = e2epod.NewPodClient(f).Create(ctx, pod)
	return pod
}

func createNonOptionalConfigMapPodWithConfig(ctx context.Context, f *framework.Framework, volumeMountPath string) *v1.Pod {
	podLogTimeout := e2epod.GetPodSecretUpdateTimeout(ctx, f.ClientSet)
	containerTimeoutArg := fmt.Sprintf("--retry_time=%v", int(podLogTimeout.Seconds()))
	falseValue := false

	createName := "cm-test-opt-create-" + string(uuid.NewUUID())
	createVolumeName := "createcm-volume"
	configMap := newConfigMap(f, createName)

	ginkgo.By(fmt.Sprintf("Creating configMap with name %s", configMap.Name))
	var err error
	if configMap, err = f.ClientSet.CoreV1().ConfigMaps(f.Namespace.Name).Create(ctx, configMap, metav1.CreateOptions{}); err != nil {
		framework.Failf("unable to create test configMap %s: %v", configMap.Name, err)
	}
	// 创建带 configMap 对象的 Pod，但使用 configMap 中不存在的 key。
	pod := createConfigMapVolumeMounttestPod(f.Namespace.Name, createVolumeName, createName, path.Join(volumeMountPath, "create"),
		"--break_on_expected_content=false", containerTimeoutArg, "--file_content_in_loop=/etc/configmap-volumes/create/data-1")
	pod.Spec.Volumes[0].VolumeSource.ConfigMap.Optional = &falseValue
	pod.Spec.Volumes[0].VolumeSource.ConfigMap.Items = []v1.KeyToPath{
		{
			Key:  "data-4",
			Path: "path/to/data-4",
		},
	}

	ginkgo.By("Creating the pod")
	pod = e2epod.NewPodClient(f).Create(ctx, pod)
	return pod
}

func createConfigMapVolumeMounttestPod(namespace, volumeName, referenceName, mountPath string, mounttestArgs ...string) *v1.Pod {
	volumes := []v1.Volume{
		{
			Name: volumeName,
			VolumeSource: v1.VolumeSource{
				ConfigMap: &v1.ConfigMapVolumeSource{
					LocalObjectReference: v1.LocalObjectReference{
						Name: referenceName,
					},
				},
			},
		},
	}
	podName := "pod-configmaps-" + string(uuid.NewUUID())
	mounttestArgs = append([]string{"mounttest"}, mounttestArgs...)
	pod := e2epod.NewAgnhostPod(namespace, podName, volumes, createMounts(volumeName, mountPath, true), nil, mounttestArgs...)
	pod.Spec.RestartPolicy = v1.RestartPolicyNever
	return pod
}
