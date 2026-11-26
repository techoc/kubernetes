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
	"path"

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

var _ = SIGDescribe("Projected configMap", func() {
	f := framework.NewDefaultFramework("projected")
	f.NamespacePodSecurityLevel = admissionapi.LevelBaseline

	/*
	   发布版本：v1.9
	   测试名称：Projected Volume, ConfigMap, volume mode default
	   描述：创建带 projected volume source 'ConfigMap' 的 Pod，以默认权限模式存储 configMap。Pod 必须能成功读取 ConfigMap 内容，volume 模式必须为 -rw-r--r--。
	*/
	framework.ConformanceIt("should be consumable from pods in volume", f.WithNodeConformance(), func(ctx context.Context) {
		doProjectedConfigMapE2EWithoutMappings(ctx, f, false, 0, nil, nil)
	})

	/*
	   发布版本：v1.9
	   测试名称：Projected Volume, ConfigMap, volume mode 0400
	   描述：创建带 projected volume source 'ConfigMap' 的 Pod，以 0400 权限模式存储 configMap。Pod 必须能成功读取 ConfigMap 内容，volume 模式必须为 -r--------。
	   此测试标记为 LinuxOnly，因为 Windows 不支持设置特定文件权限。
	*/
	framework.ConformanceIt("should be consumable from pods in volume with defaultMode set [LinuxOnly]", f.WithNodeConformance(), func(ctx context.Context) {
		defaultMode := int32(0400)
		doProjectedConfigMapE2EWithoutMappings(ctx, f, false, 0, &defaultMode, nil)
	})

	f.It("should be consumable from pods in volume as non-root with defaultMode and fsGroup set [LinuxOnly]", f.WithNodeConformance(), func(ctx context.Context) {
		// Windows 不支持 RunAsUser / FSGroup SecurityContext 选项，也不支持设置文件权限。
		e2eskipper.SkipIfNodeOSDistroIs("windows")
		defaultMode := int32(0440) /* setting fsGroup sets mode to at least 440 */
		doProjectedConfigMapE2EWithoutMappings(ctx, f, true, 1001, &defaultMode, nil)
	})

	f.It("should be consumable from pods in volume as non-root with defaultUser set [LinuxOnly]", f.WithFeatureGate(features.AtomicWriteVolumeUserFields), func(ctx context.Context) {
		// Windows 不支持为虚拟化容器账号设置文件属主。
		e2eskipper.SkipIfNodeOSDistroIs("windows")
		defaultUser := int64(1000)
		doProjectedConfigMapE2EWithoutMappings(ctx, f, true, 1001, nil, &defaultUser)
	})

	/*
	   发布版本：v1.9
	   测试名称：Projected Volume, ConfigMap, non-root user
	   描述：创建带 projected volume source 'ConfigMap' 的 Pod，以 uid 1000 的非 root 用户存储 configMap。Pod 必须能成功读取 ConfigMap 内容，volume 模式必须为 -rw-r--r--。
	*/
	framework.ConformanceIt("should be consumable from pods in volume as non-root", f.WithNodeConformance(), func(ctx context.Context) {
		doProjectedConfigMapE2EWithoutMappings(ctx, f, true, 0, nil, nil)
	})

	f.It("should be consumable from pods in volume as non-root with FSGroup [LinuxOnly]", f.WithNodeConformance(), func(ctx context.Context) {
		// Windows 不支持 RunAsUser / FSGroup SecurityContext 选项。
		e2eskipper.SkipIfNodeOSDistroIs("windows")
		doProjectedConfigMapE2EWithoutMappings(ctx, f, true, 1001, nil, nil)
	})

	/*
	   发布版本：v1.9
	   测试名称：Projected Volume, ConfigMap, mapped
	   描述：创建带 projected volume source 'ConfigMap' 的 Pod，以默认权限模式存储 configMap，并映射到自定义路径。Pod 必须能从自定义位置成功读取 ConfigMap 内容，volume 模式必须为 -rw-r--r--。
	*/
	framework.ConformanceIt("should be consumable from pods in volume with mappings", f.WithNodeConformance(), func(ctx context.Context) {
		doProjectedConfigMapE2EWithMappings(ctx, f, false, 0, nil, nil)
	})

	/*
	   发布版本：v1.9
	   测试名称：Projected Volume, ConfigMap, mapped, volume mode 0400
	   描述：创建带 projected volume source 'ConfigMap' 的 Pod，以 0400 权限模式存储 configMap，并映射到自定义路径。Pod 必须能从自定义位置成功读取 ConfigMap 内容，volume 模式必须为 -r--r--r--。
	   此测试标记为 LinuxOnly，因为 Windows 不支持设置特定文件权限。
	*/
	framework.ConformanceIt("should be consumable from pods in volume with mappings and Item mode set [LinuxOnly]", f.WithNodeConformance(), func(ctx context.Context) {
		mode := int32(0400)
		doProjectedConfigMapE2EWithMappings(ctx, f, false, 0, &mode, nil)
	})

	f.It("should be consumable from pods in volume with mappings and Item user set [LinuxOnly]", f.WithFeatureGate(features.AtomicWriteVolumeUserFields), func(ctx context.Context) {
		// Windows 不支持为虚拟化容器账号设置文件属主。
		e2eskipper.SkipIfNodeOSDistroIs("windows")
		user := int64(1000)
		doProjectedConfigMapE2EWithMappings(ctx, f, true, 1001, nil, &user)
	})

	/*
	   发布版本：v1.9
	   测试名称：Projected Volume, ConfigMap, mapped, non-root user
	   描述：创建带 projected volume source 'ConfigMap' 的 Pod，以 uid 1000 的非 root 用户存储 configMap，并映射到自定义路径。Pod 必须能从自定义位置成功读取 ConfigMap 内容，volume 模式必须为 -r--r--r--。
	*/
	framework.ConformanceIt("should be consumable from pods in volume with mappings as non-root", f.WithNodeConformance(), func(ctx context.Context) {
		doProjectedConfigMapE2EWithMappings(ctx, f, true, 0, nil, nil)
	})

	f.It("should be consumable from pods in volume with mappings as non-root with FSGroup [LinuxOnly]", f.WithNodeConformance(), func(ctx context.Context) {
		// Windows 不支持 RunAsUser / FSGroup SecurityContext 选项。
		e2eskipper.SkipIfNodeOSDistroIs("windows")
		doProjectedConfigMapE2EWithMappings(ctx, f, true, 1001, nil, nil)
	})

	/*
	   发布版本：v1.9
	   测试名称：Projected Volume, ConfigMap, update
	   描述：创建带 projected volume source 'ConfigMap' 的 Pod 存储 configMap，并执行创建和更新为新值。Pod 必须能用 value-1 创建 configMap，必须能把 configMap 中的值更新为 value-2。
	*/
	framework.ConformanceIt("updates should be reflected in volume", f.WithNodeConformance(), func(ctx context.Context) {
		podLogTimeout := e2epod.GetPodSecretUpdateTimeout(ctx, f.ClientSet)
		containerTimeoutArg := fmt.Sprintf("--retry_time=%v", int(podLogTimeout.Seconds()))

		name := "projected-configmap-test-upd-" + string(uuid.NewUUID())
		volumeName := "projected-configmap-volume"
		volumeMountPath := "/etc/projected-configmap-volume"
		configMap := &v1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: f.Namespace.Name,
				Name:      name,
			},
			Data: map[string]string{
				"data-1": "value-1",
			},
		}

		ginkgo.By(fmt.Sprintf("Creating projection with configMap that has name %s", configMap.Name))
		var err error
		if configMap, err = f.ClientSet.CoreV1().ConfigMaps(f.Namespace.Name).Create(ctx, configMap, metav1.CreateOptions{}); err != nil {
			framework.Failf("unable to create test configMap %s: %v", configMap.Name, err)
		}

		pod := createProjectedConfigMapMounttestPod(f.Namespace.Name, volumeName, name, volumeMountPath,
			"--break_on_expected_content=false", containerTimeoutArg, "--file_content_in_loop=/etc/projected-configmap-volume/data-1")

		ginkgo.By("Creating the pod")
		e2epod.NewPodClient(f).CreateSync(ctx, pod)

		pollLogs := func() (string, error) {
			return e2epod.GetPodLogs(ctx, f.ClientSet, f.Namespace.Name, pod.Name, pod.Spec.Containers[0].Name)
		}

		gomega.Eventually(ctx, pollLogs, podLogTimeout, framework.Poll).Should(gomega.ContainSubstring("value-1"))

		ginkgo.By(fmt.Sprintf("Updating configmap %v", configMap.Name))
		configMap.ResourceVersion = "" // to force update
		configMap.Data["data-1"] = "value-2"
		_, err = f.ClientSet.CoreV1().ConfigMaps(f.Namespace.Name).Update(ctx, configMap, metav1.UpdateOptions{})
		framework.ExpectNoError(err, "Failed to update configmap %q in namespace %q", configMap.Name, f.Namespace.Name)

		ginkgo.By("waiting to observe update in volume")
		gomega.Eventually(ctx, pollLogs, podLogTimeout, framework.Poll).Should(gomega.ContainSubstring("value-2"))
	})

	/*
	   发布版本：v1.9
	   测试名称：Projected Volume, ConfigMap, create, update and delete
	   描述：创建带三个容器的 Pod，分别用 create、update 和 delete 容器操作 ConfigMap。create 容器启动时必须没有 configMap，update 和 delete 容器必须以 'value-1' 的 ConfigMap 值创建。在 create 容器中创建 configMap，Pod 必须能从 create 容器读取它。在 update 容器中更新 configMap，Pod 必须能读到更新后的值。在 delete 容器中删除 configMap，Pod 必须无法从 delete 容器读取它。
	*/
	framework.ConformanceIt("optional updates should be reflected in volume", f.WithNodeConformance(), func(ctx context.Context) {
		podLogTimeout := e2epod.GetPodSecretUpdateTimeout(ctx, f.ClientSet)
		containerTimeoutArg := fmt.Sprintf("--retry_time=%v", int(podLogTimeout.Seconds()))
		trueVal := true
		volumeMountPath := "/etc/projected-configmap-volumes"

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
				Name: "pod-projected-configmaps-" + string(uuid.NewUUID()),
			},
			Spec: v1.PodSpec{
				Volumes: []v1.Volume{
					{
						Name: deleteVolumeName,
						VolumeSource: v1.VolumeSource{
							Projected: &v1.ProjectedVolumeSource{
								Sources: []v1.VolumeProjection{
									{
										ConfigMap: &v1.ConfigMapProjection{
											LocalObjectReference: v1.LocalObjectReference{
												Name: deleteName,
											},
											Optional: &trueVal,
										},
									},
								},
							},
						},
					},
					{
						Name: updateVolumeName,
						VolumeSource: v1.VolumeSource{
							Projected: &v1.ProjectedVolumeSource{
								Sources: []v1.VolumeProjection{
									{
										ConfigMap: &v1.ConfigMapProjection{
											LocalObjectReference: v1.LocalObjectReference{
												Name: updateName,
											},
											Optional: &trueVal,
										},
									},
								},
							},
						},
					},
					{
						Name: createVolumeName,
						VolumeSource: v1.VolumeSource{
							Projected: &v1.ProjectedVolumeSource{
								Sources: []v1.VolumeProjection{
									{
										ConfigMap: &v1.ConfigMapProjection{
											LocalObjectReference: v1.LocalObjectReference{
												Name: createName,
											},
											Optional: &trueVal,
										},
									},
								},
							},
						},
					},
				},
				Containers: []v1.Container{
					{
						Name:  deleteContainerName,
						Image: imageutils.GetE2EImage(imageutils.Agnhost),
						Args:  []string{"mounttest", "--break_on_expected_content=false", containerTimeoutArg, "--file_content_in_loop=/etc/projected-configmap-volumes/delete/data-1"},
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
						Args:  []string{"mounttest", "--break_on_expected_content=false", containerTimeoutArg, "--file_content_in_loop=/etc/projected-configmap-volumes/update/data-3"},
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
						Args:  []string{"mounttest", "--break_on_expected_content=false", containerTimeoutArg, "--file_content_in_loop=/etc/projected-configmap-volumes/create/data-1"},
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
		gomega.Eventually(ctx, pollCreateLogs, podLogTimeout, framework.Poll).Should(gomega.ContainSubstring("Error reading file /etc/projected-configmap-volumes/create/data-1"))

		pollUpdateLogs := func() (string, error) {
			return e2epod.GetPodLogs(ctx, f.ClientSet, f.Namespace.Name, pod.Name, updateContainerName)
		}
		gomega.Eventually(ctx, pollUpdateLogs, podLogTimeout, framework.Poll).Should(gomega.ContainSubstring("Error reading file /etc/projected-configmap-volumes/update/data-3"))

		pollDeleteLogs := func() (string, error) {
			return e2epod.GetPodLogs(ctx, f.ClientSet, f.Namespace.Name, pod.Name, deleteContainerName)
		}
		gomega.Eventually(ctx, pollDeleteLogs, podLogTimeout, framework.Poll).Should(gomega.ContainSubstring("value-1"))

		ginkgo.By(fmt.Sprintf("Deleting configmap %v", deleteConfigMap.Name))
		err = f.ClientSet.CoreV1().ConfigMaps(f.Namespace.Name).Delete(ctx, deleteConfigMap.Name, metav1.DeleteOptions{})
		framework.ExpectNoError(err, "Failed to delete configmap %q in namespace %q", deleteConfigMap.Name, f.Namespace.Name)

		ginkgo.By(fmt.Sprintf("Updating configmap %v", updateConfigMap.Name))
		updateConfigMap.ResourceVersion = "" // to force update
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
		gomega.Eventually(ctx, pollDeleteLogs, podLogTimeout, framework.Poll).Should(gomega.ContainSubstring("Error reading file /etc/projected-configmap-volumes/delete/data-1"))
	})

	/*
	   发布版本：v1.9
	   测试名称：Projected Volume, ConfigMap, multiple volume paths
	   描述：创建带 projected volume source 'ConfigMap' 的 Pod 存储 configMap。configMap 映射到两个不同的 volume mount。Pod 必须能从两个挂载点成功读取 configMap 内容。
	*/
	framework.ConformanceIt("should be consumable in multiple volumes in the same pod", f.WithNodeConformance(), func(ctx context.Context) {
		var (
			name             = "projected-configmap-test-volume-" + string(uuid.NewUUID())
			volumeName       = "projected-configmap-volume"
			volumeMountPath  = "/etc/projected-configmap-volume"
			volumeName2      = "projected-configmap-volume-2"
			volumeMountPath2 = "/etc/projected-configmap-volume-2"
			configMap        = newConfigMap(f, name)
		)

		ginkgo.By(fmt.Sprintf("Creating configMap with name %s", configMap.Name))
		var err error
		if configMap, err = f.ClientSet.CoreV1().ConfigMaps(f.Namespace.Name).Create(ctx, configMap, metav1.CreateOptions{}); err != nil {
			framework.Failf("unable to create test configMap %s: %v", configMap.Name, err)
		}

		pod := &v1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name: "pod-projected-configmaps-" + string(uuid.NewUUID()),
			},
			Spec: v1.PodSpec{
				Volumes: []v1.Volume{
					{
						Name: volumeName,
						VolumeSource: v1.VolumeSource{
							Projected: &v1.ProjectedVolumeSource{
								Sources: []v1.VolumeProjection{
									{
										ConfigMap: &v1.ConfigMapProjection{
											LocalObjectReference: v1.LocalObjectReference{
												Name: name,
											},
										},
									},
								},
							},
						},
					},
					{
						Name: volumeName2,
						VolumeSource: v1.VolumeSource{
							Projected: &v1.ProjectedVolumeSource{
								Sources: []v1.VolumeProjection{
									{

										ConfigMap: &v1.ConfigMapProjection{
											LocalObjectReference: v1.LocalObjectReference{
												Name: name,
											},
										},
									},
								},
							},
						},
					},
				},
				Containers: []v1.Container{
					{
						Name:  "projected-configmap-volume-test",
						Image: imageutils.GetE2EImage(imageutils.Agnhost),
						Args:  []string{"mounttest", "--file_content=/etc/projected-configmap-volume/data-1"},
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
			"content of file \"/etc/projected-configmap-volume/data-1\": value-1",
		})

	})

	//volume 创建期间 pod 处于 pending，直到 configMap 对象可用
	//或挂载 configMap volume 超时。pod 没有定义 configMap 对象，因此除非标记为 optional，否则应返回超时异常。
	//慢（约 5 分钟）
	f.It("Should fail non-optional pod creation due to configMap object does not exist", f.WithSlow(), f.WithNodeConformance(), func(ctx context.Context) {
		volumeMountPath := "/etc/projected-configmap-volumes"
		pod := createNonOptionalConfigMapPod(ctx, f, volumeMountPath)
		getPod := e2epod.Get(f.ClientSet, pod)
		gomega.Consistently(ctx, getPod).WithTimeout(f.Timeouts.PodStart).Should(e2epod.BeInPhase(v1.PodPending))
	})

	//为 pod 定义了 ConfigMap 对象。如果指定的 key 在 ConfigMap 中不存在，
	// pod 创建时 volume 设置会报错，除非标记为 optional。
	//慢（约 5 分钟）
	f.It("Should fail non-optional pod creation due to the key in the configMap object does not exist", f.WithSlow(), f.WithNodeConformance(), func(ctx context.Context) {
		volumeMountPath := "/etc/configmap-volumes"
		pod := createNonOptionalConfigMapPodWithConfig(ctx, f, volumeMountPath)
		getPod := e2epod.Get(f.ClientSet, pod)
		gomega.Consistently(ctx, getPod).WithTimeout(f.Timeouts.PodStart).Should(e2epod.BeInPhase(v1.PodPending))
	})
})

func doProjectedConfigMapE2EWithoutMappings(ctx context.Context, f *framework.Framework, asUser bool, fsGroup int64, defaultMode *int32, defaultUser *int64) {
	var (
		name            = "projected-configmap-test-volume-" + string(uuid.NewUUID())
		volumeName      = "projected-configmap-volume"
		volumeMountPath = "/etc/projected-configmap-volume"
		configMap       = newConfigMap(f, name)
	)

	ginkgo.By(fmt.Sprintf("Creating configMap with name %s", configMap.Name))
	var err error
	if configMap, err = f.ClientSet.CoreV1().ConfigMaps(f.Namespace.Name).Create(ctx, configMap, metav1.CreateOptions{}); err != nil {
		framework.Failf("unable to create test configMap %s: %v", configMap.Name, err)
	}

	pod := createProjectedConfigMapMounttestPod(f.Namespace.Name, volumeName, name, volumeMountPath,
		"--file_content=/etc/projected-configmap-volume/data-1",
		"--file_mode=/etc/projected-configmap-volume/data-1",
		"--file_owner=/etc/projected-configmap-volume/data-1")

	if asUser {
		setPodNonRootUser(pod)
	}

	if fsGroup != 0 {
		pod.Spec.SecurityContext.FSGroup = &fsGroup
	}

	if defaultMode != nil {
		pod.Spec.Volumes[0].VolumeSource.Projected.DefaultMode = defaultMode
	}
	if defaultUser != nil {
		pod.Spec.Volumes[0].VolumeSource.Projected.DefaultUser = defaultUser
	}

	fileModeRegexp := getFileModeRegex("/etc/projected-configmap-volume/data-1", defaultMode)
	output := []string{
		"content of file \"/etc/projected-configmap-volume/data-1\": value-1",
		fileModeRegexp,
	}
	if defaultUser != nil {
		output = append(output, fmt.Sprintf("owner UID of \"/etc/projected-configmap-volume/data-1\": %d", *defaultUser))
	}
	e2epodoutput.TestContainerOutputRegexp(ctx, f, "consume configMaps", pod, 0, output)
}

func doProjectedConfigMapE2EWithMappings(ctx context.Context, f *framework.Framework, asUser bool, fsGroup int64, itemMode *int32, itemUser *int64) {
	var (
		name            = "projected-configmap-test-volume-map-" + string(uuid.NewUUID())
		volumeName      = "projected-configmap-volume"
		volumeMountPath = "/etc/projected-configmap-volume"
		configMap       = newConfigMap(f, name)
	)

	ginkgo.By(fmt.Sprintf("Creating configMap with name %s", configMap.Name))

	var err error
	if configMap, err = f.ClientSet.CoreV1().ConfigMaps(f.Namespace.Name).Create(ctx, configMap, metav1.CreateOptions{}); err != nil {
		framework.Failf("unable to create test configMap %s: %v", configMap.Name, err)
	}

	pod := createProjectedConfigMapMounttestPod(f.Namespace.Name, volumeName, name, volumeMountPath,
		"--file_content=/etc/projected-configmap-volume/path/to/data-2",
		"--file_mode=/etc/projected-configmap-volume/path/to/data-2",
		"--file_owner=/etc/projected-configmap-volume/path/to/data-2")
	pod.Spec.Volumes[0].VolumeSource.Projected.Sources[0].ConfigMap.Items = []v1.KeyToPath{
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
		pod.Spec.Volumes[0].VolumeSource.Projected.Sources[0].ConfigMap.Items[0].Mode = itemMode
	}
	if itemUser != nil {
		pod.Spec.Volumes[0].VolumeSource.Projected.Sources[0].ConfigMap.Items[0].User = itemUser
	}

	// 如果没设置 fsGroup，只检查文件模式。如果设置了 fsGroup，
	// 最终模式会被调整，这不是我们要测的场景。
	output := []string{
		"content of file \"/etc/projected-configmap-volume/path/to/data-2\": value-2",
	}
	if fsGroup == 0 {
		fileModeRegexp := getFileModeRegex("/etc/projected-configmap-volume/path/to/data-2", itemMode)
		output = append(output, fileModeRegexp)
	}
	if itemUser != nil {
		output = append(output, fmt.Sprintf("owner UID of \"/etc/projected-configmap-volume/path/to/data-2\": %d", *itemUser))
	}
	e2epodoutput.TestContainerOutputRegexp(ctx, f, "consume configMaps", pod, 0, output)
}

func createProjectedConfigMapMounttestPod(namespace, volumeName, referenceName, mountPath string, mounttestArgs ...string) *v1.Pod {
	volumes := []v1.Volume{
		{
			Name: volumeName,
			VolumeSource: v1.VolumeSource{
				Projected: &v1.ProjectedVolumeSource{
					Sources: []v1.VolumeProjection{
						{
							ConfigMap: &v1.ConfigMapProjection{
								LocalObjectReference: v1.LocalObjectReference{
									Name: referenceName,
								},
							},
						},
					},
				},
			},
		},
	}
	podName := "pod-projected-configmaps-" + string(uuid.NewUUID())
	mounttestArgs = append([]string{"mounttest"}, mounttestArgs...)
	pod := e2epod.NewAgnhostPod(namespace, podName, volumes, createMounts(volumeName, mountPath, true), nil, mounttestArgs...)
	pod.Spec.RestartPolicy = v1.RestartPolicyNever
	return pod
}
