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
	imageutils "k8s.io/kubernetes/test/utils/image"
	admissionapi "k8s.io/pod-security-admission/api"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
)

var _ = SIGDescribe("Projected secret", func() {
	f := framework.NewDefaultFramework("projected")
	f.NamespacePodSecurityLevel = admissionapi.LevelBaseline

	/*
	   Release: v1.9
	   Testname: Projected Volume, Secrets, volume mode default
	   Description: 创建一个带 projected volume source 'secret' 的 Pod，以默认权限 mode 存储指定 key 的 secret。Pod MUST 能成功读取该 key 的内容，默认 mode MUST 为 -rw-r--r--。
	*/
	framework.ConformanceIt("should be consumable from pods in volume", f.WithNodeConformance(), func(ctx context.Context) {
		doProjectedSecretE2EWithoutMapping(ctx, f, nil, nil, "projected-secret-test-"+string(uuid.NewUUID()), nil, nil)
	})

	/*
	   Release: v1.9
	   Testname: Projected Volume, Secrets, volume mode 0400
	   Description: 创建一个带 projected volume source 'secret' 的 Pod，以指定 key 存储 secret，并在 Pod 上将权限 mode 设为 0x400。Pod MUST 能成功读取该 key 的内容，且 mode MUST 为 -r--------。
	   由于 Windows 不支持设置特定文件权限，该测试标记为 LinuxOnly。
	*/
	framework.ConformanceIt("should be consumable from pods in volume with defaultMode set [LinuxOnly]", f.WithNodeConformance(), func(ctx context.Context) {
		defaultMode := int32(0400)
		doProjectedSecretE2EWithoutMapping(ctx, f, &defaultMode, nil, "projected-secret-test-"+string(uuid.NewUUID()), nil, nil)
	})

	/*
	   Release: v1.9
	   Testname: Project Volume, Secrets, non-root, custom fsGroup
	   Description: 创建一个带 projected volume source 'secret' 的 Pod，以指定 key 存储 secret。volume 的权限 mode 设为 0440，fsgroup 设为 1001，用户设为非 root UID 1000。Pod MUST 能成功读取该 key 的内容，且 mode MUST 为 -r--r-----。
	   由于 Windows 不支持设置特定文件权限或以 UID / GID 运行，该测试标记为 LinuxOnly。
	*/
	framework.ConformanceIt("should be consumable from pods in volume as non-root with defaultMode and fsGroup set [LinuxOnly]", f.WithNodeConformance(), func(ctx context.Context) {
		defaultMode := int32(0440) /* 设置 fsGroup 会将 mode 设置为至少 440 */
		fsGroup := int64(1001)
		doProjectedSecretE2EWithoutMapping(ctx, f, &defaultMode, nil, "projected-secret-test-"+string(uuid.NewUUID()), &fsGroup, &nonRootTestUserID)
	})

	f.It("should be consumable from pods in volume as non-root with defaultUser set [LinuxOnly]", f.WithFeatureGate(features.AtomicWriteVolumeUserFields), func(ctx context.Context) {
		defaultUser := int64(1000)
		doProjectedSecretE2EWithoutMapping(ctx, f, nil, &defaultUser, "projected-secret-test-"+string(uuid.NewUUID()), nil, &nonRootTestUserID)
	})

	/*
	   Release: v1.9
	   Testname: Projected Volume, Secrets, mapped
	   Description: 创建一个带 projected volume source 'secret' 的 Pod，以默认权限 mode 存储指定 key 的 secret。secret 还被映射到自定义路径。Pod MUST 能成功读取该 key 的内容，映射 volume 上的 mode MUST 为 -r--------。
	*/
	framework.ConformanceIt("should be consumable from pods in volume with mappings", f.WithNodeConformance(), func(ctx context.Context) {
		doProjectedSecretE2EWithMapping(ctx, f, nil, nil, nil)
	})

	/*
	   Release: v1.9
	   Testname: Projected Volume, Secrets, mapped, volume mode 0400
	   Description: 创建一个带 projected volume source 'secret' 的 Pod，以指定 key 存储 secret，权限 mode 设为 0400。secret 还被映射到特定名称。Pod MUST 能成功读取该 key 的内容，且映射后的 volume 上 mode MUST 为 -r--------。
	   由于 Windows 不支持设置特定文件权限，该测试标记为 LinuxOnly。
	*/
	framework.ConformanceIt("should be consumable from pods in volume with mappings and Item Mode set [LinuxOnly]", f.WithNodeConformance(), func(ctx context.Context) {
		mode := int32(0400)
		doProjectedSecretE2EWithMapping(ctx, f, &mode, nil, nil)
	})

	f.It("should be consumable from pods in volume with mappings and Item User set [LinuxOnly]", f.WithFeatureGate(features.AtomicWriteVolumeUserFields), func(ctx context.Context) {
		user := int64(1000)
		doProjectedSecretE2EWithMapping(ctx, f, nil, &user, &user)
	})

	f.It("should be able to mount in a volume regardless of a different secret existing with same name in different namespace", f.WithNodeConformance(), func(ctx context.Context) {
		var (
			namespace2  *v1.Namespace
			err         error
			secret2Name = "projected-secret-test-" + string(uuid.NewUUID())
		)

		if namespace2, err = f.CreateNamespace(ctx, "secret-namespace", nil); err != nil {
			framework.Failf("unable to create new namespace %s: %v", namespace2.Name, err)
		}

		secret2 := secretForTest(namespace2.Name, secret2Name)
		secret2.Data = map[string][]byte{
			"this_should_not_match_content_of_other_secret": []byte("similarly_this_should_not_match_content_of_other_secret\n"),
		}
		if secret2, err = f.ClientSet.CoreV1().Secrets(namespace2.Name).Create(ctx, secret2, metav1.CreateOptions{}); err != nil {
			framework.Failf("unable to create test secret %s: %v", secret2.Name, err)
		}
		doProjectedSecretE2EWithoutMapping(ctx, f, nil, nil, secret2.Name, nil, nil)
	})

	/*
	   Release: v1.9
	   Testname: Projected Volume, Secrets, mapped, multiple paths
	   Description: 创建一个带 projected volume source 'secret' 的 Pod，以指定 key 存储 secret。secret 被映射到两个不同的 volume mount。Pod MUST 能从两个 volume mount 成功读取该 key 的内容，映射 volume 上的 mode MUST 为 -r--------。
	*/
	framework.ConformanceIt("should be consumable in multiple volumes in a pod", f.WithNodeConformance(), func(ctx context.Context) {
		// 此测试确保同一个 secret 可以在同一个 Pod 的多个
		// volume 中挂载。该测试用例用于防止
		// 破坏此用例的回归。
		var (
			name             = "projected-secret-test-" + string(uuid.NewUUID())
			volumeName       = "projected-secret-volume"
			volumeMountPath  = "/etc/projected-secret-volume"
			volumeName2      = "projected-secret-volume-2"
			volumeMountPath2 = "/etc/projected-secret-volume-2"
			secret           = secretForTest(f.Namespace.Name, name)
		)

		ginkgo.By(fmt.Sprintf("Creating secret with name %s", secret.Name))
		var err error
		if secret, err = f.ClientSet.CoreV1().Secrets(f.Namespace.Name).Create(ctx, secret, metav1.CreateOptions{}); err != nil {
			framework.Failf("unable to create test secret %s: %v", secret.Name, err)
		}

		pod := &v1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name: "pod-projected-secrets-" + string(uuid.NewUUID()),
			},
			Spec: v1.PodSpec{
				Volumes: []v1.Volume{
					{
						Name: volumeName,
						VolumeSource: v1.VolumeSource{
							Projected: &v1.ProjectedVolumeSource{
								Sources: []v1.VolumeProjection{
									{
										Secret: &v1.SecretProjection{
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
										Secret: &v1.SecretProjection{
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
						Name:  "secret-volume-test",
						Image: imageutils.GetE2EImage(imageutils.Agnhost),
						Args: []string{
							"mounttest",
							"--file_content=/etc/projected-secret-volume/data-1",
							"--file_mode=/etc/projected-secret-volume/data-1"},
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

		fileModeRegexp := getFileModeRegex("/etc/projected-secret-volume/data-1", nil)
		e2epodoutput.TestContainerOutputRegexp(ctx, f, "consume secrets", pod, 0, []string{
			"content of file \"/etc/projected-secret-volume/data-1\": value-1",
			fileModeRegexp,
		})
	})

	/*
	   Release: v1.9
	   Testname: Projected Volume, Secrets, create, update delete
	   Description: 创建一个含三个容器的 Pod，分别处理 secret 的创建、更新和删除。create 容器启动时不带 secret，update 和 delete 容器创建时带 secret 值。在 create 容器中创建 secret，Pod MUST 能从该容器读取。在 update 容器中更新 secret，Pod MUST 能读取更新后的值。在 delete 容器中删除 secret，Pod MUST 无法从该容器读取 secret。
	*/
	framework.ConformanceIt("optional updates should be reflected in volume", f.WithNodeConformance(), func(ctx context.Context) {
		podLogTimeout := e2epod.GetPodSecretUpdateTimeout(ctx, f.ClientSet)
		containerTimeoutArg := fmt.Sprintf("--retry_time=%v", int(podLogTimeout.Seconds()))
		trueVal := true
		volumeMountPath := "/etc/projected-secret-volumes"

		deleteName := "s-test-opt-del-" + string(uuid.NewUUID())
		deleteContainerName := "dels-volume-test"
		deleteVolumeName := "deletes-volume"
		deleteSecret := &v1.Secret{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: f.Namespace.Name,
				Name:      deleteName,
			},
			Data: map[string][]byte{
				"data-1": []byte("value-1"),
			},
		}

		updateName := "s-test-opt-upd-" + string(uuid.NewUUID())
		updateContainerName := "upds-volume-test"
		updateVolumeName := "updates-volume"
		updateSecret := &v1.Secret{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: f.Namespace.Name,
				Name:      updateName,
			},
			Data: map[string][]byte{
				"data-1": []byte("value-1"),
			},
		}

		createName := "s-test-opt-create-" + string(uuid.NewUUID())
		createContainerName := "creates-volume-test"
		createVolumeName := "creates-volume"
		createSecret := &v1.Secret{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: f.Namespace.Name,
				Name:      createName,
			},
			Data: map[string][]byte{
				"data-1": []byte("value-1"),
			},
		}

		ginkgo.By(fmt.Sprintf("Creating secret with name %s", deleteSecret.Name))
		var err error
		if deleteSecret, err = f.ClientSet.CoreV1().Secrets(f.Namespace.Name).Create(ctx, deleteSecret, metav1.CreateOptions{}); err != nil {
			framework.Failf("unable to create test secret %s: %v", deleteSecret.Name, err)
		}

		ginkgo.By(fmt.Sprintf("Creating secret with name %s", updateSecret.Name))
		if updateSecret, err = f.ClientSet.CoreV1().Secrets(f.Namespace.Name).Create(ctx, updateSecret, metav1.CreateOptions{}); err != nil {
			framework.Failf("unable to create test secret %s: %v", updateSecret.Name, err)
		}

		pod := &v1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name: "pod-projected-secrets-" + string(uuid.NewUUID()),
			},
			Spec: v1.PodSpec{
				Volumes: []v1.Volume{
					{
						Name: deleteVolumeName,
						VolumeSource: v1.VolumeSource{
							Projected: &v1.ProjectedVolumeSource{
								Sources: []v1.VolumeProjection{
									{
										Secret: &v1.SecretProjection{
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
										Secret: &v1.SecretProjection{
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
										Secret: &v1.SecretProjection{
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
						Args:  []string{"mounttest", "--break_on_expected_content=false", containerTimeoutArg, "--file_content_in_loop=/etc/projected-secret-volumes/delete/data-1"},
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
						Args:  []string{"mounttest", "--break_on_expected_content=false", containerTimeoutArg, "--file_content_in_loop=/etc/projected-secret-volumes/update/data-3"},
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
						Args:  []string{"mounttest", "--break_on_expected_content=false", containerTimeoutArg, "--file_content_in_loop=/etc/projected-secret-volumes/create/data-1"},
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
		gomega.Eventually(ctx, pollCreateLogs, podLogTimeout, framework.Poll).Should(gomega.ContainSubstring("Error reading file /etc/projected-secret-volumes/create/data-1"))

		pollUpdateLogs := func() (string, error) {
			return e2epod.GetPodLogs(ctx, f.ClientSet, f.Namespace.Name, pod.Name, updateContainerName)
		}
		gomega.Eventually(ctx, pollUpdateLogs, podLogTimeout, framework.Poll).Should(gomega.ContainSubstring("Error reading file /etc/projected-secret-volumes/update/data-3"))

		pollDeleteLogs := func() (string, error) {
			return e2epod.GetPodLogs(ctx, f.ClientSet, f.Namespace.Name, pod.Name, deleteContainerName)
		}
		gomega.Eventually(ctx, pollDeleteLogs, podLogTimeout, framework.Poll).Should(gomega.ContainSubstring("value-1"))

		ginkgo.By(fmt.Sprintf("Deleting secret %v", deleteSecret.Name))
		err = f.ClientSet.CoreV1().Secrets(f.Namespace.Name).Delete(ctx, deleteSecret.Name, metav1.DeleteOptions{})
		framework.ExpectNoError(err, "Failed to delete secret %q in namespace %q", deleteSecret.Name, f.Namespace.Name)

		ginkgo.By(fmt.Sprintf("Updating secret %v", updateSecret.Name))
		updateSecret.ResourceVersion = "" // 强制更新
		delete(updateSecret.Data, "data-1")
		updateSecret.Data["data-3"] = []byte("value-3")
		_, err = f.ClientSet.CoreV1().Secrets(f.Namespace.Name).Update(ctx, updateSecret, metav1.UpdateOptions{})
		framework.ExpectNoError(err, "Failed to update secret %q in namespace %q", updateSecret.Name, f.Namespace.Name)

		ginkgo.By(fmt.Sprintf("Creating secret with name %s", createSecret.Name))
		if createSecret, err = f.ClientSet.CoreV1().Secrets(f.Namespace.Name).Create(ctx, createSecret, metav1.CreateOptions{}); err != nil {
			framework.Failf("unable to create test secret %s: %v", createSecret.Name, err)
		}

		ginkgo.By("waiting to observe update in volume")

		gomega.Eventually(ctx, pollCreateLogs, podLogTimeout, framework.Poll).Should(gomega.ContainSubstring("value-1"))
		gomega.Eventually(ctx, pollUpdateLogs, podLogTimeout, framework.Poll).Should(gomega.ContainSubstring("value-3"))
		gomega.Eventually(ctx, pollDeleteLogs, podLogTimeout, framework.Poll).Should(gomega.ContainSubstring("Error reading file /etc/projected-secret-volumes/delete/data-1"))
	})

	//在 volume 创建期间 secret 处于 pending，直到 secret 对象可用
	//或挂载 secret volume 超时。该 Pod 没有定义 secret 对象，因此除非标记为 optional，否则应返回超时异常。
	//慢（约 5 分钟）
	f.It("Should fail non-optional pod creation due to secret object does not exist", f.WithSlow(), f.WithNodeConformance(), func(ctx context.Context) {
		volumeMountPath := "/etc/projected-secret-volumes"
		podName := "pod-secrets-" + string(uuid.NewUUID())
		pod := createNonOptionalSecretPod(ctx, f, volumeMountPath, podName)
		getPod := e2epod.Get(f.ClientSet, pod)
		gomega.Consistently(ctx, getPod).WithTimeout(f.Timeouts.PodStart).Should(e2epod.BeInPhase(v1.PodPending))
	})

	//为 Pod 定义了 Secret 对象，如果指定的 key 在 secret 中不存在，
	// 则在 Pod 创建期间 volume 设置将报错，除非标记为 optional。
	//慢（约 5 分钟）
	f.It("Should fail non-optional pod creation due to the key in the secret object does not exist", f.WithSlow(), f.WithNodeConformance(), func(ctx context.Context) {
		volumeMountPath := "/etc/secret-volumes"
		podName := "pod-secrets-" + string(uuid.NewUUID())
		pod := createNonOptionalSecretPodWithSecret(ctx, f, volumeMountPath, podName)
		getPod := e2epod.Get(f.ClientSet, pod)
		gomega.Consistently(ctx, getPod).WithTimeout(f.Timeouts.PodStart).Should(e2epod.BeInPhase(v1.PodPending))
	})
})

func doProjectedSecretE2EWithoutMapping(ctx context.Context, f *framework.Framework, defaultMode *int32, defaultUser *int64,
	secretName string, fsGroup *int64, uid *int64) {
	var (
		volumeName      = "projected-secret-volume"
		volumeMountPath = "/etc/projected-secret-volume"
		secret          = secretForTest(f.Namespace.Name, secretName)
	)

	ginkgo.By(fmt.Sprintf("Creating projection with secret that has name %s", secret.Name))
	var err error
	if secret, err = f.ClientSet.CoreV1().Secrets(f.Namespace.Name).Create(ctx, secret, metav1.CreateOptions{}); err != nil {
		framework.Failf("unable to create test secret %s: %v", secret.Name, err)
	}

	pod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pod-projected-secrets-" + string(uuid.NewUUID()),
			Namespace: f.Namespace.Name,
		},
		Spec: v1.PodSpec{
			Volumes: []v1.Volume{
				{
					Name: volumeName,
					VolumeSource: v1.VolumeSource{
						Projected: &v1.ProjectedVolumeSource{
							Sources: []v1.VolumeProjection{
								{
									Secret: &v1.SecretProjection{
										LocalObjectReference: v1.LocalObjectReference{
											Name: secretName,
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
					Name:  "projected-secret-volume-test",
					Image: imageutils.GetE2EImage(imageutils.Agnhost),
					Args: []string{
						"mounttest",
						"--file_content=/etc/projected-secret-volume/data-1",
						"--file_mode=/etc/projected-secret-volume/data-1",
						"--file_owner=/etc/projected-secret-volume/data-1",
					},
					VolumeMounts: []v1.VolumeMount{
						{
							Name:      volumeName,
							MountPath: volumeMountPath,
						},
					},
				},
			},
			RestartPolicy: v1.RestartPolicyNever,
		},
	}

	if defaultMode != nil {
		pod.Spec.Volumes[0].VolumeSource.Projected.DefaultMode = defaultMode
	}
	if defaultUser != nil {
		pod.Spec.Volumes[0].VolumeSource.Projected.DefaultUser = defaultUser
	}

	if fsGroup != nil || uid != nil {
		pod.Spec.SecurityContext = &v1.PodSecurityContext{
			FSGroup:   fsGroup,
			RunAsUser: uid,
		}
	}

	fileModeRegexp := getFileModeRegex("/etc/projected-secret-volume/data-1", defaultMode)
	expectedOutput := []string{
		"content of file \"/etc/projected-secret-volume/data-1\": value-1",
		fileModeRegexp,
	}
	if defaultUser != nil {
		expectedOutput = append(expectedOutput, fmt.Sprintf("owner UID of \"/etc/projected-secret-volume/data-1\": %d", *defaultUser))
	}

	e2epodoutput.TestContainerOutputRegexp(ctx, f, "consume secrets", pod, 0, expectedOutput)
}

func doProjectedSecretE2EWithMapping(ctx context.Context, f *framework.Framework, mode *int32, user, runAsUser *int64) {
	var (
		name            = "projected-secret-test-map-" + string(uuid.NewUUID())
		volumeName      = "projected-secret-volume"
		volumeMountPath = "/etc/projected-secret-volume"
		secret          = secretForTest(f.Namespace.Name, name)
	)

	ginkgo.By(fmt.Sprintf("Creating projection with secret that has name %s", secret.Name))
	var err error
	if secret, err = f.ClientSet.CoreV1().Secrets(f.Namespace.Name).Create(ctx, secret, metav1.CreateOptions{}); err != nil {
		framework.Failf("unable to create test secret %s: %v", secret.Name, err)
	}

	pod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "pod-projected-secrets-" + string(uuid.NewUUID()),
		},
		Spec: v1.PodSpec{
			Volumes: []v1.Volume{
				{
					Name: volumeName,
					VolumeSource: v1.VolumeSource{
						Projected: &v1.ProjectedVolumeSource{
							Sources: []v1.VolumeProjection{
								{
									Secret: &v1.SecretProjection{
										LocalObjectReference: v1.LocalObjectReference{
											Name: name,
										},
										Items: []v1.KeyToPath{
											{
												Key:  "data-1",
												Path: "new-path-data-1",
											},
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
					Name:  "projected-secret-volume-test",
					Image: imageutils.GetE2EImage(imageutils.Agnhost),
					Args: []string{
						"mounttest",
						"--file_content=/etc/projected-secret-volume/new-path-data-1",
						"--file_mode=/etc/projected-secret-volume/new-path-data-1",
						"--file_owner=/etc/projected-secret-volume/new-path-data-1",
					},
					VolumeMounts: []v1.VolumeMount{
						{
							Name:      volumeName,
							MountPath: volumeMountPath,
						},
					},
				},
			},
			RestartPolicy: v1.RestartPolicyNever,
		},
	}

	if mode != nil {
		pod.Spec.Volumes[0].VolumeSource.Projected.Sources[0].Secret.Items[0].Mode = mode
	}
	if user != nil {
		pod.Spec.Volumes[0].VolumeSource.Projected.Sources[0].Secret.Items[0].User = user
	}
	if runAsUser != nil {
		pod.Spec.SecurityContext = &v1.PodSecurityContext{
			RunAsUser: runAsUser,
		}
	}

	fileModeRegexp := getFileModeRegex("/etc/projected-secret-volume/new-path-data-1", mode)
	expectedOutput := []string{
		"content of file \"/etc/projected-secret-volume/new-path-data-1\": value-1",
		fileModeRegexp,
	}
	if user != nil {
		expectedOutput = append(expectedOutput, fmt.Sprintf("owner UID of \"/etc/projected-secret-volume/new-path-data-1\": %d", *user))
	}

	e2epodoutput.TestContainerOutputRegexp(ctx, f, "consume secrets", pod, 0, expectedOutput)
}
