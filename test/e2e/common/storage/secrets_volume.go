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
	apierrors "k8s.io/apimachinery/pkg/api/errors"
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

var _ = SIGDescribe("Secrets", func() {
	f := framework.NewDefaultFramework("secrets")
	f.NamespacePodSecurityLevel = admissionapi.LevelBaseline

	/*
		发布版本：v1.9
		测试名称：Secrets Volume, default
		描述：创建一个 secret。创建带 secret volume source 并挂载到容器的 Pod。Pod 必须能从容器运行时的挂载 volume 中读取 secret，secret 的文件模式默认必须为 -rw-r--r--。
	*/
	framework.ConformanceIt("should be consumable from pods in volume", f.WithNodeConformance(), func(ctx context.Context) {
		doSecretE2EWithoutMapping(ctx, f, nil, nil, "secret-test-"+string(uuid.NewUUID()), nil, nil)
	})

	/*
		发布版本：v1.9
		测试名称：Secrets Volume, volume mode 0400
		描述：创建一个 secret。创建带 secret volume source 并挂载到容器、文件模式设为 0x400 的 Pod。Pod 必须能从容器运行时的挂载 volume 中读取 secret，secret 的文件模式默认必须为 -r--------。
		此测试标记为 LinuxOnly，因为 Windows 不支持设置特定文件权限。
	*/
	framework.ConformanceIt("should be consumable from pods in volume with defaultMode set [LinuxOnly]", f.WithNodeConformance(), func(ctx context.Context) {
		defaultMode := int32(0400)
		doSecretE2EWithoutMapping(ctx, f, &defaultMode, nil, "secret-test-"+string(uuid.NewUUID()), nil, nil)
	})

	/*
		发布版本：v1.9
		测试名称：Secrets Volume, volume mode 0440, fsGroup 1001 and uid 1000
		描述：创建一个 secret。创建带 secret volume source 并挂载到容器、文件模式设为 0x440、以 uid 1000 非 root 用户和 fsGroup 1001 运行的 Pod。Pod 必须能从容器运行时的挂载 volume 中读取 secret，secret 的文件模式默认必须为 -r--r-----。
		此测试标记为 LinuxOnly，因为 Windows 不支持设置特定文件权限或以 UID / GID 运行。
	*/
	framework.ConformanceIt("should be consumable from pods in volume as non-root with defaultMode and fsGroup set [LinuxOnly]", f.WithNodeConformance(), func(ctx context.Context) {
		defaultMode := int32(0440) /* setting fsGroup sets mode to at least 440 */
		fsGroup := int64(1001)
		doSecretE2EWithoutMapping(ctx, f, &defaultMode, nil, "secret-test-"+string(uuid.NewUUID()), &fsGroup, &nonRootTestUserID)
	})

	f.It("should be consumable from pods in volume as non-root with defaultUser set [LinuxOnly]", f.WithFeatureGate(features.AtomicWriteVolumeUserFields), func(ctx context.Context) {
		defaultUser := int64(1000)
		doSecretE2EWithoutMapping(ctx, f, nil, &defaultUser, "secret-test-"+string(uuid.NewUUID()), nil, &nonRootTestUserID)
	})

	/*
		发布版本：v1.9
		测试名称：Secrets Volume, mapping
		描述：创建一个 secret。创建带 secret volume source 并挂载到容器自定义路径的 Pod。Pod 必须能从指定自定义路径的挂载 volume 中读取 secret。secret 的文件模式默认必须为 -rw-r--r--。
	*/
	framework.ConformanceIt("should be consumable from pods in volume with mappings", f.WithNodeConformance(), func(ctx context.Context) {
		doSecretE2EWithMapping(ctx, f, nil, nil, nil)
	})

	/*
		发布版本：v1.9
		测试名称：Secrets Volume, mapping, volume mode 0400
		描述：创建一个 secret。创建带 secret volume source 并挂载到容器自定义路径、文件模式设为 0x400 的 Pod。Pod 必须能从指定自定义路径的挂载 volume 中读取 secret。secret 的文件模式必须为 -r--r--r--。
		此测试标记为 LinuxOnly，因为 Windows 不支持设置特定文件权限。
	*/
	framework.ConformanceIt("should be consumable from pods in volume with mappings and Item Mode set [LinuxOnly]", f.WithNodeConformance(), func(ctx context.Context) {
		mode := int32(0400)
		doSecretE2EWithMapping(ctx, f, &mode, nil, nil)
	})

	f.It("should be consumable from pods in volume with mappings and Item User set [LinuxOnly]", f.WithFeatureGate(features.AtomicWriteVolumeUserFields), func(ctx context.Context) {
		user := int64(1000)
		doSecretE2EWithMapping(ctx, f, nil, &user, &user)
	})

	/*
		发布版本：v1.12
		测试名称：Secrets Volume, volume mode default, secret with same name in different namespace
		描述：在两个 namespace 中创建同名 secret。创建带 secret volume source 并挂载到容器的 Pod。Pod 必须能从容器运行时的挂载 volume 中读取 secret，且只能读取与 pod 所在 namespace 关联的 secret。secret 的文件模式默认必须为 -rw-r--r--。
	*/
	framework.ConformanceIt("should be able to mount in a volume regardless of a different secret existing with same name in different namespace", f.WithNodeConformance(), func(ctx context.Context) {
		var (
			namespace2  *v1.Namespace
			err         error
			secret2Name = "secret-test-" + string(uuid.NewUUID())
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
		doSecretE2EWithoutMapping(ctx, f, nil, nil, secret2.Name, nil, nil)
	})

	/*
		发布版本：v1.9
		测试名称：Secrets Volume, mapping multiple volume paths
		描述：创建一个 secret。创建带两个 secret volume source 并挂载到容器两个不同自定义路径的 Pod。Pod 必须能从两个指定自定义路径的挂载 volume 中读取 secret。
	*/
	framework.ConformanceIt("should be consumable in multiple volumes in a pod", f.WithNodeConformance(), func(ctx context.Context) {
		// 本测试确保同一个 secret 可以在同一个 pod 中
		// 挂载到多个 volume。这个用例是为了防止
		// 破坏该场景的回归。
		var (
			name             = "secret-test-" + string(uuid.NewUUID())
			volumeName       = "secret-volume"
			volumeMountPath  = "/etc/secret-volume"
			volumeName2      = "secret-volume-2"
			volumeMountPath2 = "/etc/secret-volume-2"
			secret           = secretForTest(f.Namespace.Name, name)
		)

		ginkgo.By(fmt.Sprintf("Creating secret with name %s", secret.Name))
		var err error
		if secret, err = f.ClientSet.CoreV1().Secrets(f.Namespace.Name).Create(ctx, secret, metav1.CreateOptions{}); err != nil {
			framework.Failf("unable to create test secret %s: %v", secret.Name, err)
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
						Name: volumeName2,
						VolumeSource: v1.VolumeSource{
							Secret: &v1.SecretVolumeSource{
								SecretName: name,
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
							"--file_content=/etc/secret-volume/data-1",
							"--file_mode=/etc/secret-volume/data-1"},
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

		fileModeRegexp := getFileModeRegex("/etc/secret-volume/data-1", nil)
		e2epodoutput.TestContainerOutputRegexp(ctx, f, "consume secrets", pod, 0, []string{
			"content of file \"/etc/secret-volume/data-1\": value-1",
			fileModeRegexp,
		})
	})

	/*
		发布版本：v1.9
		测试名称：Secrets Volume, create, update and delete
		描述：创建带三个容器的 Pod，分别用 create、update 和 delete 容器操作 secret volume source。create 容器启动时必须没有 secret，update 和 delete 容器必须以某个 secret 值创建。在 create 容器中创建 secret，Pod 必须能从 create 容器读取它。在 update 容器中更新 secret，Pod 必须能读到更新后的值。在 delete 容器中删除 secret，Pod 必须无法从 delete 容器读取它。
	*/
	framework.ConformanceIt("optional updates should be reflected in volume", f.WithNodeConformance(), func(ctx context.Context) {
		podLogTimeout := e2epod.GetPodSecretUpdateTimeout(ctx, f.ClientSet)
		containerTimeoutArg := fmt.Sprintf("--retry_time=%v", int(podLogTimeout.Seconds()))
		trueVal := true
		volumeMountPath := "/etc/secret-volumes"

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
				Name: "pod-secrets-" + string(uuid.NewUUID()),
			},
			Spec: v1.PodSpec{
				Volumes: []v1.Volume{
					{
						Name: deleteVolumeName,
						VolumeSource: v1.VolumeSource{
							Secret: &v1.SecretVolumeSource{
								SecretName: deleteName,
								Optional:   &trueVal,
							},
						},
					},
					{
						Name: updateVolumeName,
						VolumeSource: v1.VolumeSource{
							Secret: &v1.SecretVolumeSource{
								SecretName: updateName,
								Optional:   &trueVal,
							},
						},
					},
					{
						Name: createVolumeName,
						VolumeSource: v1.VolumeSource{
							Secret: &v1.SecretVolumeSource{
								SecretName: createName,
								Optional:   &trueVal,
							},
						},
					},
				},
				Containers: []v1.Container{
					{
						Name:  deleteContainerName,
						Image: imageutils.GetE2EImage(imageutils.Agnhost),
						Args:  []string{"mounttest", "--break_on_expected_content=false", containerTimeoutArg, "--file_content_in_loop=/etc/secret-volumes/delete/data-1"},
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
						Args:  []string{"mounttest", "--break_on_expected_content=false", containerTimeoutArg, "--file_content_in_loop=/etc/secret-volumes/update/data-3"},
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
						Args:  []string{"mounttest", "--break_on_expected_content=false", containerTimeoutArg, "--file_content_in_loop=/etc/secret-volumes/create/data-1"},
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
		gomega.Eventually(ctx, pollCreateLogs, podLogTimeout, framework.Poll).Should(gomega.ContainSubstring("Error reading file /etc/secret-volumes/create/data-1"))

		pollUpdateLogs := func() (string, error) {
			return e2epod.GetPodLogs(ctx, f.ClientSet, f.Namespace.Name, pod.Name, updateContainerName)
		}
		gomega.Eventually(ctx, pollUpdateLogs, podLogTimeout, framework.Poll).Should(gomega.ContainSubstring("Error reading file /etc/secret-volumes/update/data-3"))

		pollDeleteLogs := func() (string, error) {
			return e2epod.GetPodLogs(ctx, f.ClientSet, f.Namespace.Name, pod.Name, deleteContainerName)
		}
		gomega.Eventually(ctx, pollDeleteLogs, podLogTimeout, framework.Poll).Should(gomega.ContainSubstring("value-1"))

		ginkgo.By(fmt.Sprintf("Deleting secret %v", deleteSecret.Name))
		err = f.ClientSet.CoreV1().Secrets(f.Namespace.Name).Delete(ctx, deleteSecret.Name, metav1.DeleteOptions{})
		framework.ExpectNoError(err, "Failed to delete secret %q in namespace %q", deleteSecret.Name, f.Namespace.Name)

		ginkgo.By(fmt.Sprintf("Updating secret %v", updateSecret.Name))
		updateSecret.ResourceVersion = "" // to force update
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
		gomega.Eventually(ctx, pollDeleteLogs, podLogTimeout, framework.Poll).Should(gomega.ContainSubstring("Error reading file /etc/secret-volumes/delete/data-1"))
	})

	/*
		发布版本：v1.21
		测试名称：Secrets Volume, immutability
		描述：创建一个 secret。更新其 data 字段，更新必须成功。
			将 secret 标记为 immutable，更新必须成功。尝试更新其 data，更新必须失败。
			尝试将 secret 改回非 immutable，更新必须失败。
			尝试更新 secret 的 metadata（标签），更新必须成功。
			尝试删除 secret，删除必须成功。
	*/
	framework.ConformanceIt("should be immutable if `immutable` field is set", func(ctx context.Context) {
		name := "immutable"
		secret := secretForTest(f.Namespace.Name, name)

		currentSecret, err := f.ClientSet.CoreV1().Secrets(f.Namespace.Name).Create(ctx, secret, metav1.CreateOptions{})
		framework.ExpectNoError(err, "Failed to create secret %q in namespace %q", secret.Name, secret.Namespace)

		currentSecret.Data["data-4"] = []byte("value-4\n")
		currentSecret, err = f.ClientSet.CoreV1().Secrets(f.Namespace.Name).Update(ctx, currentSecret, metav1.UpdateOptions{})
		framework.ExpectNoError(err, "Failed to update secret %q in namespace %q", secret.Name, secret.Namespace)

		// 将 secret 标记为 immutable。
		trueVal := true
		currentSecret.Immutable = &trueVal
		currentSecret, err = f.ClientSet.CoreV1().Secrets(f.Namespace.Name).Update(ctx, currentSecret, metav1.UpdateOptions{})
		framework.ExpectNoError(err, "Failed to mark secret %q in namespace %q as immutable", secret.Name, secret.Namespace)

		// 确保现在不能更改 data。
		currentSecret.Data["data-5"] = []byte("value-5\n")
		_, err = f.ClientSet.CoreV1().Secrets(f.Namespace.Name).Update(ctx, currentSecret, metav1.UpdateOptions{})
		if !apierrors.IsInvalid(err) {
			framework.Failf("expected 'invalid' as error, got instead: %v", err)
		}

		// 确保 secret 不能从 immutable 切回 mutable。
		currentSecret, err = f.ClientSet.CoreV1().Secrets(f.Namespace.Name).Get(ctx, name, metav1.GetOptions{})
		framework.ExpectNoError(err, "Failed to get secret %q in namespace %q", secret.Name, secret.Namespace)
		if !*currentSecret.Immutable {
			framework.Failf("currentSecret %s can be switched from immutable to mutable", currentSecret.Name)
		}

		falseVal := false
		currentSecret.Immutable = &falseVal
		_, err = f.ClientSet.CoreV1().Secrets(f.Namespace.Name).Update(ctx, currentSecret, metav1.UpdateOptions{})
		if !apierrors.IsInvalid(err) {
			framework.Failf("expected 'invalid' as error, got instead: %v", err)
		}

		// 确保 metadata 可以更改。
		currentSecret, err = f.ClientSet.CoreV1().Secrets(f.Namespace.Name).Get(ctx, name, metav1.GetOptions{})
		framework.ExpectNoError(err, "Failed to get secret %q in namespace %q", secret.Name, secret.Namespace)
		currentSecret.Labels = map[string]string{"label1": "value1"}
		_, err = f.ClientSet.CoreV1().Secrets(f.Namespace.Name).Update(ctx, currentSecret, metav1.UpdateOptions{})
		framework.ExpectNoError(err, "Failed to update secret %q in namespace %q", secret.Name, secret.Namespace)

		// 确保 immutable secret 可以被删除。
		err = f.ClientSet.CoreV1().Secrets(f.Namespace.Name).Delete(ctx, name, metav1.DeleteOptions{})
		framework.ExpectNoError(err, "Failed to delete secret %q in namespace %q", secret.Name, secret.Namespace)
	})

	// volume 创建期间 pod 处于 pending，直到 secret 对象可用
	// 或挂载 secret volume 超时。pod 没有定义 secret 对象，因此除非标记为 optional，否则应返回超时异常。
	// 慢（约 5 分钟）
	f.It("Should fail non-optional pod creation due to secret object does not exist", f.WithSlow(), f.WithNodeConformance(), func(ctx context.Context) {
		volumeMountPath := "/etc/secret-volumes"
		podName := "pod-secrets-" + string(uuid.NewUUID())
		pod := createNonOptionalSecretPod(ctx, f, volumeMountPath, podName)
		getPod := e2epod.Get(f.ClientSet, pod)
		gomega.Consistently(ctx, getPod).WithTimeout(f.Timeouts.PodStart).Should(e2epod.BeInPhase(v1.PodPending))
	})

	// 为 pod 定义了 Secret 对象。如果指定的 key 在 secret 中不存在，
	// pod 创建时 volume 设置会报错，除非标记为 optional。
	// 慢（约 5 分钟）
	f.It("Should fail non-optional pod creation due to the key in the secret object does not exist", f.WithSlow(), f.WithNodeConformance(), func(ctx context.Context) {
		volumeMountPath := "/etc/secret-volumes"
		podName := "pod-secrets-" + string(uuid.NewUUID())
		pod := createNonOptionalSecretPodWithSecret(ctx, f, volumeMountPath, podName)
		getPod := e2epod.Get(f.ClientSet, pod)
		gomega.Consistently(ctx, getPod).WithTimeout(f.Timeouts.PodStart).Should(e2epod.BeInPhase(v1.PodPending))
	})
})

func secretForTest(namespace, name string) *v1.Secret {
	return &v1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: namespace,
			Name:      name,
		},
		Data: map[string][]byte{
			"data-1": []byte("value-1\n"),
			"data-2": []byte("value-2\n"),
			"data-3": []byte("value-3\n"),
		},
	}
}

func doSecretE2EWithoutMapping(ctx context.Context, f *framework.Framework, defaultMode *int32, defaultUser *int64, secretName string,
	fsGroup *int64, uid *int64) {
	var (
		volumeName      = "secret-volume"
		volumeMountPath = "/etc/secret-volume"
		secret          = secretForTest(f.Namespace.Name, secretName)
	)

	ginkgo.By(fmt.Sprintf("Creating secret with name %s", secret.Name))
	var err error
	if secret, err = f.ClientSet.CoreV1().Secrets(f.Namespace.Name).Create(ctx, secret, metav1.CreateOptions{}); err != nil {
		framework.Failf("unable to create test secret %s: %v", secret.Name, err)
	}

	pod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pod-secrets-" + string(uuid.NewUUID()),
			Namespace: f.Namespace.Name,
		},
		Spec: v1.PodSpec{
			Volumes: []v1.Volume{
				{
					Name: volumeName,
					VolumeSource: v1.VolumeSource{
						Secret: &v1.SecretVolumeSource{
							SecretName: secretName,
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
						"--file_content=/etc/secret-volume/data-1",
						"--file_mode=/etc/secret-volume/data-1",
						"--file_owner=/etc/secret-volume/data-1",
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
		pod.Spec.Volumes[0].VolumeSource.Secret.DefaultMode = defaultMode
	}
	if defaultUser != nil {
		pod.Spec.Volumes[0].VolumeSource.Secret.DefaultUser = defaultUser
	}

	if fsGroup != nil || uid != nil {
		pod.Spec.SecurityContext = &v1.PodSecurityContext{
			FSGroup:   fsGroup,
			RunAsUser: uid,
		}
	}

	fileModeRegexp := getFileModeRegex("/etc/secret-volume/data-1", defaultMode)
	expectedOutput := []string{
		"content of file \"/etc/secret-volume/data-1\": value-1",
		fileModeRegexp,
	}
	if defaultUser != nil {
		expectedOutput = append(expectedOutput, fmt.Sprintf("owner UID of \"/etc/secret-volume/data-1\": %d", *defaultUser))
	}

	e2epodoutput.TestContainerOutputRegexp(ctx, f, "consume secrets", pod, 0, expectedOutput)
}

func doSecretE2EWithMapping(ctx context.Context, f *framework.Framework, mode *int32, user, runAsUser *int64) {
	var (
		name            = "secret-test-map-" + string(uuid.NewUUID())
		volumeName      = "secret-volume"
		volumeMountPath = "/etc/secret-volume"
		secret          = secretForTest(f.Namespace.Name, name)
	)

	ginkgo.By(fmt.Sprintf("Creating secret with name %s", secret.Name))
	var err error
	if secret, err = f.ClientSet.CoreV1().Secrets(f.Namespace.Name).Create(ctx, secret, metav1.CreateOptions{}); err != nil {
		framework.Failf("unable to create test secret %s: %v", secret.Name, err)
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
			Containers: []v1.Container{
				{
					Name:  "secret-volume-test",
					Image: imageutils.GetE2EImage(imageutils.Agnhost),
					Args: []string{
						"mounttest",
						"--file_content=/etc/secret-volume/new-path-data-1",
						"--file_mode=/etc/secret-volume/new-path-data-1",
						"--file_owner=/etc/secret-volume/new-path-data-1",
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
		pod.Spec.Volumes[0].VolumeSource.Secret.Items[0].Mode = mode
	}
	if user != nil {
		pod.Spec.Volumes[0].VolumeSource.Secret.Items[0].User = user
	}
	if runAsUser != nil {
		pod.Spec.SecurityContext = &v1.PodSecurityContext{
			RunAsUser: runAsUser,
		}
	}

	fileModeRegexp := getFileModeRegex("/etc/secret-volume/new-path-data-1", mode)
	expectedOutput := []string{
		"content of file \"/etc/secret-volume/new-path-data-1\": value-1",
		fileModeRegexp,
	}
	if user != nil {
		expectedOutput = append(expectedOutput, fmt.Sprintf("owner UID of \"/etc/secret-volume/new-path-data-1\": %d", *user))
	}

	e2epodoutput.TestContainerOutputRegexp(ctx, f, "consume secrets", pod, 0, expectedOutput)
}

func createNonOptionalSecretPod(ctx context.Context, f *framework.Framework, volumeMountPath, podName string) *v1.Pod {
	podLogTimeout := e2epod.GetPodSecretUpdateTimeout(ctx, f.ClientSet)
	containerTimeoutArg := fmt.Sprintf("--retry_time=%v", int(podLogTimeout.Seconds()))
	falseValue := false

	createName := "s-test-opt-create-" + string(uuid.NewUUID())
	createContainerName := "creates-volume-test"
	createVolumeName := "creates-volume"

	// 创建一个 pod，没有创建 secret 对象，但指定了 secret volume source 引用名称
	pod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: podName,
		},
		Spec: v1.PodSpec{
			Volumes: []v1.Volume{
				{
					Name: createVolumeName,
					VolumeSource: v1.VolumeSource{
						Secret: &v1.SecretVolumeSource{
							SecretName: createName,
							Optional:   &falseValue,
						},
					},
				},
			},
			Containers: []v1.Container{
				{
					Name:  createContainerName,
					Image: imageutils.GetE2EImage(imageutils.Agnhost),
					Args:  []string{"mounttest", "--break_on_expected_content=false", containerTimeoutArg, "--file_content_in_loop=/etc/secret-volumes/create/data-1"},
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
	pod = e2epod.NewPodClient(f).Create(ctx, pod)
	return pod
}

func createNonOptionalSecretPodWithSecret(ctx context.Context, f *framework.Framework, volumeMountPath, podName string) *v1.Pod {
	podLogTimeout := e2epod.GetPodSecretUpdateTimeout(ctx, f.ClientSet)
	containerTimeoutArg := fmt.Sprintf("--retry_time=%v", int(podLogTimeout.Seconds()))
	falseValue := false

	createName := "s-test-opt-create-" + string(uuid.NewUUID())
	createContainerName := "creates-volume-test"
	createVolumeName := "creates-volume"

	secret := secretForTest(f.Namespace.Name, createName)

	ginkgo.By(fmt.Sprintf("Creating secret with name %s", secret.Name))
	var err error
	if secret, err = f.ClientSet.CoreV1().Secrets(f.Namespace.Name).Create(ctx, secret, metav1.CreateOptions{}); err != nil {
		framework.Failf("unable to create test secret %s: %v", secret.Name, err)
	}
	// 创建一个 pod 带有 secret 对象，但使用的 key 在 secret 对象中不存在。
	pod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: podName,
		},
		Spec: v1.PodSpec{
			Volumes: []v1.Volume{
				{
					Name: createVolumeName,
					VolumeSource: v1.VolumeSource{
						Secret: &v1.SecretVolumeSource{
							SecretName: createName,
							Items: []v1.KeyToPath{
								{
									Key:  "data_4",
									Path: "value-4\n",
								},
							},
							Optional: &falseValue,
						},
					},
				},
			},
			Containers: []v1.Container{
				{
					Name:  createContainerName,
					Image: imageutils.GetE2EImage(imageutils.Agnhost),
					Args:  []string{"mounttest", "--break_on_expected_content=false", containerTimeoutArg, "--file_content_in_loop=/etc/secret-volumes/create/data-1"},
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
	pod = e2epod.NewPodClient(f).Create(ctx, pod)
	return pod
}
