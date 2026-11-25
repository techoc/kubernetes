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

package auth

import (
	"context"
	"encoding/json"
	"fmt"
	"path"
	"regexp"
	"strings"
	"time"

	authenticationv1 "k8s.io/api/authentication/v1"
	v1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	utilrand "k8s.io/apimachinery/pkg/util/rand"
	"k8s.io/apimachinery/pkg/util/resourceversion"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/uuid"
	"k8s.io/apimachinery/pkg/util/wait"
	watch "k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/util/retry"
	"k8s.io/kubernetes/pkg/features"
	"k8s.io/kubernetes/plugin/pkg/admission/serviceaccount"
	apimachineryutils "k8s.io/kubernetes/test/e2e/common/apimachinery"
	"k8s.io/kubernetes/test/e2e/framework"
	e2ekubectl "k8s.io/kubernetes/test/e2e/framework/kubectl"
	e2epod "k8s.io/kubernetes/test/e2e/framework/pod"
	e2eoutput "k8s.io/kubernetes/test/e2e/framework/pod/output"
	e2eskipper "k8s.io/kubernetes/test/e2e/framework/skipper"
	imageutils "k8s.io/kubernetes/test/utils/image"
	admissionapi "k8s.io/pod-security-admission/api"
	"k8s.io/utils/ptr"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
)

const rootCAConfigMapName = "kube-root-ca.crt"

var _ = SIGDescribe("ServiceAccounts", func() {
	f := framework.NewDefaultFramework("svcaccounts")
	f.NamespacePodSecurityLevel = admissionapi.LevelBaseline

	ginkgo.It("no secret-based service account token should be auto-generated", func(ctx context.Context) {
		{
			ginkgo.By("ensuring no secret-based service account token exists")
			time.Sleep(10 * time.Second)
			sa, err := f.ClientSet.CoreV1().ServiceAccounts(f.Namespace.Name).Get(ctx, "default", metav1.GetOptions{})
			framework.ExpectNoError(err)
			gomega.Expect(sa.Secrets).To(gomega.BeEmpty())
		}
	})

	/*
		   Release: v1.9
		   Testname: Service Account Tokens Must AutoMount
		   Description: 确保 Service Account 的密钥被挂载到容器中。Pod
			包含三个容器，分别从默认 API Token 挂载路径读取 Service Account token、
			root CA 和默认命名空间。这三个文件 MUST 存在，并且 Service Account
			挂载路径 MUST 自动挂载到容器中。
	*/
	framework.ConformanceIt("should mount an API token into pods", func(ctx context.Context) {
		sa, err := f.ClientSet.CoreV1().ServiceAccounts(f.Namespace.Name).Create(ctx, &v1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Name: "mount-test"}}, metav1.CreateOptions{})
		framework.ExpectNoError(err)

		zero := int64(0)
		pod, err := f.ClientSet.CoreV1().Pods(f.Namespace.Name).Create(ctx, &v1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name: "pod-service-account-" + string(uuid.NewUUID()),
			},
			Spec: v1.PodSpec{
				ServiceAccountName: sa.Name,
				Containers: []v1.Container{{
					Name:    "test",
					Image:   imageutils.GetE2EImage(imageutils.BusyBox),
					Command: []string{"sleep", "100000"},
				}},
				TerminationGracePeriodSeconds: &zero,
				RestartPolicy:                 v1.RestartPolicyNever,
			},
		}, metav1.CreateOptions{})
		framework.ExpectNoError(err)
		framework.ExpectNoError(e2epod.WaitForPodRunningInNamespace(ctx, f.ClientSet, pod))

		// 读取正在运行的 Pod 以获取当前的节点名
		pod, err = f.ClientSet.CoreV1().Pods(f.Namespace.Name).Get(ctx, pod.Name, metav1.GetOptions{})
		framework.ExpectNoError(err)
		node, err := f.ClientSet.CoreV1().Nodes().Get(ctx, pod.Spec.NodeName, metav1.GetOptions{})
		framework.ExpectNoError(err)

		tk := e2ekubectl.NewTestKubeconfig(framework.TestContext.CertDir, framework.TestContext.Host, framework.TestContext.KubeConfig, framework.TestContext.KubeContext, framework.TestContext.KubectlPath, f.Namespace.Name)
		mountedToken, err := tk.ReadFileViaContainer(pod.Name, pod.Spec.Containers[0].Name, path.Join(serviceaccount.DefaultAPITokenMountPath, v1.ServiceAccountTokenKey))
		framework.ExpectNoError(err)
		mountedCA, err := tk.ReadFileViaContainer(pod.Name, pod.Spec.Containers[0].Name, path.Join(serviceaccount.DefaultAPITokenMountPath, v1.ServiceAccountRootCAKey))
		framework.ExpectNoError(err)
		mountedNamespace, err := tk.ReadFileViaContainer(pod.Name, pod.Spec.Containers[0].Name, path.Join(serviceaccount.DefaultAPITokenMountPath, v1.ServiceAccountNamespaceKey))
		framework.ExpectNoError(err)

		// CA 和命名空间应相同
		rootCA, err := f.ClientSet.CoreV1().ConfigMaps(f.Namespace.Name).Get(ctx, rootCAConfigMapName, metav1.GetOptions{})
		framework.ExpectNoError(err)
		framework.Logf("Got root ca configmap in namespace %q", f.Namespace.Name)
		gomega.Expect(mountedCA).To(gomega.Equal(rootCA.Data["ca.crt"]))
		gomega.Expect(mountedNamespace).To(gomega.Equal(f.Namespace.Name))
		// Token 应是标识该 Pod 的 Service Account 的有效凭据
		tokenReview := &authenticationv1.TokenReview{Spec: authenticationv1.TokenReviewSpec{Token: mountedToken}}
		tokenReview, err = f.ClientSet.AuthenticationV1().TokenReviews().Create(ctx, tokenReview, metav1.CreateOptions{})
		framework.ExpectNoError(err)
		if !tokenReview.Status.Authenticated {
			framework.Fail("tokenReview is not authenticated")
		}
		gomega.Expect(tokenReview.Status.Error).To(gomega.BeEmpty())
		gomega.Expect(tokenReview.Status.User.Username).To(gomega.Equal("system:serviceaccount:" + f.Namespace.Name + ":" + sa.Name))
		groups := sets.NewString(tokenReview.Status.User.Groups...)
		if !groups.Has("system:authenticated") {
			framework.Failf("expected system:authenticated group, had %v", groups.List())
		}
		if !groups.Has("system:serviceaccounts") {
			framework.Failf("expected system:serviceaccounts group, had %v", groups.List())
		}
		if !groups.Has("system:serviceaccounts:" + f.Namespace.Name) {
			framework.Failf("expected system:serviceaccounts:%s group, had %v", f.Namespace.Name, groups.List())
		}

		credentialID, ok := tokenReview.Status.User.Extra["authentication.kubernetes.io/credential-id"]
		if !ok || len(credentialID) != 1 || !strings.HasPrefix(credentialID[0], "JTI=") {
			framework.Failf("expected single authentication.kubernetes.io/credential-id extra info item starting with 'JTI=', got %v", credentialID)
		}

		podName, ok := tokenReview.Status.User.Extra["authentication.kubernetes.io/pod-name"]
		if !ok || len(podName) != 1 || podName[0] != pod.Name {
			framework.Failf("expected single authentication.kubernetes.io/pod-name extra info item matching %v, got %v", pod.Name, podName)
		}
		podUID, ok := tokenReview.Status.User.Extra["authentication.kubernetes.io/pod-uid"]
		if !ok || len(podUID) != 1 || podUID[0] != string(pod.UID) {
			framework.Failf("expected single authentication.kubernetes.io/pod-uid extra info item matching %v, got %v", pod.UID, podUID)
		}

		nodeName, ok := tokenReview.Status.User.Extra["authentication.kubernetes.io/node-name"]
		if !ok || len(nodeName) != 1 || nodeName[0] != node.Name {
			framework.Failf("expected single authentication.kubernetes.io/node-name extra info item matching %v, got %v", node.Name, nodeName)
		}
		nodeUID, ok := tokenReview.Status.User.Extra["authentication.kubernetes.io/node-uid"]
		if !ok || len(nodeUID) != 1 || nodeUID[0] != string(node.UID) {
			framework.Failf("expected single authentication.kubernetes.io/node-uid extra info item matching %v, got %v", node.UID, nodeUID)
		}
	})

	/*
	   Release: v1.9
	   Testname: Service account tokens auto mount optionally
	   Description: 确保只有当 AutoMountServiceToken 未设为 false 时，Service Account 密钥才会挂载到 Pod 中。这里测试以下场景。
	   1. 创建 Pod，Pod Spec 中 AutomountServiceAccountToken 为 nil
	      a) Service Account 为默认值，
	      b) Service Account 将 AutomountServiceAccountToken 设为 true，
	      c) Service Account 将 AutomountServiceAccountToken 设为 false
	   2. 创建 Pod，Pod Spec 中 AutomountServiceAccountToken 为 true
	      a) Service Account 为默认值，
	      b) Service Account 将 AutomountServiceAccountToken 设为 true，
	      c) Service Account 将 AutomountServiceAccountToken 设为 false
	   3. 创建 Pod，Pod Spec 中 AutomountServiceAccountToken 为 false
	      a) Service Account 为默认值，
	      b) Service Account 将 AutomountServiceAccountToken 设为 true，
	      c) Service Account 将 AutomountServiceAccountToken 设为 false

	   这些 Pod 中的容器 MUST 验证：只有当 Pod Spec 和 ServiceAccount 对象中的 AutomountServiceAccountToken 都不为 false 时，ServiceTokenVolume 路径才会自动挂载，即测试用例 1a、1b、2a、2b 和 2c。
	   在测试用例 1c、3a、3b 和 3c 中，ServiceTokenVolume MUST 不会自动挂载。
	*/
	framework.ConformanceIt("should allow opting out of API token automount", func(ctx context.Context) {

		var err error
		trueValue := true
		falseValue := false
		mountSA := &v1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Name: "mount"}, AutomountServiceAccountToken: &trueValue}
		nomountSA := &v1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Name: "nomount"}, AutomountServiceAccountToken: &falseValue}
		mountSA, err = f.ClientSet.CoreV1().ServiceAccounts(f.Namespace.Name).Create(ctx, mountSA, metav1.CreateOptions{})
		framework.ExpectNoError(err)
		nomountSA, err = f.ClientSet.CoreV1().ServiceAccounts(f.Namespace.Name).Create(ctx, nomountSA, metav1.CreateOptions{})
		framework.ExpectNoError(err)

		testcases := []struct {
			PodName            string
			ServiceAccountName string
			AutomountPodSpec   *bool
			ExpectTokenVolume  bool
		}{
			{
				PodName:            "pod-service-account-defaultsa",
				ServiceAccountName: "default",
				AutomountPodSpec:   nil,
				ExpectTokenVolume:  true, // 默认为 true
			},
			{
				PodName:            "pod-service-account-mountsa",
				ServiceAccountName: mountSA.Name,
				AutomountPodSpec:   nil,
				ExpectTokenVolume:  true,
			},
			{
				PodName:            "pod-service-account-nomountsa",
				ServiceAccountName: nomountSA.Name,
				AutomountPodSpec:   nil,
				ExpectTokenVolume:  false,
			},

			// 确保选择加入时 pod spec 优先
			{
				PodName:            "pod-service-account-defaultsa-mountspec",
				ServiceAccountName: "default",
				AutomountPodSpec:   &trueValue,
				ExpectTokenVolume:  true,
			},
			{
				PodName:            "pod-service-account-mountsa-mountspec",
				ServiceAccountName: mountSA.Name,
				AutomountPodSpec:   &trueValue,
				ExpectTokenVolume:  true,
			},
			{
				PodName:            "pod-service-account-nomountsa-mountspec",
				ServiceAccountName: nomountSA.Name,
				AutomountPodSpec:   &trueValue,
				ExpectTokenVolume:  true, // pod spec 优先
			},

			// 确保选择退出时 pod spec 优先
			{
				PodName:            "pod-service-account-defaultsa-nomountspec",
				ServiceAccountName: "default",
				AutomountPodSpec:   &falseValue,
				ExpectTokenVolume:  false, // pod spec 优先
			},
			{
				PodName:            "pod-service-account-mountsa-nomountspec",
				ServiceAccountName: mountSA.Name,
				AutomountPodSpec:   &falseValue,
				ExpectTokenVolume:  false, // pod spec 优先
			},
			{
				PodName:            "pod-service-account-nomountsa-nomountspec",
				ServiceAccountName: nomountSA.Name,
				AutomountPodSpec:   &falseValue,
				ExpectTokenVolume:  false, // pod spec 优先
			},
		}

		for _, tc := range testcases {
			pod := &v1.Pod{
				ObjectMeta: metav1.ObjectMeta{Name: tc.PodName},
				Spec: v1.PodSpec{
					Containers:                   []v1.Container{{Name: "token-test", Image: imageutils.GetE2EImage(imageutils.Agnhost)}},
					RestartPolicy:                v1.RestartPolicyNever,
					ServiceAccountName:           tc.ServiceAccountName,
					AutomountServiceAccountToken: tc.AutomountPodSpec,
				},
			}
			createdPod, err := f.ClientSet.CoreV1().Pods(f.Namespace.Name).Create(ctx, pod, metav1.CreateOptions{})
			framework.ExpectNoError(err)
			framework.Logf("created pod %s", tc.PodName)

			hasServiceAccountTokenVolume := false
			for _, c := range createdPod.Spec.Containers {
				for _, vm := range c.VolumeMounts {
					if vm.MountPath == serviceaccount.DefaultAPITokenMountPath {
						hasServiceAccountTokenVolume = true
					}
				}
			}

			if hasServiceAccountTokenVolume != tc.ExpectTokenVolume {
				framework.Failf("%s: expected volume=%v, got %v (%#v)", tc.PodName, tc.ExpectTokenVolume, hasServiceAccountTokenVolume, createdPod)
			} else {
				framework.Logf("pod %s service account token volume mount: %v", tc.PodName, hasServiceAccountTokenVolume)
			}
		}
	})

	/*
	  Release : v1.20
	  Testname: TokenRequestProjection should mount a projected volume with token using TokenRequest API.
	  Description: 确保 projected service account token 被挂载。
	*/
	framework.ConformanceIt("should mount projected service account token", func(ctx context.Context) {

		var (
			podName         = "test-pod-" + string(uuid.NewUUID())
			volumeName      = "test-volume"
			volumeMountPath = "/test-volume"
			tokenVolumePath = "/test-volume/token"
		)

		volumes := []v1.Volume{
			{
				Name: volumeName,
				VolumeSource: v1.VolumeSource{
					Projected: &v1.ProjectedVolumeSource{
						Sources: []v1.VolumeProjection{
							{
								ServiceAccountToken: &v1.ServiceAccountTokenProjection{
									Path:              "token",
									ExpirationSeconds: ptr.To[int64](60 * 60),
								},
							},
						},
					},
				},
			},
		}
		volumeMounts := []v1.VolumeMount{
			{
				Name:      volumeName,
				MountPath: volumeMountPath,
				ReadOnly:  true,
			},
		}
		mounttestArgs := []string{
			"mounttest",
			fmt.Sprintf("--file_content=%v", tokenVolumePath),
		}

		pod := e2epod.NewAgnhostPod(f.Namespace.Name, podName, volumes, volumeMounts, nil, mounttestArgs...)
		pod.Spec.RestartPolicy = v1.RestartPolicyNever

		output := []string{
			fmt.Sprintf("content of file \"%v\": %s", tokenVolumePath, `[A-Za-z0-9-_=]+\.[A-Za-z0-9-_=]+\.?[A-Za-z0-9-_.+/=]*`),
		}

		e2eoutput.TestContainerOutputRegexp(ctx, f, "service account token: ", pod, 0, output)
	})

	/*
	   Testname: Projected service account token file ownership and permission.
	   Description: 确保 Projected Service Account Token 挂载时文件属主和权限正确。这里测试以下场景。
	   1. 设置 RunAsUser，
	   2. 设置 FsGroup，
	   3. 同时设置 RunAsUser 和 FsGroup，
	   4. 默认情况，RunAsUser 和 FsGroup 都不设置，

	   容器 MUST 验证 projected service account token 可读，且文件模式（包括属主和权限）正确。
	*/
	f.It("should set ownership and permission when RunAsUser or FsGroup is present [LinuxOnly]", func(ctx context.Context) {
		e2eskipper.SkipIfNodeOSDistroIs("windows")

		var (
			podName         = "test-pod-" + string(uuid.NewUUID())
			volumeName      = "test-volume"
			volumeMountPath = "/test-volume"
			tokenVolumePath = "/test-volume/token"
		)

		volumes := []v1.Volume{
			{
				Name: volumeName,
				VolumeSource: v1.VolumeSource{
					Projected: &v1.ProjectedVolumeSource{
						Sources: []v1.VolumeProjection{
							{
								ServiceAccountToken: &v1.ServiceAccountTokenProjection{
									Path:              "token",
									ExpirationSeconds: ptr.To[int64](60 * 60),
								},
							},
						},
					},
				},
			},
		}
		volumeMounts := []v1.VolumeMount{
			{
				Name:      volumeName,
				MountPath: volumeMountPath,
				ReadOnly:  true,
			},
		}
		mounttestArgs := []string{
			"mounttest",
			fmt.Sprintf("--file_perm=%v", tokenVolumePath),
			fmt.Sprintf("--file_owner=%v", tokenVolumePath),
			fmt.Sprintf("--file_content=%v", tokenVolumePath),
		}

		pod := e2epod.NewAgnhostPod(f.Namespace.Name, podName, volumes, volumeMounts, nil, mounttestArgs...)
		pod.Spec.RestartPolicy = v1.RestartPolicyNever

		testcases := []struct {
			runAsUser bool
			fsGroup   bool
			wantPerm  string
			wantUID   int64
			wantGID   int64
		}{
			{
				runAsUser: true,
				wantPerm:  "-rw-------",
				wantUID:   1000,
				wantGID:   0,
			},
			{
				fsGroup:  true,
				wantPerm: "-rw-r-----",
				wantUID:  0,
				wantGID:  10000,
			},
			{
				runAsUser: true,
				fsGroup:   true,
				wantPerm:  "-rw-r-----",
				wantUID:   1000,
				wantGID:   10000,
			},
			{
				wantPerm: "-rw-r--r--",
				wantUID:  0,
				wantGID:  0,
			},
		}

		for _, tc := range testcases {
			pod.Spec.SecurityContext = &v1.PodSecurityContext{}
			if tc.runAsUser {
				pod.Spec.SecurityContext.RunAsUser = &tc.wantUID
			}
			if tc.fsGroup {
				pod.Spec.SecurityContext.FSGroup = &tc.wantGID
			}

			output := []string{
				fmt.Sprintf("perms of file \"%v\": %s", tokenVolumePath, tc.wantPerm),
				fmt.Sprintf("content of file \"%v\": %s", tokenVolumePath, `[A-Za-z0-9-_=]+\.[A-Za-z0-9-_=]+\.?[A-Za-z0-9-_.+/=]*`),
				fmt.Sprintf("owner UID of \"%v\": %d", tokenVolumePath, tc.wantUID),
				fmt.Sprintf("owner GID of \"%v\": %d", tokenVolumePath, tc.wantGID),
			}
			e2eoutput.TestContainerOutputRegexp(ctx, f, "service account token: ", pod, 0, output)
		}
	})

	f.It("should set ownership when DefaultUser or User is present [LinuxOnly]", f.WithFeatureGate(features.AtomicWriteVolumeUserFields), func(ctx context.Context) {
		e2eskipper.SkipIfNodeOSDistroIs("windows")

		var (
			podName         = "test-pod-" + string(uuid.NewUUID())
			volumeName      = "test-volume"
			volumeMountPath = "/test-volume"
			tokenVolumePath = "/test-volume/..data/token"
		)

		volumes := []v1.Volume{
			{
				Name: volumeName,
				VolumeSource: v1.VolumeSource{
					Projected: &v1.ProjectedVolumeSource{
						Sources: []v1.VolumeProjection{
							{
								ServiceAccountToken: &v1.ServiceAccountTokenProjection{
									Path:              "token",
									ExpirationSeconds: ptr.To[int64](60 * 60),
								},
							},
						},
					},
				},
			},
		}
		volumeMounts := []v1.VolumeMount{
			{
				Name:      volumeName,
				MountPath: volumeMountPath,
				ReadOnly:  true,
			},
		}
		mounttestArgs := []string{
			"mounttest",
			fmt.Sprintf("--file_perm=%v", tokenVolumePath),
			fmt.Sprintf("--file_owner=%v", tokenVolumePath),
			fmt.Sprintf("--file_content=%v", tokenVolumePath),
		}

		pod := e2epod.NewAgnhostPod(f.Namespace.Name, podName, volumes, volumeMounts, nil, mounttestArgs...)
		pod.Spec.RestartPolicy = v1.RestartPolicyNever

		testcases := []struct {
			runAsUser   *int64
			runAsGroup  *int64
			fsGroup     *int64
			defaultUser *int64
			itemUser    *int64
			wantPerm    string
			wantUID     int64
			wantGID     int64
		}{
			{
				defaultUser: ptr.To[int64](1001),
				wantPerm:    "-rw-------",
				wantUID:     1001,
				wantGID:     0,
			},
			{
				itemUser: ptr.To[int64](1001),
				wantPerm: "-rw-------",
				wantUID:  1001,
				wantGID:  0,
			},
			{
				defaultUser: ptr.To[int64](1000),
				itemUser:    ptr.To[int64](1001),
				wantPerm:    "-rw-------",
				wantUID:     1001,
				wantGID:     0,
			},
			{
				runAsUser:  ptr.To[int64](1000),
				runAsGroup: ptr.To[int64](1000),
				fsGroup:    ptr.To[int64](1000),
				itemUser:   ptr.To[int64](1001),
				wantPerm:   "-rw-r-----",
				wantUID:    1001,
				wantGID:    1000,
			},
		}

		for _, tc := range testcases {
			pod.Spec.SecurityContext = &v1.PodSecurityContext{}
			if tc.runAsUser != nil {
				pod.Spec.SecurityContext.RunAsUser = tc.runAsUser
			}
			if tc.runAsGroup != nil {
				pod.Spec.SecurityContext.RunAsGroup = tc.runAsGroup
			}
			if tc.fsGroup != nil {
				pod.Spec.SecurityContext.FSGroup = tc.fsGroup
			}
			if tc.defaultUser != nil {
				pod.Spec.Volumes[0].VolumeSource.Projected.DefaultUser = tc.defaultUser
			}
			if tc.itemUser != nil {
				pod.Spec.Volumes[0].VolumeSource.Projected.Sources[0].ServiceAccountToken.User = tc.itemUser
			}

			output := []string{
				fmt.Sprintf("perms of file \"%v\": %s", tokenVolumePath, tc.wantPerm),
				fmt.Sprintf("content of file \"%v\": %s", tokenVolumePath, `[A-Za-z0-9-_=]+\.[A-Za-z0-9-_=]+\.?[A-Za-z0-9-_.+/=]*`),
				fmt.Sprintf("owner UID of \"%v\": %d", tokenVolumePath, tc.wantUID),
				fmt.Sprintf("owner GID of \"%v\": %d", tokenVolumePath, tc.wantGID),
			}
			e2eoutput.TestContainerOutputRegexp(ctx, f, "service account token: ", pod, 0, output)
		}
	})

	f.It("should support InClusterConfig with token rotation", f.WithSlow(), func(ctx context.Context) {
		tenMin := int64(10 * 60)
		pod := &v1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: "inclusterclient"},
			Spec: v1.PodSpec{
				Containers: []v1.Container{{
					Name:  "inclusterclient",
					Image: imageutils.GetE2EImage(imageutils.Agnhost),
					Args:  []string{"inclusterclient"},
					VolumeMounts: []v1.VolumeMount{{
						MountPath: "/var/run/secrets/kubernetes.io/serviceaccount",
						Name:      "kube-api-access-e2e",
						ReadOnly:  true,
					}},
				}},
				RestartPolicy:      v1.RestartPolicyNever,
				ServiceAccountName: "default",
				Volumes: []v1.Volume{{
					Name: "kube-api-access-e2e",
					VolumeSource: v1.VolumeSource{
						Projected: &v1.ProjectedVolumeSource{
							Sources: []v1.VolumeProjection{
								{
									ServiceAccountToken: &v1.ServiceAccountTokenProjection{
										Path:              "token",
										ExpirationSeconds: &tenMin,
									},
								},
								{
									ConfigMap: &v1.ConfigMapProjection{
										LocalObjectReference: v1.LocalObjectReference{
											Name: "kube-root-ca.crt",
										},
										Items: []v1.KeyToPath{
											{
												Key:  "ca.crt",
												Path: "ca.crt",
											},
										},
									},
								},
								{
									DownwardAPI: &v1.DownwardAPIProjection{
										Items: []v1.DownwardAPIVolumeFile{
											{
												Path: "namespace",
												FieldRef: &v1.ObjectFieldSelector{
													APIVersion: "v1",
													FieldPath:  "metadata.namespace",
												},
											},
										},
									},
								},
							},
						},
					},
				}},
			},
		}
		pod, err := f.ClientSet.CoreV1().Pods(f.Namespace.Name).Create(ctx, pod, metav1.CreateOptions{})
		framework.ExpectNoError(err)

		framework.Logf("created pod")
		framework.ExpectNoError(e2epod.WaitTimeoutForPodReadyInNamespace(ctx, f.ClientSet, pod.Name, f.Namespace.Name, time.Minute))

		framework.Logf("pod is ready")

		var logs string
		if err := wait.Poll(1*time.Minute, 20*time.Minute, func() (done bool, err error) {
			framework.Logf("polling logs")
			logs, err = e2epod.GetPodLogs(ctx, f.ClientSet, f.Namespace.Name, "inclusterclient", "inclusterclient")
			if err != nil {
				framework.Logf("Error pulling logs: %v", err)
				return false, nil
			}
			tokenCount, err := ParseInClusterClientLogs(logs)
			if err != nil {
				return false, fmt.Errorf("inclusterclient reported an error: %w", err)
			}
			if tokenCount < 2 {
				framework.Logf("Retrying. Still waiting to see more unique tokens: got=%d, want=2", tokenCount)
				return false, nil
			}
			return true, nil
		}); err != nil {
			framework.Failf("Unexpected error: %v\n%s", err, logs)
		}
	})

	/*
	   Release: v1.21
	   Testname: OIDC Discovery (ServiceAccountIssuerDiscovery)
	   Description: 确保 kube-apiserver 提供正确的 OIDC discovery
	   endpoint：部署一个 Pod，用它自身的 token 对这些 endpoint 进行验证。
	*/
	framework.ConformanceIt("ServiceAccountIssuerDiscovery should support OIDC discovery of service account issuer", func(ctx context.Context) {

		// 允许测试 Pod 访问 OIDC discovery 的非资源 URL。
		// 该 role 应已作为 RBAC bootstrap policy 的一部分自动创建，
		// 但 role binding 不会。如果 RBAC 被禁用，
		// 我们跳过创建 binding。测试结束后
		// 也会确保清理该 binding。
		const clusterRoleName = "system:service-account-issuer-discovery"
		crbName := fmt.Sprintf("%s-%s", f.Namespace.Name, clusterRoleName)
		if crb, err := f.ClientSet.RbacV1().ClusterRoleBindings().Create(
			ctx,
			&rbacv1.ClusterRoleBinding{
				ObjectMeta: metav1.ObjectMeta{
					Name: crbName,
				},
				Subjects: []rbacv1.Subject{
					{
						Kind:      rbacv1.ServiceAccountKind,
						APIGroup:  "",
						Name:      "default",
						Namespace: f.Namespace.Name,
					},
				},
				RoleRef: rbacv1.RoleRef{
					Name:     clusterRoleName,
					APIGroup: rbacv1.GroupName,
					Kind:     "ClusterRole",
				},
			},
			metav1.CreateOptions{}); err != nil {
			// 容忍 RBAC 未启用
			framework.Logf("error granting ClusterRoleBinding %s: %v", crbName, err)
		} else {
			defer func() {
				framework.ExpectNoError(
					f.ClientSet.RbacV1().ClusterRoleBindings().Delete(
						ctx,
						crb.Name, metav1.DeleteOptions{}))
			}()
		}

		// 创建带 token 的 Pod。
		tokenPath := "/var/run/secrets/tokens"
		tokenName := "sa-token"
		audience := "oidc-discovery-test"
		tenMin := int64(10 * 60)

		pod := &v1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: "oidc-discovery-validator"},
			Spec: v1.PodSpec{
				Containers: []v1.Container{{
					Name:  "oidc-discovery-validator",
					Image: imageutils.GetE2EImage(imageutils.Agnhost),
					Args: []string{
						"test-service-account-issuer-discovery",
						"--token-path", path.Join(tokenPath, tokenName),
						"--audience", audience,
					},
					VolumeMounts: []v1.VolumeMount{{
						MountPath: tokenPath,
						Name:      tokenName,
						ReadOnly:  true,
					}},
				}},
				RestartPolicy:      v1.RestartPolicyNever,
				ServiceAccountName: "default",
				Volumes: []v1.Volume{{
					Name: tokenName,
					VolumeSource: v1.VolumeSource{
						Projected: &v1.ProjectedVolumeSource{
							Sources: []v1.VolumeProjection{
								{
									ServiceAccountToken: &v1.ServiceAccountTokenProjection{
										Path:              tokenName,
										ExpirationSeconds: &tenMin,
										Audience:          audience,
									},
								},
							},
						},
					},
				}},
			},
		}
		pod, err := f.ClientSet.CoreV1().Pods(f.Namespace.Name).Create(ctx, pod, metav1.CreateOptions{})
		framework.ExpectNoError(err)

		framework.Logf("created pod")
		podErr := e2epod.WaitForPodSuccessInNamespace(ctx, f.ClientSet, pod.Name, f.Namespace.Name)

		// 在调用 ExpectNoError 之前先取日志，便于排查错误。
		var logs string
		if err := wait.Poll(30*time.Second, 2*time.Minute, func() (done bool, err error) {
			framework.Logf("polling logs")
			logs, err = e2epod.GetPodLogs(ctx, f.ClientSet, f.Namespace.Name, pod.Name, pod.Spec.Containers[0].Name)
			if err != nil {
				framework.Logf("Error pulling logs: %v", err)
				return false, nil
			}
			return true, nil
		}); err != nil {
			framework.Failf("Unexpected error getting pod logs: %v\n%s", err, logs)
		} else {
			framework.Logf("Pod logs: \n%v", logs)
		}

		framework.ExpectNoError(podErr)
		framework.Logf("completed pod")
	})

	/*
		   Release: v1.19
		   Testname: ServiceAccount lifecycle test
		   Description: 创建带静态 Label 的 ServiceAccount，MUST 如 watch 事件所示被添加。
					给 ServiceAccount 打补丁 MUST 返回其新属性。
					列出 ServiceAccounts MUST 返回补丁后的测试 ServiceAccount。
					ServiceAccount 将被删除，MUST 能找到删除的 watch 事件。
	*/
	framework.ConformanceIt("should run through the lifecycle of a ServiceAccount", func(ctx context.Context) {
		testNamespaceName := f.Namespace.Name
		testServiceAccountName := "testserviceaccount"
		testServiceAccountStaticLabels := map[string]string{"test-serviceaccount-static": "true"}
		testServiceAccountStaticLabelsFlat := "test-serviceaccount-static=true"

		ginkgo.By("creating a ServiceAccount")
		testServiceAccount := v1.ServiceAccount{
			ObjectMeta: metav1.ObjectMeta{
				Name:   testServiceAccountName,
				Labels: testServiceAccountStaticLabels,
			},
		}
		createdServiceAccount, err := f.ClientSet.CoreV1().ServiceAccounts(testNamespaceName).Create(ctx, &testServiceAccount, metav1.CreateOptions{})
		framework.ExpectNoError(err, "failed to create a ServiceAccount")
		gomega.Expect(createdServiceAccount).To(apimachineryutils.HaveValidResourceVersion())

		getServiceAccount, err := f.ClientSet.CoreV1().ServiceAccounts(testNamespaceName).Get(ctx, testServiceAccountName, metav1.GetOptions{})
		framework.ExpectNoError(err, "failed to fetch the created ServiceAccount")
		gomega.Expect(createdServiceAccount.UID).To(gomega.Equal(getServiceAccount.UID))

		ginkgo.By("watching for the ServiceAccount to be added")
		resourceWatchTimeoutSeconds := int64(180)
		resourceWatch, err := f.ClientSet.CoreV1().ServiceAccounts(testNamespaceName).Watch(ctx, metav1.ListOptions{LabelSelector: testServiceAccountStaticLabelsFlat, TimeoutSeconds: &resourceWatchTimeoutSeconds})
		if err != nil {
			fmt.Println(err, "failed to setup watch on newly created ServiceAccount")
			return
		}

		resourceWatchChan := resourceWatch.ResultChan()
		eventFound := false
		for watchEvent := range resourceWatchChan {
			if watchEvent.Type == watch.Added {
				eventFound = true
				break
			}
		}
		if !eventFound {
			framework.Failf("failed to find %v event", watch.Added)
		}
		ginkgo.By("patching the ServiceAccount")
		boolFalse := false
		testServiceAccountPatchData, err := json.Marshal(v1.ServiceAccount{
			AutomountServiceAccountToken: &boolFalse,
		})
		framework.ExpectNoError(err, "failed to marshal JSON patch for the ServiceAccount")
		patchedServiceAccount, err := f.ClientSet.CoreV1().ServiceAccounts(testNamespaceName).Patch(ctx, testServiceAccountName, types.StrategicMergePatchType, []byte(testServiceAccountPatchData), metav1.PatchOptions{})
		framework.ExpectNoError(err, "failed to patch the ServiceAccount")
		gomega.Expect(resourceversion.CompareResourceVersion(createdServiceAccount.ResourceVersion, patchedServiceAccount.ResourceVersion)).To(gomega.BeNumerically("==", -1), "patched object should have a larger resource version")

		eventFound = false
		for watchEvent := range resourceWatchChan {
			if watchEvent.Type == watch.Modified {
				eventFound = true
				break
			}
		}
		if !eventFound {
			framework.Failf("failed to find %v event", watch.Modified)
		}
		ginkgo.By("finding ServiceAccount in list of all ServiceAccounts (by LabelSelector)")
		serviceAccountList, err := f.ClientSet.CoreV1().ServiceAccounts("").List(ctx, metav1.ListOptions{LabelSelector: testServiceAccountStaticLabelsFlat})
		framework.ExpectNoError(err, "failed to list ServiceAccounts by LabelSelector")
		foundServiceAccount := false
		for _, serviceAccountItem := range serviceAccountList.Items {
			if serviceAccountItem.ObjectMeta.Name == testServiceAccountName && serviceAccountItem.ObjectMeta.Namespace == testNamespaceName && *serviceAccountItem.AutomountServiceAccountToken == boolFalse {
				foundServiceAccount = true
				break
			}
		}
		if !foundServiceAccount {
			framework.Fail("failed to find the created ServiceAccount")
		}
		ginkgo.By("deleting the ServiceAccount")
		err = f.ClientSet.CoreV1().ServiceAccounts(testNamespaceName).DeleteCollection(ctx, metav1.DeleteOptions{}, metav1.ListOptions{})
		framework.ExpectNoError(err, "failed to delete the ServiceAccount by Collection")
		eventFound = false
		for watchEvent := range resourceWatchChan {
			if watchEvent.Type == watch.Deleted {
				eventFound = true
				break
			}
		}
		if !eventFound {
			framework.Failf("failed to find %v event", watch.Deleted)
		}
	})

	/*
		Release: v1.21
		Testname: RootCA ConfigMap test
		Description: 确保每个命名空间都存在一个用于 root CA 证书的 ConfigMap。
			1. 自动创建
			2. 删除后重建
			3. 修改后调和
	*/
	framework.ConformanceIt("should guarantee kube-root-ca.crt exist in any namespace", func(ctx context.Context) {
		framework.ExpectNoError(wait.PollImmediate(500*time.Millisecond, wait.ForeverTestTimeout, func() (bool, error) {
			_, err := f.ClientSet.CoreV1().ConfigMaps(f.Namespace.Name).Get(ctx, rootCAConfigMapName, metav1.GetOptions{})
			if err == nil {
				return true, nil
			}
			if apierrors.IsNotFound(err) {
				ginkgo.By("root ca configmap not found, retrying")
				return false, nil
			}
			return false, err
		}))
		framework.Logf("Got root ca configmap in namespace %q", f.Namespace.Name)

		framework.ExpectNoError(f.ClientSet.CoreV1().ConfigMaps(f.Namespace.Name).Delete(ctx, rootCAConfigMapName, metav1.DeleteOptions{GracePeriodSeconds: ptr.To[int64](0)}))
		framework.Logf("Deleted root ca configmap in namespace %q", f.Namespace.Name)

		framework.ExpectNoError(wait.Poll(500*time.Millisecond, wait.ForeverTestTimeout, func() (bool, error) {
			ginkgo.By("waiting for a new root ca configmap created")
			_, err := f.ClientSet.CoreV1().ConfigMaps(f.Namespace.Name).Get(ctx, rootCAConfigMapName, metav1.GetOptions{})
			if err == nil {
				return true, nil
			}
			if apierrors.IsNotFound(err) {
				ginkgo.By("root ca configmap not found, retrying")
				return false, nil
			}
			return false, err
		}))
		framework.Logf("Recreated root ca configmap in namespace %q", f.Namespace.Name)

		_, err := f.ClientSet.CoreV1().ConfigMaps(f.Namespace.Name).Update(ctx, &v1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Name: rootCAConfigMapName,
			},
			Data: map[string]string{
				"ca.crt": "",
			},
		}, metav1.UpdateOptions{})
		framework.ExpectNoError(err)
		framework.Logf("Updated root ca configmap in namespace %q", f.Namespace.Name)

		framework.ExpectNoError(wait.Poll(500*time.Millisecond, wait.ForeverTestTimeout, func() (bool, error) {
			ginkgo.By("waiting for the root ca configmap reconciled")
			cm, err := f.ClientSet.CoreV1().ConfigMaps(f.Namespace.Name).Get(ctx, rootCAConfigMapName, metav1.GetOptions{})
			if err != nil {
				if apierrors.IsNotFound(err) {
					ginkgo.By("root ca configmap not found, retrying")
					return false, nil
				}
				return false, err
			}
			if value, ok := cm.Data["ca.crt"]; !ok || value == "" {
				ginkgo.By("root ca configmap is not reconciled yet, retrying")
				return false, nil
			}
			return true, nil
		}))
		framework.Logf("Reconciled root ca configmap in namespace %q", f.Namespace.Name)
	})

	/*
		Release: v1.26
		Testname: ServiceAccount, update a ServiceAccount
		Description: 创建 ServiceAccount 必须成功。更新
		ServiceAccount 必须成功，且字段必须等于
		新值。
	*/
	framework.ConformanceIt("should update a ServiceAccount", func(ctx context.Context) {
		saClient := f.ClientSet.CoreV1().ServiceAccounts(f.Namespace.Name)
		saName := "e2e-sa-" + utilrand.String(5)

		initialServiceAccount := &v1.ServiceAccount{
			ObjectMeta: metav1.ObjectMeta{
				Name: saName,
			},
			AutomountServiceAccountToken: ptr.To(false),
		}

		ginkgo.By(fmt.Sprintf("Creating ServiceAccount %q ", saName))
		createdServiceAccount, err := saClient.Create(ctx, initialServiceAccount, metav1.CreateOptions{})
		framework.ExpectNoError(err)
		gomega.Expect(createdServiceAccount.AutomountServiceAccountToken).To(gomega.Equal(ptr.To(false)), "Failed to set AutomountServiceAccountToken")
		framework.Logf("AutomountServiceAccountToken: %v", *createdServiceAccount.AutomountServiceAccountToken)

		ginkgo.By(fmt.Sprintf("Updating ServiceAccount %q ", saName))
		var updatedServiceAccount *v1.ServiceAccount

		err = retry.RetryOnConflict(retry.DefaultRetry, func() error {
			updateServiceAccount, err := saClient.Get(ctx, saName, metav1.GetOptions{})
			framework.ExpectNoError(err, "Unable to get ServiceAccount %q", saName)
			updateServiceAccount.AutomountServiceAccountToken = ptr.To(true)
			updatedServiceAccount, err = saClient.Update(ctx, updateServiceAccount, metav1.UpdateOptions{})
			return err
		})
		framework.ExpectNoError(err, "Failed to update ServiceAccount")
		gomega.Expect(updatedServiceAccount.AutomountServiceAccountToken).To(gomega.Equal(ptr.To(true)), "Failed to set AutomountServiceAccountToken")
		framework.Logf("AutomountServiceAccountToken: %v", *updatedServiceAccount.AutomountServiceAccountToken)
	})

	/*
		Release: v1.32
		Testname: ServiceAccount, create and review token
		Description: 创建 ServiceAccount 必须成功。创建 ServiceAccountToken
		必须成功。token 不能为空。创建 TokenReview 必须成功。
		TokenReview 必须认证成功且无任何错误。
	*/
	framework.ConformanceIt("should create a serviceAccountToken and ensure a successful TokenReview", func(ctx context.Context) {
		ns := f.Namespace.Name
		saClient := f.ClientSet.CoreV1().ServiceAccounts(ns)
		saName := "e2e-sa-" + utilrand.String(5)

		ginkgo.By(fmt.Sprintf("Creating a Serviceaccount %q in namespace %q", saName, ns))
		_, err := f.ClientSet.CoreV1().ServiceAccounts(ns).Create(context.TODO(), &v1.ServiceAccount{
			ObjectMeta: metav1.ObjectMeta{
				Name: saName,
			},
		}, metav1.CreateOptions{})
		framework.ExpectNoError(err, "Unable to create serviceaccount %q", saName)

		ginkgo.By(fmt.Sprintf("Creating a ServiceaccountToken %q in namespace %q", saName, ns))
		request := &authenticationv1.TokenRequest{}
		response, err := saClient.CreateToken(context.TODO(), saName, request, metav1.CreateOptions{})
		framework.ExpectNoError(err, "Unable to create serviceAccountToken")
		gomega.Expect(response.Status.Token).ToNot(gomega.BeEmpty(), "confirm that a Token has been created")

		ginkgo.By(fmt.Sprintf("Creating a TokenReview for %q in namespace %q", response.Name, ns))
		tokenReview := &authenticationv1.TokenReview{Spec: authenticationv1.TokenReviewSpec{Token: response.Status.Token}}
		tokenReview, err = f.ClientSet.AuthenticationV1().TokenReviews().Create(ctx, tokenReview, metav1.CreateOptions{})
		framework.ExpectNoError(err, "failed to create a TokenReview")
		gomega.Expect(tokenReview.Status.Authenticated).To(gomega.BeTrueBecause("expect that the TokenReview is authenticated"))
		gomega.Expect(tokenReview.Status.Error).To(gomega.BeEmpty(), "confirm that there are no TokenReview errors")
	})
})

var reportLogsParser = regexp.MustCompile("([a-zA-Z0-9-_]*)=([a-zA-Z0-9-_]*)$")

// ParseInClusterClientLogs 解析使用 inclusterclient 的 Pod 日志。
func ParseInClusterClientLogs(logs string) (int, error) {
	seenTokens := map[string]struct{}{}

	lines := strings.Split(logs, "\n")
	for _, line := range lines {
		parts := reportLogsParser.FindStringSubmatch(line)
		if len(parts) != 3 {
			continue
		}

		key, value := parts[1], parts[2]
		switch key {
		case "authz_header":
			if value == "<empty>" {
				return 0, fmt.Errorf("saw empty Authorization header")
			}
			seenTokens[value] = struct{}{}
		case "status":
			if value == "failed" {
				return 0, fmt.Errorf("saw status=failed")
			}
		}
	}

	return len(seenTokens), nil
}
