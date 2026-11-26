/*
Copyright 2024 The Kubernetes Authors.

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
	"fmt"
	"strings"
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"

	appsv1 "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/apimachinery/pkg/util/uuid"
	certutil "k8s.io/client-go/util/cert"
	"k8s.io/kubernetes/pkg/features"
	"k8s.io/kubernetes/test/e2e/framework"
	e2epod "k8s.io/kubernetes/test/e2e/framework/pod"
	e2epodoutput "k8s.io/kubernetes/test/e2e/framework/pod/output"
	testutils "k8s.io/kubernetes/test/utils"
	"k8s.io/kubernetes/test/utils/hermeticpodcertificatesigner"
	imageutils "k8s.io/kubernetes/test/utils/image" // 导入 imageutils
	admissionapi "k8s.io/pod-security-admission/api"
	admissiontest "k8s.io/pod-security-admission/test"
	"k8s.io/utils/clock"
	"k8s.io/utils/ptr"
)

var _ = SIGDescribe("Projected PodCertificate",
	framework.WithFeatureGate(features.PodCertificateRequest),
	framework.WithFeatureGate(features.ClusterTrustBundle),
	framework.WithFeatureGate(features.ClusterTrustBundleProjection),
	func() {
		f := framework.NewDefaultFramework("projected-podcertificate")
		f.NamespacePodSecurityLevel = admissionapi.LevelBaseline

		var spiffeSignerName string

		ginkgo.BeforeEach(func(ctx context.Context) {
			spiffeSignerName = "e2e.example.com/" + f.UniqueName

			ginkgo.By("Starting in-process pod certificate signer...")
			signerCtx, cancelSigner := context.WithCancel(context.Background())
			ginkgo.DeferCleanup(func(ctx context.Context) {
				ginkgo.By("Stopping in-process pod certificate signer...")
				cancelSigner()
			})

			var err error
			caKeys, caCerts, err := hermeticpodcertificatesigner.GenerateCAHierarchy(1) // 只生成一次 CA
			if err != nil {
				framework.Failf("failed to generate CA for signer: %v", err)
			}

			signer := hermeticpodcertificatesigner.New(clock.RealClock{}, spiffeSignerName, framework.TestContext.ClusterDNSDomain, caKeys, caCerts, f.ClientSet)
			go signer.Run(signerCtx)
		})

		/*
			Release: v1.37
			Testname: Projected PodCertificate mTLS Connection
			Description:
			配置了 projected podCertificate 卷及相应 ClusterTrustBundle 的 Pod，必须投射出有效的叶子证书链和私钥。
			同一 CA 下持有此类证书和私钥的两个 Pod，必须能建立双向认证的 TLS（mTLS）连接。
		*/
		framework.ConformanceIt("should allow server and client pods to establish an mTLS connection [MinimumKubeletVersion:1.37]", func(ctx context.Context) {
			namespace := f.Namespace.Name
			ginkgo.By("Using namespace: " + namespace)

			securityContext := generateContainerSecurityContext()
			serverDeployment, serverService := createServerObjects(namespace, spiffeSignerName, securityContext)
			ginkgo.By("Creating server deployment...")
			_, err := f.ClientSet.AppsV1().Deployments(namespace).Create(ctx, serverDeployment, metav1.CreateOptions{})
			if err != nil {
				framework.Failf("failed to create server deployment: %v", err)
			}

			ginkgo.By("Creating server service...")
			_, err = f.ClientSet.CoreV1().Services(namespace).Create(ctx, serverService, metav1.CreateOptions{})
			if err != nil {
				framework.Failf("failed to create server service: %v", err)
			}

			clientDeployment := createClientObjects(namespace, spiffeSignerName, securityContext)
			ginkgo.By("Creating client deployment...")
			_, err = f.ClientSet.AppsV1().Deployments(namespace).Create(ctx, clientDeployment, metav1.CreateOptions{})
			if err != nil {
				framework.Failf("failed to create client deployment: %v", err)
			}

			ginkgo.By("Waiting for server deployment to complete...")
			framework.ExpectNoError(testutils.WaitForDeploymentComplete(f.ClientSet, serverDeployment, framework.Logf, time.Second, time.Minute))

			ginkgo.By("Waiting for client deployment to complete...")
			framework.ExpectNoError(testutils.WaitForDeploymentComplete(f.ClientSet, clientDeployment, framework.Logf, time.Second, time.Minute))

			ginkgo.By("Waiting for mTLS connection to be built...")

			// mtlsclient 现在会记录成功的轮询，据此检查对应模式。
			gomega.Eventually(ctx, func(ctx context.Context) (string, error) {
				clientLabels := labels.Set{"app": "client"}
				podList, listErr := f.ClientSet.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{LabelSelector: clientLabels.String()})
				if listErr != nil || len(podList.Items) == 0 {
					return "", fmt.Errorf("failed to get client pods: %w", err)
				}
				return e2epod.GetPodLogs(ctx, f.ClientSet, namespace, podList.Items[0].Name, "client")
			}, 30*time.Second, 2*time.Second).Should(gomega.MatchRegexp(`Got response body: Client Identity: spiffe://`),
				"client logs did not contain expected success message pattern")
		})

		/*
			Release: v1.37
			Testname: Projected PodCertificate UserAnnotations
			Description:
			podCertificate 卷投射必须支持把 Pod spec 中未经校验的用户注解传递给 signer。
			signer 必须能接收并处理这些注解（例如覆盖生成证书 SAN 中的 SPIFFE URI 路径）。
		*/
		framework.ConformanceIt("should honor UserAnnotations for SPIFFE URI path [MinimumKubeletVersion:1.37]", func(ctx context.Context) {
			namespace := f.Namespace.Name
			ginkgo.By("Using namespace: " + namespace)

			// 用 customPath 覆盖 spiffeID 中的路径。
			customPath := "/custom-workload"
			userAnnotations := map[string]string{
				"spiffe/path-overriding": customPath, // 与 signer 支持的 key 保持一致
			}
			inspectorPod := createInspectorPod(namespace, "path-override-pod", createPodCertificateVolumes(userAnnotations, nil, spiffeSignerName))

			ginkgo.By("Creating inspector pod with UserAnnotations...")
			_, err := f.ClientSet.CoreV1().Pods(namespace).Create(ctx, inspectorPod, metav1.CreateOptions{})
			if err != nil {
				framework.Failf("failed to create inspector pod: %v", err)
			}

			ginkgo.By("Waiting for inspector pod to be running...")
			err = e2epod.WaitForPodRunningInNamespace(ctx, f.ClientSet, inspectorPod)
			if err != nil {
				framework.Failf("inspector pod failed to start: %v", err)
			}

			ginkgo.By("Fetching and parsing certificate from pod logs...")
			logs, err := e2epod.GetPodLogs(ctx, f.ClientSet, namespace, inspectorPod.Name, inspectorPod.Spec.Containers[0].Name)
			if err != nil {
				framework.Failf("failed to get pod logs: %v", err)
			}
			certs, err := certutil.ParseCertsPEM([]byte(logs))
			if err != nil {
				framework.Failf("failed to parse certificate chain from PEM block: %v", err)
			}
			gomega.Expect(certs).ToNot(gomega.BeEmpty(), "should parse at least one certificate from the bundle")

			cert := certs[0]
			foundSPIFFEURI := false
			for _, uri := range cert.URIs {
				if uri.Scheme == "spiffe" {
					foundSPIFFEURI = true
					ginkgo.By("Found SPIFFE URI: " + uri.String())
					gomega.Expect(uri.Path).To(gomega.Equal(customPath), "SPIFFE URI path should match the custom path from UserAnnotations")
					break
				}
			}
			gomega.Expect(foundSPIFFEURI).To(gomega.BeTrueBecause("should find a SPIFFE URI in the certificate's SANs"))
		})

		ginkgo.Describe("MaxExpirationSeconds validations", func() {

			/*
				Release: v1.37
				Testname: Projected PodCertificate Default Expiration
				Description:
				如果 projected volume spec 省略 MaxExpirationSeconds，签发的证书默认有效期必须为 24 小时。
			*/
			framework.ConformanceIt("should issue certificate with default life time (24h) when MaxExpirationSeconds is not set [MinimumKubeletVersion:1.37]", func(ctx context.Context) {
				namespace := f.Namespace.Name
				ginkgo.By("Using namespace: " + namespace)

				// 创建未指定 MaxExpirationSeconds 的 Pod
				testPod := createInspectorPod(namespace, "default-duration-pod", createPodCertificateVolumes(nil, nil, spiffeSignerName))
				ginkgo.By("Creating pod without MaxExpirationSeconds...")
				createdPod, err := f.ClientSet.CoreV1().Pods(namespace).Create(ctx, testPod, metav1.CreateOptions{})
				if err != nil {
					framework.Failf("failed to create pod: %v", err)
				}

				ginkgo.By("Waiting for pod to be running...")
				err = e2epod.WaitForPodRunningInNamespace(ctx, f.ClientSet, createdPod)
				if err != nil {
					framework.Failf("Pod failed to start: %v", err)
				}

				ginkgo.By("Fetching certificate and verifying duration...")
				logs, err := e2epod.GetPodLogs(ctx, f.ClientSet, namespace, createdPod.Name, createdPod.Spec.Containers[0].Name)
				if err != nil {
					framework.Failf("failed to get pod logs: %v", err)
				}
				certs, err := certutil.ParseCertsPEM([]byte(logs))
				if err != nil {
					framework.Failf("failed to parse certificate: %v", err)
				}
				gomega.Expect(certs).ToNot(gomega.BeEmpty())
				cert := certs[0]
				lifeTime := cert.NotAfter.Sub(cert.NotBefore)
				expectedDuration := 24 * time.Hour // signer 代码中的默认值
				ginkgo.By(fmt.Sprintf("Verifying certificate duration %v is close to %v", lifeTime, expectedDuration))
				gomega.Expect(lifeTime).To(gomega.BeNumerically("==", expectedDuration), "Certificate duration should be 24 hours")
			})

			/*
				Release: v1.37
				Testname: Projected PodCertificate Custom Expiration
				Description:
				如果 projected volume spec 指定了 MaxExpirationSeconds（例如 1 小时），签发的证书有效期必须与之匹配。
			*/
			framework.ConformanceIt("should issue certificate with specified duration (1h) when MaxExpirationSeconds is set [MinimumKubeletVersion:1.37]", func(ctx context.Context) {
				namespace := f.Namespace.Name
				ginkgo.By("Using namespace: " + namespace)

				// 创建请求 1 小时有效期的 Pod
				requestedSeconds := int32(3600)
				testPod := createInspectorPod(namespace, "one-hour-duration-pod", createPodCertificateVolumes(nil, &requestedSeconds, spiffeSignerName))
				ginkgo.By("Creating pod requesting 1 hour MaxExpirationSeconds...")
				createdPod, err := f.ClientSet.CoreV1().Pods(namespace).Create(ctx, testPod, metav1.CreateOptions{})
				if err != nil {
					framework.Failf("failed to create pod: %v", err)
				}

				ginkgo.By("Waiting for pod to be running...")
				err = e2epod.WaitForPodRunningInNamespace(ctx, f.ClientSet, createdPod)
				if err != nil {
					framework.Failf("Pod failed to start: %v", err)
				}

				ginkgo.By("Fetching certificate and verifying duration...")
				logs, err := e2epod.GetPodLogs(ctx, f.ClientSet, namespace, createdPod.Name, createdPod.Spec.Containers[0].Name)
				if err != nil {
					framework.Failf("failed to get pod logs: %v", err)
				}
				certs, err := certutil.ParseCertsPEM([]byte(logs))
				if err != nil {
					framework.Failf("failed to parse certificate: %v", err)
				}
				gomega.Expect(certs).ToNot(gomega.BeEmpty())
				cert := certs[0]
				lifeTime := cert.NotAfter.Sub(cert.NotBefore)
				expectedDuration := time.Duration(requestedSeconds) * time.Second
				ginkgo.By(fmt.Sprintf("Verifying certificate duration %v is close to %v", lifeTime, expectedDuration))
				gomega.Expect(lifeTime).To(gomega.BeNumerically("==", expectedDuration), "Certificate duration should be 1 hour")
			})

			/*
				Release: v1.37
				Testname: Projected PodCertificate Expiration Maximum Validation
				Description:
				创建 projected podCertificate 卷且 MaxExpirationSeconds 超过上限 91 天的 Pod，必须在准入阶段被 API server 拒绝。
			*/
			framework.ConformanceIt("should reject pod creation when MaxExpirationSeconds exceeds maximum (91d) [MinimumKubeletVersion:1.37]", func(ctx context.Context) {
				namespace := f.Namespace.Name
				ginkgo.By("Using namespace: " + namespace)

				// 超过 91 天
				tooLongSeconds := int32((91 * 24 * 60 * 60) + 1)
				testPod := createInspectorPod(namespace, "too-long-duration-pod", createPodCertificateVolumes(nil, &tooLongSeconds, spiffeSignerName))
				ginkgo.By("Creating pod requesting >91d MaxExpirationSeconds...")
				_, err := f.ClientSet.CoreV1().Pods(namespace).Create(ctx, testPod, metav1.CreateOptions{})
				if err == nil {
					framework.Fail("Expected pod creation to fail due to too long MaxExpirationSeconds, but it succeeded")
				}
				if !strings.Contains(err.Error(), "if provided, maxExpirationSeconds must be <= 7862400") {
					framework.Fail("Error message does not match the expected")
				}
			})

			/*
				Release: v1.37
				Testname: Projected PodCertificate Expiration Minimum Validation
				Description:
				创建 projected podCertificate 卷且 MaxExpirationSeconds 低于下限 1 小时的 Pod，必须在准入阶段被 API server 拒绝。
			*/
			framework.ConformanceIt("should reject pod creation when MaxExpirationSeconds is less than minimum (1h) [MinimumKubeletVersion:1.37]", func(ctx context.Context) {
				namespace := f.Namespace.Name
				ginkgo.By("Using namespace: " + namespace)

				// 少于 1 小时
				tooShortSeconds := int32(3599)
				testPod := createInspectorPod(namespace, "too-short-duration-pod", createPodCertificateVolumes(nil, &tooShortSeconds, spiffeSignerName))
				ginkgo.By("Creating pod requesting <1h MaxExpirationSeconds...")
				_, err := f.ClientSet.CoreV1().Pods(namespace).Create(ctx, testPod, metav1.CreateOptions{})
				if err == nil {
					framework.Fail("Expected pod creation to fail due to too short MaxExpirationSeconds, but it succeeded")
				}
				if !strings.Contains(err.Error(), "if provided, maxExpirationSeconds must be >= 3600") {
					framework.Fail("Error message does not match the expected")
				}
			})
		})

		ginkgo.It("should issue certificate with user fields [LinuxOnly]", f.WithFeatureGate(features.AtomicWriteVolumeUserFields), func(ctx context.Context) {
			for _, tt := range []struct {
				name           string
				itemUser       *int64
				defaultUser    *int64
				expectedOutput []string
			}{
				{
					name:           "set ownership when DefaultUser is present",
					defaultUser:    ptr.To[int64](1000),
					expectedOutput: []string{"owner UID of \"/run/tls-config/..data/spiffe-cred-bundle.pem\": 1000"},
				},
				{
					name:           "set ownership when User is present",
					itemUser:       ptr.To[int64](1000),
					expectedOutput: []string{"owner UID of \"/run/tls-config/..data/spiffe-cred-bundle.pem\": 1000"},
				},
				{
					name:           "set ownership when DefaultUser and User is present",
					defaultUser:    ptr.To[int64](1001),
					itemUser:       ptr.To[int64](1000),
					expectedOutput: []string{"owner UID of \"/run/tls-config/..data/spiffe-cred-bundle.pem\": 1000"},
				},
			} {
				podName := "pod-" + string(uuid.NewUUID())
				volumes := createPodCertificateVolumes(nil, nil, spiffeSignerName)
				volumeMounts := []v1.VolumeMount{
					{Name: "tls-config", MountPath: "/run/tls-config"},
				}
				mounttestArgs := []string{
					"mounttest",
					"--file_owner=/run/tls-config/..data/spiffe-cred-bundle.pem",
				}
				pod := e2epod.NewAgnhostPod(f.Namespace.Name, podName, volumes, volumeMounts, nil, mounttestArgs...)
				pod.Spec.RestartPolicy = v1.RestartPolicyNever

				if tt.itemUser != nil {
					pod.Spec.Volumes[0].VolumeSource.Projected.Sources[0].PodCertificate.User = tt.itemUser
				}
				if tt.defaultUser != nil {
					pod.Spec.Volumes[0].VolumeSource.Projected.DefaultUser = tt.defaultUser
				}
				e2epodoutput.TestContainerOutputRegexp(ctx, f, "project pod certificate", pod, 0, tt.expectedOutput)
			}
		})
	})

func generateContainerSecurityContext() *v1.SecurityContext {
	desiredPSALevel := admissionapi.LevelRestricted
	desiredVersion := admissionapi.GetAPIVersion()
	minimalPod, err := admissiontest.GetMinimalValidPod(desiredPSALevel, desiredVersion)
	if err != nil {
		framework.Failf("failed to get minimal valid pod: %v", err)
	}
	return minimalPod.Spec.Containers[0].SecurityContext
}

// createServerObjects 为 mTLS server 创建 Deployment 和 Service 对象。
func createServerObjects(namespace string, spiffeSignerName string, securityContext *v1.SecurityContext) (*appsv1.Deployment, *v1.Service) {
	// 容器监听端口使用非特权端口（> 1024），因为容器以 Restricted 安全上下文运行
	// （非 root 且已丢弃 capabilities）。
	const serverPort = 8443

	replicas := int32(1)
	serverLabels := map[string]string{"app": "server"}

	signerNameVar := spiffeSignerName
	serverDeployment := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "server",
			Namespace: namespace,
			Labels:    serverLabels,
		},
		Spec: appsv1.DeploymentSpec{
			Replicas: &replicas,
			Selector: &metav1.LabelSelector{MatchLabels: serverLabels},
			Template: v1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: serverLabels},
				Spec: v1.PodSpec{
					Containers: []v1.Container{
						{
							Name:  "server",
							Image: imageutils.GetE2EImage(imageutils.Agnhost), // 使用 >= 2.59 的 agnhost 镜像
							Args: []string{
								"mtlsserver",
								fmt.Sprintf("--listen=0.0.0.0:%d", serverPort),
								"--server-creds=/run/tls-config/spiffe-cred-bundle.pem",
								"--spiffe-trust-bundle=/run/tls-config/spiffe-trust-bundle.pem",
							},
							ImagePullPolicy: v1.PullAlways,
							SecurityContext: securityContext,
							VolumeMounts:    []v1.VolumeMount{{Name: "tls-config", MountPath: "/run/tls-config"}},
						},
					},
					Volumes: []v1.Volume{{
						Name: "tls-config",
						VolumeSource: v1.VolumeSource{
							Projected: &v1.ProjectedVolumeSource{
								Sources: []v1.VolumeProjection{
									{
										PodCertificate: &v1.PodCertificateProjection{
											SignerName:           spiffeSignerName,
											CredentialBundlePath: "spiffe-cred-bundle.pem",
											KeyType:              "ECDSAP256",
										},
									},
									{
										ClusterTrustBundle: &v1.ClusterTrustBundleProjection{
											SignerName:    &signerNameVar,
											LabelSelector: &metav1.LabelSelector{},
											Path:          "spiffe-trust-bundle.pem",
										},
									},
								},
							},
						},
					}},
					RestartPolicy: v1.RestartPolicyAlways,
				},
			},
		},
	}

	serverService := &v1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: "server", Namespace: namespace},
		Spec: v1.ServiceSpec{
			Type: v1.ServiceTypeClusterIP,
			Ports: []v1.ServicePort{
				{
					Name:       "https",
					Port:       443,
					TargetPort: intstr.FromInt32(serverPort),
				},
			},
			Selector: serverLabels,
		},
	}

	return serverDeployment, serverService
}

// createClientObjects 为 mTLS client 创建 Deployment 对象。
func createClientObjects(namespace string, spiffeSignerName string, securityContext *v1.SecurityContext) *appsv1.Deployment {
	replicas := int32(1)
	clientLabels := map[string]string{"app": "client"}
	// 务必始终包含集群 DNS 域，否则
	// Windows Pod 无法解析主机名。
	fetchURL := fmt.Sprintf("https://server.%s.svc.%s/spiffe-echo", namespace, framework.TestContext.ClusterDNSDomain)
	signerNameVar := spiffeSignerName

	return &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "client",
			Namespace: namespace,
			Labels:    clientLabels,
		},
		Spec: appsv1.DeploymentSpec{
			Replicas: &replicas,
			Selector: &metav1.LabelSelector{MatchLabels: clientLabels},
			Template: v1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: clientLabels},
				Spec: v1.PodSpec{
					RestartPolicy: v1.RestartPolicyAlways,
					Containers: []v1.Container{{
						Name:  "client",
						Image: imageutils.GetE2EImage(imageutils.Agnhost), // 使用 >= 2.59 的 agnhost 镜像
						Args: []string{
							"mtlsclient",
							"--fetch-url=" + fetchURL,
							"--server-trust-bundle=/run/tls-config/spiffe-trust-bundle.pem",
							"--client-cred-bundle=/run/tls-config/spiffe-cred-bundle.pem",
						},
						ImagePullPolicy: v1.PullAlways,
						SecurityContext: securityContext,
						VolumeMounts:    []v1.VolumeMount{{Name: "tls-config", MountPath: "/run/tls-config"}},
					}},
					Volumes: []v1.Volume{{
						Name: "tls-config",
						VolumeSource: v1.VolumeSource{
							Projected: &v1.ProjectedVolumeSource{
								Sources: []v1.VolumeProjection{
									{
										ClusterTrustBundle: &v1.ClusterTrustBundleProjection{
											SignerName:    &signerNameVar,
											LabelSelector: &metav1.LabelSelector{},
											Path:          "spiffe-trust-bundle.pem",
										},
									},
									{
										PodCertificate: &v1.PodCertificateProjection{
											SignerName:           spiffeSignerName,
											CredentialBundlePath: "spiffe-cred-bundle.pem",
											KeyType:              "ECDSAP256",
										},
									},
								},
							},
						},
					}},
				},
			},
		},
	}
}

// createInspectorPod 创建一个用于打印证书并保持运行的 Pod，以便检查。
func createInspectorPod(namespace, podName string, volumes []v1.Volume) *v1.Pod {
	return &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      podName,
			Namespace: namespace,
		},
		Spec: v1.PodSpec{
			Containers: []v1.Container{
				{
					Name:  "inspector",
					Image: imageutils.GetE2EImage(imageutils.Agnhost),
					// 使用 agnhost 基础镜像中自带的标准 shell 命令
					Command: []string{"/bin/sh", "-c", "cat /run/tls-config/spiffe-cred-bundle.pem && sleep infinity"},
					VolumeMounts: []v1.VolumeMount{
						{Name: "tls-config", MountPath: "/run/tls-config"},
					},
				},
			},
			Volumes:       volumes,
			RestartPolicy: v1.RestartPolicyNever,
		},
	}
}

func createPodCertificateVolumes(userAnnotations map[string]string, maxExpirationSeconds *int32, spiffeSignerName string) []v1.Volume {
	return []v1.Volume{
		{
			Name: "tls-config",
			VolumeSource: v1.VolumeSource{
				Projected: &v1.ProjectedVolumeSource{
					Sources: []v1.VolumeProjection{
						{
							PodCertificate: &v1.PodCertificateProjection{
								SignerName:           spiffeSignerName,
								CredentialBundlePath: "spiffe-cred-bundle.pem",
								KeyType:              "ECDSAP256",
								UserAnnotations:      userAnnotations,
								MaxExpirationSeconds: maxExpirationSeconds,
							},
						},
					},
				},
			},
		},
	}
}
