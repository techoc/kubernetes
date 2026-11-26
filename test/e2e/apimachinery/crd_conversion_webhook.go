/*
Copyright 2018 The Kubernetes Authors.

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

package apimachinery

import (
	"context"
	"fmt"
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"

	appsv1 "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/dynamic"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/kubernetes/test/e2e/framework"
	e2edeployment "k8s.io/kubernetes/test/e2e/framework/deployment"
	e2eendpointslice "k8s.io/kubernetes/test/e2e/framework/endpointslice"
	"k8s.io/kubernetes/test/utils/crd"
	imageutils "k8s.io/kubernetes/test/utils/image"
	"k8s.io/kubernetes/test/utils/ktesting/format"
	admissionapi "k8s.io/pod-security-admission/api"
	"k8s.io/utils/ptr"

	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	"k8s.io/apiextensions-apiserver/test/integration"
)

const (
	secretCRDName      = "sample-custom-resource-conversion-webhook-secret"
	deploymentCRDName  = "sample-crd-conversion-webhook-deployment"
	serviceCRDName     = "e2e-test-crd-conversion-webhook"
	roleBindingCRDName = "crd-conversion-webhook-auth-reader"
)

var apiVersions = []apiextensionsv1.CustomResourceDefinitionVersion{
	{
		Name:    "v1",
		Served:  true,
		Storage: true,
		Schema: &apiextensionsv1.CustomResourceValidation{
			OpenAPIV3Schema: &apiextensionsv1.JSONSchemaProps{
				Type: "object",
				Properties: map[string]apiextensionsv1.JSONSchemaProps{
					"hostPort": {Type: "string"},
				},
			},
		},
	},
	{
		Name:    "v2",
		Served:  true,
		Storage: false,
		Schema: &apiextensionsv1.CustomResourceValidation{
			OpenAPIV3Schema: &apiextensionsv1.JSONSchemaProps{
				Type: "object",
				Properties: map[string]apiextensionsv1.JSONSchemaProps{
					"host": {Type: "string"},
					"port": {Type: "string"},
				},
			},
		},
	},
}

var alternativeAPIVersions = []apiextensionsv1.CustomResourceDefinitionVersion{
	{
		Name:    "v1",
		Served:  true,
		Storage: false,
		Schema: &apiextensionsv1.CustomResourceValidation{
			OpenAPIV3Schema: &apiextensionsv1.JSONSchemaProps{
				Type: "object",
				Properties: map[string]apiextensionsv1.JSONSchemaProps{
					"hostPort": {Type: "string"},
				},
			},
		},
	},
	{
		Name:    "v2",
		Served:  true,
		Storage: true,
		Schema: &apiextensionsv1.CustomResourceValidation{
			OpenAPIV3Schema: &apiextensionsv1.JSONSchemaProps{
				Type: "object",
				Properties: map[string]apiextensionsv1.JSONSchemaProps{
					"host": {Type: "string"},
					"port": {Type: "string"},
				},
			},
		},
	},
}

var _ = SIGDescribe("CustomResourceConversionWebhook [Privileged:ClusterAdmin]", func() {
	var certCtx *certContext
	f := framework.NewDefaultFramework("crd-webhook")
	f.NamespacePodSecurityLevel = admissionapi.LevelBaseline
	servicePort := int32(9443)
	containerPort := int32(9444)

	ginkgo.BeforeEach(func(ctx context.Context) {
		ginkgo.DeferCleanup(cleanCRDWebhookTest, f.ClientSet, f.Namespace.Name)

		ginkgo.By("Setting up server cert")
		certCtx = setupServerCert(f.Namespace.Name, serviceCRDName)
		createAuthReaderRoleBindingForCRDConversion(ctx, f, f.Namespace.Name)

		deployCustomResourceWebhookAndService(ctx, f, imageutils.GetE2EImage(imageutils.Agnhost), certCtx, servicePort, containerPort)
	})

	/*
		发布版本：v1.16
		测试名称：Custom Resource Definition Conversion Webhook, conversion custom resource
		描述：注册 conversion webhook 和自定义资源定义，创建 v1 自定义
		资源。以 v2 读取必须成功。
	*/
	framework.ConformanceIt("should be able to convert from CR v1 to CR v2", func(ctx context.Context) {
		testcrd, err := crd.CreateMultiVersionTestCRD(f, "stable.example.com", func(crd *apiextensionsv1.CustomResourceDefinition) {
			crd.Spec.Versions = apiVersions
			crd.Spec.Conversion = &apiextensionsv1.CustomResourceConversion{
				Strategy: apiextensionsv1.WebhookConverter,
				Webhook: &apiextensionsv1.WebhookConversion{
					ClientConfig: &apiextensionsv1.WebhookClientConfig{
						CABundle: certCtx.signingCert,
						Service: &apiextensionsv1.ServiceReference{
							Namespace: f.Namespace.Name,
							Name:      serviceCRDName,
							Path:      ptr.To("/crdconvert"),
							Port:      ptr.To[int32](servicePort),
						},
					},
					ConversionReviewVersions: []string{"v1", "v1beta1"},
				},
			}
			crd.Spec.PreserveUnknownFields = false
		})
		if err != nil {
			return
		}
		ginkgo.DeferCleanup(testcrd.CleanUp)
		waitWebhookConversionReady(ctx, f, testcrd.Crd, testcrd.DynamicClients, "v2")
		testCustomResourceConversionWebhook(ctx, f, testcrd.Crd, testcrd.DynamicClients)
	})

	/*
		发布版本：v1.16
		测试名称：Custom Resource Definition Conversion Webhook, convert mixed version list
		描述：注册 conversion webhook 和自定义资源定义，创建以
		v1 存储的自定义资源。将 CRD 存储版本改为 v2，再创建以 v2 存储的资源。以 v2
		列举自定义资源；结果必须同时包含两个 v2 版本的自定义资源。
	*/
	framework.ConformanceIt("should be able to convert a non homogeneous list of CRs", func(ctx context.Context) {
		testcrd, err := crd.CreateMultiVersionTestCRD(f, "stable.example.com", func(crd *apiextensionsv1.CustomResourceDefinition) {
			crd.Spec.Versions = apiVersions
			crd.Spec.Conversion = &apiextensionsv1.CustomResourceConversion{
				Strategy: apiextensionsv1.WebhookConverter,
				Webhook: &apiextensionsv1.WebhookConversion{
					ClientConfig: &apiextensionsv1.WebhookClientConfig{
						CABundle: certCtx.signingCert,
						Service: &apiextensionsv1.ServiceReference{
							Namespace: f.Namespace.Name,
							Name:      serviceCRDName,
							Path:      ptr.To("/crdconvert"),
							Port:      ptr.To[int32](servicePort),
						},
					},
					ConversionReviewVersions: []string{"v1", "v1beta1"},
				},
			}
			crd.Spec.PreserveUnknownFields = false
		})
		if err != nil {
			return
		}
		ginkgo.DeferCleanup(testcrd.CleanUp)
		waitWebhookConversionReady(ctx, f, testcrd.Crd, testcrd.DynamicClients, "v2")
		testCRListConversion(ctx, f, testcrd)
	})
})

func cleanCRDWebhookTest(ctx context.Context, client clientset.Interface, namespaceName string) {
	_ = client.CoreV1().Services(namespaceName).Delete(ctx, serviceCRDName, metav1.DeleteOptions{})
	_ = client.AppsV1().Deployments(namespaceName).Delete(ctx, deploymentCRDName, metav1.DeleteOptions{})
	_ = client.CoreV1().Secrets(namespaceName).Delete(ctx, secretCRDName, metav1.DeleteOptions{})
	_ = client.RbacV1().RoleBindings("kube-system").Delete(ctx, roleBindingCRDName, metav1.DeleteOptions{})
}

func createAuthReaderRoleBindingForCRDConversion(ctx context.Context, f *framework.Framework, namespace string) {
	ginkgo.By("Create role binding to let cr conversion webhook read extension-apiserver-authentication")
	client := f.ClientSet
	// 创建 role binding，让 webhook 能读取 extension-apiserver-authentication configmap
	_, err := client.RbacV1().RoleBindings("kube-system").Create(ctx, &rbacv1.RoleBinding{
		ObjectMeta: metav1.ObjectMeta{
			Name: roleBindingCRDName,
		},
		RoleRef: rbacv1.RoleRef{
			APIGroup: "",
			Kind:     "Role",
			Name:     "extension-apiserver-authentication-reader",
		},

		Subjects: []rbacv1.Subject{
			{
				Kind:      "ServiceAccount",
				Name:      "default",
				Namespace: namespace,
			},
		},
	}, metav1.CreateOptions{})
	if err != nil && apierrors.IsAlreadyExists(err) {
		framework.Logf("role binding %s already exists", roleBindingCRDName)
	} else {
		framework.ExpectNoError(err, "creating role binding %s:webhook to access configMap", namespace)
	}
}

func deployCustomResourceWebhookAndService(ctx context.Context, f *framework.Framework, image string, certCtx *certContext, servicePort int32, containerPort int32) {
	ginkgo.By("Deploying the custom resource conversion webhook pod")
	client := f.ClientSet

	// 创建包含 webhook 证书的 secret。
	secret := &v1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name: secretCRDName,
		},
		Type: v1.SecretTypeOpaque,
		Data: map[string][]byte{
			"tls.crt": certCtx.cert,
			"tls.key": certCtx.key,
		},
	}
	namespace := f.Namespace.Name
	_, err := client.CoreV1().Secrets(namespace).Create(ctx, secret, metav1.CreateOptions{})
	framework.ExpectNoError(err, "creating secret %q in namespace %q", secretName, namespace)

	// 创建 webhook 的 deployment
	podLabels := map[string]string{"app": "sample-crd-conversion-webhook", "crd-webhook": "true"}
	replicas := int32(1)
	mounts := []v1.VolumeMount{
		{
			Name:      "crd-conversion-webhook-certs",
			ReadOnly:  true,
			MountPath: "/webhook.local.config/certificates",
		},
	}
	volumes := []v1.Volume{
		{
			Name: "crd-conversion-webhook-certs",
			VolumeSource: v1.VolumeSource{
				Secret: &v1.SecretVolumeSource{SecretName: secretCRDName},
			},
		},
	}
	containers := []v1.Container{
		{
			Name:         "sample-crd-conversion-webhook",
			VolumeMounts: mounts,
			Args: []string{
				"crd-conversion-webhook",
				"--tls-cert-file=/webhook.local.config/certificates/tls.crt",
				"--tls-private-key-file=/webhook.local.config/certificates/tls.key",
				"-v=4",
				// 容器使用非默认端口。
				fmt.Sprintf("--port=%d", containerPort),
			},
			ReadinessProbe: &v1.Probe{
				ProbeHandler: v1.ProbeHandler{
					HTTPGet: &v1.HTTPGetAction{
						Scheme: v1.URISchemeHTTPS,
						Port:   intstr.FromInt32(containerPort),
						Path:   "/readyz",
					},
				},
				PeriodSeconds:    1,
				SuccessThreshold: 1,
				FailureThreshold: 30,
			},
			Image: image,
			Ports: []v1.ContainerPort{{ContainerPort: containerPort}},
		},
	}
	d := e2edeployment.NewDeployment(deploymentCRDName, replicas, podLabels, "", "", appsv1.RollingUpdateDeploymentStrategyType)
	d.Spec.Template.Spec.Containers = containers
	d.Spec.Template.Spec.Volumes = volumes

	deployment, err := client.AppsV1().Deployments(namespace).Create(ctx, d, metav1.CreateOptions{})
	framework.ExpectNoError(err, "creating deployment %s in namespace %s", deploymentCRDName, namespace)

	ginkgo.By("Wait for the deployment to be ready")

	err = e2edeployment.WaitForDeploymentRevisionAndImage(client, namespace, deploymentCRDName, "1", image)
	framework.ExpectNoError(err, "waiting for the deployment of image %s in %s in %s to complete", image, deploymentCRDName, namespace)

	err = e2edeployment.WaitForDeploymentComplete(client, deployment)
	framework.ExpectNoError(err, "waiting for %s deployment status valid", deploymentCRDName)

	ginkgo.By("Deploying the webhook service")

	serviceLabels := map[string]string{"crd-webhook": "true"}
	service := &v1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: namespace,
			Name:      serviceCRDName,
			Labels:    map[string]string{"test": "crd-webhook"},
		},
		Spec: v1.ServiceSpec{
			Selector: serviceLabels,
			Ports: []v1.ServicePort{
				{
					Protocol:   v1.ProtocolTCP,
					Port:       servicePort,
					TargetPort: intstr.FromInt32(containerPort),
				},
			},
		},
	}
	_, err = client.CoreV1().Services(namespace).Create(ctx, service, metav1.CreateOptions{})
	framework.ExpectNoError(err, "creating service %s in namespace %s", serviceCRDName, namespace)

	ginkgo.By("Verifying the service has paired with the endpoint")
	err = e2eendpointslice.WaitForEndpointCount(ctx, client, namespace, serviceCRDName, 1)
	framework.ExpectNoError(err, "waiting for service %s/%s have %d endpoint", namespace, serviceCRDName, 1)
}

func verifyV1Object(crd *apiextensionsv1.CustomResourceDefinition, obj *unstructured.Unstructured) {
	gomega.Expect(obj.GetAPIVersion()).To(gomega.BeEquivalentTo(crd.Spec.Group + "/v1"))
	hostPort, exists := obj.Object["hostPort"]
	if !exists {
		framework.Failf("HostPort not found.")
	}

	gomega.Expect(hostPort).To(gomega.BeEquivalentTo("localhost:8080"))
	_, hostExists := obj.Object["host"]
	if hostExists {
		framework.Failf("Host should not have been declared.")
	}
	_, portExists := obj.Object["port"]
	if portExists {
		framework.Failf("Port should not have been declared.")
	}
}

func verifyV2Object(crd *apiextensionsv1.CustomResourceDefinition, obj *unstructured.Unstructured) {
	gomega.Expect(obj.GetAPIVersion()).To(gomega.BeEquivalentTo(crd.Spec.Group + "/v2"))
	_, hostPortExists := obj.Object["hostPort"]
	if hostPortExists {
		framework.Failf("HostPort should not have been declared.")
	}
	host, hostExists := obj.Object["host"]
	if !hostExists {
		framework.Failf("Host declaration not found.")
	}
	gomega.Expect(host).To(gomega.BeEquivalentTo("localhost"))
	port, portExists := obj.Object["port"]
	if !portExists {
		framework.Failf("Port declaration not found.")
	}
	gomega.Expect(port).To(gomega.BeEquivalentTo("8080"))
}

func testCustomResourceConversionWebhook(ctx context.Context, f *framework.Framework, crd *apiextensionsv1.CustomResourceDefinition, customResourceClients map[string]dynamic.ResourceInterface) {
	name := "cr-instance-1"
	ginkgo.By("Creating a v1 custom resource")
	crInstance := &unstructured.Unstructured{
		Object: map[string]interface{}{
			"kind":       crd.Spec.Names.Kind,
			"apiVersion": crd.Spec.Group + "/v1",
			"metadata": map[string]interface{}{
				"name":      name,
				"namespace": f.Namespace.Name,
			},
			"hostPort": "localhost:8080",
		},
	}
	_, err := customResourceClients["v1"].Create(ctx, crInstance, metav1.CreateOptions{})
	framework.ExpectNoError(err)
	ginkgo.By("v2 custom resource should be converted")
	v2crd, err := customResourceClients["v2"].Get(ctx, name, metav1.GetOptions{})
	framework.ExpectNoError(err, "Getting v2 of custom resource %s", name)
	verifyV2Object(crd, v2crd)
}

func testCRListConversion(ctx context.Context, f *framework.Framework, testCrd *crd.TestCrd) {
	crd := testCrd.Crd
	customResourceClients := testCrd.DynamicClients
	name1 := "cr-instance-1"
	name2 := "cr-instance-2"
	ginkgo.By("Creating a v1 custom resource")
	crInstance := &unstructured.Unstructured{
		Object: map[string]interface{}{
			"kind":       crd.Spec.Names.Kind,
			"apiVersion": crd.Spec.Group + "/v1",
			"metadata": map[string]interface{}{
				"name":      name1,
				"namespace": f.Namespace.Name,
			},
			"hostPort": "localhost:8080",
		},
	}
	_, err := customResourceClients["v1"].Create(ctx, crInstance, metav1.CreateOptions{})
	framework.ExpectNoError(err)

	// 现在 cr-instance-1 以 v1 存储。切换存储版本
	crd, err = integration.UpdateV1CustomResourceDefinitionWithRetry(testCrd.APIExtensionClient, crd.Name, func(c *apiextensionsv1.CustomResourceDefinition) {
		c.Spec.Versions = alternativeAPIVersions
	})
	framework.ExpectNoError(err)
	ginkgo.By("Create a v2 custom resource")
	crInstance = &unstructured.Unstructured{
		Object: map[string]interface{}{
			"kind":       crd.Spec.Names.Kind,
			"apiVersion": crd.Spec.Group + "/v1",
			"metadata": map[string]interface{}{
				"name":      name2,
				"namespace": f.Namespace.Name,
			},
			"hostPort": "localhost:8080",
		},
	}

	// 修改 CRD 后，各版本的资源会被重建，可能导致
	// 连接被取消（例如 "grpc connection closed" 或 "context canceled"）。
	// 重试即可解决。
	//
	// TODO: 需要等待存储版本生效；存储版本切换不是即时完成的。
	for range 5 {
		_, err = customResourceClients["v1"].Create(ctx, crInstance, metav1.CreateOptions{})
		if err == nil {
			break
		}
	}
	framework.ExpectNoError(err)

	// 现在 v1 和 v2 对象都有了，v1 和 v2 的 list 操作都应按预期工作。

	ginkgo.By("List CRs in v1")
	list, err := customResourceClients["v1"].List(ctx, metav1.ListOptions{})
	framework.ExpectNoError(err)
	gomega.Expect(list.Items).To(gomega.HaveLen(2))
	if !((list.Items[0].GetName() == name1 && list.Items[1].GetName() == name2) ||
		(list.Items[0].GetName() == name2 && list.Items[1].GetName() == name1)) {
		framework.Failf("failed to find v1 objects with names %s and %s in the list: \n%s", name1, name2, format.Object(list.Items, 1))
	}
	verifyV1Object(crd, &list.Items[0])
	verifyV1Object(crd, &list.Items[1])

	ginkgo.By("List CRs in v2")
	list, err = customResourceClients["v2"].List(ctx, metav1.ListOptions{})
	framework.ExpectNoError(err)
	gomega.Expect(list.Items).To(gomega.HaveLen(2))
	if !((list.Items[0].GetName() == name1 && list.Items[1].GetName() == name2) ||
		(list.Items[0].GetName() == name2 && list.Items[1].GetName() == name1)) {
		framework.Failf("failed to find v2 objects with names %s and %s in the list: \n%s", name1, name2, format.Object(list.Items, 1))
	}
	verifyV2Object(crd, &list.Items[0])
	verifyV2Object(crd, &list.Items[1])
}

// waitWebhookConversionReady 持续发送需要转换的自定义资源创建请求，直到成功一次。
func waitWebhookConversionReady(ctx context.Context, f *framework.Framework, crd *apiextensionsv1.CustomResourceDefinition, customResourceClients map[string]dynamic.ResourceInterface, version string) {
	framework.ExpectNoError(wait.PollUntilContextTimeout(ctx, 100*time.Millisecond, 30*time.Second, true, func(ctx context.Context) (bool, error) {
		crInstance := &unstructured.Unstructured{
			Object: map[string]interface{}{
				"kind":       crd.Spec.Names.Kind,
				"apiVersion": crd.Spec.Group + "/" + version,
				"metadata": map[string]interface{}{
					"name":      f.UniqueName,
					"namespace": f.Namespace.Name,
				},
			},
		}
		_, err := customResourceClients[version].Create(ctx, crInstance, metav1.CreateOptions{})
		if err != nil {
			// 容忍未设置 --enable-aggregator-routing 的集群：需要等 kube-proxy
			// 配置好 service 网络，期间转换请求会返回错误
			framework.Logf("error waiting for conversion to succeed during setup: %v", err)
			return false, nil
		}

		framework.ExpectNoError(customResourceClients[version].Delete(ctx, crInstance.GetName(), metav1.DeleteOptions{}), "cleaning up stub object")
		return true, nil
	}))
}
