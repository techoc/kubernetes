/*
Copyright 2017 The Kubernetes Authors.

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
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"time"

	admissionregistrationv1 "k8s.io/api/admissionregistration/v1"
	appsv1 "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	crdclientset "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/apimachinery/pkg/util/resourceversion"
	"k8s.io/apimachinery/pkg/util/uuid"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/dynamic"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/util/retry"
	apimachineryutils "k8s.io/kubernetes/test/e2e/common/apimachinery"
	"k8s.io/kubernetes/test/e2e/framework"
	e2edeployment "k8s.io/kubernetes/test/e2e/framework/deployment"
	e2eendpointslice "k8s.io/kubernetes/test/e2e/framework/endpointslice"
	e2ekubectl "k8s.io/kubernetes/test/e2e/framework/kubectl"
	e2epod "k8s.io/kubernetes/test/e2e/framework/pod"
	"k8s.io/kubernetes/test/utils/crd"
	imageutils "k8s.io/kubernetes/test/utils/image"
	admissionapi "k8s.io/pod-security-admission/api"
	"k8s.io/utils/ptr"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
)

const (
	secretName      = "sample-webhook-secret"
	deploymentName  = "sample-webhook-deployment"
	serviceName     = "e2e-test-webhook"
	roleBindingName = "webhook-auth-reader"

	skipNamespaceLabelKey   = "skip-webhook-admission"
	skipNamespaceLabelValue = "yes"
	skipNamespaceBaseName   = "exempted-namespace"
	disallowedPodName       = "disallowed-pod"
	toBeAttachedPodName     = "to-be-attached-pod"
	hangingPodName          = "hanging-pod"
	disallowedConfigMapName = "disallowed-configmap"
	allowedConfigMapName    = "allowed-configmap"
	failNamespaceLabelKey   = "fail-closed-webhook"
	failNamespaceLabelValue = "yes"
	failNamespaceBaseName   = "fail-closed-namespace"
	addedLabelKey           = "added-label"
	addedLabelValue         = "yes"
)

var _ = SIGDescribe("AdmissionWebhook [Privileged:ClusterAdmin]", func() {
	var certCtx *certContext
	f := framework.NewDefaultFramework("webhook")
	f.NamespacePodSecurityLevel = admissionapi.LevelBaseline
	servicePort := int32(8443)
	containerPort := int32(8444)

	var client clientset.Interface
	var namespaceName string
	var markersNamespaceName string

	ginkgo.BeforeEach(func(ctx context.Context) {
		client = f.ClientSet
		namespaceName = f.Namespace.Name

		// 确保为测试创建的 namespace 带有 webhook 可选择到的 label
		labelNamespace(ctx, f, f.Namespace.Name)
		markersNamespaceName = createWebhookConfigurationReadyNamespace(ctx, f)

		ginkgo.By("Setting up server cert")
		certCtx = setupServerCert(namespaceName, serviceName)
		createAuthReaderRoleBinding(ctx, f, namespaceName)

		deployWebhookAndService(ctx, f, imageutils.GetE2EImage(imageutils.Agnhost), certCtx, servicePort, containerPort)
	})

	ginkgo.AfterEach(func(ctx context.Context) {
		cleanWebhookTest(ctx, client, namespaceName)
	})

	/*
		Release: v1.16
		Testname: Admission webhook, discovery document
		Description: /apis discovery 文档中 MUST 存在 admissionregistration.k8s.io API group。
		/apis discovery 文档中 MUST 存在 admissionregistration.k8s.io/v1 API group/version。
		/apis/admissionregistration.k8s.io/v1 discovery 文档中 MUST 存在
		mutatingwebhookconfigurations 和 validatingwebhookconfigurations 资源。
	*/
	framework.ConformanceIt("should include webhook resources in discovery documents", func(ctx context.Context) {
		{
			ginkgo.By("fetching the /apis discovery document")
			apiGroupList := &metav1.APIGroupList{}
			err := client.Discovery().RESTClient().Get().AbsPath("/apis").Do(ctx).Into(apiGroupList)
			framework.ExpectNoError(err, "fetching /apis")

			ginkgo.By("finding the admissionregistration.k8s.io API group in the /apis discovery document")
			var group *metav1.APIGroup
			for _, g := range apiGroupList.Groups {
				if g.Name == admissionregistrationv1.GroupName {
					group = &g
					break
				}
			}
			gomega.Expect(group).ToNot(gomega.BeNil(), "admissionregistration.k8s.io API group not found in /apis discovery document")

			ginkgo.By("finding the admissionregistration.k8s.io/v1 API group/version in the /apis discovery document")
			var version *metav1.GroupVersionForDiscovery
			for _, v := range group.Versions {
				if v.Version == admissionregistrationv1.SchemeGroupVersion.Version {
					version = &v
					break
				}
			}
			gomega.Expect(version).ToNot(gomega.BeNil(), "admissionregistration.k8s.io/v1 API group version not found in /apis discovery document")
		}

		{
			ginkgo.By("fetching the /apis/admissionregistration.k8s.io discovery document")
			group := &metav1.APIGroup{}
			err := client.Discovery().RESTClient().Get().AbsPath("/apis/admissionregistration.k8s.io").Do(ctx).Into(group)
			framework.ExpectNoError(err, "fetching /apis/admissionregistration.k8s.io")
			gomega.Expect(group.Name).To(gomega.Equal(admissionregistrationv1.GroupName), "verifying API group name in /apis/admissionregistration.k8s.io discovery document")

			ginkgo.By("finding the admissionregistration.k8s.io/v1 API group/version in the /apis/admissionregistration.k8s.io discovery document")
			var version *metav1.GroupVersionForDiscovery
			for _, v := range group.Versions {
				if v.Version == admissionregistrationv1.SchemeGroupVersion.Version {
					version = &v
					break
				}
			}
			gomega.Expect(version).ToNot(gomega.BeNil(), "admissionregistration.k8s.io/v1 API group version not found in /apis/admissionregistration.k8s.io discovery document")
		}

		{
			ginkgo.By("fetching the /apis/admissionregistration.k8s.io/v1 discovery document")
			apiResourceList := &metav1.APIResourceList{}
			err := client.Discovery().RESTClient().Get().AbsPath("/apis/admissionregistration.k8s.io/v1").Do(ctx).Into(apiResourceList)
			framework.ExpectNoError(err, "fetching /apis/admissionregistration.k8s.io/v1")
			gomega.Expect(apiResourceList.GroupVersion).To(gomega.Equal(admissionregistrationv1.SchemeGroupVersion.String()), "verifying API group/version in /apis/admissionregistration.k8s.io/v1 discovery document")

			ginkgo.By("finding mutatingwebhookconfigurations and validatingwebhookconfigurations resources in the /apis/admissionregistration.k8s.io/v1 discovery document")
			var (
				mutatingWebhookResource   *metav1.APIResource
				validatingWebhookResource *metav1.APIResource
			)
			for i := range apiResourceList.APIResources {
				if apiResourceList.APIResources[i].Name == "mutatingwebhookconfigurations" {
					mutatingWebhookResource = &apiResourceList.APIResources[i]
				}
				if apiResourceList.APIResources[i].Name == "validatingwebhookconfigurations" {
					validatingWebhookResource = &apiResourceList.APIResources[i]
				}
			}
			gomega.Expect(mutatingWebhookResource).ToNot(gomega.BeNil(), "mutatingwebhookconfigurations resource not found in /apis/admissionregistration.k8s.io/v1 discovery document")
			gomega.Expect(validatingWebhookResource).ToNot(gomega.BeNil(), "validatingwebhookconfigurations resource not found in /apis/admissionregistration.k8s.io/v1 discovery document")
		}
	})

	/*
		Release: v1.16
		Testname: Admission webhook, deny create
		Description: 注册一个放行 pod 和 configmap 的 admission webhook 配置。创建不符合要求的 pod/configmap，
		或将合规对象 update/patch 为不合规，MUST 被拒绝。创建会让 webhook 挂起的 pod MUST 触发 webhook 超时错误，
		且 pod 创建 MUST 被拒绝。在 webhook namespace selector 白名单内的 namespace 中创建不合规 configmap
		MUST 被放行。
	*/
	framework.ConformanceIt("should be able to deny pod and configmap creation", func(ctx context.Context) {
		registerWebhook(ctx, f, markersNamespaceName, f.UniqueName, certCtx, servicePort)
		testWebhook(ctx, f)
	})

	/*
		Release: v1.16
		Testname: Admission webhook, deny attach
		Description: 注册一个拒绝连接 pod attach sub-resource 的 admission webhook 配置。
		尝试 attach MUST 被拒绝。
	*/
	framework.ConformanceIt("should be able to deny attaching pod", func(ctx context.Context) {
		registerWebhookForAttachingPod(ctx, f, markersNamespaceName, f.UniqueName, certCtx, servicePort)
		testAttachingPodWebhook(ctx, f)
	})

	/*
		Release: v1.16
		Testname: Admission webhook, deny custom resource create and delete
		Description: 注册一个拒绝自定义资源 create、update 和 delete 的 admission webhook 配置。
		尝试 create、update、delete 自定义资源 MUST 被拒绝。
	*/
	framework.ConformanceIt("should be able to deny custom resource creation, update and deletion", func(ctx context.Context) {
		testcrd, err := crd.CreateTestCRD(f)
		if err != nil {
			return
		}
		ginkgo.DeferCleanup(testcrd.CleanUp)
		registerWebhookForCustomResource(ctx, f, markersNamespaceName, f.UniqueName, certCtx, testcrd, servicePort)
		testCustomResourceWebhook(ctx, f, testcrd.Crd, testcrd.DynamicClients["v1"])
		testBlockingCustomResourceUpdateDeletion(ctx, f, testcrd.Crd, testcrd.DynamicClients["v1"])
	})

	/*
		Release: v1.16
		Testname: Admission webhook, fail closed
		Description: 注册一个 failure policy 为 fail-closed 且没有 CA bundle 的 webhook，使其无法被调用。
		尝试需要该 admission webhook 的操作；所有操作 MUST 被拒绝。
	*/
	framework.ConformanceIt("should unconditionally reject operations on fail closed webhook", func(ctx context.Context) {
		registerFailClosedWebhook(ctx, f, markersNamespaceName, f.UniqueName, certCtx, servicePort)
		testFailClosedWebhook(ctx, f)
	})

	/*
		Release: v1.16
		Testname: Admission webhook, ordered mutation
		Description: 注册一个包含两个 webhook 的 mutating webhook 配置，二者都放行 configmap：
		第一个在 configmap 已有特定 key 时添加一个 data key，第二个在第一个 webhook 添加的 key 存在时再添加一个 key。
		尝试创建 configmap；两个 key MUST 都被加入该 configmap。
	*/
	framework.ConformanceIt("should mutate configmap", func(ctx context.Context) {
		registerMutatingWebhookForConfigMap(ctx, f, markersNamespaceName, f.UniqueName, certCtx, servicePort)
		testMutatingConfigMapWebhook(ctx, f)
	})

	/*
		Release: v1.16
		Testname: Admission webhook, mutation with defaulting
		Description: 注册一个向 pod 添加 InitContainer 的 mutating webhook。尝试创建 pod；
		MUST 添加该 InitContainer，且 TerminationMessagePolicy MUST 被默认值填充。
	*/
	framework.ConformanceIt("should mutate pod and apply defaults after mutation", func(ctx context.Context) {
		registerMutatingWebhookForPod(ctx, f, markersNamespaceName, f.UniqueName, certCtx, servicePort)
		testMutatingPodWebhook(ctx, f)
	})

	/*
		Release: v1.16
		Testname: Admission webhook, admission control not allowed on webhook configuration objects
		Description: 注册用于修改和拒绝删除 webhook 配置对象的 webhook。尝试创建和删除 webhook 配置对象；
		两个操作 MUST 都被允许，且 webhook 配置对象 MUST NOT 被这些 webhook 修改。
	*/
	framework.ConformanceIt("should not be able to mutate or prevent deletion of webhook configuration objects", func(ctx context.Context) {
		registerValidatingWebhookForWebhookConfigurations(ctx, f, markersNamespaceName, f.UniqueName+"blocking", certCtx, servicePort)
		registerMutatingWebhookForWebhookConfigurations(ctx, f, markersNamespaceName, f.UniqueName+"blocking", certCtx, servicePort)
		testWebhooksForWebhookConfigurations(ctx, f, markersNamespaceName, f.UniqueName, certCtx, servicePort)
	})

	/*
		Release: v1.16
		Testname: Admission webhook, mutate custom resource
		Description: 注册一个修改自定义资源的 webhook。尝试创建自定义资源对象；
		该资源 MUST 被修改。
	*/
	framework.ConformanceIt("should mutate custom resource", func(ctx context.Context) {
		testcrd, err := crd.CreateTestCRD(f)
		if err != nil {
			return
		}
		ginkgo.DeferCleanup(testcrd.CleanUp)
		registerMutatingWebhookForCustomResource(ctx, f, markersNamespaceName, f.UniqueName, certCtx, testcrd, servicePort)
		testMutatingCustomResourceWebhook(ctx, f, testcrd.Crd, testcrd.DynamicClients["v1"], false)
	})

	/*
		Release: v1.16
		Testname: Admission webhook, deny custom resource definition
		Description: 注册一个拒绝创建 CRD 的 webhook。尝试创建 CRD；该创建请求 MUST 被拒绝。
	*/
	framework.ConformanceIt("should deny crd creation", func(ctx context.Context) {
		registerValidatingWebhookForCRD(ctx, f, markersNamespaceName, f.UniqueName, certCtx, servicePort)

		testCRDDenyWebhook(ctx, f)
	})

	/*
		Release: v1.16
		Testname: Admission webhook, mutate custom resource with different stored version
		Description: 注册一个在 create 和 update 时修改自定义资源的 webhook。注册以 v1 为存储版本的 CRD，
		创建一个自定义资源。将该 CRD 的存储版本 patch 为 v2。尝试用新字段和值 patch 该自定义资源；
		patch MUST 成功应用。
	*/
	framework.ConformanceIt("should mutate custom resource with different stored version", func(ctx context.Context) {
		testcrd, err := createAdmissionWebhookMultiVersionTestCRDWithV1Storage(f)
		if err != nil {
			return
		}
		ginkgo.DeferCleanup(testcrd.CleanUp)
		registerMutatingWebhookForCustomResource(ctx, f, markersNamespaceName, f.UniqueName, certCtx, testcrd, servicePort)
		testMultiVersionCustomResourceWebhook(ctx, f, testcrd)
	})

	/*
		Release: v1.16
		Testname: Admission webhook, mutate custom resource with pruning
		Description: 注册向自定义对象添加字段的 mutating webhook。注册一个 schema 仅包含 webhook 所添加 data key
		之一的 CRD。尝试创建自定义资源；schema 中定义的字段 MUST 存在，未定义的字段 MUST NOT 存在。
	*/
	framework.ConformanceIt("should mutate custom resource with pruning", func(ctx context.Context) {
		const prune = true
		testcrd, err := createAdmissionWebhookMultiVersionTestCRDWithV1Storage(f, func(crd *apiextensionsv1.CustomResourceDefinition) {
			crd.Spec.PreserveUnknownFields = false
			for i := range crd.Spec.Versions {
				crd.Spec.Versions[i].Schema = &apiextensionsv1.CustomResourceValidation{
					OpenAPIV3Schema: &apiextensionsv1.JSONSchemaProps{
						Type: "object",
						Properties: map[string]apiextensionsv1.JSONSchemaProps{
							"data": {
								Type: "object",
								Properties: map[string]apiextensionsv1.JSONSchemaProps{
									"mutation-start":   {Type: "string"},
									"mutation-stage-1": {Type: "string"},
									// 故意省略 mutation-stage-2，以验证它会被裁剪掉
								},
							},
						},
					},
				}
			}
		})
		if err != nil {
			return
		}
		ginkgo.DeferCleanup(testcrd.CleanUp)
		registerMutatingWebhookForCustomResource(ctx, f, markersNamespaceName, f.UniqueName, certCtx, testcrd, servicePort)
		testMutatingCustomResourceWebhook(ctx, f, testcrd.Crd, testcrd.DynamicClients["v1"], prune)
	})

	/*
		Release: v1.16
		Testname: Admission webhook, honor timeout
		Description: 使用一个在放行对象前等待 5 秒的 webhook，配置不同的 timeout 和 failure policy 组合。
		对每种组合尝试创建 configmap。若 webhook timeout 小于 5 秒且 failure policy 为 fail，请求 MUST 超时；
		failure policy 为 ignore 时请求不应超时。webhook timeout 为 10 秒（远长于 webhook 等待时长）时，
		请求 MUST NOT 超时。
	*/
	framework.ConformanceIt("should honor timeout", func(ctx context.Context) {
		policyFail := admissionregistrationv1.Fail
		policyIgnore := admissionregistrationv1.Ignore

		ginkgo.By("Setting timeout (1s) shorter than webhook latency (5s)")
		slowWebhookCleanup := registerSlowWebhook(ctx, f, markersNamespaceName, f.UniqueName, certCtx, &policyFail, ptr.To[int32](1), servicePort)
		testSlowWebhookTimeoutFailEarly(ctx, f)
		slowWebhookCleanup(ctx)

		ginkgo.By("Having no error when timeout is shorter than webhook latency and failure policy is ignore")
		slowWebhookCleanup = registerSlowWebhook(ctx, f, markersNamespaceName, f.UniqueName, certCtx, &policyIgnore, ptr.To[int32](1), servicePort)
		testSlowWebhookTimeoutNoError(ctx, f)
		slowWebhookCleanup(ctx)

		ginkgo.By("Having no error when timeout is longer than webhook latency")
		slowWebhookCleanup = registerSlowWebhook(ctx, f, markersNamespaceName, f.UniqueName, certCtx, &policyFail, ptr.To[int32](10), servicePort)
		testSlowWebhookTimeoutNoError(ctx, f)
		slowWebhookCleanup(ctx)

		ginkgo.By("Having no error when timeout is empty (defaulted to 10s in v1)")
		slowWebhookCleanup = registerSlowWebhook(ctx, f, markersNamespaceName, f.UniqueName, certCtx, &policyFail, nil, servicePort)
		testSlowWebhookTimeoutNoError(ctx, f)
		slowWebhookCleanup(ctx)
	})

	/*
		Release: v1.31
		Testname: Admission webhook, connection pool isolation on pod restart
		Description: 验证 admission webhook 的连接池按解析出的 IP 地址隔离。
		service 后的 backend pod 重建并改变 IP 后，后续 admission 请求 must 成功路由到新 pod IP，
		而不会命中旧连接池。
	*/
	ginkgo.It("should isolate connection pool by resolved backend IP address on pod restart", func(ctx context.Context) {
		client := f.ClientSet

		ginkgo.By("Registering a validating webhook")
		registerWebhook(ctx, f, markersNamespaceName, f.UniqueName, certCtx, servicePort)

		// 获取活跃 webhook pod 的详情
		ginkgo.By("Getting the current webhook pod details")
		pods, err := client.CoreV1().Pods(f.Namespace.Name).List(ctx, metav1.ListOptions{LabelSelector: "app=sample-webhook"})
		framework.ExpectNoError(err)
		gomega.Expect(pods.Items).To(gomega.HaveLen(1))
		oldPod := pods.Items[0]
		oldIP := oldPod.Status.PodIP
		framework.Logf("Webhook currently running on pod %s with IP %s", oldPod.Name, oldIP)

		// 通过滚动更新 deployment 来重建/重启 webhook pod
		ginkgo.By("Triggering a rolling update of the webhook deployment")
		deployment, err := client.AppsV1().Deployments(f.Namespace.Name).Get(ctx, deploymentName, metav1.GetOptions{})
		framework.ExpectNoError(err)

		// 修改 deployment template 的 label 或 env 以触发滚动更新
		if deployment.Spec.Template.Annotations == nil {
			deployment.Spec.Template.Annotations = make(map[string]string)
		}
		deployment.Spec.Template.Annotations["restartedAt"] = time.Now().Format(time.RFC3339)

		deployment, err = client.AppsV1().Deployments(f.Namespace.Name).Update(ctx, deployment, metav1.UpdateOptions{})
		framework.ExpectNoError(err)

		ginkgo.By("Wait for the deployment rolling update to complete")
		err = e2edeployment.WaitForDeploymentComplete(client, deployment)
		framework.ExpectNoError(err)

		// 获取新 webhook pod 的详情
		ginkgo.By("Getting the new webhook pod details")
		newPods, err := client.CoreV1().Pods(f.Namespace.Name).List(ctx, metav1.ListOptions{LabelSelector: "app=sample-webhook"})
		framework.ExpectNoError(err)
		gomega.Expect(newPods.Items).To(gomega.HaveLen(1))
		newPod := newPods.Items[0]
		newIP := newPod.Status.PodIP
		framework.Logf("Webhook now running on pod %s with IP %s", newPod.Name, newIP)

		// 确认 IP 确实发生了变化（至少 pod 已被重建）
		gomega.Expect(newPod.Name).ToNot(gomega.Equal(oldPod.Name), "expected webhook pod to be recreated")

		// 发起新的 admission 请求并验证其成功
		ginkgo.By("Sending a new admission request to verify the webhook connection pool successfully routes to the new pod IP")
		err = wait.PollUntilContextTimeout(ctx, 100*time.Millisecond, 30*time.Second, true, func(ctx context.Context) (bool, error) {
			cm := &v1.ConfigMap{
				ObjectMeta: metav1.ObjectMeta{
					Name: string(uuid.NewUUID()),
				},
			}
			_, err = client.CoreV1().ConfigMaps(f.Namespace.Name).Create(ctx, cm, metav1.CreateOptions{})
			if err == nil {
				_ = client.CoreV1().ConfigMaps(f.Namespace.Name).Delete(ctx, cm.Name, metav1.DeleteOptions{})
				return true, nil
			}
			framework.Logf("Retrying admission request, last error: %v", err)
			return false, nil
		})
		framework.ExpectNoError(err, "admission request failed after webhook pod restart")
	})

	/*
		Release: v1.16
		Testname: Admission webhook, update validating webhook
		Description: 注册一个 validating admission webhook 配置。更新 webhook 使其不作用于 create 操作，
		然后尝试创建对象；webhook MUST NOT 拒绝该 create。再 patch webhook 使其重新作用于 create，
		尝试创建对象；webhook MUST 拒绝该 create。
	*/
	framework.ConformanceIt("patching/updating a validating webhook should work", func(ctx context.Context) {
		client := f.ClientSet
		admissionClient := client.AdmissionregistrationV1()

		ginkgo.By("Creating a validating webhook configuration")
		hook, err := createValidatingWebhookConfiguration(ctx, f, &admissionregistrationv1.ValidatingWebhookConfiguration{
			ObjectMeta: metav1.ObjectMeta{
				Name: f.UniqueName,
			},
			Webhooks: []admissionregistrationv1.ValidatingWebhook{
				newDenyConfigMapWebhookFixture(f, certCtx, servicePort),
				newValidatingIsReadyWebhookFixture(f, certCtx, servicePort),
			},
		})
		framework.ExpectNoError(err, "Creating validating webhook configuration")
		gomega.Expect(hook).To(apimachineryutils.HaveValidResourceVersion())
		defer func() {
			err := client.AdmissionregistrationV1().ValidatingWebhookConfigurations().Delete(ctx, hook.Name, metav1.DeleteOptions{})
			framework.ExpectNoError(err, "Deleting validating webhook configuration")
		}()

		// 继续之前先确认 backend 已就绪
		err = waitWebhookConfigurationReady(ctx, f, markersNamespaceName)
		framework.ExpectNoError(err, "waiting for webhook configuration to be ready")

		ginkgo.By("Creating a configMap that does not comply to the validation webhook rules")
		err = wait.PollImmediate(100*time.Millisecond, 30*time.Second, func() (bool, error) {
			cm := namedNonCompliantConfigMap(string(uuid.NewUUID()), f)
			_, err = client.CoreV1().ConfigMaps(f.Namespace.Name).Create(ctx, cm, metav1.CreateOptions{})
			if err == nil {
				err = client.CoreV1().ConfigMaps(f.Namespace.Name).Delete(ctx, cm.Name, metav1.DeleteOptions{})
				framework.ExpectNoError(err, "Deleting successfully created configMap")
				return false, nil
			}
			if !strings.Contains(err.Error(), "denied") {
				return false, err
			}
			return true, nil
		})

		ginkgo.By("Updating a validating webhook configuration's rules to not include the create operation")
		notIncludeCreateOperationFn := func(c *admissionregistrationv1.ValidatingWebhookConfiguration) {
			c.Webhooks[0].Rules[0].Operations = []admissionregistrationv1.OperationType{admissionregistrationv1.Update}
		}
		_, err = updateValidatingWebhookConfigurations(ctx, client, f.UniqueName, notIncludeCreateOperationFn)
		framework.ExpectNoError(err, "Updating validating webhook configuration")

		ginkgo.By("Creating a configMap that does not comply to the validation webhook rules")
		err = wait.PollImmediate(100*time.Millisecond, 30*time.Second, func() (bool, error) {
			cm := namedNonCompliantConfigMap(string(uuid.NewUUID()), f)
			_, err = client.CoreV1().ConfigMaps(f.Namespace.Name).Create(ctx, cm, metav1.CreateOptions{})
			if err != nil {
				if !strings.Contains(err.Error(), "denied") {
					return false, err
				}
				return false, nil
			}
			err = client.CoreV1().ConfigMaps(f.Namespace.Name).Delete(ctx, cm.Name, metav1.DeleteOptions{})
			framework.ExpectNoError(err, "Deleting successfully created configMap")
			return true, nil
		})
		framework.ExpectNoError(err, "Waiting for configMap in namespace %s to be allowed creation since webhook was updated to not validate create", f.Namespace.Name)

		ginkgo.By("Patching a validating webhook configuration's rules to include the create operation")
		oldRV := hook.ResourceVersion
		hook, err = admissionClient.ValidatingWebhookConfigurations().Patch(ctx, f.UniqueName,
			types.JSONPatchType,
			[]byte(`[{"op": "replace", "path": "/webhooks/0/rules/0/operations", "value": ["CREATE"]}]`), metav1.PatchOptions{})
		framework.ExpectNoError(err, "Patching validating webhook configuration")
		gomega.Expect(resourceversion.CompareResourceVersion(oldRV, hook.ResourceVersion)).To(gomega.BeNumerically("==", -1), "patched object should have a larger resource version")

		ginkgo.By("Creating a configMap that does not comply to the validation webhook rules")
		err = wait.PollImmediate(100*time.Millisecond, 30*time.Second, func() (bool, error) {
			cm := namedNonCompliantConfigMap(string(uuid.NewUUID()), f)
			_, err = client.CoreV1().ConfigMaps(f.Namespace.Name).Create(ctx, cm, metav1.CreateOptions{})
			if err == nil {
				err = client.CoreV1().ConfigMaps(f.Namespace.Name).Delete(ctx, cm.Name, metav1.DeleteOptions{})
				framework.ExpectNoError(err, "Deleting successfully created configMap")
				return false, nil
			}
			if !strings.Contains(err.Error(), "denied") {
				return false, err
			}
			return true, nil
		})
		framework.ExpectNoError(err, "Waiting for configMap in namespace %s to be denied creation by validating webhook", f.Namespace.Name)
	})

	/*
		Release: v1.16
		Testname: Admission webhook, update mutating webhook
		Description: 注册一个 mutating admission webhook 配置。更新 webhook 使其不作用于 create 操作，
		然后尝试创建对象；webhook MUST NOT 修改该对象。再 patch webhook 使其重新作用于 create，
		尝试创建对象；webhook MUST 修改该对象。
	*/
	framework.ConformanceIt("patching/updating a mutating webhook should work", func(ctx context.Context) {
		client := f.ClientSet
		admissionClient := client.AdmissionregistrationV1()

		ginkgo.By("Creating a mutating webhook configuration")
		hook, err := createMutatingWebhookConfiguration(ctx, f, &admissionregistrationv1.MutatingWebhookConfiguration{
			ObjectMeta: metav1.ObjectMeta{
				Name: f.UniqueName,
			},
			Webhooks: []admissionregistrationv1.MutatingWebhook{
				newMutateConfigMapWebhookFixture(f, certCtx, 1, servicePort),
				newMutatingIsReadyWebhookFixture(f, certCtx, servicePort),
			},
		})
		framework.ExpectNoError(err, "Creating mutating webhook configuration")
		gomega.Expect(hook).To(apimachineryutils.HaveValidResourceVersion())
		defer func() {
			err := client.AdmissionregistrationV1().MutatingWebhookConfigurations().Delete(ctx, hook.Name, metav1.DeleteOptions{})
			framework.ExpectNoError(err, "Deleting mutating webhook configuration")
		}()

		// 继续之前先确认 backend 已就绪
		err = waitWebhookConfigurationReady(ctx, f, markersNamespaceName)
		framework.ExpectNoError(err, "waiting for webhook configuration to be ready")

		hook, err = admissionClient.MutatingWebhookConfigurations().Get(ctx, f.UniqueName, metav1.GetOptions{})
		framework.ExpectNoError(err, "Getting mutating webhook configuration")
		ginkgo.By("Updating a mutating webhook configuration's rules to not include the create operation")
		hook.Webhooks[0].Rules[0].Operations = []admissionregistrationv1.OperationType{admissionregistrationv1.Update}
		hook, err = admissionClient.MutatingWebhookConfigurations().Update(ctx, hook, metav1.UpdateOptions{})
		framework.ExpectNoError(err, "Updating mutating webhook configuration")

		ginkgo.By("Creating a configMap that should not be mutated")
		err = wait.PollImmediate(100*time.Millisecond, 30*time.Second, func() (bool, error) {
			cm := namedToBeMutatedConfigMap(string(uuid.NewUUID()), f)
			created, err := client.CoreV1().ConfigMaps(f.Namespace.Name).Create(ctx, cm, metav1.CreateOptions{})
			if err != nil {
				return false, err
			}
			err = client.CoreV1().ConfigMaps(f.Namespace.Name).Delete(ctx, cm.Name, metav1.DeleteOptions{})
			framework.ExpectNoError(err, "Deleting successfully created configMap")
			_, ok := created.Data["mutation-stage-1"]
			return !ok, nil
		})
		framework.ExpectNoError(err, "Waiting for configMap in namespace %s this is not mutated", f.Namespace.Name)

		ginkgo.By("Patching a mutating webhook configuration's rules to include the create operation")
		oldRV := hook.ResourceVersion
		hook, err = admissionClient.MutatingWebhookConfigurations().Patch(ctx, f.UniqueName,
			types.JSONPatchType,
			[]byte(`[{"op": "replace", "path": "/webhooks/0/rules/0/operations", "value": ["CREATE"]}]`), metav1.PatchOptions{})
		framework.ExpectNoError(err, "Patching mutating webhook configuration")
		gomega.Expect(resourceversion.CompareResourceVersion(oldRV, hook.ResourceVersion)).To(gomega.BeNumerically("==", -1), "patched object should have a larger resource version")

		ginkgo.By("Creating a configMap that should be mutated")
		err = wait.PollImmediate(100*time.Millisecond, 30*time.Second, func() (bool, error) {
			cm := namedToBeMutatedConfigMap(string(uuid.NewUUID()), f)
			created, err := client.CoreV1().ConfigMaps(f.Namespace.Name).Create(ctx, cm, metav1.CreateOptions{})
			if err != nil {
				return false, err
			}
			err = client.CoreV1().ConfigMaps(f.Namespace.Name).Delete(ctx, cm.Name, metav1.DeleteOptions{})
			framework.ExpectNoError(err, "Deleting successfully created configMap")
			_, ok := created.Data["mutation-stage-1"]
			return ok, nil
		})
		framework.ExpectNoError(err, "Waiting for configMap in namespace %s to be mutated", f.Namespace.Name)
	})

	/*
		Release: v1.16
		Testname: Admission webhook, list validating webhooks
		Description: 创建 10 个都带同一 label 的 validating webhook 配置。按该 label 列出 webhook 配置；
		所有已创建的配置 MUST 都在结果中。尝试创建对象；create MUST 被拒绝。用 deletecollection 删除
		匹配该 label 的 webhook 配置；所有配置 MUST 被删除。再次尝试创建对象；create MUST NOT 被拒绝。
	*/
	framework.ConformanceIt("listing validating webhooks should work", func(ctx context.Context) {
		testListSize := 10
		testUUID := string(uuid.NewUUID())

		for i := range testListSize {
			name := fmt.Sprintf("%s-%d", f.UniqueName, i)
			_, err := createValidatingWebhookConfiguration(ctx, f, &admissionregistrationv1.ValidatingWebhookConfiguration{
				ObjectMeta: metav1.ObjectMeta{
					Name:   name,
					Labels: map[string]string{"e2e-list-test-uuid": testUUID},
				},
				Webhooks: []admissionregistrationv1.ValidatingWebhook{
					newDenyConfigMapWebhookFixture(f, certCtx, servicePort),
					newValidatingIsReadyWebhookFixture(f, certCtx, servicePort),
				},
			})
			framework.ExpectNoError(err, "Creating validating webhook configuration")
		}
		selectorListOpts := metav1.ListOptions{LabelSelector: "e2e-list-test-uuid=" + testUUID}

		ginkgo.By("Listing all of the created validation webhooks")
		list, err := client.AdmissionregistrationV1().ValidatingWebhookConfigurations().List(ctx, selectorListOpts)
		framework.ExpectNoError(err, "Listing validating webhook configurations")
		gomega.Expect(list.Items).To(gomega.HaveLen(testListSize))

		// 继续之前先确认 backend 已就绪
		err = waitWebhookConfigurationReady(ctx, f, markersNamespaceName)
		framework.ExpectNoError(err, "waiting for webhook configuration to be ready")

		ginkgo.By("Creating a configMap that does not comply to the validation webhook rules")
		err = wait.PollImmediate(100*time.Millisecond, 30*time.Second, func() (bool, error) {
			cm := namedNonCompliantConfigMap(string(uuid.NewUUID()), f)
			_, err = client.CoreV1().ConfigMaps(f.Namespace.Name).Create(ctx, cm, metav1.CreateOptions{})
			if err == nil {
				err = client.CoreV1().ConfigMaps(f.Namespace.Name).Delete(ctx, cm.Name, metav1.DeleteOptions{})
				framework.ExpectNoError(err, "Deleting successfully created configMap")
				return false, nil
			}
			if !strings.Contains(err.Error(), "denied") {
				return false, err
			}
			return true, nil
		})
		framework.ExpectNoError(err, "Waiting for configMap in namespace %s to be denied creation by validating webhook", f.Namespace.Name)

		ginkgo.By("Deleting the collection of validation webhooks")
		err = client.AdmissionregistrationV1().ValidatingWebhookConfigurations().DeleteCollection(ctx, metav1.DeleteOptions{}, selectorListOpts)
		framework.ExpectNoError(err, "Deleting collection of validating webhook configurations")

		ginkgo.By("Creating a configMap that does not comply to the validation webhook rules")
		err = wait.PollImmediate(100*time.Millisecond, 30*time.Second, func() (bool, error) {
			cm := namedNonCompliantConfigMap(string(uuid.NewUUID()), f)
			_, err = client.CoreV1().ConfigMaps(f.Namespace.Name).Create(ctx, cm, metav1.CreateOptions{})
			if err != nil {
				if !strings.Contains(err.Error(), "denied") {
					return false, err
				}
				return false, nil
			}
			err = client.CoreV1().ConfigMaps(f.Namespace.Name).Delete(ctx, cm.Name, metav1.DeleteOptions{})
			framework.ExpectNoError(err, "Deleting successfully created configMap")
			return true, nil
		})
		framework.ExpectNoError(err, "Waiting for configMap in namespace %s to be allowed creation since there are no webhooks", f.Namespace.Name)
	})

	/*
		Release: v1.16
		Testname: Admission webhook, list mutating webhooks
		Description: 创建 10 个都带同一 label 的 mutating webhook 配置。按该 label 列出 webhook 配置；
		所有已创建的配置 MUST 都在结果中。尝试创建对象；该对象 MUST 被修改。用 deletecollection 删除
		匹配该 label 的 webhook 配置；所有配置 MUST 被删除。再次尝试创建对象；该对象 MUST NOT 被修改。
	*/
	framework.ConformanceIt("listing mutating webhooks should work", func(ctx context.Context) {
		testListSize := 10
		testUUID := string(uuid.NewUUID())

		for i := range testListSize {
			name := fmt.Sprintf("%s-%d", f.UniqueName, i)
			_, err := createMutatingWebhookConfiguration(ctx, f, &admissionregistrationv1.MutatingWebhookConfiguration{
				ObjectMeta: metav1.ObjectMeta{
					Name:   name,
					Labels: map[string]string{"e2e-list-test-uuid": testUUID},
				},
				Webhooks: []admissionregistrationv1.MutatingWebhook{
					newMutateConfigMapWebhookFixture(f, certCtx, 1, servicePort),
					newMutatingIsReadyWebhookFixture(f, certCtx, servicePort),
				},
			})
			framework.ExpectNoError(err, "Creating mutating webhook configuration")
		}
		selectorListOpts := metav1.ListOptions{LabelSelector: "e2e-list-test-uuid=" + testUUID}

		ginkgo.By("Listing all of the created validation webhooks")
		list, err := client.AdmissionregistrationV1().MutatingWebhookConfigurations().List(ctx, selectorListOpts)
		framework.ExpectNoError(err, "Listing mutating webhook configurations")
		gomega.Expect(list.Items).To(gomega.HaveLen(testListSize))

		// 继续之前先确认 backend 已就绪
		err = waitWebhookConfigurationReady(ctx, f, markersNamespaceName)
		framework.ExpectNoError(err, "waiting for webhook configuration to be ready")

		ginkgo.By("Creating a configMap that should be mutated")
		err = wait.PollImmediate(100*time.Millisecond, 30*time.Second, func() (bool, error) {
			cm := namedToBeMutatedConfigMap(string(uuid.NewUUID()), f)
			created, err := client.CoreV1().ConfigMaps(f.Namespace.Name).Create(ctx, cm, metav1.CreateOptions{})
			if err != nil {
				return false, err
			}
			err = client.CoreV1().ConfigMaps(f.Namespace.Name).Delete(ctx, cm.Name, metav1.DeleteOptions{})
			framework.ExpectNoError(err, "Deleting successfully created configMap")
			_, ok := created.Data["mutation-stage-1"]
			return ok, nil
		})
		framework.ExpectNoError(err, "Waiting for configMap in namespace %s to be mutated", f.Namespace.Name)

		ginkgo.By("Deleting the collection of validation webhooks")
		err = client.AdmissionregistrationV1().MutatingWebhookConfigurations().DeleteCollection(ctx, metav1.DeleteOptions{}, selectorListOpts)
		framework.ExpectNoError(err, "Deleting collection of mutating webhook configurations")

		ginkgo.By("Creating a configMap that should not be mutated")
		err = wait.PollImmediate(100*time.Millisecond, 30*time.Second, func() (bool, error) {
			cm := namedToBeMutatedConfigMap(string(uuid.NewUUID()), f)
			created, err := client.CoreV1().ConfigMaps(f.Namespace.Name).Create(ctx, cm, metav1.CreateOptions{})
			if err != nil {
				return false, err
			}
			err = client.CoreV1().ConfigMaps(f.Namespace.Name).Delete(ctx, cm.Name, metav1.DeleteOptions{})
			framework.ExpectNoError(err, "Deleting successfully created configMap")
			_, ok := created.Data["mutation-stage-1"]
			return !ok, nil
		})
		framework.ExpectNoError(err, "Waiting for configMap in namespace %s this is not mutated", f.Namespace.Name)
	})

	/*
		Release: v1.28
		Testname: Validating Admission webhook, create and update validating webhook configuration with matchConditions
		Description: 注册一个 validating webhook 配置。验证 matchConditions 字段被 apiserver 正确存储。
		更新该 validating webhook 配置后再次获取；取回的对象 must 包含新更新的 matchConditions 字段。
	*/
	framework.ConformanceIt("should be able to create and update validating webhook configurations with match conditions", func(ctx context.Context) {
		initalMatchConditions := []admissionregistrationv1.MatchCondition{
			{
				Name:       "expression-1",
				Expression: "object.metadata.namespace == 'production'",
			},
		}

		ginkgo.By("creating a validating webhook with match conditions")
		validatingWebhookConfiguration := newValidatingWebhookWithMatchConditions(f, servicePort, certCtx, initalMatchConditions)

		_, err := createValidatingWebhookConfiguration(ctx, f, validatingWebhookConfiguration)
		framework.ExpectNoError(err)

		ginkgo.By("verifying the validating webhook match conditions")
		validatingWebhookConfiguration, err = client.AdmissionregistrationV1().ValidatingWebhookConfigurations().Get(ctx, f.UniqueName, metav1.GetOptions{})
		framework.ExpectNoError(err)
		gomega.Expect(validatingWebhookConfiguration.Webhooks[0].MatchConditions).To(gomega.Equal(initalMatchConditions), "verifying that match conditions are created")
		defer func() {
			err := client.AdmissionregistrationV1().ValidatingWebhookConfigurations().Delete(ctx, validatingWebhookConfiguration.Name, metav1.DeleteOptions{})
			framework.ExpectNoError(err, "deleting mutating webhook configuration")
		}()

		ginkgo.By("updating the validating webhook match conditions")
		updatedMatchConditions := []admissionregistrationv1.MatchCondition{
			{
				Name:       "expression-1",
				Expression: "object.metadata.namespace == 'production'",
			},
			{
				Name:       "expression-2",
				Expression: "object.metadata.namespace == 'staging'",
			},
		}

		updateMatchConditionsFn := func(c *admissionregistrationv1.ValidatingWebhookConfiguration) {
			c.Webhooks[0].MatchConditions = updatedMatchConditions
		}
		_, err = updateValidatingWebhookConfigurations(ctx, client, f.UniqueName, updateMatchConditionsFn)
		framework.ExpectNoError(err, "Updating validating webhook configuration")

		ginkgo.By("verifying the validating webhook match conditions")
		validatingWebhookConfiguration, err = client.AdmissionregistrationV1().ValidatingWebhookConfigurations().Get(ctx, f.UniqueName, metav1.GetOptions{})
		framework.ExpectNoError(err)
		gomega.Expect(validatingWebhookConfiguration.Webhooks[0].MatchConditions).To(gomega.Equal(updatedMatchConditions), "verifying that match conditions are updated")
	})

	/*
		Release: v1.28
		Testname: Mutating Admission webhook, create and update mutating webhook configuration with matchConditions
		Description: 注册一个 mutating webhook 配置。验证 matchConditions 字段被 apiserver 正确存储。
		更新该 mutating webhook 配置后再次获取；取回的对象 must 包含新更新的 matchConditions 字段。
	*/
	framework.ConformanceIt("should be able to create and update mutating webhook configurations with match conditions", func(ctx context.Context) {
		initalMatchConditions := []admissionregistrationv1.MatchCondition{
			{
				Name:       "expression-1",
				Expression: "object.metadata.namespace == 'production'",
			},
		}

		ginkgo.By("creating a mutating webhook with match conditions")
		mutatingWebhookConfiguration := newMutatingWebhookWithMatchConditions(f, servicePort, certCtx, initalMatchConditions)

		_, err := createMutatingWebhookConfiguration(ctx, f, mutatingWebhookConfiguration)
		framework.ExpectNoError(err)

		ginkgo.By("verifying the mutating webhook match conditions")
		mutatingWebhookConfiguration, err = client.AdmissionregistrationV1().MutatingWebhookConfigurations().Get(ctx, f.UniqueName, metav1.GetOptions{})
		framework.ExpectNoError(err)
		gomega.Expect(mutatingWebhookConfiguration.Webhooks[0].MatchConditions).To(gomega.Equal(initalMatchConditions), "verifying that match conditions are created")
		defer func() {
			err := client.AdmissionregistrationV1().MutatingWebhookConfigurations().Delete(ctx, mutatingWebhookConfiguration.Name, metav1.DeleteOptions{})
			framework.ExpectNoError(err, "deleting mutating webhook configuration")
		}()

		ginkgo.By("updating the mutating webhook match conditions")
		updatedMatchConditions := []admissionregistrationv1.MatchCondition{
			{
				Name:       "expression-1",
				Expression: "object.metadata.namespace == 'production'",
			},
			{
				Name:       "expression-2",
				Expression: "object.metadata.namespace == 'staging'",
			},
		}

		updateMatchConditionsFn := func(c *admissionregistrationv1.MutatingWebhookConfiguration) {
			c.Webhooks[0].MatchConditions = updatedMatchConditions
		}
		_, err = updateMutatingWebhookConfigurations(ctx, client, f.UniqueName, updateMatchConditionsFn)
		framework.ExpectNoError(err)

		ginkgo.By("verifying the mutating webhook match conditions")
		mutatingWebhookConfiguration, err = client.AdmissionregistrationV1().MutatingWebhookConfigurations().Get(ctx, f.UniqueName, metav1.GetOptions{})
		framework.ExpectNoError(err)
		gomega.Expect(mutatingWebhookConfiguration.Webhooks[0].MatchConditions).To(gomega.Equal(updatedMatchConditions), "verifying that match conditions are updated")
	})

	/*
		Release: v1.28
		Testname: Validing Admission webhook, reject validating webhook configurations with invalid matchConditions
		Description: 创建一个 matchConditions 字段中含无效 CEL 表达式的 validating webhook 配置。
		apiserver 应拒绝该创建请求，并返回 "compilation failed" 错误消息。
	*/
	framework.ConformanceIt("should reject validating webhook configurations with invalid match conditions", func(ctx context.Context) {
		initalMatchConditions := []admissionregistrationv1.MatchCondition{
			{
				Name:       "invalid-expression-1",
				Expression: "... [] bad expression",
			},
		}

		ginkgo.By("creating a validating webhook with match conditions")
		validatingWebhookConfiguration := newValidatingWebhookWithMatchConditions(f, servicePort, certCtx, initalMatchConditions)

		_, err := createValidatingWebhookConfiguration(ctx, f, validatingWebhookConfiguration)
		gomega.Expect(err).To(gomega.HaveOccurred(), "create validatingwebhookconfiguration should have been denied by the api-server")
		expectedErrMsg := "compilation failed"
		gomega.Expect(err).To(gomega.MatchError(gomega.ContainSubstring(expectedErrMsg)))
	})

	/*
		Release: v1.28
		Testname: Mutating Admission webhook, reject mutating webhook configurations with invalid matchConditions
		Description: 创建一个 matchConditions 字段中含无效 CEL 表达式的 mutating webhook 配置。
		apiserver 应拒绝该创建请求，并返回 "compilation failed" 错误消息。
	*/
	framework.ConformanceIt("should reject mutating webhook configurations with invalid match conditions", func(ctx context.Context) {
		initalMatchConditions := []admissionregistrationv1.MatchCondition{
			{
				Name:       "invalid-expression-1",
				Expression: "... [] bad expression",
			},
		}

		ginkgo.By("creating a mutating webhook with match conditions")
		mutatingWebhookConfiguration := newMutatingWebhookWithMatchConditions(f, servicePort, certCtx, initalMatchConditions)

		_, err := createMutatingWebhookConfiguration(ctx, f, mutatingWebhookConfiguration)
		gomega.Expect(err).To(gomega.HaveOccurred(), "create mutatingwebhookconfiguration should have been denied by the api-server")
		expectedErrMsg := "compilation failed"
		gomega.Expect(err).To(gomega.MatchError(gomega.ContainSubstring(expectedErrMsg)))
	})

	/*
		Release: v1.28
		Testname: Mutating Admission webhook, mutating webhook excluding object with specific name
		Description: 创建一个带 matchConditions 字段的 mutating webhook 配置，拒绝除名为 'skip-me' 的对象外的所有资源。
		创建名为 'skip-me' 的 configmap 并验证它被修改。再创建名称不是 'skip-me' 的 configmap，验证它也被修改。
	*/
	framework.ConformanceIt("should mutate everything except 'skip-me' configmaps", func(ctx context.Context) {
		skipMeMatchConditions := []admissionregistrationv1.MatchCondition{
			{
				Name:       "skip-me",
				Expression: "object.metadata.name != 'skip-me'",
			},
		}

		ginkgo.By("creating a mutating webhook with match conditions")
		namespace := f.Namespace.Name

		mutatingWebhook1 := newMutateConfigMapWebhookFixture(f, certCtx, 1, servicePort)
		mutatingWebhook1.MatchConditions = skipMeMatchConditions
		created, err := createMutatingWebhookConfiguration(ctx, f, &admissionregistrationv1.MutatingWebhookConfiguration{
			ObjectMeta: metav1.ObjectMeta{
				Name: f.UniqueName,
			},
			Webhooks: []admissionregistrationv1.MutatingWebhook{
				mutatingWebhook1,
				// 注册一个可通过 marker 请求探测的 webhook，以判断配置何时就绪。
				newMutatingIsReadyWebhookFixture(f, certCtx, servicePort),
			},
		})
		framework.ExpectNoError(err, "registering mutating webhook config %s with namespace %s", f.UniqueName, namespace)
		defer func() {
			err := client.AdmissionregistrationV1().MutatingWebhookConfigurations().Delete(ctx, created.Name, metav1.DeleteOptions{})
			framework.ExpectNoError(err, "deleting mutating webhook configuration")
		}()

		err = waitWebhookConfigurationReady(ctx, f, markersNamespaceName)
		framework.ExpectNoError(err, "waiting for webhook configuration to be ready")
		ginkgo.DeferCleanup(framework.IgnoreNotFound(client.AdmissionregistrationV1().MutatingWebhookConfigurations().Delete), f.UniqueName, metav1.DeleteOptions{})

		// 继续之前先确认 backend 已就绪
		err = waitWebhookConfigurationReady(ctx, f, markersNamespaceName)
		framework.ExpectNoError(err, "waiting for webhook configuration to be ready")

		ginkgo.By("create the configmap with a random name")

		cm := namedToBeMutatedConfigMap(string(uuid.NewUUID()), f)
		mutatedCM, err := client.CoreV1().ConfigMaps(f.Namespace.Name).Create(ctx, cm, metav1.CreateOptions{})
		framework.ExpectNoError(err, "creating configMap object")

		ginkgo.By("verify the configmap is mutated")
		expectedConfigMapData := map[string]string{
			"mutation-start":   "yes",
			"mutation-stage-1": "yes",
		}
		gomega.Expect(mutatedCM.Data).Should(gomega.Equal(expectedConfigMapData))

		ginkgo.By("create the configmap with 'skip-me' name")

		cm = namedToBeMutatedConfigMap("skip-me", f)
		skippedCM, err := client.CoreV1().ConfigMaps(f.Namespace.Name).Create(ctx, cm, metav1.CreateOptions{})
		framework.ExpectNoError(err, "creating configMap object")
		expectedConfigMapData = map[string]string{
			"mutation-start": "yes",
		}
		gomega.Expect(skippedCM.Data).Should(gomega.Equal(expectedConfigMapData))
	})
})

func newValidatingWebhookWithMatchConditions(
	f *framework.Framework,
	servicePort int32,
	certCtx *certContext,
	matchConditions []admissionregistrationv1.MatchCondition,
) *admissionregistrationv1.ValidatingWebhookConfiguration {
	sideEffects := admissionregistrationv1.SideEffectClassNone
	equivalent := admissionregistrationv1.Equivalent
	return &admissionregistrationv1.ValidatingWebhookConfiguration{
		ObjectMeta: metav1.ObjectMeta{
			Name: f.UniqueName,
		},
		Webhooks: []admissionregistrationv1.ValidatingWebhook{
			{
				Name: "validation-webhook-with-match-conditions.k8s.io",
				Rules: []admissionregistrationv1.RuleWithOperations{{
					Operations: []admissionregistrationv1.OperationType{admissionregistrationv1.Create},
					Rule: admissionregistrationv1.Rule{
						APIGroups:   []string{""},
						APIVersions: []string{"v1"},
						Resources:   []string{"configmaps"},
					},
				}},
				ClientConfig: admissionregistrationv1.WebhookClientConfig{
					Service: &admissionregistrationv1.ServiceReference{
						Namespace: f.Namespace.Name,
						Name:      serviceName,
						Path:      ptr.To("/always-deny"),
						Port:      ptr.To[int32](servicePort),
					},
					CABundle: certCtx.signingCert,
				},
				SideEffects:             &sideEffects,
				MatchPolicy:             &equivalent,
				AdmissionReviewVersions: []string{"v1"},
				// 将 webhook 作用域限制在 markers namespace
				NamespaceSelector: &metav1.LabelSelector{
					MatchLabels: map[string]string{f.UniqueName: "true"},
				},
				MatchConditions: matchConditions,
			},
			newValidatingIsReadyWebhookFixture(f, certCtx, servicePort),
		},
	}
}

func newMutatingWebhookWithMatchConditions(
	f *framework.Framework,
	servicePort int32,
	certCtx *certContext,
	matchConditions []admissionregistrationv1.MatchCondition,
) *admissionregistrationv1.MutatingWebhookConfiguration {
	sideEffects := admissionregistrationv1.SideEffectClassNone
	return &admissionregistrationv1.MutatingWebhookConfiguration{
		ObjectMeta: metav1.ObjectMeta{
			Name: f.UniqueName,
		},
		Webhooks: []admissionregistrationv1.MutatingWebhook{
			{
				Name: "adding-configmap-data.k8s.io",
				Rules: []admissionregistrationv1.RuleWithOperations{{
					Operations: []admissionregistrationv1.OperationType{admissionregistrationv1.Create},
					Rule: admissionregistrationv1.Rule{
						APIGroups:   []string{""},
						APIVersions: []string{"v1"},
						Resources:   []string{"configmaps"},
					},
				}},
				ClientConfig: admissionregistrationv1.WebhookClientConfig{
					Service: &admissionregistrationv1.ServiceReference{
						Namespace: f.Namespace.Name,
						Name:      serviceName,
						Path:      ptr.To("/mutating-configmaps"),
						Port:      ptr.To[int32](servicePort),
					},
					CABundle: certCtx.signingCert,
				},
				SideEffects:             &sideEffects,
				AdmissionReviewVersions: []string{"v1", "v1beta1"},
				// 将 webhook 作用域限制在该 namespace
				NamespaceSelector: &metav1.LabelSelector{
					MatchLabels: map[string]string{f.UniqueName: "true"},
				},
				MatchConditions: matchConditions,
			},
			newMutatingIsReadyWebhookFixture(f, certCtx, servicePort),
		},
	}
}

func createAuthReaderRoleBinding(ctx context.Context, f *framework.Framework, namespace string) {
	ginkgo.By("Create role binding to let webhook read extension-apiserver-authentication")
	client := f.ClientSet
	// 创建 role binding，允许 webhook 读取 extension-apiserver-authentication configmap
	_, err := client.RbacV1().RoleBindings("kube-system").Create(ctx, &rbacv1.RoleBinding{
		ObjectMeta: metav1.ObjectMeta{
			Name: roleBindingName,
			Annotations: map[string]string{
				rbacv1.AutoUpdateAnnotationKey: "true",
			},
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
		framework.Logf("role binding %s already exists", roleBindingName)
	} else {
		framework.ExpectNoError(err, "creating role binding %s:webhook to access configMap", namespace)
	}
}

func deployWebhookAndService(ctx context.Context, f *framework.Framework, image string, certCtx *certContext, servicePort int32, containerPort int32) {
	ginkgo.By("Deploying the webhook pod")
	client := f.ClientSet

	// 创建存放 webhook 证书的 secret
	secret := &v1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name: secretName,
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
	podLabels := map[string]string{"app": "sample-webhook", "webhook": "true"}
	replicas := int32(1)
	mounts := []v1.VolumeMount{
		{
			Name:      "webhook-certs",
			ReadOnly:  true,
			MountPath: "/webhook.local.config/certificates",
		},
	}
	volumes := []v1.Volume{
		{
			Name: "webhook-certs",
			VolumeSource: v1.VolumeSource{
				Secret: &v1.SecretVolumeSource{SecretName: secretName},
			},
		},
	}
	containers := []v1.Container{
		{
			Name:         "sample-webhook",
			VolumeMounts: mounts,
			Args: []string{
				"webhook",
				"--tls-cert-file=/webhook.local.config/certificates/tls.crt",
				"--tls-private-key-file=/webhook.local.config/certificates/tls.key",
				"-v=4",
				// 容器使用非默认端口
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
	d := e2edeployment.NewDeployment(deploymentName, replicas, podLabels, "", "", appsv1.RollingUpdateDeploymentStrategyType)
	d.Spec.Template.Spec.Containers = containers
	d.Spec.Template.Spec.Volumes = volumes

	deployment, err := client.AppsV1().Deployments(namespace).Create(ctx, d, metav1.CreateOptions{})
	framework.ExpectNoError(err, "creating deployment %s in namespace %s", deploymentName, namespace)
	ginkgo.By("Wait for the deployment to be ready")
	err = e2edeployment.WaitForDeploymentRevisionAndImage(client, namespace, deploymentName, "1", image)
	framework.ExpectNoError(err, "waiting for the deployment of image %s in %s in %s to complete", image, deploymentName, namespace)
	err = e2edeployment.WaitForDeploymentComplete(client, deployment)
	framework.ExpectNoError(err, "waiting for the deployment status valid", image, deploymentName, namespace)

	ginkgo.By("Deploying the webhook service")

	serviceLabels := map[string]string{"webhook": "true"}
	service := &v1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: namespace,
			Name:      serviceName,
			Labels:    map[string]string{"test": "webhook"},
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
	framework.ExpectNoError(err, "creating service %s in namespace %s", serviceName, namespace)

	ginkgo.By("Verifying the service has paired with the endpoint")
	err = e2eendpointslice.WaitForEndpointCount(ctx, client, namespace, serviceName, 1)
	framework.ExpectNoError(err, "waiting for service %s/%s have %d endpoint", namespace, serviceName, 1)
}

func registerWebhook(ctx context.Context, f *framework.Framework, markersNamespaceName string, configName string, certCtx *certContext, servicePort int32) {
	client := f.ClientSet
	ginkgo.By("Registering the webhook via the AdmissionRegistration API")

	namespace := f.Namespace.Name
	// 一个无法与 server 通信、且采用 fail-open 策略的 webhook
	failOpenHook := failingWebhook(namespace, "fail-open.k8s.io", servicePort)
	policyIgnore := admissionregistrationv1.Ignore
	failOpenHook.FailurePolicy = &policyIgnore
	failOpenHook.NamespaceSelector = &metav1.LabelSelector{
		MatchLabels: map[string]string{f.UniqueName: "true"},
	}

	_, err := createValidatingWebhookConfiguration(ctx, f, &admissionregistrationv1.ValidatingWebhookConfiguration{
		ObjectMeta: metav1.ObjectMeta{
			Name: configName,
		},
		Webhooks: []admissionregistrationv1.ValidatingWebhook{
			newDenyPodWebhookFixture(f, certCtx, servicePort),
			newDenyConfigMapWebhookFixture(f, certCtx, servicePort),
			// server 无法与该 webhook 通信，因此调用总是失败。
			// 由于该 webhook 配置为 fail-open，调用失败后请求应被放行。
			failOpenHook,

			// 注册一个可通过 marker 请求探测的 webhook，以判断配置何时就绪。
			newValidatingIsReadyWebhookFixture(f, certCtx, servicePort),
		},
	})
	framework.ExpectNoError(err, "registering webhook config %s with namespace %s", configName, namespace)

	err = waitWebhookConfigurationReady(ctx, f, markersNamespaceName)
	framework.ExpectNoError(err, "waiting for webhook configuration to be ready")

	ginkgo.DeferCleanup(framework.IgnoreNotFound(client.AdmissionregistrationV1().ValidatingWebhookConfigurations().Delete), configName, metav1.DeleteOptions{})
}

func registerWebhookForAttachingPod(ctx context.Context, f *framework.Framework, markersNamespaceName string, configName string, certCtx *certContext, servicePort int32) {
	client := f.ClientSet
	ginkgo.By("Registering the webhook via the AdmissionRegistration API")

	namespace := f.Namespace.Name
	sideEffectsNone := admissionregistrationv1.SideEffectClassNone

	_, err := createValidatingWebhookConfiguration(ctx, f, &admissionregistrationv1.ValidatingWebhookConfiguration{
		ObjectMeta: metav1.ObjectMeta{
			Name: configName,
		},
		Webhooks: []admissionregistrationv1.ValidatingWebhook{
			{
				Name: "deny-attaching-pod.k8s.io",
				Rules: []admissionregistrationv1.RuleWithOperations{{
					Operations: []admissionregistrationv1.OperationType{admissionregistrationv1.Connect},
					Rule: admissionregistrationv1.Rule{
						APIGroups:   []string{""},
						APIVersions: []string{"v1"},
						Resources:   []string{"pods/attach"},
					},
				}},
				ClientConfig: admissionregistrationv1.WebhookClientConfig{
					Service: &admissionregistrationv1.ServiceReference{
						Namespace: namespace,
						Name:      serviceName,
						Path:      ptr.To("/pods/attach"),
						Port:      ptr.To[int32](servicePort),
					},
					CABundle: certCtx.signingCert,
				},
				SideEffects:             &sideEffectsNone,
				AdmissionReviewVersions: []string{"v1", "v1beta1"},
				// 将 webhook 作用域限制在该 namespace
				NamespaceSelector: &metav1.LabelSelector{
					MatchLabels: map[string]string{f.UniqueName: "true"},
				},
			},
			// 注册一个可通过 marker 请求探测的 webhook，以判断配置何时就绪。
			newValidatingIsReadyWebhookFixture(f, certCtx, servicePort),
		},
	})
	framework.ExpectNoError(err, "registering webhook config %s with namespace %s", configName, namespace)

	err = waitWebhookConfigurationReady(ctx, f, markersNamespaceName)
	framework.ExpectNoError(err, "waiting for webhook configuration to be ready")

	ginkgo.DeferCleanup(framework.IgnoreNotFound(client.AdmissionregistrationV1().ValidatingWebhookConfigurations().Delete), configName, metav1.DeleteOptions{})
}

func registerMutatingWebhookForConfigMap(ctx context.Context, f *framework.Framework, markersNamespaceName string, configName string, certCtx *certContext, servicePort int32) {
	client := f.ClientSet
	ginkgo.By("Registering the mutating configmap webhook via the AdmissionRegistration API")

	namespace := f.Namespace.Name

	_, err := createMutatingWebhookConfiguration(ctx, f, &admissionregistrationv1.MutatingWebhookConfiguration{
		ObjectMeta: metav1.ObjectMeta{
			Name: configName,
		},
		Webhooks: []admissionregistrationv1.MutatingWebhook{
			newMutateConfigMapWebhookFixture(f, certCtx, 1, servicePort),
			newMutateConfigMapWebhookFixture(f, certCtx, 2, servicePort),
			// 注册一个可通过 marker 请求探测的 webhook，以判断配置何时就绪。
			newMutatingIsReadyWebhookFixture(f, certCtx, servicePort),
		},
	})
	framework.ExpectNoError(err, "registering mutating webhook config %s with namespace %s", configName, namespace)

	err = waitWebhookConfigurationReady(ctx, f, markersNamespaceName)
	framework.ExpectNoError(err, "waiting for webhook configuration to be ready")
	ginkgo.DeferCleanup(framework.IgnoreNotFound(client.AdmissionregistrationV1().MutatingWebhookConfigurations().Delete), configName, metav1.DeleteOptions{})
}

func testMutatingConfigMapWebhook(ctx context.Context, f *framework.Framework) {
	ginkgo.By("create a configmap that should be updated by the webhook")
	client := f.ClientSet
	configMap := toBeMutatedConfigMap(f)
	mutatedConfigMap, err := client.CoreV1().ConfigMaps(f.Namespace.Name).Create(ctx, configMap, metav1.CreateOptions{})
	framework.ExpectNoError(err)
	expectedConfigMapData := map[string]string{
		"mutation-start":   "yes",
		"mutation-stage-1": "yes",
		"mutation-stage-2": "yes",
	}
	if !reflect.DeepEqual(expectedConfigMapData, mutatedConfigMap.Data) {
		framework.Failf("\nexpected %#v\n, got %#v\n", expectedConfigMapData, mutatedConfigMap.Data)
	}
}

func registerMutatingWebhookForPod(ctx context.Context, f *framework.Framework, markersNamespaceName string, configName string, certCtx *certContext, servicePort int32) {
	client := f.ClientSet
	ginkgo.By("Registering the mutating pod webhook via the AdmissionRegistration API")

	namespace := f.Namespace.Name
	sideEffectsNone := admissionregistrationv1.SideEffectClassNone

	_, err := createMutatingWebhookConfiguration(ctx, f, &admissionregistrationv1.MutatingWebhookConfiguration{
		ObjectMeta: metav1.ObjectMeta{
			Name: configName,
		},
		Webhooks: []admissionregistrationv1.MutatingWebhook{
			{
				Name: "adding-init-container.k8s.io",
				Rules: []admissionregistrationv1.RuleWithOperations{{
					Operations: []admissionregistrationv1.OperationType{admissionregistrationv1.Create},
					Rule: admissionregistrationv1.Rule{
						APIGroups:   []string{""},
						APIVersions: []string{"v1"},
						Resources:   []string{"pods"},
					},
				}},
				ClientConfig: admissionregistrationv1.WebhookClientConfig{
					Service: &admissionregistrationv1.ServiceReference{
						Namespace: namespace,
						Name:      serviceName,
						Path:      ptr.To("/mutating-pods"),
						Port:      ptr.To[int32](servicePort),
					},
					CABundle: certCtx.signingCert,
				},
				SideEffects:             &sideEffectsNone,
				AdmissionReviewVersions: []string{"v1", "v1beta1"},
				// 将 webhook 作用域限制在该 namespace
				NamespaceSelector: &metav1.LabelSelector{
					MatchLabels: map[string]string{f.UniqueName: "true"},
				},
			},
			// 注册一个可通过 marker 请求探测的 webhook，以判断配置何时就绪。
			newMutatingIsReadyWebhookFixture(f, certCtx, servicePort),
		},
	})
	framework.ExpectNoError(err, "registering mutating webhook config %s with namespace %s", configName, namespace)

	err = waitWebhookConfigurationReady(ctx, f, markersNamespaceName)
	framework.ExpectNoError(err, "waiting for webhook configuration to be ready")

	ginkgo.DeferCleanup(framework.IgnoreNotFound(client.AdmissionregistrationV1().MutatingWebhookConfigurations().Delete), configName, metav1.DeleteOptions{})
}

func testMutatingPodWebhook(ctx context.Context, f *framework.Framework) {
	ginkgo.By("create a pod that should be updated by the webhook")
	client := f.ClientSet
	pod := toBeMutatedPod(f)
	mutatedPod, err := client.CoreV1().Pods(f.Namespace.Name).Create(ctx, pod, metav1.CreateOptions{})
	framework.ExpectNoError(err)
	if len(mutatedPod.Spec.InitContainers) != 1 {
		framework.Failf("expect pod to have 1 init container, got %#v", mutatedPod.Spec.InitContainers)
	}
	if got, expected := mutatedPod.Spec.InitContainers[0].Name, "webhook-added-init-container"; got != expected {
		framework.Failf("expect the init container name to be %q, got %q", expected, got)
	}
	if got, expected := mutatedPod.Spec.InitContainers[0].TerminationMessagePolicy, v1.TerminationMessageReadFile; got != expected {
		framework.Failf("expect the init terminationMessagePolicy to be default to %q, got %q", expected, got)
	}
}

func toBeMutatedPod(f *framework.Framework) *v1.Pod {
	return &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "webhook-to-be-mutated",
		},
		Spec: v1.PodSpec{
			Containers: []v1.Container{
				{
					Name:  "example",
					Image: imageutils.GetPauseImageName(),
				},
			},
		},
	}
}

func testWebhook(ctx context.Context, f *framework.Framework) {
	ginkgo.By("create a pod that should be denied by the webhook")
	client := f.ClientSet
	// 创建 pod，该请求应被拒绝
	pod := nonCompliantPod(f)
	_, err := client.CoreV1().Pods(f.Namespace.Name).Create(ctx, pod, metav1.CreateOptions{})
	gomega.Expect(err).To(gomega.HaveOccurred(), "create pod %s in namespace %s should have been denied by webhook", pod.Name, f.Namespace.Name)
	expectedErrMsg1 := "the pod contains unwanted container name"
	if !strings.Contains(err.Error(), expectedErrMsg1) {
		framework.Failf("expect error contains %q, got %q", expectedErrMsg1, err.Error())
	}
	expectedErrMsg2 := "the pod contains unwanted label"
	if !strings.Contains(err.Error(), expectedErrMsg2) {
		framework.Failf("expect error contains %q, got %q", expectedErrMsg2, err.Error())
	}

	ginkgo.By("create a pod that causes the webhook to hang")
	client = f.ClientSet
	// 创建 pod，该请求应被拒绝
	pod = hangingPod(f)
	_, err = client.CoreV1().Pods(f.Namespace.Name).Create(ctx, pod, metav1.CreateOptions{})
	gomega.Expect(err).To(gomega.HaveOccurred(), "create pod %s in namespace %s should have caused webhook to hang", pod.Name, f.Namespace.Name)
	// 确认错误来自 webhook，而非客户端
	if !strings.Contains(err.Error(), "webhook") {
		framework.Failf("expect error %q, got %q", "webhook", err.Error())
	}
	// 确认错误是超时
	if !strings.Contains(err.Error(), "deadline") {
		framework.Failf("expect error %q, got %q", "deadline", err.Error())
	}
	// 确认 pod 实际上并未创建
	if _, err := client.CoreV1().Pods(f.Namespace.Name).Get(ctx, pod.Name, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		framework.Failf("expect notfound error looking for rejected pod, got %v", err)
	}

	ginkgo.By("create a configmap that should be denied by the webhook")
	// 创建 configmap，该请求应被拒绝
	configmap := nonCompliantConfigMap(f)
	_, err = client.CoreV1().ConfigMaps(f.Namespace.Name).Create(ctx, configmap, metav1.CreateOptions{})
	gomega.Expect(err).To(gomega.HaveOccurred(), "create configmap %s in namespace %s should have been denied by the webhook", configmap.Name, f.Namespace.Name)
	expectedErrMsg := "the configmap contains unwanted key and value"
	if !strings.Contains(err.Error(), expectedErrMsg) {
		framework.Failf("expect error contains %q, got %q", expectedErrMsg, err.Error())
	}

	ginkgo.By("create a configmap that should be admitted by the webhook")
	// 创建 configmap，该请求应被放行
	configmap = &v1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name: allowedConfigMapName,
		},
		Data: map[string]string{
			"admit": "this",
		},
	}
	_, err = client.CoreV1().ConfigMaps(f.Namespace.Name).Create(ctx, configmap, metav1.CreateOptions{})
	framework.ExpectNoError(err, "failed to create configmap %s in namespace: %s", configmap.Name, f.Namespace.Name)

	ginkgo.By("update (PUT) the admitted configmap to a non-compliant one should be rejected by the webhook")
	toNonCompliantFn := func(cm *v1.ConfigMap) {
		if cm.Data == nil {
			cm.Data = map[string]string{}
		}
		cm.Data["webhook-e2e-test"] = "webhook-disallow"
	}
	_, err = updateConfigMap(ctx, client, f.Namespace.Name, allowedConfigMapName, toNonCompliantFn)
	gomega.Expect(err).To(gomega.HaveOccurred(), "update (PUT) admitted configmap %s in namespace %s to a non-compliant one should be rejected by webhook", allowedConfigMapName, f.Namespace.Name)
	if !strings.Contains(err.Error(), expectedErrMsg) {
		framework.Failf("expect error contains %q, got %q", expectedErrMsg, err.Error())
	}

	ginkgo.By("update (PATCH) the admitted configmap to a non-compliant one should be rejected by the webhook")
	patch := nonCompliantConfigMapPatch()
	_, err = client.CoreV1().ConfigMaps(f.Namespace.Name).Patch(ctx, allowedConfigMapName, types.StrategicMergePatchType, []byte(patch), metav1.PatchOptions{})
	gomega.Expect(err).To(gomega.HaveOccurred(), "update admitted configmap %s in namespace %s by strategic merge patch to a non-compliant one should be rejected by webhook. Patch: %+v", allowedConfigMapName, f.Namespace.Name, patch)
	if !strings.Contains(err.Error(), expectedErrMsg) {
		framework.Failf("expect error contains %q, got %q", expectedErrMsg, err.Error())
	}

	ginkgo.By("create a namespace that bypass the webhook")
	// 测试结束时 framework 会删除 skipNamespace
	skipNamespace, err := f.CreateNamespace(ctx, skipNamespaceBaseName, map[string]string{
		skipNamespaceLabelKey: skipNamespaceLabelValue,
		f.UniqueName:          "true",
	})
	framework.ExpectNoError(err, "creating namespace %q", skipNamespaceBaseName)
	skipNamespaceName := skipNamespace.Name

	ginkgo.By("create a configmap that violates the webhook policy but is in a whitelisted namespace")
	configmap = nonCompliantConfigMap(f)
	_, err = client.CoreV1().ConfigMaps(skipNamespaceName).Create(ctx, configmap, metav1.CreateOptions{})
	framework.ExpectNoError(err, "failed to create configmap %s in namespace: %s", configmap.Name, skipNamespaceName)
}

func testAttachingPodWebhook(ctx context.Context, f *framework.Framework) {
	ginkgo.By("create a pod")
	client := f.ClientSet
	pod := toBeAttachedPod(f)
	_, err := client.CoreV1().Pods(f.Namespace.Name).Create(ctx, pod, metav1.CreateOptions{})
	framework.ExpectNoError(err, "failed to create pod %s in namespace: %s", pod.Name, f.Namespace.Name)
	err = e2epod.WaitForPodNameRunningInNamespace(ctx, client, pod.Name, f.Namespace.Name)
	framework.ExpectNoError(err, "error while waiting for pod %s to go to Running phase in namespace: %s", pod.Name, f.Namespace.Name)

	ginkgo.By("'kubectl attach' the pod, should be denied by the webhook")
	timer := time.NewTimer(30 * time.Second)
	defer timer.Stop()
	_, err = e2ekubectl.NewKubectlCommand(f.Namespace.Name, "attach", fmt.Sprintf("--namespace=%v", f.Namespace.Name), pod.Name, "-i", "-c=container1").WithTimeout(timer.C).Exec()
	gomega.Expect(err).To(gomega.HaveOccurred(), "'kubectl attach' the pod, should be denied by the webhook")
	if e, a := "attaching to pod 'to-be-attached-pod' is not allowed", err.Error(); !strings.Contains(a, e) {
		framework.Failf("unexpected 'kubectl attach' error message. expected to contain %q, got %q", e, a)
	}
}

// failingWebhook 返回一个规则为 create configmap 的 webhook，
// 但其 client 配置无效，server 无法与之通信
func failingWebhook(namespace, name string, servicePort int32) admissionregistrationv1.ValidatingWebhook {
	sideEffectsNone := admissionregistrationv1.SideEffectClassNone

	return admissionregistrationv1.ValidatingWebhook{
		Name: name,
		Rules: []admissionregistrationv1.RuleWithOperations{{
			Operations: []admissionregistrationv1.OperationType{admissionregistrationv1.Create},
			Rule: admissionregistrationv1.Rule{
				APIGroups:   []string{""},
				APIVersions: []string{"v1"},
				Resources:   []string{"configmaps"},
			},
		}},
		ClientConfig: admissionregistrationv1.WebhookClientConfig{
			Service: &admissionregistrationv1.ServiceReference{
				Namespace: namespace,
				Name:      serviceName,
				Path:      ptr.To("/configmaps"),
				Port:      ptr.To[int32](servicePort),
			},
			// 没有 CA bundle 时，对 webhook 的调用总是失败
			CABundle: nil,
		},
		SideEffects:             &sideEffectsNone,
		AdmissionReviewVersions: []string{"v1", "v1beta1"},
	}
}

func registerFailClosedWebhook(ctx context.Context, f *framework.Framework, markersNamespaceName string, configName string, certCtx *certContext, servicePort int32) {
	ginkgo.By("Registering a webhook that server cannot talk to, with fail closed policy, via the AdmissionRegistration API")

	namespace := f.Namespace.Name
	// 一个无法与 server 通信、且采用 fail-closed 策略的 webhook
	policyFail := admissionregistrationv1.Fail
	hook := failingWebhook(namespace, "fail-closed.k8s.io", servicePort)
	hook.FailurePolicy = &policyFail
	hook.NamespaceSelector = &metav1.LabelSelector{
		MatchLabels: map[string]string{f.UniqueName: "true"},
		MatchExpressions: []metav1.LabelSelectorRequirement{
			{
				Key:      failNamespaceLabelKey,
				Operator: metav1.LabelSelectorOpIn,
				Values:   []string{failNamespaceLabelValue},
			},
		},
	}

	_, err := createValidatingWebhookConfiguration(ctx, f, &admissionregistrationv1.ValidatingWebhookConfiguration{
		ObjectMeta: metav1.ObjectMeta{
			Name: configName,
		},
		Webhooks: []admissionregistrationv1.ValidatingWebhook{
			// server 无法与该 webhook 通信，因此调用总是失败。
			// 由于该 webhook 配置为 fail-closed，调用失败后请求应被拒绝。
			hook,
			// 注册一个可通过 marker 请求探测的 webhook，以判断配置何时就绪。
			newValidatingIsReadyWebhookFixture(f, certCtx, servicePort),
		},
	})
	framework.ExpectNoError(err, "registering webhook config %s with namespace %s", configName, namespace)

	err = waitWebhookConfigurationReady(ctx, f, markersNamespaceName)
	framework.ExpectNoError(err, "waiting for webhook configuration to be ready")
	ginkgo.DeferCleanup(framework.IgnoreNotFound(f.ClientSet.AdmissionregistrationV1().ValidatingWebhookConfigurations().Delete), configName, metav1.DeleteOptions{})
}

func testFailClosedWebhook(ctx context.Context, f *framework.Framework) {
	client := f.ClientSet
	ginkgo.By("create a namespace for the webhook")
	// 测试结束时 framework 会删除 failNamespace
	failNamespace, err := f.CreateNamespace(ctx, failNamespaceBaseName, map[string]string{
		failNamespaceLabelKey: failNamespaceLabelValue,
		f.UniqueName:          "true",
	})
	framework.ExpectNoError(err, "creating namespace %q", failNamespaceBaseName)
	failNamespaceName := failNamespace.Name

	ginkgo.By("create a configmap should be unconditionally rejected by the webhook")
	configmap := &v1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name: "foo",
		},
	}
	_, err = client.CoreV1().ConfigMaps(failNamespaceName).Create(ctx, configmap, metav1.CreateOptions{})
	gomega.Expect(err).To(gomega.HaveOccurred(), "create configmap in namespace: %s should be unconditionally rejected by the webhook", failNamespaceName)
	if !apierrors.IsInternalError(err) {
		framework.Failf("expect an internal error, got %#v", err)
	}
}

func registerValidatingWebhookForWebhookConfigurations(ctx context.Context, f *framework.Framework, markersNamespaceName string, configName string, certCtx *certContext, servicePort int32) {
	var err error
	client := f.ClientSet
	ginkgo.By("Registering a validating webhook on ValidatingWebhookConfiguration and MutatingWebhookConfiguration objects, via the AdmissionRegistration API")

	namespace := f.Namespace.Name
	failurePolicy := admissionregistrationv1.Fail
	sideEffectsNone := admissionregistrationv1.SideEffectClassNone

	// 该 webhook 拒绝所有删除 validating/mutating webhook 配置对象的请求。
	// 但它不应被调用，因为涉及 webhook 配置对象的请求不会触发动态 admission webhook。
	_, err = createValidatingWebhookConfiguration(ctx, f, &admissionregistrationv1.ValidatingWebhookConfiguration{
		ObjectMeta: metav1.ObjectMeta{
			Name: configName,
		},
		Webhooks: []admissionregistrationv1.ValidatingWebhook{
			{
				Name: "deny-webhook-configuration-deletions.k8s.io",
				Rules: []admissionregistrationv1.RuleWithOperations{{
					Operations: []admissionregistrationv1.OperationType{admissionregistrationv1.Delete},
					Rule: admissionregistrationv1.Rule{
						APIGroups:   []string{"admissionregistration.k8s.io"},
						APIVersions: []string{"*"},
						Resources: []string{
							"validatingwebhookconfigurations",
							"mutatingwebhookconfigurations",
						},
					},
				}},
				ClientConfig: admissionregistrationv1.WebhookClientConfig{
					Service: &admissionregistrationv1.ServiceReference{
						Namespace: namespace,
						Name:      serviceName,
						Path:      ptr.To("/always-deny"),
						Port:      ptr.To[int32](servicePort),
					},
					CABundle: certCtx.signingCert,
				},
				SideEffects:             &sideEffectsNone,
				AdmissionReviewVersions: []string{"v1", "v1beta1"},
				FailurePolicy:           &failurePolicy,
				// 将 webhook 作用域限制在该 namespace
				NamespaceSelector: &metav1.LabelSelector{
					MatchLabels: map[string]string{f.UniqueName: "true"},
				},
			},
			// 注册一个可通过 marker 请求探测的 webhook，以判断配置何时就绪。
			newValidatingIsReadyWebhookFixture(f, certCtx, servicePort),
		},
	})
	framework.ExpectNoError(err, "registering webhook config %s with namespace %s", configName, namespace)

	err = waitWebhookConfigurationReady(ctx, f, markersNamespaceName)
	framework.ExpectNoError(err, "waiting for webhook configuration to be ready")
	ginkgo.DeferCleanup(framework.IgnoreNotFound(client.AdmissionregistrationV1().ValidatingWebhookConfigurations().Delete), configName, metav1.DeleteOptions{})
}

func registerMutatingWebhookForWebhookConfigurations(ctx context.Context, f *framework.Framework, markersNamespaceName string, configName string, certCtx *certContext, servicePort int32) {
	var err error
	client := f.ClientSet
	ginkgo.By("Registering a mutating webhook on ValidatingWebhookConfiguration and MutatingWebhookConfiguration objects, via the AdmissionRegistration API")

	namespace := f.Namespace.Name
	failurePolicy := admissionregistrationv1.Fail
	sideEffectsNone := admissionregistrationv1.SideEffectClassNone

	// 该 webhook 给所有创建 validating/mutating webhook 配置对象的请求添加 label。
	// 但它不应被调用，因为涉及 webhook 配置对象的请求不会触发动态 admission webhook。
	_, err = createMutatingWebhookConfiguration(ctx, f, &admissionregistrationv1.MutatingWebhookConfiguration{
		ObjectMeta: metav1.ObjectMeta{
			Name: configName,
		},
		Webhooks: []admissionregistrationv1.MutatingWebhook{
			{
				Name: "add-label-to-webhook-configurations.k8s.io",
				Rules: []admissionregistrationv1.RuleWithOperations{{
					Operations: []admissionregistrationv1.OperationType{admissionregistrationv1.Create},
					Rule: admissionregistrationv1.Rule{
						APIGroups:   []string{"admissionregistration.k8s.io"},
						APIVersions: []string{"*"},
						Resources: []string{
							"validatingwebhookconfigurations",
							"mutatingwebhookconfigurations",
						},
					},
				}},
				ClientConfig: admissionregistrationv1.WebhookClientConfig{
					Service: &admissionregistrationv1.ServiceReference{
						Namespace: namespace,
						Name:      serviceName,
						Path:      ptr.To("/add-label"),
						Port:      ptr.To[int32](servicePort),
					},
					CABundle: certCtx.signingCert,
				},
				SideEffects:             &sideEffectsNone,
				AdmissionReviewVersions: []string{"v1", "v1beta1"},
				FailurePolicy:           &failurePolicy,
				// 将 webhook 作用域限制在该 namespace
				NamespaceSelector: &metav1.LabelSelector{
					MatchLabels: map[string]string{f.UniqueName: "true"},
				},
			},
			// 注册一个可通过 marker 请求探测的 webhook，以判断配置何时就绪。
			newMutatingIsReadyWebhookFixture(f, certCtx, servicePort),
		},
	})
	framework.ExpectNoError(err, "registering webhook config %s with namespace %s", configName, namespace)

	err = waitWebhookConfigurationReady(ctx, f, markersNamespaceName)
	framework.ExpectNoError(err, "waiting for webhook configuration to be ready")
	ginkgo.DeferCleanup(framework.IgnoreNotFound(client.AdmissionregistrationV1().MutatingWebhookConfigurations().Delete), configName, metav1.DeleteOptions{})
}

// 该测试假设 registerValidatingWebhookForWebhookConfigurations 中定义的拒绝删除 webhook，
// 以及 registerMutatingWebhookForWebhookConfigurations 中定义的配置修改 webhook 均已存在。
func testWebhooksForWebhookConfigurations(ctx context.Context, f *framework.Framework, markersNamespaceName string, configName string, certCtx *certContext, servicePort int32) {
	var err error
	client := f.ClientSet
	ginkgo.By("Creating a dummy validating-webhook-configuration object")

	namespace := f.Namespace.Name
	failurePolicy := admissionregistrationv1.Ignore
	sideEffectsNone := admissionregistrationv1.SideEffectClassNone

	mutatedValidatingWebhookConfiguration, err := createValidatingWebhookConfiguration(ctx, f, &admissionregistrationv1.ValidatingWebhookConfiguration{
		ObjectMeta: metav1.ObjectMeta{
			Name: configName,
		},
		Webhooks: []admissionregistrationv1.ValidatingWebhook{
			{
				Name: "dummy-validating-webhook.k8s.io",
				Rules: []admissionregistrationv1.RuleWithOperations{{
					Operations: []admissionregistrationv1.OperationType{admissionregistrationv1.Create},
					// 该规则不会匹配任何真实资源，因此该 webhook 不应被调用。
					Rule: admissionregistrationv1.Rule{
						APIGroups:   []string{""},
						APIVersions: []string{"v1"},
						Resources:   []string{"invalid"},
					},
				}},
				ClientConfig: admissionregistrationv1.WebhookClientConfig{
					Service: &admissionregistrationv1.ServiceReference{
						Namespace: namespace,
						Name:      serviceName,
						// webhook service 不识别该路径，因此对该 webhook 的调用总是失败；
						// 但 failure policy 为 ignore，因此不会影响 admission 请求。
						Path: ptr.To(""),
						Port: ptr.To[int32](servicePort),
					},
					CABundle: nil,
				},
				SideEffects:             &sideEffectsNone,
				AdmissionReviewVersions: []string{"v1", "v1beta1"},
				FailurePolicy:           &failurePolicy,
				// 将 webhook 作用域限制在该 namespace
				NamespaceSelector: &metav1.LabelSelector{
					MatchLabels: map[string]string{f.UniqueName: "true"},
				},
			},
			// 注册一个可通过 marker 请求探测的 webhook，以判断配置何时就绪。
			newValidatingIsReadyWebhookFixture(f, certCtx, servicePort),
		},
	})
	framework.ExpectNoError(err, "registering webhook config %s with namespace %s", configName, namespace)
	if mutatedValidatingWebhookConfiguration.ObjectMeta.Labels != nil && mutatedValidatingWebhookConfiguration.ObjectMeta.Labels[addedLabelKey] == addedLabelValue {
		framework.Failf("expected %s not to be mutated by mutating webhooks but it was", configName)
	}

	err = waitWebhookConfigurationReady(ctx, f, markersNamespaceName)
	framework.ExpectNoError(err, "waiting for webhook configuration to be ready")

	ginkgo.By("Deleting the validating-webhook-configuration, which should be possible to remove")

	err = client.AdmissionregistrationV1().ValidatingWebhookConfigurations().Delete(ctx, configName, metav1.DeleteOptions{})
	framework.ExpectNoError(err, "deleting webhook config %s with namespace %s", configName, namespace)

	ginkgo.By("Creating a dummy mutating-webhook-configuration object")

	mutatedMutatingWebhookConfiguration, err := createMutatingWebhookConfiguration(ctx, f, &admissionregistrationv1.MutatingWebhookConfiguration{
		ObjectMeta: metav1.ObjectMeta{
			Name: configName,
		},
		Webhooks: []admissionregistrationv1.MutatingWebhook{
			{
				Name: "dummy-mutating-webhook.k8s.io",
				Rules: []admissionregistrationv1.RuleWithOperations{{
					Operations: []admissionregistrationv1.OperationType{admissionregistrationv1.Create},
					// 该规则不会匹配任何真实资源，因此该 webhook 不应被调用。
					Rule: admissionregistrationv1.Rule{
						APIGroups:   []string{""},
						APIVersions: []string{"v1"},
						Resources:   []string{"invalid"},
					},
				}},
				ClientConfig: admissionregistrationv1.WebhookClientConfig{
					Service: &admissionregistrationv1.ServiceReference{
						Namespace: namespace,
						Name:      serviceName,
						// webhook service 不识别该路径，因此对该 webhook 的调用总是失败；
						// 但 failure policy 为 ignore，因此不会影响 admission 请求。
						Path: ptr.To(""),
						Port: ptr.To[int32](servicePort),
					},
					CABundle: nil,
				},
				SideEffects:             &sideEffectsNone,
				AdmissionReviewVersions: []string{"v1", "v1beta1"},
				FailurePolicy:           &failurePolicy,
				// 将 webhook 作用域限制在该 namespace
				NamespaceSelector: &metav1.LabelSelector{
					MatchLabels: map[string]string{f.UniqueName: "true"},
				},
			},
			// 注册一个可通过 marker 请求探测的 webhook，以判断配置何时就绪。
			newMutatingIsReadyWebhookFixture(f, certCtx, servicePort),
		},
	})
	framework.ExpectNoError(err, "registering webhook config %s with namespace %s", configName, namespace)
	if mutatedMutatingWebhookConfiguration.ObjectMeta.Labels != nil && mutatedMutatingWebhookConfiguration.ObjectMeta.Labels[addedLabelKey] == addedLabelValue {
		framework.Failf("expected %s not to be mutated by mutating webhooks but it was", configName)
	}

	err = waitWebhookConfigurationReady(ctx, f, markersNamespaceName)
	framework.ExpectNoError(err, "waiting for webhook configuration to be ready")

	ginkgo.By("Deleting the mutating-webhook-configuration, which should be possible to remove")

	err = client.AdmissionregistrationV1().MutatingWebhookConfigurations().Delete(ctx, configName, metav1.DeleteOptions{})
	framework.ExpectNoError(err, "deleting webhook config %s with namespace %s", configName, namespace)
}

func nonCompliantPod(f *framework.Framework) *v1.Pod {
	return &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: disallowedPodName,
			Labels: map[string]string{
				"webhook-e2e-test": "webhook-disallow",
			},
		},
		Spec: v1.PodSpec{
			Containers: []v1.Container{
				{
					Name:  "webhook-disallow",
					Image: imageutils.GetPauseImageName(),
				},
			},
		},
	}
}

func hangingPod(f *framework.Framework) *v1.Pod {
	return &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: hangingPodName,
			Labels: map[string]string{
				"webhook-e2e-test": "wait-forever",
			},
		},
		Spec: v1.PodSpec{
			Containers: []v1.Container{
				{
					Name:  "wait-forever",
					Image: imageutils.GetPauseImageName(),
				},
			},
		},
	}
}

func toBeAttachedPod(f *framework.Framework) *v1.Pod {
	return &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: toBeAttachedPodName,
		},
		Spec: v1.PodSpec{
			Containers: []v1.Container{
				{
					Name:  "container1",
					Image: imageutils.GetPauseImageName(),
				},
			},
		},
	}
}

func nonCompliantConfigMap(f *framework.Framework) *v1.ConfigMap {
	return namedNonCompliantConfigMap(disallowedConfigMapName, f)
}

func namedNonCompliantConfigMap(name string, f *framework.Framework) *v1.ConfigMap {
	return &v1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
		},
		Data: map[string]string{
			"webhook-e2e-test": "webhook-disallow",
		},
	}
}

func toBeMutatedConfigMap(f *framework.Framework) *v1.ConfigMap {
	return namedToBeMutatedConfigMap("to-be-mutated", f)
}

func namedToBeMutatedConfigMap(name string, f *framework.Framework) *v1.ConfigMap {
	return &v1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
		},
		Data: map[string]string{
			"mutation-start": "yes",
		},
	}
}

func nonCompliantConfigMapPatch() string {
	return fmt.Sprint(`{"data":{"webhook-e2e-test":"webhook-disallow"}}`)
}

type updateConfigMapFn func(cm *v1.ConfigMap)

func updateConfigMap(ctx context.Context, c clientset.Interface, ns, name string, update updateConfigMapFn) (*v1.ConfigMap, error) {
	var cm *v1.ConfigMap
	pollErr := wait.PollImmediate(2*time.Second, 1*time.Minute, func() (bool, error) {
		var err error
		if cm, err = c.CoreV1().ConfigMaps(ns).Get(ctx, name, metav1.GetOptions{}); err != nil {
			return false, err
		}
		update(cm)
		if cm, err = c.CoreV1().ConfigMaps(ns).Update(ctx, cm, metav1.UpdateOptions{}); err == nil {
			return true, nil
		}
		// 仅在发生冲突时重试 update
		if !apierrors.IsConflict(err) {
			return false, err
		}
		return false, nil
	})
	return cm, pollErr
}

type updateValidatingWebhookConfigurationsFn func(c *admissionregistrationv1.ValidatingWebhookConfiguration)

func updateValidatingWebhookConfigurations(ctx context.Context, client clientset.Interface, name string,
	update updateValidatingWebhookConfigurationsFn) (*admissionregistrationv1.ValidatingWebhookConfiguration, error) {
	var config *admissionregistrationv1.ValidatingWebhookConfiguration
	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		var err error
		config, err = client.AdmissionregistrationV1().ValidatingWebhookConfigurations().Get(ctx, name, metav1.GetOptions{})
		framework.ExpectNoError(err, "Getting validating webhook configuration")
		update(config)
		config, err = client.AdmissionregistrationV1().ValidatingWebhookConfigurations().Update(ctx, config, metav1.UpdateOptions{})
		return err
	})
	return config, err
}

type updateMutatingWebhookConfigurationsFn func(c *admissionregistrationv1.MutatingWebhookConfiguration)

func updateMutatingWebhookConfigurations(ctx context.Context, client clientset.Interface, name string,
	update updateMutatingWebhookConfigurationsFn) (*admissionregistrationv1.MutatingWebhookConfiguration, error) {
	var config *admissionregistrationv1.MutatingWebhookConfiguration
	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		var err error
		config, err = client.AdmissionregistrationV1().MutatingWebhookConfigurations().Get(ctx, name, metav1.GetOptions{})
		framework.ExpectNoError(err, "Getting mutating webhook configuration")
		update(config)
		config, err = client.AdmissionregistrationV1().MutatingWebhookConfigurations().Update(ctx, config, metav1.UpdateOptions{})
		return err
	})
	return config, err
}

type updateCustomResourceFn func(cm *unstructured.Unstructured)

func updateCustomResource(ctx context.Context, c dynamic.ResourceInterface, ns, name string, update updateCustomResourceFn) (*unstructured.Unstructured, error) {
	var cr *unstructured.Unstructured
	pollErr := wait.PollImmediate(2*time.Second, 1*time.Minute, func() (bool, error) {
		var err error
		if cr, err = c.Get(ctx, name, metav1.GetOptions{}); err != nil {
			return false, err
		}
		update(cr)
		if cr, err = c.Update(ctx, cr, metav1.UpdateOptions{}); err == nil {
			return true, nil
		}
		// 仅在发生冲突时重试 update
		if !apierrors.IsConflict(err) {
			return false, err
		}
		return false, nil
	})
	return cr, pollErr
}

func cleanWebhookTest(ctx context.Context, client clientset.Interface, namespaceName string) {
	_ = client.CoreV1().Services(namespaceName).Delete(ctx, serviceName, metav1.DeleteOptions{})
	_ = client.AppsV1().Deployments(namespaceName).Delete(ctx, deploymentName, metav1.DeleteOptions{})
	_ = client.CoreV1().Secrets(namespaceName).Delete(ctx, secretName, metav1.DeleteOptions{})
	_ = client.RbacV1().RoleBindings("kube-system").Delete(ctx, roleBindingName, metav1.DeleteOptions{})
}

func registerWebhookForCustomResource(ctx context.Context, f *framework.Framework, markersNamespaceName string, configName string, certCtx *certContext, testcrd *crd.TestCrd, servicePort int32) {
	client := f.ClientSet
	ginkgo.By("Registering the custom resource webhook via the AdmissionRegistration API")

	namespace := f.Namespace.Name
	sideEffectsNone := admissionregistrationv1.SideEffectClassNone

	_, err := createValidatingWebhookConfiguration(ctx, f, &admissionregistrationv1.ValidatingWebhookConfiguration{
		ObjectMeta: metav1.ObjectMeta{
			Name: configName,
		},
		Webhooks: []admissionregistrationv1.ValidatingWebhook{
			{
				Name: "deny-unwanted-custom-resource-data.k8s.io",
				Rules: []admissionregistrationv1.RuleWithOperations{{
					Operations: []admissionregistrationv1.OperationType{admissionregistrationv1.Create, admissionregistrationv1.Update, admissionregistrationv1.Delete},
					Rule: admissionregistrationv1.Rule{
						APIGroups:   []string{testcrd.Crd.Spec.Group},
						APIVersions: servedAPIVersions(testcrd.Crd),
						Resources:   []string{testcrd.Crd.Spec.Names.Plural},
					},
				}},
				ClientConfig: admissionregistrationv1.WebhookClientConfig{
					Service: &admissionregistrationv1.ServiceReference{
						Namespace: namespace,
						Name:      serviceName,
						Path:      ptr.To("/custom-resource"),
						Port:      ptr.To[int32](servicePort),
					},
					CABundle: certCtx.signingCert,
				},
				SideEffects:             &sideEffectsNone,
				AdmissionReviewVersions: []string{"v1", "v1beta1"},
				// 将 webhook 作用域限制在该 namespace
				NamespaceSelector: &metav1.LabelSelector{
					MatchLabels: map[string]string{f.UniqueName: "true"},
				},
			},
			// 注册一个可通过 marker 请求探测的 webhook，以判断配置何时就绪。
			newValidatingIsReadyWebhookFixture(f, certCtx, servicePort),
		},
	})
	framework.ExpectNoError(err, "registering custom resource webhook config %s with namespace %s", configName, namespace)

	err = waitWebhookConfigurationReady(ctx, f, markersNamespaceName)
	framework.ExpectNoError(err, "waiting for webhook configuration to be ready")
	ginkgo.DeferCleanup(framework.IgnoreNotFound(client.AdmissionregistrationV1().ValidatingWebhookConfigurations().Delete), configName, metav1.DeleteOptions{})
}

func registerMutatingWebhookForCustomResource(ctx context.Context, f *framework.Framework, markersNamespaceName string, configName string, certCtx *certContext, testcrd *crd.TestCrd, servicePort int32) {
	client := f.ClientSet
	ginkgo.By(fmt.Sprintf("Registering the mutating webhook for custom resource %s via the AdmissionRegistration API", testcrd.Crd.Name))

	namespace := f.Namespace.Name
	sideEffectsNone := admissionregistrationv1.SideEffectClassNone

	_, err := createMutatingWebhookConfiguration(ctx, f, &admissionregistrationv1.MutatingWebhookConfiguration{
		ObjectMeta: metav1.ObjectMeta{
			Name: configName,
		},
		Webhooks: []admissionregistrationv1.MutatingWebhook{
			{
				Name: "mutate-custom-resource-data-stage-1.k8s.io",
				Rules: []admissionregistrationv1.RuleWithOperations{{
					Operations: []admissionregistrationv1.OperationType{admissionregistrationv1.Create, admissionregistrationv1.Update},
					Rule: admissionregistrationv1.Rule{
						APIGroups:   []string{testcrd.Crd.Spec.Group},
						APIVersions: servedAPIVersions(testcrd.Crd),
						Resources:   []string{testcrd.Crd.Spec.Names.Plural},
					},
				}},
				ClientConfig: admissionregistrationv1.WebhookClientConfig{
					Service: &admissionregistrationv1.ServiceReference{
						Namespace: namespace,
						Name:      serviceName,
						Path:      ptr.To("/mutating-custom-resource"),
						Port:      ptr.To[int32](servicePort),
					},
					CABundle: certCtx.signingCert,
				},
				SideEffects:             &sideEffectsNone,
				AdmissionReviewVersions: []string{"v1", "v1beta1"},
				// 将 webhook 作用域限制在该 namespace
				NamespaceSelector: &metav1.LabelSelector{
					MatchLabels: map[string]string{f.UniqueName: "true"},
				},
			},
			{
				Name: "mutate-custom-resource-data-stage-2.k8s.io",
				Rules: []admissionregistrationv1.RuleWithOperations{{
					Operations: []admissionregistrationv1.OperationType{admissionregistrationv1.Create},
					Rule: admissionregistrationv1.Rule{
						APIGroups:   []string{testcrd.Crd.Spec.Group},
						APIVersions: servedAPIVersions(testcrd.Crd),
						Resources:   []string{testcrd.Crd.Spec.Names.Plural},
					},
				}},
				ClientConfig: admissionregistrationv1.WebhookClientConfig{
					Service: &admissionregistrationv1.ServiceReference{
						Namespace: namespace,
						Name:      serviceName,
						Path:      ptr.To("/mutating-custom-resource"),
						Port:      ptr.To[int32](servicePort),
					},
					CABundle: certCtx.signingCert,
				},
				SideEffects:             &sideEffectsNone,
				AdmissionReviewVersions: []string{"v1", "v1beta1"},
				// 将 webhook 作用域限制在该 namespace
				NamespaceSelector: &metav1.LabelSelector{
					MatchLabels: map[string]string{f.UniqueName: "true"},
				},
			},
			// 注册一个可通过 marker 请求探测的 webhook，以判断配置何时就绪。
			newMutatingIsReadyWebhookFixture(f, certCtx, servicePort),
		},
	})
	framework.ExpectNoError(err, "registering custom resource webhook config %s with namespace %s", configName, namespace)

	err = waitWebhookConfigurationReady(ctx, f, markersNamespaceName)
	framework.ExpectNoError(err, "waiting for webhook configuration to be ready")

	ginkgo.DeferCleanup(framework.IgnoreNotFound(client.AdmissionregistrationV1().MutatingWebhookConfigurations().Delete), configName, metav1.DeleteOptions{})
}

func testCustomResourceWebhook(ctx context.Context, f *framework.Framework, crd *apiextensionsv1.CustomResourceDefinition, customResourceClient dynamic.ResourceInterface) {
	ginkgo.By("Creating a custom resource that should be denied by the webhook")
	crInstanceName := "cr-instance-1"
	crInstance := &unstructured.Unstructured{
		Object: map[string]interface{}{
			"kind":       crd.Spec.Names.Kind,
			"apiVersion": crd.Spec.Group + "/" + crd.Spec.Versions[0].Name,
			"metadata": map[string]interface{}{
				"name":      crInstanceName,
				"namespace": f.Namespace.Name,
			},
			"data": map[string]interface{}{
				"webhook-e2e-test": "webhook-disallow",
			},
		},
	}
	_, err := customResourceClient.Create(ctx, crInstance, metav1.CreateOptions{})
	gomega.Expect(err).To(gomega.HaveOccurred(), "create custom resource %s in namespace %s should be denied by webhook", crInstanceName, f.Namespace.Name)
	expectedErrMsg := "the custom resource contains unwanted data"
	if !strings.Contains(err.Error(), expectedErrMsg) {
		framework.Failf("expect error contains %q, got %q", expectedErrMsg, err.Error())
	}
}

func testBlockingCustomResourceUpdateDeletion(ctx context.Context, f *framework.Framework, crd *apiextensionsv1.CustomResourceDefinition, customResourceClient dynamic.ResourceInterface) {
	ginkgo.By("Creating a custom resource whose deletion would be denied by the webhook")
	crInstanceName := "cr-instance-2"
	crInstance := &unstructured.Unstructured{
		Object: map[string]interface{}{
			"kind":       crd.Spec.Names.Kind,
			"apiVersion": crd.Spec.Group + "/" + crd.Spec.Versions[0].Name,
			"metadata": map[string]interface{}{
				"name":      crInstanceName,
				"namespace": f.Namespace.Name,
			},
			"data": map[string]interface{}{
				"webhook-e2e-test": "webhook-nondeletable",
			},
		},
	}
	_, err := customResourceClient.Create(ctx, crInstance, metav1.CreateOptions{})
	framework.ExpectNoError(err, "failed to create custom resource %s in namespace: %s", crInstanceName, f.Namespace.Name)

	ginkgo.By("Updating the custom resource with disallowed data should be denied")
	toNonCompliantFn := func(cr *unstructured.Unstructured) {
		if _, ok := cr.Object["data"]; !ok {
			cr.Object["data"] = map[string]interface{}{}
		}
		data := cr.Object["data"].(map[string]interface{})
		data["webhook-e2e-test"] = "webhook-disallow"
	}
	_, err = updateCustomResource(ctx, customResourceClient, f.Namespace.Name, crInstanceName, toNonCompliantFn)
	gomega.Expect(err).To(gomega.HaveOccurred(), "updating custom resource %s in namespace: %s should be denied", crInstanceName, f.Namespace.Name)

	expectedErrMsg := "the custom resource contains unwanted data"
	if !strings.Contains(err.Error(), expectedErrMsg) {
		framework.Failf("expect error contains %q, got %q", expectedErrMsg, err.Error())
	}

	ginkgo.By("Deleting the custom resource should be denied")
	err = customResourceClient.Delete(ctx, crInstanceName, metav1.DeleteOptions{})
	gomega.Expect(err).To(gomega.HaveOccurred(), "deleting custom resource %s in namespace: %s should be denied", crInstanceName, f.Namespace.Name)
	expectedErrMsg1 := "the custom resource cannot be deleted because it contains unwanted key and value"
	if !strings.Contains(err.Error(), expectedErrMsg1) {
		framework.Failf("expect error contains %q, got %q", expectedErrMsg1, err.Error())
	}

	ginkgo.By("Remove the offending key and value from the custom resource data")
	toCompliantFn := func(cr *unstructured.Unstructured) {
		if _, ok := cr.Object["data"]; !ok {
			cr.Object["data"] = map[string]interface{}{}
		}
		data := cr.Object["data"].(map[string]interface{})
		data["webhook-e2e-test"] = "webhook-allow"
	}
	_, err = updateCustomResource(ctx, customResourceClient, f.Namespace.Name, crInstanceName, toCompliantFn)
	framework.ExpectNoError(err, "failed to update custom resource %s in namespace: %s", crInstanceName, f.Namespace.Name)

	ginkgo.By("Deleting the updated custom resource should be successful")
	err = customResourceClient.Delete(ctx, crInstanceName, metav1.DeleteOptions{})
	framework.ExpectNoError(err, "failed to delete custom resource %s in namespace: %s", crInstanceName, f.Namespace.Name)

}

func testMutatingCustomResourceWebhook(ctx context.Context, f *framework.Framework, crd *apiextensionsv1.CustomResourceDefinition, customResourceClient dynamic.ResourceInterface, prune bool) {
	ginkgo.By("Creating a custom resource that should be mutated by the webhook")
	crName := "cr-instance-1"
	cr := &unstructured.Unstructured{
		Object: map[string]interface{}{
			"kind":       crd.Spec.Names.Kind,
			"apiVersion": crd.Spec.Group + "/" + crd.Spec.Versions[0].Name,
			"metadata": map[string]interface{}{
				"name":      crName,
				"namespace": f.Namespace.Name,
			},
			"data": map[string]interface{}{
				"mutation-start": "yes",
			},
		},
	}
	mutatedCR, err := customResourceClient.Create(ctx, cr, metav1.CreateOptions{})
	framework.ExpectNoError(err, "failed to create custom resource %s in namespace: %s", crName, f.Namespace.Name)
	expectedCRData := map[string]interface{}{
		"mutation-start":   "yes",
		"mutation-stage-1": "yes",
	}
	if !prune {
		expectedCRData["mutation-stage-2"] = "yes"
	}
	if !reflect.DeepEqual(expectedCRData, mutatedCR.Object["data"]) {
		framework.Failf("\nexpected %#v\n, got %#v\n", expectedCRData, mutatedCR.Object["data"])
	}
}

func testMultiVersionCustomResourceWebhook(ctx context.Context, f *framework.Framework, testcrd *crd.TestCrd) {
	customResourceClient := testcrd.DynamicClients["v1"]
	ginkgo.By("Creating a custom resource while v1 is storage version")
	crName := "cr-instance-1"
	cr := &unstructured.Unstructured{
		Object: map[string]interface{}{
			"kind":       testcrd.Crd.Spec.Names.Kind,
			"apiVersion": testcrd.Crd.Spec.Group + "/" + testcrd.Crd.Spec.Versions[0].Name,
			"metadata": map[string]interface{}{
				"name":      crName,
				"namespace": f.Namespace.Name,
			},
			"data": map[string]interface{}{
				"mutation-start": "yes",
			},
		},
	}
	_, err := customResourceClient.Create(ctx, cr, metav1.CreateOptions{})
	framework.ExpectNoError(err, "failed to create custom resource %s in namespace: %s", crName, f.Namespace.Name)

	ginkgo.By("Patching Custom Resource Definition to set v2 as storage")
	apiVersionWithV2StoragePatch := `{
		"spec": {
		  "versions": [
		    {
			  "name": "v1",
			  "storage": false,
			  "served": true,
			  "schema": {
			    "openAPIV3Schema": {"x-kubernetes-preserve-unknown-fields": true, "type": "object"}
			  }
            },
		    {
			  "name": "v2",
			  "storage": true,
			  "served": true,
			  "schema": {
			    "openAPIV3Schema": {"x-kubernetes-preserve-unknown-fields": true, "type": "object"}
			  }
            }
          ]
       }
    }`
	_, err = testcrd.APIExtensionClient.ApiextensionsV1().CustomResourceDefinitions().Patch(ctx, testcrd.Crd.Name, types.StrategicMergePatchType, []byte(apiVersionWithV2StoragePatch), metav1.PatchOptions{})
	framework.ExpectNoError(err, "failed to patch custom resource definition %s in namespace: %s", testcrd.Crd.Name, f.Namespace.Name)

	ginkgo.By("Patching the custom resource while v2 is storage version")
	crDummyPatch := fmt.Sprint(`[{ "op": "add", "path": "/dummy", "value": "test" }]`)
	mutatedCR, err := testcrd.DynamicClients["v2"].Patch(ctx, crName, types.JSONPatchType, []byte(crDummyPatch), metav1.PatchOptions{})
	framework.ExpectNoError(err, "failed to patch custom resource %s in namespace: %s", crName, f.Namespace.Name)
	expectedCRData := map[string]interface{}{
		"mutation-start":   "yes",
		"mutation-stage-1": "yes",
		"mutation-stage-2": "yes",
	}
	if !reflect.DeepEqual(expectedCRData, mutatedCR.Object["data"]) {
		framework.Failf("\nexpected %#v\n, got %#v\n", expectedCRData, mutatedCR.Object["data"])
	}
	if !reflect.DeepEqual("test", mutatedCR.Object["dummy"]) {
		framework.Failf("\nexpected %#v\n, got %#v\n", "test", mutatedCR.Object["dummy"])
	}
}

func registerValidatingWebhookForCRD(ctx context.Context, f *framework.Framework, markersNamespaceName string, configName string, certCtx *certContext, servicePort int32) {
	client := f.ClientSet
	ginkgo.By("Registering the crd webhook via the AdmissionRegistration API")

	namespace := f.Namespace.Name
	sideEffectsNone := admissionregistrationv1.SideEffectClassNone

	// 该 webhook 拒绝创建带有 label "webhook-e2e-test":"webhook-disallow" 的 CRD。
	// 注意：测试并行运行且顺序不可预测，其他测试绝不能创建带该 label 的 CRD。
	_, err := createValidatingWebhookConfiguration(ctx, f, &admissionregistrationv1.ValidatingWebhookConfiguration{
		ObjectMeta: metav1.ObjectMeta{
			Name: configName,
		},
		Webhooks: []admissionregistrationv1.ValidatingWebhook{
			{
				Name: "deny-crd-with-unwanted-label.k8s.io",
				Rules: []admissionregistrationv1.RuleWithOperations{{
					Operations: []admissionregistrationv1.OperationType{admissionregistrationv1.Create},
					Rule: admissionregistrationv1.Rule{
						APIGroups:   []string{"apiextensions.k8s.io"},
						APIVersions: []string{"*"},
						Resources:   []string{"customresourcedefinitions"},
					},
				}},
				ClientConfig: admissionregistrationv1.WebhookClientConfig{
					Service: &admissionregistrationv1.ServiceReference{
						Namespace: namespace,
						Name:      serviceName,
						Path:      ptr.To("/crd"),
						Port:      ptr.To[int32](servicePort),
					},
					CABundle: certCtx.signingCert,
				},
				SideEffects:             &sideEffectsNone,
				AdmissionReviewVersions: []string{"v1", "v1beta1"},
				// 将 webhook 作用域限制在该测试
				ObjectSelector: &metav1.LabelSelector{
					MatchLabels: map[string]string{f.UniqueName: "true"},
				},
			},
			// 注册一个可通过 marker 请求探测的 webhook，以判断配置何时就绪。
			newValidatingIsReadyWebhookFixture(f, certCtx, servicePort),
		},
	})
	framework.ExpectNoError(err, "registering crd webhook config %s with namespace %s", configName, namespace)

	err = waitWebhookConfigurationReady(ctx, f, markersNamespaceName)
	framework.ExpectNoError(err, "waiting for webhook configuration to be ready")
	ginkgo.DeferCleanup(framework.IgnoreNotFound(client.AdmissionregistrationV1().ValidatingWebhookConfigurations().Delete), configName, metav1.DeleteOptions{})
}

func testCRDDenyWebhook(ctx context.Context, f *framework.Framework) {
	ginkgo.By("Creating a custom resource definition that should be denied by the webhook")
	name := fmt.Sprintf("e2e-test-%s-%s-crd", f.BaseName, "deny")
	kind := fmt.Sprintf("E2e-test-%s-%s-crd", f.BaseName, "deny")
	group := fmt.Sprintf("%s.example.com", f.BaseName)
	apiVersions := []apiextensionsv1.CustomResourceDefinitionVersion{
		{
			Name:    "v1",
			Served:  true,
			Storage: true,
			Schema: &apiextensionsv1.CustomResourceValidation{
				OpenAPIV3Schema: &apiextensionsv1.JSONSchemaProps{
					XPreserveUnknownFields: ptr.To(true),
					Type:                   "object",
				},
			},
		},
	}

	// 创建供多个测试共用的 CRD
	config, err := framework.LoadConfig()
	if err != nil {
		framework.Failf("failed to load config: %v", err)
		return
	}
	apiExtensionClient, err := crdclientset.NewForConfig(config)
	if err != nil {
		framework.Failf("failed to initialize apiExtensionClient: %v", err)
		return
	}
	crd := &apiextensionsv1.CustomResourceDefinition{
		ObjectMeta: metav1.ObjectMeta{
			Name: name + "s." + group,
			Labels: map[string]string{
				// 该 label 确保对象被路由到本测试的 webhook
				f.UniqueName: "true",
				// 这是 webhook 所禁止的 label
				"webhook-e2e-test": "webhook-disallow",
			},
		},
		Spec: apiextensionsv1.CustomResourceDefinitionSpec{
			Group:    group,
			Versions: apiVersions,
			Names: apiextensionsv1.CustomResourceDefinitionNames{
				Singular: name,
				Kind:     kind,
				ListKind: kind + "List",
				Plural:   name + "s",
			},
			Scope: apiextensionsv1.NamespaceScoped,
		},
	}

	// 创建 CRD
	_, err = apiExtensionClient.ApiextensionsV1().CustomResourceDefinitions().Create(ctx, crd, metav1.CreateOptions{})
	gomega.Expect(err).To(gomega.HaveOccurred(), "create custom resource definition %s should be denied by webhook", crd.Name)
	expectedErrMsg := "the crd contains unwanted label"
	if !strings.Contains(err.Error(), expectedErrMsg) {
		framework.Failf("expect error contains %q, got %q", expectedErrMsg, err.Error())
	}
}

func labelNamespace(ctx context.Context, f *framework.Framework, namespace string) {
	client := f.ClientSet

	// 为 namespace 添加唯一 label
	nsPatch, err := json.Marshal(map[string]interface{}{
		"metadata": map[string]interface{}{
			"labels": map[string]string{f.UniqueName: "true"},
		},
	})
	framework.ExpectNoError(err, "error marshaling namespace %s", namespace)
	_, err = client.CoreV1().Namespaces().Patch(ctx, namespace, types.StrategicMergePatchType, nsPatch, metav1.PatchOptions{})
	framework.ExpectNoError(err, "error labeling namespace %s", namespace)
}

func registerSlowWebhook(ctx context.Context, f *framework.Framework, markersNamespaceName string, configName string, certCtx *certContext, policy *admissionregistrationv1.FailurePolicyType, timeout *int32, servicePort int32) func(ctx context.Context) {
	client := f.ClientSet
	ginkgo.By("Registering slow webhook via the AdmissionRegistration API")

	namespace := f.Namespace.Name
	sideEffectsNone := admissionregistrationv1.SideEffectClassNone

	_, err := createValidatingWebhookConfiguration(ctx, f, &admissionregistrationv1.ValidatingWebhookConfiguration{
		ObjectMeta: metav1.ObjectMeta{
			Name: configName,
		},
		Webhooks: []admissionregistrationv1.ValidatingWebhook{
			{
				Name: "allow-configmap-with-delay-webhook.k8s.io",
				Rules: []admissionregistrationv1.RuleWithOperations{{
					Operations: []admissionregistrationv1.OperationType{admissionregistrationv1.Create},
					Rule: admissionregistrationv1.Rule{
						APIGroups:   []string{""},
						APIVersions: []string{"v1"},
						Resources:   []string{"configmaps"},
					},
				}},
				ClientConfig: admissionregistrationv1.WebhookClientConfig{
					Service: &admissionregistrationv1.ServiceReference{
						Namespace: namespace,
						Name:      serviceName,
						Path:      ptr.To("/always-allow-delay-5s"),
						Port:      ptr.To[int32](servicePort),
					},
					CABundle: certCtx.signingCert,
				},
				// 将 webhook 作用域限制在该 namespace
				NamespaceSelector: &metav1.LabelSelector{
					MatchLabels: map[string]string{f.UniqueName: "true"},
				},
				FailurePolicy:           policy,
				TimeoutSeconds:          timeout,
				SideEffects:             &sideEffectsNone,
				AdmissionReviewVersions: []string{"v1", "v1beta1"},
			},
			// 注册一个可通过 marker 请求探测的 webhook，以判断配置何时就绪。
			newValidatingIsReadyWebhookFixture(f, certCtx, servicePort),
		},
	})
	framework.ExpectNoError(err, "registering slow webhook config %s with namespace %s", configName, namespace)

	err = waitWebhookConfigurationReady(ctx, f, markersNamespaceName)
	framework.ExpectNoError(err, "waiting for webhook configuration to be ready")

	cleanup := func(ctx context.Context) {
		err := client.AdmissionregistrationV1().ValidatingWebhookConfigurations().Delete(ctx, configName, metav1.DeleteOptions{})
		if !apierrors.IsNotFound(err) {
			framework.ExpectNoError(err)
		}
	}

	// 调用方未清理时我们自己兜底清理，但也给调用方在测试中途清理的机会
	ginkgo.DeferCleanup(cleanup)
	return cleanup
}

func testSlowWebhookTimeoutFailEarly(ctx context.Context, f *framework.Framework) {
	ginkgo.By("Request fails when timeout (1s) is shorter than slow webhook latency (5s)")
	client := f.ClientSet
	name := "e2e-test-slow-webhook-configmap"
	_, err := client.CoreV1().ConfigMaps(f.Namespace.Name).Create(ctx, &v1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: name}}, metav1.CreateOptions{})
	gomega.Expect(err).To(gomega.HaveOccurred(), "create configmap in namespace %s should have timed-out reaching slow webhook", f.Namespace.Name)
	// http 超时消息：context deadline exceeded
	// dial 超时消息：dial tcp {address}: i/o timeout
	isTimeoutError := strings.Contains(err.Error(), `context deadline exceeded`) || strings.Contains(err.Error(), `timeout`)
	isErrorQueryingWebhook := strings.Contains(err.Error(), `/always-allow-delay-5s?timeout=1s`)
	if !isTimeoutError || !isErrorQueryingWebhook {
		framework.Failf("expect an HTTP/dial timeout error querying the slow webhook, got: %q", err.Error())
	}
}

func testSlowWebhookTimeoutNoError(ctx context.Context, f *framework.Framework) {
	client := f.ClientSet
	name := "e2e-test-slow-webhook-configmap"
	_, err := client.CoreV1().ConfigMaps(f.Namespace.Name).Create(ctx, &v1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: name}}, metav1.CreateOptions{})
	framework.ExpectNoError(err)
	err = client.CoreV1().ConfigMaps(f.Namespace.Name).Delete(ctx, name, metav1.DeleteOptions{})
	framework.ExpectNoError(err)
}

// createAdmissionWebhookMultiVersionTestCRDWithV1Storage 专门为 admission webhook 调用测试创建 CRD
func createAdmissionWebhookMultiVersionTestCRDWithV1Storage(f *framework.Framework, opts ...crd.Option) (*crd.TestCrd, error) {
	group := fmt.Sprintf("%s.example.com", f.BaseName)
	return crd.CreateMultiVersionTestCRD(f, group, append([]crd.Option{func(crd *apiextensionsv1.CustomResourceDefinition) {
		crd.Spec.Versions = []apiextensionsv1.CustomResourceDefinitionVersion{
			{
				Name:    "v1",
				Served:  true,
				Storage: true,
				Schema: &apiextensionsv1.CustomResourceValidation{
					OpenAPIV3Schema: &apiextensionsv1.JSONSchemaProps{
						XPreserveUnknownFields: ptr.To(true),
						Type:                   "object",
					},
				},
			},
			{
				Name:    "v2",
				Served:  true,
				Storage: false,
				Schema: &apiextensionsv1.CustomResourceValidation{
					OpenAPIV3Schema: &apiextensionsv1.JSONSchemaProps{
						XPreserveUnknownFields: ptr.To(true),
						Type:                   "object",
					},
				},
			},
		}
	}}, opts...)...)
}

// servedAPIVersions 返回 CRD 提供服务的 API 版本
func servedAPIVersions(crd *apiextensionsv1.CustomResourceDefinition) []string {
	ret := []string{}
	for _, v := range crd.Spec.Versions {
		if v.Served {
			ret = append(ret, v.Name)
		}
	}
	return ret
}

// createValidatingWebhookConfiguration 确保 webhook 配置限定了 object/namespace 选择范围，
// 避免干扰其他测试，然后创建该配置
func createValidatingWebhookConfiguration(ctx context.Context, f *framework.Framework, config *admissionregistrationv1.ValidatingWebhookConfiguration) (*admissionregistrationv1.ValidatingWebhookConfiguration, error) {
	for _, webhook := range config.Webhooks {
		if webhook.NamespaceSelector != nil && webhook.NamespaceSelector.MatchLabels[f.UniqueName] == "true" {
			continue
		}
		if webhook.ObjectSelector != nil && webhook.ObjectSelector.MatchLabels[f.UniqueName] == "true" {
			continue
		}
		framework.Failf(`webhook %s in config %s has no namespace or object selector with %s="true", and can interfere with other tests`, webhook.Name, config.Name, f.UniqueName)
	}
	return f.ClientSet.AdmissionregistrationV1().ValidatingWebhookConfigurations().Create(ctx, config, metav1.CreateOptions{})
}

// createMutatingWebhookConfiguration 确保 webhook 配置限定了 object/namespace 选择范围，
// 避免干扰其他测试，然后创建该配置
func createMutatingWebhookConfiguration(ctx context.Context, f *framework.Framework, config *admissionregistrationv1.MutatingWebhookConfiguration) (*admissionregistrationv1.MutatingWebhookConfiguration, error) {
	for _, webhook := range config.Webhooks {
		if webhook.NamespaceSelector != nil && webhook.NamespaceSelector.MatchLabels[f.UniqueName] == "true" {
			continue
		}
		if webhook.ObjectSelector != nil && webhook.ObjectSelector.MatchLabels[f.UniqueName] == "true" {
			continue
		}
		framework.Failf(`webhook %s in config %s has no namespace or object selector with %s="true", and can interfere with other tests`, webhook.Name, config.Name, f.UniqueName)
	}
	return f.ClientSet.AdmissionregistrationV1().MutatingWebhookConfigurations().Create(ctx, config, metav1.CreateOptions{})
}

func newDenyPodWebhookFixture(f *framework.Framework, certCtx *certContext, servicePort int32) admissionregistrationv1.ValidatingWebhook {
	sideEffectsNone := admissionregistrationv1.SideEffectClassNone
	return admissionregistrationv1.ValidatingWebhook{
		Name: "deny-unwanted-pod-container-name-and-label.k8s.io",
		Rules: []admissionregistrationv1.RuleWithOperations{{
			Operations: []admissionregistrationv1.OperationType{admissionregistrationv1.Create},
			Rule: admissionregistrationv1.Rule{
				APIGroups:   []string{""},
				APIVersions: []string{"v1"},
				Resources:   []string{"pods"},
			},
		}},
		ClientConfig: admissionregistrationv1.WebhookClientConfig{
			Service: &admissionregistrationv1.ServiceReference{
				Namespace: f.Namespace.Name,
				Name:      serviceName,
				Path:      ptr.To("/pods"),
				Port:      ptr.To[int32](servicePort),
			},
			CABundle: certCtx.signingCert,
		},
		SideEffects:             &sideEffectsNone,
		AdmissionReviewVersions: []string{"v1", "v1beta1"},
		// 将 webhook 作用域限制在该 namespace
		NamespaceSelector: &metav1.LabelSelector{
			MatchLabels: map[string]string{f.UniqueName: "true"},
		},
	}
}

func newDenyConfigMapWebhookFixture(f *framework.Framework, certCtx *certContext, servicePort int32) admissionregistrationv1.ValidatingWebhook {
	sideEffectsNone := admissionregistrationv1.SideEffectClassNone
	return admissionregistrationv1.ValidatingWebhook{
		Name: "deny-unwanted-configmap-data.k8s.io",
		Rules: []admissionregistrationv1.RuleWithOperations{{
			Operations: []admissionregistrationv1.OperationType{admissionregistrationv1.Create, admissionregistrationv1.Update, admissionregistrationv1.Delete},
			Rule: admissionregistrationv1.Rule{
				APIGroups:   []string{""},
				APIVersions: []string{"v1"},
				Resources:   []string{"configmaps"},
			},
		}},
		// webhook 会跳过带有 label "skip-webhook-admission":"yes" 的 namespace
		NamespaceSelector: &metav1.LabelSelector{
			MatchLabels: map[string]string{f.UniqueName: "true"},
			MatchExpressions: []metav1.LabelSelectorRequirement{
				{
					Key:      skipNamespaceLabelKey,
					Operator: metav1.LabelSelectorOpNotIn,
					Values:   []string{skipNamespaceLabelValue},
				},
			},
		},
		ClientConfig: admissionregistrationv1.WebhookClientConfig{
			Service: &admissionregistrationv1.ServiceReference{
				Namespace: f.Namespace.Name,
				Name:      serviceName,
				Path:      ptr.To("/configmaps"),
				Port:      ptr.To[int32](servicePort),
			},
			CABundle: certCtx.signingCert,
		},
		SideEffects:             &sideEffectsNone,
		AdmissionReviewVersions: []string{"v1", "v1beta1"},
	}
}

func newMutateConfigMapWebhookFixture(f *framework.Framework, certCtx *certContext, stage int, servicePort int32) admissionregistrationv1.MutatingWebhook {
	sideEffectsNone := admissionregistrationv1.SideEffectClassNone
	return admissionregistrationv1.MutatingWebhook{
		Name: fmt.Sprintf("adding-configmap-data-stage-%d.k8s.io", stage),
		Rules: []admissionregistrationv1.RuleWithOperations{{
			Operations: []admissionregistrationv1.OperationType{admissionregistrationv1.Create},
			Rule: admissionregistrationv1.Rule{
				APIGroups:   []string{""},
				APIVersions: []string{"v1"},
				Resources:   []string{"configmaps"},
			},
		}},
		ClientConfig: admissionregistrationv1.WebhookClientConfig{
			Service: &admissionregistrationv1.ServiceReference{
				Namespace: f.Namespace.Name,
				Name:      serviceName,
				Path:      ptr.To("/mutating-configmaps"),
				Port:      ptr.To[int32](servicePort),
			},
			CABundle: certCtx.signingCert,
		},
		SideEffects:             &sideEffectsNone,
		AdmissionReviewVersions: []string{"v1", "v1beta1"},
		// 将 webhook 作用域限制在该 namespace
		NamespaceSelector: &metav1.LabelSelector{
			MatchLabels: map[string]string{f.UniqueName: "true"},
		},
	}
}

// createWebhookConfigurationReadyNamespace 为 webhook 配置就绪 marker 单独创建 namespace，
// 避免与被测的 webhook 配置互相干扰。返回所创建 namespace 的名称。
func createWebhookConfigurationReadyNamespace(ctx context.Context, f *framework.Framework) string {
	baseName := f.BaseName + "-markers"
	// framework 会负责删除该 namespace
	ns, err := f.CreateNamespace(ctx, baseName, map[string]string{
		f.UniqueName + "-markers": "true",
	})
	framework.ExpectNoError(err, "creating namespace for webhook configuration ready markers")
	return ns.Name
}

// waitWebhookConfigurationReady 持续发送 "marker" 请求，直到 webhook 配置就绪。
// 使用 newValidatingIsReadyWebhookFixture 或 newMutatingIsReadyWebhookFixture 创建的 webhook
// 应先加入该 webhook 配置。
func waitWebhookConfigurationReady(ctx context.Context, f *framework.Framework, markersNamespaceName string) error {
	cmClient := f.ClientSet.CoreV1().ConfigMaps(markersNamespaceName)
	return wait.PollImmediate(100*time.Millisecond, 30*time.Second, func() (bool, error) {
		marker := &v1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Name: string(uuid.NewUUID()),
				Labels: map[string]string{
					f.UniqueName: "true",
				},
			},
		}
		_, err := cmClient.Create(ctx, marker, metav1.CreateOptions{})
		if err != nil {
			// always-deny webhook 不提供原因，因此检查预期的错误字符串
			if strings.Contains(err.Error(), "denied") {
				return true, nil
			}
			return false, err
		}
		// 尽力清理不再需要的 marker
		_ = cmClient.Delete(ctx, marker.GetName(), metav1.DeleteOptions{})
		framework.Logf("Waiting for webhook configuration to be ready...")
		return false, nil
	})
}

// newValidatingIsReadyWebhookFixture 创建一个 validating webhook，可加入 webhook 配置后，
// 由 waitWebhookConfigurationReady 通过 "marker" 请求探测其是否就绪
func newValidatingIsReadyWebhookFixture(f *framework.Framework, certCtx *certContext, servicePort int32) admissionregistrationv1.ValidatingWebhook {
	sideEffectsNone := admissionregistrationv1.SideEffectClassNone
	failOpen := admissionregistrationv1.Ignore
	return admissionregistrationv1.ValidatingWebhook{
		Name: "validating-is-webhook-configuration-ready.k8s.io",
		Rules: []admissionregistrationv1.RuleWithOperations{{
			Operations: []admissionregistrationv1.OperationType{admissionregistrationv1.Create},
			Rule: admissionregistrationv1.Rule{
				APIGroups:   []string{""},
				APIVersions: []string{"v1"},
				Resources:   []string{"configmaps"},
			},
		}},
		ClientConfig: admissionregistrationv1.WebhookClientConfig{
			Service: &admissionregistrationv1.ServiceReference{
				Namespace: f.Namespace.Name,
				Name:      serviceName,
				Path:      ptr.To("/always-deny"),
				Port:      ptr.To[int32](servicePort),
			},
			CABundle: certCtx.signingCert,
		},
		// service 网络路由建立期间的网络失败应被 marker 忽略
		FailurePolicy:           &failOpen,
		SideEffects:             &sideEffectsNone,
		AdmissionReviewVersions: []string{"v1", "v1beta1"},
		// 将 webhook 作用域限制在 markers namespace
		NamespaceSelector: &metav1.LabelSelector{
			MatchLabels: map[string]string{f.UniqueName + "-markers": "true"},
		},
		// 满足 createValidatingWebhookConfiguration 的隔离要求
		ObjectSelector: &metav1.LabelSelector{
			MatchLabels: map[string]string{f.UniqueName: "true"},
		},
	}
}

// newMutatingIsReadyWebhookFixture 创建一个 mutating webhook，可加入 webhook 配置后，
// 由 waitWebhookConfigurationReady 通过 "marker" 请求探测其是否就绪
func newMutatingIsReadyWebhookFixture(f *framework.Framework, certCtx *certContext, servicePort int32) admissionregistrationv1.MutatingWebhook {
	sideEffectsNone := admissionregistrationv1.SideEffectClassNone
	failOpen := admissionregistrationv1.Ignore
	return admissionregistrationv1.MutatingWebhook{
		Name: "mutating-is-webhook-configuration-ready.k8s.io",
		Rules: []admissionregistrationv1.RuleWithOperations{{
			Operations: []admissionregistrationv1.OperationType{admissionregistrationv1.Create},
			Rule: admissionregistrationv1.Rule{
				APIGroups:   []string{""},
				APIVersions: []string{"v1"},
				Resources:   []string{"configmaps"},
			},
		}},
		ClientConfig: admissionregistrationv1.WebhookClientConfig{
			Service: &admissionregistrationv1.ServiceReference{
				Namespace: f.Namespace.Name,
				Name:      serviceName,
				Path:      ptr.To("/always-deny"),
				Port:      ptr.To[int32](servicePort),
			},
			CABundle: certCtx.signingCert,
		},
		// service 网络路由建立期间的网络失败应被 marker 忽略
		FailurePolicy:           &failOpen,
		SideEffects:             &sideEffectsNone,
		AdmissionReviewVersions: []string{"v1", "v1beta1"},
		// 将 webhook 作用域限制在 markers namespace
		NamespaceSelector: &metav1.LabelSelector{
			MatchLabels: map[string]string{f.UniqueName + "-markers": "true"},
		},
		// 满足 createMutatingWebhookConfiguration 的隔离要求
		ObjectSelector: &metav1.LabelSelector{
			MatchLabels: map[string]string{f.UniqueName: "true"},
		},
	}
}
