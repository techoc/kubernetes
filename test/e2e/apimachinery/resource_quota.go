/*
Copyright 2015 The Kubernetes Authors.

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
	"strconv"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	resourceapi "k8s.io/api/resource/v1"
	schedulingv1 "k8s.io/api/scheduling/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	utilrand "k8s.io/apimachinery/pkg/util/rand"
	"k8s.io/apimachinery/pkg/util/resourceversion"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/apimachinery/pkg/watch"
	quota "k8s.io/apiserver/pkg/quota/v1"
	clientset "k8s.io/client-go/kubernetes"
	clientscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/cache"
	watchtools "k8s.io/client-go/tools/watch"
	"k8s.io/client-go/util/retry"
	"k8s.io/kubernetes/pkg/features"
	"k8s.io/kubernetes/pkg/quota/v1/evaluator/core"
	apimachineryutils "k8s.io/kubernetes/test/e2e/common/apimachinery"
	"k8s.io/kubernetes/test/e2e/feature"
	"k8s.io/kubernetes/test/e2e/framework"
	e2epv "k8s.io/kubernetes/test/e2e/framework/pv"
	"k8s.io/kubernetes/test/utils/crd"
	imageutils "k8s.io/kubernetes/test/utils/image"
	"k8s.io/kubernetes/test/utils/ktesting/format"
	admissionapi "k8s.io/pod-security-admission/api"
	"k8s.io/utils/ptr"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	"github.com/onsi/gomega/gcustom"
	gomegatypes "github.com/onsi/gomega/types"
)

const (
	// 等待 resource quota 更新的超时时间
	resourceQuotaTimeout = time.Minute
	podName              = "pfpod"
)

var classGold = "gold"
var classSilver = "silver"
var extendedResourceName = "example.com/dongle"

var _ = SIGDescribe("ResourceQuota", func() {
	f := framework.NewDefaultFramework("resourcequota")
	f.NamespacePodSecurityLevel = admissionapi.LevelBaseline

	/*
		发布版本：v1.16
		测试名称：ResourceQuota, object count quota, resourcequotas
		描述：创建 ResourceQuota。创建必须成功，其 ResourceQuotaStatus 必须与 namespace 内预期已用和总允许配额数一致。
	*/
	framework.ConformanceIt("should create a ResourceQuota and ensure its status is promptly calculated.", func(ctx context.Context) {
		ginkgo.By("Counting existing ResourceQuota")
		c, err := countResourceQuota(ctx, f.ClientSet, f.Namespace.Name)
		framework.ExpectNoError(err)

		ginkgo.By("Creating a ResourceQuota")
		quotaName := "test-quota"
		resourceQuota := newTestResourceQuota(quotaName)
		_, err = createResourceQuota(ctx, f.ClientSet, f.Namespace.Name, resourceQuota)
		framework.ExpectNoError(err)

		ginkgo.By("Ensuring resource quota status is calculated")
		usedResources := v1.ResourceList{}
		usedResources[v1.ResourceQuotas] = resource.MustParse(strconv.Itoa(c + 1))
		err = waitForResourceQuota(ctx, f.ClientSet, f.Namespace.Name, quotaName, usedResources)
		framework.ExpectNoError(err)
	})

	/*
		发布版本：v1.16
		测试名称：ResourceQuota, object count quota, service
		描述：创建 ResourceQuota。创建必须成功，其 ResourceQuotaStatus 必须与 namespace 内预期已用和总允许配额数一致。
		创建 Service。创建必须成功，Service 对象和 resourceQuota 对象的资源使用计数必须记录在 ResourceQuota 的 ResourceQuotaStatus 中。
		删除 Service。删除必须成功，Service 对象的资源使用计数必须从 ResourceQuota 的 ResourceQuotaStatus 中释放。
	*/
	framework.ConformanceIt("should create a ResourceQuota and capture the life of a service.", func(ctx context.Context) {
		ginkgo.By("Counting existing ResourceQuota")
		c, err := countResourceQuota(ctx, f.ClientSet, f.Namespace.Name)
		framework.ExpectNoError(err)

		ginkgo.By("Creating a ResourceQuota")
		quotaName := "test-quota"
		resourceQuota := newTestResourceQuota(quotaName)
		_, err = createResourceQuota(ctx, f.ClientSet, f.Namespace.Name, resourceQuota)
		framework.ExpectNoError(err)

		ginkgo.By("Ensuring resource quota status is calculated")
		usedResources := v1.ResourceList{}
		usedResources[v1.ResourceQuotas] = resource.MustParse(strconv.Itoa(c + 1))
		err = waitForResourceQuota(ctx, f.ClientSet, f.Namespace.Name, quotaName, usedResources)
		framework.ExpectNoError(err)

		ginkgo.By("Creating a Service")
		service := newTestServiceForQuota("test-service", v1.ServiceTypeClusterIP, false)
		service, err = f.ClientSet.CoreV1().Services(f.Namespace.Name).Create(ctx, service, metav1.CreateOptions{})
		framework.ExpectNoError(err)

		ginkgo.By("Creating a NodePort Service")
		nodeport := newTestServiceForQuota("test-service-np", v1.ServiceTypeNodePort, false)
		nodeport, err = f.ClientSet.CoreV1().Services(f.Namespace.Name).Create(ctx, nodeport, metav1.CreateOptions{})
		framework.ExpectNoError(err)

		ginkgo.By("Not allowing a LoadBalancer Service with NodePort to be created that exceeds remaining quota")
		loadbalancer := newTestServiceForQuota("test-service-lb", v1.ServiceTypeLoadBalancer, true)
		_, err = f.ClientSet.CoreV1().Services(f.Namespace.Name).Create(ctx, loadbalancer, metav1.CreateOptions{})
		gomega.Expect(err).To(gomega.HaveOccurred())

		ginkgo.By("Ensuring resource quota status captures service creation")
		usedResources = v1.ResourceList{}
		usedResources[v1.ResourceQuotas] = resource.MustParse(strconv.Itoa(c + 1))
		usedResources[v1.ResourceServices] = resource.MustParse("2")
		usedResources[v1.ResourceServicesNodePorts] = resource.MustParse("1")
		err = waitForResourceQuota(ctx, f.ClientSet, f.Namespace.Name, quotaName, usedResources)
		framework.ExpectNoError(err)

		ginkgo.By("Deleting Services")
		err = f.ClientSet.CoreV1().Services(f.Namespace.Name).Delete(ctx, service.Name, metav1.DeleteOptions{})
		framework.ExpectNoError(err)
		err = f.ClientSet.CoreV1().Services(f.Namespace.Name).Delete(ctx, nodeport.Name, metav1.DeleteOptions{})
		framework.ExpectNoError(err)

		ginkgo.By("Ensuring resource quota status released usage")
		usedResources[v1.ResourceServices] = resource.MustParse("0")
		usedResources[v1.ResourceServicesNodePorts] = resource.MustParse("0")
		err = waitForResourceQuota(ctx, f.ClientSet, f.Namespace.Name, quotaName, usedResources)
		framework.ExpectNoError(err)
	})

	/*
		发布版本：v1.16
		测试名称：ResourceQuota, object count quota, secret
		描述：创建 ResourceQuota。创建必须成功，其 ResourceQuotaStatus 必须与 namespace 内预期已用和总允许配额数一致。
		创建 Secret。创建必须成功，Secret 对象和 resourceQuota 对象的资源使用计数必须记录在 ResourceQuota 的 ResourceQuotaStatus 中。
		删除 Secret。删除必须成功，Secret 对象的资源使用计数必须从 ResourceQuota 的 ResourceQuotaStatus 中释放。
	*/
	framework.ConformanceIt("should create a ResourceQuota and capture the life of a secret.", func(ctx context.Context) {
		ginkgo.By("Discovering how many secrets are in namespace by default")
		found, unchanged := 0, 0
		// 高负载的服务器上 service account controller 可能变慢，导致测试运行期间计数发生变化。
		// 最多等 5 秒让计数稳定，前提是更新以稳定速率到来且不会无限期停滞。
		err := wait.PollUntilContextTimeout(ctx, 1*time.Second, 30*time.Second, false, func(ctx context.Context) (bool, error) {
			secrets, err := f.ClientSet.CoreV1().Secrets(f.Namespace.Name).List(ctx, metav1.ListOptions{})
			framework.ExpectNoError(err)
			if len(secrets.Items) == found {
				// 循环等待 secret 数量稳定 5 秒
				unchanged++
				return unchanged > 4, nil
			}
			unchanged = 0
			found = len(secrets.Items)
			return false, nil
		})
		framework.ExpectNoError(err)
		defaultSecrets := fmt.Sprintf("%d", found)
		hardSecrets := fmt.Sprintf("%d", found+1)

		ginkgo.By("Counting existing ResourceQuota")
		c, err := countResourceQuota(ctx, f.ClientSet, f.Namespace.Name)
		framework.ExpectNoError(err)

		ginkgo.By("Creating a ResourceQuota")
		quotaName := "test-quota"
		resourceQuota := newTestResourceQuota(quotaName)
		resourceQuota.Spec.Hard[v1.ResourceSecrets] = resource.MustParse(hardSecrets)
		_, err = createResourceQuota(ctx, f.ClientSet, f.Namespace.Name, resourceQuota)
		framework.ExpectNoError(err)

		ginkgo.By("Ensuring resource quota status is calculated")
		usedResources := v1.ResourceList{}
		usedResources[v1.ResourceQuotas] = resource.MustParse(strconv.Itoa(c + 1))
		usedResources[v1.ResourceSecrets] = resource.MustParse(defaultSecrets)
		err = waitForResourceQuota(ctx, f.ClientSet, f.Namespace.Name, quotaName, usedResources)
		framework.ExpectNoError(err)

		ginkgo.By("Creating a Secret")
		secret := newTestSecretForQuota("test-secret")
		secret, err = f.ClientSet.CoreV1().Secrets(f.Namespace.Name).Create(ctx, secret, metav1.CreateOptions{})
		framework.ExpectNoError(err)

		ginkgo.By("Ensuring resource quota status captures secret creation")
		usedResources = v1.ResourceList{}
		usedResources[v1.ResourceSecrets] = resource.MustParse(hardSecrets)
		// 预期有两个 secret：每个 namespace 默认会收到
		// 一个 service account token secret
		err = waitForResourceQuota(ctx, f.ClientSet, f.Namespace.Name, quotaName, usedResources)
		framework.ExpectNoError(err)

		ginkgo.By("Deleting a secret")
		err = f.ClientSet.CoreV1().Secrets(f.Namespace.Name).Delete(ctx, secret.Name, metav1.DeleteOptions{})
		framework.ExpectNoError(err)

		ginkgo.By("Ensuring resource quota status released usage")
		usedResources[v1.ResourceSecrets] = resource.MustParse(defaultSecrets)
		err = waitForResourceQuota(ctx, f.ClientSet, f.Namespace.Name, quotaName, usedResources)
		framework.ExpectNoError(err)
	})

	/*
		发布版本：v1.16
		测试名称：ResourceQuota, object count quota, pod
		描述：创建 ResourceQuota。创建必须成功，其 ResourceQuotaStatus 必须与 namespace 内预期已用和总允许配额数一致。
		创建带 CPU、Memory、EphemeralStorage 和 ExtendedResourceName 资源请求的 Pod。Pod 创建必须成功，各资源使用计数必须记录在 ResourceQuota 的 ResourceQuotaStatus 中。
		再创建一个资源请求超出剩余配额的 Pod。请求超过 ResourceQuota 限制，Pod 创建必须失败。
		更新已成功创建 Pod 的资源请求。更新必须失败，因为 Pod 不能动态修改资源需求。
		删除成功创建的 Pod。删除必须成功，且必须从 ResourceQuota 的 ResourceQuotaStatus 中释放已分配的资源计数。
	*/
	framework.ConformanceIt("should create a ResourceQuota and capture the life of a pod.", func(ctx context.Context) {
		ginkgo.By("Counting existing ResourceQuota")
		c, err := countResourceQuota(ctx, f.ClientSet, f.Namespace.Name)
		framework.ExpectNoError(err)

		ginkgo.By("Creating a ResourceQuota")
		quotaName := "test-quota"
		resourceQuota := newTestResourceQuota(quotaName)
		_, err = createResourceQuota(ctx, f.ClientSet, f.Namespace.Name, resourceQuota)
		framework.ExpectNoError(err)

		ginkgo.By("Ensuring resource quota status is calculated")
		usedResources := v1.ResourceList{}
		usedResources[v1.ResourceQuotas] = resource.MustParse(strconv.Itoa(c + 1))
		err = waitForResourceQuota(ctx, f.ClientSet, f.Namespace.Name, quotaName, usedResources)
		framework.ExpectNoError(err)

		ginkgo.By("Creating a Pod that fits quota")
		podName := "test-pod"
		requests := v1.ResourceList{}
		limits := v1.ResourceList{}
		requests[v1.ResourceCPU] = resource.MustParse("500m")
		requests[v1.ResourceMemory] = resource.MustParse("252Mi")
		requests[v1.ResourceEphemeralStorage] = resource.MustParse("30Gi")
		requests[v1.ResourceName(extendedResourceName)] = resource.MustParse("2")
		limits[v1.ResourceName(extendedResourceName)] = resource.MustParse("2")
		pod := newTestPodForQuota(f, podName, requests, limits)
		pod, err = f.ClientSet.CoreV1().Pods(f.Namespace.Name).Create(ctx, pod, metav1.CreateOptions{})
		framework.ExpectNoError(err)
		podToUpdate := pod

		ginkgo.By("Ensuring ResourceQuota status captures the pod usage")
		usedResources[v1.ResourceQuotas] = resource.MustParse(strconv.Itoa(c + 1))
		usedResources[v1.ResourcePods] = resource.MustParse("1")
		usedResources[v1.ResourceCPU] = requests[v1.ResourceCPU]
		usedResources[v1.ResourceMemory] = requests[v1.ResourceMemory]
		usedResources[v1.ResourceEphemeralStorage] = requests[v1.ResourceEphemeralStorage]
		usedResources[v1.ResourceName(v1.DefaultResourceRequestsPrefix+extendedResourceName)] = requests[v1.ResourceName(extendedResourceName)]
		err = waitForResourceQuota(ctx, f.ClientSet, f.Namespace.Name, quotaName, usedResources)
		framework.ExpectNoError(err)

		ginkgo.By("Not allowing a pod to be created that exceeds remaining quota")
		requests = v1.ResourceList{}
		requests[v1.ResourceCPU] = resource.MustParse("600m")
		requests[v1.ResourceMemory] = resource.MustParse("100Mi")
		pod = newTestPodForQuota(f, "fail-pod", requests, v1.ResourceList{})
		_, err = f.ClientSet.CoreV1().Pods(f.Namespace.Name).Create(ctx, pod, metav1.CreateOptions{})
		gomega.Expect(err).To(gomega.HaveOccurred())

		ginkgo.By("Not allowing a pod to be created that exceeds remaining quota(validation on extended resources)")
		requests = v1.ResourceList{}
		limits = v1.ResourceList{}
		requests[v1.ResourceCPU] = resource.MustParse("500m")
		requests[v1.ResourceMemory] = resource.MustParse("100Mi")
		requests[v1.ResourceEphemeralStorage] = resource.MustParse("30Gi")
		requests[v1.ResourceName(extendedResourceName)] = resource.MustParse("2")
		limits[v1.ResourceName(extendedResourceName)] = resource.MustParse("2")
		pod = newTestPodForQuota(f, "fail-pod-for-extended-resource", requests, limits)
		_, err = f.ClientSet.CoreV1().Pods(f.Namespace.Name).Create(ctx, pod, metav1.CreateOptions{})
		gomega.Expect(err).To(gomega.HaveOccurred())

		ginkgo.By("Ensuring a pod cannot update its resource requirements")
		// pod 无法动态更新资源需求。
		requests = v1.ResourceList{}
		requests[v1.ResourceCPU] = resource.MustParse("100m")
		requests[v1.ResourceMemory] = resource.MustParse("100Mi")
		requests[v1.ResourceEphemeralStorage] = resource.MustParse("10Gi")
		podToUpdate.Spec.Containers[0].Resources.Requests = requests
		_, err = f.ClientSet.CoreV1().Pods(f.Namespace.Name).Update(ctx, podToUpdate, metav1.UpdateOptions{})
		gomega.Expect(err).To(gomega.HaveOccurred())

		ginkgo.By("Ensuring attempts to update pod resource requirements did not change quota usage")
		err = waitForResourceQuota(ctx, f.ClientSet, f.Namespace.Name, quotaName, usedResources)
		framework.ExpectNoError(err)

		ginkgo.By("Deleting the pod")
		err = f.ClientSet.CoreV1().Pods(f.Namespace.Name).Delete(ctx, podName, *metav1.NewDeleteOptions(0))
		framework.ExpectNoError(err)

		ginkgo.By("Ensuring resource quota status released the pod usage")
		usedResources[v1.ResourceQuotas] = resource.MustParse(strconv.Itoa(c + 1))
		usedResources[v1.ResourcePods] = resource.MustParse("0")
		usedResources[v1.ResourceCPU] = resource.MustParse("0")
		usedResources[v1.ResourceMemory] = resource.MustParse("0")
		usedResources[v1.ResourceEphemeralStorage] = resource.MustParse("0")
		usedResources[v1.ResourceName(v1.DefaultResourceRequestsPrefix+extendedResourceName)] = resource.MustParse("0")
		err = waitForResourceQuota(ctx, f.ClientSet, f.Namespace.Name, quotaName, usedResources)
		framework.ExpectNoError(err)
	})
	/*
		发布版本：v1.16
		测试名称：ResourceQuota, object count quota, configmap
		描述：创建 ResourceQuota。创建必须成功，其 ResourceQuotaStatus 必须与 namespace 内预期已用和总允许配额数一致。
		创建 ConfigMap。创建必须成功，ConfigMap 对象的资源使用计数必须记录在 ResourceQuota 的 ResourceQuotaStatus 中。
		删除 ConfigMap。删除必须成功，ConfigMap 对象的资源使用计数必须从 ResourceQuota 的 ResourceQuotaStatus 中释放。
	*/
	framework.ConformanceIt("should create a ResourceQuota and capture the life of a configMap.", func(ctx context.Context) {
		found, unchanged := 0, 0
		// 高负载的服务器上 service account controller 可能变慢，导致测试运行期间计数发生变化。
		// 最多等 15 秒让计数稳定，前提是更新以稳定速率到来且不会无限期停滞。
		err := wait.PollUntilContextTimeout(ctx, 1*time.Second, time.Minute, false, func(ctx context.Context) (bool, error) {
			configmaps, err := f.ClientSet.CoreV1().ConfigMaps(f.Namespace.Name).List(ctx, metav1.ListOptions{})
			framework.ExpectNoError(err)
			if len(configmaps.Items) == found {
				// 循环等待 configmap 数量稳定 15 秒
				unchanged++
				return unchanged > 15, nil
			}
			unchanged = 0
			found = len(configmaps.Items)
			return false, nil
		})
		framework.ExpectNoError(err)
		defaultConfigMaps := fmt.Sprintf("%d", found)
		hardConfigMaps := fmt.Sprintf("%d", found+1)

		ginkgo.By("Counting existing ResourceQuota")
		c, err := countResourceQuota(ctx, f.ClientSet, f.Namespace.Name)
		framework.ExpectNoError(err)

		ginkgo.By("Creating a ResourceQuota")
		quotaName := "test-quota"
		resourceQuota := newTestResourceQuota(quotaName)
		resourceQuota.Spec.Hard[v1.ResourceConfigMaps] = resource.MustParse(hardConfigMaps)
		_, err = createResourceQuota(ctx, f.ClientSet, f.Namespace.Name, resourceQuota)
		framework.ExpectNoError(err)

		ginkgo.By("Ensuring resource quota status is calculated")
		usedResources := v1.ResourceList{}
		usedResources[v1.ResourceQuotas] = resource.MustParse(strconv.Itoa(c + 1))
		usedResources[v1.ResourceConfigMaps] = resource.MustParse(defaultConfigMaps)
		err = waitForResourceQuota(ctx, f.ClientSet, f.Namespace.Name, quotaName, usedResources)
		framework.ExpectNoError(err)

		ginkgo.By("Creating a ConfigMap")
		configMap := newTestConfigMapForQuota("test-configmap")
		configMap, err = f.ClientSet.CoreV1().ConfigMaps(f.Namespace.Name).Create(ctx, configMap, metav1.CreateOptions{})
		framework.ExpectNoError(err)

		ginkgo.By("Ensuring resource quota status captures configMap creation")
		usedResources = v1.ResourceList{}
		usedResources[v1.ResourceQuotas] = resource.MustParse(strconv.Itoa(c + 1))
		usedResources[v1.ResourceConfigMaps] = resource.MustParse(hardConfigMaps)
		err = waitForResourceQuota(ctx, f.ClientSet, f.Namespace.Name, quotaName, usedResources)
		framework.ExpectNoError(err)

		ginkgo.By("Deleting a ConfigMap")
		err = f.ClientSet.CoreV1().ConfigMaps(f.Namespace.Name).Delete(ctx, configMap.Name, metav1.DeleteOptions{})
		framework.ExpectNoError(err)

		ginkgo.By("Ensuring resource quota status released usage")
		usedResources[v1.ResourceConfigMaps] = resource.MustParse(defaultConfigMaps)
		err = waitForResourceQuota(ctx, f.ClientSet, f.Namespace.Name, quotaName, usedResources)
		framework.ExpectNoError(err)
	})

	/*
		发布版本：v1.16
		测试名称：ResourceQuota, object count quota, replicationController
		描述：创建 ResourceQuota。创建必须成功，其 ResourceQuotaStatus 必须与 namespace 内预期已用和总允许配额数一致。
		创建 ReplicationController。创建必须成功，ReplicationController 对象的资源使用计数必须记录在 ResourceQuota 的 ResourceQuotaStatus 中。
		删除 ReplicationController。删除必须成功，ReplicationController 对象的资源使用计数必须从 ResourceQuota 的 ResourceQuotaStatus 中释放。
	*/
	framework.ConformanceIt("should create a ResourceQuota and capture the life of a replication controller.", func(ctx context.Context) {
		ginkgo.By("Counting existing ResourceQuota")
		c, err := countResourceQuota(ctx, f.ClientSet, f.Namespace.Name)
		framework.ExpectNoError(err)

		ginkgo.By("Creating a ResourceQuota")
		quotaName := "test-quota"
		resourceQuota := newTestResourceQuota(quotaName)
		_, err = createResourceQuota(ctx, f.ClientSet, f.Namespace.Name, resourceQuota)
		framework.ExpectNoError(err)

		ginkgo.By("Ensuring resource quota status is calculated")
		usedResources := v1.ResourceList{}
		usedResources[v1.ResourceQuotas] = resource.MustParse(strconv.Itoa(c + 1))
		usedResources[v1.ResourceReplicationControllers] = resource.MustParse("0")
		err = waitForResourceQuota(ctx, f.ClientSet, f.Namespace.Name, quotaName, usedResources)
		framework.ExpectNoError(err)

		ginkgo.By("Creating a ReplicationController")
		replicationController := newTestReplicationControllerForQuota("test-rc", "nginx", 0)
		replicationController, err = f.ClientSet.CoreV1().ReplicationControllers(f.Namespace.Name).Create(ctx, replicationController, metav1.CreateOptions{})
		framework.ExpectNoError(err)

		ginkgo.By("Ensuring resource quota status captures replication controller creation")
		usedResources = v1.ResourceList{}
		usedResources[v1.ResourceReplicationControllers] = resource.MustParse("1")
		err = waitForResourceQuota(ctx, f.ClientSet, f.Namespace.Name, quotaName, usedResources)
		framework.ExpectNoError(err)

		ginkgo.By("Deleting a ReplicationController")
		// 不带 delete options 时，对象要等 GC 确认
		// 所有子对象已分离后才会真正
		// 移除。ReplicationControllers 默认使用 "orphan"，与
		// 多数资源不同。（为什么？为了保留 GC 引入之前
		// 就存在的常见工作流。）
		err = f.ClientSet.CoreV1().ReplicationControllers(f.Namespace.Name).Delete(ctx, replicationController.Name, metav1.DeleteOptions{
			PropagationPolicy: func() *metav1.DeletionPropagation {
				p := metav1.DeletePropagationBackground
				return &p
			}(),
		})
		framework.ExpectNoError(err)

		ginkgo.By("Ensuring resource quota status released usage")
		usedResources[v1.ResourceReplicationControllers] = resource.MustParse("0")
		err = waitForResourceQuota(ctx, f.ClientSet, f.Namespace.Name, quotaName, usedResources)
		framework.ExpectNoError(err)
	})

	/*
		发布版本：v1.16
		测试名称：ResourceQuota, object count quota, replicaSet
		描述：创建 ResourceQuota。创建必须成功，其 ResourceQuotaStatus 必须与 namespace 内预期已用和总允许配额数一致。
		创建 ReplicaSet。创建必须成功，ReplicaSet 对象的资源使用计数必须记录在 ResourceQuota 的 ResourceQuotaStatus 中。
		删除 ReplicaSet。删除必须成功，ReplicaSet 对象的资源使用计数必须从 ResourceQuota 的 ResourceQuotaStatus 中释放。
	*/
	framework.ConformanceIt("should create a ResourceQuota and capture the life of a replica set.", func(ctx context.Context) {
		ginkgo.By("Counting existing ResourceQuota")
		c, err := countResourceQuota(ctx, f.ClientSet, f.Namespace.Name)
		framework.ExpectNoError(err)

		ginkgo.By("Creating a ResourceQuota")
		quotaName := "test-quota"
		resourceQuota := newTestResourceQuota(quotaName)
		_, err = createResourceQuota(ctx, f.ClientSet, f.Namespace.Name, resourceQuota)
		framework.ExpectNoError(err)

		ginkgo.By("Ensuring resource quota status is calculated")
		usedResources := v1.ResourceList{}
		usedResources[v1.ResourceQuotas] = resource.MustParse(strconv.Itoa(c + 1))
		usedResources[v1.ResourceName("count/replicasets.apps")] = resource.MustParse("0")
		err = waitForResourceQuota(ctx, f.ClientSet, f.Namespace.Name, quotaName, usedResources)
		framework.ExpectNoError(err)

		ginkgo.By("Creating a ReplicaSet")
		replicaSet := newTestReplicaSetForQuota("test-rs", "nginx", 0)
		replicaSet, err = f.ClientSet.AppsV1().ReplicaSets(f.Namespace.Name).Create(ctx, replicaSet, metav1.CreateOptions{})
		framework.ExpectNoError(err)

		ginkgo.By("Ensuring resource quota status captures replicaset creation")
		usedResources = v1.ResourceList{}
		usedResources[v1.ResourceName("count/replicasets.apps")] = resource.MustParse("1")
		err = waitForResourceQuota(ctx, f.ClientSet, f.Namespace.Name, quotaName, usedResources)
		framework.ExpectNoError(err)

		ginkgo.By("Deleting a ReplicaSet")
		err = f.ClientSet.AppsV1().ReplicaSets(f.Namespace.Name).Delete(ctx, replicaSet.Name, metav1.DeleteOptions{})
		framework.ExpectNoError(err)

		ginkgo.By("Ensuring resource quota status released usage")
		usedResources[v1.ResourceName("count/replicasets.apps")] = resource.MustParse("0")
		err = waitForResourceQuota(ctx, f.ClientSet, f.Namespace.Name, quotaName, usedResources)
		framework.ExpectNoError(err)
	})

	/*
		发布版本：v1.31
		测试名称：ResourceQuota, object count quota, ResourceClaim
		描述：创建 ResourceQuota。创建必须成功，其 ResourceQuotaStatus 必须与 namespace 内预期已用和总允许配额数一致。
		创建 ResourceClaim。创建必须成功，ResourceClaim 对象的资源使用计数必须记录在 ResourceQuota 的 ResourceQuotaStatus 中。
		删除 ResourceClaim。删除必须成功，ResourceClaim 对象的资源使用计数必须从 ResourceQuota 的 ResourceQuotaStatus 中释放。
		[NotConformancePromotable] alpha 特性
	*/
	f.It("should create a ResourceQuota and capture the life of a ResourceClaim", f.WithFeatureGate(features.DynamicResourceAllocation), f.WithLabel("DRA"), func(ctx context.Context) {
		ginkgo.By("Counting existing ResourceQuota")
		c, err := countResourceQuota(ctx, f.ClientSet, f.Namespace.Name)
		framework.ExpectNoError(err)

		ginkgo.By("Creating a ResourceQuota")
		quotaName := "test-quota"
		resourceQuota := newTestResourceQuotaDRA(quotaName)
		_, err = createResourceQuota(ctx, f.ClientSet, f.Namespace.Name, resourceQuota)
		framework.ExpectNoError(err)

		ginkgo.By("Ensuring resource quota status is calculated")
		usedResources := v1.ResourceList{}
		usedResources[v1.ResourceQuotas] = resource.MustParse(strconv.Itoa(c + 1))
		usedResources[core.ClaimObjectCountName] = resource.MustParse("0")
		usedResources[core.V1ResourceByDeviceClass(classGold)] = resource.MustParse("0")
		err = waitForResourceQuota(ctx, f.ClientSet, f.Namespace.Name, quotaName, usedResources)
		framework.ExpectNoError(err)

		ginkgo.By("Creating a ResourceClaim")
		claim := newTestResourceClaimForQuota("test-claim")
		claim, err = f.ClientSet.ResourceV1().ResourceClaims(f.Namespace.Name).Create(ctx, claim, metav1.CreateOptions{})
		framework.ExpectNoError(err)

		ginkgo.By("Ensuring resource quota status captures resource claim creation")
		usedResources = v1.ResourceList{}
		usedResources[core.ClaimObjectCountName] = resource.MustParse("1")
		usedResources[core.V1ResourceByDeviceClass(classGold)] = resource.MustParse("1")
		err = waitForResourceQuota(ctx, f.ClientSet, f.Namespace.Name, quotaName, usedResources)
		framework.ExpectNoError(err)

		ginkgo.By("Deleting a ResourceClaim")
		err = f.ClientSet.ResourceV1().ResourceClaims(f.Namespace.Name).Delete(ctx, claim.Name, metav1.DeleteOptions{})
		framework.ExpectNoError(err)

		ginkgo.By("Ensuring resource quota status released usage")
		usedResources[core.ClaimObjectCountName] = resource.MustParse("0")
		usedResources[core.V1ResourceByDeviceClass(classGold)] = resource.MustParse("0")
		err = waitForResourceQuota(ctx, f.ClientSet, f.Namespace.Name, quotaName, usedResources)
		framework.ExpectNoError(err)
	})

	/*
		发布版本：v1.16
		测试名称：ResourceQuota, object count quota, pvc
		描述：创建 ResourceQuota。创建必须成功，其 ResourceQuotaStatus 必须与 namespace 内预期已用和总允许配额数一致。
		创建 PersistentVolumeClaim（PVC）请求 1G 存储。PVC 创建必须成功，PVC 和存储对象的资源使用计数必须记录在 ResourceQuota 的 ResourceQuotaStatus 中。
		删除 PVC。删除必须成功，PVC 和存储对象的资源使用计数必须从 ResourceQuota 的 ResourceQuotaStatus 中释放。
		[NotConformancePromotable] 当前测试套件没有明确验证 PV 和 PVC 行为的 e2e。
	*/
	ginkgo.It("should create a ResourceQuota and capture the life of a persistent volume claim", func(ctx context.Context) {
		ginkgo.By("Counting existing ResourceQuota")
		c, err := countResourceQuota(ctx, f.ClientSet, f.Namespace.Name)
		framework.ExpectNoError(err)

		ginkgo.By("Creating a ResourceQuota")
		quotaName := "test-quota"
		resourceQuota := newTestResourceQuota(quotaName)
		_, err = createResourceQuota(ctx, f.ClientSet, f.Namespace.Name, resourceQuota)
		framework.ExpectNoError(err)

		ginkgo.By("Ensuring resource quota status is calculated")
		usedResources := v1.ResourceList{}
		usedResources[v1.ResourceQuotas] = resource.MustParse(strconv.Itoa(c + 1))
		usedResources[v1.ResourcePersistentVolumeClaims] = resource.MustParse("0")
		usedResources[v1.ResourceRequestsStorage] = resource.MustParse("0")
		err = waitForResourceQuota(ctx, f.ClientSet, f.Namespace.Name, quotaName, usedResources)
		framework.ExpectNoError(err)

		ginkgo.By("Creating a PersistentVolumeClaim")
		pvc := newTestPersistentVolumeClaimForQuota("test-claim")
		pvc, err = f.ClientSet.CoreV1().PersistentVolumeClaims(f.Namespace.Name).Create(ctx, pvc, metav1.CreateOptions{})
		framework.ExpectNoError(err)

		ginkgo.By("Ensuring resource quota status captures persistent volume claim creation")
		usedResources = v1.ResourceList{}
		usedResources[v1.ResourcePersistentVolumeClaims] = resource.MustParse("1")
		usedResources[v1.ResourceRequestsStorage] = resource.MustParse("1Gi")
		err = waitForResourceQuota(ctx, f.ClientSet, f.Namespace.Name, quotaName, usedResources)
		framework.ExpectNoError(err)

		ginkgo.By("Deleting a PersistentVolumeClaim")
		err = f.ClientSet.CoreV1().PersistentVolumeClaims(f.Namespace.Name).Delete(ctx, pvc.Name, metav1.DeleteOptions{})
		framework.ExpectNoError(err)

		ginkgo.By("Ensuring resource quota status released usage")
		usedResources[v1.ResourcePersistentVolumeClaims] = resource.MustParse("0")
		usedResources[v1.ResourceRequestsStorage] = resource.MustParse("0")
		err = waitForResourceQuota(ctx, f.ClientSet, f.Namespace.Name, quotaName, usedResources)
		framework.ExpectNoError(err)
	})

	/*
		发布版本：v1.16
		测试名称：ResourceQuota, object count quota, storageClass
		描述：创建 ResourceQuota。创建必须成功，其 ResourceQuotaStatus 必须与 namespace 内预期已用和总允许配额数一致。
		创建指定 storageClass 的 PersistentVolumeClaim（PVC）请求 1G 存储。PVC 创建必须成功，PVC、storageClass 和存储对象的资源使用计数必须记录在 ResourceQuota 的 ResourceQuotaStatus 中。
		删除 PVC。删除必须成功，PVC、storageClass 和存储对象的资源使用计数必须从 ResourceQuota 的 ResourceQuotaStatus 中释放。
		[NotConformancePromotable] 当前测试套件没有明确验证 PV 和 PVC 行为的 e2e。
	*/
	ginkgo.It("should create a ResourceQuota and capture the life of a persistent volume claim with a storage class", func(ctx context.Context) {
		ginkgo.By("Counting existing ResourceQuota")
		c, err := countResourceQuota(ctx, f.ClientSet, f.Namespace.Name)
		framework.ExpectNoError(err)

		ginkgo.By("Creating a ResourceQuota")
		quotaName := "test-quota"
		resourceQuota := newTestResourceQuota(quotaName)
		_, err = createResourceQuota(ctx, f.ClientSet, f.Namespace.Name, resourceQuota)
		framework.ExpectNoError(err)

		ginkgo.By("Ensuring resource quota status is calculated")
		usedResources := v1.ResourceList{}
		usedResources[v1.ResourceQuotas] = resource.MustParse(strconv.Itoa(c + 1))
		usedResources[v1.ResourcePersistentVolumeClaims] = resource.MustParse("0")
		usedResources[v1.ResourceRequestsStorage] = resource.MustParse("0")
		usedResources[core.V1ResourceByStorageClass(classGold, v1.ResourcePersistentVolumeClaims)] = resource.MustParse("0")
		usedResources[core.V1ResourceByStorageClass(classGold, v1.ResourceRequestsStorage)] = resource.MustParse("0")

		err = waitForResourceQuota(ctx, f.ClientSet, f.Namespace.Name, quotaName, usedResources)
		framework.ExpectNoError(err)

		ginkgo.By("Creating a PersistentVolumeClaim with storage class")
		pvc := newTestPersistentVolumeClaimForQuota("test-claim")
		pvc.Spec.StorageClassName = &classGold
		pvc, err = f.ClientSet.CoreV1().PersistentVolumeClaims(f.Namespace.Name).Create(ctx, pvc, metav1.CreateOptions{})
		framework.ExpectNoError(err)

		ginkgo.By("Ensuring resource quota status captures persistent volume claim creation")
		usedResources = v1.ResourceList{}
		usedResources[v1.ResourcePersistentVolumeClaims] = resource.MustParse("1")
		usedResources[v1.ResourceRequestsStorage] = resource.MustParse("1Gi")
		usedResources[core.V1ResourceByStorageClass(classGold, v1.ResourcePersistentVolumeClaims)] = resource.MustParse("1")
		usedResources[core.V1ResourceByStorageClass(classGold, v1.ResourceRequestsStorage)] = resource.MustParse("1Gi")

		err = waitForResourceQuota(ctx, f.ClientSet, f.Namespace.Name, quotaName, usedResources)
		framework.ExpectNoError(err)

		ginkgo.By("Deleting a PersistentVolumeClaim")
		err = f.ClientSet.CoreV1().PersistentVolumeClaims(f.Namespace.Name).Delete(ctx, pvc.Name, metav1.DeleteOptions{})
		framework.ExpectNoError(err)

		ginkgo.By("Ensuring resource quota status released usage")
		usedResources[v1.ResourcePersistentVolumeClaims] = resource.MustParse("0")
		usedResources[v1.ResourceRequestsStorage] = resource.MustParse("0")
		usedResources[core.V1ResourceByStorageClass(classGold, v1.ResourcePersistentVolumeClaims)] = resource.MustParse("0")
		usedResources[core.V1ResourceByStorageClass(classGold, v1.ResourceRequestsStorage)] = resource.MustParse("0")

		err = waitForResourceQuota(ctx, f.ClientSet, f.Namespace.Name, quotaName, usedResources)
		framework.ExpectNoError(err)
	})

	ginkgo.It("should create a ResourceQuota and capture the life of a custom resource.", func(ctx context.Context) {
		ginkgo.By("Creating a Custom Resource Definition")
		testcrd, err := crd.CreateTestCRD(f)
		framework.ExpectNoError(err)
		ginkgo.DeferCleanup(testcrd.CleanUp)
		countResourceName := "count/" + testcrd.Crd.Spec.Names.Plural + "." + testcrd.Crd.Spec.Group
		// resourcequota controller 最多需要 30 秒发现新的自定义资源。
		// 为确保 controller 认识这个资源，我们创建一个测试
		// resourcequota 对象，并持续触发更新直到状态被更新。
		quotaName := "quota-for-" + testcrd.Crd.Spec.Names.Plural
		_, err = createResourceQuota(ctx, f.ClientSet, f.Namespace.Name, &v1.ResourceQuota{
			ObjectMeta: metav1.ObjectMeta{Name: quotaName},
			Spec: v1.ResourceQuotaSpec{
				Hard: v1.ResourceList{
					v1.ResourceName(countResourceName): resource.MustParse("0"),
				},
			},
		})
		framework.ExpectNoError(err)
		err = updateResourceQuotaUntilUsageAppears(ctx, f.ClientSet, f.Namespace.Name, quotaName, v1.ResourceName(countResourceName))
		framework.ExpectNoError(err)
		err = f.ClientSet.CoreV1().ResourceQuotas(f.Namespace.Name).Delete(ctx, quotaName, metav1.DeleteOptions{})
		framework.ExpectNoError(err)

		ginkgo.By("Counting existing ResourceQuota")
		c, err := countResourceQuota(ctx, f.ClientSet, f.Namespace.Name)
		framework.ExpectNoError(err)

		ginkgo.By("Creating a ResourceQuota")
		quotaName = "test-quota"
		resourceQuota := newTestResourceQuota(quotaName)
		resourceQuota.Spec.Hard[v1.ResourceName(countResourceName)] = resource.MustParse("1")
		_, err = createResourceQuota(ctx, f.ClientSet, f.Namespace.Name, resourceQuota)
		framework.ExpectNoError(err)

		ginkgo.By("Ensuring resource quota status is calculated")
		usedResources := v1.ResourceList{}
		usedResources[v1.ResourceQuotas] = resource.MustParse(strconv.Itoa(c + 1))
		usedResources[v1.ResourceName(countResourceName)] = resource.MustParse("0")
		err = waitForResourceQuota(ctx, f.ClientSet, f.Namespace.Name, quotaName, usedResources)
		framework.ExpectNoError(err)

		ginkgo.By("Creating a custom resource")
		resourceClient := testcrd.DynamicClients["v1"]
		testcr, err := instantiateCustomResource(ctx, &unstructured.Unstructured{
			Object: map[string]interface{}{
				"apiVersion": testcrd.Crd.Spec.Group + "/" + testcrd.Crd.Spec.Versions[0].Name,
				"kind":       testcrd.Crd.Spec.Names.Kind,
				"metadata": map[string]interface{}{
					"name": "test-cr-1",
				},
			},
		}, resourceClient, testcrd.Crd)
		framework.ExpectNoError(err)

		ginkgo.By("Ensuring resource quota status captures custom resource creation")
		usedResources = v1.ResourceList{}
		usedResources[v1.ResourceName(countResourceName)] = resource.MustParse("1")
		err = waitForResourceQuota(ctx, f.ClientSet, f.Namespace.Name, quotaName, usedResources)
		framework.ExpectNoError(err)

		ginkgo.By("Creating a second custom resource")
		_, err = instantiateCustomResource(ctx, &unstructured.Unstructured{
			Object: map[string]interface{}{
				"apiVersion": testcrd.Crd.Spec.Group + "/" + testcrd.Crd.Spec.Versions[0].Name,
				"kind":       testcrd.Crd.Spec.Names.Kind,
				"metadata": map[string]interface{}{
					"name": "test-cr-2",
				},
			},
		}, resourceClient, testcrd.Crd)
		// 只给了一份配额，因此这次创建应该失败。
		gomega.Expect(err).To(gomega.HaveOccurred())

		ginkgo.By("Deleting a custom resource")
		err = deleteCustomResource(ctx, resourceClient, testcr.GetName())
		framework.ExpectNoError(err)

		ginkgo.By("Ensuring resource quota status released usage")
		usedResources[v1.ResourceName(countResourceName)] = resource.MustParse("0")
		err = waitForResourceQuota(ctx, f.ClientSet, f.Namespace.Name, quotaName, usedResources)
		framework.ExpectNoError(err)
	})

	/*
		发布版本：v1.16
		测试名称：ResourceQuota, quota scope, Terminating and NotTerminating scope
		描述：创建两个 ResourceQuota，一个 scope 为 'Terminating'，另一个为 'NotTerminating'。为 ResourceQuota 设置 CPU 和 Memory 资源的请求和限制计数。创建必须成功，其 ResourceQuotaStatus 必须与 namespace 内预期已用和总允许配额数一致。
		创建 CPU 和 Memory ResourceRequirements 在配额范围内的 Pod。Pod 创建必须成功，使用计数必须记录在 'NotTerminating' scope 的 ResourceQuota 中，但不得出现在 'Terminating' scope 的 ResourceQuota 中。
		删除 Pod。删除必须成功，Pod 资源使用计数必须从 'NotTerminating' scope 的 ResourceQuota 中释放。
		创建带 activeDeadlineSeconds 且 CPU 和 Memory 资源需求在配额范围内的 Pod。Pod 创建必须成功，使用计数必须记录在 'Terminating' scope 的 ResourceQuota 中，但不得出现在 'NotTerminating' scope 的 ResourceQuota 中。
		删除 Pod。删除必须成功，Pod 资源使用计数必须从 'Terminating' scope 的 ResourceQuota 中释放。
	*/
	framework.ConformanceIt("should verify ResourceQuota with terminating scopes.", func(ctx context.Context) {
		ginkgo.By("Creating a ResourceQuota with terminating scope")
		quotaTerminatingName := "quota-terminating"
		resourceQuotaTerminating, err := createResourceQuota(ctx, f.ClientSet, f.Namespace.Name, newTestResourceQuotaWithScope(quotaTerminatingName, v1.ResourceQuotaScopeTerminating))
		framework.ExpectNoError(err)

		ginkgo.By("Ensuring ResourceQuota status is calculated")
		usedResources := v1.ResourceList{}
		usedResources[v1.ResourcePods] = resource.MustParse("0")
		err = waitForResourceQuota(ctx, f.ClientSet, f.Namespace.Name, resourceQuotaTerminating.Name, usedResources)
		framework.ExpectNoError(err)

		ginkgo.By("Creating a ResourceQuota with not terminating scope")
		quotaNotTerminatingName := "quota-not-terminating"
		resourceQuotaNotTerminating, err := createResourceQuota(ctx, f.ClientSet, f.Namespace.Name, newTestResourceQuotaWithScope(quotaNotTerminatingName, v1.ResourceQuotaScopeNotTerminating))
		framework.ExpectNoError(err)

		ginkgo.By("Ensuring ResourceQuota status is calculated")
		err = waitForResourceQuota(ctx, f.ClientSet, f.Namespace.Name, resourceQuotaNotTerminating.Name, usedResources)
		framework.ExpectNoError(err)

		ginkgo.By("Creating a long running pod")
		podName := "test-pod"
		requests := v1.ResourceList{}
		requests[v1.ResourceCPU] = resource.MustParse("500m")
		requests[v1.ResourceMemory] = resource.MustParse("200Mi")
		limits := v1.ResourceList{}
		limits[v1.ResourceCPU] = resource.MustParse("1")
		limits[v1.ResourceMemory] = resource.MustParse("400Mi")
		pod := newTestPodForQuota(f, podName, requests, limits)
		_, err = f.ClientSet.CoreV1().Pods(f.Namespace.Name).Create(ctx, pod, metav1.CreateOptions{})
		framework.ExpectNoError(err)

		ginkgo.By("Ensuring resource quota with not terminating scope captures the pod usage")
		usedResources[v1.ResourcePods] = resource.MustParse("1")
		usedResources[v1.ResourceRequestsCPU] = requests[v1.ResourceCPU]
		usedResources[v1.ResourceRequestsMemory] = requests[v1.ResourceMemory]
		usedResources[v1.ResourceLimitsCPU] = limits[v1.ResourceCPU]
		usedResources[v1.ResourceLimitsMemory] = limits[v1.ResourceMemory]
		err = waitForResourceQuota(ctx, f.ClientSet, f.Namespace.Name, resourceQuotaNotTerminating.Name, usedResources)
		framework.ExpectNoError(err)

		ginkgo.By("Ensuring resource quota with terminating scope ignored the pod usage")
		usedResources[v1.ResourcePods] = resource.MustParse("0")
		usedResources[v1.ResourceRequestsCPU] = resource.MustParse("0")
		usedResources[v1.ResourceRequestsMemory] = resource.MustParse("0")
		usedResources[v1.ResourceLimitsCPU] = resource.MustParse("0")
		usedResources[v1.ResourceLimitsMemory] = resource.MustParse("0")
		err = ensureResourceQuota(ctx, f.ClientSet, f.Namespace.Name, resourceQuotaTerminating.Name, usedResources)
		framework.ExpectNoError(err)

		ginkgo.By("Deleting the pod")
		err = f.ClientSet.CoreV1().Pods(f.Namespace.Name).Delete(ctx, podName, *metav1.NewDeleteOptions(0))
		framework.ExpectNoError(err)

		ginkgo.By("Ensuring resource quota status released the pod usage")
		usedResources[v1.ResourcePods] = resource.MustParse("0")
		usedResources[v1.ResourceRequestsCPU] = resource.MustParse("0")
		usedResources[v1.ResourceRequestsMemory] = resource.MustParse("0")
		usedResources[v1.ResourceLimitsCPU] = resource.MustParse("0")
		usedResources[v1.ResourceLimitsMemory] = resource.MustParse("0")
		err = waitForResourceQuota(ctx, f.ClientSet, f.Namespace.Name, resourceQuotaNotTerminating.Name, usedResources)
		framework.ExpectNoError(err)

		ginkgo.By("Creating a terminating pod")
		podName = "terminating-pod"
		pod = newTestPodForQuota(f, podName, requests, limits)
		activeDeadlineSeconds := int64(3600)
		pod.Spec.ActiveDeadlineSeconds = &activeDeadlineSeconds
		_, err = f.ClientSet.CoreV1().Pods(f.Namespace.Name).Create(ctx, pod, metav1.CreateOptions{})
		framework.ExpectNoError(err)

		ginkgo.By("Ensuring resource quota with terminating scope captures the pod usage")
		usedResources[v1.ResourcePods] = resource.MustParse("1")
		usedResources[v1.ResourceRequestsCPU] = requests[v1.ResourceCPU]
		usedResources[v1.ResourceRequestsMemory] = requests[v1.ResourceMemory]
		usedResources[v1.ResourceLimitsCPU] = limits[v1.ResourceCPU]
		usedResources[v1.ResourceLimitsMemory] = limits[v1.ResourceMemory]
		err = waitForResourceQuota(ctx, f.ClientSet, f.Namespace.Name, resourceQuotaTerminating.Name, usedResources)
		framework.ExpectNoError(err)

		ginkgo.By("Ensuring resource quota with not terminating scope ignored the pod usage")
		usedResources[v1.ResourcePods] = resource.MustParse("0")
		usedResources[v1.ResourceRequestsCPU] = resource.MustParse("0")
		usedResources[v1.ResourceRequestsMemory] = resource.MustParse("0")
		usedResources[v1.ResourceLimitsCPU] = resource.MustParse("0")
		usedResources[v1.ResourceLimitsMemory] = resource.MustParse("0")
		err = waitForResourceQuota(ctx, f.ClientSet, f.Namespace.Name, resourceQuotaNotTerminating.Name, usedResources)
		framework.ExpectNoError(err)
		err = ensureResourceQuota(ctx, f.ClientSet, f.Namespace.Name, resourceQuotaNotTerminating.Name, usedResources)
		framework.ExpectNoError(err)

		ginkgo.By("Deleting the pod")
		err = f.ClientSet.CoreV1().Pods(f.Namespace.Name).Delete(ctx, podName, *metav1.NewDeleteOptions(0))
		framework.ExpectNoError(err)

		ginkgo.By("Ensuring resource quota status released the pod usage")
		usedResources[v1.ResourcePods] = resource.MustParse("0")
		usedResources[v1.ResourceRequestsCPU] = resource.MustParse("0")
		usedResources[v1.ResourceRequestsMemory] = resource.MustParse("0")
		usedResources[v1.ResourceLimitsCPU] = resource.MustParse("0")
		usedResources[v1.ResourceLimitsMemory] = resource.MustParse("0")
		err = waitForResourceQuota(ctx, f.ClientSet, f.Namespace.Name, resourceQuotaTerminating.Name, usedResources)
		framework.ExpectNoError(err)
	})

	/*
		发布版本：v1.16
		测试名称：ResourceQuota, quota scope, BestEffort and NotBestEffort scope
		描述：创建两个 ResourceQuota，一个 scope 为 'BestEffort'，另一个为 'NotBestEffort'。创建必须成功，其 ResourceQuotaStatus 必须与 namespace 内预期已用和总允许配额数一致。
		不显式指定资源限制和请求，创建一个 'BestEffort' Pod。Pod 创建必须成功，使用计数必须记录在 'BestEffort' scope 的 ResourceQuota 中，但不得出现在 'NotBestEffort' scope 的 ResourceQuota 中。
		删除 Pod。删除必须成功，Pod 资源使用计数必须从 'BestEffort' scope 的 ResourceQuota 中释放。
		显式指定资源限制和请求，创建一个 'NotBestEffort' Pod。Pod 创建必须成功，使用计数必须记录在 'NotBestEffort' scope 的 ResourceQuota 中，但不得出现在 'BestEffort' scope 的 ResourceQuota 中。
		删除 Pod。删除必须成功，Pod 资源使用计数必须从 'NotBestEffort' scope 的 ResourceQuota 中释放。
	*/
	framework.ConformanceIt("should verify ResourceQuota with best effort scope.", func(ctx context.Context) {
		ginkgo.By("Creating a ResourceQuota with best effort scope")
		resourceQuotaBestEffort, err := createResourceQuota(ctx, f.ClientSet, f.Namespace.Name, newTestResourceQuotaWithScope("quota-besteffort", v1.ResourceQuotaScopeBestEffort))
		framework.ExpectNoError(err)

		ginkgo.By("Ensuring ResourceQuota status is calculated")
		usedResources := v1.ResourceList{}
		usedResources[v1.ResourcePods] = resource.MustParse("0")
		err = waitForResourceQuota(ctx, f.ClientSet, f.Namespace.Name, resourceQuotaBestEffort.Name, usedResources)
		framework.ExpectNoError(err)

		ginkgo.By("Creating a ResourceQuota with not best effort scope")
		resourceQuotaNotBestEffort, err := createResourceQuota(ctx, f.ClientSet, f.Namespace.Name, newTestResourceQuotaWithScope("quota-not-besteffort", v1.ResourceQuotaScopeNotBestEffort))
		framework.ExpectNoError(err)

		ginkgo.By("Ensuring ResourceQuota status is calculated")
		err = waitForResourceQuota(ctx, f.ClientSet, f.Namespace.Name, resourceQuotaNotBestEffort.Name, usedResources)
		framework.ExpectNoError(err)

		ginkgo.By("Creating a best-effort pod")
		pod := newTestPodForQuota(f, podName, v1.ResourceList{}, v1.ResourceList{})
		pod, err = f.ClientSet.CoreV1().Pods(f.Namespace.Name).Create(ctx, pod, metav1.CreateOptions{})
		framework.ExpectNoError(err)

		ginkgo.By("Ensuring resource quota with best effort scope captures the pod usage")
		usedResources[v1.ResourcePods] = resource.MustParse("1")
		err = waitForResourceQuota(ctx, f.ClientSet, f.Namespace.Name, resourceQuotaBestEffort.Name, usedResources)
		framework.ExpectNoError(err)

		ginkgo.By("Ensuring resource quota with not best effort ignored the pod usage")
		usedResources[v1.ResourcePods] = resource.MustParse("0")
		err = waitForResourceQuota(ctx, f.ClientSet, f.Namespace.Name, resourceQuotaNotBestEffort.Name, usedResources)
		framework.ExpectNoError(err)

		ginkgo.By("Deleting the pod")
		err = f.ClientSet.CoreV1().Pods(f.Namespace.Name).Delete(ctx, pod.Name, *metav1.NewDeleteOptions(0))
		framework.ExpectNoError(err)

		ginkgo.By("Ensuring resource quota status released the pod usage")
		usedResources[v1.ResourcePods] = resource.MustParse("0")
		err = waitForResourceQuota(ctx, f.ClientSet, f.Namespace.Name, resourceQuotaBestEffort.Name, usedResources)
		framework.ExpectNoError(err)

		ginkgo.By("Creating a not best-effort pod")
		requests := v1.ResourceList{}
		requests[v1.ResourceCPU] = resource.MustParse("500m")
		requests[v1.ResourceMemory] = resource.MustParse("200Mi")
		limits := v1.ResourceList{}
		limits[v1.ResourceCPU] = resource.MustParse("1")
		limits[v1.ResourceMemory] = resource.MustParse("400Mi")
		pod = newTestPodForQuota(f, "burstable-pod", requests, limits)
		pod, err = f.ClientSet.CoreV1().Pods(f.Namespace.Name).Create(ctx, pod, metav1.CreateOptions{})
		framework.ExpectNoError(err)

		ginkgo.By("Ensuring resource quota with not best effort scope captures the pod usage")
		usedResources[v1.ResourcePods] = resource.MustParse("1")
		err = waitForResourceQuota(ctx, f.ClientSet, f.Namespace.Name, resourceQuotaNotBestEffort.Name, usedResources)
		framework.ExpectNoError(err)

		ginkgo.By("Ensuring resource quota with best effort scope ignored the pod usage")
		usedResources[v1.ResourcePods] = resource.MustParse("0")
		err = waitForResourceQuota(ctx, f.ClientSet, f.Namespace.Name, resourceQuotaBestEffort.Name, usedResources)
		framework.ExpectNoError(err)

		ginkgo.By("Deleting the pod")
		err = f.ClientSet.CoreV1().Pods(f.Namespace.Name).Delete(ctx, pod.Name, *metav1.NewDeleteOptions(0))
		framework.ExpectNoError(err)

		ginkgo.By("Ensuring resource quota status released the pod usage")
		usedResources[v1.ResourcePods] = resource.MustParse("0")
		err = waitForResourceQuota(ctx, f.ClientSet, f.Namespace.Name, resourceQuotaNotBestEffort.Name, usedResources)
		framework.ExpectNoError(err)
	})

	/*
		发布版本：v1.16
		测试名称：ResourceQuota, update and delete
		描述：创建一个限制 CPU 和 Memory 的 ResourceQuota。创建必须成功。
		更新 ResourceQuota 修改 CPU 和 Memory 配额限制时，更新必须成功并反映新值。
		删除 ResourceQuota 后，它必须不再存在于 namespace 中。
	*/
	framework.ConformanceIt("should be able to update and delete ResourceQuota.", func(ctx context.Context) {
		client := f.ClientSet
		ns := f.Namespace.Name

		ginkgo.By("Creating a ResourceQuota")
		quotaName := "test-quota"
		resourceQuota := &v1.ResourceQuota{
			Spec: v1.ResourceQuotaSpec{
				Hard: v1.ResourceList{},
			},
		}
		resourceQuota.ObjectMeta.Name = quotaName
		resourceQuota.Spec.Hard[v1.ResourceCPU] = resource.MustParse("1")
		resourceQuota.Spec.Hard[v1.ResourceMemory] = resource.MustParse("500Mi")
		_, err := createResourceQuota(ctx, client, ns, resourceQuota)
		framework.ExpectNoError(err)

		ginkgo.By("Getting a ResourceQuota")
		resourceQuotaResult, err := client.CoreV1().ResourceQuotas(ns).Get(ctx, quotaName, metav1.GetOptions{})
		framework.ExpectNoError(err)
		gomega.Expect(resourceQuotaResult.Spec.Hard).To(gomega.HaveKeyWithValue(v1.ResourceCPU, resource.MustParse("1")))
		gomega.Expect(resourceQuotaResult.Spec.Hard).To(gomega.HaveKeyWithValue(v1.ResourceMemory, resource.MustParse("500Mi")))

		ginkgo.By("Updating a ResourceQuota")
		resourceQuota.Spec.Hard[v1.ResourceCPU] = resource.MustParse("2")
		resourceQuota.Spec.Hard[v1.ResourceMemory] = resource.MustParse("1Gi")
		resourceQuotaResult, err = client.CoreV1().ResourceQuotas(ns).Update(ctx, resourceQuota, metav1.UpdateOptions{})
		framework.ExpectNoError(err)
		gomega.Expect(resourceQuotaResult.Spec.Hard).To(gomega.HaveKeyWithValue(v1.ResourceCPU, resource.MustParse("2")))
		gomega.Expect(resourceQuotaResult.Spec.Hard).To(gomega.HaveKeyWithValue(v1.ResourceMemory, resource.MustParse("1Gi")))

		ginkgo.By("Verifying a ResourceQuota was modified")
		resourceQuotaResult, err = client.CoreV1().ResourceQuotas(ns).Get(ctx, quotaName, metav1.GetOptions{})
		framework.ExpectNoError(err)
		gomega.Expect(resourceQuotaResult.Spec.Hard).To(gomega.HaveKeyWithValue(v1.ResourceCPU, resource.MustParse("2")))
		gomega.Expect(resourceQuotaResult.Spec.Hard).To(gomega.HaveKeyWithValue(v1.ResourceMemory, resource.MustParse("1Gi")))

		ginkgo.By("Deleting a ResourceQuota")
		err = deleteResourceQuota(ctx, client, ns, quotaName)
		framework.ExpectNoError(err)

		ginkgo.By("Verifying the deleted ResourceQuota")
		_, err = client.CoreV1().ResourceQuotas(ns).Get(ctx, quotaName, metav1.GetOptions{})
		if !apierrors.IsNotFound(err) {
			framework.Failf("Expected `not found` error, got: %v", err)
		}
	})

	/*
		发布版本：v1.25
		测试名称：ResourceQuota, manage lifecycle of a ResourceQuota
		描述：尝试创建 CPU 和 Memory 配额限制的
		ResourceQuota。创建必须成功。尝试用 label selector 列出所有
		namespace，必须成功。必须找到一个列表
		项。patch ResourceQuota 必须成功。
		patch 之后，各字段必须等于
		新值。必须能通过 label selector 成功删除一组
		ResourceQuota。
	*/
	framework.ConformanceIt("should manage the lifecycle of a ResourceQuota", func(ctx context.Context) {
		client := f.ClientSet
		ns := f.Namespace.Name

		rqName := "e2e-quota-" + utilrand.String(5)
		label := map[string]string{"e2e-rq-label": rqName}
		labelSelector := labels.SelectorFromSet(label).String()

		ginkgo.By("Creating a ResourceQuota")
		resourceQuota := &v1.ResourceQuota{
			ObjectMeta: metav1.ObjectMeta{
				Name:   rqName,
				Labels: label,
			},
			Spec: v1.ResourceQuotaSpec{
				Hard: v1.ResourceList{},
			},
		}
		resourceQuota.Spec.Hard[v1.ResourceCPU] = resource.MustParse("1")
		resourceQuota.Spec.Hard[v1.ResourceMemory] = resource.MustParse("500Mi")
		_, err := createResourceQuota(ctx, client, ns, resourceQuota)
		framework.ExpectNoError(err)

		ginkgo.By("Getting a ResourceQuota")
		resourceQuotaResult, err := client.CoreV1().ResourceQuotas(ns).Get(ctx, rqName, metav1.GetOptions{})
		framework.ExpectNoError(err)
		gomega.Expect(resourceQuotaResult.Spec.Hard[v1.ResourceCPU]).To(gomega.Equal(resource.MustParse("1")))
		gomega.Expect(resourceQuotaResult.Spec.Hard[v1.ResourceMemory]).To(gomega.Equal(resource.MustParse("500Mi")))
		gomega.Expect(resourceQuotaResult).To(apimachineryutils.HaveValidResourceVersion())

		ginkgo.By("Listing all ResourceQuotas with LabelSelector")
		rq, err := client.CoreV1().ResourceQuotas("").List(ctx, metav1.ListOptions{LabelSelector: labelSelector})
		framework.ExpectNoError(err, "Failed to list job. %v", err)
		gomega.Expect(rq.Items).To(gomega.HaveLen(1), "Failed to find ResourceQuotes %v", rqName)

		ginkgo.By("Patching the ResourceQuota")
		payload := "{\"metadata\":{\"labels\":{\"" + rqName + "\":\"patched\"}},\"spec\":{\"hard\":{ \"memory\":\"750Mi\"}}}"
		patchedResourceQuota, err := client.CoreV1().ResourceQuotas(ns).Patch(ctx, rqName, types.StrategicMergePatchType, []byte(payload), metav1.PatchOptions{})
		framework.ExpectNoError(err, "failed to patch ResourceQuota %s in namespace %s", rqName, ns)
		gomega.Expect(patchedResourceQuota.Labels[rqName]).To(gomega.Equal("patched"), "Failed to find the label for this ResourceQuota. Current labels: %v", patchedResourceQuota.Labels)
		gomega.Expect(*patchedResourceQuota.Spec.Hard.Memory()).To(gomega.Equal(resource.MustParse("750Mi")), "Hard memory value for ResourceQuota %q is %s not 750Mi.", patchedResourceQuota.ObjectMeta.Name, patchedResourceQuota.Spec.Hard.Memory().String())
		gomega.Expect(resourceversion.CompareResourceVersion(rq.Items[0].ResourceVersion, patchedResourceQuota.ResourceVersion)).To(gomega.BeNumerically("==", -1), "patched object should have a larger resource version")

		ginkgo.By("Deleting a Collection of ResourceQuotas")
		err = client.CoreV1().ResourceQuotas(ns).DeleteCollection(ctx, metav1.DeleteOptions{}, metav1.ListOptions{LabelSelector: labelSelector})
		framework.ExpectNoError(err)

		ginkgo.By("Verifying the deleted ResourceQuota")
		_, err = client.CoreV1().ResourceQuotas(ns).Get(ctx, rqName, metav1.GetOptions{})
		if !apierrors.IsNotFound(err) {
			framework.Failf("Expected `not found` error, got: %v", err)
		}
	})

	/*
		发布版本：v1.26
		测试名称：ResourceQuota, apply changes to a ResourceQuota status
		描述：尝试创建 CPU 和 Memory 配额限制的
		ResourceQuota。创建必须成功。更新 hard
		状态值必须成功，且必须能查到新值。
		上报的 hard 状态值必须等于 spec 中的 hard 值。
		patch spec hard 值必须成功，且必须能查到
		新值。patch hard 状态值必须成功。
		上报的 hard 状态值必须等于 spec 中新的 hard 值。
		获取 /status 必须成功，且上报的 hard 状态
		值必须等于 spec hard 值。再次 patch hard 状态
		值必须成功。patch /status 时 spec 必须保持
		不变。
	*/
	framework.ConformanceIt("should apply changes to a resourcequota status", func(ctx context.Context) {
		ns := f.Namespace.Name
		rqClient := f.ClientSet.CoreV1().ResourceQuotas(ns)
		rqName := "e2e-rq-status-" + utilrand.String(5)
		label := map[string]string{"e2e-rq-label": rqName}
		labelSelector := labels.SelectorFromSet(label).String()

		w := &cache.ListWatch{
			WatchFunc: func(options metav1.ListOptions) (watch.Interface, error) {
				options.LabelSelector = labelSelector
				return rqClient.Watch(ctx, options)
			},
		}

		rqList, err := f.ClientSet.CoreV1().ResourceQuotas("").List(ctx, metav1.ListOptions{LabelSelector: labelSelector})
		framework.ExpectNoError(err, "failed to list Services")

		ginkgo.By(fmt.Sprintf("Creating resourceQuota %q", rqName))
		resourceQuota := &v1.ResourceQuota{
			ObjectMeta: metav1.ObjectMeta{
				Name:   rqName,
				Labels: label,
			},
			Spec: v1.ResourceQuotaSpec{
				Hard: v1.ResourceList{
					v1.ResourceCPU:    resource.MustParse("500m"),
					v1.ResourceMemory: resource.MustParse("500Mi"),
				},
			},
		}
		_, err = createResourceQuota(ctx, f.ClientSet, ns, resourceQuota)
		framework.ExpectNoError(err)

		initialResourceQuota, err := rqClient.Get(ctx, rqName, metav1.GetOptions{})
		framework.ExpectNoError(err)
		gomega.Expect(*initialResourceQuota.Spec.Hard.Cpu()).To(gomega.Equal(resource.MustParse("500m")), "Hard cpu value for ResourceQuota %q is %s not 500m.", initialResourceQuota.Name, initialResourceQuota.Spec.Hard.Cpu().String())
		framework.Logf("Resource quota %q reports spec: hard cpu limit of %s", rqName, initialResourceQuota.Spec.Hard.Cpu())
		gomega.Expect(*initialResourceQuota.Spec.Hard.Memory()).To(gomega.Equal(resource.MustParse("500Mi")), "Hard memory value for ResourceQuota %q is %s not 500Mi.", initialResourceQuota.Name, initialResourceQuota.Spec.Hard.Memory().String())
		framework.Logf("Resource quota %q reports spec: hard memory limit of %s", rqName, initialResourceQuota.Spec.Hard.Memory())

		ginkgo.By(fmt.Sprintf("Updating resourceQuota %q /status", rqName))
		var updatedResourceQuota *v1.ResourceQuota
		hardLimits := quota.Add(v1.ResourceList{}, initialResourceQuota.Spec.Hard)

		err = retry.RetryOnConflict(retry.DefaultRetry, func() error {
			updateStatus, err := rqClient.Get(ctx, rqName, metav1.GetOptions{})
			framework.ExpectNoError(err, "Unable to get ResourceQuota %q", rqName)
			updateStatus.Status = v1.ResourceQuotaStatus{
				Hard: hardLimits,
			}
			updatedResourceQuota, err = rqClient.UpdateStatus(ctx, updateStatus, metav1.UpdateOptions{})
			return err
		})
		framework.ExpectNoError(err, "Failed to update resourceQuota")

		ginkgo.By(fmt.Sprintf("Confirm /status for %q resourceQuota via watch", rqName))
		ctxUntil, cancel := context.WithTimeout(ctx, f.Timeouts.PodStartShort)
		defer cancel()

		_, err = watchtools.Until(ctxUntil, rqList.ResourceVersion, w, func(event watch.Event) (bool, error) {
			if rq, ok := event.Object.(*v1.ResourceQuota); ok {
				found := rq.Name == updatedResourceQuota.Name &&
					rq.Namespace == ns &&
					apiequality.Semantic.DeepEqual(rq.Status.Hard, updatedResourceQuota.Spec.Hard)
				if !found {
					framework.Logf("observed resourceQuota %q in namespace %q with hard status: %#v", rq.Name, rq.Namespace, rq.Status.Hard)
					return false, nil
				}
				framework.Logf("Found resourceQuota %q in namespace %q with hard status: %#v", rq.Name, rq.Namespace, rq.Status.Hard)
				return found, nil
			}
			framework.Logf("Observed event: %+v", event.Object)
			return false, nil
		})
		framework.ExpectNoError(err, "failed to locate ResourceQuota %q in namespace %q", updatedResourceQuota.Name, ns)
		framework.Logf("ResourceQuota %q /status was updated", updatedResourceQuota.Name)

		// patch /status 前先同步 resourceQuota 列表
		rqList, err = f.ClientSet.CoreV1().ResourceQuotas("").List(ctx, metav1.ListOptions{LabelSelector: labelSelector})
		framework.ExpectNoError(err, "failed to list Services")

		ginkgo.By("Patching hard spec values for cpu & memory")
		xResourceQuota, err := rqClient.Patch(ctx, updatedResourceQuota.Name, types.StrategicMergePatchType,
			[]byte(`{"spec":{"hard":{"cpu":"1","memory":"1Gi"}}}`),
			metav1.PatchOptions{})
		framework.ExpectNoError(err, "Could not patch resourcequota %q. Error: %v", xResourceQuota.Name, err)
		framework.Logf("Resource quota %q reports spec: hard cpu limit of %s", rqName, xResourceQuota.Spec.Hard.Cpu())
		framework.Logf("Resource quota %q reports spec: hard memory limit of %s", rqName, xResourceQuota.Spec.Hard.Memory())

		ginkgo.By(fmt.Sprintf("Patching %q /status", rqName))
		hardLimits = quota.Add(v1.ResourceList{}, xResourceQuota.Spec.Hard)

		rqStatusJSON, err := json.Marshal(hardLimits)
		framework.ExpectNoError(err)
		patchedResourceQuota, err := rqClient.Patch(ctx, rqName, types.StrategicMergePatchType,
			[]byte(`{"status": {"hard": `+string(rqStatusJSON)+`}}`),
			metav1.PatchOptions{}, "status")
		framework.ExpectNoError(err)

		ginkgo.By(fmt.Sprintf("Confirm /status for %q resourceQuota via watch", rqName))
		ctxUntil, cancel = context.WithTimeout(ctx, f.Timeouts.PodStartShort)
		defer cancel()

		_, err = watchtools.Until(ctxUntil, rqList.ResourceVersion, w, func(event watch.Event) (bool, error) {
			if rq, ok := event.Object.(*v1.ResourceQuota); ok {
				found := rq.Name == patchedResourceQuota.Name &&
					rq.Namespace == ns &&
					apiequality.Semantic.DeepEqual(rq.Status.Hard, patchedResourceQuota.Spec.Hard)
				if !found {
					framework.Logf("observed resourceQuota %q in namespace %q with hard status: %#v", rq.Name, rq.Namespace, rq.Status.Hard)
					return false, nil
				}
				framework.Logf("Found resourceQuota %q in namespace %q with hard status: %#v", rq.Name, rq.Namespace, rq.Status.Hard)
				return found, nil
			}
			framework.Logf("Observed event: %+v", event.Object)
			return false, nil
		})
		framework.ExpectNoError(err, "failed to locate ResourceQuota %q in namespace %q", patchedResourceQuota.Name, ns)
		framework.Logf("ResourceQuota %q /status was patched", patchedResourceQuota.Name)

		ginkgo.By(fmt.Sprintf("Get %q /status", rqName))
		rqResource := schema.GroupVersionResource{Group: "", Version: "v1", Resource: "resourcequotas"}
		unstruct, err := f.DynamicClient.Resource(rqResource).Namespace(ns).Get(ctx, resourceQuota.Name, metav1.GetOptions{}, "status")
		framework.ExpectNoError(err)

		rq, err := unstructuredToResourceQuota(unstruct)
		framework.ExpectNoError(err, "Getting the status of the resource quota %q", rq.Name)

		gomega.Expect(*rq.Status.Hard.Cpu()).To(gomega.Equal(resource.MustParse("1")), "Hard cpu value for ResourceQuota %q is %s not 1.", rq.Name, rq.Status.Hard.Cpu().String())
		framework.Logf("Resourcequota %q reports status: hard cpu of %s", rqName, rq.Status.Hard.Cpu())
		gomega.Expect(*rq.Status.Hard.Memory()).To(gomega.Equal(resource.MustParse("1Gi")), "Hard memory value for ResourceQuota %q is %s not 1Gi.", rq.Name, rq.Status.Hard.Memory().String())
		framework.Logf("Resourcequota %q reports status: hard memory of %s", rqName, rq.Status.Hard.Memory())

		// 再次 patch /status 前先同步 resourceQuota 列表
		rqList, err = f.ClientSet.CoreV1().ResourceQuotas("").List(ctx, metav1.ListOptions{LabelSelector: labelSelector})
		framework.ExpectNoError(err, "failed to list Services")

		ginkgo.By(fmt.Sprintf("Repatching %q /status before checking Spec is unchanged", rqName))
		newHardLimits := v1.ResourceList{
			v1.ResourceCPU:    resource.MustParse("2"),
			v1.ResourceMemory: resource.MustParse("2Gi"),
		}
		rqStatusJSON, err = json.Marshal(newHardLimits)
		framework.ExpectNoError(err)

		repatchedResourceQuota, err := rqClient.Patch(ctx, rqName, types.StrategicMergePatchType,
			[]byte(`{"status": {"hard": `+string(rqStatusJSON)+`}}`),
			metav1.PatchOptions{}, "status")
		framework.ExpectNoError(err)

		gomega.Expect(*repatchedResourceQuota.Status.Hard.Cpu()).To(gomega.Equal(resource.MustParse("2")), "Hard cpu value for ResourceQuota %q is %s not 2.", repatchedResourceQuota.Name, repatchedResourceQuota.Status.Hard.Cpu().String())
		framework.Logf("Resourcequota %q reports status: hard cpu of %s", repatchedResourceQuota.Name, repatchedResourceQuota.Status.Hard.Cpu())
		gomega.Expect(*repatchedResourceQuota.Status.Hard.Memory()).To(gomega.Equal(resource.MustParse("2Gi")), "Hard memory value for ResourceQuota %q is %s not 2Gi.", repatchedResourceQuota.Name, repatchedResourceQuota.Status.Hard.Memory().String())
		framework.Logf("Resourcequota %q reports status: hard memory of %s", repatchedResourceQuota.Name, repatchedResourceQuota.Status.Hard.Memory())

		_, err = watchtools.Until(ctxUntil, rqList.ResourceVersion, w, func(event watch.Event) (bool, error) {
			if rq, ok := event.Object.(*v1.ResourceQuota); ok {
				found := rq.Name == patchedResourceQuota.Name &&
					rq.Namespace == ns &&
					*rq.Status.Hard.Cpu() == resource.MustParse("2") &&
					*rq.Status.Hard.Memory() == resource.MustParse("2Gi")
				if !found {
					framework.Logf("observed resourceQuota %q in namespace %q with hard status: %#v", rq.Name, rq.Namespace, rq.Status.Hard)
					return false, nil
				}
				framework.Logf("Found resourceQuota %q in namespace %q with hard status: %#v", rq.Name, rq.Namespace, rq.Status.Hard)
				return found, nil
			}
			framework.Logf("Observed event: %+v", event.Object)
			return false, nil
		})
		framework.ExpectNoError(err, "failed to locate ResourceQuota %q in namespace %q", patchedResourceQuota.Name, ns)

		// resource_quota_controller 会忽略状态变更，因此必须等 controller 完整
		// 重新同步后再协调状态。默认每 5 分钟全量同步一次，所以至少要额外轮询
		// 一分钟，以防刚开始轮询时全量同步刚发生过，必须再等
		// 下一次全量同步。
		// Ref: https://issues.k8s.io/121911
		err = wait.PollUntilContextTimeout(ctx, 5*time.Second, 6*time.Minute, true, func(ctx context.Context) (bool, error) {
			resourceQuotaResult, err := rqClient.Get(ctx, rqName, metav1.GetOptions{})
			if err != nil {
				return false, nil
			}

			if *resourceQuotaResult.Spec.Hard.Cpu() == *resourceQuotaResult.Status.Hard.Cpu() {
				if *resourceQuotaResult.Status.Hard.Cpu() != resource.MustParse("1") {
					framework.Logf("Hard cpu status value for ResourceQuota %q is %s not 1.", repatchedResourceQuota.Name, resourceQuotaResult.Status.Hard.Cpu().String())
					return false, nil
				}
				if *resourceQuotaResult.Status.Hard.Memory() != resource.MustParse("1Gi") {
					framework.Logf("Hard memory status value for ResourceQuota %q is %s not 1Gi.", repatchedResourceQuota.Name, resourceQuotaResult.Status.Hard.Memory().String())
					return false, nil
				}
				framework.Logf("ResourceQuota %q Spec was unchanged and /status reset", resourceQuotaResult.Name)
				return true, nil
			}
			framework.Logf("ResourceQuota %q Spec and Status does not match: %#v", resourceQuotaResult.Name, resourceQuotaResult)
			return false, nil
		})
		if err != nil {
			framework.Failf("Error waiting for ResourceQuota %q to reset its Status: %v", patchedResourceQuota.Name, err)
		}

	})
})

var _ = SIGDescribe("ResourceQuota", framework.WithFeatureGate(features.VolumeAttributesClass), func() {
	f := framework.NewDefaultFramework("resourcequota-volumeattributesclass")
	f.NamespacePodSecurityLevel = admissionapi.LevelBaseline

	ginkgo.It("should verify ResourceQuota's volume attributes class scope (quota set to pvc count: 1) against 2 pvcs with same volume attributes class.", func(ctx context.Context) {
		hard := v1.ResourceList{}
		hard[v1.ResourceRequestsStorage] = resource.MustParse("5Gi")
		hard[v1.ResourcePersistentVolumeClaims] = resource.MustParse("1")

		ginkgo.By("Creating a ResourceQuota with volume attributes class scope")
		quota, err := createResourceQuota(ctx, f.ClientSet, f.Namespace.Name, newTestResourceQuotaWithScopeForVolumeAttributesClass("quota-volumeattributesclass", hard, v1.ScopeSelectorOpIn, []string{classGold}))
		framework.ExpectNoError(err)

		ginkgo.By("Ensuring ResourceQuota status is calculated")
		usedResources := v1.ResourceList{}
		usedResources[v1.ResourceRequestsStorage] = resource.MustParse("0")
		usedResources[v1.ResourcePersistentVolumeClaims] = resource.MustParse("0")
		err = waitForResourceQuota(ctx, f.ClientSet, f.Namespace.Name, quota.Name, usedResources)
		framework.ExpectNoError(err)

		ginkgo.By("Creating a pvc with volume attributes class")
		pvc1 := newTestPersistentVolumeClaimForQuota("test-claim-1")
		pvc1.Spec.StorageClassName = ptr.To("")
		pvc1.Spec.VolumeAttributesClassName = &classGold
		pvc1, err = f.ClientSet.CoreV1().PersistentVolumeClaims(f.Namespace.Name).Create(ctx, pvc1, metav1.CreateOptions{})
		framework.ExpectNoError(err)

		ginkgo.By("Ensuring resource quota with volume attributes class scope captures the pvc usage")
		usedResources[v1.ResourceRequestsStorage] = resource.MustParse("1Gi")
		usedResources[v1.ResourcePersistentVolumeClaims] = resource.MustParse("1")
		err = waitForResourceQuota(ctx, f.ClientSet, f.Namespace.Name, quota.Name, usedResources)
		framework.ExpectNoError(err)

		ginkgo.By("Creating 2nd pod with priority class should fail")
		pvc2 := newTestPersistentVolumeClaimForQuota("test-claim-2")
		pvc2.Spec.StorageClassName = ptr.To("")
		pvc2.Spec.VolumeAttributesClassName = &classGold
		_, err = f.ClientSet.CoreV1().PersistentVolumeClaims(f.Namespace.Name).Create(ctx, pvc2, metav1.CreateOptions{})
		gomega.Expect(err).To(gomega.MatchError(apierrors.IsForbidden, "expect a forbidden error when creating a PVC that exceeds quota"))

		ginkgo.By("Deleting first pvc")
		err = f.ClientSet.CoreV1().PersistentVolumeClaims(f.Namespace.Name).Delete(ctx, pvc1.Name, metav1.DeleteOptions{})
		framework.ExpectNoError(err)

		ginkgo.By("Ensuring resource quota status released the pvc usage")
		usedResources[v1.ResourceRequestsStorage] = resource.MustParse("0")
		usedResources[v1.ResourcePersistentVolumeClaims] = resource.MustParse("0")
		err = waitForResourceQuota(ctx, f.ClientSet, f.Namespace.Name, quota.Name, usedResources)
		framework.ExpectNoError(err)
	})

	ginkgo.It("should verify ResourceQuota's volume attributes class scope (quota set to pvc count: 1) against a pvc with different volume attributes class.", func(ctx context.Context) {
		hard := v1.ResourceList{}
		hard[v1.ResourceRequestsStorage] = resource.MustParse("5Gi")
		hard[v1.ResourcePersistentVolumeClaims] = resource.MustParse("1")

		ginkgo.By("Creating 2 ResourceQuotas with volume attributes class scope")
		quotaGold, err := createResourceQuota(ctx, f.ClientSet, f.Namespace.Name, newTestResourceQuotaWithScopeForVolumeAttributesClass("quota-volumeattributesclass-gold", hard, v1.ScopeSelectorOpIn, []string{classGold}))
		framework.ExpectNoError(err)
		quotaSilver, err := createResourceQuota(ctx, f.ClientSet, f.Namespace.Name, newTestResourceQuotaWithScopeForVolumeAttributesClass("quota-volumeattributesclass-silver", hard, v1.ScopeSelectorOpIn, []string{classSilver}))
		framework.ExpectNoError(err)

		ginkgo.By("Ensuring all ResourceQuotas status is calculated")
		usedResources := v1.ResourceList{}
		usedResources[v1.ResourceRequestsStorage] = resource.MustParse("0")
		usedResources[v1.ResourcePersistentVolumeClaims] = resource.MustParse("0")
		err = waitForResourceQuota(ctx, f.ClientSet, f.Namespace.Name, quotaGold.Name, usedResources)
		framework.ExpectNoError(err)
		err = waitForResourceQuota(ctx, f.ClientSet, f.Namespace.Name, quotaSilver.Name, usedResources)
		framework.ExpectNoError(err)

		ginkgo.By("Creating a pvc with volume attributes class gold")
		pvcName := "test-claim"
		pvc, pv := newTestPersistentVolumeClaimWithFakeCSIVolumeForQuota(f.Namespace.Name, pvcName, &classGold)
		_, err = f.ClientSet.CoreV1().PersistentVolumeClaims(f.Namespace.Name).Create(ctx, pvc, metav1.CreateOptions{})
		framework.ExpectNoError(err)
		_, err = f.ClientSet.CoreV1().PersistentVolumes().Create(ctx, pv, metav1.CreateOptions{})
		framework.ExpectNoError(err)
		ginkgo.DeferCleanup(framework.IgnoreNotFound(f.ClientSet.CoreV1().PersistentVolumes().Delete), pv.Name, metav1.DeleteOptions{})

		ginkgo.By("Waiting for the PVC to be bound")
		pvs, err := e2epv.WaitForPVClaimBoundPhase(ctx, f.ClientSet, []*v1.PersistentVolumeClaim{pvc}, framework.ClaimProvisionTimeout)
		framework.ExpectNoError(err)
		gomega.Expect(pvs).To(gomega.HaveLen(1))
		gomega.Expect(pvs[0].Name).To(gomega.Equal(pv.Name), "Expected PV %q to be bound to PVC %q", pv.Name, pvc.Name)

		ginkgo.By("Ensuring resource quota with volume attributes class scope captures the pvc usage")
		usedResources[v1.ResourceRequestsStorage] = resource.MustParse("1Gi")
		usedResources[v1.ResourcePersistentVolumeClaims] = resource.MustParse("1")
		err = waitForResourceQuota(ctx, f.ClientSet, f.Namespace.Name, quotaGold.Name, usedResources)
		framework.ExpectNoError(err)

		ginkgo.By("Updating the desired volume attributes class of the pvc to silver")
		err = retry.RetryOnConflict(retry.DefaultRetry, func() error {
			gotPVC, err := f.ClientSet.CoreV1().PersistentVolumeClaims(f.Namespace.Name).Get(ctx, pvcName, metav1.GetOptions{})
			if err != nil {
				return err
			}
			gotPVC.Spec.VolumeAttributesClassName = &classSilver
			_, err = f.ClientSet.CoreV1().PersistentVolumeClaims(f.Namespace.Name).Update(ctx, gotPVC, metav1.UpdateOptions{})
			return err
		})
		framework.ExpectNoError(err)

		ginkgo.By("Ensuring resource quota with volume attributes class scope captures the pvc usage")
		// pvc 引用两个不同的 class：spec 中的代表期望的 class，
		// status 中的代表当前的 class。因此每项配额的实际用量都是 1。
		usedResources[v1.ResourceRequestsStorage] = resource.MustParse("1Gi")
		usedResources[v1.ResourcePersistentVolumeClaims] = resource.MustParse("1")
		err = waitForResourceQuota(ctx, f.ClientSet, f.Namespace.Name, quotaSilver.Name, usedResources)
		framework.ExpectNoError(err)
		err = waitForResourceQuota(ctx, f.ClientSet, f.Namespace.Name, quotaGold.Name, usedResources)
		framework.ExpectNoError(err)

		ginkgo.By("Update the pv to have the volume attributes class silver")
		err = retry.RetryOnConflict(retry.DefaultRetry, func() error {
			gotPV, err := f.ClientSet.CoreV1().PersistentVolumes().Get(ctx, pv.Name, metav1.GetOptions{})
			if err != nil {
				return err
			}
			gotPV.Spec.VolumeAttributesClassName = &classSilver
			_, err = f.ClientSet.CoreV1().PersistentVolumes().Update(ctx, gotPV, metav1.UpdateOptions{})
			return err
		})
		framework.ExpectNoError(err)

		ginkgo.By("Update the current volume attributes class of the pvc to silver")
		err = retry.RetryOnConflict(retry.DefaultRetry, func() error { // no real driver, so we have to simulate the driver behavior
			gotPVC, err := f.ClientSet.CoreV1().PersistentVolumeClaims(f.Namespace.Name).Get(ctx, pvcName, metav1.GetOptions{})
			if err != nil {
				return err
			}
			gotPVC.Status.CurrentVolumeAttributesClassName = &classSilver
			_, err = f.ClientSet.CoreV1().PersistentVolumeClaims(f.Namespace.Name).UpdateStatus(ctx, gotPVC, metav1.UpdateOptions{})
			return err
		})
		framework.ExpectNoError(err)

		ginkgo.By("Ensuring resource quota with volume attributes class scope captures the pvc usage")
		usedResources[v1.ResourceRequestsStorage] = resource.MustParse("0")
		usedResources[v1.ResourcePersistentVolumeClaims] = resource.MustParse("0")
		err = waitForResourceQuota(ctx, f.ClientSet, f.Namespace.Name, quotaGold.Name, usedResources)
		framework.ExpectNoError(err)
		usedResources[v1.ResourceRequestsStorage] = resource.MustParse("1Gi")
		usedResources[v1.ResourcePersistentVolumeClaims] = resource.MustParse("1")
		err = waitForResourceQuota(ctx, f.ClientSet, f.Namespace.Name, quotaSilver.Name, usedResources)
		framework.ExpectNoError(err)
	})
})

var _ = SIGDescribe("ResourceQuota", func() {
	f := framework.NewDefaultFramework("scope-selectors")
	f.NamespacePodSecurityLevel = admissionapi.LevelBaseline
	ginkgo.It("should verify ResourceQuota with best effort scope using scope-selectors.", func(ctx context.Context) {
		ginkgo.By("Creating a ResourceQuota with best effort scope")
		resourceQuotaBestEffort, err := createResourceQuota(ctx, f.ClientSet, f.Namespace.Name, newTestResourceQuotaWithScopeSelector("quota-besteffort", v1.ResourceQuotaScopeBestEffort))
		framework.ExpectNoError(err)

		ginkgo.By("Ensuring ResourceQuota status is calculated")
		usedResources := v1.ResourceList{}
		usedResources[v1.ResourcePods] = resource.MustParse("0")
		err = waitForResourceQuota(ctx, f.ClientSet, f.Namespace.Name, resourceQuotaBestEffort.Name, usedResources)
		framework.ExpectNoError(err)

		ginkgo.By("Creating a ResourceQuota with not best effort scope")
		resourceQuotaNotBestEffort, err := createResourceQuota(ctx, f.ClientSet, f.Namespace.Name, newTestResourceQuotaWithScopeSelector("quota-not-besteffort", v1.ResourceQuotaScopeNotBestEffort))
		framework.ExpectNoError(err)

		ginkgo.By("Ensuring ResourceQuota status is calculated")
		err = waitForResourceQuota(ctx, f.ClientSet, f.Namespace.Name, resourceQuotaNotBestEffort.Name, usedResources)
		framework.ExpectNoError(err)

		ginkgo.By("Creating a best-effort pod")
		pod := newTestPodForQuota(f, podName, v1.ResourceList{}, v1.ResourceList{})
		pod, err = f.ClientSet.CoreV1().Pods(f.Namespace.Name).Create(ctx, pod, metav1.CreateOptions{})
		framework.ExpectNoError(err)

		ginkgo.By("Ensuring resource quota with best effort scope captures the pod usage")
		usedResources[v1.ResourcePods] = resource.MustParse("1")
		err = waitForResourceQuota(ctx, f.ClientSet, f.Namespace.Name, resourceQuotaBestEffort.Name, usedResources)
		framework.ExpectNoError(err)

		ginkgo.By("Ensuring resource quota with not best effort ignored the pod usage")
		usedResources[v1.ResourcePods] = resource.MustParse("0")
		err = waitForResourceQuota(ctx, f.ClientSet, f.Namespace.Name, resourceQuotaNotBestEffort.Name, usedResources)
		framework.ExpectNoError(err)

		ginkgo.By("Deleting the pod")
		err = f.ClientSet.CoreV1().Pods(f.Namespace.Name).Delete(ctx, pod.Name, *metav1.NewDeleteOptions(0))
		framework.ExpectNoError(err)

		ginkgo.By("Ensuring resource quota status released the pod usage")
		usedResources[v1.ResourcePods] = resource.MustParse("0")
		err = waitForResourceQuota(ctx, f.ClientSet, f.Namespace.Name, resourceQuotaBestEffort.Name, usedResources)
		framework.ExpectNoError(err)

		ginkgo.By("Creating a not best-effort pod")
		requests := v1.ResourceList{}
		requests[v1.ResourceCPU] = resource.MustParse("500m")
		requests[v1.ResourceMemory] = resource.MustParse("200Mi")
		limits := v1.ResourceList{}
		limits[v1.ResourceCPU] = resource.MustParse("1")
		limits[v1.ResourceMemory] = resource.MustParse("400Mi")
		pod = newTestPodForQuota(f, "burstable-pod", requests, limits)
		pod, err = f.ClientSet.CoreV1().Pods(f.Namespace.Name).Create(ctx, pod, metav1.CreateOptions{})
		framework.ExpectNoError(err)

		ginkgo.By("Ensuring resource quota with not best effort scope captures the pod usage")
		usedResources[v1.ResourcePods] = resource.MustParse("1")
		err = waitForResourceQuota(ctx, f.ClientSet, f.Namespace.Name, resourceQuotaNotBestEffort.Name, usedResources)
		framework.ExpectNoError(err)

		ginkgo.By("Ensuring resource quota with best effort scope ignored the pod usage")
		usedResources[v1.ResourcePods] = resource.MustParse("0")
		err = waitForResourceQuota(ctx, f.ClientSet, f.Namespace.Name, resourceQuotaBestEffort.Name, usedResources)
		framework.ExpectNoError(err)

		ginkgo.By("Deleting the pod")
		err = f.ClientSet.CoreV1().Pods(f.Namespace.Name).Delete(ctx, pod.Name, *metav1.NewDeleteOptions(0))
		framework.ExpectNoError(err)

		ginkgo.By("Ensuring resource quota status released the pod usage")
		usedResources[v1.ResourcePods] = resource.MustParse("0")
		err = waitForResourceQuota(ctx, f.ClientSet, f.Namespace.Name, resourceQuotaNotBestEffort.Name, usedResources)
		framework.ExpectNoError(err)
	})
	ginkgo.It("should verify ResourceQuota with terminating scopes through scope selectors.", func(ctx context.Context) {
		ginkgo.By("Creating a ResourceQuota with terminating scope")
		quotaTerminatingName := "quota-terminating"
		resourceQuotaTerminating, err := createResourceQuota(ctx, f.ClientSet, f.Namespace.Name, newTestResourceQuotaWithScopeSelector(quotaTerminatingName, v1.ResourceQuotaScopeTerminating))
		framework.ExpectNoError(err)

		ginkgo.By("Ensuring ResourceQuota status is calculated")
		usedResources := v1.ResourceList{}
		usedResources[v1.ResourcePods] = resource.MustParse("0")
		err = waitForResourceQuota(ctx, f.ClientSet, f.Namespace.Name, resourceQuotaTerminating.Name, usedResources)
		framework.ExpectNoError(err)

		ginkgo.By("Creating a ResourceQuota with not terminating scope")
		quotaNotTerminatingName := "quota-not-terminating"
		resourceQuotaNotTerminating, err := createResourceQuota(ctx, f.ClientSet, f.Namespace.Name, newTestResourceQuotaWithScopeSelector(quotaNotTerminatingName, v1.ResourceQuotaScopeNotTerminating))
		framework.ExpectNoError(err)

		ginkgo.By("Ensuring ResourceQuota status is calculated")
		err = waitForResourceQuota(ctx, f.ClientSet, f.Namespace.Name, resourceQuotaNotTerminating.Name, usedResources)
		framework.ExpectNoError(err)

		requests := v1.ResourceList{}
		requests[v1.ResourceCPU] = resource.MustParse("500m")
		requests[v1.ResourceMemory] = resource.MustParse("200Mi")
		limits := v1.ResourceList{}
		limits[v1.ResourceCPU] = resource.MustParse("1")
		limits[v1.ResourceMemory] = resource.MustParse("400Mi")

		podName1 := "test-pod"
		pod1 := newTestPodForQuota(f, podName1, requests, limits)

		podName2 := "terminating-pod"
		pod2 := newTestPodForQuota(f, podName2, requests, limits)
		activeDeadlineSeconds := int64(3600)
		pod2.Spec.ActiveDeadlineSeconds = &activeDeadlineSeconds

		ginkgo.By("Creating a long running pod")
		_, err = f.ClientSet.CoreV1().Pods(f.Namespace.Name).Create(ctx, pod1, metav1.CreateOptions{})
		framework.ExpectNoError(err)

		ginkgo.By("Ensuring resource quota with not terminating scope captures the pod usage")
		usedResources[v1.ResourcePods] = resource.MustParse("1")
		usedResources[v1.ResourceRequestsCPU] = requests[v1.ResourceCPU]
		usedResources[v1.ResourceRequestsMemory] = requests[v1.ResourceMemory]
		usedResources[v1.ResourceLimitsCPU] = limits[v1.ResourceCPU]
		usedResources[v1.ResourceLimitsMemory] = limits[v1.ResourceMemory]
		err = waitForResourceQuota(ctx, f.ClientSet, f.Namespace.Name, resourceQuotaNotTerminating.Name, usedResources)
		framework.ExpectNoError(err)

		ginkgo.By("Ensuring resource quota with terminating scope ignored the pod usage")
		usedResources[v1.ResourcePods] = resource.MustParse("0")
		usedResources[v1.ResourceRequestsCPU] = resource.MustParse("0")
		usedResources[v1.ResourceRequestsMemory] = resource.MustParse("0")
		usedResources[v1.ResourceLimitsCPU] = resource.MustParse("0")
		usedResources[v1.ResourceLimitsMemory] = resource.MustParse("0")
		err = ensureResourceQuota(ctx, f.ClientSet, f.Namespace.Name, resourceQuotaTerminating.Name, usedResources)
		framework.ExpectNoError(err)

		ginkgo.By("Updating the pod to have an active deadline")
		err = retry.RetryOnConflict(retry.DefaultRetry, func() error {
			gotPod, err := f.ClientSet.CoreV1().Pods(f.Namespace.Name).Get(ctx, podName1, metav1.GetOptions{})
			if err != nil {
				return err
			}
			gotPod.Spec.ActiveDeadlineSeconds = &activeDeadlineSeconds
			_, err = f.ClientSet.CoreV1().Pods(f.Namespace.Name).Update(ctx, gotPod, metav1.UpdateOptions{})
			return err
		})
		framework.ExpectNoError(err)

		ginkgo.By("Creating second terminating pod")
		_, err = f.ClientSet.CoreV1().Pods(f.Namespace.Name).Create(ctx, pod2, metav1.CreateOptions{})
		gomega.Expect(err).To(gomega.MatchError(apierrors.IsForbidden, "expect a forbidden error when creating a Pod that exceeds quota"))

		ginkgo.By("Ensuring resource quota with terminating scope captures the pod usage")
		usedResources[v1.ResourcePods] = resource.MustParse("1")
		usedResources[v1.ResourceRequestsCPU] = requests[v1.ResourceCPU]
		usedResources[v1.ResourceRequestsMemory] = requests[v1.ResourceMemory]
		usedResources[v1.ResourceLimitsCPU] = limits[v1.ResourceCPU]
		usedResources[v1.ResourceLimitsMemory] = limits[v1.ResourceMemory]
		err = waitForResourceQuota(ctx, f.ClientSet, f.Namespace.Name, resourceQuotaTerminating.Name, usedResources)
		framework.ExpectNoError(err)

		ginkgo.By("Ensuring resource quota with not terminating scope ignored the pod usage")
		usedResources[v1.ResourcePods] = resource.MustParse("0")
		usedResources[v1.ResourceRequestsCPU] = resource.MustParse("0")
		usedResources[v1.ResourceRequestsMemory] = resource.MustParse("0")
		usedResources[v1.ResourceLimitsCPU] = resource.MustParse("0")
		usedResources[v1.ResourceLimitsMemory] = resource.MustParse("0")
		err = waitForResourceQuota(ctx, f.ClientSet, f.Namespace.Name, resourceQuotaNotTerminating.Name, usedResources)
		framework.ExpectNoError(err)

		ginkgo.By("Deleting the pod")
		err = f.ClientSet.CoreV1().Pods(f.Namespace.Name).Delete(ctx, podName1, *metav1.NewDeleteOptions(0))
		framework.ExpectNoError(err)

		ginkgo.By("Ensuring resource quota status released the pod usage")
		usedResources[v1.ResourcePods] = resource.MustParse("0")
		usedResources[v1.ResourceRequestsCPU] = resource.MustParse("0")
		usedResources[v1.ResourceRequestsMemory] = resource.MustParse("0")
		usedResources[v1.ResourceLimitsCPU] = resource.MustParse("0")
		usedResources[v1.ResourceLimitsMemory] = resource.MustParse("0")
		err = waitForResourceQuota(ctx, f.ClientSet, f.Namespace.Name, resourceQuotaTerminating.Name, usedResources)
		framework.ExpectNoError(err)

		ginkgo.By("Creating a terminating pod")
		// 重试创建 pod，因为 admission controller 看到的配额用量可能滞后于 ResourceQuota 状态。
		err = wait.PollUntilContextTimeout(ctx, 1*time.Second, 30*time.Second, false, func(ctx context.Context) (bool, error) {
			_, err = f.ClientSet.CoreV1().Pods(f.Namespace.Name).Create(ctx, pod2, metav1.CreateOptions{})
			if err != nil {
				if apierrors.IsForbidden(err) {
					return false, nil
				}
				return false, err
			}
			return true, nil
		})
		framework.ExpectNoError(err)

		ginkgo.By("Ensuring resource quota with terminating scope captures the pod usage")
		usedResources[v1.ResourcePods] = resource.MustParse("1")
		usedResources[v1.ResourceRequestsCPU] = requests[v1.ResourceCPU]
		usedResources[v1.ResourceRequestsMemory] = requests[v1.ResourceMemory]
		usedResources[v1.ResourceLimitsCPU] = limits[v1.ResourceCPU]
		usedResources[v1.ResourceLimitsMemory] = limits[v1.ResourceMemory]
		err = waitForResourceQuota(ctx, f.ClientSet, f.Namespace.Name, resourceQuotaTerminating.Name, usedResources)
		framework.ExpectNoError(err)

		ginkgo.By("Ensuring resource quota with not terminating scope ignored the pod usage")
		usedResources[v1.ResourcePods] = resource.MustParse("0")
		usedResources[v1.ResourceRequestsCPU] = resource.MustParse("0")
		usedResources[v1.ResourceRequestsMemory] = resource.MustParse("0")
		usedResources[v1.ResourceLimitsCPU] = resource.MustParse("0")
		usedResources[v1.ResourceLimitsMemory] = resource.MustParse("0")
		err = waitForResourceQuota(ctx, f.ClientSet, f.Namespace.Name, resourceQuotaNotTerminating.Name, usedResources)
		framework.ExpectNoError(err)

		ginkgo.By("Deleting the pod")
		err = f.ClientSet.CoreV1().Pods(f.Namespace.Name).Delete(ctx, podName2, *metav1.NewDeleteOptions(0))
		framework.ExpectNoError(err)

		ginkgo.By("Ensuring resource quota status released the pod usage")
		usedResources[v1.ResourcePods] = resource.MustParse("0")
		usedResources[v1.ResourceRequestsCPU] = resource.MustParse("0")
		usedResources[v1.ResourceRequestsMemory] = resource.MustParse("0")
		usedResources[v1.ResourceLimitsCPU] = resource.MustParse("0")
		usedResources[v1.ResourceLimitsMemory] = resource.MustParse("0")
		err = waitForResourceQuota(ctx, f.ClientSet, f.Namespace.Name, resourceQuotaTerminating.Name, usedResources)
		framework.ExpectNoError(err)
	})
})

var _ = SIGDescribe("ResourceQuota", feature.PodPriority, func() {
	f := framework.NewDefaultFramework("resourcequota-priorityclass")
	f.NamespacePodSecurityLevel = admissionapi.LevelBaseline

	ginkgo.It("should verify ResourceQuota's priority class scope (quota set to pod count: 1) against a pod with same priority class.", func(ctx context.Context) {

		_, err := f.ClientSet.SchedulingV1().PriorityClasses().Create(ctx, &schedulingv1.PriorityClass{ObjectMeta: metav1.ObjectMeta{Name: "pclass1"}, Value: int32(1000)}, metav1.CreateOptions{})
		if err != nil && !apierrors.IsAlreadyExists(err) {
			framework.Failf("unexpected error while creating priority class: %v", err)
		}

		hard := v1.ResourceList{}
		hard[v1.ResourcePods] = resource.MustParse("1")

		ginkgo.By("Creating a ResourceQuota with priority class scope")
		resourceQuotaPriorityClass, err := createResourceQuota(ctx, f.ClientSet, f.Namespace.Name, newTestResourceQuotaWithScopeForPriorityClass("quota-priorityclass", hard, v1.ScopeSelectorOpIn, []string{"pclass1"}))
		framework.ExpectNoError(err)

		ginkgo.By("Ensuring ResourceQuota status is calculated")
		usedResources := v1.ResourceList{}
		usedResources[v1.ResourcePods] = resource.MustParse("0")
		err = waitForResourceQuota(ctx, f.ClientSet, f.Namespace.Name, resourceQuotaPriorityClass.Name, usedResources)
		framework.ExpectNoError(err)

		ginkgo.By("Creating a pod with priority class")
		podName := "testpod-pclass1"
		pod := newTestPodForQuotaWithPriority(f, podName, v1.ResourceList{}, v1.ResourceList{}, "pclass1")
		pod, err = f.ClientSet.CoreV1().Pods(f.Namespace.Name).Create(ctx, pod, metav1.CreateOptions{})
		framework.ExpectNoError(err)

		ginkgo.By("Ensuring resource quota with priority class scope captures the pod usage")
		usedResources[v1.ResourcePods] = resource.MustParse("1")
		err = waitForResourceQuota(ctx, f.ClientSet, f.Namespace.Name, resourceQuotaPriorityClass.Name, usedResources)
		framework.ExpectNoError(err)

		ginkgo.By("Deleting the pod")
		err = f.ClientSet.CoreV1().Pods(f.Namespace.Name).Delete(ctx, pod.Name, *metav1.NewDeleteOptions(0))
		framework.ExpectNoError(err)

		ginkgo.By("Ensuring resource quota status released the pod usage")
		usedResources[v1.ResourcePods] = resource.MustParse("0")
		err = waitForResourceQuota(ctx, f.ClientSet, f.Namespace.Name, resourceQuotaPriorityClass.Name, usedResources)
		framework.ExpectNoError(err)
	})

	ginkgo.It("should verify ResourceQuota's priority class scope (quota set to pod count: 1) against 2 pods with same priority class.", func(ctx context.Context) {

		_, err := f.ClientSet.SchedulingV1().PriorityClasses().Create(ctx, &schedulingv1.PriorityClass{ObjectMeta: metav1.ObjectMeta{Name: "pclass2"}, Value: int32(1000)}, metav1.CreateOptions{})
		if err != nil && !apierrors.IsAlreadyExists(err) {
			framework.Failf("unexpected error while creating priority class: %v", err)
		}

		hard := v1.ResourceList{}
		hard[v1.ResourcePods] = resource.MustParse("1")

		ginkgo.By("Creating a ResourceQuota with priority class scope")
		resourceQuotaPriorityClass, err := createResourceQuota(ctx, f.ClientSet, f.Namespace.Name, newTestResourceQuotaWithScopeForPriorityClass("quota-priorityclass", hard, v1.ScopeSelectorOpIn, []string{"pclass2"}))
		framework.ExpectNoError(err)

		ginkgo.By("Ensuring ResourceQuota status is calculated")
		usedResources := v1.ResourceList{}
		usedResources[v1.ResourcePods] = resource.MustParse("0")
		err = waitForResourceQuota(ctx, f.ClientSet, f.Namespace.Name, resourceQuotaPriorityClass.Name, usedResources)
		framework.ExpectNoError(err)

		ginkgo.By("Creating first pod with priority class should pass")
		podName := "testpod-pclass2-1"
		pod := newTestPodForQuotaWithPriority(f, podName, v1.ResourceList{}, v1.ResourceList{}, "pclass2")
		pod, err = f.ClientSet.CoreV1().Pods(f.Namespace.Name).Create(ctx, pod, metav1.CreateOptions{})
		framework.ExpectNoError(err)

		ginkgo.By("Ensuring resource quota with priority class scope captures the pod usage")
		usedResources[v1.ResourcePods] = resource.MustParse("1")
		err = waitForResourceQuota(ctx, f.ClientSet, f.Namespace.Name, resourceQuotaPriorityClass.Name, usedResources)
		framework.ExpectNoError(err)

		ginkgo.By("Creating 2nd pod with priority class should fail")
		podName2 := "testpod-pclass2-2"
		pod2 := newTestPodForQuotaWithPriority(f, podName2, v1.ResourceList{}, v1.ResourceList{}, "pclass2")
		_, err = f.ClientSet.CoreV1().Pods(f.Namespace.Name).Create(ctx, pod2, metav1.CreateOptions{})
		gomega.Expect(err).To(gomega.HaveOccurred())

		ginkgo.By("Deleting first pod")
		err = f.ClientSet.CoreV1().Pods(f.Namespace.Name).Delete(ctx, pod.Name, *metav1.NewDeleteOptions(0))
		framework.ExpectNoError(err)

		ginkgo.By("Ensuring resource quota status released the pod usage")
		usedResources[v1.ResourcePods] = resource.MustParse("0")
		err = waitForResourceQuota(ctx, f.ClientSet, f.Namespace.Name, resourceQuotaPriorityClass.Name, usedResources)
		framework.ExpectNoError(err)
	})

	ginkgo.It("should verify ResourceQuota's priority class scope (quota set to pod count: 1) against 2 pods with different priority class.", func(ctx context.Context) {

		_, err := f.ClientSet.SchedulingV1().PriorityClasses().Create(ctx, &schedulingv1.PriorityClass{ObjectMeta: metav1.ObjectMeta{Name: "pclass3"}, Value: int32(1000)}, metav1.CreateOptions{})
		if err != nil && !apierrors.IsAlreadyExists(err) {
			framework.Failf("unexpected error while creating priority class: %v", err)
		}

		hard := v1.ResourceList{}
		hard[v1.ResourcePods] = resource.MustParse("1")

		ginkgo.By("Creating a ResourceQuota with priority class scope")
		resourceQuotaPriorityClass, err := createResourceQuota(ctx, f.ClientSet, f.Namespace.Name, newTestResourceQuotaWithScopeForPriorityClass("quota-priorityclass", hard, v1.ScopeSelectorOpIn, []string{"pclass4"}))
		framework.ExpectNoError(err)

		ginkgo.By("Ensuring ResourceQuota status is calculated")
		usedResources := v1.ResourceList{}
		usedResources[v1.ResourcePods] = resource.MustParse("0")
		err = waitForResourceQuota(ctx, f.ClientSet, f.Namespace.Name, resourceQuotaPriorityClass.Name, usedResources)
		framework.ExpectNoError(err)

		ginkgo.By("Creating a pod with priority class with pclass3")
		podName := "testpod-pclass3-1"
		pod := newTestPodForQuotaWithPriority(f, podName, v1.ResourceList{}, v1.ResourceList{}, "pclass3")
		pod, err = f.ClientSet.CoreV1().Pods(f.Namespace.Name).Create(ctx, pod, metav1.CreateOptions{})
		framework.ExpectNoError(err)

		ginkgo.By("Ensuring resource quota with priority class scope remains same")
		usedResources[v1.ResourcePods] = resource.MustParse("0")
		err = waitForResourceQuota(ctx, f.ClientSet, f.Namespace.Name, resourceQuotaPriorityClass.Name, usedResources)
		framework.ExpectNoError(err)

		ginkgo.By("Creating a 2nd pod with priority class pclass3")
		podName2 := "testpod-pclass2-2"
		pod2 := newTestPodForQuotaWithPriority(f, podName2, v1.ResourceList{}, v1.ResourceList{}, "pclass3")
		pod2, err = f.ClientSet.CoreV1().Pods(f.Namespace.Name).Create(ctx, pod2, metav1.CreateOptions{})
		framework.ExpectNoError(err)

		ginkgo.By("Ensuring resource quota with priority class scope remains same")
		usedResources[v1.ResourcePods] = resource.MustParse("0")
		err = waitForResourceQuota(ctx, f.ClientSet, f.Namespace.Name, resourceQuotaPriorityClass.Name, usedResources)
		framework.ExpectNoError(err)

		ginkgo.By("Deleting both pods")
		err = f.ClientSet.CoreV1().Pods(f.Namespace.Name).Delete(ctx, pod.Name, *metav1.NewDeleteOptions(0))
		framework.ExpectNoError(err)
		err = f.ClientSet.CoreV1().Pods(f.Namespace.Name).Delete(ctx, pod2.Name, *metav1.NewDeleteOptions(0))
		framework.ExpectNoError(err)
	})

	ginkgo.It("should verify ResourceQuota's multiple priority class scope (quota set to pod count: 2) against 2 pods with same priority classes.", func(ctx context.Context) {
		_, err := f.ClientSet.SchedulingV1().PriorityClasses().Create(ctx, &schedulingv1.PriorityClass{ObjectMeta: metav1.ObjectMeta{Name: "pclass5"}, Value: int32(1000)}, metav1.CreateOptions{})
		if err != nil && !apierrors.IsAlreadyExists(err) {
			framework.Failf("unexpected error while creating priority class: %v", err)
		}

		_, err = f.ClientSet.SchedulingV1().PriorityClasses().Create(ctx, &schedulingv1.PriorityClass{ObjectMeta: metav1.ObjectMeta{Name: "pclass6"}, Value: int32(1000)}, metav1.CreateOptions{})
		if err != nil && !apierrors.IsAlreadyExists(err) {
			framework.Failf("unexpected error while creating priority class: %v", err)
		}

		hard := v1.ResourceList{}
		hard[v1.ResourcePods] = resource.MustParse("2")

		ginkgo.By("Creating a ResourceQuota with priority class scope")
		resourceQuotaPriorityClass, err := createResourceQuota(ctx, f.ClientSet, f.Namespace.Name, newTestResourceQuotaWithScopeForPriorityClass("quota-priorityclass", hard, v1.ScopeSelectorOpIn, []string{"pclass5", "pclass6"}))
		framework.ExpectNoError(err)

		ginkgo.By("Ensuring ResourceQuota status is calculated")
		usedResources := v1.ResourceList{}
		usedResources[v1.ResourcePods] = resource.MustParse("0")
		err = waitForResourceQuota(ctx, f.ClientSet, f.Namespace.Name, resourceQuotaPriorityClass.Name, usedResources)
		framework.ExpectNoError(err)

		ginkgo.By("Creating a pod with priority class pclass5")
		podName := "testpod-pclass5"
		pod := newTestPodForQuotaWithPriority(f, podName, v1.ResourceList{}, v1.ResourceList{}, "pclass5")
		pod, err = f.ClientSet.CoreV1().Pods(f.Namespace.Name).Create(ctx, pod, metav1.CreateOptions{})
		framework.ExpectNoError(err)

		ginkgo.By("Ensuring resource quota with priority class is updated with the pod usage")
		usedResources[v1.ResourcePods] = resource.MustParse("1")
		err = waitForResourceQuota(ctx, f.ClientSet, f.Namespace.Name, resourceQuotaPriorityClass.Name, usedResources)
		framework.ExpectNoError(err)

		ginkgo.By("Creating 2nd pod with priority class pclass6")
		podName2 := "testpod-pclass6"
		pod2 := newTestPodForQuotaWithPriority(f, podName2, v1.ResourceList{}, v1.ResourceList{}, "pclass6")
		pod2, err = f.ClientSet.CoreV1().Pods(f.Namespace.Name).Create(ctx, pod2, metav1.CreateOptions{})
		framework.ExpectNoError(err)

		ginkgo.By("Ensuring resource quota with priority class scope is updated with the pod usage")
		usedResources[v1.ResourcePods] = resource.MustParse("2")
		err = waitForResourceQuota(ctx, f.ClientSet, f.Namespace.Name, resourceQuotaPriorityClass.Name, usedResources)
		framework.ExpectNoError(err)

		ginkgo.By("Deleting both pods")
		err = f.ClientSet.CoreV1().Pods(f.Namespace.Name).Delete(ctx, pod.Name, *metav1.NewDeleteOptions(0))
		framework.ExpectNoError(err)
		err = f.ClientSet.CoreV1().Pods(f.Namespace.Name).Delete(ctx, pod2.Name, *metav1.NewDeleteOptions(0))
		framework.ExpectNoError(err)

		ginkgo.By("Ensuring resource quota status released the pod usage")
		usedResources[v1.ResourcePods] = resource.MustParse("0")
		err = waitForResourceQuota(ctx, f.ClientSet, f.Namespace.Name, resourceQuotaPriorityClass.Name, usedResources)
		framework.ExpectNoError(err)
	})

	ginkgo.It("should verify ResourceQuota's priority class scope (quota set to pod count: 1) against a pod with different priority class (ScopeSelectorOpNotIn).", func(ctx context.Context) {

		_, err := f.ClientSet.SchedulingV1().PriorityClasses().Create(ctx, &schedulingv1.PriorityClass{ObjectMeta: metav1.ObjectMeta{Name: "pclass7"}, Value: int32(1000)}, metav1.CreateOptions{})
		if err != nil && !apierrors.IsAlreadyExists(err) {
			framework.Failf("unexpected error while creating priority class: %v", err)
		}

		hard := v1.ResourceList{}
		hard[v1.ResourcePods] = resource.MustParse("1")

		ginkgo.By("Creating a ResourceQuota with priority class scope")
		resourceQuotaPriorityClass, err := createResourceQuota(ctx, f.ClientSet, f.Namespace.Name, newTestResourceQuotaWithScopeForPriorityClass("quota-priorityclass", hard, v1.ScopeSelectorOpNotIn, []string{"pclass7"}))
		framework.ExpectNoError(err)

		ginkgo.By("Ensuring ResourceQuota status is calculated")
		usedResources := v1.ResourceList{}
		usedResources[v1.ResourcePods] = resource.MustParse("0")
		err = waitForResourceQuota(ctx, f.ClientSet, f.Namespace.Name, resourceQuotaPriorityClass.Name, usedResources)
		framework.ExpectNoError(err)

		ginkgo.By("Creating a pod with priority class pclass7")
		podName := "testpod-pclass7"
		pod := newTestPodForQuotaWithPriority(f, podName, v1.ResourceList{}, v1.ResourceList{}, "pclass7")
		pod, err = f.ClientSet.CoreV1().Pods(f.Namespace.Name).Create(ctx, pod, metav1.CreateOptions{})
		framework.ExpectNoError(err)

		ginkgo.By("Ensuring resource quota with priority class is not used")
		usedResources[v1.ResourcePods] = resource.MustParse("0")
		err = waitForResourceQuota(ctx, f.ClientSet, f.Namespace.Name, resourceQuotaPriorityClass.Name, usedResources)
		framework.ExpectNoError(err)

		ginkgo.By("Deleting the pod")
		err = f.ClientSet.CoreV1().Pods(f.Namespace.Name).Delete(ctx, pod.Name, *metav1.NewDeleteOptions(0))
		framework.ExpectNoError(err)
	})

	ginkgo.It("should verify ResourceQuota's priority class scope (quota set to pod count: 1) against a pod with different priority class (ScopeSelectorOpExists).", func(ctx context.Context) {

		_, err := f.ClientSet.SchedulingV1().PriorityClasses().Create(ctx, &schedulingv1.PriorityClass{ObjectMeta: metav1.ObjectMeta{Name: "pclass8"}, Value: int32(1000)}, metav1.CreateOptions{})
		if err != nil && !apierrors.IsAlreadyExists(err) {
			framework.Failf("unexpected error while creating priority class: %v", err)
		}

		hard := v1.ResourceList{}
		hard[v1.ResourcePods] = resource.MustParse("1")

		ginkgo.By("Creating a ResourceQuota with priority class scope")
		resourceQuotaPriorityClass, err := createResourceQuota(ctx, f.ClientSet, f.Namespace.Name, newTestResourceQuotaWithScopeForPriorityClass("quota-priorityclass", hard, v1.ScopeSelectorOpExists, []string{}))
		framework.ExpectNoError(err)

		ginkgo.By("Ensuring ResourceQuota status is calculated")
		usedResources := v1.ResourceList{}
		usedResources[v1.ResourcePods] = resource.MustParse("0")
		err = waitForResourceQuota(ctx, f.ClientSet, f.Namespace.Name, resourceQuotaPriorityClass.Name, usedResources)
		framework.ExpectNoError(err)

		ginkgo.By("Creating a pod with priority class pclass8")
		podName := "testpod-pclass8"
		pod := newTestPodForQuotaWithPriority(f, podName, v1.ResourceList{}, v1.ResourceList{}, "pclass8")
		pod, err = f.ClientSet.CoreV1().Pods(f.Namespace.Name).Create(ctx, pod, metav1.CreateOptions{})
		framework.ExpectNoError(err)

		ginkgo.By("Ensuring resource quota with priority class is updated with the pod usage")
		usedResources[v1.ResourcePods] = resource.MustParse("1")
		err = waitForResourceQuota(ctx, f.ClientSet, f.Namespace.Name, resourceQuotaPriorityClass.Name, usedResources)
		framework.ExpectNoError(err)

		ginkgo.By("Deleting the pod")
		err = f.ClientSet.CoreV1().Pods(f.Namespace.Name).Delete(ctx, pod.Name, *metav1.NewDeleteOptions(0))
		framework.ExpectNoError(err)

		ginkgo.By("Ensuring resource quota status released the pod usage")
		usedResources[v1.ResourcePods] = resource.MustParse("0")
		err = waitForResourceQuota(ctx, f.ClientSet, f.Namespace.Name, resourceQuotaPriorityClass.Name, usedResources)
		framework.ExpectNoError(err)
	})

	ginkgo.It("should verify ResourceQuota's priority class scope (cpu, memory quota set) against a pod with same priority class.", func(ctx context.Context) {

		_, err := f.ClientSet.SchedulingV1().PriorityClasses().Create(ctx, &schedulingv1.PriorityClass{ObjectMeta: metav1.ObjectMeta{Name: "pclass9"}, Value: int32(1000)}, metav1.CreateOptions{})
		if err != nil && !apierrors.IsAlreadyExists(err) {
			framework.Failf("unexpected error while creating priority class: %v", err)
		}

		hard := v1.ResourceList{}
		hard[v1.ResourcePods] = resource.MustParse("1")
		hard[v1.ResourceRequestsCPU] = resource.MustParse("1")
		hard[v1.ResourceRequestsMemory] = resource.MustParse("1Gi")
		hard[v1.ResourceLimitsCPU] = resource.MustParse("3")
		hard[v1.ResourceLimitsMemory] = resource.MustParse("3Gi")

		ginkgo.By("Creating a ResourceQuota with priority class scope")
		resourceQuotaPriorityClass, err := createResourceQuota(ctx, f.ClientSet, f.Namespace.Name, newTestResourceQuotaWithScopeForPriorityClass("quota-priorityclass", hard, v1.ScopeSelectorOpIn, []string{"pclass9"}))
		framework.ExpectNoError(err)

		ginkgo.By("Ensuring ResourceQuota status is calculated")
		usedResources := v1.ResourceList{}
		usedResources[v1.ResourcePods] = resource.MustParse("0")
		usedResources[v1.ResourceRequestsCPU] = resource.MustParse("0")
		usedResources[v1.ResourceRequestsMemory] = resource.MustParse("0Gi")
		usedResources[v1.ResourceLimitsCPU] = resource.MustParse("0")
		usedResources[v1.ResourceLimitsMemory] = resource.MustParse("0Gi")
		err = waitForResourceQuota(ctx, f.ClientSet, f.Namespace.Name, resourceQuotaPriorityClass.Name, usedResources)
		framework.ExpectNoError(err)

		ginkgo.By("Creating a pod with priority class")
		podName := "testpod-pclass9"
		request := v1.ResourceList{}
		request[v1.ResourceCPU] = resource.MustParse("1")
		request[v1.ResourceMemory] = resource.MustParse("1Gi")
		limit := v1.ResourceList{}
		limit[v1.ResourceCPU] = resource.MustParse("2")
		limit[v1.ResourceMemory] = resource.MustParse("2Gi")

		pod := newTestPodForQuotaWithPriority(f, podName, request, limit, "pclass9")
		pod, err = f.ClientSet.CoreV1().Pods(f.Namespace.Name).Create(ctx, pod, metav1.CreateOptions{})
		framework.ExpectNoError(err)

		ginkgo.By("Ensuring resource quota with priority class scope captures the pod usage")
		usedResources[v1.ResourcePods] = resource.MustParse("1")
		usedResources[v1.ResourceRequestsCPU] = resource.MustParse("1")
		usedResources[v1.ResourceRequestsMemory] = resource.MustParse("1Gi")
		usedResources[v1.ResourceLimitsCPU] = resource.MustParse("2")
		usedResources[v1.ResourceLimitsMemory] = resource.MustParse("2Gi")
		err = waitForResourceQuota(ctx, f.ClientSet, f.Namespace.Name, resourceQuotaPriorityClass.Name, usedResources)
		framework.ExpectNoError(err)

		ginkgo.By("Deleting the pod")
		err = f.ClientSet.CoreV1().Pods(f.Namespace.Name).Delete(ctx, pod.Name, *metav1.NewDeleteOptions(0))
		framework.ExpectNoError(err)

		ginkgo.By("Ensuring resource quota status released the pod usage")
		usedResources[v1.ResourcePods] = resource.MustParse("0")
		usedResources[v1.ResourceRequestsCPU] = resource.MustParse("0")
		usedResources[v1.ResourceRequestsMemory] = resource.MustParse("0Gi")
		usedResources[v1.ResourceLimitsCPU] = resource.MustParse("0")
		usedResources[v1.ResourceLimitsMemory] = resource.MustParse("0Gi")
		err = waitForResourceQuota(ctx, f.ClientSet, f.Namespace.Name, resourceQuotaPriorityClass.Name, usedResources)
		framework.ExpectNoError(err)
	})

})

var _ = SIGDescribe("ResourceQuota", func() {
	f := framework.NewDefaultFramework("cross-namespace-pod-affinity")
	f.NamespacePodSecurityLevel = admissionapi.LevelBaseline
	ginkgo.It("should verify ResourceQuota with cross namespace pod affinity scope using scope-selectors.", func(ctx context.Context) {
		ginkgo.By("Creating a ResourceQuota with cross namespace pod affinity scope")
		quota, err := createResourceQuota(
			ctx, f.ClientSet, f.Namespace.Name, newTestResourceQuotaWithScopeSelector("quota-cross-namespace-pod-affinity", v1.ResourceQuotaScopeCrossNamespacePodAffinity))
		framework.ExpectNoError(err)

		ginkgo.By("Ensuring ResourceQuota status is calculated")
		wantUsedResources := v1.ResourceList{v1.ResourcePods: resource.MustParse("0")}
		err = waitForResourceQuota(ctx, f.ClientSet, f.Namespace.Name, quota.Name, wantUsedResources)
		framework.ExpectNoError(err)

		ginkgo.By("Creating a pod that does not use cross namespace affinity")
		pod := newTestPodWithAffinityForQuota(f, "no-cross-namespace-affinity", &v1.Affinity{
			PodAntiAffinity: &v1.PodAntiAffinity{
				RequiredDuringSchedulingIgnoredDuringExecution: []v1.PodAffinityTerm{{
					TopologyKey: "region",
				}}}})
		pod, err = f.ClientSet.CoreV1().Pods(f.Namespace.Name).Create(ctx, pod, metav1.CreateOptions{})
		framework.ExpectNoError(err)

		ginkgo.By("Creating a pod that uses namespaces field")
		podWithNamespaces := newTestPodWithAffinityForQuota(f, "with-namespaces", &v1.Affinity{
			PodAntiAffinity: &v1.PodAntiAffinity{
				RequiredDuringSchedulingIgnoredDuringExecution: []v1.PodAffinityTerm{{
					TopologyKey: "region",
					Namespaces:  []string{"ns1"},
				}}}})
		podWithNamespaces, err = f.ClientSet.CoreV1().Pods(f.Namespace.Name).Create(ctx, podWithNamespaces, metav1.CreateOptions{})
		framework.ExpectNoError(err)

		ginkgo.By("Ensuring resource quota captures podWithNamespaces usage")
		wantUsedResources[v1.ResourcePods] = resource.MustParse("1")
		err = waitForResourceQuota(ctx, f.ClientSet, f.Namespace.Name, quota.Name, wantUsedResources)
		framework.ExpectNoError(err)

		ginkgo.By("Creating a pod that uses namespaceSelector field")
		podWithNamespaceSelector := newTestPodWithAffinityForQuota(f, "with-namespace-selector", &v1.Affinity{
			PodAntiAffinity: &v1.PodAntiAffinity{
				RequiredDuringSchedulingIgnoredDuringExecution: []v1.PodAffinityTerm{{
					TopologyKey: "region",
					NamespaceSelector: &metav1.LabelSelector{
						MatchExpressions: []metav1.LabelSelectorRequirement{
							{
								Key:      "team",
								Operator: metav1.LabelSelectorOpIn,
								Values:   []string{"ads"},
							},
						},
					}}}}})
		podWithNamespaceSelector, err = f.ClientSet.CoreV1().Pods(f.Namespace.Name).Create(ctx, podWithNamespaceSelector, metav1.CreateOptions{})
		framework.ExpectNoError(err)

		ginkgo.By("Ensuring resource quota captures podWithNamespaceSelector usage")
		wantUsedResources[v1.ResourcePods] = resource.MustParse("2")
		err = waitForResourceQuota(ctx, f.ClientSet, f.Namespace.Name, quota.Name, wantUsedResources)
		framework.ExpectNoError(err)

		ginkgo.By("Deleting the pods")
		err = f.ClientSet.CoreV1().Pods(f.Namespace.Name).Delete(ctx, pod.Name, *metav1.NewDeleteOptions(0))
		framework.ExpectNoError(err)
		err = f.ClientSet.CoreV1().Pods(f.Namespace.Name).Delete(ctx, podWithNamespaces.Name, *metav1.NewDeleteOptions(0))
		framework.ExpectNoError(err)
		err = f.ClientSet.CoreV1().Pods(f.Namespace.Name).Delete(ctx, podWithNamespaceSelector.Name, *metav1.NewDeleteOptions(0))
		framework.ExpectNoError(err)

		ginkgo.By("Ensuring resource quota status released the pod usage")
		wantUsedResources[v1.ResourcePods] = resource.MustParse("0")
		err = waitForResourceQuota(ctx, f.ClientSet, f.Namespace.Name, quota.Name, wantUsedResources)
		framework.ExpectNoError(err)
	})
})

// newTestResourceQuotaWithScopeSelector 返回一个 quota，对 scopeSelector 测试强制默认约束
func newTestResourceQuotaWithScopeSelector(name string, scope v1.ResourceQuotaScope) *v1.ResourceQuota {
	hard := v1.ResourceList{}
	hard[v1.ResourcePods] = resource.MustParse("5")
	switch scope {
	case v1.ResourceQuotaScopeTerminating, v1.ResourceQuotaScopeNotTerminating:
		hard[v1.ResourcePods] = resource.MustParse("1")
		hard[v1.ResourceRequestsCPU] = resource.MustParse("1")
		hard[v1.ResourceRequestsMemory] = resource.MustParse("500Mi")
		hard[v1.ResourceLimitsCPU] = resource.MustParse("2")
		hard[v1.ResourceLimitsMemory] = resource.MustParse("1Gi")
	}
	return &v1.ResourceQuota{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec: v1.ResourceQuotaSpec{Hard: hard,
			ScopeSelector: &v1.ScopeSelector{
				MatchExpressions: []v1.ScopedResourceSelectorRequirement{
					{
						ScopeName: scope,
						Operator:  v1.ScopeSelectorOpExists},
				},
			},
		},
	}
}

// newTestResourceQuotaWithScope 返回一个 quota，对 scope 测试强制默认约束
func newTestResourceQuotaWithScope(name string, scope v1.ResourceQuotaScope) *v1.ResourceQuota {
	hard := v1.ResourceList{}
	hard[v1.ResourcePods] = resource.MustParse("5")
	switch scope {
	case v1.ResourceQuotaScopeTerminating, v1.ResourceQuotaScopeNotTerminating:
		hard[v1.ResourceRequestsCPU] = resource.MustParse("1")
		hard[v1.ResourceRequestsMemory] = resource.MustParse("500Mi")
		hard[v1.ResourceLimitsCPU] = resource.MustParse("2")
		hard[v1.ResourceLimitsMemory] = resource.MustParse("1Gi")
	}
	return &v1.ResourceQuota{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec:       v1.ResourceQuotaSpec{Hard: hard, Scopes: []v1.ResourceQuotaScope{scope}},
	}
}

// newTestResourceQuotaWithScopeForPriorityClass 返回一个 quota，
// 对 ResourceQuotaScopePriorityClass scope 测试强制默认约束
func newTestResourceQuotaWithScopeForPriorityClass(name string, hard v1.ResourceList, op v1.ScopeSelectorOperator, values []string) *v1.ResourceQuota {
	return &v1.ResourceQuota{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec: v1.ResourceQuotaSpec{Hard: hard,
			ScopeSelector: &v1.ScopeSelector{
				MatchExpressions: []v1.ScopedResourceSelectorRequirement{
					{
						ScopeName: v1.ResourceQuotaScopePriorityClass,
						Operator:  op,
						Values:    values,
					},
				},
			},
		},
	}
}

// newTestResourceQuotaWithScopeForVolumeAttributesClass 返回一个 quota，
// 对 ResourceQuotaScopeVolumeAttributesClass scope 测试强制默认约束
func newTestResourceQuotaWithScopeForVolumeAttributesClass(name string, hard v1.ResourceList, op v1.ScopeSelectorOperator, values []string) *v1.ResourceQuota {
	return &v1.ResourceQuota{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec: v1.ResourceQuotaSpec{Hard: hard,
			ScopeSelector: &v1.ScopeSelector{
				MatchExpressions: []v1.ScopedResourceSelectorRequirement{
					{
						ScopeName: v1.ResourceQuotaScopeVolumeAttributesClass,
						Operator:  op,
						Values:    values,
					},
				},
			},
		},
	}
}

// newTestResourceQuota 返回一个强制默认约束的 quota，用于测试
func newTestResourceQuota(name string) *v1.ResourceQuota {
	hard := v1.ResourceList{}
	hard[v1.ResourcePods] = resource.MustParse("5")
	hard[v1.ResourceServices] = resource.MustParse("10")
	hard[v1.ResourceServicesNodePorts] = resource.MustParse("1")
	hard[v1.ResourceServicesLoadBalancers] = resource.MustParse("1")
	hard[v1.ResourceReplicationControllers] = resource.MustParse("10")
	hard[v1.ResourceQuotas] = resource.MustParse("1")
	hard[v1.ResourceCPU] = resource.MustParse("1")
	hard[v1.ResourceMemory] = resource.MustParse("500Mi")
	hard[v1.ResourceConfigMaps] = resource.MustParse("10")
	hard[v1.ResourceSecrets] = resource.MustParse("10")
	hard[v1.ResourcePersistentVolumeClaims] = resource.MustParse("10")
	hard[v1.ResourceRequestsStorage] = resource.MustParse("10Gi")
	hard[v1.ResourceEphemeralStorage] = resource.MustParse("50Gi")
	hard[core.V1ResourceByStorageClass(classGold, v1.ResourcePersistentVolumeClaims)] = resource.MustParse("10")
	hard[core.V1ResourceByStorageClass(classGold, v1.ResourceRequestsStorage)] = resource.MustParse("10Gi")
	// 测试对已发现资源类型的配额
	hard[v1.ResourceName("count/replicasets.apps")] = resource.MustParse("5")
	// 测试对扩展资源的配额
	hard[v1.ResourceName(v1.DefaultResourceRequestsPrefix+extendedResourceName)] = resource.MustParse("3")
	return &v1.ResourceQuota{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec:       v1.ResourceQuotaSpec{Hard: hard},
	}
}

// newTestResourceQuotaDRA 返回一个包含 ResourceClaim 对象硬限制的 quota。
func newTestResourceQuotaDRA(name string) *v1.ResourceQuota {
	quota := newTestResourceQuota(name)
	quota.Spec.Hard[core.ClaimObjectCountName] = resource.MustParse("1")
	quota.Spec.Hard[core.V1ResourceByDeviceClass(classGold)] = resource.MustParse("1")
	return quota
}

// newTestPodForQuota 返回一个带指定 requests 和 limits 的 pod
func newTestPodForQuota(f *framework.Framework, name string, requests v1.ResourceList, limits v1.ResourceList) *v1.Pod {
	return &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
		},
		Spec: v1.PodSpec{
			// 通过确保 quota 测试 pod 不会被调度到节点上，
			// 避免并行测试运行时干扰其他测试负载
			NodeSelector: map[string]string{
				"x-test.k8s.io/unsatisfiable": "not-schedulable",
			},
			Containers: []v1.Container{
				{
					Name:  "pause",
					Image: imageutils.GetPauseImageName(),
					Resources: v1.ResourceRequirements{
						Requests: requests,
						Limits:   limits,
					},
				},
			},
		},
	}
}

// newTestPodForQuotaWithPriority 返回一个带指定 requests、limits 和 priority class 的 pod
func newTestPodForQuotaWithPriority(f *framework.Framework, name string, requests v1.ResourceList, limits v1.ResourceList, pclass string) *v1.Pod {
	return &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
		},
		Spec: v1.PodSpec{
			// 通过确保 quota 测试 pod 不会被调度到节点上，
			// 避免并行测试运行时干扰其他测试负载
			NodeSelector: map[string]string{
				"x-test.k8s.io/unsatisfiable": "not-schedulable",
			},
			Containers: []v1.Container{
				{
					Name:  "pause",
					Image: imageutils.GetPauseImageName(),
					Resources: v1.ResourceRequirements{
						Requests: requests,
						Limits:   limits,
					},
				},
			},
			PriorityClassName: pclass,
		},
	}
}

// newTestPodForQuota 返回一个带指定 requests 和 limits 的 pod
func newTestPodWithAffinityForQuota(f *framework.Framework, name string, affinity *v1.Affinity) *v1.Pod {
	return &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
		},
		Spec: v1.PodSpec{
			// 通过确保 quota 测试 pod 不会被调度到节点上，
			// 避免并行测试运行时干扰其他测试负载
			NodeSelector: map[string]string{
				"x-test.k8s.io/unsatisfiable": "not-schedulable",
			},
			Affinity: affinity,
			Containers: []v1.Container{
				{
					Name:      "pause",
					Image:     imageutils.GetPauseImageName(),
					Resources: v1.ResourceRequirements{},
				},
			},
		},
	}
}

// newTestPersistentVolumeClaimForQuota 返回一个简单的 persistent volume claim
func newTestPersistentVolumeClaimForQuota(name string) *v1.PersistentVolumeClaim {
	return &v1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
		},
		Spec: v1.PersistentVolumeClaimSpec{
			AccessModes: []v1.PersistentVolumeAccessMode{
				v1.ReadWriteOnce,
				v1.ReadOnlyMany,
			},
			Resources: v1.VolumeResourceRequirements{
				Requests: v1.ResourceList{
					v1.ResourceName(v1.ResourceStorage): resource.MustParse("1Gi"),
				},
			},
		},
	}
}

func newTestPersistentVolumeClaimWithFakeCSIVolumeForQuota(namespace, name string, vacName *string) (*v1.PersistentVolumeClaim, *v1.PersistentVolume) {
	volumeName := fmt.Sprintf("quota-fake-volume-%s-%s", namespace, name)
	pvc := &v1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
		Spec: v1.PersistentVolumeClaimSpec{
			StorageClassName:          ptr.To(""), // avoid binding to a real storage class
			VolumeAttributesClassName: vacName,
			Selector: &metav1.LabelSelector{
				MatchLabels: map[string]string{
					// 通过确保 quota 测试 claim 不会绑定到其他测试正在使用的 volume，
					// 避免并行测试运行时干扰其他测试负载
					"x-test.k8s.io/satisfiable": "fake-volume",
				},
			},
			AccessModes: []v1.PersistentVolumeAccessMode{v1.ReadWriteOnce},
			Resources: v1.VolumeResourceRequirements{
				Requests: v1.ResourceList{
					v1.ResourceName(v1.ResourceStorage): resource.MustParse("1Gi"),
				},
			},
			VolumeName: volumeName,
		},
	}
	pv := &v1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name: volumeName,
			Labels: map[string]string{
				"x-test.k8s.io/satisfiable": "fake-volume",
			},
		},
		Spec: v1.PersistentVolumeSpec{
			StorageClassName:              "",
			VolumeAttributesClassName:     vacName,
			PersistentVolumeReclaimPolicy: v1.PersistentVolumeReclaimDelete,
			AccessModes:                   []v1.PersistentVolumeAccessMode{v1.ReadWriteOnce},
			Capacity: v1.ResourceList{
				v1.ResourceName(v1.ResourceStorage): resource.MustParse("1Gi"),
			},
			PersistentVolumeSource: v1.PersistentVolumeSource{
				CSI: &v1.CSIPersistentVolumeSource{
					Driver:       "fake.csi.k8s.io",
					VolumeHandle: "volume-handle",
				},
			},
			ClaimRef: &v1.ObjectReference{
				APIVersion: "v1",
				Kind:       "PersistentVolumeClaim",
				Namespace:  namespace,
				Name:       name,
			},
		},
	}
	return pvc, pv
}

// newTestResourceClaimForQuota 返回一个简单的 resource claim
func newTestResourceClaimForQuota(name string) *resourceapi.ResourceClaim {
	return &resourceapi.ResourceClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
		},
		Spec: resourceapi.ResourceClaimSpec{
			Devices: resourceapi.DeviceClaim{
				Requests: []resourceapi.DeviceRequest{{
					Name: "req-0",
					Exactly: &resourceapi.ExactDeviceRequest{
						DeviceClassName: classGold,
					},
				}},
			},
		},
	}
}

// newTestReplicationControllerForQuota 返回一个简单的 replication controller
func newTestReplicationControllerForQuota(name, image string, replicas int32) *v1.ReplicationController {
	return &v1.ReplicationController{
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
		},
		Spec: v1.ReplicationControllerSpec{
			Replicas: ptr.To[int32](replicas),
			Selector: map[string]string{
				"name": name,
			},
			Template: &v1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{"name": name},
				},
				Spec: v1.PodSpec{
					Containers: []v1.Container{
						{
							Name:  name,
							Image: image,
						},
					},
				},
			},
		},
	}
}

// newTestReplicaSetForQuota 返回一个简单的 replica set
func newTestReplicaSetForQuota(name, image string, replicas int32) *appsv1.ReplicaSet {
	zero := int64(0)
	return &appsv1.ReplicaSet{
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
		},
		Spec: appsv1.ReplicaSetSpec{
			Replicas: &replicas,
			Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"name": name}},
			Template: v1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{"name": name},
				},
				Spec: v1.PodSpec{
					TerminationGracePeriodSeconds: &zero,
					Containers: []v1.Container{
						{
							Name:  name,
							Image: image,
						},
					},
				},
			},
		},
	}
}

// newTestServiceForQuota 返回一个简单的 service
func newTestServiceForQuota(name string, serviceType v1.ServiceType, allocateLoadBalancerNodePorts bool) *v1.Service {
	var allocateNPs *bool
	// 仅当 service 类型为 LB 时才设置 allocateLoadBalancerNodePorts
	if serviceType == v1.ServiceTypeLoadBalancer {
		allocateNPs = &allocateLoadBalancerNodePorts
	}

	return &v1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
		},
		Spec: v1.ServiceSpec{
			Type: serviceType,
			Ports: []v1.ServicePort{{
				Port:       80,
				TargetPort: intstr.FromInt32(80),
			}},
			AllocateLoadBalancerNodePorts: allocateNPs,
		},
	}
}

func newTestConfigMapForQuota(name string) *v1.ConfigMap {
	return &v1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
		},
		Data: map[string]string{
			"a": "b",
		},
	}
}

func newTestSecretForQuota(name string) *v1.Secret {
	return &v1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
		},
		Data: map[string][]byte{
			"data-1": []byte("value-1\n"),
			"data-2": []byte("value-2\n"),
			"data-3": []byte("value-3\n"),
		},
	}
}

// 在指定 namespace 中创建 resourceQuota
func createResourceQuota(ctx context.Context, c clientset.Interface, namespace string, resourceQuota *v1.ResourceQuota) (*v1.ResourceQuota, error) {
	return c.CoreV1().ResourceQuotas(namespace).Create(ctx, resourceQuota, metav1.CreateOptions{})
}

// 删除指定名称的 resourceQuota
func deleteResourceQuota(ctx context.Context, c clientset.Interface, namespace, name string) error {
	return c.CoreV1().ResourceQuotas(namespace).Delete(ctx, name, metav1.DeleteOptions{})
}

// countResourceQuota 统计指定 namespace 中 ResourceQuota 的数量
// 高负载的服务器上 service account controller 可能变慢，导致测试运行期间计数发生变化。
// 最多等 5 秒让计数稳定，前提是更新以稳定速率到来且不会无限期停滞。
func countResourceQuota(ctx context.Context, c clientset.Interface, namespace string) (int, error) {
	found, unchanged := 0, 0
	return found, wait.PollUntilContextTimeout(ctx, 1*time.Second, 30*time.Second, false, func(ctx context.Context) (bool, error) {
		resourceQuotas, err := c.CoreV1().ResourceQuotas(namespace).List(ctx, metav1.ListOptions{})
		framework.ExpectNoError(err)
		if len(resourceQuotas.Items) == found {
			// 循环等待 resource quota 数量稳定 5 秒
			unchanged++
			return unchanged > 4, nil
		}
		unchanged = 0
		found = len(resourceQuotas.Items)
		return false, nil
	})
}

// 等待 resource quota 状态显示预期的已用资源值。
// 其他资源忽略。
func waitForResourceQuota(ctx context.Context, c clientset.Interface, ns, quotaName string, used v1.ResourceList) error {
	var lastResourceQuota *v1.ResourceQuota
	err := framework.Gomega().Eventually(ctx, framework.GetObject(c.CoreV1().ResourceQuotas(ns).Get, quotaName, metav1.GetOptions{})).Should(haveUsedResources(used, &lastResourceQuota))
	if lastResourceQuota != nil && err == nil {
		framework.Logf("Got expected ResourceQuota:\n%s", format.Object(lastResourceQuota, 1))
	}
	return err
}

// 确保 resource quota 状态显示预期的已用资源值且不再变化。
// 其他资源忽略。
func ensureResourceQuota(ctx context.Context, c clientset.Interface, ns, quotaName string, used v1.ResourceList) error {
	// 检查时间越长，越有把握确认值确实没变。
	// 但也不能等太久，否则测试会变慢。
	err := framework.Gomega().Consistently(ctx, framework.GetObject(c.CoreV1().ResourceQuotas(ns).Get, quotaName, metav1.GetOptions{})).WithTimeout(10 * time.Second).Should(haveUsedResources(used, nil))
	return err
}

func haveUsedResources(used v1.ResourceList, lastResourceQuota **v1.ResourceQuota) gomegatypes.GomegaMatcher {
	// 模板会输出实际的 ResourceQuota 对象 YAML。
	// 其中 ManagedFields 尤其值得关注，因为
	// kube-apiserver 和 kube-controller-manager 都会设置 status。
	return gcustom.MakeMatcher(func(resourceQuota *v1.ResourceQuota) (bool, error) {
		if lastResourceQuota != nil {
			*lastResourceQuota = resourceQuota
		}
		// used 可能还未计算出来
		if resourceQuota.Status.Used == nil {
			return false, nil
		}
		// 验证 quota 显示预期的已用资源值
		for k, v := range used {
			if actualValue, found := resourceQuota.Status.Used[k]; !found || (actualValue.Cmp(v) != 0) {
				return false, nil
			}
		}
		return true, nil
	}).WithTemplate("Expected:\n{{.FormattedActual}}\n{{.To}} have the following .status.used entries:\n{{range $key, $value := .Data}}    {{$key}}: \"{{$value.ToUnstructured}}\"\n{{end}}").WithTemplateData(used /* Formatting of the map is done inside the template. */)
}

// updateResourceQuotaUntilUsageAppears 持续更新 resource quota 对象，
// 直到指定资源名的用量被填充。
func updateResourceQuotaUntilUsageAppears(ctx context.Context, c clientset.Interface, ns, quotaName string, resourceName v1.ResourceName) error {
	return wait.PollUntilContextTimeout(ctx, framework.Poll, resourceQuotaTimeout, false, func(ctx context.Context) (bool, error) {
		resourceQuota, err := c.CoreV1().ResourceQuotas(ns).Get(ctx, quotaName, metav1.GetOptions{})
		if err != nil {
			return false, err
		}
		// 验证 quota 显示预期的已用资源值
		_, ok := resourceQuota.Status.Used[resourceName]
		if ok {
			return true, nil
		}

		current := resourceQuota.Spec.Hard[resourceName]
		current.Add(resource.MustParse("1"))
		resourceQuota.Spec.Hard[resourceName] = current
		_, err = c.CoreV1().ResourceQuotas(ns).Update(ctx, resourceQuota, metav1.UpdateOptions{})
		// 忽略冲突，因为可能已有其他人更新过它。
		if apierrors.IsConflict(err) {
			return false, nil
		}
		return false, err
	})
}

func unstructuredToResourceQuota(obj *unstructured.Unstructured) (*v1.ResourceQuota, error) {
	json, err := runtime.Encode(unstructured.UnstructuredJSONScheme, obj)
	if err != nil {
		return nil, err
	}
	rq := &v1.ResourceQuota{}
	err = runtime.DecodeInto(clientscheme.Codecs.LegacyCodec(v1.SchemeGroupVersion), json, rq)

	return rq, err
}
