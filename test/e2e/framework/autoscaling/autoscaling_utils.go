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

package autoscaling

import (
	"context"
	"crypto/tls"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	autoscalingv1 "k8s.io/api/autoscaling/v1"
	autoscalingv2 "k8s.io/api/autoscaling/v2"
	v1 "k8s.io/api/core/v1"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	crdclientset "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset"
	"k8s.io/apiextensions-apiserver/test/integration/fixtures"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/dynamic"
	clientset "k8s.io/client-go/kubernetes"
	scaleclient "k8s.io/client-go/scale"
	apiregistrationv1 "k8s.io/kube-aggregator/pkg/apis/apiregistration/v1"
	aggregatorclient "k8s.io/kube-aggregator/pkg/client/clientset_generated/clientset"
	"k8s.io/kubernetes/test/e2e/framework"
	e2edebug "k8s.io/kubernetes/test/e2e/framework/debug"
	e2eendpointslice "k8s.io/kubernetes/test/e2e/framework/endpointslice"
	e2ekubectl "k8s.io/kubernetes/test/e2e/framework/kubectl"
	e2epod "k8s.io/kubernetes/test/e2e/framework/pod"
	e2eresource "k8s.io/kubernetes/test/e2e/framework/resource"
	e2eservice "k8s.io/kubernetes/test/e2e/framework/service"
	testutils "k8s.io/kubernetes/test/utils"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	"k8s.io/utils/ptr"

	imageutils "k8s.io/kubernetes/test/utils/image"
)

const (
	dynamicConsumptionTimeInSeconds = 30
	dynamicRequestSizeInMillicores  = 100
	dynamicRequestSizeInMegabytes   = 100
	dynamicRequestSizeCustomMetric  = 10
	port                            = 80
	portName                        = "http"
	targetPort                      = 8080
	sidecarTargetPort               = 8081
	externalMetricsServerPort       = 6443
	externalMetricsServicePort      = 443
	timeoutRC                       = 120 * time.Second
	invalidKind                     = "ERROR: invalid workload kind for resource consumer"
	customMetricName                = "QPS"
	serviceInitializationTimeout    = 2 * time.Minute
	serviceInitializationInterval   = 15 * time.Second
	megabytes                       = 1024 * 1024
	crdVersion                      = "v1"
	crdKind                         = "TestCRD"
	crdGroup                        = "autoscalinge2e.example.com"
	crdName                         = "testcrd"
	crdNamePlural                   = "testcrds"
)

var (
	// KindDeployment 是 Deployment 的 GVK
	KindDeployment = schema.GroupVersionKind{Group: "apps", Version: "v1", Kind: "Deployment"}
	// KindReplicaSet 是 ReplicaSet 的 GVK
	KindReplicaSet = schema.GroupVersionKind{Group: "apps", Version: "v1", Kind: "ReplicaSet"}
	// KindCRD 是用于测试目的 CRD 的 GVK
	KindCRD = schema.GroupVersionKind{Group: crdGroup, Version: crdVersion, Kind: crdKind}
)

// ScalingDirection 标识 HPA Behavior 的扩缩方向。
type ScalingDirection int

const (
	DirectionUnknown ScalingDirection = iota
	ScaleUpDirection
	ScaleDownDirection
)

/*
ResourceConsumer 是一个测试工具，用于创建指定的 CPU 或内存用量。
典型用例：
rc.ConsumeCPU(600)
// ... 在此检查你的假设
rc.ConsumeCPU(300)
// ... 在此检查你的假设
*/
type ResourceConsumer struct {
	name                     string
	controllerName           string
	kind                     schema.GroupVersionKind
	nsName                   string
	clientSet                clientset.Interface
	apiExtensionClient       crdclientset.Interface
	dynamicClient            dynamic.Interface
	resourceClient           dynamic.ResourceInterface
	scaleClient              scaleclient.ScalesGetter
	customMetricName         string
	cpu                      chan int
	mem                      chan int
	customMetric             chan int
	stopCPU                  chan int
	stopMem                  chan int
	stopCustomMetric         chan int
	stopWaitGroup            sync.WaitGroup
	consumptionTimeInSeconds int
	sleepTime                time.Duration
	requestSizeInMillicores  int
	requestSizeInMegabytes   int
	requestSizeCustomMetric  int
	sidecarStatus            SidecarStatusType
	sidecarType              SidecarWorkloadType
	cpuPerPod                chan int
	stopCPUPerPod            chan int
}

// ExternalMetricsController 提供在运行时控制 external metrics server 的方法
type ExternalMetricsController struct {
	clientSet   clientset.Interface
	serviceName string
	namespace   string
}

// NewExternalMetricsController 为 external metrics server 创建一个新的 controller
func NewExternalMetricsController(clientSet clientset.Interface, serviceName, namespace string) *ExternalMetricsController {
	return &ExternalMetricsController{
		clientSet:   clientSet,
		serviceName: serviceName,
		namespace:   namespace,
	}
}

// CreateMetric 在 external metrics server 上创建一个新 metric
func (emc *ExternalMetricsController) CreateMetric(ctx context.Context, metricName string, value int64, labels map[string]string, shouldFail bool) error {
	params := url.Values{}
	params.Set("value", strconv.FormatInt(value, 10))
	params.Set("fail", strconv.FormatBool(shouldFail))
	if len(labels) > 0 {
		params.Set("labels", formatLabels(labels))
	}
	return emc.sendMetricRequest(ctx, "create", metricName, params)
}

// SetMetricValue 更新现有 metric 的值
func (emc *ExternalMetricsController) SetMetricValue(ctx context.Context, metricName string, value int64, labels map[string]string) error {
	params := url.Values{}
	params.Set("value", strconv.FormatInt(value, 10))
	if len(labels) > 0 {
		params.Set("labels", formatLabels(labels))
	}
	return emc.sendMetricRequest(ctx, "set", metricName, params)
}

// SetMetricFail 配置 metric 是否应返回错误
func (emc *ExternalMetricsController) SetMetricFail(ctx context.Context, metricName string, shouldFail bool, labels map[string]string) error {
	params := url.Values{}
	params.Set("fail", strconv.FormatBool(shouldFail))
	if len(labels) > 0 {
		params.Set("labels", formatLabels(labels))
	}
	return emc.sendMetricRequest(ctx, "fail", metricName, params)
}

func (emc *ExternalMetricsController) sendMetricRequest(ctx context.Context, action, metricName string, params url.Values) error {
	return framework.Gomega().Eventually(ctx, func(ctx context.Context) error {
		return emc.doRequestWithPortForward(ctx, action, metricName, params)
	}).WithTimeout(serviceInitializationTimeout).WithPolling(serviceInitializationInterval).Should(gomega.Succeed())
}

func (emc *ExternalMetricsController) doRequestWithPortForward(ctx context.Context, action, metricName string, params url.Values) error {
	// 找到 Pod
	pods, err := emc.clientSet.CoreV1().Pods(emc.namespace).List(ctx, metav1.ListOptions{
		LabelSelector: "name=" + emc.serviceName,
	})
	if err != nil {
		return fmt.Errorf("failed to list pods: %w", err)
	}
	if len(pods.Items) == 0 {
		return fmt.Errorf("no pods found for service %s", emc.serviceName)
	}
	podName := pods.Items[0].Name

	// 设置 port-forward
	config, err := framework.LoadConfig()
	if err != nil {
		return fmt.Errorf("failed to load config: %w", err)
	}

	// 重新使用 NewTransport，它根据 KEP-4006 同时支持 SPDY 和 WebSocket
	transport := e2epod.NewTransport(emc.clientSet, config)
	// external metrics server 使用自签名证书。
	transport.TLSClientConfig = &tls.Config{InsecureSkipVerify: true} //nolint:gosec
	client := &http.Client{
		Transport: transport,
		Timeout:   30 * time.Second,
	}

	// test/e2e/framework/pod/dial.go#ParseAddr 期望
	// URL 采用 <namespace>.<pod>:<port number> 格式
	requestURL := fmt.Sprintf("https://%s.%s:%d/%s/%s", emc.namespace, podName, externalMetricsServerPort, action, metricName)
	if len(params) > 0 {
		requestURL += "?" + params.Encode()
	}

	framework.Logf("ExternalMetrics %s URL: %s", action, requestURL)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, requestURL, nil)
	if err != nil {
		return err
	}

	resp, err := client.Do(req)
	if err != nil {
		framework.Logf("ExternalMetrics %s failure: %v", action, err)
		return err
	}
	defer resp.Body.Close() //nolint:errcheck

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	return nil
}

// RunExternalMetricsServer 创建并返回一个用于控制服务器的 ExternalMetricsController
func RunExternalMetricsServer(ctx context.Context, c clientset.Interface, ns, name string, serviceAnnotations map[string]string) *ExternalMetricsController {
	runServiceAndApiregistrationForExternalMetricsServer(ctx, c, ns, name, serviceAnnotations)
	return NewExternalMetricsController(c, name, ns)
}

// CleanupExternalMetricsServer 清理 external metrics server 资源
func CleanupExternalMetricsServer(ctx context.Context, c clientset.Interface, ns, name string) {
	ginkgo.By(fmt.Sprintf("Cleaning up external metrics server %s", name))

	// 删除 APIService
	config, err := framework.LoadConfig()
	if err == nil {
		aggClient, err := aggregatorclient.NewForConfig(config)
		if err == nil {
			err = aggClient.ApiregistrationV1().APIServices().Delete(ctx, "v1beta1.external.metrics.k8s.io", metav1.DeleteOptions{})
			if err != nil {
				framework.Logf("Error deleting APIService: %v", err)
			}
		}
	}

	// 删除 deployment
	err = c.AppsV1().Deployments(ns).Delete(ctx, name, metav1.DeleteOptions{})
	if err != nil {
		framework.Logf("Error deleting deployment: %v", err)
	}

	// 删除 service
	err = c.CoreV1().Services(ns).Delete(ctx, name, metav1.DeleteOptions{})
	if err != nil {
		framework.Logf("Error deleting service: %v", err)
	}
}

// NewDynamicResourceConsumer 是创建新 dynamic ResourceConsumer 的包装器
func NewDynamicResourceConsumer(ctx context.Context, name, nsName string, kind schema.GroupVersionKind, replicas, initCPUTotal, initMemoryTotal, initCustomMetric int, cpuLimit, memLimit int64, clientset clientset.Interface, scaleClient scaleclient.ScalesGetter, enableSidecar SidecarStatusType, sidecarType SidecarWorkloadType, podResources *v1.ResourceRequirements) *ResourceConsumer {
	return NewResourceConsumer(ctx, name, nsName, kind, replicas, customMetricName, initCPUTotal, initMemoryTotal, initCustomMetric, dynamicConsumptionTimeInSeconds,
		dynamicRequestSizeInMillicores, dynamicRequestSizeInMegabytes, dynamicRequestSizeCustomMetric, cpuLimit, memLimit, clientset, scaleClient, nil, nil, enableSidecar, sidecarType, podResources)
}

// getSidecarContainer 返回 sidecar 容器
func getSidecarContainer(name string, cpuLimit, memLimit int64) v1.Container {
	container := v1.Container{
		Name:    name + "-sidecar",
		Image:   imageutils.GetE2EImage(imageutils.ResourceConsumer),
		Command: []string{"/consumer", "-port=8081"},
		Ports:   []v1.ContainerPort{{ContainerPort: 80}},
	}

	if cpuLimit > 0 || memLimit > 0 {
		container.Resources.Limits = v1.ResourceList{}
		container.Resources.Requests = v1.ResourceList{}
	}

	if cpuLimit > 0 {
		container.Resources.Limits[v1.ResourceCPU] = *resource.NewMilliQuantity(cpuLimit, resource.DecimalSI)
		container.Resources.Requests[v1.ResourceCPU] = *resource.NewMilliQuantity(cpuLimit, resource.DecimalSI)
	}

	if memLimit > 0 {
		container.Resources.Limits[v1.ResourceMemory] = *resource.NewQuantity(memLimit*megabytes, resource.DecimalSI)
		container.Resources.Requests[v1.ResourceMemory] = *resource.NewQuantity(memLimit*megabytes, resource.DecimalSI)
	}

	return container
}

/*
NewResourceConsumer 创建新的 ResourceConsumer
initCPUTotal 参数单位为 millicores
initMemoryTotal 参数单位为 megabytes
memLimit 参数单位为 megabytes，memLimit 是单个 pod 可消耗的最大内存量
cpuLimit 参数单位为 millicores，cpuLimit 是单个 pod 可消耗的最大 CPU 量
*/
func NewResourceConsumer(ctx context.Context, name, nsName string, kind schema.GroupVersionKind, replicas int, customMetricName string, initCPUTotal, initMemoryTotal, initCustomMetric, consumptionTimeInSeconds, requestSizeInMillicores,
	requestSizeInMegabytes int, requestSizeCustomMetric int, cpuLimit, memLimit int64, clientset clientset.Interface, scaleClient scaleclient.ScalesGetter, podAnnotations, serviceAnnotations map[string]string, sidecarStatus SidecarStatusType, sidecarType SidecarWorkloadType, podResources *v1.ResourceRequirements) *ResourceConsumer {
	if podAnnotations == nil {
		podAnnotations = make(map[string]string)
	}
	if serviceAnnotations == nil {
		serviceAnnotations = make(map[string]string)
	}

	var additionalContainers []v1.Container

	if sidecarStatus == Enable {
		sidecarContainer := getSidecarContainer(name, cpuLimit, memLimit)
		additionalContainers = append(additionalContainers, sidecarContainer)
	}

	config, err := framework.LoadConfig()
	framework.ExpectNoError(err)
	apiExtensionClient, err := crdclientset.NewForConfig(config)
	framework.ExpectNoError(err)
	dynamicClient, err := dynamic.NewForConfig(config)
	framework.ExpectNoError(err)
	resourceClient := dynamicClient.Resource(schema.GroupVersionResource{Group: crdGroup, Version: crdVersion, Resource: crdNamePlural}).Namespace(nsName)

	runServiceAndWorkloadForResourceConsumer(ctx, clientset, resourceClient, apiExtensionClient, nsName, name, kind, replicas, cpuLimit, memLimit, podAnnotations, serviceAnnotations, additionalContainers, podResources)
	controllerName := name + "-ctrl"
	// 如果 sidecar 已启用且繁忙，为 sidecar 运行 service 和 consumer
	if sidecarStatus == Enable && sidecarType == Busy {
		runServiceAndSidecarForResourceConsumer(ctx, clientset, nsName, name, kind, replicas, serviceAnnotations)
		controllerName = name + "-sidecar-ctrl"
	}

	rc := &ResourceConsumer{
		name:                     name,
		controllerName:           controllerName,
		kind:                     kind,
		nsName:                   nsName,
		clientSet:                clientset,
		apiExtensionClient:       apiExtensionClient,
		scaleClient:              scaleClient,
		resourceClient:           resourceClient,
		dynamicClient:            dynamicClient,
		customMetricName:         customMetricName,
		cpu:                      make(chan int),
		mem:                      make(chan int),
		customMetric:             make(chan int),
		stopCPU:                  make(chan int),
		stopMem:                  make(chan int),
		stopCustomMetric:         make(chan int),
		cpuPerPod:                make(chan int),
		stopCPUPerPod:            make(chan int),
		consumptionTimeInSeconds: consumptionTimeInSeconds,
		sleepTime:                time.Duration(consumptionTimeInSeconds) * time.Second,
		requestSizeInMillicores:  requestSizeInMillicores,
		requestSizeInMegabytes:   requestSizeInMegabytes,
		requestSizeCustomMetric:  requestSizeCustomMetric,
		sidecarType:              sidecarType,
		sidecarStatus:            sidecarStatus,
	}

	go rc.makeConsumeCPURequests(ctx)
	rc.ConsumeCPU(initCPUTotal)
	go rc.makeConsumeMemRequests(ctx)
	rc.ConsumeMem(initMemoryTotal)
	go rc.makeConsumeCustomMetric(ctx)
	rc.ConsumeCustomMetric(initCustomMetric)
	go rc.makeConsumeCPUPerPodRequests(ctx)
	return rc
}

// ConsumeCPU 消耗指定量的 CPU
func (rc *ResourceConsumer) ConsumeCPU(millicores int) {
	framework.Logf("RC %s: consume %v millicores in total", rc.name, millicores)
	rc.cpu <- millicores
}

// ConsumeMem 消耗指定量的内存
func (rc *ResourceConsumer) ConsumeMem(megabytes int) {
	framework.Logf("RC %s: consume %v MB in total", rc.name, megabytes)
	rc.mem <- megabytes
}

// ConsumeCustomMetric 消耗指定量的自定义 metric
func (rc *ResourceConsumer) ConsumeCustomMetric(amount int) {
	framework.Logf("RC %s: consume custom metric %v in total", rc.name, amount)
	rc.customMetric <- amount
}

func (rc *ResourceConsumer) makeConsumeCPURequests(ctx context.Context) {
	defer ginkgo.GinkgoRecover()
	rc.stopWaitGroup.Add(1)
	defer rc.stopWaitGroup.Done()
	tick := time.After(time.Duration(0))
	millicores := 0
	for {
		select {
		case millicores = <-rc.cpu:
			if millicores != 0 {
				framework.Logf("RC %s: setting consumption to %v millicores in total", rc.name, millicores)
			} else {
				framework.Logf("RC %s: disabling CPU consumption", rc.name)
			}
		case <-tick:
			if millicores != 0 {
				framework.Logf("RC %s: sending request to consume %d millicores", rc.name, millicores)
				rc.sendConsumeCPURequest(ctx, millicores)
			}
			tick = time.After(rc.sleepTime)
		case <-ctx.Done():
			framework.Logf("RC %s: stopping CPU consumer: %v", rc.name, ctx.Err())
			return
		case <-rc.stopCPU:
			framework.Logf("RC %s: stopping CPU consumer", rc.name)
			return
		}
	}
}

func (rc *ResourceConsumer) makeConsumeMemRequests(ctx context.Context) {
	defer ginkgo.GinkgoRecover()
	rc.stopWaitGroup.Add(1)
	defer rc.stopWaitGroup.Done()
	tick := time.After(time.Duration(0))
	megabytes := 0
	for {
		select {
		case megabytes = <-rc.mem:
			if megabytes != 0 {
				framework.Logf("RC %s: setting consumption to %v MB in total", rc.name, megabytes)
			} else {
				framework.Logf("RC %s: disabling mem consumption", rc.name)
			}
		case <-tick:
			if megabytes != 0 {
				framework.Logf("RC %s: sending request to consume %d MB", rc.name, megabytes)
				rc.sendConsumeMemRequest(ctx, megabytes)
			}
			tick = time.After(rc.sleepTime)
		case <-ctx.Done():
			framework.Logf("RC %s: stopping mem consumer: %v", rc.name, ctx.Err())
			return
		case <-rc.stopMem:
			framework.Logf("RC %s: stopping mem consumer", rc.name)
			return
		}
	}
}

func (rc *ResourceConsumer) makeConsumeCustomMetric(ctx context.Context) {
	defer ginkgo.GinkgoRecover()
	rc.stopWaitGroup.Add(1)
	defer rc.stopWaitGroup.Done()
	tick := time.After(time.Duration(0))
	delta := 0
	for {
		select {
		case delta = <-rc.customMetric:
			if delta != 0 {
				framework.Logf("RC %s: setting bump of metric %s to %d in total", rc.name, rc.customMetricName, delta)
			} else {
				framework.Logf("RC %s: disabling consumption of custom metric %s", rc.name, rc.customMetricName)
			}
		case <-tick:
			if delta != 0 {
				framework.Logf("RC %s: sending request to consume %d of custom metric %s", rc.name, delta, rc.customMetricName)
				rc.sendConsumeCustomMetric(ctx, delta)
			}
			tick = time.After(rc.sleepTime)
		case <-ctx.Done():
			framework.Logf("RC %s: stopping metric consumer: %v", rc.name, ctx.Err())
			return
		case <-rc.stopCustomMetric:
			framework.Logf("RC %s: stopping metric consumer", rc.name)
			return
		}
	}
}

func (rc *ResourceConsumer) sendConsumeCPURequest(ctx context.Context, millicores int) {
	err := framework.Gomega().Eventually(ctx, func(ctx context.Context) error {
		proxyRequest, err := e2eservice.GetServicesProxyRequest(rc.clientSet, rc.clientSet.CoreV1().RESTClient().Post())
		if err != nil {
			return err
		}
		req := proxyRequest.Namespace(rc.nsName).
			Name(fmt.Sprintf("%s:%s", rc.controllerName, portName)).
			Suffix("ConsumeCPU").
			Param("millicores", strconv.Itoa(millicores)).
			Param("durationSec", strconv.Itoa(rc.consumptionTimeInSeconds)).
			Param("requestSizeMillicores", strconv.Itoa(rc.requestSizeInMillicores))
		framework.Logf("ConsumeCPU URL: %v", *req.URL())
		_, err = req.DoRaw(ctx)
		if err != nil {
			framework.Logf("ConsumeCPU failure: %v", err)
			return err
		}
		return nil
	}).WithTimeout(serviceInitializationTimeout).WithPolling(serviceInitializationInterval).Should(gomega.Succeed())

	// 测试已结束（ctx 已取消），因此不要因 PollUntilContextTimeout 的 err 而失败
	// 这是清理任务取消 context 带来的副作用。
	if ctx.Err() != nil {
		return
	}

	framework.ExpectNoError(err)
}

// sendConsumeMemRequest 发送用于消耗内存的 POST 请求
func (rc *ResourceConsumer) sendConsumeMemRequest(ctx context.Context, megabytes int) {
	err := framework.Gomega().Eventually(ctx, func(ctx context.Context) error {
		proxyRequest, err := e2eservice.GetServicesProxyRequest(rc.clientSet, rc.clientSet.CoreV1().RESTClient().Post())
		if err != nil {
			return err
		}
		req := proxyRequest.Namespace(rc.nsName).
			Name(fmt.Sprintf("%s:%s", rc.controllerName, portName)).
			Suffix("ConsumeMem").
			Param("megabytes", strconv.Itoa(megabytes)).
			Param("durationSec", strconv.Itoa(rc.consumptionTimeInSeconds)).
			Param("requestSizeMegabytes", strconv.Itoa(rc.requestSizeInMegabytes))
		framework.Logf("ConsumeMem URL: %v", *req.URL())
		_, err = req.DoRaw(ctx)
		if err != nil {
			framework.Logf("ConsumeMem failure: %v", err)
			return err
		}
		return nil
	}).WithTimeout(serviceInitializationTimeout).WithPolling(serviceInitializationInterval).Should(gomega.Succeed())

	// 测试已结束（ctx 已取消），因此不要因 PollUntilContextTimeout 的 err 而失败
	// 这是清理任务取消 context 带来的副作用。
	if ctx.Err() != nil {
		return
	}

	framework.ExpectNoError(err)
}

// sendConsumeCustomMetric 发送用于消耗自定义 metric 的 POST 请求
func (rc *ResourceConsumer) sendConsumeCustomMetric(ctx context.Context, delta int) {
	err := framework.Gomega().Eventually(ctx, func(ctx context.Context) error {
		proxyRequest, err := e2eservice.GetServicesProxyRequest(rc.clientSet, rc.clientSet.CoreV1().RESTClient().Post())
		if err != nil {
			return err
		}
		req := proxyRequest.Namespace(rc.nsName).
			Name(fmt.Sprintf("%s:%s", rc.controllerName, portName)).
			Suffix("BumpMetric").
			Param("metric", rc.customMetricName).
			Param("delta", strconv.Itoa(delta)).
			Param("durationSec", strconv.Itoa(rc.consumptionTimeInSeconds)).
			Param("requestSizeMetrics", strconv.Itoa(rc.requestSizeCustomMetric))
		framework.Logf("ConsumeCustomMetric URL: %v", *req.URL())
		_, err = req.DoRaw(ctx)
		if err != nil {
			framework.Logf("ConsumeCustomMetric failure: %v", err)
			return err
		}
		return nil
	}).WithTimeout(serviceInitializationTimeout).WithPolling(serviceInitializationInterval).Should(gomega.Succeed())

	// 测试已结束（ctx 已取消），因此不要因 PollUntilContextTimeout 的 err 而失败
	// 这是清理任务取消 context 带来的副作用。
	if ctx.Err() != nil {
		return
	}

	framework.ExpectNoError(err)
}

/*
ConsumeCPUPerPod 通过 Kubernetes pod proxy API 直接向每个 consumer pod 发送 CPU 负载，
绕过 kube-proxy 不确定的负载均衡。millicoresTotal 会在所有运行中的 pod 间平均分配，
确保无论集群网络配置如何，每个 pod 获得相同份额。
*/
func (rc *ResourceConsumer) ConsumeCPUPerPod(millicoresTotal int) {
	framework.Logf("RC %s: consume %v millicores in total (evenly distributed per pod)", rc.name, millicoresTotal)
	rc.cpuPerPod <- millicoresTotal
}

func (rc *ResourceConsumer) makeConsumeCPUPerPodRequests(ctx context.Context) {
	defer ginkgo.GinkgoRecover()
	rc.stopWaitGroup.Add(1)
	defer rc.stopWaitGroup.Done()
	tick := time.After(time.Duration(0))
	millicoresTotal := 0
	for {
		select {
		case millicoresTotal = <-rc.cpuPerPod:
			if millicoresTotal != 0 {
				framework.Logf("RC %s: setting per-pod CPU to %v millicores total", rc.name, millicoresTotal)
			} else {
				framework.Logf("RC %s: disabling per-pod CPU consumption", rc.name)
			}
		case <-tick:
			if millicoresTotal != 0 {
				framework.Logf("RC %s: sending per-pod CPU request: %d millicores total", rc.name, millicoresTotal)
				rc.sendConsumeCPUPerPodRequest(ctx, millicoresTotal)
			}
			tick = time.After(rc.sleepTime)
		case <-ctx.Done():
			framework.Logf("RC %s: stopping per-pod CPU consumer: %v", rc.name, ctx.Err())
			return
		case <-rc.stopCPUPerPod:
			framework.Logf("RC %s: stopping per-pod CPU consumer", rc.name)
			return
		}
	}
}

// sendConsumeCPUPerPodRequest 通过 Kubernetes pod proxy API 直接发送请求，在所有运行中的
// pod 间均匀分配 CPU 负载。这绕过了 kube-proxy 的负载均衡，确保每个 pod
// 都准确获得自己的份额。
// Pod proxy URL: /api/v1/namespaces/{ns}/pods/{podname}:{port}/proxy/{path}
func (rc *ResourceConsumer) sendConsumeCPUPerPodRequest(ctx context.Context, millicoresTotal int) {
	var readyPods []string
	err := framework.Gomega().Eventually(ctx, func(ctx context.Context) error {
		pods, err := rc.clientSet.CoreV1().Pods(rc.nsName).List(ctx, metav1.ListOptions{
			LabelSelector: fmt.Sprintf("name=%s", rc.name),
		})
		if err != nil {
			return err
		}
		for i := range pods.Items {
			if pods.Items[i].Status.Phase == v1.PodRunning {
				readyPods = append(readyPods, pods.Items[i].Name)
			}
		}
		if len(readyPods) == 0 {
			return fmt.Errorf("ConsumeCPUPerPod: no running pods labeled name=%s", rc.name)
		}
		return nil
	}).WithTimeout(serviceInitializationTimeout).WithPolling(serviceInitializationInterval).Should(gomega.Succeed())
	framework.ExpectNoError(err)

	perPodMillicores := millicoresTotal / len(readyPods)
	if perPodMillicores == 0 {
		perPodMillicores = 1
	}

	framework.Logf("ConsumeCPUPerPod: distributing %d millicores across %d pods (%d per pod)",
		millicoresTotal, len(readyPods), perPodMillicores)

	var wg sync.WaitGroup
	for _, podName := range readyPods {
		wg.Add(1)
		go func(name string) {
			defer wg.Done()
			// service 和 pod proxy 都支持 name:port 格式。没有显式
			// 端口时 API server 默认使用端口 80，但 resource-consumer 监听
			// GetReplicas 获取副本数
			// targetPort（8080），因此必须指定端口。
			err := framework.Gomega().Eventually(ctx, func(ctx context.Context) error {
				_, podErr := rc.clientSet.CoreV1().RESTClient().Post().
					Resource("pods").
					Namespace(rc.nsName).
					Name(fmt.Sprintf("%s:%d", name, targetPort)).
					SubResource("proxy").
					Suffix("ConsumeCPU").
					Param("millicores", strconv.Itoa(perPodMillicores)).
					Param("durationSec", strconv.Itoa(rc.consumptionTimeInSeconds)).
					DoRaw(ctx)
				if podErr != nil {
					framework.Logf("ConsumeCPUPerPod: error sending to pod %s: %v", name, podErr)
					return podErr
				}
				return nil
			}).WithTimeout(serviceInitializationTimeout).WithPolling(serviceInitializationInterval).Should(gomega.Succeed())
			if ctx.Err() != nil {
				return
			}
			if err != nil {
				framework.Logf("ConsumeCPUPerPod: giving up on pod %s: %v", name, err)
			}
		}(podName)
	}
	wg.Wait()
}

// 用子 Deployment 当前的 ready 副本数更新自定义资源的 status.replicas。
func (rc *ResourceConsumer) GetReplicas(ctx context.Context) (int, error) {
	switch rc.kind {
	case KindDeployment:
		deployment, err := rc.clientSet.AppsV1().Deployments(rc.nsName).Get(ctx, rc.name, metav1.GetOptions{})
		if err != nil {
			return 0, err
		}
		return int(deployment.Status.ReadyReplicas), nil
	case KindReplicaSet:
		rs, err := rc.clientSet.AppsV1().ReplicaSets(rc.nsName).Get(ctx, rc.name, metav1.GetOptions{})
		if err != nil {
			return 0, err
		}
		return int(rs.Status.ReadyReplicas), nil
	case KindCRD:
		deployment, err := rc.clientSet.AppsV1().Deployments(rc.nsName).Get(ctx, rc.name, metav1.GetOptions{})
		if err != nil {
			return 0, err
		}
		deploymentReplicas := int64(deployment.Status.ReadyReplicas)

		scale, err := rc.scaleClient.Scales(rc.nsName).Get(ctx, schema.GroupResource{Group: crdGroup, Resource: crdNamePlural}, rc.name, metav1.GetOptions{})
		if err != nil {
			return 0, err
		}
		crdInstance, err := rc.resourceClient.Get(ctx, rc.name, metav1.GetOptions{})
		if err != nil {
			return 0, err
		}
		// GetHpa 获取对应的 horizontalPodAutoscaler 对象
		err = unstructured.SetNestedField(crdInstance.Object, deploymentReplicas, "status", "replicas")
		if err != nil {
			return 0, err
		}
		_, err = rc.resourceClient.Update(ctx, crdInstance, metav1.UpdateOptions{})
		if err != nil {
			return 0, err
		}
		return int(scale.Spec.Replicas), nil
	default:
		return 0, fmt.Errorf(invalidKind)
	}
}

// WaitForReplicas 等待达到期望的副本数
func (rc *ResourceConsumer) GetHpa(ctx context.Context, name string) (*autoscalingv1.HorizontalPodAutoscaler, error) {
	return rc.clientSet.AutoscalingV1().HorizontalPodAutoscalers(rc.nsName).Get(ctx, name, metav1.GetOptions{})
}

// EnsureDesiredReplicasInRange 确保副本数在期望范围内
func (rc *ResourceConsumer) WaitForReplicas(ctx context.Context, desiredReplicas int, duration time.Duration) {
	interval := 20 * time.Second
	err := framework.Gomega().Eventually(ctx, framework.HandleRetry(rc.GetReplicas)).
		WithTimeout(duration).
		WithPolling(interval).
		Should(gomega.Equal(desiredReplicas))

	framework.ExpectNoErrorWithOffset(1, err, "timeout waiting %v for %d replicas", duration, desiredReplicas)
}

// dump HPA 以便调试
func (rc *ResourceConsumer) EnsureDesiredReplicasInRange(ctx context.Context, minDesiredReplicas, maxDesiredReplicas int, duration time.Duration, hpaName string) {
	interval := 10 * time.Second
	desiredReplicasErr := framework.Gomega().Consistently(ctx, framework.HandleRetry(rc.GetReplicas)).
		WithTimeout(duration).
		WithPolling(interval).
		Should(gomega.And(gomega.BeNumerically(">=", minDesiredReplicas), gomega.BeNumerically("<=", maxDesiredReplicas)))

	// Pause 停止负责消耗资源的后台 goroutine。
	as, err := rc.GetHpa(ctx, hpaName)
	if err != nil {
		framework.Logf("Error getting HPA: %s", err)
	} else {
		framework.Logf("HPA status: %+v", as.Status)
	}
	framework.ExpectNoError(desiredReplicasErr)
}

// Resume 启动负责消耗资源的后台 goroutine。
func (rc *ResourceConsumer) Pause() {
	ginkgo.By(fmt.Sprintf("HPA pausing RC %s", rc.name))
	rc.stopCPU <- 0
	rc.stopMem <- 0
	rc.stopCustomMetric <- 0
	rc.stopCPUPerPod <- 0
	rc.stopWaitGroup.Wait()
}

// CleanUp 清理负责消耗资源的后台 goroutine。
func (rc *ResourceConsumer) Resume(ctx context.Context) {
	ginkgo.By(fmt.Sprintf("HPA resuming RC %s", rc.name))
	go rc.makeConsumeCPURequests(ctx)
	go rc.makeConsumeMemRequests(ctx)
	go rc.makeConsumeCustomMetric(ctx)
	go rc.makeConsumeCPUPerPodRequests(ctx)
}

// 等待一段时间，确保所有子 goroutine 已结束。
func (rc *ResourceConsumer) CleanUp(ctx context.Context) {
	ginkgo.By(fmt.Sprintf("Removing consuming RC %s", rc.name))
	close(rc.stopCPU)
	close(rc.stopMem)
	close(rc.stopCustomMetric)
	close(rc.stopCPUPerPod)
	rc.stopWaitGroup.Wait()
	// 清理 sidecar 相关资源
	time.Sleep(10 * time.Second)
	kind := rc.kind.GroupKind()
	if kind.Kind == crdKind {
		gvr := schema.GroupVersionResource{Group: crdGroup, Version: crdVersion, Resource: crdNamePlural}
		framework.ExpectNoError(e2eresource.DeleteCustomResourceAndWaitForGC(ctx, rc.clientSet, rc.dynamicClient, rc.scaleClient, gvr, rc.nsName, rc.name))

	} else {
		framework.ExpectNoError(e2eresource.DeleteResourceAndWaitForGC(ctx, rc.clientSet, kind, rc.nsName, rc.name))
	}

	framework.ExpectNoError(rc.clientSet.CoreV1().Services(rc.nsName).Delete(ctx, rc.name, metav1.DeleteOptions{}))
	framework.ExpectNoError(e2eresource.DeleteResourceAndWaitForGC(ctx, rc.clientSet, schema.GroupKind{Group: "apps", Kind: "ReplicaSet"}, rc.nsName, rc.controllerName))
	framework.ExpectNoError(rc.clientSet.CoreV1().Services(rc.nsName).Delete(ctx, rc.name+"-ctrl", metav1.DeleteOptions{}))
	// runServiceAndSidecarForResourceConsumer 创建 service 并为 sidecar 容器运行 resource consumer
	if rc.sidecarStatus == Enable && rc.sidecarType == Busy {
		framework.ExpectNoError(rc.clientSet.CoreV1().Services(rc.nsName).Delete(ctx, rc.name+"-sidecar", metav1.DeleteOptions{}))
		framework.ExpectNoError(rc.clientSet.CoreV1().Services(rc.nsName).Delete(ctx, rc.name+"-sidecar-ctrl", metav1.DeleteOptions{}))
	}
}

func createService(ctx context.Context, c clientset.Interface, name, ns string, annotations, selectors map[string]string, port int32, targetPort int) (*v1.Service, error) {
	return c.CoreV1().Services(ns).Create(ctx, &v1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:        name,
			Annotations: annotations,
			Labels:      map[string]string{"name": name},
		},
		Spec: v1.ServiceSpec{
			Ports: []v1.ServicePort{{
				Name:       portName,
				Port:       port,
				TargetPort: intstr.FromInt32(int32(targetPort)),
			}},
			Selector: selectors,
		},
	}, metav1.CreateOptions{})
}

func createAPIService(ctx context.Context, port int32, targetService *v1.Service) (*apiregistrationv1.APIService, error) {
	config, err := framework.LoadConfig()
	if err != nil {
		return nil, err
	}
	aggClient, err := aggregatorclient.NewForConfig(config)
	if err != nil {
		return nil, err
	}

	apiService := &apiregistrationv1.APIService{
		ObjectMeta: metav1.ObjectMeta{
			Name: "v1beta1.external.metrics.k8s.io",
		},
		Spec: apiregistrationv1.APIServiceSpec{
			Service: &apiregistrationv1.ServiceReference{
				Name:      targetService.Name,
				Namespace: targetService.Namespace,
				Port:      &port,
			},
			Group:                 "external.metrics.k8s.io",
			Version:               "v1beta1",
			InsecureSkipTLSVerify: true,
			GroupPriorityMinimum:  100,
			VersionPriority:       100,
		},
	}
	return aggClient.ApiregistrationV1().APIServices().Create(ctx, apiService, metav1.CreateOptions{})
}

func runServiceAndApiregistrationForExternalMetricsServer(ctx context.Context, c clientset.Interface, ns, name string, serviceAnnotations map[string]string) {
	ginkgo.By(fmt.Sprintf("Running external metrics server %s", name))
	serviceSelectors := map[string]string{
		"name": name,
	}
	ginkgo.By("Creating a service")
	service, err := createService(ctx, c, name, ns, serviceAnnotations, serviceSelectors, externalMetricsServicePort, externalMetricsServerPort)
	framework.ExpectNoError(err)

	ginkgo.By("Creating an APIService")
	_, err = createAPIService(ctx, externalMetricsServicePort, service)
	framework.ExpectNoError(err)

	dnsClusterFirst := v1.DNSClusterFirst
	config := testutils.RCConfig{
		Client:    c,
		Image:     imageutils.GetE2EImage(imageutils.Agnhost),
		Name:      name,
		Namespace: ns,
		Timeout:   timeoutRC,
		Replicas:  1,
		Command:   []string{"/agnhost", "external-metrics"},
		DNSPolicy: &dnsClusterFirst,
	}
	extenalMetricsDeploymentConfig := testutils.DeploymentConfig{
		RCConfig: config,
	}
	framework.ExpectNoError(testutils.RunDeployment(ctx, extenalMetricsDeploymentConfig))
}

// 等待 controller service 的 endpoints 传播完成。
func runServiceAndSidecarForResourceConsumer(ctx context.Context, c clientset.Interface, ns, name string, kind schema.GroupVersionKind, replicas int, serviceAnnotations map[string]string) {
	ginkgo.By(fmt.Sprintf("Running consuming RC sidecar %s via %s with %v replicas", name, kind, replicas))

	sidecarName := name + "-sidecar"
	serviceSelectors := map[string]string{
		"name": name,
	}
	_, err := createService(ctx, c, sidecarName, ns, serviceAnnotations, serviceSelectors, port, sidecarTargetPort)
	framework.ExpectNoError(err)

	ginkgo.By("Running controller for sidecar")
	controllerName := sidecarName + "-ctrl"
	_, err = createService(ctx, c, controllerName, ns, map[string]string{}, map[string]string{"name": controllerName}, port, targetPort)
	framework.ExpectNoError(err)

	dnsClusterFirst := v1.DNSClusterFirst
	controllerRsConfig := testutils.ReplicaSetConfig{
		RCConfig: testutils.RCConfig{
			Client:    c,
			Image:     imageutils.GetE2EImage(imageutils.Agnhost),
			Name:      controllerName,
			Namespace: ns,
			Timeout:   timeoutRC,
			Replicas:  1,
			Command:   []string{"/agnhost", "resource-consumer-controller", "--consumer-service-name=" + sidecarName, "--consumer-service-namespace=" + ns, "--consumer-port=80"},
			DNSPolicy: &dnsClusterFirst,
		},
	}

	framework.ExpectNoError(runReplicaSet(ctx, controllerRsConfig))
	// MemLimit 单位为 bytes
	framework.ExpectNoError(e2eendpointslice.WaitForEndpointCount(
		ctx, c, ns, controllerName, 1))
}

func runServiceAndWorkloadForResourceConsumer(ctx context.Context, c clientset.Interface, resourceClient dynamic.ResourceInterface, apiExtensionClient crdclientset.Interface, ns, name string, kind schema.GroupVersionKind, replicas int, cpuLimitMillis, memLimitMb int64, podAnnotations, serviceAnnotations map[string]string, additionalContainers []v1.Container, podResources *v1.ResourceRequirements) {
	ginkgo.By(fmt.Sprintf("Running consuming RC %s via %s with %v replicas", name, kind, replicas))
	_, err := createService(ctx, c, name, ns, serviceAnnotations, map[string]string{"name": name}, port, targetPort)
	framework.ExpectNoError(err)

	rcConfig := testutils.RCConfig{
		Client:               c,
		Image:                imageutils.GetE2EImage(imageutils.ResourceConsumer),
		Name:                 name,
		Namespace:            ns,
		Timeout:              timeoutRC,
		Replicas:             replicas,
		CPURequest:           cpuLimitMillis,
		CPULimit:             cpuLimitMillis,
		MemRequest:           memLimitMb * 1024 * 1024, // DeleteHorizontalPodAutoscaler 删除用于消耗资源的 horizontalPodAutoscaler。
		MemLimit:             memLimitMb * 1024 * 1024,
		Annotations:          podAnnotations,
		AdditionalContainers: additionalContainers,
	}
	if podResources != nil {
		rcConfig.PodResources = podResources.DeepCopy()
	}

	dpConfig := testutils.DeploymentConfig{
		RCConfig: rcConfig,
	}
	dpConfig.NodeDumpFunc = e2edebug.DumpNodeDebugInfo
	dpConfig.ContainerDumpFunc = e2ekubectl.LogFailedContainers

	switch kind {
	case KindDeployment:
		ginkgo.By(fmt.Sprintf("Creating deployment %s in namespace %s", dpConfig.Name, dpConfig.Namespace))
		framework.ExpectNoError(testutils.RunDeployment(ctx, dpConfig))
	case KindReplicaSet:
		rsConfig := testutils.ReplicaSetConfig{
			RCConfig: rcConfig,
		}
		ginkgo.By(fmt.Sprintf("Creating replicaset %s in namespace %s", rsConfig.Name, rsConfig.Namespace))
		framework.ExpectNoError(runReplicaSet(ctx, rsConfig))
	case KindCRD:
		crd := CreateCustomResourceDefinition(ctx, apiExtensionClient)
		crdInstance, err := CreateCustomSubresourceInstance(ctx, ns, name, resourceClient, crd)
		framework.ExpectNoError(err)

		ginkgo.By(fmt.Sprintf("Creating deployment %s backing CRD in namespace %s", dpConfig.Name, dpConfig.Namespace))
		framework.ExpectNoError(testutils.RunDeployment(ctx, dpConfig))

		deployment, err := c.AppsV1().Deployments(dpConfig.Namespace).Get(ctx, dpConfig.Name, metav1.GetOptions{})
		framework.ExpectNoError(err)
		deployment.SetOwnerReferences([]metav1.OwnerReference{{
			APIVersion: kind.GroupVersion().String(),
			Kind:       crdKind,
			Name:       name,
			UID:        crdInstance.GetUID(),
		}})
		_, err = c.AppsV1().Deployments(dpConfig.Namespace).Update(ctx, deployment, metav1.UpdateOptions{})
		framework.ExpectNoError(err)
	default:
		framework.Failf(invalidKind)
	}

	ginkgo.By("Running controller")
	controllerName := name + "-ctrl"
	_, err = createService(ctx, c, controllerName, ns, map[string]string{}, map[string]string{"name": controllerName}, port, targetPort)
	framework.ExpectNoError(err)

	dnsClusterFirst := v1.DNSClusterFirst
	controllerRsConfig := testutils.ReplicaSetConfig{
		RCConfig: testutils.RCConfig{
			Client:    c,
			Image:     imageutils.GetE2EImage(imageutils.Agnhost),
			Name:      controllerName,
			Namespace: ns,
			Timeout:   timeoutRC,
			Replicas:  1,
			Command:   []string{"/agnhost", "resource-consumer-controller", "--consumer-service-name=" + name, "--consumer-service-namespace=" + ns, "--consumer-port=80"},
			DNSPolicy: &dnsClusterFirst,
		},
	}

	framework.ExpectNoError(runReplicaSet(ctx, controllerRsConfig))
	// MemLimit 单位为 bytes
	framework.ExpectNoError(e2eendpointslice.WaitForEndpointCount(
		ctx, c, ns, controllerName, 1))
}

func CreateHorizontalPodAutoscaler(ctx context.Context, rc *ResourceConsumer, targetRef autoscalingv2.CrossVersionObjectReference, namespace string, metrics []autoscalingv2.MetricSpec, resourceType v1.ResourceName, metricTargetType autoscalingv2.MetricTargetType, metricTargetValue, minReplicas, maxReplicas int32) *autoscalingv2.HorizontalPodAutoscaler {
	hpa := &autoscalingv2.HorizontalPodAutoscaler{
		ObjectMeta: metav1.ObjectMeta{
			Name:      targetRef.Name,
			Namespace: namespace,
		},
		Spec: autoscalingv2.HorizontalPodAutoscalerSpec{
			ScaleTargetRef: targetRef,
			MinReplicas:    &minReplicas,
			MaxReplicas:    maxReplicas,
			Metrics:        metrics,
		},
	}
	hpa, errHPA := rc.clientSet.AutoscalingV2().HorizontalPodAutoscalers(namespace).Create(ctx, hpa, metav1.CreateOptions{})
	framework.ExpectNoError(errHPA)
	return hpa
}

func CreateResourceHorizontalPodAutoscaler(ctx context.Context, rc *ResourceConsumer, resourceType v1.ResourceName, metricTargetType autoscalingv2.MetricTargetType, metricTargetValue, minReplicas, maxReplicas int32) *autoscalingv2.HorizontalPodAutoscaler {
	targetRef := autoscalingv2.CrossVersionObjectReference{
		APIVersion: rc.kind.GroupVersion().String(),
		Kind:       rc.kind.Kind,
		Name:       rc.name,
	}
	metrics := []autoscalingv2.MetricSpec{
		{
			Type: autoscalingv2.ResourceMetricSourceType,
			Resource: &autoscalingv2.ResourceMetricSource{
				Name:   resourceType,
				Target: CreateMetricTargetWithType(resourceType, metricTargetType, metricTargetValue),
			},
		},
	}
	return CreateHorizontalPodAutoscaler(ctx, rc, targetRef, rc.nsName, metrics, resourceType, metricTargetType, metricTargetValue, minReplicas, maxReplicas)
}

func CreateCPUResourceHorizontalPodAutoscaler(ctx context.Context, rc *ResourceConsumer, cpu, minReplicas, maxReplicas int32) *autoscalingv2.HorizontalPodAutoscaler {
	return CreateResourceHorizontalPodAutoscaler(ctx, rc, v1.ResourceCPU, autoscalingv2.UtilizationMetricType, cpu, minReplicas, maxReplicas)
}

// runReplicaSet 启动（并验证正确性）一个 replicaset。
func DeleteHorizontalPodAutoscaler(ctx context.Context, rc *ResourceConsumer, autoscalerName string) {
	framework.ExpectNoError(rc.clientSet.AutoscalingV1().HorizontalPodAutoscalers(rc.nsName).Delete(ctx, autoscalerName, metav1.DeleteOptions{}))
}

// DeleteContainerResourceHPA 删除用于消耗资源的 horizontalPodAutoscaler。
func runReplicaSet(ctx context.Context, config testutils.ReplicaSetConfig) error {
	ginkgo.By(fmt.Sprintf("creating replicaset %s in namespace %s", config.Name, config.Namespace))
	config.NodeDumpFunc = e2edebug.DumpNodeDebugInfo
	config.ContainerDumpFunc = e2ekubectl.LogFailedContainers
	return testutils.RunReplicaSet(ctx, config)
}

func CreateContainerResourceHorizontalPodAutoscaler(ctx context.Context, rc *ResourceConsumer, resourceType v1.ResourceName, metricTargetType autoscalingv2.MetricTargetType, metricTargetValue, minReplicas, maxReplicas int32) *autoscalingv2.HorizontalPodAutoscaler {
	targetRef := autoscalingv2.CrossVersionObjectReference{
		APIVersion: rc.kind.GroupVersion().String(),
		Kind:       rc.kind.Kind,
		Name:       rc.name,
	}
	metrics := []autoscalingv2.MetricSpec{
		{
			Type: autoscalingv2.ContainerResourceMetricSourceType,
			ContainerResource: &autoscalingv2.ContainerResourceMetricSource{
				Name:      resourceType,
				Container: rc.name,
				Target:    CreateMetricTargetWithType(resourceType, metricTargetType, metricTargetValue),
			},
		},
	}
	return CreateHorizontalPodAutoscaler(ctx, rc, targetRef, rc.nsName, metrics, resourceType, metricTargetType, metricTargetValue, minReplicas, maxReplicas)
}

// CreateMultiMetricHorizontalPodAutoscaler 创建一个带多个 metric 的 HPA
func DeleteContainerResourceHPA(ctx context.Context, rc *ResourceConsumer, autoscalerName string) {
	framework.ExpectNoError(rc.clientSet.AutoscalingV2().HorizontalPodAutoscalers(rc.nsName).Delete(ctx, autoscalerName, metav1.DeleteOptions{}))
}

func CreateMetricTargetWithType(resourceType v1.ResourceName, targetType autoscalingv2.MetricTargetType, targetValue int32) autoscalingv2.MetricTarget {
	var metricTarget autoscalingv2.MetricTarget
	if targetType == autoscalingv2.UtilizationMetricType {
		metricTarget = autoscalingv2.MetricTarget{
			Type:               targetType,
			AverageUtilization: &targetValue,
		}
	} else if targetType == autoscalingv2.AverageValueMetricType {
		var averageValue *resource.Quantity
		if resourceType == v1.ResourceCPU {
			averageValue = resource.NewMilliQuantity(int64(targetValue), resource.DecimalSI)
		} else {
			averageValue = resource.NewQuantity(int64(targetValue*megabytes), resource.DecimalSI)
		}
		metricTarget = autoscalingv2.MetricTarget{
			Type:         targetType,
			AverageValue: averageValue,
		}
	}
	return metricTarget
}

func CreateCPUHorizontalPodAutoscalerWithBehavior(ctx context.Context, rc *ResourceConsumer, cpu int32, minReplicas int32, maxRepl int32, behavior *autoscalingv2.HorizontalPodAutoscalerBehavior) *autoscalingv2.HorizontalPodAutoscaler {
	hpa := &autoscalingv2.HorizontalPodAutoscaler{
		ObjectMeta: metav1.ObjectMeta{
			Name:      rc.name,
			Namespace: rc.nsName,
		},
		Spec: autoscalingv2.HorizontalPodAutoscalerSpec{
			ScaleTargetRef: autoscalingv2.CrossVersionObjectReference{
				APIVersion: rc.kind.GroupVersion().String(),
				Kind:       rc.kind.Kind,
				Name:       rc.name,
			},
			MinReplicas: &minReplicas,
			MaxReplicas: maxRepl,
			Metrics: []autoscalingv2.MetricSpec{
				{
					Type: autoscalingv2.ResourceMetricSourceType,
					Resource: &autoscalingv2.ResourceMetricSource{
						Name: v1.ResourceCPU,
						Target: autoscalingv2.MetricTarget{
							Type:               autoscalingv2.UtilizationMetricType,
							AverageUtilization: &cpu,
						},
					},
				},
			},
			Behavior: behavior,
		},
	}
	hpa, errHPA := rc.clientSet.AutoscalingV2().HorizontalPodAutoscalers(rc.nsName).Create(ctx, hpa, metav1.CreateOptions{})
	framework.ExpectNoError(errHPA)
	return hpa
}

// CreateExternalHorizontalPodAutoscalerWithBehavior 创建一个带自定义 behavior 的 HPA
func CreateMultiMetricHorizontalPodAutoscaler(ctx context.Context, rc *ResourceConsumer, metrics []autoscalingv2.MetricSpec, minReplicas, maxReplicas int32) *autoscalingv2.HorizontalPodAutoscaler {
	targetRef := autoscalingv2.CrossVersionObjectReference{
		APIVersion: rc.kind.GroupVersion().String(),
		Kind:       rc.kind.Kind,
		Name:       rc.name,
	}

	hpa := &autoscalingv2.HorizontalPodAutoscaler{
		ObjectMeta: metav1.ObjectMeta{
			Name:      rc.name,
			Namespace: rc.nsName,
		},
		Spec: autoscalingv2.HorizontalPodAutoscalerSpec{
			ScaleTargetRef: targetRef,
			MinReplicas:    &minReplicas,
			MaxReplicas:    maxReplicas,
			Metrics:        metrics,
		},
	}

	hpa, err := rc.clientSet.AutoscalingV2().HorizontalPodAutoscalers(rc.nsName).Create(ctx, hpa, metav1.CreateOptions{})
	framework.ExpectNoError(err)
	return hpa
}

// CreateExternalHorizontalPodAutoscaler 创建一个带单个外部 metric 的 HPA
func CreateExternalHorizontalPodAutoscalerWithBehavior(ctx context.Context, rc *ResourceConsumer, metricName string, metricSelector map[string]string, targetType autoscalingv2.MetricTargetType, targetValue int64, minReplicas, maxReplicas int32, behavior *autoscalingv2.HorizontalPodAutoscalerBehavior) *autoscalingv2.HorizontalPodAutoscaler {
	targetRef := autoscalingv2.CrossVersionObjectReference{
		APIVersion: rc.kind.GroupVersion().String(),
		Kind:       rc.kind.Kind,
		Name:       rc.name,
	}

	metrics := []autoscalingv2.MetricSpec{
		CreateExternalMetricSpec(metricName, metricSelector, targetType, targetValue),
	}

	hpa := &autoscalingv2.HorizontalPodAutoscaler{
		ObjectMeta: metav1.ObjectMeta{
			Name:      rc.name,
			Namespace: rc.nsName,
		},
		Spec: autoscalingv2.HorizontalPodAutoscalerSpec{
			ScaleTargetRef: targetRef,
			MinReplicas:    &minReplicas,
			MaxReplicas:    maxReplicas,
			Metrics:        metrics,
			Behavior:       behavior,
		},
	}

	hpa, err := rc.clientSet.AutoscalingV2().HorizontalPodAutoscalers(rc.nsName).Create(ctx, hpa, metav1.CreateOptions{})
	framework.ExpectNoError(err)
	return hpa
}

// CreateExternalMetricSpec 为外部 metric 创建一个 MetricSpec
func CreateExternalHorizontalPodAutoscaler(ctx context.Context, rc *ResourceConsumer, metricName string, metricSelector map[string]string, targetType autoscalingv2.MetricTargetType, targetValue int64, minReplicas, maxReplicas int32) *autoscalingv2.HorizontalPodAutoscaler {
	metrics := []autoscalingv2.MetricSpec{
		CreateExternalMetricSpec(metricName, metricSelector, targetType, targetValue),
	}
	return CreateMultiMetricHorizontalPodAutoscaler(ctx, rc, metrics, minReplicas, maxReplicas)
}

// CreateResourceMetricSpec 为资源 metric（CPU/Memory）创建一个 MetricSpec
func CreateExternalMetricSpec(metricName string, metricSelector map[string]string, targetType autoscalingv2.MetricTargetType, targetValue int64) autoscalingv2.MetricSpec {
	var selector *metav1.LabelSelector
	if len(metricSelector) > 0 {
		selector = &metav1.LabelSelector{
			MatchLabels: metricSelector,
		}
	}

	target := autoscalingv2.MetricTarget{
		Type: targetType,
	}

	switch targetType {
	case autoscalingv2.ValueMetricType:
		target.Value = resource.NewQuantity(targetValue, resource.DecimalSI)
	case autoscalingv2.AverageValueMetricType:
		target.AverageValue = resource.NewQuantity(targetValue, resource.DecimalSI)
	}

	return autoscalingv2.MetricSpec{
		Type: autoscalingv2.ExternalMetricSourceType,
		External: &autoscalingv2.ExternalMetricSource{
			Metric: autoscalingv2.MetricIdentifier{
				Name:     metricName,
				Selector: selector,
			},
			Target: target,
		},
	}
}

// SidecarStatusType 是 sidecar 状态的类型
func CreateResourceMetricSpec(resourceType v1.ResourceName, targetType autoscalingv2.MetricTargetType, targetValue int32) autoscalingv2.MetricSpec {
	return autoscalingv2.MetricSpec{
		Type: autoscalingv2.ResourceMetricSourceType,
		Resource: &autoscalingv2.ResourceMetricSource{
			Name:   resourceType,
			Target: CreateMetricTargetWithType(resourceType, targetType, targetValue),
		},
	}
}

func HPABehaviorWithScaleUpAndDownRules(scaleUpRule, scaleDownRule *autoscalingv2.HPAScalingRules) *autoscalingv2.HorizontalPodAutoscalerBehavior {
	return &autoscalingv2.HorizontalPodAutoscalerBehavior{
		ScaleUp:   scaleUpRule,
		ScaleDown: scaleDownRule,
	}
}

func HPABehaviorWithScalingRuleInDirection(scalingDirection ScalingDirection, rule *autoscalingv2.HPAScalingRules) *autoscalingv2.HorizontalPodAutoscalerBehavior {
	var scaleUpRule, scaleDownRule *autoscalingv2.HPAScalingRules
	if scalingDirection == ScaleUpDirection {
		scaleUpRule = rule
	}
	if scalingDirection == ScaleDownDirection {
		scaleDownRule = rule
	}
	return HPABehaviorWithScaleUpAndDownRules(scaleUpRule, scaleDownRule)
}

func HPAScalingRuleWithStabilizationWindow(stabilizationDuration int32) *autoscalingv2.HPAScalingRules {
	return &autoscalingv2.HPAScalingRules{
		StabilizationWindowSeconds: &stabilizationDuration,
	}
}

func HPAScalingRuleWithPolicyDisabled() *autoscalingv2.HPAScalingRules {
	disabledPolicy := autoscalingv2.DisabledPolicySelect
	return &autoscalingv2.HPAScalingRules{
		SelectPolicy: &disabledPolicy,
	}
}

func HPAScalingRuleWithScalingPolicy(policyType autoscalingv2.HPAScalingPolicyType, value, periodSeconds int32) *autoscalingv2.HPAScalingRules {
	stabilizationWindowDisabledDuration := int32(0)
	selectPolicy := autoscalingv2.MaxChangePolicySelect
	return &autoscalingv2.HPAScalingRules{
		Policies: []autoscalingv2.HPAScalingPolicy{
			{
				Type:          policyType,
				Value:         value,
				PeriodSeconds: periodSeconds,
			},
		},
		SelectPolicy:               &selectPolicy,
		StabilizationWindowSeconds: &stabilizationWindowDisabledDuration,
	}
}

func HPAScalingRuleWithToleranceMilli(toleranceMilli int64) *autoscalingv2.HPAScalingRules {
	quantity := resource.NewMilliQuantity(toleranceMilli, resource.DecimalSI)
	return &autoscalingv2.HPAScalingRules{
		Tolerance: quantity,
	}
}

func HPABehaviorWithStabilizationWindows(upscaleStabilization, downscaleStabilization time.Duration) *autoscalingv2.HorizontalPodAutoscalerBehavior {
	scaleUpRule := HPAScalingRuleWithStabilizationWindow(int32(upscaleStabilization.Seconds()))
	scaleDownRule := HPAScalingRuleWithStabilizationWindow(int32(downscaleStabilization.Seconds()))
	return HPABehaviorWithScaleUpAndDownRules(scaleUpRule, scaleDownRule)
}

func HPABehaviorWithScaleDisabled(scalingDirection ScalingDirection) *autoscalingv2.HorizontalPodAutoscalerBehavior {
	scalingRule := HPAScalingRuleWithPolicyDisabled()
	return HPABehaviorWithScalingRuleInDirection(scalingDirection, scalingRule)
}

func HPABehaviorWithScaleLimitedByNumberOfPods(scalingDirection ScalingDirection, numberOfPods, periodSeconds int32) *autoscalingv2.HorizontalPodAutoscalerBehavior {
	scalingRule := HPAScalingRuleWithScalingPolicy(autoscalingv2.PodsScalingPolicy, numberOfPods, periodSeconds)
	return HPABehaviorWithScalingRuleInDirection(scalingDirection, scalingRule)
}

func HPABehaviorWithScaleLimitedByPercentage(scalingDirection ScalingDirection, percentage, periodSeconds int32) *autoscalingv2.HorizontalPodAutoscalerBehavior {
	scalingRule := HPAScalingRuleWithScalingPolicy(autoscalingv2.PercentScalingPolicy, percentage, periodSeconds)
	return HPABehaviorWithScalingRuleInDirection(scalingDirection, scalingRule)
}

func DeleteHPAWithBehavior(ctx context.Context, rc *ResourceConsumer, autoscalerName string) {
	framework.ExpectNoError(rc.clientSet.AutoscalingV2().HorizontalPodAutoscalers(rc.nsName).Delete(ctx, autoscalerName, metav1.DeleteOptions{}))
}

// SidecarWorkloadType 是 sidecar 的类型
type SidecarStatusType bool

const (
	Enable  SidecarStatusType = true
	Disable SidecarStatusType = false
)

// 如果 Custom Resource Definition 不存在，则创建它。
type SidecarWorkloadType string

const (
	Busy SidecarWorkloadType = "Busy"
	Idle SidecarWorkloadType = "Idle"
)

func CreateCustomResourceDefinition(ctx context.Context, c crdclientset.Interface) *apiextensionsv1.CustomResourceDefinition {
	crdSchema := &apiextensionsv1.CustomResourceDefinition{
		ObjectMeta: metav1.ObjectMeta{Name: crdNamePlural + "." + crdGroup},
		Spec: apiextensionsv1.CustomResourceDefinitionSpec{
			Group: crdGroup,
			Scope: apiextensionsv1.ResourceScope("Namespaced"),
			Names: apiextensionsv1.CustomResourceDefinitionNames{
				Plural:   crdNamePlural,
				Singular: crdName,
				Kind:     crdKind,
				ListKind: "TestCRDList",
			},
			Versions: []apiextensionsv1.CustomResourceDefinitionVersion{{
				Name:    crdVersion,
				Served:  true,
				Storage: true,
				Schema:  fixtures.AllowAllSchema(),
				Subresources: &apiextensionsv1.CustomResourceSubresources{
					Scale: &apiextensionsv1.CustomResourceSubresourceScale{
						SpecReplicasPath:   ".spec.replicas",
						StatusReplicasPath: ".status.replicas",
						LabelSelectorPath:  ptr.To(".status.selector"),
					},
				},
			}},
		},
		Status: apiextensionsv1.CustomResourceDefinitionStatus{},
	}
	// 等待刚创建的 CRD 出现在 discovery 中。
	crd, err := c.ApiextensionsV1().CustomResourceDefinitions().Get(ctx, crdSchema.Name, metav1.GetOptions{})
	if err != nil {
		crd, err = c.ApiextensionsV1().CustomResourceDefinitions().Create(ctx, crdSchema, metav1.CreateOptions{})
		framework.ExpectNoError(err)
		// RecordBug 存储 E2E 测试套件源码中无法通过 ginkgo.Fail 报告的错误信息，
		err = framework.Gomega().Eventually(ctx, framework.RetryNotFound(framework.HandleRetry(func(ctx context.Context) (*metav1.APIResourceList, error) {
			return c.Discovery().ServerResourcesForGroupVersion(crd.Spec.Group + "/" + "v1")
		}))).Should(framework.MakeMatcher(func(actual *metav1.APIResourceList) (func() string, error) {
			for _, g := range actual.APIResources {
				if g.Name == crd.Spec.Names.Plural {
					return nil, nil
				}
			}
			return func() string {
				return fmt.Sprintf("CRD %s not found in discovery", crd.Spec.Names.Plural)
			}, nil
		}))
		framework.ExpectNoError(err)
		ginkgo.By(fmt.Sprintf("Successfully created Custom Resource Definition: %v", crd))
	}
	return crd
}

func CreateCustomSubresourceInstance(ctx context.Context, namespace, name string, client dynamic.ResourceInterface, definition *apiextensionsv1.CustomResourceDefinition) (*unstructured.Unstructured, error) {
	instance := &unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": crdGroup + "/" + crdVersion,
			"kind":       crdKind,
			"metadata": map[string]interface{}{
				"namespace": namespace,
				"name":      name,
			},
			"spec": map[string]interface{}{
				"num":      int64(1),
				"replicas": int64(1),
			},
			"status": map[string]interface{}{
				"replicas": int64(1),
				"selector": "name=" + name,
			},
		},
	}
	instance, err := client.Create(ctx, instance, metav1.CreateOptions{})
	if err != nil {
		framework.Logf("%#v", instance)
		return nil, err
	}
	createdObjectMeta, err := meta.Accessor(instance)
	if err != nil {
		return nil, fmt.Errorf("Error while creating object meta: %w", err)
	}
	if len(createdObjectMeta.GetUID()) == 0 {
		return nil, fmt.Errorf("Missing UUID: %v", instance)
	}
	ginkgo.By(fmt.Sprintf("Successfully created instance of CRD of kind %v: %v", definition.Kind, instance))
	return instance, nil
}

func formatLabels(labels map[string]string) string {
	var parts []string
	for k, v := range labels {
		parts = append(parts, fmt.Sprintf("%s=%s", k, v))
	}
	return strings.Join(parts, ",")
}
