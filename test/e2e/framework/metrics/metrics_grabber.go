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

package metrics

import (
	"context"
	"errors"
	"fmt"
	"net"
	"regexp"
	"strconv"
	"sync"
	"time"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	k8snet "k8s.io/apimachinery/pkg/util/net"
	"k8s.io/apimachinery/pkg/util/wait"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/test/e2e/framework"
	e2epod "k8s.io/kubernetes/test/e2e/framework/pod"
	e2epodoutput "k8s.io/kubernetes/test/e2e/framework/pod/output"
)

const (
	// kubeSchedulerPort 是 scheduler status server 的默认端口。
	kubeSchedulerPort = 10259
	// kubeControllerManagerPort 是 controller manager status server 的默认端口。
	kubeControllerManagerPort = 10257
	// snapshotControllerPort 是 snapshot controller 的端口。
	snapshotControllerPort = 9102
	// kubeProxyPort 是 kube-proxy status server 的默认端口。
	kubeProxyPort = 10249
)

// MetricsGrabbingDisabledError 是不支持采集指标时由 MetricsGrabber 的各个
// Wrap 函数包装的错误。检查指标数据的测试遇到它时应跳过检查。
var MetricsGrabbingDisabledError = errors.New("metrics grabbing disabled")

// Collection 汇总各组件采集到的指标。
type Collection struct {
	APIServerMetrics          APIServerMetrics
	APIServerMetricsSLIs      APIServerMetrics
	ControllerManagerMetrics  ControllerManagerMetrics
	SnapshotControllerMetrics SnapshotControllerMetrics
	KubeletMetrics            map[string]KubeletMetrics
	SchedulerMetrics          SchedulerMetrics
	ClusterAutoscalerMetrics  ClusterAutoscalerMetrics
}

// Grabber 提供从各组件采集指标的函数。
type Grabber struct {
	client                             clientset.Interface
	externalClient                     clientset.Interface
	config                             *rest.Config
	grabFromAPIServer                  bool
	grabFromControllerManager          bool
	grabFromKubelets                   bool
	grabFromScheduler                  bool
	grabFromClusterAutoscaler          bool
	grabFromSnapshotController         bool
	kubeScheduler                      string
	waitForSchedulerReadyOnce          sync.Once
	kubeControllerManager              string
	waitForControllerManagerReadyOnce  sync.Once
	snapshotController                 string
	waitForSnapshotControllerReadyOnce sync.Once
}

// NewMetricsGrabber 准备从多个组件采集指标。应在这些组件运行时调用，
// 因为它需要与组件通信，判断哪些组件可以采集指标。
//
// 指标采集是可选的调试功能，并非所有集群都支持。如果某个组件不支持，
// 对应的 Grab 函数会立即返回由 MetricsGrabbingDisabledError 派生的错误。
func NewMetricsGrabber(ctx context.Context, c clientset.Interface, ec clientset.Interface, config *rest.Config, kubelets bool, scheduler bool, controllers bool, apiServer bool, clusterAutoscaler bool, snapshotController bool) (*Grabber, error) {
	kubeScheduler := ""
	kubeControllerManager := ""
	snapshotControllerManager := ""

	regKubeScheduler := regexp.MustCompile("kube-scheduler-.*")
	regKubeControllerManager := regexp.MustCompile("kube-controller-manager-.*")
	regSnapshotController := regexp.MustCompile("volume-snapshot-controller.*")

	if (scheduler || controllers) && config == nil {
		return nil, errors.New("a rest config is required for grabbing kube-controller and kube-controller-manager metrics")
	}

	podList, err := c.CoreV1().Pods(metav1.NamespaceSystem).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, err
	}
	if len(podList.Items) < 1 {
		klog.Warningf("Can't find any pods in namespace %s to grab metrics from", metav1.NamespaceSystem)
	}
	for _, pod := range podList.Items {
		if regKubeScheduler.MatchString(pod.Name) {
			kubeScheduler = pod.Name
		}
		if regKubeControllerManager.MatchString(pod.Name) {
			kubeControllerManager = pod.Name
		}
		if regSnapshotController.MatchString(pod.Name) {
			snapshotControllerManager = pod.Name
		}
		if kubeScheduler != "" && kubeControllerManager != "" && snapshotControllerManager != "" {
			break
		}
	}
	if clusterAutoscaler && ec == nil {
		klog.Warningf("Did not receive an external client interface. Grabbing metrics from ClusterAutoscaler is disabled.")
	}

	return &Grabber{
		client:                     c,
		externalClient:             ec,
		config:                     config,
		grabFromAPIServer:          apiServer,
		grabFromControllerManager:  checkPodDebugHandlers(ctx, c, controllers, "kube-controller-manager", kubeControllerManager),
		grabFromKubelets:           kubelets,
		grabFromScheduler:          checkPodDebugHandlers(ctx, c, scheduler, "kube-scheduler", kubeScheduler),
		grabFromClusterAutoscaler:  clusterAutoscaler,
		grabFromSnapshotController: checkPodDebugHandlers(ctx, c, snapshotController, "snapshot-controller", snapshotControllerManager),
		kubeScheduler:              kubeScheduler,
		kubeControllerManager:      kubeControllerManager,
		snapshotController:         snapshotControllerManager,
	}, nil
}

func checkPodDebugHandlers(ctx context.Context, c clientset.Interface, requested bool, component, podName string) bool {
	if !requested {
		return false
	}
	if podName == "" {
		klog.Warningf("Can't find %s pod. Grabbing metrics from %s is disabled.", component, component)
		return false
	}

	// pod 所在主机上的 debug handler 可能被禁用。
	// 可以尝试获取日志输出来间接判断。
	limit := int64(1)
	if _, err := c.CoreV1().Pods(metav1.NamespaceSystem).GetLogs(podName, &v1.PodLogOptions{LimitBytes: &limit}).DoRaw(ctx); err != nil {
		klog.Warningf("Can't retrieve log output of %s (%q). Debug handlers might be disabled in kubelet. Grabbing metrics from %s is disabled.",
			podName, err, component)
		return false
	}

	// 指标采集可用。
	return true
}

// HasControlPlanePods 在 metrics grabber 找到 control plane pod 时返回 true。
func (g *Grabber) HasControlPlanePods() bool {
	return g.kubeScheduler != "" && g.kubeControllerManager != ""
}

// GrabFromKubelet 从 kubelet 采集指标。
func (g *Grabber) GrabFromKubelet(ctx context.Context, nodeName string) (KubeletMetrics, error) {
	nodes, err := g.client.CoreV1().Nodes().List(ctx, metav1.ListOptions{FieldSelector: fields.Set{"metadata.name": nodeName}.AsSelector().String()})
	if err != nil {
		return KubeletMetrics{}, err
	}
	if len(nodes.Items) != 1 {
		return KubeletMetrics{}, fmt.Errorf("Error listing nodes with name %v, got %v", nodeName, nodes.Items)
	}
	kubeletPort := nodes.Items[0].Status.DaemonEndpoints.KubeletEndpoint.Port
	return g.grabFromKubeletInternal(ctx, nodeName, int(kubeletPort), "metrics")
}

// GrabResourceMetricsFromKubelet 从 kubelet 采集资源指标。
func (g *Grabber) GrabResourceMetricsFromKubelet(ctx context.Context, nodeName string) (KubeletMetrics, error) {
	nodes, err := g.client.CoreV1().Nodes().List(ctx, metav1.ListOptions{FieldSelector: fields.Set{"metadata.name": nodeName}.AsSelector().String()})
	if err != nil {
		return KubeletMetrics{}, err
	}
	if len(nodes.Items) != 1 {
		return KubeletMetrics{}, fmt.Errorf("Error listing nodes with name %v, got %v", nodeName, nodes.Items)
	}
	kubeletPort := nodes.Items[0].Status.DaemonEndpoints.KubeletEndpoint.Port
	return g.grabFromKubeletInternal(ctx, nodeName, int(kubeletPort), "metrics/resource")
}

func (g *Grabber) grabFromKubeletInternal(ctx context.Context, nodeName string, kubeletPort int, pathSuffix string) (KubeletMetrics, error) {
	if kubeletPort <= 0 || kubeletPort > 65535 {
		return KubeletMetrics{}, fmt.Errorf("Invalid Kubelet port %v. Skipping Kubelet's metrics gathering", kubeletPort)
	}
	output, err := g.getMetricsFromNode(ctx, nodeName, int(kubeletPort), pathSuffix)
	if err != nil {
		return KubeletMetrics{}, err
	}
	return parseKubeletMetrics(output)
}

func (g *Grabber) getMetricsFromNode(ctx context.Context, nodeName string, kubeletPort int, pathSuffix string) (string, error) {
	// proxy 请求容易超时，因此设置 45 秒客户端超时并配合重试。
	var err error
	var output []byte
	err = wait.PollUntilContextTimeout(ctx, 15*time.Second, 2*time.Minute, true, func(ctx context.Context) (done bool, retErr error) {
		rawOutput, err := g.client.CoreV1().RESTClient().Get().
			Resource("nodes").
			SubResource("proxy").
			Name(fmt.Sprintf("%v:%v", nodeName, kubeletPort)).
			Suffix(pathSuffix).
			Timeout(45 * time.Second).
			Do(ctx).Raw()
		if err != nil {
			if k8snet.IsTimeout(err) {
				klog.Warningf("Metrics rest call timed out")
				return false, nil
			}
			klog.Warningf("Metrics rest call errored: %v", err)
			return false, nil
		}
		output = rawOutput
		return true, nil
	})
	return string(output), err
}

// GrabFromKubeProxy 从 kube-proxy 采集指标。
func (g *Grabber) GrabFromKubeProxy(ctx context.Context, nodeName string) (KubeProxyMetrics, error) {
	nodes, err := g.client.CoreV1().Nodes().List(ctx, metav1.ListOptions{FieldSelector: fields.Set{"metadata.name": nodeName}.AsSelector().String()})
	if err != nil {
		return KubeProxyMetrics{}, err
	}

	if len(nodes.Items) != 1 {
		return KubeProxyMetrics{}, fmt.Errorf("error listing nodes with name %v, got %v", nodeName, nodes.Items)
	}
	output, err := g.grabFromKubeProxy(ctx, nodeName)
	if err != nil {
		return KubeProxyMetrics{}, err
	}
	return parseKubeProxyMetrics(output)
}

func (g *Grabber) grabFromKubeProxy(ctx context.Context, nodeName string) (string, error) {
	hostCmdPodName := fmt.Sprintf("grab-kube-proxy-metrics-%s", framework.RandomSuffix())
	hostCmdPod := e2epod.NewExecPodSpec(metav1.NamespaceSystem, hostCmdPodName, true)
	nodeSelection := e2epod.NodeSelection{Name: nodeName}
	e2epod.SetNodeSelection(&hostCmdPod.Spec, nodeSelection)
	if _, err := g.client.CoreV1().Pods(metav1.NamespaceSystem).Create(ctx, hostCmdPod, metav1.CreateOptions{}); err != nil {
		return "", fmt.Errorf("failed to create pod to fetch metrics: %w", err)
	}
	if err := e2epod.WaitTimeoutForPodReadyInNamespace(ctx, g.client, hostCmdPodName, metav1.NamespaceSystem, 5*time.Minute); err != nil {
		return "", fmt.Errorf("error waiting for pod to be up: %w", err)
	}

	host := "127.0.0.1"
	if framework.TestContext.ClusterIsIPv6() {
		host = "::1"
	}

	stdout, err := e2epodoutput.RunHostCmd(metav1.NamespaceSystem, hostCmdPodName, fmt.Sprintf("curl --silent %s/metrics", net.JoinHostPort(host, strconv.Itoa(kubeProxyPort))))
	_ = g.client.CoreV1().Pods(metav1.NamespaceSystem).Delete(ctx, hostCmdPodName, metav1.DeleteOptions{})
	return stdout, err
}

// GrabFromScheduler 从 scheduler 采集指标。
func (g *Grabber) GrabFromScheduler(ctx context.Context) (SchedulerMetrics, error) {
	if !g.grabFromScheduler {
		return SchedulerMetrics{}, fmt.Errorf("kube-scheduler: %w", MetricsGrabbingDisabledError)
	}

	var err error

	g.waitForSchedulerReadyOnce.Do(func() {
		if readyErr := e2epod.WaitTimeoutForPodReadyInNamespace(ctx, g.client, g.kubeScheduler, metav1.NamespaceSystem, 5*time.Minute); readyErr != nil {
			err = fmt.Errorf("error waiting for kube-scheduler pod to be ready: %w", readyErr)
		}
	})
	if err != nil {
		return SchedulerMetrics{}, err
	}

	var lastMetricsFetchErr error
	var output string
	if metricsWaitErr := wait.PollUntilContextTimeout(ctx, time.Second, time.Minute, true, func(ctx context.Context) (bool, error) {
		output, lastMetricsFetchErr = g.getSecureMetricsFromPod(ctx, g.kubeScheduler, metav1.NamespaceSystem, kubeSchedulerPort)
		return lastMetricsFetchErr == nil, nil
	}); metricsWaitErr != nil {
		err := fmt.Errorf("error waiting for kube-scheduler pod to expose metrics: %v; %v", metricsWaitErr, lastMetricsFetchErr)
		return SchedulerMetrics{}, err
	}

	return parseSchedulerMetrics(output)
}

// GrabFromClusterAutoscaler 从 cluster autoscaler 采集指标。
func (g *Grabber) GrabFromClusterAutoscaler(ctx context.Context) (ClusterAutoscalerMetrics, error) {
	if !g.HasControlPlanePods() && g.externalClient == nil {
		return ClusterAutoscalerMetrics{}, fmt.Errorf("ClusterAutoscaler: %w", MetricsGrabbingDisabledError)
	}
	var client clientset.Interface
	var namespace string
	if g.externalClient != nil {
		client = g.externalClient
		namespace = "kubemark"
	} else {
		client = g.client
		namespace = metav1.NamespaceSystem
	}
	output, err := g.getMetricsFromPod(ctx, client, "cluster-autoscaler", namespace, 8085)
	if err != nil {
		return ClusterAutoscalerMetrics{}, err
	}
	return parseClusterAutoscalerMetrics(output)
}

// GrabFromControllerManager 从 controller manager 采集指标。
func (g *Grabber) GrabFromControllerManager(ctx context.Context) (ControllerManagerMetrics, error) {
	if !g.grabFromControllerManager {
		return ControllerManagerMetrics{}, fmt.Errorf("kube-controller-manager: %w", MetricsGrabbingDisabledError)
	}

	var err error

	g.waitForControllerManagerReadyOnce.Do(func() {
		if readyErr := e2epod.WaitTimeoutForPodReadyInNamespace(ctx, g.client, g.kubeControllerManager, metav1.NamespaceSystem, 5*time.Minute); readyErr != nil {
			err = fmt.Errorf("error waiting for kube-controller-manager pod to be ready: %w", readyErr)
		}
	})
	if err != nil {
		return ControllerManagerMetrics{}, err
	}

	var output string
	var lastMetricsFetchErr error
	if metricsWaitErr := wait.PollUntilContextTimeout(ctx, time.Second, time.Minute, true, func(ctx context.Context) (bool, error) {
		output, lastMetricsFetchErr = g.getSecureMetricsFromPod(ctx, g.kubeControllerManager, metav1.NamespaceSystem, kubeControllerManagerPort)
		return lastMetricsFetchErr == nil, nil
	}); metricsWaitErr != nil {
		err := fmt.Errorf("error waiting for kube-controller-manager to expose metrics: %v; %v", metricsWaitErr, lastMetricsFetchErr)
		return ControllerManagerMetrics{}, err
	}

	return parseControllerManagerMetrics(output)
}

// GrabFromSnapshotController 从 snapshot controller 采集指标。
func (g *Grabber) GrabFromSnapshotController(ctx context.Context, podName string, port int) (SnapshotControllerMetrics, error) {
	if !g.grabFromSnapshotController {
		return SnapshotControllerMetrics{}, fmt.Errorf("volume-snapshot-controller: %w", MetricsGrabbingDisabledError)
	}

	// 测试配置 flag 提供了覆盖值就使用它们，
	// 否则采用 volume-snapshot-controller 默认 pod 名和端口。
	if podName == "" {
		podName = g.snapshotController
	}
	if port == 0 {
		port = snapshotControllerPort
	}

	var err error

	g.waitForSnapshotControllerReadyOnce.Do(func() {
		if readyErr := e2epod.WaitTimeoutForPodReadyInNamespace(ctx, g.client, podName, metav1.NamespaceSystem, 5*time.Minute); readyErr != nil {
			err = fmt.Errorf("error waiting for volume-snapshot-controller pod to be ready: %w", readyErr)
		}
	})
	if err != nil {
		return SnapshotControllerMetrics{}, err
	}

	var output string
	var lastMetricsFetchErr error
	if metricsWaitErr := wait.PollUntilContextTimeout(ctx, time.Second, time.Minute, true, func(ctx context.Context) (bool, error) {
		output, lastMetricsFetchErr = g.getMetricsFromPod(ctx, g.client, podName, metav1.NamespaceSystem, port)
		return lastMetricsFetchErr == nil, nil
	}); metricsWaitErr != nil {
		err = fmt.Errorf("error waiting for volume-snapshot-controller pod to expose metrics: %v; %v", metricsWaitErr, lastMetricsFetchErr)
		return SnapshotControllerMetrics{}, err
	}

	return parseSnapshotControllerMetrics(output)
}

// GrabFromAPIServer 从 API server 采集指标。
func (g *Grabber) GrabFromAPIServer(ctx context.Context) (APIServerMetrics, error) {
	output, err := g.getMetricsFromAPIServer(ctx)
	if err != nil {
		return APIServerMetrics{}, err
	}
	return parseAPIServerMetrics(output)
}

// GrabMetricsSLIsFromAPIServer 从 API server 采集 SLI 指标。
func (g *Grabber) GrabMetricsSLIsFromAPIServer(ctx context.Context) (APIServerMetrics, error) {
	output, err := g.getMetricsSLIsFromAPIServer(ctx)
	if err != nil {
		return APIServerMetrics{}, err
	}
	return parseAPIServerMetrics(output)
}

func (g *Grabber) getMetricsFromAPIServer(ctx context.Context) (string, error) {
	rawOutput, err := g.client.CoreV1().RESTClient().Get().RequestURI("/metrics").Do(ctx).Raw()
	if err != nil {
		return "", err
	}
	return string(rawOutput), nil
}

func (g *Grabber) getMetricsSLIsFromAPIServer(ctx context.Context) (string, error) {
	rawOutput, err := g.client.CoreV1().RESTClient().Get().RequestURI("/metrics/slis").Do(ctx).Raw()
	if err != nil {
		return "", err
	}
	return string(rawOutput), nil
}

// Grab 从对应的各个组件采集指标。
func (g *Grabber) Grab(ctx context.Context) (Collection, error) {
	result := Collection{}
	var errs []error
	if g.grabFromAPIServer {
		metrics, err := g.GrabFromAPIServer(ctx)
		if err != nil {
			errs = append(errs, err)
		} else {
			result.APIServerMetrics = metrics
		}
		metrics, err = g.GrabMetricsSLIsFromAPIServer(ctx)
		if err != nil {
			errs = append(errs, err)
		} else {
			result.APIServerMetricsSLIs = metrics
		}
	}
	if g.grabFromScheduler {
		metrics, err := g.GrabFromScheduler(ctx)
		if err != nil {
			errs = append(errs, err)
		} else {
			result.SchedulerMetrics = metrics
		}
	}
	if g.grabFromControllerManager {
		metrics, err := g.GrabFromControllerManager(ctx)
		if err != nil {
			errs = append(errs, err)
		} else {
			result.ControllerManagerMetrics = metrics
		}
	}
	if g.grabFromSnapshotController {
		metrics, err := g.GrabFromSnapshotController(ctx, g.snapshotController, snapshotControllerPort)
		if err != nil {
			errs = append(errs, err)
		} else {
			result.SnapshotControllerMetrics = metrics
		}
	}
	if g.grabFromClusterAutoscaler {
		metrics, err := g.GrabFromClusterAutoscaler(ctx)
		if err != nil {
			errs = append(errs, err)
		} else {
			result.ClusterAutoscalerMetrics = metrics
		}
	}
	if g.grabFromKubelets {
		result.KubeletMetrics = make(map[string]KubeletMetrics)
		nodes, err := g.client.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
		if err != nil {
			errs = append(errs, err)
		} else {
			for _, node := range nodes.Items {
				kubeletPort := node.Status.DaemonEndpoints.KubeletEndpoint.Port
				metrics, err := g.grabFromKubeletInternal(ctx, node.Name, int(kubeletPort), "metrics")
				if err != nil {
					errs = append(errs, err)
				}
				result.KubeletMetrics[node.Name] = metrics
			}
		}
	}
	if len(errs) > 0 {
		return result, fmt.Errorf("Errors while grabbing metrics: %v", errs)
	}
	return result, nil
}

// getMetricsFromPod 从非安全端口获取指标数据。
func (g *Grabber) getMetricsFromPod(ctx context.Context, client clientset.Interface, podName string, namespace string, port int) (string, error) {
	rawOutput, err := client.CoreV1().RESTClient().Get().
		Namespace(namespace).
		Resource("pods").
		SubResource("proxy").
		Name(fmt.Sprintf("%s:%d", podName, port)).
		Suffix("metrics").
		Do(ctx).Raw()
	if err != nil {
		return "", err
	}
	return string(rawOutput), nil
}

// getSecureMetricsFromPod 从使用 TLS 并校验客户端凭据的 pod 获取指标。
// 概念上相当于 "kubectl port-forward" + "kubectl get --raw
// https://localhost:<port>/metrics"。它使用与 kubelet 相同的凭据。
func (g *Grabber) getSecureMetricsFromPod(ctx context.Context, podName string, namespace string, port int) (string, error) {
	dialer := e2epod.NewDialer(g.client, g.config)
	metricConfig := rest.CopyConfig(g.config)
	addr := e2epod.Addr{
		Namespace: namespace,
		PodName:   podName,
		Port:      port,
	}
	metricConfig.Dial = func(ctx context.Context, network, address string) (net.Conn, error) {
		return dialer.DialContainerPort(ctx, addr)
	}
	// 按理说这样就能校验服务端，但实际只是通过了服务器名称检查，
	// 证书校验仍然失败。
	metricConfig.Host = addr.String()
	metricConfig.ServerName = "localhost"
	// 用 API server 的 root CA 校验 pod 证书会报 "unknown root
	// certificate"。在客户端关闭证书校验可以绕开这个问题，
	// 对 E2E 测试来说足够了。
	metricConfig.Insecure = true
	metricConfig.CAFile = ""
	metricConfig.CAData = nil

	// 使用 clientset.NewForConfig 是因为它构造出的 metricClient.RESTClient()
	// 可以直接用，而 rest.RESTClientFor() 构造的 client 不行。
	metricClient, err := clientset.NewForConfig(metricConfig)
	if err != nil {
		return "", err
	}

	rawOutput, err := metricClient.RESTClient().Get().
		AbsPath("metrics").
		Do(ctx).Raw()
	if err != nil {
		return "", err
	}
	return string(rawOutput), nil
}
