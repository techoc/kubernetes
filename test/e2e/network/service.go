/*
Copyright 2016 The Kubernetes Authors.

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

package network

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"net"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	utilnet "k8s.io/apimachinery/pkg/util/net"
	utilrand "k8s.io/apimachinery/pkg/util/rand"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/wait"
	watch "k8s.io/apimachinery/pkg/watch"
	admissionapi "k8s.io/pod-security-admission/api"

	"k8s.io/apimachinery/pkg/util/resourceversion"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
	watchtools "k8s.io/client-go/tools/watch"
	"k8s.io/client-go/util/retry"

	cloudprovider "k8s.io/cloud-provider"
	"k8s.io/utils/ptr"

	podutil "k8s.io/kubernetes/pkg/api/v1/pod"
	apimachineryutils "k8s.io/kubernetes/test/e2e/common/apimachinery"
	"k8s.io/kubernetes/test/e2e/framework"
	e2edeployment "k8s.io/kubernetes/test/e2e/framework/deployment"
	e2eendpointslice "k8s.io/kubernetes/test/e2e/framework/endpointslice"
	e2enetwork "k8s.io/kubernetes/test/e2e/framework/network"
	e2enode "k8s.io/kubernetes/test/e2e/framework/node"
	e2epod "k8s.io/kubernetes/test/e2e/framework/pod"
	e2eoutput "k8s.io/kubernetes/test/e2e/framework/pod/output"
	e2eservice "k8s.io/kubernetes/test/e2e/framework/service"
	e2eskipper "k8s.io/kubernetes/test/e2e/framework/skipper"
	"k8s.io/kubernetes/test/e2e/network/common"
	imageutils "k8s.io/kubernetes/test/utils/image"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
)

const (
	defaultServeHostnameServicePort = 80
	defaultServeHostnameServiceName = "svc-hostname"

	// AffinityTimeout 是 CheckAffinity 允许的最长耗时；既要足够长，
	// 让繁忙 CI 集群中 AffinityConfirmCount 次 HTTP 请求能够完成，
	// 又不能太长，因为在预期无 affinity 的测试中必须一直等到超时。
	AffinityTimeout = 2 * time.Minute

	// AffinityConfirmCount 是确认 affinity 生效所需的连续请求次数。
	AffinityConfirmCount = 15

	// SessionAffinityTimeout 是两次请求之间等待 session affinity 超时的秒数，
	// 超时后再尝试一次 load-balancer 请求。
	SessionAffinityTimeout = 125

	// 用于查找 kube-proxy 和 kube-apiserver pod 的 label
	kubeProxyLabelName     = "kube-proxy"
	clusterAddonLabelKey   = "k8s-app"
	kubeAPIServerLabelName = "kube-apiserver"
	clusterComponentKey    = "component"

	svcReadyTimeout = 1 * time.Minute
)

var (
	defaultServeHostnameService = v1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name: defaultServeHostnameServiceName,
		},
		Spec: v1.ServiceSpec{
			Ports: []v1.ServicePort{{
				Port:       int32(defaultServeHostnameServicePort),
				TargetPort: intstr.FromInt32(9376),
				Protocol:   v1.ProtocolTCP,
			}},
			Selector: map[string]string{
				"name": defaultServeHostnameServiceName,
			},
		},
	}
)

// affinityCheckFromPod 返回轮询间隔、超时时间，以及从 execPod 访问 Service 并收集响应 host 的函数。
func affinityCheckFromPod(execPod *v1.Pod, serviceIP string, servicePort int) (time.Duration, time.Duration, func() []string) {
	timeout := AffinityTimeout
	// 按每次连接最多 2 秒计算间隔
	interval := 2 * AffinityConfirmCount * time.Second

	serviceIPPort := net.JoinHostPort(serviceIP, strconv.Itoa(servicePort))
	curl := fmt.Sprintf(`curl -q -s --connect-timeout 2 --max-time 60 http://%s/`, serviceIPPort)
	cmd := fmt.Sprintf("for i in $(seq 0 %d); do echo; %s ; done", AffinityConfirmCount, curl)
	getHosts := func() []string {
		stdout, err := e2eoutput.RunHostCmd(execPod.Namespace, execPod.Name, cmd)
		if err != nil {
			framework.Logf("Failed to get response from %s. Retry until timeout", serviceIPPort)
			return nil
		}
		return strings.Split(stdout, "\n")
	}

	return interval, timeout, getHosts
}

// affinityCheckFromTest 返回轮询间隔、超时时间，以及从测试进程自身访问 Service 并收集响应 host 的函数。
func affinityCheckFromTest(ctx context.Context, cs clientset.Interface, serviceIP string, servicePort int) (time.Duration, time.Duration, func() []string) {
	interval := 2 * time.Second
	timeout := e2eservice.GetServiceLoadBalancerPropagationTimeout(ctx, cs)

	params := &e2enetwork.HTTPPokeParams{Timeout: 2 * time.Second}
	getHosts := func() []string {
		var hosts []string
		for range AffinityConfirmCount {
			result := e2enetwork.PokeHTTP(serviceIP, servicePort, "", params)
			if result.Status == e2enetwork.HTTPSuccess {
				hosts = append(hosts, string(result.Body))
			}
		}
		return hosts
	}

	return interval, timeout, getHosts
}

// checkAffinity 检查 Service 的 affinity 是否按预期工作。
// 预期有 affinity 时，连续观察到 affinityConfirmCount 次相同响应即返回 true；
// 预期无 affinity 时，则持续观察直到出现不同的响应。
// 只有发生意外错误时才会返回 false。
func checkAffinity(ctx context.Context, cs clientset.Interface, execPod *v1.Pod, serviceIP string, servicePort int, shouldHold bool) bool {
	var interval, timeout time.Duration
	var getHosts func() []string
	if execPod != nil {
		interval, timeout, getHosts = affinityCheckFromPod(execPod, serviceIP, servicePort)
	} else {
		interval, timeout, getHosts = affinityCheckFromTest(ctx, cs, serviceIP, servicePort)
	}

	var tracker affinityTracker
	if pollErr := wait.PollImmediate(interval, timeout, func() (bool, error) {
		hosts := getHosts()
		for _, host := range hosts {
			if len(host) > 0 {
				tracker.recordHost(strings.TrimSpace(host))
			}
		}

		trackerFulfilled, affinityHolds := tracker.checkHostTrace(AffinityConfirmCount)
		if !trackerFulfilled {
			return false, nil
		}

		if !shouldHold && !affinityHolds {
			return true, nil
		}
		if shouldHold && affinityHolds {
			return true, nil
		}
		return false, nil
	}); pollErr != nil {
		trackerFulfilled, _ := tracker.checkHostTrace(AffinityConfirmCount)
		if !wait.Interrupted(pollErr) {
			checkAffinityFailed(tracker, pollErr.Error())
			return false
		}
		if !trackerFulfilled {
			checkAffinityFailed(tracker, "Connection timed out or not enough responses.")
		}
		if shouldHold {
			checkAffinityFailed(tracker, "Affinity should hold but didn't.")
		} else {
			checkAffinityFailed(tracker, "Affinity shouldn't hold but did.")
		}
		return true
	}
	return true
}

// affinityTracker 在 affinity 测试中记录请求到达的目标。
type affinityTracker struct {
	hostTrace []string
}

// 记录来自指定 host 的响应。
func (at *affinityTracker) recordHost(host string) {
	at.hostTrace = append(at.hostTrace, host)
	framework.Logf("Received response from host: %s", host)
}

// 检查最近count次请求是否都落到同一个 host。
func (at *affinityTracker) checkHostTrace(count int) (fulfilled, affinityHolds bool) {
	fulfilled = (len(at.hostTrace) >= count)
	if len(at.hostTrace) == 0 {
		return fulfilled, true
	}
	last := at.hostTrace[0:]
	if len(at.hostTrace)-count >= 0 {
		last = at.hostTrace[len(at.hostTrace)-count:]
	}
	host := at.hostTrace[len(at.hostTrace)-1]
	for _, h := range last {
		if h != host {
			return fulfilled, false
		}
	}
	return fulfilled, true
}

func checkAffinityFailed(tracker affinityTracker, err string) {
	framework.Logf("%v", tracker.hostTrace)
	framework.Fail(err)
}

// StartServeHostnameService 创建一个返回自身 hostname 的 Deployment，
// 并在其上创建对应的 Service。
func StartServeHostnameService(ctx context.Context, c clientset.Interface, svc *v1.Service, ns string, replicas int) ([]string, string, error) {
	podNames := make([]string, replicas)
	name := svc.ObjectMeta.Name
	ginkgo.By("creating service " + name + " in namespace " + ns)
	_, err := c.CoreV1().Services(ns).Create(ctx, svc, metav1.CreateOptions{})
	if err != nil {
		return podNames, "", err
	}

	deploymentConfig := e2edeployment.NewDeployment(name,
		int32(replicas),
		map[string]string{"name": name},
		name,
		imageutils.GetE2EImage(imageutils.Agnhost),
		appsv1.RecreateDeploymentStrategyType)
	deploymentConfig.Spec.Template.Spec.Containers[0].Command = []string{"/agnhost", "serve-hostname"}
	deployment, err := c.AppsV1().Deployments(ns).Create(ctx, deploymentConfig, metav1.CreateOptions{})
	framework.ExpectNoError(err, "failed to create deployment %s in namespace %s", name, ns)

	err = e2edeployment.WaitForDeploymentComplete(c, deployment)
	framework.ExpectNoError(err, "failed to wait for deployment %s in namespace %s", name, ns)

	pods, err := e2edeployment.GetPodsForDeployment(ctx, c, deployment)
	framework.ExpectNoError(err, "failed to get pods for deployment %s in namespace %s", name, ns)

	if len(pods.Items) != replicas {
		return podNames, "", fmt.Errorf("incorrect number of running pods: %v", len(pods.Items))
	}

	for i := range pods.Items {
		podNames[i] = pods.Items[i].ObjectMeta.Name
	}
	sort.StringSlice(podNames).Sort()

	service, err := c.CoreV1().Services(ns).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return podNames, "", err
	}
	if service.Spec.ClusterIP == "" {
		return podNames, "", fmt.Errorf("service IP is blank for %v", name)
	}
	serviceIP := service.Spec.ClusterIP
	return podNames, serviceIP, nil
}

// StopServeHostnameService 删除指定的 Service 及其 Deployment。
func StopServeHostnameService(ctx context.Context, clientset clientset.Interface, ns, name string) error {
	if err := clientset.AppsV1().Deployments(ns).Delete(ctx, name, metav1.DeleteOptions{}); err != nil {
		return err
	}
	if err := clientset.CoreV1().Services(ns).Delete(ctx, name, metav1.DeleteOptions{}); err != nil {
		return err
	}
	return nil
}

// verifyServeHostnameServiceUp 分别从 hostNetwork 的 host exec pod 和普通容器网络的 exec pod
// 访问指定的 serviceIP:servicePort。Service 中的每个 pod 都应返回自身名称，
// 去重排序后与 expectedPods 列表比对。
func verifyServeHostnameServiceUp(ctx context.Context, c clientset.Interface, ns string, expectedPods []string, serviceIP string, servicePort int) error {
	// 从宿主机网络验证
	hostExecPod := launchHostExecPod(ctx, c, ns, "verify-service-up-host-exec-pod", nil)

	// 从容器网络验证
	execPod := e2epod.CreateExecPodOrFail(ctx, c, ns, "verify-service-up-exec-pod-", nil)
	defer func() {
		e2epod.DeletePodOrFail(ctx, c, ns, hostExecPod.Name)
		e2epod.DeletePodOrFail(ctx, c, ns, execPod.Name)
	}()

	// 从 pod 内部验证 Service
	cmdFunc := func(podName string) string {
		wgetCmd := "wget -q -O -"
		// Windows 镜像中的 wget 可能不支持 -T 选项
		if !framework.NodeOSDistroIs("windows") {
			wgetCmd += " -T 1"
		}
		serviceIPPort := net.JoinHostPort(serviceIP, strconv.Itoa(servicePort))
		cmd := fmt.Sprintf("for i in $(seq 1 %d); do %s http://%s 2>&1 || true; echo; done",
			50*len(expectedPods), wgetCmd, serviceIPPort)
		framework.Logf("Executing cmd %q in pod %v/%v", cmd, ns, podName)
		// TODO: 改为通过 netexec pod 的 exec-over-http，替代 kubectl exec。
		output, err := e2eoutput.RunHostCmd(ns, podName, cmd)
		if err != nil {
			framework.Logf("error while kubectl execing %q in pod %v/%v: %v\nOutput: %v", cmd, ns, podName, err, output)
		}
		return output
	}

	expectedEndpoints := sets.NewString(expectedPods...)
	ginkgo.By(fmt.Sprintf("verifying service has %d reachable backends", len(expectedPods)))
	for _, podName := range []string{hostExecPod.Name, execPod.Name} {
		passed := false
		gotEndpoints := sets.NewString()

		// 在一段时间内重试 cmdFunc
		for start := time.Now(); time.Since(start) < e2eservice.KubeProxyLagTimeout; time.Sleep(5 * time.Second) {
			for _, endpoint := range strings.Split(cmdFunc(podName), "\n") {
				trimmedEp := strings.TrimSpace(endpoint)
				if trimmedEp != "" {
					gotEndpoints.Insert(trimmedEp)
				}
			}
			// TODO: 只检查实际 endpoint 是否为期望集合的超集，可以忽略
			// "wget timed out" 之类的偶发网络抖动，但这种情况本应很少，
			// 需要有更好的方式统计其发生频率。
			if gotEndpoints.IsSuperset(expectedEndpoints) {
				if !gotEndpoints.Equal(expectedEndpoints) {
					framework.Logf("Ignoring unexpected output wgetting endpoints of service %s: %v", serviceIP, gotEndpoints.Difference(expectedEndpoints))
				}
				passed = true
				break
			}
			framework.Logf("Unable to reach the following endpoints of service %s: %v", serviceIP, expectedEndpoints.Difference(gotEndpoints))
		}
		if !passed {
			// 排序后更容易肉眼比对差异。
			exp := expectedEndpoints.List()
			got := gotEndpoints.List()
			sort.StringSlice(exp).Sort()
			sort.StringSlice(got).Sort()
			return fmt.Errorf("service verification failed for: %s\nexpected %v\nreceived %v", serviceIP, exp, got)
		}
	}
	return nil
}

// verifyServeHostnameServiceDown 验证指定 Service 已不再提供服务。
func verifyServeHostnameServiceDown(ctx context.Context, c clientset.Interface, ns string, serviceIP string, servicePort int) error {
	// 从宿主机网络验证
	hostExecPod := launchHostExecPod(ctx, c, ns, "verify-service-down-host-exec-pod", nil)
	defer func() {
		e2epod.DeletePodOrFail(ctx, c, ns, hostExecPod.Name)
	}()

	ipPort := net.JoinHostPort(serviceIP, strconv.Itoa(servicePort))
	// CentOS 和 RHEL 发行版自带的 curl 会把 IPv6 地址外的方括号误当作 glob 模式，
	// 因此用 -g 关闭 globbing 以正确处理 IPv6。
	command := fmt.Sprintf(
		"curl -g -s --connect-timeout 2 http://%s && echo service-down-failed", ipPort)

	for start := time.Now(); time.Since(start) < e2eservice.KubeProxyLagTimeout; time.Sleep(5 * time.Second) {
		output, err := e2eoutput.RunHostCmd(ns, hostExecPod.Name, command)
		if err != nil {
			framework.Logf("error while kubectl execing %q in pod %v/%v: %v\nOutput: %v", command, ns, hostExecPod.Name, err, output)
		}
		if !strings.Contains(output, "service-down-failed") {
			return nil
		}
		framework.Logf("service still alive - still waiting")
	}

	return fmt.Errorf("waiting for service to be down timed out")
}

// testNotReachableHTTP 检查发往给定 host 和 port 的 HTTP 请求无法建立连接。
func testNotReachableHTTP(ctx context.Context, host string, port int, timeout time.Duration) {
	pollfn := func(ctx context.Context) (bool, error) {
		result := e2enetwork.PokeHTTP(host, port, "/", nil)
		if result.Code == 0 {
			return true, nil
		}
		return false, nil // 调用方可以重试
	}

	if err := wait.PollUntilContextTimeout(ctx, framework.Poll, timeout, true, pollfn); err != nil {
		framework.Failf("HTTP service %v:%v reachable after %v: %v", host, port, timeout, err)
	}
}

// UDPPokeParams 定义 UDP 探测的参数。
type UDPPokeParams struct {
	Timeout  time.Duration
	Response string
}

// UDPPokeResult 保存 UDP 探测的结果。
type UDPPokeResult struct {
	Status   UDPPokeStatus
	Error    error  // 发生错误时填写
	Response []byte // code != 0 时填写
}

// UDPPokeStatus 表示 UDP 探测的状态。
type UDPPokeStatus string

const (
	// UDPSuccess 表示探测成功。
	UDPSuccess UDPPokeStatus = "Success"
	// UDPError 表示探测发生错误。
	UDPError UDPPokeStatus = "UnknownError"
	// UDPTimeout 表示探测超时。
	UDPTimeout UDPPokeStatus = "TimedOut"
	// UDPRefused 表示连接被拒绝。
	UDPRefused UDPPokeStatus = "ConnectionRefused"
	// UDPBadResponse 表示收到异常响应。
	UDPBadResponse UDPPokeStatus = "BadResponse"
	// 新增错误状态时，必须检查所有调用方是否需要同步处理。
)

// PokeUDP 尝试连接指定 host 的 port 并发送请求，调用方可按需指定额外的成功判定参数。
//
// 状态会结合已知调用方的需要，尽可能精确地分类。
//
// 除 Success 之外的状态都会填写 error。
//
// 只要 UDP 交互完成，就会填写 response，即使其他判定参数使本次探测被视为失败。
func PokeUDP(host string, port int, request string, params *UDPPokeParams) UDPPokeResult {
	hostPort := net.JoinHostPort(host, strconv.Itoa(port))
	url := fmt.Sprintf("udp://%s", hostPort)

	ret := UDPPokeResult{}

	// 输入确实出现过非法的情况，这里必须先校验。
	// 只有这些情况会直接让测试失败，相当于 ASSERT()。
	if host == "" {
		framework.Failf("Got empty host for UDP poke (%s)", url)
		return ret
	}
	if port == 0 {
		framework.Failf("Got port==0 for UDP poke (%s)", url)
		return ret
	}

	// 设置默认参数。
	if params == nil {
		params = &UDPPokeParams{}
	}

	framework.Logf("Poking %v", url)

	con, err := net.Dial("udp", hostPort)
	if err != nil {
		ret.Status = UDPError
		ret.Error = err
		framework.Logf("Poke(%q): %v", url, err)
		return ret
	}

	_, err = con.Write([]byte(fmt.Sprintf("%s\n", request)))
	if err != nil {
		ret.Error = err
		var neterr net.Error
		if errors.As(err, &neterr) && neterr.Timeout() {
			ret.Status = UDPTimeout
		} else if strings.Contains(err.Error(), "connection refused") {
			ret.Status = UDPRefused
		} else {
			ret.Status = UDPError
		}
		framework.Logf("Poke(%q): %v", url, err)
		return ret
	}

	if params.Timeout != 0 {
		err = con.SetDeadline(time.Now().Add(params.Timeout))
		if err != nil {
			ret.Status = UDPError
			ret.Error = err
			framework.Logf("Poke(%q): %v", url, err)
			return ret
		}
	}

	bufsize := len(params.Response) + 1
	if bufsize == 0 {
		bufsize = 4096
	}
	var buf = make([]byte, bufsize)
	n, err := con.Read(buf)
	if err != nil {
		ret.Error = err
		var neterr net.Error
		if errors.As(err, &neterr) && neterr.Timeout() {
			ret.Status = UDPTimeout
		} else if strings.Contains(err.Error(), "connection refused") {
			ret.Status = UDPRefused
		} else {
			ret.Status = UDPError
		}
		framework.Logf("Poke(%q): %v", url, err)
		return ret
	}
	ret.Response = buf[0:n]

	if params.Response != "" && string(ret.Response) != params.Response {
		ret.Status = UDPBadResponse
		ret.Error = fmt.Errorf("response does not match expected string: %q", string(ret.Response))
		framework.Logf("Poke(%q): %v", url, ret.Error)
		return ret
	}

	ret.Status = UDPSuccess
	framework.Logf("Poke(%q): success", url)
	return ret
}

// testReachableUDP 检查给定 host 在指定 port 上提供 UDP 服务。
func testReachableUDP(ctx context.Context, host string, port int, timeout time.Duration) {
	pollfn := func(ctx context.Context) (bool, error) {
		result := PokeUDP(host, port, "echo hello", &UDPPokeParams{
			Timeout:  3 * time.Second,
			Response: "hello",
		})
		if result.Status == UDPSuccess {
			return true, nil
		}
		return false, nil // 调用方可以重试
	}

	if err := wait.PollUntilContextTimeout(ctx, framework.Poll, timeout, true, pollfn); err != nil {
		framework.Failf("Could not reach UDP service through %v:%v after %v: %v", host, port, timeout, err)
	}
}

// testNotReachableUDP 检查给定 host 在指定 port 上不提供 UDP 服务。
func testNotReachableUDP(ctx context.Context, host string, port int, timeout time.Duration) {
	pollfn := func(ctx context.Context) (bool, error) {
		result := PokeUDP(host, port, "echo hello", &UDPPokeParams{Timeout: 3 * time.Second})
		if result.Status != UDPSuccess && result.Status != UDPError {
			return true, nil
		}
		return false, nil // 调用方可以重试
	}
	if err := wait.PollUntilContextTimeout(ctx, framework.Poll, timeout, true, pollfn); err != nil {
		framework.Failf("UDP service %v:%v reachable after %v: %v", host, port, timeout, err)
	}
}

// TestHTTPHealthCheckNodePort 用指定请求测试到给定 host 和 port 的 HTTP 连接。
func TestHTTPHealthCheckNodePort(ctx context.Context, host string, port int, request string, timeout time.Duration, expectSucceed bool, threshold int) error {
	count := 0
	condition := func(ctx context.Context) (bool, error) {
		success, _ := testHTTPHealthCheckNodePort(host, port, request)
		if success && expectSucceed ||
			!success && !expectSucceed {
			count++
		}
		if count >= threshold {
			return true, nil
		}
		return false, nil
	}

	if err := wait.PollUntilContextTimeout(ctx, time.Second, timeout, true, condition); err != nil {
		return fmt.Errorf("error waiting for healthCheckNodePort: expected at least %d succeed=%v on %v%v, got %d", threshold, expectSucceed, host, port, count)
	}
	return nil
}

func testHTTPHealthCheckNodePort(ip string, port int, request string) (bool, error) {
	ipPort := net.JoinHostPort(ip, strconv.Itoa(port))
	url := fmt.Sprintf("http://%s%s", ipPort, request)
	if ip == "" || port == 0 {
		framework.Failf("Got empty IP for reachability check (%s)", url)
		return false, fmt.Errorf("invalid input ip or port")
	}
	framework.Logf("Testing HTTP health check on %v", url)
	resp, err := httpGetNoConnectionPoolTimeout(url, 5*time.Second)
	if err != nil {
		framework.Logf("Got error testing for reachability of %s: %v", url, err)
		return false, err
	}
	defer func() { _ = resp.Body.Close() }()
	if err != nil {
		framework.Logf("Got error reading response from %s: %v", url, err)
		return false, err
	}
	// HealthCheck responder 在没有本地 endpoint 时返回 503
	if resp.StatusCode == 503 {
		return false, nil
	}
	// HealthCheck responder 在有本地 endpoint 时返回 200
	if resp.StatusCode == 200 {
		return true, nil
	}
	return false, fmt.Errorf("unexpected HTTP response code %s from health check responder at %s", resp.Status, url)
}

func testHTTPHealthCheckNodePortFromTestContainer(ctx context.Context, config *e2enetwork.NetworkingTestConfig, host string, port int, timeout time.Duration, expectSucceed bool, threshold int) error {
	count := 0
	pollFn := func(ctx context.Context) (bool, error) {
		statusCode, err := config.GetHTTPCodeFromTestContainer(ctx,
			"/healthz",
			host,
			port)
		if err != nil {
			framework.Logf("Got error reading status code from http://%s:%d/healthz via test container: %v", host, port, err)
			return false, nil
		}
		framework.Logf("Got status code from http://%s:%d/healthz via test container: %d", host, port, statusCode)
		success := statusCode == 200
		if (success && expectSucceed) ||
			(!success && !expectSucceed) {
			count++
		}
		return count >= threshold, nil
	}
	err := wait.PollUntilContextTimeout(ctx, time.Second, timeout, true, pollFn)
	if err != nil {
		return fmt.Errorf("error waiting for healthCheckNodePort: expected at least %d succeed=%v on %v:%v/healthz, got %d", threshold, expectSucceed, host, port, count)
	}
	return nil
}

// 执行 HTTP GET，但不复用 TCP 连接。
// 复用连接会掩盖 iptables 规则已变化但未被观察到的问题。
func httpGetNoConnectionPoolTimeout(url string, timeout time.Duration) (*http.Response, error) {
	tr := utilnet.SetTransportDefaults(&http.Transport{
		DisableKeepAlives: true,
	})
	client := &http.Client{
		Transport: tr,
		Timeout:   timeout,
	}
	return client.Get(url)
}

func getServeHostnameService(name string) *v1.Service {
	svc := defaultServeHostnameService.DeepCopy()
	svc.ObjectMeta.Name = name
	svc.Spec.Selector["name"] = name
	return svc
}

// waitForAPIServerUp 等待 kube-apiserver 启动完成。
func waitForAPIServerUp(ctx context.Context, c clientset.Interface) error {
	for start := time.Now(); time.Since(start) < time.Minute; time.Sleep(5 * time.Second) {
		body, err := c.CoreV1().RESTClient().Get().AbsPath("/healthz").Do(ctx).Raw()
		if err == nil && string(body) == "ok" {
			return nil
		}
	}
	return fmt.Errorf("waiting for apiserver timed out")
}

// getEndpointNodesWithInternalIP 返回 Service endpoint 所在节点的 nodename:internal-ip 映射。
func getEndpointNodesWithInternalIP(ctx context.Context, jig *e2eservice.TestJig) (map[string]string, error) {
	nodes, err := jig.ListNodesWithEndpoint(ctx)
	if err != nil {
		return nil, err
	}
	endpointsNodeMap := make(map[string]string, len(nodes))
	for _, node := range nodes {
		internalIPs := e2enode.GetAddresses(&node, v1.NodeInternalIP)
		if len(internalIPs) < 1 {
			return nil, fmt.Errorf("no internal ip found for node %s", node.Name)
		}
		endpointsNodeMap[node.Name] = internalIPs[0]
	}
	return endpointsNodeMap, nil
}

var _ = common.SIGDescribe("Services", func() {
	f := framework.NewDefaultFramework("services")
	f.NamespacePodSecurityLevel = admissionapi.LevelPrivileged

	var cs clientset.Interface

	ginkgo.BeforeEach(func() {
		cs = f.ClientSet
	})

	// TODO: TCP/UDP 和多端口 Service 已通过 DNS 测试覆盖，这里应该补一个更简单的多端口 TCP 测试。

	/*
		Release: v1.9
		Testname: Service, endpoints
		Description: 创建一个没有 Pod 的 Service，该 Service MUST 正常运行且 endpoint 为空。向 Service 添加一个 Pod，Service MUST 展示该 Pod 暴露端口对应的所有 endpoint。再添加第二个 Pod，两个 Pod 暴露的所有端口列表 MUST 有效且有对应的 Service endpoint。删除第二个 Pod 后，endpoint 集合 MUST 只展示第一个容器暴露的端口。两个 Pod 都删除后，Service 的 endpoint MUST 为空。
	*/
	framework.ConformanceIt("should serve a basic endpoint from pods", func(ctx context.Context) {
		serviceName := "endpoint-test2"
		ns := f.Namespace.Name
		jig := e2eservice.NewTestJig(cs, ns, serviceName)

		ginkgo.By("creating service " + serviceName + " in namespace " + ns)
		ginkgo.DeferCleanup(func(ctx context.Context) {
			err := cs.CoreV1().Services(ns).Delete(ctx, serviceName, metav1.DeleteOptions{})
			framework.ExpectNoError(err, "failed to delete service: %s in namespace: %s", serviceName, ns)
		})
		svc, err := jig.CreateTCPServiceWithPort(ctx, nil, 80)
		framework.ExpectNoError(err)

		err = e2eendpointslice.WaitForEndpointCount(ctx, cs, ns, serviceName, 0)
		framework.ExpectNoError(err)

		names := map[string]bool{}
		ginkgo.DeferCleanup(func(ctx context.Context) {
			for name := range names {
				err := cs.CoreV1().Pods(ns).Delete(ctx, name, metav1.DeleteOptions{})
				framework.ExpectNoError(err, "failed to delete pod: %s in namespace: %s", name, ns)
			}
		})

		name1 := "pod1"
		name2 := "pod2"

		ginkgo.By("creating first endpoint pod for service")
		createPodOrFail(ctx, f, ns, name1, jig.Labels, []v1.ContainerPort{{ContainerPort: 80}}, "netexec", "--http-port", "80")
		names[name1] = true
		err = e2eendpointslice.WaitForEndpointPods(ctx, cs, ns, serviceName, name1)
		framework.ExpectNoError(err)

		ginkgo.By("Checking if the Service forwards traffic to pod1")
		execPod := e2epod.CreateExecPodOrFail(ctx, cs, ns, "execpod", nil)
		err = jig.CheckServiceReachability(ctx, svc, execPod)
		framework.ExpectNoError(err)

		ginkgo.By("creating second endpoint pod for service")
		createPodOrFail(ctx, f, ns, name2, jig.Labels, []v1.ContainerPort{{ContainerPort: 80}}, "netexec", "--http-port", "80")
		names[name2] = true
		err = e2eendpointslice.WaitForEndpointPods(ctx, cs, ns, serviceName, name1, name2)
		framework.ExpectNoError(err)

		ginkgo.By("Checking if the Service forwards traffic to pod1 and pod2")
		err = jig.CheckServiceReachability(ctx, svc, execPod)
		framework.ExpectNoError(err)

		ginkgo.By("deleting first endpoint pod")
		e2epod.DeletePodOrFail(ctx, cs, ns, name1)
		delete(names, name1)
		err = e2eendpointslice.WaitForEndpointPods(ctx, cs, ns, serviceName, name2)
		framework.ExpectNoError(err)

		ginkgo.By("Checking if the Service forwards traffic to pod2")
		err = jig.CheckServiceReachability(ctx, svc, execPod)
		framework.ExpectNoError(err)

		ginkgo.By("deleting second endpoint pod")
		e2epod.DeletePodOrFail(ctx, cs, ns, name2)
		delete(names, name2)
		err = e2eendpointslice.WaitForEndpointCount(ctx, cs, ns, serviceName, 0)
		framework.ExpectNoError(err)
	})

	/*
		Release: v1.9
		Testname: Service, endpoints with multiple ports
		Description: 创建有两个端口但尚无 Pod 的 Service，该 Service MUST 正常运行且 endpoint 为空。为第一个端口添加 Pod，Service MUST 列出该 Pod 在此端口上的一个 endpoint。为第二个端口再添加 Pod，Service MUST 列出两个 endpoint。删除第一个 Pod 后，Service MUST 只列出第二个 Pod 的 endpoint。删除第二个 Pod 后，Service 的 endpoint MUST 为空。
	*/
	framework.ConformanceIt("should serve multiport endpoints from pods", func(ctx context.Context) {
		// 这里刻意不测试端口重打包（repacking）功能，放在集成测试中验证更合适。
		serviceName := "multi-endpoint-test"
		ns := f.Namespace.Name
		jig := e2eservice.NewTestJig(cs, ns, serviceName)

		ginkgo.DeferCleanup(func(ctx context.Context) {
			err := cs.CoreV1().Services(ns).Delete(ctx, serviceName, metav1.DeleteOptions{})
			framework.ExpectNoError(err, "failed to delete service: %s in namespace: %s", serviceName, ns)
		})

		svc1port := "svc1"
		svc2port := "svc2"

		ginkgo.By("creating service " + serviceName + " in namespace " + ns)
		svc, err := jig.CreateTCPService(ctx, func(service *v1.Service) {
			service.Spec.Ports = []v1.ServicePort{
				{
					Name:       "portname1",
					Port:       80,
					TargetPort: intstr.FromString(svc1port),
				},
				{
					Name:       "portname2",
					Port:       81,
					TargetPort: intstr.FromString(svc2port),
				},
			}
		})
		framework.ExpectNoError(err)

		port1 := 100
		port2 := 101
		err = e2eendpointslice.WaitForEndpointCount(ctx, cs, ns, serviceName, 0)
		framework.ExpectNoError(err)

		names := map[string]bool{}
		ginkgo.DeferCleanup(func(ctx context.Context) {
			for name := range names {
				err := cs.CoreV1().Pods(ns).Delete(ctx, name, metav1.DeleteOptions{})
				framework.ExpectNoError(err, "failed to delete pod: %s in namespace: %s", name, ns)
			}
		})

		containerPorts1 := []v1.ContainerPort{
			{
				Name:          svc1port,
				ContainerPort: int32(port1),
			},
		}
		containerPorts2 := []v1.ContainerPort{
			{
				Name:          svc2port,
				ContainerPort: int32(port2),
			},
		}

		podname1 := "pod1"
		podname2 := "pod2"

		ginkgo.By("creating pod1 serving port1")
		createPodOrFail(ctx, f, ns, podname1, jig.Labels, containerPorts1, "netexec", "--http-port", strconv.Itoa(port1))
		names[podname1] = true
		err = e2eendpointslice.WaitForEndpointPorts(ctx, cs, ns, serviceName,
			[]e2eendpointslice.PortMapping{
				{Name: "portname1", Target: podname1, TargetPort: int32(port1)},
			},
		)
		framework.ExpectNoError(err)

		ginkgo.By("creating pod2 serving port2")
		createPodOrFail(ctx, f, ns, podname2, jig.Labels, containerPorts2, "netexec", "--http-port", strconv.Itoa(port2))
		names[podname2] = true
		err = e2eendpointslice.WaitForEndpointPorts(ctx, cs, ns, serviceName,
			[]e2eendpointslice.PortMapping{
				{Name: "portname1", Target: podname1, TargetPort: int32(port1)},
				{Name: "portname2", Target: podname2, TargetPort: int32(port2)},
			},
		)
		framework.ExpectNoError(err)

		ginkgo.By("Checking if the Service forwards traffic to pods")
		execPod := e2epod.CreateExecPodOrFail(ctx, cs, ns, "execpod", nil)
		err = jig.CheckServiceReachability(ctx, svc, execPod)
		framework.ExpectNoError(err)

		ginkgo.By("deleting pod1 serving port1")
		e2epod.DeletePodOrFail(ctx, cs, ns, podname1)
		delete(names, podname1)
		err = e2eendpointslice.WaitForEndpointPorts(ctx, cs, ns, serviceName,
			[]e2eendpointslice.PortMapping{
				{Name: "portname2", Target: podname2, TargetPort: int32(port2)},
			},
		)
		framework.ExpectNoError(err)

		ginkgo.By("deleting pod2 serving port2")
		e2epod.DeletePodOrFail(ctx, cs, ns, podname2)
		delete(names, podname2)
		err = e2eendpointslice.WaitForEndpointCount(ctx, cs, ns, serviceName, 0)
		framework.ExpectNoError(err)
	})

	ginkgo.It("should be updated after adding or deleting ports", func(ctx context.Context) {
		serviceName := "edit-port-test"
		ns := f.Namespace.Name
		jig := e2eservice.NewTestJig(cs, ns, serviceName)

		svc1port := "svc1"
		ginkgo.By("creating service " + serviceName + " in namespace " + ns)
		svc, err := jig.CreateTCPService(ctx, func(service *v1.Service) {
			service.Spec.Ports = []v1.ServicePort{
				{
					Name:       "portname1",
					Port:       80,
					TargetPort: intstr.FromString(svc1port),
				},
			}
		})
		framework.ExpectNoError(err)
		err = e2eendpointslice.WaitForEndpointCount(ctx, cs, ns, serviceName, 0)
		framework.ExpectNoError(err)

		ginkgo.By("Creating a pod to serve port1")
		podname1 := "pod1"
		port1 := 100
		containerPorts1 := []v1.ContainerPort{
			{
				Name:          svc1port,
				ContainerPort: int32(port1),
			},
		}
		createPodOrFail(ctx, f, ns, podname1, jig.Labels, containerPorts1, "netexec", "--http-port", strconv.Itoa(port1))

		ginkgo.By("Waiting for an EndpointSlice pointing to port1")
		err = e2eendpointslice.WaitForEndpointPorts(ctx, cs, ns, serviceName,
			[]e2eendpointslice.PortMapping{
				{Name: "portname1", Target: podname1, TargetPort: int32(port1)},
			},
		)
		framework.ExpectNoError(err)

		ginkgo.By("Checking if the Service " + serviceName + " forwards traffic to " + podname1)
		execPod := e2epod.CreateExecPodOrFail(ctx, cs, ns, "execpod", nil)
		err = jig.CheckServiceReachability(ctx, svc, execPod)
		framework.ExpectNoError(err)

		ginkgo.By("Adding a new port to service " + serviceName)
		svc2port := "svc2"
		svc, err = jig.UpdateService(ctx, func(s *v1.Service) {
			s.Spec.Ports = []v1.ServicePort{
				{
					Name:       "portname1",
					Port:       80,
					TargetPort: intstr.FromString(svc1port),
				},
				{
					Name:       "portname2",
					Port:       81,
					TargetPort: intstr.FromString(svc2port),
				},
			}
		})
		framework.ExpectNoError(err)

		ginkgo.By("Creating a pod to serve port2")
		podname2 := "pod2"
		port2 := 101
		containerPorts2 := []v1.ContainerPort{
			{
				Name:          svc2port,
				ContainerPort: int32(port2),
			},
		}
		createPodOrFail(ctx, f, ns, podname2, jig.Labels, containerPorts2, "netexec", "--http-port", strconv.Itoa(port2))

		ginkgo.By("Waiting for EndpointSlices pointing to both ports")
		err = e2eendpointslice.WaitForEndpointPorts(ctx, cs, ns, serviceName,
			[]e2eendpointslice.PortMapping{
				{Name: "portname1", Target: podname1, TargetPort: int32(port1)},
				{Name: "portname2", Target: podname2, TargetPort: int32(port2)},
			},
		)
		framework.ExpectNoError(err)

		ginkgo.By("Checking if the Service forwards traffic to " + podname1 + " and " + podname2)
		err = jig.CheckServiceReachability(ctx, svc, execPod)
		framework.ExpectNoError(err)

		ginkgo.By("Deleting a port from service " + serviceName)
		svc, err = jig.UpdateService(ctx, func(s *v1.Service) {
			s.Spec.Ports = []v1.ServicePort{
				{
					Name:       "portname1",
					Port:       80,
					TargetPort: intstr.FromString(svc1port),
				},
			}
		})
		framework.ExpectNoError(err)

		ginkgo.By("Waiting for the EndpointSlices to only point to the port1")
		err = e2eendpointslice.WaitForEndpointPorts(ctx, cs, ns, serviceName,
			[]e2eendpointslice.PortMapping{
				{Name: "portname1", Target: podname1, TargetPort: int32(port1)},
			},
		)
		framework.ExpectNoError(err)

		ginkgo.By("Checking if the Service forwards traffic to " + podname1 + " and not forwards to " + podname2)
		err = jig.CheckServiceReachability(ctx, svc, execPod)
		framework.ExpectNoError(err)
	})

	// NOTE: 根据 Kubernetes 网络模型的基本要求（https://kubernetes.io/docs/concepts/services-networking/），
	// Pod 无需 NAT 即可与其他节点上的 Pod 通信，因此不应伪装（masquerade）内部 Pod 流量，详见 #126089。
	ginkgo.It("should preserve source pod IP for traffic thru service cluster IP [LinuxOnly]", func(ctx context.Context) {
		// 该测试会创建 HostNetwork=true 的 Pod，Windows 不支持这种配置。
		e2eskipper.SkipIfNodeOSDistroIs("windows")

		serviceName := "sourceip-test"
		ns := f.Namespace.Name

		ginkgo.By("creating a TCP service " + serviceName + " with type=ClusterIP in namespace " + ns)
		jig := e2eservice.NewTestJig(cs, ns, serviceName)
		jig.ExternalIPs = false
		servicePort := 8080
		tcpService, err := jig.CreateTCPServiceWithPort(ctx, nil, int32(servicePort))
		framework.ExpectNoError(err)
		ginkgo.DeferCleanup(func(ctx context.Context) {
			framework.Logf("Cleaning up the sourceip test service")
			err := cs.CoreV1().Services(ns).Delete(ctx, serviceName, metav1.DeleteOptions{})
			framework.ExpectNoError(err, "failed to delete service: %s in namespace: %s", serviceName, ns)
		})
		serviceIP := tcpService.Spec.ClusterIP
		framework.Logf("sourceip-test cluster ip: %s", serviceIP)

		ginkgo.By("Picking 2 Nodes to test whether source IP is preserved or not")
		nodes, err := e2enode.GetBoundedReadySchedulableNodes(ctx, cs, 2)
		framework.ExpectNoError(err)
		nodeCounts := len(nodes.Items)
		if nodeCounts < 2 {
			e2eskipper.Skipf("The test requires at least two ready nodes on %s, but found %v", framework.TestContext.Provider, nodeCounts)
		}

		ginkgo.By("Creating a webserver pod to be part of the TCP service which echoes back source ip")
		serverPodName := "echo-sourceip"
		pod := e2epod.NewAgnhostPod(ns, serverPodName, nil, nil, nil, "netexec", "--http-port", strconv.Itoa(servicePort))
		pod.Labels = jig.Labels
		_, err = cs.CoreV1().Pods(ns).Create(ctx, pod, metav1.CreateOptions{})
		framework.ExpectNoError(err)
		framework.ExpectNoError(e2epod.WaitTimeoutForPodReadyInNamespace(ctx, f.ClientSet, pod.Name, f.Namespace.Name, framework.PodStartTimeout))
		ginkgo.DeferCleanup(func(ctx context.Context) {
			framework.Logf("Cleaning up the echo server pod")
			err := cs.CoreV1().Pods(ns).Delete(ctx, serverPodName, metav1.DeleteOptions{})
			framework.ExpectNoError(err, "failed to delete pod: %s on node", serverPodName)
		})
		err = e2eendpointslice.WaitForEndpointCount(ctx, cs, ns, serviceName, 1)
		framework.ExpectNoError(err)

		ginkgo.By("Creating pause pod deployment")
		deployment := createPausePodDeployment(ctx, cs, "pause-pod", ns, nodeCounts)

		ginkgo.DeferCleanup(func(ctx context.Context) {
			framework.Logf("Deleting deployment")
			err = cs.AppsV1().Deployments(ns).Delete(ctx, deployment.Name, metav1.DeleteOptions{})
			framework.ExpectNoError(err, "Failed to delete deployment %s", deployment.Name)
		})

		framework.ExpectNoError(e2edeployment.WaitForDeploymentComplete(cs, deployment), "Failed to complete pause pod deployment")

		deployment, err = cs.AppsV1().Deployments(ns).Get(ctx, deployment.Name, metav1.GetOptions{})
		framework.ExpectNoError(err, "Error in retrieving pause pod deployment")
		labelSelector, err := metav1.LabelSelectorAsSelector(deployment.Spec.Selector)

		pausePods, err := cs.CoreV1().Pods(ns).List(ctx, metav1.ListOptions{LabelSelector: labelSelector.String()})
		framework.ExpectNoError(err, "Error in listing pods associated with pause pod deployments")

		gomega.Expect(pausePods.Items[0].Spec.NodeName).NotTo(gomega.Equal(pausePods.Items[1].Spec.NodeName))

		serviceAddress := net.JoinHostPort(serviceIP, strconv.Itoa(servicePort))

		for _, pausePod := range pausePods.Items {
			sourceIP, execPodIP := execSourceIPTest(pausePod, serviceAddress)
			ginkgo.By("Verifying the preserved source ip")
			gomega.Expect(sourceIP).To(gomega.Equal(execPodIP),
				"expected preserved source IP is %s, if not, please check whether the internal pod traffic is masqueraded",
				sourceIP)
		}
	})

	ginkgo.It("should allow pods to hairpin back to themselves through services", func(ctx context.Context) {
		serviceName := "hairpin-test"
		ns := f.Namespace.Name

		ginkgo.By("creating a TCP service " + serviceName + " with type=ClusterIP in namespace " + ns)
		jig := e2eservice.NewTestJig(cs, ns, serviceName)
		jig.ExternalIPs = false
		servicePort := 8080
		svc, err := jig.CreateTCPServiceWithPort(ctx, nil, int32(servicePort))
		framework.ExpectNoError(err)
		serviceIP := svc.Spec.ClusterIP
		framework.Logf("hairpin-test cluster ip: %s", serviceIP)

		ginkgo.By("creating a client/server pod")
		serverPodName := "hairpin"
		podTemplate := e2epod.NewAgnhostPod(ns, serverPodName, nil, nil, nil, "netexec", "--http-port", strconv.Itoa(servicePort))
		podTemplate.Labels = jig.Labels
		pod, err := cs.CoreV1().Pods(ns).Create(ctx, podTemplate, metav1.CreateOptions{})
		framework.ExpectNoError(err)
		framework.ExpectNoError(e2epod.WaitTimeoutForPodReadyInNamespace(ctx, f.ClientSet, pod.Name, f.Namespace.Name, framework.PodStartTimeout))

		ginkgo.By("waiting for the service to expose an endpoint")
		err = e2eendpointslice.WaitForEndpointCount(ctx, cs, ns, serviceName, 1)
		framework.ExpectNoError(err)

		ginkgo.By("Checking if the pod can reach itself")
		err = jig.CheckServiceReachability(ctx, svc, pod)
		framework.ExpectNoError(err)
	})

	ginkgo.It("should be able to up and down services", func(ctx context.Context) {
		ns := f.Namespace.Name
		numPods, servicePort := 3, defaultServeHostnameServicePort

		svc1 := "up-down-1"
		svc2 := "up-down-2"
		svc3 := "up-down-3"

		ginkgo.By("creating " + svc1 + " in namespace " + ns)
		podNames1, svc1IP, err := StartServeHostnameService(ctx, cs, getServeHostnameService(svc1), ns, numPods)
		framework.ExpectNoError(err, "failed to create deployment with service: %s in the namespace: %s", svc1, ns)
		ginkgo.By("creating " + svc2 + " in namespace " + ns)
		podNames2, svc2IP, err := StartServeHostnameService(ctx, cs, getServeHostnameService(svc2), ns, numPods)
		framework.ExpectNoError(err, "failed to create deployment with service: %s in the namespace: %s", svc2, ns)

		ginkgo.By("verifying service " + svc1 + " is up")
		framework.ExpectNoError(verifyServeHostnameServiceUp(ctx, cs, ns, podNames1, svc1IP, servicePort))

		ginkgo.By("verifying service " + svc2 + " is up")
		framework.ExpectNoError(verifyServeHostnameServiceUp(ctx, cs, ns, podNames2, svc2IP, servicePort))

		// 停止 Service 1 并确认它已消失。
		ginkgo.By("stopping service " + svc1)
		framework.ExpectNoError(StopServeHostnameService(ctx, f.ClientSet, ns, svc1))

		ginkgo.By("verifying service " + svc1 + " is not up")
		framework.ExpectNoError(verifyServeHostnameServiceDown(ctx, cs, ns, svc1IP, servicePort))
		ginkgo.By("verifying service " + svc2 + " is still up")
		framework.ExpectNoError(verifyServeHostnameServiceUp(ctx, cs, ns, podNames2, svc2IP, servicePort))

		// 启动另一个 Service 并确认两者都可用。
		ginkgo.By("creating service " + svc3 + " in namespace " + ns)
		podNames3, svc3IP, err := StartServeHostnameService(ctx, cs, getServeHostnameService(svc3), ns, numPods)
		framework.ExpectNoError(err, "failed to create deployment with service: %s in the namespace: %s", svc3, ns)

		if svc2IP == svc3IP {
			framework.Failf("service IPs conflict: %v", svc2IP)
		}

		ginkgo.By("verifying service " + svc2 + " is still up")
		framework.ExpectNoError(verifyServeHostnameServiceUp(ctx, cs, ns, podNames2, svc2IP, servicePort))

		ginkgo.By("verifying service " + svc3 + " is up")
		framework.ExpectNoError(verifyServeHostnameServiceUp(ctx, cs, ns, podNames3, svc3IP, servicePort))
	})

	ginkgo.It("should work after the service has been recreated", func(ctx context.Context) {
		serviceName := "service-deletion"
		ns := f.Namespace.Name
		numPods, servicePort := 1, defaultServeHostnameServicePort

		ginkgo.By("creating the service " + serviceName + " in namespace " + ns)
		ginkgo.DeferCleanup(StopServeHostnameService, f.ClientSet, ns, serviceName)
		podNames, svcIP, _ := StartServeHostnameService(ctx, cs, getServeHostnameService(serviceName), ns, numPods)
		framework.ExpectNoError(verifyServeHostnameServiceUp(ctx, cs, ns, podNames, svcIP, servicePort))

		ginkgo.By("deleting the service " + serviceName + " in namespace " + ns)
		err := cs.CoreV1().Services(ns).Delete(ctx, serviceName, metav1.DeleteOptions{})
		framework.ExpectNoError(err)

		ginkgo.By("Waiting for the service " + serviceName + " in namespace " + ns + " to disappear")
		if pollErr := wait.PollImmediate(framework.Poll, e2eservice.RespondingTimeout, func() (bool, error) {
			_, err := cs.CoreV1().Services(ns).Get(ctx, serviceName, metav1.GetOptions{})
			if err != nil {
				if apierrors.IsNotFound(err) {
					framework.Logf("Service %s/%s is gone.", ns, serviceName)
					return true, nil
				}
				return false, err
			}
			framework.Logf("Service %s/%s still exists", ns, serviceName)
			return false, nil
		}); pollErr != nil {
			framework.Failf("Failed to wait for service to disappear: %v", pollErr)
		}

		ginkgo.By("recreating the service " + serviceName + " in namespace " + ns)
		svc, err := cs.CoreV1().Services(ns).Create(ctx, getServeHostnameService(serviceName), metav1.CreateOptions{})
		framework.ExpectNoError(err)
		framework.ExpectNoError(verifyServeHostnameServiceUp(ctx, cs, ns, podNames, svc.Spec.ClusterIP, servicePort))
	})

	f.It("should work after restarting kube-proxy", f.WithDisruptive(), func(ctx context.Context) {
		kubeProxyLabelSet := map[string]string{clusterAddonLabelKey: kubeProxyLabelName}
		e2eskipper.SkipUnlessComponentRunsAsPodsAndClientCanDeleteThem(ctx, kubeProxyLabelName, cs, metav1.NamespaceSystem, kubeProxyLabelSet)

		// TODO: 这里改用 ServiceTestJig
		ns := f.Namespace.Name
		numPods, servicePort := 3, defaultServeHostnameServicePort

		svc1 := "restart-proxy-1"
		svc2 := "restart-proxy-2"

		ginkgo.DeferCleanup(StopServeHostnameService, f.ClientSet, ns, svc1)
		podNames1, svc1IP, err := StartServeHostnameService(ctx, cs, getServeHostnameService(svc1), ns, numPods)
		framework.ExpectNoError(err, "failed to create deployment with service: %s in the namespace: %s", svc1, ns)

		ginkgo.DeferCleanup(StopServeHostnameService, f.ClientSet, ns, svc2)
		podNames2, svc2IP, err := StartServeHostnameService(ctx, cs, getServeHostnameService(svc2), ns, numPods)
		framework.ExpectNoError(err, "failed to create deployment with service: %s in the namespace: %s", svc2, ns)

		if svc1IP == svc2IP {
			framework.Failf("VIPs conflict: %v", svc1IP)
		}

		framework.ExpectNoError(verifyServeHostnameServiceUp(ctx, cs, ns, podNames1, svc1IP, servicePort))
		framework.ExpectNoError(verifyServeHostnameServiceUp(ctx, cs, ns, podNames2, svc2IP, servicePort))

		if err := restartComponent(ctx, cs, kubeProxyLabelName, metav1.NamespaceSystem, kubeProxyLabelSet); err != nil {
			framework.Failf("error restarting kube-proxy: %v", err)
		}
		framework.ExpectNoError(verifyServeHostnameServiceUp(ctx, cs, ns, podNames1, svc1IP, servicePort))
		framework.ExpectNoError(verifyServeHostnameServiceUp(ctx, cs, ns, podNames2, svc2IP, servicePort))
	})

	f.It("should work after restarting apiserver", f.WithDisruptive(), func(ctx context.Context) {

		e2eskipper.SkipUnlessComponentRunsAsPodsAndClientCanDeleteThem(ctx, kubeAPIServerLabelName, cs, metav1.NamespaceSystem, map[string]string{clusterComponentKey: kubeAPIServerLabelName})

		// TODO: 这里改用 ServiceTestJig
		ns := f.Namespace.Name
		numPods, servicePort := 3, defaultServeHostnameServicePort

		svc1 := "restart-apiserver-1"
		svc2 := "restart-apiserver-2"

		ginkgo.DeferCleanup(StopServeHostnameService, f.ClientSet, ns, svc1)
		podNames1, svc1IP, err := StartServeHostnameService(ctx, cs, getServeHostnameService(svc1), ns, numPods)
		framework.ExpectNoError(err, "failed to create deployment with service: %s in the namespace: %s", svc1, ns)

		framework.ExpectNoError(verifyServeHostnameServiceUp(ctx, cs, ns, podNames1, svc1IP, servicePort))

		// 重启 apiserver
		ginkgo.By("Restarting apiserver")
		if err := restartApiserver(ctx, ns, cs); err != nil {
			framework.Failf("error restarting apiserver: %v", err)
		}
		ginkgo.By("Waiting for apiserver to come up by polling /healthz")
		if err := waitForAPIServerUp(ctx, cs); err != nil {
			framework.Failf("error while waiting for apiserver up: %v", err)
		}
		framework.ExpectNoError(verifyServeHostnameServiceUp(ctx, cs, ns, podNames1, svc1IP, servicePort))

		// 新建一个 Service，检查它不会复用之前的 IP。
		ginkgo.DeferCleanup(StopServeHostnameService, f.ClientSet, ns, svc2)
		podNames2, svc2IP, err := StartServeHostnameService(ctx, cs, getServeHostnameService(svc2), ns, numPods)
		framework.ExpectNoError(err, "failed to create deployment with service: %s in the namespace: %s", svc2, ns)

		if svc1IP == svc2IP {
			framework.Failf("VIPs conflict: %v", svc1IP)
		}
		framework.ExpectNoError(verifyServeHostnameServiceUp(ctx, cs, ns, podNames1, svc1IP, servicePort))
		framework.ExpectNoError(verifyServeHostnameServiceUp(ctx, cs, ns, podNames2, svc2IP, servicePort))
	})

	/*
		Release: v1.16
		Testname: Service, NodePort Service
		Description: 创建 TCP NodePort Service，并从客户端 Pod 测试可达性。
		客户端 Pod MUST 能通过 Service 名称和 ClusterIP 在 Service 端口上访问该 NodePort Service，
		也 MUST 能通过节点的 internal IP 和 external IP 在 NodePort 上访问。
	*/
	framework.ConformanceIt("should be able to create a functioning NodePort service", func(ctx context.Context) {
		serviceName := "nodeport-test"
		ns := f.Namespace.Name

		jig := e2eservice.NewTestJig(cs, ns, serviceName)

		ginkgo.By("creating service " + serviceName + " with type=NodePort in namespace " + ns)
		nodePortService, err := jig.CreateTCPService(ctx, func(svc *v1.Service) {
			svc.Spec.Type = v1.ServiceTypeNodePort
			svc.Spec.Ports = []v1.ServicePort{
				{Port: 80, Name: "http", Protocol: v1.ProtocolTCP, TargetPort: intstr.FromInt32(9376)},
			}
		})
		framework.ExpectNoError(err)
		err = jig.CreateServicePods(ctx, 2)
		framework.ExpectNoError(err)
		execPod := e2epod.CreateExecPodOrFail(ctx, cs, ns, "execpod", nil)
		err = jig.CheckServiceReachability(ctx, nodePortService, execPod)
		framework.ExpectNoError(err)
	})

	/*
		创建一个 External IP 未分配给任何网络接口的 ClusterIP Service。
		这里使用的 IP 段依据 [RFC 5737](https://tools.ietf.org/html/rfc5737) 第 3 节保留用于文档，
		任何主机都不应使用。
	*/
	ginkgo.It("should be possible to connect to a service via ExternalIP when the external IP is not assigned to a node", func(ctx context.Context) {
		serviceName := "externalip-test"
		ns := f.Namespace.Name
		externalIP := "203.0.113.250"
		if framework.TestContext.ClusterIsIPv6() {
			externalIP = "2001:DB8::cb00:71fa"
		}

		jig := e2eservice.NewTestJig(cs, ns, serviceName)
		jig.ExternalIPs = false

		ginkgo.By("creating service " + serviceName + " with type=clusterIP in namespace " + ns)
		clusterIPService, err := jig.CreateTCPService(ctx, func(svc *v1.Service) {
			svc.Spec.Type = v1.ServiceTypeClusterIP
			svc.Spec.ExternalIPs = []string{externalIP}
			svc.Spec.Ports = []v1.ServicePort{
				{Port: 80, Name: "http", Protocol: v1.ProtocolTCP, TargetPort: intstr.FromInt32(9376)},
			}
		})
		if err != nil && strings.Contains(err.Error(), "Use of external IPs is denied by admission control") {
			e2eskipper.Skipf("Admission controller to deny services with external IPs is enabled - skip.")
		}
		framework.ExpectNoError(err)
		err = jig.CreateServicePods(ctx, 2)
		framework.ExpectNoError(err)
		execPod := e2epod.CreateExecPodOrFail(ctx, cs, ns, "execpod", nil)
		err = jig.CheckServiceReachability(ctx, clusterIPService, execPod)
		framework.ExpectNoError(err)
	})

	/*
		Testname: Service, update NodePort, same port different protocol
		Description: 创建接受 TCP 请求的 Service。默认情况下，创建的 Service MUST 是 ClusterIP 类型且 MUST 分配 ClusterIP。
		将 Service 类型更新为支持 TCP 协议的 NodePort 后，MUST 能通过 nodeIP 和分配的 NodePort 访问以处理 TCP 请求。
		再将此 NodePort Service 更新为在同一 Service 端口 80 上同时使用 TCP 和 UDP 两种协议，Service 更新 MUST 成功，
		为 Service 分配两个 NodePort，且 Service MUST 能在同一端口 80 上同时处理 TCP 和 UDP 请求。
	*/
	ginkgo.It("should be able to update service type to NodePort listening on same port number but different protocols", func(ctx context.Context) {
		serviceName := "nodeport-update-service"
		ns := f.Namespace.Name
		jig := e2eservice.NewTestJig(cs, ns, serviceName)
		jig.ExternalIPs = false

		ginkgo.By("creating a TCP service " + serviceName + " with type=ClusterIP in namespace " + ns)
		tcpService, err := jig.CreateTCPService(ctx, nil)
		framework.ExpectNoError(err)
		ginkgo.DeferCleanup(func(ctx context.Context) {
			framework.Logf("Cleaning up the updating NodePorts test service")
			err := cs.CoreV1().Services(ns).Delete(ctx, serviceName, metav1.DeleteOptions{})
			framework.ExpectNoError(err, "failed to delete service: %s in namespace: %s", serviceName, ns)
		})
		framework.Logf("Service Port TCP: %v", tcpService.Spec.Ports[0].Port)

		ginkgo.By("changing the TCP service to type=NodePort")
		nodePortService, err := jig.UpdateService(ctx, func(s *v1.Service) {
			s.Spec.Type = v1.ServiceTypeNodePort
			s.Spec.Ports = []v1.ServicePort{
				{
					Name:       "tcp-port",
					Port:       80,
					Protocol:   v1.ProtocolTCP,
					TargetPort: intstr.FromInt32(9376),
				},
			}
		})
		framework.ExpectNoError(err)

		err = jig.CreateServicePods(ctx, 2)
		framework.ExpectNoError(err)
		execPod := e2epod.CreateExecPodOrFail(ctx, cs, ns, "execpod", nil)
		err = jig.CheckServiceReachability(ctx, nodePortService, execPod)
		framework.ExpectNoError(err)

		ginkgo.By("Updating NodePort service to listen TCP and UDP based requests over same Port")
		nodePortService, err = jig.UpdateService(ctx, func(s *v1.Service) {
			s.Spec.Type = v1.ServiceTypeNodePort
			s.Spec.Ports = []v1.ServicePort{
				{
					Name:       "tcp-port",
					Port:       80,
					Protocol:   v1.ProtocolTCP,
					TargetPort: intstr.FromInt32(9376),
				},
				{
					Name:       "udp-port",
					Port:       80,
					Protocol:   v1.ProtocolUDP,
					TargetPort: intstr.FromInt32(9376),
				},
			}
		})
		framework.ExpectNoError(err)
		err = jig.CheckServiceReachability(ctx, nodePortService, execPod)
		framework.ExpectNoError(err)
		nodePortCounts := len(nodePortService.Spec.Ports)
		gomega.Expect(nodePortCounts).To(gomega.Equal(2), "updated service should have two Ports but found %d Ports", nodePortCounts)

		for _, port := range nodePortService.Spec.Ports {
			gomega.Expect(port.NodePort).ToNot(gomega.BeZero(), "NodePort service failed to allocate NodePort for Port %s", port.Name)
			framework.Logf("NodePort service allocates NodePort: %d for Port: %s over Protocol: %s", port.NodePort, port.Name, port.Protocol)
		}
	})

	/*
		Release: v1.16
		Testname: Service, change type, ExternalName to ClusterIP
		Description: 创建指向外部 DNS 的 ExternalName 类型 Service，该 Service MUST 不被分配 ClusterIP。
		移除 ExternalName 字段、将 80 设为 Service 端口、TCP 设为协议，把 Service 从 ExternalName 更新为 ClusterIP。
		Service 更新 MUST 成功并分配 ClusterIP，且 MUST 能通过 serviceName 和 ClusterIP 在指定 Service 端口上访问。
	*/
	framework.ConformanceIt("should be able to change the type from ExternalName to ClusterIP", func(ctx context.Context) {
		serviceName := "externalname-service"
		ns := f.Namespace.Name
		jig := e2eservice.NewTestJig(cs, ns, serviceName)

		ginkgo.By("creating a service " + serviceName + " with the type=ExternalName in namespace " + ns)
		_, err := jig.CreateExternalNameService(ctx, nil)
		framework.ExpectNoError(err)
		ginkgo.DeferCleanup(func(ctx context.Context) {
			framework.Logf("Cleaning up the ExternalName to ClusterIP test service")
			err := cs.CoreV1().Services(ns).Delete(ctx, serviceName, metav1.DeleteOptions{})
			framework.ExpectNoError(err, "failed to delete service %s in namespace %s", serviceName, ns)
		})

		ginkgo.By("changing the ExternalName service to type=ClusterIP")
		clusterIPService, err := jig.UpdateService(ctx, func(s *v1.Service) {
			s.Spec.Type = v1.ServiceTypeClusterIP
			s.Spec.ExternalName = ""
			s.Spec.Ports = []v1.ServicePort{
				{Port: 80, Name: "http", Protocol: v1.ProtocolTCP, TargetPort: intstr.FromInt32(9376)},
			}
		})
		framework.ExpectNoError(err)

		err = jig.CreateServicePods(ctx, 2)
		framework.ExpectNoError(err)
		execPod := e2epod.CreateExecPodOrFail(ctx, cs, ns, "execpod", nil)
		err = jig.CheckServiceReachability(ctx, clusterIPService, execPod)
		framework.ExpectNoError(err)
	})

	/*
		Release: v1.16
		Testname: Service, change type, ExternalName to NodePort
		Description: 创建指向外部 DNS 的 ExternalName 类型 Service，该 Service MUST 不被分配 ClusterIP。
		将 Service 从 ExternalName 更新为 NodePort，指定 80 为 Service 端口、TCP 为协议。
		Service 更新 MUST 成功：在每个节点的 IP 上通过动态分配的 NodePort 暴露 Service，并 MUST 分配 ClusterIP 用于路由请求。
		Service MUST 能通过 serviceName 和 ClusterIP 在 servicePort 上访问，也 MUST 能通过节点 IP 和 NodePort 访问。
	*/
	framework.ConformanceIt("should be able to change the type from ExternalName to NodePort", func(ctx context.Context) {
		serviceName := "externalname-service"
		ns := f.Namespace.Name
		jig := e2eservice.NewTestJig(cs, ns, serviceName)

		ginkgo.By("creating a service " + serviceName + " with the type=ExternalName in namespace " + ns)
		_, err := jig.CreateExternalNameService(ctx, nil)
		framework.ExpectNoError(err)
		ginkgo.DeferCleanup(func(ctx context.Context) {
			framework.Logf("Cleaning up the ExternalName to NodePort test service")
			err := cs.CoreV1().Services(ns).Delete(ctx, serviceName, metav1.DeleteOptions{})
			framework.ExpectNoError(err, "failed to delete service %s in namespace %s", serviceName, ns)
		})

		ginkgo.By("changing the ExternalName service to type=NodePort")
		nodePortService, err := jig.UpdateService(ctx, func(s *v1.Service) {
			s.Spec.Type = v1.ServiceTypeNodePort
			s.Spec.ExternalName = ""
			s.Spec.Ports = []v1.ServicePort{
				{Port: 80, Name: "http", Protocol: v1.ProtocolTCP, TargetPort: intstr.FromInt32(9376)},
			}
		})
		framework.ExpectNoError(err)
		err = jig.CreateServicePods(ctx, 2)
		framework.ExpectNoError(err)

		execPod := e2epod.CreateExecPodOrFail(ctx, cs, ns, "execpod", nil)
		err = jig.CheckServiceReachability(ctx, nodePortService, execPod)
		framework.ExpectNoError(err)
	})

	/*
		Release: v1.16
		Testname: Service, change type, ClusterIP to ExternalName
		Description: 创建 ClusterIP 类型的 Service，创建 MUST 成功并分配 ClusterIP。
		通过将 CNAME 记录设为 externalName，把 Service 类型从 ClusterIP 更新为 ExternalName。Service 更新 MUST 成功且 MUST 不再关联 ClusterIP。
		Service MUST 能解析为 IP 地址（返回 A 记录），确保指向指定的 externalName。
	*/
	framework.ConformanceIt("should be able to change the type from ClusterIP to ExternalName", func(ctx context.Context) {
		serviceName := "clusterip-service"
		ns := f.Namespace.Name
		jig := e2eservice.NewTestJig(cs, ns, serviceName)

		ginkgo.By("creating a service " + serviceName + " with the type=ClusterIP in namespace " + ns)
		_, err := jig.CreateTCPService(ctx, nil)
		framework.ExpectNoError(err)
		ginkgo.DeferCleanup(func(ctx context.Context) {
			framework.Logf("Cleaning up the ClusterIP to ExternalName test service")
			err := cs.CoreV1().Services(ns).Delete(ctx, serviceName, metav1.DeleteOptions{})
			framework.ExpectNoError(err, "failed to delete service %s in namespace %s", serviceName, ns)
		})

		ginkgo.By("Creating active service to test reachability when its FQDN is referred as externalName for another service")
		externalServiceName := "externalsvc"
		externalServiceFQDN := createAndGetExternalServiceFQDN(ctx, cs, ns, externalServiceName)
		ginkgo.DeferCleanup(StopServeHostnameService, f.ClientSet, ns, externalServiceName)

		ginkgo.By("changing the ClusterIP service to type=ExternalName")
		externalNameService, err := jig.UpdateService(ctx, func(s *v1.Service) {
			s.Spec.Type = v1.ServiceTypeExternalName
			s.Spec.ExternalName = externalServiceFQDN
		})
		framework.ExpectNoError(err)
		if externalNameService.Spec.ClusterIP != "" {
			framework.Failf("Spec.ClusterIP was not cleared")
		}
		execPod := e2epod.CreateExecPodOrFail(ctx, cs, ns, "execpod", nil)
		err = jig.CheckServiceReachability(ctx, externalNameService, execPod)
		framework.ExpectNoError(err)
	})

	/*
		Release: v1.16
		Testname: Service, change type, NodePort to ExternalName
		Description: 创建 NodePort 类型的 Service，创建 MUST 成功：在每个节点的 IP 上通过动态分配的 NodePort 暴露 Service，并 MUST 分配 ClusterIP 用于路由请求。
		通过将 CNAME 记录设为 externalName，把 Service 类型从 NodePort 更新为 ExternalName。Service 更新 MUST 成功，MUST 不再关联 ClusterIP，且分配的 NodePort MUST 被释放。
		Service MUST 能解析为 IP 地址（返回 A 记录），确保指向指定的 externalName。
	*/
	framework.ConformanceIt("should be able to change the type from NodePort to ExternalName", func(ctx context.Context) {
		serviceName := "nodeport-service"
		ns := f.Namespace.Name
		jig := e2eservice.NewTestJig(cs, ns, serviceName)

		ginkgo.By("creating a service " + serviceName + " with the type=NodePort in namespace " + ns)
		_, err := jig.CreateTCPService(ctx, func(svc *v1.Service) {
			svc.Spec.Type = v1.ServiceTypeNodePort
		})
		framework.ExpectNoError(err)
		ginkgo.DeferCleanup(func(ctx context.Context) {
			framework.Logf("Cleaning up the NodePort to ExternalName test service")
			err := cs.CoreV1().Services(ns).Delete(ctx, serviceName, metav1.DeleteOptions{})
			framework.ExpectNoError(err, "failed to delete service %s in namespace %s", serviceName, ns)
		})

		ginkgo.By("Creating active service to test reachability when its FQDN is referred as externalName for another service")
		externalServiceName := "externalsvc"
		externalServiceFQDN := createAndGetExternalServiceFQDN(ctx, cs, ns, externalServiceName)
		ginkgo.DeferCleanup(StopServeHostnameService, f.ClientSet, ns, externalServiceName)

		ginkgo.By("changing the NodePort service to type=ExternalName")
		externalNameService, err := jig.UpdateService(ctx, func(s *v1.Service) {
			s.Spec.Type = v1.ServiceTypeExternalName
			s.Spec.ExternalName = externalServiceFQDN
		})
		framework.ExpectNoError(err)
		if externalNameService.Spec.ClusterIP != "" {
			framework.Failf("Spec.ClusterIP was not cleared")
		}
		execPod := e2epod.CreateExecPodOrFail(ctx, cs, ns, "execpod", nil)
		err = jig.CheckServiceReachability(ctx, externalNameService, execPod)
		framework.ExpectNoError(err)
	})

	ginkgo.It("should prevent NodePort collisions", func(ctx context.Context) {
		// TODO: 这里改用 ServiceTestJig
		baseName := "nodeport-collision-"
		serviceName1 := baseName + "1"
		serviceName2 := baseName + "2"
		ns := f.Namespace.Name

		t := NewServerTest(cs, ns, serviceName1)
		defer func() {
			defer ginkgo.GinkgoRecover()
			errs := t.Cleanup()
			if len(errs) != 0 {
				framework.Failf("errors in cleanup: %v", errs)
			}
		}()

		ginkgo.By("creating service " + serviceName1 + " with type NodePort in namespace " + ns)
		service := t.BuildServiceSpec()
		service.Spec.Type = v1.ServiceTypeNodePort
		result, err := t.CreateService(service)
		framework.ExpectNoError(err, "failed to create service: %s in namespace: %s", serviceName1, ns)

		if result.Spec.Type != v1.ServiceTypeNodePort {
			framework.Failf("got unexpected Spec.Type for new service: %v", result)
		}
		if len(result.Spec.Ports) != 1 {
			framework.Failf("got unexpected len(Spec.Ports) for new service: %v", result)
		}
		port := result.Spec.Ports[0]
		if port.NodePort == 0 {
			framework.Failf("got unexpected Spec.Ports[0].NodePort for new service: %v", result)
		}

		ginkgo.By("creating service " + serviceName2 + " with conflicting NodePort")
		service2 := t.BuildServiceSpec()
		service2.Name = serviceName2
		service2.Spec.Type = v1.ServiceTypeNodePort
		service2.Spec.Ports[0].NodePort = port.NodePort
		result2, err := t.CreateService(service2)
		if err == nil {
			framework.Failf("Created service with conflicting NodePort: %v", result2)
		}
		expectedErr := fmt.Sprintf("%d.*port is already allocated", port.NodePort)
		gomega.Expect(fmt.Sprintf("%v", err)).To(gomega.MatchRegexp(expectedErr))

		ginkgo.By("deleting service " + serviceName1 + " to release NodePort")
		err = t.DeleteService(serviceName1)
		framework.ExpectNoError(err, "failed to delete service: %s in namespace: %s", serviceName1, ns)

		ginkgo.By("creating service " + serviceName2 + " with no-longer-conflicting NodePort")
		_, err = t.CreateService(service2)
		framework.ExpectNoError(err, "failed to create service: %s in namespace: %s", serviceName1, ns)
	})

	ginkgo.It("should check NodePort out-of-range", func(ctx context.Context) {
		// TODO: 这里改用 ServiceTestJig
		serviceName := "nodeport-range-test"
		ns := f.Namespace.Name

		t := NewServerTest(cs, ns, serviceName)
		defer func() {
			defer ginkgo.GinkgoRecover()
			errs := t.Cleanup()
			if len(errs) != 0 {
				framework.Failf("errors in cleanup: %v", errs)
			}
		}()

		service := t.BuildServiceSpec()
		service.Spec.Type = v1.ServiceTypeNodePort

		ginkgo.By("creating service " + serviceName + " with type NodePort in namespace " + ns)
		service, err := t.CreateService(service)
		framework.ExpectNoError(err, "failed to create service: %s in namespace: %s", serviceName, ns)

		if service.Spec.Type != v1.ServiceTypeNodePort {
			framework.Failf("got unexpected Spec.Type for new service: %v", service)
		}
		if len(service.Spec.Ports) != 1 {
			framework.Failf("got unexpected len(Spec.Ports) for new service: %v", service)
		}
		port := service.Spec.Ports[0]
		if port.NodePort == 0 {
			framework.Failf("got unexpected Spec.Ports[0].nodePort for new service: %v", service)
		}
		if !e2eservice.NodePortRange.Contains(int(port.NodePort)) {
			framework.Failf("got unexpected (out-of-range) port for new service: %v", service)
		}

		outOfRangeNodePort := 0
		for {
			outOfRangeNodePort = 1 + rand.Intn(65535)
			if !e2eservice.NodePortRange.Contains(outOfRangeNodePort) {
				break
			}
		}
		ginkgo.By(fmt.Sprintf("changing service "+serviceName+" to out-of-range NodePort %d", outOfRangeNodePort))
		result, err := e2eservice.UpdateService(ctx, cs, ns, serviceName, func(s *v1.Service) {
			s.Spec.Ports[0].NodePort = int32(outOfRangeNodePort)
		})
		if err == nil {
			framework.Failf("failed to prevent update of service with out-of-range NodePort: %v", result)
		}
		expectedErr := fmt.Sprintf("%d.*port is not in the valid range", outOfRangeNodePort)
		gomega.Expect(fmt.Sprintf("%v", err)).To(gomega.MatchRegexp(expectedErr))

		ginkgo.By("deleting original service " + serviceName)
		err = t.DeleteService(serviceName)
		framework.ExpectNoError(err, "failed to delete service: %s in namespace: %s", serviceName, ns)

		ginkgo.By(fmt.Sprintf("creating service "+serviceName+" with out-of-range NodePort %d", outOfRangeNodePort))
		service = t.BuildServiceSpec()
		service.Spec.Type = v1.ServiceTypeNodePort
		service.Spec.Ports[0].NodePort = int32(outOfRangeNodePort)
		service, err = t.CreateService(service)
		if err == nil {
			framework.Failf("failed to prevent create of service with out-of-range NodePort (%d): %v", outOfRangeNodePort, service)
		}
		gomega.Expect(fmt.Sprintf("%v", err)).To(gomega.MatchRegexp(expectedErr))
	})

	ginkgo.It("should release NodePorts on delete", func(ctx context.Context) {
		// TODO: 这里改用 ServiceTestJig
		serviceName := "nodeport-reuse"
		ns := f.Namespace.Name

		t := NewServerTest(cs, ns, serviceName)
		defer func() {
			defer ginkgo.GinkgoRecover()
			errs := t.Cleanup()
			if len(errs) != 0 {
				framework.Failf("errors in cleanup: %v", errs)
			}
		}()

		var service *v1.Service
		baseService := t.BuildServiceSpec()
		baseService.Spec.Type = v1.ServiceTypeNodePort
		numberOfRetries := 5
		ginkgo.By("creating service " + serviceName + " with type NodePort in namespace " + ns)
		var err error
		for range numberOfRetries {
			port, err := e2eservice.GetUnusedStaticNodePort()
			framework.ExpectNoError(err, "Static node port allocator was not able to find a free nodeport.")
			baseService.Spec.Ports[0].NodePort = port
			service, err = t.CreateService(baseService)
			// 稍后会删除该 Service 并用同一个 nodeport 重建，需防止其他 e2e 测试
			// 占用旧的 nodeport 导致重建冲突，因此使用 ReserveStaticNodePort 预留。
			if err == nil {
				nodePort := service.Spec.Ports[0].NodePort
				ok := e2eservice.ReserveStaticNodePort(nodePort)
				if !ok {
					// 无法预留已分配的端口，说明该端口无效或已被其他测试预留。
					// 这表明代码存在问题，已有日志信息可用于排查。
					framework.Failf("Static node port allocator was not able to reserve nodeport: %d", nodePort)
				}
				break
			}
			if apierrors.IsInvalid(err) {
				framework.Logf("node port %d is already allocated to other service, retrying ... : %v", port, err)
				continue
			}
			framework.ExpectNoError(err, "failed to create service: %s in namespace: %s", serviceName, ns)

		}

		nodePort := service.Spec.Ports[0].NodePort
		ginkgo.DeferCleanup(func(ctx context.Context) {
			err := cs.CoreV1().Services(ns).Delete(ctx, serviceName, metav1.DeleteOptions{})
			if err != nil && !apierrors.IsNotFound(err) {
				framework.ExpectNoError(err, "failed to delete service %s in namespace %s", serviceName, ns)
			}
			e2eservice.ReleaseStaticNodePort(nodePort)
		})

		if service.Spec.Type != v1.ServiceTypeNodePort {
			framework.Failf("got unexpected Spec.Type for new service: %v", service)
		}
		if len(service.Spec.Ports) != 1 {
			framework.Failf("got unexpected len(Spec.Ports) for new service: %v", service)
		}
		port := service.Spec.Ports[0]
		if port.NodePort == 0 {
			framework.Failf("got unexpected Spec.Ports[0].nodePort for new service: %v", service)
		}
		if !e2eservice.NodePortRange.Contains(int(port.NodePort)) {
			framework.Failf("got unexpected (out-of-range) port for new service: %v", service)
		}

		ginkgo.By("deleting original service " + serviceName)
		err = t.DeleteService(serviceName)
		framework.ExpectNoError(err, "failed to delete service: %s in namespace: %s", serviceName, ns)

		hostExec := launchHostExecPod(ctx, f.ClientSet, f.Namespace.Name, "hostexec", nil)
		cmd := fmt.Sprintf(`! ss -ant46 'sport = :%d' | tail -n +2 | grep LISTEN`, nodePort)
		var stdout string
		if pollErr := wait.PollImmediate(framework.Poll, e2eservice.KubeProxyLagTimeout, func() (bool, error) {
			var err error
			stdout, err = e2eoutput.RunHostCmd(hostExec.Namespace, hostExec.Name, cmd)
			if err != nil {
				framework.Logf("expected node port (%d) to not be in use, stdout: %v", nodePort, stdout)
				return false, nil
			}
			return true, nil
		}); pollErr != nil {
			framework.Failf("expected node port (%d) to not be in use in %v, stdout: %v", nodePort, e2eservice.KubeProxyLagTimeout, stdout)
		}

		ginkgo.By(fmt.Sprintf("creating service "+serviceName+" with same NodePort %d", nodePort))
		service = t.BuildServiceSpec()
		service.Spec.Type = v1.ServiceTypeNodePort
		service.Spec.Ports[0].NodePort = nodePort
		_, err = t.CreateService(service)
		framework.ExpectNoError(err, "failed to create service: %s in namespace: %s", serviceName, ns)
	})

	ginkgo.It("should create endpoints for unready pods", func(ctx context.Context) {
		serviceName := "tolerate-unready"
		ns := f.Namespace.Name

		t := NewServerTest(cs, ns, serviceName)
		defer func() {
			defer ginkgo.GinkgoRecover()
			errs := t.Cleanup()
			if len(errs) != 0 {
				framework.Failf("errors in cleanup: %v", errs)
			}
		}()

		t.Name = "slow-terminating-unready-pod"
		t.Image = imageutils.GetE2EImage(imageutils.Agnhost)
		port := int32(80)
		terminateSeconds := int64(100)

		service := &v1.Service{
			ObjectMeta: metav1.ObjectMeta{
				Name:      t.ServiceName,
				Namespace: t.Namespace,
			},
			Spec: v1.ServiceSpec{
				Selector: t.Labels,
				Ports: []v1.ServicePort{{
					Name:       "http",
					Port:       port,
					TargetPort: intstr.FromInt32(port),
				}},
				PublishNotReadyAddresses: true,
			},
		}

		deploymentSpec := e2edeployment.NewDeployment(t.Name,
			1,
			t.Labels,
			t.Name,
			t.Image,
			appsv1.RecreateDeploymentStrategyType)
		deploymentSpec.Spec.Template.Spec.Containers[0] = v1.Container{
			Args:  []string{"netexec", fmt.Sprintf("--http-port=%d", port)},
			Name:  t.Name,
			Image: t.Image,
			Ports: []v1.ContainerPort{{ContainerPort: port, Protocol: v1.ProtocolTCP}},
			ReadinessProbe: &v1.Probe{
				ProbeHandler: v1.ProbeHandler{
					Exec: &v1.ExecAction{
						Command: []string{"/bin/false"},
					},
				},
			},
			Lifecycle: &v1.Lifecycle{
				PreStop: &v1.LifecycleHandler{
					Exec: &v1.ExecAction{
						Command: []string{"/bin/sleep", fmt.Sprintf("%d", terminateSeconds)},
					},
				},
			},
		}
		deploymentSpec.Spec.Template.Spec.TerminationGracePeriodSeconds = &terminateSeconds

		ginkgo.By(fmt.Sprintf("creating Deployment %v with selectors %v", deploymentSpec.Name, deploymentSpec.Spec.Selector))
		_, err := t.CreateDeployment(deploymentSpec)
		framework.ExpectNoError(err)

		ginkgo.By(fmt.Sprintf("creating Service %v with selectors %v", service.Name, service.Spec.Selector))
		_, err = t.CreateService(service)
		framework.ExpectNoError(err)

		ginkgo.By("Verifying pods for Deployment " + t.Name)
		framework.ExpectNoError(e2epod.VerifyPods(ctx, t.Client, t.Namespace, t.Name, labels.SelectorFromSet(t.Labels), false, 1))

		svcName := fmt.Sprintf("%v.%v.svc.%v", serviceName, f.Namespace.Name, framework.TestContext.ClusterDNSDomain)
		ginkgo.By("Waiting for endpoints of Service with DNS name " + svcName)

		execPod := e2epod.CreateExecPodOrFail(ctx, f.ClientSet, f.Namespace.Name, "execpod-", nil)
		execPodName := execPod.Name
		cmd := fmt.Sprintf("curl -q -s --connect-timeout 2 http://%s:%d/", svcName, port)
		var stdout string
		if pollErr := wait.PollUntilContextTimeout(ctx, framework.Poll, e2eservice.KubeProxyLagTimeout, true, func(ctx context.Context) (bool, error) {
			var err error
			stdout, err = e2eoutput.RunHostCmd(f.Namespace.Name, execPodName, cmd)
			if err != nil {
				framework.Logf("expected un-ready endpoint for Service %v, stdout: %v, err %v", t.Name, stdout, err)
				return false, nil
			}
			return true, nil
		}); pollErr != nil {
			framework.Failf("expected un-ready endpoint for Service %v within %v, stdout: %v", t.Name, e2eservice.KubeProxyLagTimeout, stdout)
		}

		ginkgo.By("Scaling down deployment to zero")
		_, err = e2edeployment.UpdateDeploymentWithRetries(f.ClientSet, t.Namespace, t.Name, func(deployment *appsv1.Deployment) {
			deployment.Spec.Replicas = ptr.To[int32](0)
		})
		framework.ExpectNoError(err)

		ginkgo.By("Update service to not tolerate unready services")
		_, err = e2eservice.UpdateService(ctx, f.ClientSet, t.Namespace, t.ServiceName, func(s *v1.Service) {
			s.Spec.PublishNotReadyAddresses = false
		})
		framework.ExpectNoError(err)

		ginkgo.By("Check if pod is unreachable")
		cmd = fmt.Sprintf("curl -q -s --connect-timeout 2 http://%s:%d/; test \"$?\" -ne \"0\"", svcName, port)
		if pollErr := wait.PollImmediate(framework.Poll, e2eservice.KubeProxyLagTimeout, func() (bool, error) {
			var err error
			stdout, err = e2eoutput.RunHostCmd(f.Namespace.Name, execPodName, cmd)
			if err != nil {
				framework.Logf("expected un-ready endpoint for Service %v, stdout: %v, err %v", t.Name, stdout, err)
				return false, nil
			}
			return true, nil
		}); pollErr != nil {
			framework.Failf("expected un-ready endpoint for Service %v within %v, stdout: %v", t.Name, e2eservice.KubeProxyLagTimeout, stdout)
		}

		ginkgo.By("Update service to tolerate unready services again")
		_, err = e2eservice.UpdateService(ctx, f.ClientSet, t.Namespace, t.ServiceName, func(s *v1.Service) {
			s.Spec.PublishNotReadyAddresses = true
		})
		framework.ExpectNoError(err)

		ginkgo.By("Check if terminating pod is available through service")
		cmd = fmt.Sprintf("curl -q -s --connect-timeout 2 http://%s:%d/", svcName, port)
		if pollErr := wait.PollImmediate(framework.Poll, e2eservice.KubeProxyLagTimeout, func() (bool, error) {
			var err error
			stdout, err = e2eoutput.RunHostCmd(f.Namespace.Name, execPodName, cmd)
			if err != nil {
				framework.Logf("expected un-ready endpoint for Service %v, stdout: %v, err %v", t.Name, stdout, err)
				return false, nil
			}
			return true, nil
		}); pollErr != nil {
			framework.Failf("expected un-ready endpoint for Service %v within %v, stdout: %v", t.Name, e2eservice.KubeProxyLagTimeout, stdout)
		}

		ginkgo.By("Remove pods immediately")
		label := labels.SelectorFromSet(t.Labels)
		options := metav1.ListOptions{LabelSelector: label.String()}
		podClient := t.Client.CoreV1().Pods(f.Namespace.Name)
		pods, err := podClient.List(ctx, options)
		if err != nil {
			framework.Logf("warning: error retrieving pods: %s", err)
		} else {
			for _, pod := range pods.Items {
				var gracePeriodSeconds int64 = 0
				err := podClient.Delete(ctx, pod.Name, metav1.DeleteOptions{GracePeriodSeconds: &gracePeriodSeconds})
				if err != nil {
					framework.Logf("warning: error force deleting pod '%s': %s", pod.Name, err)
				}
			}
		}
	})

	ginkgo.It("should be able to connect to terminating and unready endpoints if PublishNotReadyAddresses is true", func(ctx context.Context) {
		nodes, err := e2enode.GetBoundedReadySchedulableNodes(ctx, cs, 2)
		framework.ExpectNoError(err)
		nodeCounts := len(nodes.Items)
		if nodeCounts < 2 {
			e2eskipper.Skipf("The test requires at least two ready nodes on %s, but found %v", framework.TestContext.Provider, nodeCounts)
		}
		node0 := nodes.Items[0]
		node1 := nodes.Items[1]

		serviceName := "svc-tolerate-unready"
		ns := f.Namespace.Name
		servicePort := 80

		ginkgo.By("creating a NodePort TCP service " + serviceName + " that PublishNotReadyAddresses on" + ns)
		jig := e2eservice.NewTestJig(cs, ns, serviceName)
		svc, err := jig.CreateTCPService(ctx, func(svc *v1.Service) {
			svc.Spec.Ports = []v1.ServicePort{
				{Port: 80, Name: "http", Protocol: v1.ProtocolTCP, TargetPort: intstr.FromInt32(80)},
			}
			svc.Spec.Type = v1.ServiceTypeNodePort
			svc.Spec.PublishNotReadyAddresses = true
		})
		framework.ExpectNoError(err, "failed to create Service")

		ginkgo.By("Creating 1 webserver pod to be part of the TCP service")
		gracePeriod := int64(300)
		webserverPod0 := &v1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name: "webserver-pod",
			},
			Spec: v1.PodSpec{
				Containers: []v1.Container{
					{
						Name:  "agnhost",
						Image: imageutils.GetE2EImage(imageutils.Agnhost),
						Args:  []string{"netexec", "--http-port=80", fmt.Sprintf("--delay-shutdown=%d", gracePeriod)},
						Ports: []v1.ContainerPort{
							{
								ContainerPort: 80,
							},
						},
						ReadinessProbe: &v1.Probe{
							ProbeHandler: v1.ProbeHandler{
								HTTPGet: &v1.HTTPGetAction{
									Path: "/readyz",
									Port: intstr.IntOrString{
										IntVal: int32(80),
									},
									Scheme: v1.URISchemeHTTP,
								},
							},
						},
						LivenessProbe: &v1.Probe{
							ProbeHandler: v1.ProbeHandler{
								HTTPGet: &v1.HTTPGetAction{
									Path: "/healthz",
									Port: intstr.IntOrString{
										IntVal: int32(80),
									},
									Scheme: v1.URISchemeHTTP,
								},
							},
						},
					},
				},
			},
		}
		webserverPod0.Labels = jig.Labels
		webserverPod0.Spec.TerminationGracePeriodSeconds = ptr.To(gracePeriod)
		e2epod.SetNodeSelection(&webserverPod0.Spec, e2epod.NodeSelection{Name: node0.Name})

		_, err = cs.CoreV1().Pods(ns).Create(ctx, webserverPod0, metav1.CreateOptions{})
		framework.ExpectNoError(err, "failed to create pod")
		err = e2epod.WaitTimeoutForPodReadyInNamespace(ctx, f.ClientSet, webserverPod0.Name, f.Namespace.Name, framework.PodStartTimeout)
		if err != nil {
			framework.Failf("error waiting for pod %s to be ready %v", webserverPod0.Name, err)
		}
		err = e2eendpointslice.WaitForEndpointCount(ctx, cs, ns, serviceName, 1)
		framework.ExpectNoError(err)

		ginkgo.By("Creating 1 pause pods that will try to connect to the webservers")
		pausePod1 := e2epod.NewAgnhostPod(ns, "pause-pod-1", nil, nil, nil)
		e2epod.SetNodeSelection(&pausePod1.Spec, e2epod.NodeSelection{Name: node1.Name})

		pausePod1, err = cs.CoreV1().Pods(ns).Create(ctx, pausePod1, metav1.CreateOptions{})
		framework.ExpectNoError(err, "failed to create pod")
		err = e2epod.WaitTimeoutForPodReadyInNamespace(ctx, f.ClientSet, pausePod1.Name, f.Namespace.Name, framework.PodStartTimeout)
		if err != nil {
			framework.Failf("error waiting for pod %s to be ready %v", pausePod1.Name, err)
		}

		// webserver 删除后仍应继续通过 Service 提供服务，因为：
		//  - 它有 100 秒的 termination grace period
		//  - 它处于 unready 状态，但 PublishNotReadyAddresses 为 true
		err = cs.CoreV1().Pods(ns).Delete(ctx, webserverPod0.Name, metav1.DeleteOptions{})
		framework.ExpectNoError(err)

		// 等待 Pod 变为 unready
		err = e2epod.WaitForPodCondition(ctx, f.ClientSet, f.Namespace.Name, webserverPod0.Name, "pod not ready", framework.PodStartTimeout, func(pod *v1.Pod) (bool, error) {
			return !podutil.IsPodReady(pod), nil
		})
		if err != nil {
			framework.Failf("error waiting for pod %s to be unready %v", webserverPod0.Name, err)
		}
		// 断言 5 次 pause pod 都能连接该 Service
		nodeIPs0 := e2enode.GetAddresses(&node0, v1.NodeInternalIP)
		nodeIPs1 := e2enode.GetAddresses(&node1, v1.NodeInternalIP)
		clusterIPAddress := net.JoinHostPort(svc.Spec.ClusterIP, strconv.Itoa(servicePort))
		nodePortAddress0 := net.JoinHostPort(nodeIPs0[0], strconv.Itoa(int(svc.Spec.Ports[0].NodePort)))
		nodePortAddress1 := net.JoinHostPort(nodeIPs1[0], strconv.Itoa(int(svc.Spec.Ports[0].NodePort)))
		// 每 5 秒连接该 Service（endpoint 处于 unready 且 terminating）共 3 次
		for range 5 {
			execHostnameTest(*pausePod1, clusterIPAddress, webserverPod0.Name)
			execHostnameTest(*pausePod1, nodePortAddress0, webserverPod0.Name)
			execHostnameTest(*pausePod1, nodePortAddress1, webserverPod0.Name)
			time.Sleep(5 * time.Second)
		}
	})

	ginkgo.It("should not be able to connect to terminating and unready endpoints if PublishNotReadyAddresses is false", func(ctx context.Context) {
		nodes, err := e2enode.GetBoundedReadySchedulableNodes(ctx, cs, 2)
		framework.ExpectNoError(err)
		nodeCounts := len(nodes.Items)
		if nodeCounts < 2 {
			e2eskipper.Skipf("The test requires at least two ready nodes on %s, but found %v", framework.TestContext.Provider, nodeCounts)
		}
		node0 := nodes.Items[0]
		node1 := nodes.Items[1]

		serviceName := "svc-not-tolerate-unready"
		ns := f.Namespace.Name
		servicePort := 80

		ginkgo.By("creating a NodePort TCP service " + serviceName + " that PublishNotReadyAddresses on" + ns)
		jig := e2eservice.NewTestJig(cs, ns, serviceName)
		svc, err := jig.CreateTCPService(ctx, func(svc *v1.Service) {
			svc.Spec.Ports = []v1.ServicePort{
				{Port: 80, Name: "http", Protocol: v1.ProtocolTCP, TargetPort: intstr.FromInt32(80)},
			}
			svc.Spec.Type = v1.ServiceTypeNodePort
			svc.Spec.PublishNotReadyAddresses = false
		})
		framework.ExpectNoError(err, "failed to create Service")

		ginkgo.By("Creating 1 webserver pod to be part of the TCP service")
		gracePeriod := int64(300)
		webserverPod0 := &v1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name: "webserver-pod",
			},
			Spec: v1.PodSpec{
				Containers: []v1.Container{
					{
						Name:  "agnhost",
						Image: imageutils.GetE2EImage(imageutils.Agnhost),
						Args:  []string{"netexec", "--http-port=80", fmt.Sprintf("--delay-shutdown=%d", gracePeriod)},
						Ports: []v1.ContainerPort{
							{
								ContainerPort: 80,
							},
						},
						ReadinessProbe: &v1.Probe{
							ProbeHandler: v1.ProbeHandler{
								HTTPGet: &v1.HTTPGetAction{
									Path: "/readyz",
									Port: intstr.IntOrString{
										IntVal: int32(80),
									},
									Scheme: v1.URISchemeHTTP,
								},
							},
						},
						LivenessProbe: &v1.Probe{
							ProbeHandler: v1.ProbeHandler{
								HTTPGet: &v1.HTTPGetAction{
									Path: "/healthz",
									Port: intstr.IntOrString{
										IntVal: int32(80),
									},
									Scheme: v1.URISchemeHTTP,
								},
							},
						},
					},
				},
			},
		}
		webserverPod0.Labels = jig.Labels
		webserverPod0.Spec.TerminationGracePeriodSeconds = ptr.To(gracePeriod)
		e2epod.SetNodeSelection(&webserverPod0.Spec, e2epod.NodeSelection{Name: node0.Name})

		_, err = cs.CoreV1().Pods(ns).Create(ctx, webserverPod0, metav1.CreateOptions{})
		framework.ExpectNoError(err, "failed to create pod")
		err = e2epod.WaitTimeoutForPodReadyInNamespace(ctx, f.ClientSet, webserverPod0.Name, f.Namespace.Name, framework.PodStartTimeout)
		if err != nil {
			framework.Failf("error waiting for pod %s to be ready %v", webserverPod0.Name, err)
		}
		err = e2eendpointslice.WaitForEndpointCount(ctx, cs, ns, serviceName, 1)
		framework.ExpectNoError(err)

		ginkgo.By("Creating 1 pause pods that will try to connect to the webservers")
		pausePod1 := e2epod.NewAgnhostPod(ns, "pause-pod-1", nil, nil, nil)
		e2epod.SetNodeSelection(&pausePod1.Spec, e2epod.NodeSelection{Name: node1.Name})

		pausePod1, err = cs.CoreV1().Pods(ns).Create(ctx, pausePod1, metav1.CreateOptions{})
		framework.ExpectNoError(err, "failed to create pod")
		err = e2epod.WaitTimeoutForPodReadyInNamespace(ctx, f.ClientSet, pausePod1.Name, f.Namespace.Name, framework.PodStartTimeout)
		if err != nil {
			framework.Failf("error waiting for pod %s to be ready %v", pausePod1.Name, err)
		}

		// webserver 删除后应停止通过 Service 提供服务，因为：
		//  - 它有 100 秒的 termination grace period
		//  - 它处于 unready 状态，且 PublishNotReadyAddresses 为 false
		err = cs.CoreV1().Pods(ns).Delete(ctx, webserverPod0.Name, metav1.DeleteOptions{})
		framework.ExpectNoError(err)

		// 等待 Pod 变为 unready
		err = e2epod.WaitForPodCondition(ctx, f.ClientSet, f.Namespace.Name, webserverPod0.Name, "pod not ready", framework.PodStartTimeout, func(pod *v1.Pod) (bool, error) {
			return !podutil.IsPodReady(pod), nil
		})
		if err != nil {
			framework.Failf("error waiting for pod %s to be unready %v", webserverPod0.Name, err)
		}

		nodeIPs0 := e2enode.GetAddresses(&node0, v1.NodeInternalIP)
		nodeIPs1 := e2enode.GetAddresses(&node1, v1.NodeInternalIP)
		nodePortAddress0 := net.JoinHostPort(nodeIPs0[0], strconv.Itoa(int(svc.Spec.Ports[0].NodePort)))
		nodePortAddress1 := net.JoinHostPort(nodeIPs1[0], strconv.Itoa(int(svc.Spec.Ports[0].NodePort)))

		// 等待变更传播到两个节点。
		cmd := fmt.Sprintf(`curl -q -s --connect-timeout 5 %s/hostname`, nodePortAddress0)
		if pollErr := wait.PollUntilContextTimeout(ctx, framework.Poll, e2eservice.KubeProxyEndpointLagTimeout, true, func(_ context.Context) (bool, error) {
			_, err := e2eoutput.RunHostCmd(pausePod1.Namespace, pausePod1.Name, cmd)
			if err != nil {
				return true, nil
			}
			return false, nil
		}); pollErr != nil {
			framework.ExpectNoError(pollErr, "pod on node0 still serves traffic")
		}
		cmd = fmt.Sprintf(`curl -q -s --connect-timeout 5 %s/hostname`, nodePortAddress1)
		if pollErr := wait.PollUntilContextTimeout(ctx, framework.Poll, e2eservice.KubeProxyEndpointLagTimeout, true, func(_ context.Context) (bool, error) {
			_, err := e2eoutput.RunHostCmd(pausePod1.Namespace, pausePod1.Name, cmd)
			if err != nil {
				return true, nil
			}
			return false, nil
		}); pollErr != nil {
			framework.ExpectNoError(pollErr, "pod on node1 still serves traffic")
		}

		clusterIPAddress := net.JoinHostPort(svc.Spec.ClusterIP, strconv.Itoa(servicePort))
		// 每 5 秒连接该 Service 共 3 次，预期连接失败
		for range 5 {
			cmd = fmt.Sprintf(`curl -q -s --connect-timeout 5 %s/hostname`, clusterIPAddress)
			_, err := e2eoutput.RunHostCmd(pausePod1.Namespace, pausePod1.Name, cmd)
			gomega.Expect(err).To(gomega.HaveOccurred(), "expected error when trying to connect to cluster IP")

			cmd = fmt.Sprintf(`curl -q -s --connect-timeout 5 %s/hostname`, nodePortAddress0)
			_, err = e2eoutput.RunHostCmd(pausePod1.Namespace, pausePod1.Name, cmd)
			gomega.Expect(err).To(gomega.HaveOccurred(), "expected error when trying to connect to NodePort address")

			cmd = fmt.Sprintf(`curl -q -s --connect-timeout 5 %s/hostname`, nodePortAddress1)
			_, err = e2eoutput.RunHostCmd(pausePod1.Namespace, pausePod1.Name, cmd)
			gomega.Expect(err).To(gomega.HaveOccurred(), "expected error when trying to connect to NodePort address")

			time.Sleep(5 * time.Second)
		}
	})

	/*
		Release: v1.19
		Testname: Service, ClusterIP type, session affinity to ClientIP
		Description: Create a service of type "ClusterIP". Service's sessionAffinity is set to "ClientIP". Service creation MUST be successful by assigning "ClusterIP" to the service.
		Create a Deployment to ensure that 3 pods are running and are targeted by the service to serve hostname of the pod when requests are sent to the service.
		Create another pod to make requests to the service. Service MUST serve the hostname from the same pod of the replica for all consecutive requests.
		Service MUST be reachable over serviceName and the ClusterIP on servicePort.
		[LinuxOnly]: Windows does not support session affinity.
	*/
	framework.ConformanceIt("should have session affinity work for service with type clusterIP [LinuxOnly]", func(ctx context.Context) {
		svc := getServeHostnameService("affinity-clusterip")
		svc.Spec.Type = v1.ServiceTypeClusterIP
		execAffinityTestForNonLBService(ctx, f, cs, svc)
	})

	ginkgo.It("should have session affinity timeout work for service with type clusterIP [LinuxOnly]", func(ctx context.Context) {
		svc := getServeHostnameService("affinity-clusterip-timeout")
		svc.Spec.Type = v1.ServiceTypeClusterIP
		execAffinityTestForSessionAffinityTimeout(ctx, f, cs, svc)
	})

	/*
		Release: v1.19
		Testname: Service, ClusterIP type, session affinity to None
		Description: Create a service of type "ClusterIP". Service's sessionAffinity is set to "ClientIP". Service creation MUST be successful by assigning "ClusterIP" to the service.
		Create a Deployment to ensure that 3 pods are running and are targeted by the service to serve hostname of the pod when requests are sent to the service.
		Create another pod to make requests to the service. Update the service's sessionAffinity to "None". Service update MUST be successful. When a requests are made to the service, it MUST be able serve the hostname from any pod of the replica.
		When service's sessionAffinily is updated back to "ClientIP", service MUST serve the hostname from the same pod of the replica for all consecutive requests.
		Service MUST be reachable over serviceName and the ClusterIP on servicePort.
		[LinuxOnly]: Windows does not support session affinity.
	*/
	framework.ConformanceIt("should be able to switch session affinity for service with type clusterIP [LinuxOnly]", func(ctx context.Context) {
		svc := getServeHostnameService("affinity-clusterip-transition")
		svc.Spec.Type = v1.ServiceTypeClusterIP
		execAffinityTestForNonLBServiceWithTransition(ctx, f, cs, svc)
	})

	/*
		Release: v1.19
		Testname: Service, NodePort type, session affinity to ClientIP
		Description: Create a service of type "NodePort" and provide service port and protocol. Service's sessionAffinity is set to "ClientIP". Service creation MUST be successful by assigning a "ClusterIP" to service and allocating NodePort on all nodes.
		Create a Deployment to ensure that 3 pods are running and are targeted by the service to serve hostname of the pod when a requests are sent to the service.
		Create another pod to make requests to the service on node's IP and NodePort. Service MUST serve the hostname from the same pod of the replica for all consecutive requests.
		Service MUST be reachable over serviceName and the ClusterIP on servicePort. Service MUST also be reachable over node's IP on NodePort.
		[LinuxOnly]: Windows does not support session affinity.
	*/
	framework.ConformanceIt("should have session affinity work for NodePort service [LinuxOnly]", func(ctx context.Context) {
		svc := getServeHostnameService("affinity-nodeport")
		svc.Spec.Type = v1.ServiceTypeNodePort
		execAffinityTestForNonLBService(ctx, f, cs, svc)
	})

	ginkgo.It("should have session affinity timeout work for NodePort service [LinuxOnly]", func(ctx context.Context) {
		svc := getServeHostnameService("affinity-nodeport-timeout")
		svc.Spec.Type = v1.ServiceTypeNodePort
		execAffinityTestForSessionAffinityTimeout(ctx, f, cs, svc)
	})

	/*
		Release: v1.19
		Testname: Service, NodePort type, session affinity to None
		Description: Create a service of type "NodePort" and provide service port and protocol. Service's sessionAffinity is set to "ClientIP". Service creation MUST be successful by assigning a "ClusterIP" to the service and allocating NodePort on all the nodes.
		Create a Deployment to ensure that 3 pods are running and are targeted by the service to serve hostname of the pod when requests are sent to the service.
		Create another pod to make requests to the service. Update the service's sessionAffinity to "None". Service update MUST be successful. When a requests are made to the service on node's IP and NodePort, service MUST be able serve the hostname from any pod of the replica.
		When service's sessionAffinily is updated back to "ClientIP", service MUST serve the hostname from the same pod of the replica for all consecutive requests.
		Service MUST be reachable over serviceName and the ClusterIP on servicePort. Service MUST also be reachable over node's IP on NodePort.
		[LinuxOnly]: Windows does not support session affinity.
	*/
	framework.ConformanceIt("should be able to switch session affinity for NodePort service [LinuxOnly]", func(ctx context.Context) {
		svc := getServeHostnameService("affinity-nodeport-transition")
		svc.Spec.Type = v1.ServiceTypeNodePort
		execAffinityTestForNonLBServiceWithTransition(ctx, f, cs, svc)
	})

	ginkgo.It("should implement service.kubernetes.io/service-proxy-name", func(ctx context.Context) {
		ns := f.Namespace.Name
		numPods, servicePort := 3, defaultServeHostnameServicePort
		serviceProxyNameLabels := map[string]string{"service.kubernetes.io/service-proxy-name": "foo-bar"}

		// 创建两个 Service，分别覆盖两种初始状态以及动态更新：
		// svcDisabled：创建时带标签，始终禁用；提前创建并在后期再次校验，确保它从未可用。
		// svcToggled：创建时不带标签，随后切换标签，并在每一步验证连通性。

		ginkgo.By("creating service-disabled in namespace " + ns)
		svcDisabled := getServeHostnameService("service-proxy-disabled")
		svcDisabled.ObjectMeta.Labels = serviceProxyNameLabels
		_, svcDisabledIP, err := StartServeHostnameService(ctx, cs, svcDisabled, ns, numPods)
		framework.ExpectNoError(err, "failed to create deployment with service: %s in the namespace: %s", svcDisabledIP, ns)

		ginkgo.By("creating service in namespace " + ns)
		svcToggled := getServeHostnameService("service-proxy-toggled")
		podToggledNames, svcToggledIP, err := StartServeHostnameService(ctx, cs, svcToggled, ns, numPods)
		framework.ExpectNoError(err, "failed to create deployment with service: %s in the namespace: %s", svcToggledIP, ns)

		jig := e2eservice.NewTestJig(cs, ns, svcToggled.ObjectMeta.Name)

		ginkgo.By("verifying service is up")
		framework.ExpectNoError(verifyServeHostnameServiceUp(ctx, cs, ns, podToggledNames, svcToggledIP, servicePort))

		ginkgo.By("verifying service-disabled is not up")
		framework.ExpectNoError(verifyServeHostnameServiceDown(ctx, cs, ns, svcDisabledIP, servicePort))

		ginkgo.By("adding service-proxy-name label")
		_, err = jig.UpdateService(ctx, func(svc *v1.Service) {
			svc.ObjectMeta.Labels = serviceProxyNameLabels
		})
		framework.ExpectNoError(err)

		ginkgo.By("verifying service is not up")
		framework.ExpectNoError(verifyServeHostnameServiceDown(ctx, cs, ns, svcToggledIP, servicePort))

		ginkgo.By("removing service-proxy-name annotation")
		_, err = jig.UpdateService(ctx, func(svc *v1.Service) {
			svc.ObjectMeta.Labels = nil
		})
		framework.ExpectNoError(err)

		ginkgo.By("verifying service is up")
		framework.ExpectNoError(verifyServeHostnameServiceUp(ctx, cs, ns, podToggledNames, svcToggledIP, servicePort))

		ginkgo.By("verifying service-disabled is still not up")
		framework.ExpectNoError(verifyServeHostnameServiceDown(ctx, cs, ns, svcDisabledIP, servicePort))
	})

	ginkgo.It("should implement service.kubernetes.io/headless", func(ctx context.Context) {
		var err error

		ginkgo.By("Creating a Service with no selector and no endpoints")
		ns := f.Namespace.Name
		servicePort := 80
		svc := &v1.Service{
			ObjectMeta: metav1.ObjectMeta{
				Name: "example-custom-endpoints",
			},
			Spec: v1.ServiceSpec{
				Ports: []v1.ServicePort{{
					Name:     "port80",
					Port:     int32(servicePort),
					Protocol: v1.ProtocolTCP,
				}},
			},
		}
		svc, err = cs.CoreV1().Services(f.Namespace.Name).Create(ctx, svc, metav1.CreateOptions{})
		framework.ExpectNoError(err, "error creating Service")
		svcIP := svc.Spec.ClusterIP

		ginkgo.By("Creating a Pod to handle HTTP requests")
		podName := "pod-handle-http-request"
		serverPod := e2epod.NewAgnhostPodFromContainers(
			"", podName, nil,
			e2epod.NewAgnhostContainer("container-handle-8090-request", nil, []v1.ContainerPort{{ContainerPort: 8090, Protocol: v1.ProtocolTCP}}, "serve-hostname", "--port", "8090"),
		)
		pod := e2epod.NewPodClient(f).CreateSync(ctx, serverPod)

		addressType := discoveryv1.AddressTypeIPv4
		if framework.TestContext.ClusterIsIPv6() {
			addressType = discoveryv1.AddressTypeIPv6
		}

		ginkgo.By("Creating an EndpointSlice without headless label")
		eps := &discoveryv1.EndpointSlice{
			ObjectMeta: metav1.ObjectMeta{GenerateName: "e2e-custom-slice",
				Labels: map[string]string{
					discoveryv1.LabelServiceName: svc.Name,
					discoveryv1.LabelManagedBy:   "e2e-test" + ns,
				}},
			AddressType: addressType,
			Endpoints: []discoveryv1.Endpoint{{
				Addresses:  []string{pod.Status.PodIP},
				Conditions: discoveryv1.EndpointConditions{Ready: ptr.To(true)},
			}},
			Ports: []discoveryv1.EndpointPort{{
				Name:     ptr.To("port80"),
				Port:     ptr.To[int32](8090),
				Protocol: ptr.To(v1.ProtocolTCP),
			}},
		}

		eps, err = f.ClientSet.DiscoveryV1().EndpointSlices(ns).Create(ctx, eps, metav1.CreateOptions{})
		framework.ExpectNoError(err)

		// 必须能够连接到该 Service
		ginkgo.By("Creating a client pod that will try to connect to the webserver")
		clientPod := e2epod.NewAgnhostPod(ns, "client-pod", nil, nil, nil)
		e2epod.NewPodClient(f).CreateSync(ctx, clientPod)

		ginkgo.By("Test connection to the webserver")
		framework.ExpectNoError(verifyServeHostnameServiceUp(ctx, cs, ns, []string{podName}, svcIP, servicePort))

		ginkgo.By("Updating the EndpointSlice to include headless label")
		eps.ObjectMeta.Labels[v1.IsHeadlessService] = ""
		_, err = f.ClientSet.DiscoveryV1().EndpointSlices(ns).Update(ctx, eps, metav1.UpdateOptions{})
		framework.ExpectNoError(err)

		ginkgo.By("Verify that service is no longer reachable")
		framework.ExpectNoError(verifyServeHostnameServiceDown(ctx, cs, ns, svcIP, servicePort))
	})

	ginkgo.It("should be rejected when no endpoints exist", func(ctx context.Context) {
		namespace := f.Namespace.Name
		serviceName := "no-pods"
		jig := e2eservice.NewTestJig(cs, namespace, serviceName)
		nodes, err := e2enode.GetBoundedReadySchedulableNodes(ctx, cs, e2eservice.MaxNodesForEndpointsTests)
		framework.ExpectNoError(err)
		port := 80

		ginkgo.By("creating a service with no endpoints")
		_, err = jig.CreateTCPServiceWithPort(ctx, nil, int32(port))
		framework.ExpectNoError(err)

		nodeName := nodes.Items[0].Name
		podName := "execpod-noendpoints"

		ginkgo.By(fmt.Sprintf("creating %v on node %v", podName, nodeName))
		execPod := e2epod.CreateExecPodOrFail(ctx, f.ClientSet, namespace, podName, func(pod *v1.Pod) {
			nodeSelection := e2epod.NodeSelection{Name: nodeName}
			e2epod.SetNodeSelection(&pod.Spec, nodeSelection)
		})

		serviceAddress := net.JoinHostPort(serviceName, strconv.Itoa(port))
		framework.Logf("waiting up to %v to connect to %v", e2eservice.KubeProxyEndpointLagTimeout, serviceAddress)
		cmd := fmt.Sprintf("/agnhost connect --timeout=3s %s", serviceAddress)

		ginkgo.By(fmt.Sprintf("hitting service %v from pod %v on node %v", serviceAddress, podName, nodeName))
		expectedErr := "REFUSED"
		if pollErr := wait.PollImmediate(framework.Poll, e2eservice.KubeProxyEndpointLagTimeout, func() (bool, error) {
			_, err := e2eoutput.RunHostCmd(execPod.Namespace, execPod.Name, cmd)

			if err != nil {
				if strings.Contains(err.Error(), expectedErr) {
					framework.Logf("error contained '%s', as expected: %s", expectedErr, err.Error())
					return true, nil
				}
				framework.Logf("error didn't contain '%s', keep trying: %s", expectedErr, err.Error())
				return false, nil
			}
			return true, errors.New("expected connect call to fail")
		}); pollErr != nil {
			framework.ExpectNoError(pollErr)
		}
	})

	// 针对 https://issues.k8s.io/109414 和 https://issues.k8s.io/109718 的回归测试
	ginkgo.It("should be rejected for evicted pods (no endpoints exist)", func(ctx context.Context) {
		namespace := f.Namespace.Name
		serviceName := "evicted-pods"
		jig := e2eservice.NewTestJig(cs, namespace, serviceName)
		nodes, err := e2enode.GetBoundedReadySchedulableNodes(ctx, cs, e2eservice.MaxNodesForEndpointsTests)
		framework.ExpectNoError(err)
		nodeName := nodes.Items[0].Name

		port := 80

		ginkgo.By("creating a service with no endpoints")
		_, err = jig.CreateTCPServiceWithPort(ctx, func(s *v1.Service) {
			// 同时设置 publishNotReadyAddresses 以覆盖边界场景
			s.Spec.PublishNotReadyAddresses = true
		}, int32(port))
		framework.ExpectNoError(err)

		// 在某个节点上创建用于驱逐的 Pod
		ginkgo.By("creating a client pod that is going to be evicted for the service " + serviceName)
		evictedPod := e2epod.NewAgnhostPod(namespace, "evicted-pod", nil, nil, nil)
		// 使用 /dev/urandom 而非 /dev/zero，避免写入被带内联压缩的文件系统折叠掉。
		evictedPod.Spec.Containers[0].Command = []string{"/bin/sh", "-c", "sleep 10; dd if=/dev/urandom of=file bs=1M count=10; sleep 10000"}
		evictedPod.Spec.Containers[0].Name = "evicted-pod"
		evictedPod.Spec.Containers[0].Resources = v1.ResourceRequirements{
			Limits: v1.ResourceList{"ephemeral-storage": resource.MustParse("5Mi")},
		}
		e2epod.NewPodClient(f).Create(ctx, evictedPod)
		err = e2epod.WaitForPodTerminatedInNamespace(ctx, f.ClientSet, evictedPod.Name, "Evicted", f.Namespace.Name)
		if err != nil {
			framework.Failf("error waiting for pod to be evicted: %v", err)
		}

		podName := "execpod-evictedpods"
		ginkgo.By(fmt.Sprintf("creating %v on node %v", podName, nodeName))
		execPod := e2epod.CreateExecPodOrFail(ctx, f.ClientSet, namespace, podName, func(pod *v1.Pod) {
			nodeSelection := e2epod.NodeSelection{Name: nodeName}
			e2epod.SetNodeSelection(&pod.Spec, nodeSelection)
		})

		err = e2eendpointslice.WaitForEndpointCount(ctx, cs, namespace, serviceName, 0)
		framework.ExpectNoError(err)

		serviceAddress := net.JoinHostPort(serviceName, strconv.Itoa(port))
		framework.Logf("waiting up to %v to connect to %v", e2eservice.KubeProxyEndpointLagTimeout, serviceAddress)
		cmd := fmt.Sprintf("/agnhost connect --timeout=3s %s", serviceAddress)

		ginkgo.By(fmt.Sprintf("hitting service %v from pod %v on node %v expected to be refused", serviceAddress, podName, nodeName))
		expectedErr := "REFUSED"
		if pollErr := wait.PollImmediate(framework.Poll, e2eservice.KubeProxyEndpointLagTimeout, func() (bool, error) {
			_, err := e2eoutput.RunHostCmd(execPod.Namespace, execPod.Name, cmd)

			if err != nil {
				if strings.Contains(err.Error(), expectedErr) {
					framework.Logf("error contained '%s', as expected: %s", expectedErr, err.Error())
					return true, nil
				}
				framework.Logf("error didn't contain '%s', keep trying: %s", expectedErr, err.Error())
				return false, nil
			}
			return true, errors.New("expected connect call to fail")
		}); pollErr != nil {
			framework.ExpectNoError(pollErr)
		}
	})

	ginkgo.It("should respect internalTrafficPolicy=Local Pod to Pod", func(ctx context.Context) {
		// windows kube-proxy 尚未支持此特性
		// TODO: 当基于 Windows 的代理实现 internalTrafficPolicy 后移除此跳过逻辑
		e2eskipper.SkipIfNodeOSDistroIs("windows")

		nodes, err := e2enode.GetBoundedReadySchedulableNodes(ctx, cs, 2)
		framework.ExpectNoError(err)
		nodeCounts := len(nodes.Items)
		if nodeCounts < 2 {
			e2eskipper.Skipf("The test requires at least two ready nodes on %s, but found %v", framework.TestContext.Provider, nodeCounts)
		}
		node0 := nodes.Items[0]
		node1 := nodes.Items[1]

		serviceName := "svc-itp"
		ns := f.Namespace.Name
		servicePort := 80

		ginkgo.By("creating a TCP service " + serviceName + " with type=ClusterIP and internalTrafficPolicy=Local in namespace " + ns)
		local := v1.ServiceInternalTrafficPolicyLocal
		jig := e2eservice.NewTestJig(cs, ns, serviceName)
		svc, err := jig.CreateTCPService(ctx, func(svc *v1.Service) {
			svc.Spec.Ports = []v1.ServicePort{
				{Port: 80, Name: "http", Protocol: v1.ProtocolTCP, TargetPort: intstr.FromInt32(80)},
			}
			svc.Spec.InternalTrafficPolicy = &local
		})
		framework.ExpectNoError(err)

		ginkgo.By("Creating 1 webserver pod to be part of the TCP service")
		webserverPod0 := e2epod.NewAgnhostPod(ns, "echo-hostname-0", nil, nil, nil, "netexec", "--http-port", strconv.Itoa(servicePort))
		webserverPod0.Labels = jig.Labels
		e2epod.SetNodeSelection(&webserverPod0.Spec, e2epod.NodeSelection{Name: node0.Name})

		_, err = cs.CoreV1().Pods(ns).Create(ctx, webserverPod0, metav1.CreateOptions{})
		framework.ExpectNoError(err)
		framework.ExpectNoError(e2epod.WaitTimeoutForPodReadyInNamespace(ctx, f.ClientSet, webserverPod0.Name, f.Namespace.Name, framework.PodStartTimeout))

		err = e2eendpointslice.WaitForEndpointCount(ctx, cs, ns, serviceName, 1)
		framework.ExpectNoError(err)

		ginkgo.By("Creating 2 pause pods that will try to connect to the webservers")
		pausePod0 := e2epod.NewAgnhostPod(ns, "pause-pod-0", nil, nil, nil)
		e2epod.SetNodeSelection(&pausePod0.Spec, e2epod.NodeSelection{Name: node0.Name})

		pausePod0, err = cs.CoreV1().Pods(ns).Create(ctx, pausePod0, metav1.CreateOptions{})
		framework.ExpectNoError(err)
		framework.ExpectNoError(e2epod.WaitTimeoutForPodReadyInNamespace(ctx, f.ClientSet, pausePod0.Name, f.Namespace.Name, framework.PodStartTimeout))

		pausePod1 := e2epod.NewAgnhostPod(ns, "pause-pod-1", nil, nil, nil)
		e2epod.SetNodeSelection(&pausePod1.Spec, e2epod.NodeSelection{Name: node1.Name})

		pausePod1, err = cs.CoreV1().Pods(ns).Create(ctx, pausePod1, metav1.CreateOptions{})
		framework.ExpectNoError(err)
		framework.ExpectNoError(e2epod.WaitTimeoutForPodReadyInNamespace(ctx, f.ClientSet, pausePod1.Name, f.Namespace.Name, framework.PodStartTimeout))

		// 连续 5 次验证第一个 pause Pod 可以在本地连接到 Service，第二个则超时报错
		serviceAddress := net.JoinHostPort(svc.Spec.ClusterIP, strconv.Itoa(servicePort))
		for range 5 {
			// 第一个 pause Pod 应与 webserver 位于同一节点，因此能通过 clusterIP 连接本地 Pod
			execHostnameTest(*pausePod0, serviceAddress, webserverPod0.Name)

			// 第二个 pause Pod 位于不同节点，因此每次都应遇到连接错误
			cmd := fmt.Sprintf(`curl -q -s --connect-timeout 5 %s/hostname`, serviceAddress)
			_, err := e2eoutput.RunHostCmd(pausePod1.Namespace, pausePod1.Name, cmd)
			gomega.Expect(err).To(gomega.HaveOccurred(), "expected error when trying to connect to cluster IP")
		}
	})

	ginkgo.It("should respect internalTrafficPolicy=Local Pod (hostNetwork: true) to Pod", func(ctx context.Context) {
		// windows kube-proxy 尚未支持此特性
		// TODO: 当基于 Windows 的代理实现 internalTrafficPolicy 后移除此跳过逻辑
		e2eskipper.SkipIfNodeOSDistroIs("windows")

		nodes, err := e2enode.GetBoundedReadySchedulableNodes(ctx, cs, 2)
		framework.ExpectNoError(err)
		nodeCounts := len(nodes.Items)
		if nodeCounts < 2 {
			e2eskipper.Skipf("The test requires at least two ready nodes on %s, but found %v", framework.TestContext.Provider, nodeCounts)
		}
		node0 := nodes.Items[0]
		node1 := nodes.Items[1]

		serviceName := "svc-itp"
		ns := f.Namespace.Name
		servicePort := 8000

		ginkgo.By("creating a TCP service " + serviceName + " with type=ClusterIP and internalTrafficPolicy=Local in namespace " + ns)
		local := v1.ServiceInternalTrafficPolicyLocal
		jig := e2eservice.NewTestJig(cs, ns, serviceName)
		svc, err := jig.CreateTCPService(ctx, func(svc *v1.Service) {
			svc.Spec.Ports = []v1.ServicePort{
				{Port: 8000, Name: "http", Protocol: v1.ProtocolTCP, TargetPort: intstr.FromInt32(8000)},
			}
			svc.Spec.InternalTrafficPolicy = &local
		})
		framework.ExpectNoError(err)

		ginkgo.By("Creating 1 webserver pod to be part of the TCP service")
		webserverPod0 := e2epod.NewAgnhostPod(ns, "echo-hostname-0", nil, nil, nil, "netexec", "--http-port", strconv.Itoa(servicePort))
		webserverPod0.Labels = jig.Labels
		e2epod.SetNodeSelection(&webserverPod0.Spec, e2epod.NodeSelection{Name: node0.Name})

		_, err = cs.CoreV1().Pods(ns).Create(ctx, webserverPod0, metav1.CreateOptions{})
		framework.ExpectNoError(err)
		framework.ExpectNoError(e2epod.WaitTimeoutForPodReadyInNamespace(ctx, f.ClientSet, webserverPod0.Name, f.Namespace.Name, framework.PodStartTimeout))

		err = e2eendpointslice.WaitForEndpointCount(ctx, cs, ns, serviceName, 1)
		framework.ExpectNoError(err)

		ginkgo.By("Creating 2 pause pods that will try to connect to the webservers")
		pausePod0 := e2epod.NewAgnhostPod(ns, "pause-pod-0", nil, nil, nil)
		pausePod0.Spec.HostNetwork = true
		e2epod.SetNodeSelection(&pausePod0.Spec, e2epod.NodeSelection{Name: node0.Name})

		pausePod0, err = cs.CoreV1().Pods(ns).Create(ctx, pausePod0, metav1.CreateOptions{})
		framework.ExpectNoError(err)
		framework.ExpectNoError(e2epod.WaitTimeoutForPodReadyInNamespace(ctx, f.ClientSet, pausePod0.Name, f.Namespace.Name, framework.PodStartTimeout))

		pausePod1 := e2epod.NewAgnhostPod(ns, "pause-pod-1", nil, nil, nil)
		pausePod1.Spec.HostNetwork = true
		e2epod.SetNodeSelection(&pausePod1.Spec, e2epod.NodeSelection{Name: node1.Name})

		pausePod1, err = cs.CoreV1().Pods(ns).Create(ctx, pausePod1, metav1.CreateOptions{})
		framework.ExpectNoError(err)
		framework.ExpectNoError(e2epod.WaitTimeoutForPodReadyInNamespace(ctx, f.ClientSet, pausePod1.Name, f.Namespace.Name, framework.PodStartTimeout))

		// 连续 5 次验证第一个 pause Pod 可以在本地连接到 Service，第二个则超时报错
		serviceAddress := net.JoinHostPort(svc.Spec.ClusterIP, strconv.Itoa(servicePort))
		for range 5 {
			// 第一个 pause Pod 应与 webserver 位于同一节点，因此能通过 clusterIP 连接本地 Pod
			execHostnameTest(*pausePod0, serviceAddress, webserverPod0.Name)

			// 第二个 pause Pod 位于不同节点，因此每次都应遇到连接错误
			cmd := fmt.Sprintf(`curl -q -s --connect-timeout 5 %s/hostname`, serviceAddress)
			_, err := e2eoutput.RunHostCmd(pausePod1.Namespace, pausePod1.Name, cmd)
			gomega.Expect(err).To(gomega.HaveOccurred(), "expected error when trying to connect to cluster IP")
		}
	})

	ginkgo.It("should respect internalTrafficPolicy=Local Pod and Node, to Pod (hostNetwork: true)", func(ctx context.Context) {
		// windows kube-proxy 尚未支持此特性
		// TODO: 当基于 Windows 的代理实现 internalTrafficPolicy 后移除此跳过逻辑
		e2eskipper.SkipIfNodeOSDistroIs("windows")

		nodes, err := e2enode.GetBoundedReadySchedulableNodes(ctx, cs, 2)
		framework.ExpectNoError(err)
		nodeCounts := len(nodes.Items)
		if nodeCounts < 2 {
			e2eskipper.Skipf("The test requires at least two ready nodes on %s, but found %v", framework.TestContext.Provider, nodeCounts)
		}
		node0 := nodes.Items[0]
		node1 := nodes.Items[1]

		serviceName := "svc-itp"
		ns := f.Namespace.Name
		servicePort := 80
		// 若 Pod 无法绑定该端口，会因使用 hostNetwork 而启动失败，进而导致测试失败；
		// 选用不常见端口可降低这种概率。
		endpointPort := 10180

		ginkgo.By("creating a TCP service " + serviceName + " with type=ClusterIP and internalTrafficPolicy=Local in namespace " + ns)
		local := v1.ServiceInternalTrafficPolicyLocal
		jig := e2eservice.NewTestJig(cs, ns, serviceName)
		svc, err := jig.CreateTCPService(ctx, func(svc *v1.Service) {
			svc.Spec.Ports = []v1.ServicePort{
				{Port: 80, Name: "http", Protocol: v1.ProtocolTCP, TargetPort: intstr.FromInt(endpointPort)},
			}
			svc.Spec.InternalTrafficPolicy = &local
		})
		framework.ExpectNoError(err)

		ginkgo.By("Creating 1 webserver pod to be part of the TCP service")
		webserverPod0 := e2epod.NewAgnhostPod(ns, "echo-hostname-0", nil, nil, nil, "netexec", "--http-port", strconv.Itoa(endpointPort), "--udp-port", strconv.Itoa(endpointPort))
		webserverPod0.Labels = jig.Labels
		webserverPod0.Spec.HostNetwork = true
		webserverPod0.Spec.Containers[0].Env = append(webserverPod0.Spec.Containers[0].Env, v1.EnvVar{
			Name: "NODE_NAME",
			ValueFrom: &v1.EnvVarSource{
				FieldRef: &v1.ObjectFieldSelector{FieldPath: "spec.nodeName"},
			},
		})
		e2epod.SetNodeSelection(&webserverPod0.Spec, e2epod.NodeSelection{Name: node0.Name})

		_, err = cs.CoreV1().Pods(ns).Create(ctx, webserverPod0, metav1.CreateOptions{})
		framework.ExpectNoError(err)
		framework.ExpectNoError(e2epod.WaitTimeoutForPodReadyInNamespace(ctx, f.ClientSet, webserverPod0.Name, f.Namespace.Name, framework.PodStartTimeout))

		err = e2eendpointslice.WaitForEndpointCount(ctx, cs, ns, serviceName, 1)
		framework.ExpectNoError(err)

		ginkgo.By("Creating 2 pause pods that will try to connect to the webserver")
		pausePod0 := e2epod.NewAgnhostPod(ns, "pause-pod-0", nil, nil, nil)
		e2epod.SetNodeSelection(&pausePod0.Spec, e2epod.NodeSelection{Name: node0.Name})

		pausePod0, err = cs.CoreV1().Pods(ns).Create(ctx, pausePod0, metav1.CreateOptions{})
		framework.ExpectNoError(err)
		framework.ExpectNoError(e2epod.WaitTimeoutForPodReadyInNamespace(ctx, f.ClientSet, pausePod0.Name, f.Namespace.Name, framework.PodStartTimeout))

		pausePod1 := e2epod.NewAgnhostPod(ns, "pause-pod-1", nil, nil, nil)
		e2epod.SetNodeSelection(&pausePod1.Spec, e2epod.NodeSelection{Name: node1.Name})

		pausePod1, err = cs.CoreV1().Pods(ns).Create(ctx, pausePod1, metav1.CreateOptions{})
		framework.ExpectNoError(err)
		framework.ExpectNoError(e2epod.WaitTimeoutForPodReadyInNamespace(ctx, f.ClientSet, pausePod1.Name, f.Namespace.Name, framework.PodStartTimeout))

		// 连续 5 次验证第一个 pause Pod 可以在本地连接到 Service，第二个则超时报错
		serviceAddress := net.JoinHostPort(svc.Spec.ClusterIP, strconv.Itoa(servicePort))
		for range 5 {
			// 第一个 pause Pod 应与 webserver 位于同一节点，因此能通过 clusterIP 连接本地 Pod
			// 后端 Pod 使用 hostNetwork，因此期望值是节点名
			execNodenameTest(*pausePod0, serviceAddress, node0.Name)

			// 第二个 pause Pod 位于不同节点，因此每次都应遇到连接错误
			cmd := fmt.Sprintf(`curl -q -s --connect-timeout 5 %s/hostname`, serviceAddress)
			_, err := e2eoutput.RunHostCmd(pausePod1.Namespace, pausePod1.Name, cmd)
			gomega.Expect(err).To(gomega.HaveOccurred(), "expected error when trying to connect to cluster IP")
		}

		ginkgo.By("Creating 2 pause hostNetwork pods that will try to connect to the webserver")
		pausePod2 := e2epod.NewAgnhostPod(ns, "pause-pod-2", nil, nil, nil)
		pausePod2.Spec.HostNetwork = true
		e2epod.SetNodeSelection(&pausePod2.Spec, e2epod.NodeSelection{Name: node0.Name})

		pausePod2, err = cs.CoreV1().Pods(ns).Create(ctx, pausePod2, metav1.CreateOptions{})
		framework.ExpectNoError(err)
		framework.ExpectNoError(e2epod.WaitTimeoutForPodReadyInNamespace(ctx, f.ClientSet, pausePod2.Name, f.Namespace.Name, framework.PodStartTimeout))

		pausePod3 := e2epod.NewAgnhostPod(ns, "pause-pod-3", nil, nil, nil)
		pausePod3.Spec.HostNetwork = true
		e2epod.SetNodeSelection(&pausePod3.Spec, e2epod.NodeSelection{Name: node1.Name})

		pausePod3, err = cs.CoreV1().Pods(ns).Create(ctx, pausePod3, metav1.CreateOptions{})
		framework.ExpectNoError(err)
		framework.ExpectNoError(e2epod.WaitTimeoutForPodReadyInNamespace(ctx, f.ClientSet, pausePod3.Name, f.Namespace.Name, framework.PodStartTimeout))

		// 连续 5 次验证第一个 pause Pod 可以在本地连接到 Service，第二个则超时报错
		for range 5 {
			// 第一个 pause Pod 应与 webserver 位于同一节点，因此能通过 clusterIP 连接本地 Pod
			// 后端 Pod 使用 hostNetwork，因此期望值是节点名
			execNodenameTest(*pausePod2, serviceAddress, node0.Name)

			// 第二个 pause Pod 位于不同节点，因此每次都应遇到连接错误
			cmd := fmt.Sprintf(`curl -q -s --connect-timeout 5 %s/hostname`, serviceAddress)
			_, err := e2eoutput.RunHostCmd(pausePod3.Namespace, pausePod3.Name, cmd)
			gomega.Expect(err).To(gomega.HaveOccurred(), "expected error when trying to connect to cluster IP")
		}
	})

	ginkgo.It("should support externalTrafficPolicy=Local for type=NodePort", func(ctx context.Context) {
		e2eskipper.SkipUnlessNodeCountIsAtLeast(2)

		namespace := f.Namespace.Name
		serviceName := "external-local-nodeport"
		jig := e2eservice.NewTestJig(cs, namespace, serviceName)

		ginkgo.By("creating the service")
		svc, err := jig.CreateOnlyLocalNodePortService(ctx, true)
		framework.ExpectNoError(err, "creating the service")
		tcpNodePort := int(svc.Spec.Ports[0].NodePort)
		nodePortStr := fmt.Sprintf("%d", tcpNodePort)
		framework.Logf("NodePort is %s", nodePortStr)

		// 获取（唯一的）endpoint 所在节点名和 IP
		var endpointNodeName, endpointNodeIP string
		endpointsNodeMap, err := getEndpointNodesWithInternalIP(ctx, jig)
		framework.ExpectNoError(err, "fetching endpoint node IPs")
		for node, nodeIP := range endpointsNodeMap {
			framework.Logf("endpoint is on node %s (%s)", node, nodeIP)
			endpointNodeName = node
			endpointNodeIP = nodeIP
			break
		}

		// 需确保 endpoint 与执行 exec 的 Pod 不在同一节点，这样流量的源 IP 才是节点的“公网” IP；
		// 同节点 Pod 间流量可能会使用 "docker0" 之类的地址。
		ginkgo.By("creating a HostNetwork exec pod on a different node")
		execPod := launchHostExecPod(ctx, cs, namespace, "hostexec", &endpointNodeName)
		framework.Logf("execPod IP is %q", execPod.Status.PodIP)

		ginkgo.By("connecting from the execpod to the NodePort on the endpoint's node")
		cmd := fmt.Sprintf("curl -g -q -s --connect-timeout 3 http://%s/clientip", net.JoinHostPort(endpointNodeIP, nodePortStr))
		var clientIP string
		err = wait.PollImmediate(framework.Poll, e2eservice.KubeProxyLagTimeout, func() (bool, error) {
			clientIPPort, err := e2eoutput.RunHostCmd(execPod.Namespace, execPod.Name, cmd)
			if err != nil {
				framework.Logf("error connecting: %v", err)
				return false, nil
			}
			clientIP, _, err = net.SplitHostPort(clientIPPort)
			framework.ExpectNoError(err, "parsing clientip output")
			return true, nil
		})
		framework.ExpectNoError(err, "connecting to nodeport service")
		if clientIP != execPod.Status.PodIP {
			framework.Failf("Source IP %s is not the client IP", clientIP)
		}
	})

	ginkgo.It("should fail health check node port if there are only terminating endpoints", func(ctx context.Context) {
		// windows kube-proxy 尚未支持此特性
		e2eskipper.SkipIfNodeOSDistroIs("windows")

		nodes, err := e2enode.GetBoundedReadySchedulableNodes(ctx, cs, 2)
		framework.ExpectNoError(err)
		nodeCounts := len(nodes.Items)
		if nodeCounts < 2 {
			e2eskipper.Skipf("The test requires at least two ready nodes on %s, but found %v", framework.TestContext.Provider, nodeCounts)
		}
		node0 := nodes.Items[0]

		serviceName := "svc-proxy-terminating"
		ns := f.Namespace.Name
		servicePort := 80

		ginkgo.By("creating a TCP service " + serviceName + " where all pods are terminating" + ns)
		jig := e2eservice.NewTestJig(cs, ns, serviceName)
		svc, err := jig.CreateTCPService(ctx, func(svc *v1.Service) {
			svc.Spec.Ports = []v1.ServicePort{
				{Port: 80, Name: "http", Protocol: v1.ProtocolTCP, TargetPort: intstr.FromInt32(80)},
			}
			svc.Spec.Type = v1.ServiceTypeLoadBalancer
			svc.Spec.ExternalTrafficPolicy = v1.ServiceExternalTrafficPolicyLocal
		})
		framework.ExpectNoError(err)

		ginkgo.By("Creating 1 webserver pod to be part of the TCP service")
		webserverPod0 := e2epod.NewAgnhostPod(ns, "echo-hostname-0", nil, nil, nil, "netexec", "--http-port", strconv.Itoa(servicePort), "--delay-shutdown", "100")
		webserverPod0.Labels = jig.Labels
		webserverPod0.Spec.TerminationGracePeriodSeconds = ptr.To[int64](100)
		e2epod.SetNodeSelection(&webserverPod0.Spec, e2epod.NodeSelection{Name: node0.Name})

		_, err = cs.CoreV1().Pods(ns).Create(ctx, webserverPod0, metav1.CreateOptions{})
		framework.ExpectNoError(err)
		framework.ExpectNoError(e2epod.WaitTimeoutForPodReadyInNamespace(ctx, f.ClientSet, webserverPod0.Name, f.Namespace.Name, framework.PodStartTimeout))
		err = e2eendpointslice.WaitForEndpointCount(ctx, cs, ns, serviceName, 1)
		framework.ExpectNoError(err)

		pausePod0 := e2epod.NewAgnhostPod(ns, "pause-pod-0", nil, nil, nil)
		e2epod.SetNodeSelection(&pausePod0.Spec, e2epod.NodeSelection{Name: node0.Name})

		pausePod0, err = cs.CoreV1().Pods(ns).Create(ctx, pausePod0, metav1.CreateOptions{})
		framework.ExpectNoError(err)
		framework.ExpectNoError(e2epod.WaitTimeoutForPodReadyInNamespace(ctx, f.ClientSet, pausePod0.Name, f.Namespace.Name, framework.PodStartTimeout))

		nodeIPs := e2enode.GetAddresses(&node0, v1.NodeInternalIP)
		healthCheckNodePortAddr := net.JoinHostPort(nodeIPs[0], strconv.Itoa(int(svc.Spec.HealthCheckNodePort)))
		// 验证存在就绪 endpoint 时，kube-proxy 的 health check node port 返回 200
		err = wait.PollImmediate(time.Second, time.Minute, func() (bool, error) {
			cmd := fmt.Sprintf(`curl -s -o /dev/null -w "%%{http_code}" --max-time 5 http://%s/healthz`,
				healthCheckNodePortAddr)
			out, err := e2eoutput.RunHostCmd(pausePod0.Namespace, pausePod0.Name, cmd)
			if err != nil {
				framework.Logf("unexpected error trying to connect to nodeport %s : %v",
					healthCheckNodePortAddr,
					err)
				return false, nil
			}

			expectedOut := "200"
			if out != expectedOut {
				framework.Logf("expected output: %s , got %s", expectedOut, out)
				return false, nil
			}
			return true, nil
		})
		framework.ExpectNoError(err)

		// 删除后 webserver 仍应通过 Service 提供流量，尽管 health check node port 应返回 503
		ginkgo.By("Terminating the webserver pod")
		err = cs.CoreV1().Pods(ns).Delete(ctx, webserverPod0.Name, metav1.DeleteOptions{})
		framework.ExpectNoError(err)

		// 验证没有就绪 endpoint 时，kube-proxy 的 health check node port 返回 503
		err = wait.PollUntilContextTimeout(ctx, time.Second, time.Minute, true, func(ctx context.Context) (bool, error) {
			cmd := fmt.Sprintf(`curl -s -o /dev/null -w "%%{http_code}" --max-time 5 http://%s/healthz`, healthCheckNodePortAddr)
			out, err := e2eoutput.RunHostCmd(pausePod0.Namespace, pausePod0.Name, cmd)
			if err != nil {
				framework.Logf("unexpected error trying to connect to nodeport %s : %v", healthCheckNodePortAddr, err)
				return false, nil
			}

			expectedOut := "503"
			if out != expectedOut {
				framework.Logf("expected output: %s , got %s", expectedOut, out)
				return false, nil
			}
			return true, nil
		})
		framework.ExpectNoError(err)

		// 同时验证 health check node port 显示 0 个 endpoint 并返回 503 期间，该 endpoint 仍能承载流量。
		nodePortAddress := net.JoinHostPort(nodeIPs[0], strconv.Itoa(int(svc.Spec.Ports[0].NodePort)))
		execHostnameTest(*pausePod0, nodePortAddress, webserverPod0.Name)
	})

	ginkgo.It("should fallback to terminating endpoints when there are no ready endpoints with internalTrafficPolicy=Cluster", func(ctx context.Context) {
		// windows kube-proxy 尚未支持此特性
		e2eskipper.SkipIfNodeOSDistroIs("windows")

		nodes, err := e2enode.GetBoundedReadySchedulableNodes(ctx, cs, 2)
		framework.ExpectNoError(err)
		nodeCounts := len(nodes.Items)
		if nodeCounts < 2 {
			e2eskipper.Skipf("The test requires at least two ready nodes on %s, but found %v", framework.TestContext.Provider, nodeCounts)
		}
		node0 := nodes.Items[0]
		node1 := nodes.Items[1]

		serviceName := "svc-proxy-terminating"
		ns := f.Namespace.Name
		servicePort := 80

		ginkgo.By("creating a TCP service " + serviceName + " where all pods are terminating" + ns)
		jig := e2eservice.NewTestJig(cs, ns, serviceName)
		svc, err := jig.CreateTCPService(ctx, func(svc *v1.Service) {
			svc.Spec.Ports = []v1.ServicePort{
				{Port: 80, Name: "http", Protocol: v1.ProtocolTCP, TargetPort: intstr.FromInt32(80)},
			}
		})
		framework.ExpectNoError(err)

		ginkgo.By("Creating 1 webserver pod to be part of the TCP service")
		webserverPod0 := e2epod.NewAgnhostPod(ns, "echo-hostname-0", nil, nil, nil, "netexec", "--http-port", strconv.Itoa(servicePort), "--delay-shutdown", "100")
		webserverPod0.Labels = jig.Labels
		webserverPod0.Spec.TerminationGracePeriodSeconds = ptr.To[int64](100)
		e2epod.SetNodeSelection(&webserverPod0.Spec, e2epod.NodeSelection{Name: node0.Name})

		_, err = cs.CoreV1().Pods(ns).Create(ctx, webserverPod0, metav1.CreateOptions{})
		framework.ExpectNoError(err)
		framework.ExpectNoError(e2epod.WaitTimeoutForPodReadyInNamespace(ctx, f.ClientSet, webserverPod0.Name, f.Namespace.Name, framework.PodStartTimeout))
		err = e2eendpointslice.WaitForEndpointCount(ctx, cs, ns, serviceName, 1)
		framework.ExpectNoError(err)

		ginkgo.By("Creating 2 pause pods that will try to connect to the webservers")
		pausePod0 := e2epod.NewAgnhostPod(ns, "pause-pod-0", nil, nil, nil)
		e2epod.SetNodeSelection(&pausePod0.Spec, e2epod.NodeSelection{Name: node0.Name})

		pausePod0, err = cs.CoreV1().Pods(ns).Create(ctx, pausePod0, metav1.CreateOptions{})
		framework.ExpectNoError(err)
		framework.ExpectNoError(e2epod.WaitTimeoutForPodReadyInNamespace(ctx, f.ClientSet, pausePod0.Name, f.Namespace.Name, framework.PodStartTimeout))

		pausePod1 := e2epod.NewAgnhostPod(ns, "pause-pod-1", nil, nil, nil)
		e2epod.SetNodeSelection(&pausePod1.Spec, e2epod.NodeSelection{Name: node1.Name})

		pausePod1, err = cs.CoreV1().Pods(ns).Create(ctx, pausePod1, metav1.CreateOptions{})
		framework.ExpectNoError(err)
		framework.ExpectNoError(e2epod.WaitTimeoutForPodReadyInNamespace(ctx, f.ClientSet, pausePod1.Name, f.Namespace.Name, framework.PodStartTimeout))

		// 删除后 webserver 仍应通过 Service 提供流量，因为：
		//  - 它有 100 秒的终止宽限期
		//  - 它是唯一的就绪 endpoint
		err = cs.CoreV1().Pods(ns).Delete(ctx, webserverPod0.Name, metav1.DeleteOptions{})
		framework.ExpectNoError(err)

		// 连续 5 次验证所有 endpoint 都在终止时，本地和远端 Pod 仍能连接到 Service
		serviceAddress := net.JoinHostPort(svc.Spec.ClusterIP, strconv.Itoa(servicePort))
		for range 5 {
			// 存在一个 internalTrafficPolicy=Cluster 的 Service，唯一 endpoint webserver0 位于 node0 且正在终止。
			// pausePod0 和 pausePod1 分别位于 node0 和 node1。
			// pausePod0 -> Service clusterIP 成功，因为流量策略为 "Cluster"
			// pausePod1 -> Service clusterIP 成功，因为流量策略为 "Cluster"
			execHostnameTest(*pausePod0, serviceAddress, webserverPod0.Name)
			execHostnameTest(*pausePod1, serviceAddress, webserverPod0.Name)

			time.Sleep(5 * time.Second)
		}
	})

	ginkgo.It("should fallback to local terminating endpoints when there are no ready endpoints with internalTrafficPolicy=Local", func(ctx context.Context) {
		// windows kube-proxy 尚未支持此特性
		e2eskipper.SkipIfNodeOSDistroIs("windows")

		nodes, err := e2enode.GetBoundedReadySchedulableNodes(ctx, cs, 2)
		framework.ExpectNoError(err)
		nodeCounts := len(nodes.Items)
		if nodeCounts < 2 {
			e2eskipper.Skipf("The test requires at least two ready nodes on %s, but found %v", framework.TestContext.Provider, nodeCounts)
		}
		node0 := nodes.Items[0]
		node1 := nodes.Items[1]

		serviceName := "svc-proxy-terminating"
		ns := f.Namespace.Name
		servicePort := 80

		ginkgo.By("creating a TCP service " + serviceName + " where all pods are terminating" + ns)
		jig := e2eservice.NewTestJig(cs, ns, serviceName)
		local := v1.ServiceInternalTrafficPolicyLocal
		svc, err := jig.CreateTCPService(ctx, func(svc *v1.Service) {
			svc.Spec.Ports = []v1.ServicePort{
				{Port: 80, Name: "http", Protocol: v1.ProtocolTCP, TargetPort: intstr.FromInt32(80)},
			}
			svc.Spec.InternalTrafficPolicy = &local
		})
		framework.ExpectNoError(err)

		ginkgo.By("Creating 1 webserver pod to be part of the TCP service")
		webserverPod0 := e2epod.NewAgnhostPod(ns, "echo-hostname-0", nil, nil, nil, "netexec", "--http-port", strconv.Itoa(servicePort), "--delay-shutdown", "100")
		webserverPod0.Labels = jig.Labels
		webserverPod0.Spec.TerminationGracePeriodSeconds = ptr.To[int64](100)
		e2epod.SetNodeSelection(&webserverPod0.Spec, e2epod.NodeSelection{Name: node0.Name})

		_, err = cs.CoreV1().Pods(ns).Create(ctx, webserverPod0, metav1.CreateOptions{})
		framework.ExpectNoError(err)
		framework.ExpectNoError(e2epod.WaitTimeoutForPodReadyInNamespace(ctx, f.ClientSet, webserverPod0.Name, f.Namespace.Name, framework.PodStartTimeout))
		err = e2eendpointslice.WaitForEndpointCount(ctx, cs, ns, serviceName, 1)
		framework.ExpectNoError(err)

		ginkgo.By("Creating 2 pause pods that will try to connect to the webservers")
		pausePod0 := e2epod.NewAgnhostPod(ns, "pause-pod-0", nil, nil, nil)
		e2epod.SetNodeSelection(&pausePod0.Spec, e2epod.NodeSelection{Name: node0.Name})

		pausePod0, err = cs.CoreV1().Pods(ns).Create(ctx, pausePod0, metav1.CreateOptions{})
		framework.ExpectNoError(err)
		framework.ExpectNoError(e2epod.WaitTimeoutForPodReadyInNamespace(ctx, f.ClientSet, pausePod0.Name, f.Namespace.Name, framework.PodStartTimeout))

		pausePod1 := e2epod.NewAgnhostPod(ns, "pause-pod-1", nil, nil, nil)
		e2epod.SetNodeSelection(&pausePod1.Spec, e2epod.NodeSelection{Name: node1.Name})

		pausePod1, err = cs.CoreV1().Pods(ns).Create(ctx, pausePod1, metav1.CreateOptions{})
		framework.ExpectNoError(err)
		framework.ExpectNoError(e2epod.WaitTimeoutForPodReadyInNamespace(ctx, f.ClientSet, pausePod1.Name, f.Namespace.Name, framework.PodStartTimeout))

		// 删除后 webserver 仍应通过 Service 提供流量，因为：
		//  - 它有 100 秒的终止宽限期
		//  - 它是唯一的就绪 endpoint
		err = cs.CoreV1().Pods(ns).Delete(ctx, webserverPod0.Name, metav1.DeleteOptions{})
		framework.ExpectNoError(err)

		// 连续 5 次验证第一个 pause Pod 可以在本地连接到 Service，第二个则超时报错
		serviceAddress := net.JoinHostPort(svc.Spec.ClusterIP, strconv.Itoa(servicePort))
		for range 5 {
			// 存在一个 internalTrafficPolicy=Local 的 Service，唯一 endpoint webserver0 位于 node0 且正在终止。
			// pausePod0 和 pausePod1 分别位于 node0 和 node1。
			// pausePod0 -> Service clusterIP 成功，因为 webserver0 在 node0 且流量策略为 "Local"
			// pausePod1 -> Service clusterIP 失败，因为 webserver0 在另一节点且流量策略为 "Local"
			execHostnameTest(*pausePod0, serviceAddress, webserverPod0.Name)

			cmd := fmt.Sprintf(`curl -q -s --connect-timeout 5 %s/hostname`, serviceAddress)
			_, err := e2eoutput.RunHostCmd(pausePod1.Namespace, pausePod1.Name, cmd)
			gomega.Expect(err).To(gomega.HaveOccurred(), "expected error when trying to connect to cluster IP")

			time.Sleep(5 * time.Second)
		}
	})

	ginkgo.It("should fallback to terminating endpoints when there are no ready endpoints with externallTrafficPolicy=Cluster", func(ctx context.Context) {
		// windows kube-proxy 尚未支持此特性
		e2eskipper.SkipIfNodeOSDistroIs("windows")

		nodes, err := e2enode.GetBoundedReadySchedulableNodes(ctx, cs, 2)
		framework.ExpectNoError(err)
		nodeCounts := len(nodes.Items)
		if nodeCounts < 2 {
			e2eskipper.Skipf("The test requires at least two ready nodes on %s, but found %v", framework.TestContext.Provider, nodeCounts)
		}
		node0 := nodes.Items[0]
		node1 := nodes.Items[1]

		serviceName := "svc-proxy-terminating"
		ns := f.Namespace.Name
		servicePort := 80

		ginkgo.By("creating a TCP service " + serviceName + " where all pods are terminating" + ns)
		jig := e2eservice.NewTestJig(cs, ns, serviceName)
		svc, err := jig.CreateTCPService(ctx, func(svc *v1.Service) {
			svc.Spec.Ports = []v1.ServicePort{
				{Port: 80, Name: "http", Protocol: v1.ProtocolTCP, TargetPort: intstr.FromInt32(80)},
			}
			svc.Spec.Type = v1.ServiceTypeNodePort
		})
		framework.ExpectNoError(err)

		ginkgo.By("Creating 1 webserver pod to be part of the TCP service")
		webserverPod0 := e2epod.NewAgnhostPod(ns, "echo-hostname-0", nil, nil, nil, "netexec", "--http-port", strconv.Itoa(servicePort), "--delay-shutdown", "100")
		webserverPod0.Labels = jig.Labels
		webserverPod0.Spec.TerminationGracePeriodSeconds = ptr.To[int64](100)
		e2epod.SetNodeSelection(&webserverPod0.Spec, e2epod.NodeSelection{Name: node0.Name})

		_, err = cs.CoreV1().Pods(ns).Create(ctx, webserverPod0, metav1.CreateOptions{})
		framework.ExpectNoError(err)
		framework.ExpectNoError(e2epod.WaitTimeoutForPodReadyInNamespace(ctx, f.ClientSet, webserverPod0.Name, f.Namespace.Name, framework.PodStartTimeout))
		err = e2eendpointslice.WaitForEndpointCount(ctx, cs, ns, serviceName, 1)
		framework.ExpectNoError(err)

		ginkgo.By("Creating 2 pause pods that will try to connect to the webservers")
		pausePod0 := e2epod.NewAgnhostPod(ns, "pause-pod-0", nil, nil, nil)
		e2epod.SetNodeSelection(&pausePod0.Spec, e2epod.NodeSelection{Name: node0.Name})

		pausePod0, err = cs.CoreV1().Pods(ns).Create(ctx, pausePod0, metav1.CreateOptions{})
		framework.ExpectNoError(err)
		framework.ExpectNoError(e2epod.WaitTimeoutForPodReadyInNamespace(ctx, f.ClientSet, pausePod0.Name, f.Namespace.Name, framework.PodStartTimeout))

		pausePod1 := e2epod.NewAgnhostPod(ns, "pause-pod-1", nil, nil, nil)
		e2epod.SetNodeSelection(&pausePod1.Spec, e2epod.NodeSelection{Name: node1.Name})

		pausePod1, err = cs.CoreV1().Pods(ns).Create(ctx, pausePod1, metav1.CreateOptions{})
		framework.ExpectNoError(err)
		framework.ExpectNoError(e2epod.WaitTimeoutForPodReadyInNamespace(ctx, f.ClientSet, pausePod1.Name, f.Namespace.Name, framework.PodStartTimeout))

		// 删除后 webserver 仍应通过 Service 提供流量，因为：
		//  - 它有 100 秒的终止宽限期
		//  - 它是唯一的就绪 endpoint
		err = cs.CoreV1().Pods(ns).Delete(ctx, webserverPod0.Name, metav1.DeleteOptions{})
		framework.ExpectNoError(err)

		// 连续 5 次验证所有 endpoint 都在终止时，本地和远端 Pod 仍能连接到 Service 的 NodePort
		nodeIPs := e2enode.GetAddresses(&node0, v1.NodeInternalIP)
		nodePortAddress := net.JoinHostPort(nodeIPs[0], strconv.Itoa(int(svc.Spec.Ports[0].NodePort)))
		for range 5 {
			// 存在一个 Type=NodePort、externalTrafficPolicy=Cluster 的 Service，唯一 endpoint webserver0 位于 node0 且正在终止。
			// pausePod0 和 pausePod1 分别位于 node0 和 node1。
			// pausePod0 -> node0 node port 成功，因为 webserver0 在 node0 且流量策略为 "Cluster"
			// pausePod1 -> node0 node port 成功，因为 webserver0 在 node0 且流量策略为 "Cluster"
			execHostnameTest(*pausePod0, nodePortAddress, webserverPod0.Name)
			execHostnameTest(*pausePod1, nodePortAddress, webserverPod0.Name)

			time.Sleep(5 * time.Second)
		}
	})

	ginkgo.It("should fallback to local terminating endpoints when there are no ready endpoints with externalTrafficPolicy=Local", func(ctx context.Context) {
		// windows kube-proxy 尚未支持此特性
		e2eskipper.SkipIfNodeOSDistroIs("windows")

		nodes, err := e2enode.GetBoundedReadySchedulableNodes(ctx, cs, 2)
		framework.ExpectNoError(err)
		nodeCounts := len(nodes.Items)
		if nodeCounts < 2 {
			e2eskipper.Skipf("The test requires at least two ready nodes on %s, but found %v", framework.TestContext.Provider, nodeCounts)
		}
		node0 := nodes.Items[0]
		node1 := nodes.Items[1]

		serviceName := "svc-proxy-terminating"
		ns := f.Namespace.Name
		servicePort := 80

		ginkgo.By("creating a TCP service " + serviceName + " where all pods are terminating" + ns)
		jig := e2eservice.NewTestJig(cs, ns, serviceName)
		svc, err := jig.CreateTCPService(ctx, func(svc *v1.Service) {
			svc.Spec.Ports = []v1.ServicePort{
				{Port: 80, Name: "http", Protocol: v1.ProtocolTCP, TargetPort: intstr.FromInt32(80)},
			}
			svc.Spec.Type = v1.ServiceTypeNodePort
			svc.Spec.ExternalTrafficPolicy = v1.ServiceExternalTrafficPolicyLocal
		})
		framework.ExpectNoError(err)

		ginkgo.By("Creating 1 webserver pod to be part of the TCP service")
		webserverPod0 := e2epod.NewAgnhostPod(ns, "echo-hostname-0", nil, nil, nil, "netexec", "--http-port", strconv.Itoa(servicePort), "--delay-shutdown", "100")
		webserverPod0.Labels = jig.Labels
		webserverPod0.Spec.TerminationGracePeriodSeconds = ptr.To[int64](100)
		e2epod.SetNodeSelection(&webserverPod0.Spec, e2epod.NodeSelection{Name: node0.Name})

		_, err = cs.CoreV1().Pods(ns).Create(ctx, webserverPod0, metav1.CreateOptions{})
		framework.ExpectNoError(err)
		framework.ExpectNoError(e2epod.WaitTimeoutForPodReadyInNamespace(ctx, f.ClientSet, webserverPod0.Name, f.Namespace.Name, framework.PodStartTimeout))
		err = e2eendpointslice.WaitForEndpointCount(ctx, cs, ns, serviceName, 1)
		framework.ExpectNoError(err)

		ginkgo.By("Creating 2 pause pods that will try to connect to the webservers")
		pausePod0 := e2epod.NewAgnhostPod(ns, "pause-pod-0", nil, nil, nil)
		e2epod.SetNodeSelection(&pausePod0.Spec, e2epod.NodeSelection{Name: node0.Name})

		pausePod0, err = cs.CoreV1().Pods(ns).Create(ctx, pausePod0, metav1.CreateOptions{})
		framework.ExpectNoError(err)
		framework.ExpectNoError(e2epod.WaitTimeoutForPodReadyInNamespace(ctx, f.ClientSet, pausePod0.Name, f.Namespace.Name, framework.PodStartTimeout))

		pausePod1 := e2epod.NewAgnhostPod(ns, "pause-pod-1", nil, nil, nil)
		e2epod.SetNodeSelection(&pausePod1.Spec, e2epod.NodeSelection{Name: node1.Name})

		pausePod1, err = cs.CoreV1().Pods(ns).Create(ctx, pausePod1, metav1.CreateOptions{})
		framework.ExpectNoError(err)
		framework.ExpectNoError(e2epod.WaitTimeoutForPodReadyInNamespace(ctx, f.ClientSet, pausePod1.Name, f.Namespace.Name, framework.PodStartTimeout))

		// 删除后 webserver 仍应通过 Service 提供流量，因为：
		//  - 它有 100 秒的终止宽限期
		//  - 它是唯一的就绪 endpoint
		err = cs.CoreV1().Pods(ns).Delete(ctx, webserverPod0.Name, metav1.DeleteOptions{})
		framework.ExpectNoError(err)

		// 连续 5 次验证第一个 pause Pod 可以在本地连接到 Service，第二个则超时报错
		nodeIPs0 := e2enode.GetAddresses(&node0, v1.NodeInternalIP)
		nodeIPs1 := e2enode.GetAddresses(&node1, v1.NodeInternalIP)
		nodePortAddress0 := net.JoinHostPort(nodeIPs0[0], strconv.Itoa(int(svc.Spec.Ports[0].NodePort)))
		nodePortAddress1 := net.JoinHostPort(nodeIPs1[0], strconv.Itoa(int(svc.Spec.Ports[0].NodePort)))
		for range 5 {
			// 存在一个 Type=NodePort、externalTrafficPolicy=Local 的 Service，唯一 endpoint webserver0 位于 node0 且正在终止。
			// pausePod0 和 pausePod1 分别位于 node0 和 node1。
			// pausePod0 -> node1 node port 失败，因为该连接属于“外部”且没有本地 endpoint
			// pausePod1 -> node0 node port 成功，因为 webserver0 在 node0
			// pausePod0 -> node0 node port 成功，因为 webserver0 在 node0
			//
			// 注意：对 kube-proxy 而言 pausePod1 -> node1 会成功，因为 kube-proxy 将 Pod 到同节点 NodePort
			// 的连接既不算内部也不算外部，总是使用 Cluster 流量策略。但此处不测该场景，
			// 因为并非所有网络实现都遵循 kube-proxy 对“目标”流量策略的解释。
			// See: https://github.com/kubernetes/kubernetes/pull/123622
			cmd := fmt.Sprintf(`curl -q -s --connect-timeout 5 %s/hostname`, nodePortAddress1)
			_, err := e2eoutput.RunHostCmd(pausePod0.Namespace, pausePod0.Name, cmd)
			gomega.Expect(err).To(gomega.HaveOccurred(), "expected error when trying to connect to node port for pausePod0")

			execHostnameTest(*pausePod0, nodePortAddress0, webserverPod0.Name)
			execHostnameTest(*pausePod1, nodePortAddress0, webserverPod0.Name)

			time.Sleep(5 * time.Second)
		}
	})

	/*
	   Release: v1.18
	   Testname: Find Kubernetes Service in default Namespace
	   Description: List all Services in all Namespaces, response MUST include a Service named Kubernetes with the Namespace of default.
	*/
	framework.ConformanceIt("should find a service from listing all namespaces", func(ctx context.Context) {
		ginkgo.By("fetching services")
		svcs, _ := f.ClientSet.CoreV1().Services("").List(ctx, metav1.ListOptions{})

		foundSvc := false
		for _, svc := range svcs.Items {
			if svc.ObjectMeta.Name == "kubernetes" && svc.ObjectMeta.Namespace == "default" {
				foundSvc = true
				break
			}
		}

		if !foundSvc {
			framework.Fail("could not find service 'kubernetes' in service list in all namespaces")
		}
	})

	/*
		Release: v1.21
		Testname: Service, complete ServiceStatus lifecycle
		Description: Create a service, the service MUST exist.
		When retrieving /status the action MUST be validated.
		When patching /status the action MUST be validated.
		When updating /status the action MUST be validated.
		When patching a service the action MUST be validated.
	*/
	framework.ConformanceIt("should complete a service status lifecycle", func(ctx context.Context) {

		ns := f.Namespace.Name
		svcResource := schema.GroupVersionResource{Group: "", Version: "v1", Resource: "services"}
		svcClient := f.ClientSet.CoreV1().Services(ns)

		testSvcName := "test-service-" + utilrand.String(5)
		testSvcLabels := map[string]string{"test-service-static": "true"}
		testSvcLabelsFlat := "test-service-static=true"

		w := &cache.ListWatch{
			WatchFunc: func(options metav1.ListOptions) (watch.Interface, error) {
				options.LabelSelector = testSvcLabelsFlat
				return cs.CoreV1().Services(ns).Watch(ctx, options)
			},
		}

		svcList, err := cs.CoreV1().Services("").List(ctx, metav1.ListOptions{LabelSelector: testSvcLabelsFlat})
		framework.ExpectNoError(err, "failed to list Services")

		ginkgo.By("creating a Service")
		testService := v1.Service{
			ObjectMeta: metav1.ObjectMeta{
				Name:   testSvcName,
				Labels: testSvcLabels,
			},
			Spec: v1.ServiceSpec{
				Type: "LoadBalancer",
				Ports: []v1.ServicePort{{
					Name:       "http",
					Protocol:   v1.ProtocolTCP,
					Port:       int32(80),
					TargetPort: intstr.FromInt32(80),
				}},
				LoadBalancerClass: ptr.To("example.com/internal-vip"),
			},
		}
		_, err = cs.CoreV1().Services(ns).Create(ctx, &testService, metav1.CreateOptions{})
		framework.ExpectNoError(err, "failed to create Service")

		ginkgo.By("watching for the Service to be added")
		ctxUntil, cancel := context.WithTimeout(ctx, svcReadyTimeout)
		defer cancel()
		_, err = watchtools.Until(ctxUntil, svcList.ResourceVersion, w, func(event watch.Event) (bool, error) {
			if svc, ok := event.Object.(*v1.Service); ok {
				found := svc.ObjectMeta.Name == testService.ObjectMeta.Name &&
					svc.ObjectMeta.Namespace == ns &&
					svc.Labels["test-service-static"] == "true"
				if !found {
					framework.Logf("observed Service %v in namespace %v with labels: %v & ports %v", svc.ObjectMeta.Name, svc.ObjectMeta.Namespace, svc.Labels, svc.Spec.Ports)
					return false, nil
				}
				framework.Logf("Found Service %v in namespace %v with labels: %v & ports %v", svc.ObjectMeta.Name, svc.ObjectMeta.Namespace, svc.Labels, svc.Spec.Ports)
				return found, nil
			}
			framework.Logf("Observed event: %+v", event.Object)
			return false, nil
		})
		framework.ExpectNoError(err, "Failed to locate Service %v in namespace %v", testService.ObjectMeta.Name, ns)
		framework.Logf("Service %s created", testSvcName)

		ginkgo.By("Getting /status")
		svcStatusUnstructured, err := f.DynamicClient.Resource(svcResource).Namespace(ns).Get(ctx, testSvcName, metav1.GetOptions{}, "status")
		framework.ExpectNoError(err, "Failed to fetch ServiceStatus of Service %s in namespace %s", testSvcName, ns)
		svcStatusBytes, err := json.Marshal(svcStatusUnstructured)
		framework.ExpectNoError(err, "Failed to marshal unstructured response. %v", err)

		var svcStatus v1.Service
		err = json.Unmarshal(svcStatusBytes, &svcStatus)
		framework.ExpectNoError(err, "Failed to unmarshal JSON bytes to a Service object type")
		framework.Logf("Service %s has LoadBalancer: %v", testSvcName, svcStatus.Status.LoadBalancer)

		ginkgo.By("patching the ServiceStatus")
		lbStatus := v1.LoadBalancerStatus{
			Ingress: []v1.LoadBalancerIngress{{IP: "203.0.113.1"}},
		}
		lbStatusJSON, err := json.Marshal(lbStatus)
		framework.ExpectNoError(err, "Failed to marshal JSON. %v", err)
		_, err = svcClient.Patch(ctx, testSvcName, types.MergePatchType,
			[]byte(`{"metadata":{"annotations":{"patchedstatus":"true"}},"status":{"loadBalancer":`+string(lbStatusJSON)+`}}`),
			metav1.PatchOptions{}, "status")
		framework.ExpectNoError(err, "Could not patch service status", err)

		ginkgo.By("watching for the Service to be patched")
		ctxUntil, cancel = context.WithTimeout(ctx, svcReadyTimeout)
		defer cancel()

		_, err = watchtools.Until(ctxUntil, svcList.ResourceVersion, w, func(event watch.Event) (bool, error) {
			if svc, ok := event.Object.(*v1.Service); ok {
				found := svc.ObjectMeta.Name == testService.ObjectMeta.Name &&
					svc.ObjectMeta.Namespace == ns &&
					svc.Annotations["patchedstatus"] == "true"
				if !found {
					framework.Logf("observed Service %v in namespace %v with annotations: %v & LoadBalancer: %v", svc.ObjectMeta.Name, svc.ObjectMeta.Namespace, svc.Annotations, svc.Status.LoadBalancer)
					return false, nil
				}
				framework.Logf("Found Service %v in namespace %v with annotations: %v & LoadBalancer: %v", svc.ObjectMeta.Name, svc.ObjectMeta.Namespace, svc.Annotations, svc.Status.LoadBalancer)
				return found, nil
			}
			framework.Logf("Observed event: %+v", event.Object)
			return false, nil
		})
		framework.ExpectNoError(err, "failed to locate Service %v in namespace %v", testService.ObjectMeta.Name, ns)
		framework.Logf("Service %s has service status patched", testSvcName)

		ginkgo.By("updating the ServiceStatus")

		var statusToUpdate, updatedStatus *v1.Service
		err = retry.RetryOnConflict(retry.DefaultRetry, func() error {
			statusToUpdate, err = svcClient.Get(ctx, testSvcName, metav1.GetOptions{})
			framework.ExpectNoError(err, "Unable to retrieve service %s", testSvcName)

			statusToUpdate.Status.Conditions = append(statusToUpdate.Status.Conditions, metav1.Condition{
				Type:    "StatusUpdate",
				Status:  metav1.ConditionTrue,
				Reason:  "E2E",
				Message: "Set from e2e test",
			})

			updatedStatus, err = svcClient.UpdateStatus(ctx, statusToUpdate, metav1.UpdateOptions{})
			return err
		})
		framework.ExpectNoError(err, "\n\n Failed to UpdateStatus. %v\n\n", err)
		framework.Logf("updatedStatus.Conditions: %#v", updatedStatus.Status.Conditions)

		ginkgo.By("watching for the Service to be updated")
		ctxUntil, cancel = context.WithTimeout(ctx, svcReadyTimeout)
		defer cancel()
		_, err = watchtools.Until(ctxUntil, svcList.ResourceVersion, w, func(event watch.Event) (bool, error) {
			if svc, ok := event.Object.(*v1.Service); ok {
				found := svc.ObjectMeta.Name == testService.ObjectMeta.Name &&
					svc.ObjectMeta.Namespace == ns &&
					svc.Annotations["patchedstatus"] == "true"
				if !found {
					framework.Logf("Observed Service %v in namespace %v with annotations: %v & Conditions: %v", svc.ObjectMeta.Name, svc.ObjectMeta.Namespace, svc.Annotations, svc.Status.Conditions)
					return false, nil
				}
				for _, cond := range svc.Status.Conditions {
					if cond.Type == "StatusUpdate" &&
						cond.Reason == "E2E" &&
						cond.Message == "Set from e2e test" {
						framework.Logf("Found Service %v in namespace %v with annotations: %v & Conditions: %v", svc.ObjectMeta.Name, svc.ObjectMeta.Namespace, svc.Annotations, svc.Status.Conditions)
						return found, nil
					}
				}
				framework.Logf("Observed Service %v in namespace %v with annotations: %v & Conditions: %v", svc.ObjectMeta.Name, svc.ObjectMeta.Namespace, svc.Annotations, svc.Status.Conditions)
				return false, nil
			}
			framework.Logf("Observed event: %+v", event.Object)
			return false, nil
		})
		framework.ExpectNoError(err, "failed to locate Service %v in namespace %v", testService.ObjectMeta.Name, ns)
		framework.Logf("Service %s has service status updated", testSvcName)

		ginkgo.By("patching the service")
		servicePatchPayload, err := json.Marshal(v1.Service{
			ObjectMeta: metav1.ObjectMeta{
				Labels: map[string]string{
					"test-service": "patched",
				},
			},
		})

		_, err = svcClient.Patch(ctx, testSvcName, types.StrategicMergePatchType, []byte(servicePatchPayload), metav1.PatchOptions{})
		framework.ExpectNoError(err, "failed to patch service. %v", err)

		ginkgo.By("watching for the Service to be patched")
		ctxUntil, cancel = context.WithTimeout(ctx, svcReadyTimeout)
		defer cancel()
		_, err = watchtools.Until(ctxUntil, svcList.ResourceVersion, w, func(event watch.Event) (bool, error) {
			if svc, ok := event.Object.(*v1.Service); ok {
				found := svc.ObjectMeta.Name == testService.ObjectMeta.Name &&
					svc.ObjectMeta.Namespace == ns &&
					svc.Labels["test-service"] == "patched"
				if !found {
					framework.Logf("observed Service %v in namespace %v with labels: %v", svc.ObjectMeta.Name, svc.ObjectMeta.Namespace, svc.Labels)
					return false, nil
				}
				framework.Logf("Found Service %v in namespace %v with labels: %v", svc.ObjectMeta.Name, svc.ObjectMeta.Namespace, svc.Labels)
				return found, nil
			}
			framework.Logf("Observed event: %+v", event.Object)
			return false, nil
		})
		framework.ExpectNoError(err, "failed to locate Service %v in namespace %v", testService.ObjectMeta.Name, ns)
		framework.Logf("Service %s patched", testSvcName)

		ginkgo.By("deleting the service")
		err = cs.CoreV1().Services(ns).Delete(ctx, testSvcName, metav1.DeleteOptions{})
		framework.ExpectNoError(err, "failed to delete the Service. %v", err)

		ginkgo.By("watching for the Service to be deleted")
		ctxUntil, cancel = context.WithTimeout(ctx, svcReadyTimeout)
		defer cancel()
		_, err = watchtools.Until(ctxUntil, svcList.ResourceVersion, w, func(event watch.Event) (bool, error) {
			switch event.Type {
			case watch.Deleted:
				if svc, ok := event.Object.(*v1.Service); ok {
					found := svc.ObjectMeta.Name == testService.ObjectMeta.Name &&
						svc.ObjectMeta.Namespace == ns &&
						svc.Labels["test-service-static"] == "true"
					if !found {
						framework.Logf("observed Service %v in namespace %v with labels: %v & annotations: %v", svc.ObjectMeta.Name, svc.ObjectMeta.Namespace, svc.Labels, svc.Annotations)
						return false, nil
					}
					framework.Logf("Found Service %v in namespace %v with labels: %v & annotations: %v", svc.ObjectMeta.Name, svc.ObjectMeta.Namespace, svc.Labels, svc.Annotations)
					return found, nil
				}
			default:
				framework.Logf("Observed event: %+v", event.Type)
			}
			return false, nil
		})
		framework.ExpectNoError(err, "failed to delete Service %v in namespace %v", testService.ObjectMeta.Name, ns)
		framework.Logf("Service %s deleted", testSvcName)
	})

	/*
		Release: v1.23
		Testname: Service, deletes a collection of services
		Description: Create three services with the required
		labels and ports. It MUST locate three services in the
		test namespace. It MUST succeed at deleting a collection
		of services via a label selector. It MUST locate only
		one service after deleting the service collection.
	*/
	framework.ConformanceIt("should delete a collection of services", func(ctx context.Context) {

		ns := f.Namespace.Name
		svcClient := f.ClientSet.CoreV1().Services(ns)
		svcResource := schema.GroupVersionResource{Group: "", Version: "v1", Resource: "services"}
		svcDynamicClient := f.DynamicClient.Resource(svcResource).Namespace(ns)

		svcLabel := map[string]string{"e2e-test-service": "delete"}
		deleteLabel := labels.SelectorFromSet(svcLabel).String()

		ginkgo.By("creating a collection of services")

		testServices := []struct {
			name  string
			label map[string]string
			port  int
		}{
			{
				name:  "e2e-svc-a-" + utilrand.String(5),
				label: map[string]string{"e2e-test-service": "delete"},
				port:  8001,
			},
			{
				name:  "e2e-svc-b-" + utilrand.String(5),
				label: map[string]string{"e2e-test-service": "delete"},
				port:  8002,
			},
			{
				name:  "e2e-svc-c-" + utilrand.String(5),
				label: map[string]string{"e2e-test-service": "keep"},
				port:  8003,
			},
		}

		for _, testService := range testServices {
			func() {
				framework.Logf("Creating %s", testService.name)

				svc := v1.Service{
					ObjectMeta: metav1.ObjectMeta{
						Name:   testService.name,
						Labels: testService.label,
					},
					Spec: v1.ServiceSpec{
						Type: "ClusterIP",
						Ports: []v1.ServicePort{{
							Name:       "http",
							Protocol:   v1.ProtocolTCP,
							Port:       int32(testService.port),
							TargetPort: intstr.FromInt(testService.port),
						}},
					},
				}
				_, err := svcClient.Create(ctx, &svc, metav1.CreateOptions{})
				framework.ExpectNoError(err, "failed to create Service")

			}()
		}

		svcList, err := cs.CoreV1().Services(ns).List(ctx, metav1.ListOptions{})
		framework.ExpectNoError(err, "failed to list Services")
		gomega.Expect(svcList.Items).To(gomega.HaveLen(3), "Required count of services out of sync")

		ginkgo.By("deleting service collection")
		err = svcDynamicClient.DeleteCollection(ctx, metav1.DeleteOptions{}, metav1.ListOptions{LabelSelector: deleteLabel})
		framework.ExpectNoError(err, "failed to delete service collection. %v", err)

		svcList, err = cs.CoreV1().Services(ns).List(ctx, metav1.ListOptions{})
		framework.ExpectNoError(err, "failed to list Services")
		gomega.Expect(svcList.Items).To(gomega.HaveLen(1), "Required count of services out of sync")

		framework.Logf("Collection of services has been deleted")
	})

	/*
		Release: v1.29
		Testname: Service, should serve endpoints on same port and different protocols.
		Description: Create one service with two ports, same port number and different protocol TCP and UDP.
		It MUST be able to forward traffic to both ports.
		Update the Service to expose only the TCP port, it MUST succeed to connect to the TCP port and fail
		to connect to the UDP port.
		Update the Service to expose only the UDP port, it MUST succeed to connect to the UDP port and fail
		to connect to the TCP port.
	*/
	framework.ConformanceIt("should serve endpoints on same port and different protocols", func(ctx context.Context) {
		serviceName := "multiprotocol-test"
		testLabels := map[string]string{"app": "multiport"}
		ns := f.Namespace.Name
		containerPort := 80

		svcTCPport := v1.ServicePort{
			Name:       "tcp-port",
			Port:       80,
			TargetPort: intstr.FromInt(containerPort),
			Protocol:   v1.ProtocolTCP,
		}
		svcUDPport := v1.ServicePort{
			Name:       "udp-port",
			Port:       80,
			TargetPort: intstr.FromInt(containerPort),
			Protocol:   v1.ProtocolUDP,
		}

		ginkgo.By("creating service " + serviceName + " in namespace " + ns)

		testService := v1.Service{
			ObjectMeta: metav1.ObjectMeta{
				Name:   serviceName,
				Labels: testLabels,
			},
			Spec: v1.ServiceSpec{
				Type:     v1.ServiceTypeClusterIP,
				Selector: testLabels,
				Ports:    []v1.ServicePort{svcTCPport, svcUDPport},
			},
		}
		service, err := cs.CoreV1().Services(ns).Create(ctx, &testService, metav1.CreateOptions{})
		framework.ExpectNoError(err, "failed to create Service")

		containerPorts := []v1.ContainerPort{{
			Name:          svcTCPport.Name,
			ContainerPort: int32(containerPort),
			Protocol:      v1.ProtocolTCP,
		}, {
			Name:          svcUDPport.Name,
			ContainerPort: int32(containerPort),
			Protocol:      v1.ProtocolUDP,
		}}
		podname1 := "pod1"
		ginkgo.By("creating pod " + podname1 + " in namespace " + ns)
		createPodOrFail(ctx, f, ns, podname1, testLabels, containerPorts, "netexec", "--http-port", strconv.Itoa(containerPort), "--udp-port", strconv.Itoa(containerPort))

		ginkgo.By("waiting for EndpointSlices for all ports")
		err = e2eendpointslice.WaitForEndpointPorts(ctx, cs, ns, serviceName,
			[]e2eendpointslice.PortMapping{
				{Name: svcTCPport.Name, Protocol: v1.ProtocolTCP, Target: podname1, TargetPort: int32(containerPort)},
				{Name: svcUDPport.Name, Protocol: v1.ProtocolUDP, Target: podname1, TargetPort: int32(containerPort)},
			},
		)
		framework.ExpectNoError(err)

		ginkgo.By("Checking if the Service forwards traffic to the TCP and UDP port")
		execPod := e2epod.CreateExecPodOrFail(ctx, cs, ns, "execpod", nil)
		err = testEndpointReachability(ctx, service.Spec.ClusterIP, 80, v1.ProtocolTCP, execPod, 30*time.Second)
		if err != nil {
			framework.Failf("Failed to connect to Service TCP port: %v", err)
		}
		err = testEndpointReachability(ctx, service.Spec.ClusterIP, 80, v1.ProtocolUDP, execPod, 30*time.Second)
		if err != nil {
			framework.Failf("Failed to connect to Service UDP port: %v", err)
		}

		ginkgo.By("updating the service to have only a TCP port")
		service, err = cs.CoreV1().Services(ns).Get(ctx, serviceName, metav1.GetOptions{})
		if err != nil {
			framework.Failf("failed to get Service %q: %v", serviceName, err)
		}
		gomega.Expect(service).To(apimachineryutils.HaveValidResourceVersion())
		service.Spec.Ports = []v1.ServicePort{svcTCPport}
		updatedSVC, err := cs.CoreV1().Services(ns).Update(ctx, service, metav1.UpdateOptions{})
		if err != nil {
			framework.Failf("failed to get Service %q: %v", serviceName, err)
		}
		gomega.Expect(resourceversion.CompareResourceVersion(service.ResourceVersion, updatedSVC.ResourceVersion)).To(gomega.BeNumerically("==", -1), "updated object should have a larger resource version")

		ginkgo.By("Checking if the Service forwards traffic to TCP only")
		err = testEndpointReachability(ctx, service.Spec.ClusterIP, 80, v1.ProtocolTCP, execPod, 30*time.Second)
		if err != nil {
			framework.Failf("Failed to connect to Service TCP port: %v", err)
		}
		// 需要把 NetworkProgrammingLatency 考虑在内
		// testEndpointReachability tries 3 times every 3 second
		// 在 30 秒内重试，检查端口是否停止转发
		gomega.Eventually(ctx, func() error {
			return testEndpointReachability(ctx, service.Spec.ClusterIP, 80, v1.ProtocolUDP, execPod, 6*time.Second)
		}).WithTimeout(30 * time.Second).WithPolling(5 * time.Second).ShouldNot(gomega.BeNil())

		ginkgo.By("updating the service to have only a UDP port")
		service, err = cs.CoreV1().Services(ns).Get(ctx, serviceName, metav1.GetOptions{})
		if err != nil {
			framework.Failf("failed to get Service %q: %v", serviceName, err)
		}
		service.Spec.Ports = []v1.ServicePort{svcUDPport}
		_, err = cs.CoreV1().Services(ns).Update(ctx, service, metav1.UpdateOptions{})
		if err != nil {
			framework.Failf("failed to update Service %q: %v", serviceName, err)
		}

		ginkgo.By("Checking if the Service forwards traffic to UDP only")
		err = testEndpointReachability(ctx, service.Spec.ClusterIP, 80, v1.ProtocolUDP, execPod, 30*time.Second)
		if err != nil {
			framework.Failf("Failed to connect to Service UDP port: %v", err)
		}
		// 需要把 NetworkProgrammingLatency 考虑在内
		// testEndpointReachability tries 3 times every 3 second
		// 在 30 秒内重试，检查端口是否停止转发
		gomega.Eventually(ctx, func() error {
			return testEndpointReachability(ctx, service.Spec.ClusterIP, 80, v1.ProtocolTCP, execPod, 6*time.Second)
		}).WithTimeout(30 * time.Second).WithPolling(5 * time.Second).ShouldNot(gomega.BeNil())
	})

	/*
		Release: v1.26
		Testname: Service, same ports with different protocols on a Load Balancer Service
		Description: Create a LoadBalancer service with two ports that have the same value but use different protocols. Add a Pod that listens on both ports. The Pod must be reachable via the ClusterIP and both ports
	*/
	ginkgo.It("should serve endpoints on same port and different protocol for internal traffic on Type LoadBalancer", func(ctx context.Context) {
		serviceName := "multiprotocol-lb-test"
		ns := f.Namespace.Name
		jig := e2eservice.NewTestJig(cs, ns, serviceName)

		ginkgo.DeferCleanup(func(ctx context.Context) {
			err := cs.CoreV1().Services(ns).Delete(ctx, serviceName, metav1.DeleteOptions{})
			framework.ExpectNoError(err, "failed to delete service: %s in namespace: %s", serviceName, ns)
		})

		svc1port := "svc1"
		svc2port := "svc2"

		ginkgo.By("creating service " + serviceName + " in namespace " + ns)
		svc, err := jig.CreateLoadBalancerServiceWaitForClusterIPOnly(func(service *v1.Service) {
			service.Spec.Ports = []v1.ServicePort{
				{
					Name:       "portname1",
					Port:       80,
					TargetPort: intstr.FromString(svc1port),
					Protocol:   v1.ProtocolTCP,
				},
				{
					Name:       "portname2",
					Port:       80,
					TargetPort: intstr.FromString(svc2port),
					Protocol:   v1.ProtocolUDP,
				},
			}
		})
		framework.ExpectNoError(err)

		containerPort := 100

		names := map[string]bool{}
		ginkgo.DeferCleanup(func(ctx context.Context) {
			for name := range names {
				err := cs.CoreV1().Pods(ns).Delete(ctx, name, metav1.DeleteOptions{})
				framework.ExpectNoError(err, "failed to delete pod: %s in namespace: %s", name, ns)
			}
		})

		containerPorts := []v1.ContainerPort{
			{
				Name:          svc1port,
				ContainerPort: int32(containerPort),
				Protocol:      v1.ProtocolTCP,
			},
			{
				Name:          svc2port,
				ContainerPort: int32(containerPort),
				Protocol:      v1.ProtocolUDP,
			},
		}

		podname1 := "pod1"

		createPodOrFail(ctx, f, ns, podname1, jig.Labels, containerPorts, "netexec", "--http-port", strconv.Itoa(containerPort), "--udp-port", strconv.Itoa(containerPort))
		err = e2eendpointslice.WaitForEndpointPorts(ctx, cs, ns, serviceName,
			[]e2eendpointslice.PortMapping{
				{Name: "portname1", Protocol: v1.ProtocolTCP, Target: podname1, TargetPort: int32(containerPort)},
				{Name: "portname2", Protocol: v1.ProtocolUDP, Target: podname1, TargetPort: int32(containerPort)},
			},
		)
		framework.ExpectNoError(err)

		ginkgo.By("Checking if the Service forwards traffic to pods")
		execPod := e2epod.CreateExecPodOrFail(ctx, cs, ns, "execpod", nil)
		err = jig.CheckServiceReachability(ctx, svc, execPod)
		framework.ExpectNoError(err)
		e2epod.DeletePodOrFail(ctx, cs, ns, podname1)
	})

	// 标记为 [Serial]，因为不能与 [Feature:SCTPConnectivity] 测试同时运行，
	// 后者可能导致 sctp.ko 被加载。
	f.It("should allow creating a basic SCTP service with pod and endpoints [LinuxOnly]", f.WithSerial(), func(ctx context.Context) {
		serviceName := "sctp-endpoint-test"
		ns := f.Namespace.Name
		jig := e2eservice.NewTestJig(cs, ns, serviceName)

		ginkgo.By("getting the state of the sctp module on nodes")
		nodes, err := e2enode.GetBoundedReadySchedulableNodes(ctx, cs, 2)
		framework.ExpectNoError(err)
		sctpLoadedAtStart := CheckSCTPModuleLoadedOnNodes(ctx, f, nodes)

		ginkgo.By("creating service " + serviceName + " in namespace " + ns)
		_, err = jig.CreateSCTPServiceWithPort(ctx, nil, 5060)
		framework.ExpectNoError(err)
		ginkgo.DeferCleanup(func(ctx context.Context) {
			err := cs.CoreV1().Services(ns).Delete(ctx, serviceName, metav1.DeleteOptions{})
			framework.ExpectNoError(err, "failed to delete service: %s in namespace: %s", serviceName, ns)
		})

		err = e2enetwork.WaitForService(ctx, f.ClientSet, ns, serviceName, true, 5*time.Second, e2eservice.TestTimeout)
		framework.ExpectNoError(err, fmt.Sprintf("error while waiting for service:%s err: %v", serviceName, err))

		ginkgo.By("validating endpoints do not exist yet")
		err = e2eendpointslice.WaitForEndpointCount(ctx, cs, ns, serviceName, 0)
		framework.ExpectNoError(err)

		ginkgo.By("creating a pod for the service")
		names := map[string]bool{}

		name1 := "pod1"

		createPodOrFail(ctx, f, ns, name1, jig.Labels, []v1.ContainerPort{{ContainerPort: 5060, Protocol: v1.ProtocolSCTP}})
		names[name1] = true
		ginkgo.DeferCleanup(func(ctx context.Context) {
			for name := range names {
				err := cs.CoreV1().Pods(ns).Delete(ctx, name, metav1.DeleteOptions{})
				framework.ExpectNoError(err, "failed to delete pod: %s in namespace: %s", name, ns)
			}
		})

		ginkgo.By("validating endpoints exists")
		err = e2eendpointslice.WaitForEndpointCount(ctx, cs, ns, serviceName, 1)
		framework.ExpectNoError(err)

		ginkgo.By("deleting the pod")
		e2epod.DeletePodOrFail(ctx, cs, ns, name1)
		delete(names, name1)
		ginkgo.By("validating endpoints do not exist anymore")
		err = e2eendpointslice.WaitForEndpointCount(ctx, cs, ns, serviceName, 0)
		framework.ExpectNoError(err)

		ginkgo.By("validating sctp module is still not loaded")
		sctpLoadedAtEnd := CheckSCTPModuleLoadedOnNodes(ctx, f, nodes)
		if !sctpLoadedAtStart && sctpLoadedAtEnd {
			framework.Failf("The state of the sctp module has changed due to the test case")
		}
	})

	ginkgo.It("should implement NodePort and HealthCheckNodePort correctly when ExternalTrafficPolicy changes", func(ctx context.Context) {
		// 这里使用 LoadBalancer Service，但只测试由 service proxy 管理的部分；
		// loadbalancer.go 中有另一个测试负责负载均衡器部分。

		namespace := f.Namespace.Name
		serviceName := "external-local-update"
		jig := e2eservice.NewTestJig(cs, namespace, serviceName)

		nodes, err := e2enode.GetBoundedReadySchedulableNodes(ctx, cs, 3)
		framework.ExpectNoError(err, "listing nodes")
		if len(nodes.Items) != 3 {
			e2eskipper.Skipf("Need at least 3 schedulable nodes for this test")
		}

		ginkgo.By("creating the service")
		var svc *v1.Service
		numberOfRetries := 5
		for range numberOfRetries {
			port, err := e2eservice.GetUnusedStaticNodePort()
			framework.ExpectNoError(err, "Static node port allocator was not able to find a free nodeport.")
			svc, err = jig.CreateLoadBalancerServiceWaitForClusterIPOnly(func(svc *v1.Service) {
				svc.Spec.ExternalTrafficPolicy = v1.ServiceExternalTrafficPolicyLocal
				svc.Spec.HealthCheckNodePort = port
			})
			// 稍后会把该 Service 切换为 Cluster 流量策略，但切换时不能让其他 e2e 测试占用旧的
			// HealthCheckNodePort，因此这里使用 ReserveStaticNodePort。
			if err == nil {
				staticHealthCheckPort := svc.Spec.HealthCheckNodePort
				ok := e2eservice.ReserveStaticNodePort(staticHealthCheckPort)
				if !ok {
					// 无法预留已分配的端口，说明该端口无效或已被其他测试预留。
					// 这表明代码存在问题，已有日志信息可用于排查。
					framework.Failf("Static node port allocator was not able to reserve healthcheck nodeport: %d", staticHealthCheckPort)
				}
				break
			}
			if apierrors.IsInvalid(err) {
				framework.Logf("node port %d is already allocated to other service, retrying ... : %v", port, err)
				continue
			}
			framework.ExpectNoError(err, "failed to create service: %s in namespace: %s", serviceName, namespace)

		}

		ginkgo.DeferCleanup(func(ctx context.Context) {
			err := cs.CoreV1().Services(namespace).Delete(ctx, serviceName, metav1.DeleteOptions{})
			framework.ExpectNoError(err, "failed to delete service: %s in namespace: %s", serviceName, namespace)
			e2eservice.ReleaseStaticNodePort(svc.Spec.HealthCheckNodePort)
		})

		nodePortStr := fmt.Sprintf("%d", svc.Spec.Ports[0].NodePort)
		hcNodePortStr := fmt.Sprintf("%d", svc.Spec.HealthCheckNodePort)
		framework.Logf("NodePort is %s, HealthCheckNodePort is %s", nodePortStr, hcNodePortStr)

		ginkgo.By("creating an endpoint for the service")
		_, err = jig.Run(ctx, nil)
		framework.ExpectNoError(err, "creating the endpoint")

		var endpointNode, endpointNodeIP string
		endpointsNodeMap, err := getEndpointNodesWithInternalIP(ctx, jig)
		framework.ExpectNoError(err, "fetching endpoint node IP")
		for node, nodeIP := range endpointsNodeMap {
			framework.Logf("endpoint is on node %s (%s)", node, nodeIP)
			endpointNode = node
			endpointNodeIP = nodeIP
			break
		}

		ginkgo.By("creating a HostNetwork exec pod on a different node from the endpoint")
		execPod := launchHostExecPod(ctx, cs, namespace, "hostexec", &endpointNode)
		hostExecPodNodeIP := execPod.Status.HostIP

		ginkgo.By("finding a third node with neither the endpoint nor the hostexec pod")
		var thirdNodeIP string
		for i := range nodes.Items {
			node := &nodes.Items[i]
			ips := e2enode.GetAddresses(node, v1.NodeInternalIP)
			if len(ips) < 1 {
				framework.Failf("No internal ip found for node %s", node.Name)
			}
			if ips[0] != endpointNodeIP && ips[0] != hostExecPodNodeIP {
				thirdNodeIP = ips[0]
				break
			}
		}
		gomega.Expect(thirdNodeIP).NotTo(gomega.Equal(""), "cluster has at least 3 nodes but could not find a third node IP?")

		checkOneNodePort := func(nodeIP string, expectResponse bool, trafficPolicy v1.ServiceExternalTrafficPolicy, deadline time.Time) {
			// hostExecPod 是 hostNetwork Pod。为选择连接的源 IP，需要绑定应用，
			// 否则“内核会选择网卡上与目标地址或下一跳路由同网的第一个地址”。
			// Ref: http://linux-ip.net/html/routing-saddr-selection.html
			cmd := fmt.Sprintf("curl -g -s --interface %s --connect-timeout 3 http://%s/clientip", hostExecPodNodeIP, net.JoinHostPort(nodeIP, nodePortStr))
			var clientIP string
			err := wait.PollUntilContextTimeout(ctx, framework.Poll, time.Until(deadline), true, func(ctx context.Context) (bool, error) {
				out, err := e2eoutput.RunHostCmd(namespace, execPod.Name, cmd)
				if !expectResponse {
					return err != nil, nil
				} else if err != nil {
					return false, nil
				}

				clientIP, _, err = net.SplitHostPort(out)
				if err != nil {
					return false, fmt.Errorf("could not parse clientip response %q: %w", out, err)
				}
				return true, nil
			})
			framework.ExpectNoError(err, "expected correct response within timeout")
			if expectResponse && trafficPolicy == v1.ServiceExternalTrafficPolicyLocal {
				gomega.Expect(clientIP).To(gomega.Equal(hostExecPodNodeIP), "expected client IP to be preserved")
			}
		}

		checkOneHealthCheck := func(nodeIP string, expectResponse bool, expectStatus string, deadline time.Time) {
			// "-i" 表示在响应中返回 HTTP 头
			cmd := fmt.Sprintf("curl -g -i -s --connect-timeout 3 http://%s/", net.JoinHostPort(nodeIP, hcNodePortStr))
			err := wait.PollUntilContextTimeout(ctx, framework.Poll, time.Until(deadline), true, func(ctx context.Context) (bool, error) {
				out, err := e2eoutput.RunHostCmd(namespace, execPod.Name, cmd)
				if !expectResponse {
					return err != nil, nil
				} else if err != nil {
					return false, nil
				}

				status := strings.Split(out, "\n")[0]
				gotResponse := strings.Contains(status, expectStatus)
				return gotResponse, nil
			})
			framework.ExpectNoError(err, "expected response containing %s", expectStatus)
		}

		// （每一轮检查中，所有节点都必须在单次
		// KubeProxyEndpointLagTimeout；不希望每次 checkOneNodePort 都等待完整的
		// KubeProxyEndpointLagTimeout 间隔。
		deadline := time.Now().Add(e2eservice.KubeProxyEndpointLagTimeout)

		ginkgo.By("ensuring that the HealthCheckNodePort reports healthy on the endpoint node when ExternalTrafficPolicy is Local")
		checkOneHealthCheck(endpointNodeIP, true, "200 OK", deadline)
		ginkgo.By("ensuring that the HealthCheckNodePort reports UN-healthy on the execpod node when ExternalTrafficPolicy is Local")
		checkOneHealthCheck(hostExecPodNodeIP, true, "503 Service Unavailable", deadline)
		ginkgo.By("ensuring that the HealthCheckNodePort reports UN-healthy on the third node when ExternalTrafficPolicy is Local")
		checkOneHealthCheck(thirdNodeIP, true, "503 Service Unavailable", deadline)

		ginkgo.By("ensuring that the NodePort responds on the endpoint node when ExternalTrafficPolicy is Local")
		checkOneNodePort(endpointNodeIP, true, v1.ServiceExternalTrafficPolicyLocal, deadline)
		ginkgo.By("ensuring that the NodePort responds (due to short-circuit rule) on the execpod node when ExternalTrafficPolicy is Local")
		checkOneNodePort(hostExecPodNodeIP, true, v1.ServiceExternalTrafficPolicyLocal, deadline)
		ginkgo.By("ensuring that the NodePort does not respond on the third node when ExternalTrafficPolicy is Local")
		checkOneNodePort(thirdNodeIP, false, v1.ServiceExternalTrafficPolicyLocal, deadline)

		ginkgo.By("changing ExternalTrafficPolicy to Cluster")
		oldHealthCheckNodePort := svc.Spec.HealthCheckNodePort
		svc, err = jig.UpdateService(ctx, func(svc *v1.Service) {
			svc.Spec.ExternalTrafficPolicy = v1.ServiceExternalTrafficPolicyCluster
		})
		framework.ExpectNoError(err, "updating Service")
		if svc.Spec.HealthCheckNodePort > 0 {
			framework.Failf("Service HealthCheck NodePort still present")
		}
		deadline = time.Now().Add(e2eservice.KubeProxyEndpointLagTimeout)

		ginkgo.By("ensuring that the HealthCheckNodePort no longer responds on the endpoint node when ExternalTrafficPolicy is Cluster")
		checkOneHealthCheck(endpointNodeIP, false, "", deadline)
		ginkgo.By("ensuring that the HealthCheckNodePort no longer responds on the execpod node when ExternalTrafficPolicy is Cluster")
		checkOneHealthCheck(hostExecPodNodeIP, false, "", deadline)
		ginkgo.By("ensuring that the HealthCheckNodePort no longer responds on the third node when ExternalTrafficPolicy is Cluster")
		checkOneHealthCheck(thirdNodeIP, false, "", deadline)

		ginkgo.By("ensuring that the NodePort responds on the endpoint node when ExternalTrafficPolicy is Cluster")
		checkOneNodePort(endpointNodeIP, true, v1.ServiceExternalTrafficPolicyCluster, deadline)
		ginkgo.By("ensuring that the NodePort responds on the execpod node when ExternalTrafficPolicy is Cluster")
		checkOneNodePort(hostExecPodNodeIP, true, v1.ServiceExternalTrafficPolicyCluster, deadline)
		ginkgo.By("ensuring that the NodePort responds on the third node when ExternalTrafficPolicy is Cluster")
		checkOneNodePort(thirdNodeIP, true, v1.ServiceExternalTrafficPolicyCluster, deadline)

		ginkgo.By("changing ExternalTrafficPolicy back to Local")
		_, err = jig.UpdateService(ctx, func(svc *v1.Service) {
			svc.Spec.ExternalTrafficPolicy = v1.ServiceExternalTrafficPolicyLocal
			// 请求与之前相同的 healthCheckNodePort，以覆盖用户指定分配路径
			svc.Spec.HealthCheckNodePort = oldHealthCheckNodePort
		})
		if err != nil {
			// 引入全局静态 NodePort 分配器来跨测试同步，避免其他测试抢占本测试的 NodePort 造成不稳定。
			// 记录有问题的 Service，便于定位出问题的测试。
			// 打印所有 IP，从中查找期望的 IP。
			list, _ := cs.CoreV1().Services(metav1.NamespaceAll).List(ctx, metav1.ListOptions{})
			for _, svc := range list.Items {
				if svc.Spec.HealthCheckNodePort == oldHealthCheckNodePort {
					framework.Failf("Service %#v stole NodePort from current test, please ensure that test is using the Static NodePort Allocator https://github.com/kubernetes/kubernetes/pull/127153 : %v", svc, err)
				}
				for _, port := range svc.Spec.Ports {
					if port.NodePort == oldHealthCheckNodePort {
						framework.Failf("Service %#v stole NodePort from current test, please ensure that test is using the Static NodePort Allocator https://github.com/kubernetes/kubernetes/pull/127153 : %v", svc, err)
					}
				}
			}
		}
		framework.ExpectNoError(err, "updating ExternalTrafficPolicy and HealthCheckNodePort")
		deadline = time.Now().Add(e2eservice.KubeProxyEndpointLagTimeout)

		ginkgo.By("ensuring that all NodePorts and HealthCheckNodePorts show the correct behavior again after changing ExternalTrafficPolicy back to Local")
		checkOneHealthCheck(endpointNodeIP, true, "200 OK", deadline)
		checkOneHealthCheck(hostExecPodNodeIP, true, "503 Service Unavailable", deadline)
		checkOneHealthCheck(thirdNodeIP, true, "503 Service Unavailable", deadline)
		checkOneNodePort(endpointNodeIP, true, v1.ServiceExternalTrafficPolicyLocal, deadline)
		checkOneNodePort(hostExecPodNodeIP, true, v1.ServiceExternalTrafficPolicyLocal, deadline)
		checkOneNodePort(thirdNodeIP, false, v1.ServiceExternalTrafficPolicyLocal, deadline)
	})

	ginkgo.It("should connect to the ports exposed by restartable init containers", func(ctx context.Context) {
		serviceName := "sidecar-with-port"
		ns := f.Namespace.Name

		t := NewServerTest(cs, ns, serviceName)
		defer func() {
			defer ginkgo.GinkgoRecover()
			errs := t.Cleanup()
			if len(errs) != 0 {
				framework.Failf("errors in cleanup: %v", errs)
			}
		}()

		name := "sidecar-port"
		port := int32(8080)
		namedPort := "http-sidecar"

		service := t.BuildServiceSpec()
		service.Spec.Ports = []v1.ServicePort{
			{
				Name:       namedPort,
				Port:       port,
				TargetPort: intstr.FromInt(int(port)),
			},
		}
		ports := []v1.ContainerPort{{Name: namedPort, ContainerPort: port, Protocol: v1.ProtocolTCP}}
		args := []string{"netexec", fmt.Sprintf("--http-port=%d", port)}
		createPodWithRestartableInitContainerOrFail(ctx, f, ns, name, t.Labels, ports, args...)

		ginkgo.By(fmt.Sprintf("creating Service %v with selectors %v", service.Name, service.Spec.Selector))
		service, err := t.CreateService(service)
		framework.ExpectNoError(err)

		checkServiceReachabilityFromExecPod(ctx, f.ClientSet, ns, service.Name, service.Spec.ClusterIP, port)
	})

	ginkgo.It("should connect to the named ports exposed by restartable init containers", func(ctx context.Context) {
		serviceName := "sidecar-with-named-port"
		ns := f.Namespace.Name

		t := NewServerTest(cs, ns, serviceName)
		defer func() {
			defer ginkgo.GinkgoRecover()
			errs := t.Cleanup()
			if len(errs) != 0 {
				framework.Failf("errors in cleanup: %v", errs)
			}
		}()

		name := "sidecar-port"
		port := int32(8080)
		namedPort := "http-sidecar"

		service := t.BuildServiceSpec()
		service.Spec.Ports = []v1.ServicePort{
			{
				Name:       namedPort,
				Port:       port,
				TargetPort: intstr.FromString(namedPort),
			},
		}
		ports := []v1.ContainerPort{{Name: namedPort, ContainerPort: port, Protocol: v1.ProtocolTCP}}
		args := []string{"netexec", fmt.Sprintf("--http-port=%d", port)}
		createPodWithRestartableInitContainerOrFail(ctx, f, ns, name, t.Labels, ports, args...)

		ginkgo.By(fmt.Sprintf("creating Service %v with selectors %v", service.Name, service.Spec.Selector))
		service, err := t.CreateService(service)
		framework.ExpectNoError(err)

		checkServiceReachabilityFromExecPod(ctx, f.ClientSet, ns, service.Name, service.Spec.ClusterIP, port)
	})

	ginkgo.It("should support named targetPorts that resolve to different ports on different endpoints", func(ctx context.Context) {
		serviceName := "mutable-named-port"
		ns := f.Namespace.Name

		t := NewServerTest(cs, ns, serviceName)
		defer func() {
			defer ginkgo.GinkgoRecover()
			errs := t.Cleanup()
			if len(errs) != 0 {
				framework.Failf("errors in cleanup: %v", errs)
			}
		}()

		containerPortName := "mutable-port"
		oldContainerPortNum := int32(8080)
		modifiedPortNum := int32(8000)
		containerPortDesc := []v1.ContainerPort{{Name: containerPortName, ContainerPort: oldContainerPortNum, Protocol: v1.ProtocolTCP}}
		modifiedPortDesc := []v1.ContainerPort{{Name: containerPortName, ContainerPort: modifiedPortNum, Protocol: v1.ProtocolTCP}}
		args := []string{"serve-hostname", fmt.Sprintf("--port=%d", oldContainerPortNum)}
		for _, podName := range []string{"testpod0", "testpod1"} {
			createPodOrFail(ctx, f, ns, podName, t.Labels, containerPortDesc, args...)
		}

		service := t.BuildServiceSpec()
		servicePort := int32(8001)
		service.Spec.Ports = []v1.ServicePort{
			{
				Name:       serviceName,
				Port:       servicePort,
				TargetPort: intstr.FromString(containerPortName),
			},
		}

		ginkgo.By(fmt.Sprintf("creating Service %v with selectors %v", service.Name, service.Spec.Selector))
		service, err := t.CreateService(service)
		framework.ExpectNoError(err)
		jig := e2eservice.NewTestJig(cs, ns, serviceName)
		execPod := e2epod.CreateExecPodOrFail(ctx, cs, ns, "execpod", nil)
		ginkgo.DeferCleanup(func(ctx context.Context) {
			framework.Logf("Cleaning up the exec pod")
			err := cs.CoreV1().Pods(ns).Delete(ctx, execPod.Name, metav1.DeleteOptions{})
			framework.ExpectNoError(err, "failed to delete pod: %s in namespace: %s", execPod.Name, ns)
		})

		err = jig.CheckServiceReachability(ctx, service, execPod)
		framework.ExpectNoError(err)
		if !checkAffinity(ctx, cs, execPod, service.Spec.ClusterIP, int(servicePort), false) {
			framework.Failf("reachability check failed on the service %s (%s:%d), expected to reach both endpoints", service.Name, service.Spec.ClusterIP, servicePort)
		}

		ginkgo.By(fmt.Sprintf("delete pod %s and recreate with a different port number under port name %s", "testpod0", containerPortName))
		args[1] = fmt.Sprintf("--port=%d", modifiedPortNum)
		e2epod.DeletePodOrFail(ctx, f.ClientSet, ns, "testpod0")
		createPodOrFail(ctx, f, ns, "testpod2", t.Labels, modifiedPortDesc, args...)
		err = jig.CheckServiceReachability(ctx, service, execPod)
		framework.ExpectNoError(err)
		if !checkAffinity(ctx, cs, execPod, service.Spec.ClusterIP, int(servicePort), false) {
			framework.Failf("reachability check failed on the service %s (%s:%d), expected to reach both endpoints", service.Name, service.Spec.ClusterIP, servicePort)
		}

		ginkgo.By(fmt.Sprintf("delete pod %s and recreate with a different port number under port name %s", "testpod1", containerPortName))
		e2epod.DeletePodOrFail(ctx, f.ClientSet, ns, "testpod1")
		createPodOrFail(ctx, f, ns, "testpod3", t.Labels, modifiedPortDesc, args...)
		err = jig.CheckServiceReachability(ctx, service, execPod)
		framework.ExpectNoError(err)
		if !checkAffinity(ctx, cs, execPod, service.Spec.ClusterIP, int(servicePort), false) {
			framework.Failf("reachability check failed on the service %s (%s:%d), expected to reach both endpoints", service.Name, service.Spec.ClusterIP, servicePort)
		}
	})
})

// execAffinityTestForSessionAffinityTimeout 封装非 LoadBalancer Service 的会话亲和性测试逻辑。
// 创建 Service 时启用会话亲和性并配置较短的超时，以便验证超时后亲和性必须发生变化。
func execAffinityTestForSessionAffinityTimeout(ctx context.Context, f *framework.Framework, cs clientset.Interface, svc *v1.Service) {
	ns := f.Namespace.Name
	numPods, servicePort, serviceName := 3, defaultServeHostnameServicePort, svc.ObjectMeta.Name
	ginkgo.By("creating service in namespace " + ns)
	serviceType := svc.Spec.Type
	// 将亲和性超时设为与连接请求数相等
	svcSessionAffinityTimeout := int32(SessionAffinityTimeout)
	svc.Spec.SessionAffinity = v1.ServiceAffinityClientIP
	svc.Spec.SessionAffinityConfig = &v1.SessionAffinityConfig{
		ClientIP: &v1.ClientIPConfig{TimeoutSeconds: &svcSessionAffinityTimeout},
	}
	_, _, err := StartServeHostnameService(ctx, cs, svc, ns, numPods)
	framework.ExpectNoError(err, "failed to create deployment with service in the namespace: %s", ns)
	ginkgo.DeferCleanup(StopServeHostnameService, cs, ns, serviceName)
	jig := e2eservice.NewTestJig(cs, ns, serviceName)
	svc, err = jig.Client.CoreV1().Services(ns).Get(ctx, serviceName, metav1.GetOptions{})
	framework.ExpectNoError(err, "failed to fetch service: %s in namespace: %s", serviceName, ns)
	var svcIP string
	if serviceType == v1.ServiceTypeNodePort {
		nodes, err := e2enode.GetReadySchedulableNodes(ctx, cs)
		framework.ExpectNoError(err)
		// 节点地址必须与 ClusterIP 属于同一 IP 族
		family := svc.Spec.IPFamilies[0]
		svcIP = e2enode.FirstAddressByTypeAndFamily(nodes, v1.NodeInternalIP, family)
		if svcIP == "" {
			svcIP = e2enode.FirstAddressByTypeAndFamily(nodes, v1.NodeExternalIP, family)
		}
		gomega.Expect(svcIP).NotTo(gomega.BeEmpty(), "failed to get Node IP for family: %s", family)
		servicePort = int(svc.Spec.Ports[0].NodePort)
	} else {
		svcIP = svc.Spec.ClusterIP
	}

	execPod := e2epod.CreateExecPodOrFail(ctx, cs, ns, "execpod-affinity", nil)
	ginkgo.DeferCleanup(func(ctx context.Context) {
		framework.Logf("Cleaning up the exec pod")
		err := cs.CoreV1().Pods(ns).Delete(ctx, execPod.Name, metav1.DeleteOptions{})
		framework.ExpectNoError(err, "failed to delete pod: %s in namespace: %s", execPod.Name, ns)
	})
	err = jig.CheckServiceReachability(ctx, svc, execPod)
	framework.ExpectNoError(err)

	// 超时前 Service 应保持会话粘滞
	if !checkAffinity(ctx, cs, execPod, svcIP, servicePort, true) {
		framework.Failf("the service %s (%s:%d) should be sticky until the timeout expires", svc.Name, svcIP, servicePort)
	}
	// 但超时后应返回不同的 hostname
	// 多次尝试，避免碰巧两次命中同一个 Pod
	hosts := sets.NewString()
	cmd := fmt.Sprintf(`curl -q -s --connect-timeout 2 http://%s/`, net.JoinHostPort(svcIP, strconv.Itoa(servicePort)))
	for range 10 {
		hostname, err := e2eoutput.RunHostCmd(execPod.Namespace, execPod.Name, cmd)
		if err == nil {
			hosts.Insert(hostname)
			if hosts.Len() > 1 {
				return
			}
			// 某些情况下 ipvs 在超时后未删除持久连接；用 'ipvsadm -lnc' 可以看到过期时间从 '00:00' 变成 '13171233:02'
			//
			// pro expire state       source             virtual            destination
			// TCP 00:00  NONE        10.105.253.160:0   10.105.253.160:80  10.244.1.25:9376
			//
			// pro expire state       source             virtual            destination
			// TCP 13171233:02 NONE        10.105.253.160:0   10.105.253.160:80  10.244.1.25:9376
			//
			// 再过 2 秒连接才会确保被删除，因此这里等待 'svcSessionAffinityTimeout+5' 秒规避该问题。
			// TODO: 查明过期连接未被删除的原因，并在 ipvs 侧修复。
			time.Sleep(time.Duration(svcSessionAffinityTimeout+5) * time.Second)
		}
	}
	framework.Fail("Session is sticky after reaching the timeout")
}

func execAffinityTestForNonLBServiceWithTransition(ctx context.Context, f *framework.Framework, cs clientset.Interface, svc *v1.Service) {
	execAffinityTestForNonLBServiceWithOptionalTransition(ctx, f, cs, svc, true)
}

func execAffinityTestForNonLBService(ctx context.Context, f *framework.Framework, cs clientset.Interface, svc *v1.Service) {
	execAffinityTestForNonLBServiceWithOptionalTransition(ctx, f, cs, svc, false)
}

// execAffinityTestForNonLBServiceWithOptionalTransition 封装非 LoadBalancer Service 的会话亲和性测试逻辑。
// 创建 Service 时启用会话亲和性。若 isTransitionTest 为 true，则会关闭/重新开启会话亲和性，
// 并验证 Service 能否收敛到稳定的亲和状态。
func execAffinityTestForNonLBServiceWithOptionalTransition(ctx context.Context, f *framework.Framework, cs clientset.Interface, svc *v1.Service, isTransitionTest bool) {
	ns := f.Namespace.Name
	numPods, servicePort, serviceName := 3, defaultServeHostnameServicePort, svc.ObjectMeta.Name
	ginkgo.By("creating service in namespace " + ns)
	serviceType := svc.Spec.Type
	svc.Spec.SessionAffinity = v1.ServiceAffinityClientIP
	_, _, err := StartServeHostnameService(ctx, cs, svc, ns, numPods)
	framework.ExpectNoError(err, "failed to create deployment with service in the namespace: %s", ns)
	ginkgo.DeferCleanup(StopServeHostnameService, cs, ns, serviceName)
	jig := e2eservice.NewTestJig(cs, ns, serviceName)
	svc, err = jig.Client.CoreV1().Services(ns).Get(ctx, serviceName, metav1.GetOptions{})
	framework.ExpectNoError(err, "failed to fetch service: %s in namespace: %s", serviceName, ns)
	var svcIP string
	if serviceType == v1.ServiceTypeNodePort {
		nodes, err := e2enode.GetReadySchedulableNodes(ctx, cs)
		framework.ExpectNoError(err)
		// 节点地址必须与 ClusterIP 属于同一 IP 族
		family := svc.Spec.IPFamilies[0]
		svcIP = e2enode.FirstAddressByTypeAndFamily(nodes, v1.NodeInternalIP, family)
		if svcIP == "" {
			svcIP = e2enode.FirstAddressByTypeAndFamily(nodes, v1.NodeExternalIP, family)
		}
		gomega.Expect(svcIP).NotTo(gomega.BeEmpty(), "failed to get Node IP for family: %s", family)
		servicePort = int(svc.Spec.Ports[0].NodePort)
	} else {
		svcIP = svc.Spec.ClusterIP
	}

	execPod := e2epod.CreateExecPodOrFail(ctx, cs, ns, "execpod-affinity", nil)
	ginkgo.DeferCleanup(func(ctx context.Context) {
		framework.Logf("Cleaning up the exec pod")
		err := cs.CoreV1().Pods(ns).Delete(ctx, execPod.Name, metav1.DeleteOptions{})
		framework.ExpectNoError(err, "failed to delete pod: %s in namespace: %s", execPod.Name, ns)
	})
	err = jig.CheckServiceReachability(ctx, svc, execPod)
	framework.ExpectNoError(err)

	if !isTransitionTest {
		if !checkAffinity(ctx, cs, execPod, svcIP, servicePort, true) {
			framework.Failf("Failed to check affinity for service %s/%s", ns, svc.Name)
		}
	}
	if isTransitionTest {
		_, err = jig.UpdateService(ctx, func(svc *v1.Service) {
			svc.Spec.SessionAffinity = v1.ServiceAffinityNone
		})
		framework.ExpectNoError(err)
		if !checkAffinity(ctx, cs, execPod, svcIP, servicePort, false) {
			framework.Failf("Failed to check affinity for service %s/%s without session affinity", ns, svc.Name)
		}
		_, err = jig.UpdateService(ctx, func(svc *v1.Service) {
			svc.Spec.SessionAffinity = v1.ServiceAffinityClientIP
		})
		framework.ExpectNoError(err)
		if !checkAffinity(ctx, cs, execPod, svcIP, servicePort, true) {
			framework.Failf("Failed to check affinity for service %s/%s with session affinity", ns, svc.Name)
		}
	}
}

func execAffinityTestForLBServiceWithTransition(ctx context.Context, f *framework.Framework, cs clientset.Interface, svc *v1.Service) {
	execAffinityTestForLBServiceWithOptionalTransition(ctx, f, cs, svc, true)
}

func execAffinityTestForLBService(ctx context.Context, f *framework.Framework, cs clientset.Interface, svc *v1.Service) {
	execAffinityTestForLBServiceWithOptionalTransition(ctx, f, cs, svc, false)
}

// execAffinityTestForLBServiceWithOptionalTransition 封装 LoadBalancer Service 的会话亲和性测试逻辑，
// 与 execAffinityTestForNonLBServiceWithOptionalTransition 类似。
func execAffinityTestForLBServiceWithOptionalTransition(ctx context.Context, f *framework.Framework, cs clientset.Interface, svc *v1.Service, isTransitionTest bool) {
	numPods, ns, serviceName := 3, f.Namespace.Name, svc.ObjectMeta.Name

	ginkgo.By("creating service in namespace " + ns)
	svc.Spec.SessionAffinity = v1.ServiceAffinityClientIP
	_, _, err := StartServeHostnameService(ctx, cs, svc, ns, numPods)
	framework.ExpectNoError(err, "failed to create deployment with service in the namespace: %s", ns)
	jig := e2eservice.NewTestJig(cs, ns, serviceName)
	ginkgo.By("waiting for loadbalancer for service " + ns + "/" + serviceName)
	svc, err = jig.WaitForLoadBalancer(ctx, e2eservice.GetServiceLoadBalancerCreationTimeout(ctx, cs))
	framework.ExpectNoError(err)
	ginkgo.DeferCleanup(func(ctx context.Context) {
		podNodePairs, err := e2enode.PodNodePairs(ctx, cs, ns)
		framework.Logf("[pod,node] pairs: %+v; err: %v", podNodePairs, err)
		_ = StopServeHostnameService(ctx, cs, ns, serviceName)
		lb := cloudprovider.DefaultLoadBalancerName(svc)
		framework.Logf("cleaning load balancer resource for %s", lb)
		e2eservice.CleanupServiceResources(ctx, cs, lb, framework.TestContext.CloudConfig.Region, framework.TestContext.CloudConfig.Zone)
	})
	ingressIP := e2eservice.GetIngressPoint(&svc.Status.LoadBalancer.Ingress[0])
	port := int(svc.Spec.Ports[0].Port)

	if !isTransitionTest {
		if !checkAffinity(ctx, cs, nil, ingressIP, port, true) {
			framework.Failf("Failed to verify affinity for loadbalance service %s/%s", ns, serviceName)
		}
	}
	if isTransitionTest {
		svc, err = jig.UpdateService(ctx, func(svc *v1.Service) {
			svc.Spec.SessionAffinity = v1.ServiceAffinityNone
		})
		framework.ExpectNoError(err)
		if !checkAffinity(ctx, cs, nil, ingressIP, port, false) {
			framework.Failf("Failed to verify affinity for loadbalance service %s/%s without session affinity ", ns, serviceName)
		}
		svc, err = jig.UpdateService(ctx, func(svc *v1.Service) {
			svc.Spec.SessionAffinity = v1.ServiceAffinityClientIP
		})
		framework.ExpectNoError(err)
		if !checkAffinity(ctx, cs, nil, ingressIP, port, true) {
			framework.Failf("Failed to verify affinity for loadbalance service %s/%s with session affinity ", ns, serviceName)
		}
	}
}

func createAndGetExternalServiceFQDN(ctx context.Context, cs clientset.Interface, ns, serviceName string) string {
	_, _, err := StartServeHostnameService(ctx, cs, getServeHostnameService(serviceName), ns, 2)
	framework.ExpectNoError(err, "Expected Service %s to be running", serviceName)
	return fmt.Sprintf("%s.%s.svc.%s", serviceName, ns, framework.TestContext.ClusterDNSDomain)
}

func createPausePodDeployment(ctx context.Context, cs clientset.Interface, name, ns string, replicas int) *appsv1.Deployment {
	labels := map[string]string{"deployment": "agnhost-pause"}
	pauseDeployment := e2edeployment.NewDeployment(name, int32(replicas), labels, "", "", appsv1.RollingUpdateDeploymentStrategyType)

	pauseDeployment.Spec.Template.Spec.Containers[0] = e2epod.NewAgnhostContainer("agnhost-pause", nil, nil, "pause")
	pauseDeployment.Spec.Template.Spec.Affinity = &v1.Affinity{
		PodAntiAffinity: &v1.PodAntiAffinity{
			RequiredDuringSchedulingIgnoredDuringExecution: []v1.PodAffinityTerm{
				{
					LabelSelector: &metav1.LabelSelector{MatchLabels: labels},
					TopologyKey:   "kubernetes.io/hostname",
					Namespaces:    []string{ns},
				},
			},
		},
	}

	deployment, err := cs.AppsV1().Deployments(ns).Create(ctx, pauseDeployment, metav1.CreateOptions{})
	framework.ExpectNoError(err, "Error in creating deployment for pause pod")
	return deployment
}

// createPodOrFail 创建带有指定 containerPorts 的 Pod。
func createPodOrFail(ctx context.Context, f *framework.Framework, ns, name string, labels map[string]string, containerPorts []v1.ContainerPort, args ...string) {
	ginkgo.By(fmt.Sprintf("Creating pod %s in namespace %s", name, ns))
	pod := e2epod.NewAgnhostPod(ns, name, nil, nil, containerPorts, args...)
	pod.ObjectMeta.Labels = labels
	// 添加一个无用环境变量，绕过 docker 的问题。
	// https://github.com/docker/docker/issues/14203
	pod.Spec.Containers[0].Env = []v1.EnvVar{{Name: "FOO", Value: " "}}
	e2epod.NewPodClient(f).CreateSync(ctx, pod)
}

// createPodWithRestartableInitContainerOrFail 创建带有可重启 init container 及指定 containerPorts 的 Pod。
func createPodWithRestartableInitContainerOrFail(ctx context.Context, f *framework.Framework, ns, name string, labels map[string]string, containerPorts []v1.ContainerPort, args ...string) {
	ginkgo.By(fmt.Sprintf("Creating pod %s with restartable init containers in namespace %s", name, ns))
	pod := e2epod.NewAgnhostPod(ns, name, nil, nil, nil, "pause")
	pod.ObjectMeta.Labels = labels
	restartPolicyAlways := v1.ContainerRestartPolicyAlways
	init := e2epod.NewAgnhostContainer(name, nil, containerPorts, args...)
	init.RestartPolicy = &restartPolicyAlways
	pod.Spec.InitContainers = []v1.Container{init}
	e2epod.NewPodClient(f).CreateSync(ctx, pod)
}

// launchHostExecPod 在指定命名空间启动 hostexec Pod 并等待其进入 Running。
// 若 avoidNode 非 nil，则确保 Pod 不会被调度到该节点。
func launchHostExecPod(ctx context.Context, client clientset.Interface, ns, name string, avoidNode *string) *v1.Pod {
	framework.Logf("Creating new host exec pod")
	hostExecPod := e2epod.NewExecPodSpec(ns, name, true)
	if avoidNode != nil {
		hostExecPod.Spec.Affinity = &v1.Affinity{
			NodeAffinity: &v1.NodeAffinity{
				RequiredDuringSchedulingIgnoredDuringExecution: &v1.NodeSelector{
					NodeSelectorTerms: []v1.NodeSelectorTerm{{
						MatchFields: []v1.NodeSelectorRequirement{{
							Key:      "metadata.name",
							Operator: "NotIn",
							Values:   []string{*avoidNode},
						}},
					}},
				},
			},
		}
	}
	_, err := client.CoreV1().Pods(ns).Create(ctx, hostExecPod, metav1.CreateOptions{})
	framework.ExpectNoError(err, "creating host exec pod")
	err = e2epod.WaitTimeoutForPodReadyInNamespace(ctx, client, name, ns, framework.PodStartTimeout)
	framework.ExpectNoError(err, "waiting for host exec pod")
	// 重新获取 PodIP 等信息
	pod, err := client.CoreV1().Pods(ns).Get(ctx, name, metav1.GetOptions{})
	framework.ExpectNoError(err, "getting podIP of host exec pod")
	return pod
}

// checkReachabilityFromPod 检查从指定 Pod 出发的连通性。
func checkReachabilityFromPod(ctx context.Context, expectToBeReachable bool, timeout time.Duration, namespace, pod, target string) {
	cmd := fmt.Sprintf("wget -T 5 -qO- %q", target)
	err := wait.PollUntilContextTimeout(ctx, framework.Poll, timeout, true, func(ctx context.Context) (bool, error) {
		_, err := e2eoutput.RunHostCmd(namespace, pod, cmd)
		if expectToBeReachable && err != nil {
			framework.Logf("Expect target to be reachable. But got err: %v. Retry until timeout", err)
			return false, nil
		}

		if !expectToBeReachable && err == nil {
			framework.Logf("Expect target NOT to be reachable. But it is reachable. Retry until timeout")
			return false, nil
		}
		return true, nil
	})
	framework.ExpectNoError(err)
}

// checkServiceReachabilityFromExecPod 创建专用客户端 Pod，进入其中检查到指定目标主机和端口的连通性。
func checkServiceReachabilityFromExecPod(ctx context.Context, client clientset.Interface, namespace, name, clusterIP string, port int32) {
	// 这里不依赖通过 Service 名称进行 DNS 查询，因为只想测试命名端口能否从该 Service 访问。
	serverHost := net.JoinHostPort(clusterIP, strconv.Itoa(int(port)))
	ginkgo.By("creating a dedicated client to send request to the http server " + serverHost)
	execPod := e2epod.CreateExecPodOrFail(ctx, client, namespace, "execpod-", nil)
	execPodName := execPod.Name
	cmd := fmt.Sprintf("curl -q -s --connect-timeout 2 http://%s/", serverHost)
	var stdout string
	if pollErr := wait.PollUntilContextTimeout(ctx, framework.Poll, e2eservice.KubeProxyLagTimeout, true, func(ctx context.Context) (bool, error) {
		var err error
		stdout, err = e2eoutput.RunHostCmd(namespace, execPodName, cmd)
		if err != nil {
			framework.Logf("error trying to connect to service %s: %v, ... retrying", name, err)
			return false, nil
		}
		return true, nil
	}); pollErr != nil {
		framework.Failf("connection to the Service %v within %v should be succeeded, stdout: %v", name, e2eservice.KubeProxyLagTimeout, stdout)
	}
}

func restartApiserver(ctx context.Context, namespace string, cs clientset.Interface) error {
	return restartComponent(ctx, cs, kubeAPIServerLabelName, metav1.NamespaceSystem, map[string]string{clusterComponentKey: kubeAPIServerLabelName})
}

// restartComponent 重启组件对应的 static Pod
func restartComponent(ctx context.Context, cs clientset.Interface, cName, ns string, matchLabels map[string]string) error {
	pods, err := e2epod.GetPods(ctx, cs, ns, matchLabels)
	if err != nil {
		return fmt.Errorf("failed to get %s's pods, err: %w", cName, err)
	}
	if len(pods) == 0 {
		return fmt.Errorf("%s pod count is 0", cName)
	}

	if err := e2epod.DeletePodsWithGracePeriod(ctx, cs, pods, 0); err != nil {
		return fmt.Errorf("failed to restart component: %s, err: %w", cName, err)
	}

	_, err = e2epod.PodsCreatedByLabel(ctx, cs, ns, cName, int32(len(pods)), labels.SelectorFromSet(matchLabels))
	return err
}
