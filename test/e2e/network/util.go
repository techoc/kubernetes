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

package network

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"regexp"
	"strings"
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/kubernetes/test/e2e/framework"
	e2eendpointslice "k8s.io/kubernetes/test/e2e/framework/endpointslice"
	e2ekubectl "k8s.io/kubernetes/test/e2e/framework/kubectl"
	e2enetwork "k8s.io/kubernetes/test/e2e/framework/network"
	e2eoutput "k8s.io/kubernetes/test/e2e/framework/pod/output"
	"k8s.io/kubernetes/test/e2e/storage/utils"
)

// secondNodePortSvcName 是第二个 node port service 的名称
const secondNodePortSvcName = "second-node-port-service"

// GetHTTPContent 通过 HTTP 返回给定 url 的内容。
func GetHTTPContent(host string, port int, timeout time.Duration, url string) (string, error) {
	var body bytes.Buffer
	pollErr := wait.PollImmediate(framework.Poll, timeout, func() (bool, error) {
		result := e2enetwork.PokeHTTP(host, port, url, nil)
		if result.Status == e2enetwork.HTTPSuccess {
			body.Write(result.Body)
			return true, nil
		}
		return false, nil
	})
	if pollErr != nil {
		framework.Logf("Could not reach HTTP service through %v:%v%v after %v: %v", host, port, url, timeout, pollErr)
	}
	return body.String(), pollErr
}

// GetHTTPContentFromTestContainer 通过测试容器用 HTTP 返回给定 url 的内容。
func GetHTTPContentFromTestContainer(ctx context.Context, config *e2enetwork.NetworkingTestConfig, host string, port int, timeout time.Duration, dialCmd string) (string, error) {
	var body string
	pollFn := func() (bool, error) {
		resp, err := config.GetResponseFromTestContainer(ctx, "http", dialCmd, host, port)
		if err != nil || len(resp.Errors) > 0 || len(resp.Responses) == 0 {
			return false, nil
		}
		body = resp.Responses[0]
		return true, nil
	}
	if pollErr := wait.PollImmediate(framework.Poll, timeout, pollFn); pollErr != nil {
		return "", pollErr
	}
	return body, nil
}

// DescribeSvc 记录给定 namespace 下 kubectl describe svc 的输出
func DescribeSvc(ns string) {
	framework.Logf("\nOutput of kubectl describe svc:\n")
	desc, _ := e2ekubectl.RunKubectl(
		ns, "describe", "svc", fmt.Sprintf("--namespace=%v", ns))
	framework.Logf("%s", desc)
}

// CheckSCTPModuleLoadedOnNodes 检查列表中是否有节点加载了
// sctp.ko 模块
// 出于安全考虑，也为了允许集群使用用户态 SCTP 实现，
// 我们要求仅创建 SCTP Pod/Service/NetworkPolicy 不得导致
// sctp 内核模块被加载。
func CheckSCTPModuleLoadedOnNodes(ctx context.Context, f *framework.Framework, nodes *v1.NodeList) bool {
	hostExec := utils.NewHostExec(f)
	ginkgo.DeferCleanup(hostExec.Cleanup)
	re := regexp.MustCompile(`^\s*sctp\s+`)
	cmd := "lsmod | grep sctp"
	for _, node := range nodes.Items {
		framework.Logf("Executing cmd %q on node %v", cmd, node.Name)
		result, err := hostExec.IssueCommandWithResult(ctx, cmd, &node)
		if err != nil {
			framework.Logf("sctp module is not loaded or error occurred while executing command %s on node: %v", cmd, err)
		}
		for _, line := range strings.Split(result, "\n") {
			if found := re.Find([]byte(line)); found != nil {
				framework.Logf("the sctp module is loaded on node: %v", node.Name)
				return true
			}
		}
		framework.Logf("the sctp module is not loaded on node: %v", node.Name)
	}
	return false
}

// execSourceIPTest 从给定 Pod 执行 curl 访问目标地址的 "/clientip" 端点，
// 检查源 IP 是否被保留。
func execSourceIPTest(sourcePod v1.Pod, targetAddr string) (string, string) {
	var (
		err     error
		stdout  string
		timeout = 2 * time.Minute
	)

	framework.Logf("Waiting up to %v to get response from %s", timeout, targetAddr)
	cmd := fmt.Sprintf(`curl -q -s --connect-timeout 30 %s/clientip`, targetAddr)
	for start := time.Now(); time.Since(start) < timeout; time.Sleep(2 * time.Second) {
		stdout, err = e2eoutput.RunHostCmd(sourcePod.Namespace, sourcePod.Name, cmd)
		if err != nil {
			framework.Logf("got err: %v, retry until timeout", err)
			continue
		}
		// 需要检查输出，因为出错时可能会省略。
		if strings.TrimSpace(stdout) == "" {
			framework.Logf("got empty stdout, retry until timeout")
			continue
		}
		break
	}

	framework.ExpectNoError(err)

	// RunHostCmd 返回的 stdout 格式为 x.x.x.x:port 或 [xx:xx:xx::x]:port
	host, _, err := net.SplitHostPort(stdout)
	if err != nil {
		// 输出格式不符预期时让 ginkgo 测试失败。
		framework.Failf("exec pod returned unexpected stdout: [%v]\n", stdout)
	}
	return sourcePod.Status.PodIP, host
}

// execHostnameTest 从给定 Pod 执行 curl 访问目标地址的 "/hostname" 端点，
// 检查目标目标的 hostname。
// 它还会把 FQDN 转成 hostname，因此如果
// targetHostname 传入 FQDN，比较时只考虑 hostname 部分。
func execHostnameTest(sourcePod v1.Pod, targetAddr, targetHostname string) {
	var (
		err     error
		stdout  string
		timeout = 2 * time.Minute
	)

	framework.Logf("Waiting up to %v to get response from %s", timeout, targetAddr)
	cmd := fmt.Sprintf(`curl -q -s --max-time 30 %s/hostname`, targetAddr)
	for start := time.Now(); time.Since(start) < timeout; time.Sleep(2 * time.Second) {
		stdout, err = e2eoutput.RunHostCmd(sourcePod.Namespace, sourcePod.Name, cmd)
		if err != nil {
			framework.Logf("got err: %v, retry until timeout", err)
			continue
		}
		// 需要检查输出，因为出错时可能会省略。
		if strings.TrimSpace(stdout) == "" {
			framework.Logf("got empty stdout, retry until timeout")
			continue
		}
		break
	}

	// 确保比较的是 hostname 而非 FQDN
	targetHostname = strings.Split(targetHostname, ".")[0]
	hostname := strings.TrimSpace(strings.Split(stdout, ".")[0])

	framework.ExpectNoError(err)
	gomega.Expect(hostname).To(gomega.Equal(targetHostname))
}

// execNodenameTest 从给定 Pod 执行 curl 访问目标地址的 "/envvar?var=NODE_NAME" 端点。
// 与 execHostnameTest 不同，它不去掉 FQDN 后缀，因为 Kubernetes
// node 名称始终是短标识符，不是 FQDN。
// xref: https://issues.k8s.io/121018
func execNodenameTest(sourcePod v1.Pod, targetAddr, expectedNodeName string) {
	var (
		err     error
		stdout  string
		timeout = 2 * time.Minute
	)

	framework.Logf("Waiting up to %v to get response from %s", timeout, targetAddr)
	cmd := fmt.Sprintf(`curl -q -s --max-time 30 '%s/envvar?var=NODE_NAME'`, targetAddr)
	for start := time.Now(); time.Since(start) < timeout; time.Sleep(2 * time.Second) {
		stdout, err = e2eoutput.RunHostCmd(sourcePod.Namespace, sourcePod.Name, cmd)
		if err != nil {
			framework.Logf("got err: %v, retry until timeout", err)
			continue
		}
		if strings.TrimSpace(stdout) == "" {
			framework.Logf("got empty stdout, retry until timeout")
			continue
		}
		break
	}

	framework.ExpectNoError(err, "not able to connect to %s after %v", targetAddr, timeout)
	gomega.Expect(strings.TrimSpace(stdout)).To(gomega.Equal(expectedNodeName))
}

// createSecondNodePortService 创建一个与 config.NodePortService 使用相同 selector 和相同 HTTP 端口的 service
func createSecondNodePortService(ctx context.Context, f *framework.Framework, config *e2enetwork.NetworkingTestConfig) (*v1.Service, int) {
	svc := &v1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name: secondNodePortSvcName,
		},
		Spec: v1.ServiceSpec{
			Type: v1.ServiceTypeNodePort,
			Ports: []v1.ServicePort{
				{
					Port:       e2enetwork.ClusterHTTPPort,
					Name:       "http",
					Protocol:   v1.ProtocolTCP,
					TargetPort: intstr.FromInt32(e2enetwork.EndpointHTTPPort),
				},
			},
			Selector: config.NodePortService.Spec.Selector,
		},
	}

	createdService := config.CreateService(ctx, svc)

	err := e2eendpointslice.WaitForEndpointCount(ctx, f.ClientSet, config.Namespace, secondNodePortSvcName, len(config.EndpointPods))
	framework.ExpectNoError(err, "failed to validate endpoints for service %s in namespace: %s", secondNodePortSvcName, config.Namespace)

	var httpPort int
	for _, p := range createdService.Spec.Ports {
		switch p.Protocol {
		case v1.ProtocolTCP:
			httpPort = int(p.NodePort)
		default:
			continue
		}
	}

	return createdService, httpPort
}

// testEndpointReachability 测试到 endpoint（即 IP、ServiceName）和端口的连通性。测试请求从指定的 execPod 发起。
// 目前支持基于 TCP 和 UDP 协议的 service
func testEndpointReachability(ctx context.Context, endpoint string, port int32, protocol v1.Protocol, execPod *v1.Pod, timeout time.Duration) error {
	cmd := ""
	switch protocol {
	case v1.ProtocolTCP:
		cmd = fmt.Sprintf("echo hostName | nc -v -t -w 2 %s %v", endpoint, port)
	case v1.ProtocolUDP:
		cmd = fmt.Sprintf("echo hostName | nc -v -u -w 2 %s %v", endpoint, port)
	default:
		return fmt.Errorf("service reachability check is not supported for %v", protocol)
	}

	err := wait.PollUntilContextTimeout(ctx, framework.Poll, timeout, true, func(ctx context.Context) (bool, error) {
		stdout, err := e2eoutput.RunHostCmd(execPod.Namespace, execPod.Name, cmd)
		if err != nil {
			framework.Logf("Service reachability failing with error: %v\nRetrying...", err)
			return false, nil
		}
		trimmed := strings.TrimSpace(stdout)
		if trimmed != "" {
			return true, nil
		}
		return false, nil
	})
	if err != nil {
		return fmt.Errorf("service is not reachable within %v timeout on endpoint %s %d over %s protocol", timeout, endpoint, port, protocol)
	}
	return nil
}
