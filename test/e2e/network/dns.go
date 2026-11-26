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

package network

import (
	"context"
	"fmt"
	"strings"
	"time"

	v1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/kubernetes/pkg/features"
	"k8s.io/kubernetes/test/e2e/framework"
	e2enode "k8s.io/kubernetes/test/e2e/framework/node"
	e2epod "k8s.io/kubernetes/test/e2e/framework/pod"
	e2eoutput "k8s.io/kubernetes/test/e2e/framework/pod/output"
	e2eservice "k8s.io/kubernetes/test/e2e/framework/service"
	"k8s.io/kubernetes/test/e2e/network/common"
	imageutils "k8s.io/kubernetes/test/utils/image"
	admissionapi "k8s.io/pod-security-admission/api"
	"k8s.io/utils/ptr"

	"github.com/onsi/ginkgo/v2"
)

const dnsTestPodHostName = "dns-querier-1"
const dnsTestServiceName = "dns-test-service"

var _ = common.SIGDescribe("DNS", func() {
	f := framework.NewDefaultFramework("dns")
	f.NamespacePodSecurityLevel = admissionapi.LevelBaseline

	/*
		发布版本：v1.9
		测试名称：DNS, cluster
		描述：创建 Pod 后，该 pod 必须能通过 DNS 解析集群 dns 条目，如 kubernetes.default。
	*/
	framework.ConformanceIt("should provide DNS for the cluster", func(ctx context.Context) {
		// 我们需要能够解析的所有名称。
		// TODO: 启动一个独立的测试 service，验证对该 service 的 dns 解析可用。
		// 注意：这里只包含 FQDN 和主机名；部分名称的测试见下面的用例
		namesToResolve := []string{
			fmt.Sprintf("kubernetes.default.svc.%s", framework.TestContext.ClusterDNSDomain),
		}
		// TODO: 双栈下要同时验证 IPv4 和 IPv6 地址族
		// TODO: 应该改变整个测试机制，用同一组探测
		// 针对一组已知的不同基础镜像运行。目前只针对
		// agnhost（Alpine，使用 musl libc）和 glibc-dns-testing
		// （顾名思义，使用 glibc）测试。
		muslProbeCmd, muslFileNames := createProbeCommand(namesToResolve, nil, "", "musl", f.Namespace.Name, framework.TestContext.ClusterDNSDomain, framework.TestContext.ClusterIsIPv6())
		muslProber := dnsQuerier{name: "musl", image: imageutils.Agnhost, cmd: muslProbeCmd}
		glibcProbeCmd, glibcFileNames := createProbeCommand(namesToResolve, nil, "", "glibc", f.Namespace.Name, framework.TestContext.ClusterDNSDomain, framework.TestContext.ClusterIsIPv6())
		glibcProber := dnsQuerier{name: "glibc", image: imageutils.GlibcDnsTesting, cmd: glibcProbeCmd}
		ginkgo.By("Running these commands against musl: " + muslProbeCmd + "\n")
		ginkgo.By("Running these commands against glibc: " + glibcProbeCmd + "\n")

		// 运行一个探测 DNS 并通过 HTTP 暴露结果的 pod。
		ginkgo.By("creating a pod to probe DNS")
		pod := createDNSPod(f.Namespace.Name, []dnsQuerier{muslProber, glibcProber}, dnsTestPodHostName, dnsTestServiceName)
		validateDNSResults(ctx, f, pod, append(muslFileNames, glibcFileNames...))
	})

	// 因 #8512 添加。这对 GCE 和 GKE 部署至关重要。
	f.It("should provide DNS for the cluster", f.WithProvider("gce"), func(ctx context.Context) {
		namesToResolve := []string{"google.com"}
		// Windows 容器默认没有到 GCE metadata server 的路由。
		if !framework.NodeOSDistroIs("windows") {
			namesToResolve = append(namesToResolve, "metadata")
		}

		// TODO: 双栈下要同时验证 IPv4 和 IPv6 地址族
		muslProbeCmd, muslFileNames := createProbeCommand(namesToResolve, nil, "", "musl", f.Namespace.Name, framework.TestContext.ClusterDNSDomain, framework.TestContext.ClusterIsIPv6())
		muslProber := dnsQuerier{name: "musl", image: imageutils.Agnhost, cmd: muslProbeCmd}
		glibcProbeCmd, glibcFileNames := createProbeCommand(namesToResolve, nil, "", "glibc", f.Namespace.Name, framework.TestContext.ClusterDNSDomain, framework.TestContext.ClusterIsIPv6())
		glibcProber := dnsQuerier{name: "glibc", image: imageutils.GlibcDnsTesting, cmd: glibcProbeCmd}
		ginkgo.By("Running these commands against musl: " + muslProbeCmd + "\n")
		ginkgo.By("Running these commands against glibc: " + glibcProbeCmd + "\n")

		// 运行一个探测 DNS 并通过 HTTP 暴露结果的 pod。
		ginkgo.By("creating a pod to probe DNS")
		pod := createDNSPod(f.Namespace.Name, []dnsQuerier{muslProber, glibcProber}, dnsTestPodHostName, dnsTestServiceName)
		validateDNSResults(ctx, f, pod, append(muslFileNames, glibcFileNames...))
	})

	// [LinuxOnly]：Windows 目前不支持解析 PQDN。
	ginkgo.It("should resolve DNS of partial qualified names for the cluster [LinuxOnly]", func(ctx context.Context) {
		// 我们需要能够解析的所有名称。
		namesToResolve := []string{
			"kubernetes.default",
			"kubernetes.default.svc",
		}
		hostFQDN := fmt.Sprintf("%s.%s.%s.svc.%s", dnsTestPodHostName, dnsTestServiceName, f.Namespace.Name, framework.TestContext.ClusterDNSDomain)
		hostEntries := []string{hostFQDN, dnsTestPodHostName}
		// TODO: 双栈下要同时验证 IPv4 和 IPv6 地址族
		muslProbeCmd, muslFileNames := createProbeCommand(namesToResolve, hostEntries, "", "musl", f.Namespace.Name, framework.TestContext.ClusterDNSDomain, framework.TestContext.ClusterIsIPv6())
		muslProber := dnsQuerier{name: "musl", image: imageutils.Agnhost, cmd: muslProbeCmd}
		glibcProbeCmd, glibcFileNames := createProbeCommand(namesToResolve, hostEntries, "", "glibc", f.Namespace.Name, framework.TestContext.ClusterDNSDomain, framework.TestContext.ClusterIsIPv6())
		glibcProber := dnsQuerier{name: "glibc", image: imageutils.GlibcDnsTesting, cmd: glibcProbeCmd}
		ginkgo.By("Running these commands against musl: " + muslProbeCmd + "\n")
		ginkgo.By("Running these commands against glibc: " + glibcProbeCmd + "\n")

		// 运行一个探测 DNS 并通过 HTTP 暴露结果的 pod。
		ginkgo.By("creating a pod to probe DNS")
		pod := createDNSPod(f.Namespace.Name, []dnsQuerier{muslProber, glibcProber}, dnsTestPodHostName, dnsTestServiceName)
		validateDNSResults(ctx, f, pod, append(muslFileNames, glibcFileNames...))
	})

	/*
		发布版本：v1.14
		测试名称：DNS, cluster
		描述：创建 Pod 后，该 pod 必须能通过 /etc/hosts 解析集群 dns 条目，如 kubernetes.default。
	*/
	framework.ConformanceIt("should provide /etc/hosts entries for the cluster", func(ctx context.Context) {
		hostFQDN := fmt.Sprintf("%s.%s.%s.svc.%s", dnsTestPodHostName, dnsTestServiceName, f.Namespace.Name, framework.TestContext.ClusterDNSDomain)
		hostEntries := []string{hostFQDN, dnsTestPodHostName}
		// TODO: 双栈下要同时验证 IPv4 和 IPv6 地址族
		muslProbeCmd, muslFileNames := createProbeCommand(nil, hostEntries, "", "musl", f.Namespace.Name, framework.TestContext.ClusterDNSDomain, framework.TestContext.ClusterIsIPv6())
		muslProber := dnsQuerier{name: "musl", image: imageutils.Agnhost, cmd: muslProbeCmd}
		glibcProbeCmd, glibcFileNames := createProbeCommand(nil, hostEntries, "", "glibc", f.Namespace.Name, framework.TestContext.ClusterDNSDomain, framework.TestContext.ClusterIsIPv6())
		glibcProber := dnsQuerier{name: "glibc", image: imageutils.GlibcDnsTesting, cmd: glibcProbeCmd}
		ginkgo.By("Running these commands against musl: " + muslProbeCmd + "\n")
		ginkgo.By("Running these commands against glibc: " + glibcProbeCmd + "\n")

		// 运行一个探测 /etc/hosts 并通过 HTTP 暴露结果的 pod。
		ginkgo.By("creating a pod to probe /etc/hosts")
		pod := createDNSPod(f.Namespace.Name, []dnsQuerier{muslProber, glibcProber}, dnsTestPodHostName, dnsTestServiceName)
		validateDNSResults(ctx, f, pod, append(muslFileNames, glibcFileNames...))
	})

	/*
		发布版本：v1.9
		测试名称：DNS, services
		描述：创建 headless service 后，该 service 必须能解析所有必需的 service endpoint。创建 service 后，同一 namespace 中的任何 pod 都必须能通过所有预期的 DNS 名称解析该 service。
	*/
	framework.ConformanceIt("should provide DNS for services", func(ctx context.Context) {
		// 注意：这里只包含 FQDN 和主机名；部分名称的测试见下面的用例
		// 创建一个测试用 headless service。
		ginkgo.By("Creating a test headless service")
		testServiceSelector := map[string]string{
			"dns-test": "true",
		}
		headlessService := e2eservice.CreateServiceSpec(dnsTestServiceName, "", true, testServiceSelector)
		_, err := f.ClientSet.CoreV1().Services(f.Namespace.Name).Create(ctx, headlessService, metav1.CreateOptions{})
		framework.ExpectNoError(err, "failed to create headless service: %s", dnsTestServiceName)

		regularServiceName := "test-service-2"
		regularService := e2eservice.CreateServiceSpec(regularServiceName, "", false, testServiceSelector)
		regularService, err = f.ClientSet.CoreV1().Services(f.Namespace.Name).Create(ctx, regularService, metav1.CreateOptions{})
		framework.ExpectNoError(err, "failed to create regular service: %s", regularServiceName)

		// 我们需要能够解析的所有名称。
		// TODO: 创建更多 endpoint，验证 headless service
		// 会返回多条 A 记录。
		namesToResolve := []string{
			fmt.Sprintf("%s.%s.svc.%s", headlessService.Name, f.Namespace.Name, framework.TestContext.ClusterDNSDomain),
			fmt.Sprintf("_http._tcp.%s.%s.svc.%s", headlessService.Name, f.Namespace.Name, framework.TestContext.ClusterDNSDomain),
			fmt.Sprintf("_http._tcp.%s.%s.svc.%s", regularService.Name, f.Namespace.Name, framework.TestContext.ClusterDNSDomain),
		}

		// TODO: 双栈下要同时验证 IPv4 和 IPv6 地址族
		muslProbeCmd, muslFileNames := createProbeCommand(namesToResolve, nil, regularService.Spec.ClusterIP, "musl", f.Namespace.Name, framework.TestContext.ClusterDNSDomain, framework.TestContext.ClusterIsIPv6())
		muslProber := dnsQuerier{name: "musl", image: imageutils.Agnhost, cmd: muslProbeCmd}
		glibcProbeCmd, glibcFileNames := createProbeCommand(namesToResolve, nil, regularService.Spec.ClusterIP, "glibc", f.Namespace.Name, framework.TestContext.ClusterDNSDomain, framework.TestContext.ClusterIsIPv6())
		glibcProber := dnsQuerier{name: "glibc", image: imageutils.GlibcDnsTesting, cmd: glibcProbeCmd}
		ginkgo.By("Running these commands against musl: " + muslProbeCmd + "\n")
		ginkgo.By("Running these commands against glibc: " + glibcProbeCmd + "\n")

		// 运行一个探测 DNS 并通过 HTTP 暴露结果的 pod。
		ginkgo.By("creating a pod to probe DNS")
		pod := createDNSPod(f.Namespace.Name, []dnsQuerier{muslProber, glibcProber}, dnsTestPodHostName, dnsTestServiceName)
		pod.ObjectMeta.Labels = testServiceSelector

		validateDNSResults(ctx, f, pod, append(muslFileNames, glibcFileNames...))
	})

	framework.It("should provide DNS for a service with multiple EndpointSlices and no Endpoints", func(ctx context.Context) {
		ginkgo.By("Creating a test headless, selectorless service")
		service := e2eservice.CreateServiceSpec(dnsTestServiceName, "", true, nil)
		_, err := f.ClientSet.CoreV1().Services(f.Namespace.Name).Create(ctx, service, metav1.CreateOptions{})
		framework.ExpectNoError(err, "failed to create headless service: %s", dnsTestServiceName)

		ginkgo.By("Manually creating two EndpointSlices for the service")
		addressType := discoveryv1.AddressTypeIPv4
		ips := []string{"1.2.3.4", "5.6.7.8"}
		if framework.TestContext.ClusterIsIPv6() {
			addressType = discoveryv1.AddressTypeIPv6
			ips = []string{"fc00::1234", "fc00::5678"}
		}
		slice1 := &discoveryv1.EndpointSlice{
			ObjectMeta: metav1.ObjectMeta{
				GenerateName: dnsTestServiceName + "-",
				Labels: map[string]string{
					discoveryv1.LabelServiceName: dnsTestServiceName,
					discoveryv1.LabelManagedBy:   "e2e-test-" + f.Namespace.Name,
				},
			},
			AddressType: addressType,
			Endpoints: []discoveryv1.Endpoint{{
				Addresses:  ips[:1],
				Conditions: discoveryv1.EndpointConditions{Ready: ptr.To(true)},
			}},
			Ports: []discoveryv1.EndpointPort{{
				Name:     ptr.To("http"),
				Port:     ptr.To[int32](80),
				Protocol: ptr.To(v1.ProtocolTCP),
			}},
		}
		_, err = f.ClientSet.DiscoveryV1().EndpointSlices(f.Namespace.Name).Create(ctx, slice1, metav1.CreateOptions{})
		framework.ExpectNoError(err)

		slice2 := &discoveryv1.EndpointSlice{
			ObjectMeta: metav1.ObjectMeta{
				GenerateName: dnsTestServiceName + "-",
				Labels: map[string]string{
					discoveryv1.LabelServiceName: dnsTestServiceName,
					discoveryv1.LabelManagedBy:   "e2e-test-" + f.Namespace.Name,
				},
			},
			AddressType: addressType,
			Endpoints: []discoveryv1.Endpoint{{
				Addresses:  ips[1:],
				Conditions: discoveryv1.EndpointConditions{Ready: ptr.To(true)},
			}},
			Ports: []discoveryv1.EndpointPort{{
				Name:     ptr.To("https"),
				Port:     ptr.To[int32](443),
				Protocol: ptr.To(v1.ProtocolTCP),
			}},
		}
		_, err = f.ClientSet.DiscoveryV1().EndpointSlices(f.Namespace.Name).Create(ctx, slice2, metav1.CreateOptions{})
		framework.ExpectNoError(err)

		namesToResolve := []string{
			fmt.Sprintf("%s.%s.svc.%s", service.Name, f.Namespace.Name, framework.TestContext.ClusterDNSDomain),
			fmt.Sprintf("_http._tcp.%s.%s.svc.%s", service.Name, f.Namespace.Name, framework.TestContext.ClusterDNSDomain),
			fmt.Sprintf("_https._tcp.%s.%s.svc.%s", service.Name, f.Namespace.Name, framework.TestContext.ClusterDNSDomain),
		}

		muslProbeCmd, muslFileNames := createProbeCommand(namesToResolve, nil, "", "musl", f.Namespace.Name, framework.TestContext.ClusterDNSDomain, framework.TestContext.ClusterIsIPv6())
		muslProber := dnsQuerier{name: "musl", image: imageutils.Agnhost, cmd: muslProbeCmd}
		glibcProbeCmd, glibcFileNames := createProbeCommand(namesToResolve, nil, "", "glibc", f.Namespace.Name, framework.TestContext.ClusterDNSDomain, framework.TestContext.ClusterIsIPv6())
		glibcProber := dnsQuerier{name: "glibc", image: imageutils.GlibcDnsTesting, cmd: glibcProbeCmd}
		ginkgo.By("Running these commands against musl: " + muslProbeCmd + "\n")
		ginkgo.By("Running these commands against glibc: " + glibcProbeCmd + "\n")

		// 运行一个探测 DNS 并通过 HTTP 暴露结果的 pod。
		ginkgo.By("creating a pod to probe DNS")
		pod := createDNSPod(f.Namespace.Name, []dnsQuerier{muslProber, glibcProber}, dnsTestPodHostName, dnsTestServiceName)

		validateDNSResults(ctx, f, pod, append(muslFileNames, glibcFileNames...))
	})

	/*
		发布版本：v1.17
		测试名称：DNS, PQDN for services
		描述：创建 headless service 和普通 service。两个 service 都必须能通过提供 A 记录和 SRV 记录，解析其 service endpoint 的部分限定 DNS 名称。
		[LinuxOnly]：Windows 目前不支持解析 PQDN。
	*/
	framework.ConformanceIt("should resolve DNS of partial qualified names for services [LinuxOnly]", func(ctx context.Context) {
		// 创建一个测试用 headless service。
		ginkgo.By("Creating a test headless service")
		testServiceSelector := map[string]string{
			"dns-test": "true",
		}
		headlessService := e2eservice.CreateServiceSpec(dnsTestServiceName, "", true, testServiceSelector)
		_, err := f.ClientSet.CoreV1().Services(f.Namespace.Name).Create(ctx, headlessService, metav1.CreateOptions{})
		framework.ExpectNoError(err, "failed to create headless service: %s", dnsTestServiceName)

		regularServiceName := "test-service-2"
		regularService := e2eservice.CreateServiceSpec(regularServiceName, "", false, testServiceSelector)
		regularService, err = f.ClientSet.CoreV1().Services(f.Namespace.Name).Create(ctx, regularService, metav1.CreateOptions{})
		framework.ExpectNoError(err, "failed to create regular service: %s", regularServiceName)

		// 我们需要能够解析的所有名称。
		// 针对 headless service。
		namesToResolve := []string{
			headlessService.Name,
			fmt.Sprintf("%s.%s", headlessService.Name, f.Namespace.Name),
			fmt.Sprintf("%s.%s.svc", headlessService.Name, f.Namespace.Name),
			fmt.Sprintf("_http._tcp.%s.%s.svc", headlessService.Name, f.Namespace.Name),
			fmt.Sprintf("_http._tcp.%s.%s.svc", regularService.Name, f.Namespace.Name),
		}

		// TODO: 双栈下要同时验证 IPv4 和 IPv6 地址族
		muslProbeCmd, muslFileNames := createProbeCommand(namesToResolve, nil, regularService.Spec.ClusterIP, "musl", f.Namespace.Name, framework.TestContext.ClusterDNSDomain, framework.TestContext.ClusterIsIPv6())
		muslProber := dnsQuerier{name: "musl", image: imageutils.Agnhost, cmd: muslProbeCmd}
		glibcProbeCmd, glibcFileNames := createProbeCommand(namesToResolve, nil, regularService.Spec.ClusterIP, "glibc", f.Namespace.Name, framework.TestContext.ClusterDNSDomain, framework.TestContext.ClusterIsIPv6())
		glibcProber := dnsQuerier{name: "glibc", image: imageutils.GlibcDnsTesting, cmd: glibcProbeCmd}
		ginkgo.By("Running these commands against musl: " + muslProbeCmd + "\n")
		ginkgo.By("Running these commands against glibc: " + glibcProbeCmd + "\n")

		// 运行一个探测 DNS 并通过 HTTP 暴露结果的 pod。
		ginkgo.By("creating a pod to probe DNS")
		pod := createDNSPod(f.Namespace.Name, []dnsQuerier{muslProber, glibcProber}, dnsTestPodHostName, dnsTestServiceName)
		pod.ObjectMeta.Labels = testServiceSelector

		validateDNSResults(ctx, f, pod, append(muslFileNames, glibcFileNames...))
	})

	/*
		发布版本：v1.15
		测试名称：DNS, resolve the hostname
		描述：创建带标签的 headless service。创建带匹配 service 标签的 Pod，其 hostname 和 subdomain 与 service 名称相同。
		Pod 必须能通过在该名称提供 A 记录，解析自己的完全限定域名和 hostname。
	*/
	framework.ConformanceIt("should provide DNS for pods for Hostname", func(ctx context.Context) {
		// 创建一个测试用 headless service。
		ginkgo.By("Creating a test headless service")
		testServiceSelector := map[string]string{
			"dns-test-hostname-attribute": "true",
		}
		serviceName := "dns-test-service-2"
		podHostname := "dns-querier-2"
		headlessService := e2eservice.CreateServiceSpec(serviceName, "", true, testServiceSelector)
		_, err := f.ClientSet.CoreV1().Services(f.Namespace.Name).Create(ctx, headlessService, metav1.CreateOptions{})
		framework.ExpectNoError(err, "failed to create headless service: %s", serviceName)

		hostFQDN := fmt.Sprintf("%s.%s.%s.svc.%s", podHostname, serviceName, f.Namespace.Name, framework.TestContext.ClusterDNSDomain)
		hostNames := []string{hostFQDN, podHostname}
		// TODO: 双栈下要同时验证 IPv4 和 IPv6 地址族
		muslProbeCmd, muslFileNames := createProbeCommand(nil, hostNames, "", "musl", f.Namespace.Name, framework.TestContext.ClusterDNSDomain, framework.TestContext.ClusterIsIPv6())
		muslProber := dnsQuerier{name: "musl", image: imageutils.Agnhost, cmd: muslProbeCmd}
		glibcProbeCmd, glibcFileNames := createProbeCommand(nil, hostNames, "", "glibc", f.Namespace.Name, framework.TestContext.ClusterDNSDomain, framework.TestContext.ClusterIsIPv6())
		glibcProber := dnsQuerier{name: "glibc", image: imageutils.GlibcDnsTesting, cmd: glibcProbeCmd}
		ginkgo.By("Running these commands against musl: " + muslProbeCmd + "\n")
		ginkgo.By("Running these commands against glibc: " + glibcProbeCmd + "\n")

		// 运行一个探测 DNS 并通过 HTTP 暴露结果的 pod。
		ginkgo.By("creating a pod to probe DNS")
		pod1 := createDNSPod(f.Namespace.Name, []dnsQuerier{muslProber, glibcProber}, dnsTestPodHostName, dnsTestServiceName)
		pod1.ObjectMeta.Labels = testServiceSelector
		pod1.Spec.Hostname = podHostname
		pod1.Spec.Subdomain = serviceName

		validateDNSResults(ctx, f, pod1, append(muslFileNames, glibcFileNames...))
	})

	/*
		发布版本：v1.15
		测试名称：DNS, resolve the subdomain
		描述：创建带标签的 headless service。创建带匹配 service 标签的 Pod，其 hostname 和 subdomain 与 service 名称相同。
		Pod 必须能通过在该名称提供 A 记录，解析自己的完全限定域名和 subdomain。
	*/
	framework.ConformanceIt("should provide DNS for pods for Subdomain", func(ctx context.Context) {
		// 创建一个测试用 headless service。
		ginkgo.By("Creating a test headless service")
		testServiceSelector := map[string]string{
			"dns-test-hostname-attribute": "true",
		}
		serviceName := "dns-test-service-2"
		podHostname := "dns-querier-2"
		headlessService := e2eservice.CreateServiceSpec(serviceName, "", true, testServiceSelector)
		_, err := f.ClientSet.CoreV1().Services(f.Namespace.Name).Create(ctx, headlessService, metav1.CreateOptions{})
		framework.ExpectNoError(err, "failed to create headless service: %s", serviceName)

		hostFQDN := fmt.Sprintf("%s.%s.%s.svc.%s", podHostname, serviceName, f.Namespace.Name, framework.TestContext.ClusterDNSDomain)
		subdomain := fmt.Sprintf("%s.%s.svc.%s", serviceName, f.Namespace.Name, framework.TestContext.ClusterDNSDomain)
		namesToResolve := []string{hostFQDN, subdomain}
		// TODO: 双栈下要同时验证 IPv4 和 IPv6 地址族
		muslProbeCmd, muslFileNames := createProbeCommand(namesToResolve, nil, "", "musl", f.Namespace.Name, framework.TestContext.ClusterDNSDomain, framework.TestContext.ClusterIsIPv6())
		muslProber := dnsQuerier{name: "musl", image: imageutils.Agnhost, cmd: muslProbeCmd}
		glibcProbeCmd, glibcFileNames := createProbeCommand(namesToResolve, nil, "", "glibc", f.Namespace.Name, framework.TestContext.ClusterDNSDomain, framework.TestContext.ClusterIsIPv6())
		glibcProber := dnsQuerier{name: "glibc", image: imageutils.GlibcDnsTesting, cmd: glibcProbeCmd}
		ginkgo.By("Running these commands against musl: " + muslProbeCmd + "\n")
		ginkgo.By("Running these commands against glibc: " + glibcProbeCmd + "\n")

		// 运行一个探测 DNS 并通过 HTTP 暴露结果的 pod。
		ginkgo.By("creating a pod to probe DNS")
		pod1 := createDNSPod(f.Namespace.Name, []dnsQuerier{muslProber, glibcProber}, dnsTestPodHostName, dnsTestServiceName)
		pod1.ObjectMeta.Labels = testServiceSelector
		pod1.Spec.Hostname = podHostname
		pod1.Spec.Subdomain = serviceName

		validateDNSResults(ctx, f, pod1, append(muslFileNames, glibcFileNames...))
	})

	/*
		发布版本：v1.15
		测试名称：DNS, for ExternalName Services
		描述：创建带 externalName 的 service。Pod 必须能通过 CNAME 解析该 service 的地址。修改 service 的 externalName 后，Pod 必须解析到新的 DNS 条目。
		把 service 类型从 externalName 改为 ClusterIP，Pod 必须能通过 A 记录解析该 service。
	*/
	framework.ConformanceIt("should provide DNS for ExternalName services", func(ctx context.Context) {
		// 创建一个测试用 ExternalName service。
		ginkgo.By("Creating a test externalName service")
		serviceName := "dns-test-service-3"
		externalNameService := e2eservice.CreateServiceSpec(serviceName, "foo.example.com", false, nil)
		_, err := f.ClientSet.CoreV1().Services(f.Namespace.Name).Create(ctx, externalNameService, metav1.CreateOptions{})
		framework.ExpectNoError(err, "failed to create ExternalName service: %s", serviceName)

		hostFQDN := fmt.Sprintf("%s.%s.svc.%s", serviceName, f.Namespace.Name, framework.TestContext.ClusterDNSDomain)
		muslProbeCmd, muslFileName := createTargetedProbeCommand(hostFQDN, "CNAME", "musl")
		muslProber := dnsQuerier{name: "musl", image: imageutils.Agnhost, cmd: muslProbeCmd}
		glibcProbeCmd, glibcFileName := createTargetedProbeCommand(hostFQDN, "CNAME", "glibc")
		glibcProber := dnsQuerier{name: "glibc", image: imageutils.GlibcDnsTesting, cmd: glibcProbeCmd}
		ginkgo.By("Running these commands against musl: " + muslProbeCmd + "\n")
		ginkgo.By("Running these commands against glibc: " + glibcProbeCmd + "\n")

		// 运行一个探测 DNS 并通过 HTTP 暴露结果的 pod。
		ginkgo.By("creating a pod to probe DNS")
		pod1 := createDNSPod(f.Namespace.Name, []dnsQuerier{muslProber, glibcProber}, dnsTestPodHostName, dnsTestServiceName)

		validateTargetedProbeOutput(ctx, f, pod1, []string{muslFileName, glibcFileName}, "foo.example.com.")

		// 测试修改 externalName 字段
		ginkgo.By("changing the externalName to bar.example.com")
		_, err = e2eservice.UpdateService(ctx, f.ClientSet, f.Namespace.Name, serviceName, func(s *v1.Service) {
			s.Spec.ExternalName = "bar.example.com"
		})
		framework.ExpectNoError(err, "failed to change externalName of service: %s", serviceName)
		muslProbeCmd, muslFileName = createTargetedProbeCommand(hostFQDN, "CNAME", "musl")
		muslProber = dnsQuerier{name: "musl", image: imageutils.Agnhost, cmd: muslProbeCmd}
		glibcProbeCmd, glibcFileName = createTargetedProbeCommand(hostFQDN, "CNAME", "glibc")
		glibcProber = dnsQuerier{name: "glibc", image: imageutils.GlibcDnsTesting, cmd: glibcProbeCmd}
		ginkgo.By("Running these commands against musl: " + muslProbeCmd + "\n")
		ginkgo.By("Running these commands against glibc: " + glibcProbeCmd + "\n")

		// 运行一个探测 DNS 并通过 HTTP 暴露结果的 pod。
		ginkgo.By("creating a second pod to probe DNS")
		pod2 := createDNSPod(f.Namespace.Name, []dnsQuerier{muslProber, glibcProber}, dnsTestPodHostName, dnsTestServiceName)

		validateTargetedProbeOutput(ctx, f, pod2, []string{muslFileName, glibcFileName}, "bar.example.com.")

		// 测试把类型从 ExternalName 改为 ClusterIP
		ginkgo.By("changing the service to type=ClusterIP")
		_, err = e2eservice.UpdateService(ctx, f.ClientSet, f.Namespace.Name, serviceName, func(s *v1.Service) {
			s.Spec.Type = v1.ServiceTypeClusterIP
			s.Spec.Ports = []v1.ServicePort{
				{Port: 80, Name: "http", Protocol: v1.ProtocolTCP},
			}
		})
		framework.ExpectNoError(err, "failed to change service type to ClusterIP for service: %s", serviceName)
		targetRecord := "A"
		if framework.TestContext.ClusterIsIPv6() {
			targetRecord = "AAAA"
		}
		// TODO: 双栈时可以从这里运行两次 createTargetedProbeCommand()
		// 一个查 A 记录，另一个查 AAAA 记录
		muslProbeCmd, muslFileName = createTargetedProbeCommand(hostFQDN, targetRecord, "musl")
		muslProber = dnsQuerier{name: "musl", image: imageutils.Agnhost, cmd: muslProbeCmd}
		glibcProbeCmd, glibcFileName = createTargetedProbeCommand(hostFQDN, targetRecord, "glibc")
		glibcProber = dnsQuerier{name: "glibc", image: imageutils.GlibcDnsTesting, cmd: glibcProbeCmd}
		ginkgo.By("Running these commands against musl: " + muslProbeCmd + "\n")
		ginkgo.By("Running these commands against glibc: " + glibcProbeCmd + "\n")

		// 运行一个探测 DNS 并通过 HTTP 暴露结果的 pod。
		ginkgo.By("creating a third pod to probe DNS")
		pod3 := createDNSPod(f.Namespace.Name, []dnsQuerier{muslProber, glibcProber}, dnsTestPodHostName, dnsTestServiceName)

		svc, err := f.ClientSet.CoreV1().Services(f.Namespace.Name).Get(ctx, externalNameService.Name, metav1.GetOptions{})
		framework.ExpectNoError(err, "failed to get service: %s", externalNameService.Name)

		validateTargetedProbeOutput(ctx, f, pod3, []string{muslFileName, glibcFileName}, svc.Spec.ClusterIP)
	})

	/*
		发布版本：v1.17
		测试名称：DNS, custom dnsConfig
		描述：创建一个 DNSPolicy 为 None 且带自定义 DNS 配置的 Pod，指定 nameservers 和 search path 条目。
		Pod 创建必须成功，且提供的 DNS 配置必须在 Pod 中生效。
	*/
	framework.ConformanceIt("should support configurable pod DNS nameservers", func(ctx context.Context) {
		ginkgo.By("Creating a pod with dnsPolicy=None and customized dnsConfig...")
		testServerIP := "1.1.1.1"
		testSearchPath := "resolv.conf.local"
		testAgnhostPod := e2epod.NewAgnhostPod(f.Namespace.Name, "test-dns-nameservers", nil, nil, nil)
		testAgnhostPod.Spec.DNSPolicy = v1.DNSNone
		testAgnhostPod.Spec.DNSConfig = &v1.PodDNSConfig{
			Nameservers: []string{testServerIP},
			Searches:    []string{testSearchPath},
		}
		testAgnhostPod, err := f.ClientSet.CoreV1().Pods(f.Namespace.Name).Create(ctx, testAgnhostPod, metav1.CreateOptions{})
		framework.ExpectNoError(err, "failed to create pod: %s", testAgnhostPod.Name)
		framework.Logf("Created pod %v", testAgnhostPod)
		err = e2epod.WaitTimeoutForPodReadyInNamespace(ctx, f.ClientSet, testAgnhostPod.Name, f.Namespace.Name, framework.PodStartTimeout)
		framework.ExpectNoError(err, "failed to wait for pod %s to be running", testAgnhostPod.Name)

		runCommand := func(arg string) string {
			cmd := []string{"/agnhost", arg}
			stdout, stderr, err := e2epod.Exec(f.TContext(ctx), e2epod.ExecOptions{
				Command:       cmd,
				Namespace:     f.Namespace.Name,
				PodName:       testAgnhostPod.Name,
				ContainerName: testAgnhostPod.Spec.Containers[0].Name,
				CaptureStdout: true,
				CaptureStderr: true,
			})
			framework.ExpectNoError(err, "failed to run command '/agnhost %s' on pod, stdout: %v, stderr: %v, err: %v", arg, stdout, stderr, err)
			return stdout
		}

		ginkgo.By("Verifying customized DNS suffix list is configured on pod...")
		stdout := runCommand("dns-suffix")
		if !strings.Contains(stdout, testSearchPath) {
			framework.Failf("customized DNS suffix list not found configured in pod, expected to contain: %s, got: %s", testSearchPath, stdout)
		}

		ginkgo.By("Verifying customized DNS server is configured on pod...")
		stdout = runCommand("dns-server-list")
		if !strings.Contains(stdout, testServerIP) {
			framework.Failf("customized DNS server not found in configured in pod, expected to contain: %s, got: %s", testServerIP, stdout)
		}
	})

	ginkgo.It("should support configurable pod resolv.conf", func(ctx context.Context) {
		ginkgo.By("Preparing a test DNS service with injected DNS names...")
		testInjectedIP := "1.1.1.1"
		testDNSNameShort := "notexistname"
		testSearchPath := "resolv.conf.local"
		testDNSNameFull := fmt.Sprintf("%s.%s", testDNSNameShort, testSearchPath)

		corednsConfig := generateCoreDNSConfigmap(f.Namespace.Name, map[string]string{
			testDNSNameFull: testInjectedIP,
		})
		corednsConfig, err := f.ClientSet.CoreV1().ConfigMaps(f.Namespace.Name).Create(ctx, corednsConfig, metav1.CreateOptions{})
		framework.ExpectNoError(err, "unable to create test configMap %s", corednsConfig.Name)

		testServerPod := generateCoreDNSServerPod(corednsConfig)
		testServerPod, err = f.ClientSet.CoreV1().Pods(f.Namespace.Name).Create(ctx, testServerPod, metav1.CreateOptions{})
		framework.ExpectNoError(err, "failed to create pod: %s", testServerPod.Name)
		framework.Logf("Created pod %v", testServerPod)
		err = e2epod.WaitForPodNameRunningInNamespace(ctx, f.ClientSet, testServerPod.Name, f.Namespace.Name)
		framework.ExpectNoError(err, "failed to wait for pod %s to be running", testServerPod.Name)

		// 获取 server pod 的 IP。
		testServerPod, err = f.ClientSet.CoreV1().Pods(f.Namespace.Name).Get(ctx, testServerPod.Name, metav1.GetOptions{})
		framework.ExpectNoError(err, "failed to get pod %v", testServerPod.Name)
		testServerIP := testServerPod.Status.PodIP
		framework.Logf("testServerIP is %s", testServerIP)

		ginkgo.By("Creating a pod with dnsPolicy=None and customized dnsConfig...")
		testUtilsPod := e2epod.NewAgnhostPod(f.Namespace.Name, "e2e-dns-utils", nil, nil, nil)
		testUtilsPod.Spec.DNSPolicy = v1.DNSNone
		testNdotsValue := "2"
		testUtilsPod.Spec.DNSConfig = &v1.PodDNSConfig{
			Nameservers: []string{testServerIP},
			Searches:    []string{testSearchPath},
			Options: []v1.PodDNSConfigOption{
				{
					Name:  "ndots",
					Value: &testNdotsValue,
				},
			},
		}
		testUtilsPod, err = f.ClientSet.CoreV1().Pods(f.Namespace.Name).Create(ctx, testUtilsPod, metav1.CreateOptions{})
		framework.ExpectNoError(err, "failed to create pod: %s", testUtilsPod.Name)
		framework.Logf("Created pod %v", testUtilsPod)
		err = e2epod.WaitForPodNameRunningInNamespace(ctx, f.ClientSet, testUtilsPod.Name, f.Namespace.Name)
		framework.ExpectNoError(err, "failed to wait for pod %s to be running", testUtilsPod.Name)

		ginkgo.By("Verifying customized DNS option is configured on pod...")
		// TODO: 找到比检查实际 resolv.conf 文件更好的方式。
		cmd := []string{"cat", "/etc/resolv.conf"}
		stdout, stderr, err := e2epod.Exec(f.TContext(ctx), e2epod.ExecOptions{
			Command:       cmd,
			Namespace:     f.Namespace.Name,
			PodName:       testUtilsPod.Name,
			ContainerName: testUtilsPod.Spec.Containers[0].Name,
			CaptureStdout: true,
			CaptureStderr: true,
		})
		framework.ExpectNoError(err, "failed to examine resolv,conf file on pod, stdout: %v, stderr: %v, err: %v", stdout, stderr, err)
		if !strings.Contains(stdout, "ndots:2") {
			framework.Failf("customized DNS options not found in resolv.conf, got: %s", stdout)
		}

		ginkgo.By("Verifying customized name server and search path are working...")
		// 对 not-exist-dns-name 执行 dig，看是否返回注入的 DNS 记录。
		// 这同时验证两点：
		// - 自定义 search path 被追加。
		// - DNS 查询被发送到指定服务器。
		cmd = []string{"dig", "+short", "+search", testDNSNameShort}
		digFunc := func() (bool, error) {
			stdout, stderr, err := e2epod.Exec(f.TContext(ctx), e2epod.ExecOptions{
				Command:       cmd,
				Namespace:     f.Namespace.Name,
				PodName:       testUtilsPod.Name,
				ContainerName: testUtilsPod.Spec.Containers[0].Name,
				CaptureStdout: true,
				CaptureStderr: true,
			})
			if err != nil {
				framework.Logf("ginkgo.Failed to execute dig command, stdout:%v, stderr: %v, err: %v", stdout, stderr, err)
				return false, nil
			}
			res := strings.Split(stdout, "\n")
			if len(res) != 1 || res[0] != testInjectedIP {
				framework.Logf("Expect command `%v` to return %s, got: %v", cmd, testInjectedIP, res)
				return false, nil
			}
			return true, nil
		}
		err = wait.PollImmediate(5*time.Second, 3*time.Minute, digFunc)
		framework.ExpectNoError(err, "failed to verify customized name server and search path")

		// TODO: 为其他 DNSPolicy 添加更多测试用例。
	})

	ginkgo.It("should work with the pod containing more than 6 DNS search paths and longer than 256 search list characters", func(ctx context.Context) {
		// 我们需要能够解析的所有名称。
		namesToResolve := []string{
			"kubernetes.default",
			"kubernetes.default.svc",
		}
		hostFQDN := fmt.Sprintf("%s.%s.%s.svc.%s", dnsTestPodHostName, dnsTestServiceName, f.Namespace.Name, framework.TestContext.ClusterDNSDomain)
		hostEntries := []string{hostFQDN, dnsTestPodHostName}
		// TODO: 双栈下要同时验证 IPv4 和 IPv6 地址族
		muslProbeCmd, muslFileNames := createProbeCommand(namesToResolve, hostEntries, "", "musl", f.Namespace.Name, framework.TestContext.ClusterDNSDomain, framework.TestContext.ClusterIsIPv6())
		muslProber := dnsQuerier{name: "musl", image: imageutils.Agnhost, cmd: muslProbeCmd}
		glibcProbeCmd, glibcFileNames := createProbeCommand(namesToResolve, hostEntries, "", "glibc", f.Namespace.Name, framework.TestContext.ClusterDNSDomain, framework.TestContext.ClusterIsIPv6())
		glibcProber := dnsQuerier{name: "glibc", image: imageutils.GlibcDnsTesting, cmd: glibcProbeCmd}
		ginkgo.By("Running these commands against musl: " + muslProbeCmd + "\n")
		ginkgo.By("Running these commands against glibc: " + glibcProbeCmd + "\n")

		ginkgo.By("Creating a pod with expanded DNS configuration to probe DNS")
		testNdotsValue := "5"
		testSearchPaths := []string{
			fmt.Sprintf("%038d.k8s.io", 1),
			fmt.Sprintf("%038d.k8s.io", 2),
			fmt.Sprintf("%038d.k8s.io", 3),
			fmt.Sprintf("%038d.k8s.io", 4),
			fmt.Sprintf("%038d.k8s.io", 5),
			fmt.Sprintf("%038d.k8s.io", 6), // 260 characters
		}
		pod := createDNSPod(f.Namespace.Name, []dnsQuerier{muslProber, glibcProber}, dnsTestPodHostName, dnsTestServiceName)
		pod.Spec.DNSPolicy = v1.DNSClusterFirst
		pod.Spec.DNSConfig = &v1.PodDNSConfig{
			Searches: testSearchPaths,
			Options: []v1.PodDNSConfigOption{
				{
					Name:  "ndots",
					Value: &testNdotsValue,
				},
			},
		}
		validateDNSResults(ctx, f, pod, append(muslFileNames, glibcFileNames...))
	})

	ginkgo.It("should work with a search path containing an underscore and a search path with a single dot", func(ctx context.Context) {
		// 我们需要能够解析的所有名称。
		namesToResolve := []string{
			"kubernetes.default",
			"kubernetes.default.svc",
		}
		hostFQDN := fmt.Sprintf("%s.%s.%s.svc.%s", dnsTestPodHostName, dnsTestServiceName, f.Namespace.Name, framework.TestContext.ClusterDNSDomain)
		hostEntries := []string{hostFQDN, dnsTestPodHostName}
		// TODO: 双栈下要同时验证 IPv4 和 IPv6 地址族
		muslProbeCmd, muslFileNames := createProbeCommand(namesToResolve, hostEntries, "", "musl", f.Namespace.Name, framework.TestContext.ClusterDNSDomain, framework.TestContext.ClusterIsIPv6())
		muslProber := dnsQuerier{name: "musl", image: imageutils.Agnhost, cmd: muslProbeCmd}
		glibcProbeCmd, glibcFileNames := createProbeCommand(namesToResolve, hostEntries, "", "glibc", f.Namespace.Name, framework.TestContext.ClusterDNSDomain, framework.TestContext.ClusterIsIPv6())
		glibcProber := dnsQuerier{name: "glibc", image: imageutils.GlibcDnsTesting, cmd: glibcProbeCmd}
		ginkgo.By("Running these commands against musl: " + muslProbeCmd + "\n")
		ginkgo.By("Running these commands against glibc: " + glibcProbeCmd + "\n")

		ginkgo.By("Creating a pod with expanded DNS configuration to probe DNS")
		testSearchPaths := []string{
			".",
			"_sip._tcp.abc_d.example.com",
		}
		pod := createDNSPod(f.Namespace.Name, []dnsQuerier{muslProber, glibcProber}, dnsTestPodHostName, dnsTestServiceName)
		pod.Spec.DNSPolicy = v1.DNSClusterFirst
		pod.Spec.DNSConfig = &v1.PodDNSConfig{
			Searches: testSearchPaths,
		}
		validateDNSResults(ctx, f, pod, append(muslFileNames, glibcFileNames...))
	})

	framework.It("should work with a service name that starts with a digit", framework.WithFeatureGate(features.RelaxedServiceNameValidation), func(ctx context.Context) {
		svcName := "1kubernetes"
		svc := &v1.Service{
			ObjectMeta: metav1.ObjectMeta{
				Name: svcName,
			},
			Spec: v1.ServiceSpec{
				Ports: []v1.ServicePort{{Port: 443}},
			},
		}
		_, err := f.ClientSet.CoreV1().Services(f.Namespace.Name).Create(ctx, svc, metav1.CreateOptions{})
		framework.ExpectNoError(err, "error creating Service")

		namesToResolve := []string{
			fmt.Sprintf("%s.%s.svc.%s", svcName, f.Namespace.Name, framework.TestContext.ClusterDNSDomain),
		}

		muslProbeCmd, muslFileNames := createProbeCommand(namesToResolve, nil, "", "musl", f.Namespace.Name, framework.TestContext.ClusterDNSDomain, framework.TestContext.ClusterIsIPv6())
		muslProber := dnsQuerier{name: "musl", image: imageutils.Agnhost, cmd: muslProbeCmd}
		glibcProbeCmd, glibcFileNames := createProbeCommand(namesToResolve, nil, "", "glibc", f.Namespace.Name, framework.TestContext.ClusterDNSDomain, framework.TestContext.ClusterIsIPv6())
		glibcProber := dnsQuerier{name: "glibc", image: imageutils.GlibcDnsTesting, cmd: glibcProbeCmd}
		ginkgo.By("Running these commands against musl: " + muslProbeCmd + "\n")
		ginkgo.By("Running these commands against glibc: " + glibcProbeCmd + "\n")

		// 运行一个探测 DNS 并通过 HTTP 暴露结果的 pod。
		ginkgo.By("creating a pod to probe DNS")
		pod := createDNSPod(f.Namespace.Name, []dnsQuerier{muslProber, glibcProber}, dnsTestPodHostName, dnsTestServiceName)
		validateDNSResults(ctx, f, pod, append(muslFileNames, glibcFileNames...))
	})
})

var _ = common.SIGDescribe("DNS HostNetwork", func() {
	f := framework.NewDefaultFramework("hostnetworkdns")
	f.NamespacePodSecurityLevel = admissionapi.LevelPrivileged

	ginkgo.It("should resolve DNS of partial qualified names for services on hostNetwork pods with dnsPolicy: ClusterFirstWithHostNet [LinuxOnly]", func(ctx context.Context) {
		// 创建一个测试用 headless service。
		ginkgo.By("Creating a test headless service")
		testServiceSelector := map[string]string{
			"dns-test": "true",
		}
		headlessService := e2eservice.CreateServiceSpec(dnsTestServiceName, "", true, testServiceSelector)
		_, err := f.ClientSet.CoreV1().Services(f.Namespace.Name).Create(ctx, headlessService, metav1.CreateOptions{})
		framework.ExpectNoError(err, "failed to create headless service: %s", dnsTestServiceName)

		regularServiceName := "test-service-2"
		regularService := e2eservice.CreateServiceSpec(regularServiceName, "", false, testServiceSelector)
		regularService, err = f.ClientSet.CoreV1().Services(f.Namespace.Name).Create(ctx, regularService, metav1.CreateOptions{})
		framework.ExpectNoError(err, "failed to create regular service: %s", regularServiceName)

		// 我们需要能够解析的所有名称。
		// 针对 headless service。
		namesToResolve := []string{
			headlessService.Name,
			fmt.Sprintf("%s.%s", headlessService.Name, f.Namespace.Name),
			fmt.Sprintf("%s.%s.svc", headlessService.Name, f.Namespace.Name),
			fmt.Sprintf("_http._tcp.%s.%s.svc", headlessService.Name, f.Namespace.Name),
			fmt.Sprintf("_http._tcp.%s.%s.svc", regularService.Name, f.Namespace.Name),
		}

		// TODO: 双栈下要同时验证 IPv4 和 IPv6 地址族
		muslProbeCmd, muslFileNames := createProbeCommand(namesToResolve, nil, regularService.Spec.ClusterIP, "musl", f.Namespace.Name, framework.TestContext.ClusterDNSDomain, framework.TestContext.ClusterIsIPv6())
		muslProber := dnsQuerier{name: "musl", image: imageutils.Agnhost, cmd: muslProbeCmd}
		glibcProbeCmd, glibcFileNames := createProbeCommand(namesToResolve, nil, regularService.Spec.ClusterIP, "glibc", f.Namespace.Name, framework.TestContext.ClusterDNSDomain, framework.TestContext.ClusterIsIPv6())
		glibcProber := dnsQuerier{name: "glibc", image: imageutils.GlibcDnsTesting, cmd: glibcProbeCmd}
		ginkgo.By("Running these commands against musl: " + muslProbeCmd + "\n")
		ginkgo.By("Running these commands against glibc: " + glibcProbeCmd + "\n")

		// 运行一个探测 DNS 并通过 HTTP 暴露结果的 pod。
		ginkgo.By("creating a pod to probe DNS")
		pod := createDNSPod(f.Namespace.Name, []dnsQuerier{muslProber, glibcProber}, dnsTestPodHostName, dnsTestServiceName)
		pod.ObjectMeta.Labels = testServiceSelector
		pod.Spec.HostNetwork = true
		pod.Spec.DNSPolicy = v1.DNSClusterFirstWithHostNet
		validateDNSResults(ctx, f, pod, append(muslFileNames, glibcFileNames...))
	})

	// https://issues.k8s.io/67019
	ginkgo.It("spec.Hostname field is not silently ignored and is used for hostname for a Pod", func(ctx context.Context) {
		ginkgo.By("Creating a pod by setting a hostname")

		testAgnhostPod := e2epod.NewAgnhostPod(f.Namespace.Name, "test-dns-hostname", nil, nil, nil)
		testAgnhostPod.Spec.Hostname = dnsTestPodHostName
		testAgnhostPod, err := f.ClientSet.CoreV1().Pods(f.Namespace.Name).Create(ctx, testAgnhostPod, metav1.CreateOptions{})
		framework.ExpectNoError(err, "failed to created pod: %s", testAgnhostPod.Name)

		err = e2epod.WaitTimeoutForPodReadyInNamespace(ctx, f.ClientSet, testAgnhostPod.Name, f.Namespace.Name, framework.PodStartTimeout)
		framework.ExpectNoError(err, "failed to wait for pod %s to be running", testAgnhostPod.Name)

		stdout, err := e2eoutput.RunHostCmd(testAgnhostPod.Namespace, testAgnhostPod.Name, "hostname")
		framework.ExpectNoError(err, "failed to run command hostname: %s", stdout)
		hostname := strings.TrimSpace(stdout)
		if testAgnhostPod.Spec.Hostname != hostname {
			framework.Failf("expected hostname: %s, got: %s", testAgnhostPod.Spec.Hostname, hostname)
		}
	})

	// https://issues.k8s.io/67019
	ginkgo.It("spec.Hostname field is silently ignored and the node hostname is used when hostNetwork is set to true for a Pod", func(ctx context.Context) {
		ginkgo.By("Creating a pod by setting a hostNetwork to true")

		testAgnhostPod := e2epod.NewAgnhostPod(f.Namespace.Name, "test-dns-hostnetwork", nil, nil, nil)
		testAgnhostPod.Spec.Hostname = dnsTestPodHostName
		testAgnhostPod.Spec.HostNetwork = true
		node, err := e2enode.GetRandomReadySchedulableNode(ctx, f.ClientSet)
		framework.ExpectNoError(err)
		nodeSelection := e2epod.NodeSelection{}
		e2epod.SetAffinity(&nodeSelection, node.Name)
		e2epod.SetNodeSelection(&testAgnhostPod.Spec, nodeSelection)

		testAgnhostPod, err = f.ClientSet.CoreV1().Pods(f.Namespace.Name).Create(ctx, testAgnhostPod, metav1.CreateOptions{})
		framework.ExpectNoError(err, "failed to created pod: %s", testAgnhostPod.Name)

		err = e2epod.WaitTimeoutForPodReadyInNamespace(ctx, f.ClientSet, testAgnhostPod.Name, f.Namespace.Name, framework.PodStartTimeout)
		framework.ExpectNoError(err, "failed to wait for pod %s to be running", testAgnhostPod.Name)

		stdout, err := e2eoutput.RunHostCmd(testAgnhostPod.Namespace, testAgnhostPod.Name, "hostname")
		framework.ExpectNoError(err, "failed to run command hostname: %s", stdout)
		hostname := strings.TrimSpace(stdout)
		if dnsTestPodHostName == hostname {
			framework.Failf("https://issues.k8s.io/67019 expected spec.Hostname %s to be ignored", hostname)
		}
	})

})
