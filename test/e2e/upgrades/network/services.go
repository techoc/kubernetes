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

package network

import (
	"context"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/kubernetes/test/e2e/framework"
	e2eservice "k8s.io/kubernetes/test/e2e/framework/service"
	"k8s.io/kubernetes/test/e2e/upgrades"

	"github.com/onsi/ginkgo/v2"
)

// ServiceUpgradeTest 检查 Service 在集群升级前后均可用。
// 仅升级 master 时，还会检查升级过程中 Service 是否保持可用。
type ServiceUpgradeTest struct {
	jig          *e2eservice.TestJig
	tcpService   *v1.Service
	tcpIngressIP string
	svcPort      int
}

// Name 返回该测试的跟踪名称。
func (ServiceUpgradeTest) Name() string { return "service-upgrade" }

// Setup 创建带负载均衡器的 Service，并确认其可访问。
func (t *ServiceUpgradeTest) Setup(ctx context.Context, f *framework.Framework) {
	serviceName := "service-test"
	jig := e2eservice.NewTestJig(f.ClientSet, f.Namespace.Name, serviceName)

	ns := f.Namespace
	cs := f.ClientSet

	ginkgo.By("creating a TCP service " + serviceName + " with type=LoadBalancer in namespace " + ns.Name)
	_, err := jig.CreateTCPService(ctx, func(s *v1.Service) {
		s.Spec.Type = v1.ServiceTypeLoadBalancer
	})
	framework.ExpectNoError(err)
	tcpService, err := jig.WaitForLoadBalancer(ctx, e2eservice.GetServiceLoadBalancerCreationTimeout(ctx, cs))
	framework.ExpectNoError(err)

	// 获取访问所需的信息
	tcpIngressIP := e2eservice.GetIngressPoint(&tcpService.Status.LoadBalancer.Ingress[0])
	svcPort := int(tcpService.Spec.Ports[0].Port)

	ginkgo.By("creating pod to be part of service " + serviceName)
	deployment, err := jig.Run(ctx, jig.AddDeploymentAntiAffinity)
	framework.ExpectNoError(err)

	ginkgo.By("creating a PodDisruptionBudget to cover the Deployment")
	_, err = jig.CreatePDB(ctx, deployment)
	framework.ExpectNoError(err)

	// 先访问一次，确认准备就绪
	ginkgo.By("hitting the pod through the service's LoadBalancer")
	timeout := e2eservice.LoadBalancerLagTimeoutDefault
	if framework.ProviderIs("aws") {
		timeout = e2eservice.LoadBalancerLagTimeoutAWS
	}
	e2eservice.TestReachableHTTP(ctx, tcpIngressIP, svcPort, timeout)

	t.jig = jig
	t.tcpService = tcpService
	t.tcpIngressIP = tcpIngressIP
	t.svcPort = svcPort
}

// Test 对该 Service 执行连通性检查。
func (t *ServiceUpgradeTest) Test(ctx context.Context, f *framework.Framework, done <-chan struct{}, upgrade upgrades.UpgradeType) {
	switch upgrade {
	case upgrades.MasterUpgrade, upgrades.ClusterUpgrade:
		t.test(ctx, f, done, true, true)
	case upgrades.NodeUpgrade:
		t.test(ctx, f, done, true, false)
	default:
		t.test(ctx, f, done, false, false)
	}
}

// Teardown 清理剩余资源。
func (t *ServiceUpgradeTest) Teardown(ctx context.Context, f *framework.Framework) {
	// 依赖删除 namespace 来清理所有资源
}

func (t *ServiceUpgradeTest) test(ctx context.Context, f *framework.Framework, done <-chan struct{}, testDuringDisruption, testFinalizer bool) {
	if testDuringDisruption {
		// 持续验证
		ginkgo.By("continuously hitting the pod through the service's LoadBalancer")
		// TODO (pohly): 增加 context 支持
		wait.Until(func() {
			e2eservice.TestReachableHTTP(ctx, t.tcpIngressIP, t.svcPort, e2eservice.LoadBalancerLagTimeoutDefault)
		}, framework.Poll, done)
	} else {
		// 阻塞等待升级完成
		ginkgo.By("waiting for upgrade to finish without checking if service remains up")
		<-done
	}

	// 再访问一次
	ginkgo.By("hitting the pod through the service's LoadBalancer")
	e2eservice.TestReachableHTTP(ctx, t.tcpIngressIP, t.svcPort, e2eservice.LoadBalancerLagTimeoutDefault)
	if testFinalizer {
		defer func() {
			ginkgo.By("Check that service can be deleted with finalizer")
			e2eservice.WaitForServiceDeletedWithFinalizer(ctx, t.jig.Client, t.tcpService.Namespace, t.tcpService.Name)
		}()
		ginkgo.By("Check that finalizer is present on loadBalancer type service")
		e2eservice.WaitForServiceUpdatedWithFinalizer(ctx, t.jig.Client, t.tcpService.Namespace, t.tcpService.Name, true)
	}
}
