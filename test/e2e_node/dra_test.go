/*
Copyright 2023 The Kubernetes Authors.

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

/*
DRA (Dynamic Resource Allocation) 的 E2E Node 测试
覆盖 DRA 中与 node 相关的部分
可在 Linux 上按如下方式本地运行：
    make test-e2e-node FOCUS='\[Feature:DynamicResourceAllocation\]' SKIP='\[Flaky\]' PARALLELISM=1 \
       TEST_ARGS='--feature-gates="DynamicResourceAllocation=true,ResourceHealthStatus=true,DRAConsumableCapacity=true" --service-feature-gates="DynamicResourceAllocation=true,ResourceHealthStatus=true,DRAConsumableCapacity=true" --runtime-config=api/all=true'
*/

package e2enode

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"path"
	"regexp"
	"sort"
	"strings"
	"sync/atomic"
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	"github.com/onsi/gomega/gstruct"
	"github.com/onsi/gomega/types"

	v1 "k8s.io/api/core/v1"
	resourceapi "k8s.io/api/resource/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	apitypes "k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/uuid"
	"k8s.io/client-go/kubernetes"
	"k8s.io/component-base/metrics/testutil"
	"k8s.io/klog/v2"
	admissionapi "k8s.io/pod-security-admission/api"
	"k8s.io/utils/ptr"

	"k8s.io/kubernetes/pkg/features"
	"k8s.io/kubernetes/test/e2e/feature"
	"k8s.io/kubernetes/test/e2e/framework"
	e2epod "k8s.io/kubernetes/test/e2e/framework/pod"
	e2eskipper "k8s.io/kubernetes/test/e2e/framework/skipper"

	"k8s.io/dynamic-resource-allocation/kubeletplugin"
	"k8s.io/dynamic-resource-allocation/resourceslice"
	drahealthv1 "k8s.io/kubelet/pkg/apis/dra-health/v1"
	drahealthv1alpha1 "k8s.io/kubelet/pkg/apis/dra-health/v1alpha1"
	testdriver "k8s.io/kubernetes/test/e2e/dra/test-driver/app"
	testdrivergomega "k8s.io/kubernetes/test/e2e/dra/test-driver/gomega"
)

const (
	driverName                = "test-driver.cdi.k8s.io"
	kubeletPlugin1Name        = "test-driver1.cdi.k8s.io"
	kubeletPlugin2Name        = "test-driver2.cdi.k8s.io"
	cdiDir                    = "/var/run/cdi"
	pluginRegistrationTimeout = time.Second * 60 // 等待 node plugin 注册的时长
	podInPendingStateTimeout  = time.Second * 60 // 等待 pod 保持 pending 状态的时长

	// kubeletRetryPeriod 是 kubelet 在遇到非致命失败后重试启动容器的间隔。
	// 不包含上一次尝试本身的耗时（?!）。
	//
	// 取值来自 https://github.com/kubernetes/kubernetes/commit/0449cef8fd5217d394c5cd331d852bd50983e6b3.
	kubeletRetryPeriod = 90 * time.Second

	// retryTestTimeout 是一次 pod 启动失败后再成功所需的最长时长。
	//
	// 其他测试也用它作为超时，因为即使测试通常更快完成，它也是合理的上限。
	retryTestTimeout = kubeletRetryPeriod + 30*time.Second
)

// 测试依赖容器运行时的 CDI 支持以及 DRA feature gate。
// "DRA" label 用于在 Ginkgo label filter 中筛选 DRA 相关测试。
var _ = framework.SIGDescribe("node")(framework.WithLabel("DRA"), feature.DynamicResourceAllocation, framework.WithFeatureGate(features.DynamicResourceAllocation), func() {
	f := framework.NewDefaultFramework("dra-node")
	f.NamespacePodSecurityLevel = admissionapi.LevelBaseline

	ginkgo.BeforeEach(func() {
		ginkgo.DeferCleanup(func(ctx context.Context) {
			// 测试结束时 plugin 和 kubelet 被杀掉，会留下 ResourceSlices。
			// 垃圾回收也许最终会清理它们（不确定 node 实例如何管理），但可能要等很久，所以显式清理。
			framework.ExpectNoError(f.ClientSet.ResourceV1().ResourceSlices().DeleteCollection(ctx, metav1.DeleteOptions{}, metav1.ListOptions{}))
		})
	})

	f.Context("Resource Kubelet Plugin", f.WithSerial(), func() {
		ginkgo.It("must register after Kubelet restart", func(ctx context.Context) {
			kubeletPlugin := newKubeletPlugin(ctx, f.ClientSet, f.Namespace.Name, getNodeName(ctx, f), driverName)

			oldCalls := kubeletPlugin.GetGRPCCalls()
			getNewCalls := func() []testdriver.GRPCCall {
				calls := kubeletPlugin.GetGRPCCalls()
				return calls[len(oldCalls):]
			}

			ginkgo.By("restarting Kubelet")
			restartKubelet(ctx, true)

			ginkgo.By("wait for Kubelet plugin re-registration")
			gomega.Eventually(getNewCalls).WithTimeout(pluginRegistrationTimeout).Should(testdrivergomega.BeRegistered)
		})

		ginkgo.It("must register after plugin restart", func(ctx context.Context) {
			kubeletPlugin := newKubeletPlugin(ctx, f.ClientSet, f.Namespace.Name, getNodeName(ctx, f), driverName)

			ginkgo.By("restart Kubelet Plugin")
			kubeletPlugin.Stop()
			kubeletPlugin = newKubeletPlugin(ctx, f.ClientSet, f.Namespace.Name, getNodeName(ctx, f), driverName)

			ginkgo.By("wait for Kubelet plugin re-registration")
			gomega.Eventually(kubeletPlugin.GetGRPCCalls).WithTimeout(pluginRegistrationTimeout).Should(testdrivergomega.BeRegistered)
		})

		// 验证 kubelet plugin manager 在 GetInfo 调用失败时会重试注册，
		// 调用恢复后能注册成功。
		ginkgo.It("must recover and register after registration failure", func(ctx context.Context) {
			kubeletPlugin := newKubeletPlugin(ctx, f.ClientSet, f.Namespace.Name, getNodeName(ctx, f), driverName)

			ginkgo.By("set GetInfo failure mode")
			kubeletPlugin.SetGetInfoError(fmt.Errorf("simulated GetInfo failure"))
			kubeletPlugin.ResetGRPCCalls()

			ginkgo.By("restart Kubelet")
			restartKubelet(ctx, true)

			ginkgo.By("wait for GetInfo call to fail")
			gomega.Eventually(kubeletPlugin.GetGRPCCalls).WithTimeout(retryTestTimeout).Should(testdrivergomega.GetInfoFailed())
			gomega.Expect(kubeletPlugin.GetGRPCCalls()).ShouldNot(testdrivergomega.BeRegistered, "Expect plugin not to be registered due to GetInfo failure")

			ginkgo.By("unset GetInfo failure mode")
			kubeletPlugin.SetGetInfoError(nil)

			ginkgo.By("wait for Kubelet plugin re-registration")
			gomega.Eventually(kubeletPlugin.GetGRPCCalls).WithTimeout(pluginRegistrationTimeout).Should(testdrivergomega.BeRegistered)
		})

		// 验证 kubelet plugin manager 在 NotifyRegistrationStatus 调用失败时会重试注册，
		// 调用恢复后能注册成功。
		ginkgo.It("must recover and register after NotifyRegistrationStatus failure", func(ctx context.Context) {
			kubeletPlugin := newKubeletPlugin(ctx, f.ClientSet, f.Namespace.Name, getNodeName(ctx, f), driverName)

			ginkgo.By("set NotifyRegistrationStatus failure mode")
			kubeletPlugin.SetNotifyRegistrationStatusError(fmt.Errorf("simulated NotifyRegistrationStatus failure"))
			kubeletPlugin.ResetGRPCCalls()

			ginkgo.By("restart Kubelet")
			restartKubelet(ctx, true)

			ginkgo.By("wait for NotifyRegistrationStatus call to fail")
			gomega.Eventually(kubeletPlugin.GetGRPCCalls).WithTimeout(retryTestTimeout).Should(testdrivergomega.NotifyRegistrationStatusFailed())
			gomega.Expect(kubeletPlugin.GetGRPCCalls()).ShouldNot(testdrivergomega.BeRegistered, "Expect plugin not to be registered due to NotifyRegistrationStatus failure")

			ginkgo.By("unset NotifyRegistrationStatus failure mode")
			kubeletPlugin.SetNotifyRegistrationStatusError(nil)

			ginkgo.By("wait for Kubelet plugin re-registration")
			gomega.Eventually(kubeletPlugin.GetGRPCCalls).WithTimeout(pluginRegistrationTimeout).Should(testdrivergomega.BeRegistered)
		})

		ginkgo.It("must process pod created when kubelet is not running", func(ctx context.Context) {
			newKubeletPlugin(ctx, f.ClientSet, f.Namespace.Name, getNodeName(ctx, f), driverName)

			// 停止 Kubelet
			ginkgo.By("stop kubelet")
			restartKubelet := mustStopKubelet(ctx, f)
			pod := createTestObjects(ctx, f.ClientSet, getNodeName(ctx, f), f.Namespace.Name, "draclass", "external-claim", "drapod", true, []string{driverName})
			// Pod 必须处于 pending 状态
			err := e2epod.WaitForPodCondition(ctx, f.ClientSet, f.Namespace.Name, pod.Name, "Pending", framework.PodStartShortTimeout, func(pod *v1.Pod) (bool, error) {
				return pod.Status.Phase == v1.PodPending, nil
			})
			framework.ExpectNoError(err)
			ginkgo.By("restart kubelet")
			restartKubelet(ctx)
			// Pod 应成功启动
			err = e2epod.WaitForPodSuccessInNamespaceTimeout(ctx, f.ClientSet, pod.Name, f.Namespace.Name, framework.PodStartShortTimeout)
			framework.ExpectNoError(err)
		})

		ginkgo.It("must keep pod in pending state if NodePrepareResources times out", func(ctx context.Context) {
			kubeletPlugin := newKubeletPlugin(ctx, f.ClientSet, f.Namespace.Name, getNodeName(ctx, f), driverName)

			unblockNodePrepareResources := kubeletPlugin.BlockNodePrepareResources()
			defer unblockNodePrepareResources()
			pod := createTestObjects(ctx, f.ClientSet, getNodeName(ctx, f), f.Namespace.Name, "draclass", "external-claim", "drapod", true, []string{driverName})

			ginkgo.By("wait for pod to be in Pending state")
			err := e2epod.WaitForPodCondition(ctx, f.ClientSet, f.Namespace.Name, pod.Name, "Pending", framework.PodStartShortTimeout, func(pod *v1.Pod) (bool, error) {
				return pod.Status.Phase == v1.PodPending, nil
			})
			framework.ExpectNoError(err)

			// TODO: 实现后检查 condition 或 event
			// 详见 https://github.com/kubernetes/kubernetes/issues/118468
			ginkgo.By("check that pod is consistently in Pending state")
			gomega.Consistently(ctx, e2epod.Get(f.ClientSet, pod)).WithTimeout(podInPendingStateTimeout).Should(e2epod.BeInPhase(v1.PodPending),
				"Pod should be in Pending state as resource preparation time outed")
		})

		ginkgo.It("must run pod if NodePrepareResources fails and then succeeds", func(ctx context.Context) {
			kubeletPlugin := newKubeletPlugin(ctx, f.ClientSet, f.Namespace.Name, getNodeName(ctx, f), driverName)

			unsetNodePrepareResourcesFailureMode := kubeletPlugin.SetNodePrepareResourcesFailureMode()
			pod := createTestObjects(ctx, f.ClientSet, getNodeName(ctx, f), f.Namespace.Name, "draclass", "external-claim", "drapod", true, []string{driverName})

			ginkgo.By("wait for pod to be in Pending state")
			err := e2epod.WaitForPodCondition(ctx, f.ClientSet, f.Namespace.Name, pod.Name, "Pending", framework.PodStartShortTimeout, func(pod *v1.Pod) (bool, error) {
				return pod.Status.Phase == v1.PodPending, nil
			})
			framework.ExpectNoError(err)

			ginkgo.By("wait for NodePrepareResources call to fail")
			gomega.Eventually(kubeletPlugin.GetGRPCCalls).WithTimeout(retryTestTimeout).Should(testdrivergomega.NodePrepareResourcesFailed)

			unsetNodePrepareResourcesFailureMode()

			ginkgo.By("wait for NodePrepareResources call to succeed")
			gomega.Eventually(kubeletPlugin.GetGRPCCalls).WithTimeout(retryTestTimeout).Should(testdrivergomega.NodePrepareResourcesSucceeded)

			ginkgo.By("wait for pod to succeed")
			err = e2epod.WaitForPodSuccessInNamespace(ctx, f.ClientSet, pod.Name, f.Namespace.Name)
			framework.ExpectNoError(err)
		})

		ginkgo.It("must run pod if NodeUnprepareResources fails and then succeeds", func(ctx context.Context) {
			kubeletPlugin := newKubeletPlugin(ctx, f.ClientSet, f.Namespace.Name, getNodeName(ctx, f), driverName)

			unsetNodeUnprepareResourcesFailureMode := kubeletPlugin.SetNodeUnprepareResourcesFailureMode()
			pod := createTestObjects(ctx, f.ClientSet, getNodeName(ctx, f), f.Namespace.Name, "draclass", "external-claim", "drapod", true, []string{driverName})

			ginkgo.By("wait for NodePrepareResources call to succeed")
			gomega.Eventually(kubeletPlugin.GetGRPCCalls).WithTimeout(retryTestTimeout).Should(testdrivergomega.NodePrepareResourcesSucceeded)

			ginkgo.By("wait for NodeUnprepareResources call to fail")
			gomega.Eventually(kubeletPlugin.GetGRPCCalls).WithTimeout(retryTestTimeout).Should(testdrivergomega.NodeUnprepareResourcesFailed)

			unsetNodeUnprepareResourcesFailureMode()

			ginkgo.By("wait for NodeUnprepareResources call to succeed")
			gomega.Eventually(kubeletPlugin.GetGRPCCalls).WithTimeout(retryTestTimeout).Should(testdrivergomega.NodeUnprepareResourcesSucceeded)

			ginkgo.By("wait for pod to succeed")
			err := e2epod.WaitForPodSuccessInNamespace(ctx, f.ClientSet, pod.Name, f.Namespace.Name)
			framework.ExpectNoError(err)
		})

		ginkgo.It("must retry NodePrepareResources after Kubelet restart", func(ctx context.Context) {
			kubeletPlugin := newKubeletPlugin(ctx, f.ClientSet, f.Namespace.Name, getNodeName(ctx, f), driverName)

			unsetNodePrepareResourcesFailureMode := kubeletPlugin.SetNodePrepareResourcesFailureMode()
			pod := createTestObjects(ctx, f.ClientSet, getNodeName(ctx, f), f.Namespace.Name, "draclass", "external-claim", "drapod", true, []string{driverName})

			ginkgo.By("wait for pod to be in Pending state")
			err := e2epod.WaitForPodCondition(ctx, f.ClientSet, f.Namespace.Name, pod.Name, "Pending", framework.PodStartShortTimeout, func(pod *v1.Pod) (bool, error) {
				return pod.Status.Phase == v1.PodPending, nil
			})
			framework.ExpectNoError(err)

			ginkgo.By("wait for NodePrepareResources call to fail")
			gomega.Eventually(kubeletPlugin.GetGRPCCalls).WithTimeout(retryTestTimeout).Should(testdrivergomega.NodePrepareResourcesFailed)

			ginkgo.By("stop Kubelet")
			restartKubelet := mustStopKubelet(ctx, f)

			unsetNodePrepareResourcesFailureMode()

			ginkgo.By("restart Kubelet")
			restartKubelet(ctx)

			ginkgo.By("wait for NodePrepareResources call to succeed")
			gomega.Eventually(kubeletPlugin.GetGRPCCalls).WithTimeout(retryTestTimeout).Should(testdrivergomega.NodePrepareResourcesSucceeded)

			ginkgo.By("wait for pod to succeed")
			err = e2epod.WaitForPodSuccessInNamespace(ctx, f.ClientSet, pod.Name, f.Namespace.Name)
			framework.ExpectNoError(err)
		})

		ginkgo.It("must retry NodeUnprepareResources after Kubelet restart", func(ctx context.Context) {
			kubeletPlugin := newKubeletPlugin(ctx, f.ClientSet, f.Namespace.Name, getNodeName(ctx, f), driverName)

			unsetNodeUnprepareResourcesFailureMode := kubeletPlugin.SetNodeUnprepareResourcesFailureMode()
			pod := createTestObjects(ctx, f.ClientSet, getNodeName(ctx, f), f.Namespace.Name, "draclass", "external-claim", "drapod", true, []string{driverName})
			ginkgo.By("wait for NodePrepareResources call to succeed")
			gomega.Eventually(kubeletPlugin.GetGRPCCalls).WithTimeout(retryTestTimeout).Should(testdrivergomega.NodePrepareResourcesSucceeded)

			ginkgo.By("wait for NodeUnprepareResources call to fail")
			gomega.Eventually(kubeletPlugin.GetGRPCCalls).WithTimeout(retryTestTimeout).Should(testdrivergomega.NodeUnprepareResourcesFailed)

			ginkgo.By("stop Kubelet")
			restartKubelet := mustStopKubelet(ctx, f)

			unsetNodeUnprepareResourcesFailureMode()

			ginkgo.By("restart Kubelet")
			restartKubelet(ctx)

			ginkgo.By("wait for NodeUnprepareResources call to succeed")
			gomega.Eventually(kubeletPlugin.GetGRPCCalls).WithTimeout(retryTestTimeout).Should(testdrivergomega.NodeUnprepareResourcesSucceeded)

			ginkgo.By("wait for pod to succeed")
			err := e2epod.WaitForPodSuccessInNamespace(ctx, f.ClientSet, pod.Name, f.Namespace.Name)
			framework.ExpectNoError(err)
		})

		ginkgo.It("must call NodeUnprepareResources for deleted pod", func(ctx context.Context) {
			kubeletPlugin := newKubeletPlugin(ctx, f.ClientSet, f.Namespace.Name, getNodeName(ctx, f), driverName)

			unsetNodeUnprepareResourcesFailureMode := kubeletPlugin.SetNodeUnprepareResourcesFailureMode()
			pod := createTestObjects(ctx, f.ClientSet, getNodeName(ctx, f), f.Namespace.Name, "draclass", "external-claim", "drapod", false, []string{driverName})

			ginkgo.By("wait for NodePrepareResources call to succeed")
			gomega.Eventually(kubeletPlugin.GetGRPCCalls).WithTimeout(retryTestTimeout).Should(testdrivergomega.NodePrepareResourcesSucceeded)

			ginkgo.By("wait for NodeUnprepareResources call to fail")
			gomega.Eventually(kubeletPlugin.GetGRPCCalls).WithTimeout(retryTestTimeout).Should(testdrivergomega.NodeUnprepareResourcesFailed)

			ginkgo.By("delete pod")
			e2epod.DeletePodOrFail(ctx, f.ClientSet, f.Namespace.Name, pod.Name)

			ginkgo.By("wait for NodeUnprepareResources call to fail")
			gomega.Eventually(kubeletPlugin.GetGRPCCalls).WithTimeout(retryTestTimeout).Should(testdrivergomega.NodeUnprepareResourcesFailed)

			unsetNodeUnprepareResourcesFailureMode()

			ginkgo.By("wait for NodeUnprepareResources call to succeed")
			gomega.Eventually(kubeletPlugin.GetGRPCCalls).WithTimeout(retryTestTimeout).Should(testdrivergomega.NodeUnprepareResourcesSucceeded)
		})

		ginkgo.It("must call NodeUnprepareResources for deleted pod after Kubelet restart", func(ctx context.Context) {
			kubeletPlugin := newKubeletPlugin(ctx, f.ClientSet, f.Namespace.Name, getNodeName(ctx, f), driverName)

			unsetNodeUnprepareResourcesFailureMode := kubeletPlugin.SetNodeUnprepareResourcesFailureMode()
			pod := createTestObjects(ctx, f.ClientSet, getNodeName(ctx, f), f.Namespace.Name, "draclass", "external-claim", "drapod", false, []string{driverName})

			ginkgo.By("wait for NodePrepareResources call to succeed")
			gomega.Eventually(kubeletPlugin.GetGRPCCalls).WithTimeout(retryTestTimeout).Should(testdrivergomega.NodePrepareResourcesSucceeded)

			ginkgo.By("wait for NodeUnprepareResources call to fail")
			gomega.Eventually(kubeletPlugin.GetGRPCCalls).WithTimeout(retryTestTimeout).Should(testdrivergomega.NodeUnprepareResourcesFailed)

			ginkgo.By("delete pod")
			err := e2epod.DeletePodWithGracePeriod(ctx, f.ClientSet, pod, 0)
			framework.ExpectNoError(err)

			ginkgo.By("wait for NodeUnprepareResources call to fail")
			gomega.Eventually(kubeletPlugin.GetGRPCCalls).WithTimeout(retryTestTimeout).Should(testdrivergomega.NodeUnprepareResourcesFailed)

			ginkgo.By("restart Kubelet")
			restartKubelet(ctx, true)

			ginkgo.By("wait for NodeUnprepareResources call to fail")
			gomega.Eventually(kubeletPlugin.GetGRPCCalls).WithTimeout(retryTestTimeout).Should(testdrivergomega.NodeUnprepareResourcesFailed)

			unsetNodeUnprepareResourcesFailureMode()

			ginkgo.By("wait for NodeUnprepareResources call to succeed")
			gomega.Eventually(kubeletPlugin.GetGRPCCalls).WithTimeout(retryTestTimeout).Should(testdrivergomega.NodeUnprepareResourcesSucceeded)
		})

		ginkgo.It("must not call NodePrepareResources for deleted pod after Kubelet restart", func(ctx context.Context) {
			kubeletPlugin := newKubeletPlugin(ctx, f.ClientSet, f.Namespace.Name, getNodeName(ctx, f), driverName)

			unblockNodePrepareResources := kubeletPlugin.BlockNodePrepareResources()
			pod := createTestObjects(ctx, f.ClientSet, getNodeName(ctx, f), f.Namespace.Name, "draclass", "external-claim", "drapod", false, []string{driverName})

			ginkgo.By("wait for pod to be in Pending state")
			err := e2epod.WaitForPodCondition(ctx, f.ClientSet, f.Namespace.Name, pod.Name, "Pending", framework.PodStartShortTimeout, func(pod *v1.Pod) (bool, error) {
				return pod.Status.Phase == v1.PodPending, nil
			})
			framework.ExpectNoError(err)

			ginkgo.By("stop Kubelet")
			restartKubelet := mustStopKubelet(ctx, f)

			ginkgo.By("delete pod")
			e2epod.DeletePodOrFail(ctx, f.ClientSet, f.Namespace.Name, pod.Name)

			unblockNodePrepareResources()

			ginkgo.By("restart Kubelet")
			restartKubelet(ctx)

			calls := kubeletPlugin.CountCalls("/NodePrepareResources")
			ginkgo.By("make sure NodePrepareResources is not called again")
			gomega.Consistently(func() int {
				return kubeletPlugin.CountCalls("/NodePrepareResources")
			}).WithTimeout(retryTestTimeout).Should(gomega.Equal(calls))
		})

		functionalListenAfterRegistration := func(ctx context.Context, datadir string, opts ...any) {
			nodeName := getNodeName(ctx, f)

			ginkgo.By("start DRA registrar")
			registrar := newRegistrar(ctx, f.ClientSet, nodeName, driverName, opts...)

			ginkgo.By("wait for registration to complete")
			gomega.Eventually(registrar.GetGRPCCalls).WithTimeout(pluginRegistrationTimeout).Should(testdrivergomega.BeRegistered)

			ginkgo.By("start DRA plugin service")
			draService := newDRAService(ctx, f.ClientSet, f.Namespace.Name, nodeName, driverName, datadir, opts...)

			pod := createTestObjects(ctx, f.ClientSet, nodeName, f.Namespace.Name, "draclass", "external-claim", "drapod", false, []string{driverName})

			ginkgo.By("wait for NodePrepareResources call to succeed")
			gomega.Eventually(draService.GetGRPCCalls).WithTimeout(retryTestTimeout).Should(testdrivergomega.NodePrepareResourcesSucceeded)

			ginkgo.By("wait for pod to succeed")
			err := e2epod.WaitForPodSuccessInNamespace(ctx, f.ClientSet, pod.Name, f.Namespace.Name)
			framework.ExpectNoError(err)
		}
		ginkgo.DescribeTable("must be functional when plugin starts to listen on a service socket after registration",
			functionalListenAfterRegistration,
			ginkgo.Entry("2 sockets", ""),
			ginkgo.Entry(
				"1 common socket",
				kubeletplugin.KubeletRegistryDir,
				kubeletplugin.PluginDataDirectoryPath(kubeletplugin.KubeletRegistryDir),
				kubeletplugin.PluginSocket(driverName+"-common.sock"),
			),
		)

		functionalAfterServiceReconnect := func(ctx context.Context, datadir string, opts ...any) {
			nodeName := getNodeName(ctx, f)

			ginkgo.By("start DRA registrar")
			registrar := newRegistrar(ctx, f.ClientSet, nodeName, driverName, opts...)

			ginkgo.By("wait for registration to complete")
			gomega.Eventually(registrar.GetGRPCCalls).WithTimeout(pluginRegistrationTimeout).Should(testdrivergomega.BeRegistered)

			ginkgo.By("start DRA plugin service")
			draService := newDRAService(ctx, f.ClientSet, f.Namespace.Name, nodeName, driverName, datadir, opts...)

			pod := createTestObjects(ctx, f.ClientSet, getNodeName(ctx, f), f.Namespace.Name, "draclass", "external-claim", "drasleeppod" /* 启用休眠 */, false /* 下面会删除 pod */, []string{driverName})

			ginkgo.By("wait for NodePrepareResources call to succeed")
			gomega.Eventually(draService.GetGRPCCalls).WithTimeout(retryTestTimeout).Should(testdrivergomega.NodePrepareResourcesSucceeded)

			ginkgo.By("stop plugin")
			draService.Stop()

			ginkgo.By("waiting for pod to run")
			err := e2epod.WaitForPodRunningInNamespace(ctx, f.ClientSet, pod)
			framework.ExpectNoError(err)

			ginkgo.By("wait for ResourceSlice removal, indicating detection of disconnect")
			gomega.Eventually(ctx, listResources(f.ClientSet)).Should(gomega.BeEmpty(), "ResourceSlices without plugin")

			ginkgo.By("restarting plugin")
			draService = newDRAService(ctx, f.ClientSet, f.Namespace.Name, nodeName, driverName, datadir, opts...)

			ginkgo.By("stopping pod and waiting for it to get removed, indicating that NodeUnprepareResources must have been called")
			err = f.ClientSet.CoreV1().Pods(pod.Namespace).Delete(ctx, pod.Name, metav1.DeleteOptions{})
			framework.ExpectNoError(err, "delete pod")
			gomega.Eventually(draService.GetGRPCCalls).WithTimeout(retryTestTimeout).Should(testdrivergomega.NodeUnprepareResourcesSucceeded)
			err = e2epod.WaitForPodNotFoundInNamespace(ctx, f.ClientSet, pod.Name, pod.Namespace, f.Timeouts.PodDelete)
			framework.ExpectNoError(err, "pod + claim shutdown")
		}
		ginkgo.DescribeTable("must be functional after service reconnect",
			functionalAfterServiceReconnect,
			ginkgo.Entry("2 sockets", ""),
			ginkgo.Entry(
				"1 common socket",
				kubeletplugin.KubeletRegistryDir,
				kubeletplugin.PluginDataDirectoryPath(kubeletplugin.KubeletRegistryDir),
				kubeletplugin.PluginSocket(driverName+"-common.sock"),
			),
		)

		failOnClosedListener := func(
			ctx context.Context,
			service func(ctx context.Context, clientSet kubernetes.Interface, testNamespace, nodeName, driverName, datadir string, opts ...any) *testdriver.ExamplePlugin,
			listenerOptionFun func(listen func(ctx context.Context, path string) (net.Listener, error)) kubeletplugin.Option,
		) {
			ginkgo.By("create a custom listener")
			var listener net.Listener
			errorMsg := "simulated listener failure"
			getListener := func(ctx context.Context, socketPath string) (net.Listener, error) {
				listener = newErrorOnCloseListener(errors.New(errorMsg))
				return listener, nil
			}

			ginkgo.By("create a context with a cancel function")
			tCtx, cancel := context.WithCancelCause(ctx)
			defer cancel(nil)

			ginkgo.By("start service")
			service(
				ctx,
				f.ClientSet,
				f.Namespace.Name,
				getNodeName(ctx, f),
				driverName,
				"",
				listenerOptionFun(getListener),
				testdriver.CancelMainContext(cancel),
			)

			ginkgo.By("close listener to make the grpc.Server.Serve() fail")
			framework.ExpectNoError(listener.Close())

			ginkgo.By("check that the context is canceled with an expected error and cause")
			gomega.Eventually(tCtx.Err).Should(gomega.MatchError(gomega.ContainSubstring("context canceled")), "Context should be canceled by the error handler")
			gomega.Expect(context.Cause(tCtx).Error()).To(gomega.ContainSubstring(errorMsg), "Context should be canceled with the expected cause")
		}
		// wrappedNewRegistrar 用与 newDRAService 相同的签名创建 registrar，
		// 以便两者都能放进 DescribeTable。
		wrappedNewRegistrar := func(ctx context.Context, clientSet kubernetes.Interface, testNamespace, nodeName, driverName, datadir string, opts ...any) *testdriver.ExamplePlugin {
			return newRegistrar(ctx, clientSet, nodeName, driverName, opts...)
		}
		ginkgo.DescribeTable("must report gRPC serving error",
			failOnClosedListener,
			ginkgo.Entry("for registrar", wrappedNewRegistrar, kubeletplugin.RegistrarListener),
			ginkgo.Entry("for DRA service", newDRAService, kubeletplugin.PluginListener),
		)

		// 验证 Kubelet 在整个 plugin 生命周期内与 DRA plugin 只建立一条 gRPC 连接。
		//
		// 方法是：
		// - 使用自定义 gRPC listener 统计接受的连接数。
		// - 依次创建 pod，触发 NodePrepareResources 和 NodeUnprepareResources。
		// - 断言 listener 只接受了一条连接 —— 因为每次 net.Listener.Accept() 调用
		// 都对应一个新传入连接 —— 从而确保所有调用复用了同一条 gRPC 连接。
		ginkgo.It("must use one gRPC connection for all service calls", func(ctx context.Context) {
			var listener *countingListener
			var err error
			getListener := func(_ context.Context, socketPath string) (net.Listener, error) {
				listener, err = newCountingListener(socketPath)
				return listener, err
			}

			nodeName := getNodeName(ctx, f)

			ginkgo.By("start DRA registrar")
			registrar := newRegistrar(ctx, f.ClientSet, nodeName, driverName)

			ginkgo.By("wait for registration to complete")
			gomega.Eventually(registrar.GetGRPCCalls).WithTimeout(pluginRegistrationTimeout).Should(testdrivergomega.BeRegistered)

			ginkgo.By("start DRA plugin service")
			draService := newDRAService(ctx, f.ClientSet, f.Namespace.Name, nodeName, driverName, "", kubeletplugin.PluginListener(getListener))

			for _, suffix := range []string{"-1", "-2"} {
				draService.ResetGRPCCalls()

				ginkgo.By("create test objects " + suffix)
				pod := createTestObjects(ctx, f.ClientSet, nodeName, f.Namespace.Name, "draclass"+suffix, "external-claim"+suffix, "drapod"+suffix, true, []string{driverName})

				ginkgo.By("wait for NodePrepareResources call to succeed " + suffix)
				gomega.Eventually(draService.GetGRPCCalls).WithTimeout(retryTestTimeout).Should(testdrivergomega.NodePrepareResourcesSucceeded)

				ginkgo.By("wait for NodeUnprepareResources call to succeed " + suffix)
				gomega.Eventually(draService.GetGRPCCalls).WithTimeout(retryTestTimeout).Should(testdrivergomega.NodeUnprepareResourcesSucceeded)

				ginkgo.By("wait for pod to succeed " + suffix)
				err = e2epod.WaitForPodSuccessInNamespace(ctx, f.ClientSet, pod.Name, f.Namespace.Name)
				framework.ExpectNoError(err)

				ginkgo.By("check that listener.Accept was called only once " + suffix)
				gomega.Expect(listener.acceptCount()).To(gomega.Equal(1))
			}
		})
	})

	f.Context("Two resource Kubelet Plugins", f.WithSerial(), func() {
		// start 创建 plugin，context 取消时它们会被停止。
		start := func(ctx context.Context) (*testdriver.ExamplePlugin, *testdriver.ExamplePlugin) {
			kubeletPlugin1 := newKubeletPlugin(ctx, f.ClientSet, f.Namespace.Name, getNodeName(ctx, f), kubeletPlugin1Name)
			kubeletPlugin2 := newKubeletPlugin(ctx, f.ClientSet, f.Namespace.Name, getNodeName(ctx, f), kubeletPlugin2Name)

			ginkgo.By("wait for Kubelet plugin registration")
			gomega.Eventually(kubeletPlugin1.GetGRPCCalls()).WithTimeout(pluginRegistrationTimeout).Should(testdrivergomega.BeRegistered)
			gomega.Eventually(kubeletPlugin2.GetGRPCCalls()).WithTimeout(pluginRegistrationTimeout).Should(testdrivergomega.BeRegistered)

			return kubeletPlugin1, kubeletPlugin2
		}

		ginkgo.It("must prepare and unprepare resources", func(ctx context.Context) {
			kubeletPlugin1, kubeletPlugin2 := start(ctx)

			pod := createTestObjects(ctx, f.ClientSet, getNodeName(ctx, f), f.Namespace.Name, "draclass", "external-claim", "drapod", true, []string{kubeletPlugin1Name, kubeletPlugin2Name})

			ginkgo.By("wait for pod to succeed")
			err := e2epod.WaitForPodSuccessInNamespace(ctx, f.ClientSet, pod.Name, f.Namespace.Name)
			framework.ExpectNoError(err)

			ginkgo.By("wait for NodePrepareResources calls to succeed")
			gomega.Eventually(kubeletPlugin1.GetGRPCCalls).WithTimeout(retryTestTimeout).Should(testdrivergomega.NodePrepareResourcesSucceeded)
			gomega.Eventually(kubeletPlugin2.GetGRPCCalls).WithTimeout(retryTestTimeout).Should(testdrivergomega.NodePrepareResourcesSucceeded)

			ginkgo.By("wait for NodeUnprepareResources calls to succeed")
			gomega.Eventually(kubeletPlugin1.GetGRPCCalls).WithTimeout(retryTestTimeout).Should(testdrivergomega.NodeUnprepareResourcesSucceeded)
			gomega.Eventually(kubeletPlugin2.GetGRPCCalls).WithTimeout(retryTestTimeout).Should(testdrivergomega.NodeUnprepareResourcesSucceeded)
		})

		ginkgo.It("must provide metrics", func(ctx context.Context) {
			kubeletPlugin1, kubeletPlugin2 := start(ctx)

			pod := createTestObjects(ctx, f.ClientSet, getNodeName(ctx, f), f.Namespace.Name, "draclass", "external-claim", "drasleeppod", false, []string{kubeletPlugin1Name, kubeletPlugin2Name})

			ginkgo.By("wait for pod to succeed")
			err := e2epod.WaitForPodRunningInNamespace(ctx, f.ClientSet, pod)
			framework.ExpectNoError(err)
			gomega.Expect(kubeletPlugin1.GetGRPCCalls()).Should(testdrivergomega.NodePrepareResourcesSucceeded, "Plugin 1 should have prepared resources.")
			gomega.Expect(kubeletPlugin2.GetGRPCCalls()).Should(testdrivergomega.NodePrepareResourcesSucceeded, "Plugin 2 should have prepared resources.")
			gomega.Expect(getKubeletMetrics(ctx)).Should(gstruct.MatchKeys(gstruct.IgnoreExtras, gstruct.Keys{
				"dra_resource_claims_in_use": gstruct.MatchAllElements(driverNameLabelKey, gstruct.Elements{
					"<any>":            timelessSample(1),
					kubeletPlugin1Name: timelessSample(1),
					kubeletPlugin2Name: timelessSample(1),
				}),
			}), "metrics while pod is running")

			ginkgo.By("delete pod")
			err = f.ClientSet.CoreV1().Pods(pod.Namespace).Delete(ctx, pod.Name, metav1.DeleteOptions{})
			framework.ExpectNoError(err)
			err = e2epod.WaitForPodNotFoundInNamespace(ctx, f.ClientSet, pod.Name, pod.Namespace, f.Timeouts.PodDelete)
			framework.ExpectNoError(err)
			gomega.Expect(kubeletPlugin1.GetGRPCCalls()).Should(testdrivergomega.NodeUnprepareResourcesSucceeded, "Plugin 2 should have unprepared resources.")
			gomega.Expect(kubeletPlugin2.GetGRPCCalls()).Should(testdrivergomega.NodeUnprepareResourcesSucceeded, "Plugin 2 should have unprepared resources.")
			gomega.Expect(getKubeletMetrics(ctx)).Should(gstruct.MatchKeys(gstruct.IgnoreExtras, gstruct.Keys{
				"dra_resource_claims_in_use": gstruct.MatchAllElements(driverNameLabelKey, gstruct.Elements{
					"<any>": timelessSample(0),
				}),
			}), "metrics after deleting pod")
		})

		ginkgo.It("must run pod if NodePrepareResources fails for one plugin and then succeeds", func(ctx context.Context) {
			_, kubeletPlugin2 := start(ctx)

			unsetNodePrepareResourcesFailureMode := kubeletPlugin2.SetNodePrepareResourcesFailureMode()
			pod := createTestObjects(ctx, f.ClientSet, getNodeName(ctx, f), f.Namespace.Name, "draclass", "external-claim", "drapod", true, []string{kubeletPlugin1Name, kubeletPlugin2Name})

			ginkgo.By("wait for pod to be in Pending state")
			err := e2epod.WaitForPodCondition(ctx, f.ClientSet, f.Namespace.Name, pod.Name, "Pending", framework.PodStartShortTimeout, func(pod *v1.Pod) (bool, error) {
				return pod.Status.Phase == v1.PodPending, nil
			})
			framework.ExpectNoError(err)

			ginkgo.By("wait for plugin2 NodePrepareResources call to fail")
			gomega.Eventually(kubeletPlugin2.GetGRPCCalls).WithTimeout(retryTestTimeout).Should(testdrivergomega.NodePrepareResourcesFailed)

			unsetNodePrepareResourcesFailureMode()

			ginkgo.By("wait for plugin2 NodePrepareResources call to succeed")
			gomega.Eventually(kubeletPlugin2.GetGRPCCalls).WithTimeout(retryTestTimeout).Should(testdrivergomega.NodePrepareResourcesSucceeded)

			ginkgo.By("wait for pod to succeed")
			err = e2epod.WaitForPodSuccessInNamespace(ctx, f.ClientSet, pod.Name, f.Namespace.Name)
			framework.ExpectNoError(err)
		})

		ginkgo.It("must run pod if NodeUnprepareResources fails for one plugin and then succeeds", func(ctx context.Context) {
			kubeletPlugin1, kubeletPlugin2 := start(ctx)

			unsetNodeUnprepareResourcesFailureMode := kubeletPlugin2.SetNodeUnprepareResourcesFailureMode()
			pod := createTestObjects(ctx, f.ClientSet, getNodeName(ctx, f), f.Namespace.Name, "draclass", "external-claim", "drapod", true, []string{kubeletPlugin1Name, kubeletPlugin2Name})

			ginkgo.By("wait for plugin1 NodePrepareResources call to succeed")
			gomega.Eventually(kubeletPlugin1.GetGRPCCalls).WithTimeout(retryTestTimeout).Should(testdrivergomega.NodePrepareResourcesSucceeded)

			ginkgo.By("wait for plugin2 NodePrepareResources call to succeed")
			gomega.Eventually(kubeletPlugin2.GetGRPCCalls).WithTimeout(retryTestTimeout).Should(testdrivergomega.NodePrepareResourcesSucceeded)

			ginkgo.By("wait for plugin2 NodeUnprepareResources call to fail")
			gomega.Eventually(kubeletPlugin2.GetGRPCCalls).WithTimeout(retryTestTimeout).Should(testdrivergomega.NodeUnprepareResourcesFailed)

			unsetNodeUnprepareResourcesFailureMode()

			ginkgo.By("wait for plugin2 NodeUnprepareResources call to succeed")
			gomega.Eventually(kubeletPlugin2.GetGRPCCalls).WithTimeout(retryTestTimeout).Should(testdrivergomega.NodeUnprepareResourcesSucceeded)

			ginkgo.By("wait for pod to succeed")
			err := e2epod.WaitForPodSuccessInNamespace(ctx, f.ClientSet, pod.Name, f.Namespace.Name)
			framework.ExpectNoError(err)
		})

		ginkgo.It("must run pod if NodePrepareResources is in progress for one plugin when Kubelet restarts", func(ctx context.Context) {
			kubeletPlugin1, kubeletPlugin2 := start(ctx)

			unblockNodePrepareResources := kubeletPlugin1.BlockNodePrepareResources()
			pod := createTestObjects(ctx, f.ClientSet, getNodeName(ctx, f), f.Namespace.Name, "draclass", "external-claim", "drapod", true, []string{kubeletPlugin1Name, kubeletPlugin2Name})

			ginkgo.By("wait for pod to be in Pending state")
			err := e2epod.WaitForPodCondition(ctx, f.ClientSet, f.Namespace.Name, pod.Name, "Pending", framework.PodStartShortTimeout, func(pod *v1.Pod) (bool, error) {
				return pod.Status.Phase == v1.PodPending, nil
			})
			framework.ExpectNoError(err)

			ginkgo.By("restart Kubelet")
			restartKubelet(ctx, true)

			unblockNodePrepareResources()

			ginkgo.By("wait for plugin2 NodePrepareResources call to succeed")
			gomega.Eventually(kubeletPlugin2.GetGRPCCalls).WithTimeout(retryTestTimeout).Should(testdrivergomega.NodePrepareResourcesSucceeded)

			ginkgo.By("wait for pod to succeed")
			err = e2epod.WaitForPodSuccessInNamespace(ctx, f.ClientSet, pod.Name, f.Namespace.Name)
			framework.ExpectNoError(err)
		})

		ginkgo.It("must call NodeUnprepareResources again if it's in progress for one plugin when Kubelet restarts", func(ctx context.Context) {
			kubeletPlugin1, kubeletPlugin2 := start(ctx)

			unblockNodeUnprepareResources := kubeletPlugin2.BlockNodeUnprepareResources()
			pod := createTestObjects(ctx, f.ClientSet, getNodeName(ctx, f), f.Namespace.Name, "draclass", "external-claim", "drapod", true, []string{kubeletPlugin1Name, kubeletPlugin2Name})

			ginkgo.By("wait for plugin1 NodePrepareResources call to succeed")
			gomega.Eventually(kubeletPlugin1.GetGRPCCalls).WithTimeout(retryTestTimeout).Should(testdrivergomega.NodePrepareResourcesSucceeded)

			ginkgo.By("wait for plugin2 NodePrepareResources call to succeed")
			gomega.Eventually(kubeletPlugin2.GetGRPCCalls).WithTimeout(retryTestTimeout).Should(testdrivergomega.NodePrepareResourcesSucceeded)

			ginkgo.By("restart Kubelet")
			restartKubelet(ctx, true)

			unblockNodeUnprepareResources()

			ginkgo.By("wait for plugin2 NodeUnprepareResources call to succeed")
			gomega.Eventually(kubeletPlugin2.GetGRPCCalls).WithTimeout(retryTestTimeout).Should(testdrivergomega.NodeUnprepareResourcesSucceeded)

			ginkgo.By("wait for pod to succeed")
			err := e2epod.WaitForPodSuccessInNamespace(ctx, f.ClientSet, pod.Name, f.Namespace.Name)
			framework.ExpectNoError(err)
		})
	})

	f.Context("ResourceSlice", f.WithSerial(), func() {
		f.It("must be removed on kubelet startup", f.WithDisruptive(), func(ctx context.Context) {
			ginkgo.By("stop kubelet")
			restartKubelet := mustStopKubelet(ctx, f)
			ginkgo.DeferCleanup(func() {
				if restartKubelet != nil {
					restartKubelet(ctx)
				}
			})

			ginkgo.By("create some ResourceSlices")
			nodeName := getNodeName(ctx, f)
			otherNodeName := nodeName + "-other"
			createTestResourceSlice(ctx, f.ClientSet, nodeName, driverName)
			createTestResourceSlice(ctx, f.ClientSet, nodeName+"-other", driverName)

			matchAll := gomega.ConsistOf(matchResourcesByNodeName(nodeName), matchResourcesByNodeName(otherNodeName))
			matchOtherNode := gomega.ConsistOf(matchResourcesByNodeName(otherNodeName))

			gomega.Consistently(ctx, listResources(f.ClientSet)).WithTimeout(5*time.Second).Should(matchAll, "ResourceSlices without kubelet")

			ginkgo.By("restart kubelet")
			restartKubelet(ctx)
			restartKubelet = nil

			ginkgo.By("wait for exactly the node's ResourceSlice to get deleted")
			gomega.Eventually(ctx, listResources(f.ClientSet)).Should(matchOtherNode, "ResourceSlices with kubelet")
			gomega.Consistently(ctx, listResources(f.ClientSet)).WithTimeout(5*time.Second).Should(matchOtherNode, "ResourceSlices with kubelet")
		})

		f.It("must be removed after plugin unregistration", func(ctx context.Context) {
			nodeName := getNodeName(ctx, f)
			matchNode := gomega.ConsistOf(matchResourcesByNodeName(nodeName))

			ginkgo.By("start plugin and wait for ResourceSlice")
			kubeletPlugin := newKubeletPlugin(ctx, f.ClientSet, f.Namespace.Name, getNodeName(ctx, f), driverName)
			gomega.Eventually(ctx, listResources(f.ClientSet)).Should(matchNode, "ResourceSlice from kubelet plugin")
			gomega.Consistently(ctx, listResources(f.ClientSet)).WithTimeout(5*time.Second).Should(matchNode, "ResourceSlice from kubelet plugin")

			ginkgo.By("stop plugin and wait for ResourceSlice removal")
			kubeletPlugin.Stop()
			gomega.Eventually(ctx, listResources(f.ClientSet)).Should(gomega.BeEmpty(), "ResourceSlices with no plugin")
			gomega.Consistently(ctx, listResources(f.ClientSet)).WithTimeout(5*time.Second).Should(gomega.BeEmpty(), "ResourceSlices with no plugin")
		})

		removedIfPluginStopsAfterRegistration := func(ctx context.Context, datadir string, opts ...any) {
			nodeName := getNodeName(ctx, f)

			ginkgo.By("start DRA registrar")
			registrar := newRegistrar(ctx, f.ClientSet, nodeName, driverName, opts...)

			ginkgo.By("wait for registration to complete")
			gomega.Eventually(registrar.GetGRPCCalls).WithTimeout(pluginRegistrationTimeout).Should(testdrivergomega.BeRegistered)

			ginkgo.By("start DRA plugin service")
			kubeletPlugin := newDRAService(ctx, f.ClientSet, f.Namespace.Name, nodeName, driverName, datadir, opts...)

			ginkgo.By("wait for ResourceSlice to be created by plugin")
			matchNode := gomega.ConsistOf(matchResourcesByNodeName(nodeName))
			gomega.Eventually(ctx, listResources(f.ClientSet)).Should(matchNode, "ResourceSlices")
			gomega.Consistently(ctx, listResources(f.ClientSet)).WithTimeout(5*time.Second).Should(matchNode, "ResourceSlices")

			ginkgo.By("stop plugin")
			kubeletPlugin.Stop()

			ginkgo.By("wait for ResourceSlice removal")
			gomega.Eventually(ctx, listResources(f.ClientSet)).Should(gomega.BeEmpty(), "ResourceSlices")
			gomega.Consistently(ctx, listResources(f.ClientSet)).WithTimeout(5*time.Second).Should(gomega.BeEmpty(), "ResourceSlices")
		}
		ginkgo.DescribeTable("must be removed if plugin stops after registration",
			removedIfPluginStopsAfterRegistration,
			ginkgo.Entry("2 sockets", ""),
			ginkgo.Entry(
				"1 common socket",
				kubeletplugin.KubeletRegistryDir,
				kubeletplugin.PluginDataDirectoryPath(kubeletplugin.KubeletRegistryDir),
				kubeletplugin.PluginSocket(driverName+"-common.sock"),
			),
		)

		f.It("must be removed if plugin is unresponsive after registration", func(ctx context.Context) {
			nodeName := getNodeName(ctx, f)

			ginkgo.By("start DRA registrar")
			registrar := newRegistrar(ctx, f.ClientSet, nodeName, driverName)
			ginkgo.By("wait for registration to complete")
			gomega.Eventually(registrar.GetGRPCCalls).WithTimeout(pluginRegistrationTimeout).Should(testdrivergomega.BeRegistered)

			ginkgo.By("create a ResourceSlice")
			createTestResourceSlice(ctx, f.ClientSet, nodeName, driverName)
			gomega.Eventually(ctx, listResources(f.ClientSet)).Should(gomega.ConsistOf(matchResourcesByNodeName(nodeName)), "ResourceSlices without plugin")

			ginkgo.By("wait for ResourceSlice removal")
			gomega.Eventually(ctx, listResources(f.ClientSet)).Should(gomega.BeEmpty(), "ResourceSlices without plugin")
			gomega.Consistently(ctx, listResources(f.ClientSet)).WithTimeout(5*time.Second).Should(gomega.BeEmpty(), "ResourceSlices without plugin")
		})

		testRemoveIfRestartsQuickly := func(ctx context.Context, datadir string, opts ...any) {
			nodeName := getNodeName(ctx, f)

			ginkgo.By("start DRA registrar")
			registrar := newRegistrar(ctx, f.ClientSet, nodeName, driverName, opts...)

			ginkgo.By("wait for registration to complete")
			gomega.Eventually(registrar.GetGRPCCalls).WithTimeout(pluginRegistrationTimeout).Should(testdrivergomega.BeRegistered)

			ginkgo.By("start DRA plugin service")
			kubeletPlugin := newDRAService(ctx, f.ClientSet, f.Namespace.Name, nodeName, driverName, datadir, opts...)

			ginkgo.By("wait for ResourceSlice to be created by plugin")
			matchNode := gomega.ConsistOf(matchResourcesByNodeName(nodeName))
			gomega.Eventually(ctx, listResources(f.ClientSet)).Should(matchNode, "ResourceSlices")
			var slices []resourceapi.ResourceSlice
			gomega.Consistently(ctx, listAndStoreResources(f.ClientSet, &slices)).WithTimeout(5*time.Second).Should(matchNode, "ResourceSlices")

			ginkgo.By("stop plugin")
			kubeletPlugin.Stop()

			// 从 "must be removed if plugin is unresponsive after registration" 可知 kubelet
			// 最终会发现连接断开，但无法确定具体时刻（需要新增 "registered DRA plugins" 指标才能观测）。
			// 稍等几秒即可，这远短于清理延迟。
			time.Sleep(5 * time.Second)

			ginkgo.By("restarting plugin")
			newDRAService(ctx, f.ClientSet, f.Namespace.Name, nodeName, driverName, datadir, opts...)

			ginkgo.By("ensuring unchanged ResourceSlices")
			gomega.Consistently(ctx, listResources(f.ClientSet)).WithTimeout(time.Minute).Should(gomega.Equal(slices), "ResourceSlices")
		}
		ginkgo.DescribeTable("must not be removed if plugin restarts quickly enough",
			testRemoveIfRestartsQuickly,
			ginkgo.Entry("2 sockets", ""),
			ginkgo.Entry(
				"1 common socket",
				kubeletplugin.KubeletRegistryDir,
				kubeletplugin.PluginDataDirectoryPath(kubeletplugin.KubeletRegistryDir),
				kubeletplugin.PluginSocket(driverName+"-common.sock"),
			),
		)
	})

	f.Context("Resource Health", framework.WithFeatureGate(features.ResourceHealthStatus), f.WithSerial(), func() {

		// testHealthTransitions 验证 DRA plugin 上报的设备健康状态变化
		// (Healthy -> Unhealthy -> Healthy) 能正确反映到 Pod status，
		// 且 kubelet 通过预期的 DRAResourceHealth gRPC API 版本消费了这些状态。
		testHealthTransitions := func(ctx context.Context, prefix string, expectedHealthMethod string, opts ...pluginOption) {
			ginkgo.By("Starting the test driver with channel-based control")
			kubeletPlugin := newKubeletPlugin(ctx, f.ClientSet, f.Namespace.Name, getNodeName(ctx, f), driverName, opts...)

			pod, claimName, poolNameForTest, deviceNameForTest := setupAndVerifyHealthyPod(
				ctx, f, kubeletPlugin, driverName,
				prefix+"-class", prefix+"-claim", prefix+"-pod", "pool-a", "dev-0",
			)

			ginkgo.By("Verifying the negotiated health API version")
			gomega.Expect(kubeletPlugin.GetGRPCCalls()).To(gomega.ContainElement(gomega.HaveField("FullMethod", expectedHealthMethod)), "expected health stream on %s", expectedHealthMethod)

			ginkgo.By("Setting device health to Unhealthy via control channel")
			kubeletPlugin.HealthControlChan <- testdriver.DeviceHealthUpdate{
				PoolName:   poolNameForTest,
				DeviceName: deviceNameForTest,
				Health:     "Unhealthy",
			}

			ginkgo.By("Verifying device health is now Unhealthy")
			gomega.Eventually(ctx, func(ctx context.Context) (string, error) {
				return getDeviceHealthFromAPIServer(f, pod.Namespace, pod.Name, driverName, claimName, poolNameForTest, deviceNameForTest)
			}).WithTimeout(60*time.Second).WithPolling(2*time.Second).Should(gomega.Equal("Unhealthy"), "Device health should update to Unhealthy")

			ginkgo.By("Setting device health back to Healthy via control channel")
			kubeletPlugin.HealthControlChan <- testdriver.DeviceHealthUpdate{
				PoolName:   poolNameForTest,
				DeviceName: deviceNameForTest,
				Health:     "Healthy",
			}

			ginkgo.By("Verifying device health has recovered to Healthy")
			gomega.Eventually(ctx, func(ctx context.Context) (string, error) {
				return getDeviceHealthFromAPIServer(f, pod.Namespace, pod.Name, driverName, claimName, poolNameForTest, deviceNameForTest)
			}).WithTimeout(60*time.Second).WithPolling(2*time.Second).Should(gomega.Equal("Healthy"), "Device health should recover and update to Healthy")
		}

		ginkgo.It("should reflect device health changes in the Pod's status", func(ctx context.Context) {
			// plugin 同时提供多个版本时 kubelet 会选择最新版本，这是默认行为
			testHealthTransitions(ctx, "health-test", nodeWatchResourcesV1Method)
		})

		ginkgo.It("should reflect device health changes for a plugin which only supports the v1alpha1 health API", func(ctx context.Context) {
			// 模拟在 v1 API 出现之前发布的 driver。
			// TODO(harche): 1.40 时随 kubelet 的 v1alpha1 client 支持一起移除。
			testHealthTransitions(ctx, "health-v1alpha1", nodeWatchResourcesV1Alpha1Method, withoutHealthV1())
		})

		ginkgo.It("should reflect device health changes for a plugin which only supports the v1 health API", func(ctx context.Context) {
			testHealthTransitions(ctx, "health-v1", nodeWatchResourcesV1Method, withoutHealthV1alpha1())
		})

		// 验证 Kubelet 在整个 plugin 生命周期内与 DRA plugin 只建立一条 gRPC 连接。
		//
		// 方法是：
		// - 使用自定义 gRPC listener 统计接受的连接数。
		// - 依次创建 pod，触发 NodePrepareResources 和 NodeUnprepareResources。
		// - 断言 listener 只接受了一条连接 —— 因为每次 net.Listener.Accept() 调用
		// 都对应一个新传入连接 —— 从而确保所有调用复用了同一条 gRPC 连接。
		//
		// NOTE: 这是启用 ResourceHealth 时测试 service 连接复用的类似用例的复制版本。
		// ResourceHealth feature 成熟后应与原测试合并。
		ginkgo.It("must reuse one gRPC connection for service and health-monitoring calls", func(ctx context.Context) {
			var listener *countingListener
			var err error
			getListener := func(_ context.Context, socketPath string) (net.Listener, error) {
				listener, err = newCountingListener(socketPath)
				return listener, err
			}

			nodeName := getNodeName(ctx, f)

			ginkgo.By("start DRA registrar")
			registrar := newRegistrar(ctx, f.ClientSet, nodeName, driverName)

			ginkgo.By("wait for registration to complete")
			gomega.Eventually(registrar.GetGRPCCalls).WithTimeout(pluginRegistrationTimeout).Should(testdrivergomega.BeRegistered)

			ginkgo.By("start DRA plugin service")
			draService := newDRAService(ctx, f.ClientSet, f.Namespace.Name, nodeName, driverName, "", kubeletplugin.PluginListener(getListener))

			for _, suffix := range []string{"-1", "-2"} {
				draService.ResetGRPCCalls()

				setupAndVerifyHealthyPod(
					ctx, f, draService, driverName,
					"health-test-class"+suffix,
					"health-test-claim"+suffix,
					"health-test-pod"+suffix,
					"pool-a"+suffix,
					"dev-0"+suffix,
				)

				ginkgo.By("check that listener.Accept was called only once " + suffix)
				gomega.Expect(listener.acceptCount()).To(gomega.Equal(1))
			}
		})

		// 验证 DRA plugin 停止时设备健康状态变为 "Unknown"，重启后恢复为 "Healthy"
		ginkgo.It("should update health to Unknown when plugin stops and recover upon restart", func(ctx context.Context) {
			ginkgo.By("Starting the test driver")
			kubeletPlugin := newKubeletPlugin(ctx, f.ClientSet, f.Namespace.Name, getNodeName(ctx, f), driverName)

			pod, claimName, poolNameForTest, deviceNameForTest := setupAndVerifyHealthyPod(
				ctx, f, kubeletPlugin, driverName,
				"unknown-test-class", "unknown-test-claim", "unknown-test-pod", "pool-b", "dev-1",
			)

			ginkgo.By("Stopping the DRA plugin to simulate a crash")
			kubeletPlugin.Stop()

			ginkgo.By("Verifying device health transitions to 'Unknown'")
			gomega.Eventually(ctx, func(ctx context.Context) (string, error) {
				return getDeviceHealthFromAPIServer(f, pod.Namespace, pod.Name, driverName, claimName, poolNameForTest, deviceNameForTest)
			}).WithTimeout(2*time.Minute).WithPolling(5*time.Second).Should(gomega.Equal("Unknown"), "Device health should become Unknown after plugin stops")

			ginkgo.By("Restarting the DRA plugin to simulate recovery")
			// 重新初始化 plugin，它会向 Kubelet 重新注册
			kubeletPlugin = newKubeletPlugin(ctx, f.ClientSet, f.Namespace.Name, getNodeName(ctx, f), driverName)

			ginkgo.By("Forcing a 'Healthy' status update after restart")
			kubeletPlugin.HealthControlChan <- testdriver.DeviceHealthUpdate{
				PoolName:   poolNameForTest,
				DeviceName: deviceNameForTest,
				Health:     "Healthy",
			}

			ginkgo.By("Verifying device health recovers to 'Healthy'")
			gomega.Eventually(ctx, func(ctx context.Context) (string, error) {
				return getDeviceHealthFromAPIServer(f, pod.Namespace, pod.Name, driverName, claimName, poolNameForTest, deviceNameForTest)
			}).WithTimeout(60*time.Second).WithPolling(2*time.Second).Should(gomega.Equal("Healthy"), "Device health should recover to Healthy after plugin restarts")
		})

		// 验证连接断开后的重连
		ginkgo.It("should automatically reconnect both DRA and Health API after connection drop", func(ctx context.Context) {
			ginkgo.By("Starting the test driver")
			kubeletPlugin := newKubeletPlugin(ctx, f.ClientSet, f.Namespace.Name, getNodeName(ctx, f), driverName)

			ginkgo.By("Creating initial pod to establish connection")
			pod, claimName, poolNameForTest, deviceNameForTest := setupAndVerifyHealthyPod(
				ctx, f, kubeletPlugin, driverName,
				"reconnect-test-class", "reconnect-test-claim", "reconnect-test-pod", "pool-reconnect", "dev-reconnect-0",
			)

			ginkgo.By("Simulating connection drop by stopping the plugin")
			kubeletPlugin.Stop()

			ginkgo.By("Verifying health transitions to Unknown after connection drop")
			gomega.Eventually(ctx, func(ctx context.Context) (string, error) {
				return getDeviceHealthFromAPIServer(f, pod.Namespace, pod.Name, driverName, claimName, poolNameForTest, deviceNameForTest)
			}).WithTimeout(2*time.Minute).WithPolling(5*time.Second).Should(gomega.Equal("Unknown"),
				"Health should become Unknown when plugin connection is lost")

			ginkgo.By("Restarting the plugin to simulate reconnection")
			kubeletPlugin = newKubeletPlugin(ctx, f.ClientSet, f.Namespace.Name, getNodeName(ctx, f), driverName)

			ginkgo.By("Waiting for plugin registration after restart")
			gomega.Eventually(kubeletPlugin.GetGRPCCalls).WithTimeout(pluginRegistrationTimeout).Should(testdrivergomega.BeRegistered,
				"Plugin should re-register after restart")

			ginkgo.By("Creating a new pod to trigger NodePrepareResources after reconnection")
			pod2, claimName2, poolNameForTest2, deviceNameForTest2 := setupAndVerifyHealthyPod(
				ctx, f, kubeletPlugin, driverName,
				"reconnect-test-class-2", "reconnect-test-claim-2", "reconnect-test-pod-2", "pool-reconnect-2", "dev-reconnect-1",
			)

			ginkgo.By("Verifying health updates continue to work")
			kubeletPlugin.HealthControlChan <- testdriver.DeviceHealthUpdate{
				PoolName:   poolNameForTest2,
				DeviceName: deviceNameForTest2,
				Health:     "Unhealthy",
			}

			gomega.Eventually(ctx, func(ctx context.Context) (string, error) {
				return getDeviceHealthFromAPIServer(f, pod2.Namespace, pod2.Name, driverName, claimName2, poolNameForTest2, deviceNameForTest2)
			}).WithTimeout(60*time.Second).WithPolling(2*time.Second).Should(gomega.Equal("Unhealthy"),
				"Health API should continue working after reconnection")
		})

		// 验证并发操作
		ginkgo.It("should handle concurrent DRA operations and health monitoring without connection issues", func(ctx context.Context) {
			ginkgo.By("Starting the test driver")
			kubeletPlugin := newKubeletPlugin(ctx, f.ClientSet, f.Namespace.Name, getNodeName(ctx, f), driverName)

			numPods := 3
			pods := make([]*v1.Pod, numPods)
			claimNames := make([]string, numPods)
			poolNames := make([]string, numPods)
			deviceNames := make([]string, numPods)

			ginkgo.By(fmt.Sprintf("Creating %d pods concurrently to stress test connection management", numPods))
			for i := range numPods {
				className := fmt.Sprintf("concurrent-class-%d", i)
				claimNames[i] = fmt.Sprintf("concurrent-claim-%d", i)
				podName := fmt.Sprintf("concurrent-pod-%d", i)
				poolNames[i] = fmt.Sprintf("pool-concurrent-%d", i)
				deviceNames[i] = fmt.Sprintf("dev-concurrent-%d", i)

				pods[i] = createHealthTestPodAndClaim(ctx, f, driverName, podName, claimNames[i], className, poolNames[i], deviceNames[i])
			}

			ginkgo.By("Waiting for all pods to be running")
			for i, pod := range pods {
				framework.ExpectNoError(e2epod.WaitForPodRunningInNamespace(ctx, f.ClientSet, pod),
					fmt.Sprintf("Pod %d should be running", i))
			}

			ginkgo.By("Verifying NodePrepareResources was called for all pods")
			gomega.Eventually(func() int {
				return kubeletPlugin.CountCalls("/NodePrepareResources")
			}).WithTimeout(retryTestTimeout).Should(gomega.BeNumerically(">=", numPods),
				"NodePrepareResources should be called at least once per pod")

			ginkgo.By("Sending health updates for all devices concurrently")
			for i := range numPods {
				kubeletPlugin.HealthControlChan <- testdriver.DeviceHealthUpdate{
					PoolName:   poolNames[i],
					DeviceName: deviceNames[i],
					Health:     "Healthy",
				}
			}

			ginkgo.By("Verifying all health updates are correctly reflected")
			for i := range numPods {
				pod := pods[i]
				poolName := poolNames[i]
				deviceName := deviceNames[i]
				claimName := claimNames[i]

				gomega.Eventually(ctx, func(ctx context.Context) (string, error) {
					return getDeviceHealthFromAPIServer(f, pod.Namespace, pod.Name, driverName, claimName, poolName, deviceName)
				}).WithTimeout(60*time.Second).WithPolling(2*time.Second).Should(gomega.Equal("Healthy"),
					fmt.Sprintf("Health for device %s should be Healthy", deviceName))
			}

			ginkgo.By("Changing health status for all devices to verify continued operation")
			for i := range numPods {
				kubeletPlugin.HealthControlChan <- testdriver.DeviceHealthUpdate{
					PoolName:   poolNames[i],
					DeviceName: deviceNames[i],
					Health:     "Unhealthy",
				}
			}

			ginkgo.By("Verifying all health changes are correctly reflected")
			for i := range numPods {
				pod := pods[i]
				poolName := poolNames[i]
				deviceName := deviceNames[i]
				claimName := claimNames[i]

				gomega.Eventually(ctx, func(ctx context.Context) (string, error) {
					return getDeviceHealthFromAPIServer(f, pod.Namespace, pod.Name, driverName, claimName, poolName, deviceName)
				}).WithTimeout(60*time.Second).WithPolling(2*time.Second).Should(gomega.Equal("Unhealthy"),
					fmt.Sprintf("Health for device %s should be Unhealthy", deviceName))
			}
		})
	})

	// 与上面的 "Resource Health" context 对应，但这里包含需要在 feature gate 关闭时运行的测试。
	// 由于没有通用方法提前过滤这类测试，只能在运行时检查。
	f.Context("Resource Health", f.WithSerial(), func() {

		ginkgo.BeforeEach(func() {
			if e2eskipper.IsFeatureGateEnabled(features.ResourceHealthStatus) {
				e2eskipper.Skipf("feature %s is enabled", features.ResourceHealthStatus)
			}
		})

		// 验证 ResourceHealthStatus feature gate 关闭时 Kubelet 不会向 Pod 添加健康状态
		ginkgo.It("should not add health status to Pod when feature gate is disabled", func(ctx context.Context) {

			ginkgo.By("Starting a test driver")
			newKubeletPlugin(ctx, f.ClientSet, f.Namespace.Name, getNodeName(ctx, f), driverName, withHealthService(false))

			className := "gate-disabled-class"
			claimName := "gate-disabled-claim"
			podName := "gate-disabled-pod"
			poolNameForTest := "pool-d"
			deviceNameForTest := "dev-3"

			pod := createHealthTestPodAndClaim(ctx, f, driverName, podName, claimName, className, poolNameForTest, deviceNameForTest)

			ginkgo.By("Waiting for the pod to be running")
			framework.ExpectNoError(e2epod.WaitForPodRunningInNamespace(ctx, f.ClientSet, pod))

			ginkgo.By("Consistently verifying that the allocatedResourcesStatus field remains absent")
			gomega.Consistently(func(ctx context.Context) error {
				p, err := f.ClientSet.CoreV1().Pods(pod.Namespace).Get(ctx, pod.Name, metav1.GetOptions{})
				if err != nil {
					return err
				}
				for _, containerStatus := range p.Status.ContainerStatuses {
					if containerStatus.Name == "testcontainer" {
						if len(containerStatus.AllocatedResourcesStatus) != 0 {
							return fmt.Errorf("expected allocatedResourcesStatus to be absent, but found %d entries", len(containerStatus.AllocatedResourcesStatus))
						}
						return nil
					}
				}
				return fmt.Errorf("could not find container 'testcontainer' in pod status")
			}).WithContext(ctx).WithTimeout(30*time.Second).WithPolling(2*time.Second).Should(gomega.Succeed(), "The allocatedResourcesStatus field should be absent when the feature gate is disabled")
		})

		// 这些测试验证使用 ShareID 分配的设备也能正确上报健康状态，确保两个 feature 正常协作
		f.Context("Resource Health with ShareID", framework.WithFeatureGate(features.ResourceHealthStatus), framework.WithFeatureGate(features.DRAConsumableCapacity), f.WithSerial(), func() {

			ginkgo.BeforeEach(func() {
				// feature gate 已启用时跳过（本测试需要自己启用它们）
				if e2eskipper.IsFeatureGateEnabled(features.ResourceHealthStatus) {
					e2eskipper.Skipf("feature %s is already enabled", features.ResourceHealthStatus)
				}
				if e2eskipper.IsFeatureGateEnabled(features.DRAConsumableCapacity) {
					e2eskipper.Skipf("feature %s is already enabled", features.DRAConsumableCapacity)
				}
			})

			// 验证使用 ShareID 分配的设备健康状态变化能正确反映到 Pod status
			ginkgo.It("should reflect device health changes for devices with ShareID", func(ctx context.Context) {
				ginkgo.By("Starting the test driver")
				kubeletPlugin := newKubeletPlugin(ctx, f.ClientSet, f.Namespace.Name, getNodeName(ctx, f), driverName)

				ginkgo.By("wait for registration to complete")
				gomega.Eventually(kubeletPlugin.GetGRPCCalls).WithTimeout(pluginRegistrationTimeout).Should(testdrivergomega.BeRegistered)

				poolName := "shareid-health-pool"
				deviceName := "shareid-health-device"
				testShareID := uuid.NewUUID()

				// 创建 allocation result 中包含 ShareID 的测试对象
				pod := createTestObjectsWithShareID(ctx, f, driverName, "shareid-health-class", "shareid-health-claim", "shareid-health-pod", poolName, deviceName, &testShareID)

				ginkgo.By("wait for NodePrepareResources call to succeed")
				gomega.Eventually(kubeletPlugin.GetGRPCCalls).WithTimeout(retryTestTimeout).Should(testdrivergomega.NodePrepareResourcesSucceeded)

				ginkgo.By("wait for pod to be running")
				framework.ExpectNoError(e2epod.WaitForPodRunningInNamespace(ctx, f.ClientSet, pod))

				ginkgo.By("Setting device health to Healthy via control channel")
				kubeletPlugin.HealthControlChan <- testdriver.DeviceHealthUpdate{
					PoolName:   poolName,
					DeviceName: deviceName,
					Health:     "Healthy",
				}

				ginkgo.By("Verifying device health is Healthy in the pod status")
				gomega.Eventually(ctx, func(ctx context.Context) (string, error) {
					return getDeviceHealthFromAPIServer(f, pod.Namespace, pod.Name, driverName, "shareid-health-claim", poolName, deviceName)
				}).WithTimeout(60*time.Second).WithPolling(2*time.Second).Should(gomega.Equal("Healthy"), "Device with ShareID should show Healthy status")

				ginkgo.By("Setting device health to Unhealthy via control channel")
				kubeletPlugin.HealthControlChan <- testdriver.DeviceHealthUpdate{
					PoolName:   poolName,
					DeviceName: deviceName,
					Health:     "Unhealthy",
				}

				ginkgo.By("Verifying device health is now Unhealthy in the pod status")
				gomega.Eventually(ctx, func(ctx context.Context) (string, error) {
					return getDeviceHealthFromAPIServer(f, pod.Namespace, pod.Name, driverName, "shareid-health-claim", poolName, deviceName)
				}).WithTimeout(60*time.Second).WithPolling(2*time.Second).Should(gomega.Equal("Unhealthy"), "Device with ShareID should update to Unhealthy")

				ginkgo.By("Setting device health back to Healthy")
				kubeletPlugin.HealthControlChan <- testdriver.DeviceHealthUpdate{
					PoolName:   poolName,
					DeviceName: deviceName,
					Health:     "Healthy",
				}

				ginkgo.By("Verifying device health has recovered to Healthy")
				gomega.Eventually(ctx, func(ctx context.Context) (string, error) {
					return getDeviceHealthFromAPIServer(f, pod.Namespace, pod.Name, driverName, "shareid-health-claim", poolName, deviceName)
				}).WithTimeout(60*time.Second).WithPolling(2*time.Second).Should(gomega.Equal("Healthy"), "Device with ShareID should recover to Healthy")
			})
		})

		f.Context("Device ShareID", framework.WithFeatureGate(features.DRAConsumableCapacity), f.WithSerial(), func() {

			ginkgo.BeforeEach(func() {
				if e2eskipper.IsFeatureGateEnabled(features.DRAConsumableCapacity) {
					e2eskipper.Skipf("feature %s is enabled", features.DRAConsumableCapacity)
				}
			})

			testWithDeviceRequestAllocationResults := func(ctx context.Context, results []resourceapi.DeviceRequestAllocationResult) {
				nodeName := getNodeName(ctx, f)
				kubeletPlugin := newKubeletPlugin(ctx, f.ClientSet, f.Namespace.Name, getNodeName(ctx, f), driverName)

				ginkgo.By("wait for registration to complete")
				gomega.Eventually(kubeletPlugin.GetGRPCCalls).WithTimeout(pluginRegistrationTimeout).Should(testdrivergomega.BeRegistered)

				pod := createTestObjects(ctx, f.ClientSet, nodeName, f.Namespace.Name, "draclass", "external-claim", "drapod", false, []string{driverName}, results...)

				ginkgo.By("wait for NodePrepareResources call to succeed with given allocation results")
				gomega.Eventually(kubeletPlugin.GetGRPCCalls).WithTimeout(retryTestTimeout).Should(testdrivergomega.NodePrepareResourcesSucceeded)

				ginkgo.By("wait for pod to succeed")
				err := e2epod.WaitForPodSuccessInNamespace(ctx, f.ClientSet, pod.Name, f.Namespace.Name)
				framework.ExpectNoError(err)

				ginkgo.By("delete pod")
				e2epod.DeletePodOrFail(ctx, f.ClientSet, f.Namespace.Name, pod.Name)

				ginkgo.By("wait for NodeUnprepareResources call to succeed")
				gomega.Eventually(kubeletPlugin.GetGRPCCalls).WithTimeout(retryTestTimeout).Should(testdrivergomega.NodeUnprepareResourcesSucceeded)
			}
			ginkgo.DescribeTable("must be functional when plugin starts to listen on a service socket after registration with the specified allocation results",
				testWithDeviceRequestAllocationResults,
				ginkgo.Entry("without ShareID", nil),
				ginkgo.Entry("with a ShareID", []resourceapi.DeviceRequestAllocationResult{
					{
						Driver:      driverName,
						Pool:        "some-pool",
						Device:      "some-device",
						Request:     "my-request",
						ShareID:     ptr.To(uuid.NewUUID()),
						AdminAccess: ptr.To(false),
					},
				},
				),
				ginkgo.Entry("same device with two ShareIDs", []resourceapi.DeviceRequestAllocationResult{
					{
						Driver:      driverName,
						Pool:        "some-pool",
						Device:      "some-device",
						Request:     "my-request",
						ShareID:     ptr.To(uuid.NewUUID()),
						AdminAccess: ptr.To(false),
					},
					{
						Driver:      driverName,
						Pool:        "some-pool",
						Device:      "some-device",
						Request:     "my-request",
						ShareID:     ptr.To(uuid.NewUUID()),
						AdminAccess: ptr.To(false),
					},
				},
				),
			)

		})
	})
})

// pluginOption 定义用于配置 test driver 的 functional option
type pluginOption func(*testdriver.Options)

// withHealthService 是显式启用或禁用 health service 的 pluginOption
func withHealthService(enabled bool) pluginOption {
	return func(o *testdriver.Options) {
		o.EnableHealthService = enabled
	}
}

// withoutHealthV1 将 driver 限制为 v1alpha1 DRAResourceHealth gRPC API，
// 模拟在 v1 API 出现之前发布的 driver。过渡期内 kubelet 会继续消费 v1alpha1 三个版本。
func withoutHealthV1() pluginOption {
	return func(o *testdriver.Options) {
		o.DisableHealthV1 = true
	}
}

// withoutHealthV1alpha1 将 driver 限制为 v1 DRAResourceHealth gRPC API，
// 模拟已放弃 v1alpha1 支持的 driver
func withoutHealthV1alpha1() pluginOption {
	return func(o *testdriver.Options) {
		o.DisableHealthV1alpha1 = true
	}
}

// kubelet 订阅设备健康更新所用的 gRPC 方法，取决于协商出的 DRAResourceHealth API 版本
const (
	nodeWatchResourcesV1Alpha1Method = "/" + drahealthv1alpha1.DRAResourceHealthService + "/NodeWatchResources"
	nodeWatchResourcesV1Method       = "/" + drahealthv1.DRAResourceHealthService + "/NodeWatchResources"
)

// 启动 Kubelet plugin 并等待注册完成
func newKubeletPlugin(ctx context.Context, clientSet kubernetes.Interface, testNamespace string, nodeName, driverName string, options ...pluginOption) *testdriver.ExamplePlugin {
	ginkgo.By("start Kubelet plugin")
	logger := klog.LoggerWithValues(klog.LoggerWithName(klog.Background(), "DRA kubelet plugin "+driverName), "node", nodeName)
	ctx = klog.NewContext(ctx, logger)

	// 确保目录存在，必要时创建。尽早发现 setup 问题，避免后续无法创建目录。
	err := os.MkdirAll(cdiDir, os.FileMode(0750))
	framework.ExpectNoError(err, "create CDI directory")
	datadir := path.Join(kubeletplugin.KubeletPluginsDir, driverName) // 使用默认值，下面不会覆盖
	err = os.MkdirAll(datadir, 0750)
	framework.ExpectNoError(err, "create DRA socket directory")

	pluginOpts := testdriver.Options{
		EnableHealthService: true,
	}
	for _, option := range options {
		option(&pluginOpts)
	}

	plugin, err := testdriver.StartPlugin(
		context.WithoutCancel(ctx), // 停止操作由 draServiceCleanup 负责
		cdiDir,
		driverName,
		clientSet,
		nodeName,
		testdriver.FileOperations{
			DriverResources: &resourceslice.DriverResources{
				Pools: map[string]resourceslice.Pool{
					nodeName: {
						Slices: []resourceslice.Slice{{
							Devices: []resourceapi.Device{
								{
									Name: "device-00",
								},
							},
						}},
					},
				},
			},
		},
		pluginOpts,
	)
	framework.ExpectNoError(err)

	ginkgo.DeferCleanup(func(ctx context.Context) { draServiceCleanup(ctx, clientSet, driverName, testNamespace, plugin) })

	gomega.Eventually(plugin.GetGRPCCalls).WithTimeout(pluginRegistrationTimeout).Should(testdrivergomega.BeRegistered)

	return plugin
}

// newRegistrar 为指定 DRA driver 启动 registrar，不启动 DRA gRPC service
func newRegistrar(ctx context.Context, clientSet kubernetes.Interface, nodeName, driverName string, opts ...any) *testdriver.ExamplePlugin {
	ginkgo.By("start only Kubelet plugin registrar")
	logger := klog.LoggerWithValues(klog.LoggerWithName(klog.Background(), "kubelet plugin registrar "+driverName))
	ctx = klog.NewContext(ctx, logger)

	allOpts := []any{
		// 拆分部署场景下，kubelet 根据 registrar 的注册信息协商 health service，
		// 因此即使由独立 service 实例提供服务，registrar 也必须声明 health 能力。
		testdriver.Options{EnableHealthService: true},
		kubeletplugin.DRAService(false),
	}

	allOpts = append(allOpts, opts...)

	registrar, err := testdriver.StartPlugin(
		context.WithoutCancel(ctx), // 在测试清理或提前请求时停止
		cdiDir,
		driverName,
		clientSet,
		nodeName,
		testdriver.FileOperations{},
		allOpts...,
	)
	framework.ExpectNoError(err, "start only Kubelet plugin registrar")
	ginkgo.DeferCleanup(registrar.Stop)
	return registrar
}

// newDRAService 为指定 node 和 driver 启动 DRA gRPC service。
// 它会确保所需目录存在，启动 plugin，并注册清理函数以在测试后删除创建的资源。
// 参数：
//   - ctx: 用于控制取消和日志的 context。
//   - clientSet: 用于与集群交互的 Kubernetes client 接口。
//   - nodeName: plugin 运行的 node 名称。
//   - driverName: DRA driver 名称。
//   - datadir: DRA socket 和状态文件所在目录。
//     必须与通过 [kubeletplugin.PluginDataDirectoryPath] 指定的路径一致。
//     未使用该 option 时可留空，此时使用默认路径。
//   - opts: plugin 配置的其他选项。
//
// 返回值：
//   - 指向已启动 ExamplePlugin 实例的指针。
func newDRAService(ctx context.Context, clientSet kubernetes.Interface, testNamespace, nodeName, driverName, datadir string, opts ...any) *testdriver.ExamplePlugin {
	ginkgo.By("start only Kubelet plugin")
	logger := klog.LoggerWithValues(klog.LoggerWithName(klog.Background(), "kubelet plugin "+driverName), "node", nodeName)
	ctx = klog.NewContext(ctx, logger)

	allOpts := []any{
		testdriver.Options{EnableHealthService: true},
		kubeletplugin.RegistrationService(false),
	}
	allOpts = append(allOpts, opts...)

	// 确保目录存在，必要时创建。尽早发现 setup 问题，避免后续无法创建目录。
	err := os.MkdirAll(cdiDir, os.FileMode(0750))
	framework.ExpectNoError(err, "create CDI directory")

	// 未提供 datadir 时使用默认路径，并确保其存在
	if datadir == "" {
		datadir = path.Join(kubeletplugin.KubeletPluginsDir, driverName)
	}

	err = os.MkdirAll(datadir, 0750)
	framework.ExpectNoError(err, "create DRA socket directory")

	plugin, err := testdriver.StartPlugin(
		context.WithoutCancel(ctx), // 停止操作由 draServiceCleanup 负责
		cdiDir,
		driverName,
		clientSet,
		nodeName,
		testdriver.FileOperations{
			DriverResources: &resourceslice.DriverResources{
				Pools: map[string]resourceslice.Pool{
					nodeName: {
						Slices: []resourceslice.Slice{{
							Devices: []resourceapi.Device{
								{
									Name: "device-00",
								},
							},
						}},
					},
				},
			},
		},
		allOpts...,
	)
	framework.ExpectNoError(err)

	ginkgo.DeferCleanup(func(ctx context.Context) { draServiceCleanup(ctx, clientSet, driverName, testNamespace, plugin) })

	return plugin
}

func draServiceCleanup(ctx context.Context, clientSet kubernetes.Interface, driverName string, testNamespace string, plugin *testdriver.ExamplePlugin) {
	// 无论成功失败，最后都要停止
	defer func() {
		ginkgo.By("Stopping the driver and deleting ResourceSlices...")
		plugin.Stop()

		// kubelet 最终应该会做这件事，但这里还是显式确保。
		// 另有专门的测试检查这一点。
		framework.ExpectNoError(clientSet.ResourceV1().ResourceSlices().DeleteCollection(ctx, metav1.DeleteOptions{}, metav1.ListOptions{FieldSelector: resourceapi.ResourceSliceSelectorDriver + "=" + driverName}))
	}()

	// 曾有测试在 driver 关闭前没删除 pod，导致泄漏使用中的 ResourceClaim
	// (https://github.com/kubernetes/kubernetes/issues/133304)。namespace 删除通常会处理，
	// 但那发生在稍后，而且没有 driver 时 pod 会一直 pending。因此在关闭 driver *之前*
	// 先删除测试 namespace 中的所有 pod。
	ginkgo.By("Deleting pods and waiting for ResourceClaims to be released...")
	framework.ExpectNoError(clientSet.CoreV1().Pods(testNamespace).DeleteCollection(ctx, metav1.DeleteOptions{}, metav1.ListOptions{}))

	// 在这里失败比下面的指标检查更直观。
	// 但仍会检查指标，以确保它们确实下降。
	gomega.Eventually(ctx, framework.ListObjects(clientSet.CoreV1().Pods(testNamespace).List, metav1.ListOptions{})).Should(gomega.HaveField("Items", gomega.BeEmpty()), "All pods should be deleted, shut down and removed.")

	// kubelet 完全处理完 pod 删除需要一段时间，因此允许延迟。
	// 通常 driver 不会有 dra_resource_claims_in_use 条目（由 gstruct.IgnoreMissing 处理）。
	// 如果存在，其值必须为 0。
	gomega.Eventually(ctx, getKubeletMetrics).Should(gstruct.MatchKeys(gstruct.IgnoreExtras, gstruct.Keys{
		"dra_resource_claims_in_use": gstruct.MatchElements(driverNameLabelKey, gstruct.IgnoreExtras|gstruct.IgnoreMissing, gstruct.Elements{
			driverName: timelessSample(0),
		}),
	}), "The test should have deleted all pods.\nInstead some claims prepared by the driver %s are still in use at the time when the driver gets uninstalled.", driverName)
}

// createTestObjects 创建测试所需的对象
// NOTE: Node e2e 不运行 scheduler 和 controller manager，因此对象必须自带全部数据，
// 以便 API server 正确处理并在不依赖 scheduler 和 DRA controller 的情况下落到 node 上。
//
// 不增加更多参数，而是通过 podName 决定 pod 的行为。
func createTestObjects(ctx context.Context, clientSet kubernetes.Interface, nodename, namespace, className, claimName, podName string, deferPodDeletion bool, driverNames []string,
	deviceRequestResults ...resourceapi.DeviceRequestAllocationResult) *v1.Pod {
	// 先建 DeviceClass，驱动才能据此上报设备
	class := &resourceapi.DeviceClass{
		ObjectMeta: metav1.ObjectMeta{
			Name: className,
		},
	}
	_, err := clientSet.ResourceV1().DeviceClasses().Create(ctx, class, metav1.CreateOptions{})
	framework.ExpectNoError(err)

	ginkgo.DeferCleanup(clientSet.ResourceV1().DeviceClasses().Delete, className, metav1.DeleteOptions{})

	// 创建 ResourceClaim
	podClaimName := "resource-claim"
	claim := &resourceapi.ResourceClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name: claimName,
		},
		Spec: resourceapi.ResourceClaimSpec{
			Devices: resourceapi.DeviceClaim{
				Requests: []resourceapi.DeviceRequest{{
					Name: "my-request",
					Exactly: &resourceapi.ExactDeviceRequest{
						DeviceClassName: className,
					},
				}},
			},
		},
	}
	createdClaim, err := clientSet.ResourceV1().ResourceClaims(namespace).Create(ctx, claim, metav1.CreateOptions{})
	framework.ExpectNoError(err)

	ginkgo.DeferCleanup(clientSet.ResourceV1().ResourceClaims(namespace).Delete, claimName, metav1.DeleteOptions{})

	// pod 用 grep 检查自身 env。每个 driver 注入自己的参数，变量名包含 driver 名称。
	// 排序让顺序确定，这样 env 输出合并为一行后单个 grep 就能匹配。
	nameToEnv := func(driverName string) string {
		return "DRA_" + regexp.MustCompile(`[^a-z0-9]`).ReplaceAllString(driverName, "_")
	}
	var expectedEnv []string
	sort.Strings(driverNames)
	for _, driverName := range driverNames {
		expectedEnv = append(expectedEnv, nameToEnv(driverName)+"=PARAM1_VALUE")
	}
	containerName := "testcontainer"
	pod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      podName,
			Namespace: namespace,
		},
		Spec: v1.PodSpec{
			NodeName: nodename, // scheduler 未运行，直接指定 node
			ResourceClaims: []v1.PodResourceClaim{
				{
					Name:              podClaimName,
					ResourceClaimName: &claimName,
				},
			},
			Containers: []v1.Container{
				{
					Name:  containerName,
					Image: e2epod.GetDefaultTestImage(),
					Resources: v1.ResourceRequirements{
						Claims: []v1.ResourceClaim{{Name: podClaimName}},
					},
					// env 注入失败时 pod 会失败，错误信息会出现在
					// ... Terminated:&ContainerStateTerminated{ExitCode:1,Signal:0,Reason:Error,Message:ERROR: ... 中
					Command: []string{"/bin/sh", "-c", "if ! echo $(env) | grep -q " + strings.Join(expectedEnv, ".*") + "; then echo ERROR: unexpected env: $(env) >/dev/termination-log; exit 1 ; fi"},
				},
			},
			RestartPolicy: v1.RestartPolicyNever,
		},
	}
	if podName == "drasleeppod" {
		// 与上面相同，额外无限休眠
		pod.Spec.Containers[0].Command[2] += "&& sleep 100000"
	}
	createdPod, err := clientSet.CoreV1().Pods(namespace).Create(ctx, pod, metav1.CreateOptions{})
	framework.ExpectNoError(err)

	if deferPodDeletion {
		ginkgo.DeferCleanup(clientSet.CoreV1().Pods(namespace).Delete, podName, metav1.DeleteOptions{})
	}

	// 更新 claim status：设置 ReservedFor 和 AllocationResult
	// NOTE: 通常由 DRA controller 或 scheduler 完成

	var results []resourceapi.DeviceRequestAllocationResult
	if len(deviceRequestResults) > 0 {
		// 指定 DeviceRequestAllocationResult，以测试 ShareID 等特定的 allocation result 场景
		results = deviceRequestResults
	} else {
		// 为每个 driver 生成 `some-device` 的 DeviceRequestAllocationResult
		results = make([]resourceapi.DeviceRequestAllocationResult, len(driverNames))
		for i, driverName := range driverNames {
			results[i] = resourceapi.DeviceRequestAllocationResult{
				Driver:      driverName,
				Pool:        "some-pool",
				Device:      "some-device",
				Request:     claim.Spec.Devices.Requests[0].Name,
				AdminAccess: ptr.To(false),
			}
		}
	}

	config := make([]resourceapi.DeviceAllocationConfiguration, len(driverNames))

	for i, driverName := range driverNames {
		config[i] = resourceapi.DeviceAllocationConfiguration{
			Source: resourceapi.AllocationConfigSourceClaim,
			DeviceConfiguration: resourceapi.DeviceConfiguration{
				Opaque: &resourceapi.OpaqueDeviceConfiguration{
					Driver:     driverName,
					Parameters: runtime.RawExtension{Raw: []byte(`{"` + nameToEnv(driverName) + `":"PARAM1_VALUE"}`)},
				},
			},
		}
	}

	createdClaim.Status = resourceapi.ResourceClaimStatus{
		ReservedFor: []resourceapi.ResourceClaimConsumerReference{
			{Resource: "pods", Name: podName, UID: createdPod.UID},
		},
		Allocation: &resourceapi.AllocationResult{
			Devices: resourceapi.DeviceAllocationResult{
				Results: results,
				Config:  config,
			},
		},
	}
	_, err = clientSet.ResourceV1().ResourceClaims(namespace).UpdateStatus(ctx, createdClaim, metav1.UpdateOptions{})
	framework.ExpectNoError(err)

	return pod
}

func createTestResourceSlice(ctx context.Context, clientSet kubernetes.Interface, nodeName, driverName string) {
	slice := &resourceapi.ResourceSlice{
		ObjectMeta: metav1.ObjectMeta{
			Name: nodeName,
		},
		Spec: resourceapi.ResourceSliceSpec{
			NodeName: &nodeName,
			Driver:   driverName,
			Pool: resourceapi.ResourcePool{
				Name:               nodeName,
				ResourceSliceCount: 1,
			},
		},
	}

	ginkgo.By(fmt.Sprintf("Creating ResourceSlice %s", nodeName))
	slice, err := clientSet.ResourceV1().ResourceSlices().Create(ctx, slice, metav1.CreateOptions{})
	framework.ExpectNoError(err, "create ResourceSlice")
	ginkgo.DeferCleanup(func(ctx context.Context) {
		ginkgo.By(fmt.Sprintf("Deleting ResourceSlice %s", nodeName))
		err := clientSet.ResourceV1().ResourceSlices().Delete(ctx, slice.Name, metav1.DeleteOptions{})
		if !apierrors.IsNotFound(err) {
			framework.ExpectNoError(err, "delete ResourceSlice")
		}
	})
}

func listResources(client kubernetes.Interface) func(ctx context.Context) ([]resourceapi.ResourceSlice, error) {
	return func(ctx context.Context) ([]resourceapi.ResourceSlice, error) {
		slices, err := client.ResourceV1().ResourceSlices().List(ctx, metav1.ListOptions{})
		if err != nil {
			return nil, err
		}
		return slices.Items, nil
	}
}

func listAndStoreResources(client kubernetes.Interface, lastSlices *[]resourceapi.ResourceSlice) func(ctx context.Context) ([]resourceapi.ResourceSlice, error) {
	return func(ctx context.Context) ([]resourceapi.ResourceSlice, error) {
		slices, err := client.ResourceV1().ResourceSlices().List(ctx, metav1.ListOptions{})
		if err != nil {
			return nil, err
		}
		*lastSlices = slices.Items
		return *lastSlices, nil
	}
}

func matchResourcesByNodeName(nodeName string) types.GomegaMatcher {
	return gomega.HaveField("Spec.NodeName", gstruct.PointTo(gomega.Equal(nodeName)))
}

// 从主 API server 查询 pod status 的辅助函数
func getDeviceHealthFromAPIServer(f *framework.Framework, namespace, podName, driverName, claimName, poolName, deviceName string) (string, error) {
	// 从 API server 获取 Pod 对象
	pod, err := f.ClientSet.CoreV1().Pods(namespace).Get(context.Background(), podName, metav1.GetOptions{})
	if err != nil {
		if apierrors.IsNotFound(err) {
			return "NotFound", nil
		}
		return "", fmt.Errorf("failed to get pod %s/%s: %w", namespace, podName, err)
	}

	// 按照 Kubelet manager 代码的构造方式生成 device 的唯一 ID
	expectedResourceID := v1.ResourceID(fmt.Sprintf("%s/%s/%s", driverName, poolName, deviceName))

	expectedResourceStatusNameSimple := v1.ResourceName(fmt.Sprintf("claim:%s", claimName))
	expectedResourceStatusNameWithRequest := v1.ResourceName(fmt.Sprintf("claim:%s/%s", claimName, "my-request"))

	// 遍历 container statuses
	for _, containerStatus := range pod.Status.ContainerStatuses {
		if containerStatus.AllocatedResourcesStatus != nil {
			for _, resourceStatus := range containerStatus.AllocatedResourcesStatus {
				if resourceStatus.Name != expectedResourceStatusNameSimple && resourceStatus.Name != expectedResourceStatusNameWithRequest {
					continue
				}
				for _, resourceHealth := range resourceStatus.Resources {
					if resourceHealth.ResourceID == expectedResourceID || strings.HasPrefix(string(resourceHealth.ResourceID), driverName) {
						return string(resourceHealth.Health), nil
					}
				}
			}
		}
	}

	return "NotFound", nil
}

// createHealthTestPodAndClaim 是 Resource Health 测试的专用辅助函数。
// 它创建所有必需对象（DeviceClass、ResourceClaim、Pod），并确保 pod 长时间运行、
// claim 从指定 pool 分配。
func createHealthTestPodAndClaim(ctx context.Context, f *framework.Framework, driverName, podName, claimName, className, poolName, deviceName string) *v1.Pod {
	ginkgo.By(fmt.Sprintf("Creating DeviceClass %q", className))
	dc := &resourceapi.DeviceClass{
		ObjectMeta: metav1.ObjectMeta{
			Name: className,
		},
	}
	_, err := f.ClientSet.ResourceV1().DeviceClasses().Create(ctx, dc, metav1.CreateOptions{})
	framework.ExpectNoError(err, "failed to create DeviceClass "+className)
	ginkgo.DeferCleanup(func(ctx context.Context) {
		err := f.ClientSet.ResourceV1().DeviceClasses().Delete(ctx, className, metav1.DeleteOptions{})
		if err != nil && !apierrors.IsNotFound(err) {
			framework.Failf("Failed to delete DeviceClass %s: %v", className, err)
		}
	})
	ginkgo.By(fmt.Sprintf("Creating ResourceClaim %q", claimName))
	claim := &resourceapi.ResourceClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name: claimName,
		},
		Spec: resourceapi.ResourceClaimSpec{
			Devices: resourceapi.DeviceClaim{
				Requests: []resourceapi.DeviceRequest{{
					Name: "my-request",
					Exactly: &resourceapi.ExactDeviceRequest{
						DeviceClassName: className,
					},
				}},
			},
		},
	}

	_, err = f.ClientSet.ResourceV1().ResourceClaims(f.Namespace.Name).Create(ctx, claim, metav1.CreateOptions{})
	framework.ExpectNoError(err, "failed to create ResourceClaim "+claimName)
	ginkgo.DeferCleanup(func(ctx context.Context) {
		err := f.ClientSet.ResourceV1().ResourceClaims(f.Namespace.Name).Delete(ctx, claimName, metav1.DeleteOptions{})
		if err != nil && !apierrors.IsNotFound(err) {
			framework.Failf("Failed to delete ResourceClaim %s: %v", claimName, err)
		}
	})
	ginkgo.By(fmt.Sprintf("Creating long-running Pod %q (without claim allocation yet)", podName))
	pod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      podName,
			Namespace: f.Namespace.Name,
		},
		Spec: v1.PodSpec{
			NodeName:      getNodeName(ctx, f),
			RestartPolicy: v1.RestartPolicyNever,
			ResourceClaims: []v1.PodResourceClaim{
				{Name: claimName, ResourceClaimName: &claimName},
			},
			Containers: []v1.Container{
				{
					Name:    "testcontainer",
					Image:   e2epod.GetDefaultTestImage(),
					Command: []string{"/bin/sh", "-c", "sleep 600"},
					Resources: v1.ResourceRequirements{
						Claims: []v1.ResourceClaim{{Name: claimName, Request: "my-request"}},
					},
				},
			},
		},
	}
	// 在 API server 上创建 pod，以获得真实 UID
	createdPod, err := f.ClientSet.CoreV1().Pods(f.Namespace.Name).Create(ctx, pod, metav1.CreateOptions{})
	framework.ExpectNoError(err, "failed to create Pod "+podName)
	ginkgo.DeferCleanup(func(ctx context.Context) {
		e2epod.DeletePodOrFail(ctx, f.ClientSet, createdPod.Namespace, createdPod.Name)
	})

	// patch Pod status 加入 ResourceClaimStatuses，模拟 scheduler 的行为
	patch, err := json.Marshal(v1.Pod{
		Status: v1.PodStatus{
			ResourceClaimStatuses: []v1.PodResourceClaimStatus{
				{Name: claimName, ResourceClaimName: &claimName},
			},
		},
	})
	framework.ExpectNoError(err, "failed to marshal patch for Pod status")
	_, err = f.ClientSet.CoreV1().Pods(f.Namespace.Name).Patch(ctx, createdPod.Name, apitypes.StrategicMergePatchType, patch, metav1.PatchOptions{}, "status")
	framework.ExpectNoError(err, "failed to patch Pod status with ResourceClaimStatuses")

	ginkgo.By(fmt.Sprintf("Allocating claim %q to pod %q with its real UID", claimName, podName))
	// 获取刚创建的 claim，确保更新时拿到最新版本
	claimToUpdate, err := f.ClientSet.ResourceV1().ResourceClaims(f.Namespace.Name).Get(ctx, claimName, metav1.GetOptions{})
	framework.ExpectNoError(err, "failed to get latest version of ResourceClaim "+claimName)

	// 更新 claim status，将其 ReserveFor 为 *真实* 的 pod UID
	claimToUpdate.Status = resourceapi.ResourceClaimStatus{
		ReservedFor: []resourceapi.ResourceClaimConsumerReference{
			{Resource: "pods", Name: createdPod.Name, UID: createdPod.UID},
		},
		Allocation: &resourceapi.AllocationResult{
			Devices: resourceapi.DeviceAllocationResult{
				Results: []resourceapi.DeviceRequestAllocationResult{
					{
						Driver:  driverName,
						Pool:    poolName,
						Device:  deviceName,
						Request: "my-request",
					},
				},
			},
		},
	}
	_, err = f.ClientSet.ResourceV1().ResourceClaims(f.Namespace.Name).UpdateStatus(ctx, claimToUpdate, metav1.UpdateOptions{})
	framework.ExpectNoError(err, "failed to update ResourceClaim status for test")

	return createdPod
}

// setupAndVerifyHealthyPod 创建带健康监控的测试 pod，并验证其达到 healthy 状态。
// 它封装了健康监控测试中反复出现的流程：创建 pod、等待 NodePrepareResources、
// 启动 pod、验证健康状态。
func setupAndVerifyHealthyPod(
	ctx context.Context,
	f *framework.Framework,
	plugin *testdriver.ExamplePlugin,
	driverName string,
	className string,
	claimName string,
	podName string,
	poolName string,
	deviceName string,
) (*v1.Pod, string, string, string) {
	pod := createHealthTestPodAndClaim(ctx, f, driverName, podName, claimName, className, poolName, deviceName)

	ginkgo.By("wait for NodePrepareResources call to succeed")
	gomega.Eventually(plugin.GetGRPCCalls).WithTimeout(retryTestTimeout).Should(testdrivergomega.NodePrepareResourcesSucceeded)

	ginkgo.By("Waiting for the pod to be running")
	framework.ExpectNoError(e2epod.WaitForPodRunningInNamespace(ctx, f.ClientSet, pod))

	ginkgo.By("Forcing a 'Healthy' status update to establish a baseline")
	plugin.HealthControlChan <- testdriver.DeviceHealthUpdate{
		PoolName:   poolName,
		DeviceName: deviceName,
		Health:     "Healthy",
	}

	ginkgo.By("Verifying device health is now Healthy in the pod status")
	gomega.Eventually(ctx, func(ctx context.Context) (string, error) {
		return getDeviceHealthFromAPIServer(f, pod.Namespace, pod.Name, driverName, claimName, poolName, deviceName)
	}).WithTimeout(30*time.Second).WithPolling(1*time.Second).Should(gomega.Equal("Healthy"), "Device health should be Healthy after explicit update")

	return pod, claimName, poolName, deviceName
}

// createTestObjectsWithShareID 创建测试 ShareID 与健康状态集成的对象
// (DeviceClass、ResourceClaim、Pod)，ShareID 包含在 device allocation result 中
func createTestObjectsWithShareID(ctx context.Context, f *framework.Framework, driverName, className, claimName, podName, poolName, deviceName string, shareID *apitypes.UID) *v1.Pod {
	ginkgo.By(fmt.Sprintf("Creating DeviceClass %q", className))
	dc := &resourceapi.DeviceClass{
		ObjectMeta: metav1.ObjectMeta{
			Name: className,
		},
	}
	_, err := f.ClientSet.ResourceV1().DeviceClasses().Create(ctx, dc, metav1.CreateOptions{})
	framework.ExpectNoError(err, "failed to create DeviceClass "+className)
	ginkgo.DeferCleanup(func(ctx context.Context) {
		err := f.ClientSet.ResourceV1().DeviceClasses().Delete(ctx, className, metav1.DeleteOptions{})
		if err != nil && !apierrors.IsNotFound(err) {
			framework.Failf("Failed to delete DeviceClass %s: %v", className, err)
		}
	})

	ginkgo.By(fmt.Sprintf("Creating ResourceClaim %q", claimName))
	claim := &resourceapi.ResourceClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name: claimName,
		},
		Spec: resourceapi.ResourceClaimSpec{
			Devices: resourceapi.DeviceClaim{
				Requests: []resourceapi.DeviceRequest{{
					Name: "my-request",
					Exactly: &resourceapi.ExactDeviceRequest{
						DeviceClassName: className,
					},
				}},
			},
		},
	}

	_, err = f.ClientSet.ResourceV1().ResourceClaims(f.Namespace.Name).Create(ctx, claim, metav1.CreateOptions{})
	framework.ExpectNoError(err, "failed to create ResourceClaim "+claimName)
	ginkgo.DeferCleanup(func(ctx context.Context) {
		err := f.ClientSet.ResourceV1().ResourceClaims(f.Namespace.Name).Delete(ctx, claimName, metav1.DeleteOptions{})
		if err != nil && !apierrors.IsNotFound(err) {
			framework.Failf("Failed to delete ResourceClaim %s: %v", claimName, err)
		}
	})

	ginkgo.By(fmt.Sprintf("Creating long-running Pod %q", podName))
	pod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      podName,
			Namespace: f.Namespace.Name,
		},
		Spec: v1.PodSpec{
			NodeName:      getNodeName(ctx, f),
			RestartPolicy: v1.RestartPolicyNever,
			ResourceClaims: []v1.PodResourceClaim{
				{Name: claimName, ResourceClaimName: &claimName},
			},
			Containers: []v1.Container{
				{
					Name:    "testcontainer",
					Image:   e2epod.GetDefaultTestImage(),
					Command: []string{"/bin/sh", "-c", "sleep 600"},
					Resources: v1.ResourceRequirements{
						Claims: []v1.ResourceClaim{{Name: claimName, Request: "my-request"}},
					},
				},
			},
		},
	}

	createdPod, err := f.ClientSet.CoreV1().Pods(f.Namespace.Name).Create(ctx, pod, metav1.CreateOptions{})
	framework.ExpectNoError(err, "failed to create Pod "+podName)
	ginkgo.DeferCleanup(func(ctx context.Context) {
		e2epod.DeletePodOrFail(ctx, f.ClientSet, createdPod.Namespace, createdPod.Name)
	})

	// patch Pod status 加入 ResourceClaimStatuses
	patch, err := json.Marshal(v1.Pod{
		Status: v1.PodStatus{
			ResourceClaimStatuses: []v1.PodResourceClaimStatus{
				{Name: claimName, ResourceClaimName: &claimName},
			},
		},
	})
	framework.ExpectNoError(err, "failed to marshal patch for Pod status")
	_, err = f.ClientSet.CoreV1().Pods(f.Namespace.Name).Patch(ctx, createdPod.Name, apitypes.StrategicMergePatchType, patch, metav1.PatchOptions{}, "status")
	framework.ExpectNoError(err, "failed to patch Pod status with ResourceClaimStatuses")

	ginkgo.By(fmt.Sprintf("Allocating claim %q with ShareID", claimName))
	claimToUpdate, err := f.ClientSet.ResourceV1().ResourceClaims(f.Namespace.Name).Get(ctx, claimName, metav1.GetOptions{})
	framework.ExpectNoError(err, "failed to get latest version of ResourceClaim "+claimName)

	// 更新 claim status，在 allocation result 中包含 ShareID
	claimToUpdate.Status = resourceapi.ResourceClaimStatus{
		ReservedFor: []resourceapi.ResourceClaimConsumerReference{
			{Resource: "pods", Name: createdPod.Name, UID: createdPod.UID},
		},
		Allocation: &resourceapi.AllocationResult{
			Devices: resourceapi.DeviceAllocationResult{
				Results: []resourceapi.DeviceRequestAllocationResult{
					{
						Driver:  driverName,
						Pool:    poolName,
						Device:  deviceName,
						Request: "my-request",
						ShareID: shareID, // 在 allocation 中包含 ShareID
					},
				},
			},
		},
	}
	_, err = f.ClientSet.ResourceV1().ResourceClaims(f.Namespace.Name).UpdateStatus(ctx, claimToUpdate, metav1.UpdateOptions{})
	framework.ExpectNoError(err, "failed to update ResourceClaim status with ShareID")

	return createdPod
}

// errorOnCloseListener 是 mock net.Listener，Accept() 会一直阻塞直到 Close() 被调用，
// 随后 Accept() 返回预设错误。
//
// 这在测试或模拟环境中很有用：无需真实网络活动即可让 grpc.Server.Serve()
// 带着已知错误干净退出。
type errorOnCloseListener struct {
	ch     chan struct{}
	closed atomic.Bool
	err    error
}

// newErrorOnCloseListener 创建一个 listener，Close 被调用后 Accept 会返回指定错误
func newErrorOnCloseListener(err error) *errorOnCloseListener {
	return &errorOnCloseListener{
		ch:  make(chan struct{}),
		err: err,
	}
}

// Accept 一直阻塞到 Close 被调用，然后返回配置的错误
func (l *errorOnCloseListener) Accept() (net.Conn, error) {
	<-l.ch
	return nil, l.err
}

// Close 解除 Accept 的阻塞，使其返回配置的错误。可安全重复调用
func (l *errorOnCloseListener) Close() error {
	if l.closed.Swap(true) {
		return nil // 已关闭
	}
	close(l.ch)
	return nil
}

// Addr 返回占位的 Unix 地址，仅用于满足 net.Listener 接口
func (*errorOnCloseListener) Addr() net.Addr {
	return &net.UnixAddr{Name: "errorOnCloseListener", Net: "unix"}
}

// driverNameLabelKey 将 metric sample 映射到其 driver_name label 值。
// 与 gstruct.MatchAllElements 配合使用
func driverNameLabelKey(element any) string {
	el := element.(*testutil.Sample)
	return string(el.Metric[testutil.LabelName("driver_name")])
}

// countingListener 是 net.Listener 的包装，统计 Accept 被调用的次数。
// 它嵌入 net.Listener，并用 atomic 计数器安全记录 accept 操作次数
type countingListener struct {
	net.Listener
	acceptCalls atomic.Int32
}

// newCountingListener 创建 Unix domain socket listener，
// 并用 countingListener 包装以统计接受的连接数
func newCountingListener(socketPath string) (*countingListener, error) {
	addr, err := net.ResolveUnixAddr("unix", socketPath)
	if err != nil {
		return nil, err
	}

	l, err := net.ListenUnix("unix", addr)
	if err != nil {
		return nil, err
	}

	return &countingListener{Listener: l}, nil
}

// Accept 等待并返回接受的连接，成功时递增 acceptCalls 计数器
func (l *countingListener) Accept() (net.Conn, error) {
	conn, err := l.Listener.Accept()
	if err == nil {
		l.acceptCalls.Add(1)
	}
	return conn, err
}

// acceptCount 返回 listener 上 accept 方法被调用的次数
func (l *countingListener) acceptCount() int {
	return int(l.acceptCalls.Load())
}
