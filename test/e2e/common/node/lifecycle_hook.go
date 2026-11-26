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

package node

import (
	"context"
	"fmt"
	"net/url"
	"strings"
	"time"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	utilrand "k8s.io/apimachinery/pkg/util/rand"
	"k8s.io/kubernetes/pkg/features"
	"k8s.io/kubernetes/test/e2e/feature"
	"k8s.io/kubernetes/test/e2e/framework"
	e2enode "k8s.io/kubernetes/test/e2e/framework/node"
	e2epod "k8s.io/kubernetes/test/e2e/framework/pod"
	imageutils "k8s.io/kubernetes/test/utils/image"
	admissionapi "k8s.io/pod-security-admission/api"
	"k8s.io/utils/ptr"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
)

var _ = SIGDescribe("Container Lifecycle Hook", func() {
	f := framework.NewDefaultFramework("container-lifecycle-hook")
	f.NamespacePodSecurityLevel = admissionapi.LevelBaseline
	var podClient *e2epod.PodClient
	const (
		podCheckInterval     = 1 * time.Second
		postStartWaitTimeout = 2 * time.Minute
		preStopWaitTimeout   = 30 * time.Second
	)
	ginkgo.Context("when create a pod with lifecycle hook", func() {
		var (
			targetIP, targetURL, targetNode string
		)

		// 通过创建 HTTP server 并查看其日志来测试探针，
		// 确认它收到了预期的探测请求。但这里有个难点：
		// 测试 PostStart 时，HTTP server 不能是
		// 我们正在启动的容器，因为 PostStart 可能在容器启动前运行。
		// 可以用 sidecar 容器解决，但对 PreStop 不行，
		// 因为我们无法可靠保证 sidecar 在 Delete pod 后
		// 还能存活足够久，让我们有机会
		// 读取日志。
		//
		// 为了让两种情况都成立，我们运行一个独立的 pod 负责记录日志，
		// 再在测试 pod 中用 sidecar 容器接收发往
		// 测试 pod IP 的探测请求，并通过向日志
		// pod 发起请求来处理它们。
		//
		// （Exec 场景简单得多，直接让它 curl
		// 日志 pod 即可。）
		podHandleHookRequest := e2epod.NewAgnhostPodFromContainers(
			"", "pod-handle-http-request", nil,
			e2epod.NewAgnhostContainer("container-handle-http-request", nil, nil, "netexec"),
		)
		// netexecConnect 接受 target 和 request，返回一个 netexec
		// 命令，该命令会连接 `target` 并请求 `req`。
		netexecConnect := func(target, req string) string {
			return fmt.Sprintf("/dial?host=%s&port=8080&request=%s",
				target,
				url.QueryEscape(strings.TrimLeft(req, "/")),
			)
		}

		ginkgo.BeforeEach(func(ctx context.Context) {
			node, err := e2enode.GetRandomReadySchedulableNode(ctx, f.ClientSet)
			framework.ExpectNoError(err)
			targetNode = node.Name
			nodeSelection := e2epod.NodeSelection{}
			e2epod.SetAffinity(&nodeSelection, targetNode)
			e2epod.SetNodeSelection(&podHandleHookRequest.Spec, nodeSelection)

			podClient = e2epod.NewPodClient(f)
			ginkgo.By("create the container to handle the HTTPGet hook request.")
			newPod := podClient.CreateSync(ctx, podHandleHookRequest)
			targetIP = newPod.Status.PodIP
			targetURL = targetIP
			if strings.Contains(targetIP, ":") {
				targetURL = fmt.Sprintf("[%s]", targetIP)
			}
		})
		testPodWithHook := func(ctx context.Context, podWithHook *v1.Pod) {
			testContainer := podWithHook.Spec.Containers[0]
			scheme := v1.URISchemeHTTP
			if testContainer.Lifecycle.PostStart != nil && testContainer.Lifecycle.PostStart.HTTPGet != nil {
				scheme = testContainer.Lifecycle.PostStart.HTTPGet.Scheme
			} else if testContainer.Lifecycle.PreStop != nil && testContainer.Lifecycle.PreStop.HTTPGet != nil {
				scheme = testContainer.Lifecycle.PreStop.HTTPGet.Scheme
			}

			ginkgo.By("add a sidecar to the test pod")
			sidecarArgs := []string{
				"netexec",
				"--udp-port", "-1",
			}
			if scheme == v1.URISchemeHTTPS {
				sidecarArgs = append(sidecarArgs,
					"--tls-cert-file", "/localhost.crt",
					"--tls-private-key-file", "/localhost.key",
				)
			}
			sidecarContainer := e2epod.NewAgnhostContainer("container-redirect", nil, nil, sidecarArgs...)
			sidecarContainer.RestartPolicy = ptr.To(v1.ContainerRestartPolicyAlways)
			sidecarContainer.StartupProbe = &v1.Probe{
				ProbeHandler: v1.ProbeHandler{
					HTTPGet: &v1.HTTPGetAction{
						Scheme: scheme,
						Path:   "/echo?msg=startup",
						Port:   intstr.FromInt32(8080),
					},
				},
			}
			podWithHook.Spec.InitContainers = append(
				[]v1.Container{sidecarContainer},
				podWithHook.Spec.InitContainers...,
			)

			ginkgo.By("create the pod with lifecycle hook")
			podClient.CreateSync(ctx, podWithHook)
			if podWithHook.Spec.Containers[0].Lifecycle.PostStart != nil {
				ginkgo.By("check poststart hook")
				gomega.Eventually(ctx, func(ctx context.Context) error {
					return podClient.MatchContainerOutput(ctx, podHandleHookRequest.Name, podHandleHookRequest.Spec.Containers[0].Name,
						`GET /echo\?msg=poststart`)
				}, postStartWaitTimeout, podCheckInterval).Should(gomega.BeNil())
			}
			ginkgo.By("delete the pod with lifecycle hook")
			podClient.DeleteSync(ctx, podWithHook.Name, *metav1.NewDeleteOptions(15), f.Timeouts.PodDelete)
			if podWithHook.Spec.Containers[0].Lifecycle.PreStop != nil {
				ginkgo.By("check prestop hook")
				gomega.Eventually(ctx, func(ctx context.Context) error {
					return podClient.MatchContainerOutput(ctx, podHandleHookRequest.Name, podHandleHookRequest.Spec.Containers[0].Name,
						`GET /echo\?msg=prestop`)
				}, preStopWaitTimeout, podCheckInterval).Should(gomega.BeNil())
			}
		}
		/*
			发布版本：v1.9
			测试名称：Pod Lifecycle, post start exec hook
			描述：当容器生命周期中通过 'Exec' action 指定 post start handler 时，handler 必须在容器启动后被调用。创建一个提供 http 请求的 server pod，再创建第二个带容器生命周期的 pod，其 post start 通过 ExecAction 调用 server pod，以验证 post start 已执行。
		*/
		framework.ConformanceIt("should execute poststart exec hook properly", f.WithNodeConformance(), func(ctx context.Context) {
			lifecycle := &v1.Lifecycle{
				PostStart: &v1.LifecycleHandler{
					Exec: &v1.ExecAction{
						Command: []string{"sh", "-c", "curl http://" + targetURL + ":8080/echo?msg=poststart"},
					},
				},
			}
			podWithHook := getPodWithHook("pod-with-poststart-exec-hook", imageutils.GetE2EImage(imageutils.Agnhost), lifecycle)

			testPodWithHook(ctx, podWithHook)
		})
		/*
			发布版本：v1.9
			测试名称：Pod Lifecycle, prestop exec hook
			描述：当容器生命周期中通过 'Exec' action 指定 pre-stop handler 时，handler 必须在容器终止前被调用。创建一个提供 http 请求的 server pod，再创建第二个带容器生命周期的 pod，其 pre-stop 通过 ExecAction 调用 server pod，以验证 pre-stop 已执行。
		*/
		framework.ConformanceIt("should execute prestop exec hook properly", f.WithNodeConformance(), func(ctx context.Context) {
			lifecycle := &v1.Lifecycle{
				PreStop: &v1.LifecycleHandler{
					Exec: &v1.ExecAction{
						Command: []string{"sh", "-c", "curl http://" + targetURL + ":8080/echo?msg=prestop"},
					},
				},
			}
			podWithHook := getPodWithHook("pod-with-prestop-exec-hook", imageutils.GetE2EImage(imageutils.Agnhost), lifecycle)
			testPodWithHook(ctx, podWithHook)
		})
		/*
			发布版本：v1.9
			测试名称：Pod Lifecycle, post start http hook
			描述：当容器生命周期中通过 HttpGet action 指定 post start handler 时，handler 必须在容器启动后被调用。创建一个提供 http 请求的 server pod，在同一节点上创建第二个带容器生命周期的 pod，其 post start 调用 server pod，以验证 post start 已执行。
		*/
		framework.ConformanceIt("should execute poststart http hook properly", f.WithNodeConformance(), func(ctx context.Context) {
			lifecycle := &v1.Lifecycle{
				PostStart: &v1.LifecycleHandler{
					HTTPGet: &v1.HTTPGetAction{
						Path: netexecConnect(targetIP, "/echo?msg=poststart"),
						Port: intstr.FromInt32(8080),
					},
				},
			}
			podWithHook := getPodWithHook("pod-with-poststart-http-hook", imageutils.GetPauseImageName(), lifecycle)
			// 确保测试 pod 与 webserver 在同一节点上创建。
			nodeSelection := e2epod.NodeSelection{}
			e2epod.SetAffinity(&nodeSelection, targetNode)
			e2epod.SetNodeSelection(&podWithHook.Spec, nodeSelection)
			testPodWithHook(ctx, podWithHook)
		})
		/*
			发布版本：v1.23
			测试名称：Pod Lifecycle, poststart https hook
			描述：当容器生命周期中通过 'HttpGet' action 指定 post-start handler 时，handler 必须在容器终止前被调用。创建一个提供 https 请求的 server pod，在同一节点上创建第二个带容器生命周期的 pod，其 post-start 调用 server pod，以验证 post-start 已执行。
		*/
		f.It("should execute poststart https hook properly", f.WithNodeConformance(), func(ctx context.Context) {
			lifecycle := &v1.Lifecycle{
				PostStart: &v1.LifecycleHandler{
					HTTPGet: &v1.HTTPGetAction{
						Scheme: v1.URISchemeHTTPS,
						Path:   netexecConnect(targetIP, "/echo?msg=poststart"),
						Port:   intstr.FromInt32(8080),
					},
				},
			}
			podWithHook := getPodWithHook("pod-with-poststart-https-hook", imageutils.GetPauseImageName(), lifecycle)
			// 确保测试 pod 与 webserver 在同一节点上创建。
			nodeSelection := e2epod.NodeSelection{}
			e2epod.SetAffinity(&nodeSelection, targetNode)
			e2epod.SetNodeSelection(&podWithHook.Spec, nodeSelection)
			testPodWithHook(ctx, podWithHook)
		})
		/*
			发布版本：v1.9
			测试名称：Pod Lifecycle, prestop http hook
			描述：当容器生命周期中通过 'HttpGet' action 指定 pre-stop handler 时，handler 必须在容器终止前被调用。创建一个提供 http 请求的 server pod，在同一节点上创建第二个带容器生命周期的 pod，其 pre-stop 调用 server pod，以验证 pre-stop 已执行。
		*/
		framework.ConformanceIt("should execute prestop http hook properly", f.WithNodeConformance(), func(ctx context.Context) {
			lifecycle := &v1.Lifecycle{
				PreStop: &v1.LifecycleHandler{
					HTTPGet: &v1.HTTPGetAction{
						Path: netexecConnect(targetIP, "/echo?msg=prestop"),
						Port: intstr.FromInt32(8080),
					},
				},
			}
			podWithHook := getPodWithHook("pod-with-prestop-http-hook", imageutils.GetPauseImageName(), lifecycle)
			// 确保测试 pod 与 webserver 在同一节点上创建。
			nodeSelection := e2epod.NodeSelection{}
			e2epod.SetAffinity(&nodeSelection, targetNode)
			e2epod.SetNodeSelection(&podWithHook.Spec, nodeSelection)
			testPodWithHook(ctx, podWithHook)
		})
		/*
			发布版本：v1.23
			测试名称：Pod Lifecycle, prestop https hook
			描述：当容器生命周期中通过 'HttpGet' action 指定 pre-stop handler 时，handler 必须在容器终止前被调用。创建一个提供 https 请求的 server pod，在同一节点上创建第二个带容器生命周期的 pod，其 pre-stop 调用 server pod，以验证 pre-stop 已执行。
		*/
		f.It("should execute prestop https hook properly", f.WithNodeConformance(), func(ctx context.Context) {
			lifecycle := &v1.Lifecycle{
				PreStop: &v1.LifecycleHandler{
					HTTPGet: &v1.HTTPGetAction{
						Scheme: v1.URISchemeHTTPS,
						Path:   netexecConnect(targetIP, "/echo?msg=prestop"),
						Port:   intstr.FromInt32(8080),
					},
				},
			}
			podWithHook := getPodWithHook("pod-with-prestop-https-hook", imageutils.GetPauseImageName(), lifecycle)
			// 确保测试 pod 与 webserver 在同一节点上创建。
			nodeSelection := e2epod.NodeSelection{}
			e2epod.SetAffinity(&nodeSelection, targetNode)
			e2epod.SetNodeSelection(&podWithHook.Spec, nodeSelection)
			testPodWithHook(ctx, podWithHook)
		})
	})
})

var _ = SIGDescribe(framework.WithNodeConformance(), "Restartable Init Container Lifecycle Hook", func() {
	f := framework.NewDefaultFramework("restartable-init-container-lifecycle-hook")
	f.NamespacePodSecurityLevel = admissionapi.LevelBaseline
	var podClient *e2epod.PodClient
	const (
		podCheckInterval     = 1 * time.Second
		postStartWaitTimeout = 2 * time.Minute
		preStopWaitTimeout   = 30 * time.Second
	)
	ginkgo.Context("when create a pod with lifecycle hook", func() {
		var (
			targetIP, targetURL, targetNode string
		)

		// 参见上文 "Container Lifecycle Hook" 测试中的说明
		podHandleHookRequest := e2epod.NewAgnhostPodFromContainers(
			"", "pod-handle-http-request", nil,
			e2epod.NewAgnhostContainer("container-handle-http-request", nil, nil, "netexec"),
		)
		netexecConnect := func(target, req string) string {
			return fmt.Sprintf("/dial?host=%s&port=8080&request=%s",
				target,
				url.QueryEscape(strings.TrimLeft(req, "/")),
			)
		}

		ginkgo.BeforeEach(func(ctx context.Context) {
			node, err := e2enode.GetRandomReadySchedulableNode(ctx, f.ClientSet)
			framework.ExpectNoError(err)
			targetNode = node.Name
			nodeSelection := e2epod.NodeSelection{}
			e2epod.SetAffinity(&nodeSelection, targetNode)
			e2epod.SetNodeSelection(&podHandleHookRequest.Spec, nodeSelection)

			podClient = e2epod.NewPodClient(f)
			ginkgo.By("create the container to handle the HTTPGet hook request.")
			newPod := podClient.CreateSync(ctx, podHandleHookRequest)
			targetIP = newPod.Status.PodIP
			targetURL = targetIP
			if strings.Contains(targetIP, ":") {
				targetURL = fmt.Sprintf("[%s]", targetIP)
			}
		})
		testPodWithHook := func(ctx context.Context, podWithHook *v1.Pod) {
			testContainer := podWithHook.Spec.InitContainers[0]
			scheme := v1.URISchemeHTTP
			if testContainer.Lifecycle.PostStart != nil && testContainer.Lifecycle.PostStart.HTTPGet != nil {
				scheme = testContainer.Lifecycle.PostStart.HTTPGet.Scheme
			} else if testContainer.Lifecycle.PreStop != nil && testContainer.Lifecycle.PreStop.HTTPGet != nil {
				scheme = testContainer.Lifecycle.PreStop.HTTPGet.Scheme
			}

			ginkgo.By("add a sidecar to the test pod")
			sidecarArgs := []string{
				"netexec",
				"--udp-port", "-1",
			}
			if scheme == v1.URISchemeHTTPS {
				sidecarArgs = append(sidecarArgs,
					"--tls-cert-file", "/localhost.crt",
					"--tls-private-key-file", "/localhost.key",
				)
			}
			sidecarContainer := e2epod.NewAgnhostContainer("container-redirect", nil, nil, sidecarArgs...)
			sidecarContainer.RestartPolicy = ptr.To(v1.ContainerRestartPolicyAlways)
			sidecarContainer.StartupProbe = &v1.Probe{
				ProbeHandler: v1.ProbeHandler{
					HTTPGet: &v1.HTTPGetAction{
						Scheme: scheme,
						Path:   "/echo?msg=startup",
						Port:   intstr.FromInt32(8080),
					},
				},
			}
			podWithHook.Spec.InitContainers = append(
				[]v1.Container{sidecarContainer},
				podWithHook.Spec.InitContainers...,
			)

			ginkgo.By("create the pod with lifecycle hook")
			podClient.CreateSync(ctx, podWithHook)
			if testContainer.Lifecycle.PostStart != nil {
				ginkgo.By("check poststart hook")
				gomega.Eventually(ctx, func(ctx context.Context) error {
					return podClient.MatchContainerOutput(ctx, podHandleHookRequest.Name, podHandleHookRequest.Spec.Containers[0].Name,
						`GET /echo\?msg=poststart`)
				}, postStartWaitTimeout, podCheckInterval).Should(gomega.BeNil())
			}
			ginkgo.By("delete the pod with lifecycle hook")
			podClient.DeleteSync(ctx, podWithHook.Name, *metav1.NewDeleteOptions(15), f.Timeouts.PodDelete)
			if testContainer.Lifecycle.PreStop != nil {
				ginkgo.By("check prestop hook")
				gomega.Eventually(ctx, func(ctx context.Context) error {
					return podClient.MatchContainerOutput(ctx, podHandleHookRequest.Name, podHandleHookRequest.Spec.Containers[0].Name,
						`GET /echo\?msg=prestop`)
				}, preStopWaitTimeout, podCheckInterval).Should(gomega.BeNil())
			}
		}
		/*
			发布版本：v1.28
			测试名称：Pod Lifecycle with restartable init container, post start exec hook
			描述：当容器生命周期中通过 'Exec' action 指定
			post start handler 时，handler 必须在
			容器启动后被调用。创建一个提供 http
			请求的 server pod，再创建第二个带容器生命周期的
			pod，其 post start 通过 ExecAction 调用 server pod，
			以验证 post start 已执行。
		*/
		ginkgo.It("should execute poststart exec hook properly", func(ctx context.Context) {
			lifecycle := &v1.Lifecycle{
				PostStart: &v1.LifecycleHandler{
					Exec: &v1.ExecAction{
						Command: []string{"sh", "-c", "curl http://" + targetURL + ":8080/echo?msg=poststart"},
					},
				},
			}
			podWithHook := getSidecarPodWithHook("pod-with-poststart-exec-hook", imageutils.GetE2EImage(imageutils.Agnhost), lifecycle)

			testPodWithHook(ctx, podWithHook)
		})
		/*
			发布版本：v1.28
			测试名称：Pod Lifecycle with restartable init container, prestop exec hook
			描述：当容器生命周期中通过 'Exec' action 指定
			pre-stop handler 时，handler 必须在
			容器终止前被调用。创建一个提供 http
			请求的 server pod，再创建第二个带容器生命周期的
			pod，其 pre-stop 通过 ExecAction 调用 server pod，
			以验证 pre-stop 已执行。
		*/
		ginkgo.It("should execute prestop exec hook properly", func(ctx context.Context) {
			lifecycle := &v1.Lifecycle{
				PreStop: &v1.LifecycleHandler{
					Exec: &v1.ExecAction{
						Command: []string{"sh", "-c", "curl http://" + targetURL + ":8080/echo?msg=prestop"},
					},
				},
			}
			podWithHook := getSidecarPodWithHook("pod-with-prestop-exec-hook", imageutils.GetE2EImage(imageutils.Agnhost), lifecycle)
			testPodWithHook(ctx, podWithHook)
		})
		/*
			发布版本：v1.28
			测试名称：Pod Lifecycle with restartable init container, post start http hook
			描述：当容器生命周期中通过 HttpGet action 指定
			post start handler 时，handler 必须在
			容器启动后被调用。创建一个提供 http
			请求的 server pod，在同一节点上创建第二个带容器
			生命周期的 pod，其 post start 调用 server pod，
			以验证 post start 已执行。
		*/
		ginkgo.It("should execute poststart http hook properly", func(ctx context.Context) {
			lifecycle := &v1.Lifecycle{
				PostStart: &v1.LifecycleHandler{
					HTTPGet: &v1.HTTPGetAction{
						Path: netexecConnect(targetIP, "/echo?msg=poststart"),
						Port: intstr.FromInt32(8080),
					},
				},
			}
			podWithHook := getSidecarPodWithHook("pod-with-poststart-http-hook", imageutils.GetPauseImageName(), lifecycle)
			// 确保测试 pod 与 webserver 在同一节点上创建。
			nodeSelection := e2epod.NodeSelection{}
			e2epod.SetAffinity(&nodeSelection, targetNode)
			e2epod.SetNodeSelection(&podWithHook.Spec, nodeSelection)
			testPodWithHook(ctx, podWithHook)
		})
		/*
			发布版本：v1.28
			测试名称：Pod Lifecycle with restartable init container, poststart https hook
			描述：当容器生命周期中通过 'HttpGet' action 指定
			post-start handler 时，handler 必须在
			容器终止前被调用。创建一个提供 https
			请求的 server pod，在同一节点上创建第二个带
			容器生命周期的 pod，其 post-start 调用 server pod，
			以验证 post-start 已执行。
		*/
		ginkgo.It("should execute poststart https hook properly", func(ctx context.Context) {
			lifecycle := &v1.Lifecycle{
				PostStart: &v1.LifecycleHandler{
					HTTPGet: &v1.HTTPGetAction{
						Scheme: v1.URISchemeHTTPS,
						Path:   netexecConnect(targetIP, "/echo?msg=poststart"),
						Port:   intstr.FromInt32(8080),
					},
				},
			}
			podWithHook := getSidecarPodWithHook("pod-with-poststart-https-hook", imageutils.GetPauseImageName(), lifecycle)
			// 确保测试 pod 与 webserver 在同一节点上创建。
			nodeSelection := e2epod.NodeSelection{}
			e2epod.SetAffinity(&nodeSelection, targetNode)
			e2epod.SetNodeSelection(&podWithHook.Spec, nodeSelection)
			testPodWithHook(ctx, podWithHook)
		})
		/*
			发布版本：v1.28
			测试名称：Pod Lifecycle with restartable init container, prestop http hook
			描述：当容器生命周期中通过 'HttpGet' action 指定
			pre-stop handler 时，handler 必须在
			容器终止前被调用。创建一个提供 http
			请求的 server pod，在同一节点上创建第二个带
			容器生命周期的 pod，其 pre-stop 调用 server pod，
			以验证 pre-stop 已执行。
		*/
		ginkgo.It("should execute prestop http hook properly", func(ctx context.Context) {
			lifecycle := &v1.Lifecycle{
				PreStop: &v1.LifecycleHandler{
					HTTPGet: &v1.HTTPGetAction{
						Path: netexecConnect(targetIP, "/echo?msg=prestop"),
						Port: intstr.FromInt32(8080),
					},
				},
			}
			podWithHook := getSidecarPodWithHook("pod-with-prestop-http-hook", imageutils.GetPauseImageName(), lifecycle)
			// 确保测试 pod 与 webserver 在同一节点上创建。
			nodeSelection := e2epod.NodeSelection{}
			e2epod.SetAffinity(&nodeSelection, targetNode)
			e2epod.SetNodeSelection(&podWithHook.Spec, nodeSelection)
			testPodWithHook(ctx, podWithHook)
		})
		/*
			发布版本：v1.28
			测试名称：Pod Lifecycle with restartable init container, prestop https hook
			描述：当容器生命周期中通过 'HttpGet' action 指定
			pre-stop handler 时，handler 必须在
			容器终止前被调用。创建一个提供 https
			请求的 server pod，在同一节点上创建第二个带
			容器生命周期的 pod，其 pre-stop 调用 server pod，
			以验证 pre-stop 已执行。
		*/
		ginkgo.It("should execute prestop https hook properly", func(ctx context.Context) {
			lifecycle := &v1.Lifecycle{
				PreStop: &v1.LifecycleHandler{
					HTTPGet: &v1.HTTPGetAction{
						Scheme: v1.URISchemeHTTPS,
						Path:   netexecConnect(targetIP, "/echo?msg=prestop"),
						Port:   intstr.FromInt32(8080),
					},
				},
			}
			podWithHook := getSidecarPodWithHook("pod-with-prestop-https-hook", imageutils.GetPauseImageName(), lifecycle)
			// 确保测试 pod 与 webserver 在同一节点上创建。
			nodeSelection := e2epod.NodeSelection{}
			e2epod.SetAffinity(&nodeSelection, targetNode)
			e2epod.SetNodeSelection(&podWithHook.Spec, nodeSelection)
			testPodWithHook(ctx, podWithHook)
		})
	})
})

func getPodWithHook(name string, image string, lifecycle *v1.Lifecycle) *v1.Pod {
	return &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
		},
		Spec: v1.PodSpec{
			Containers: []v1.Container{
				{
					Name:      name,
					Image:     image,
					Lifecycle: lifecycle,
				},
			},
		},
	}
}

func getSidecarPodWithHook(name string, image string, lifecycle *v1.Lifecycle) *v1.Pod {
	return &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
		},
		Spec: v1.PodSpec{
			InitContainers: []v1.Container{
				{
					Name:      name,
					Image:     image,
					Lifecycle: lifecycle,
					RestartPolicy: func() *v1.ContainerRestartPolicy {
						restartPolicy := v1.ContainerRestartPolicyAlways
						return &restartPolicy
					}(),
				},
			},
			Containers: []v1.Container{
				{
					Name:  "main",
					Image: imageutils.GetPauseImageName(),
				},
			},
		},
	}
}

func validDuration(duration time.Duration, low, high int64) bool {
	return duration >= time.Second*time.Duration(low) && duration <= time.Second*time.Duration(high)
}

var _ = SIGDescribe("Lifecycle Sleep Hook", framework.WithNodeConformance(), func() {
	f := framework.NewDefaultFramework("pod-lifecycle-sleep-action")
	f.NamespacePodSecurityLevel = admissionapi.LevelBaseline
	var podClient *e2epod.PodClient

	ginkgo.Context("when create a pod with lifecycle hook using sleep action", func() {
		ginkgo.BeforeEach(func(ctx context.Context) {
			podClient = e2epod.NewPodClient(f)
		})

		var finalizer = "test/finalizer"
		/*
			发布版本：v1.34
			测试名称：Pod Lifecycle, prestop sleep hook
			描述：当容器生命周期中通过 'Sleep' action 指定 pre-stop handler 时，handler 必须在容器终止前被调用。创建一个测试 pod，验证其终止时间与终止时指定的 sleep 时间一致。
		*/
		ginkgo.It("valid prestop hook using sleep action", func(ctx context.Context) {
			const sleepSeconds = 50
			const gracePeriod = 100
			lifecycle := &v1.Lifecycle{
				PreStop: &v1.LifecycleHandler{
					Sleep: &v1.SleepAction{Seconds: sleepSeconds},
				},
			}
			name := "pod-with-prestop-sleep-hook"
			podWithHook := getPodWithHook(name, imageutils.GetPauseImageName(), lifecycle)
			podWithHook.Finalizers = append(podWithHook.Finalizers, finalizer)
			podWithHook.Spec.TerminationGracePeriodSeconds = ptr.To[int64](gracePeriod)
			ginkgo.By("create the pod with lifecycle hook using sleep action")
			p := podClient.CreateSync(ctx, podWithHook)
			defer podClient.RemoveFinalizer(ctx, name, finalizer)
			ginkgo.By("delete the pod with lifecycle hook using sleep action")
			_ = podClient.Delete(ctx, podWithHook.Name, metav1.DeleteOptions{})
			p, err := podClient.Get(ctx, p.Name, metav1.GetOptions{})
			if err != nil {
				framework.Failf("failed getting pod after deletion")
			}
			// deletionTimestamp 等于 delete_time + tgps
			// TODO: issues.k8s.io/132205 解决后减小 sleep_seconds 和 tgps
			// 由于 issues.k8s.io/132205，这里在容器变为 terminated 之前就拿到了 deletionTimestamp
			deletionTS := p.DeletionTimestamp.Time
			if err := e2epod.WaitForContainerTerminated(ctx, f.ClientSet, p.Namespace, p.Name, name, sleepSeconds*2*time.Second); err != nil {
				framework.Failf("failed waiting for container terminated")
			}

			p, err = podClient.Get(ctx, p.Name, metav1.GetOptions{})
			if err != nil {
				framework.Failf("failed getting pod after deletion")
			}
			// finishAt 等于 delete_time + sleep_duration
			finishAt := p.Status.ContainerStatuses[0].State.Terminated.FinishedAt

			// sleep_duration = (delete_time + sleep_duration) - (delete_time + tgps) + tgps
			sleepDuration := finishAt.Sub(deletionTS) + time.Second*gracePeriod

			// sleep_duration 应满足
			// 大于 50 秒（pod 应 sleep 50 秒）
			// 小于 gracePeriodSeconds（此处为 100 秒）
			if !validDuration(sleepDuration, sleepSeconds, gracePeriod) {
				framework.Failf("unexpected delay duration before killing the pod, finishAt = %v, deletionAt= %v", finishAt, deletionTS)
			}
		})

		/*
			发布版本：v1.34
			测试名称：Pod Lifecycle, prestop sleep hook with low gracePeriodSeconds
			描述：当容器生命周期中通过 'Sleep' action 指定 pre-stop handler 时，handler 必须在容器终止前被调用。创建一个测试 pod，终止前将其 `gracePeriodSeconds` 改为小于 sleep 时间的值。然后检查终止时间是否与 `gracePeriodSeconds` 一致。
		*/
		ginkgo.It("reduce GracePeriodSeconds during runtime", func(ctx context.Context) {
			const sleepSeconds = 50
			lifecycle := &v1.Lifecycle{
				PreStop: &v1.LifecycleHandler{
					Sleep: &v1.SleepAction{Seconds: sleepSeconds},
				},
			}
			name := "pod-with-prestop-sleep-hook"
			podWithHook := getPodWithHook(name, imageutils.GetPauseImageName(), lifecycle)
			podWithHook.Finalizers = append(podWithHook.Finalizers, finalizer)
			podWithHook.Spec.TerminationGracePeriodSeconds = ptr.To[int64](100)
			ginkgo.By("create the pod with lifecycle hook using sleep action")
			p := podClient.CreateSync(ctx, podWithHook)
			defer podClient.RemoveFinalizer(ctx, name, finalizer)
			ginkgo.By("delete the pod with lifecycle hook using sleep action")

			const gracePeriod = 30
			_ = podClient.Delete(ctx, podWithHook.Name, *metav1.NewDeleteOptions(gracePeriod))
			p, err := podClient.Get(ctx, p.Name, metav1.GetOptions{})
			if err != nil {
				framework.Failf("failed getting pod after deletion")
			}
			// deletionTimestamp 等于 delete_time + tgps
			// TODO: issues.k8s.io/132205 解决后减小 sleep_seconds 和 tgps
			// 由于 issues.k8s.io/132205，这里在容器变为 terminated 之前就拿到了 deletionTimestamp
			deletionTS := p.DeletionTimestamp.Time
			if err := e2epod.WaitForContainerTerminated(ctx, f.ClientSet, p.Namespace, p.Name, name, sleepSeconds*2*time.Second); err != nil {
				framework.Failf("failed waiting for container terminated")
			}
			p, err = podClient.Get(ctx, p.Name, metav1.GetOptions{})
			if err != nil {
				framework.Failf("failed getting pod after deletion")
			}
			// finishAt 等于 delete_time + sleep_duration
			finishAt := p.Status.ContainerStatuses[0].State.Terminated.FinishedAt

			// sleep_duration = (delete_time + sleep_duration) - (delete_time + tgps) + tgps
			sleepDuration := finishAt.Sub(deletionTS) + time.Second*gracePeriod
			// sleep_duration 应满足
			// 大于 30 秒（这里把 gracePeriodSeconds 改为 30 秒，小于 sleep action）
			// 小于 sleep action（确保它没有生效）
			if !validDuration(sleepDuration, gracePeriod, sleepSeconds) {
				framework.Failf("unexpected delay duration before killing the pod, finishAt = %v, deletionAt= %v", finishAt, deletionTS)
			}
		})

		/*
			发布版本：v1.34
			测试名称：Pod Lifecycle, prestop sleep hook with erroneous startup command
			描述：当容器生命周期中通过 'Sleep' action 指定 pre-stop handler 时，handler 必须在容器终止前被调用。创建一个启动命令错误的测试 pod，终止时检查它是否忽略了 sleep 时间。
		*/
		ginkgo.It("ignore terminated container", func(ctx context.Context) {
			const sleepSeconds = 10
			const gracePeriod = 30
			lifecycle := &v1.Lifecycle{
				PreStop: &v1.LifecycleHandler{
					Sleep: &v1.SleepAction{Seconds: sleepSeconds},
				},
			}
			name := "pod-with-prestop-sleep-hook"
			podWithHook := getPodWithHook(name, imageutils.GetE2EImage(imageutils.BusyBox), lifecycle)
			podWithHook.Spec.TerminationGracePeriodSeconds = ptr.To[int64](gracePeriod)
			podWithHook.Spec.Containers[0].Command = []string{"/bin/sh"}
			// 如果容器一创建就退出，
			// 由于内部竞态，finishAt - startedAt 可能为负
			// 所以需要让它运行一段时间
			podWithHook.Spec.Containers[0].Args = []string{"-c", "sleep 3"}
			podWithHook.Spec.RestartPolicy = v1.RestartPolicyNever
			ginkgo.By("create the pod with lifecycle hook using sleep action")
			p := podClient.Create(ctx, podWithHook)
			defer podClient.DeleteSync(ctx, podWithHook.Name, metav1.DeleteOptions{}, f.Timeouts.PodDelete)
			// 使用 framework 的 PodDelete 超时而非 gracePeriod*time.Second，
			// 为较慢的 runtime（例如 VM 隔离容器）留出余量
			// 完成 Terminated 状态转换。下面的实际正确性检查使用
			// 容器自身的 StartedAt/FinishedAt 时间戳，不受
			// 这里等待时长的影响。
			framework.ExpectNoError(e2epod.WaitForContainerTerminated(ctx, f.ClientSet, f.Namespace.Name, p.Name, name, f.Timeouts.PodDelete))

			p, err := podClient.Get(ctx, p.Name, metav1.GetOptions{})
			if err != nil {
				framework.Failf("failed getting pod after deletion")
			}
			finishAt := p.Status.ContainerStatuses[0].State.Terminated.FinishedAt
			startedAt := p.Status.ContainerStatuses[0].State.Terminated.StartedAt
			cost := finishAt.Sub(startedAt.Time)
			// cost 应满足
			// 小于 sleep action（容器已终止，sleep action 应被忽略）
			if !validDuration(cost, 0, sleepSeconds) {
				framework.Failf("unexpected delay duration before killing the pod, cost = %v", cost)
			}
		})
	})
})

var _ = SIGDescribe("Lifecycle sleep action zero value", framework.WithNodeConformance(), func() {
	f := framework.NewDefaultFramework("pod-lifecycle-sleep-action-allow-zero")
	f.NamespacePodSecurityLevel = admissionapi.LevelBaseline
	var podClient *e2epod.PodClient

	ginkgo.Context("when create a pod with lifecycle hook using sleep action with a duration of zero seconds", func() {
		ginkgo.BeforeEach(func(ctx context.Context) {
			podClient = e2epod.NewPodClient(f)
		})
		ginkgo.It("prestop hook using sleep action with zero duration", func(ctx context.Context) {
			lifecycle := &v1.Lifecycle{
				PreStop: &v1.LifecycleHandler{
					Sleep: &v1.SleepAction{Seconds: 0},
				},
			}
			podWithHook := getPodWithHook("pod-with-prestop-sleep-hook-zero-duration", imageutils.GetPauseImageName(), lifecycle)
			ginkgo.By("create the pod with lifecycle hook using sleep action with zero duration")
			podClient.CreateSync(ctx, podWithHook)
			ginkgo.By("delete the pod with lifecycle hook using sleep action with zero duration")
			start := time.Now()
			podClient.DeleteSync(ctx, podWithHook.Name, metav1.DeleteOptions{}, f.Timeouts.PodDelete)
			cost := time.Since(start)
			// cost 应满足
			// 大于 0 秒（pod 不应 sleep，handler 应立即返回）
			// 小于 gracePeriodSeconds（此处默认 30 秒）
			if !validDuration(cost, 0, 30) {
				framework.Failf("unexpected delay duration before killing the pod, cost = %v", cost)
			}
		})

	})
})

var _ = SIGDescribe(feature.ContainerStopSignals, framework.WithFeatureGate(features.ContainerStopSignals), func() {
	f := framework.NewDefaultFramework("container-stop-signals")
	f.NamespacePodSecurityLevel = admissionapi.LevelBaseline
	var podClient *e2epod.PodClient
	sigterm := v1.SIGTERM
	podName := "pod-" + utilrand.String(5)

	ginkgo.Context("when create a pod with a StopSignal lifecycle", func() {
		ginkgo.BeforeEach(func(ctx context.Context) {
			podClient = e2epod.NewPodClient(f)
		})
		ginkgo.It("StopSignal defined with pod.OS", func(ctx context.Context) {

			testPod := e2epod.MustMixinRestrictedPodSecurity(&v1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name: podName,
				},
				Spec: v1.PodSpec{
					OS: &v1.PodOS{
						Name: v1.Linux,
					},
					Containers: []v1.Container{
						{
							Name:  "test",
							Image: imageutils.GetPauseImageName(),
							Lifecycle: &v1.Lifecycle{
								StopSignal: &sigterm,
							},
						},
					},
				},
			})

			ginkgo.By("submitting the pod to kubernetes")
			pod := podClient.CreateSync(ctx, testPod)
			framework.ExpectNoError(e2epod.WaitForPodRunningInNamespace(ctx, f.ClientSet, pod), "Pod didn't start when custom StopSignal was passed in Lifecycle")
		})
	})
})
