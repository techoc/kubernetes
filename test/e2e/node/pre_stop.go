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

package node

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"time"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/uuid"
	"k8s.io/apimachinery/pkg/util/wait"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/kubernetes/pkg/cluster/ports"
	"k8s.io/kubernetes/test/e2e/framework"
	e2ekubelet "k8s.io/kubernetes/test/e2e/framework/kubelet"
	e2epod "k8s.io/kubernetes/test/e2e/framework/pod"
	imageutils "k8s.io/kubernetes/test/utils/image"
	admissionapi "k8s.io/pod-security-admission/api"

	"github.com/onsi/ginkgo/v2"
)

// 状态部分克隆自 webserver.go
type State struct {
	Received map[string]int
}

func testPreStop(ctx context.Context, c clientset.Interface, ns string) {
	// 这是将接收 preStop 通知的 server
	podDescr := e2epod.NewAgnhostPod(ns, "server", nil, nil, []v1.ContainerPort{{ContainerPort: 8080}}, "nettest")
	ginkgo.By(fmt.Sprintf("Creating server pod %s in namespace %s", podDescr.Name, ns))
	podDescr, err := c.CoreV1().Pods(ns).Create(ctx, podDescr, metav1.CreateOptions{})
	framework.ExpectNoError(err, fmt.Sprintf("creating pod %s", podDescr.Name))

	// 测试结束时，通过删除 pod 清理。
	ginkgo.DeferCleanup(func(ctx context.Context) error {
		ginkgo.By("Deleting the server pod")
		return c.CoreV1().Pods(ns).Delete(ctx, podDescr.Name, metav1.DeleteOptions{})
	})

	ginkgo.By("Waiting for pods to come up.")
	err = e2epod.WaitForPodRunningInNamespace(ctx, c, podDescr)
	framework.ExpectNoError(err, "waiting for server pod to start")

	val := "{\"Source\": \"prestop\"}"

	podOut, err := c.CoreV1().Pods(ns).Get(ctx, podDescr.Name, metav1.GetOptions{})
	framework.ExpectNoError(err, "getting pod info")

	podURL := net.JoinHostPort(podOut.Status.PodIP, "8080")

	preStopDescr := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "tester",
		},
		Spec: v1.PodSpec{
			Containers: []v1.Container{
				{
					Name:    "tester",
					Image:   imageutils.GetE2EImage(imageutils.BusyBox),
					Command: []string{"sleep", "600"},
					Lifecycle: &v1.Lifecycle{
						PreStop: &v1.LifecycleHandler{
							Exec: &v1.ExecAction{
								Command: []string{
									"wget", "-O-", "--post-data=" + val, fmt.Sprintf("http://%s/write", podURL),
								},
							},
						},
					},
				},
			},
		},
	}

	ginkgo.By(fmt.Sprintf("Creating tester pod %s in namespace %s", preStopDescr.Name, ns))
	preStopDescr, err = c.CoreV1().Pods(ns).Create(ctx, preStopDescr, metav1.CreateOptions{})
	framework.ExpectNoError(err, fmt.Sprintf("creating pod %s", preStopDescr.Name))
	deletePreStop := true

	// 测试结束时，通过删除 pod 清理。
	ginkgo.DeferCleanup(func(ctx context.Context) error {
		if deletePreStop {
			ginkgo.By("Deleting the tester pod")
			return c.CoreV1().Pods(ns).Delete(ctx, preStopDescr.Name, metav1.DeleteOptions{})
		}
		return nil
	})

	err = e2epod.WaitForPodRunningInNamespace(ctx, c, preStopDescr)
	framework.ExpectNoError(err, "waiting for tester pod to start")

	// 删除带 preStop handler 的 pod。
	ginkgo.By("Deleting pre-stop pod")
	if err := c.CoreV1().Pods(ns).Delete(ctx, preStopDescr.Name, metav1.DeleteOptions{}); err == nil {
		deletePreStop = false
	}
	framework.ExpectNoError(err, fmt.Sprintf("deleting pod: %s", preStopDescr.Name))

	// 验证 server 收到了 web poke。
	err = wait.Poll(time.Second*5, time.Second*60, func() (bool, error) {

		ctx, cancel := context.WithTimeout(ctx, framework.SingleCallTimeout)
		defer cancel()

		var body []byte
		body, err = c.CoreV1().RESTClient().Get().
			Namespace(ns).
			Resource("pods").
			SubResource("proxy").
			Name(podDescr.Name).
			Suffix("read").
			DoRaw(ctx)

		if err != nil {
			if ctx.Err() != nil {
				framework.Failf("Error validating prestop: %v", err)
				return true, err
			}
			ginkgo.By(fmt.Sprintf("Error validating prestop: %v", err))
		} else {
			framework.Logf("Saw: %s", string(body))
			state := State{}
			err := json.Unmarshal(body, &state)
			if err != nil {
				framework.Logf("Error parsing: %v", err)
				return false, nil
			}
			if state.Received["prestop"] != 0 {
				return true, nil
			}
		}
		return false, nil
	})
	framework.ExpectNoError(err, "validating pre-stop.")
}

var _ = SIGDescribe("PreStop", func() {
	f := framework.NewDefaultFramework("prestop")
	f.NamespacePodSecurityLevel = admissionapi.LevelBaseline
	var podClient *e2epod.PodClient
	ginkgo.BeforeEach(func() {
		podClient = e2epod.NewPodClient(f)
	})

	/*
		发布版本：v1.9
		测试名称：Pods, prestop hook
		描述：创建一个带 rest 端点 '/write' 的 server pod，该端点会修改 state.Received 字段。创建一个带 pre-stop handler 的 Pod，它向 server Pod 的 /write 端点发送请求。验证带 pre-stop hook 的 Pod 正在运行。删除带 pre-stop hook 的 Pod。Pod 被删除前，如果配置了 handler，必须调用它。验证 Pod 已删除，并通过检查 server Pod 上收到的状态确认 prestop hook 被调用。
	*/
	framework.ConformanceIt("should call prestop when killing a pod", func(ctx context.Context) {
		testPreStop(ctx, f.ClientSet, f.Namespace.Name)
	})

	ginkgo.It("graceful pod terminated should wait until preStop hook completes the process", func(ctx context.Context) {
		gracefulTerminationPeriodSeconds := int64(30)
		ginkgo.By("creating the pod")
		name := "pod-prestop-hook-" + string(uuid.NewUUID())
		pod := getPodWithpreStopLifeCycle(name)

		ginkgo.By("submitting the pod to kubernetes")
		podClient.Create(ctx, pod)

		ginkgo.By("waiting for pod running")
		framework.ExpectNoError(e2epod.WaitForPodNameRunningInNamespace(ctx, f.ClientSet, pod.Name, f.Namespace.Name))

		var err error
		pod, err = podClient.Get(ctx, pod.Name, metav1.GetOptions{})
		framework.ExpectNoError(err, "failed to GET scheduled pod")

		ginkgo.By("deleting the pod gracefully")
		err = podClient.Delete(ctx, pod.Name, *metav1.NewDeleteOptions(gracefulTerminationPeriodSeconds))
		framework.ExpectNoError(err, "failed to delete pod")

		// 等待时间小于 gracePeriod 终止时间，确保
		// preStop hook 仍在执行。
		time.Sleep(15 * time.Second)

		ginkgo.By("verifying the pod is running while in the graceful period termination")
		result := &v1.PodList{}
		err = wait.Poll(time.Second*5, time.Second*60, func() (bool, error) {
			client, err := e2ekubelet.ProxyRequest(ctx, f.ClientSet, pod.Spec.NodeName, "pods", ports.KubeletPort)
			framework.ExpectNoError(err, "failed to get the pods of the node")
			err = client.Into(result)
			framework.ExpectNoError(err, "failed to parse the pods of the node")

			for _, kubeletPod := range result.Items {
				if pod.Name != kubeletPod.Name {
					continue
				} else if kubeletPod.Status.Phase == v1.PodRunning {
					framework.Logf("pod is running")
					return true, err
				}
			}
			return false, err
		})

		framework.ExpectNoError(err, "validate-pod-is-running")
	})
})

func getPodWithpreStopLifeCycle(name string) *v1.Pod {
	return &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
		},
		Spec: v1.PodSpec{
			Containers: []v1.Container{
				{
					Name:  "nginx",
					Image: imageutils.GetE2EImage(imageutils.Nginx),
					Lifecycle: &v1.Lifecycle{
						PreStop: &v1.LifecycleHandler{
							Exec: &v1.ExecAction{
								Command: []string{"sh", "-c", "while true; do echo preStop; sleep 1; done"},
							},
						},
					},
				},
			},
		},
	}
}
