/*
Copyright 2020 The Kubernetes Authors.

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

/* 本测试检查 setHostnameAsFQDN PodSpec 字段是否按预期工作。
 */

package e2enode

import (
	"context"
	"crypto/rand"
	"fmt"
	"math/big"
	"time"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/kubernetes/pkg/kubelet/events"
	admissionapi "k8s.io/pod-security-admission/api"

	"k8s.io/kubernetes/test/e2e/framework"
	e2eevents "k8s.io/kubernetes/test/e2e/framework/events"
	e2epod "k8s.io/kubernetes/test/e2e/framework/pod"
	e2eoutput "k8s.io/kubernetes/test/e2e/framework/pod/output"
	imageutils "k8s.io/kubernetes/test/utils/image"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
)

func generatePodName(base string) string {
	id, err := rand.Int(rand.Reader, big.NewInt(214748))
	if err != nil {
		return base
	}
	return fmt.Sprintf("%s-%d", base, id)
}

func testPod(podnamebase string) *v1.Pod {
	podName := generatePodName(podnamebase)
	pod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:        podName,
			Labels:      map[string]string{"name": podName},
			Annotations: map[string]string{},
		},
		Spec: v1.PodSpec{
			Containers: []v1.Container{
				{
					Name:  "test-container",
					Image: imageutils.GetE2EImage(imageutils.GlibcDnsTesting),
				},
			},
			RestartPolicy: v1.RestartPolicyNever,
		},
	}

	return pod
}

var _ = SIGDescribe("Hostname of Pod", framework.WithNodeConformance(), func() {
	f := framework.NewDefaultFramework("hostfqdn")
	f.NamespacePodSecurityLevel = admissionapi.LevelBaseline
	/*
	   Release: v1.19
	   Testname: Create Pod without fully qualified domain name (FQDN)
	   Description: spec 中未定义 subdomain 字段的 Pod 没有 FQDN。
	*/
	ginkgo.It("a pod without subdomain field does not have FQDN", func(ctx context.Context) {
		pod := testPod("hostfqdn")
		pod.Spec.Containers[0].Command = []string{"sh", "-c", "echo $(hostname)';'$(hostname -f)';'"}
		output := []string{fmt.Sprintf("%s;%s;", pod.ObjectMeta.Name, pod.ObjectMeta.Name)}
		// 创建 Pod。
		e2eoutput.TestContainerOutput(ctx, f, "shortname only", pod, 0, output)
	})

	/*
	   Release: v1.19
	   Testname: Create Pod without FQDN, setHostnameAsFQDN field set to true
	   Description: spec 中未定义 subdomain 字段的 Pod 没有 FQDN，因此 setHostnameAsFQDN 字段不起作用。
	*/
	ginkgo.It("a pod without FQDN is not affected by SetHostnameAsFQDN field", func(ctx context.Context) {
		pod := testPod("hostfqdn")
		// 将 setHostnameAsFQDN 设置为 true 不应产生任何效果。
		setHostnameAsFQDN := true
		pod.Spec.SetHostnameAsFQDN = &setHostnameAsFQDN
		pod.Spec.Containers[0].Command = []string{"sh", "-c", "echo $(hostname)';'$(hostname -f)';'"}
		output := []string{fmt.Sprintf("%s;%s;", pod.ObjectMeta.Name, pod.ObjectMeta.Name)}
		// 创建 Pod。
		e2eoutput.TestContainerOutput(ctx, f, "shortname only", pod, 0, output)
	})

	/*
	   Release: v1.19
	   Testname: Create Pod with FQDN, setHostnameAsFQDN field not defined.
	   Description: spec 中定义了 subdomain 字段的 Pod 具有 FQDN。hostname 命令返回短名称（此处为 Pod 名称），hostname -f 返回 FQDN。
	*/
	ginkgo.It("a pod with subdomain field has FQDN, hostname is shortname", func(ctx context.Context) {
		pod := testPod("hostfqdn")
		pod.Spec.Containers[0].Command = []string{"sh", "-c", "echo $(hostname)';'$(hostname -f)';'"}
		subdomain := "t"
		// 设置 PodSpec 的 subdomain 字段，为 Pod 生成 FQDN。
		pod.Spec.Subdomain = subdomain
		// 预期的 Pod FQDN。
		hostFQDN := fmt.Sprintf("%s.%s.%s.svc.%s", pod.ObjectMeta.Name, subdomain, f.Namespace.Name, framework.TestContext.ClusterDNSDomain)
		output := []string{fmt.Sprintf("%s;%s;", pod.ObjectMeta.Name, hostFQDN)}
		// 创建 Pod。
		e2eoutput.TestContainerOutput(ctx, f, "shortname and fqdn", pod, 0, output)
	})

	/*
	   Release: v1.19
	   Testname: Create Pod with FQDN, setHostnameAsFQDN field set to true.
	   Description: spec 中定义了 subdomain 字段的 Pod 具有 FQDN。当 setHostnameAsFQDN: true 时，hostname 被设置为 FQDN，此时 hostname 和 hostname -f 两个命令都返回 Pod 的 FQDN。
	*/
	ginkgo.It("a pod with subdomain field has FQDN, when setHostnameAsFQDN is set to true, the FQDN is set as hostname", func(ctx context.Context) {
		pod := testPod("hostfqdn")
		pod.Spec.Containers[0].Command = []string{"sh", "-c", "echo $(hostname)';'$(hostname -f)';'"}
		subdomain := "t"
		// 设置 PodSpec 的 subdomain 字段，为 Pod 生成 FQDN。
		pod.Spec.Subdomain = subdomain
		// 设置 PodSpec 的 setHostnameAsFQDN，将 FQDN 用作 hostname。
		setHostnameAsFQDN := true
		pod.Spec.SetHostnameAsFQDN = &setHostnameAsFQDN
		// 预期的 Pod FQDN。
		hostFQDN := fmt.Sprintf("%s.%s.%s.svc.%s", pod.ObjectMeta.Name, subdomain, f.Namespace.Name, framework.TestContext.ClusterDNSDomain)
		// FQDN 超过 64 个字符时直接失败，否则 Pod 会一直处于 Pending 直到测试超时。
		// Linux 中内核 hostname 字段上限为 64 个字符，本测试将其设置为 Pod 的 FQDN。
		gomega.Expect(len(hostFQDN)).Should(gomega.BeNumerically("<=", 64), "The FQDN of the Pod cannot be longer than 64 characters, requested %s which is %d characters long.", hostFQDN, len(hostFQDN))
		output := []string{fmt.Sprintf("%s;%s;", hostFQDN, hostFQDN)}
		// 创建 Pod。
		e2eoutput.TestContainerOutput(ctx, f, "fqdn and fqdn", pod, 0, output)
	})

	/*
	   Release: v1.20
	   Testname: Fail to Create Pod with longer than 64 bytes FQDN when setHostnameAsFQDN field set to true.
	   Description: spec 中定义了 subdomain 字段的 Pod 具有 FQDN。当 setHostnameAsFQDN: true 时，hostname 被设置为 FQDN。由于内核 hostname 字段上限为 64 字节，若 Pod FQDN 超过 64 字节，将产生与 FailedCreatePodSandBox 相关的事件。
	*/

	ginkgo.It("a pod configured to set FQDN as hostname will remain in Pending "+
		"state generating FailedCreatePodSandBox events when the FQDN is "+
		"longer than 64 bytes", func(ctx context.Context) {
		// 55 个字符的名称加上 -<int>.t.svc.cluster.local 会远超 64 字节。
		pod := testPod("hostfqdnveryveryveryverylongforfqdntobemorethan64bytes")
		pod.Spec.Containers[0].Command = []string{"sh", "-c", "echo $(hostname)';'$(hostname -f)';'"}
		subdomain := "t"
		// 设置 PodSpec 的 subdomain 字段，为 Pod 生成 FQDN。
		pod.Spec.Subdomain = subdomain
		// 设置 PodSpec 的 setHostnameAsFQDN，将 FQDN 用作 hostname。
		setHostnameAsFQDN := true
		pod.Spec.SetHostnameAsFQDN = &setHostnameAsFQDN
		// 创建 Pod。
		launchedPod := e2epod.NewPodClient(f).Create(ctx, pod)
		// 确保删除 Pod。
		ginkgo.DeferCleanup(e2epod.NewPodClient(f).DeleteSync, launchedPod.Name, metav1.DeleteOptions{}, f.Timeouts.PodDelete)

		// Pod 应保持 Pending 状态，并产生原因为 FailedCreatePodSandBox 的事件。
		// 预期错误消息。
		expectedMessage := "Failed to create pod sandbox: failed " +
			"to construct FQDN from pod hostname and cluster domain, FQDN "
		framework.Logf("Waiting for Pod to generate FailedCreatePodSandBox event.")
		// 等待原因为 FailedCreatePodSandBox 的事件。
		expectSandboxFailureEvent(ctx, f, launchedPod, expectedMessage)
		// 检查 Pod 处于 Pending 阶段。
		err := checkPodIsPending(ctx, f, launchedPod.ObjectMeta.Name, launchedPod.ObjectMeta.Namespace)
		framework.ExpectNoError(err)

	})
})

// expectSandboxFailureEvent 轮询原因为 "FailedCreatePodSandBox" 且包含预期消息的字符串的事件。
func expectSandboxFailureEvent(ctx context.Context, f *framework.Framework, pod *v1.Pod, msg string) {
	eventSelector := fields.Set{
		"involvedObject.kind":      "Pod",
		"involvedObject.name":      pod.Name,
		"involvedObject.namespace": f.Namespace.Name,
		"reason":                   events.FailedCreatePodSandBox,
	}.AsSelector().String()
	framework.ExpectNoError(e2eevents.WaitTimeoutForEvent(ctx,
		f.ClientSet, f.Namespace.Name, eventSelector, msg, framework.PodEventTimeout))
}

func checkPodIsPending(ctx context.Context, f *framework.Framework, podName, namespace string) error {
	c := f.ClientSet
	// 在看到创建 Pod 失败的事件后调用本函数，此时 Pod 已创建，应处于 Pending 状态。
	// 给 30 秒时间获取 Pod，避免因获取 Pod 时的瞬时问题而失败。
	fetchPodTimeout := 30 * time.Second
	return e2epod.WaitForPodCondition(ctx, c, namespace, podName, "Failed to Create Pod", fetchPodTimeout, func(pod *v1.Pod) (bool, error) {
		// 期望 Pod 已被调度且处于 Pending 状态。
		if pod.Status.Phase == v1.PodPending {
			for _, cond := range pod.Status.Conditions {
				if cond.Type == v1.PodScheduled && cond.Status == v1.ConditionTrue {
					return true, nil
				}
			}
		}
		// 如果 Pod 进入这些状态，说明 FQDN 很可能短于 64 字节。
		if pod.Status.Phase == v1.PodRunning || pod.Status.Phase == v1.PodSucceeded || pod.Status.Phase == v1.PodFailed {
			return true, fmt.Errorf("Expected pod %q in namespace %q to be in phase Pending, but got phase: %v", podName, namespace, pod.Status.Phase)
		}
		return false, nil
	})
}
