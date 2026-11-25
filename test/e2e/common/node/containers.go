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
	"time"

	"github.com/onsi/gomega"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/uuid"
	"k8s.io/kubernetes/test/e2e/framework"
	e2epod "k8s.io/kubernetes/test/e2e/framework/pod"
	e2epodoutput "k8s.io/kubernetes/test/e2e/framework/pod/output"
	admissionapi "k8s.io/pod-security-admission/api"
)

var _ = SIGDescribe("Containers", func() {
	f := framework.NewDefaultFramework("containers")
	f.NamespacePodSecurityLevel = admissionapi.LevelBaseline

	/*
		Release: v1.9
		Testname: Containers, without command and arguments
		Description: Pod 未指定容器命令时，MUST 使用镜像 entrypoint 中的默认命令和参数
	*/
	framework.ConformanceIt("should use the image defaults if command and args are blank", f.WithNodeConformance(), func(ctx context.Context) {
		pod := entrypointTestPod(f.Namespace.Name)
		pod.Spec.Containers[0].Args = nil
		pod = e2epod.NewPodClient(f).Create(ctx, pod)
		err := e2epod.WaitForPodNameRunningInNamespace(ctx, f.ClientSet, pod.Name, f.Namespace.Name)
		framework.ExpectNoError(err, "Expected pod %q to be running, got error: %v", pod.Name, err)
		pollLogs := func() (string, error) {
			return e2epod.GetPodLogs(ctx, f.ClientSet, f.Namespace.Name, pod.Name, pod.Spec.Containers[0].Name)
		}

		// agnhost 镜像默认 entrypoint / args 为 "/agnhost pause"，会打印 "Paused"。
		gomega.Eventually(ctx, pollLogs, time.Minute, framework.Poll).Should(gomega.ContainSubstring("Paused"))
	})

	/*
		Release: v1.9
		Testname: Containers, with arguments
		Description: Pod 未指定容器命令时，MUST 使用镜像 entrypoint 中的默认命令；但 Pod spec 中指定的参数 MUST 覆盖镜像参数。
	*/
	framework.ConformanceIt("should be able to override the image's default arguments (container cmd)", f.WithNodeConformance(), func(ctx context.Context) {
		pod := entrypointTestPod(f.Namespace.Name, "entrypoint-tester", "override", "arguments")
		e2epodoutput.TestContainerOutput(ctx, f, "override arguments", pod, 0, []string{
			"[/agnhost entrypoint-tester override arguments]",
		})
	})

	// 注意：覆盖 entrypoint 时，镜像的参数（container cmd）会被忽略。
	/*
		Release: v1.9
		Testname: Containers, with command
		Description: Pod 指定容器命令时，MUST NOT 使用镜像 entrypoint 中的默认命令；Pod spec 中的命令 MUST 覆盖镜像命令。
	*/
	framework.ConformanceIt("should be able to override the image's default command (container entrypoint)", f.WithNodeConformance(), func(ctx context.Context) {
		pod := entrypointTestPod(f.Namespace.Name, "entrypoint-tester")
		pod.Spec.Containers[0].Command = []string{"/agnhost-2"}

		e2epodoutput.TestContainerOutput(ctx, f, "override command", pod, 0, []string{
			"[/agnhost-2 entrypoint-tester]",
		})
	})

	/*
		Release: v1.9
		Testname: Containers, with command and arguments
		Description: Pod 指定容器命令和参数时，MUST NOT 使用镜像 entrypoint 中的默认命令和参数；Pod spec 中的命令和参数 MUST 覆盖镜像中的对应内容。
	*/
	framework.ConformanceIt("should be able to override the image's default command and arguments", f.WithNodeConformance(), func(ctx context.Context) {
		pod := entrypointTestPod(f.Namespace.Name, "entrypoint-tester", "override", "arguments")
		pod.Spec.Containers[0].Command = []string{"/agnhost-2"}

		e2epodoutput.TestContainerOutput(ctx, f, "override all", pod, 0, []string{
			"[/agnhost-2 entrypoint-tester override arguments]",
		})
	})
})

// 返回一个典型的 entrypoint 测试 Pod
func entrypointTestPod(namespace string, entrypointArgs ...string) *v1.Pod {
	podName := "client-containers-" + string(uuid.NewUUID())
	pod := e2epod.NewAgnhostPod(namespace, podName, nil, nil, nil, entrypointArgs...)

	one := int64(1)
	pod.Spec.TerminationGracePeriodSeconds = &one
	pod.Spec.RestartPolicy = v1.RestartPolicyNever
	return pod
}
