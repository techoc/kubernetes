/*
Copyright 2025 The Kubernetes Authors.

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

package registry

import (
	"context"
	"fmt"
	"time"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/pod-security-admission/api"
	"k8s.io/pod-security-admission/test"
	"k8s.io/utils/ptr"

	"k8s.io/kubernetes/test/e2e/framework"
	e2edaemonset "k8s.io/kubernetes/test/e2e/framework/daemonset"
	e2epod "k8s.io/kubernetes/test/e2e/framework/pod"
	imageutils "k8s.io/kubernetes/test/utils/image"
)

const (
	dockerCredsFmt = `{
	"auths": {
		"%s": {
			"auth": "%s"
		}
	}
}`
	user1creds = "dXNlcjpwYXNzd29yZA==" // 用户名:密码
)

// SetupRegistry 运行 agnhost 镜像中的 `fake-registry-server --private`。
// registry 通过 HostPort 5000 暴露，这样调度到本地的 pod 就能经由 kubelet
// 查询该 registry。
// registry 只以 HTTP（无 TLS）模式运行，因此采用这种取巧做法：CRI 通常
// 允许 localhost，无需针对 CRI 做额外配置。
//
// 默认情况下，本函数以 DaemonSet 形式在所有节点上运行 registry；
// 但也可以只在单个 `pod` 中运行，用于 kube-controller-manager 未运行的场景
// （例如 Node Conformance 测试套件）。
//
// 本函数返回：
// - registry 的节点本地地址
// - 运行 registry 的节点名集合，主要仅在 podOnly 场景下有用
// - error
//
// TODO: 等 https://github.com/kubernetes/kubernetes/issues/132955 处理完，
// 也许可以把集群中的单个 endpoint 代理到每个节点的 localhost 端口，
// 而不必使用 DaemonSet。
func SetupRegistry(ctx context.Context, f *framework.Framework, podOnly bool) (string, []string, error) {
	podTestLabel := "test-registry-pod-" + f.UniqueName
	pod, err := podManifest(podTestLabel)
	if err != nil {
		return "", nil, err
	}

	if podOnly {
		podClient := e2epod.NewPodClient(f)
		pod = podClient.Create(ctx, pod)
		framework.ExpectNoError(e2epod.WaitForPodRunningInNamespace(ctx, f.ClientSet, pod))
	} else {
		labels := map[string]string{"kube-e2e": podTestLabel}
		daemonset := e2edaemonset.NewDaemonSet("", "", labels, nil, nil, nil)
		daemonset.GenerateName = "test-registry-"
		daemonset.Spec.Template.Spec = pod.Spec

		daemonset, err = f.ClientSet.AppsV1().DaemonSets(f.Namespace.Name).Create(ctx, daemonset, metav1.CreateOptions{})
		if err != nil {
			return "", nil, err
		}

		err = wait.PollUntilContextTimeout(ctx, 1*time.Second, 120*time.Second, true, func(ctx context.Context) (done bool, err error) {
			return e2edaemonset.CheckRunningOnAllNodes(ctx, f, daemonset)
		})
		if err != nil {
			return "", nil, err
		}
	}

	pods, err := f.ClientSet.CoreV1().Pods(f.Namespace.Name).List(ctx, metav1.ListOptions{LabelSelector: "kube-e2e=" + podTestLabel})
	if err != nil {
		return "", nil, err
	}
	podNodes := make([]string, 0, len(pods.Items))
	for _, pod := range pods.Items {
		podNodes = append(podNodes, pod.Spec.NodeName)
	}

	return "localhost:5000", podNodes, nil
}

func podManifest(podTestLabel string) (*v1.Pod, error) {
	pod, err := test.GetMinimalValidPod(api.LevelRestricted, api.MajorMinorVersion(1, 25))
	if err != nil {
		return nil, err
	}

	pod.ObjectMeta.GenerateName = "test-registry-"
	pod.ObjectMeta.Labels = map[string]string{"kube-e2e": podTestLabel}

	pod.Spec.InitContainers = nil
	pod.Spec.Containers[0].Name = "registry"
	pod.Spec.Containers[0].Image = imageutils.GetE2EImage(imageutils.Agnhost)
	pod.Spec.Containers[0].ImagePullPolicy = v1.PullIfNotPresent
	pod.Spec.Containers[0].Args = []string{"fake-registry-server", "--private"}
	pod.Spec.Containers[0].Ports = []v1.ContainerPort{
		{
			Name:          "http",
			ContainerPort: 5000,
			HostPort:      5000,
		},
	}
	pod.Spec.Containers[0].SecurityContext.RunAsUser = ptr.To[int64](5123)
	pod.Spec.Containers[0].SecurityContext.WindowsOptions = &v1.WindowsSecurityContextOptions{
		RunAsUserName: ptr.To("NT AUTHORITY\\SYSTEM"),
		HostProcess:   ptr.To(true),
	}
	// 设置 HostNetwork 为 true，这样 registry 可以通过 localhost:<hostport>
	// 访问，避开 CNI 的各种怪异行为。
	pod.Spec.HostNetwork = true

	return pod, nil
}

// User1DockerSecret 创建一个 secret，其中包含从 agnhost fake-registry-server
// 拉取镜像所需的 docker 凭据。
func User1DockerSecret(registryAddress string) *v1.Secret {
	return &v1.Secret{
		Type: v1.SecretTypeDockerConfigJson,
		Data: map[string][]byte{
			v1.DockerConfigJsonKey: fmt.Appendf(nil, dockerCredsFmt, registryAddress, user1creds),
		},
	}
}
