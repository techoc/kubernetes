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
	"strings"
	"time"

	"github.com/onsi/ginkgo/v2"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/test/e2e/framework"
	e2epod "k8s.io/kubernetes/test/e2e/framework/pod"
	admissionapi "k8s.io/pod-security-admission/api"
)

const (
	etcHostsPodName             = "test-pod"
	etcHostsHostNetworkPodName  = "test-host-network-pod"
	etcHostsPartialContent      = "# Kubernetes-managed hosts file."
	etcHostsPathLinux           = "/etc/hosts"
	etcHostsPathWindows         = `C:\Windows\System32\drivers\etc\hosts`
	etcHostsOriginalPathLinux   = "/etc/hosts-original"
	etcHostsOriginalPathWindows = `C:\Windows\System32\drivers\etc\hosts-original`
)

// KubeletManagedHostConfig 定义运行 kubelet 托管 /etc/hosts 测试用例所需的类型
type KubeletManagedHostConfig struct {
	hostNetworkPod *v1.Pod
	pod            *v1.Pod
	f              *framework.Framework
}

var _ = SIGDescribe("KubeletManagedEtcHosts", func() {
	f := framework.NewDefaultFramework("e2e-kubelet-etc-hosts")
	f.NamespacePodSecurityLevel = admissionapi.LevelPrivileged
	config := &KubeletManagedHostConfig{
		f: f,
	}

	/*
		Release: v1.9
		Testname: Kubelet, managed etc hosts
		Description: 创建一个 hostNetwork=false 的 Pod，其中一个容器挂载 host 上的 /etc/hosts 文件。再创建一个 hostNetwork=true 的 Pod。
			1. hostNetwork=false 的 Pod，其容器的 /etc/hosts MUST 由 kubelet 托管。
			2. hostNetwork=false 但容器挂载了 host 的 /etc/hosts 时，该文件 MUST 不由 kubelet 托管。
			3. hostNetwork=true 的 Pod，其 /etc/hosts 文件 MUST 不由 kubelet 托管。
	*/
	framework.ConformanceIt("should test kubelet managed /etc/hosts file", f.WithNodeConformance(), func(ctx context.Context) {
		ginkgo.By("Setting up the test")
		config.setup(ctx)

		ginkgo.By("Running the test")
		config.verifyEtcHosts()
	})
})

func (config *KubeletManagedHostConfig) verifyEtcHosts() {
	ginkgo.By("Verifying /etc/hosts of container is kubelet-managed for pod with hostNetwork=false")
	assertManagedStatus(config, etcHostsPodName, true, "busybox-1")
	assertManagedStatus(config, etcHostsPodName, true, "busybox-2")

	ginkgo.By("Verifying /etc/hosts of container is not kubelet-managed since container specifies /etc/hosts mount")
	assertManagedStatus(config, etcHostsPodName, false, "busybox-3")

	ginkgo.By("Verifying /etc/hosts content of container is not kubelet-managed for pod with hostNetwork=true")
	assertManagedStatus(config, etcHostsHostNetworkPodName, false, "busybox-1")
	assertManagedStatus(config, etcHostsHostNetworkPodName, false, "busybox-2")
}

func (config *KubeletManagedHostConfig) setup(ctx context.Context) {
	ginkgo.By("Creating hostNetwork=false pod")
	config.createPodWithoutHostNetwork(ctx)

	ginkgo.By("Creating hostNetwork=true pod")
	config.createPodWithHostNetwork(ctx)
}

func (config *KubeletManagedHostConfig) createPodWithoutHostNetwork(ctx context.Context) {
	podSpec := config.createPodSpec(etcHostsPodName)
	config.pod = e2epod.NewPodClient(config.f).CreateSync(ctx, podSpec)
}

func (config *KubeletManagedHostConfig) createPodWithHostNetwork(ctx context.Context) {
	podSpec := config.createPodSpecWithHostNetwork(etcHostsHostNetworkPodName)
	config.hostNetworkPod = e2epod.NewPodClient(config.f).CreateSync(ctx, podSpec)
}

func assertManagedStatus(
	config *KubeletManagedHostConfig, podName string, expectedIsManaged bool, name string) {
	// TODO: workaround for https://github.com/kubernetes/kubernetes/issues/34256
	//
	// 重试直到 /etc/hosts 内容出现或超时。若 /etc/hosts 挂载正常，
	// 这里会立即成功。
	const retryTimeout = 30 * time.Second

	retryCount := 0
	etcHostsContent := ""
	etcHostsPath, etcHostsOriginalPath := getEtcHostsPath()

	for startTime := time.Now(); time.Since(startTime) < retryTimeout; {
		etcHostsContent = config.getFileContents(podName, name, etcHostsPath)
		etcHostsOriginalContent := config.getFileContents(podName, name, etcHostsOriginalPath)

		// 两个文件都有内容后再判断
		if len(etcHostsContent) > 0 && len(etcHostsOriginalContent) > 0 {
			// 文件相同说明 kubernetes 完全没动过；
			// 带 header 说明 kubernetes 未使用 host network，
			// 而是根据 Pod IP 生成了文件
			isManaged := strings.HasPrefix(etcHostsContent, etcHostsPartialContent) &&
				etcHostsContent != etcHostsOriginalContent
			if expectedIsManaged == isManaged {
				return
			}
		}

		klog.Warningf(
			"For pod: %s, name: %s, expected %t, (/etc/hosts was %q), (/etc/hosts-original was %q), retryCount: %d",
			podName, name, expectedIsManaged, etcHostsContent, etcHostsOriginalContent, retryCount)

		retryCount++
		time.Sleep(100 * time.Millisecond)
	}

	if expectedIsManaged {
		framework.Failf(
			"/etc/hosts file should be kubelet managed (name: %s, retries: %d). /etc/hosts contains %q",
			name, retryCount, etcHostsContent)
	} else {
		framework.Failf(
			"/etc/hosts file should no be kubelet managed (name: %s, retries: %d). /etc/hosts contains %q",
			name, retryCount, etcHostsContent)
	}
}

func (config *KubeletManagedHostConfig) getFileContents(podName, containerName, path string) string {
	return e2epod.ExecCommandInContainer(config.f, podName, containerName, "cat", path)
}

func (config *KubeletManagedHostConfig) createPodSpec(podName string) *v1.Pod {
	hostPathType := new(v1.HostPathType)
	*hostPathType = v1.HostPathType(string(v1.HostPathFileOrCreate))
	etcHostsPath, etcHostsOriginalPath := getEtcHostsPath()
	mounts := []v1.VolumeMount{
		{
			Name:      "host-etc-hosts",
			MountPath: etcHostsOriginalPath,
		},
	}
	multipleMounts := []v1.VolumeMount{
		mounts[0],
		{
			Name:      "host-etc-hosts",
			MountPath: etcHostsPath,
		},
	}
	pod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: podName,
		},
		Spec: v1.PodSpec{
			Containers: []v1.Container{
				e2epod.NewAgnhostContainer("busybox-1", mounts, nil),
				e2epod.NewAgnhostContainer("busybox-2", mounts, nil),
				e2epod.NewAgnhostContainer("busybox-3", multipleMounts, nil),
			},
			Volumes: []v1.Volume{
				{
					Name: "host-etc-hosts",
					VolumeSource: v1.VolumeSource{
						HostPath: &v1.HostPathVolumeSource{
							Path: etcHostsPath,
							Type: hostPathType,
						},
					},
				},
			},
		},
	}

	return pod
}

func (config *KubeletManagedHostConfig) createPodSpecWithHostNetwork(podName string) *v1.Pod {
	hostPathType := new(v1.HostPathType)
	*hostPathType = v1.HostPathType(string(v1.HostPathFileOrCreate))
	etcHostsPath, etcHostsOriginalPath := getEtcHostsPath()
	mounts := []v1.VolumeMount{
		{
			Name:      "host-etc-hosts",
			MountPath: etcHostsOriginalPath,
		},
	}
	pod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: podName,
		},
		Spec: v1.PodSpec{
			HostNetwork:     true,
			SecurityContext: &v1.PodSecurityContext{},
			Containers: []v1.Container{
				e2epod.NewAgnhostContainer("busybox-1", mounts, nil),
				e2epod.NewAgnhostContainer("busybox-2", mounts, nil),
			},
			Volumes: []v1.Volume{
				{
					Name: "host-etc-hosts",
					VolumeSource: v1.VolumeSource{
						HostPath: &v1.HostPathVolumeSource{
							Path: etcHostsPath,
							Type: hostPathType,
						},
					},
				},
			},
		},
	}
	return pod
}

func getEtcHostsPath() (string, string) {
	if framework.NodeOSDistroIs("windows") {
		return etcHostsPathWindows, etcHostsOriginalPathWindows
	}
	return etcHostsPathLinux, etcHostsOriginalPathLinux
}
