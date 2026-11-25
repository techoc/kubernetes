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

package node

import (
	"context"
	"fmt"
	"strings"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/kubernetes/test/e2e/framework"
	e2enode "k8s.io/kubernetes/test/e2e/framework/node"
	e2epod "k8s.io/kubernetes/test/e2e/framework/pod"
	"k8s.io/kubernetes/test/e2e/storage/utils"
	imageutils "k8s.io/kubernetes/test/utils/image"
	admissionapi "k8s.io/pod-security-admission/api"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
)

func preparePod(name string, node *v1.Node, propagation *v1.MountPropagationMode, hostDir string) *v1.Pod {
	const containerName = "cntr"
	bTrue := true
	var oneSecond int64 = 1
	// Pod 准备 /mnt/test/<podname> 后进入休眠。
	cmd := fmt.Sprintf("mkdir /mnt/test/%[1]s; sleep 3600", name)
	pod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
		},
		Spec: v1.PodSpec{
			NodeName: node.Name,
			Containers: []v1.Container{
				{
					Name:    containerName,
					Image:   imageutils.GetE2EImage(imageutils.BusyBox),
					Command: []string{"sh", "-c", cmd},
					VolumeMounts: []v1.VolumeMount{
						{
							Name:             "host",
							MountPath:        "/mnt/test",
							MountPropagation: propagation,
						},
					},
					SecurityContext: &v1.SecurityContext{
						Privileged: &bTrue,
					},
				},
			},
			Volumes: []v1.Volume{
				{
					Name: "host",
					VolumeSource: v1.VolumeSource{
						HostPath: &v1.HostPathVolumeSource{
							Path: hostDir,
						},
					},
				},
			},
			// 加速 Pod 终止
			TerminationGracePeriodSeconds: &oneSecond,
		},
	}
	return pod
}

var _ = SIGDescribe("Mount propagation", func() {
	f := framework.NewDefaultFramework("mount-propagation")
	f.NamespacePodSecurityLevel = admissionapi.LevelPrivileged

	ginkgo.It("should propagate mounts within defined scopes", func(ctx context.Context) {
		// 该测试运行两个 Pod：master 和 slave，在共享的 /var/lib/kubelet/XXXX 目录上
		// 分别设置各自的挂载传播。两者都在其子目录挂载 tmpfs，
		// 测试检查这些挂载是否传播到了正确位置。

		hostExec := utils.NewHostExec(f)
		ginkgo.DeferCleanup(hostExec.Cleanup)

		// 选择一个运行所有 Pod 的节点。
		node, err := e2enode.GetRandomReadySchedulableNode(ctx, f.ClientSet)
		framework.ExpectNoError(err)

		// 未设置命名空间时测试失败。命名空间应当唯一，否则可能误删用户数据。
		if len(f.Namespace.Name) == 0 {
			gomega.Expect(f.Namespace.Name).NotTo(gomega.BeEmpty())
			return
		}

		// hostDir 是通过 HostPath 在所有 Pod 间共享的目录。
		// 要足够随机，避免与并行运行的其他测试冲突。
		hostDir := "/var/lib/kubelet/" + f.Namespace.Name
		ginkgo.DeferCleanup(func(ctx context.Context) error {
			cleanCmd := fmt.Sprintf("rm -rf %q", hostDir)
			return hostExec.IssueCommand(ctx, cleanCmd, node)
		})

		podClient := e2epod.NewPodClient(f)
		bidirectional := v1.MountPropagationBidirectional
		master := podClient.CreateSync(ctx, preparePod("master", node, &bidirectional, hostDir))

		hostToContainer := v1.MountPropagationHostToContainer
		slave := podClient.CreateSync(ctx, preparePod("slave", node, &hostToContainer, hostDir))

		none := v1.MountPropagationNone
		private := podClient.CreateSync(ctx, preparePod("private", node, &none, hostDir))
		defaultPropagation := podClient.CreateSync(ctx, preparePod("default", node, nil, hostDir))

		// 检查各 Pod 能否看到彼此的目录；这里只校验 HostPath 相同，不校验挂载传播。
		podNames := []string{master.Name, slave.Name, private.Name, defaultPropagation.Name}
		for _, podName := range podNames {
			for _, dirName := range podNames {
				cmd := fmt.Sprintf("test -d /mnt/test/%s", dirName)
				e2epod.ExecShellInPod(ctx, f, podName, cmd)
			}
		}

		// 每个 Pod 都在 /mnt/test/<podname> 挂载一个 tmpfs 并放入一个文件。
		for _, podName := range podNames {
			cmd := fmt.Sprintf("mount -t tmpfs e2e-mount-propagation-%[1]s /mnt/test/%[1]s; echo %[1]s > /mnt/test/%[1]s/file", podName)
			e2epod.ExecShellInPod(ctx, f, podName, cmd)

			// 测试结束时卸载 tmpfs
			cmd = fmt.Sprintf("umount /mnt/test/%s", podName)
			ginkgo.DeferCleanup(e2epod.ExecShellInPod, f, podName, cmd)
		}

		// 宿主机在 testdir/host 挂载一个 tmpfs 并放入文件，
		// 用于检查从宿主机到 Pod 的挂载传播。
		cmd := fmt.Sprintf("mkdir %[1]q/host; mount -t tmpfs e2e-mount-propagation-host %[1]q/host; echo host > %[1]q/host/file", hostDir)
		err = hostExec.IssueCommand(ctx, cmd, node)
		framework.ExpectNoError(err)

		ginkgo.DeferCleanup(func(ctx context.Context) error {
			cmd := fmt.Sprintf("umount %q/host", hostDir)
			return hostExec.IssueCommand(ctx, cmd, node)
		})

		// 现在检查挂载是否传播到了正确的容器。
		// expectedMounts 是 Pod 名 -> 该 Pod 中应可见挂载的映射。
		expectedMounts := map[string]sets.String{
			// master 只能看到自己的挂载，看不到 slave 的。
			"master": sets.NewString("master", "host"),
			// slave 能看到 master 的挂载和自己的挂载。
			"slave": sets.NewString("master", "slave", "host"),
			// private 只能看到自己的挂载
			"private": sets.NewString("private"),
			// default（等同 private）只能看到自己的挂载
			"default": sets.NewString("default"),
		}
		dirNames := append(podNames, "host")
		for podName, mounts := range expectedMounts {
			for _, mountName := range dirNames {
				cmd := fmt.Sprintf("cat /mnt/test/%s/file", mountName)
				stdout, stderr, err := e2epod.ExecShellInPodWithFullOutput(ctx, f, podName, cmd)
				framework.Logf("pod %s mount %s: stdout: %q, stderr: %q error: %v", podName, mountName, stdout, stderr, err)
				msg := fmt.Sprintf("When checking pod %s and directory %s", podName, mountName)
				shouldBeVisible := mounts.Has(mountName)
				if shouldBeVisible {
					framework.ExpectNoError(err, "%s: failed to run %q", msg, cmd)
					gomega.Expect(stdout).To(gomega.Equal(mountName), msg)
				} else {
					// 这里*期望* cat 返回错误
					gomega.Expect(err).To(gomega.HaveOccurred(), msg)
				}
			}
		}

		// 查找 kubelet PID，确保操作的是 kubelet 的 mount namespace
		kubeletPid := pidOfKubelet(ctx, hostExec, node)
		enterKubeletMountNS := fmt.Sprintf("nsenter -t %d -m", kubeletPid)

		// 检查 master 和宿主机的挂载是否传播到了容器运行时的 mount namespace
		for _, mountName := range []string{"host", master.Name} {
			cmd := fmt.Sprintf("%s cat \"%s/%s/file\"", enterKubeletMountNS, hostDir, mountName)
			framework.Logf("running command '%s' on node '%s'", cmd, node.Name)
			output, err := hostExec.IssueCommandWithResult(ctx, cmd, node)
			framework.ExpectNoError(err, "host container namespace should see mount from %s: %s", mountName, output)
			output = strings.TrimSuffix(output, "\n")
			gomega.Expect(output).To(gomega.Equal(mountName), "host container namespace should see mount contents from %s", mountName)
		}

		// 检查 slave、private 和 default 的挂载没有传播到容器运行时的 mount namespace
		for _, podName := range []string{slave.Name, private.Name, defaultPropagation.Name} {
			cmd := fmt.Sprintf("%s test ! -e \"%s/%s/file\"", enterKubeletMountNS, hostDir, podName)
			framework.Logf("running command '%s' on node '%s'", cmd, node.Name)
			output, err := hostExec.IssueCommandWithResult(ctx, cmd, node)
			framework.ExpectNoError(err, "host container namespace shouldn't see mount from %s: %s", podName, output)
		}
	})
})
