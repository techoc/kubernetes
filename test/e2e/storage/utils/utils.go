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

package utils

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"math"
	"math/rand"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"

	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/client-go/dynamic"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/kubernetes/test/e2e/framework"
	e2epod "k8s.io/kubernetes/test/e2e/framework/pod"
	e2essh "k8s.io/kubernetes/test/e2e/framework/ssh"
	e2evolume "k8s.io/kubernetes/test/e2e/framework/volume"
	imageutils "k8s.io/kubernetes/test/utils/image"
)

// KubeletOpt 类型定义
type KubeletOpt string

const (
	// NodeStateTimeout 定义超时
	NodeStateTimeout = 1 * time.Minute
	// KStart 定义启动值
	KStart KubeletOpt = "start"
	// KStop 定义停止值
	KStop KubeletOpt = "stop"
	// KRestart 定义重启值
	KRestart     KubeletOpt = "restart"
	minValidSize string     = "1Ki"
	maxValidSize string     = "10Ei"
)

// VerifyFSGroupInPod 验证传入的 filePath 包含预期的 FSGroup
func VerifyFSGroupInPod(ctx context.Context, f *framework.Framework, filePath, expectedFSGroup string, pod *v1.Pod) {
	cmd := fmt.Sprintf("ls -l %s", filePath)
	stdout, stderr, err := e2epod.ExecShellInPodWithFullOutput(ctx, f, pod.Name, cmd)
	framework.ExpectNoError(err)
	framework.Logf("pod %s/%s exec for cmd %s, stdout: %s, stderr: %s", pod.Namespace, pod.Name, cmd, stdout, stderr)
	fsGroupResult := strings.Fields(stdout)[3]
	gomega.Expect(expectedFSGroup).To(gomega.Equal(fsGroupResult), "Expected fsGroup of %s, got %s", expectedFSGroup, fsGroupResult)
}

// getKubeletRunning 返回 kubelet 是否正在运行
func getKubeletRunning(ctx context.Context, nodeIP string) bool {
	command := "systemctl show kubelet --property ActiveState --value"
	framework.Logf("Attempting `%s`", command)
	sshResult, err := e2essh.SSH(ctx, command, nodeIP, framework.TestContext.Provider)
	framework.ExpectNoError(err, fmt.Sprintf("SSH to Node %q errored.", nodeIP))
	e2essh.LogResult(sshResult)
	gomega.Expect(sshResult.Code).To(gomega.BeZero(), "Failed to get kubelet status")
	gomega.Expect(sshResult.Stdout).NotTo(gomega.BeEmpty(), "Kubelet status should not be Empty")
	return strings.TrimSpace(sshResult.Stdout) == "active"
}

// TestKubeletRestartsAndRestoresMount 测试 kubelet 重启后挂载到 Pod 的 volume 是否仍保持挂载
func TestKubeletRestartsAndRestoresMount(ctx context.Context, c clientset.Interface, f *framework.Framework, clientPod *v1.Pod, volumePath string) {
	byteLen := 64
	seed := time.Now().UTC().UnixNano()

	ginkgo.By("Writing to the volume.")
	CheckWriteToPath(ctx, f, clientPod, v1.PersistentVolumeFilesystem, false, volumePath, byteLen, seed)

	ginkgo.By("Restarting kubelet")
	KubeletCommand(ctx, KRestart, c, clientPod)

	ginkgo.By("Wait 20s for the volume to become stable")
	time.Sleep(20 * time.Second)

	ginkgo.By("Testing that written file is accessible.")
	CheckReadFromPath(ctx, f, clientPod, v1.PersistentVolumeFilesystem, false, volumePath, byteLen, seed)

	framework.Logf("Volume mount detected on pod %s and written file %s is readable post-restart.", clientPod.Name, volumePath)
}

// TestKubeletRestartsAndRestoresMap 测试 kubelet 重启后映射到 Pod 的 volume 是否仍保持映射
func TestKubeletRestartsAndRestoresMap(ctx context.Context, c clientset.Interface, f *framework.Framework, clientPod *v1.Pod, volumePath string) {
	byteLen := 64
	seed := time.Now().UTC().UnixNano()

	ginkgo.By("Writing to the volume.")
	CheckWriteToPath(ctx, f, clientPod, v1.PersistentVolumeBlock, false, volumePath, byteLen, seed)

	ginkgo.By("Restarting kubelet")
	KubeletCommand(ctx, KRestart, c, clientPod)

	ginkgo.By("Wait 20s for the volume to become stable")
	time.Sleep(20 * time.Second)

	ginkgo.By("Testing that written pv is accessible.")
	CheckReadFromPath(ctx, f, clientPod, v1.PersistentVolumeBlock, false, volumePath, byteLen, seed)

	framework.Logf("Volume map detected on pod %s and written data %s is readable post-restart.", clientPod.Name, volumePath)
}

// TestVolumeUnmountsFromDeletedPodWithForceOption 测试 kubelet 停机期间客户端 Pod 被删除时 volume 是否被卸载。
// forceDelete 为 true 表示 Pod 被强制删除。
// checkSubpath 为 true 表示应检查 subpath。
// 如果设置了 secondPod，它会在 kubelet 停机期间启动，以检查在旧 Pod 被删除、新 Pod 启动期间 volume 是否可用。
func TestVolumeUnmountsFromDeletedPodWithForceOption(ctx context.Context, c clientset.Interface, f *framework.Framework, clientPod *v1.Pod, forceDelete bool, checkSubpath bool, secondPod *v1.Pod, volumePath string) {
	nodeIP, err := getHostAddress(ctx, c, clientPod)
	framework.ExpectNoError(err)
	nodeIP = nodeIP + ":22"

	ginkgo.By("Expecting the volume mount to be found.")
	result, err := e2essh.SSH(ctx, fmt.Sprintf("mount | grep %s | grep -v volume-subpaths", clientPod.UID), nodeIP, framework.TestContext.Provider)
	e2essh.LogResult(result)
	framework.ExpectNoError(err, "Encountered SSH error.")
	gomega.Expect(result.Code).To(gomega.Equal(0), fmt.Sprintf("Expected grep exit code of 0, got %d", result.Code))

	if checkSubpath {
		ginkgo.By("Expecting the volume subpath mount to be found.")
		result, err := e2essh.SSH(ctx, fmt.Sprintf("cat /proc/self/mountinfo | grep %s | grep volume-subpaths", clientPod.UID), nodeIP, framework.TestContext.Provider)
		e2essh.LogResult(result)
		framework.ExpectNoError(err, "Encountered SSH error.")
		gomega.Expect(result.Code).To(gomega.Equal(0), fmt.Sprintf("Expected grep exit code of 0, got %d", result.Code))
	}

	ginkgo.By("Writing to the volume.")
	byteLen := 64
	seed := time.Now().UTC().UnixNano()
	CheckWriteToPath(ctx, f, clientPod, v1.PersistentVolumeFilesystem, false, volumePath, byteLen, seed)

	// 此命令用于确保测试结束后无论成功失败都启动 kubelet。
	ginkgo.DeferCleanup(KubeletCommand, KStart, c, clientPod)
	ginkgo.By("Stopping the kubelet.")
	KubeletCommand(ctx, KStop, c, clientPod)

	if secondPod != nil {
		ginkgo.By("Starting the second pod")
		_, err = c.CoreV1().Pods(clientPod.Namespace).Create(context.TODO(), secondPod, metav1.CreateOptions{})
		framework.ExpectNoError(err, "when starting the second pod")
	}

	ginkgo.By(fmt.Sprintf("Deleting Pod %q", clientPod.Name))
	if forceDelete {
		err = c.CoreV1().Pods(clientPod.Namespace).Delete(ctx, clientPod.Name, *metav1.NewDeleteOptions(0))
	} else {
		err = c.CoreV1().Pods(clientPod.Namespace).Delete(ctx, clientPod.Name, metav1.DeleteOptions{})
	}
	framework.ExpectNoError(err)

	ginkgo.By("Starting the kubelet and waiting for pod to delete.")
	KubeletCommand(ctx, KStart, c, clientPod)
	err = e2epod.WaitForPodNotFoundInNamespace(ctx, f.ClientSet, clientPod.Name, f.Namespace.Name, f.Timeouts.PodDelete)
	if err != nil {
		framework.ExpectNoError(err, "Expected pod to be not found.")
	}

	if forceDelete {
		// 使用 forceDelete 时，Pod 会立即从 API server 删除，无法确定 volume 何时被拆除，
		// 因此等待一段时间让操作完成
		time.Sleep(30 * time.Second)
	}

	if secondPod != nil {
		ginkgo.By("Waiting for the second pod.")
		err = e2epod.WaitForPodRunningInNamespace(ctx, c, secondPod)
		framework.ExpectNoError(err, "while waiting for the second pod Running")

		ginkgo.By("Getting the second pod uuid.")
		secondPod, err := c.CoreV1().Pods(secondPod.Namespace).Get(context.TODO(), secondPod.Name, metav1.GetOptions{})
		framework.ExpectNoError(err, "getting the second UID")

		ginkgo.By("Expecting the volume mount to be found in the second pod.")
		result, err := e2essh.SSH(ctx, fmt.Sprintf("mount | grep %s | grep -v volume-subpaths", secondPod.UID), nodeIP, framework.TestContext.Provider)
		e2essh.LogResult(result)
		framework.ExpectNoError(err, "Encountered SSH error when checking the second pod.")
		gomega.Expect(result.Code).To(gomega.Equal(0), fmt.Sprintf("Expected grep exit code of 0, got %d", result.Code))

		ginkgo.By("Testing that written file is accessible in the second pod.")
		CheckReadFromPath(ctx, f, secondPod, v1.PersistentVolumeFilesystem, false, volumePath, byteLen, seed)
		err = c.CoreV1().Pods(secondPod.Namespace).Delete(context.TODO(), secondPod.Name, metav1.DeleteOptions{})
		framework.ExpectNoError(err, "when deleting the second pod")
		err = e2epod.WaitForPodNotFoundInNamespace(ctx, f.ClientSet, secondPod.Name, f.Namespace.Name, f.Timeouts.PodDelete)
		framework.ExpectNoError(err, "when waiting for the second pod to disappear")
	}

	ginkgo.By("Expecting the volume mount not to be found.")
	result, err = e2essh.SSH(ctx, fmt.Sprintf("mount | grep %s | grep -v volume-subpaths", clientPod.UID), nodeIP, framework.TestContext.Provider)
	e2essh.LogResult(result)
	framework.ExpectNoError(err, "Encountered SSH error.")
	gomega.Expect(result.Stdout).To(gomega.BeEmpty(), "Expected grep stdout to be empty (i.e. no mount found).")
	framework.Logf("Volume unmounted on node %s", clientPod.Spec.NodeName)

	if checkSubpath {
		ginkgo.By("Expecting the volume subpath mount not to be found.")
		result, err = e2essh.SSH(ctx, fmt.Sprintf("cat /proc/self/mountinfo | grep %s | grep volume-subpaths", clientPod.UID), nodeIP, framework.TestContext.Provider)
		e2essh.LogResult(result)
		framework.ExpectNoError(err, "Encountered SSH error.")
		gomega.Expect(result.Stdout).To(gomega.BeEmpty(), "Expected grep stdout to be empty (i.e. no subpath mount found).")
		framework.Logf("Subpath volume unmounted on node %s", clientPod.Spec.NodeName)
	}

}

// TestVolumeUnmountsFromDeletedPod 测试 kubelet 停机期间客户端 Pod 被删除时 volume 是否被卸载。
func TestVolumeUnmountsFromDeletedPod(ctx context.Context, c clientset.Interface, f *framework.Framework, clientPod *v1.Pod, volumePath string) {
	TestVolumeUnmountsFromDeletedPodWithForceOption(ctx, c, f, clientPod, false, false, nil, volumePath)
}

// TestVolumeUnmountsFromForceDeletedPod 测试 kubelet 停机期间客户端 Pod 被强制删除时 volume 是否被卸载。
func TestVolumeUnmountsFromForceDeletedPod(ctx context.Context, c clientset.Interface, f *framework.Framework, clientPod *v1.Pod, volumePath string) {
	TestVolumeUnmountsFromDeletedPodWithForceOption(ctx, c, f, clientPod, true, false, nil, volumePath)
}

// TestVolumeUnmapsFromDeletedPodWithForceOption 测试 kubelet 停机期间客户端 Pod 被删除时 volume 是否被解除映射。
// forceDelete 为 true 表示 Pod 被强制删除。
func TestVolumeUnmapsFromDeletedPodWithForceOption(ctx context.Context, c clientset.Interface, f *framework.Framework, clientPod *v1.Pod, forceDelete bool, devicePath string) {
	nodeIP, err := getHostAddress(ctx, c, clientPod)
	framework.ExpectNoError(err, "Failed to get nodeIP.")
	nodeIP = nodeIP + ":22"

	// 创建用于检查路径是否存在的命令
	podDirectoryCmd := fmt.Sprintf("ls /var/lib/kubelet/pods/%s/volumeDevices/*/ | grep '.'", clientPod.UID)
	if isSudoPresent(ctx, nodeIP, framework.TestContext.Provider) {
		podDirectoryCmd = fmt.Sprintf("sudo sh -c \"%s\"", podDirectoryCmd)
	}
	// global 目录中的目录名不可预测，但设备符号链接
	// 的名称与 pod.UID 相同。因此只需查找名称为 pod.UID 的项。
	globalBlockDirectoryCmd := fmt.Sprintf("find /var/lib/kubelet/plugins -name %s", clientPod.UID)
	if isSudoPresent(ctx, nodeIP, framework.TestContext.Provider) {
		globalBlockDirectoryCmd = fmt.Sprintf("sudo sh -c \"%s\"", globalBlockDirectoryCmd)
	}

	ginkgo.By("Expecting the symlinks from PodDeviceMapPath to be found.")
	result, err := e2essh.SSH(ctx, podDirectoryCmd, nodeIP, framework.TestContext.Provider)
	e2essh.LogResult(result)
	framework.ExpectNoError(err, "Encountered SSH error.")
	gomega.Expect(result.Code).To(gomega.Equal(0), fmt.Sprintf("Expected grep exit code of 0, got %d", result.Code))

	ginkgo.By("Expecting the symlinks from global map path to be found.")
	result, err = e2essh.SSH(ctx, globalBlockDirectoryCmd, nodeIP, framework.TestContext.Provider)
	e2essh.LogResult(result)
	framework.ExpectNoError(err, "Encountered SSH error.")
	gomega.Expect(result.Code).To(gomega.Equal(0), fmt.Sprintf("Expected find exit code of 0, got %d", result.Code))

	// 此命令用于确保测试结束后无论成功失败都启动 kubelet。
	ginkgo.DeferCleanup(KubeletCommand, KStart, c, clientPod)
	ginkgo.By("Stopping the kubelet.")
	KubeletCommand(ctx, KStop, c, clientPod)

	ginkgo.By(fmt.Sprintf("Deleting Pod %q", clientPod.Name))
	if forceDelete {
		err = c.CoreV1().Pods(clientPod.Namespace).Delete(ctx, clientPod.Name, *metav1.NewDeleteOptions(0))
	} else {
		err = c.CoreV1().Pods(clientPod.Namespace).Delete(ctx, clientPod.Name, metav1.DeleteOptions{})
	}
	framework.ExpectNoError(err, "Failed to delete pod.")

	ginkgo.By("Starting the kubelet and waiting for pod to delete.")
	KubeletCommand(ctx, KStart, c, clientPod)
	err = e2epod.WaitForPodNotFoundInNamespace(ctx, f.ClientSet, clientPod.Name, f.Namespace.Name, f.Timeouts.PodDelete)
	framework.ExpectNoError(err, "Expected pod to be not found.")

	if forceDelete {
		// 使用 forceDelete 时，Pod 会立即从 API server 删除，无法确定 volume 何时被拆除，
		// 因此等待一段时间让操作完成
		time.Sleep(30 * time.Second)
	}

	ginkgo.By("Expecting the symlink from PodDeviceMapPath not to be found.")
	result, err = e2essh.SSH(ctx, podDirectoryCmd, nodeIP, framework.TestContext.Provider)
	e2essh.LogResult(result)
	framework.ExpectNoError(err, "Encountered SSH error.")
	gomega.Expect(result.Stdout).To(gomega.BeEmpty(), "Expected grep stdout to be empty.")

	ginkgo.By("Expecting the symlinks from global map path not to be found.")
	result, err = e2essh.SSH(ctx, globalBlockDirectoryCmd, nodeIP, framework.TestContext.Provider)
	e2essh.LogResult(result)
	framework.ExpectNoError(err, "Encountered SSH error.")
	gomega.Expect(result.Stdout).To(gomega.BeEmpty(), "Expected find stdout to be empty.")

	framework.Logf("Volume unmaped on node %s", clientPod.Spec.NodeName)
}

// TestVolumeUnmapsFromDeletedPod 测试 kubelet 停机期间客户端 Pod 被删除时 volume 是否被解除映射。
func TestVolumeUnmapsFromDeletedPod(ctx context.Context, c clientset.Interface, f *framework.Framework, clientPod *v1.Pod, devicePath string) {
	TestVolumeUnmapsFromDeletedPodWithForceOption(ctx, c, f, clientPod, false, devicePath)
}

// TestVolumeUnmapsFromForceDeletedPod 测试 kubelet 停机期间客户端 Pod 被强制删除时 volume 是否被解除映射。
func TestVolumeUnmapsFromForceDeletedPod(ctx context.Context, c clientset.Interface, f *framework.Framework, clientPod *v1.Pod, devicePath string) {
	TestVolumeUnmapsFromDeletedPodWithForceOption(ctx, c, f, clientPod, true, devicePath)
}

// RunInPodWithVolume 在 Pod 中运行命令，并将给定 claim 挂载到 /mnt 目录。
func RunInPodWithVolume(ctx context.Context, c clientset.Interface, t *framework.TimeoutContext, ns, claimName, command string) {
	pod := &v1.Pod{
		TypeMeta: metav1.TypeMeta{
			Kind:       "Pod",
			APIVersion: "v1",
		},
		ObjectMeta: metav1.ObjectMeta{
			GenerateName: "pvc-volume-tester-",
		},
		Spec: v1.PodSpec{
			Containers: []v1.Container{
				{
					Name:    "volume-tester",
					Image:   imageutils.GetE2EImage(imageutils.BusyBox),
					Command: []string{"/bin/sh"},
					Args:    []string{"-c", command},
					VolumeMounts: []v1.VolumeMount{
						{
							Name:      "my-volume",
							MountPath: "/mnt/test",
						},
					},
				},
			},
			RestartPolicy: v1.RestartPolicyNever,
			Volumes: []v1.Volume{
				{
					Name: "my-volume",
					VolumeSource: v1.VolumeSource{
						PersistentVolumeClaim: &v1.PersistentVolumeClaimVolumeSource{
							ClaimName: claimName,
							ReadOnly:  false,
						},
					},
				},
			},
		},
	}
	pod, err := c.CoreV1().Pods(ns).Create(ctx, pod, metav1.CreateOptions{})
	framework.ExpectNoError(err, "Failed to create pod: %v", err)
	ginkgo.DeferCleanup(e2epod.DeletePodOrFail, c, ns, pod.Name)
	framework.ExpectNoError(e2epod.WaitForPodSuccessInNamespaceTimeout(ctx, c, pod.Name, pod.Namespace, t.PodStartSlow))
}

// StartExternalProvisioner 创建 external provisioner Pod
func StartExternalProvisioner(ctx context.Context, c clientset.Interface, ns string, externalPluginName string) *v1.Pod {
	podClient := c.CoreV1().Pods(ns)

	provisionerPod := &v1.Pod{
		TypeMeta: metav1.TypeMeta{
			Kind:       "Pod",
			APIVersion: "v1",
		},
		ObjectMeta: metav1.ObjectMeta{
			GenerateName: "external-provisioner-",
		},

		Spec: v1.PodSpec{
			Containers: []v1.Container{
				{
					Name:  "nfs-provisioner",
					Image: imageutils.GetE2EImage(imageutils.NFSProvisioner),
					SecurityContext: &v1.SecurityContext{
						Capabilities: &v1.Capabilities{
							Add: []v1.Capability{"DAC_READ_SEARCH"},
						},
					},
					Args: []string{
						"-provisioner=" + externalPluginName,
						"-grace-period=0",
					},
					Ports: []v1.ContainerPort{
						{Name: "nfs", ContainerPort: 2049},
						{Name: "mountd", ContainerPort: 20048},
						{Name: "rpcbind", ContainerPort: 111},
						{Name: "rpcbind-udp", ContainerPort: 111, Protocol: v1.ProtocolUDP},
					},
					Env: []v1.EnvVar{
						{
							Name: "POD_IP",
							ValueFrom: &v1.EnvVarSource{
								FieldRef: &v1.ObjectFieldSelector{
									FieldPath: "status.podIP",
								},
							},
						},
					},
					ImagePullPolicy: v1.PullIfNotPresent,
					VolumeMounts: []v1.VolumeMount{
						{
							Name:      "export-volume",
							MountPath: "/export",
						},
					},
				},
			},
			Volumes: []v1.Volume{
				{
					Name: "export-volume",
					VolumeSource: v1.VolumeSource{
						EmptyDir: &v1.EmptyDirVolumeSource{},
					},
				},
			},
		},
	}
	provisionerPod, err := podClient.Create(ctx, provisionerPod, metav1.CreateOptions{})
	framework.ExpectNoError(err, "Failed to create %s pod: %v", provisionerPod.Name, err)

	framework.ExpectNoError(e2epod.WaitForPodRunningInNamespace(ctx, c, provisionerPod))

	ginkgo.By("locating the provisioner pod")
	pod, err := podClient.Get(ctx, provisionerPod.Name, metav1.GetOptions{})
	framework.ExpectNoError(err, "Cannot locate the provisioner pod %v: %v", provisionerPod.Name, err)

	return pod
}

func isSudoPresent(ctx context.Context, nodeIP string, provider string) bool {
	framework.Logf("Checking if sudo command is present")
	sshResult, err := e2essh.SSH(ctx, "sudo --version", nodeIP, provider)
	framework.ExpectNoError(err, "SSH to %q errored.", nodeIP)
	if !strings.Contains(sshResult.Stderr, "command not found") {
		return true
	}
	return false
}

// CheckReadWriteToPath 检查路径是否可读写
func CheckReadWriteToPath(ctx context.Context, f *framework.Framework, pod *v1.Pod, volMode v1.PersistentVolumeMode, path string) {
	if volMode == v1.PersistentVolumeBlock {
		// random -> file1
		err := e2epod.VerifyExecInPodSucceed(ctx, f, pod, "dd if=/dev/urandom of=/tmp/file1 bs=64 count=1")
		framework.ExpectNoError(err, "Failed to write to file1")
		// file1 -> dev（写入 dev）
		err = e2epod.VerifyExecInPodSucceed(ctx, f, pod, fmt.Sprintf("dd if=/tmp/file1 of=%s bs=64 count=1", path))
		framework.ExpectNoError(err, "Failed to write to block volume")
		// dev -> file2（从 dev 读取）
		err = e2epod.VerifyExecInPodSucceed(ctx, f, pod, fmt.Sprintf("dd if=%s of=/tmp/file2 bs=64 count=1", path))
		framework.ExpectNoError(err, "Failed to read from block volume")
		// file1 == file2（检查内容）
		err = e2epod.VerifyExecInPodSucceed(ctx, f, pod, "diff /tmp/file1 /tmp/file2")
		framework.ExpectNoError(err, "Failed to compare file1 and file2")
		// 清理临时文件
		err = e2epod.VerifyExecInPodSucceed(ctx, f, pod, "rm -f /tmp/file1 /tmp/file2")
		framework.ExpectNoError(err, "Failed to clean up temp files")

		// 检查向块 volume 写入文件是否失败
		err = e2epod.VerifyExecInPodFail(ctx, f, pod, fmt.Sprintf("echo 'Hello world.' > %s/file1.txt", path), 1)
		framework.ExpectNoError(err, "Expected write to block volume to fail")
	} else {
		// text -> file1（写入文件）
		err := e2epod.VerifyExecInPodSucceed(ctx, f, pod, fmt.Sprintf("echo 'Hello world.' > %s/file1.txt", path))
		framework.ExpectNoError(err, "Failed to write to file1")
		// grep file1（读取文件并检查内容）
		err = e2epod.VerifyExecInPodSucceed(ctx, f, pod, readFile("Hello word.", path))
		framework.ExpectNoError(err, "Failed to read from file1")
		// 检查将目录作为块 volume 写入是否失败
		err = e2epod.VerifyExecInPodFail(ctx, f, pod, fmt.Sprintf("dd if=/dev/urandom of=%s bs=64 count=1", path), 1)
		framework.ExpectNoError(err, "Expected write to directory to fail")
	}
}

func readFile(content, path string) string {
	if framework.NodeOSDistroIs("windows") {
		return fmt.Sprintf("Select-String '%s' %s/file1.txt", content, path)
	}
	return fmt.Sprintf("grep 'Hello world.' %s/file1.txt", path)
}

// genBinDataFromSeed 用随机种子生成 binData
func genBinDataFromSeed(len int, seed int64) []byte {
	binData := make([]byte, len)
	r := rand.New(rand.NewSource(seed))

	_, err := r.Read(binData)
	if err != nil {
		fmt.Printf("Error: %v\n", err)
	}

	return binData
}

// CheckReadFromPath 验证文件是否可被正确读取。
//
// 注意：directIO 不适用于默认的 BusyBox Pod。directIO 正常工作的条件是：
// 对块模式 PVC 读取完整扇区（通常一个扇区 512 字节），
// 或对文件读取内存页（通常
// 4096 字节）。
func CheckReadFromPath(ctx context.Context, f *framework.Framework, pod *v1.Pod, volMode v1.PersistentVolumeMode, directIO bool, path string, len int, seed int64) {
	var pathForVolMode string
	var iflag string

	if volMode == v1.PersistentVolumeBlock {
		pathForVolMode = path
	} else {
		pathForVolMode = filepath.Join(path, "file1.txt")
	}

	if directIO {
		iflag = "iflag=direct"
	}

	sum := sha256.Sum256(genBinDataFromSeed(len, seed))

	err := e2epod.VerifyExecInPodSucceed(ctx, f, pod, fmt.Sprintf("dd if=%s %s bs=%d count=1 | sha256sum", pathForVolMode, iflag, len))
	framework.ExpectNoError(err, "Failed to read from %s", pathForVolMode)
	err = e2epod.VerifyExecInPodSucceed(ctx, f, pod, fmt.Sprintf("dd if=%s %s bs=%d count=1 | sha256sum | grep -Fq %x", pathForVolMode, iflag, len, sum))
	framework.ExpectNoError(err, "Failed to read from %s", pathForVolMode)
}

// CheckWriteToPath 验证文件是否可被正确写入。
//
// 注意：nocache 不适用于默认的 BusyBox Pod。要无缓存读取，
// 请在 CheckReadFromPath 中启用 directIO，并查看关于长度要求的提示。
func CheckWriteToPath(ctx context.Context, f *framework.Framework, pod *v1.Pod, volMode v1.PersistentVolumeMode, nocache bool, path string, len int, seed int64) {
	var pathForVolMode string
	var oflag string

	if volMode == v1.PersistentVolumeBlock {
		pathForVolMode = path
	} else {
		pathForVolMode = filepath.Join(path, "file1.txt")
	}

	if nocache {
		oflag = "oflag=nocache"
	}

	encoded := base64.StdEncoding.EncodeToString(genBinDataFromSeed(len, seed))

	err := e2epod.VerifyExecInPodSucceed(ctx, f, pod, fmt.Sprintf("echo %s | base64 -d | sha256sum", encoded))
	framework.ExpectNoError(err, "Failed to generate sha256sum of encoded data")
	err = e2epod.VerifyExecInPodSucceed(ctx, f, pod, fmt.Sprintf("echo %s | base64 -d | dd of=%s %s bs=%d count=1", encoded, pathForVolMode, oflag, len))
	framework.ExpectNoError(err, "Failed to write to %s", pathForVolMode)
}

// GetSectorSize 返回设备的扇区大小。
func GetSectorSize(ctx context.Context, f *framework.Framework, pod *v1.Pod, device string) int {
	stdout, _, err := e2epod.ExecShellInPodWithFullOutput(ctx, f, pod.Name, fmt.Sprintf("blockdev --getss %s", device))
	framework.ExpectNoError(err, "Failed to get sector size of %s", device)
	ss, err := strconv.Atoi(stdout)
	framework.ExpectNoError(err, "Sector size returned by blockdev command isn't integer value.")

	return ss
}

// findMountPoints 返回给定节点上指定目录下的所有挂载点。
func findMountPoints(ctx context.Context, hostExec HostExec, node *v1.Node, dir string) []string {
	result, err := hostExec.IssueCommandWithResult(ctx, fmt.Sprintf(`find %s -type d -exec mountpoint {} \; | grep 'is a mountpoint$' || true`, dir), node)
	framework.ExpectNoError(err, "Encountered HostExec error.")
	var mountPoints []string
	if err != nil {
		for _, line := range strings.Split(result, "\n") {
			if line == "" {
				continue
			}
			mountPoints = append(mountPoints, strings.TrimSuffix(line, " is a mountpoint"))
		}
	}
	return mountPoints
}

// FindVolumeGlobalMountPoints 返回给定 Pod 所在节点上的所有 volume 全局挂载点。
func FindVolumeGlobalMountPoints(ctx context.Context, hostExec HostExec, node *v1.Node) sets.String {
	return sets.NewString(findMountPoints(ctx, hostExec, node, "/var/lib/kubelet/plugins")...)
}

// CreateDriverNamespace 为 CSI driver 安装创建一个命名空间。
// 该命名空间仍会被跟踪，并确保测试结束时被删除。
func CreateDriverNamespace(ctx context.Context, f *framework.Framework) *v1.Namespace {
	ginkgo.By(fmt.Sprintf("Building a driver namespace object, basename %s", f.Namespace.Name))
	// driver 命名空间将在前缀上与测试命名空间绑定
	namespace, err := f.CreateNamespace(ctx, f.Namespace.Name, map[string]string{
		"e2e-framework":      f.BaseName,
		"e2e-test-namespace": f.Namespace.Name,
	})
	framework.ExpectNoError(err)

	if framework.TestContext.VerifyServiceAccount {
		ginkgo.By("Waiting for a default service account to be provisioned in namespace")
		err = framework.WaitForDefaultServiceAccountInNamespace(ctx, f.ClientSet, namespace.Name)
		framework.ExpectNoError(err)
	} else {
		framework.Logf("Skipping waiting for service account")
	}
	return namespace
}

// WaitForGVRDeletion 等待非命名空间级对象被删除
func WaitForGVRDeletion(ctx context.Context, c dynamic.Interface, gvr schema.GroupVersionResource, objectName string, poll, timeout time.Duration) error {
	framework.Logf("Waiting up to %v for %s %s to be deleted", timeout, gvr.Resource, objectName)

	if successful := WaitUntil(poll, timeout, func() bool {
		_, err := c.Resource(gvr).Get(ctx, objectName, metav1.GetOptions{})
		if err != nil && apierrors.IsNotFound(err) {
			framework.Logf("%s %v is not found and has been deleted", gvr.Resource, objectName)
			return true
		} else if err != nil {
			framework.Logf("Get %s returned an error: %v", objectName, err.Error())
		} else {
			framework.Logf("%s %v has been found and is not deleted", gvr.Resource, objectName)
		}

		return false
	}); successful {
		return nil
	}

	return fmt.Errorf("%s %s is not deleted within %v", gvr.Resource, objectName, timeout)
}

// EnsureGVRDeletion 检查给定时间段内是否从未找到由 group/version/kind 和 name 定义的对象
func EnsureGVRDeletion(ctx context.Context, c dynamic.Interface, gvr schema.GroupVersionResource, objectName string, poll, timeout time.Duration, namespace string) error {
	var resourceClient dynamic.ResourceInterface
	if namespace != "" {
		resourceClient = c.Resource(gvr).Namespace(namespace)
	} else {
		resourceClient = c.Resource(gvr)
	}

	err := framework.Gomega().Eventually(ctx, func(ctx context.Context) error {
		_, err := resourceClient.Get(ctx, objectName, metav1.GetOptions{})
		return err
	}).WithTimeout(timeout).WithPolling(poll).Should(gomega.MatchError(apierrors.IsNotFound, fmt.Sprintf("failed to delete %s %s", gvr, objectName)))
	return err
}

// EnsureNoGVRDeletion 检查由 group/version/kind 和 name 定义的对象在给定时间段内未被删除
func EnsureNoGVRDeletion(ctx context.Context, c dynamic.Interface, gvr schema.GroupVersionResource, objectName string, poll, timeout time.Duration, namespace string) error {
	var resourceClient dynamic.ResourceInterface
	if namespace != "" {
		resourceClient = c.Resource(gvr).Namespace(namespace)
	} else {
		resourceClient = c.Resource(gvr)
	}
	err := framework.Gomega().Consistently(ctx, func(ctx context.Context) error {
		_, err := resourceClient.Get(ctx, objectName, metav1.GetOptions{})
		if err != nil {
			return fmt.Errorf("failed to get %s %s: %w", gvr.Resource, objectName, err)
		}
		return nil
	}).WithTimeout(timeout).WithPolling(poll).Should(gomega.Succeed())
	return err
}

// WaitForNamespacedGVRDeletion 等待命名空间级对象被删除
func WaitForNamespacedGVRDeletion(ctx context.Context, c dynamic.Interface, gvr schema.GroupVersionResource, ns, objectName string, poll, timeout time.Duration) error {
	framework.Logf("Waiting up to %v for %s %s to be deleted", timeout, gvr.Resource, objectName)

	if successful := WaitUntil(poll, timeout, func() bool {
		_, err := c.Resource(gvr).Namespace(ns).Get(ctx, objectName, metav1.GetOptions{})
		if err != nil && apierrors.IsNotFound(err) {
			framework.Logf("%s %s is not found in namespace %s and has been deleted", gvr.Resource, objectName, ns)
			return true
		} else if err != nil {
			framework.Logf("Get %s in namespace %s returned an error: %v", objectName, ns, err.Error())
		} else {
			framework.Logf("%s %s has been found in namespace %s and is not deleted", gvr.Resource, objectName, ns)
		}

		return false
	}); successful {
		return nil
	}

	return fmt.Errorf("%s %s in namespace %s is not deleted within %v", gvr.Resource, objectName, ns, timeout)
}

// WaitForOwnedResourcesDeleted 等待命名空间中（若 ns=="" 则为集群级）
// 具有指定 ownerUID 的给定 GVR 的所有对象被删除。
func WaitForOwnedResourcesDeleted(ctx context.Context,
	c dynamic.Interface,
	gvr schema.GroupVersionResource,
	ns string, // 集群级资源为空
	ownerUID types.UID,
	poll, timeout time.Duration) error {
	scope := "cluster-scoped"
	if ns != "" {
		scope = fmt.Sprintf("namespace %s", ns)
	}
	framework.Logf("Waiting up to %v for all %s %s owned by %s to be deleted", timeout, gvr.Resource, scope, ownerUID)

	successful := WaitUntil(poll, timeout, func() bool {
		var list *unstructured.UnstructuredList
		var err error
		if ns == "" {
			list, err = c.Resource(gvr).List(ctx, metav1.ListOptions{})
		} else {
			list, err = c.Resource(gvr).Namespace(ns).List(ctx, metav1.ListOptions{})
		}
		if err != nil {
			framework.Logf("List %s %s returned error: %v", gvr.Resource, scope, err)
			return false
		}

		for _, item := range list.Items {
			for _, owner := range item.GetOwnerReferences() {
				if owner.UID == ownerUID {
					framework.Logf("%s %s is still present, waiting...", gvr.Resource, item.GetName())
					return false
				}
			}
		}

		framework.Logf("All %s %s owned by %s have been deleted", gvr.Resource, scope, ownerUID)
		return true
	})

	if successful {
		return nil
	}
	return fmt.Errorf("some %s %s owned by %s were not deleted within %v", gvr.Resource, scope, ownerUID, timeout)
}

// FilterResourcesByOwner 过滤 unstructured 资源，只保留由指定 owner kind 和 UID 拥有的资源
func FilterResourcesByOwner(resources []unstructured.Unstructured, ownerKind string, ownerUID types.UID) []unstructured.Unstructured {
	var filtered []unstructured.Unstructured
	for _, resource := range resources {
		for _, owner := range resource.GetOwnerReferences() {
			if owner.Kind == ownerKind && owner.UID == ownerUID {
				filtered = append(filtered, resource)
				break
			}
		}
	}
	return filtered
}

// WaitUntil 反复运行 checkDone 直到超时
func WaitUntil(poll, timeout time.Duration, checkDone func() bool) bool {
	// TODO (pohly): 用 gomega.Eventually 替换
	for start := time.Now(); time.Since(start) < timeout; time.Sleep(poll) {
		if checkDone() {
			framework.Logf("WaitUntil finished successfully after %v", time.Since(start))
			return true
		}
	}

	framework.Logf("WaitUntil failed after reaching the timeout %v", timeout)
	return false
}

// WaitForGVRFinalizer 等待给定 GVR 的对象包含 finalizer
// 如果命名空间为空，则假定它是非命名空间级对象
func WaitForGVRFinalizer(ctx context.Context, c dynamic.Interface, gvr schema.GroupVersionResource, objectName, objectNamespace, finalizer string, poll, timeout time.Duration) error {
	framework.Logf("Waiting up to %v for object %s %s of resource %s to contain finalizer %s", timeout, objectNamespace, objectName, gvr.Resource, finalizer)
	var (
		err      error
		resource *unstructured.Unstructured
	)
	if successful := WaitUntil(poll, timeout, func() bool {
		switch objectNamespace {
		case "":
			resource, err = c.Resource(gvr).Get(ctx, objectName, metav1.GetOptions{})
		default:
			resource, err = c.Resource(gvr).Namespace(objectNamespace).Get(ctx, objectName, metav1.GetOptions{})
		}
		if err != nil {
			framework.Logf("Failed to get object %s %s with err: %v. Will retry in %v", objectNamespace, objectName, err, timeout)
			return false
		}
		for _, f := range resource.GetFinalizers() {
			if f == finalizer {
				return true
			}
		}
		return false
	}); successful {
		return nil
	}
	if err == nil {
		err = fmt.Errorf("finalizer %s not added to object %s %s of resource %s", finalizer, objectNamespace, objectName, gvr)
	}
	return err
}

// VerifyFilePathGIDInPod 验证目标文件路径的预期 GID
func VerifyFilePathGIDInPod(ctx context.Context, f *framework.Framework, filePath, expectedGID string, pod *v1.Pod) {
	cmd := fmt.Sprintf("ls -l %s", filePath)
	stdout, stderr, err := e2epod.ExecShellInPodWithFullOutput(ctx, f, pod.Name, cmd)
	framework.ExpectNoError(err)
	framework.Logf("pod %s/%s exec for cmd %s, stdout: %s, stderr: %s", pod.Namespace, pod.Name, cmd, stdout, stderr)
	ll := strings.Fields(stdout)
	framework.Logf("stdout split: %v, expected gid: %v", ll, expectedGID)
	gomega.Expect(ll[3]).To(gomega.Equal(expectedGID))
}

// ChangeFilePathGIDInPod 更改目标文件路径的 GID。
func ChangeFilePathGIDInPod(ctx context.Context, f *framework.Framework, filePath, targetGID string, pod *v1.Pod) {
	cmd := fmt.Sprintf("chgrp %s %s", targetGID, filePath)
	_, _, err := e2epod.ExecShellInPodWithFullOutput(ctx, f, pod.Name, cmd)
	framework.ExpectNoError(err)
	VerifyFilePathGIDInPod(ctx, f, filePath, targetGID, pod)
}

// DeleteStorageClass 删除传入的 StorageClass，并捕获 "Not Found" 以外的错误
func DeleteStorageClass(ctx context.Context, cs clientset.Interface, className string) error {
	err := cs.StorageV1().StorageClasses().Delete(ctx, className, metav1.DeleteOptions{})
	if err != nil && !apierrors.IsNotFound(err) {
		return err
	}
	return nil
}

// CreateVolumeSource 创建 volume source 对象
func CreateVolumeSource(pvcName string, readOnly bool) *v1.VolumeSource {
	return &v1.VolumeSource{
		PersistentVolumeClaim: &v1.PersistentVolumeClaimVolumeSource{
			ClaimName: pvcName,
			ReadOnly:  readOnly,
		},
	}
}

// TryFunc 尝试执行函数，如有错误则返回
func TryFunc(f func()) error {
	var err error
	if f == nil {
		return nil
	}
	defer func() {
		if recoverError := recover(); recoverError != nil {
			err = fmt.Errorf("%v", recoverError)
		}
	}()
	f()
	return err
}

// GetSizeRangesIntersection 接受两个存储大小范围实例，确定其
// 交集（如果存在），并返回交集的最小值，
// 作为测试中 claim 的大小。
// 如果未设置值，表示没有大小上下限限制，我们为其设置默认大小。
func GetSizeRangesIntersection(first e2evolume.SizeRange, second e2evolume.SizeRange) (string, error) {
	var firstMin, firstMax, secondMin, secondMax resource.Quantity
	var err error

	// 如果未设置 SizeRange，则指定最小或最大大小
	if len(first.Min) == 0 {
		first.Min = minValidSize
	}
	if len(first.Max) == 0 {
		first.Max = maxValidSize
	}
	if len(second.Min) == 0 {
		second.Min = minValidSize
	}
	if len(second.Max) == 0 {
		second.Max = maxValidSize
	}

	if firstMin, err = resource.ParseQuantity(first.Min); err != nil {
		return "", err
	}
	if firstMax, err = resource.ParseQuantity(first.Max); err != nil {
		return "", err
	}
	if secondMin, err = resource.ParseQuantity(second.Min); err != nil {
		return "", err
	}
	if secondMax, err = resource.ParseQuantity(second.Max); err != nil {
		return "", err
	}

	interSectionStart := math.Max(float64(firstMin.Value()), float64(secondMin.Value()))
	intersectionEnd := math.Min(float64(firstMax.Value()), float64(secondMax.Value()))

	// 应返回交集的最小值作为 claim 大小
	var intersectionMin resource.Quantity

	if intersectionEnd-interSectionStart >= 0 { // 有交集
		intersectionMin = *resource.NewQuantity(int64(interSectionStart), "BinarySI") // 将值转换为 BinarySI 格式，例如 5Gi
		// 返回交集的最小值作为 claim 大小
		return intersectionMin.String(), nil
	}
	return "", fmt.Errorf("intersection of size ranges %+v, %+v is null", first, second)
}
