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

/*
 * 本测试检查各种 VolumeSource 是否正常工作。
 *
 * 测试 volume 有两种方式：
 * 1) 使用容器化 server（NFS、Ceph、iSCSI 等）
 * 测试创建一个 server pod，导出一个简单的 'index.html' 文件。
 * 然后使用相应的 VolumeSource 把该文件导入 client pod，检查 client pod 能否看到它：
 * 将文件导入 web server 根目录，再从那里加载 index.html。
 *
 * 这些测试仅在允许 privileged container 时才能工作，因为导出各种文件系统
 * （如 NFS）通常需要在 server pod 里挂载或使用其他特权手段。
 *
 * 注意：server container 仅用于测试，不应在生产环境中使用。
 *
 * 2) 使用 Kubernetes 外部的 server
 * 被测试的 Kubernetes 集群之外必须存在相应的 server。测试自身创建一个新 volume，
 * 然后检查 Kubernetes 能否将其作为 volume 使用。
 */

package volume

import (
	"context"
	"crypto/sha256"
	"fmt"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/kubernetes/test/e2e/framework"
	e2ekubectl "k8s.io/kubernetes/test/e2e/framework/kubectl"
	e2epod "k8s.io/kubernetes/test/e2e/framework/pod"
	e2epodoutput "k8s.io/kubernetes/test/e2e/framework/pod/output"
	imageutils "k8s.io/kubernetes/test/utils/image"
	admissionapi "k8s.io/pod-security-admission/api"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
)

const (
	// Kb 是 kilobyte 对应的字节数
	Kb int64 = 1000
	// Mb 是 megabyte 对应的字节数
	Mb int64 = 1000 * Kb
	// Gb 是 gigabyte 对应的字节数
	Gb int64 = 1000 * Mb
	// Tb 是 terabyte 对应的字节数
	Tb int64 = 1000 * Gb
	// KiB 是 kibibyte 对应的字节数
	KiB int64 = 1024
	// MiB 是 mebibyte 对应的字节数
	MiB int64 = 1024 * KiB
	// GiB 是 gibibyte 对应的字节数
	GiB int64 = 1024 * MiB
	// TiB 是 tebibyte 对应的字节数
	TiB int64 = 1024 * GiB

	// VolumeServerPodStartupTimeout 是等待 volume server（Ceph 等）完成初始化的时长。
	VolumeServerPodStartupTimeout = 3 * time.Minute

	// PodCleanupTimeout 是等待 pod 清理并卸载 volume 的时长，避免过早拆除
	// 运行 NFS/Ceph server 的容器。
	PodCleanupTimeout = 20 * time.Second
)

// SizeRange 封装由最小值、最大值和步长（Quantity 字符串）表示的大小范围。
// 所有值都是可选的。未设置时视为无限制：Min 取很小的值（如 1ki），Max 取很大的值（如 10Ei），
// 这样就可以计算给定区间之间的交集（如果存在）。
type SizeRange struct {
	// Max 是包含单位的 Quantity 字符串，例如 "3Gi"。
	// 未设置时会被赋予默认的最大有效值 10Ei，定义在 test/e2e/storage/testsuites/base.go 中
	Max string
	// Min 是包含单位的 Quantity 字符串，例如 "1Gi"。
	// 未设置时会被赋予默认的最小有效值 1Ki，定义在 test/e2e/storage/testsuites/base.go 中
	Min string
	// Step 是包含单位的 Quantity 字符串，例如 "1Gi"。
	// 表示 volume 容量每次扩容的增量。未设置时不会赋予默认值。
	Step string
}

// TestConfig 是单个测试的配置。测试由以下部分组成：
// - server pod - 运行 serverImage，暴露 ports[]
// - client pod - 无需特殊配置
type TestConfig struct {
	Namespace string
	// 所有 pod 的前缀，通常是测试名称。
	Prefix string
	// server pod 使用的容器镜像名称。
	ServerImage string
	// server pod 暴露的端口，仅支持 TCP。
	ServerPorts []int
	// 在容器镜像中运行的命令。
	ServerCmds []string
	// 传给容器镜像的参数。
	ServerArgs []string
	// 需要从 host 挂载到 server container 的 volume
	// 格式：<host（源）路径> -> <container（目标）路径>
	// 如果 <host（源）路径> 为空，则挂载 tmpfs emptydir
	ServerVolumes map[string]string
	// 启动 client 前要等待的日志消息
	ServerReadyMessage string
	// server 是否使用 HostNetwork
	ServerHostNetwork bool
	// 是否等待 pod 成功终止。
	// false 表示 pod 会长期运行
	WaitForCompletion bool
	// ClientNodeSelection 限制 client pod 在哪些节点上运行，默认任意节点。
	ClientNodeSelection e2epod.NodeSelection
}

// Test 包含一个要挂载到 client pod 的 volume 及其预期内容。
type Test struct {
	Volume v1.VolumeSource
	Mode   v1.PersistentVolumeMode
	// FileSystem 模式下要读/写的文件名
	File            string
	ExpectedContent string
}

// NewNFSServer 是 CreateStorageServer 针对 NFS 的封装。
func NewNFSServer(ctx context.Context, cs clientset.Interface, namespace string, args []string) (config TestConfig, pod *v1.Pod, host string) {
	return NewNFSServerWithNodeName(ctx, cs, namespace, args, "")
}

func NewNFSServerWithNodeName(ctx context.Context, cs clientset.Interface, namespace string, args []string, nodeName string) (config TestConfig, pod *v1.Pod, host string) {
	config = TestConfig{
		Namespace:          namespace,
		Prefix:             "nfs",
		ServerImage:        imageutils.GetE2EImage(imageutils.VolumeNFSServer),
		ServerPorts:        []int{2049},
		ServerVolumes:      map[string]string{"": "/exports"},
		ServerReadyMessage: "NFS started",
	}
	if nodeName != "" {
		config.ClientNodeSelection = e2epod.NodeSelection{Name: nodeName}
	}

	if len(args) > 0 {
		config.ServerArgs = args
	}
	pod, host = CreateStorageServer(ctx, cs, config)
	if strings.Contains(host, ":") {
		host = "[" + host + "]"
	}
	return config, pod, host
}

// 在传入 nfs-server 的唯一容器中执行 `rpc.nfsd 1` 来重启它。
// 该命令将 nfs server 线程数从（大概）0 改回 1，从而让 nfs 能重新接受连接。
func RestartNFSServer(ctx context.Context, f *framework.Framework, serverPod *v1.Pod) {
	const startcmd = "rpc.nfsd 1"
	_, _, err := e2epod.ExecShellInPodWithFullOutput(ctx, f, serverPod.Name, startcmd)
	framework.ExpectNoError(err)
}

// 在传入 nfs-server 的唯一容器中执行 `rpc.nfsd 0` 来停止它。
// 该命令将 nfs server 线程数改为 0，从而关闭所有已打开的 nfs 连接。
func StopNFSServer(ctx context.Context, f *framework.Framework, serverPod *v1.Pod) {
	const stopcmd = "rpc.nfsd 0 && for i in $(seq 200); do rpcinfo -p | grep -q nfs || break; sleep 1; done"
	_, _, err := e2epod.ExecShellInPodWithFullOutput(ctx, f, serverPod.Name, stopcmd)
	framework.ExpectNoError(err)
}

// CreateStorageServer 封装了 startVolumeServer()。传入 storage server 配置，
// 返回 pod 指针和 IP 地址字符串。
// 注意：内部调用了 Expect()，因此不返回错误。
func CreateStorageServer(ctx context.Context, cs clientset.Interface, config TestConfig) (pod *v1.Pod, ip string) {
	pod = startVolumeServer(ctx, cs, config)
	gomega.Expect(pod).NotTo(gomega.BeNil(), "storage server pod should not be nil")
	ip = pod.Status.PodIP
	gomega.Expect(ip).NotTo(gomega.BeEmpty(), fmt.Sprintf("pod %s's IP should not be empty", pod.Name))
	framework.Logf("%s server pod IP address: %s", config.Prefix, ip)
	return pod, ip
}

// GetVolumeAttachmentName 返回由 provisioner、config.ClientNodeSelection 名称，
// 以及与传入 claimName/claimNamespace 对应 PVC 所绑定的 PV 的 VolumeAttachment 名称
// 组合计算出的哈希值。
func GetVolumeAttachmentName(ctx context.Context, cs clientset.Interface, config TestConfig, provisioner string, claimName string, claimNamespace string) string {
	var nodeName string
	// 对于 provisioning 测试，ClientNodeSelection 未设置，因此我们不知道
	// 与传入 claimName/claimNamespace 对应 PVC 所绑定 PV 的 VolumeAttachment 的 NodeName。
	// 我们需要这个 NodeName，因为它用于生成返回的 attachmentName，
	// 也用于 WaitForVolumeAttachmentTerminated 中查找特定 VolumeAttachment。
	// 为获取 VolumeAttachment 的 nodeName，我们列出所有 VolumeAttachment，
	// 找到 PersistentVolumeName 等于该 PVC 所绑定 PV 的那个，再从中取出 NodeName。
	if config.ClientNodeSelection.Name == "" {
		claim, _ := cs.CoreV1().PersistentVolumeClaims(claimNamespace).Get(ctx, claimName, metav1.GetOptions{})
		pvName := claim.Spec.VolumeName
		volumeAttachments, _ := cs.StorageV1().VolumeAttachments().List(ctx, metav1.ListOptions{})
		for _, volumeAttachment := range volumeAttachments.Items {
			if *volumeAttachment.Spec.Source.PersistentVolumeName == pvName {
				nodeName = volumeAttachment.Spec.NodeName
				break
			}
		}
	} else {
		nodeName = config.ClientNodeSelection.Name
	}
	handle := getVolumeHandle(ctx, cs, claimName, claimNamespace)
	attachmentHash := sha256.Sum256([]byte(fmt.Sprintf("%s%s%s", handle, provisioner, nodeName)))
	return fmt.Sprintf("csi-%x", attachmentHash)
}

// getVolumeHandle 返回与传入 claimName/claimNamespace 对应 PVC 所绑定 PV 的 VolumeHandle。
func getVolumeHandle(ctx context.Context, cs clientset.Interface, claimName string, claimNamespace string) string {
	// 重新获取 PVC，以拿到绑定了 volume 的最新状态
	claim, err := cs.CoreV1().PersistentVolumeClaims(claimNamespace).Get(ctx, claimName, metav1.GetOptions{})
	if err != nil {
		framework.ExpectNoError(err, "Cannot get PVC")
		return ""
	}
	pvName := claim.Spec.VolumeName
	pv, err := cs.CoreV1().PersistentVolumes().Get(ctx, pvName, metav1.GetOptions{})
	if err != nil {
		framework.ExpectNoError(err, "Cannot get PV")
		return ""
	}
	if pv.Spec.CSI == nil {
		gomega.Expect(pv.Spec.CSI).NotTo(gomega.BeNil())
		return ""
	}
	return pv.Spec.CSI.VolumeHandle
}

// WaitForVolumeAttachmentCreated 等待传入 attachmentName 对应的 VolumeAttachment 被创建。
func WaitForVolumeAttachmentCreated(ctx context.Context, attachmentName string, cs clientset.Interface, interval, timeout time.Duration) error {
	waitErr := wait.PollUntilContextTimeout(ctx, interval, timeout, true, func(ctx context.Context) (bool, error) {
		_, err := cs.StorageV1().VolumeAttachments().Get(ctx, attachmentName, metav1.GetOptions{})
		if err != nil {
			if apierrors.IsNotFound(err) {
				return false, nil
			}
			return false, err
		}
		return true, nil
	})
	if waitErr != nil {
		return fmt.Errorf("error waiting volume attachment %v to be created: %w", attachmentName, waitErr)
	}
	return nil
}

// WaitForVolumeAttachmentTerminated 等待传入 attachmentName 对应的 VolumeAttachment 被终止。
func WaitForVolumeAttachmentTerminated(ctx context.Context, attachmentName string, cs clientset.Interface, timeout time.Duration) error {
	waitErr := wait.PollUntilContextTimeout(ctx, 10*time.Second, timeout, true, func(ctx context.Context) (bool, error) {
		_, err := cs.StorageV1().VolumeAttachments().Get(ctx, attachmentName, metav1.GetOptions{})
		if err != nil {
			// 找不到 volumeattachment 对象即表示它已被终止。
			if apierrors.IsNotFound(err) {
				return true, nil
			}
			return false, err
		}
		return false, nil
	})
	if waitErr != nil {
		return fmt.Errorf("error waiting volume attachment %v to terminate: %v", attachmentName, waitErr)
	}
	return nil
}

// startVolumeServer 启动 config.serverImage 指定的容器，并暴露所有 config.serverPorts。
// 返回的 pod 应用于获取 server IP 地址并创建相应的 VolumeSource。
func startVolumeServer(ctx context.Context, client clientset.Interface, config TestConfig) *v1.Pod {
	podClient := client.CoreV1().Pods(config.Namespace)

	portCount := len(config.ServerPorts)
	serverPodPorts := make([]v1.ContainerPort, portCount)

	for i := range portCount {
		portName := fmt.Sprintf("%s-%d", config.Prefix, i)

		serverPodPorts[i] = v1.ContainerPort{
			Name:          portName,
			ContainerPort: int32(config.ServerPorts[i]),
			Protocol:      v1.ProtocolTCP,
		}
	}

	volumeCount := len(config.ServerVolumes)
	volumes := make([]v1.Volume, volumeCount)
	mounts := make([]v1.VolumeMount, volumeCount)

	i := 0
	for src, dst := range config.ServerVolumes {
		mountName := fmt.Sprintf("path%d", i)
		volumes[i].Name = mountName
		if src == "" {
			volumes[i].VolumeSource.EmptyDir = &v1.EmptyDirVolumeSource{}
		} else {
			volumes[i].VolumeSource.HostPath = &v1.HostPathVolumeSource{
				Path: src,
			}
		}

		mounts[i].Name = mountName
		mounts[i].ReadOnly = false
		mounts[i].MountPath = dst

		i++
	}

	serverPodName := fmt.Sprintf("%s-server", config.Prefix)
	ginkgo.By(fmt.Sprint("creating ", serverPodName, " pod"))
	privileged := new(bool)
	*privileged = true

	restartPolicy := v1.RestartPolicyAlways
	if config.WaitForCompletion {
		restartPolicy = v1.RestartPolicyNever
	}
	serverPod := &v1.Pod{
		TypeMeta: metav1.TypeMeta{
			Kind:       "Pod",
			APIVersion: "v1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name: serverPodName,
			Labels: map[string]string{
				"role": serverPodName,
			},
		},

		Spec: v1.PodSpec{
			HostNetwork: config.ServerHostNetwork,
			Containers: []v1.Container{
				{
					Name:  serverPodName,
					Image: config.ServerImage,
					SecurityContext: &v1.SecurityContext{
						Privileged: privileged,
					},
					Command:      config.ServerCmds,
					Args:         config.ServerArgs,
					Ports:        serverPodPorts,
					VolumeMounts: mounts,
				},
			},
			Volumes:       volumes,
			RestartPolicy: restartPolicy,
		},
	}

	if config.ClientNodeSelection.Name != "" {
		serverPod.Spec.NodeName = config.ClientNodeSelection.Name
	}

	var pod *v1.Pod
	serverPod, err := podClient.Create(ctx, serverPod, metav1.CreateOptions{})
	// server pod 已存在也可以。TODO: 让调用方控制这一行为
	if err != nil {
		if apierrors.IsAlreadyExists(err) {
			framework.Logf("Ignore \"already-exists\" error, re-get pod...")
			ginkgo.By(fmt.Sprintf("re-getting the %q server pod", serverPodName))
			serverPod, err = podClient.Get(ctx, serverPodName, metav1.GetOptions{})
			framework.ExpectNoError(err, "Cannot re-get the server pod %q: %v", serverPodName, err)
			pod = serverPod
		} else {
			framework.ExpectNoError(err, "Failed to create %q pod: %v", serverPodName, err)
		}
	}
	if config.WaitForCompletion {
		framework.ExpectNoError(e2epod.WaitForPodSuccessInNamespace(ctx, client, serverPod.Name, serverPod.Namespace))
		framework.ExpectNoError(podClient.Delete(ctx, serverPod.Name, metav1.DeleteOptions{}))
	} else {
		framework.ExpectNoError(e2epod.WaitForPodRunningInNamespace(ctx, client, serverPod))
		if pod == nil {
			ginkgo.By(fmt.Sprintf("locating the %q server pod", serverPodName))
			pod, err = podClient.Get(ctx, serverPodName, metav1.GetOptions{})
			framework.ExpectNoError(err, "Cannot locate the server pod %q: %v", serverPodName, err)
		}
	}
	if config.ServerReadyMessage != "" {
		_, err := e2epodoutput.LookForStringInLogWithoutKubectl(ctx, client, pod.Namespace, pod.Name, serverPodName, config.ServerReadyMessage, VolumeServerPodStartupTimeout)
		framework.ExpectNoError(err, "Failed to find %q in pod logs: %s", config.ServerReadyMessage, err)
	}
	return pod
}

// TestServerCleanup 清理 server pod。
func TestServerCleanup(ctx context.Context, f *framework.Framework, config TestConfig) {
	ginkgo.By(fmt.Sprint("cleaning the environment after ", config.Prefix))
	defer ginkgo.GinkgoRecover()

	if config.ServerImage == "" {
		return
	}

	err := e2epod.DeletePodWithWaitByName(ctx, f.ClientSet, config.Prefix+"-server", config.Namespace)
	framework.ExpectNoError(err, "delete pod %v in namespace %v", config.Prefix+"-server", config.Namespace)
}

func runVolumeTesterPod(ctx context.Context, client clientset.Interface, timeouts *framework.TimeoutContext, config TestConfig, podSuffix string, privileged bool, fsGroup *int64, tests []Test, slow bool) (*v1.Pod, error) {
	ginkgo.By(fmt.Sprint("starting ", config.Prefix, "-", podSuffix))
	var gracePeriod int64 = 1
	var command string

	/**
	该条件用于修复在 SELinux 环境下运行 storage e2e 测试的问题。
	HostPath volume plugin 在 host 的 /tmp 下创建目录并挂载为 volume。
	injector pod 向 volume 写入内容，client pod 尝试读取并校验。
	当 host 启用 SELinux 时，client pod 会因权限被拒而无法读取内容。
	因此以 privileged 方式启动 client pod，使其在 SELinux 启用时也能访问 volume 内容。
	*/
	securityLevel := admissionapi.LevelBaseline // TODO (#118184): 同时支持 LevelRestricted
	if privileged || config.Prefix == "hostpathsymlink" || config.Prefix == "hostpath" {
		securityLevel = admissionapi.LevelPrivileged
	}
	command = "while true ; do sleep 2; done "
	seLinuxOptions := &v1.SELinuxOptions{Level: "s0:c0,c1"}
	clientPod := &v1.Pod{
		TypeMeta: metav1.TypeMeta{
			Kind:       "Pod",
			APIVersion: "v1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name: config.Prefix + "-" + podSuffix,
			Labels: map[string]string{
				"role": config.Prefix + "-" + podSuffix,
			},
		},
		Spec: v1.PodSpec{
			Containers: []v1.Container{
				{
					Name:       config.Prefix + "-" + podSuffix,
					Image:      e2epod.GetDefaultTestImage(),
					WorkingDir: "/opt",
					// 一个命令式、易于调试的容器，负责读/写 volume 内容，
					// 供测试扫描或人工查看。本测试使用的最小容器里 /opt 预期为空。
					Command:      e2epod.GenerateScriptCmd(command),
					VolumeMounts: []v1.VolumeMount{},
				},
			},
			TerminationGracePeriodSeconds: &gracePeriod,
			SecurityContext:               e2epod.GeneratePodSecurityContext(fsGroup, seLinuxOptions),
			Volumes:                       []v1.Volume{},
		},
	}
	e2epod.SetNodeSelection(&clientPod.Spec, config.ClientNodeSelection)

	for i, test := range tests {
		volumeName := fmt.Sprintf("%s-%s-%d", config.Prefix, "volume", i)

		// host 启用 SELinux 时需要让容器特权运行，这样测试才能向 /tmp 之类的位置写入数据。
		// 另外，由于下面这个 Docker bug，privileged 容器目前无法映射设备，
		// 所以 block volume 不使用 privileged。
		// https://github.com/moby/moby/issues/35991
		if privileged && test.Mode == v1.PersistentVolumeBlock {
			securityLevel = admissionapi.LevelBaseline
		}
		clientPod.Spec.Containers[0].SecurityContext = e2epod.GenerateContainerSecurityContext(securityLevel)

		if test.Mode == v1.PersistentVolumeBlock {
			clientPod.Spec.Containers[0].VolumeDevices = append(clientPod.Spec.Containers[0].VolumeDevices, v1.VolumeDevice{
				Name:       volumeName,
				DevicePath: fmt.Sprintf("/opt/%d", i),
			})
		} else {
			clientPod.Spec.Containers[0].VolumeMounts = append(clientPod.Spec.Containers[0].VolumeMounts, v1.VolumeMount{
				Name:      volumeName,
				MountPath: fmt.Sprintf("/opt/%d", i),
			})
		}
		clientPod.Spec.Volumes = append(clientPod.Spec.Volumes, v1.Volume{
			Name:         volumeName,
			VolumeSource: test.Volume,
		})
	}
	podsNamespacer := client.CoreV1().Pods(config.Namespace)
	clientPod, err := podsNamespacer.Create(ctx, clientPod, metav1.CreateOptions{})
	if err != nil {
		return nil, err
	}
	if slow {
		err = e2epod.WaitTimeoutForPodRunningInNamespace(ctx, client, clientPod.Name, clientPod.Namespace, timeouts.PodStartSlow)
	} else {
		err = e2epod.WaitTimeoutForPodRunningInNamespace(ctx, client, clientPod.Name, clientPod.Namespace, timeouts.PodStart)
	}
	if err != nil {
		e2epod.DeletePodOrFail(ctx, client, clientPod.Namespace, clientPod.Name)
		_ = e2epod.WaitForPodNotFoundInNamespace(ctx, client, clientPod.Name, clientPod.Namespace, timeouts.PodDelete)
		return nil, err
	}
	return clientPod, nil
}

func testVolumeContent(ctx context.Context, f *framework.Framework, pod *v1.Pod, containerName string, fsGroup *int64, fsType string, tests []Test) {
	ginkgo.By("Checking that text file contents are perfect.")
	for i, test := range tests {
		if test.Mode == v1.PersistentVolumeBlock {
			// Block：检查内容
			deviceName := fmt.Sprintf("/opt/%d", i)
			commands := GenerateReadBlockCmd(deviceName, len(test.ExpectedContent))
			_, err := e2epodoutput.LookForStringInPodExecToContainer(pod.Namespace, pod.Name, containerName, commands, test.ExpectedContent, time.Minute)
			framework.ExpectNoError(err, "failed: finding the contents of the block device %s.", deviceName)

			// 确认它是真正的块设备
			err = CheckVolumeModeOfPath(ctx, f, pod, test.Mode, deviceName)
			framework.ExpectNoError(err, "failed: getting the right privileges in the block device %v", deviceName)
		} else {
			// Filesystem：检查内容
			fileName := fmt.Sprintf("/opt/%d/%s", i, test.File)
			commands := GenerateReadFileCmd(fileName)
			_, err := e2epodoutput.LookForStringInPodExecToContainer(pod.Namespace, pod.Name, containerName, commands, test.ExpectedContent, time.Minute)
			framework.ExpectNoError(err, "failed: finding the contents of the mounted file %s.", fileName)

			// 确认目录已被挂载
			dirName := filepath.Dir(fileName)
			err = CheckVolumeModeOfPath(ctx, f, pod, test.Mode, dirName)
			framework.ExpectNoError(err, "failed: getting the right privileges in the directory %v", dirName)

			if !framework.NodeOSDistroIs("windows") {
				// Filesystem：检查 fsgroup
				if fsGroup != nil {
					ginkgo.By("Checking fsGroup is correct.")
					_, err = e2epodoutput.LookForStringInPodExecToContainer(pod.Namespace, pod.Name, containerName, []string{"ls", "-ld", dirName}, strconv.Itoa(int(*fsGroup)), time.Minute)
					framework.ExpectNoError(err, "failed: getting the right privileges in the file %v", int(*fsGroup))
				}

				// Filesystem：检查 fsType
				if fsType != "" {
					ginkgo.By("Checking fsType is correct.")
					_, err = e2epodoutput.LookForStringInPodExecToContainer(pod.Namespace, pod.Name, containerName, []string{"grep", " " + dirName + " ", "/proc/mounts"}, fsType, time.Minute)
					framework.ExpectNoError(err, "failed: getting the right fsType %s", fsType)
				}
			}
		}
	}
}

// TestVolumeClient 使用给定的 VolumeSource（由 startVolumeServer() 导出）启动 client pod，
// 并检查该 pod 是否能看到预期数据（例如来自 server pod 的数据）。
// 可以指定多个 Test，在一个 pod 中挂载多个 volume。
// 动态 provisioning（设置了 "WaitForFirstConsumer" 且提供的 PVC 尚未绑定）、
// pod 创建、调度及完整启动（含 volume 挂载）的超时时间为 pod.podStartTimeout。
// 适用于请求对空 volume 进行“常规”动态 provisioning 的场景。
func TestVolumeClient(ctx context.Context, f *framework.Framework, config TestConfig, fsGroup *int64, fsType string, tests []Test) {
	testVolumeClient(ctx, f, config, fsGroup, fsType, tests, false)
}

// TestVolumeClientSlow 与 TestVolumeClient 相同，只是超时时间不同。
// 动态 provisioning（设置了 "WaitForFirstConsumer" 且提供的 PVC 尚未绑定）、
// pod 创建、调度及完整启动（含 volume 挂载）的超时时间为 pod.slowPodStartTimeout。
// 适用于请求“特殊”动态 provisioning 的场景，如 volume cloning 或 snapshot restore。
func TestVolumeClientSlow(ctx context.Context, f *framework.Framework, config TestConfig, fsGroup *int64, fsType string, tests []Test) {
	testVolumeClient(ctx, f, config, fsGroup, fsType, tests, true)
}

func testVolumeClient(ctx context.Context, f *framework.Framework, config TestConfig, fsGroup *int64, fsType string, tests []Test, slow bool) {
	timeouts := f.Timeouts
	clientPod, err := runVolumeTesterPod(ctx, f.ClientSet, timeouts, config, "client", false, fsGroup, tests, slow)
	if err != nil {
		framework.Failf("Failed to create client pod: %v", err)
	}
	defer func() {
		// testVolumeClient 在一个测试中可能被调用多次，因此返回前必须清理。
		e2epod.DeletePodOrFail(ctx, f.ClientSet, clientPod.Namespace, clientPod.Name)
		framework.ExpectNoError(e2epod.WaitForPodNotFoundInNamespace(ctx, f.ClientSet, clientPod.Name, clientPod.Namespace, timeouts.PodDelete))
	}()

	testVolumeContent(ctx, f, clientPod, "", fsGroup, fsType, tests)

	ginkgo.By("Repeating the test on an ephemeral container (if enabled)")
	ec := &v1.EphemeralContainer{
		EphemeralContainerCommon: v1.EphemeralContainerCommon(clientPod.Spec.Containers[0]),
	}
	ec.Resources = v1.ResourceRequirements{}
	ec.Name = "volume-ephemeral-container"
	err = e2epod.NewPodClient(f).AddEphemeralContainerSync(ctx, clientPod, ec, timeouts.PodStart)
	// 该功能禁用时，API server 对该子资源会返回 NotFound
	framework.ExpectNoError(err, "failed to add ephemeral container for re-test")
	testVolumeContent(ctx, f, clientPod, ec.Name, fsGroup, fsType, tests)
}

// InjectContent 将带有给定内容的 index.html 写入指定 volume。它通过启动一个
// 辅助 pod 来写入文件。volume 必须可写。
func InjectContent(ctx context.Context, f *framework.Framework, config TestConfig, fsGroup *int64, fsType string, tests []Test) {
	privileged := true
	timeouts := f.Timeouts
	if framework.NodeOSDistroIs("windows") {
		privileged = false
	}
	injectorPod, err := runVolumeTesterPod(ctx, f.ClientSet, timeouts, config, "injector", privileged, fsGroup, tests, false /*slow*/)
	if err != nil {
		framework.Failf("Failed to create injector pod: %v", err)
		return
	}
	defer func() {
		// 函数返回前必须删除该 pod，因为测试依赖 volume 未被占用。
		e2epod.DeletePodOrFail(ctx, f.ClientSet, injectorPod.Namespace, injectorPod.Name)
		framework.ExpectNoError(e2epod.WaitForPodNotFoundInNamespace(ctx, f.ClientSet, injectorPod.Name, injectorPod.Namespace, timeouts.PodDelete))
	}()

	ginkgo.By("Writing text file contents in the container.")
	for i, test := range tests {
		commands := []string{"exec", injectorPod.Name, fmt.Sprintf("--namespace=%v", injectorPod.Namespace), "--"}
		if test.Mode == v1.PersistentVolumeBlock {
			// Block：写入内容
			deviceName := fmt.Sprintf("/opt/%d", i)
			commands = append(commands, generateWriteBlockCmd(test.ExpectedContent, deviceName)...)

		} else {
			// Filesystem：写入内容
			fileName := fmt.Sprintf("/opt/%d/%s", i, test.File)
			commands = append(commands, generateWriteFileCmd(test.ExpectedContent, fileName)...)
		}
		out, err := e2ekubectl.RunKubectl(injectorPod.Namespace, commands...)
		framework.ExpectNoError(err, "failed: writing the contents: %s", out)
	}

	// 检查数据是否真的写入了该 pod。
	// 这用于测试非持久化 volume 类型
	testVolumeContent(ctx, f, injectorPod, "", fsGroup, fsType, tests)
}

// generateWriteCmd 供 generateWriteBlockCmd 和 generateWriteFileCmd 使用
func generateWriteCmd(content, path string) []string {
	var commands []string
	commands = []string{"/bin/sh", "-c", "echo '" + content + "' > " + path + "; sync"}
	return commands
}

// GenerateReadBlockCmd 生成从给定路径的块设备读取数据的命令行。
func GenerateReadBlockCmd(fullPath string, numberOfCharacters int) []string {
	var commands []string
	commands = []string{"head", "-c", strconv.Itoa(numberOfCharacters), fullPath}
	return commands
}

// generateWriteBlockCmd 生成向块设备写入给定内容的命令行。
func generateWriteBlockCmd(content, fullPath string) []string {
	return generateWriteCmd(content, fullPath)
}

// GenerateReadFileCmd 生成从给定路径的文件读取数据的命令行。
func GenerateReadFileCmd(fullPath string) []string {
	var commands []string
	commands = []string{"cat", fullPath}
	return commands
}

// generateWriteFileCmd 生成向给定路径的文件写入给定内容的命令行。
func generateWriteFileCmd(content, fullPath string) []string {
	return generateWriteCmd(content, fullPath)
}

// CheckVolumeModeOfPath 检查 volume 的模式
func CheckVolumeModeOfPath(ctx context.Context, f *framework.Framework, pod *v1.Pod, volMode v1.PersistentVolumeMode, path string) error {
	if volMode == v1.PersistentVolumeBlock {
		// 检查块设备是否存在
		if err := e2epod.VerifyExecInPodSucceed(ctx, f, pod, fmt.Sprintf("test -b %s", path)); err != nil {
			return err
		}

		// 再次确认它不是目录
		if err := e2epod.VerifyExecInPodFail(ctx, f, pod, fmt.Sprintf("test -d %s", path), 1); err != nil {
			return err
		}
	} else {
		// 检查目录是否存在
		if err := e2epod.VerifyExecInPodSucceed(ctx, f, pod, fmt.Sprintf("test -d %s", path)); err != nil {
			return err
		}

		// 再次确认它不是块设备
		if err := e2epod.VerifyExecInPodFail(ctx, f, pod, fmt.Sprintf("test -b %s", path), 1); err != nil {
			return err
		}
	}
	return nil
}
