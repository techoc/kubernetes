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

package kuberuntime

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	goruntime "runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	codes "google.golang.org/grpc/codes"

	crierror "k8s.io/cri-api/pkg/errors"

	"github.com/opencontainers/selinux/go-selinux"
	grpcstatus "google.golang.org/grpc/status"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kubetypes "k8s.io/apimachinery/pkg/types"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/sets"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	runtimeapi "k8s.io/cri-api/pkg/apis/runtime/v1"
	remote "k8s.io/cri-client/pkg"
	"k8s.io/klog/v2"
	kubelettypes "k8s.io/kubelet/pkg/types"
	podutil "k8s.io/kubernetes/pkg/api/v1/pod"
	"k8s.io/kubernetes/pkg/features"
	"k8s.io/kubernetes/pkg/kubelet/cm"
	kubecontainer "k8s.io/kubernetes/pkg/kubelet/container"
	"k8s.io/kubernetes/pkg/kubelet/events"
	proberesults "k8s.io/kubernetes/pkg/kubelet/prober/results"
	"k8s.io/kubernetes/pkg/kubelet/types"
	kubeutil "k8s.io/kubernetes/pkg/kubelet/util"
	"k8s.io/kubernetes/pkg/kubelet/util/format"
	"k8s.io/kubernetes/pkg/util/tail"
	volumeutil "k8s.io/kubernetes/pkg/volume/util"
	"k8s.io/utils/buffer"
)

// 容器生命周期各阶段失败时使用的哨兵错误，便于把错误归类到具体步骤。
var (
	// ErrCreateContainerConfig 表示创建容器配置失败。
	ErrCreateContainerConfig = errors.New("CreateContainerConfigError")
	// ErrPreCreateHook 表示执行 PreCreateHook 失败。
	ErrPreCreateHook = errors.New("PreCreateHookError")
	// ErrCreateContainer 表示创建容器失败。
	ErrCreateContainer = errors.New("CreateContainerError")
	// ErrPreStartHook 表示执行 PreStartHook 失败。
	ErrPreStartHook = errors.New("PreStartHookError")
	// ErrPostStartHook 表示执行 PostStartHook 失败。
	ErrPostStartHook = errors.New("PostStartHookError")
)

// recordContainerEvent 供运行时管理器记录所有容器相关事件。
// 它做了一些健全性检查，避免写入可能压垮 apiserver 的事件：特别地，
// 保证事件消息中不出现 containerID，因为 containerID 会导致大量难以聚合的
// 独立事件。所有对 containerID 的引用都会被替换为稳定且用户熟悉的 containerName。
func (m *kubeGenericRuntimeManager) recordContainerEvent(ctx context.Context, pod *v1.Pod, container *v1.Container, containerID, eventType, reason, message string, args ...interface{}) {
	logger := klog.FromContext(ctx)
	ref, err := kubecontainer.GenerateContainerRef(pod, container)
	if err != nil {
		logger.Error(err, "Can't make a container ref", "pod", klog.KObj(pod), "podUID", pod.UID, "containerName", container.Name)
		return
	}
	eventMessage := message
	if len(args) > 0 {
		eventMessage = fmt.Sprintf(message, args...)
	}
	// 这是一种变通：运行时返回的错误经常包含 containerID，
	// 会破坏事件去重能力。该处理能显著减少独立事件数量。
	if containerID != "" {
		eventMessage = strings.Replace(eventMessage, containerID, container.Name, -1)
	}
	m.recorder.WithLogger(logger).Event(ref, eventType, reason, eventMessage)
}

// startSpec 封装启动容器所需的 spec，既可以是普通/init 容器，也可以是临时容器。
// 临时容器包含普通/init 容器的全部字段以及额外字段。两种情况下 startSpec.container 都会被设置。
type startSpec struct {
	container          *v1.Container
	ephemeralContainer *v1.EphemeralContainer
}

// containerStartSpec 为普通/init 容器构造 startSpec。
func containerStartSpec(c *v1.Container) *startSpec {
	return &startSpec{container: c}
}

// ephemeralContainerStartSpec 为临时容器构造 startSpec。
func ephemeralContainerStartSpec(ec *v1.EphemeralContainer) *startSpec {
	return &startSpec{
		container:          (*v1.Container)(&ec.EphemeralContainerCommon),
		ephemeralContainer: ec,
	}
}

// getTargetID 返回用于临时容器命名空间目标的 kubecontainer.ContainerID。
// 目标保存在 EphemeralContainer.TargetContainerName 中，必须通过 podStatus 解析为
// ContainerID。目标容器必须已经存在，这通常不是问题，因为 Pod 创建时不允许临时容器。
func (s *startSpec) getTargetID(podStatus *kubecontainer.PodStatus) (*kubecontainer.ContainerID, error) {
	if s.ephemeralContainer == nil || s.ephemeralContainer.TargetContainerName == "" {
		return nil, nil
	}

	targetStatus := podStatus.FindContainerStatusByName(s.ephemeralContainer.TargetContainerName)
	if targetStatus == nil {
		return nil, fmt.Errorf("unable to find target container %v", s.ephemeralContainer.TargetContainerName)
	}

	return &targetStatus.ID, nil
}

// calcRestartCountByLogDir 通过日志目录中的日志文件名推断重启次数。
// 节点重启后某些运行时会清空自身状态，导致重启计数归零并从 0.log 覆盖旧日志；
// 这里以目录中最新的 {restartCount}.log 为准。
func calcRestartCountByLogDir(path string) (int, error) {
	// 路径不存在不算错误
	if _, err := os.Stat(path); err != nil {
		return 0, nil
	}
	files, err := os.ReadDir(path)
	if err != nil {
		return 0, err
	}
	if len(files) == 0 {
		return 0, nil
	}
	restartCount := 0
	restartCountLogFileRegex := regexp.MustCompile(`^(\d+)\.log(\..*)?`)
	for _, file := range files {
		if file.IsDir() {
			continue
		}
		matches := restartCountLogFileRegex.FindStringSubmatch(file.Name())
		if len(matches) == 0 {
			continue
		}
		count, err := strconv.Atoi(matches[1])
		if err != nil {
			// kubelet 不太可能创建这种文件，
			// 更可能是以随机数字命名的自定义文件
			continue
		}
		count++
		if count > restartCount {
			restartCount = count
		}
	}
	return restartCount, nil
}

// getPodRuntimeHandler 返回 Pod 使用的 runtime handler 名称。
func (m *kubeGenericRuntimeManager) getPodRuntimeHandler(pod *v1.Pod) (podRuntimeHandler string, err error) {
	// RuntimeClassInImageCriAPI 特性门控开启时，传递指定 runtime class 的
	// runtime handler 信息；未指定 runtime class 时传递空字符串。
	if utilfeature.DefaultFeatureGate.Enabled(features.RuntimeClassInImageCriAPI) {
		if pod.Spec.RuntimeClassName != nil && *pod.Spec.RuntimeClassName != "" {
			podRuntimeHandler, err = m.runtimeClassManager.LookupRuntimeHandler(pod.Spec.RuntimeClassName)
			if err != nil {
				msg := fmt.Sprintf("Failed to lookup runtimeHandler for runtimeClassName %v", pod.Spec.RuntimeClassName)
				return msg, err
			}
		}
	}

	return podRuntimeHandler, nil
}

// startContainer 启动容器，出错时返回说明失败原因的消息。
// 启动流程如下：
// * 拉取镜像
// * 创建容器
// * 启动容器
// * 执行 post start 生命周期钩子（如适用）
func (m *kubeGenericRuntimeManager) startContainer(ctx context.Context, podSandboxID string, podSandboxConfig *runtimeapi.PodSandboxConfig, spec *startSpec, pod *v1.Pod, podStatus *kubecontainer.PodStatus, pullSecrets []v1.Secret, podIP string, podIPs []string, imageVolumes kubecontainer.ImageVolumes) (string, error) {
	logger := klog.FromContext(ctx)
	container := spec.container

	// 步骤 1：拉取镜像。
	podRuntimeHandler, err := m.getPodRuntimeHandler(pod)
	if err != nil {
		return "", err
	}

	ref, err := kubecontainer.GenerateContainerRef(pod, container)
	if err != nil {
		logger.Error(err, "Couldn't make a ref to pod", "pod", klog.KObj(pod), "containerName", container.Name)
	}

	imageRef, msg, err := m.imagePuller.EnsureImageExists(ctx, ref, pod, container.Image, pullSecrets, podSandboxConfig, podRuntimeHandler, container.ImagePullPolicy)
	if err != nil {
		s, _ := grpcstatus.FromError(err)
		m.recordContainerEvent(ctx, pod, container, "", v1.EventTypeWarning, events.FailedToCreateContainer, "Error: %v", s.Message())
		return msg, err
	}

	// 步骤 2：创建容器。
	// 新容器的 RestartCount 应为 0
	restartCount := 0
	containerStatus := podStatus.FindContainerStatusByName(container.Name)
	if containerStatus != nil {
		restartCount = containerStatus.RestartCount + 1
	} else {
		// 容器运行时保存容器状态和重启计数。节点重启后某些运行时会清空状态，
		// 导致 restartCount 归零，日志从 0.log 开始，从而覆盖或追加到已有日志。
		//
		// 因此检查日志目录是否存在，并通过日志文件名 {restartCount}.log
		// 找到最新的 restartCount 再加 1。
		logDir := BuildContainerLogsDirectory(m.podLogsDirectory, pod.Namespace, pod.Name, pod.UID, container.Name)
		restartCount, err = calcRestartCountByLogDir(logDir)
		if err != nil {
			logger.Info("Cannot calculate restartCount from the log directory", "logDir", logDir, "err", err)
			restartCount = 0
		}
	}

	target, err := spec.getTargetID(podStatus)
	if err != nil {
		s, _ := grpcstatus.FromError(err)
		m.recordContainerEvent(ctx, pod, container, "", v1.EventTypeWarning, events.FailedToCreateContainer, "Error: %v", s.Message())
		return s.Message(), ErrCreateContainerConfig
	}

	containerConfig, cleanupAction, err := m.generateContainerConfig(ctx, container, pod, restartCount, podIP, imageRef, podIPs, target, imageVolumes)
	if cleanupAction != nil {
		defer cleanupAction()
	}
	if err != nil {
		s, _ := grpcstatus.FromError(err)
		m.recordContainerEvent(ctx, pod, container, "", v1.EventTypeWarning, events.FailedToCreateContainer, "Error: %v", s.Message())
		return s.Message(), ErrCreateContainerConfig
	}

	// 创建容器时将资源标记为已生效。
	if err := m.setActuatedContainerResources(logger, pod, container); err != nil {
		m.recordContainerEvent(ctx, pod, container, "", v1.EventTypeWarning, events.FailedToCreateContainer, "Error: %v", err)
		return err.Error(), ErrCreateContainerConfig
	}

	err = m.internalLifecycle.PreCreateContainer(logger, pod, container, containerConfig)
	if err != nil {
		s, _ := grpcstatus.FromError(err)
		m.recordContainerEvent(ctx, pod, container, "", v1.EventTypeWarning, events.FailedToCreateContainer, "Internal PreCreateContainer hook failed: %v", s.Message())
		return s.Message(), ErrPreCreateHook
	}

	containerID, err := m.runtimeService.CreateContainer(ctx, podSandboxID, containerConfig, podSandboxConfig)
	if err != nil {
		s, _ := grpcstatus.FromError(err)
		m.recordContainerEvent(ctx, pod, container, containerID, v1.EventTypeWarning, events.FailedToCreateContainer, "Error: %v", s.Message())
		return s.Message(), ErrCreateContainer
	}
	err = m.internalLifecycle.PreStartContainer(logger, pod, container, containerID)
	if err != nil {
		s, _ := grpcstatus.FromError(err)
		m.recordContainerEvent(ctx, pod, container, containerID, v1.EventTypeWarning, events.FailedToStartContainer, "Internal PreStartContainer hook failed: %v", s.Message())
		return s.Message(), ErrPreStartHook
	}
	m.recordContainerEvent(ctx, pod, container, containerID, v1.EventTypeNormal, events.CreatedContainer, "Container created")

	// 步骤 3：启动容器。
	err = m.runtimeService.StartContainer(ctx, containerID)
	if err != nil {
		s, _ := grpcstatus.FromError(err)
		m.recordContainerEvent(ctx, pod, container, containerID, v1.EventTypeWarning, events.FailedToStartContainer, "Error: %v", s.Message())
		return s.Message(), kubecontainer.ErrRunContainer
	}
	m.recordContainerEvent(ctx, pod, container, containerID, v1.EventTypeNormal, events.StartedContainer, "Container started")

	// 为兼容集群日志，把容器日志软链接到旧版容器日志路径。
	// TODO(random-liu): 集群日志支持 CRI 容器日志路径后移除此逻辑。
	containerMeta := containerConfig.GetMetadata()
	sandboxMeta := podSandboxConfig.GetMetadata()
	legacySymlink := legacyLogSymlink(containerID, containerMeta.Name, sandboxMeta.Name,
		sandboxMeta.Namespace)
	containerLog := filepath.Join(podSandboxConfig.LogDirectory, containerConfig.LogPath)
	// 仅在 containerLog 路径存在（或错误不是 IsNotExist）时创建旧版软链接。
	// 若 containerLog 不存在，只会生成悬空的 legacySymlink，稍后会被容器 GC 移除，
	// 因此一开始就不应创建。使用 docker 的 journald 日志驱动时会出现这种情况。
	if _, err := m.osInterface.Stat(containerLog); !os.IsNotExist(err) {
		if err := m.osInterface.Symlink(containerLog, legacySymlink); err != nil {
			logger.Error(err, "Failed to create legacy symbolic link", "path", legacySymlink,
				"containerID", containerID, "containerLogPath", containerLog)
		}
	}

	// 步骤 4：执行 post start 钩子。
	if container.Lifecycle != nil && container.Lifecycle.PostStart != nil {
		kubeContainerID := kubecontainer.ContainerID{
			Type: m.runtimeName,
			ID:   containerID,
		}
		msg, handlerErr := m.runner.Run(ctx, kubeContainerID, pod, container, container.Lifecycle.PostStart)
		if handlerErr != nil {
			logger.Error(handlerErr, "Failed to execute PostStartHook", "pod", klog.KObj(pod),
				"podUID", pod.UID, "containerName", container.Name, "containerID", kubeContainerID.String())
			// 事件中不记录消息内容，避免泄露服务端 secret。
			m.recordContainerEvent(ctx, pod, container, kubeContainerID.ID, v1.EventTypeWarning, events.FailedPostStartHook, "PostStartHook failed")
			if err := m.killContainer(ctx, pod, kubeContainerID, container.Name, "FailedPostStartHook", reasonFailedPostStartHook, nil, nil); err != nil {
				logger.Error(err, "Failed to kill container", "pod", klog.KObj(pod),
					"podUID", pod.UID, "containerName", container.Name, "containerID", kubeContainerID.String())
			}
			return msg, ErrPostStartHook
		}
	}

	return "", nil
}

// generateContainerConfig 为 kubelet runtime v1 生成容器配置。
func (m *kubeGenericRuntimeManager) generateContainerConfig(ctx context.Context, container *v1.Container, pod *v1.Pod, restartCount int, podIP, imageRef string, podIPs []string, nsTarget *kubecontainer.ContainerID, imageVolumes kubecontainer.ImageVolumes) (*runtimeapi.ContainerConfig, func(), error) {
	opts, cleanupAction, err := m.runtimeHelper.GenerateRunContainerOptions(ctx, pod, container, podIP, podIPs, imageVolumes)
	if err != nil {
		return nil, nil, err
	}

	uid, username, err := m.getImageUser(ctx, container.Image)
	if err != nil {
		return nil, cleanupAction, err
	}

	// 校验 RunAsNonRoot。非 root 校验只支持数字用户。
	if err := verifyRunAsNonRoot(ctx, pod, container, uid, username); err != nil {
		return nil, cleanupAction, err
	}

	command, args := kubecontainer.ExpandContainerCommandAndArgs(container, opts.Envs)
	logDir := BuildContainerLogsDirectory(m.podLogsDirectory, pod.Namespace, pod.Name, pod.UID, container.Name)
	err = m.osInterface.MkdirAll(logDir, 0755)
	if err != nil {
		return nil, cleanupAction, fmt.Errorf("create container log directory for container %s failed: %v", container.Name, err)
	}
	containerLogsPath := buildContainerLogsPath(container.Name, restartCount)
	stopsignal := getContainerConfigStopSignal(container)
	restartCountUint32 := uint32(restartCount)
	config := &runtimeapi.ContainerConfig{
		Metadata: &runtimeapi.ContainerMetadata{
			Name:    container.Name,
			Attempt: restartCountUint32,
		},
		Image:       &runtimeapi.ImageSpec{Image: imageRef, UserSpecifiedImage: container.Image},
		Command:     command,
		Args:        args,
		WorkingDir:  container.WorkingDir,
		Labels:      newContainerLabels(container, pod),
		Annotations: newContainerAnnotations(ctx, container, pod, restartCount, opts),
		Devices:     makeDevices(opts),
		CDIDevices:  makeCDIDevices(opts),
		Mounts:      m.makeMounts(opts, container),
		LogPath:     containerLogsPath,
		Stdin:       container.Stdin,
		StdinOnce:   container.StdinOnce,
		Tty:         container.TTY,
	}

	if stopsignal != nil {
		config.StopSignal = *stopsignal
	}
	// 设置平台相关的配置。
	if err := m.applyPlatformSpecificContainerConfig(ctx, config, container, pod, uid, username, nsTarget); err != nil {
		return nil, cleanupAction, err
	}

	// 设置环境变量
	envs := make([]*runtimeapi.KeyValue, len(opts.Envs))
	for idx := range opts.Envs {
		e := opts.Envs[idx]
		envs[idx] = &runtimeapi.KeyValue{
			Key:   e.Name,
			Value: []byte(e.Value),
		}
	}
	config.Envs = envs

	return config, cleanupAction, nil
}

// updateContainerResources 把容器的资源变更下发给运行时，成功后记录为已生效。
func (m *kubeGenericRuntimeManager) updateContainerResources(ctx context.Context, pod *v1.Pod, container *v1.Container, containerID kubecontainer.ContainerID) error {
	logger := klog.FromContext(ctx)
	containerResources := m.generateContainerResources(ctx, pod, container)
	if containerResources == nil {
		return fmt.Errorf("container %q updateContainerResources failed: cannot generate resources config", containerID.String())
	}
	err := m.runtimeService.UpdateContainerResources(ctx, containerID.ID, containerResources)
	if err == nil {
		// 仅在运行时更新成功后才记录已生效资源，避免把未落地的配置当作实际状态。
		err = m.setActuatedContainerResources(logger, pod, container)
	}
	return err
}

// setActuatedContainerResources 记录容器当前实际生效的资源。
// 开启 Pod 级垂直扩缩容时，limit 需按 Pod 与容器资源的合并结果计算。
func (m *kubeGenericRuntimeManager) setActuatedContainerResources(logger klog.Logger, pod *v1.Pod, container *v1.Container) error {
	containerResources := container.Resources
	if utilfeature.DefaultFeatureGate.Enabled(features.InPlacePodLevelResourcesVerticalScaling) {
		containerResources = *containerResources.DeepCopy()
		containerResources.Limits = kubeutil.GetLimits(&kubeutil.ResourceOpts{PodResources: pod.Spec.Resources, ContainerResources: &container.Resources})
	}
	return m.actuatedState.SetContainerResources(logger, pod.UID, container.Name, containerResources)
}

// updatePodSandboxResources 更新 sandbox 的 Pod 级资源，属于尽力而为的操作。
func (m *kubeGenericRuntimeManager) updatePodSandboxResources(ctx context.Context, sandboxID string, pod *v1.Pod, podResources *cm.ResourceConfig) error {
	logger := klog.FromContext(ctx)
	podResourcesRequest := m.generateUpdatePodSandboxResourcesRequest(sandboxID, pod, podResources)
	if podResourcesRequest == nil {
		return fmt.Errorf("sandboxID %q updatePodSandboxResources failed: cannot generate resources config", sandboxID)
	}

	_, err := m.runtimeService.UpdatePodSandboxResources(ctx, podResourcesRequest)
	if err != nil {
		stat, _ := grpcstatus.FromError(err)

		// containerd 2.2 返回 code 为 'Unknown'、消息为 'not implemented yet' 的错误，
		// 而不是 'Unimplemented'。该问题正在 https://github.com/containerd/containerd/pull/13023
		// 修复，待 containerd 2.2 不再受支持后可移除这段硬编码字符串检查。
		const unimplementedMsg = "not implemented"
		if stat.Code() == codes.Unimplemented || (stat.Code() == codes.Unknown && strings.Contains(stat.Message(), unimplementedMsg)) {
			logger.V(3).Info("updatePodSandboxResources failed: unimplemented; this call is best-effort: proceeding with resize", "sandboxID", sandboxID)
			return nil
		}
		return fmt.Errorf("updatePodSandboxResources failed for sanboxID %q: %w", sandboxID, err)
	}
	return nil
}

// makeDevices 为 kubelet runtime v1 生成容器设备列表。
func makeDevices(opts *kubecontainer.RunContainerOptions) []*runtimeapi.Device {
	devices := make([]*runtimeapi.Device, len(opts.Devices))

	for idx := range opts.Devices {
		device := opts.Devices[idx]
		devices[idx] = &runtimeapi.Device{
			HostPath:      device.PathOnHost,
			ContainerPath: device.PathInContainer,
			Permissions:   device.Permissions,
		}
	}

	return devices
}

// makeCDIDevices 为 kubelet runtime v1 生成容器 CDI 设备列表。
func makeCDIDevices(opts *kubecontainer.RunContainerOptions) []*runtimeapi.CDIDevice {
	devices := make([]*runtimeapi.CDIDevice, len(opts.CDIDevices))

	for i, device := range opts.CDIDevices {
		devices[i] = &runtimeapi.CDIDevice{
			Name: device.Name,
		}
	}

	return devices
}

// makeMounts 为 kubelet runtime v1 生成容器卷挂载列表。
func (m *kubeGenericRuntimeManager) makeMounts(opts *kubecontainer.RunContainerOptions, container *v1.Container) []*runtimeapi.Mount {
	volumeMounts := []*runtimeapi.Mount{}

	for idx := range opts.Mounts {
		v := opts.Mounts[idx]
		selinuxRelabel := v.SELinuxRelabel && selinux.GetEnabled()
		mount := &runtimeapi.Mount{
			HostPath:          v.HostPath,
			ContainerPath:     v.ContainerPath,
			Readonly:          v.ReadOnly,
			SelinuxRelabel:    selinuxRelabel,
			Propagation:       v.Propagation,
			RecursiveReadOnly: v.RecursiveReadOnly,
			Image:             v.Image,
			ImageSubPath:      v.ImageSubPath,
			MountOptions:      v.BindMountOptions,
		}

		volumeMounts = append(volumeMounts, mount)
	}

	// 在 kubelet 之外（此处）创建并挂载日志文件，是因为文件位置依赖容器 ID，
	// 且必须在真正启动容器之前创建并挂载。
	if opts.PodContainerDir != "" && len(container.TerminationMessagePath) != 0 {
		// PodContainerDir 已包含 Pod UID 和容器名，足够唯一；
		// 这里再加一个随机 id，使同一容器的不同实例路径也不同。
		cid := makeUID()
		containerLogPath := filepath.Join(opts.PodContainerDir, cid)
		fs, err := m.osInterface.Create(containerLogPath)
		if err != nil {
			utilruntime.HandleError(fmt.Errorf("error on creating termination-log file %q: %v", containerLogPath, err))
		} else {
			fs.Close()

			// 需要 Chmod，是因为 os.Create() 最终调用 open(2)，
			// 实际模式为 "mode & ~umask"。这里确保无论 umask 如何都使用指定模式。
			if err := m.osInterface.Chmod(containerLogPath, 0666); err != nil {
				utilruntime.HandleError(fmt.Errorf("unable to set termination-log file permissions %q: %v", containerLogPath, err))
			}

			// Windows 上卷挂载路径必须是 C:/ 形式，否则会失败
			containerLogPath = volumeutil.MakeAbsolutePath(goruntime.GOOS, containerLogPath)
			terminationMessagePath := volumeutil.MakeAbsolutePath(goruntime.GOOS, container.TerminationMessagePath)
			selinuxRelabel := selinux.GetEnabled()
			volumeMounts = append(volumeMounts, &runtimeapi.Mount{
				HostPath:       containerLogPath,
				ContainerPath:  terminationMessagePath,
				SelinuxRelabel: selinuxRelabel,
			})
		}
	}

	return volumeMounts
}

// makeUID 返回随机生成的字符串。
func makeUID() string {
	return fmt.Sprintf("%08x", rand.Uint32())
}

// getTerminationMessage 在文件系统中查找给定的 termination message 路径，
// 返回有限长度的内容；第二个返回值表示是否应回退到读取日志。
func getTerminationMessage(status *runtimeapi.ContainerStatus, terminationMessagePath string, fallbackToLogs bool) (string, bool) {
	if len(terminationMessagePath) == 0 {
		return "", fallbackToLogs
	}
	// Windows 上卷挂载路径必须是 C:/ 形式，否则会失败
	terminationMessagePath = volumeutil.MakeAbsolutePath(goruntime.GOOS, terminationMessagePath)
	for _, mount := range status.Mounts {
		if mount.ContainerPath != terminationMessagePath {
			continue
		}
		path := mount.HostPath
		data, _, err := tail.ReadAtMost(path, kubecontainer.MaxContainerTerminationMessageLength)
		if err != nil {
			if os.IsNotExist(err) {
				return "", fallbackToLogs
			}
			return fmt.Sprintf("Error on reading termination log %s: %v", path, err), false
		}
		return string(data), (fallbackToLogs && len(data) == 0)
	}
	return "", fallbackToLogs
}

// readLastStringFromContainerLogs 尝试从 path 对应的 CRI 日志末尾读取最多 max log 行。
func (m *kubeGenericRuntimeManager) readLastStringFromContainerLogs(ctx context.Context, path string) string {
	value := int64(kubecontainer.MaxContainerTerminationMessageLogLines)
	buf, _ := buffer.NewTypedRingFixed[byte](kubecontainer.MaxContainerTerminationMessageLogLength)
	if err := m.ReadLogs(ctx, path, "", &v1.PodLogOptions{TailLines: &value}, buf, buf); err != nil {
		return fmt.Sprintf("Error on reading termination message from logs: %v", err)
	}
	return string(buf.Slice())
}

// convertToKubeContainerStatus 把 CRI 容器状态转换为 kubelet 内部状态，
// 对已退出的容器补充 termination message。
func (m *kubeGenericRuntimeManager) convertToKubeContainerStatus(ctx context.Context, podUID kubetypes.UID, status *runtimeapi.ContainerStatus) (cStatus *kubecontainer.Status) {
	cStatus = m.toKubeContainerStatus(ctx, podUID, status, m.runtimeName)
	if status.State == runtimeapi.ContainerState_CONTAINER_EXITED {
		// 按需填充 termination message。
		annotatedInfo := getContainerInfoFromAnnotations(ctx, status.Annotations)
		// 容器连启动都没能完成时肯定没有日志，无需回退到日志。
		fallbackToLogs := annotatedInfo.TerminationMessagePolicy == v1.TerminationMessageFallbackToLogsOnError &&
			cStatus.ExitCode != 0 && cStatus.Reason != "ContainerCannotRun"
		tMessage, checkLogs := getTerminationMessage(status, annotatedInfo.TerminationMessagePath, fallbackToLogs)
		if checkLogs {
			tMessage = m.readLastStringFromContainerLogs(ctx, status.GetLogPath())
		}
		// 应用写入的 termination message 非空时追加到消息中
		if len(tMessage) != 0 {
			if len(cStatus.Message) != 0 {
				cStatus.Message += ": "
			}
			cStatus.Message += tMessage
		}
	}
	return cStatus
}

// listOptions 定义容器/sandbox 列表查询的过滤条件。
type listOptions struct {
	// podUID 为要过滤的 Pod UID，可选。
	podUID kubetypes.UID
	// onlyRunningReady 为 true 时只返回就绪的 sandbox 或运行中的容器，可选。
	onlyRunningReady bool
}

// containerFilter 根据 listOptions 构造 CRI 容器过滤条件。
func (o *listOptions) containerFilter() *runtimeapi.ContainerFilter {
	filter := &runtimeapi.ContainerFilter{}
	if o.podUID != "" {
		filter.LabelSelector = map[string]string{kubelettypes.KubernetesPodUIDLabel: string(o.podUID)}
	}
	if o.onlyRunningReady {
		filter.State = &runtimeapi.ContainerStateValue{
			State: runtimeapi.ContainerState_CONTAINER_RUNNING,
		}
	}
	return filter
}

// sandboxFilter 根据 listOptions 构造 CRI sandbox 过滤条件。
func (o *listOptions) sandboxFilter() *runtimeapi.PodSandboxFilter {
	filter := &runtimeapi.PodSandboxFilter{}
	if o.podUID != "" {
		filter.LabelSelector = map[string]string{kubelettypes.KubernetesPodUIDLabel: string(o.podUID)}
	}
	if o.onlyRunningReady {
		filter.State = &runtimeapi.PodSandboxStateValue{
			State: runtimeapi.PodSandboxState_SANDBOX_READY,
		}
	}
	return filter
}

// getContainers 按过滤条件列出容器。
func (m *kubeGenericRuntimeManager) getContainers(ctx context.Context, opts listOptions) ([]*runtimeapi.Container, error) {
	return m.runtimeService.ListContainers(ctx, opts.containerFilter())
}

// getSandboxes 按过滤条件列出 Pod sandbox。
func (m *kubeGenericRuntimeManager) getSandboxes(ctx context.Context, opts listOptions) ([]*runtimeapi.PodSandbox, error) {
	return m.runtimeService.ListPodSandbox(ctx, opts.sandboxFilter())
}

// getPodContainerStatuses 获取 Pod 中所有容器的状态。
func (m *kubeGenericRuntimeManager) getPodContainerStatuses(ctx context.Context, pod *kubecontainer.Pod, activePodSandboxID string) ([]*kubecontainer.Status, []*kubecontainer.Status, error) {
	logger := klog.FromContext(ctx)

	statuses := []*kubecontainer.Status{}
	activeContainerStatuses := []*kubecontainer.Status{}
	// TODO: 优化检查范围，限制每个容器名称最多检查的实例数。
	for _, c := range pod.Containers {
		resp, err := m.runtimeService.ContainerStatus(ctx, c.ID.ID, false)
		// List（ListContainers）与检查（ContainerStatus）之间，其他线程可能移除该容器，这是正常的。
		// 上一次调用（ListContainers）不会因 Pod 容器不存在而失败，
		// 因此本方法也不应失败，而应像上次调用失败一样忽略该错误。
		if crierror.IsNotFound(err) {
			continue
		}
		if err != nil {
			// 这里只记录日志；错误会由 GetPodStatus 上报。
			logger.V(4).Info("ContainerStatus return error", "containerID", c.ID.ID, "err", err)
			return nil, nil, err
		}
		status := resp.GetStatus()
		if status == nil {
			return nil, nil, remote.ErrContainerStatusNil
		}
		cStatus := m.convertToKubeContainerStatus(ctx, pod.ID, status)
		statuses = append(statuses, cStatus)
		if c.PodSandboxID == activePodSandboxID {
			activeContainerStatuses = append(activeContainerStatuses, cStatus)
		}
	}

	sort.Sort(containerStatusByCreated(statuses))
	sort.Sort(containerStatusByCreated(activeContainerStatuses))
	return statuses, activeContainerStatuses, nil
}

// toKubeContainerStatus 把 CRI 容器状态转换为 kubelet 内部容器状态。
func (m *kubeGenericRuntimeManager) toKubeContainerStatus(ctx context.Context, podUID kubetypes.UID, status *runtimeapi.ContainerStatus, runtimeName string) *kubecontainer.Status {
	annotatedInfo := getContainerInfoFromAnnotations(ctx, status.Annotations)
	labeledInfo := getContainerInfoFromLabels(ctx, status.Labels)
	var cStatusResources *kubecontainer.ContainerResources
	if utilfeature.DefaultFeatureGate.Enabled(features.InPlacePodVerticalScaling) {
		// 若运行时上报了 cpu 和内存资源信息，则加入容器状态
		cStatusResources = toKubeContainerResources(status.Resources)

		// 用已生效的资源补齐缺失的资源值。
		if cStatusResources == nil || cStatusResources.MemoryRequest == nil {
			if actuatedResources, ok := m.actuatedState.GetContainerResources(podUID, labeledInfo.ContainerName); ok {
				if cStatusResources == nil {
					cStatusResources = &kubecontainer.ContainerResources{
						CPURequest:    actuatedResources.Requests.Cpu(),
						CPULimit:      actuatedResources.Limits.Cpu(),
						MemoryRequest: actuatedResources.Requests.Memory(),
						MemoryLimit:   actuatedResources.Limits.Memory(),
					}
				} else if cStatusResources.MemoryRequest == nil {
					if memReq := actuatedResources.Requests[v1.ResourceMemory]; !memReq.IsZero() {
						cStatusResources.MemoryRequest = &memReq
					}
				}
			}
		}
	}

	// 为兼容旧版运行时：status.ImageId 在 v1.30 才加入
	imageID := status.ImageRef
	if status.ImageId != "" {
		imageID = status.ImageId
	}

	var cStatusUser *kubecontainer.ContainerUser
	if utilfeature.DefaultFeatureGate.Enabled(features.SupplementalGroupsPolicy) {
		cStatusUser = toKubeContainerUser(status.User)
	}

	var cStatusStopSignal *v1.Signal
	if utilfeature.DefaultFeatureGate.Enabled(features.ContainerStopSignals) {
		signal := status.GetStopSignal().String()
		// Signal_SIGNAL_RUNTIME_DEFAULT 表示运行时未返回 StopSignal，
		// 只会发生在容器运行时版本尚不支持 StopSignal 时
		if signal != "" && signal != runtimeapi.Signal_SIGNAL_RUNTIME_DEFAULT.String() {
			cStatusStopSignal = runtimeSignalToString(status.GetStopSignal())
		}
	}

	cStatus := &kubecontainer.Status{
		ID: kubecontainer.ContainerID{
			Type: runtimeName,
			ID:   status.Id,
		},
		Name:                labeledInfo.ContainerName,
		Image:               status.Image.Image,
		ImageID:             imageID,
		ImageRef:            status.ImageRef,
		ImageRuntimeHandler: status.Image.RuntimeHandler,
		Hash:                annotatedInfo.Hash,
		RestartCount:        annotatedInfo.RestartCount,
		State:               toKubeContainerState(status.State),
		CreatedAt:           time.Unix(0, status.CreatedAt),
		Resources:           cStatusResources,
		User:                cStatusUser,
		StopSignal:          cStatusStopSignal,
	}

	if status.State != runtimeapi.ContainerState_CONTAINER_CREATED {
		// 容器不处于 created 状态，说明已尝试并启动过，设置 StartedAt 时间。
		cStatus.StartedAt = time.Unix(0, status.StartedAt)
	}
	if status.State == runtimeapi.ContainerState_CONTAINER_EXITED {
		cStatus.Reason = status.Reason
		cStatus.Message = status.Message
		cStatus.ExitCode = int(status.ExitCode)
		cStatus.FinishedAt = time.Unix(0, status.FinishedAt)
	}

	for _, mount := range status.Mounts {
		cStatus.Mounts = append(cStatus.Mounts, kubecontainer.Mount{
			HostPath:          mount.HostPath,
			ContainerPath:     mount.ContainerPath,
			ReadOnly:          mount.Readonly,
			RecursiveReadOnly: mount.RecursiveReadOnly,
			SELinuxRelabel:    mount.SelinuxRelabel,
			Propagation:       mount.Propagation,
			Image:             mount.Image,
			ImageSubPath:      mount.ImageSubPath,
		})
	}
	return cStatus
}

// executePreStopHook 在适用时执行 pre-stop 生命周期钩子，并返回其耗时。
func (m *kubeGenericRuntimeManager) executePreStopHook(ctx context.Context, pod *v1.Pod, containerID kubecontainer.ContainerID, containerSpec *v1.Container, gracePeriod int64) int64 {
	logger := klog.FromContext(ctx)
	logger.V(3).Info("Running preStop hook", "pod", klog.KObj(pod), "podUID", pod.UID, "containerName", containerSpec.Name, "containerID", containerID.String())

	start := metav1.Now()
	done := make(chan struct{})
	go func() {
		defer close(done)
		defer utilruntime.HandleCrashWithContext(ctx)
		if _, err := m.runner.Run(ctx, containerID, pod, containerSpec, containerSpec.Lifecycle.PreStop); err != nil {
			logger.Error(err, "PreStop hook failed", "pod", klog.KObj(pod), "podUID", pod.UID,
				"containerName", containerSpec.Name, "containerID", containerID.String())
			// 事件中不记录消息内容，避免泄露服务端 secret。
			m.recordContainerEvent(ctx, pod, containerSpec, containerID.ID, v1.EventTypeWarning, events.FailedPreStopHook, "PreStopHook failed")
		}
	}()

	select {
	case <-time.After(time.Duration(gracePeriod) * time.Second):
		logger.V(2).Info("PreStop hook not completed in grace period", "pod", klog.KObj(pod), "podUID", pod.UID,
			"containerName", containerSpec.Name, "containerID", containerID.String(), "gracePeriod", gracePeriod)
	case <-done:
		logger.V(3).Info("PreStop hook completed", "pod", klog.KObj(pod), "podUID", pod.UID,
			"containerName", containerSpec.Name, "containerID", containerID.String())
	}

	return int64(metav1.Now().Sub(start.Time).Seconds())
}

// restoreSpecsFromContainerLabels 恢复 kill 容器所需的全部信息。
// 某些情况下（例如 kubelet 重启期间 Pod 被删除）kill 容器时可能拿不到 Pod 和容器 spec。
// 为此我们已把必要信息写入容器 label，这里从 label 中取出并还原 spec。
// TODO(random-liu): 增加 node e2e 测试覆盖该行为。
// TODO(random-liu): 把生命周期处理器改为只接收所需信息，从而无需构造假对象。
func (m *kubeGenericRuntimeManager) restoreSpecsFromContainerLabels(ctx context.Context, containerID kubecontainer.ContainerID) (*v1.Pod, *v1.Container, error) {
	var pod *v1.Pod
	var container *v1.Container
	resp, err := m.runtimeService.ContainerStatus(ctx, containerID.ID, false)
	if err != nil {
		return nil, nil, err
	}
	s := resp.GetStatus()
	if s == nil {
		return nil, nil, remote.ErrContainerStatusNil
	}

	l := getContainerInfoFromLabels(ctx, s.Labels)
	a := getContainerInfoFromAnnotations(ctx, s.Annotations)
	// 注意以下并非完整 spec，kill 容器相关代码不应使用未恢复的字段。
	pod = &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			UID:                        l.PodUID,
			Name:                       l.PodName,
			Namespace:                  l.PodNamespace,
			DeletionGracePeriodSeconds: a.PodDeletionGracePeriod,
		},
		Spec: v1.PodSpec{
			TerminationGracePeriodSeconds: a.PodTerminationGracePeriod,
		},
	}
	container = &v1.Container{
		Name:                   l.ContainerName,
		Ports:                  a.ContainerPorts,
		TerminationMessagePath: a.TerminationMessagePath,
	}
	if a.PreStopHandler != nil {
		container.Lifecycle = &v1.Lifecycle{
			PreStop: a.PreStopHandler,
		}
	}
	return pod, container, nil
}

// killContainer 通过以下步骤 kill 容器：
// * 执行 pre-stop 生命周期钩子（如适用）。
// * 停止容器。
func (m *kubeGenericRuntimeManager) killContainer(ctx context.Context, pod *v1.Pod, containerID kubecontainer.ContainerID, containerName string, message string, reason containerKillReason, gracePeriodOverride *int64, ordering *terminationOrdering) error {
	logger := klog.FromContext(ctx)
	var containerSpec *v1.Container
	if pod != nil {
		if containerSpec = kubecontainer.GetContainerSpec(pod, containerName); containerSpec == nil {
			return fmt.Errorf("failed to get containerSpec %q (id=%q) in pod %q when killing container for reason %q",
				containerName, containerID.String(), format.Pod(pod), message)
		}
	} else {
		// 任一 spec 为 nil 时恢复必要信息。
		restoredPod, restoredContainer, err := m.restoreSpecsFromContainerLabels(ctx, containerID)
		if err != nil {
			return err
		}
		pod, containerSpec = restoredPod, restoredContainer
	}

	// 从这里开始，pod 和 container 一定非 nil。
	gracePeriod := setTerminationGracePeriod(ctx, pod, containerSpec, containerName, containerID, reason)

	if len(message) == 0 {
		message = fmt.Sprintf("Stopping container %s", containerSpec.Name)
	}
	m.recordContainerEvent(ctx, pod, containerSpec, containerID.ID, v1.EventTypeNormal, events.KillingContainer, "%v", message)

	if gracePeriodOverride != nil {
		gracePeriod = *gracePeriodOverride
		logger.V(3).Info("Killing container with a grace period override", "pod", klog.KObj(pod), "podUID", pod.UID,
			"containerName", containerName, "containerID", containerID.String(), "gracePeriod", gracePeriod)
	}

	// 在适用且剩余时间足够时执行 pre-stop 生命周期钩子
	if containerSpec.Lifecycle != nil && containerSpec.Lifecycle.PreStop != nil && gracePeriod > 0 {
		gracePeriod = gracePeriod - m.executePreStopHook(ctx, pod, containerID, containerSpec, gracePeriod)
	}

	// 若关心终止顺序，在剩余时间内等待该容器轮到退出
	if ordering != nil && gracePeriod > 0 {
		// grace period 以秒为单位，等待时间向下取整
		gracePeriod -= int64(ordering.waitForTurn(containerName, gracePeriod))
	}

	// 始终给容器一个最小关闭窗口，避免不必要的 SIGKILL
	if gracePeriod < minimumGracePeriodInSeconds {
		gracePeriod = minimumGracePeriodInSeconds
	}

	logger.V(2).Info("Killing container with a grace period", "pod", klog.KObj(pod), "podUID", pod.UID,
		"containerName", containerName, "containerID", containerID.String(), "gracePeriod", gracePeriod)

	err := m.runtimeService.StopContainer(ctx, containerID.ID, gracePeriod)
	if err != nil && !crierror.IsNotFound(err) {
		logger.Error(err, "Container termination failed with gracePeriod", "pod", klog.KObj(pod), "podUID", pod.UID,
			"containerName", containerName, "containerID", containerID.String(), "gracePeriod", gracePeriod)
		return err
	}
	logger.V(3).Info("Container exited normally", "pod", klog.KObj(pod), "podUID", pod.UID,
		"containerName", containerName, "containerID", containerID.String())

	if ordering != nil {
		ordering.containerTerminated(containerName)
	}

	return nil
}

// killContainersWithSyncResult 并发 kill Pod 的所有容器，并汇总同步结果。
func (m *kubeGenericRuntimeManager) killContainersWithSyncResult(ctx context.Context, pod *v1.Pod, runningPod kubecontainer.Pod, gracePeriodOverride *int64) (syncResults []*kubecontainer.SyncResult) {
	logger := klog.FromContext(ctx)
	containerResults := make(chan *kubecontainer.SyncResult, len(runningPod.Containers))
	wg := sync.WaitGroup{}

	wg.Add(len(runningPod.Containers))
	var termOrdering *terminationOrdering
	if types.HasRestartableInitContainer(pod) {
		var runningContainerNames []string
		for _, container := range runningPod.Containers {
			runningContainerNames = append(runningContainerNames, container.Name)
		}
		termOrdering = newTerminationOrdering(pod, runningContainerNames)
	}
	for _, container := range runningPod.Containers {
		go func(container *kubecontainer.Container) {
			defer utilruntime.HandleCrashWithContext(ctx)
			defer wg.Done()

			killContainerResult := kubecontainer.NewSyncResult(kubecontainer.KillContainer, container.Name)
			if err := m.killContainer(ctx, pod, container.ID, container.Name, "", reasonUnknown, gracePeriodOverride, termOrdering); err != nil {
				killContainerResult.Fail(kubecontainer.ErrKillContainer, err.Error())
				// 日志使用 runningPod，因为传入的 pod 可能为 nil。
				logger.Error(err, "Kill container failed", "pod", klog.KRef(runningPod.Namespace, runningPod.Name), "podUID", runningPod.ID,
					"containerName", container.Name, "containerID", container.ID)
			}
			containerResults <- killContainerResult
		}(container)
	}
	wg.Wait()
	close(containerResults)

	for containerResult := range containerResults {
		syncResults = append(syncResults, containerResult)
	}
	return
}

// pruneInitContainersBeforeStart 确保在开始创建 init 容器之前，减少仍存在的
// init 容器数量：每个名称只保留最近一次已终止的实例，以降低容器 GC 的负载。
func (m *kubeGenericRuntimeManager) pruneInitContainersBeforeStart(ctx context.Context, pod *v1.Pod, podStatus *kubecontainer.PodStatus) {
	logger := klog.FromContext(ctx)
	// 每个 init 容器只应保留最后一次执行，且仅当它属于需要保留的 init 容器列表时才保留。
	initContainerNames := sets.New[string]()
	for _, container := range pod.Spec.InitContainers {
		initContainerNames.Insert(container.Name)
	}
	for name := range initContainerNames {
		count := 0
		for _, status := range podStatus.ContainerStatuses {
			if status.Name != name ||
				(status.State != kubecontainer.ContainerStateExited &&
					status.State != kubecontainer.ContainerStateUnknown) {
				continue
			}
			// 移除处于 unknown 状态的 init 容器。调用 pruneInitContainersBeforeStart
			// 之前它本应已被停止。
			count++
			// 保留该名称的第一个 init 容器
			if count == 1 {
				continue
			}
			// 裁剪掉其他同名 init 容器
			logger.V(4).Info("Removing init container", "containerName", status.Name, "containerID", status.ID.ID, "count", count)
			if err := m.removeContainer(ctx, status.ID.ID, false); err != nil {
				utilruntime.HandleError(fmt.Errorf("failed to remove pod init container %q: %v; Skipping pod %q", status.Name, err, format.Pod(pod)))
				continue
			}
		}
	}
}

// purgeInitContainers 移除所有 init 容器。该函数不检查容器状态，
// 因为它假定调用前所有 init 容器都已被停止。
func (m *kubeGenericRuntimeManager) purgeInitContainers(ctx context.Context, pod *v1.Pod, podStatus *kubecontainer.PodStatus) {
	logger := klog.FromContext(ctx)
	initContainerNames := sets.New[string]()
	for _, container := range pod.Spec.InitContainers {
		initContainerNames.Insert(container.Name)
	}
	for name := range initContainerNames {
		count := 0
		for _, status := range podStatus.ContainerStatuses {
			if status.Name != name {
				continue
			}
			count++
			// 清除所有同名 init 容器
			logger.V(4).Info("Removing init container", "containerName", status.Name, "containerID", status.ID.ID, "count", count)
			if err := m.removeContainer(ctx, status.ID.ID, false); err != nil {
				utilruntime.HandleError(fmt.Errorf("failed to remove pod init container %q: %v; Skipping pod %q", status.Name, err, format.Pod(pod)))
				continue
			}
		}
	}
}

// HasAnyRegularContainerCreated 判断是否已有普通容器被创建，
// 这表示所有 init 容器都已完成初始化。
func HasAnyRegularContainerCreated(pod *v1.Pod, podStatus *kubecontainer.PodStatus) bool {
	for _, container := range pod.Spec.Containers {
		status := podStatus.FindContainerStatusByName(container.Name)
		if status == nil {
			continue
		}
		switch status.State {
		case kubecontainer.ContainerStateCreated,
			kubecontainer.ContainerStateRunning,
			kubecontainer.ContainerStateExited:
			return true
		default:
			// 忽略其他状态
		}
	}
	return false
}

// computeInitContainerActions 针对 init 容器在 changes 上设置所需动作，
// 包括初始化 init 容器以及保持可重启 init 容器运行。
// 返回值表示 Pod 是否已完成初始化。
//
// 动作包括：
// - 启动第一个尚未启动的 init 容器。
// - 重启所有已启动但未运行的可重启 init 容器。
// - kill 所有不存活或未启动的可重启 init 容器。
func (m *kubeGenericRuntimeManager) computeInitContainerActions(ctx context.Context, pod *v1.Pod, podStatus *kubecontainer.PodStatus, changes *podActions) bool {
	logger := klog.FromContext(ctx)
	if len(pod.Spec.InitContainers) == 0 {
		return true
	}

	// 若 ActiveContainerStatuses 中包含主容器（即来自 pod.Spec.Containers），
	// 说明当前 sandbox 上已有主容器启动，意味着所有 init 容器都已完成。
	// 这里把 Pod 视为已初始化，可避免运行时可能已 GC 掉的不可重启 init 容器被错误地重新执行。
	//
	// 使用 ActiveContainerStatuses 把检查限定在当前 sandbox，这样上个 sandbox
	// 遗留的主容器（例如节点重启后）不会错误地把 Pod 标记为已初始化。
	podHasInitialized := kubecontainer.HasAnyActiveRegularContainerStarted(&pod.Spec, podStatus)

	// isPreviouslyInitialized 表示当前 init 容器此前是否已初始化。
	isPreviouslyInitialized := podHasInitialized
	// ContainerRestartRules 特性门控允许单个 init 容器在 Pod 级重启策略为 Never 时
	// 仍然重启（在容器级检查）。该 restartOnFailure 会在特性门控开启时被覆盖。
	restartOnFailure := shouldRestartOnFailure(pod)

	// 注意这里逆序遍历 init 容器以找到下一个要运行的 init 容器：
	// 已完成的 init 容器可能因各种原因被运行时移除，因此 kubelet 应依赖
	// 最少数量的 init 容器——即最后一个。
	//
	// 找到下一个要运行的 init 容器后，继续遍历其余容器以找出需要重启的可重启 init 容器。
	for i := len(pod.Spec.InitContainers) - 1; i >= 0; i-- {
		container := &pod.Spec.InitContainers[i]
		status := podStatus.FindActiveContainerStatusByName(container.Name)
		logger.V(4).Info("Computing init container action", "pod", klog.KObj(pod), "container", container.Name, "status", status)
		if status == nil {
			// 若容器此前已初始化但找不到状态，说明其最后状态被移除；
			// 若它是可重启 init 容器，则重启它。
			if isPreviouslyInitialized && podutil.IsRestartableInitContainer(container) {
				changes.InitContainersToStart = append(changes.InitContainersToStart, i)
			}
			continue
		}

		if isPreviouslyInitialized && !podutil.IsRestartableInitContainer(container) {
			// 初始化完成后，只需保持可重启 init 容器继续运行
			continue
		}

		switch status.State {
		case kubecontainer.ContainerStateCreated:
			// 主同步循环本应一步完成容器的创建和启动。若 init 容器处于 created 状态，
			// 很可能是运行时启动失败。为避免容器卡在 created 状态，重启它。
			changes.InitContainersToStart = append(changes.InitContainersToStart, i)

		case kubecontainer.ContainerStateRunning:
			// 不可重启的 init 容器只可能因扩缩容而需要重启。
			if !podutil.IsRestartableInitContainer(container) {
				if utilfeature.DefaultFeatureGate.Enabled(features.InPlacePodVerticalScalingInitContainers) {
					// 若扩缩容策略要求重启该容器，computePodResizeAction 会更新 changes
					_ = m.computePodResizeAction(ctx, pod, i, true, status, changes)
				}
				break
			} else {
				if container.StartupProbe != nil {
					startup, found := m.startupManager.Get(status.ID)
					if !found {
						// 启动探针尚未运行，等待
						break
					}
					if startup != proberesults.Success {
						if startup == proberesults.Failure {
							// 可重启 init 容器启动探针失败时重启它
							changes.ContainersToKill[status.ID] = containerToKillInfo{
								name:      container.Name,
								container: container,
								message:   fmt.Sprintf("Init container %s failed startup probe", container.Name),
								reason:    reasonStartupProbe,
							}
							changes.InitContainersToStart = append(changes.InitContainersToStart, i)
						}
						break
					}
				}

				logger.V(4).Info("Init container has been initialized", "pod", klog.KObj(pod), "container", container.Name)
				if i == (len(pod.Spec.InitContainers) - 1) {
					podHasInitialized = true
				} else if !isPreviouslyInitialized {
					// 该 init 容器首次初始化完成，启动下一个
					changes.InitContainersToStart = append(changes.InitContainersToStart, i+1)
				}

				// 重启定义已发生变化的运行中 sidecar 容器。
				if _, _, changed := containerChanged(container, status); changed {
					changes.ContainersToKill[status.ID] = containerToKillInfo{
						name:      container.Name,
						container: container,
						message:   fmt.Sprintf("Init container %s definition changed", container.Name),
						reason:    "",
					}
					changes.InitContainersToStart = append(changes.InitContainersToStart, i)
					break
				}

				// 可重启 init 容器在决定启动下一个 init 容器时无需考虑自身的 liveness 探针。
				if container.LivenessProbe != nil {
					liveness, found := m.livenessManager.Get(status.ID)
					if !found {
						// liveness 探针尚未运行，等待
						break
					}
					if liveness == proberesults.Failure {
						// 可重启 init 容器 liveness 探针失败时重启它
						changes.ContainersToKill[status.ID] = containerToKillInfo{
							name:      container.Name,
							container: container,
							message:   fmt.Sprintf("Init container %s failed liveness probe", container.Name),
							reason:    reasonLivenessProbe,
						}
						changes.InitContainersToStart = append(changes.InitContainersToStart, i)
						// 容器正在重启，无需其他动作。
						break
					}
				}

				if !m.computePodResizeAction(ctx, pod, i, true, status, changes) {
					// 若扩缩容策略要求重启该容器，computePodResizeAction 会更新 changes
					break
				}
			}

		// 若 init 容器失败且重启策略为 Never，Pod 进入终态；否则重启该 init 容器。
		case kubecontainer.ContainerStateExited:
			if podutil.IsRestartableInitContainer(container) {
				changes.InitContainersToStart = append(changes.InitContainersToStart, i)
			} else {
				// 非可重启的 init 容器：失败时按重启策略决定是否终止 Pod。
				if isInitContainerFailed(status) {
					restartOnFailure := restartOnFailure
					if utilfeature.DefaultFeatureGate.Enabled(features.ContainerRestartRules) {
						restartOnFailure = kubecontainer.ShouldContainerBeRestarted(logger, container, pod, podStatus)
					}
					if !restartOnFailure {
						changes.KillPod = true
						changes.InitContainersToStart = nil
						return false
					}
					changes.InitContainersToStart = append(changes.InitContainersToStart, i)
					break
				}

				logger.V(4).Info("Init container has been initialized", "pod", klog.KObj(pod), "container", container.Name)
				if i == (len(pod.Spec.InitContainers) - 1) {
					podHasInitialized = true
				} else {
					// 该 init 容器首次初始化完成，启动下一个
					changes.InitContainersToStart = append(changes.InitContainersToStart, i+1)
				}
			}

		// 未知状态：可重启的 init 容器先杀掉再重启；否则按失败处理。
		default:
			if podutil.IsRestartableInitContainer(container) {
				// 可重启 init 容器处于未知状态时，重启它。
				changes.ContainersToKill[status.ID] = containerToKillInfo{
					name:      container.Name,
					container: container,
					message: fmt.Sprintf("Init container is in %q state, try killing it before restart",
						status.State),
					reason: reasonUnknown,
				}
				changes.InitContainersToStart = append(changes.InitContainersToStart, i)
			} else {
				// 非可重启的 init 容器处于未知状态，按失败路径处理。
				if !isInitContainerFailed(status) {
					logger.V(4).Info("This should not happen, init container is in unknown state but not failed", "pod", klog.KObj(pod), "containerStatus", status)
				}

				restartOnFailure := restartOnFailure
				if utilfeature.DefaultFeatureGate.Enabled(features.ContainerRestartRules) {
					// 仅使用容器级重启策略。不评估容器级重启规则，
					// 因为容器可能尚未退出，没有可用于规则的退出码。
					if container.RestartPolicy != nil {
						restartOnFailure = *container.RestartPolicy != v1.ContainerRestartPolicyNever
					}
				}
				if !restartOnFailure {
					changes.KillPod = true
					changes.InitContainersToStart = nil
					return false
				}

				// init 容器处于未知状态时，重启它。
				changes.ContainersToKill[status.ID] = containerToKillInfo{
					name:      container.Name,
					container: container,
					message: fmt.Sprintf("Init container is in %q state, try killing it before restart",
						status.State),
					reason: reasonUnknown,
				}
				changes.InitContainersToStart = append(changes.InitContainersToStart, i)
			}
		}

		if !isPreviouslyInitialized {
			// 当前 init 容器之前的那个已完成初始化
			isPreviouslyInitialized = true
		}
	}

	// 说明没有 init 容器被启动过，启动第一个
	if !isPreviouslyInitialized {
		changes.InitContainersToStart = append(changes.InitContainersToStart, 0)
	}

	// 反转 InitContainersToStart：上面的循环是逆序遍历的，
	// 但需要按 Pod spec 中的顺序启动。
	l := len(changes.InitContainersToStart)
	for i := 0; i < l/2; i++ {
		changes.InitContainersToStart[i], changes.InitContainersToStart[l-1-i] =
			changes.InitContainersToStart[l-1-i], changes.InitContainersToStart[i]
	}

	if !changes.UpdatePodLevelResources && !changes.UpdatePodResources {
		// 若有正在启动的容器已扩缩容，则需要调整 Pod 资源。
		changes.UpdatePodResources = m.startingResizedContainer(logger, pod, pod.Spec.InitContainers, changes.InitContainersToStart)
	}

	return podHasInitialized
}

// GetContainerLogs 返回指定容器的日志。
func (m *kubeGenericRuntimeManager) GetContainerLogs(ctx context.Context, pod *v1.Pod, containerID kubecontainer.ContainerID, logOptions *v1.PodLogOptions, stdout, stderr io.Writer) (err error) {
	logger := klog.FromContext(ctx)
	resp, err := m.runtimeService.ContainerStatus(ctx, containerID.ID, false)
	if err != nil {
		logger.V(4).Info("Failed to get container status", "containerID", containerID.String(), "err", err)
		return fmt.Errorf("unable to retrieve container logs for %v", containerID.String())
	}
	status := resp.GetStatus()
	if status == nil {
		return remote.ErrContainerStatusNil
	}
	return m.ReadLogs(ctx, status.GetLogPath(), containerID.ID, logOptions, stdout, stderr)
}

// GetExec 获取运行时提供 exec 请求的端点。
func (m *kubeGenericRuntimeManager) GetExec(ctx context.Context, id kubecontainer.ContainerID, cmd []string, stdin, stdout, stderr, tty bool) (*url.URL, error) {
	req := &runtimeapi.ExecRequest{
		ContainerId: id.ID,
		Cmd:         cmd,
		Tty:         tty,
		Stdin:       stdin,
		Stdout:      stdout,
		Stderr:      stderr,
	}
	resp, err := m.runtimeService.Exec(ctx, req)
	if err != nil {
		return nil, err
	}

	return url.Parse(resp.Url)
}

// GetAttach 获取运行时提供 attach 请求的端点。
func (m *kubeGenericRuntimeManager) GetAttach(ctx context.Context, id kubecontainer.ContainerID, stdin, stdout, stderr, tty bool) (*url.URL, error) {
	req := &runtimeapi.AttachRequest{
		ContainerId: id.ID,
		Stdin:       stdin,
		Stdout:      stdout,
		Stderr:      stderr,
		Tty:         tty,
	}
	resp, err := m.runtimeService.Attach(ctx, req)
	if err != nil {
		return nil, err
	}
	return url.Parse(resp.Url)
}

// RunInContainer 在容器中同步执行命令并返回输出。
func (m *kubeGenericRuntimeManager) RunInContainer(ctx context.Context, id kubecontainer.ContainerID, cmd []string, timeout time.Duration) ([]byte, error) {
	stdout, stderr, err := m.runtimeService.ExecSync(ctx, id.ID, cmd, timeout)
	// 注意（tallclair）：这里无法正确交错 stdout 和 stderr，但对日志用途已足够。
	// 若将来需要更精确的输出顺序，需为 ExecSyncRequest 增加合并输出的选项。
	return append(stdout, stderr...), err
}

// removeContainer 移除容器，并可选地移除容器日志。
// 注意先移除日志再移除容器：若日志移除失败，容器不会被移除，kubelet 之后会重试，
// 从而保证在指定移除日志时日志与容器一起被移除。
// 这里假定容器只会在非运行状态下被移除，此时不会再写入日志。
// 若保留日志，日志会一直保留到 Pod 被移除且 kubelet GC 触发。
func (m *kubeGenericRuntimeManager) removeContainer(ctx context.Context, containerID string, keepLogs bool) error {
	logger := klog.FromContext(ctx)
	logger.V(4).Info("Removing container", "containerID", containerID)
	// 调用内部容器 post-stop 生命周期钩子。
	if err := m.internalLifecycle.PostStopContainer(logger, containerID); err != nil {
		return err
	}

	// 移除容器日志。
	// TODO: 分离日志与容器生命周期管理。
	if !keepLogs {
		if err := m.removeContainerLog(ctx, containerID); err != nil {
			return err
		}
	}
	// 移除容器。
	return m.runtimeService.RemoveContainer(ctx, containerID)
}

// removeContainerLog 移除容器日志。
func (m *kubeGenericRuntimeManager) removeContainerLog(ctx context.Context, containerID string) error {
	// 使用日志管理器移除轮转日志。
	err := m.logManager.Clean(ctx, containerID)
	if err != nil {
		return err
	}

	resp, err := m.runtimeService.ContainerStatus(ctx, containerID, false)
	if err != nil {
		return fmt.Errorf("failed to get container status %q: %v", containerID, err)
	}
	status := resp.GetStatus()
	if status == nil {
		return remote.ErrContainerStatusNil
	}
	// 移除旧版容器日志软链接。
	// TODO(random-liu): 集群日志支持 CRI 容器日志路径后移除此逻辑。
	labeledInfo := getContainerInfoFromLabels(ctx, status.Labels)
	legacySymlink := legacyLogSymlink(containerID, labeledInfo.ContainerName, labeledInfo.PodName,
		labeledInfo.PodNamespace)
	if err := m.osInterface.Remove(legacySymlink); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("failed to remove container %q log legacy symbolic link %q: %v",
			containerID, legacySymlink, err)
	}
	return nil
}

// DeleteContainer 移除容器。
func (m *kubeGenericRuntimeManager) DeleteContainer(ctx context.Context, containerID kubecontainer.ContainerID) error {
	return m.removeContainer(ctx, containerID.ID, false)
}

// setTerminationGracePeriod 确定 kill 容器时使用的 grace period。
func setTerminationGracePeriod(ctx context.Context, pod *v1.Pod, containerSpec *v1.Container, containerName string, containerID kubecontainer.ContainerID, reason containerKillReason) int64 {
	gracePeriod := int64(minimumGracePeriodInSeconds)
	switch {
	case pod.DeletionGracePeriodSeconds != nil:
		return *pod.DeletionGracePeriodSeconds
	case pod.Spec.TerminationGracePeriodSeconds != nil:
		switch reason {
		case reasonStartupProbe:
			if isProbeTerminationGracePeriodSecondsSet(ctx, pod, containerSpec, containerSpec.StartupProbe, containerName, containerID, "StartupProbe") {
				return *containerSpec.StartupProbe.TerminationGracePeriodSeconds
			}
		case reasonLivenessProbe:
			if isProbeTerminationGracePeriodSecondsSet(ctx, pod, containerSpec, containerSpec.LivenessProbe, containerName, containerID, "LivenessProbe") {
				return *containerSpec.LivenessProbe.TerminationGracePeriodSeconds
			}
		}
		return *pod.Spec.TerminationGracePeriodSeconds
	}
	return gracePeriod
}

// isProbeTerminationGracePeriodSecondsSet 判断探针级 grace period 是否已设置且有效。
func isProbeTerminationGracePeriodSecondsSet(ctx context.Context, pod *v1.Pod, containerSpec *v1.Container, probe *v1.Probe, containerName string, containerID kubecontainer.ContainerID, probeType string) bool {
	logger := klog.FromContext(ctx)
	if probe != nil && probe.TerminationGracePeriodSeconds != nil {
		// 探针级 grace period 大于 Pod 级时记录日志，便于排查终止时间超预期的问题。
		if *probe.TerminationGracePeriodSeconds > *pod.Spec.TerminationGracePeriodSeconds {
			logger.V(4).Info("Using probe-level grace period that is greater than the pod-level grace period", "pod", klog.KObj(pod), "podUID", pod.UID, "containerName", containerName, "containerID", containerID.String(), "probeType", probeType, "probeGracePeriod", *probe.TerminationGracePeriodSeconds, "podGracePeriod", *pod.Spec.TerminationGracePeriodSeconds)
		}
		return true
	}
	return false
}
