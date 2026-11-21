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
	"hash/fnv"
	"path/filepath"
	"strconv"
	"strings"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	runtimeapi "k8s.io/cri-api/pkg/apis/runtime/v1"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/features"
	"k8s.io/kubernetes/pkg/kubelet/cm"
	kubecontainer "k8s.io/kubernetes/pkg/kubelet/container"
	"k8s.io/kubernetes/pkg/security/apparmor"
)

// containerByCreatedThenID 按创建时间倒序、创建时间相同时按 ID 升序排列容器，
// 实现 sort.Interface。
type containerByCreatedThenID []*runtimeapi.Container

// Len 返回切片长度，实现 sort.Interface。
func (b containerByCreatedThenID) Len() int { return len(b) }

// Swap 交换索引 i 和 j 处的元素，实现 sort.Interface。
func (b containerByCreatedThenID) Swap(i, j int) { b[i], b[j] = b[j], b[i] }

// Less 比较两个容器：创建时间较新的排在前面，创建时间相同时 ID 较小的排在前面。
func (b containerByCreatedThenID) Less(i, j int) bool {
	if b[i].CreatedAt != b[j].CreatedAt {
		return b[i].CreatedAt > (b[j].CreatedAt)
	}
	return b[i].Id < b[j].Id
}

// podSandboxByCreatedThenID 按 attempt 号与创建时间倒序排列 Pod sandbox，最新的排在前面，
// 实现 sort.Interface。
type podSandboxByCreatedThenID []*runtimeapi.PodSandbox

// Len 返回切片长度，实现 sort.Interface。
func (p podSandboxByCreatedThenID) Len() int { return len(p) }

// Swap 交换索引 i 和 j 处的元素，实现 sort.Interface。
func (p podSandboxByCreatedThenID) Swap(i, j int) { p[i], p[j] = p[j], p[i] }

// Less 优先比较 attempt 号，其次比较创建时间（倒序），最后按 ID 升序打破平局。
func (p podSandboxByCreatedThenID) Less(i, j int) bool {
	if p[i].Metadata == nil || p[j].Metadata == nil || (p[i].Metadata.Attempt == p[j].Metadata.Attempt) {
		if p[i].CreatedAt != p[j].CreatedAt {
			return p[i].CreatedAt > p[j].CreatedAt
		}
		return p[i].Id < p[j].Id
	}
	return p[i].Metadata.Attempt > p[j].Metadata.Attempt
}

// containerStatusByCreated 按创建时间倒序排列容器状态，实现 sort.Interface。
type containerStatusByCreated []*kubecontainer.Status

// Len 返回切片长度，实现 sort.Interface。
func (c containerStatusByCreated) Len() int { return len(c) }

// Swap 交换索引 i 和 j 处的元素，实现 sort.Interface。
func (c containerStatusByCreated) Swap(i, j int) { c[i], c[j] = c[j], c[i] }

// Less 按创建时间倒序比较，较新的状态排在前面。
func (c containerStatusByCreated) Less(i, j int) bool { return c[i].CreatedAt.After(c[j].CreatedAt) }

// toKubeContainerState 把 runtimeapi.ContainerState 转换为 kubecontainer.State。
func toKubeContainerState(state runtimeapi.ContainerState) kubecontainer.State {
	switch state {
	case runtimeapi.ContainerState_CONTAINER_CREATED:
		return kubecontainer.ContainerStateCreated
	case runtimeapi.ContainerState_CONTAINER_RUNNING:
		return kubecontainer.ContainerStateRunning
	case runtimeapi.ContainerState_CONTAINER_EXITED:
		return kubecontainer.ContainerStateExited
	case runtimeapi.ContainerState_CONTAINER_UNKNOWN:
		return kubecontainer.ContainerStateUnknown
	}

	return kubecontainer.ContainerStateUnknown
}

// toRuntimeProtocol 把 v1.Protocol 转换为 runtimeapi.Protocol，未知协议默认按 TCP 处理。
func toRuntimeProtocol(logger klog.Logger, protocol v1.Protocol) runtimeapi.Protocol {
	switch protocol {
	case v1.ProtocolTCP:
		return runtimeapi.Protocol_TCP
	case v1.ProtocolUDP:
		return runtimeapi.Protocol_UDP
	case v1.ProtocolSCTP:
		return runtimeapi.Protocol_SCTP
	}

	logger.Info("Unknown protocol, defaulting to TCP", "protocol", protocol)
	return runtimeapi.Protocol_TCP
}

// toKubeContainer 把 runtimeapi.Container 转换为 kubecontainer.Container。
func (m *kubeGenericRuntimeManager) toKubeContainer(ctx context.Context, c *runtimeapi.Container) (*kubecontainer.Container, error) {
	if c == nil || c.Id == "" || c.Image == nil {
		return nil, fmt.Errorf("unable to convert a nil pointer to a runtime container")
	}

	// 兼容旧版运行时：c.ImageId 是 v1.30 才加入的字段，缺失时回退到 ImageRef。
	imageID := c.ImageRef
	if c.ImageId != "" {
		imageID = c.ImageId
	}

	annotatedInfo := getContainerInfoFromAnnotations(ctx, c.Annotations)
	return &kubecontainer.Container{
		ID:                  kubecontainer.ContainerID{Type: m.runtimeName, ID: c.Id},
		Name:                c.GetMetadata().GetName(),
		ImageID:             imageID,
		ImageRef:            c.ImageRef,
		ImageRuntimeHandler: c.Image.RuntimeHandler,
		Image:               c.Image.Image,
		Hash:                annotatedInfo.Hash,
		State:               toKubeContainerState(c.State),
		PodSandboxID:        c.PodSandboxId,
		CreatedAt:           c.CreatedAt,
	}, nil
}

// sandboxToKubeContainer 把 runtimeapi.PodSandbox 转换为 kubecontainer.Container。
// 这样做只是为了让 PLEG 把 sandbox 当作 kubecontainer.Container 处理，避免对 PLEG 做大改。
// TODO: Remove this once it becomes obsolete.
func (m *kubeGenericRuntimeManager) sandboxToKubeContainer(s *runtimeapi.PodSandbox) (*kubecontainer.Container, error) {
	if s == nil || s.Id == "" {
		return nil, fmt.Errorf("unable to convert a nil pointer to a runtime container")
	}

	return &kubecontainer.Container{
		ID:           kubecontainer.ContainerID{Type: m.runtimeName, ID: s.Id},
		State:        kubecontainer.SandboxToContainerState(s.State),
		PodSandboxID: s.Id,
		CreatedAt:    s.CreatedAt,
	}, nil
}

// getImageUser 获取运行镜像命令所使用的 uid 或用户名，并保证两者最多只设置一个。
func (m *kubeGenericRuntimeManager) getImageUser(ctx context.Context, image string) (*int64, string, error) {
	resp, err := m.imageService.ImageStatus(ctx, &runtimeapi.ImageSpec{Image: image}, false)
	if err != nil {
		return nil, "", err
	}
	imageStatus := resp.GetImage()

	if imageStatus != nil {
		if imageStatus.Uid != nil {
			return &imageStatus.GetUid().Value, "", nil
		}

		if imageStatus.Username != "" {
			return nil, imageStatus.Username, nil
		}
	}

	// 两者都未设置时，按 root 处理。
	return new(int64), "", nil
}

// isInitContainerFailed 在满足以下任一条件时返回 true：
// 1. 容器已退出且退出码非零。
// 2. 容器处于未知状态。
// 3. 容器被 OOMKilled。
func isInitContainerFailed(status *kubecontainer.Status) bool {
	// 发生 OOMKilled 时，init 容器应被视为失败。
	if status.Reason == "OOMKilled" {
		return true
	}

	if status.State == kubecontainer.ContainerStateExited && status.ExitCode != 0 {
		return true
	}

	if status.State == kubecontainer.ContainerStateUnknown {
		return true
	}

	return false
}

// GetBackoffKey 生成用于唯一标识 (pod, container) 的退避键，以便跟踪容器退避。
// 该键应包含与退避相关的容器内容，使得任何变化都会生成新的键。
func GetBackoffKey(pod *v1.Pod, container *v1.Container) string {
	// 包含稳定标识（name、namespace、uid）以及变化时应重置退避的字段。
	key := []string{
		pod.Name,
		pod.Namespace,
		string(pod.UID),
		container.Name,
		container.Image,
		container.Resources.String(),
	}
	hash := fnv.New64a()
	hash.Write([]byte(strings.Join(key, "/")))
	return strconv.FormatUint(hash.Sum64(), 16)
}

// logPathDelimiter 是日志路径中使用的分隔符。
const logPathDelimiter = "_"

// buildContainerLogsPath 构建容器日志相对于 Pod 日志目录的路径。
func buildContainerLogsPath(containerName string, restartCount int) string {
	return filepath.Join(containerName, fmt.Sprintf("%d.log", restartCount))
}

// BuildContainerLogsDirectory 构建 Pod 内某容器日志目录的绝对路径。
func BuildContainerLogsDirectory(podLogsDir, podNamespace, podName string, podUID types.UID, containerName string) string {
	return filepath.Join(BuildPodLogsDirectory(podLogsDir, podNamespace, podName, podUID), containerName)
}

// BuildPodLogsDirectory 构建 Pod sandbox 日志目录的绝对路径。
func BuildPodLogsDirectory(podLogsDir, podNamespace, podName string, podUID types.UID) string {
	return filepath.Join(podLogsDir, strings.Join([]string{podNamespace, podName,
		string(podUID)}, logPathDelimiter))
}

// parsePodUIDFromLogsDirectory 解析 Pod 日志目录名并返回 Pod UID。
// 同时支持旧的日志目录 /var/log/pods/UID 和新的日志目录 /var/log/pods/NAMESPACE_NAME_UID。
func parsePodUIDFromLogsDirectory(name string) types.UID {
	parts := strings.Split(name, logPathDelimiter)
	return types.UID(parts[len(parts)-1])
}

// toKubeRuntimeStatus 把 runtimeapi.RuntimeStatus 转换为 kubecontainer.RuntimeStatus。
func toKubeRuntimeStatus(status *runtimeapi.RuntimeStatus, handlers []*runtimeapi.RuntimeHandler, features *runtimeapi.RuntimeFeatures) *kubecontainer.RuntimeStatus {
	conditions := []kubecontainer.RuntimeCondition{}
	for _, c := range status.GetConditions() {
		conditions = append(conditions, kubecontainer.RuntimeCondition{
			Type:    kubecontainer.RuntimeConditionType(c.Type),
			Status:  c.Status,
			Reason:  c.Reason,
			Message: c.Message,
		})
	}
	retHandlers := make([]kubecontainer.RuntimeHandler, len(handlers))
	for i, h := range handlers {
		supportsRRO := false
		supportsUserns := false
		if h.Features != nil {
			supportsRRO = h.Features.RecursiveReadOnlyMounts
			supportsUserns = h.Features.UserNamespaces
		}
		retHandlers[i] = kubecontainer.RuntimeHandler{
			Name:                            h.Name,
			SupportsRecursiveReadOnlyMounts: supportsRRO,
			SupportsUserNamespaces:          supportsUserns,
		}
	}
	var retFeatures *kubecontainer.RuntimeFeatures
	if features != nil {
		retFeatures = &kubecontainer.RuntimeFeatures{
			SupplementalGroupsPolicy:  features.SupplementalGroupsPolicy,
			UserNamespacesHostNetwork: features.UserNamespacesHostNetwork,
			MountOptions:              features.MountOptions,
		}
	}
	return &kubecontainer.RuntimeStatus{Conditions: conditions, Handlers: retHandlers, Features: retFeatures}
}

// fieldSeccompProfile 把 v1.SeccompProfile 转换为 runtimeapi.SecurityProfile。
// 未设置 profile 时，按 fallbackToRuntimeDefault 决定回退到 RuntimeDefault 还是 Unconfined。
func fieldSeccompProfile(scmp *v1.SeccompProfile, profileRootPath string, fallbackToRuntimeDefault bool) (*runtimeapi.SecurityProfile, error) {
	if scmp == nil {
		if fallbackToRuntimeDefault {
			return &runtimeapi.SecurityProfile{
				ProfileType: runtimeapi.SecurityProfile_RuntimeDefault,
			}, nil
		}
		return &runtimeapi.SecurityProfile{
			ProfileType: runtimeapi.SecurityProfile_Unconfined,
		}, nil
	}
	if scmp.Type == v1.SeccompProfileTypeRuntimeDefault {
		return &runtimeapi.SecurityProfile{
			ProfileType: runtimeapi.SecurityProfile_RuntimeDefault,
		}, nil
	}
	if scmp.Type == v1.SeccompProfileTypeLocalhost {
		if scmp.LocalhostProfile != nil && len(*scmp.LocalhostProfile) > 0 {
			fname := filepath.Join(profileRootPath, *scmp.LocalhostProfile)
			return &runtimeapi.SecurityProfile{
				ProfileType:  runtimeapi.SecurityProfile_Localhost,
				LocalhostRef: fname,
			}, nil
		} else {
			return nil, fmt.Errorf("localhostProfile must be set if seccompProfile type is Localhost.")
		}
	}
	return &runtimeapi.SecurityProfile{
		ProfileType: runtimeapi.SecurityProfile_Unconfined,
	}, nil
}

// getSeccompProfile 按容器级、Pod 级的优先级解析 seccomp 配置，均未设置时使用回退策略。
func (m *kubeGenericRuntimeManager) getSeccompProfile(annotations map[string]string, containerName string,
	podSecContext *v1.PodSecurityContext, containerSecContext *v1.SecurityContext, fallbackToRuntimeDefault bool) (*runtimeapi.SecurityProfile, error) {
	// 容器级字段优先应用
	if containerSecContext != nil && containerSecContext.SeccompProfile != nil {
		return fieldSeccompProfile(containerSecContext.SeccompProfile, m.seccompProfileRoot, fallbackToRuntimeDefault)
	}

	// 容器未定义 seccomp 时，尝试应用 Pod 级字段
	if podSecContext != nil && podSecContext.SeccompProfile != nil {
		return fieldSeccompProfile(podSecContext.SeccompProfile, m.seccompProfileRoot, fallbackToRuntimeDefault)
	}

	if fallbackToRuntimeDefault {
		return &runtimeapi.SecurityProfile{
			ProfileType: runtimeapi.SecurityProfile_RuntimeDefault,
		}, nil
	}

	return &runtimeapi.SecurityProfile{
		ProfileType: runtimeapi.SecurityProfile_Unconfined,
	}, nil
}

// getAppArmorProfile 把 Pod/容器的 AppArmor 配置转换为 runtimeapi.SecurityProfile，
// 同时返回兼容旧版运行时的废弃 profile 字符串。
func getAppArmorProfile(pod *v1.Pod, container *v1.Container) (*runtimeapi.SecurityProfile, string, error) {
	profile := apparmor.GetProfile(pod, container)
	if profile == nil {
		return nil, "", nil
	}

	var (
		securityProfile   *runtimeapi.SecurityProfile
		deprecatedProfile string // 已废弃的 apparmor profile 格式，仍为兼容旧版运行时而提供。
	)

	switch profile.Type {
	case v1.AppArmorProfileTypeRuntimeDefault:
		securityProfile = &runtimeapi.SecurityProfile{
			ProfileType: runtimeapi.SecurityProfile_RuntimeDefault,
		}
		deprecatedProfile = v1.DeprecatedAppArmorBetaProfileRuntimeDefault

	case v1.AppArmorProfileTypeUnconfined:
		securityProfile = &runtimeapi.SecurityProfile{
			ProfileType: runtimeapi.SecurityProfile_Unconfined,
		}
		deprecatedProfile = v1.DeprecatedAppArmorBetaProfileNameUnconfined

	case v1.AppArmorProfileTypeLocalhost:
		if profile.LocalhostProfile == nil {
			return nil, "", errors.New("missing localhost apparmor profile name")
		}
		securityProfile = &runtimeapi.SecurityProfile{
			ProfileType:  runtimeapi.SecurityProfile_Localhost,
			LocalhostRef: *profile.LocalhostProfile,
		}
		deprecatedProfile = v1.DeprecatedAppArmorBetaProfileNamePrefix + *profile.LocalhostProfile

	default:
		// 不应发生。
		return nil, "", fmt.Errorf("unknown apparmor profile type: %q", profile.Type)
	}

	return securityProfile, deprecatedProfile, nil
}

// mergeResourceConfig 把 update 中非零的字段合并到 source 上，返回新的 ResourceConfig。
// source 或 update 为 nil 时直接返回另一方。
func mergeResourceConfig(source, update *cm.ResourceConfig) *cm.ResourceConfig {
	if source == nil {
		return update
	}
	if update == nil {
		return source
	}

	merged := *source

	if update.Memory != nil {
		merged.Memory = update.Memory
	}
	if update.CPUSet.Size() > 0 {
		merged.CPUSet = update.CPUSet
	}
	if update.CPUShares != nil {
		merged.CPUShares = update.CPUShares
	}
	if update.CPUQuota != nil {
		merged.CPUQuota = update.CPUQuota
	}
	if update.CPUPeriod != nil {
		merged.CPUPeriod = update.CPUPeriod
	}
	if update.PidsLimit != nil {
		merged.PidsLimit = update.PidsLimit
	}

	if update.HugePageLimit != nil {
		if merged.HugePageLimit == nil {
			merged.HugePageLimit = make(map[int64]int64)
		}
		for k, v := range update.HugePageLimit {
			merged.HugePageLimit[k] = v
		}
	}

	if update.Unified != nil {
		if merged.Unified == nil {
			merged.Unified = make(map[string]string)
		}
		for k, v := range update.Unified {
			merged.Unified[k] = v
		}
	}

	return &merged
}

// convertResourceConfigToLinuxContainerResources 把 cm.ResourceConfig 转换为
// runtimeapi.LinuxContainerResources。
func convertResourceConfigToLinuxContainerResources(rc *cm.ResourceConfig) *runtimeapi.LinuxContainerResources {
	if rc == nil {
		return nil
	}

	lcr := &runtimeapi.LinuxContainerResources{}

	if rc.CPUPeriod != nil {
		lcr.CpuPeriod = int64(*rc.CPUPeriod)
	}
	if rc.CPUQuota != nil {
		lcr.CpuQuota = *rc.CPUQuota
	}
	if rc.CPUShares != nil {
		lcr.CpuShares = int64(*rc.CPUShares)
	}
	if rc.Memory != nil {
		lcr.MemoryLimitInBytes = *rc.Memory
	}
	if rc.CPUSet.Size() > 0 {
		lcr.CpusetCpus = rc.CPUSet.String()
	}

	if rc.Unified != nil {
		lcr.Unified = make(map[string]string, len(rc.Unified))
		for k, v := range rc.Unified {
			lcr.Unified[k] = v
		}
	}

	return lcr
}

// signalNameToRuntimeEnum 建立信号名称到 CRI 信号枚举的映射。
var signalNameToRuntimeEnum = map[string]runtimeapi.Signal{
	"SIGABRT":     runtimeapi.Signal_SIGNAL_SIGABRT,
	"SIGALRM":     runtimeapi.Signal_SIGNAL_SIGALRM,
	"SIGBUS":      runtimeapi.Signal_SIGNAL_SIGBUS,
	"SIGCHLD":     runtimeapi.Signal_SIGNAL_SIGCHLD,
	"SIGCLD":      runtimeapi.Signal_SIGNAL_SIGCLD,
	"SIGCONT":     runtimeapi.Signal_SIGNAL_SIGCONT,
	"SIGFPE":      runtimeapi.Signal_SIGNAL_SIGFPE,
	"SIGHUP":      runtimeapi.Signal_SIGNAL_SIGHUP,
	"SIGILL":      runtimeapi.Signal_SIGNAL_SIGILL,
	"SIGINT":      runtimeapi.Signal_SIGNAL_SIGINT,
	"SIGIO":       runtimeapi.Signal_SIGNAL_SIGIO,
	"SIGIOT":      runtimeapi.Signal_SIGNAL_SIGIOT,
	"SIGKILL":     runtimeapi.Signal_SIGNAL_SIGKILL,
	"SIGPIPE":     runtimeapi.Signal_SIGNAL_SIGPIPE,
	"SIGPOLL":     runtimeapi.Signal_SIGNAL_SIGPOLL,
	"SIGPROF":     runtimeapi.Signal_SIGNAL_SIGPROF,
	"SIGPWR":      runtimeapi.Signal_SIGNAL_SIGPWR,
	"SIGQUIT":     runtimeapi.Signal_SIGNAL_SIGQUIT,
	"SIGSEGV":     runtimeapi.Signal_SIGNAL_SIGSEGV,
	"SIGSTKFLT":   runtimeapi.Signal_SIGNAL_SIGSTKFLT,
	"SIGSTOP":     runtimeapi.Signal_SIGNAL_SIGSTOP,
	"SIGSYS":      runtimeapi.Signal_SIGNAL_SIGSYS,
	"SIGTERM":     runtimeapi.Signal_SIGNAL_SIGTERM,
	"SIGTRAP":     runtimeapi.Signal_SIGNAL_SIGTRAP,
	"SIGTSTP":     runtimeapi.Signal_SIGNAL_SIGTSTP,
	"SIGTTIN":     runtimeapi.Signal_SIGNAL_SIGTTIN,
	"SIGTTOU":     runtimeapi.Signal_SIGNAL_SIGTTOU,
	"SIGURG":      runtimeapi.Signal_SIGNAL_SIGURG,
	"SIGUSR1":     runtimeapi.Signal_SIGNAL_SIGUSR1,
	"SIGUSR2":     runtimeapi.Signal_SIGNAL_SIGUSR2,
	"SIGVTALRM":   runtimeapi.Signal_SIGNAL_SIGVTALRM,
	"SIGWINCH":    runtimeapi.Signal_SIGNAL_SIGWINCH,
	"SIGXCPU":     runtimeapi.Signal_SIGNAL_SIGXCPU,
	"SIGXFSZ":     runtimeapi.Signal_SIGNAL_SIGXFSZ,
	"SIGRTMIN":    runtimeapi.Signal_SIGNAL_SIGRTMIN,
	"SIGRTMIN+1":  runtimeapi.Signal_SIGNAL_SIGRTMINPLUS1,
	"SIGRTMIN+2":  runtimeapi.Signal_SIGNAL_SIGRTMINPLUS2,
	"SIGRTMIN+3":  runtimeapi.Signal_SIGNAL_SIGRTMINPLUS3,
	"SIGRTMIN+4":  runtimeapi.Signal_SIGNAL_SIGRTMINPLUS4,
	"SIGRTMIN+5":  runtimeapi.Signal_SIGNAL_SIGRTMINPLUS5,
	"SIGRTMIN+6":  runtimeapi.Signal_SIGNAL_SIGRTMINPLUS6,
	"SIGRTMIN+7":  runtimeapi.Signal_SIGNAL_SIGRTMINPLUS7,
	"SIGRTMIN+8":  runtimeapi.Signal_SIGNAL_SIGRTMINPLUS8,
	"SIGRTMIN+9":  runtimeapi.Signal_SIGNAL_SIGRTMINPLUS9,
	"SIGRTMIN+10": runtimeapi.Signal_SIGNAL_SIGRTMINPLUS10,
	"SIGRTMIN+11": runtimeapi.Signal_SIGNAL_SIGRTMINPLUS11,
	"SIGRTMIN+12": runtimeapi.Signal_SIGNAL_SIGRTMINPLUS12,
	"SIGRTMIN+13": runtimeapi.Signal_SIGNAL_SIGRTMINPLUS13,
	"SIGRTMIN+14": runtimeapi.Signal_SIGNAL_SIGRTMINPLUS14,
	"SIGRTMIN+15": runtimeapi.Signal_SIGNAL_SIGRTMINPLUS15,
	"SIGRTMAX-14": runtimeapi.Signal_SIGNAL_SIGRTMAXMINUS14,
	"SIGRTMAX-13": runtimeapi.Signal_SIGNAL_SIGRTMAXMINUS13,
	"SIGRTMAX-12": runtimeapi.Signal_SIGNAL_SIGRTMAXMINUS12,
	"SIGRTMAX-11": runtimeapi.Signal_SIGNAL_SIGRTMAXMINUS11,
	"SIGRTMAX-10": runtimeapi.Signal_SIGNAL_SIGRTMAXMINUS10,
	"SIGRTMAX-9":  runtimeapi.Signal_SIGNAL_SIGRTMAXMINUS9,
	"SIGRTMAX-8":  runtimeapi.Signal_SIGNAL_SIGRTMAXMINUS8,
	"SIGRTMAX-7":  runtimeapi.Signal_SIGNAL_SIGRTMAXMINUS7,
	"SIGRTMAX-6":  runtimeapi.Signal_SIGNAL_SIGRTMAXMINUS6,
	"SIGRTMAX-5":  runtimeapi.Signal_SIGNAL_SIGRTMAXMINUS5,
	"SIGRTMAX-4":  runtimeapi.Signal_SIGNAL_SIGRTMAXMINUS4,
	"SIGRTMAX-3":  runtimeapi.Signal_SIGNAL_SIGRTMAXMINUS3,
	"SIGRTMAX-2":  runtimeapi.Signal_SIGNAL_SIGRTMAXMINUS2,
	"SIGRTMAX-1":  runtimeapi.Signal_SIGNAL_SIGRTMAXMINUS1,
	"SIGRTMAX":    runtimeapi.Signal_SIGNAL_SIGRTMAX,
}

// getContainerConfigStopSignal 返回容器配置中指定的停止信号；特性未开启或未设置时返回 nil。
func getContainerConfigStopSignal(container *v1.Container) (stopsignal *runtimeapi.Signal) {
	if utilfeature.DefaultFeatureGate.Enabled(features.ContainerStopSignals) {
		if container.Lifecycle != nil && container.Lifecycle.StopSignal != nil {
			var signalValue runtimeapi.Signal
			signalStr := string(*container.Lifecycle.StopSignal)
			signalValue = signalNameToRuntimeEnum[signalStr]
			return &signalValue
		} else {
			return nil
		}
	}

	return nil
}

// runtimeSignalToString 把 CRI 信号枚举转换为 v1.Signal 字符串；未知信号返回空字符串。
func runtimeSignalToString(signal runtimeapi.Signal) *v1.Signal {
	var convertedSignal v1.Signal
	for key, value := range signalNameToRuntimeEnum {
		if value == signal {
			convertedSignal = v1.Signal(key)
		}
	}

	return &convertedSignal
}
