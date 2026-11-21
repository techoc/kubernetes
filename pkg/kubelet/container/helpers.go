/*
Copyright 2015 The Kubernetes Authors.

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

package container

import (
	"context"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"strings"

	"k8s.io/klog/v2"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/client-go/tools/record"
	runtimeapi "k8s.io/cri-api/pkg/apis/runtime/v1"
	statsapi "k8s.io/kubelet/pkg/apis/stats/v1alpha1"
	podutil "k8s.io/kubernetes/pkg/api/v1/pod"
	"k8s.io/kubernetes/pkg/features"
	sc "k8s.io/kubernetes/pkg/securitycontext"
	hashutil "k8s.io/kubernetes/pkg/util/hash"
	"k8s.io/kubernetes/third_party/forked/golang/expansion"
	utilsnet "k8s.io/utils/net"
)

// HandlerRunner 为容器执行生命周期钩子。
type HandlerRunner interface {
	Run(ctx context.Context, containerID ContainerID, pod *v1.Pod, container *v1.Container, handler *v1.LifecycleHandler) (string, error)
}

// RuntimeHelper 包装 kubelet 的能力，使容器运行时可以获取 RunContainerOptions、
// DNS 设置、主机 IP 等必要信息。
type RuntimeHelper interface {
	GenerateRunContainerOptions(ctx context.Context, pod *v1.Pod, container *v1.Container, podIP string, podIPs []string, imageVolumes ImageVolumes) (contOpts *RunContainerOptions, cleanupAction func(), err error)
	GetPodDNS(ctx context.Context, pod *v1.Pod) (dnsConfig *runtimeapi.DNSConfig, err error)
	// GetPodCgroupParent 返回 pod 的 CgroupName 标识及其在宿主上的字面 cgroupfs 形式。
	GetPodCgroupParent(pod *v1.Pod) string
	GetPodDir(podUID types.UID) string
	GeneratePodHostNameAndDomain(logger klog.Logger, pod *v1.Pod) (hostname string, hostDomain string, err error)
	// GetExtraSupplementalGroupsForPod 返回 Pod 的额外补充组。
	// 这些组来自 Pod 依赖的持久卷上的注解。
	GetExtraSupplementalGroupsForPod(pod *v1.Pod) []int64

	// GetOrCreateUserNamespaceMappings 返回 sandbox 用户命名空间的配置。
	GetOrCreateUserNamespaceMappings(logger klog.Logger, pod *v1.Pod, runtimeHandler string) (*runtimeapi.UserNamespace, error)

	// PrepareDynamicResources 为 pod 准备动态资源。
	PrepareDynamicResources(ctx context.Context, pod *v1.Pod) error

	// UnprepareDynamicResources 取消 pod 的动态资源准备。
	UnprepareDynamicResources(ctx context.Context, pod *v1.Pod) error

	// RequestPodReinspect 标记某个 pod 需要在下次 relist 时重新检查。
	RequestPodReinspect(types.UID)

	// PodCPUAndMemoryStats 读取最新的 CPU 与内存用量统计。
	PodCPUAndMemoryStats(context.Context, *v1.Pod, *PodStatus) (*statsapi.PodStats, error)

	// OnPodSandboxReady 在 pod sandbox、网络与卷就绪后回调，
	// 用于更新 PodReadyToStartContainers 状况。
	OnPodSandboxReady(ctx context.Context, pod *v1.Pod) error

	// ResizeEphemeralVolume 直接触发指定卷的扩容。
	ResizeEphemeralVolume(pod *v1.Pod, volumeName string, newSize *resource.Quantity) error
}

// ShouldContainerBeRestarted 判断容器是否需要重启。
// TODO(yifan): Think about how to refactor this.
func ShouldContainerBeRestarted(logger klog.Logger, container *v1.Container, pod *v1.Pod, podStatus *PodStatus) bool {
	// pod 一旦被标记删除，就不应再重启，避免删除过程中被重新拉起。
	if pod.DeletionTimestamp != nil {
		return false
	}
	// 取容器当前状态作为判断依据。
	status := podStatus.FindContainerStatusByName(container.Name)
	// 容器从未启动过时需要启动。
	// NOTE(random-liu): 如果所有历史容器都被 GC 掉，这里同样会返回 true。
	if status == nil {
		return true
	}
	// 运行中的容器无需重启。
	if status.State == ContainerStateRunning {
		return false
	}
	// 状态未知或仅处于 created 的容器一律重启，确保最终收敛到期望状态。
	if status.State == ContainerStateUnknown || status.State == ContainerStateCreated {
		return true
	}
	// 容器已退出时按重启策略判断；启用 ContainerRestartRules 特性时以容器重启规则为准。
	if utilfeature.DefaultFeatureGate.Enabled(features.ContainerRestartRules) {
		return podutil.ContainerShouldRestart(*container, pod.Spec, int32(status.ExitCode))
	}
	if pod.Spec.RestartPolicy == v1.RestartPolicyNever {
		logger.V(4).Info("Already ran container, do nothing", "pod", klog.KObj(pod), "containerName", container.Name)
		return false
	}
	if pod.Spec.RestartPolicy == v1.RestartPolicyOnFailure {
		// 检查退出码。
		if status.ExitCode == 0 {
			logger.V(4).Info("Already successfully ran container, do nothing", "pod", klog.KObj(pod), "containerName", container.Name)
			return false
		}
	}
	return true
}

// ShouldAllContainersRestart 判断 pod 是否需要重启。
// 先检查 apiPodStatus 中是否带 AllContainersRestarting 状况，再检查 podStatus 或
// apiPodStatus 中是否有容器按匹配规则退出。
func ShouldAllContainersRestart(pod *v1.Pod, podStatus *PodStatus, apiPodStatus *v1.PodStatus) bool {
	// 优先采纳 API 侧整体重启结论，它已由 API 直接给出，无需重复推断。
	if apiPodStatus != nil {
		for _, cond := range apiPodStatus.Conditions {
			if cond.Type == v1.AllContainersRestarting && cond.Status == v1.ConditionTrue {
				return true
			}
		}
	}

	// 把 API 容器状态按名称建索引，便于按容器名快速查找退出状态。
	nameToAPIStatus := make(map[string]*v1.ContainerStatus)
	if apiPodStatus != nil {
		for i := range apiPodStatus.InitContainerStatuses {
			nameToAPIStatus[apiPodStatus.InitContainerStatuses[i].Name] = &apiPodStatus.InitContainerStatuses[i]
		}
		for i := range apiPodStatus.ContainerStatuses {
			nameToAPIStatus[apiPodStatus.ContainerStatuses[i].Name] = &apiPodStatus.ContainerStatuses[i]
		}
	}

	// 遍历 pod 内所有容器，检查是否有退出的容器命中“重启全部容器”规则。
	for c := range podutil.ContainerIter(&pod.Spec, podutil.InitContainers|podutil.Containers) {
		if c == nil {
			continue
		}
		if podStatus != nil {
			status := podStatus.FindContainerStatusByName(c.Name)
			if status == nil || status.State != ContainerStateExited {
				continue
			}
			exitCode := int32(status.ExitCode)
			rule, ok := podutil.FindMatchingContainerRestartRule(*c, exitCode)
			if ok && rule.Action == v1.ContainerRestartRuleActionRestartAllContainers {
				return true
			}
		}
		if apiPodStatus != nil {
			apiStatus, ok := nameToAPIStatus[c.Name]
			if !ok || apiStatus.State.Terminated == nil {
				continue
			}
			exitCode := apiStatus.State.Terminated.ExitCode
			rule, ok := podutil.FindMatchingContainerRestartRule(*c, exitCode)
			if ok && rule.Action == v1.ContainerRestartRuleActionRestartAllContainers {
				return true
			}
		}
	}

	return false
}

// AllContainersRestartCleanedUp 判断所有容器是否都已从运行时和 podStatus 中移除。
func AllContainersRestartCleanedUp(pod *v1.Pod, podStatus *PodStatus) bool {
	for c := range podutil.ContainerIter(&pod.Spec, podutil.Containers|podutil.InitContainers) {
		if podStatus.FindContainerStatusByName(c.Name) != nil {
			return false
		}
	}
	return true
}

// HashContainer 返回容器的哈希，用于比较运行中的容器与其期望 spec 是否一致。
// 注意：修改哈希字段时，也要同步更新 container_hash_test.go 中的 hashValues。
func HashContainer(container *v1.Container) uint64 {
	hash := fnv.New32a()
	// 只哈希影响运行状态的字段，使哈希能可靠反映容器是否需要重建。
	containerJSON, _ := json.Marshal(pickFieldsToHash(container))
	hashutil.DeepHashObject(hash, containerJSON)
	return uint64(hash.Sum32())
}

// pickFieldsToHash 挑选会影响容器运行状态的字段用于哈希，
// 目前只包含 `image` 与 `name`。若 kubelet 将来允许变更其他字段，必须同步扩充此列表。
func pickFieldsToHash(container *v1.Container) map[string]string {
	retval := map[string]string{
		"name":  container.Name,
		"image": container.Image,
	}
	return retval
}

// envVarsToMap 把 EnvVar 切片转换为“环境变量名 -> 值”的映射。
func envVarsToMap(envs []EnvVar) map[string]string {
	result := map[string]string{}
	for _, env := range envs {
		result[env.Name] = env.Value
	}
	return result
}

// v1EnvVarsToMap 把 v1.EnvVar 切片转换为“环境变量名 -> 值”的映射。
func v1EnvVarsToMap(envs []v1.EnvVar) map[string]string {
	result := map[string]string{}
	for _, env := range envs {
		result[env.Name] = env.Value
	}

	return result
}

// ExpandContainerCommandOnlyStatic 仅用容器环境定义中的静态变量值做替换，
// 不处理 valueFrom 引用。未绑定的变量既不会展开也不会被替换为空串，
// 即 "echo $(MISSING) => echo $(MISSING)"。
// TODO: callers should use ExpandContainerCommandAndArgs with a fully resolved list of environment.
func ExpandContainerCommandOnlyStatic(containerCommand []string, envs []v1.EnvVar) (command []string) {
	mapping := expansion.MappingFuncFor(v1EnvVarsToMap(envs))
	// 逐个展开命令参数；空命令直接返回空切片。
	if len(containerCommand) != 0 {
		for _, cmd := range containerCommand {
			command = append(command, expansion.Expand(cmd, mapping))
		}
	}
	return command
}

// ExpandContainerVolumeMounts 用给定 EnvVar 的值替换变量引用，展开 VolumeMount 的 subpath。
func ExpandContainerVolumeMounts(mount v1.VolumeMount, envs []EnvVar) (string, error) {

	envmap := envVarsToMap(envs)
	missingKeys := sets.New[string]()
	// 收集所有缺失的变量名，一次性报错，方便用户一次修正所有配置错误。
	expanded := expansion.Expand(mount.SubPathExpr, func(key string) string {
		value, ok := envmap[key]
		if !ok || len(value) == 0 {
			missingKeys.Insert(key)
		}
		return value
	})

	if len(missingKeys) > 0 {
		return "", fmt.Errorf("missing value for %s", strings.Join(sets.List(missingKeys), ", "))
	}
	return expanded, nil
}

// ExpandContainerCommandAndArgs 用给定 EnvVar 的值替换变量引用，展开 Container 的 command 与 args。
func ExpandContainerCommandAndArgs(container *v1.Container, envs []EnvVar) (command []string, args []string) {
	mapping := expansion.MappingFuncFor(envVarsToMap(envs))

	if len(container.Command) != 0 {
		for _, cmd := range container.Command {
			command = append(command, expansion.Expand(cmd, mapping))
		}
	}

	if len(container.Args) != 0 {
		for _, arg := range container.Args {
			args = append(args, expansion.Expand(arg, mapping))
		}
	}

	return command, args
}

// FilterEventRecorder 包装事件记录器，过滤掉基础设施容器等隐式容器产生的事件。
func FilterEventRecorder(recorder record.EventRecorderLogger) record.EventRecorderLogger {
	return &innerEventRecorder{
		recorder: recorder,
	}
}

// innerEventRecorder 是可过滤事件对象的事件记录器包装。
type innerEventRecorder struct {
	recorder record.EventRecorderLogger
}

// WithLogger 返回绑定了指定 logger 的新事件记录器，底层记录器也同步切换 logger。
func (irecorder *innerEventRecorder) WithLogger(logger klog.Logger) record.EventRecorderLogger {
	return &innerEventRecorder{
		recorder: irecorder.recorder.WithLogger(logger),
	}
}

// shouldRecordEvent 判断事件对象是否需要记录，仅接受非隐式容器的 ObjectReference。
func (irecorder *innerEventRecorder) shouldRecordEvent(object runtime.Object) (*v1.ObjectReference, bool) {
	if ref, ok := object.(*v1.ObjectReference); ok {
		// 先断言再判 nil：类型化 nil 指针不会让断言失败，必须在断言后检查（见 issue 95552）。
		if ref == nil {
			return nil, false
		}
		if !strings.HasPrefix(ref.FieldPath, ImplicitContainerPrefix) {
			return ref, true
		}
	}
	return nil, false
}

// Event 记录事件，隐式容器产生的事件会被丢弃。
func (irecorder *innerEventRecorder) Event(object runtime.Object, eventtype, reason, message string) {
	if ref, ok := irecorder.shouldRecordEvent(object); ok {
		irecorder.recorder.Event(ref, eventtype, reason, message)
	}
}

// Eventf 记录格式化事件，隐式容器产生的事件会被丢弃。
func (irecorder *innerEventRecorder) Eventf(object runtime.Object, eventtype, reason, messageFmt string, args ...interface{}) {
	if ref, ok := irecorder.shouldRecordEvent(object); ok {
		irecorder.recorder.Eventf(ref, eventtype, reason, messageFmt, args...)
	}

}

// AnnotatedEventf 记录带注解的格式化事件，隐式容器产生的事件会被丢弃。
func (irecorder *innerEventRecorder) AnnotatedEventf(object runtime.Object, annotations map[string]string, eventtype, reason, messageFmt string, args ...interface{}) {
	if ref, ok := irecorder.shouldRecordEvent(object); ok {
		//nolint:forbidigo // Legacy usage
		irecorder.recorder.AnnotatedEventf(ref, annotations, eventtype, reason, messageFmt, args...)
	}

}

// IsHostNetworkPod 返回给定 Pod 是否使用宿主网络。Pod 不能为 nil。
func IsHostNetworkPod(pod *v1.Pod) bool {
	return pod.Spec.HostNetwork
}

// ConvertPodStatusToRunningPod 根据 PodStatus 和容器运行时名称构造 Pod。
// TODO(random-liu): Convert PodStatus to running Pod, should be deprecated soon
func ConvertPodStatusToRunningPod(runtimeName string, podStatus *PodStatus) Pod {
	runningPod := Pod{
		ID:        podStatus.ID,
		Name:      podStatus.Name,
		Namespace: podStatus.Namespace,
	}
	// 只把运行中的容器纳入结果，已退出的容器不属于“运行中的 pod”。
	for _, containerStatus := range podStatus.ContainerStatuses {
		if containerStatus.State != ContainerStateRunning {
			continue
		}
		container := &Container{
			ID:                  containerStatus.ID,
			Name:                containerStatus.Name,
			Image:               containerStatus.Image,
			ImageID:             containerStatus.ImageID,
			ImageRef:            containerStatus.ImageRef,
			ImageRuntimeHandler: containerStatus.ImageRuntimeHandler,
			Hash:                containerStatus.Hash,
			State:               containerStatus.State,
		}
		runningPod.Containers = append(runningPod.Containers, container)
	}

	// 把 sandbox 也作为容器填入 kubecontainer.Pod，使 PLEG 能以统一形式处理。
	for _, sandbox := range podStatus.SandboxStatuses {
		runningPod.Sandboxes = append(runningPod.Sandboxes, &Container{
			ID:    ContainerID{Type: runtimeName, ID: sandbox.Id},
			State: SandboxToContainerState(sandbox.State),
		})
	}
	return runningPod
}

// SandboxToContainerState 把 runtimeapi.PodSandboxState 转换为 kubecontainer.State。
// 之所以需要转换，是为了把 sandbox 当作 kubecontainer.Container 返回，
// 避免对 PLEG 做大改动。
// TODO: Remove this once it becomes obsolete.
func SandboxToContainerState(state runtimeapi.PodSandboxState) State {
	switch state {
	case runtimeapi.PodSandboxState_SANDBOX_READY:
		return ContainerStateRunning
	case runtimeapi.PodSandboxState_SANDBOX_NOTREADY:
		return ContainerStateExited
	}
	return ContainerStateUnknown
}

// GetContainerSpec 按容器名从 pod 中查找容器 spec。
func GetContainerSpec(pod *v1.Pod, containerName string) *v1.Container {
	var containerSpec *v1.Container
	// 命中即返回 false 提前终止遍历，无需检查其余容器。
	podutil.VisitContainers(&pod.Spec, podutil.AllFeatureEnabledContainers(), func(c *v1.Container, containerType podutil.ContainerType) bool {
		if containerName == c.Name {
			containerSpec = c
			return false
		}
		return true
	})
	return containerSpec
}

// HasPrivilegedContainer 返回 pod 中是否有任一容器开启了特权模式。
func HasPrivilegedContainer(pod *v1.Pod) bool {
	var hasPrivileged bool
	// 找到第一个特权容器即可提前返回，无需遍历全部容器。
	podutil.VisitContainers(&pod.Spec, podutil.AllFeatureEnabledContainers(), func(c *v1.Container, containerType podutil.ContainerType) bool {
		if c.SecurityContext != nil && c.SecurityContext.Privileged != nil && *c.SecurityContext.Privileged {
			hasPrivileged = true
			return false
		}
		return true
	})
	return hasPrivileged
}

// HasWindowsHostProcessContainer 返回 pod 中是否有任一容器是 HostProcess 容器。
func HasWindowsHostProcessContainer(pod *v1.Pod) bool {
	var hasHostProcess bool
	// 找到第一个 HostProcess 容器即可提前终止遍历。
	podutil.VisitContainers(&pod.Spec, podutil.AllFeatureEnabledContainers(), func(c *v1.Container, containerType podutil.ContainerType) bool {
		if sc.HasWindowsHostProcessRequest(pod, c) {
			hasHostProcess = true
			return false
		}
		return true
	})

	return hasHostProcess
}

// AllContainersAreWindowsHostProcess 返回 pod 中所有容器是否都是 HostProcess 容器。
func AllContainersAreWindowsHostProcess(pod *v1.Pod) bool {
	allHostProcess := true
	// 只要发现一个非 HostProcess 容器即可提前终止并判定为 false。
	podutil.VisitContainers(&pod.Spec, podutil.AllFeatureEnabledContainers(), func(c *v1.Container, containerType podutil.ContainerType) bool {
		if !sc.HasWindowsHostProcessRequest(pod, c) {
			allHostProcess = false
			return false
		}
		return true
	})

	return allHostProcess
}

// MakePortMappings 把 API 的端口定义转换为内部端口映射。
func MakePortMappings(logger klog.Logger, container *v1.Container) (ports []PortMapping) {
	names := make(map[string]struct{})
	for _, p := range container.Ports {
		pm := PortMapping{
			HostPort:      int(p.HostPort),
			ContainerPort: int(p.ContainerPort),
			Protocol:      p.Protocol,
			HostIP:        p.HostIP,
		}

		// 需要确定该条目适用的地址族，确保不同地址族下重复的 containerPort/protocol
		// 规则互不冲突（见 https://github.com/kubernetes/kubernetes/issues/82373）。
		family := "any"
		if p.HostIP != "" {
			if utilsnet.IsIPv6String(p.HostIP) {
				family = "v6"
			} else {
				family = "v4"
			}
		}

		var name = p.Name
		if name == "" {
			// 无显式名称时用地址族、协议与端口组合生成唯一名称，保证映射可唯一标识。
			name = fmt.Sprintf("%s-%s-%s:%d:%d", family, p.Protocol, p.HostIP, p.ContainerPort, p.HostPort)
		}

		// 同名端口只保留第一个，防止重复定义造成运行时行为歧义，并记录日志提示用户。
		if _, ok := names[name]; ok {
			logger.Info("Port name conflicted, it is defined more than once", "portName", name)
			continue
		}
		ports = append(ports, pm)
		names[name] = struct{}{}
	}
	return
}

// HasAnyRegularContainerStarted 返回是否有普通容器已启动，这表示所有 init 容器均已完成初始化。
// Deprecated: 该函数在 pod sandbox 重建后并不准确，请改用 HasAnyActiveRegularContainerStarted。
func HasAnyRegularContainerStarted(spec *v1.PodSpec, statuses []v1.ContainerStatus) bool {
	if len(statuses) == 0 {
		return false
	}

	// 用容器名集合过滤，仅关注普通容器，跳过 init 容器。
	containerNames := make(map[string]struct{})
	for _, c := range spec.Containers {
		containerNames[c.Name] = struct{}{}
	}

	for _, status := range statuses {
		if _, ok := containerNames[status.Name]; !ok {
			continue
		}
		if status.State.Running != nil || status.State.Terminated != nil {
			return true
		}
	}

	return false
}

// HasAnyActiveRegularContainerStarted 返回当前 pod sandbox 中是否有普通容器已启动，
// 这表示所有 init 容器均已完成初始化。
func HasAnyActiveRegularContainerStarted(spec *v1.PodSpec, podStatus *PodStatus) bool {
	if podStatus == nil {
		return false
	}

	// 先收集普通容器名称，便于后续与活跃状态快速比对。
	containerNames := sets.New[string]()
	for _, c := range spec.Containers {
		containerNames.Insert(c.Name)
	}

	// 只检查活跃容器的状态，避免把旧 sandbox 中遗留的容器误判为已启动。
	for _, status := range podStatus.ActiveContainerStatuses {
		if !containerNames.Has(status.Name) {
			continue
		}
		return true
	}

	return false
}
