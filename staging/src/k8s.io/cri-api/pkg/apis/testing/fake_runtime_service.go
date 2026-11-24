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

package testing

import (
	"context"
	"fmt"
	"reflect"
	"sync"
	"time"

	"google.golang.org/protobuf/proto"
	runtimeapi "k8s.io/cri-api/pkg/apis/runtime/v1"
)

var (
	// FakeVersion 是假运行时对外报告的版本号，同时被用作运行时版本与 API 版本。
	FakeVersion = "0.1.0"

	// FakeRuntimeName 是假运行时对外报告的运行时名称。
	FakeRuntimeName = "fakeRuntime"

	// FakePodSandboxIPs 是假运行时为 sandbox 分配的 IP 列表；第一个作为主 IP，其余作为附加 IP。
	FakePodSandboxIPs = []string{"192.168.192.168"}
)

// FakePodSandbox 是 runtimeapi.PodSandboxStatus 的假实现，额外记录了创建 sandbox 时传入的 runtime handler。
type FakePodSandbox struct {
	// PodSandboxStatus 保存 sandbox 的运行时状态，嵌入后可直接访问其字段。
	runtimeapi.PodSandboxStatus
	// RuntimeHandler 记录 RunPodSandbox 请求传入的 runtime handler，便于测试断言其被正确传递。
	RuntimeHandler string
}

// FakeContainer 是容器的假实现，在容器状态之外还保存所属 sandbox 的 ID 与 Linux 资源配置。
type FakeContainer struct {
	// ContainerStatus 保存容器的运行时状态，嵌入后可直接访问其字段。
	runtimeapi.ContainerStatus

	// LinuxResources 保存 Linux 容器特有的资源限制，供测试校验资源相关行为。
	LinuxResources *runtimeapi.LinuxContainerResources

	// SandboxID 是该容器所属 PodSandbox 的 ID，用于按 sandbox 过滤容器。
	SandboxID string
}

// FakeRuntimeService 是 CRI RuntimeService 接口的假实现，用内存 map 模拟容器与 sandbox 的生命周期，并记录调用历史与错误注入。
type FakeRuntimeService struct {
	// 嵌入互斥锁，保护 Called、Errors 以及各资源 map 等共享字段的并发访问安全。
	sync.Mutex

	// Called 按调用顺序记录方法名，供 AssertCalls/GetCalls 进行断言。
	Called []string
	// Errors 按方法名保存待注入的错误队列，每次调用弹出队首错误。
	Errors map[string][]error

	// 以下字段是假运行时对外返回的各类状态数据：
	// FakeStatus 是 Status 返回的运行时状态；
	// Containers 与 Sandboxes 分别按 ID 索引假容器与假 sandbox；
	// FakeContainerStats 与 FakePodSandboxStats 存放手动注入的统计信息；
	// FakePodSandboxMetrics、FakeMetricDescriptors、FakeContainerMetrics 存放指标数据与指标描述符；
	// FakeLinuxConfiguration 是 RuntimeConfig 返回的 Linux 运行时配置。
	FakeStatus             *runtimeapi.RuntimeStatus
	Containers             map[string]*FakeContainer
	Sandboxes              map[string]*FakePodSandbox
	FakeContainerStats     map[string]*runtimeapi.ContainerStats
	FakePodSandboxStats    map[string]*runtimeapi.PodSandboxStats
	FakePodSandboxMetrics  map[string]*runtimeapi.PodSandboxMetrics
	FakeMetricDescriptors  map[string]*runtimeapi.MetricDescriptor
	FakeContainerMetrics   map[string]*runtimeapi.ContainerMetrics
	FakeLinuxConfiguration *runtimeapi.LinuxRuntimeConfiguration

	// ErrorOnSandboxCreate 为 true 时，RunPodSandbox 会强制返回错误，用于测试创建失败路径。
	ErrorOnSandboxCreate bool

	// CheckpointedPods 按顺序记录收到的 CheckpointPod 请求，供测试断言传入的请求内容。
	CheckpointedPods []*runtimeapi.CheckpointPodRequest
	// CheckpointPodDeadline 记录最近一次 CheckpointPod 调用所观察到的 context 截止时间。
	CheckpointPodDeadline time.Time

	// RestoredPods 按顺序记录收到的 RestorePod 请求，供测试断言传入的 sandbox 配置。
	RestoredPods []*runtimeapi.RestorePodRequest
	// RestorePodDeadline 记录最近一次 RestorePod 调用所观察到的 context 截止时间。
	RestorePodDeadline time.Time
}

// GetContainerID 根据 sandbox ID、容器名和 attempt 查找并返回唯一匹配的容器 ID。
func (r *FakeRuntimeService) GetContainerID(sandboxID, name string, attempt uint32) (string, error) {
	r.Lock() // 加互斥锁，保护共享的 Containers map。
	defer r.Unlock()

	// 遍历所有容器，按 sandboxID、名称和 attempt 三元组精确匹配目标容器。
	for id, c := range r.Containers {
		if c.SandboxID == sandboxID && c.Metadata.Name == name && c.Metadata.Attempt == attempt {
			return id, nil // 找到匹配项直接返回其 ID。
		}
	}
	// 未找到匹配容器时，返回包含三元组信息的错误，便于定位问题。
	return "", fmt.Errorf("container (name, attempt, sandboxID)=(%q, %d, %q) not found", name, attempt, sandboxID)
}

// SetFakeSandboxes 用给定的 sandbox 列表重建内部的 Sandboxes map（以 sandbox ID 为键）。
func (r *FakeRuntimeService) SetFakeSandboxes(sandboxes []*FakePodSandbox) {
	r.Lock() // 加互斥锁，避免重建 map 时与其他方法并发访问。
	defer r.Unlock()

	r.Sandboxes = make(map[string]*FakePodSandbox) // 先清空并重建 map，确保不残留旧的 sandbox。
	for _, sandbox := range sandboxes {
		sandboxID := sandbox.Id
		r.Sandboxes[sandboxID] = sandbox // 以 sandbox 自身 ID 为键写入 map。
	}
}

// SetFakeContainers 用给定的容器列表重建内部的 Containers map（以容器 ID 为键）。
func (r *FakeRuntimeService) SetFakeContainers(containers []*FakeContainer) {
	r.Lock() // 加互斥锁，避免重建 map 时与其他方法并发访问。
	defer r.Unlock()

	r.Containers = make(map[string]*FakeContainer) // 先清空并重建 map，确保不残留旧的容器。
	for _, c := range containers {
		containerID := c.Id
		r.Containers[containerID] = c // 以容器自身 ID 为键写入 map。
	}

}

// AssertCalls 断言记录到的调用序列与期望的 calls 完全一致（顺序与元素都相同）。
func (r *FakeRuntimeService) AssertCalls(calls []string) error {
	r.Lock() // 加互斥锁，安全读取共享的 Called 切片。
	defer r.Unlock()

	// 使用 DeepEqual 比较整个调用序列，顺序和元素都必须一致。
	if !reflect.DeepEqual(calls, r.Called) {
		return fmt.Errorf("expected %#v, got %#v", calls, r.Called) // 不一致时带上期望值与实际值方便排查。
	}
	return nil
}

// GetCalls 返回当前已记录调用序列的副本，避免调用方修改内部切片。
func (r *FakeRuntimeService) GetCalls() []string {
	r.Lock() // 加互斥锁，安全读取共享的 Called 切片。
	defer r.Unlock()
	return append([]string{}, r.Called...) // 复制切片后返回，防止外部修改影响内部状态。
}

// InjectError 向指定方法的错误队列追加一个错误，将在下一次对应调用时弹出。
func (r *FakeRuntimeService) InjectError(f string, err error) {
	r.Lock() // 加互斥锁，保护共享的 Errors map。
	defer r.Unlock()
	r.Errors[f] = append(r.Errors[f], err) // 追加到队尾，保证多次注入按先进先出顺序生效。
}

// popError 弹出并返回指定方法错误队列的队首错误；调用方必须已持有锁。
func (r *FakeRuntimeService) popError(f string) error {
	if r.Errors == nil { // 错误表尚未初始化时，视为没有错误可注入。
		return nil
	}
	errs := r.Errors[f] // 取出该方法对应的错误队列。
	if len(errs) == 0 { // 队列为空表示当前没有待注入的错误。
		return nil
	}
	err, errs := errs[0], errs[1:] // 弹出队首错误，剩余元素保留在队列中。
	r.Errors[f] = errs             // 写回缩短后的队列，保证下次调用取到下一个错误。
	return err
}

// NewFakeRuntimeService 创建并初始化一个 FakeRuntimeService，各 map 与切片均置为空。
func NewFakeRuntimeService() *FakeRuntimeService {
	// 初始化所有集合字段，避免调用方在使用前还需手动 make。
	return &FakeRuntimeService{
		Called:                make([]string, 0),
		Errors:                make(map[string][]error),
		Containers:            make(map[string]*FakeContainer),
		Sandboxes:             make(map[string]*FakePodSandbox),
		FakeContainerStats:    make(map[string]*runtimeapi.ContainerStats),
		FakePodSandboxStats:   make(map[string]*runtimeapi.PodSandboxStats),
		FakePodSandboxMetrics: make(map[string]*runtimeapi.PodSandboxMetrics),
		FakeContainerMetrics:  make(map[string]*runtimeapi.ContainerMetrics),
	}
}

// Version 返回假运行时的版本信息。
func (r *FakeRuntimeService) Version(_ context.Context, apiVersion string) (*runtimeapi.VersionResponse, error) {
	r.Lock() // 加互斥锁，保护 Called 与 Errors 等共享字段。
	defer r.Unlock()

	r.Called = append(r.Called, "Version")        // 记录本次调用。
	if err := r.popError("Version"); err != nil { // 检查并消费注入的错误，存在则直接返回。
		return nil, err
	}

	// 构造版本响应，各版本字段统一使用 FakeVersion，运行时名称使用 FakeRuntimeName。
	return &runtimeapi.VersionResponse{
		Version:           FakeVersion,
		RuntimeName:       FakeRuntimeName,
		RuntimeVersion:    FakeVersion,
		RuntimeApiVersion: FakeVersion,
	}, nil
}

// Status 返回假运行时的状态。
func (r *FakeRuntimeService) Status(_ context.Context, verbose bool) (*runtimeapi.StatusResponse, error) {
	r.Lock() // 加互斥锁，保护 Called 与 Errors 等共享字段。
	defer r.Unlock()

	r.Called = append(r.Called, "Status")        // 记录本次调用。
	if err := r.popError("Status"); err != nil { // 检查并消费注入的错误，存在则直接返回。
		return nil, err
	}

	// 将预置的 FakeStatus 包装为状态响应返回。
	return &runtimeapi.StatusResponse{Status: r.FakeStatus}, nil
}

// RunPodSandbox 模拟创建 PodSandbox：记录调用、生成 sandbox 状态并为其分配 IP 与 runtime handler。
func (r *FakeRuntimeService) RunPodSandbox(_ context.Context, config *runtimeapi.PodSandboxConfig, runtimeHandler string) (string, error) {
	r.Lock() // 加互斥锁，保护 Sandboxes、Called、Errors 等共享字段。
	defer r.Unlock()

	r.Called = append(r.Called, "RunPodSandbox")        // 记录本次调用。
	if err := r.popError("RunPodSandbox"); err != nil { // 检查并消费注入的错误，存在则直接返回。
		return "", err
	}

	if r.ErrorOnSandboxCreate { // 通过开关强制注入创建失败，用于测试错误路径。
		return "", fmt.Errorf("error on sandbox create")
	}

	// 真实运行时会随机生成 ID，这里改用 BuildSandboxName() 生成的固定名称，便于测试预先构造期望的 sandbox。
	podSandboxID := BuildSandboxName(config.Metadata)
	createdAt := time.Now().UnixNano() // 记录创建时间戳。
	// 构造 sandbox 状态并写入 map：状态设为 READY，填入配置元数据、创建时间、主 IP 与命名空间选项。
	r.Sandboxes[podSandboxID] = &FakePodSandbox{
		PodSandboxStatus: runtimeapi.PodSandboxStatus{
			Id:        podSandboxID,
			Metadata:  config.Metadata,
			State:     runtimeapi.PodSandboxState_SANDBOX_READY,
			CreatedAt: createdAt,
			Network: &runtimeapi.PodSandboxNetworkStatus{
				Ip: FakePodSandboxIPs[0], // 列表第一个 IP 作为 sandbox 的主 IP。
			},
			// 必须设置 Linux.Namespaces.Options，否则 podSandboxChanged 会误判网络命名空间发生变化，导致反复重建 sandbox 而使 Pod 创建失败。
			// 判断依据为 `sandboxStatus.GetLinux().GetNamespaces().GetOptions().GetNetwork() != networkNamespaceForPod(pod)`。
			Linux: &runtimeapi.LinuxPodSandboxStatus{
				Namespaces: &runtimeapi.Namespace{
					Options: config.GetLinux().GetSecurityContext().GetNamespaceOptions(), // 沿用配置中的命名空间选项。
				},
			},
			Labels:         config.Labels,
			Annotations:    config.Annotations,
			RuntimeHandler: runtimeHandler,
		},
		RuntimeHandler: runtimeHandler, // 在假实现外层也记录 runtime handler，便于测试直接读取。
	}
	// 将 FakePodSandboxIPs 中除主 IP 之外的地址作为附加 IP 写入 sandbox 网络状态。
	additionalIPs := FakePodSandboxIPs[1:]                               // 跳过已用作主 IP 的第一个元素。
	additionalPodIPs := make([]*runtimeapi.PodIP, 0, len(additionalIPs)) // 预分配切片，容量与附加 IP 数量一致。
	for _, ip := range additionalIPs {                                   // 逐个把附加 IP 包装为 PodIP 对象。
		additionalPodIPs = append(additionalPodIPs, &runtimeapi.PodIP{
			Ip: ip,
		})
	}
	r.Sandboxes[podSandboxID].PodSandboxStatus.Network.AdditionalIps = additionalPodIPs // 将附加 IP 列表写入刚创建的 sandbox。
	return podSandboxID, nil                                                            // 返回新 sandbox 的 ID。
}

// StopPodSandbox 模拟停止 sandbox，将其状态置为 SANDBOX_NOTREADY。
func (r *FakeRuntimeService) StopPodSandbox(_ context.Context, podSandboxID string) error {
	r.Lock() // 加互斥锁，保护 Sandboxes、Called、Errors 等共享字段。
	defer r.Unlock()

	r.Called = append(r.Called, "StopPodSandbox")        // 记录本次调用。
	if err := r.popError("StopPodSandbox"); err != nil { // 检查并消费注入的错误，存在则直接返回。
		return err
	}

	if s, ok := r.Sandboxes[podSandboxID]; ok { // 找到目标 sandbox 后修改其状态。
		s.State = runtimeapi.PodSandboxState_SANDBOX_NOTREADY // 标记为未就绪，表示已停止。
	} else {
		return fmt.Errorf("pod sandbox %s not found", podSandboxID) // 找不到 sandbox 时返回错误。
	}

	return nil
}

// RemovePodSandbox 模拟删除 sandbox，直接将其从内部 map 中移除。
func (r *FakeRuntimeService) RemovePodSandbox(_ context.Context, podSandboxID string) error {
	r.Lock() // 加互斥锁，保护 Sandboxes、Called、Errors 等共享字段。
	defer r.Unlock()

	r.Called = append(r.Called, "RemovePodSandbox")        // 记录本次调用。
	if err := r.popError("RemovePodSandbox"); err != nil { // 检查并消费注入的错误，存在则直接返回。
		return err
	}

	// 从 map 中删除目标 sandbox；对不存在的 ID 执行删除不会产生副作用。
	delete(r.Sandboxes, podSandboxID)

	return nil
}

// PodSandboxStatus 返回指定 sandbox 的状态副本。
func (r *FakeRuntimeService) PodSandboxStatus(_ context.Context, podSandboxID string, verbose bool) (*runtimeapi.PodSandboxStatusResponse, error) {
	r.Lock() // 加互斥锁，保护 Sandboxes、Called、Errors 等共享字段。
	defer r.Unlock()

	r.Called = append(r.Called, "PodSandboxStatus")        // 记录本次调用。
	if err := r.popError("PodSandboxStatus"); err != nil { // 检查并消费注入的错误，存在则直接返回。
		return nil, err
	}

	s, ok := r.Sandboxes[podSandboxID] // 按 ID 查找目标 sandbox。
	if !ok {
		return nil, fmt.Errorf("pod sandbox %q not found", podSandboxID) // 不存在则返回错误。
	}

	status := proto.Clone(&s.PodSandboxStatus).(*runtimeapi.PodSandboxStatus) // 深拷贝状态，避免调用方修改内部存储。
	return &runtimeapi.PodSandboxStatusResponse{Status: status}, nil          // 将副本包装为响应返回。
}

// ListPodSandbox 返回 sandbox 列表，支持按 ID、状态和标签选择器过滤。
func (r *FakeRuntimeService) ListPodSandbox(_ context.Context, filter *runtimeapi.PodSandboxFilter) ([]*runtimeapi.PodSandbox, error) {
	r.Lock() // 加互斥锁，保护 Sandboxes、Called、Errors 等共享字段。
	defer r.Unlock()

	r.Called = append(r.Called, "ListPodSandbox")        // 记录本次调用。
	if err := r.popError("ListPodSandbox"); err != nil { // 检查并消费注入的错误，存在则直接返回。
		return nil, err
	}

	result := make([]*runtimeapi.PodSandbox, 0) // 预分配结果切片。
	for id, s := range r.Sandboxes {            // 遍历所有 sandbox 并逐个应用过滤条件。
		if filter != nil { // 仅当提供了 filter 时才执行过滤。
			if filter.Id != "" && filter.Id != id { // 按 ID 精确过滤，不匹配则跳过。
				continue
			}
			if filter.State != nil && filter.GetState().State != s.State { // 按状态过滤，不匹配则跳过。
				continue
			}
			if filter.LabelSelector != nil && !filterInLabels(filter.LabelSelector, s.GetLabels()) { // 按标签选择器过滤，不匹配则跳过。
				continue
			}
		}

		// 通过所有过滤条件后，构造精简的 PodSandbox 信息加入结果列表。
		result = append(result, &runtimeapi.PodSandbox{
			Id:             s.Id,
			Metadata:       s.Metadata,
			State:          s.State,
			CreatedAt:      s.CreatedAt,
			Labels:         s.Labels,
			Annotations:    s.Annotations,
			RuntimeHandler: s.RuntimeHandler,
		})
	}

	return result, nil
}

// PortForward 模拟端口转发请求，仅记录调用并返回空响应。
func (r *FakeRuntimeService) PortForward(context.Context, *runtimeapi.PortForwardRequest) (*runtimeapi.PortForwardResponse, error) {
	r.Lock() // 加互斥锁，保护 Called 与 Errors 等共享字段。
	defer r.Unlock()

	r.Called = append(r.Called, "PortForward")        // 记录本次调用。
	if err := r.popError("PortForward"); err != nil { // 检查并消费注入的错误，存在则直接返回。
		return nil, err
	}

	return &runtimeapi.PortForwardResponse{}, nil // 返回空的端口转发响应。
}

// CreateContainer 模拟创建容器：生成容器 ID，保存初始状态（CREATED）及所属 sandbox 与资源配置。
func (r *FakeRuntimeService) CreateContainer(_ context.Context, podSandboxID string, config *runtimeapi.ContainerConfig, sandboxConfig *runtimeapi.PodSandboxConfig) (string, error) {
	r.Lock() // 加互斥锁，保护 Containers、Called、Errors 等共享字段。
	defer r.Unlock()

	r.Called = append(r.Called, "CreateContainer")        // 记录本次调用。
	if err := r.popError("CreateContainer"); err != nil { // 检查并消费注入的错误，存在则直接返回。
		return "", err
	}

	// 真实运行时会随机生成容器 ID，这里改用 BuildContainerName() 生成的固定名称，便于测试预先构造期望的容器。
	containerID := BuildContainerName(config.Metadata, podSandboxID)
	createdAt := time.Now().UnixNano()                          // 记录创建时间戳。
	createdState := runtimeapi.ContainerState_CONTAINER_CREATED // 初始状态固定为 CREATED。
	imageRef := config.Image.Image                              // 记录镜像引用字符串。
	// 构造假容器并写入 map：填入元数据、镜像、标签、所属 sandbox 与 Linux 资源限制。
	r.Containers[containerID] = &FakeContainer{
		ContainerStatus: runtimeapi.ContainerStatus{
			Id:          containerID,
			Metadata:    config.Metadata,
			Image:       config.Image,
			ImageRef:    imageRef,
			CreatedAt:   createdAt,
			State:       createdState,
			Labels:      config.Labels,
			Annotations: config.Annotations,
		},
		SandboxID:      podSandboxID,                     // 记录所属 sandbox，便于按 sandbox 过滤容器。
		LinuxResources: config.GetLinux().GetResources(), // 保存 Linux 容器的资源限制。
	}

	return containerID, nil // 返回新创建容器的 ID。
}

// StartContainer 模拟启动容器，将其状态置为 RUNNING 并记录启动时间。
func (r *FakeRuntimeService) StartContainer(_ context.Context, containerID string) error {
	r.Lock() // 加互斥锁，保护 Containers、Called、Errors 等共享字段。
	defer r.Unlock()

	r.Called = append(r.Called, "StartContainer")        // 记录本次调用。
	if err := r.popError("StartContainer"); err != nil { // 检查并消费注入的错误，存在则直接返回。
		return err
	}

	c, ok := r.Containers[containerID] // 按 ID 查找目标容器。
	if !ok {
		return fmt.Errorf("container %s not found", containerID) // 容器不存在时返回错误。
	}

	// 将容器状态切换为 RUNNING，表示容器已启动。
	c.State = runtimeapi.ContainerState_CONTAINER_RUNNING
	c.StartedAt = time.Now().UnixNano() // 记录容器启动时间戳。

	return nil
}

// StopContainer 模拟停止容器，将其状态置为 EXITED 并记录结束时间。
func (r *FakeRuntimeService) StopContainer(_ context.Context, containerID string, timeout int64) error {
	r.Lock() // 加互斥锁，保护 Containers、Called、Errors 等共享字段。
	defer r.Unlock()

	r.Called = append(r.Called, "StopContainer")        // 记录本次调用。
	if err := r.popError("StopContainer"); err != nil { // 检查并消费注入的错误，存在则直接返回。
		return err
	}

	c, ok := r.Containers[containerID] // 按 ID 查找目标容器。
	if !ok {
		return fmt.Errorf("container %q not found", containerID) // 容器不存在时返回错误。
	}

	// 将容器状态切换为 EXITED，并记录结束时间戳。
	finishedAt := time.Now().UnixNano()                       // 记录容器结束时间戳。
	exitedState := runtimeapi.ContainerState_CONTAINER_EXITED // 目标状态为已退出。
	c.State = exitedState                                     // 更新容器状态。
	c.FinishedAt = finishedAt                                 // 写入结束时间。

	return nil
}

// RemoveContainer 模拟删除容器，从内部 map 中移除目标容器。
func (r *FakeRuntimeService) RemoveContainer(_ context.Context, containerID string) error {
	r.Lock() // 加互斥锁，保护 Containers、Called、Errors 等共享字段。
	defer r.Unlock()

	r.Called = append(r.Called, "RemoveContainer")        // 记录本次调用。
	if err := r.popError("RemoveContainer"); err != nil { // 检查并消费注入的错误，存在则直接返回。
		return err
	}

	// 从 map 中删除目标容器；对不存在的 ID 执行删除不会产生副作用。
	delete(r.Containers, containerID)

	return nil
}

// ListContainers 返回容器列表，支持按 ID、sandbox ID、状态和标签选择器过滤。
func (r *FakeRuntimeService) ListContainers(_ context.Context, filter *runtimeapi.ContainerFilter) ([]*runtimeapi.Container, error) {
	r.Lock() // 加互斥锁，保护 Containers、Called、Errors 等共享字段。
	defer r.Unlock()

	r.Called = append(r.Called, "ListContainers")        // 记录本次调用。
	if err := r.popError("ListContainers"); err != nil { // 检查并消费注入的错误，存在则直接返回。
		return nil, err
	}

	result := make([]*runtimeapi.Container, 0) // 预分配结果切片。
	for _, s := range r.Containers {           // 遍历所有容器并逐个应用过滤条件。
		if filter != nil { // 仅当提供了 filter 时才执行过滤。
			if filter.Id != "" && filter.Id != s.Id { // 按容器 ID 精确过滤。
				continue
			}
			if filter.PodSandboxId != "" && filter.PodSandboxId != s.SandboxID { // 按所属 sandbox ID 过滤。
				continue
			}
			if filter.State != nil && filter.GetState().State != s.State { // 按容器状态过滤。
				continue
			}
			if filter.LabelSelector != nil && !filterInLabels(filter.LabelSelector, s.GetLabels()) { // 按标签选择器过滤。
				continue
			}
		}

		// 通过所有过滤条件后，构造精简的 Container 信息加入结果列表。
		result = append(result, &runtimeapi.Container{
			Id:           s.Id,
			CreatedAt:    s.CreatedAt,
			PodSandboxId: s.SandboxID,
			Metadata:     s.Metadata,
			State:        s.State,
			Image:        s.Image,
			ImageRef:     s.ImageRef,
			Labels:       s.Labels,
			Annotations:  s.Annotations,
		})
	}

	return result, nil
}

// ContainerStatus 返回指定容器的状态副本。
func (r *FakeRuntimeService) ContainerStatus(_ context.Context, containerID string, verbose bool) (*runtimeapi.ContainerStatusResponse, error) {
	r.Lock() // 加互斥锁，保护 Containers、Called、Errors 等共享字段。
	defer r.Unlock()

	r.Called = append(r.Called, "ContainerStatus")        // 记录本次调用。
	if err := r.popError("ContainerStatus"); err != nil { // 检查并消费注入的错误，存在则直接返回。
		return nil, err
	}

	c, ok := r.Containers[containerID] // 按 ID 查找目标容器。
	if !ok {
		return nil, fmt.Errorf("container %q not found", containerID) // 容器不存在时返回错误。
	}

	status := proto.Clone(&c.ContainerStatus).(*runtimeapi.ContainerStatus) // 深拷贝状态，避免调用方修改内部存储。
	return &runtimeapi.ContainerStatusResponse{Status: status}, nil         // 将副本包装为响应返回。
}

// UpdateContainerResources 模拟更新容器资源，仅记录调用并返回可能注入的错误。
func (r *FakeRuntimeService) UpdateContainerResources(context.Context, string, *runtimeapi.ContainerResources) error {
	r.Lock() // 加互斥锁，保护 Called 与 Errors 等共享字段。
	defer r.Unlock()

	r.Called = append(r.Called, "UpdateContainerResources") // 记录本次调用。
	return r.popError("UpdateContainerResources")           // 直接返回注入的错误（若有）。
}

// ExecSync 模拟容器内命令的同步执行，仅记录调用并返回可能注入的错误，输出均为空。
func (r *FakeRuntimeService) ExecSync(_ context.Context, containerID string, cmd []string, timeout time.Duration) (stdout []byte, stderr []byte, err error) {
	r.Lock() // 加互斥锁，保护 Called 与 Errors 等共享字段。
	defer r.Unlock()

	r.Called = append(r.Called, "ExecSync") // 记录本次调用。
	err = r.popError("ExecSync")            // 从错误队列取出错误，通过命名返回值返回。
	return                                  // stdout、stderr 保持零值，不模拟真实命令输出。
}

// Exec 模拟 Exec 请求，仅记录调用并返回空响应。
func (r *FakeRuntimeService) Exec(context.Context, *runtimeapi.ExecRequest) (*runtimeapi.ExecResponse, error) {
	r.Lock() // 加互斥锁，保护 Called 与 Errors 等共享字段。
	defer r.Unlock()

	r.Called = append(r.Called, "Exec")        // 记录本次调用。
	if err := r.popError("Exec"); err != nil { // 检查并消费注入的错误，存在则直接返回。
		return nil, err
	}

	return &runtimeapi.ExecResponse{}, nil // 返回空的 Exec 响应。
}

// Attach 模拟 Attach 请求，仅记录调用并返回空响应。
func (r *FakeRuntimeService) Attach(_ context.Context, req *runtimeapi.AttachRequest) (*runtimeapi.AttachResponse, error) {
	r.Lock() // 加互斥锁，保护 Called 与 Errors 等共享字段。
	defer r.Unlock()

	r.Called = append(r.Called, "Attach")        // 记录本次调用。
	if err := r.popError("Attach"); err != nil { // 检查并消费注入的错误，存在则直接返回。
		return nil, err
	}

	return &runtimeapi.AttachResponse{}, nil // 返回空的 Attach 响应。
}

// UpdateRuntimeConfig 模拟更新运行时配置，仅记录调用并返回可能注入的错误。
func (r *FakeRuntimeService) UpdateRuntimeConfig(_ context.Context, runtimeCOnfig *runtimeapi.RuntimeConfig) error {
	r.Lock() // 加互斥锁，保护 Called 与 Errors 等共享字段。
	defer r.Unlock()

	r.Called = append(r.Called, "UpdateRuntimeConfig") // 记录本次调用。
	return r.popError("UpdateRuntimeConfig")           // 直接返回注入的错误（若有）。
}

// SetFakeContainerStats 用给定的统计信息列表重建容器统计 map（以统计信息自带的容器 ID 为键）。
func (r *FakeRuntimeService) SetFakeContainerStats(containerStats []*runtimeapi.ContainerStats) {
	r.Lock() // 加互斥锁，避免重建 map 时与其他方法并发访问。
	defer r.Unlock()

	r.FakeContainerStats = make(map[string]*runtimeapi.ContainerStats) // 先清空并重建 map。
	for _, s := range containerStats {
		r.FakeContainerStats[s.Attributes.Id] = s // 以统计信息中的容器 ID 为键写入 map。
	}
}

// ContainerStats 返回指定容器的统计信息。
func (r *FakeRuntimeService) ContainerStats(_ context.Context, containerID string) (*runtimeapi.ContainerStats, error) {
	r.Lock() // 加互斥锁，保护 FakeContainerStats、Called、Errors 等共享字段。
	defer r.Unlock()

	r.Called = append(r.Called, "ContainerStats")        // 记录本次调用。
	if err := r.popError("ContainerStats"); err != nil { // 检查并消费注入的错误，存在则直接返回。
		return nil, err
	}

	s, found := r.FakeContainerStats[containerID] // 按容器 ID 查找统计信息。
	if !found {
		return nil, fmt.Errorf("no stats for container %q", containerID) // 未找到统计信息时返回错误。
	}
	return s, nil
}

// ListContainerStats 返回容器统计列表，支持按 ID、sandbox ID 和标签选择器过滤。
func (r *FakeRuntimeService) ListContainerStats(_ context.Context, filter *runtimeapi.ContainerStatsFilter) ([]*runtimeapi.ContainerStats, error) {
	r.Lock() // 加互斥锁，保护 Containers、FakeContainerStats、Called、Errors 等共享字段。
	defer r.Unlock()

	r.Called = append(r.Called, "ListContainerStats")        // 记录本次调用。
	if err := r.popError("ListContainerStats"); err != nil { // 检查并消费注入的错误，存在则直接返回。
		return nil, err
	}

	var result []*runtimeapi.ContainerStats
	// 遍历容器：先按 filter 过滤容器，再查找该容器是否存在对应的统计信息。
	for _, c := range r.Containers {
		if filter != nil { // 仅当提供了 filter 时才执行过滤。
			if filter.Id != "" && filter.Id != c.Id { // 按容器 ID 精确过滤。
				continue
			}
			if filter.PodSandboxId != "" && filter.PodSandboxId != c.SandboxID { // 按所属 sandbox ID 过滤。
				continue
			}
			if filter.LabelSelector != nil && !filterInLabels(filter.LabelSelector, c.GetLabels()) { // 按标签选择器过滤。
				continue
			}
		}
		s, found := r.FakeContainerStats[c.Id] // 查找该容器的统计信息。
		if !found {                            // 没有注入统计信息的容器直接跳过。
			continue
		}
		result = append(result, s) // 将通过过滤且存在统计信息的结果加入列表。
	}

	return result, nil
}

// SetFakePodSandboxStats 用给定的统计信息列表重建 sandbox 统计 map（以统计信息自带的 sandbox ID 为键）。
func (r *FakeRuntimeService) SetFakePodSandboxStats(podStats []*runtimeapi.PodSandboxStats) {
	r.Lock() // 加互斥锁，避免重建 map 时与其他方法并发访问。
	defer r.Unlock()

	r.FakePodSandboxStats = make(map[string]*runtimeapi.PodSandboxStats) // 先清空并重建 map。
	for _, s := range podStats {
		r.FakePodSandboxStats[s.Attributes.Id] = s // 以统计信息中的 sandbox ID 为键写入 map。
	}
}

// PodSandboxStats 返回指定 sandbox 的统计信息。
func (r *FakeRuntimeService) PodSandboxStats(_ context.Context, podSandboxID string) (*runtimeapi.PodSandboxStats, error) {
	r.Lock() // 加互斥锁，保护 FakePodSandboxStats、Called、Errors 等共享字段。
	defer r.Unlock()

	r.Called = append(r.Called, "PodSandboxStats")        // 记录本次调用。
	if err := r.popError("PodSandboxStats"); err != nil { // 检查并消费注入的错误，存在则直接返回。
		return nil, err
	}

	s, found := r.FakePodSandboxStats[podSandboxID] // 按 sandbox ID 查找统计信息。
	if !found {
		return nil, fmt.Errorf("no stats for pod sandbox %q", podSandboxID) // 未找到统计信息时返回错误。
	}
	return s, nil
}

// ListPodSandboxStats 返回 sandbox 统计列表，支持按 ID 和标签选择器过滤。
func (r *FakeRuntimeService) ListPodSandboxStats(_ context.Context, filter *runtimeapi.PodSandboxStatsFilter) ([]*runtimeapi.PodSandboxStats, error) {
	r.Lock() // 加互斥锁，保护 Sandboxes、FakePodSandboxStats、Called、Errors 等共享字段。
	defer r.Unlock()

	r.Called = append(r.Called, "ListPodSandboxStats")        // 记录本次调用。
	if err := r.popError("ListPodSandboxStats"); err != nil { // 检查并消费注入的错误，存在则直接返回。
		return nil, err
	}

	var result []*runtimeapi.PodSandboxStats
	// 遍历 sandbox：先按 filter 过滤，再查找该 sandbox 是否存在对应的统计信息。
	for _, sb := range r.Sandboxes {
		if filter != nil { // 仅当提供了 filter 时才执行过滤。
			if filter.Id != "" && filter.Id != sb.Id { // 按 sandbox ID 精确过滤。
				continue
			}
			if filter.LabelSelector != nil && !filterInLabels(filter.LabelSelector, sb.GetLabels()) { // 按标签选择器过滤。
				continue
			}
		}
		s, found := r.FakePodSandboxStats[sb.Id] // 查找该 sandbox 的统计信息。
		if !found {                              // 没有注入统计信息的 sandbox 直接跳过。
			continue
		}
		result = append(result, s) // 将通过过滤且存在统计信息的结果加入列表。
	}

	return result, nil
}

// ReopenContainerLog 模拟重新打开容器日志文件，仅记录调用并返回可能注入的错误。
func (r *FakeRuntimeService) ReopenContainerLog(_ context.Context, containerID string) error {
	r.Lock() // 加互斥锁，保护 Called 与 Errors 等共享字段。
	defer r.Unlock()

	r.Called = append(r.Called, "ReopenContainerLog") // 记录本次调用。

	if err := r.popError("ReopenContainerLog"); err != nil { // 检查并消费注入的错误，存在则直接返回。
		return err
	}

	return nil
}

// CheckpointContainer 模拟容器 checkpoint，仅记录调用并返回可能注入的错误。
func (r *FakeRuntimeService) CheckpointContainer(_ context.Context, options *runtimeapi.CheckpointContainerRequest) error {
	r.Lock() // 加互斥锁，保护 Called 与 Errors 等共享字段。
	defer r.Unlock()

	r.Called = append(r.Called, "CheckpointContainer") // 记录本次调用。

	if err := r.popError("CheckpointContainer"); err != nil { // 检查并消费注入的错误，存在则直接返回。
		return err
	}

	return nil
}

// CheckpointPod 模拟 Pod checkpoint：记录调用、请求参数与 context 截止时间，并返回可能注入的错误。
func (r *FakeRuntimeService) CheckpointPod(ctx context.Context, options *runtimeapi.CheckpointPodRequest) error {
	r.Lock() // 加互斥锁，保护 CheckpointedPods、Called、Errors 等共享字段。
	defer r.Unlock()

	r.Called = append(r.Called, "CheckpointPod")             // 记录本次调用。
	r.CheckpointedPods = append(r.CheckpointedPods, options) // 保存请求参数，供测试断言传入的内容。
	r.CheckpointPodDeadline, _ = ctx.Deadline()              // 记录 context 截止时间，忽略是否存在 deadline 的布尔值。

	if err := r.popError("CheckpointPod"); err != nil { // 检查并消费注入的错误，存在则直接返回。
		return err
	}

	return nil
}

// RestorePod 模拟 Pod 恢复：记录请求与截止时间，并为每个容器配置构造一条恢复结果。
func (r *FakeRuntimeService) RestorePod(ctx context.Context, options *runtimeapi.RestorePodRequest) (*runtimeapi.RestorePodResponse, error) {
	r.Lock() // 加互斥锁，保护 RestoredPods、Called、Errors 等共享字段。
	defer r.Unlock()

	r.Called = append(r.Called, "RestorePod")        // 记录本次调用。
	r.RestoredPods = append(r.RestoredPods, options) // 保存请求参数，供测试断言传入的 sandbox 配置。
	r.RestorePodDeadline, _ = ctx.Deadline()         // 记录 context 截止时间，忽略是否存在 deadline 的布尔值。

	if err := r.popError("RestorePod"); err != nil { // 检查并消费注入的错误，存在则直接返回。
		return nil, err
	}

	// 构造恢复响应，sandbox ID 使用固定的假值。
	response := &runtimeapi.RestorePodResponse{PodSandboxId: "fake-restored-pod-id"}
	for _, config := range options.GetContainerConfigs() { // 遍历请求中的每个容器配置。
		name := config.GetMetadata().GetName()           // 从元数据中取出容器名。
		containerID := "fake-restored-container-" + name // 用假前缀与容器名拼出恢复后的容器 ID。
		// 将容器名与生成的 ID 写入恢复结果列表。
		response.RestoredContainers = append(response.RestoredContainers, &runtimeapi.RestoredContainer{
			Name:        name,
			ContainerId: containerID,
		})
	}
	return response, nil // 返回构造好的恢复响应。
}

// GetContainerEvents 是容器事件订阅接口的实现；假实现不推送任何事件，直接返回 nil。
func (f *FakeRuntimeService) GetContainerEvents(ctx context.Context, containerEventsCh chan *runtimeapi.ContainerEventResponse, connectionEstablishedCallback func(runtimeapi.RuntimeService_GetContainerEventsClient)) error {
	return nil // 不产生容器事件，也不调用连接建立回调。
}

// SetFakeMetricDescriptors 用给定的描述符列表重建指标描述符 map（以描述符名称为键）。
func (r *FakeRuntimeService) SetFakeMetricDescriptors(descs []*runtimeapi.MetricDescriptor) {
	r.Lock() // 加互斥锁，避免重建 map 时与其他方法并发访问。
	defer r.Unlock()

	r.FakeMetricDescriptors = make(map[string]*runtimeapi.MetricDescriptor) // 先清空并重建 map。
	for _, d := range descs {
		r.FakeMetricDescriptors[d.Name] = d // 以描述符名称为键写入 map。
	}
}

// ListMetricDescriptors 返回所有已注入的指标描述符，这些描述符对应 ListPodSandboxMetrics 中的指标。
func (r *FakeRuntimeService) ListMetricDescriptors(_ context.Context) ([]*runtimeapi.MetricDescriptor, error) {
	r.Lock() // 加互斥锁，保护 FakeMetricDescriptors、Called、Errors 等共享字段。
	defer r.Unlock()

	r.Called = append(r.Called, "ListMetricDescriptors")        // 记录本次调用。
	if err := r.popError("ListMetricDescriptors"); err != nil { // 检查并消费注入的错误，存在则直接返回。
		return nil, err
	}

	descs := make([]*runtimeapi.MetricDescriptor, 0, len(r.FakeMetricDescriptors)) // 预分配切片，容量与描述符数量一致。
	for _, d := range r.FakeMetricDescriptors {                                    // 将 map 中的描述符汇总为切片返回。
		descs = append(descs, d)
	}

	return descs, nil
}

// SetFakePodSandboxMetrics 用给定的指标列表重建 sandbox 指标 map（以 sandbox ID 为键）。
func (r *FakeRuntimeService) SetFakePodSandboxMetrics(podStats []*runtimeapi.PodSandboxMetrics) {
	r.Lock() // 加互斥锁，避免重建 map 时与其他方法并发访问。
	defer r.Unlock()

	r.FakePodSandboxMetrics = make(map[string]*runtimeapi.PodSandboxMetrics) // 先清空并重建 map。
	for _, s := range podStats {
		r.FakePodSandboxMetrics[s.PodSandboxId] = s // 以指标自带的 sandbox ID 为键写入 map。
	}
}

// ListPodSandboxMetrics 返回所有已注入指标数据的 sandbox 指标。
func (r *FakeRuntimeService) ListPodSandboxMetrics(_ context.Context) ([]*runtimeapi.PodSandboxMetrics, error) {
	r.Lock() // 加互斥锁，保护 Sandboxes、FakePodSandboxMetrics、Called、Errors 等共享字段。
	defer r.Unlock()

	r.Called = append(r.Called, "ListPodSandboxMetrics")        // 记录本次调用。
	if err := r.popError("ListPodSandboxMetrics"); err != nil { // 检查并消费注入的错误，存在则直接返回。
		return nil, err
	}

	var result []*runtimeapi.PodSandboxMetrics
	// 遍历 sandbox，只返回那些已经注入了指标数据的对象。
	for _, sb := range r.Sandboxes {
		s, found := r.FakePodSandboxMetrics[sb.Id] // 按 sandbox ID 查找指标。
		if !found {                                // 没有注入指标的 sandbox 直接跳过。
			continue
		}
		result = append(result, s) // 将找到的指标加入结果列表。
	}

	return result, nil
}

// RuntimeConfig 返回假运行时的配置。
func (r *FakeRuntimeService) RuntimeConfig(_ context.Context) (*runtimeapi.RuntimeConfigResponse, error) {
	r.Lock() // 加互斥锁，保护 Called 与 Errors 等共享字段。
	defer r.Unlock()

	r.Called = append(r.Called, "RuntimeConfig")        // 记录本次调用。
	if err := r.popError("RuntimeConfig"); err != nil { // 检查并消费注入的错误，存在则直接返回。
		return nil, err
	}

	// 将预置的 Linux 运行时配置包装为响应返回。
	return &runtimeapi.RuntimeConfigResponse{Linux: r.FakeLinuxConfiguration}, nil
}

// UpdatePodSandboxResources 模拟更新 sandbox 资源，仅记录调用并返回可能注入的错误。
func (r *FakeRuntimeService) UpdatePodSandboxResources(context.Context, *runtimeapi.UpdatePodSandboxResourcesRequest) (*runtimeapi.UpdatePodSandboxResourcesResponse, error) {
	r.Lock() // 加互斥锁，保护 Called 与 Errors 等共享字段。
	defer r.Unlock()

	r.Called = append(r.Called, "UpdatePodSandboxResources")        // 记录本次调用。
	if err := r.popError("UpdatePodSandboxResources"); err != nil { // 检查并消费注入的错误，存在则直接返回。
		return nil, err
	}

	return &runtimeapi.UpdatePodSandboxResourcesResponse{}, nil // 返回空响应。
}

// Close 模拟关闭内部 gRPC 客户端连接，仅记录调用并返回可能注入的错误。
func (r *FakeRuntimeService) Close(_ context.Context) error {
	r.Lock() // 加互斥锁，保护 Called 与 Errors 等共享字段。
	defer r.Unlock()

	r.Called = append(r.Called, "Close")        // 记录本次调用。
	if err := r.popError("Close"); err != nil { // 检查并消费注入的错误，存在则直接返回。
		return err
	}

	return nil
}
