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

package testing

import (
	"context"
	"io"
	"net/url"
	"reflect"
	"sync"
	"time"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/flowcontrol"
	runtimeapi "k8s.io/cri-api/pkg/apis/runtime/v1"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/credentialprovider"
	kubecontainer "k8s.io/kubernetes/pkg/kubelet/container"
	kubetypes "k8s.io/kubernetes/pkg/kubelet/types"
	"k8s.io/kubernetes/pkg/volume"
)

// TB 是 fake 运行时需要的最小 testing 接口，便于在测试之外复用。
type TB interface {
	Errorf(format string, args ...any)
}

// FakePod 把运行时的 Pod 与网络命名空间路径组合在一起。
type FakePod struct {
	// Pod 是运行时看到的 Pod 对象。
	Pod *kubecontainer.Pod
	// NetnsPath 是该 Pod 的网络命名空间路径。
	NetnsPath string
}

// FakeRuntime 是用于测试的容器运行时假实现。
type FakeRuntime struct {
	sync.Mutex
	// CalledFunctions 按调用顺序记录方法名，供断言使用。
	CalledFunctions []string
	// PodList 是 GetPods(false) 返回的活跃 Pod；AllPodList 是全量 Pod。
	PodList    []*FakePod
	AllPodList []*FakePod
	// ImageList 保存 PullImage 成功拉取的镜像。
	ImageList []kubecontainer.Image
	// ImageFsStats 与 ContainerFsStats 由测试注入，供 ImageFsInfo 返回。
	ImageFsStats     []*runtimeapi.FilesystemUsage
	ContainerFsStats []*runtimeapi.FilesystemUsage
	// APIPodStatus 与 PodStatus 是预置的 Pod 状态返回值。
	APIPodStatus v1.PodStatus
	PodStatus    kubecontainer.PodStatus
	// StartedPods、KilledPods、StartedContainers、KilledContainers 记录操作过的对象。
	StartedPods       []string
	KilledPods        []string
	StartedContainers []string
	KilledContainers  []string
	// RuntimeStatus 是 Status() 的返回值。
	RuntimeStatus *kubecontainer.RuntimeStatus
	// VersionInfo、APIVersionInfo、RuntimeType 分别决定各版本/类型查询的结果。
	VersionInfo    string
	APIVersionInfo string
	RuntimeType    string
	// PodResizeInProgress 控制 IsPodResizeInProgress 的返回值。
	PodResizeInProgress bool
	// SyncResults 非空时直接作为 SyncPod 的结果返回。
	SyncResults *kubecontainer.PodSyncResult
	// Err 是大多数方法的通用错误；InspectErr 与 StatusErr 分别用于镜像检查与状态查询。
	Err        error
	InspectErr error
	StatusErr  error
	// BlockImagePulls 为 true 时，所有 PullImage() 调用都会阻塞，
	// 直到 UnblockImagePulls() 被调用；用于模拟运行时的镜像拉取延迟。
	BlockImagePulls bool
	// imagePullTokenBucket 存放放行令牌，惰性初始化。
	imagePullTokenBucket chan bool
	// imagePullErrBucket 向被 BlockImagePulls 阻塞的 PullImage() 注入错误，
	// 用于模拟并行拉取中部分调用失败。
	imagePullErrBucket chan error
	// SwapBehavior 按容器名配置 Swap 行为。
	SwapBehavior map[string]kubetypes.SwapBehavior
	// T 用于在断言失败时报告错误。
	T TB
}

// FakeHost 是流式接口返回的伪造主机地址。
const FakeHost = "localhost:12345"

// FakeStreamingRuntime 在 FakeRuntime 基础上实现流式接口。
type FakeStreamingRuntime struct {
	*FakeRuntime
}

// 编译期断言：FakeStreamingRuntime 必须实现 StreamingRuntime 接口。
var _ kubecontainer.StreamingRuntime = &FakeStreamingRuntime{}

// 编译期断言：FakeRuntime 必须实现 Runtime 接口。
var _ kubecontainer.Runtime = &FakeRuntime{}

// FakeVersion 是带字符串版本号的 Version 实现。
type FakeVersion struct {
	// Version 是用于比较的版本字符串。
	Version string
}

// String 返回版本字符串。
func (fv *FakeVersion) String() string {
	return fv.Version
}

// Compare 按字符串字典序比较版本，相等返回 0。
func (fv *FakeVersion) Compare(other string) (int, error) {
	result := 0
	if fv.Version > other {
		result = 1
	} else if fv.Version < other {
		result = -1
	}
	return result, nil
}

// podsGetter 抽象出 GetPods 方法，供 RuntimeCache 假实现使用。
type podsGetter interface {
	GetPods(context.Context, bool) ([]*kubecontainer.Pod, error)
}

// FakeRuntimeCache 是 RuntimeCache 的简单假实现。
type FakeRuntimeCache struct {
	// getter 负责真正列出 Pod。
	getter podsGetter
}

// NewFakeRuntimeCache 用给定 getter 构造 RuntimeCache 假实现。
func NewFakeRuntimeCache(getter podsGetter) kubecontainer.RuntimeCache {
	return &FakeRuntimeCache{getter}
}

// GetPods 转调 getter，并强制 all=false（只取活跃 Pod）。
func (f *FakeRuntimeCache) GetPods(ctx context.Context) ([]*kubecontainer.Pod, error) {
	return f.getter.GetPods(ctx, false)
}

// ForceUpdateIfOlder 是空实现，假缓存没有过期语义。
func (f *FakeRuntimeCache) ForceUpdateIfOlder(context.Context, time.Time) error {
	return nil
}

// UpdatePodCIDR 满足 CRI 接口要求；假实现不做任何事。
func (f *FakeRuntime) UpdatePodCIDR(_ context.Context, c string) error {
	return nil
}

// assertList 比较两个列表，不一致时通过 T 报告错误。
func (f *FakeRuntime) assertList(expect []string, test []string) bool {
	if !reflect.DeepEqual(expect, test) {
		f.T.Errorf("AssertList: expected %#v, got %#v", expect, test)
		return false
	}
	return true
}

// AssertCalls 断言已调用的方法序列与期望一致。
func (f *FakeRuntime) AssertCalls(calls []string) bool {
	f.Lock()
	defer f.Unlock()
	return f.assertList(calls, f.CalledFunctions)
}

// AssertCallCounts 断言指定方法被调用的次数。
func (f *FakeRuntime) AssertCallCounts(funcName string, expectedCount int) bool {
	f.Lock()
	defer f.Unlock()
	actualCount := 0
	for _, c := range f.CalledFunctions {
		if funcName == c {
			actualCount += 1
		}
	}
	if expectedCount != actualCount {
		f.T.Errorf("AssertCallCounts: expected %s to be called %d times, but was actually called %d times.", funcName, expectedCount, actualCount)
		return false
	}
	return true
}

// AssertStartedPods 断言启动过的 Pod 列表与期望一致。
func (f *FakeRuntime) AssertStartedPods(pods []string) bool {
	f.Lock()
	defer f.Unlock()
	return f.assertList(pods, f.StartedPods)
}

// AssertKilledPods 断言被杀掉的 Pod 列表与期望一致。
func (f *FakeRuntime) AssertKilledPods(pods []string) bool {
	f.Lock()
	defer f.Unlock()
	return f.assertList(pods, f.KilledPods)
}

// AssertStartedContainers 断言启动过的容器列表与期望一致。
func (f *FakeRuntime) AssertStartedContainers(containers []string) bool {
	f.Lock()
	defer f.Unlock()
	return f.assertList(containers, f.StartedContainers)
}

// AssertKilledContainers 断言被杀掉的容器列表与期望一致。
func (f *FakeRuntime) AssertKilledContainers(containers []string) bool {
	f.Lock()
	defer f.Unlock()
	return f.assertList(containers, f.KilledContainers)
}

// Type 返回 RuntimeType。
func (f *FakeRuntime) Type() string {
	return f.RuntimeType
}

// Version 返回由 VersionInfo 构造的版本对象。
func (f *FakeRuntime) Version(_ context.Context) (kubecontainer.Version, error) {
	f.Lock()
	defer f.Unlock()

	f.CalledFunctions = append(f.CalledFunctions, "Version")
	return &FakeVersion{Version: f.VersionInfo}, f.Err
}

// APIVersion 返回由 APIVersionInfo 构造的版本对象。
func (f *FakeRuntime) APIVersion() (kubecontainer.Version, error) {
	f.Lock()
	defer f.Unlock()

	f.CalledFunctions = append(f.CalledFunctions, "APIVersion")
	return &FakeVersion{Version: f.APIVersionInfo}, f.Err
}

// Status 返回预置的 RuntimeStatus 与 StatusErr。
func (f *FakeRuntime) Status(_ context.Context) (*kubecontainer.RuntimeStatus, error) {
	f.Lock()
	defer f.Unlock()

	f.CalledFunctions = append(f.CalledFunctions, "Status")
	return f.RuntimeStatus, f.StatusErr
}

// GetPods 根据 all 返回全量或活跃 Pod 列表。
func (f *FakeRuntime) GetPods(_ context.Context, all bool) ([]*kubecontainer.Pod, error) {
	f.Lock()
	defer f.Unlock()

	var pods []*kubecontainer.Pod

	f.CalledFunctions = append(f.CalledFunctions, "GetPods")
	// all=true 时取全量列表，否则只取活跃列表。
	if all {
		for _, fakePod := range f.AllPodList {
			pods = append(pods, fakePod.Pod)
		}
	} else {
		for _, fakePod := range f.PodList {
			pods = append(pods, fakePod.Pod)
		}
	}
	return pods, f.Err
}

// GetPod 按 UID 查找 Pod，先查活跃列表再查全量列表。
func (f *FakeRuntime) GetPod(_ context.Context, podUID types.UID) (*kubecontainer.Pod, error) {
	f.Lock()
	defer f.Unlock()

	if f.Err != nil {
		return nil, f.Err
	}

	for _, fakePod := range f.PodList {
		if fakePod.Pod.ID == podUID {
			return fakePod.Pod, nil
		}
	}
	// 活跃列表没找到时再查全量列表，最后返回 ErrPodNotFound。
	for _, fakePod := range f.AllPodList {
		if fakePod.Pod.ID == podUID {
			return fakePod.Pod, nil
		}
	}
	return nil, kubecontainer.ErrPodNotFound
}

// SyncPod 记录启动的 Pod 与容器，并返回预设结果或通用错误。
func (f *FakeRuntime) SyncPod(_ context.Context, pod *v1.Pod, _ *kubecontainer.PodStatus, _ []v1.Secret, backOff *flowcontrol.Backoff, _ bool) (result kubecontainer.PodSyncResult) {
	f.Lock()
	defer f.Unlock()

	f.CalledFunctions = append(f.CalledFunctions, "SyncPod")
	// 记录启动对象，便于测试断言同步行为。
	f.StartedPods = append(f.StartedPods, string(pod.UID))
	for _, c := range pod.Spec.Containers {
		f.StartedContainers = append(f.StartedContainers, c.Name)
	}
	if f.SyncResults != nil {
		return *f.SyncResults
	}
	// TODO(random-liu): Add SyncResult for starting and killing containers
	if f.Err != nil {
		result.Fail(f.Err)
	}
	return
}

// KillPod 记录被杀掉的 Pod 与容器，并返回通用错误。
func (f *FakeRuntime) KillPod(_ context.Context, pod *v1.Pod, runningPod kubecontainer.Pod, gracePeriodOverride *int64) error {
	f.Lock()
	defer f.Unlock()

	f.CalledFunctions = append(f.CalledFunctions, "KillPod")
	f.KilledPods = append(f.KilledPods, string(runningPod.ID))
	for _, c := range runningPod.Containers {
		f.KilledContainers = append(f.KilledContainers, c.Name)
	}
	return f.Err
}

// RunContainerInPod 记录容器启动，并把容器加入 Pod 规格。
func (f *FakeRuntime) RunContainerInPod(container v1.Container, pod *v1.Pod, volumeMap map[string]volume.VolumePlugin) error {
	f.Lock()
	defer f.Unlock()

	f.CalledFunctions = append(f.CalledFunctions, "RunContainerInPod")
	f.StartedContainers = append(f.StartedContainers, container.Name)

	// 先追加容器，再检查同名容器，保持与原实现顺序一致。
	pod.Spec.Containers = append(pod.Spec.Containers, container)
	for _, c := range pod.Spec.Containers {
		if c.Name == container.Name { // Container already in the pod.
			return f.Err
		}
	}
	pod.Spec.Containers = append(pod.Spec.Containers, container)
	return f.Err
}

// KillContainerInPod 记录容器删除并返回通用错误。
func (f *FakeRuntime) KillContainerInPod(container v1.Container, pod *v1.Pod) error {
	f.Lock()
	defer f.Unlock()

	f.CalledFunctions = append(f.CalledFunctions, "KillContainerInPod")
	f.KilledContainers = append(f.KilledContainers, container.Name)
	return f.Err
}

// GeneratePodStatus 返回预置的 PodStatus 副本。
func (f *FakeRuntime) GeneratePodStatus(event *runtimeapi.ContainerEventResponse) *kubecontainer.PodStatus {
	f.Lock()
	defer f.Unlock()

	f.CalledFunctions = append(f.CalledFunctions, "GeneratePodStatus")
	status := f.PodStatus
	return &status
}

// GetPodStatus 返回预置的 PodStatus 副本与通用错误。
func (f *FakeRuntime) GetPodStatus(_ context.Context, pod *kubecontainer.Pod) (*kubecontainer.PodStatus, error) {
	f.Lock()
	defer f.Unlock()

	f.CalledFunctions = append(f.CalledFunctions, "GetPodStatus")
	status := f.PodStatus
	return &status, f.Err
}

// GetContainerLogs 不做实际输出，只记录调用并返回通用错误。
func (f *FakeRuntime) GetContainerLogs(_ context.Context, pod *v1.Pod, containerID kubecontainer.ContainerID, logOptions *v1.PodLogOptions, stdout, stderr io.Writer) (err error) {
	f.Lock()
	defer f.Unlock()

	f.CalledFunctions = append(f.CalledFunctions, "GetContainerLogs")
	return f.Err
}

// PullImage 模拟镜像拉取；BlockImagePulls 可让调用阻塞，
// 直至显式放行、注入错误或 context 取消。
func (f *FakeRuntime) PullImage(ctx context.Context, image kubecontainer.ImageSpec, creds []credentialprovider.TrackedAuthConfig, podSandboxConfig *runtimeapi.PodSandboxConfig) (string, *credentialprovider.TrackedAuthConfig, error) {
	f.Lock()
	f.CalledFunctions = append(f.CalledFunctions, "PullImage")

	// 惰性初始化令牌/错误通道，允许测试提前注入。
	if f.imagePullTokenBucket == nil {
		f.imagePullTokenBucket = make(chan bool, 1)
	}
	if f.imagePullErrBucket == nil {
		f.imagePullErrBucket = make(chan error, 1)
	}

	// 读取标志后立即释放锁，阻塞等待期间不能占着锁。
	blockImagePulls := f.BlockImagePulls
	f.Unlock()

	// 阻塞等待显式放行或错误注入，同时响应 context 取消。
	if blockImagePulls {
		// 在把镜像加入 f.ImageList 之前阻塞。
		select {
		case <-ctx.Done():
		case <-f.imagePullTokenBucket:
		case pullImageErr := <-f.imagePullErrBucket:
			return "", nil, pullImageErr
		}
	}

	f.Lock()
	defer f.Unlock()

	// 只有未预设通用错误时才把镜像加入列表。
	if f.Err == nil {
		i := kubecontainer.Image{
			ID:   image.Image,
			Spec: image,
		}
		f.ImageList = append(f.ImageList, i)
	}

	// 拉取时提供了凭证的话，至少回传第一个。
	var retCreds *credentialprovider.TrackedAuthConfig = nil
	if len(creds) > 0 {
		retCreds = &creds[0]
	}

	return image.Image, retCreds, f.Err
}

// UnblockImagePulls 在 BlockImagePulls 为 true 时放行指定数量的镜像拉取。
func (f *FakeRuntime) UnblockImagePulls(count int) {
	if f.imagePullTokenBucket != nil {
		// 非阻塞发送令牌，桶满时丢弃多余令牌。
		for i := 0; i < count; i++ {
			select {
			case f.imagePullTokenBucket <- true:
			default:
			}
		}
	}
}

// SendImagePullError 向被 BlockImagePulls 阻塞的 PullImage() 发送错误；
// 收到错误后 PullImage() 立即返回。
func (f *FakeRuntime) SendImagePullError(err error) {
	if f.imagePullErrBucket != nil {
		// 非阻塞发送，避免没有等待者时挂起测试。
		select {
		case f.imagePullErrBucket <- err:
		default:
		}
	}
}

// GetImageRef 在镜像列表中查找 ID；未找到时返回 InspectErr。
func (f *FakeRuntime) GetImageRef(_ context.Context, image kubecontainer.ImageSpec) (string, error) {
	f.Lock()
	defer f.Unlock()

	f.CalledFunctions = append(f.CalledFunctions, "GetImageRef")
	for _, i := range f.ImageList {
		if i.ID == image.Image {
			return i.ID, nil
		}
	}
	return "", f.InspectErr
}

// GetImageSize 是空实现，大小恒为 0。
func (f *FakeRuntime) GetImageSize(_ context.Context, image kubecontainer.ImageSpec) (uint64, error) {
	f.Lock()
	defer f.Unlock()

	f.CalledFunctions = append(f.CalledFunctions, "GetImageSize")
	return 0, f.Err
}

// ListImages 返回镜像列表的副本。
func (f *FakeRuntime) ListImages(_ context.Context) ([]kubecontainer.Image, error) {
	f.Lock()
	defer f.Unlock()

	f.CalledFunctions = append(f.CalledFunctions, "ListImages")
	return snapshot(f.ImageList), f.Err
}

// snapshot 复制切片，避免调用方修改 FakeRuntime 内部状态。
func snapshot(imageList []kubecontainer.Image) []kubecontainer.Image {
	result := make([]kubecontainer.Image, len(imageList))
	copy(result, imageList)
	return result
}

// RemoveImage 从镜像列表中删除匹配项，并返回通用错误。
func (f *FakeRuntime) RemoveImage(_ context.Context, image kubecontainer.ImageSpec) error {
	f.Lock()
	defer f.Unlock()

	f.CalledFunctions = append(f.CalledFunctions, "RemoveImage")
	// 未找到匹配项时 index 保持 0，会删除第一个元素。
	index := 0
	for i := range f.ImageList {
		if f.ImageList[i].ID == image.Image {
			index = i
			break
		}
	}
	f.ImageList = append(f.ImageList[:index], f.ImageList[index+1:]...)

	return f.Err
}

// GarbageCollect 只记录调用并返回通用错误。
func (f *FakeRuntime) GarbageCollect(_ context.Context, gcPolicy kubecontainer.GCPolicy, ready bool, evictNonDeletedPods bool) error {
	f.Lock()
	defer f.Unlock()

	f.CalledFunctions = append(f.CalledFunctions, "GarbageCollect")
	return f.Err
}

// DeleteContainer 只记录调用并返回通用错误。
func (f *FakeRuntime) DeleteContainer(_ context.Context, containerID kubecontainer.ContainerID) error {
	f.Lock()
	defer f.Unlock()

	f.CalledFunctions = append(f.CalledFunctions, "DeleteContainer")
	return f.Err
}

// CheckpointContainer 只记录调用并返回通用错误。
func (f *FakeRuntime) CheckpointContainer(_ context.Context, options *runtimeapi.CheckpointContainerRequest) error {
	f.Lock()
	defer f.Unlock()

	f.CalledFunctions = append(f.CalledFunctions, "CheckpointContainer")
	return f.Err
}

// ListMetricDescriptors 返回空列表与通用错误。
func (f *FakeRuntime) ListMetricDescriptors(_ context.Context) ([]*runtimeapi.MetricDescriptor, error) {
	f.Lock()
	defer f.Unlock()

	f.CalledFunctions = append(f.CalledFunctions, "ListMetricDescriptors")
	return nil, f.Err
}

// ListPodSandboxMetrics 返回空列表与通用错误。
func (f *FakeRuntime) ListPodSandboxMetrics(_ context.Context) ([]*runtimeapi.PodSandboxMetrics, error) {
	f.Lock()
	defer f.Unlock()

	f.CalledFunctions = append(f.CalledFunctions, "ListPodSandboxMetrics")
	return nil, f.Err
}

// SetContainerFsStats 注入容器文件系统使用量，用于依赖注入。
func (f *FakeRuntime) SetContainerFsStats(val []*runtimeapi.FilesystemUsage) {
	f.ContainerFsStats = val
}

// SetImageFsStats 注入镜像文件系统使用量，用于依赖注入。
func (f *FakeRuntime) SetImageFsStats(val []*runtimeapi.FilesystemUsage) {
	f.ImageFsStats = val
}

// ImageStats 返回 nil 与通用错误。
func (f *FakeRuntime) ImageStats(_ context.Context) (*kubecontainer.ImageStats, error) {
	f.Lock()
	defer f.Unlock()

	f.CalledFunctions = append(f.CalledFunctions, "ImageStats")
	return nil, f.Err
}

// ImageFsInfo 用注入的 ImageFsStats 与 ContainerFsStats 构造响应。
func (f *FakeRuntime) ImageFsInfo(_ context.Context) (*runtimeapi.ImageFsInfoResponse, error) {
	f.Lock()
	defer f.Unlock()

	f.CalledFunctions = append(f.CalledFunctions, "ImageFsInfo")
	resp := &runtimeapi.ImageFsInfoResponse{
		ImageFilesystems:     f.ImageFsStats,
		ContainerFilesystems: f.ContainerFsStats,
	}
	return resp, f.Err
}

// GetExec 返回指向 FakeHost 的伪造 URL。
func (f *FakeStreamingRuntime) GetExec(_ context.Context, id kubecontainer.ContainerID, cmd []string, stdin, stdout, stderr, tty bool) (*url.URL, error) {
	f.Lock()
	defer f.Unlock()

	f.CalledFunctions = append(f.CalledFunctions, "GetExec")
	return &url.URL{Host: FakeHost}, f.Err
}

// GetAttach 返回指向 FakeHost 的伪造 URL。
func (f *FakeStreamingRuntime) GetAttach(_ context.Context, id kubecontainer.ContainerID, stdin, stdout, stderr, tty bool) (*url.URL, error) {
	f.Lock()
	defer f.Unlock()

	f.CalledFunctions = append(f.CalledFunctions, "GetAttach")
	return &url.URL{Host: FakeHost}, f.Err
}

// GetPortForward 返回指向 FakeHost 的伪造 URL。
func (f *FakeStreamingRuntime) GetPortForward(_ context.Context, podName, podNamespace string, podUID types.UID, ports []int32) (*url.URL, error) {
	f.Lock()
	defer f.Unlock()

	f.CalledFunctions = append(f.CalledFunctions, "GetPortForward")
	return &url.URL{Host: FakeHost}, f.Err
}

// FakeContainerCommandRunner 是 CommandRunner 的假实现，记录调用参数。
type FakeContainerCommandRunner struct {
	// 预设返回值。
	Stdout string
	Err    error

	// 调用时记录的实际参数。
	ContainerID kubecontainer.ContainerID
	Cmd         []string
}

// 编译期断言：FakeContainerCommandRunner 必须实现 CommandRunner 接口。
var _ kubecontainer.CommandRunner = &FakeContainerCommandRunner{}

// RunInContainer 记录参数并返回预设的输出与错误。
func (f *FakeContainerCommandRunner) RunInContainer(_ context.Context, containerID kubecontainer.ContainerID, cmd []string, timeout time.Duration) ([]byte, error) {
	// 记录实际调用参数，便于测试断言。
	f.ContainerID = containerID
	f.Cmd = cmd

	return []byte(f.Stdout), f.Err
}

// GetContainerStatus 返回空状态与通用错误。
func (f *FakeRuntime) GetContainerStatus(_ context.Context, _ types.UID, _ kubecontainer.ContainerID) (status *kubecontainer.Status, err error) {
	f.Lock()
	defer f.Unlock()

	f.CalledFunctions = append(f.CalledFunctions, "GetContainerStatus")
	return &kubecontainer.Status{}, f.Err
}

// GetContainerSwapBehavior 按容器名返回配置的 Swap 行为。
func (f *FakeRuntime) GetContainerSwapBehavior(pod *v1.Pod, container *v1.Container) kubetypes.SwapBehavior {
	if f.SwapBehavior != nil && f.SwapBehavior[container.Name] != "" {
		return f.SwapBehavior[container.Name]
	}
	// 未显式配置时默认 NoSwap。
	return kubetypes.NoSwap
}

// IsPodResizeInProgress 返回预置的 PodResizeInProgress。
func (f *FakeRuntime) IsPodResizeInProgress(allocatedPod *v1.Pod, podStatus *kubecontainer.PodStatus) bool {
	return f.PodResizeInProgress
}

// UpdateActuatedPodLevelResources 是空实现，始终成功。
func (f *FakeRuntime) UpdateActuatedPodLevelResources(logger klog.Logger, allocatedPod *v1.Pod) error {
	return nil
}

// InitializeActuatedPod 是空实现，不维护任何实际状态。
func (f *FakeRuntime) InitializeActuatedPod(_ klog.Logger, _ *v1.Pod) {
}
