/*
Copyright 2022 The Kubernetes Authors.

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

package app

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"

	resourceapi "k8s.io/api/resource/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/client-go/kubernetes"
	cgoresource "k8s.io/client-go/kubernetes/typed/resource/v1"
	draclient "k8s.io/dynamic-resource-allocation/client"
	"k8s.io/dynamic-resource-allocation/kubeletplugin"
	"k8s.io/dynamic-resource-allocation/resourceclaim"
	"k8s.io/dynamic-resource-allocation/resourceslice"
	"k8s.io/klog/v2"
	cdi "tags.cncf.io/container-device-interface/specs-go"
)

type Options struct {
	EnableHealthService bool

	// DisableHealthV1 和 DisableHealthV1alpha1 限制 driver 对外声明并提供的
	// DRAResourceHealth gRPC API 版本，用于测试 kubelet 面对仅支持单一版本的插件时的行为。
	// 零值保持两者都启用，与 kubeletplugin helper 的默认行为一致。
	DisableHealthV1       bool
	DisableHealthV1alpha1 bool
}

type DeviceHealthUpdate struct {
	PoolName   string
	DeviceName string
	Health     string
	Message    string
}

type deviceHealthInfo struct {
	status  string
	message string
}

type ExamplePlugin struct {
	stopCh         <-chan struct{}
	logger         klog.Logger
	resourceClient cgoresource.ResourceV1Interface
	d              *kubeletplugin.Helper
	fileOps        FileOperations

	cdiDir     string
	driverName string
	nodeName   string

	// 需要互斥锁是因为其他 goroutine 也会检查这些状态。
	// 仅在 gRPC server 中串行化并不够，写入仍可能与读取竞争。
	mutex     sync.Mutex
	prepared  map[ClaimID][]kubeletplugin.Device // prepared claims -> result of nodePrepareResource
	gRPCCalls []GRPCCall

	healthMutex       sync.Mutex
	deviceHealth      map[string]deviceHealthInfo
	HealthControlChan chan DeviceHealthUpdate

	blockPrepareResourcesMutex   sync.Mutex
	blockUnprepareResourcesMutex sync.Mutex

	prepareResourcesFailure   error
	failPrepareResourcesMutex sync.Mutex

	unprepareResourcesFailure   error
	failUnprepareResourcesMutex sync.Mutex

	// cancelMainContext 用于取消上层 context。
	// 如果已设置，HandleError 会调用它。
	cancelMainContext context.CancelCauseFunc
}

var _ kubeletplugin.DRAPlugin = &ExamplePlugin{}

type GRPCCall struct {
	// FullMethod 是完全限定的方法名，例如 /package.service/method。
	FullMethod string

	// Request 保存调用的参数。
	Request interface{}

	// Response 保存插件的回复。对于进行中的调用为 nil。
	Response interface{}

	// Err 保存插件返回的错误。对于进行中或成功的调用为 nil。
	Err error
}

// ClaimID 同时包含 claim 名称和 UID，便于调试。
// 不包含 namespace，因为 E2E 测试中它是随机的，而 UID 足以确保 ClaimID 唯一。
type ClaimID struct {
	Name string
	UID  types.UID
}

type Device struct {
	PoolName    string
	DeviceName  string
	RequestName string
	CDIDeviceID string
}

var _ kubeletplugin.DRAPlugin = &ExamplePlugin{}

// getJSONFilePath 返回 CDI 文件当前或应当所在的绝对路径。
func (ex *ExamplePlugin) getJSONFilePath(claimUID types.UID, requestName string) string {
	baseRequestRef := resourceclaim.BaseRequestRef(requestName)
	return filepath.Join(ex.cdiDir, fmt.Sprintf("%s-%s-%s.json", ex.driverName, claimUID, baseRequestRef))
}

// FileOperations 定义处理 CDI 文件及其他配置的可选回调。
type FileOperations struct {
	// Create 必须覆盖已有文件。
	Create func(name string, content []byte) error

	// Remove 必须删除文件。文件不存在时不应返回错误。
	Remove func(name string) error

	// HandleError 是处理 ResourceSlice 发布问题的可选回调。
	HandleError func(ctx context.Context, err error, msg string)

	// DriverResources 提供 driver 构造并发布 ResourceSlices 所需的信息。
	DriverResources *resourceslice.DriverResources
}

// StartPlugin 启动 DRA kubelet plugin 所需的各个 server。
func StartPlugin(ctx context.Context, cdiDir, driverName string, kubeClient kubernetes.Interface, nodeName string, fileOps FileOperations, opts ...any) (*ExamplePlugin, error) {
	logger := klog.FromContext(ctx)

	if fileOps.Create == nil {
		fileOps.Create = func(name string, content []byte) error {
			return os.WriteFile(name, content, os.FileMode(0644))
		}
	}
	if fileOps.Remove == nil {
		fileOps.Remove = func(name string) error {
			if err := os.Remove(name); err != nil && !os.IsNotExist(err) {
				return err
			}
			return nil
		}
	}

	publicOpts := []kubeletplugin.Option{
		kubeletplugin.DriverName(driverName),
		kubeletplugin.NodeName(nodeName),
		kubeletplugin.KubeClient(kubeClient),
	}

	testOpts := &options{}
	pluginOpts := &Options{}
	for _, opt := range opts {
		switch typedOpt := opt.(type) {
		case Options:
			*pluginOpts = typedOpt
		case TestOption:
			if err := typedOpt(testOpts); err != nil {
				return nil, fmt.Errorf("apply test option: %w", err)
			}
		case kubeletplugin.Option:
			publicOpts = append(publicOpts, typedOpt)
		default:
			return nil, fmt.Errorf("unexpected option type %T", opt)
		}
	}

	ex := &ExamplePlugin{
		stopCh:            ctx.Done(),
		logger:            logger,
		resourceClient:    draclient.New(kubeClient),
		fileOps:           fileOps,
		cdiDir:            cdiDir,
		driverName:        driverName,
		nodeName:          nodeName,
		prepared:          make(map[ClaimID][]kubeletplugin.Device),
		cancelMainContext: testOpts.cancelMainContext,
		deviceHealth:      make(map[string]deviceHealthInfo),
		HealthControlChan: make(chan DeviceHealthUpdate, 10),
	}

	publicOpts = append(publicOpts,
		kubeletplugin.GRPCInterceptor(ex.recordGRPCCall),
		kubeletplugin.GRPCStreamInterceptor(ex.recordGRPCStream),
		// ExamplePlugin 始终实现 WatchHealthStatus；是否真正对外声明并提供设备健康状态，
		// 以及支持哪些 API 版本，由这里控制。
		kubeletplugin.HealthService(pluginOpts.EnableHealthService),
		kubeletplugin.HealthV1(!pluginOpts.DisableHealthV1),
		kubeletplugin.HealthV1alpha1(!pluginOpts.DisableHealthV1alpha1),
	)
	d, err := kubeletplugin.Start(ctx, ex, publicOpts...)
	if err != nil {
		return nil, fmt.Errorf("start kubelet plugin: %w", err)
	}
	ex.d = d

	if fileOps.DriverResources != nil {
		if err := ex.d.PublishResources(ctx, *fileOps.DriverResources); err != nil {
			return nil, fmt.Errorf("start kubelet plugin: publish resources: %w", err)
		}
	}

	return ex, nil
}

// PublishResources 重新发布 driver 的资源，替换此前发布的集合。
// 这会重置 resourceslice controller 的期望状态：如果 apiserver 之前丢弃了某些字段
// （例如因为某个 feature gate 被禁用），controller 会将期望状态锁定为实际能存储的内容，
// 以避免陷入高频更新循环。当 apiserver 再次接受原始资源时，重新发布即可恢复它们。
func (ex *ExamplePlugin) PublishResources(ctx context.Context, resources resourceslice.DriverResources) error {
	return ex.d.PublishResources(ctx, resources)
}

// Stop 确保所有 server 都已停止并释放资源。
func (ex *ExamplePlugin) Stop() {
	ex.d.Stop()
}

func (ex *ExamplePlugin) IsRegistered() bool {
	status := ex.d.RegistrationStatus()
	if status == nil {
		return false
	}
	return status.PluginRegistered
}

func (ex *ExamplePlugin) HandleError(ctx context.Context, err error, msg string) {
	if ex.fileOps.HandleError != nil {
		ex.fileOps.HandleError(ctx, err, msg)
		return
	}
	utilruntime.HandleErrorWithContext(ctx, err, msg)
	if ex.cancelMainContext != nil {
		ex.cancelMainContext(err)
	}
}

// BlockNodePrepareResources 锁定 blockPrepareResourcesMutex，并返回用于解锁的函数
func (ex *ExamplePlugin) BlockNodePrepareResources() func() {
	ex.blockPrepareResourcesMutex.Lock()
	return func() {
		ex.blockPrepareResourcesMutex.Unlock()
	}
}

// BlockNodeUnprepareResources 锁定 blockUnprepareResourcesMutex，并返回用于解锁的函数
func (ex *ExamplePlugin) BlockNodeUnprepareResources() func() {
	ex.blockUnprepareResourcesMutex.Lock()
	return func() {
		ex.blockUnprepareResourcesMutex.Unlock()
	}
}

// SetNodePrepareResourcesFailureMode 为 NodePrepareResources 调用设置失败模式，
// 并返回用于清除该失败模式的函数
func (ex *ExamplePlugin) SetNodePrepareResourcesFailureMode() func() {
	ex.failPrepareResourcesMutex.Lock()
	ex.prepareResourcesFailure = errors.New("simulated PrepareResources failure")
	ex.failPrepareResourcesMutex.Unlock()

	return func() {
		ex.failPrepareResourcesMutex.Lock()
		ex.prepareResourcesFailure = nil
		ex.failPrepareResourcesMutex.Unlock()
	}
}

func (ex *ExamplePlugin) getPrepareResourcesFailure() error {
	ex.failPrepareResourcesMutex.Lock()
	defer ex.failPrepareResourcesMutex.Unlock()
	return ex.prepareResourcesFailure
}

// SetNodeUnprepareResourcesFailureMode 为 NodeUnprepareResources 调用设置失败模式，
// 并返回用于清除该失败模式的函数
func (ex *ExamplePlugin) SetNodeUnprepareResourcesFailureMode() func() {
	ex.failUnprepareResourcesMutex.Lock()
	ex.unprepareResourcesFailure = errors.New("simulated UnprepareResources failure")
	ex.failUnprepareResourcesMutex.Unlock()

	return func() {
		ex.failUnprepareResourcesMutex.Lock()
		ex.unprepareResourcesFailure = nil
		ex.failUnprepareResourcesMutex.Unlock()
	}
}

func (ex *ExamplePlugin) getUnprepareResourcesFailure() error {
	ex.failUnprepareResourcesMutex.Lock()
	defer ex.failUnprepareResourcesMutex.Unlock()
	return ex.unprepareResourcesFailure
}

// NodePrepareResource 确保该 claim 的 CDI 文件（每个 request 一个）存在。
// 它使用确定性的文件名，既简化了 NodeUnprepareResource（无需记住或查找文件名），
// 也保证了幂等性（再次调用时只需重写文件）。
func (ex *ExamplePlugin) nodePrepareResource(ctx context.Context, claim *resourceapi.ResourceClaim) ([]kubeletplugin.Device, error) {
	logger := klog.FromContext(ctx)

	ex.mutex.Lock()
	defer ex.mutex.Unlock()
	ex.blockPrepareResourcesMutex.Lock()
	defer ex.blockPrepareResourcesMutex.Unlock()

	claimID := ClaimID{Name: claim.Name, UID: claim.UID}
	if result, ok := ex.prepared[claimID]; ok {
		// 幂等调用，无需处理。
		return result, nil
	}

	var devices []kubeletplugin.Device
	for _, result := range claim.Status.Allocation.Devices.Results {
		// 只处理分配给当前 driver 的结果。
		if ex.driverName != result.Driver {
			continue
		}

		baseRequestName := resourceclaim.BaseRequestRef(result.Request)

		// driver 按变量在 results 中出现的顺序合并所有环境变量（后者覆盖前者）。
		configs := resourceclaim.ConfigForResult(claim.Status.Allocation.Devices.Config, result)
		env := make(map[string]string)
		for i, config := range configs {
			// 只使用属于当前 driver 的配置。
			if config.Opaque.Driver != ex.driverName {
				continue
			}
			if err := extractParameters(config.Opaque.Parameters, &env, config.Source == resourceapi.AllocationConfigSourceClass); err != nil {
				return nil, fmt.Errorf("parameters in config #%d: %w", i, err)
			}
		}

		// 同时设置 claim_<claim name>_<request name>=true 环境变量，
		// 用于识别哪些设备被映射进了容器。
		claimReqName := "claim_" + claim.Name + "_" + baseRequestName
		claimReqName = regexp.MustCompile(`[^a-zA-Z0-9]`).ReplaceAllString(claimReqName, "_")
		env[claimReqName] = "true"

		deviceName := "claim-" + string(claim.UID) + "-" + baseRequestName
		vendor := ex.driverName
		class := "test"
		cdiDeviceID := vendor + "/" + class + "=" + deviceName

		// CDI 要求环境变量以字符串集合的形式提供。
		envs := []string{}
		for key, val := range env {
			envs = append(envs, key+"="+val)
		}
		sort.Strings(envs)

		if len(envs) == 0 {
			// CDI 不支持空的 ContainerEdits。例如 kubelet+crio 会报：
			//    CDI device injection failed: unresolvable CDI devices ...
			//
			// 此时不注入任何内容，DRA 支持这种做法。
			continue
		}

		spec := &cdi.Spec{
			Kind: vendor + "/" + class,
			Devices: []cdi.Device{
				{
					Name: deviceName,
					ContainerEdits: cdi.ContainerEdits{
						Env: envs,
					},
				},
			},
		}
		minVersion, err := cdi.MinimumRequiredVersion(spec)
		if err != nil {
			return nil, fmt.Errorf("determine CDI spec version: %w", err)
		}
		spec.Version = minVersion
		filePath := ex.getJSONFilePath(claim.UID, baseRequestName)
		buffer, err := json.Marshal(spec)
		if err != nil {
			return nil, fmt.Errorf("marshal spec: %w", err)
		}
		if err := ex.fileOps.Create(filePath, buffer); err != nil {
			return nil, fmt.Errorf("failed to write CDI file: %w", err)
		}
		device := kubeletplugin.Device{
			PoolName:     result.Pool,
			DeviceName:   result.Device,
			ShareID:      result.ShareID,
			Requests:     []string{result.Request}, // 这里也可以返回 baseRequestName。
			CDIDeviceIDs: []string{cdiDeviceID},
			Metadata: &kubeletplugin.DeviceMetadata{
				Attributes: map[string]resourceapi.DeviceAttribute{
					"driverName": {StringValue: &ex.driverName},
					"pool":       {StringValue: &result.Pool},
					"device":     {StringValue: &result.Device},
				},
			},
		}
		devices = append(devices, device)
	}

	logger.V(3).Info("CDI file(s) created", "devices", devices)
	ex.prepared[claimID] = devices
	return devices, nil
}

func extractParameters(parameters runtime.RawExtension, env *map[string]string, admin bool) error {
	if len(parameters.Raw) == 0 {
		return nil
	}
	kind := "user"
	if admin {
		kind = "admin"
	}
	var data map[string]string
	if err := json.Unmarshal(parameters.Raw, &data); err != nil {
		return fmt.Errorf("decoding %s parameters: %w", kind, err)
	}
	if len(data) > 0 && *env == nil {
		*env = make(map[string]string)
	}
	for key, value := range data {
		(*env)[kind+"_"+key] = value
	}
	return nil
}

func (ex *ExamplePlugin) PrepareResourceClaims(ctx context.Context, claims []*resourceapi.ResourceClaim) (map[types.UID]kubeletplugin.PrepareResult, error) {
	if failure := ex.getPrepareResourcesFailure(); failure != nil {
		return nil, failure
	}

	result := make(map[types.UID]kubeletplugin.PrepareResult)
	for _, claim := range claims {
		devices, err := ex.nodePrepareResource(ctx, claim)
		var claimResult kubeletplugin.PrepareResult
		if err != nil {
			claimResult.Err = err
		} else {
			claimResult.Devices = devices
		}
		result[claim.UID] = claimResult
	}
	return result, nil
}

// NodeUnprepareResource 删除 NodePrepareResource 创建的 CDI 文件。
// 它是幂等的，因此文件已被删除时不会报错。
func (ex *ExamplePlugin) nodeUnprepareResource(ctx context.Context, claimRef kubeletplugin.NamespacedObject) error {
	ex.blockUnprepareResourcesMutex.Lock()
	defer ex.blockUnprepareResourcesMutex.Unlock()

	logger := klog.FromContext(ctx)

	ex.mutex.Lock()
	defer ex.mutex.Unlock()
	claimID := ClaimID{Name: claimRef.Name, UID: claimRef.UID}
	devices, ok := ex.prepared[claimID]
	if !ok {
		// 幂等调用，无需处理。
		return nil
	}

	for _, device := range devices {
		// 实际上通常只 prepare 一个，但这里不做此假设。
		for _, request := range device.Requests {
			filePath := ex.getJSONFilePath(claimRef.UID, request)
			if err := ex.fileOps.Remove(filePath); err != nil {
				return fmt.Errorf("error removing CDI file: %w", err)
			}
			logger.V(3).Info("CDI file removed", "path", filePath)
		}
	}

	delete(ex.prepared, claimID)

	return nil
}

func (ex *ExamplePlugin) UnprepareResourceClaims(ctx context.Context, claims []kubeletplugin.NamespacedObject) (map[types.UID]error, error) {
	result := make(map[types.UID]error)

	if failure := ex.getUnprepareResourcesFailure(); failure != nil {
		return nil, failure
	}

	for _, claimRef := range claims {
		err := ex.nodeUnprepareResource(ctx, claimRef)
		result[claimRef.UID] = err
	}
	return result, nil
}

// UnprepareClaim 在 plugin 上手动 unprepare 单个 resource claim。
// 当测试中 kubelet 有意跳过 unprepare（例如 SkipNodeOperationNodeUnprepareResources）时，
// 需要用它来保证对已 prepare 资源的清理断言能够通过。
func (ex *ExamplePlugin) UnprepareClaim(ctx context.Context, claim *resourceapi.ResourceClaim) error {
	return ex.nodeUnprepareResource(ctx, kubeletplugin.NamespacedObject{
		NamespacedName: types.NamespacedName{Namespace: claim.Namespace, Name: claim.Name},
		UID:            claim.UID,
	})
}

func (ex *ExamplePlugin) GetPreparedResources() []ClaimID {
	ex.mutex.Lock()
	defer ex.mutex.Unlock()
	var prepared []ClaimID
	for claimID := range ex.prepared {
		prepared = append(prepared, claimID)
	}
	return prepared
}

func (ex *ExamplePlugin) recordGRPCCall(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp interface{}, err error) {
	call := GRPCCall{
		FullMethod: info.FullMethod,
		Request:    req,
	}
	ex.mutex.Lock()
	ex.gRPCCalls = append(ex.gRPCCalls, call)
	index := len(ex.gRPCCalls) - 1
	ex.mutex.Unlock()

	// 此处不持有互斥锁，以允许并发调用。
	call.Response, call.Err = handler(ctx, req)

	ex.mutex.Lock()
	ex.gRPCCalls[index] = call
	ex.mutex.Unlock()

	return call.Response, call.Err
}

func (ex *ExamplePlugin) recordGRPCStream(srv interface{}, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) (err error) {
	ex.mutex.Lock()
	// 先追加一个空的 GRPCCall 结构体，以获取其索引。
	ex.gRPCCalls = append(ex.gRPCCalls, GRPCCall{})

	pCall := &ex.gRPCCalls[len(ex.gRPCCalls)-1]

	pCall.FullMethod = info.FullMethod
	ex.mutex.Unlock()

	defer func() {
		ex.mutex.Lock()
		defer ex.mutex.Unlock()
		pCall.Err = err
	}()

	err = handler(srv, stream)
	return err
}

func (ex *ExamplePlugin) GetGRPCCalls() []GRPCCall {
	ex.mutex.Lock()
	defer ex.mutex.Unlock()

	// 必须返回新的 slice，否则新增的调用会对调用方可见。
	// 还要复制其中的元素，因为 recordGRPCCall 会修改它们。
	calls := make([]GRPCCall, 0, len(ex.gRPCCalls))
	calls = append(calls, ex.gRPCCalls...)
	return calls
}

// ResetGRPCCalls 清除内部记录的插件 GRPC 调用。
// 在测试中验证插件注册行为（尤其是注册重试场景）时，可借此从干净状态开始。
func (ex *ExamplePlugin) ResetGRPCCalls() {
	ex.mutex.Lock()
	defer ex.mutex.Unlock()
	ex.gRPCCalls = nil
}

// CountCalls 统计方法名以给定后缀结尾的 GRPC 调用次数。
func (ex *ExamplePlugin) CountCalls(methodSuffix string) int {
	count := 0
	for _, call := range ex.GetGRPCCalls() {
		if strings.HasSuffix(call.FullMethod, methodSuffix) {
			count += 1
		}
	}
	return count
}

func (ex *ExamplePlugin) UpdateStatus(ctx context.Context, resourceClaim *resourceapi.ResourceClaim) (*resourceapi.ResourceClaim, error) {
	return ex.resourceClient.ResourceClaims(resourceClaim.Namespace).UpdateStatus(ctx, resourceClaim, metav1.UpdateOptions{})
}

// SetGetInfoError 设置插件 GetInfo 调用返回的错误。
// 测试中可用它模拟注册失败场景，验证 kubelet plugin manager 在 GetInfo 失败时会重试注册。
//
// 调用 SetGetInfoError(nil) 可恢复 GetInfo 的正常行为。
func (ex *ExamplePlugin) SetGetInfoError(err error) {
	ex.d.SetGetInfoError(err)
}

// SetNotifyRegistrationStatusError 设置插件 NotifyRegistrationStatus 调用返回的错误。
// 测试中可用它模拟注册失败场景，验证 kubelet plugin manager 在 NotifyRegistrationStatus 失败时会重试注册。
//
// 调用 SetNotifyRegistrationStatusError(nil) 可恢复 NotifyRegistrationStatus 的正常行为。
func (ex *ExamplePlugin) SetNotifyRegistrationStatusError(err error) {
	ex.d.SetNotifyRegistrationStatusError(err)
}

// WatchHealthStatus 通过版本无关的 helper API 流式发送设备健康报告，
// 实现 [kubeletplugin.DRAPlugin]。helper 会将报告转换为 kubelet 支持的
// DRAResourceHealth gRPC 版本。
func (ex *ExamplePlugin) WatchHealthStatus(ctx context.Context, reports chan<- kubeletplugin.DeviceHealthReport) error {
	logger := klog.FromContext(ctx)
	logger.V(3).Info("Starting dynamic WatchHealthStatus stream")

	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	// send 构造当前报告并发送，同时响应取消信号。
	// 流结束后返回 false，调用方据此停止。
	send := func() bool {
		report := ex.buildHealthReport()
		logger.V(5).Info("Test driver sending health update", "report", report)
		select {
		case <-ctx.Done():
			return false
		case reports <- report:
			return true
		}
	}

	// 立即发送一次初始更新，汇报预先配置好的设备状态。
	if !send() {
		logger.V(3).Info("WatchHealthStatus stream canceled by kubelet")
		return nil
	}

	for {
		select {
		case <-ctx.Done():
			logger.V(3).Info("WatchHealthStatus stream canceled by kubelet")
			return nil
		case update, ok := <-ex.HealthControlChan:
			if !ok {
				logger.V(3).Info("HealthControlChan closed, exiting WatchHealthStatus stream.")
				return nil
			}
			logger.V(3).Info("Received health update from control channel", "update", update)
			ex.healthMutex.Lock()
			key := update.PoolName + "/" + update.DeviceName
			ex.deviceHealth[key] = deviceHealthInfo{
				status:  update.Health,
				message: update.Message,
			}
			ex.healthMutex.Unlock()

			if !send() {
				return nil
			}
		case <-ticker.C:
			if !send() {
				return nil
			}
		}
	}
}

// buildHealthReport 根据 deviceHealth map 的当前状态动态构造健康报告。
func (ex *ExamplePlugin) buildHealthReport() kubeletplugin.DeviceHealthReport {
	devices := []kubeletplugin.DeviceHealth{}

	ex.healthMutex.Lock()
	for key, healthInfo := range ex.deviceHealth {
		parts := strings.SplitN(key, "/", 2)
		if len(parts) != 2 {
			continue
		}

		var status kubeletplugin.HealthStatus
		switch healthInfo.status {
		case "Healthy":
			status = kubeletplugin.HealthStatusHealthy
		case "Unhealthy":
			status = kubeletplugin.HealthStatusUnhealthy
		default:
			status = kubeletplugin.HealthStatusUnknown
		}

		devices = append(devices, kubeletplugin.DeviceHealth{
			PoolName:    parts[0],
			DeviceName:  parts[1],
			Health:      status,
			LastUpdated: time.Now(),
			Message:     healthInfo.message,
		})
	}
	ex.healthMutex.Unlock()

	// 排序以保证测试中的顺序稳定。
	sort.Slice(devices, func(i, j int) bool {
		if devices[i].PoolName != devices[j].PoolName {
			return devices[i].PoolName < devices[j].PoolName
		}
		return devices[i].DeviceName < devices[j].DeviceName
	})

	return kubeletplugin.DeviceHealthReport{Devices: devices}
}
