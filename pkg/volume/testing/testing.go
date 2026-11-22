/*
Copyright 2014 The Kubernetes Authors.

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
	"fmt"
	"os"
	"path/filepath"
	goruntime "runtime"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"k8s.io/klog/v2"

	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/utils/exec"
	testingexec "k8s.io/utils/exec/testing"
	utilstrings "k8s.io/utils/strings"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/uuid"
	utiltesting "k8s.io/client-go/util/testing"
	"k8s.io/kubernetes/pkg/volume"
	"k8s.io/kubernetes/pkg/volume/util/recyclerclient"
	volumetypes "k8s.io/kubernetes/pkg/volume/util/types"
	"k8s.io/kubernetes/pkg/volume/util/volumepathhandler"
)

const (
	// storage class 中指定的钩子，表示预期 provisioning 失败。
	ExpectProvisionFailureKey = "expect-provision-failure"
	// 将该节点标记为"不确定"：首次 attach 返回超时错误，后续调用成功。
	UncertainAttachNode = "uncertain-attach-node"
	// 在该节点上 detach 操作会持续失败。
	FailDetachNode = "fail-detach-node"
	// 将该节点标记为"超时"：attach 总返回超时错误，但操作实际已成功。
	TimeoutAttachNode = "timeout-attach-node"
	// 将该节点标记为"多方挂载"，即允许卷同时连接到多个节点。
	MultiAttachNode = "multi-attach-node"
	// 该卷名使 Setup 调用超时，但卷实际会完成挂载。
	TimeoutOnSetupVolumeName = "timeout-setup-volume"
	// 该卷名使 Setup 调用失败。
	FailOnSetupVolumeName = "fail-setup-volume"
	// 该卷名使 Setup 先超时、后失败。
	TimeoutAndFailOnSetupVolumeName = "timeout-and-fail-setup-volume"
	// 该卷名使首次挂载成功，后续尝试超时。
	SuccessAndTimeoutSetupVolumeName = "success-and-timeout-setup-volume-name"
	// 该卷名使首次挂载成功，后续尝试失败。
	SuccessAndFailOnSetupVolumeName = "success-and-failed-setup-device-name"

	// 该卷名使 MountDevice 调用超时，但 Setup 会完成。
	TimeoutOnMountDeviceVolumeName = "timeout-mount-device-volume"
	// 该卷名使首次 MountDevice 调用超时、第二次失败。
	TimeoutAndFailOnMountDeviceVolumeName = "timeout-and-fail-mount-device-name"
	// 该卷名使 MountDevice 操作失败。
	FailMountDeviceVolumeName = "fail-mount-device-volume-name"
	// 该卷名使首次挂载成功，后续尝试超时。
	SuccessAndTimeoutDeviceName = "success-and-timeout-device-name"
	// 该卷名使首次 MountDevice 成功，后续尝试失败。
	SuccessAndFailOnMountDeviceName = "success-and-failed-mount-device-name"

	// 该卷名使 NodeExpandVolume 返回 FailedPrecondition 错误。
	FailWithInUseVolumeName       = "fail-expansion-in-use"
	FailWithUnSupportedVolumeName = "fail-expansion-unsupported"

	FailVolumeExpansion = "fail-expansion-test"

	InfeasibleNodeExpansion      = "infeasible-fail-node-expansion"
	OtherFinalNodeExpansionError = "other-final-node-expansion-error"

	deviceNotMounted     = "deviceNotMounted"
	deviceMountUncertain = "deviceMountUncertain"
	deviceMounted        = "deviceMounted"

	volumeNotMounted     = "volumeNotMounted"
	volumeMountUncertain = "volumeMountUncertain"
	volumeMounted        = "volumeMounted"

	FailNewMounter = "fail-new-mounter"
)

// CommandScript 用于预配置待执行命令，以及可选的合并输出（stdout+stderr）与返回码。
type CommandScript struct {
	// Cmd 为待执行命令，如 "ls"。
	Cmd string
	// Args 为传给命令的参数列表，如 "-a"。
	Args []string
	// Output 为命令返回的 stdout 与 stderr 合并输出。
	Output string
	// ReturnCode 为命令退出码；非零值会让命令返回带该退出码的错误。
	ReturnCode int
}

// ScriptCommands 为 FakeExec 配置一组预期命令；实际调用次数超过脚本数量会触发 panic。
// 默认不校验命令参数与顺序：若脚本指定了 Output，首次命令调用即返回该输出，即使调用的是另一条命令。
// 这主要用于断言命令调用次数。若需校验具体命令与参数，请将 fe.ExectOrder 设为 true。
func ScriptCommands(fe *testingexec.FakeExec, scripts []CommandScript) {
	fe.DisableScripts = false
	for _, script := range scripts {
		fakeCmd := &testingexec.FakeCmd{}
		cmdAction := makeFakeCmd(fakeCmd, script.Cmd, script.Args...)
		outputAction := makeFakeOutput(script.Output, script.ReturnCode)
		fakeCmd.CombinedOutputScript = append(fakeCmd.CombinedOutputScript, outputAction)
		fe.CommandScript = append(fe.CommandScript, cmdAction)
	}
}

// makeFakeCmd 返回一个 FakeCommandAction，用给定的命令与参数初始化 FakeCmd。
func makeFakeCmd(fakeCmd *testingexec.FakeCmd, cmd string, args ...string) testingexec.FakeCommandAction {
	fc := fakeCmd
	c := cmd
	a := args
	return func(cmd string, args ...string) exec.Cmd {
		command := testingexec.InitFakeCmd(fc, c, a...)
		return command
	}
}

// makeFakeOutput 返回一个 FakeAction，它输出指定内容并可模拟非零退出码。
func makeFakeOutput(output string, rc int) testingexec.FakeAction {
	o := output
	var e error
	if rc != 0 {
		e = testingexec.FakeExitError{Status: rc}
	}
	return func() ([]byte, []byte, error) {
		return []byte(o), nil, e
	}
}

// ProbeVolumePlugins 返回测试用的 FakeVolumePlugin 列表。
func ProbeVolumePlugins(config volume.VolumeConfig) []volume.VolumePlugin {
	if _, ok := config.OtherAttributes["fake-property"]; ok {
		return []volume.VolumePlugin{
			&FakeVolumePlugin{
				PluginName: "fake-plugin",
				Host:       nil,
				// SomeFakeProperty: config.OtherAttributes["fake-property"] -- 字符串，可能需要插件自行解析。
			},
		}
	}
	return []volume.VolumePlugin{&FakeVolumePlugin{PluginName: "fake-plugin"}}
}

// FakeVolumePlugin 用于测试，尽量实现完整的插件接口，但实际只创建空目录。
// 用法：
//
//	volume.RegisterPlugin(&FakePlugin{"fake-name"})
type FakeVolumePlugin struct {
	sync.RWMutex
	PluginName             string
	Host                   volume.VolumeHost
	Config                 volume.VolumeConfig
	LastProvisionerOptions volume.VolumeOptions
	LastResizeOptions      volume.NodeResizeOptions
	NewAttacherCallCount   int
	NewDetacherCallCount   int
	NodeExpandCallCount    int
	VolumeLimits           map[string]int64
	VolumeLimitsError      error
	LimitKey               string
	ProvisionDelaySeconds  int
	SupportsRemount        bool
	SupportsSELinux        bool
	DisableNodeExpansion   bool
	CanSupportFn           func(*volume.Spec) bool
	VerifyExhaustedEnabled bool

	// 默认 false，即默认可 attach。
	NonAttachable bool

	// 按需注入回调。
	WaitForAttachHook func(spec *volume.Spec, devicePath string, pod *v1.Pod, spectimeout time.Duration) (string, error)
	UnmountDeviceHook func(globalMountPath string) error
	SetUpHook         func(plugin volume.VolumePlugin, mounterArgs volume.MounterArgs) error

	// 向 NewUnmounter 注入错误。
	NewUnmounterError error

	Mounters             []*FakeVolume
	Unmounters           []*FakeVolume
	Attachers            []*FakeVolume
	Detachers            []*FakeVolume
	BlockVolumeMappers   []*FakeVolume
	BlockVolumeUnmappers []*FakeVolume
}

var _ volume.VolumePlugin = &FakeVolumePlugin{}
var _ volume.BlockVolumePlugin = &FakeVolumePlugin{}
var _ volume.RecyclableVolumePlugin = &FakeVolumePlugin{}
var _ volume.DeletableVolumePlugin = &FakeVolumePlugin{}
var _ volume.ProvisionableVolumePlugin = &FakeVolumePlugin{}
var _ volume.AttachableVolumePlugin = &FakeVolumePlugin{}
var _ volume.DeviceMountableVolumePlugin = &FakeVolumePlugin{}
var _ volume.NodeExpandableVolumePlugin = &FakeVolumePlugin{}

func (plugin *FakeVolumePlugin) getFakeVolume(list *[]*FakeVolume) *FakeVolume {
	if list != nil {
		volumeList := *list
		if len(volumeList) > 0 {
			volume := volumeList[0]
			volume.Lock()
			defer volume.Unlock()
			volume.WaitForAttachHook = plugin.WaitForAttachHook
			volume.UnmountDeviceHook = plugin.UnmountDeviceHook
			volume.SetUpHook = plugin.SetUpHook
			return volume
		}
	}
	volume := &FakeVolume{
		WaitForAttachHook: plugin.WaitForAttachHook,
		UnmountDeviceHook: plugin.UnmountDeviceHook,
		SetUpHook:         plugin.SetUpHook,
	}
	volume.VolumesAttached = make(map[string]sets.Set[string])
	volume.DeviceMountState = make(map[string]string)
	volume.VolumeMountState = make(map[string]string)
	if list != nil {
		*list = append(*list, volume)
	}
	return volume
}

// Init 初始化插件，记录 VolumeHost。
func (plugin *FakeVolumePlugin) Init(host volume.VolumeHost) error {
	plugin.Lock()
	defer plugin.Unlock()
	plugin.Host = host
	return nil
}

// GetPluginName 返回插件名称。
func (plugin *FakeVolumePlugin) GetPluginName() string {
	plugin.RLock()
	defer plugin.RUnlock()
	return plugin.PluginName
}

// GetVolumeName 从卷规格中提取卷名，取不到时回退到 spec.Name()。
func (plugin *FakeVolumePlugin) GetVolumeName(spec *volume.Spec) (string, error) {
	var volumeName string
	if spec.Volume != nil && spec.Volume.GCEPersistentDisk != nil {
		volumeName = spec.Volume.GCEPersistentDisk.PDName
	} else if spec.Volume != nil && spec.Volume.RBD != nil {
		volumeName = spec.Volume.RBD.RBDImage
	} else if spec.PersistentVolume != nil &&
		spec.PersistentVolume.Spec.GCEPersistentDisk != nil {
		volumeName = spec.PersistentVolume.Spec.GCEPersistentDisk.PDName
	} else if spec.PersistentVolume != nil &&
		spec.PersistentVolume.Spec.RBD != nil {
		volumeName = spec.PersistentVolume.Spec.RBD.RBDImage
	} else if spec.Volume != nil && spec.Volume.CSI != nil {
		volumeName = spec.Volume.CSI.Driver
	}
	if volumeName == "" {
		volumeName = spec.Name()
	}
	return volumeName, nil
}

// CanSupport 判断是否支持给定卷规格，默认全部支持。
func (plugin *FakeVolumePlugin) CanSupport(spec *volume.Spec) bool {
	if plugin.CanSupportFn != nil {
		return plugin.CanSupportFn(spec)
	}

	return true
}

// RequiresRemount 返回是否需要重挂载。
func (plugin *FakeVolumePlugin) RequiresRemount(spec *volume.Spec) bool {
	return plugin.SupportsRemount
}

// SupportsMountOption 返回是否支持挂载选项。
func (plugin *FakeVolumePlugin) SupportsMountOption() bool {
	return true
}

// SupportsSELinuxContextMount 返回是否支持 SELinux 上下文挂载。
func (plugin *FakeVolumePlugin) SupportsSELinuxContextMount(spec *volume.Spec) (bool, error) {
	return plugin.SupportsSELinux, nil
}

// NewMounter 创建并返回一个 FakeVolume mounter。
func (plugin *FakeVolumePlugin) NewMounter(spec *volume.Spec, pod *v1.Pod) (volume.Mounter, error) {
	plugin.Lock()
	defer plugin.Unlock()
	if spec.Name() == FailNewMounter {
		return nil, fmt.Errorf("AlwaysFailNewMounter")
	}
	fakeVolume := plugin.getFakeVolume(&plugin.Mounters)
	fakeVolume.Lock()
	defer fakeVolume.Unlock()
	fakeVolume.PodUID = pod.UID
	fakeVolume.VolName = spec.Name()
	fakeVolume.Plugin = plugin
	fakeVolume.MetricsNil = volume.MetricsNil{}
	return fakeVolume, nil
}

// GetMounters 返回所有已创建的 mounter。
func (plugin *FakeVolumePlugin) GetMounters() (Mounters []*FakeVolume) {
	plugin.RLock()
	defer plugin.RUnlock()
	return plugin.Mounters
}

// NewUnmounter 创建并返回一个 FakeVolume unmounter。
func (plugin *FakeVolumePlugin) NewUnmounter(volName string, podUID types.UID) (volume.Unmounter, error) {
	plugin.Lock()
	defer plugin.Unlock()
	if plugin.NewUnmounterError != nil {
		return nil, plugin.NewUnmounterError
	}
	fakeVolume := plugin.getFakeVolume(&plugin.Unmounters)
	fakeVolume.Lock()
	defer fakeVolume.Unlock()
	fakeVolume.PodUID = podUID
	fakeVolume.VolName = volName
	fakeVolume.Plugin = plugin
	fakeVolume.MetricsNil = volume.MetricsNil{}
	return fakeVolume, nil
}

// GetUnmounters 返回所有已创建的 unmounter。
func (plugin *FakeVolumePlugin) GetUnmounters() (Unmounters []*FakeVolume) {
	plugin.RLock()
	defer plugin.RUnlock()
	return plugin.Unmounters
}

// 块卷支持。
func (plugin *FakeVolumePlugin) NewBlockVolumeMapper(spec *volume.Spec, pod *v1.Pod) (volume.BlockVolumeMapper, error) {
	plugin.Lock()
	defer plugin.Unlock()
	volume := plugin.getFakeVolume(&plugin.BlockVolumeMappers)
	volume.Lock()
	defer volume.Unlock()
	if pod != nil {
		volume.PodUID = pod.UID
	}
	volume.VolName = spec.Name()
	volume.Plugin = plugin
	return volume, nil
}

// 块卷支持。
func (plugin *FakeVolumePlugin) GetBlockVolumeMapper() (BlockVolumeMappers []*FakeVolume) {
	plugin.RLock()
	defer plugin.RUnlock()
	return plugin.BlockVolumeMappers
}

// 块卷支持。
func (plugin *FakeVolumePlugin) NewBlockVolumeUnmapper(volName string, podUID types.UID) (volume.BlockVolumeUnmapper, error) {
	plugin.Lock()
	defer plugin.Unlock()
	volume := plugin.getFakeVolume(&plugin.BlockVolumeUnmappers)
	volume.Lock()
	defer volume.Unlock()
	volume.PodUID = podUID
	volume.VolName = volName
	volume.Plugin = plugin
	return volume, nil
}

// 块卷支持。
func (plugin *FakeVolumePlugin) GetBlockVolumeUnmapper() (BlockVolumeUnmappers []*FakeVolume) {
	plugin.RLock()
	defer plugin.RUnlock()
	return plugin.BlockVolumeUnmappers
}

// NewAttacher 创建 attacher，并累加调用计数。
func (plugin *FakeVolumePlugin) NewAttacher() (volume.Attacher, error) {
	plugin.Lock()
	defer plugin.Unlock()
	plugin.NewAttacherCallCount = plugin.NewAttacherCallCount + 1
	return plugin.getFakeVolume(&plugin.Attachers), nil
}

// NewDeviceMounter 复用 NewAttacher 构造设备挂载器。
func (plugin *FakeVolumePlugin) NewDeviceMounter() (volume.DeviceMounter, error) {
	return plugin.NewAttacher()
}

// GetAttachers 返回所有已创建的 attacher。
func (plugin *FakeVolumePlugin) GetAttachers() (Attachers []*FakeVolume) {
	plugin.RLock()
	defer plugin.RUnlock()
	return plugin.Attachers
}

// GetNewAttacherCallCount 返回 NewAttacher 的调用次数。
func (plugin *FakeVolumePlugin) GetNewAttacherCallCount() int {
	plugin.RLock()
	defer plugin.RUnlock()
	return plugin.NewAttacherCallCount
}

// NewDetacher 创建 detacher，并使其与首个 attacher 共享连接状态。
func (plugin *FakeVolumePlugin) NewDetacher() (volume.Detacher, error) {
	plugin.Lock()
	defer plugin.Unlock()
	plugin.NewDetacherCallCount = plugin.NewDetacherCallCount + 1
	detacher := plugin.getFakeVolume(&plugin.Detachers)
	attacherList := plugin.Attachers
	if len(attacherList) > 0 {
		detacherList := plugin.Detachers
		if len(detacherList) > 0 {
			detacherList[0].VolumesAttached = attacherList[0].VolumesAttached
		}

	}
	return detacher, nil
}

// NewDeviceUnmounter 复用 NewDetacher 构造设备卸载器。
func (plugin *FakeVolumePlugin) NewDeviceUnmounter() (volume.DeviceUnmounter, error) {
	return plugin.NewDetacher()
}

// GetDetachers 返回所有已创建的 detacher。
func (plugin *FakeVolumePlugin) GetDetachers() (Detachers []*FakeVolume) {
	plugin.RLock()
	defer plugin.RUnlock()
	return plugin.Detachers
}

// GetNewDetacherCallCount 返回 NewDetacher 的调用次数。
func (plugin *FakeVolumePlugin) GetNewDetacherCallCount() int {
	plugin.RLock()
	defer plugin.RUnlock()
	return plugin.NewDetacherCallCount
}

// CanAttach 返回卷是否可 attach，由 NonAttachable 控制。
func (plugin *FakeVolumePlugin) CanAttach(spec *volume.Spec) (bool, error) {
	return !plugin.NonAttachable, nil
}

// VerifyExhaustedResource 返回是否启用资源耗尽校验。
func (plugin *FakeVolumePlugin) VerifyExhaustedResource(spec *volume.Spec) bool {
	return plugin.VerifyExhaustedEnabled
}

// CanDeviceMount 表示始终可进行设备挂载。
func (plugin *FakeVolumePlugin) CanDeviceMount(spec *volume.Spec) (bool, error) {
	return true, nil
}

// Recycle 回收卷；测试中为空实现。
func (plugin *FakeVolumePlugin) Recycle(pvName string, spec *volume.Spec, eventRecorder recyclerclient.RecycleEventRecorder) error {
	return nil
}

// NewDeleter 创建一个 FakeDeleter。
func (plugin *FakeVolumePlugin) NewDeleter(logger klog.Logger, spec *volume.Spec) (volume.Deleter, error) {
	return &FakeDeleter{"/attributesTransferredFromSpec", volume.MetricsNil{}}, nil
}

// NewProvisioner 创建 FakeProvisioner，并记录传入的卷选项。
func (plugin *FakeVolumePlugin) NewProvisioner(logger klog.Logger, options volume.VolumeOptions) (volume.Provisioner, error) {
	plugin.Lock()
	defer plugin.Unlock()
	plugin.LastProvisionerOptions = options
	return &FakeProvisioner{options, plugin.Host, plugin.ProvisionDelaySeconds}, nil
}

// GetAccessModes 返回支持的访问模式；测试中为空列表。
func (plugin *FakeVolumePlugin) GetAccessModes() []v1.PersistentVolumeAccessMode {
	return []v1.PersistentVolumeAccessMode{}
}

// ConstructVolumeSpec 根据卷名与挂载路径重建卷规格。
func (plugin *FakeVolumePlugin) ConstructVolumeSpec(volumeName, mountPath string) (volume.ReconstructedVolume, error) {
	return volume.ReconstructedVolume{
		Spec: &volume.Spec{
			Volume: &v1.Volume{
				Name: volumeName,
			},
		},
	}, nil
}

// 块卷支持。
func (plugin *FakeVolumePlugin) ConstructBlockVolumeSpec(podUID types.UID, volumeName, mountPath string) (*volume.Spec, error) {
	return &volume.Spec{
		Volume: &v1.Volume{
			Name: volumeName,
		},
	}, nil
}

// GetDeviceMountRefs 返回设备挂载点的引用列表；测试中为空。
func (plugin *FakeVolumePlugin) GetDeviceMountRefs(deviceMountPath string) ([]string, error) {
	return []string{}, nil
}

// 可扩容卷支持。
func (plugin *FakeVolumePlugin) ExpandVolumeDevice(spec *volume.Spec, newSize resource.Quantity, oldSize resource.Quantity) (resource.Quantity, error) {
	return resource.Quantity{}, nil
}

// RequiresFSResize 返回是否需要文件系统扩容。
func (plugin *FakeVolumePlugin) RequiresFSResize() bool {
	return !plugin.DisableNodeExpansion
}

// NodeExpand 模拟节点扩容，按卷名注入各类错误或成功结果。
func (plugin *FakeVolumePlugin) NodeExpand(resizeOptions volume.NodeResizeOptions) (bool, error) {
	plugin.NodeExpandCallCount++
	plugin.LastResizeOptions = resizeOptions
	if resizeOptions.VolumeSpec.Name() == FailWithInUseVolumeName {
		return false, volumetypes.NewFailedPreconditionError("volume-in-use")
	}
	if resizeOptions.VolumeSpec.Name() == FailWithUnSupportedVolumeName {
		return false, volumetypes.NewOperationNotSupportedError("volume-unsupported")
	}

	if resizeOptions.VolumeSpec.Name() == InfeasibleNodeExpansion {
		return false, volumetypes.NewInfeasibleError("infeasible-expansion")
	}

	if resizeOptions.VolumeSpec.Name() == OtherFinalNodeExpansionError {
		return false, fmt.Errorf("other-final-node-expansion-error")
	}

	if resizeOptions.VolumeSpec.Name() == FailVolumeExpansion {
		return false, fmt.Errorf("fail volume expansion for volume: %s", FailVolumeExpansion)
	}
	return true, nil
}

// GetVolumeLimits 返回预设的卷数量限制及其错误。
func (plugin *FakeVolumePlugin) GetVolumeLimits() (map[string]int64, error) {
	return plugin.VolumeLimits, plugin.VolumeLimitsError
}

// VolumeLimitKey 返回卷限制的键。
func (plugin *FakeVolumePlugin) VolumeLimitKey(spec *volume.Spec) string {
	return plugin.LimitKey
}

// FakeBasicVolumePlugin 包装 FakeVolumePlugin，但仅实现 VolumePlugin 接口，
// 便于测试依赖插件接口的逻辑。
type FakeBasicVolumePlugin struct {
	Plugin FakeVolumePlugin
}

// GetPluginName 返回插件名称。
func (f *FakeBasicVolumePlugin) GetPluginName() string {
	return f.Plugin.GetPluginName()
}

// GetVolumeName 从卷规格中提取卷名，取不到时回退到 spec.Name()。
func (f *FakeBasicVolumePlugin) GetVolumeName(spec *volume.Spec) (string, error) {
	return f.Plugin.GetVolumeName(spec)
}

// CanSupport 通过卷名是否以插件名开头判断是否支持该卷规格，便于在测试中按卷选择插件。
func (f *FakeBasicVolumePlugin) CanSupport(spec *volume.Spec) bool {
	return strings.HasPrefix(spec.Name(), f.GetPluginName())
}

// ConstructVolumeSpec 根据卷名与挂载路径重建卷规格。
func (f *FakeBasicVolumePlugin) ConstructVolumeSpec(ame, mountPath string) (volume.ReconstructedVolume, error) {
	return f.Plugin.ConstructVolumeSpec(ame, mountPath)
}

// Init 初始化插件，记录 VolumeHost。
func (f *FakeBasicVolumePlugin) Init(ost volume.VolumeHost) error {
	return f.Plugin.Init(ost)
}

// NewMounter 创建并返回一个 FakeVolume mounter。
func (f *FakeBasicVolumePlugin) NewMounter(spec *volume.Spec, pod *v1.Pod) (volume.Mounter, error) {
	return f.Plugin.NewMounter(spec, pod)
}

// NewUnmounter 创建并返回一个 FakeVolume unmounter。
func (f *FakeBasicVolumePlugin) NewUnmounter(volName string, podUID types.UID) (volume.Unmounter, error) {
	return f.Plugin.NewUnmounter(volName, podUID)
}

// RequiresRemount 返回是否需要重挂载。
func (f *FakeBasicVolumePlugin) RequiresRemount(spec *volume.Spec) bool {
	return f.Plugin.RequiresRemount(spec)
}

// SupportsSELinuxContextMount 返回是否支持 SELinux 上下文挂载。
func (f *FakeBasicVolumePlugin) SupportsSELinuxContextMount(spec *volume.Spec) (bool, error) {
	return f.Plugin.SupportsSELinuxContextMount(spec)
}

// SupportsMountOption 返回是否支持挂载选项。
func (f *FakeBasicVolumePlugin) SupportsMountOption() bool {
	return f.Plugin.SupportsMountOption()
}

var _ volume.VolumePlugin = &FakeBasicVolumePlugin{}

// FakeDeviceMountableVolumePlugin 基于 FakeBasicVolumePlugin，额外实现设备挂载接口。
type FakeDeviceMountableVolumePlugin struct {
	FakeBasicVolumePlugin
}

// CanDeviceMount 表示始终可进行设备挂载。
func (f *FakeDeviceMountableVolumePlugin) CanDeviceMount(spec *volume.Spec) (bool, error) {
	return true, nil
}

// NewDeviceMounter 复用 NewAttacher 构造设备挂载器。
func (f *FakeDeviceMountableVolumePlugin) NewDeviceMounter() (volume.DeviceMounter, error) {
	return f.Plugin.NewDeviceMounter()
}

// NewDeviceUnmounter 复用 NewDetacher 构造设备卸载器。
func (f *FakeDeviceMountableVolumePlugin) NewDeviceUnmounter() (volume.DeviceUnmounter, error) {
	return f.Plugin.NewDeviceUnmounter()
}

// GetDeviceMountRefs 返回设备挂载点的引用列表；测试中为空。
func (f *FakeDeviceMountableVolumePlugin) GetDeviceMountRefs(deviceMountPath string) ([]string, error) {
	return f.Plugin.GetDeviceMountRefs(deviceMountPath)
}

var _ volume.VolumePlugin = &FakeDeviceMountableVolumePlugin{}
var _ volume.DeviceMountableVolumePlugin = &FakeDeviceMountableVolumePlugin{}

// FakeAttachableVolumePlugin 基于 FakeDeviceMountableVolumePlugin，额外实现 attach 接口。
type FakeAttachableVolumePlugin struct {
	FakeDeviceMountableVolumePlugin
}

// NewAttacher 创建 attacher，并累加调用计数。
func (f *FakeAttachableVolumePlugin) NewAttacher() (volume.Attacher, error) {
	return f.Plugin.NewAttacher()
}

// NewDetacher 创建 detacher，并使其与首个 attacher 共享连接状态。
func (f *FakeAttachableVolumePlugin) NewDetacher() (volume.Detacher, error) {
	return f.Plugin.NewDetacher()
}

// CanAttach 返回卷是否可 attach，由 NonAttachable 控制。
func (f *FakeAttachableVolumePlugin) CanAttach(spec *volume.Spec) (bool, error) {
	return true, nil
}

// VerifyExhaustedResource 返回是否启用资源耗尽校验。
func (f *FakeAttachableVolumePlugin) VerifyExhaustedResource(spec *volume.Spec) bool {
	return false
}

var _ volume.VolumePlugin = &FakeAttachableVolumePlugin{}
var _ volume.AttachableVolumePlugin = &FakeAttachableVolumePlugin{}

// FakeFileVolumePlugin 是仅支持文件卷的最小插件假实现。
type FakeFileVolumePlugin struct {
}

// Init 初始化插件，记录 VolumeHost。
func (plugin *FakeFileVolumePlugin) Init(host volume.VolumeHost) error {
	return nil
}

// GetPluginName 返回插件名称。
func (plugin *FakeFileVolumePlugin) GetPluginName() string {
	return "fake-file-plugin"
}

// GetVolumeName 从卷规格中提取卷名，取不到时回退到 spec.Name()。
func (plugin *FakeFileVolumePlugin) GetVolumeName(spec *volume.Spec) (string, error) {
	return "", nil
}

// CanSupport 判断是否支持给定卷规格，默认全部支持。
func (plugin *FakeFileVolumePlugin) CanSupport(spec *volume.Spec) bool {
	return true
}

// RequiresRemount 返回是否需要重挂载。
func (plugin *FakeFileVolumePlugin) RequiresRemount(spec *volume.Spec) bool {
	return false
}

// SupportsMountOption 返回是否支持挂载选项。
func (plugin *FakeFileVolumePlugin) SupportsMountOption() bool {
	return false
}

// SupportsSELinuxContextMount 返回是否支持 SELinux 上下文挂载。
func (plugin *FakeFileVolumePlugin) SupportsSELinuxContextMount(spec *volume.Spec) (bool, error) {
	return false, nil
}

// NewMounter 创建并返回一个 FakeVolume mounter。
func (plugin *FakeFileVolumePlugin) NewMounter(spec *volume.Spec, podRef *v1.Pod) (volume.Mounter, error) {
	return nil, nil
}

// NewUnmounter 创建并返回一个 FakeVolume unmounter。
func (plugin *FakeFileVolumePlugin) NewUnmounter(name string, podUID types.UID) (volume.Unmounter, error) {
	return nil, nil
}

// ConstructVolumeSpec 根据卷名与挂载路径重建卷规格。
func (plugin *FakeFileVolumePlugin) ConstructVolumeSpec(volumeName, mountPath string) (volume.ReconstructedVolume, error) {
	return volume.ReconstructedVolume{}, nil
}

// NewFakeFileVolumePlugin 返回仅支持文件卷的假插件列表。
func NewFakeFileVolumePlugin() []volume.VolumePlugin {
	return []volume.VolumePlugin{&FakeFileVolumePlugin{}}
}

// FakeVolume 模拟卷的挂载、连接及块设备映射行为，并记录各操作的调用次数。
type FakeVolume struct {
	sync.RWMutex
	PodUID  types.UID
	VolName string
	Plugin  *FakeVolumePlugin
	volume.MetricsNil
	VolumesAttached  map[string]sets.Set[string]
	DeviceMountState map[string]string
	VolumeMountState map[string]string

	// 按需注入回调。
	WaitForAttachHook func(spec *volume.Spec, devicePath string, pod *v1.Pod, spectimeout time.Duration) (string, error)
	UnmountDeviceHook func(globalMountPath string) error
	SetUpHook         func(plugin volume.VolumePlugin, mounterArgs volume.MounterArgs) error

	SetUpCallCount              int
	TearDownCallCount           int
	AttachCallCount             int
	DetachCallCount             int
	WaitForAttachCallCount      int
	MountDeviceCallCount        int
	UnmountDeviceCallCount      int
	GetDeviceMountPathCallCount int
	SetUpDeviceCallCount        int
	TearDownDeviceCallCount     int
	MapPodDeviceCallCount       int
	UnmapPodDeviceCallCount     int
	GlobalMapPathCallCount      int
	PodDeviceMapPathCallCount   int
}

func getUniqueVolumeName(spec *volume.Spec) (string, error) {
	var volumeName string
	if spec.Volume != nil && spec.Volume.GCEPersistentDisk != nil {
		volumeName = spec.Volume.GCEPersistentDisk.PDName
	} else if spec.Volume != nil && spec.Volume.RBD != nil {
		volumeName = spec.Volume.RBD.RBDImage
	} else if spec.PersistentVolume != nil &&
		spec.PersistentVolume.Spec.GCEPersistentDisk != nil {
		volumeName = spec.PersistentVolume.Spec.GCEPersistentDisk.PDName
	} else if spec.PersistentVolume != nil &&
		spec.PersistentVolume.Spec.RBD != nil {
		volumeName = spec.PersistentVolume.Spec.RBD.RBDImage
	}
	if volumeName == "" {
		volumeName = spec.Name()
	}
	return volumeName, nil
}

// GetAttributes 返回卷属性：可写、受管且需要 SELinux 重标记。
func (_ *FakeVolume) GetAttributes() volume.Attributes {
	return volume.Attributes{
		ReadOnly:       false,
		Managed:        true,
		SELinuxRelabel: true,
	}
}

// SetUp 执行挂载并记录调用次数，可注入 SetupHook。
func (fv *FakeVolume) SetUp(mounterArgs volume.MounterArgs) error {
	fv.Lock()
	defer fv.Unlock()
	err := fv.setupInternal(mounterArgs)
	fv.SetUpCallCount++
	if fv.SetUpHook != nil {
		return fv.SetUpHook(fv.Plugin, mounterArgs)
	}
	return err
}

func (fv *FakeVolume) setupInternal(mounterArgs volume.MounterArgs) error {
	if fv.VolName == TimeoutOnSetupVolumeName {
		fv.VolumeMountState[fv.VolName] = volumeMountUncertain
		return volumetypes.NewUncertainProgressError("time out on setup")
	}

	if fv.VolName == FailOnSetupVolumeName {
		fv.VolumeMountState[fv.VolName] = volumeNotMounted
		return fmt.Errorf("mounting volume failed")
	}

	if fv.VolName == TimeoutAndFailOnSetupVolumeName {
		_, ok := fv.VolumeMountState[fv.VolName]
		if !ok {
			fv.VolumeMountState[fv.VolName] = volumeMountUncertain
			return volumetypes.NewUncertainProgressError("time out on setup")
		}
		fv.VolumeMountState[fv.VolName] = volumeNotMounted
		return fmt.Errorf("mounting volume failed")

	}

	if fv.VolName == SuccessAndFailOnSetupVolumeName {
		_, ok := fv.VolumeMountState[fv.VolName]
		if ok {
			fv.VolumeMountState[fv.VolName] = volumeNotMounted
			return fmt.Errorf("mounting volume failed")
		}
	}

	if fv.VolName == SuccessAndTimeoutSetupVolumeName {
		_, ok := fv.VolumeMountState[fv.VolName]
		if ok {
			fv.VolumeMountState[fv.VolName] = volumeMountUncertain
			return volumetypes.NewUncertainProgressError("time out on setup")
		}
	}

	fv.VolumeMountState[fv.VolName] = volumeNotMounted
	return fv.SetUpAt(fv.getPath(), mounterArgs)
}

// GetSetUpCallCount 返回 SetUp 的调用次数。
func (fv *FakeVolume) GetSetUpCallCount() int {
	fv.RLock()
	defer fv.RUnlock()
	return fv.SetUpCallCount
}

// SetUpAt 在指定目录创建挂载目录。
func (fv *FakeVolume) SetUpAt(dir string, mounterArgs volume.MounterArgs) error {
	return os.MkdirAll(dir, 0750)
}

// GetPath 返回卷的宿主机路径。
func (fv *FakeVolume) GetPath() string {
	fv.RLock()
	defer fv.RUnlock()
	return fv.getPath()
}

func (fv *FakeVolume) getPath() string {
	return filepath.Join(fv.Plugin.Host.GetPodVolumeDir(fv.PodUID, utilstrings.EscapeQualifiedName(fv.Plugin.PluginName), fv.VolName))
}

// TearDown 执行卸载并记录调用次数。
func (fv *FakeVolume) TearDown() error {
	fv.Lock()
	defer fv.Unlock()
	fv.TearDownCallCount++
	return fv.TearDownAt(fv.getPath())
}

// GetTearDownCallCount 返回 TearDown 的调用次数。
func (fv *FakeVolume) GetTearDownCallCount() int {
	fv.RLock()
	defer fv.RUnlock()
	return fv.TearDownCallCount
}

// TearDownAt 删除指定目录下的所有内容。
func (fv *FakeVolume) TearDownAt(dir string) error {
	return os.RemoveAll(dir)
}

// 块卷支持。
func (fv *FakeVolume) SetUpDevice() (string, error) {
	fv.Lock()
	defer fv.Unlock()
	if fv.VolName == TimeoutOnMountDeviceVolumeName {
		fv.DeviceMountState[fv.VolName] = deviceMountUncertain
		return "", volumetypes.NewUncertainProgressError("mount failed")
	}
	if fv.VolName == FailMountDeviceVolumeName {
		fv.DeviceMountState[fv.VolName] = deviceNotMounted
		return "", fmt.Errorf("error mapping disk: %s", fv.VolName)
	}

	if fv.VolName == TimeoutAndFailOnMountDeviceVolumeName {
		_, ok := fv.DeviceMountState[fv.VolName]
		if !ok {
			fv.DeviceMountState[fv.VolName] = deviceMountUncertain
			return "", volumetypes.NewUncertainProgressError("timed out mounting error")
		}
		fv.DeviceMountState[fv.VolName] = deviceNotMounted
		return "", fmt.Errorf("error mapping disk: %s", fv.VolName)
	}

	if fv.VolName == SuccessAndTimeoutDeviceName {
		_, ok := fv.DeviceMountState[fv.VolName]
		if ok {
			fv.DeviceMountState[fv.VolName] = deviceMountUncertain
			return "", volumetypes.NewUncertainProgressError("error mounting state")
		}
	}
	if fv.VolName == SuccessAndFailOnMountDeviceName {
		_, ok := fv.DeviceMountState[fv.VolName]
		if ok {
			return "", fmt.Errorf("error mapping disk: %s", fv.VolName)
		}
	}

	fv.DeviceMountState[fv.VolName] = deviceMounted
	fv.SetUpDeviceCallCount++

	return "", nil
}

// GetStagingPath 返回块卷的 staging 路径。
func (fv *FakeVolume) GetStagingPath() string {
	return filepath.Join(fv.Plugin.Host.GetVolumeDevicePluginDir(utilstrings.EscapeQualifiedName(fv.Plugin.PluginName)), "staging", fv.VolName)
}

// 块卷支持。
func (fv *FakeVolume) GetSetUpDeviceCallCount() int {
	fv.RLock()
	defer fv.RUnlock()
	return fv.SetUpDeviceCallCount
}

// 块卷支持。
func (fv *FakeVolume) GetGlobalMapPath(spec *volume.Spec) (string, error) {
	fv.Lock()
	defer fv.Unlock()
	fv.GlobalMapPathCallCount++
	return fv.getGlobalMapPath()
}

// 块卷支持。
func (fv *FakeVolume) getGlobalMapPath() (string, error) {
	return filepath.Join(fv.Plugin.Host.GetVolumeDevicePluginDir(utilstrings.EscapeQualifiedName(fv.Plugin.PluginName)), "pluginDependentPath"), nil
}

// 块卷支持。
func (fv *FakeVolume) GetGlobalMapPathCallCount() int {
	fv.RLock()
	defer fv.RUnlock()
	return fv.GlobalMapPathCallCount
}

// 块卷支持。
func (fv *FakeVolume) GetPodDeviceMapPath() (string, string) {
	fv.RLock()
	defer fv.RUnlock()
	fv.PodDeviceMapPathCallCount++
	return fv.getPodDeviceMapPath()
}

// 块卷支持。
func (fv *FakeVolume) getPodDeviceMapPath() (string, string) {
	return filepath.Join(fv.Plugin.Host.GetPodVolumeDeviceDir(fv.PodUID, utilstrings.EscapeQualifiedName(fv.Plugin.PluginName))), fv.VolName
}

// 块卷支持。
func (fv *FakeVolume) GetPodDeviceMapPathCallCount() int {
	fv.RLock()
	defer fv.RUnlock()
	return fv.PodDeviceMapPathCallCount
}

// 块卷支持。
func (fv *FakeVolume) TearDownDevice(mapPath string, devicePath string) error {
	fv.Lock()
	defer fv.Unlock()
	fv.TearDownDeviceCallCount++
	return nil
}

// 块卷支持。
func (fv *FakeVolume) GetTearDownDeviceCallCount() int {
	fv.RLock()
	defer fv.RUnlock()
	return fv.TearDownDeviceCallCount
}

// 块卷支持。
func (fv *FakeVolume) UnmapPodDevice() error {
	fv.Lock()
	defer fv.Unlock()
	fv.UnmapPodDeviceCallCount++
	return nil
}

// 块卷支持。
func (fv *FakeVolume) GetUnmapPodDeviceCallCount() int {
	fv.RLock()
	defer fv.RUnlock()
	return fv.UnmapPodDeviceCallCount
}

// 块卷支持。
func (fv *FakeVolume) MapPodDevice() (string, error) {
	fv.Lock()
	defer fv.Unlock()

	if fv.VolName == TimeoutOnSetupVolumeName {
		fv.VolumeMountState[fv.VolName] = volumeMountUncertain
		return "", volumetypes.NewUncertainProgressError("time out on setup")
	}

	if fv.VolName == FailOnSetupVolumeName {
		fv.VolumeMountState[fv.VolName] = volumeNotMounted
		return "", fmt.Errorf("mounting volume failed")
	}

	if fv.VolName == TimeoutAndFailOnSetupVolumeName {
		_, ok := fv.VolumeMountState[fv.VolName]
		if !ok {
			fv.VolumeMountState[fv.VolName] = volumeMountUncertain
			return "", volumetypes.NewUncertainProgressError("time out on setup")
		}
		fv.VolumeMountState[fv.VolName] = volumeNotMounted
		return "", fmt.Errorf("mounting volume failed")

	}

	if fv.VolName == SuccessAndFailOnSetupVolumeName {
		_, ok := fv.VolumeMountState[fv.VolName]
		if ok {
			fv.VolumeMountState[fv.VolName] = volumeNotMounted
			return "", fmt.Errorf("mounting volume failed")
		}
	}

	if fv.VolName == SuccessAndTimeoutSetupVolumeName {
		_, ok := fv.VolumeMountState[fv.VolName]
		if ok {
			fv.VolumeMountState[fv.VolName] = volumeMountUncertain
			return "", volumetypes.NewUncertainProgressError("time out on setup")
		}
	}

	fv.VolumeMountState[fv.VolName] = volumeMounted
	fv.MapPodDeviceCallCount++
	return "", nil
}

// 块卷支持。
func (fv *FakeVolume) GetMapPodDeviceCallCount() int {
	fv.RLock()
	defer fv.RUnlock()
	return fv.MapPodDeviceCallCount
}

// Attach 模拟将卷连接到节点，按卷名/节点名注入多种连接状态。
func (fv *FakeVolume) Attach(spec *volume.Spec, nodeName types.NodeName) (string, error) {
	fv.Lock()
	defer fv.Unlock()
	fv.AttachCallCount++

	volumeName, err := getUniqueVolumeName(spec)
	if err != nil {
		return "", err
	}
	volumeNodes, exist := fv.VolumesAttached[volumeName]
	if exist {
		if nodeName == UncertainAttachNode {
			return "/dev/vdb-test", nil
		}
		// 即便之前因超时而记录过连接，仍需持续返回错误，避免 reconciler 误判为已连接。
		if nodeName == TimeoutAttachNode {
			return "", fmt.Errorf("timed out to attach volume %q to node %q", volumeName, nodeName)
		}
		if volumeNodes.Has(string(nodeName)) || volumeNodes.Has(MultiAttachNode) || nodeName == MultiAttachNode {
			volumeNodes.Insert(string(nodeName))
			return "/dev/vdb-test", nil
		}
		return "", fmt.Errorf("volume %q trying to attach to node %q is already attached to node %q", volumeName, nodeName, volumeNodes)
	}

	fv.VolumesAttached[volumeName] = sets.New[string](string(nodeName))
	if nodeName == UncertainAttachNode || nodeName == TimeoutAttachNode {
		return "", fmt.Errorf("timed out to attach volume %q to node %q", volumeName, nodeName)
	}
	return "/dev/vdb-test", nil
}

// GetAttachCallCount 返回 Attach 的调用次数。
func (fv *FakeVolume) GetAttachCallCount() int {
	fv.RLock()
	defer fv.RUnlock()
	return fv.AttachCallCount
}

// WaitForAttach 模拟等待连接完成，可注入 WaitForAttachHook。
func (fv *FakeVolume) WaitForAttach(spec *volume.Spec, devicePath string, pod *v1.Pod, spectimeout time.Duration) (string, error) {
	fv.Lock()
	defer fv.Unlock()
	fv.WaitForAttachCallCount++
	if fv.WaitForAttachHook != nil {
		return fv.WaitForAttachHook(spec, devicePath, pod, spectimeout)
	}
	return "/dev/sdb", nil
}

// GetWaitForAttachCallCount 返回 WaitForAttach 的调用次数。
func (fv *FakeVolume) GetWaitForAttachCallCount() int {
	fv.RLock()
	defer fv.RUnlock()
	return fv.WaitForAttachCallCount
}

// GetDeviceMountPath 返回设备挂载路径（测试中为空字符串）并记录调用次数。
func (fv *FakeVolume) GetDeviceMountPath(spec *volume.Spec) (string, error) {
	fv.Lock()
	defer fv.Unlock()
	fv.GetDeviceMountPathCallCount++
	return "", nil
}

func (fv *FakeVolume) mountDeviceInternal(spec *volume.Spec, devicePath string, deviceMountPath string) error {
	fv.Lock()
	defer fv.Unlock()
	if spec.Name() == TimeoutOnMountDeviceVolumeName {
		fv.DeviceMountState[spec.Name()] = deviceMountUncertain
		return volumetypes.NewUncertainProgressError("mount failed")
	}

	if spec.Name() == FailMountDeviceVolumeName {
		fv.DeviceMountState[spec.Name()] = deviceNotMounted
		return fmt.Errorf("error mounting disk: %s", devicePath)
	}

	if spec.Name() == TimeoutAndFailOnMountDeviceVolumeName {
		_, ok := fv.DeviceMountState[spec.Name()]
		if !ok {
			fv.DeviceMountState[spec.Name()] = deviceMountUncertain
			return volumetypes.NewUncertainProgressError("timed out mounting error")
		}
		fv.DeviceMountState[spec.Name()] = deviceNotMounted
		return fmt.Errorf("error mounting disk: %s", devicePath)
	}

	if spec.Name() == SuccessAndTimeoutDeviceName {
		_, ok := fv.DeviceMountState[spec.Name()]
		if ok {
			fv.DeviceMountState[spec.Name()] = deviceMountUncertain
			return volumetypes.NewUncertainProgressError("error mounting state")
		}
	}

	if spec.Name() == SuccessAndFailOnMountDeviceName {
		_, ok := fv.DeviceMountState[spec.Name()]
		if ok {
			return fmt.Errorf("error mounting disk: %s", devicePath)
		}
	}
	fv.DeviceMountState[spec.Name()] = deviceMounted
	fv.MountDeviceCallCount++
	return nil
}

// MountDevice 挂载设备，实际逻辑委托给 mountDeviceInternal。
func (fv *FakeVolume) MountDevice(spec *volume.Spec, devicePath string, deviceMountPath string, _ volume.DeviceMounterArgs) error {
	return fv.mountDeviceInternal(spec, devicePath, deviceMountPath)
}

// GetMountDeviceCallCount 返回 MountDevice 的调用次数。
func (fv *FakeVolume) GetMountDeviceCallCount() int {
	fv.RLock()
	defer fv.RUnlock()
	return fv.MountDeviceCallCount
}

// GetUnmountDeviceCallCount 返回 UnmountDevice 的调用次数。
func (fv *FakeVolume) GetUnmountDeviceCallCount() int {
	fv.RLock()
	defer fv.RUnlock()
	return fv.UnmountDeviceCallCount
}

// Detach 模拟断开卷，按节点名注入失败并维护连接状态。
func (fv *FakeVolume) Detach(volumeName string, nodeName types.NodeName) error {
	fv.Lock()
	defer fv.Unlock()

	node := string(nodeName)
	volumeNodes, exist := fv.VolumesAttached[volumeName]
	if !exist || !volumeNodes.Has(node) {
		return fmt.Errorf("trying to detach volume %q that is not attached to the node %q", volumeName, node)
	}

	fv.DetachCallCount++
	if nodeName == FailDetachNode {
		return fmt.Errorf("fail to detach volume %q to node %q", volumeName, nodeName)
	}

	volumeNodes.Delete(node)
	if volumeNodes.Len() == 0 {
		delete(fv.VolumesAttached, volumeName)
	}

	return nil
}

// VolumesAreAttached 返回卷连接状态；测试中始终返回 nil。
func (fv *FakeVolume) VolumesAreAttached(spec []*volume.Spec, nodeName types.NodeName) (map[*volume.Spec]bool, error) {
	fv.Lock()
	defer fv.Unlock()
	return nil, nil
}

// GetDetachCallCount 返回 Detach 的调用次数。
func (fv *FakeVolume) GetDetachCallCount() int {
	fv.RLock()
	defer fv.RUnlock()
	return fv.DetachCallCount
}

// UnmountDevice 模拟卸载设备，可注入 UnmountDeviceHook。
func (fv *FakeVolume) UnmountDevice(globalMountPath string) error {
	fv.Lock()
	defer fv.Unlock()
	fv.UnmountDeviceCallCount++
	if fv.UnmountDeviceHook != nil {
		return fv.UnmountDeviceHook(globalMountPath)
	}
	return nil
}

// FakeDeleter 是卷删除器的假实现。
type FakeDeleter struct {
	path string
	volume.MetricsNil
}

// Delete 删除卷；测试中为空实现。
func (fd *FakeDeleter) Delete() error {
	// 测试中始终成功。
	return nil
}

// GetPath 返回 FakeDeleter 的路径。
func (fd *FakeDeleter) GetPath() string {
	return fd.path
}

// FakeProvisioner 是卷供应器的假实现，可按选项注入失败与延迟。
type FakeProvisioner struct {
	Options               volume.VolumeOptions
	Host                  volume.VolumeHost
	ProvisionDelaySeconds int
}

// Provision 模拟创建 PV，可按参数注入 provisioning 失败或延迟。
func (fc *FakeProvisioner) Provision(selectedNode *v1.Node, allowedTopologies []v1.TopologySelectorTerm) (*v1.PersistentVolume, error) {
	// 通过参数注入 provisioning 失败。
	if fc.Options.Parameters != nil {
		if _, ok := fc.Options.Parameters[ExpectProvisionFailureKey]; ok {
			return nil, fmt.Errorf("expected error")
		}
	}
	fullpath := fmt.Sprintf("/%s/hostpath_pv/%s", os.TempDir(), uuid.NewUUID())

	pv := &v1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name: fc.Options.PVName,
			Annotations: map[string]string{
				"kubernetes.io/createdby": "fakeplugin-provisioner",
			},
		},
		Spec: v1.PersistentVolumeSpec{
			PersistentVolumeReclaimPolicy: fc.Options.PersistentVolumeReclaimPolicy,
			AccessModes:                   fc.Options.PVC.Spec.AccessModes,
			Capacity: v1.ResourceList{
				v1.ResourceName(v1.ResourceStorage): fc.Options.PVC.Spec.Resources.Requests[v1.ResourceName(v1.ResourceStorage)],
			},
			PersistentVolumeSource: v1.PersistentVolumeSource{
				HostPath: &v1.HostPathVolumeSource{
					Path: fullpath,
				},
			},
		},
	}

	if fc.ProvisionDelaySeconds > 0 {
		time.Sleep(time.Duration(fc.ProvisionDelaySeconds) * time.Second)
	}

	return pv, nil
}

var _ volumepathhandler.BlockVolumePathHandler = &FakeVolumePathHandler{}

// NewBlockVolumePathHandler 创建一个 FakeVolumePathHandler。
func NewBlockVolumePathHandler() volumepathhandler.BlockVolumePathHandler {
	return &FakeVolumePathHandler{}
}

// FakeVolumePathHandler 是块设备路径处理器的假实现。
type FakeVolumePathHandler struct {
	sync.RWMutex
}

// MapDevice 模拟设备映射；测试中为空实现。
func (fv *FakeVolumePathHandler) MapDevice(devicePath string, mapDir string, linkName string, bindMount bool) error {
	// 测试中始终成功。
	return nil
}

// UnmapDevice 模拟解除设备映射；测试中为空实现。
func (fv *FakeVolumePathHandler) UnmapDevice(mapDir string, linkName string, bindMount bool) error {
	// 测试中始终成功。
	return nil
}

// RemoveMapPath 删除映射路径；测试中为空实现。
func (fv *FakeVolumePathHandler) RemoveMapPath(mapPath string) error {
	// 测试中始终成功。
	return nil
}

// IsSymlinkExist 检查符号链接是否存在；测试中始终返回 true。
func (fv *FakeVolumePathHandler) IsSymlinkExist(mapPath string) (bool, error) {
	// 测试中始终成功。
	return true, nil
}

// IsDeviceBindMountExist 检查设备 bind mount 是否存在；测试中始终返回 true。
func (fv *FakeVolumePathHandler) IsDeviceBindMountExist(mapPath string) (bool, error) {
	// 测试中始终成功。
	return true, nil
}

// GetDeviceBindMountRefs 返回设备 bind mount 的引用列表；测试中为空。
func (fv *FakeVolumePathHandler) GetDeviceBindMountRefs(devPath string, mapPath string) ([]string, error) {
	// 测试中始终成功。
	return []string{}, nil
}

// FindGlobalMapPathUUIDFromPod 根据 Pod 查找全局映射路径的 UUID；测试中返回空。
func (fv *FakeVolumePathHandler) FindGlobalMapPathUUIDFromPod(pluginDir, mapPath string, podUID types.UID) (string, error) {
	// 测试中始终成功。
	return "", nil
}

// AttachFileDevice 模拟挂载文件设备；测试中返回空。
func (fv *FakeVolumePathHandler) AttachFileDevice(path string) (string, error) {
	// 测试中始终成功。
	return "", nil
}

// DetachFileDevice 模拟卸载文件设备；测试中为空实现。
func (fv *FakeVolumePathHandler) DetachFileDevice(path string) error {
	// 测试中始终成功。
	return nil
}

// GetLoopDevice 返回预设的 loop 设备路径。
func (fv *FakeVolumePathHandler) GetLoopDevice(path string) (string, error) {
	// 测试中始终成功。
	return "/dev/loop1", nil
}

// FindEmptyDirectoryUsageOnTmpfs 返回本机 tmpfs 上空目录的预期占用量。
func FindEmptyDirectoryUsageOnTmpfs() (*resource.Quantity, error) {
	// Windows 下没有 du 命令，且空目录大小恒为 0。
	if goruntime.GOOS == "windows" {
		used, err := resource.ParseQuantity("0")
		return &used, err
	}
	tmpDir, err := utiltesting.MkTmpdir("metrics_du_test")
	if err != nil {
		return nil, err
	}
	defer os.RemoveAll(tmpDir)
	out, err := exec.New().Command("nice", "-n", "19", "du", "-x", "-s", "-B", "1", tmpDir).CombinedOutput()
	if err != nil {
		return nil, fmt.Errorf("failed command 'du' on %s with error %v", tmpDir, err)
	}
	used, err := resource.ParseQuantity(strings.Fields(string(out))[0])
	if err != nil {
		return nil, fmt.Errorf("failed to parse 'du' output %s due to error %v", out, err)
	}
	used.Format = resource.BinarySI
	return &used, nil
}

// VerifyAttachCallCount 确保至少一个 attacher 的 Attach 调用次数达到预期。
func VerifyAttachCallCount(
	expectedAttachCallCount int,
	fakeVolumePlugin *FakeVolumePlugin) error {
	for _, attacher := range fakeVolumePlugin.GetAttachers() {
		actualCallCount := attacher.GetAttachCallCount()
		if actualCallCount >= expectedAttachCallCount {
			return nil
		}
	}

	return fmt.Errorf(
		"No attachers have expected AttachCallCount. Expected: <%v>.",
		expectedAttachCallCount)
}

// VerifyZeroAttachCalls 确保所有 attacher 的 Attach 调用次数均为零。
func VerifyZeroAttachCalls(fakeVolumePlugin *FakeVolumePlugin) error {
	for _, attacher := range fakeVolumePlugin.GetAttachers() {
		actualCallCount := attacher.GetAttachCallCount()
		if actualCallCount != 0 {
			return fmt.Errorf(
				"At least one attacher has non-zero AttachCallCount: <%v>.",
				actualCallCount)
		}
	}

	return nil
}

// VerifyWaitForAttachCallCount 确保至少一个 attacher 的 WaitForAttach 调用次数达到预期。
func VerifyWaitForAttachCallCount(
	expectedWaitForAttachCallCount int,
	fakeVolumePlugin *FakeVolumePlugin) error {
	for _, attacher := range fakeVolumePlugin.GetAttachers() {
		actualCallCount := attacher.GetWaitForAttachCallCount()
		if actualCallCount >= expectedWaitForAttachCallCount {
			return nil
		}
	}

	return fmt.Errorf(
		"No Attachers have expected WaitForAttachCallCount. Expected: <%v>.",
		expectedWaitForAttachCallCount)
}

// VerifyZeroWaitForAttachCallCount 确保所有 attacher 的 WaitForAttach 调用次数均为零。
func VerifyZeroWaitForAttachCallCount(fakeVolumePlugin *FakeVolumePlugin) error {
	for _, attacher := range fakeVolumePlugin.GetAttachers() {
		actualCallCount := attacher.GetWaitForAttachCallCount()
		if actualCallCount != 0 {
			return fmt.Errorf(
				"At least one attacher has non-zero WaitForAttachCallCount: <%v>.",
				actualCallCount)
		}
	}

	return nil
}

// VerifyMountDeviceCallCount 确保至少一个 attacher 的 MountDevice 调用次数达到预期。
func VerifyMountDeviceCallCount(
	expectedMountDeviceCallCount int,
	fakeVolumePlugin *FakeVolumePlugin) error {
	for _, attacher := range fakeVolumePlugin.GetAttachers() {
		actualCallCount := attacher.GetMountDeviceCallCount()
		if actualCallCount >= expectedMountDeviceCallCount {
			return nil
		}
	}

	return fmt.Errorf(
		"No Attachers have expected MountDeviceCallCount. Expected: <%v>.",
		expectedMountDeviceCallCount)
}

// VerifyUnmountDeviceCallCount 校验至少一个 detacher 的 UnmountDevice 调用次数达到预期。
func VerifyUnmountDeviceCallCount(expectedCallCount int, fakeVolumePlugin *FakeVolumePlugin) error {
	detachers := fakeVolumePlugin.GetDetachers()
	if len(detachers) == 0 && (expectedCallCount == 0) {
		return nil
	}
	actualCallCount := 0
	for _, detacher := range detachers {
		actualCallCount = detacher.GetUnmountDeviceCallCount()
		if expectedCallCount == 0 && actualCallCount == expectedCallCount {
			return nil
		}

		if (expectedCallCount > 0) && (actualCallCount >= expectedCallCount) {
			return nil
		}
	}

	return fmt.Errorf(
		"Expected DeviceUnmount Call %d, got %d",
		expectedCallCount, actualCallCount)
}

// VerifyZeroMountDeviceCallCount 确保所有 attacher 的 MountDevice 调用次数均为零。
func VerifyZeroMountDeviceCallCount(fakeVolumePlugin *FakeVolumePlugin) error {
	for _, attacher := range fakeVolumePlugin.GetAttachers() {
		actualCallCount := attacher.GetMountDeviceCallCount()
		if actualCallCount != 0 {
			return fmt.Errorf(
				"At least one attacher has non-zero MountDeviceCallCount: <%v>.",
				actualCallCount)
		}
	}

	return nil
}

// VerifySetUpCallCount 确保至少一个 mounter 的 SetUp 调用次数达到预期。
func VerifySetUpCallCount(
	expectedSetUpCallCount int,
	fakeVolumePlugin *FakeVolumePlugin) error {
	for _, mounter := range fakeVolumePlugin.GetMounters() {
		actualCallCount := mounter.GetSetUpCallCount()
		if actualCallCount >= expectedSetUpCallCount {
			return nil
		}
	}

	return fmt.Errorf(
		"No Mounters have expected SetUpCallCount. Expected: <%v>.",
		expectedSetUpCallCount)
}

// VerifyZeroSetUpCallCount 确保所有 mounter 的 SetUp 调用次数均为零。
func VerifyZeroSetUpCallCount(fakeVolumePlugin *FakeVolumePlugin) error {
	for _, mounter := range fakeVolumePlugin.GetMounters() {
		actualCallCount := mounter.GetSetUpCallCount()
		if actualCallCount != 0 {
			return fmt.Errorf(
				"At least one mounter has non-zero SetUpCallCount: <%v>.",
				actualCallCount)
		}
	}

	return nil
}

// VerifyTearDownCallCount 确保至少一个 unmounter 的 TearDown 调用次数达到预期。
func VerifyTearDownCallCount(
	expectedTearDownCallCount int,
	fakeVolumePlugin *FakeVolumePlugin) error {
	unmounters := fakeVolumePlugin.GetUnmounters()
	if len(unmounters) == 0 && (expectedTearDownCallCount == 0) {
		return nil
	}

	for _, unmounter := range unmounters {
		actualCallCount := unmounter.GetTearDownCallCount()
		if expectedTearDownCallCount == 0 && actualCallCount == expectedTearDownCallCount {
			return nil
		}

		if (expectedTearDownCallCount > 0) && (actualCallCount >= expectedTearDownCallCount) {
			return nil
		}
	}

	return fmt.Errorf(
		"No Unmounters have expected SetUpCallCount. Expected: <%v>.",
		expectedTearDownCallCount)
}

// VerifyZeroTearDownCallCount 确保所有 mounter 的 TearDown 调用次数均为零。
func VerifyZeroTearDownCallCount(fakeVolumePlugin *FakeVolumePlugin) error {
	for _, mounter := range fakeVolumePlugin.GetMounters() {
		actualCallCount := mounter.GetTearDownCallCount()
		if actualCallCount != 0 {
			return fmt.Errorf(
				"At least one mounter has non-zero TearDownCallCount: <%v>.",
				actualCallCount)
		}
	}

	return nil
}

// VerifyDetachCallCount 确保至少一个 detacher 的 Detach 调用次数等于预期。
func VerifyDetachCallCount(
	expectedDetachCallCount int,
	fakeVolumePlugin *FakeVolumePlugin) error {
	for _, detacher := range fakeVolumePlugin.GetDetachers() {
		actualCallCount := detacher.GetDetachCallCount()
		if actualCallCount == expectedDetachCallCount {
			return nil
		}
	}

	return fmt.Errorf(
		"No Detachers have expected DetachCallCount. Expected: <%v>.",
		expectedDetachCallCount)
}

// VerifyZeroDetachCallCount 确保所有 detacher 的 Detach 调用次数均为零。
func VerifyZeroDetachCallCount(fakeVolumePlugin *FakeVolumePlugin) error {
	for _, detacher := range fakeVolumePlugin.GetDetachers() {
		actualCallCount := detacher.GetDetachCallCount()
		if actualCallCount != 0 {
			return fmt.Errorf(
				"At least one detacher has non-zero DetachCallCount: <%v>.",
				actualCallCount)
		}
	}

	return nil
}

// VerifyTearDownDeviceCallCount 确保至少一个 unmapper 的 TearDownDevice 调用次数达到预期。
func VerifyTearDownDeviceCallCount(
	expectedTearDownDeviceCallCount int,
	fakeVolumePlugin *FakeVolumePlugin) error {
	for _, unmapper := range fakeVolumePlugin.GetBlockVolumeUnmapper() {
		actualCallCount := unmapper.GetTearDownDeviceCallCount()
		if actualCallCount >= expectedTearDownDeviceCallCount {
			return nil
		}
	}

	return fmt.Errorf(
		"No Unmapper have expected TearDownDeviceCallCount. Expected: <%v>.",
		expectedTearDownDeviceCallCount)
}

// VerifyZeroTearDownDeviceCallCount 确保所有 unmapper 的 TearDownDevice 调用次数均为零。
func VerifyZeroTearDownDeviceCallCount(fakeVolumePlugin *FakeVolumePlugin) error {
	for _, unmapper := range fakeVolumePlugin.GetBlockVolumeUnmapper() {
		actualCallCount := unmapper.GetTearDownDeviceCallCount()
		if actualCallCount != 0 {
			return fmt.Errorf(
				"At least one unmapper has non-zero TearDownDeviceCallCount: <%v>.",
				actualCallCount)
		}
	}

	return nil
}

// VerifyUnmapPodDeviceCallCount 确保至少一个 unmapper 的 UnmapPodDevice 调用次数达到预期。
func VerifyUnmapPodDeviceCallCount(
	expectedUnmapPodDeviceCallCount int,
	fakeVolumePlugin *FakeVolumePlugin) error {
	for _, unmapper := range fakeVolumePlugin.GetBlockVolumeUnmapper() {
		actualCallCount := unmapper.GetUnmapPodDeviceCallCount()
		if actualCallCount >= expectedUnmapPodDeviceCallCount {
			return nil
		}
	}

	return fmt.Errorf(
		"No Unmapper have expected UnmapPodDeviceCallCount. Expected: <%v>.",
		expectedUnmapPodDeviceCallCount)
}

// VerifyZeroUnmapPodDeviceCallCount 确保所有 unmapper 的 UnmapPodDevice 调用次数均为零。
func VerifyZeroUnmapPodDeviceCallCount(fakeVolumePlugin *FakeVolumePlugin) error {
	for _, unmapper := range fakeVolumePlugin.GetBlockVolumeUnmapper() {
		actualCallCount := unmapper.GetUnmapPodDeviceCallCount()
		if actualCallCount != 0 {
			return fmt.Errorf(
				"At least one unmapper has non-zero UnmapPodDeviceCallCount: <%v>.",
				actualCallCount)
		}
	}

	return nil
}

// VerifyGetGlobalMapPathCallCount 确保至少一个 mapper 的 GetGlobalMapPath 调用次数等于预期。
func VerifyGetGlobalMapPathCallCount(
	expectedGlobalMapPathCallCount int,
	fakeVolumePlugin *FakeVolumePlugin) error {
	for _, mapper := range fakeVolumePlugin.GetBlockVolumeMapper() {
		actualCallCount := mapper.GetGlobalMapPathCallCount()
		if actualCallCount == expectedGlobalMapPathCallCount {
			return nil
		}
	}

	return fmt.Errorf(
		"No Mappers have expected GetGlobalMapPathCallCount. Expected: <%v>.",
		expectedGlobalMapPathCallCount)
}

// VerifyGetPodDeviceMapPathCallCount 确保至少一个 mapper 的 GetPodDeviceMapPath 调用次数等于预期。
func VerifyGetPodDeviceMapPathCallCount(
	expectedPodDeviceMapPathCallCount int,
	fakeVolumePlugin *FakeVolumePlugin) error {
	for _, mapper := range fakeVolumePlugin.GetBlockVolumeMapper() {
		actualCallCount := mapper.GetPodDeviceMapPathCallCount()
		if actualCallCount == expectedPodDeviceMapPathCallCount {
			return nil
		}
	}

	return fmt.Errorf(
		"No Mappers have expected GetPodDeviceMapPathCallCount. Expected: <%v>.",
		expectedPodDeviceMapPathCallCount)
}

// VerifyGetMapPodDeviceCallCount 确保至少一个 mapper 的 MapPodDevice 调用次数达到预期。
func VerifyGetMapPodDeviceCallCount(
	expectedMapPodDeviceCallCount int,
	fakeVolumePlugin *FakeVolumePlugin) error {
	for _, mapper := range fakeVolumePlugin.GetBlockVolumeMapper() {
		actualCallCount := mapper.GetMapPodDeviceCallCount()
		if actualCallCount >= expectedMapPodDeviceCallCount {
			return nil
		}
	}

	return fmt.Errorf(
		"No Mapper have expected MapPodDeviceCallCount. Expected: <%v>.",
		expectedMapPodDeviceCallCount)
}

// GetTestVolumePluginMgr 创建并初始化测试用卷插件管理器及 FakeVolumePlugin。
func GetTestVolumePluginMgr(t *testing.T) (*volume.VolumePluginMgr, *FakeVolumePlugin) {
	plugins := ProbeVolumePlugins(volume.VolumeConfig{})
	v := NewFakeVolumeHost(
		t,
		t.TempDir(), /* rootDir */
		nil,         /* kubeClient */
		plugins,     /* plugins */
	)
	return v.GetPluginMgr(), plugins[0].(*FakeVolumePlugin)
}

// GetTestKubeletVolumePluginMgr 创建并初始化测试用 kubelet 卷插件管理器。
func GetTestKubeletVolumePluginMgr(t *testing.T) (*volume.VolumePluginMgr, *FakeVolumePlugin) {
	plugins := ProbeVolumePlugins(volume.VolumeConfig{})
	v := NewFakeKubeletVolumeHost(
		t,
		t.TempDir(), /* rootDir */
		nil,         /* kubeClient */
		plugins,     /* plugins */
	)
	return v.GetPluginMgr(), plugins[0].(*FakeVolumePlugin)
}

// GetTestKubeletVolumePluginMgrWithNode 创建带指定节点的 kubelet 卷插件管理器。
func GetTestKubeletVolumePluginMgrWithNode(t *testing.T, node *v1.Node) (*volume.VolumePluginMgr, *FakeVolumePlugin) {
	plugins := ProbeVolumePlugins(volume.VolumeConfig{})
	v := NewFakeKubeletVolumeHost(
		t,
		t.TempDir(), /* rootDir */
		nil,         /* kubeClient */
		plugins,     /* plugins */
	)
	v.WithNode(node)

	return v.GetPluginMgr(), plugins[0].(*FakeVolumePlugin)
}

// GetTestKubeletVolumePluginMgrWithNodeAndRoot 创建带指定节点和根目录的 kubelet 卷插件管理器。
func GetTestKubeletVolumePluginMgrWithNodeAndRoot(t *testing.T, node *v1.Node, rootDir string) (*volume.VolumePluginMgr, *FakeVolumePlugin) {
	plugins := ProbeVolumePlugins(volume.VolumeConfig{})
	v := NewFakeKubeletVolumeHost(
		t,
		rootDir, /* rootDir */
		nil,     /* kubeClient */
		plugins, /* plugins */
	)
	v.WithNode(node)

	return v.GetPluginMgr(), plugins[0].(*FakeVolumePlugin)
}

// CreateTestPVC 返回一个可用于 provisioning 测试的 PVC。
func CreateTestPVC(capacity string, accessModes []v1.PersistentVolumeAccessMode) *v1.PersistentVolumeClaim {
	claim := v1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "dummy",
			Namespace: "default",
		},
		Spec: v1.PersistentVolumeClaimSpec{
			AccessModes: accessModes,
			Resources: v1.VolumeResourceRequirements{
				Requests: v1.ResourceList{
					v1.ResourceName(v1.ResourceStorage): resource.MustParse(capacity),
				},
			},
		},
	}
	return &claim
}

// MetricsEqualIgnoreTimestamp 比较两个 Metrics，忽略时间戳字段。
func MetricsEqualIgnoreTimestamp(a *volume.Metrics, b *volume.Metrics) bool {
	available := a.Available == b.Available
	capacity := a.Capacity == b.Capacity
	used := a.Used == b.Used
	inodes := a.Inodes == b.Inodes
	inodesFree := a.InodesFree == b.InodesFree
	inodesUsed := a.InodesUsed == b.InodesUsed
	return available && capacity && used && inodes && inodesFree && inodesUsed
}

// ContainsAccessMode 判断给定访问模式是否在列表中。
func ContainsAccessMode(modes []v1.PersistentVolumeAccessMode, mode v1.PersistentVolumeAccessMode) bool {
	return slices.Contains(modes, mode)
}
