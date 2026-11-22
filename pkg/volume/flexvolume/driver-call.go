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

package flexvolume

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"k8s.io/klog/v2"

	"k8s.io/kubernetes/pkg/volume"
)

const (
	// 驱动命令名
	initCmd          = "init"
	getVolumeNameCmd = "getvolumename"

	isAttached = "isattached"

	attachCmd        = "attach"
	waitForAttachCmd = "waitforattach"
	mountDeviceCmd   = "mountdevice"

	detachCmd        = "detach"
	unmountDeviceCmd = "unmountdevice"

	mountCmd   = "mount"
	unmountCmd = "unmount"

	expandVolumeCmd = "expandvolume"
	expandFSCmd     = "expandfs"

	// 传给驱动的选项键
	optionFSType         = "kubernetes.io/fsType"
	optionReadWrite      = "kubernetes.io/readwrite"
	optionKeySecret      = "kubernetes.io/secret"
	optionFSGroup        = "kubernetes.io/mounterArgs.FsGroup"
	optionPVorVolumeName = "kubernetes.io/pvOrVolumeName"

	optionKeyPodName      = "kubernetes.io/pod.name"
	optionKeyPodNamespace = "kubernetes.io/pod.namespace"
	optionKeyPodUID       = "kubernetes.io/pod.uid"

	optionKeyServiceAccountName = "kubernetes.io/serviceAccount.name"
)

const (
	// StatusSuccess 表示命令执行成功。
	StatusSuccess = "Success"
	// StatusNotSupported 表示驱动不支持该命令。
	StatusNotSupported = "Not supported"
)

// 驱动调用超时错误。
var (
	errTimeout = fmt.Errorf("timeout")
)

// DriverCall 封装 FlexVolume 与驱动之间的调用契约。
// 调用方需负责按命令要求提供完整参数。
type DriverCall struct {
	Command string
	Timeout time.Duration
	plugin  *flexVolumePlugin
	args    []string
}

// NewDriverCall 创建一个无超时限制的驱动调用。
func (plugin *flexVolumePlugin) NewDriverCall(command string) *DriverCall {
	return plugin.NewDriverCallWithTimeout(command, 0)
}

// NewDriverCallWithTimeout 创建带指定超时的驱动调用。
func (plugin *flexVolumePlugin) NewDriverCallWithTimeout(command string, timeout time.Duration) *DriverCall {
	return &DriverCall{
		Command: command,
		Timeout: timeout,
		plugin:  plugin,
		args:    []string{command},
	}
}

// Append 向驱动调用的参数列表追加一个参数。
func (dc *DriverCall) Append(arg string) {
	dc.args = append(dc.args, arg)
}

// AppendSpec 将卷规格序列化为 JSON 后追加到驱动调用参数列表。
func (dc *DriverCall) AppendSpec(spec *volume.Spec, host volume.VolumeHost, extraOptions map[string]string) error {
	optionsForDriver, err := NewOptionsForDriver(spec, host, extraOptions)
	if err != nil {
		return err
	}

	jsonBytes, err := json.Marshal(optionsForDriver)
	if err != nil {
		return fmt.Errorf("failed to marshal spec, error: %s", err.Error())
	}

	dc.Append(string(jsonBytes))
	return nil
}

// Run 执行驱动调用并返回驱动状态。
// 已知不支持的命令直接返回 Not supported，避免重复启动外部进程。
func (dc *DriverCall) Run() (*DriverStatus, error) {
	if dc.plugin.isUnsupported(dc.Command) {
		return nil, errors.New(StatusNotSupported)
	}
	execPath := dc.plugin.getExecutable()

	cmd := dc.plugin.runner.Command(execPath, dc.args...)

	timeout := false
	if dc.Timeout > 0 {
		// 超时时终止进程；用标志区分 timeout 与驱动自身报错。
		timer := time.AfterFunc(dc.Timeout, func() {
			timeout = true
			cmd.Stop()
		})
		defer timer.Stop()
	}

	output, execErr := cmd.CombinedOutput()
	if execErr != nil {
		if timeout {
			return nil, errTimeout
		}
		// 即便进程退出码非零，也要解析输出：驱动可能在标准输出里
		// 返回结构化的 "Not supported"，需据此缓存不支持状态。
		_, err := handleCmdResponse(dc.Command, output)
		if err == nil {
			// 进程报错但响应声称成功，属于驱动 bug，仍按执行错误上报。
			klog.Errorf("FlexVolume: driver bug: %s: exec error (%s) but no error in response.", execPath, execErr)
			return nil, execErr
		}
		if isCmdNotSupportedErr(err) {
			dc.plugin.unsupported(dc.Command)
		} else {
			klog.Warningf("FlexVolume: driver call failed: executable: %s, args: %s, error: %s, output: %q", execPath, dc.args, execErr.Error(), output)
		}
		return nil, err
	}

	status, err := handleCmdResponse(dc.Command, output)
	if err != nil {
		if isCmdNotSupportedErr(err) {
			dc.plugin.unsupported(dc.Command)
		}
		return nil, err
	}

	return status, nil
}

// OptionsForDriver 表示传给驱动的卷规格选项（JSON 序列化后作为命令行参数）。
type OptionsForDriver map[string]string

// NewOptionsForDriver 根据卷规格构造驱动选项。
func NewOptionsForDriver(spec *volume.Spec, host volume.VolumeHost, extraOptions map[string]string) (OptionsForDriver, error) {

	volSourceFSType, err := getFSType(spec)
	if err != nil {
		return nil, err
	}

	readOnly, err := getReadOnly(spec)
	if err != nil {
		return nil, err
	}

	volSourceOptions, err := getOptions(spec)
	if err != nil {
		return nil, err
	}

	options := map[string]string{}

	options[optionFSType] = volSourceFSType

	if readOnly {
		options[optionReadWrite] = "ro"
	} else {
		options[optionReadWrite] = "rw"
	}

	options[optionPVorVolumeName] = spec.Name()

	// 先写额外选项，再写卷自带选项：后者优先级更高，可覆盖前者。
	for key, value := range extraOptions {
		options[key] = value
	}

	for key, value := range volSourceOptions {
		options[key] = value
	}

	return OptionsForDriver(options), nil
}

// DriverStatus 表示驱动调用的返回结构。
type DriverStatus struct {
	// 调用状态，取值为 "Success"、"Failure" 或 "Not supported"。
	Status string `json:"status"`
	// 成功或失败的原因说明。
	Message string `json:"message,omitempty"`
	// 已挂载设备的路径，仅对 attach 调用有效，例如 /dev/sdx。
	DevicePath string `json:"device,omitempty"`
	// 集群内唯一的卷名称。
	VolumeName string `json:"volumeName,omitempty"`
	// 表示卷是否已挂载到当前节点。
	Attached bool `json:"attached,omitempty"`
	// 驱动支持的能力。默认假定全部支持；
	// 若驱动不支持某项能力，可对该字段返回 false。
	Capabilities *DriverCapabilities `json:",omitempty"`
	// 扩容完成后卷的实际大小，单位为字节。
	ActualVolumeSize int64 `json:"volumeNewSize,omitempty"`
}

// DriverCapabilities 描述驱动支持的能力。
type DriverCapabilities struct {
	Attach           bool `json:"attach"`
	SELinuxRelabel   bool `json:"selinuxRelabel"`
	SupportsMetrics  bool `json:"supportsMetrics"`
	FSGroup          bool `json:"fsGroup"`
	RequiresFSResize bool `json:"requiresFSResize"`
}

// defaultCapabilities 返回驱动能力的默认值：除指标采集外默认全部支持。
// 驱动可通过响应覆盖这些默认值。
func defaultCapabilities() *DriverCapabilities {
	return &DriverCapabilities{
		Attach:           true,
		SELinuxRelabel:   true,
		SupportsMetrics:  false,
		FSGroup:          true,
		RequiresFSResize: true,
	}
}

// isCmdNotSupportedErr 判断错误是否表示驱动不支持该命令。
func isCmdNotSupportedErr(err error) bool {
	return err != nil && err.Error() == StatusNotSupported
}

// handleCmdResponse 解析驱动的标准输出，根据 status 字段返回状态或对应错误。
// 未显式给出的能力字段由 defaultCapabilities 填充。
func handleCmdResponse(cmd string, output []byte) (*DriverStatus, error) {
	status := DriverStatus{
		Capabilities: defaultCapabilities(),
	}
	if err := json.Unmarshal(output, &status); err != nil {
		klog.Errorf("Failed to unmarshal output for command: %s, output: %q, error: %s", cmd, string(output), err.Error())
		return nil, err
	} else if status.Status == StatusNotSupported {
		klog.V(5).Infof("%s command is not supported by the driver", cmd)
		return nil, errors.New(status.Status)
	} else if status.Status != StatusSuccess {
		errMsg := fmt.Sprintf("%s command failed, status: %s, reason: %s", cmd, status.Status, status.Message)
		klog.Error(errMsg)
		return nil, fmt.Errorf("%s", errMsg)
	}

	return &status, nil
}
