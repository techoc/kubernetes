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

package util

import (
	"errors"
	"fmt"
	"strings"

	"github.com/opencontainers/selinux/go-selinux"
	"github.com/opencontainers/selinux/go-selinux/label"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/sets"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	v1helper "k8s.io/kubernetes/pkg/apis/core/v1/helper"
	"k8s.io/kubernetes/pkg/features"
	"k8s.io/kubernetes/pkg/volume"
)

// SELinuxLabelTranslator 把进程的 v1.SELinuxOptions 转换为 SELinux 文件标签。
type SELinuxLabelTranslator interface {
	// SELinuxOptionsToFileLabel 根据容器进程的 SELinuxOptions 返回文件标签。
	// Role、User 或 Type 为空时使用系统默认值。
	// 在未启用或不支持 SELinux 的平台上返回空字符串且无错误。
	SELinuxOptionsToFileLabel(opts *v1.SELinuxOptions) (string, error)

	// SELinuxEnabled 判断操作系统是否启用了 SELinux 支持。
	SELinuxEnabled() bool
}

// translator 是 SELinuxLabelTranslator 的真实实现。
// 仅在启用 SELinux 的 Linux 上做转换，其他平台始终返回空字符串且无错误。
type translator struct{}

var _ SELinuxLabelTranslator = &translator{}

// NewSELinuxLabelTranslator 返回适用于当前平台的 SELinuxLabelTranslator。
func NewSELinuxLabelTranslator() SELinuxLabelTranslator {
	return &translator{}
}

// SELinuxOptionsToFileLabel 根据容器进程的 SELinuxOptions 返回文件标签。
// Role、User 或 Type 为空时使用系统默认值。
// 在未启用或不支持 SELinux 的平台上返回空字符串且无错误。
func (l *translator) SELinuxOptionsToFileLabel(opts *v1.SELinuxOptions) (string, error) {
	if opts == nil {
		return "", nil
	}

	args := contextOptions(opts)
	if len(args) == 0 {
		return "", nil
	}

	processLabel, fileLabel, err := label.InitLabels(args)
	if err != nil {
		// 理论上不会走到这里：InitLabels 仅在参数含未知选项时失败，
		// 而 contextOptions 产生的选项都是已知的。
		return "", &SELinuxLabelTranslationError{msg: err.Error()}
	}
	// InitLabels 可能在 kubelet 侧分配唯一的进程标签，但容器运行时并不会使用它。
	// 及时释放避免标签泄漏；对未分配的标签调用 ReleaseLabel 是空操作。
	selinux.ReleaseLabel(processLabel)

	return fileLabel, nil
}

// contextOptions 把 SELinuxOptions 转换为 label.InitLabels 接受的 []string 参数。
func contextOptions(opts *v1.SELinuxOptions) []string {
	if opts == nil {
		return nil
	}
	args := make([]string, 0, 3)
	if opts.User != "" {
		args = append(args, "user:"+opts.User)
	}
	if opts.Role != "" {
		args = append(args, "role:"+opts.Role)
	}
	if opts.Type != "" {
		args = append(args, "type:"+opts.Type)
	}
	if opts.Level != "" {
		args = append(args, "level:"+opts.Level)
	}
	return args
}

// SELinuxEnabled 报告当前系统是否启用了 SELinux。
func (l *translator) SELinuxEnabled() bool {
	return selinux.GetEnabled()
}

// fakeTranslator 是用于单元测试的假实现，按固定规则生成标签。
type fakeTranslator struct{}

var _ SELinuxLabelTranslator = &fakeTranslator{}

// NewFakeSELinuxLabelTranslator 返回单元测试用的假转换器。
// 在没有启用或不支持 SELinux 的平台上模拟真实转换器的行为。
func NewFakeSELinuxLabelTranslator() SELinuxLabelTranslator {
	return &fakeTranslator{}
}

// SELinuxOptionsToFileLabel 按固定规则为给定选项生成文件标签。
func (l *fakeTranslator) SELinuxOptionsToFileLabel(opts *v1.SELinuxOptions) (string, error) {
	if opts == nil {
		return "", nil
	}
	// 空字段使用 Fedora 的系统默认值填充，模拟真实转换器的行为。
	user := opts.User
	if user == "" {
		user = "system_u"
	}

	role := opts.Role
	if role == "" {
		role = "object_r"
	}

	// opts 描述容器内进程的上下文，需把进程类型 container_t 换成文件类型 container_file_t。
	// 上下文的其他部分在进程和文件之间保持一致。
	fileType := opts.Type
	if fileType == "" || fileType == "container_t" {
		fileType = "container_file_t"
	}

	level := opts.Level
	if level == "" {
		// level 为空时真实转换器会随机分配，这里用固定值便于断言。
		level = "s0:c998,c999"
	}

	ctx := fmt.Sprintf("%s:%s:%s:%s", user, role, fileType, level)
	return ctx, nil
}

// SELinuxEnabled 在测试中始终返回 true，保证走转换逻辑。
func (l *fakeTranslator) SELinuxEnabled() bool {
	return true
}

// SELinuxLabelTranslationError 表示 SELinux 标签转换失败。
type SELinuxLabelTranslationError struct {
	msg string
}

// Error 返回标签转换失败的具体原因。
func (e *SELinuxLabelTranslationError) Error() string {
	return e.msg
}

// IsSELinuxLabelTranslationError 判断错误链中是否包含 SELinuxLabelTranslationError。
func IsSELinuxLabelTranslationError(err error) bool {
	var seLinuxError *SELinuxLabelTranslationError
	return errors.As(err, &seLinuxError)
}

// SupportsSELinuxContextMount 检查给定卷是否支持 -o context 方式的 SELinux 挂载。
func SupportsSELinuxContextMount(volumeSpec *volume.Spec, volumePluginMgr *volume.VolumePluginMgr) (bool, error) {
	plugin, _ := volumePluginMgr.FindPluginBySpec(volumeSpec)
	if plugin != nil {
		return plugin.SupportsSELinuxContextMount(volumeSpec)
	}

	return false, nil
}

// VolumeSupportsSELinuxMount 判断给定卷的访问模式是否允许带 SELinux 挂载选项挂载。
// 需要 feature gate 开启，且默认 gate 开启时要求卷只有一个 RWOP 访问模式。
func VolumeSupportsSELinuxMount(volumeSpec *volume.Spec) bool {
	if !utilfeature.DefaultFeatureGate.Enabled(features.SELinuxMountReadWriteOncePod) {
		return false
	}
	if volumeSpec.PersistentVolume == nil {
		return false
	}
	if utilfeature.DefaultFeatureGate.Enabled(features.SELinuxMount) {
		return true
	}

	// 仅开启了 SELinuxMountReadWriteOncePod 特性时，需要按访问模式判断。
	if len(volumeSpec.PersistentVolume.Spec.AccessModes) != 1 {
		// RWOP 卷必须只有一个访问模式，避免与其他节点共享写权限。
		return false
	}
	if !v1helper.ContainsAccessMode(volumeSpec.PersistentVolume.Spec.AccessModes, v1.ReadWriteOncePod) {
		// 不是 RWOP 卷，不能安全地按 SELinux 标签独占挂载。
		return false
	}
	// 是 RWOP 卷
	return true
}

// MultipleSELinuxLabelsError 表示同一个卷被 Pod 内多个容器挂载且各自使用不同的 SELinux 标签。
type MultipleSELinuxLabelsError struct {
	labels []string
}

// Error 列出发现的所有 SELinux 标签。
func (e *MultipleSELinuxLabelsError) Error() string {
	return fmt.Sprintf("multiple SELinux labels found: %s", strings.Join(e.labels, ","))
}

// Labels 返回导致冲突的所有 SELinux 标签。
func (e *MultipleSELinuxLabelsError) Labels() []string {
	return e.labels
}

// IsMultipleSELinuxLabelsError 判断错误链中是否包含 MultipleSELinuxLabelsError。
func IsMultipleSELinuxLabelsError(err error) bool {
	var multiError *MultipleSELinuxLabelsError
	return errors.As(err, &multiError)
}

// AddSELinuxMountOption 向挂载选项列表追加 -o context="XYZ"。
func AddSELinuxMountOption(options []string, seLinuxContext string) []string {
	if !utilfeature.DefaultFeatureGate.Enabled(features.SELinuxMountReadWriteOncePod) {
		return options
	}
	// SELinux 上下文里 MCS 范围自带逗号，必须用双引号包住整个 context 值。
	// 例如：dirsync,context="system_u:object_r:container_file_t:s0:c15,c25",noatime
	return append(options, fmt.Sprintf("context=%q", seLinuxContext))
}

// SELinuxLabelInfo 描述为 Pod 挂载卷时应使用的 SELinux 标签信息。
type SELinuxLabelInfo struct {
	// SELinuxMountLabel 是挂载卷时应使用的 SELinux 标签。
	// 仅当卷插件支持 SELinuxMount 且 Pod 没有通过 SELinuxChangePolicy 退出时为非空。
	SELinuxMountLabel string
	// SELinuxProcessLabel 是容器运行时为 Pod 使用的 SELinux 标签。
	// 无论卷插件是否支持 SELinuxMount、Pod 是否退出，该字段都会被设置。
	SELinuxProcessLabel string
	// PluginSupportsSELinuxContextMount 表示卷插件是否支持 SELinux 挂载。
	PluginSupportsSELinuxContextMount bool
}

// GetMountSELinuxLabel 返回挂载给定卷时应使用的 SELinux 标签。
// effectiveSELinuxContainerLabels 来自 volumeutil.GetPodVolumeNames，包含 Pod 内所有使用该卷的容器的 SELinuxOptions；
// 容器未设置时用 PodSecurityContext.SELinuxOptions 补齐。
// 本函数不判断卷访问模式：feature gate 由调用方检查，调用方还需按 gate / 访问模式 / 标签上报不同指标。
func GetMountSELinuxLabel(volumeSpec *volume.Spec, effectiveSELinuxContainerLabels []*v1.SELinuxOptions, podSecurityContext *v1.PodSecurityContext, volumePluginMgr *volume.VolumePluginMgr, seLinuxTranslator SELinuxLabelTranslator) (SELinuxLabelInfo, error) {
	info := SELinuxLabelInfo{}
	if !utilfeature.DefaultFeatureGate.Enabled(features.SELinuxMountReadWriteOncePod) {
		return info, nil
	}

	if !seLinuxTranslator.SELinuxEnabled() {
		return info, nil
	}

	pluginSupportsSELinuxContextMount, err := SupportsSELinuxContextMount(volumeSpec, volumePluginMgr)
	if err != nil {
		return info, err
	}

	info.PluginSupportsSELinuxContextMount = pluginSupportsSELinuxContextMount

	// 汇总所有使用该卷的容器的 SELinux 标签，用集合去重后判断是否一致。
	labels := sets.New[string]()
	for _, containerLabel := range effectiveSELinuxContainerLabels {
		lbl, err := seLinuxTranslator.SELinuxOptionsToFileLabel(containerLabel)
		if err != nil {
			fullErr := fmt.Errorf("failed to construct SELinux label from context %q: %w", containerLabel, err)
			return info, fullErr
		}
		labels.Insert(lbl)
	}

	// 一个卷只能有一个挂载标签，多标签意味着容器间标签冲突。
	if labels.Len() > 1 {
		// Pod 中多个容器用不同标签访问同一卷，无法用单一 context 挂载。
		return info, &MultipleSELinuxLabelsError{labels: labels.UnsortedList()}
	}
	if labels.Len() == 0 {
		return info, nil
	}

	lbl, _ := labels.PopAny()
	info.SELinuxProcessLabel = lbl
	info.SELinuxMountLabel = lbl

	if utilfeature.DefaultFeatureGate.Enabled(features.SELinuxChangePolicy) &&
		podSecurityContext != nil &&
		podSecurityContext.SELinuxChangePolicy != nil &&
		*podSecurityContext.SELinuxChangePolicy == v1.SELinuxChangePolicyRecursive {
		// Pod 选择递归改标签，挂载时不应再用 -o context 限制标签。
		info.SELinuxMountLabel = ""
	}

	if !pluginSupportsSELinuxContextMount {
		// 插件不支持 SELinux 挂载，不能用 -o context，否则挂载会失败。
		info.SELinuxMountLabel = ""
	}

	return info, nil
}
