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

// Package apparmor 提供 AppArmor 配置文件的验证和辅助功能。
// AppArmor 是 Linux 内核的安全模块，用于限制进程的能力。
// 该包主要功能：
//   - 验证节点是否支持 AppArmor（内核和构建层面）
//   - 验证 Pod 请求的 AppArmor profile 是否有效
//   - 获取容器应使用的 AppArmor profile（支持优先级：container > annotation > pod）
package apparmor

import (
	"errors"
	"fmt"
	"strings"

	v1 "k8s.io/api/core/v1"
	podutil "k8s.io/kubernetes/pkg/api/v1/pod"
	"k8s.io/kubernetes/third_party/forked/libcontainer/apparmor"
)

// isDisabledBuild 标记 AppArmor 是否在构建时被禁用。
// 当构建标签不是 linux 时（见 validate_disabled.go），该值设为 true。
// 非 Linux 平台不支持 AppArmor，因此直接禁用。
var isDisabledBuild bool

// Validator 接口用于验证带有 AppArmor profile 的 Pod 是否可以在节点上运行。
// Validate 检查 Pod 级别的 profile 配置是否有效。
// ValidateHost 检查节点是否支持 AppArmor。
type Validator interface {
	Validate(pod *v1.Pod) error
	ValidateHost() error
}

// NewValidator 创建一个新的 AppArmor 验证器。
// 在创建时会立即检查主机是否支持 AppArmor，如果不支持，
// 返回的验证器会在后续 Validate 调用中返回预存的错误。
// 这种设计避免了重复检查主机状态，因为 AppArmor 支持在运行时不会动态变化。
func NewValidator() Validator {
	if err := validateHost(); err != nil {
		return &validator{validateHostErr: err}
	}
	return &validator{}
}

type validator struct {
	// validateHostErr 存储主机验证错误。
	// 如果在创建时主机不支持 AppArmor，该字段保存错误信息，
	// 后续 Validate 调用直接返回此错误，避免重复检查。
	validateHostErr error
}

// Validate 检查 Pod 的 AppArmor 配置是否有效。
// 如果 Pod 不需要 AppArmor（未指定非 Unconfined profile），直接返回 nil。
// 否则，先验证主机支持，再遍历所有容器检查 profile 名称是否合法。
func (v *validator) Validate(pod *v1.Pod) error {
	if !isRequired(pod) {
		return nil
	}

	// Pod 需要 AppArmor，但主机不支持，返回预存的错误。
	if v.ValidateHost() != nil {
		return v.validateHostErr
	}

	var retErr error
	podutil.VisitContainers(&pod.Spec, podutil.AllContainers, func(container *v1.Container, containerType podutil.ContainerType) bool {
		profile := GetProfile(pod, container)
		if profile == nil {
			return true
		}

		// 仅对 Localhost 类型的 profile 检查名称是否为空。
		// RuntimeDefault 和 Unconfined 类型不需要名称，天然有效。
		// 注意：此检查本应属于 API 验证层（validation.ValidateAppArmorProfileFormat），
		// 但此处作为额外的安全网，防止空名称到达 Kubelet。
		if profile.Type == v1.AppArmorProfileTypeLocalhost {
			if profile.LocalhostProfile == nil || strings.TrimSpace(*profile.LocalhostProfile) == "" {
				retErr = fmt.Errorf("invalid empty AppArmor profile name: %q", profile)
				return false
			}
		}
		return true
	})

	return retErr
}

// ValidateHost 验证主机和运行时是否能够执行 AppArmor profile。
// 仅在 Kubelet 启动时检查一次，不重新评估主机状态。
// 因为启用或禁用 AppArmor 支持需要重启 Kubelet，运行时不会动态变化。
func (v *validator) ValidateHost() error {
	return v.validateHostErr
}

// validateHost 验证主机和运行时是否能够执行 AppArmor profile。
// 检查两个层面：构建时是否支持 Linux，运行时内核是否启用 AppArmor。
func validateHost() error {
	// 检查构建支持：非 Linux 平台编译的二进制文件不支持 AppArmor。
	if isDisabledBuild {
		return errors.New("binary not compiled for linux")
	}

	// 检查内核支持：即使二进制文件支持，主机内核也必须启用 AppArmor。
	// apparmor.IsEnabled() 读取 /sys/kernel/security/apparmor 来确认。
	if !apparmor.IsEnabled() {
		return errors.New("AppArmor is not enabled on the host")
	}

	return nil
}
