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

package v1

import (
	"unsafe"

	"k8s.io/apimachinery/pkg/conversion"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/controller-manager/config"
)

// 包初始化时把本文件的转换函数注册进 localSchemeBuilder，
// 之后 AddToScheme 会一并执行，使 scheme 具备 v1 <-> 内部版本的转换能力。
func init() {
	localSchemeBuilder.Register(RegisterConversions)
}

// v1 版本的锁类型常量；v1 只支持 leases。
const ResourceLockLeases = "leases"

// RegisterConversions 向给定 scheme 注册转换函数；
// 导出是为了让调用方能自行组装任意 scheme。
// 向 scheme 注册自动生成的双向转换函数。
// 公开该函数是为了让调用方能构造任意 scheme 时也能复用这些转换。
func RegisterConversions(s *runtime.Scheme) error {
	// 逐个注册：ControllerLeaderConfiguration 的两个方向。
	if err := s.AddGeneratedConversionFunc((*ControllerLeaderConfiguration)(nil), (*config.ControllerLeaderConfiguration)(nil), func(a, b interface{}, scope conversion.Scope) error {
		return Convert_v1_ControllerLeaderConfiguration_To_config_ControllerLeaderConfiguration(a.(*ControllerLeaderConfiguration), b.(*config.ControllerLeaderConfiguration), scope)
	}); err != nil {
		return err
	}
	if err := s.AddGeneratedConversionFunc((*config.ControllerLeaderConfiguration)(nil), (*ControllerLeaderConfiguration)(nil), func(a, b interface{}, scope conversion.Scope) error {
		return Convert_config_ControllerLeaderConfiguration_To_v1_ControllerLeaderConfiguration(a.(*config.ControllerLeaderConfiguration), b.(*ControllerLeaderConfiguration), scope)
	}); err != nil {
		return err
	}
	// 再注册 LeaderMigrationConfiguration 的两个方向。
	if err := s.AddGeneratedConversionFunc((*LeaderMigrationConfiguration)(nil), (*config.LeaderMigrationConfiguration)(nil), func(a, b interface{}, scope conversion.Scope) error {
		return Convert_v1_LeaderMigrationConfiguration_To_config_LeaderMigrationConfiguration(a.(*LeaderMigrationConfiguration), b.(*config.LeaderMigrationConfiguration), scope)
	}); err != nil {
		return err
	}
	if err := s.AddGeneratedConversionFunc((*config.LeaderMigrationConfiguration)(nil), (*LeaderMigrationConfiguration)(nil), func(a, b interface{}, scope conversion.Scope) error {
		return Convert_config_LeaderMigrationConfiguration_To_v1_LeaderMigrationConfiguration(a.(*config.LeaderMigrationConfiguration), b.(*LeaderMigrationConfiguration), scope)
	}); err != nil {
		return err
	}
	return nil
}

// 内部版本 -> v1：直接复制字段；列表用 unsafe 指针转换复用同一块内存（两个结构布局相同）。
// v1 没有 ResourceLock 字段，因此无需（也无法）输出它。
func Convert_config_LeaderMigrationConfiguration_To_v1_LeaderMigrationConfiguration(in *config.LeaderMigrationConfiguration, out *LeaderMigrationConfiguration, s conversion.Scope) error {
	out.LeaderName = in.LeaderName
	out.ControllerLeaders = *(*[]ControllerLeaderConfiguration)(unsafe.Pointer(&in.ControllerLeaders))
	return nil
}

// v1 -> 内部版本：复制字段，并显式把 ResourceLock 设为默认的 "leases"。
func Convert_v1_LeaderMigrationConfiguration_To_config_LeaderMigrationConfiguration(in *LeaderMigrationConfiguration, out *config.LeaderMigrationConfiguration, s conversion.Scope) error {
	out.LeaderName = in.LeaderName
	out.ControllerLeaders = *(*[]config.ControllerLeaderConfiguration)(unsafe.Pointer(&in.ControllerLeaders))
	// v1 配置中没有锁类型，转换时统一补上 leases。
	out.ResourceLock = ResourceLockLeases
	return nil
}

// ControllerLeaderConfiguration 字段一一对应，逐个复制即可。
func Convert_v1_ControllerLeaderConfiguration_To_config_ControllerLeaderConfiguration(in *ControllerLeaderConfiguration, out *config.ControllerLeaderConfiguration, s conversion.Scope) error {
	out.Name = in.Name
	out.Component = in.Component
	return nil
}

// 反向转换同上，没有需要特殊处理的字段。
func Convert_config_ControllerLeaderConfiguration_To_v1_ControllerLeaderConfiguration(in *config.ControllerLeaderConfiguration, out *ControllerLeaderConfiguration, s conversion.Scope) error {
	out.Name = in.Name
	out.Component = in.Component
	return nil
}
