/*
Copyright 2020 The Kubernetes Authors.

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

package config

import (
	"fmt"
	"os"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	util "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/validation/field"
	internal "k8s.io/controller-manager/config"
	v1 "k8s.io/controller-manager/config/v1"
	"k8s.io/controller-manager/config/v1alpha1"
	"k8s.io/controller-manager/config/v1beta1"
)

// ResourceLockLeases 是 'leases' API 对应的 resourceLock 取值。
// 迁移配置中允许的两种资源锁类型常量。
const ResourceLockLeases = "leases"

// ResourceLockEndpoints is the resourceLock value for 'endpoints' API
const ResourceLockEndpoints = "endpoints"

// 专用于解析迁移配置文件的 scheme：注册内部版本以及所有支持的外部版本。
var cfgScheme = runtime.NewScheme()

// 依次注册 internal / v1alpha1 / v1beta1 / v1，并设置版本优先级。
// 优先级决定多版本同时存在时哪个版本为“存储/首选”版本（这里最终以 v1 优先）。
func init() {
	// internal
	util.Must(internal.AddToScheme(cfgScheme))
	// 内部版本先注册，转换的目标类型必须存在。

	// v1alpha1
	util.Must(v1alpha1.AddToScheme(cfgScheme))
	util.Must(cfgScheme.SetVersionPriority(v1alpha1.SchemeGroupVersion))

	// v1beta1
	util.Must(v1beta1.AddToScheme(cfgScheme))
	util.Must(cfgScheme.SetVersionPriority(v1beta1.SchemeGroupVersion))

	// v1
	util.Must(v1.AddToScheme(cfgScheme))
	util.Must(cfgScheme.SetVersionPriority(v1.SchemeGroupVersion))
}

// ReadLeaderMigrationConfiguration 从给定路径的 YAML 文件读取迁移配置，文件不存在时返回错误。
// 解析出的配置可能并不合法，这里只做解析：
// 1) 读文件；
// 2) 用严格模式的通用解码器解析（未知字段/重复字段会报错）；
// 3) 断言结果为内部版本类型。
// 注意：这里只做解析，不做语义校验。
func ReadLeaderMigrationConfiguration(configFilePath string) (*internal.LeaderMigrationConfiguration, error) {
	data, err := os.ReadFile(configFilePath)
	// 文件不存在或不可读时立即返回带路径的错误。
	if err != nil {
		return nil, fmt.Errorf("unable to read leader migration configuration from %q: %w", configFilePath, err)
	}
	// 严格解码：未知字段与重复字段都视为错误；External 版本会自动转换为内部版本。
	config, gvk, err := serializer.NewCodecFactory(cfgScheme, serializer.EnableStrict).UniversalDecoder().Decode(data, nil, nil)
	if err != nil {
		return nil, err
	}
	internalConfig, ok := config.(*internal.LeaderMigrationConfiguration)
	// 类型断言：确保解析出的确实是迁移配置，而不是同一 scheme 中的其它类型。
	if !ok {
		return nil, fmt.Errorf("unexpected config type: %v", gvk)
	}
	return internalConfig, nil
}

// ValidateLeaderMigrationConfiguration 校验 LeaderMigrationConfiguration 的常见错误，
// 未发现问题时返回 nil：leaderName 必填、resourceLock 只能是 leases/endpoints、
// 每个 controllerLeaders 条目的 name 与 component 必填。
func ValidateLeaderMigrationConfiguration(config *internal.LeaderMigrationConfiguration) (allErrs field.ErrorList) {
	if config.LeaderName == "" {
		// 迁移锁必须有名字，否则 KCM/CCM 无法就同一把锁达成一致。
		allErrs = append(allErrs, field.Required(field.NewPath("leaderName"),
			"leaderName must be set for LeaderMigrationConfiguration"))
	}
	if config.ResourceLock != ResourceLockLeases && config.ResourceLock != ResourceLockEndpoints {
		// 只接受两种已知锁类型。
		allErrs = append(allErrs, field.Invalid(field.NewPath("resourceLock"), config.ResourceLock,
			"resource Lock must be one of 'leases' or 'endpoints'"))
	}
	// 校验 controllerLeaders：逐项校验控制器列表，并带上数组下标作为字段路径，便于定位是哪一条出错。
	fldPath := field.NewPath("controllerLeaders")
	for i, controllerLeader := range config.ControllerLeaders {
		path := fldPath.Index(i)
		allErrs = append(allErrs, validateControllerLeaderConfiguration(path, &controllerLeader)...)
	}
	return
}

// 校验单个控制器条目；component 决定由哪个组件运行，name 必须与控制器注册名一致。
func validateControllerLeaderConfiguration(path *field.Path, config *internal.ControllerLeaderConfiguration) (allErrs field.ErrorList) {
	if config == nil {
		// nil 条目直接跳过，避免解引用 panic。
		return
	}
	if config.Component == "" {
		allErrs = append(allErrs, field.Required(path.Child("component"), ""))
	}
	if config.Name == "" {
		allErrs = append(allErrs, field.Required(path.Child("name"), ""))
	}
	return
}
