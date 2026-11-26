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

package storage

import (
	"fmt"
	"strings"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	apimachineryversion "k8s.io/apimachinery/pkg/util/version"
	"k8s.io/apiserver/pkg/util/compatibility"
	basecompatibility "k8s.io/component-base/compatibility"
)

// ResourceEncodingConfig 决定资源在 etcd 中的存储版本与内存中的内部版本。
// 存储版本影响持久化格式与滚动升级/降级兼容性，因此必须稳定。
type ResourceEncodingConfig interface {
	// 返回资源的序列化版本。TODO：逻辑上可能有多个「匹配」的 Kind，
	// 这里本应返回 GroupVersionKind；当前为兼容旧行为只返回 GroupVersion。
	StorageEncodingFor(schema.GroupResource) (schema.GroupVersion, error)

	// 返回存储层应转换到的内存表示版本。
	InMemoryEncodingFor(schema.GroupResource) (schema.GroupVersion, error)
}

// CompatibilityResourceEncodingConfig 在存储版本选择上考虑版本仿真（emulation）。
type CompatibilityResourceEncodingConfig interface {
	BackwardCompatibileStorageEncodingFor(schema.GroupResource, runtime.Object) (schema.GroupVersion, error)
}

// DefaultResourceEncodingConfig 是 ResourceEncodingConfig 的默认实现。
type DefaultResourceEncodingConfig struct {
	// 记录针对单个资源的显式编码覆盖配置。
	resources        map[schema.GroupResource]*OverridingResourceEncoding
	scheme           *runtime.Scheme
	effectiveVersion basecompatibility.EffectiveVersion
}

// OverridingResourceEncoding 保存单个资源的显式存储/内部版本覆盖。
type OverridingResourceEncoding struct {
	ExternalResourceEncoding schema.GroupVersion
	InternalResourceEncoding schema.GroupVersion
}

var _ ResourceEncodingConfig = &DefaultResourceEncodingConfig{}

// NewDefaultResourceEncodingConfig 使用全局组件注册表中的 Kubernetes 有效版本构造配置。
func NewDefaultResourceEncodingConfig(scheme *runtime.Scheme) *DefaultResourceEncodingConfig {
	return NewDefaultResourceEncodingConfigForEffectiveVersion(scheme, compatibility.DefaultComponentGlobalsRegistry.EffectiveVersionFor(basecompatibility.DefaultKubeComponent))
}

// NewDefaultResourceEncodingConfigForEffectiveVersion 允许显式指定有效版本，
// 便于测试或独立组件复用。
func NewDefaultResourceEncodingConfigForEffectiveVersion(scheme *runtime.Scheme, effectiveVersion basecompatibility.EffectiveVersion) *DefaultResourceEncodingConfig {
	return &DefaultResourceEncodingConfig{resources: map[schema.GroupResource]*OverridingResourceEncoding{}, scheme: scheme, effectiveVersion: effectiveVersion}
}

// SetResourceEncoding 为指定资源设置显式的存储版本与内部版本覆盖。
func (o *DefaultResourceEncodingConfig) SetResourceEncoding(resourceBeingStored schema.GroupResource, externalEncodingVersion, internalVersion schema.GroupVersion) {
	o.resources[resourceBeingStored] = &OverridingResourceEncoding{
		ExternalResourceEncoding: externalEncodingVersion,
		InternalResourceEncoding: internalVersion,
	}
}

// SetEffectiveVersion 替换有效版本；应在 server 启动完成前调用。
func (o *DefaultResourceEncodingConfig) SetEffectiveVersion(effectiveVersion basecompatibility.EffectiveVersion) {
	o.effectiveVersion = effectiveVersion
}

// StorageEncodingFor 返回资源的存储版本：优先显式覆盖，否则用组内最高优先级的
// 外部版本。组未注册时报错，避免静默写入错误版本。
func (o *DefaultResourceEncodingConfig) StorageEncodingFor(resource schema.GroupResource) (schema.GroupVersion, error) {
	if !o.scheme.IsGroupRegistered(resource.Group) {
		return schema.GroupVersion{}, fmt.Errorf("group %q is not registered in scheme", resource.Group)
	}

	resourceOverride, resourceExists := o.resources[resource]
	if resourceExists {
		return resourceOverride.ExternalResourceEncoding, nil
	}

	// 未配置覆盖项时，取该组优先级最高的外部版本。
	return o.scheme.PrioritizedVersionsForGroup(resource.Group)[0], nil
}

// BackwardCompatibileStorageEncodingFor 返回兼容（可回滚）的存储版本。
// 与 StorageEncodingFor 的区别在于会依据仿真版本挑选 n-1 可回滚的版本，
// 保证降级到上一版本时仍能读取数据。
func (o *DefaultResourceEncodingConfig) BackwardCompatibileStorageEncodingFor(resource schema.GroupResource, example runtime.Object) (schema.GroupVersion, error) {
	if !o.scheme.IsGroupRegistered(resource.Group) {
		return schema.GroupVersion{}, fmt.Errorf("group %q is not registered in scheme", resource.Group)
	}

	// Always respect overrides
	resourceOverride, resourceExists := o.resources[resource]
	if resourceExists {
		return resourceOverride.ExternalResourceEncoding, nil
	}

	return emulatedStorageVersion(
		o.scheme.PrioritizedVersionsForGroup(resource.Group)[0],
		example,
		o.effectiveVersion,
		o.scheme)
}

// InMemoryEncodingFor 返回内存中的内部版本；未覆盖时默认为组的 internal 版本。
func (o *DefaultResourceEncodingConfig) InMemoryEncodingFor(resource schema.GroupResource) (schema.GroupVersion, error) {
	if !o.scheme.IsGroupRegistered(resource.Group) {
		return schema.GroupVersion{}, fmt.Errorf("group %q is not registered in scheme", resource.Group)
	}

	resourceOverride, resourceExists := o.resources[resource]
	if resourceExists {
		return resourceOverride.InternalResourceEncoding, nil
	}
	return schema.GroupVersion{Group: resource.Group, Version: runtime.APIVersionInternal}, nil
}

// 由 types.go 中 "k8s:prerelease-lifecycle-gen:introduced" 标记生成的对象接口。
type introducedInterface interface {
	APILifecycleIntroduced() (major, minor int)
}

// emulatedStorageVersion 在版本仿真开启时选择存储版本。
// 策略是两轮扫描：先找在 MinCompatibilityVersion 之前引入的非 alpha 版本
// （保证降级 n-1 仍可读），再放宽到 EmulationVersion 之前引入的版本。
// 两级都找不到说明该资源在当前仿真版本下本就不应被服务，退回二进制版本。
func emulatedStorageVersion(binaryVersionOfResource schema.GroupVersion, example runtime.Object, effectiveVersion basecompatibility.EffectiveVersion, scheme *runtime.Scheme) (schema.GroupVersion, error) {
	if example == nil || effectiveVersion == nil {
		return binaryVersionOfResource, nil
	}

	// 在 scheme 中按示例对象找出同 Group-Kind 的所有版本，取生命周期覆盖当前仿真版本的
	// 最高优先级版本。找不到时用二进制版本（此时该 API 本就应被禁用）。
	gvks, _, err := scheme.ObjectKinds(example)
	if err != nil {
		return schema.GroupVersion{}, err
	}

	var gvk schema.GroupVersionKind
	for _, item := range gvks {
		if item.Group != binaryVersionOfResource.Group {
			continue
		}

		gvk = item
		break
	}

	if len(gvk.Kind) == 0 {
		return schema.GroupVersion{}, fmt.Errorf("object %T has no GVKs registered in scheme", example)
	}

	// VersionsForGroupKind 按优先级返回版本列表。
	versions := scheme.VersionsForGroupKind(schema.GroupKind{Group: gvk.Group, Kind: gvk.Kind})

	compatibilityVersion := effectiveVersion.MinCompatibilityVersion()
	emulationVersion := effectiveVersion.EmulationVersion()

	// 返回该 kind 版本首次引入的发行版本；没有生命周期信息，或兼容版本为 0.0
	//（测试用于启用全部 API）时返回 nil。
	introducedVersion := func(gv schema.GroupVersion) (*apimachineryversion.Version, error) {
		obj, err := scheme.New(schema.GroupVersionKind{Group: gv.Group, Version: gv.Version, Kind: gvk.Kind})
		if err != nil {
			return nil, err
		}
		if introduced, ok := obj.(introducedInterface); ok && (compatibilityVersion.Major() > 0 || compatibilityVersion.Minor() > 0) {
			major, minor := introduced.APILifecycleIntroduced()
			return apimachineryversion.MajorMinor(uint(major), uint(minor)), nil
		}
		return nil, nil
	}

	// 第一轮：找在 MinCompatibilityVersion 前引入、可回退 n-1 的最高优先级非 alpha 版本。
	// alpha API 没有任何兼容性承诺，保留对其的回退兼容没有意义。
	for _, gv := range versions {
		if gv.Version == runtime.APIVersionInternal || strings.Contains(gv.Version, "alpha") {
			continue
		}
		ver, err := introducedVersion(gv)
		if err != nil {
			return schema.GroupVersion{}, err
		}
		if ver == nil || !ver.GreaterThan(compatibilityVersion) {
			return gv, nil
		}
	}

	// 第二轮：没有可回退的非 alpha 版本时（如只有 alpha、或前一版本是 alpha 的新 beta），
	// 退而求其次，找在 EmulationVersion 前引入的最高优先级版本。
	for _, gv := range versions {
		if gv.Version == runtime.APIVersionInternal {
			continue
		}
		ver, err := introducedVersion(gv)
		if err != nil {
			return schema.GroupVersion{}, err
		}
		if ver == nil || !ver.GreaterThan(emulationVersion) {
			return gv, nil
		}
	}

	// 走到这里说明所有版本都在仿真版本之后引入，该资源在此仿真版本下本就不应提供服务。
	return binaryVersionOfResource, nil
}
