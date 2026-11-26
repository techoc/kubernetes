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

package resourceconfig

import (
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/version"
	serverstore "k8s.io/apiserver/pkg/server/storage"
	cliflag "k8s.io/component-base/cli/flag"
)

// GroupVersionRegistry 提供对已注册组版本的查询。
//
// 抽象出该接口是为了让 resourceconfig 不直接依赖 scheme 的具体实现，便于测试替换。
type GroupVersionRegistry interface {
	// 给定组是否已注册。
	IsGroupRegistered(group string) bool
	// 给定版本是否已注册。
	IsVersionRegistered(v schema.GroupVersion) bool
	// 按优先级返回所有已注册的组版本。
	PrioritizedVersionsAllGroups() []schema.GroupVersion
	// 按优先级返回单个组下的版本。
	PrioritizedVersionsForGroup(group string) []schema.GroupVersion
}

// MergeResourceEncodingConfigs 把默认编码配置与指定的 GroupVersionResource 覆盖项合并。
//
// 覆盖项的存储版本固定为内部版本：外部版本只用于编解码，etcd 中统一存内部形态。
func MergeResourceEncodingConfigs(
	defaultResourceEncoding *serverstore.DefaultResourceEncodingConfig,
	resourceEncodingOverrides []schema.GroupVersionResource,
) *serverstore.DefaultResourceEncodingConfig {
	resourceEncodingConfig := defaultResourceEncoding
	for _, gvr := range resourceEncodingOverrides {
		resourceEncodingConfig.SetResourceEncoding(gvr.GroupResource(), gvr.GroupVersion(),
			schema.GroupVersion{Group: gvr.Group, Version: runtime.APIVersionInternal})
	}
	return resourceEncodingConfig
}

// --runtime-config 参数可识别的 API 组开关值。
//
// 这些是 --runtime-config 的聚合开关，可一次启用/禁用一类 API 版本。
const (
	APIAll   = "api/all"
	APIGA    = "api/ga"
	APIBeta  = "api/beta"
	APIAlpha = "api/alpha"
)

// 版本字符串的分类正则：GA 为 vN，beta 为 vNbetaM，alpha 为 vNalphaM。
// 匹配基于版本名而非组，因此同一开关会作用于所有组。
var (
	gaPattern    = regexp.MustCompile(`^v\d+$`)
	betaPattern  = regexp.MustCompile(`^v\d+beta\d+$`)
	alphaPattern = regexp.MustCompile(`^v\d+alpha\d+$`)

	groupVersionMatchers = map[string]func(gv schema.GroupVersion) bool{
		// 匹配所有 API 版本
		APIAll: func(gv schema.GroupVersion) bool { return true },
		// 匹配 v[0-9]+ 形式的所有 GA 版本
		APIGA: func(gv schema.GroupVersion) bool { return gaPattern.MatchString(gv.Version) },
		// 匹配所有 beta 版本
		APIBeta: func(gv schema.GroupVersion) bool { return betaPattern.MatchString(gv.Version) },
		// 匹配所有 alpha 版本
		APIAlpha: func(gv schema.GroupVersion) bool { return alphaPattern.MatchString(gv.Version) },
	}

	// 固定应用顺序：先 api/all，再按成熟度收窄，后者可覆盖前者的结果。
	groupVersionMatchersOrder = []string{APIAll, APIGA, APIBeta, APIAlpha}
)

// MergeAPIResourceConfigs 把默认资源配置与覆盖项合并。
// 未注册的组会被跳过；组已注册但版本未注册则报错。
//
// 两级覆盖：先处理 api/* 聚合开关，再处理显式的 group/version[/resource] 项，
// 后者优先级更高（见 applyVersionAndResourcePreferences）。
func MergeAPIResourceConfigs(
	defaultAPIResourceConfig *serverstore.ResourceConfig,
	resourceConfigOverrides cliflag.ConfigurationMap,
	registry GroupVersionRegistry,
) (*serverstore.ResourceConfig, error) {
	resourceConfig := defaultAPIResourceConfig
	overrides := resourceConfigOverrides

	for _, flag := range groupVersionMatchersOrder {
		if value, ok := overrides[flag]; ok {
			if value == "false" {
				resourceConfig.DisableMatchingVersions(groupVersionMatchers[flag])
			} else if value == "true" {
				resourceConfig.EnableMatchingVersions(groupVersionMatchers[flag])
			} else {
				return nil, fmt.Errorf("invalid value %v=%v", flag, value)
			}
		}
	}
	if err := applyVersionAndResourcePreferences(resourceConfig, overrides, registry); err != nil {
		return nil, err
	}
	return resourceConfig, nil
}

// applyVersionAndResourcePreferences 解析 group/version 与 group/version/resource 形式的覆盖项。
//
// 两阶段应用是刻意的：版本偏好先落地，资源偏好后落地，从而保证资源级配置拥有最高优先级，
// 并能覆盖“版本被禁用但个别资源仍需启用”的场景。
func applyVersionAndResourcePreferences(
	resourceConfig *serverstore.ResourceConfig,
	overrides cliflag.ConfigurationMap,
	registry GroupVersionRegistry,
) error {
	type versionEnablementPreference struct {
		key          string
		enabled      bool
		groupVersion schema.GroupVersion
	}
	type resourceEnablementPreference struct {
		key                  string
		enabled              bool
		groupVersionResource schema.GroupVersionResource
	}
	versionPreferences := []versionEnablementPreference{}
	resourcePreferences := []resourceEnablementPreference{}

	// "<resourceSpecifier>={true|false}" 用于启用/禁用 API，优先级高于 api/all。
	// 遍历 runtimeConfig 中指定的所有 group/version 覆盖项。
	for key := range overrides {
		// 聚合开关已在上面处理，这里跳过。
		if _, ok := groupVersionMatchers[key]; ok {
			continue
		}

		tokens := strings.Split(key, "/")
		// 非 2/3 段的键不是本函数的职责，直接忽略，交由其他配置路径处理。
		if len(tokens) < 2 || len(tokens) > 3 {
			continue
		}
		groupVersionString := tokens[0] + "/" + tokens[1]
		groupVersion, err := schema.ParseGroupVersion(groupVersionString)
		if err != nil {
			return fmt.Errorf("invalid key %s", key)
		}

		// 跳过未注册的组。
		//
		// 未注册的组静默跳过：允许同一份配置在不同发行版（组件集不同）上复用。
		if !registry.IsGroupRegistered(groupVersion.Group) {
			continue
		}

		// 校验组版本确已注册。
		//
		// 组已注册但版本未注册则是配置错误，必须报错而不是静默忽略。
		if !registry.IsVersionRegistered(groupVersion) {
			return fmt.Errorf("group version %s that has not been registered", groupVersion.String())
		}
		enabled, err := getRuntimeConfigValue(overrides, key, false)
		if err != nil {
			return err
		}

		switch len(tokens) {
		case 2:
			versionPreferences = append(versionPreferences, versionEnablementPreference{
				key:          key,
				enabled:      enabled,
				groupVersion: groupVersion,
			})
		case 3:
			// 资源名在 API 中始终为小写复数；大小写不符通常是拼写错误，提前拒绝。
			if strings.ToLower(tokens[2]) != tokens[2] {
				return fmt.Errorf("invalid key %v: group/version/resource and resource is always lowercase plural, not %q", key, tokens[2])
			}
			resourcePreferences = append(resourcePreferences, resourceEnablementPreference{
				key:                  key,
				enabled:              enabled,
				groupVersionResource: groupVersion.WithResource(tokens[2]),
			})
		}
	}

	// 先应用版本级偏好，覆盖此前硬编码的同类设置。
	for _, versionPreference := range versionPreferences {
		if versionPreference.enabled {
			// "group/version=true"：启用该组版本
			resourceConfig.ExplicitlyEnableVersions(versionPreference.groupVersion)

		} else {
			// "group/version=false"：禁用该组版本
			resourceConfig.ExplicitlyDisableVersions(versionPreference.groupVersion)
		}
	}

	// 最后应用资源级偏好，使其拥有最高优先级。
	for _, resourcePreference := range resourcePreferences {
		if resourcePreference.enabled {
			// "group/version/resource=true"：启用该资源
			resourceConfig.ExplicitlyEnableResources(resourcePreference.groupVersionResource)
		} else {
			resourceConfig.ExplicitlyDisableResources(resourcePreference.groupVersionResource)
		}
	}
	return nil
}

// getRuntimeConfigValue 解析单个开关的布尔值。
// 空字符串按 true 处理（`--runtime-config=foo/bar` 等价于显式开启），这是命令行惯例。
func getRuntimeConfigValue(overrides cliflag.ConfigurationMap, apiKey string, defaultValue bool) (bool, error) {
	flagValue, ok := overrides[apiKey]
	if ok {
		if flagValue == "" {
			return true, nil
		}
		boolValue, err := strconv.ParseBool(flagValue)
		if err != nil {
			return false, fmt.Errorf("invalid value of %s: %s, err: %v", apiKey, flagValue, err)
		}
		return boolValue, nil
	}
	return defaultValue, nil
}

// ParseGroups 从 resourceConfig 中解析出组名。
//
// 与 ParseGroupVersions 不同，这里保留重复的组名（未去重），由调用方决定是否需要去重。
func ParseGroups(resourceConfig cliflag.ConfigurationMap) ([]string, error) {
	groupVersions, err := ParseGroupVersions(resourceConfig)
	if err != nil {
		return nil, err
	}
	groups := []string{}
	for _, gv := range groupVersions {
		groups = append(groups, gv.Group)
	}
	return groups, nil
}

// ParseGroupVersions 从 resourceConfig 中解析出组版本。
//
// 与 MergeAPIResourceConfigs 不同，这里不做注册表校验，纯粹做语法解析，
// 因此允许解析尚未注册的组版本（用于预检配置）。
func ParseGroupVersions(resourceConfig cliflag.ConfigurationMap) ([]schema.GroupVersion, error) {
	groupVersions := []schema.GroupVersion{}
	for key := range resourceConfig {
		if _, ok := groupVersionMatchers[key]; ok {
			continue
		}
		tokens := strings.Split(key, "/")
		if len(tokens) != 2 && len(tokens) != 3 {
			return nil, fmt.Errorf("runtime-config invalid key %s", key)
		}
		groupVersionString := tokens[0] + "/" + tokens[1]
		groupVersion, err := schema.ParseGroupVersion(groupVersionString)
		if err != nil {
			return nil, fmt.Errorf("runtime-config invalid key %s", key)
		}
		groupVersions = append(groupVersions, groupVersion)
	}

	return groupVersions, nil
}

// EmulationForwardCompatibleResourceConfig 基于现有配置新建 ResourceConfig，
// 额外启用已启用资源的所有更高优先级版本（不含 alpha），
// 用于在新 API 版本引入时保证向前兼容。
//
// 用途：模拟“升级后”的 API 面，使旧版本客户端在启用新版本前就能感知更高优先级的版本。
// alpha 版本不参与，因为其契约不稳定，不应被隐式启用。
func EmulationForwardCompatibleResourceConfig(
	resourceConfig *serverstore.ResourceConfig,
	resourceConfigOverrides cliflag.ConfigurationMap,
	registry GroupVersionRegistry,
) (*serverstore.ResourceConfig, error) {
	ret := serverstore.NewResourceConfig()
	for gv, enabled := range resourceConfig.GroupVersionConfigs {
		ret.GroupVersionConfigs[gv] = enabled
		if !enabled {
			continue
		}
		// alpha API 不适用向前兼容模拟。
		if alphaPattern.MatchString(gv.Version) {
			continue
		}
		// 组版本启用时，所有更高优先级版本（即列表中排在该版本之前的）也应隐式启用。
		//
		// 按 Kubernetes 版本语义降序排列（v2 优先于 v1beta1），在遇到当前版本前逐个启用。
		prioritizedVersions := registry.PrioritizedVersionsForGroup(gv.Group)
		sort.Slice(prioritizedVersions, func(i, j int) bool {
			return version.CompareKubeAwareVersionStrings(prioritizedVersions[i].Version, prioritizedVersions[j].Version) > 0
		})
		for _, pgv := range prioritizedVersions {
			if pgv.Version == gv.Version {
				break
			}
			ret.EnableVersions(pgv)
		}
	}
	for gvr, enabled := range resourceConfig.ResourceConfigs {
		ret.ResourceConfigs[gvr] = enabled
		if !enabled {
			continue
		}
		// alpha API 不适用向前兼容模拟。
		if alphaPattern.MatchString(gvr.Version) {
			continue
		}
		// 资源启用时，同名资源的更高优先级版本（即列表中排在该版本之前的）也应隐式启用。
		//
		// 资源级同理，但只在同一资源名下启用更高版本，不影响同组的其他资源。
		prioritizedVersions := registry.PrioritizedVersionsForGroup(gvr.Group)
		sort.Slice(prioritizedVersions, func(i, j int) bool {
			return version.CompareKubeAwareVersionStrings(prioritizedVersions[i].Version, prioritizedVersions[j].Version) > 0
		})
		for _, pgv := range prioritizedVersions {
			if pgv.Version == gvr.Version {
				break
			}
			ret.EnableResources(pgv.WithResource(gvr.Resource))
		}
	}
	// 存在更高优先级版本的覆盖项时，需要重新应用用户偏好。
	//
	// 重新应用用户覆盖项：上面为兼容性隐式启用的版本可能与本应禁用的显式配置冲突，
	// 必须让显式配置最终生效。
	if err := applyVersionAndResourcePreferences(ret, resourceConfigOverrides, registry); err != nil {
		return nil, err
	}
	return ret, nil
}
