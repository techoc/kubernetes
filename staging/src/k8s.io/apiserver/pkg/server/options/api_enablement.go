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

package options

import (
	"fmt"
	"strings"

	"github.com/spf13/pflag"

	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apiserver/pkg/server"
	"k8s.io/apiserver/pkg/server/resourceconfig"
	serverstore "k8s.io/apiserver/pkg/server/storage"
	cliflag "k8s.io/component-base/cli/flag"
	"k8s.io/klog/v2"
)

// APIEnablementOptions 包含要开启和关闭哪些资源的选项。
// 对小型聚合 API server 而言此选项是必需的；"普通" API server 通常不需要
//
// 对应 --runtime-config 参数：以 key=value 的形式覆盖默认的资源启用状态。
type APIEnablementOptions struct {
	RuntimeConfig cliflag.ConfigurationMap
}

// NewAPIEnablementOptions 创建带空 RuntimeConfig 的选项对象。
func NewAPIEnablementOptions() *APIEnablementOptions {
	return &APIEnablementOptions{
		RuntimeConfig: make(cliflag.ConfigurationMap),
	}
}

// AddFlags 为指定 APIServer 向给定 FlagSet 添加 flag
func (s *APIEnablementOptions) AddFlags(fs *pflag.FlagSet) {
	if s == nil {
		return
	}
	fs.Var(&s.RuntimeConfig, "runtime-config", ""+
		"A set of key=value pairs that enable or disable built-in APIs. Supported options are:\n"+
		"v1=true|false for the core API group\n"+
		"<group>/<version>=true|false for a specific API group and version (e.g. apps/v1=true)\n"+
		"api/all=true|false controls all API versions\n"+
		"api/ga=true|false controls all API versions of the form v[0-9]+\n"+
		"api/beta=true|false controls all API versions of the form v[0-9]+beta[0-9]+\n"+
		"api/alpha=true|false controls all API versions of the form v[0-9]+alpha[0-9]+\n"+
		"api/legacy is deprecated, and will be removed in a future version")
}

// Validate 用一组 registry 校验 RuntimeConfig。
// 通常该列表只有一个元素，即进程的 apiserver registry。
// 但在委托 apiserver 这种高级（通常不推荐）场景中可能有多个。
// Validate 会过滤掉每个 registry 中已知的组，
// 若还有剩余则报错。
//
// 注意：这里只校验“组是否存在”，不校验具体版本/资源是否可用，那部分由
// MergeAPIResourceConfigs 在 ApplyTo 中处理。
func (s *APIEnablementOptions) Validate(registries ...GroupRegistry) []error {
	if s == nil {
		return nil
	}

	errors := []error{}
	if s.RuntimeConfig[resourceconfig.APIAll] == "false" && len(s.RuntimeConfig) == 1 {
		// 不允许只设置 api/all=false，这样 apiserver 启动毫无意义。
		return append(errors, fmt.Errorf("invalid runtime-config with only %v=false", resourceconfig.APIAll))
	}

	groupVersions, err := resourceconfig.ParseGroupVersions(s.RuntimeConfig)
	if err != nil {
		return append(errors, err)
	}

	unknownGroupVersions := sets.New[string]()
	for _, groupVersion := range groupVersions {
		if !isGroupRegistered(groupVersion.Group, registries) {
			unknownGroupVersions.Insert(groupVersion.String())
		}
	}
	if len(unknownGroupVersions) != 0 {
		errors = append(errors, fmt.Errorf("unknown api groups %s", strings.Join(sets.List(unknownGroupVersions), ",")))
	}

	return errors
}

// ApplyTo 用默认值与 registry 覆盖 MergedResourceConfig
//
// 合并默认资源启用状态与 --runtime-config 覆盖项；若启用了版本模拟
// （EmulationForwardCompatible），再按模拟版本调整启用集合。
func (s *APIEnablementOptions) ApplyTo(c *server.Config, defaultResourceConfig *serverstore.ResourceConfig, registry resourceconfig.GroupVersionRegistry) error {
	if s == nil {
		return nil
	}

	mergedResourceConfig, err := resourceconfig.MergeAPIResourceConfigs(defaultResourceConfig, s.RuntimeConfig, registry)
	if err != nil {
		return err
	}
	// 在适用时为 API enablement 应用前向兼容的模拟行为。
	if c.EmulationForwardCompatible {
		mergedResourceConfig, err = resourceconfig.EmulationForwardCompatibleResourceConfig(mergedResourceConfig, s.RuntimeConfig, registry)
	}

	c.MergedResourceConfig = mergedResourceConfig

	binVersion, emulatedVersion := c.EffectiveVersion.BinaryVersion(), c.EffectiveVersion.EmulationVersion()
	// 模拟版本低于二进制版本时，开启 alpha API 属于不受支持的行为：
	// 只告警而不拒绝启动，给运维保留显式承担风险的余地。
	if binVersion != nil && emulatedVersion != nil && (binVersion.Major() != emulatedVersion.Major() || binVersion.Minor() != emulatedVersion.Minor()) {
		for _, version := range registry.PrioritizedVersionsAllGroups() {
			if strings.Contains(version.Version, "alpha") {
				// 发出警告前先检查该 alpha API 是否确实已启用
				entireVersionEnabled := c.MergedResourceConfig.ExplicitGroupVersionConfigs[version]
				individualResourceEnabled := false
				for resource, enabled := range c.MergedResourceConfig.ExplicitResourceConfigs {
					if enabled && resource.Group == version.Group && resource.Version == version.Version {
						individualResourceEnabled = true
						break
					}
				}
				if entireVersionEnabled || individualResourceEnabled {
					klog.Warningf("alpha api enabled with emulated version %s instead of the binary's version %s, this is unsupported, proceed at your own risk: api=%s", emulatedVersion, binVersion, version.String())
				}
			}
		}
	}

	return err
}

// GroupRegistry 提供检查给定组是否已注册的方法。
type GroupRegistry interface {
	// 给定组已注册时 IsGroupRegistered 返回 true。
	IsGroupRegistered(group string) bool
}

// isGroupRegistered 只要任一 registry 认识该组就算通过；用于组合多个
// apiserver 的 registry（委托场景）。
func isGroupRegistered(group string, registries []GroupRegistry) bool {
	for _, registry := range registries {
		if registry.IsGroupRegistered(group) {
			return true
		}
	}
	return false
}
