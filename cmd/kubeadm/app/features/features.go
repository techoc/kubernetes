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

package features

import (
	"fmt"
	"sort"
	"strconv"
	"strings"

	"k8s.io/apimachinery/pkg/util/version"
	"k8s.io/component-base/featuregate"
	"k8s.io/klog/v2"

	"k8s.io/kubernetes/cmd/kubeadm/app/util/errors"
)

// Feature gate 按区分大小写的字母序排列（大写字母在小写字母之前）。
// 这样新增项会分散在文件各处，而不是挤在一起，减少合并冲突。
const (
	// RootlessControlPlane 预计在 v1.22 进入 alpha。
	RootlessControlPlane = "RootlessControlPlane"
)

// InitFeatureGates 是 init 命令使用的默认 feature gate 集合。
var InitFeatureGates = FeatureList{
	RootlessControlPlane: {FeatureSpec: featuregate.FeatureSpec{Default: false, PreRelease: featuregate.Alpha},
		DeprecationMessage: "Deprecated in favor of the core kubelet feature UserNamespacesSupport which is beta since 1.30." +
			" Once UserNamespacesSupport graduates to GA, kubeadm will start using it and RootlessControlPlane will be removed.",
	},
}

// Feature 表示一个受 feature gate 控制的功能。
type Feature struct {
	featuregate.FeatureSpec
	// MinimumVersion 是启用该 feature gate 所需的最低 Kubernetes 版本；nil 表示不额外限制。
	MinimumVersion *version.Version
	// HiddenInHelpText 控制 KnownFeatures 是否在帮助文本中隐藏该 feature gate。
	HiddenInHelpText bool
	// DeprecationMessage 是废弃 feature gate 时向用户展示的迁移说明。
	DeprecationMessage string
}

// FeatureList 是 feature gate 名称到配置的映射。
type FeatureList map[string]Feature

// ValidateVersion 检查 feature gate 列表与目标 Kubernetes 版本是否兼容。
func ValidateVersion(allFeatures FeatureList, requestedFeatures map[string]bool, requestedVersion string) error {
	if requestedVersion == "" {
		return nil
	}
	parsedExpVersion, err := version.ParseSemantic(requestedVersion)
	if err != nil {
		return errors.Wrapf(err, "error parsing version %s", requestedVersion)
	}
	for k := range requestedFeatures {
		if minVersion := allFeatures[k].MinimumVersion; minVersion != nil {
			if !parsedExpVersion.AtLeast(minVersion) {
				return errors.Errorf(
					"the requested Kubernetes version (%s) is incompatible with the %s feature gate, which needs %s as a minimum",
					requestedVersion, k, minVersion)
			}
		}
	}
	return nil
}

// Enabled 判断某个 feature gate 是否已启用。
func Enabled(featureList map[string]bool, featureName string) bool {
	if enabled, ok := featureList[featureName]; ok {
		return enabled
	}
	return InitFeatureGates[featureName].Default
}

// Supports 判断给定的 feature gate 集合是否包含该 feature。
func Supports(featureList FeatureList, featureName string) bool {
	for k := range featureList {
		if featureName == k {
			return true
		}
	}
	return false
}

// KnownFeatures 返回 FeatureList 中所有已知 feature 的名称列表。
func KnownFeatures(f *FeatureList) []string {
	var known []string
	for k, v := range *f {
		if v.HiddenInHelpText {
			continue
		}

		pre := ""
		if v.PreRelease != featuregate.GA {
			pre = fmt.Sprintf("%s - ", v.PreRelease)
		}
		known = append(known, fmt.Sprintf("%s=true|false (%sdefault=%t)", k, pre, v.Default))
	}
	sort.Strings(known)
	return known
}

// NewFeatureGate 解析 "key1=value1,key2=value2,..." 形式的字符串，
// 返回已知 key 的 map[string]bool；遇到未知 key 则报错。
func NewFeatureGate(f *FeatureList, value string) (map[string]bool, error) {
	featureGate := map[string]bool{}
	for _, s := range strings.Split(value, ",") {
		if len(s) == 0 {
			continue
		}

		arr := strings.SplitN(s, "=", 2)
		if len(arr) != 2 {
			return nil, errors.Errorf("missing bool value for feature-gate key:%s", s)
		}

		k := strings.TrimSpace(arr[0])
		v := strings.TrimSpace(arr[1])

		featureSpec, ok := (*f)[k]
		if !ok {
			return nil, errors.Errorf("unrecognized feature-gate key: %s", k)
		}

		if featureSpec.PreRelease == featuregate.Deprecated {
			klog.Warningf("Setting deprecated feature gate %s=%s. It will be removed in a future release.", k, v)
		}

		boolValue, err := strconv.ParseBool(v)
		if err != nil {
			return nil, errors.Errorf("invalid value %v for feature-gate key: %s, use true|false instead", v, k)
		}
		featureGate[k] = boolValue
	}

	return featureGate, nil
}

// CheckDeprecatedFlags 对照当前 feature gate 集合检查已有的 feature gate flag。
// 升级时用它确认旧版 kubeadm 创建的集群所用 feature gate 仍受当前版本支持。
func CheckDeprecatedFlags(f *FeatureList, features map[string]bool) map[string]string {
	deprecatedMsg := map[string]string{}
	for k := range features {
		featureSpec, ok := (*f)[k]
		if !ok {
			// 理论上不会走到这里：这个分支只是哨兵，用来在 flag 仍被使用时
			// 阻止误删（先标记废弃，过一个发布周期再删除）。
			deprecatedMsg[k] = fmt.Sprintf("Unknown feature gate flag: %s", k)
		}

		if featureSpec.PreRelease == featuregate.Deprecated {
			if _, ok := deprecatedMsg[k]; !ok {
				deprecatedMsg[k] = featureSpec.DeprecationMessage
			}
		}
	}

	return deprecatedMsg
}
