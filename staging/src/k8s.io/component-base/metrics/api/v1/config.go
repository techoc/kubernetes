/*
Copyright 2025 The Kubernetes Authors.

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
	"fmt"
	"os"
	"path/filepath"
	"regexp"

	"github.com/blang/semver/v4"
	"go.yaml.in/yaml/v2"
	"k8s.io/apimachinery/pkg/util/validation/field"
)

var (
	// labelExpr、metricNameExpr 遵循 Prometheus 的标签/指标命名规则，
	// 用于在配置加载阶段尽早拒绝非法名称。
	labelExpr                = `[a-zA-Z_][a-zA-Z0-9_]*`
	metricNameExpr           = `[a-zA-Z_:][a-zA-Z0-9_:]*`
	allowListMappingKeyRegex = regexp.MustCompile(`^` + metricNameExpr + `,` + labelExpr + `$`)
	metricNameRegex          = regexp.MustCompile(`^` + metricNameExpr + `$`)
)

// Validate 校验 MetricsConfiguration 的各字段，并检查互斥项：
// allowListMapping 与 allowListMappingManifest 不能同时设置。
func Validate(c *MetricsConfiguration, currentVersion semver.Version, fldPath *field.Path) field.ErrorList {
	errs := field.ErrorList{}
	if c == nil {
		return errs
	}
	errs = append(errs, validateShowHiddenMetricsVersion(currentVersion, c.ShowHiddenMetricsForVersion, fldPath.Child("showHiddenMetricsForVersion"))...)
	errs = append(errs, validateDisabledMetrics(c.DisabledMetrics, fldPath.Child("disabledMetrics"))...)
	errs = append(errs, validateAllowListMapping(c.AllowListMapping, fldPath.Child("allowListMapping"))...)
	errs = append(errs, validateAllowListMappingManifest(c.AllowListMappingManifest, fldPath.Child("allowListMappingManifest"))...)
	// 两种来源会互相覆盖，同时出现时语义不明确，直接拒绝。
	if c.AllowListMapping != nil && c.AllowListMappingManifest != "" {
		errs = append(errs, field.Invalid(fldPath, c, "cannot specify both allowListMapping and allowListMappingManifest"))
	}

	return errs
}

// validateAllowListMapping 校验映射 key 必须是 "<metricName>,<labelName>" 格式。
func validateAllowListMapping(allowListMapping map[string]string, fldPath *field.Path) field.ErrorList {
	errs := field.ErrorList{}
	for k := range allowListMapping {
		if !allowListMappingKeyRegex.MatchString(k) {
			return append(errs, field.Invalid(fldPath, allowListMapping, fmt.Sprintf("must have keys with format `metricName,labelName` where metricName matches %q and labelName matches %q", metricNameExpr, labelExpr)))
		}
	}

	return errs
}

// validateAllowListMappingManifest validates the allow list mapping manifest file.
// This function is used to validate the manifest file provided via the flag --allow-metric-labels-manifest, or the configuration file.
// In the former case, the path resolution is relative to the current working directory.
// In the latter case, the path resolution is relative to the configuration file's location, and components are required to pass in the resolved absolute path.
// NOTE: If its the latter case, components are expected to pass in the *absolute* path to the manifest file.
func validateAllowListMappingManifest(allowListMappingManifestPath string, fldPath *field.Path) field.ErrorList {
	errs := field.ErrorList{}
	if allowListMappingManifestPath == "" {
		return errs
	}
	data, err := os.ReadFile(filepath.Clean(allowListMappingManifestPath))
	if err != nil {
		return append(errs, field.Invalid(fldPath, allowListMappingManifestPath, fmt.Errorf("failed to read allow list manifest: %w", err).Error()))
	}
	allowListMapping := make(map[string]string)
	err = yaml.Unmarshal(data, &allowListMapping)
	if err != nil {
		return append(errs, field.Invalid(fldPath, allowListMappingManifestPath, fmt.Errorf("failed to parse allow list manifest: %w", err).Error()))
	}
	// 清单内容与内联映射使用同一套 key 规则，避免两份配置语义不一致。
	allowListMappingErrs := validateAllowListMapping(allowListMapping, fldPath)
	if len(allowListMappingErrs) > 0 {
		return append(errs, field.Invalid(fldPath, allowListMappingManifestPath, fmt.Sprintf("invalid allow list mapping in manifest: %v", allowListMappingErrs)))
	}

	return errs
}

// validateDisabledMetrics 要求被禁用的指标名是完整限定名（符合 Prometheus 命名规则）。
func validateDisabledMetrics(names []string, fldPath *field.Path) field.ErrorList {
	errs := field.ErrorList{}
	for _, name := range names {
		if !metricNameRegex.MatchString(name) {
			return append(errs, field.Invalid(fldPath, names, fmt.Sprintf("must be fully qualified metric names matching %q, got %q", metricNameRegex.String(), name)))
		}
	}

	return errs
}

// validateShowHiddenMetricsVersion 只允许省略或填写上一个次版本：
// 展示隐藏指标的目的是让人在上一个版本中注意到即将被移除的指标，
// 因此其他版本没有意义。
func validateShowHiddenMetricsVersion(currentVersion semver.Version, targetVersionStr string, fldPath *field.Path) field.ErrorList {
	errs := field.ErrorList{}
	if targetVersionStr == "" {
		return errs
	}

	validVersionStr := fmt.Sprintf("%d.%d", currentVersion.Major, currentVersion.Minor-1)
	if targetVersionStr != validVersionStr {
		return append(errs, field.Invalid(fldPath, targetVersionStr, fmt.Sprintf("must be omitted or have the value '%v'; only the previous minor version is allowed", validVersionStr)))
	}

	return errs
}

// ValidateShowHiddenMetricsVersionForKubeletBackwardCompatOnly 校验 Kubelet 旧版字段
// showHiddenMetricsForVersion。保留它是为了兼容 Kubelet 逐字段暴露配置的历史。
// TODO: This is kept here for backward compatibility in Kubelet (as metrics configuration fields were exposed on an individual basis earlier).
// TODO: Revisit this after Kubelet supports the new metrics configuration API: https://github.com/kubernetes/kubernetes/pull/123426
func ValidateShowHiddenMetricsVersionForKubeletBackwardCompatOnly(currentVersion semver.Version, targetVersionStr string) error {
	errs := validateShowHiddenMetricsVersion(currentVersion, targetVersionStr, field.NewPath("showHiddenMetricsForVersion"))
	if len(errs) > 0 {
		return fmt.Errorf("invalid showHiddenMetricsForVersion: %v", errs.ToAggregate().Error())
	}

	return nil
}
