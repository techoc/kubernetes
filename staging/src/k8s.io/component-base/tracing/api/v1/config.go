/*
Copyright 2021 The Kubernetes Authors.

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
	"net/url"
	"strings"

	"k8s.io/apimachinery/pkg/util/validation/field"
	"k8s.io/component-base/featuregate"
)

var (
	// 采样率以“每百万”为单位，避免浮点数；上限即 100% 采样。
	maxSamplingRatePerMillion = int32(1000000)
)

// ValidateTracingConfiguration validates the tracing configuration
//
// ValidateTracingConfiguration 只校验显式设置的可选字段；配置为 nil
// 表示未启用 tracing，直接视为合法。
// 返回字段路径化的错误，便于在 --config 校验时定位到具体字段。
func ValidateTracingConfiguration(traceConfig *TracingConfiguration, featureGate featuregate.FeatureGate, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}
	if traceConfig == nil {
		return allErrs
	}
	if traceConfig.SamplingRatePerMillion != nil {
		allErrs = append(allErrs, validateSamplingRate(*traceConfig.SamplingRatePerMillion, fldPath.Child("samplingRatePerMillion"))...)
	}
	if traceConfig.Endpoint != nil {
		allErrs = append(allErrs, validateEndpoint(*traceConfig.Endpoint, fldPath.Child("endpoint"))...)
	}
	return allErrs
}

// validateSamplingRate 校验采样率落在 [0, 1e6] 内；负数与超过 100% 都无意义。
func validateSamplingRate(rate int32, fldPath *field.Path) field.ErrorList {
	errs := field.ErrorList{}
	if rate < 0 {
		errs = append(errs, field.Invalid(
			fldPath, rate,
			"sampling rate must be positive",
		))
	}
	if rate > maxSamplingRatePerMillion {
		errs = append(errs, field.Invalid(
			fldPath, rate,
			"sampling rate per million must be less than or equal to one million",
		))
	}
	return errs
}

// validateEndpoint 校验 gRPC collector 端点。
// 未写 scheme 时按 dns:// 处理，因此裸 host:port 也是合法输入。
func validateEndpoint(endpoint string, fldPath *field.Path) field.ErrorList {
	errs := field.ErrorList{}
	if !strings.Contains(endpoint, "//") {
		endpoint = "dns://" + endpoint
	}
	url, err := url.Parse(endpoint)
	if err != nil {
		errs = append(errs, field.Invalid(
			fldPath, endpoint,
			err.Error(),
		))
		return errs
	}
	// gRPC 的 naming 只支持这几种 scheme；其他值会在运行期失败，提前拒绝。
	switch url.Scheme {
	case "dns":
	case "unix":
	case "unix-abstract":
	default:
		errs = append(errs, field.Invalid(
			fldPath, endpoint,
			fmt.Sprintf("unsupported scheme: %v.  Options are none, dns, unix, or unix-abstract.  See https://github.com/grpc/grpc/blob/master/doc/naming.md", url.Scheme),
		))
	}
	return errs
}
