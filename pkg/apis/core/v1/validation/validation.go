/*
Copyright 2014 The Kubernetes Authors.

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

package validation

import (
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/validation/field"
)

const isNegativeErrorMsg string = `must be greater than or equal to 0`

var validLogStreams = sets.New[string](
	v1.LogStreamStdout,
	v1.LogStreamStderr,
	v1.LogStreamAll,
)

// ValidatePodLogOptions 校验 Pod 日志请求中已设置选项的取值是否合法，所有错误都会汇总到 ErrorList 返回。
// 主要约束：tailLines 非负、limitBytes 与 sinceSeconds 大于 0、sinceTime 与 sinceSeconds 互斥、
// stream 必须为合法值，且指定具体 stream 时不允许同时指定 tailLines。
func ValidatePodLogOptions(opts *v1.PodLogOptions) field.ErrorList {
	allErrs := field.ErrorList{}
	if opts.TailLines != nil && *opts.TailLines < 0 {
		allErrs = append(allErrs, field.Invalid(field.NewPath("tailLines"), *opts.TailLines, isNegativeErrorMsg))
	}
	if opts.LimitBytes != nil && *opts.LimitBytes < 1 {
		allErrs = append(allErrs, field.Invalid(field.NewPath("limitBytes"), *opts.LimitBytes, "must be greater than 0"))
	}
	switch {
	case opts.SinceSeconds != nil && opts.SinceTime != nil:
		allErrs = append(allErrs, field.Forbidden(field.NewPath(""), "at most one of `sinceTime` or `sinceSeconds` may be specified"))
	case opts.SinceSeconds != nil:
		if *opts.SinceSeconds < 1 {
			allErrs = append(allErrs, field.Invalid(field.NewPath("sinceSeconds"), *opts.SinceSeconds, "must be greater than 0"))
		}
	}
	// 若请求没有携带任何 URL 参数，默认值填充可能不会执行，因此 opts.Stream 可能为 nil。
	if opts.Stream != nil {
		if !validLogStreams.Has(*opts.Stream) {
			allErrs = append(allErrs, field.NotSupported(field.NewPath("stream"), *opts.Stream, validLogStreams.UnsortedList()))
		}
		if *opts.Stream != v1.LogStreamAll && opts.TailLines != nil {
			allErrs = append(allErrs, field.Forbidden(field.NewPath(""), "`tailLines` and specific `stream` are mutually exclusive for now"))
		}
	}
	return allErrs
}
