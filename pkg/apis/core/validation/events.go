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
	"fmt"
	"reflect"
	"time"

	v1 "k8s.io/api/core/v1"
	eventsv1beta1 "k8s.io/api/events/v1beta1"
	apimachineryvalidation "k8s.io/apimachinery/pkg/api/validation"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"k8s.io/kubernetes/pkg/apis/core"
)

const (
	ReportingInstanceLengthLimit = 128
	ActionLengthLimit            = 128
	ReasonLengthLimit            = 128
	NoteLengthLimit              = 1024
)

// ValidateEventCreate 校验 Event 的创建请求。
// 无论请求版本如何，都必须先通过旧版校验；对于 events.k8s.io/v1 及更新版本，
// 还会执行更严格的校验。为向后兼容，core/v1 与 events/v1beta1 只做旧版校验。
func ValidateEventCreate(event *core.Event, requestVersion schema.GroupVersion) field.ErrorList {
	// 确保事件始终通过旧版校验。
	allErrs := legacyValidateEvent(event, requestVersion)
	if requestVersion == v1.SchemeGroupVersion || requestVersion == eventsv1beta1.SchemeGroupVersion {
		// 为向后兼容，不再做进一步校验。
		return allErrs
	}

	// 严格校验适用于通过 events.k8s.io/v1 API 及更新版本发起的创建请求。
	allErrs = append(allErrs, ValidateObjectMeta(&event.ObjectMeta, true, apimachineryvalidation.NameIsDNSSubdomain, field.NewPath("metadata"))...)
	allErrs = append(allErrs, validateV1EventSeries(event)...)
	zeroTime := time.Time{}
	if event.EventTime.Time == zeroTime {
		allErrs = append(allErrs, field.Required(field.NewPath("eventTime"), ""))
	}
	if event.Type != v1.EventTypeNormal && event.Type != v1.EventTypeWarning {
		allErrs = append(allErrs, field.Invalid(field.NewPath("type"), "", fmt.Sprintf("has invalid value: %v", event.Type)))
	}
	// 新 API 中事件时间和序列相关字段取代了旧的 first/lastTimestamp、count 等字段，必须保持未设置。
	if event.FirstTimestamp.Time != zeroTime {
		allErrs = append(allErrs, field.Invalid(field.NewPath("firstTimestamp"), "", "needs to be unset"))
	}
	if event.LastTimestamp.Time != zeroTime {
		allErrs = append(allErrs, field.Invalid(field.NewPath("lastTimestamp"), "", "needs to be unset"))
	}
	if event.Count != 0 {
		allErrs = append(allErrs, field.Invalid(field.NewPath("count"), "", "needs to be unset"))
	}
	if event.Source.Component != "" || event.Source.Host != "" {
		allErrs = append(allErrs, field.Invalid(field.NewPath("source"), "", "needs to be unset"))
	}
	return allErrs
}

// ValidateEventUpdate 校验 Event 的更新请求。
// 旧事件先做旧版校验；events.k8s.io/v1 及更新版本还会校验元数据更新、
// 序列变化以及一系列不可变字段，并对 eventTime 做微秒级容差的不可变检查。
func ValidateEventUpdate(newEvent, oldEvent *core.Event, requestVersion schema.GroupVersion) field.ErrorList {
	// 确保新事件始终通过旧版校验。
	allErrs := legacyValidateEvent(newEvent, requestVersion)
	if requestVersion == v1.SchemeGroupVersion || requestVersion == eventsv1beta1.SchemeGroupVersion {
		// 为向后兼容，不再做进一步校验。
		return allErrs
	}

	// 严格校验适用于通过 events.k8s.io/v1 API 及更新版本发起的更新请求。
	allErrs = append(allErrs, ValidateObjectMetaUpdate(&newEvent.ObjectMeta, &oldEvent.ObjectMeta, field.NewPath("metadata"))...)
	// 若序列发生变化，则校验新的数据。
	if !reflect.DeepEqual(newEvent.Series, oldEvent.Series) {
		allErrs = append(allErrs, validateV1EventSeries(newEvent)...)
	}

	// 以下字段不允许更新。
	allErrs = append(allErrs, ValidateImmutableField(newEvent.InvolvedObject, oldEvent.InvolvedObject, field.NewPath("involvedObject"))...)
	allErrs = append(allErrs, ValidateImmutableField(newEvent.Reason, oldEvent.Reason, field.NewPath("reason"))...)
	allErrs = append(allErrs, ValidateImmutableField(newEvent.Message, oldEvent.Message, field.NewPath("message"))...)
	allErrs = append(allErrs, ValidateImmutableField(newEvent.Source, oldEvent.Source, field.NewPath("source"))...)
	allErrs = append(allErrs, ValidateImmutableField(newEvent.FirstTimestamp, oldEvent.FirstTimestamp, field.NewPath("firstTimestamp"))...)
	allErrs = append(allErrs, ValidateImmutableField(newEvent.LastTimestamp, oldEvent.LastTimestamp, field.NewPath("lastTimestamp"))...)
	allErrs = append(allErrs, ValidateImmutableField(newEvent.Count, oldEvent.Count, field.NewPath("count"))...)
	allErrs = append(allErrs, ValidateImmutableField(newEvent.Reason, oldEvent.Reason, field.NewPath("reason"))...)
	allErrs = append(allErrs, ValidateImmutableField(newEvent.Type, oldEvent.Type, field.NewPath("type"))...)

	// 禁止 eventTime 发生超过微秒精度的变更。
	// 容忍亚微秒级变更是为了兼容两类客户端：序列化时正确截断到微秒精度的客户端，
	// 以及使用错误的纳秒精度 protobuf 序列化的客户端。
	// 参见 https://github.com/kubernetes/kubernetes/issues/111928
	newTruncated := newEvent.EventTime.Truncate(time.Microsecond).UTC()
	oldTruncated := oldEvent.EventTime.Truncate(time.Microsecond).UTC()
	if newTruncated != oldTruncated {
		allErrs = append(allErrs, ValidateImmutableField(newEvent.EventTime, oldEvent.EventTime, field.NewPath("eventTime"))...)
	}

	allErrs = append(allErrs, ValidateImmutableField(newEvent.Action, oldEvent.Action, field.NewPath("action"))...)
	allErrs = append(allErrs, ValidateImmutableField(newEvent.Related, oldEvent.Related, field.NewPath("related"))...)
	allErrs = append(allErrs, ValidateImmutableField(newEvent.ReportingController, oldEvent.ReportingController, field.NewPath("reportingController"))...)
	allErrs = append(allErrs, ValidateImmutableField(newEvent.ReportingInstance, oldEvent.ReportingInstance, field.NewPath("reportingInstance"))...)

	return allErrs
}

// validateV1EventSeries 校验 events.k8s.io/v1 的 Event Series：count 至少为 2，且 lastObservedTime 必须设置。
func validateV1EventSeries(event *core.Event) field.ErrorList {
	allErrs := field.ErrorList{}
	zeroTime := time.Time{}
	if event.Series != nil {
		if event.Series.Count < 2 {
			allErrs = append(allErrs, field.Invalid(field.NewPath("series.count"), "", "should be at least 2"))
		}
		if event.Series.LastObservedTime.Time == zeroTime {
			allErrs = append(allErrs, field.Required(field.NewPath("series.lastObservedTime"), ""))
		}
	}
	return allErrs
}

// legacyValidateEvent 校验 Event 的语义是否合理，兼容新旧两种事件格式。
func legacyValidateEvent(event *core.Event, requestVersion schema.GroupVersion) field.ErrorList {
	allErrs := field.ErrorList{}
	// 取零值时间用于判断字段是否被设置。
	zeroTime := time.Time{}

	reportingControllerFieldName := "reportingController"
	if requestVersion == v1.SchemeGroupVersion {
		reportingControllerFieldName = "reportingComponent"
	}

	// EventTime 为零说明是旧格式事件；非零则是新格式事件，需按新规则校验。
	if event.EventTime.Time == zeroTime {
		// 确保 event.Namespace 与 involvedObject.Namespace 一致。
		if len(event.InvolvedObject.Namespace) == 0 {
			// involvedObject.Namespace 为空时，event.Namespace 也必须为空（或为 "default"，兼容旧客户端）。
			if event.Namespace != metav1.NamespaceNone && event.Namespace != metav1.NamespaceDefault {
				allErrs = append(allErrs, field.Invalid(field.NewPath("involvedObject", "namespace"), event.InvolvedObject.Namespace, "does not match event.namespace"))
			}
		} else {
			// involvedObject.Namespace 非空时，event.Namespace 必须与其一致。
			if event.Namespace != event.InvolvedObject.Namespace {
				allErrs = append(allErrs, field.Invalid(field.NewPath("involvedObject", "namespace"), event.InvolvedObject.Namespace, "does not match event.namespace"))
			}
		}

	} else {
		if len(event.InvolvedObject.Namespace) == 0 && event.Namespace != metav1.NamespaceDefault && event.Namespace != metav1.NamespaceSystem {
			allErrs = append(allErrs, field.Invalid(field.NewPath("involvedObject", "namespace"), event.InvolvedObject.Namespace, "does not match event.namespace"))
		}
		if len(event.ReportingController) == 0 {
			allErrs = append(allErrs, field.Required(field.NewPath(reportingControllerFieldName), ""))
		}
		allErrs = append(allErrs, ValidateQualifiedName(event.ReportingController, field.NewPath(reportingControllerFieldName))...)
		if len(event.ReportingInstance) == 0 {
			allErrs = append(allErrs, field.Required(field.NewPath("reportingInstance"), ""))
		}
		if len(event.ReportingInstance) > ReportingInstanceLengthLimit {
			allErrs = append(allErrs, field.Invalid(field.NewPath("reportingInstance"), "", fmt.Sprintf("can have at most %v characters", ReportingInstanceLengthLimit)))
		}
		if len(event.Action) == 0 {
			allErrs = append(allErrs, field.Required(field.NewPath("action"), ""))
		}
		if len(event.Action) > ActionLengthLimit {
			allErrs = append(allErrs, field.Invalid(field.NewPath("action"), "", fmt.Sprintf("can have at most %v characters", ActionLengthLimit)))
		}
		if len(event.Reason) == 0 {
			allErrs = append(allErrs, field.Required(field.NewPath("reason"), ""))
		}
		if len(event.Reason) > ReasonLengthLimit {
			allErrs = append(allErrs, field.Invalid(field.NewPath("reason"), "", fmt.Sprintf("can have at most %v characters", ReasonLengthLimit)))
		}
		if len(event.Message) > NoteLengthLimit {
			allErrs = append(allErrs, field.Invalid(field.NewPath("message"), "", fmt.Sprintf("can have at most %v characters", NoteLengthLimit)))
		}
	}
	return allErrs
}
