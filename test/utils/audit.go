/*
Copyright 2018 The Kubernetes Authors.

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

package utils

import (
	"bufio"
	"fmt"
	"io"
	"reflect"
	"sort"
	"strings"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apiserver/pkg/admission/plugin/webhook/mutating"
	auditinternal "k8s.io/apiserver/pkg/apis/audit"
	"k8s.io/apiserver/pkg/audit"
)

// AuditEvent 是审计事件的简化表示，用于测试目的
type AuditEvent struct {
	ID                      types.UID
	Level                   auditinternal.Level
	Stage                   auditinternal.Stage
	RequestURI              string
	Verb                    string
	Code                    int32
	StatusMessage           string
	User                    string
	ImpersonatedUser        string
	ImpersonatedUID         string
	ImpersonatedGroups      string
	ImpersonationConstraint *string
	Resource                string
	Namespace               string
	RequestObject           bool
	ResponseObject          bool
	AuthorizeDecision       string

	// 本包中的 Check 函数会接管这些 map 的所有权。调用
	// Check 函数后，不应再引用这些 map。
	AdmissionWebhookMutationAnnotations map[string]string
	AdmissionWebhookPatchAnnotations    map[string]string

	// 只有在向 testEventFromInternalFiltered 提供过滤器时才会填充
	CustomAuditAnnotations map[string]string
}

type AuditAnnotationsFilter func(key, val string) bool

// MissingEventsReport 提供关于是否有事件缺失的分析
type MissingEventsReport struct {
	FirstEventChecked *auditinternal.Event
	LastEventChecked  *auditinternal.Event
	NumEventsChecked  int
	MissingEvents     []AuditEvent
	AllEvents         []AuditEvent
}

// String 返回报告的人类可读字符串表示
func (m *MissingEventsReport) String() string {
	return fmt.Sprintf(`missing %d events

- first event checked: %#v

- last event checked: %#v

- number of events checked: %d

- missing events: %#v`, len(m.MissingEvents), m.FirstEventChecked, m.LastEventChecked, m.NumEventsChecked, m.MissingEvents)
}

// CheckAuditLines 在审计日志中搜索预期的审计行。
func CheckAuditLines(stream io.Reader, expected []AuditEvent, version schema.GroupVersion) (missingReport *MissingEventsReport, err error) {
	return CheckAuditLinesFiltered(stream, expected, version, nil)
}

// CheckAuditLinesFiltered 在审计日志中搜索预期的审计行，customAnnotationsFilter
// 控制哪些审计注解会被添加到 AuditEvent.CustomAuditAnnotations 中。
// 如果 customAnnotationsFilter 为 nil，AuditEvent.CustomAuditAnnotations 将为空。
func CheckAuditLinesFiltered(stream io.Reader, expected []AuditEvent, version schema.GroupVersion, customAnnotationsFilter AuditAnnotationsFilter) (missingReport *MissingEventsReport, err error) {
	expectations := newAuditEventTracker(expected)

	scanner := bufio.NewScanner(stream)

	missingReport = &MissingEventsReport{
		MissingEvents: expected,
	}

	var i int
	for i = 0; scanner.Scan(); i++ {
		line := scanner.Text()

		e := &auditinternal.Event{}
		decoder := audit.Codecs.UniversalDecoder(version)
		if err := runtime.DecodeInto(decoder, []byte(line), e); err != nil {
			return missingReport, fmt.Errorf("failed decoding buf: %s, apiVersion: %s", line, version)
		}
		if i == 0 {
			missingReport.FirstEventChecked = e
		}
		missingReport.LastEventChecked = e

		event, err := testEventFromInternalFiltered(e, customAnnotationsFilter)
		if err != nil {
			return missingReport, err
		}

		expectations.Mark(event)
		missingReport.AllEvents = append(missingReport.AllEvents, event)
	}
	if err := scanner.Err(); err != nil {
		return missingReport, err
	}

	missingReport.MissingEvents = expectations.Missing()
	missingReport.NumEventsChecked = i
	return missingReport, nil
}

// testEventFromInternalFiltered 接收一个内部审计事件并返回一个测试事件，customAnnotationsFilter
// 控制哪些审计注解会被添加到 AuditEvent.CustomAuditAnnotations 中。
// 如果 customAnnotationsFilter 为 nil，AuditEvent.CustomAuditAnnotations 将为空。
func testEventFromInternalFiltered(e *auditinternal.Event, customAnnotationsFilter AuditAnnotationsFilter) (AuditEvent, error) {
	event := AuditEvent{
		Level:      e.Level,
		Stage:      e.Stage,
		RequestURI: e.RequestURI,
		Verb:       e.Verb,
		User:       e.User.Username,
	}
	if e.ObjectRef != nil {
		event.Namespace = e.ObjectRef.Namespace
		event.Resource = e.ObjectRef.Resource
	}
	if e.ResponseStatus != nil {
		event.Code = e.ResponseStatus.Code
		event.StatusMessage = e.ResponseStatus.Message
	}
	if e.ResponseObject != nil {
		event.ResponseObject = true
	}
	if e.RequestObject != nil {
		event.RequestObject = true
	}
	if e.ImpersonatedUser != nil {
		event.ImpersonatedUser = e.ImpersonatedUser.Username
		event.ImpersonatedUID = e.ImpersonatedUser.UID
		sort.Strings(e.ImpersonatedUser.Groups)
		event.ImpersonatedGroups = strings.Join(e.ImpersonatedUser.Groups, ",")
	}
	if e.AuthenticationMetadata != nil {
		event.ImpersonationConstraint = &e.AuthenticationMetadata.ImpersonationConstraint
	}
	event.AuthorizeDecision = e.Annotations["authorization.k8s.io/decision"]
	for k, v := range e.Annotations {
		if strings.HasPrefix(k, mutating.PatchAuditAnnotationPrefix) {
			if event.AdmissionWebhookPatchAnnotations == nil {
				event.AdmissionWebhookPatchAnnotations = map[string]string{}
			}
			event.AdmissionWebhookPatchAnnotations[k] = v
		} else if strings.HasPrefix(k, mutating.MutationAuditAnnotationPrefix) {
			if event.AdmissionWebhookMutationAnnotations == nil {
				event.AdmissionWebhookMutationAnnotations = map[string]string{}
			}
			event.AdmissionWebhookMutationAnnotations[k] = v
		} else if customAnnotationsFilter != nil && customAnnotationsFilter(k, v) {
			if event.CustomAuditAnnotations == nil {
				event.CustomAuditAnnotations = map[string]string{}
			}
			event.CustomAuditAnnotations[k] = v
		}
	}
	return event, nil
}

// auditEvent 是 AuditEvent 之上的私有包装，由 auditEventTracker 使用
type auditEvent struct {
	event AuditEvent
	found bool
}

// auditEventTracker 跟踪对 AuditEvent 的预期，并将匹配的事件标记为已找到
type auditEventTracker struct {
	events []*auditEvent
}

// newAuditEventTracker 创建一个跟踪器，用于跟踪预期事件是否被找到
func newAuditEventTracker(expected []AuditEvent) *auditEventTracker {
	expectations := &auditEventTracker{events: []*auditEvent{}}
	for _, event := range expected {
		// 我们复制 event 中指向 map 的引用
		expectations.events = append(expectations.events, &auditEvent{event: event, found: false})
	}
	return expectations
}

// Mark 如果给定事件符合预期，则将其标记为已找到
func (t *auditEventTracker) Mark(event AuditEvent) {
	for _, e := range t.events {
		if reflect.DeepEqual(e.event, event) {
			e.found = true
		}
	}
}

// Missing 报告那些符合预期但未找到的事件
func (t *auditEventTracker) Missing() []AuditEvent {
	var missing []AuditEvent
	for _, e := range t.events {
		if !e.found {
			missing = append(missing, e.event)
		}
	}
	return missing
}
