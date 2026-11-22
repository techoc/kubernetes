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

package admission

import (
	"context"
	"fmt"

	"k8s.io/apiserver/pkg/audit"
)

// auditHandler logs annotations set by other admission handlers
//
// 通过装饰器而非在每个插件里散落审计调用，保证注解收集与插件实现解耦。
type auditHandler struct {
	Interface
}

var _ Interface = &auditHandler{}
var _ MutationInterface = &auditHandler{}
var _ ValidationInterface = &auditHandler{}

// WithAudit is a decorator for a admission phase. It saves annotations
// of attribute into the audit event. Attributes passed to the Admit and
// Validate function must be instance of privateAnnotationsGetter or
// AnnotationsGetter, otherwise an error is returned.
func WithAudit(i Interface) Interface {
	if i == nil {
		return i
	}
	return &auditHandler{Interface: i}
}

// Admit 先做注解能力检查，再委托给内层变异插件，最后把注解写入审计事件。
// 即使内层插件返回错误也仍然记录注解：失败请求的上下文同样有审计价值。
func (handler *auditHandler) Admit(ctx context.Context, a Attributes, o ObjectInterfaces) error {
	// 插件不处理该操作时直接跳过，避免无谓的注解检查。
	if !handler.Interface.Handles(a.GetOperation()) {
		return nil
	}
	if err := ensureAnnotationGetter(a); err != nil {
		return err
	}
	var err error
	if mutator, ok := handler.Interface.(MutationInterface); ok {
		err = mutator.Admit(ctx, a, o)
		handler.logAnnotations(ctx, a)
	}
	return err
}

// Validate 与 Admit 对称：只对 ValidationInterface 实现生效。
func (handler *auditHandler) Validate(ctx context.Context, a Attributes, o ObjectInterfaces) error {
	if !handler.Interface.Handles(a.GetOperation()) {
		return nil
	}
	if err := ensureAnnotationGetter(a); err != nil {
		return err
	}
	var err error
	if validator, ok := handler.Interface.(ValidationInterface); ok {
		err = validator.Validate(ctx, a, o)
		handler.logAnnotations(ctx, a)
	}
	return err
}

// ensureAnnotationGetter 校验 attributes 具备提供审计注解的能力。
// 两种接口都接受：privateAnnotationsGetter 供内置插件使用，
// AnnotationsGetter 供外部插件使用。
func ensureAnnotationGetter(a Attributes) error {
	_, okPrivate := a.(privateAnnotationsGetter)
	_, okPublic := a.(AnnotationsGetter)
	if okPrivate || okPublic {
		return nil
	}
	return fmt.Errorf("attributes must be an instance of privateAnnotationsGetter or AnnotationsGetter")
}

// logAnnotations 按事件级别收集注解并合并进当前审计事件。
// 级别决定了注解的详略（例如 Metadata 级别不收集请求体相关注解）。
func (handler *auditHandler) logAnnotations(ctx context.Context, a Attributes) {
	ae := audit.AuditContextFrom(ctx)
	if ae == nil {
		return
	}

	var annotations map[string]string
	switch a := a.(type) {
	case privateAnnotationsGetter:
		annotations = a.getAnnotations(ae.GetEventLevel())
	case AnnotationsGetter:
		annotations = a.GetAnnotations(ae.GetEventLevel())
	default:
		// this will never happen, because we have already checked it in ensureAnnotationGetter
	}

	audit.AddAuditAnnotationsMap(ctx, annotations)
}
