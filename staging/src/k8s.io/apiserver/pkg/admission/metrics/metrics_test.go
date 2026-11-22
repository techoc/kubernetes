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

package metrics

import (
	"context"
	"fmt"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apiserver/pkg/admission"
	"k8s.io/component-base/metrics/legacyregistry"
)

// 共享的测试属性：指标只关心标签（operation/type/rejected 等），因此这里
// 用固定的 GVK/GVR 和一次 CREATE 请求即可，无需构造真实对象。
var (
	kind     = schema.GroupVersionKind{Group: "kgroup", Version: "kversion", Kind: "kind"}
	resource = schema.GroupVersionResource{Group: "rgroup", Version: "rversion", Resource: "resource"}
	attr     = admission.NewAttributesRecord(nil, nil, kind, "ns", "name", resource, "subresource", admission.Create, &metav1.CreateOptions{}, false, nil)
)

// TestObserveAdmissionStep 验证 WithStepMetrics 会在 admit 与 validate 两个
// 阶段分别记录直方图与摘要，且 rejected 标签为 false。
func TestObserveAdmissionStep(t *testing.T) {
	// 指标注册在全局 registry 中，测试结束必须清理，否则会污染同包其它用例。
	defer Metrics.reset()
	defer legacyregistry.Reset()
	handler := WithStepMetrics(&mutatingAndValidatingFakeHandler{admission.NewHandler(admission.Create), true, true})
	if err := handler.(admission.MutationInterface).Admit(context.TODO(), attr, nil); err != nil {
		t.Errorf("Unexpected error in admit: %v", err)
	}
	if err := handler.(admission.ValidationInterface).Validate(context.TODO(), attr, nil); err != nil {
		t.Errorf("Unexpected error in validate: %v", err)
	}
	wantLabels := map[string]string{
		"operation": string(admission.Create),
		"type":      "admit",
		"rejected":  "false",
	}
	expectHistogramCountTotal(t, "apiserver_admission_step_admission_duration_seconds", wantLabels, 1)
	expectFindMetric(t, "apiserver_admission_step_admission_duration_seconds_summary", wantLabels)

	wantLabels["type"] = "validate"
	expectHistogramCountTotal(t, "apiserver_admission_step_admission_duration_seconds", wantLabels, 1)
	expectFindMetric(t, "apiserver_admission_step_admission_duration_seconds_summary", wantLabels)
}

// TestObserveAdmissionController 验证 WithControllerMetrics 为每个插件名
// 单独打标签；name 标签是区分不同 admission controller 的关键维度。
func TestObserveAdmissionController(t *testing.T) {
	defer Metrics.reset()
	defer legacyregistry.Reset()
	handler := WithControllerMetrics(&mutatingAndValidatingFakeHandler{admission.NewHandler(admission.Create), true, true}, "a")
	if err := handler.(admission.MutationInterface).Admit(context.TODO(), attr, nil); err != nil {
		t.Errorf("Unexpected error in admit: %v", err)
	}
	if err := handler.(admission.ValidationInterface).Validate(context.TODO(), attr, nil); err != nil {
		t.Errorf("Unexpected error in validate: %v", err)
	}
	wantLabels := map[string]string{
		"name":      "a",
		"operation": string(admission.Create),
		"type":      "admit",
		"rejected":  "false",
	}
	expectHistogramCountTotal(t, "apiserver_admission_controller_admission_duration_seconds", wantLabels, 1)

	wantLabels["type"] = "validate"
	expectHistogramCountTotal(t, "apiserver_admission_controller_admission_duration_seconds", wantLabels, 1)
}

// TestObserveWebhook 验证 webhook 请求计数与耗时直方图；非 200/300 的
// 状态码会被归类为 "600"（表示非 HTTP 状态），用于区分“没拿到响应”。
func TestObserveWebhook(t *testing.T) {
	defer Metrics.reset()
	defer legacyregistry.Reset()
	Metrics.ObserveWebhook(context.TODO(), "x", 2*time.Second, false, attr, stepAdmit, 200)
	Metrics.ObserveWebhook(context.TODO(), "x", 2*time.Second, true, attr, stepValidate, 654)
	wantLabelsCounterAdmit := map[string]string{
		"name":      "x",
		"operation": string(admission.Create),
		"type":      "admit",
		"code":      "200",
		"rejected":  "false",
	}
	wantLabelsCounterValidate := map[string]string{
		"name":      "x",
		"operation": string(admission.Create),
		"type":      "validate",
		"code":      "600",
		"rejected":  "true",
	}
	wantLabelsHistogramAdmit := map[string]string{
		"name":      "x",
		"operation": string(admission.Create),
		"type":      "admit",
		"rejected":  "false",
	}
	wantLabelsHistogramValidate := map[string]string{
		"name":      "x",
		"operation": string(admission.Create),
		"type":      "validate",
		"rejected":  "true",
	}

	expectCounterValue(t, "apiserver_admission_webhook_request_total", wantLabelsCounterAdmit, 1)
	expectCounterValue(t, "apiserver_admission_webhook_request_total", wantLabelsCounterValidate, 1)

	expectHistogramCountTotal(t, "apiserver_admission_webhook_admission_duration_seconds", wantLabelsHistogramAdmit, 1)
	expectHistogramCountTotal(t, "apiserver_admission_webhook_admission_duration_seconds", wantLabelsHistogramValidate, 1)
}

// TestObserveWebhookRejection 验证拒绝计数按 error_type 与 rejection_code
// 区分；rejection_code=0 表示 apiserver 内部错误（webhook 未返回有效状态码）。
func TestObserveWebhookRejection(t *testing.T) {
	Metrics.reset()
	Metrics.ObserveWebhookRejection(context.TODO(), "x", stepAdmit, string(admission.Create), WebhookRejectionNoError, 500)
	Metrics.ObserveWebhookRejection(context.TODO(), "x", stepAdmit, string(admission.Create), WebhookRejectionNoError, 654)
	Metrics.ObserveWebhookRejection(context.TODO(), "x", stepValidate, string(admission.Update), WebhookRejectionCallingWebhookError, 501)
	Metrics.ObserveWebhookRejection(context.TODO(), "x", stepValidate, string(admission.Update), WebhookRejectionAPIServerInternalError, 0)
	wantLabels := map[string]string{
		"name":           "x",
		"operation":      string(admission.Create),
		"type":           "admit",
		"error_type":     "no_error",
		"rejection_code": "500",
	}
	wantLabels600 := map[string]string{
		"name":           "x",
		"operation":      string(admission.Create),
		"type":           "admit",
		"error_type":     "no_error",
		"rejection_code": "600",
	}
	wantLabelsCallingWebhookError := map[string]string{
		"name":           "x",
		"operation":      string(admission.Update),
		"type":           "validate",
		"error_type":     "calling_webhook_error",
		"rejection_code": "501",
	}
	wantLabelsAPIServerInternalError := map[string]string{
		"name":           "x",
		"operation":      string(admission.Update),
		"type":           "validate",
		"error_type":     "apiserver_internal_error",
		"rejection_code": "0",
	}
	expectCounterValue(t, "apiserver_admission_webhook_rejection_count", wantLabels, 1)
	expectCounterValue(t, "apiserver_admission_webhook_rejection_count", wantLabels600, 1)
	expectCounterValue(t, "apiserver_admission_webhook_rejection_count", wantLabelsCallingWebhookError, 1)
	expectCounterValue(t, "apiserver_admission_webhook_rejection_count", wantLabelsAPIServerInternalError, 1)
}

// TestObserveWebhookFailOpen 验证 failurePolicy=FailOpen 且调用失败时的计数，
// 这类事件意味着请求被放行但准入保障已失效，运维上需要关注。
func TestObserveWebhookFailOpen(t *testing.T) {
	defer Metrics.reset()
	defer legacyregistry.Reset()
	Metrics.ObserveWebhookFailOpen(context.TODO(), "x", stepAdmit)
	Metrics.ObserveWebhookFailOpen(context.TODO(), "x", stepValidate)
	wantLabelsCounterAdmit := map[string]string{
		"name": "x",
		"type": "admit",
	}
	wantLabelsCounterValidate := map[string]string{
		"name": "x",
		"type": "validate",
	}
	expectCounterValue(t, "apiserver_admission_webhook_fail_open_count", wantLabelsCounterAdmit, 1)
	expectCounterValue(t, "apiserver_admission_webhook_fail_open_count", wantLabelsCounterValidate, 1)
}

// TestObserveMatchConditionEvalError 覆盖 webhook 与 policy 两类来源在
// admit/validate 两个阶段下的 matchCondition 求值错误计数。
func TestObserveMatchConditionEvalError(t *testing.T) {
	defer Metrics.reset()
	defer legacyregistry.Reset()
	Metrics.ObserveMatchConditionEvalError(context.TODO(), "x", kindWebhook, stepAdmit, string(admission.Create))
	Metrics.ObserveMatchConditionEvalError(context.TODO(), "x", kindWebhook, stepValidate, string(admission.Create))
	Metrics.ObserveMatchConditionEvalError(context.TODO(), "x", kindPolicy, stepAdmit, string(admission.Create))
	Metrics.ObserveMatchConditionEvalError(context.TODO(), "x", kindPolicy, stepValidate, string(admission.Create))
	wantLabelsCounterPolicyAdmit := map[string]string{
		"name":      "x",
		"type":      "admit",
		"kind":      "policy",
		"operation": string(admission.Create),
	}
	wantLabelsCounterPolicyValidate := map[string]string{
		"name":      "x",
		"type":      "validate",
		"kind":      "policy",
		"operation": string(admission.Create),
	}
	wantLabelsCounterWebhookAdmit := map[string]string{
		"name":      "x",
		"type":      "admit",
		"kind":      "webhook",
		"operation": string(admission.Create),
	}
	wantLabelsCounterWebhookValidate := map[string]string{
		"name":      "x",
		"type":      "validate",
		"kind":      "webhook",
		"operation": string(admission.Create),
	}
	expectCounterValue(t, "apiserver_admission_match_condition_evaluation_errors_total", wantLabelsCounterPolicyAdmit, 1)
	expectCounterValue(t, "apiserver_admission_match_condition_evaluation_errors_total", wantLabelsCounterPolicyValidate, 1)
	expectCounterValue(t, "apiserver_admission_match_condition_evaluation_errors_total", wantLabelsCounterWebhookAdmit, 1)
	expectCounterValue(t, "apiserver_admission_match_condition_evaluation_errors_total", wantLabelsCounterWebhookValidate, 1)
}

// TestObserveMatchConditionExclusion 验证 matchCondition 返回 false 导致
// hook 被跳过时的计数；这类“排除”不等于错误，需要独立指标区分。
func TestObserveMatchConditionExclusion(t *testing.T) {
	defer Metrics.reset()
	defer legacyregistry.Reset()
	Metrics.ObserveMatchConditionExclusion(context.TODO(), "x", kindWebhook, stepAdmit, string(admission.Create))
	Metrics.ObserveMatchConditionExclusion(context.TODO(), "x", kindWebhook, stepValidate, string(admission.Create))
	Metrics.ObserveMatchConditionExclusion(context.TODO(), "x", kindPolicy, stepAdmit, string(admission.Create))
	Metrics.ObserveMatchConditionExclusion(context.TODO(), "x", kindPolicy, stepValidate, string(admission.Create))
	wantLabelsCounterPolicyAdmit := map[string]string{
		"name":      "x",
		"type":      "admit",
		"kind":      "policy",
		"operation": string(admission.Create),
	}
	wantLabelsCounterPolicyValidate := map[string]string{
		"name":      "x",
		"type":      "validate",
		"kind":      "policy",
		"operation": string(admission.Create),
	}
	wantLabelsCounterWebhookAdmit := map[string]string{
		"name":      "x",
		"type":      "admit",
		"kind":      "webhook",
		"operation": string(admission.Create),
	}
	wantLabelsCounterWebhookValidate := map[string]string{
		"name":      "x",
		"type":      "validate",
		"kind":      "webhook",
		"operation": string(admission.Create),
	}
	metrics, _ := legacyregistry.DefaultGatherer.Gather()
	for _, mf := range metrics {
		t.Logf("%v", *mf.Name)
	}
	expectCounterValue(t, "apiserver_admission_match_condition_exclusions_total", wantLabelsCounterPolicyAdmit, 1)
	expectCounterValue(t, "apiserver_admission_match_condition_exclusions_total", wantLabelsCounterPolicyValidate, 1)
	expectCounterValue(t, "apiserver_admission_match_condition_exclusions_total", wantLabelsCounterWebhookAdmit, 1)
	expectCounterValue(t, "apiserver_admission_match_condition_exclusions_total", wantLabelsCounterWebhookValidate, 1)
}

// TestObserveMatchConditionEvaluationTime 验证求值耗时直方图按来源/阶段打标签。
func TestObserveMatchConditionEvaluationTime(t *testing.T) {
	defer Metrics.reset()
	defer legacyregistry.Reset()
	Metrics.ObserveMatchConditionEvaluationTime(context.TODO(), 2*time.Second, "x", kindPolicy, stepAdmit, string(admission.Create))
	Metrics.ObserveMatchConditionEvaluationTime(context.TODO(), 2*time.Second, "x", kindPolicy, stepValidate, string(admission.Create))
	Metrics.ObserveMatchConditionEvaluationTime(context.TODO(), 2*time.Second, "x", kindWebhook, stepAdmit, string(admission.Create))
	Metrics.ObserveMatchConditionEvaluationTime(context.TODO(), 2*time.Second, "x", kindWebhook, stepValidate, string(admission.Create))
	wantLabelsCounterPolicyAdmit := map[string]string{
		"name":      "x",
		"type":      "admit",
		"kind":      "policy",
		"operation": string(admission.Create),
	}
	wantLabelsCounterPolicyValidate := map[string]string{
		"name":      "x",
		"type":      "validate",
		"kind":      "policy",
		"operation": string(admission.Create),
	}
	wantLabelsCounterWebhookAdmit := map[string]string{
		"name":      "x",
		"type":      "admit",
		"kind":      "webhook",
		"operation": string(admission.Create),
	}
	wantLabelsCounterWebhookValidate := map[string]string{
		"name":      "x",
		"type":      "validate",
		"kind":      "webhook",
		"operation": string(admission.Create),
	}
	expectHistogramCountTotal(t, "apiserver_admission_match_condition_evaluation_seconds", wantLabelsCounterPolicyAdmit, 1)
	expectHistogramCountTotal(t, "apiserver_admission_match_condition_evaluation_seconds", wantLabelsCounterPolicyValidate, 1)
	expectHistogramCountTotal(t, "apiserver_admission_match_condition_evaluation_seconds", wantLabelsCounterWebhookAdmit, 1)
	expectHistogramCountTotal(t, "apiserver_admission_match_condition_evaluation_seconds", wantLabelsCounterWebhookValidate, 1)
}

// TestWithMetrics 验证 WithMetrics 包装器：只有当底层 handler 实现了对应
// 接口（MutationInterface/ValidationInterface）时才应产生指标，且拒绝时
// rejected 标签必须为 true。表驱动覆盖 mutating/validating/both 三种形态。
func TestWithMetrics(t *testing.T) {
	defer Metrics.reset()
	defer legacyregistry.Reset()
	// admit/validate 字段既表示期望的成功与否，也决定后续标签过滤条件。
	type Test struct {
		name            string
		ns              string
		operation       admission.Operation
		options         runtime.Object
		handler         admission.Interface
		admit, validate bool
	}
	for _, test := range []Test{
		{
			"both-interfaces-admit-and-validate",
			"some-ns",
			admission.Create,
			&metav1.CreateOptions{},
			&mutatingAndValidatingFakeHandler{admission.NewHandler(admission.Create, admission.Update), true, true},
			true, true,
		},
		{
			"both-interfaces-dont-admit",
			"some-ns",
			admission.Create,
			&metav1.CreateOptions{},
			&mutatingAndValidatingFakeHandler{admission.NewHandler(admission.Create, admission.Update), false, true},
			false, true,
		},
		{
			"both-interfaces-admit-dont-validate",
			"some-ns",
			admission.Create,
			&metav1.CreateOptions{},
			&mutatingAndValidatingFakeHandler{admission.NewHandler(admission.Create, admission.Update), true, false},
			true, false,
		},
		{
			"validate-interfaces-validate",
			"some-ns",
			admission.Create,
			&metav1.CreateOptions{},
			&validatingFakeHandler{admission.NewHandler(admission.Create, admission.Update), true},
			true, true,
		},
		{
			"validate-interfaces-dont-validate",
			"some-ns",
			admission.Create,
			&metav1.CreateOptions{},
			&validatingFakeHandler{admission.NewHandler(admission.Create, admission.Update), false},
			true, false,
		},
		{
			"mutating-interfaces-admit",
			"some-ns",
			admission.Create,
			&metav1.CreateOptions{},
			&mutatingFakeHandler{admission.NewHandler(admission.Create, admission.Update), true},
			true, true,
		},
		{
			"mutating-interfaces-dont-admit",
			"some-ns",
			admission.Create,
			&metav1.CreateOptions{},
			&mutatingFakeHandler{admission.NewHandler(admission.Create, admission.Update), false},
			false, true,
		},
	} {
		Metrics.reset()

		h := WithMetrics(test.handler, Metrics.ObserveAdmissionController, test.name)

		// test mutation
		// 这里传入空 GVK/GVR 是有意的：指标只依赖 operation 等标签，
		// 构造完整属性反而会引入无关噪声。
		err := h.(admission.MutationInterface).Admit(context.TODO(), admission.NewAttributesRecord(nil, nil, schema.GroupVersionKind{}, test.ns, "", schema.GroupVersionResource{}, "", test.operation, test.options, false, nil), nil)
		if test.admit && err != nil {
			t.Errorf("expected admit to succeed, but failed: %v", err)
			continue
		} else if !test.admit && err == nil {
			t.Errorf("expected admit to fail, but it succeeded")
			continue
		}

		filter := map[string]string{"type": "admit", "rejected": "false"}
		if !test.admit {
			filter["rejected"] = "true"
		}
		if _, mutating := test.handler.(admission.MutationInterface); mutating {
			expectHistogramCountTotal(t, "apiserver_admission_controller_admission_duration_seconds", filter, 1)
		} else {
			expectHistogramCountTotal(t, "apiserver_admission_controller_admission_duration_seconds", filter, 0)
		}

		if err != nil {
			// skip validation step if mutation failed
			// 变更失败时 apiserver 不会进入验证阶段，所以这里也不能断言 validate 指标。
			continue
		}

		// test validation
		err = h.(admission.ValidationInterface).Validate(context.TODO(), admission.NewAttributesRecord(nil, nil, schema.GroupVersionKind{}, test.ns, "", schema.GroupVersionResource{}, "", test.operation, test.options, false, nil), nil)
		if test.validate && err != nil {
			t.Errorf("expected admit to succeed, but failed: %v", err)
			continue
		} else if !test.validate && err == nil {
			t.Errorf("expected validation to fail, but it succeeded")
			continue
		}

		filter = map[string]string{"type": "validate", "rejected": "false"}
		if !test.validate {
			filter["rejected"] = "true"
		}
		if _, validating := test.handler.(admission.ValidationInterface); validating {
			expectHistogramCountTotal(t, "apiserver_admission_controller_admission_duration_seconds", filter, 1)
		} else {
			expectHistogramCountTotal(t, "apiserver_admission_controller_admission_duration_seconds", filter, 0)
		}
	}
}

// mutatingAndValidatingFakeHandler 同时实现变更与验证接口，便于验证包装器
// 在两条路径上都记录指标。
type mutatingAndValidatingFakeHandler struct {
	*admission.Handler
	admit    bool
	validate bool
}

func (h *mutatingAndValidatingFakeHandler) Admit(ctx context.Context, a admission.Attributes, o admission.ObjectInterfaces) (err error) {
	if h.admit {
		return nil
	}
	return fmt.Errorf("don't admit")
}

func (h *mutatingAndValidatingFakeHandler) Validate(ctx context.Context, a admission.Attributes, o admission.ObjectInterfaces) (err error) {
	if h.validate {
		return nil
	}
	return fmt.Errorf("don't validate")
}

// validatingFakeHandler 只实现验证接口。
type validatingFakeHandler struct {
	*admission.Handler
	validate bool
}

func (h *validatingFakeHandler) Validate(ctx context.Context, a admission.Attributes, o admission.ObjectInterfaces) (err error) {
	if h.validate {
		return nil
	}
	return fmt.Errorf("don't validate")
}

// mutatingFakeHandler 只实现变更接口。
type mutatingFakeHandler struct {
	*admission.Handler
	admit bool
}

func (h *mutatingFakeHandler) Admit(ctx context.Context, a admission.Attributes, o admission.ObjectInterfaces) (err error) {
	if h.admit {
		return nil
	}
	return fmt.Errorf("don't admit")
}
