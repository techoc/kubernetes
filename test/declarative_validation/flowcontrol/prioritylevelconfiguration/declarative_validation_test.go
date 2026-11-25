/*
Copyright The Kubernetes Authors.

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

package prioritylevelconfiguration

import (
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/validation/field"
	genericapirequest "k8s.io/apiserver/pkg/endpoints/request"
	apitesting "k8s.io/kubernetes/pkg/api/testing"
	flowcontrol "k8s.io/kubernetes/pkg/apis/flowcontrol"
	registry "k8s.io/kubernetes/pkg/registry/flowcontrol/prioritylevelconfiguration"
	"k8s.io/kubernetes/test/declarative_validation/meta"
)

func TestDeclarativeValidate(t *testing.T) {
	for _, apiVersion := range apiVersions {
		t.Run(apiVersion, func(t *testing.T) {
			testDeclarativeValidate(t, apiVersion)
		})
	}
}

func testDeclarativeValidate(t *testing.T, apiVersion string) {
	ctx := genericapirequest.WithRequestInfo(genericapirequest.NewDefaultContext(), &genericapirequest.RequestInfo{
		APIGroup:   "flowcontrol.apiserver.k8s.io",
		APIVersion: apiVersion,
	})

	specPath := field.NewPath("spec")

	testCases := map[string]struct {
		input        flowcontrol.PriorityLevelConfiguration
		expectedErrs field.ErrorList
	}{
		"valid: Limited allows Reject without queuing": {
			input: mkPLC(),
		},
		"valid: Queue requires queuing": {
			input: mkPLC(tweakLimitResponseType(flowcontrol.LimitResponseTypeQueue)),
		},
		"valid: Exempt without exempt config": {
			input: mkPLC(tweakExempt()),
		},
		"valid: Exempt with exempt config": {
			input: mkPLC(tweakExempt(), tweakExemptConfig(&flowcontrol.ExemptPriorityLevelConfiguration{
				NominalConcurrencyShares: ptrInt32(100),
			})),
		},
		"spec.type: Limited with limited=nil": {
			input: mkPLC(tweakLimited(nil)),
			expectedErrs: field.ErrorList{
				field.Required(specPath.Child("limited"), "").MarkCoveredByDeclarative().MarkBeta(),
			},
		},
		"spec.type: Exempt with limited set": {
			input: mkPLC(tweakExempt(), tweakLimited(&flowcontrol.LimitedPriorityLevelConfiguration{
				NominalConcurrencyShares: 100,
				LimitResponse: flowcontrol.LimitResponse{
					Type: flowcontrol.LimitResponseTypeReject,
				},
			})),
			expectedErrs: field.ErrorList{
				// 必需的对象级检查：'exempt' 的 spec 与引导配置不同（仅手写校验）
				field.Invalid(specPath, nil, "").MarkFromImperative(),
				field.Forbidden(specPath.Child("limited"), "").MarkCoveredByDeclarative().MarkBeta(),
			},
		},
		"spec.type: Limited with exempt set": {
			input: mkPLC(tweakExemptConfig(&flowcontrol.ExemptPriorityLevelConfiguration{})),
			expectedErrs: field.ErrorList{
				field.Forbidden(specPath.Child("exempt"), "").MarkCoveredByDeclarative().MarkBeta(),
			},
		},
		"limitResponse.type: Queue with queuing=nil": {
			input: mkPLC(tweakLimitResponseType(flowcontrol.LimitResponseTypeQueue), tweakQueuing(nil)),
			expectedErrs: field.ErrorList{
				field.Required(specPath.Child("limited", "limitResponse", "queuing"), "").MarkCoveredByDeclarative().MarkBeta(),
			},
		},
		"limitResponse.type: Reject with queuing set": {
			input: mkPLC(tweakQueuing(&flowcontrol.QueuingConfiguration{
				Queues:           64,
				HandSize:         8,
				QueueLengthLimit: 50,
			})),
			expectedErrs: field.ErrorList{
				field.Forbidden(specPath.Child("limited", "limitResponse", "queuing"), "").MarkCoveredByDeclarative().MarkBeta(),
			},
		},
		"spec.type: empty": {
			input: mkPLC(tweakSpecType(""), tweakLimited(nil)),
			expectedErrs: field.ErrorList{
				field.Required(specPath.Child("type"), "").MarkCoveredByDeclarative().MarkBeta(),
			},
		},
		"limitResponse.type: empty": {
			input: mkPLC(tweakLimitResponseType("")),
			expectedErrs: field.ErrorList{
				field.Required(specPath.Child("limited", "limitResponse", "type"), "").MarkCoveredByDeclarative().MarkBeta(),
			},
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			apitesting.VerifyValidationEquivalence(t, ctx, &tc.input, registry.Strategy, tc.expectedErrs)
		})
	}
	obj := mkPLC()
	meta.RunObjectMetaTestCases(t, ctx, &obj, registry.Strategy, meta.WithStringentFinalizerValidation())
}

func TestDeclarativeValidateUpdate(t *testing.T) {
	for _, apiVersion := range apiVersions {
		t.Run(apiVersion, func(t *testing.T) {
			testDeclarativeValidateUpdate(t, apiVersion)
		})
	}
}

func testDeclarativeValidateUpdate(t *testing.T, apiVersion string) {
	specPath := field.NewPath("spec")

	testCases := map[string]struct {
		old          flowcontrol.PriorityLevelConfiguration
		update       flowcontrol.PriorityLevelConfiguration
		expectedErrs field.ErrorList
	}{
		"valid update (no changes)": {
			old:    mkPLC(),
			update: mkPLC(),
		},
		"valid update: Reject to Queue": {
			old:    mkPLC(),
			update: mkPLC(tweakLimitResponseType(flowcontrol.LimitResponseTypeQueue)),
		},
		"update: limited set to nil": {
			old:    mkPLC(),
			update: mkPLC(tweakLimited(nil)),
			expectedErrs: field.ErrorList{
				field.Required(specPath.Child("limited"), "").MarkCoveredByDeclarative().MarkBeta(),
			},
		},
		"update: add exempt field to Limited PLC": {
			old:    mkPLC(),
			update: mkPLC(tweakExemptConfig(&flowcontrol.ExemptPriorityLevelConfiguration{})),
			expectedErrs: field.ErrorList{
				field.Forbidden(specPath.Child("exempt"), "").MarkCoveredByDeclarative().MarkBeta(),
			},
		},
		"update: Queue with queuing set to nil": {
			old:    mkPLC(tweakLimitResponseType(flowcontrol.LimitResponseTypeQueue)),
			update: mkPLC(tweakLimitResponseType(flowcontrol.LimitResponseTypeQueue), tweakQueuing(nil)),
			expectedErrs: field.ErrorList{
				field.Required(specPath.Child("limited", "limitResponse", "queuing"), "").MarkCoveredByDeclarative().MarkBeta(),
			},
		},
		"update: Reject with queuing added": {
			old: mkPLC(),
			update: mkPLC(tweakQueuing(&flowcontrol.QueuingConfiguration{
				Queues:           64,
				HandSize:         8,
				QueueLengthLimit: 50,
			})),
			expectedErrs: field.ErrorList{
				field.Forbidden(specPath.Child("limited", "limitResponse", "queuing"), "").MarkCoveredByDeclarative().MarkBeta(),
			},
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			tc.old.ResourceVersion = "1"
			tc.update.ResourceVersion = "2"
			ctx := genericapirequest.WithRequestInfo(genericapirequest.NewDefaultContext(), &genericapirequest.RequestInfo{
				APIPrefix:         "apis",
				APIGroup:          "flowcontrol.apiserver.k8s.io",
				APIVersion:        apiVersion,
				Resource:          "prioritylevelconfigurations",
				Name:              tc.old.Name,
				IsResourceRequest: true,
				Verb:              "update",
			})
			apitesting.VerifyUpdateValidationEquivalence(t, ctx, &tc.update, &tc.old, registry.Strategy, tc.expectedErrs)
		})
	}

	ctx := genericapirequest.WithRequestInfo(genericapirequest.NewDefaultContext(), &genericapirequest.RequestInfo{
		APIPrefix:         "apis",
		APIGroup:          "flowcontrol.apiserver.k8s.io",
		APIVersion:        apiVersion,
		Resource:          "prioritylevelconfigurations",
		Name:              "test-limited",
		IsResourceRequest: true,
		Verb:              "update",
	})
	updateObj := mkPLC()
	meta.RunObjectMetaUpdateTestCases(t, ctx, &updateObj, registry.Strategy, meta.WithStringentFinalizerValidation())
}

// mkPLC 创建一个合法的 Limited PriorityLevelConfiguration，LimitResponse 为 Reject。
func mkPLC(tweaks ...func(*flowcontrol.PriorityLevelConfiguration)) flowcontrol.PriorityLevelConfiguration {
	obj := flowcontrol.PriorityLevelConfiguration{
		ObjectMeta: metav1.ObjectMeta{
			Name: "test-limited",
		},
		Spec: flowcontrol.PriorityLevelConfigurationSpec{
			Type: flowcontrol.PriorityLevelEnablementLimited,
			Limited: &flowcontrol.LimitedPriorityLevelConfiguration{
				NominalConcurrencyShares: 100,
				LimitResponse: flowcontrol.LimitResponse{
					Type: flowcontrol.LimitResponseTypeReject,
				},
			},
		},
	}
	for _, tweak := range tweaks {
		tweak(&obj)
	}
	return obj
}

// tweakExempt 将 PLC 切换为 Exempt 类型，并使用必需的 "exempt" 名称。
func tweakExempt() func(*flowcontrol.PriorityLevelConfiguration) {
	return func(obj *flowcontrol.PriorityLevelConfiguration) {
		obj.Name = "exempt"
		obj.Spec.Type = flowcontrol.PriorityLevelEnablementExempt
		obj.Spec.Limited = nil
	}
}

// tweakLimited 直接设置 Limited 字段。
func tweakLimited(limited *flowcontrol.LimitedPriorityLevelConfiguration) func(*flowcontrol.PriorityLevelConfiguration) {
	return func(obj *flowcontrol.PriorityLevelConfiguration) {
		obj.Spec.Limited = limited
	}
}

// tweakSpecType 直接设置 Spec.Type 字段。
func tweakSpecType(t flowcontrol.PriorityLevelEnablement) func(*flowcontrol.PriorityLevelConfiguration) {
	return func(obj *flowcontrol.PriorityLevelConfiguration) {
		obj.Spec.Type = t
	}
}

// tweakExemptConfig 设置 Exempt 配置字段。
func tweakExemptConfig(exempt *flowcontrol.ExemptPriorityLevelConfiguration) func(*flowcontrol.PriorityLevelConfiguration) {
	return func(obj *flowcontrol.PriorityLevelConfiguration) {
		obj.Spec.Exempt = exempt
	}
}

// tweakLimitResponseType 设置 LimitResponse 类型，若为 Queue 则自动填充 Queuing。
func tweakLimitResponseType(lrt flowcontrol.LimitResponseType) func(*flowcontrol.PriorityLevelConfiguration) {
	return func(obj *flowcontrol.PriorityLevelConfiguration) {
		if obj.Spec.Limited == nil {
			return
		}
		obj.Spec.Limited.LimitResponse.Type = lrt
		if lrt == flowcontrol.LimitResponseTypeQueue {
			obj.Spec.Limited.LimitResponse.Queuing = &flowcontrol.QueuingConfiguration{
				Queues:           64,
				HandSize:         8,
				QueueLengthLimit: 50,
			}
		} else {
			obj.Spec.Limited.LimitResponse.Queuing = nil
		}
	}
}

// tweakQueuing 直接设置 Queuing 字段，覆盖自动填充的值。
func tweakQueuing(queuing *flowcontrol.QueuingConfiguration) func(*flowcontrol.PriorityLevelConfiguration) {
	return func(obj *flowcontrol.PriorityLevelConfiguration) {
		if obj.Spec.Limited == nil {
			return
		}
		obj.Spec.Limited.LimitResponse.Queuing = queuing
	}
}

func ptrInt32(v int32) *int32 {
	return &v
}
