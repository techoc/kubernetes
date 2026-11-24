/*
Copyright 2016 The Kubernetes Authors.

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

package util

import (
	"fmt"
	"strings"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/selection"
	utilerrors "k8s.io/apimachinery/pkg/util/errors"
	"k8s.io/apiserver/pkg/authentication/user"
	"k8s.io/apiserver/pkg/authorization/authorizer"
	authorizationapi "k8s.io/kubernetes/pkg/apis/authorization"
)

// ResourceAttributesFrom 将请求中的资源属性与用户身份组合为 Authorizer 所需的 AttributesRecord，并解析标签和字段选择器。
func ResourceAttributesFrom(user user.Info, in authorizationapi.ResourceAttributes) authorizer.AttributesRecord {
	ret := authorizer.AttributesRecord{
		User:            user,
		Verb:            in.Verb,
		Namespace:       in.Namespace,
		APIGroup:        in.Group,
		APIVersion:      matchAllVersionIfEmpty(in.Version),
		Resource:        in.Resource,
		Subresource:     in.Subresource,
		Name:            in.Name,
		ResourceRequest: true,
	}

	if in.LabelSelector != nil {
		if len(in.LabelSelector.RawSelector) > 0 {
			labelSelector, err := labels.Parse(in.LabelSelector.RawSelector)
			if err != nil {
				ret.LabelSelectorRequirements, ret.LabelSelectorParsingErr = nil, err
			} else {
				requirements, _ /*selectable*/ := labelSelector.Requirements()
				ret.LabelSelectorRequirements, ret.LabelSelectorParsingErr = requirements, nil
			}
		}
		if len(in.LabelSelector.Requirements) > 0 {
			ret.LabelSelectorRequirements, ret.LabelSelectorParsingErr = labelSelectorAsSelector(in.LabelSelector.Requirements)
		}
	}

	if in.FieldSelector != nil {
		if len(in.FieldSelector.RawSelector) > 0 {
			fieldSelector, err := fields.ParseSelector(in.FieldSelector.RawSelector)
			if err != nil {
				ret.FieldSelectorRequirements, ret.FieldSelectorParsingErr = nil, err
			} else {
				ret.FieldSelectorRequirements, ret.FieldSelectorParsingErr = fieldSelector.Requirements(), nil
			}
		}
		if len(in.FieldSelector.Requirements) > 0 {
			ret.FieldSelectorRequirements, ret.FieldSelectorParsingErr = fieldSelectorAsSelector(in.FieldSelector.Requirements)
		}
	}

	return ret
}

var labelSelectorOpToSelectionOp = map[metav1.LabelSelectorOperator]selection.Operator{
	metav1.LabelSelectorOpIn:           selection.In,
	metav1.LabelSelectorOpNotIn:        selection.NotIn,
	metav1.LabelSelectorOpExists:       selection.Exists,
	metav1.LabelSelectorOpDoesNotExist: selection.DoesNotExist,
}

// labelSelectorAsSelector 将 API LabelSelectorRequirement 转换为内部 labels.Requirements；无效项单独记录错误，其余有效项继续保留。
func labelSelectorAsSelector(requirements []metav1.LabelSelectorRequirement) (labels.Requirements, error) {
	if len(requirements) == 0 {
		return nil, nil
	}
	reqs := make([]labels.Requirement, 0, len(requirements))
	var errs []error
	for _, expr := range requirements {
		op, ok := labelSelectorOpToSelectionOp[expr.Operator]
		if !ok {
			errs = append(errs, fmt.Errorf("%q is not a valid label selector operator", expr.Operator))
			continue
		}
		values := expr.Values
		if len(values) == 0 {
			values = nil
		}
		req, err := labels.NewRequirement(expr.Key, op, values)
		if err != nil {
			errs = append(errs, err)
			continue
		}
		reqs = append(reqs, *req)
	}

	// 如果所有 Requirement 都因格式或操作符无效而被跳过，则返回 nil，避免返回一个非 nil 的空切片。
	if len(reqs) == 0 {
		reqs = nil
	}

	// 同时返回已成功解析的 Requirement 和累计错误，使授权器仍可评估有效条件。
	// Requirement 之间按 AND 组合；忽略无法识别的条件只会扩大匹配范围，不会意外缩小授权范围。
	return labels.Requirements(reqs), utilerrors.NewAggregate(errs)
}

// fieldSelectorAsSelector 将 API FieldSelectorRequirement 转换为内部字段选择器条件；当前仅支持 In/NotIn 且每项必须恰有一个值。
func fieldSelectorAsSelector(requirements []metav1.FieldSelectorRequirement) (fields.Requirements, error) {
	if len(requirements) == 0 {
		return nil, nil
	}

	reqs := make([]fields.Requirement, 0, len(requirements))
	var errs []error
	for _, expr := range requirements {
		if len(expr.Values) > 1 {
			errs = append(errs, fmt.Errorf("fieldSelectors do not yet support multiple values"))
			continue
		}

		switch expr.Operator {
		case metav1.FieldSelectorOpIn:
			if len(expr.Values) != 1 {
				errs = append(errs, fmt.Errorf("fieldSelectors in must have one value"))
				continue
			}
			// 转换为 fields.Requirement 时使用 Equals，与解析原始字段选择器后的语义保持一致。
			reqs = append(reqs, fields.Requirement{Field: expr.Key, Operator: selection.Equals, Value: expr.Values[0]})
		case metav1.FieldSelectorOpNotIn:
			if len(expr.Values) != 1 {
				errs = append(errs, fmt.Errorf("fieldSelectors not in must have one value"))
				continue
			}
			// 转换为 fields.Requirement 时使用 NotEquals，与解析原始字段选择器后的语义保持一致。
			reqs = append(reqs, fields.Requirement{Field: expr.Key, Operator: selection.NotEquals, Value: expr.Values[0]})
		case metav1.FieldSelectorOpExists, metav1.FieldSelectorOpDoesNotExist:
			errs = append(errs, fmt.Errorf("fieldSelectors do not yet support %v", expr.Operator))
			continue
		default:
			errs = append(errs, fmt.Errorf("%q is not a valid field selector operator", expr.Operator))
			continue
		}
	}

	// 如果所有 Requirement 都因格式或操作符无效而被跳过，则返回 nil，避免返回一个非 nil 的空切片。
	if len(reqs) == 0 {
		reqs = nil
	}

	// 同时返回已成功解析的 Requirement 和累计错误，使授权器仍可评估有效条件。
	// Requirement 之间按 AND 组合；忽略无法识别的条件只会扩大匹配范围，不会意外缩小授权范围。
	return fields.Requirements(reqs), utilerrors.NewAggregate(errs)
}

// NonResourceAttributesFrom 将用户身份与非资源请求属性组合为 Authorizer 所需的 AttributesRecord。
func NonResourceAttributesFrom(user user.Info, in authorizationapi.NonResourceAttributes) authorizer.AttributesRecord {
	return authorizer.AttributesRecord{
		User:            user,
		ResourceRequest: false,
		Path:            in.Path,
		Verb:            in.Verb,
	}
}

// convertToUserInfoExtra 将 API ExtraValue 转换为认证用户信息使用的 map[string][]string；nil 输入保持为 nil。
func convertToUserInfoExtra(extra map[string]authorizationapi.ExtraValue) map[string][]string {
	if extra == nil {
		return nil
	}
	ret := map[string][]string{}
	for k, v := range extra {
		ret[k] = []string(v)
	}

	return ret
}

// AuthorizationAttributesFrom 根据 SubjectAccessReviewSpec 构造资源请求或非资源请求对应的授权属性。
func AuthorizationAttributesFrom(spec authorizationapi.SubjectAccessReviewSpec) authorizer.AttributesRecord {
	userToCheck := &user.DefaultInfo{
		Name:   spec.User,
		Groups: spec.Groups,
		UID:    spec.UID,
		Extra:  convertToUserInfoExtra(spec.Extra),
	}

	var authorizationAttributes authorizer.AttributesRecord
	if spec.ResourceAttributes != nil {
		authorizationAttributes = ResourceAttributesFrom(userToCheck, *spec.ResourceAttributes)
	} else {
		authorizationAttributes = NonResourceAttributesFrom(userToCheck, *spec.NonResourceAttributes)
	}

	return authorizationAttributes
}

// matchAllVersionIfEmpty 在请求未指定 API Version 时返回通配符 *，表示匹配所有版本。
func matchAllVersionIfEmpty(version string) string {
	if len(version) == 0 {
		return "*"
	}
	return version
}

// BuildEvaluationError 汇总 Authorizer 执行错误以及字段/标签选择器解析错误，生成写入 SubjectAccessReview.Status 的诊断信息。
func BuildEvaluationError(evaluationError error, attrs authorizer.AttributesRecord) string {
	var evaluationErrors []string
	if evaluationError != nil {
		evaluationErrors = append(evaluationErrors, evaluationError.Error())
	}
	if reqs, err := attrs.GetFieldSelector(); err != nil {
		if len(reqs) > 0 {
			evaluationErrors = append(evaluationErrors, "spec.resourceAttributes.fieldSelector partially ignored due to parse error")
		} else {
			evaluationErrors = append(evaluationErrors, "spec.resourceAttributes.fieldSelector ignored due to parse error")
		}
	}
	if reqs, err := attrs.GetLabelSelector(); err != nil {
		if len(reqs) > 0 {
			evaluationErrors = append(evaluationErrors, "spec.resourceAttributes.labelSelector partially ignored due to parse error")
		} else {
			evaluationErrors = append(evaluationErrors, "spec.resourceAttributes.labelSelector ignored due to parse error")
		}
	}
	return strings.Join(evaluationErrors, "; ")
}
