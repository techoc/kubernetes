/*
Copyright 2024 The Kubernetes Authors.

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

// Package patch 实现 MutatingAdmissionPolicy 的变更（mutation）方式：
// 目前支持 CEL 表达式返回 JSON Patch，以及 Server-Side Apply 风格的
// ApplyConfiguration。每种 Patcher 接收准入请求并返回变更后的对象。
package patch

import (
	"context"
	gojson "encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strconv"

	celgo "github.com/google/cel-go/cel"

	"github.com/google/cel-go/common/types"
	"github.com/google/cel-go/common/types/traits"
	"google.golang.org/protobuf/types/known/structpb"
	jsonpatch "gopkg.in/evanphx/json-patch.v4"

	admissionv1 "k8s.io/api/admission/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/serializer/json"
	plugincel "k8s.io/apiserver/pkg/admission/plugin/cel"
	"k8s.io/apiserver/pkg/cel/mutation"
	"k8s.io/apiserver/pkg/cel/mutation/dynamic"
	pointer "k8s.io/utils/ptr"
)

// JSONPatchCondition contains the inputs needed to compile and evaluate a cel expression
// that returns a JSON patch value.
//
// 该表达式必须求值为 JSONPatch 类型的列表；列表中的每个元素对应一个 JSON Patch 操作。
type JSONPatchCondition struct {
	Expression string
}

var _ plugincel.ExpressionAccessor = &JSONPatchCondition{}

// GetExpression 返回用于编译的策略表达式。
func (v *JSONPatchCondition) GetExpression() string {
	return v.Expression
}

// ReturnTypes 声明表达式的静态返回类型为 JSONPatch 列表，编译器据此做类型检查。
func (v *JSONPatchCondition) ReturnTypes() []*celgo.Type {
	return []*celgo.Type{celgo.ListType(jsonPatchType)}
}

var jsonPatchType = types.NewObjectType("JSONPatch")

// NewJSONPatcher creates a patcher that performs a JSON Patch mutation.
//
// patchEvaluator 负责编译并求值策略中的 CEL 表达式，此处只关心求值结果。
func NewJSONPatcher(patchEvaluator plugincel.MutatingEvaluator) Patcher {
	return &jsonPatcher{patchEvaluator}
}

type jsonPatcher struct {
	PatchEvaluator plugincel.MutatingEvaluator
}

// Patch 求值 CEL 表达式得到一组 JSON Patch 操作，并把它们应用到请求对象上，
// 返回变更后的对象。注意：这里的 patch 由策略作者编写，因此结果必须重新解码，
// 以保证类型、默认值等约束仍然成立。
func (e *jsonPatcher) Patch(ctx context.Context, r Request, runtimeCELCostBudget int64) (runtime.Object, error) {
	// 构造准入请求视图，供 CEL 里的 request 变量使用。
	admissionRequest := plugincel.CreateAdmissionRequest(
		r.VersionedAttributes.Attributes,
		metav1.GroupVersionResource(r.MatchedResource),
		metav1.GroupVersionKind(r.VersionedAttributes.VersionedKind))

	// 编译错误在策略被接受时就应暴露；这里再做一次保护性检查。
	compileErrors := e.PatchEvaluator.CompilationErrors()
	if len(compileErrors) > 0 {
		return nil, errors.Join(compileErrors...)
	}
	patchObj, _, err := e.evaluatePatchExpression(ctx, e.PatchEvaluator, runtimeCELCostBudget, r, admissionRequest)
	if err != nil {
		return nil, err
	}
	o := r.ObjectInterfaces
	// 用严格序列化器把对象编码为 JSON；patch 只能作用于该 JSON 表示。
	jsonSerializer := json.NewSerializerWithOptions(json.DefaultMetaFactory, o.GetObjectCreater(), o.GetObjectTyper(), json.SerializerOptions{Pretty: false, Strict: true})
	objJS, err := runtime.Encode(jsonSerializer, r.VersionedAttributes.VersionedObject.Object())
	if err != nil {
		return nil, fmt.Errorf("failed to create JSON patch: %w", err)
	}
	patchedJS, err := patchObj.Apply(objJS)
	if err != nil {
		if errors.Is(err, jsonpatch.ErrTestFailed) {
			// JSON Patch 的 test 操作失败是策略作者表达的“条件不满足”，
			// 按 RFC 6902 语义应当整体不应用，而不是报错。
			// If a json patch fails a test operation, the patch must not be applied
			return r.VersionedAttributes.VersionedObject.Object(), nil
		}
		return nil, fmt.Errorf("JSON Patch: %w", err)
	}

	// 按原对象是否为 unstructured 选择解码目标类型，避免改变调用方期望的表示。
	var newVersionedObject runtime.Object
	if _, ok := r.VersionedAttributes.VersionedObject.Object().(*unstructured.Unstructured); ok {
		newVersionedObject = &unstructured.Unstructured{}
	} else {
		newVersionedObject, err = o.GetObjectCreater().New(r.VersionedAttributes.VersionedKind)
		if err != nil {
			return nil, apierrors.NewInternalError(err)
		}
	}

	// 重新解码而不是直接信任 patch 结果，让类型转换/校验像正常请求一样生效。
	if newVersionedObject, _, err = jsonSerializer.Decode(patchedJS, nil, newVersionedObject); err != nil {
		return nil, apierrors.NewInternalError(err)
	}

	return newVersionedObject, nil
}

// evaluatePatchExpression 在给定预算内求值策略表达式，并把 CEL 结果转换为
// jsonpatch.Patch。剩余预算会返回给调用方，以便同一次准入中多个表达式共享总预算。
func (e *jsonPatcher) evaluatePatchExpression(ctx context.Context, patchEvaluator plugincel.MutatingEvaluator, remainingBudget int64, r Request, admissionRequest *admissionv1.AdmissionRequest) (jsonpatch.Patch, int64, error) {
	var err error
	var eval plugincel.EvaluationResult
	eval, remainingBudget, err = patchEvaluator.ForInput(ctx, r.VersionedAttributes, admissionRequest, r.OptionalVariables, r.Namespace, remainingBudget)
	if err != nil {
		return nil, -1, err
	}
	if eval.Error != nil {
		return nil, -1, eval.Error
	}
	refVal := eval.EvalResult

	// the return type can be any valid CEL value.
	// Scalars, maps and lists are used to set the value when the path points to a field of that type.
	// ObjectVal is used when the path points to a struct. A map like "{"field1": 1, "fieldX": bool}" is not
	// possible in Kubernetes CEL because maps and lists may not have mixed types.
	//
	// 表达式结果必须是 list(JSONPatch)。下面这些类型断言在理论上不会失败，
	// 因为编译阶段已检查返回类型；保留它们是为了在类型系统被绕过时快速失败。

	iter, ok := refVal.(traits.Lister)
	if !ok {
		// Should never happen since compiler checks return type.
		return nil, -1, fmt.Errorf("type mismatch: JSONPatchType.expression should evaluate to array")
	}
	result := jsonpatch.Patch{}
	for it := iter.Iterator(); it.HasNext() == types.True; {
		v := it.Next()
		patchObj, err := v.ConvertToNative(reflect.TypeOf(&mutation.JSONPatchVal{}))
		if err != nil {
			// Should never happen since return type is checked by compiler.
			return nil, -1, fmt.Errorf("type mismatch: JSONPatchType.expression should evaluate to array of JSONPatch: %w", err)
		}
		op, ok := patchObj.(*mutation.JSONPatchVal)
		if !ok {
			// Should never happen since return type is checked by compiler.
			return nil, -1, fmt.Errorf("type mismatch: JSONPatchType.expression should evaluate to array of JSONPatch, got element of %T", patchObj)
		}

		// Construct a JSON Patch from the evaluated CEL expression
		// 对 op/path/from 做 Quote，确保它们作为 JSON 字符串而不是裸值写入 patch。
		resultOp := jsonpatch.Operation{}
		resultOp["op"] = pointer.To(gojson.RawMessage(strconv.Quote(op.Op)))
		resultOp["path"] = pointer.To(gojson.RawMessage(strconv.Quote(op.Path)))
		if len(op.From) > 0 {
			resultOp["from"] = pointer.To(gojson.RawMessage(strconv.Quote(op.From)))
		}
		if op.Val != nil {
			if objVal, ok := op.Val.(*dynamic.ObjectVal); ok {
				// TODO: Object initializers are insufficiently type checked.
				// In the interim, we use this sanity check to detect type mismatches
				// between field names and Object initializers. For example,
				// "Object.spec{ selector: Object.spec.wrong{}}" is detected as a mismatch.
				// Before beta, attaching full type information both to Object initializers and
				// the "object" and "oldObject" variables is needed. This will allow CEL to
				// perform comprehensive runtime type checking.
				//
				// 在完整类型信息到位之前，用字段路径与初始化器类型名校验兜底。
				err := objVal.CheckTypeNamesMatchFieldPathNames()
				if err != nil {
					return nil, -1, fmt.Errorf("type mismatch: %w", err)
				}
			}
			// CEL data literals representing arbitrary JSON values can be serialized to JSON for use in
			// JSON Patch if first converted to pb.Value.
			// 借由 structpb.Value 中转，把任意 CEL 值统一序列化为 JSON。
			v, err := op.Val.ConvertToNative(reflect.TypeOf(&structpb.Value{}))
			if err != nil {
				return nil, -1, fmt.Errorf("JSONPath valueExpression evaluated to a type that could not marshal to JSON: %w", err)
			}
			b, err := gojson.Marshal(v)
			if err != nil {
				return nil, -1, fmt.Errorf("JSONPath valueExpression evaluated to a type that could not marshal to JSON: %w", err)
			}
			resultOp["value"] = pointer.To[gojson.RawMessage](b)
		}

		result = append(result, resultOp)
	}

	return result, remainingBudget, nil
}
