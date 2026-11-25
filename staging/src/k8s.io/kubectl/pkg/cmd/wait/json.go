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

package wait

import (
	"context"
	"errors"
	"fmt"
	"io"
	"reflect"
	"strings"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/cli-runtime/pkg/resource"
	"k8s.io/client-go/util/jsonpath"
)

// JSONPathWait 持有一个 JSONPath 解析器，能够
// 检查 JSONPath 条件，并与 API server 提供的 JSON 输出进行比较。
type JSONPathWait struct {
	matchAnyValue  bool
	jsonPathValue  string
	jsonPathParser *jsonpath.JSONPath
	// 发生错误时会写入 errOut
	errOut io.Writer
}

// IsJSONPathConditionMet 满足条件检查接口 ConditionFunc 的要求
func (j JSONPathWait) IsJSONPathConditionMet(ctx context.Context, info *resource.Info, o *WaitOptions) (runtime.Object, bool, error) {
	return getObjAndCheckCondition(ctx, info, o, j.isJSONPathConditionMet, j.checkCondition)
}

// isJSONPathConditionMet 是 IsJSONPathConditionMet 的辅助函数，
// 用于检查 watch 事件以及 JSONPathWait 条件是否满足
func (j JSONPathWait) isJSONPathConditionMet(event watch.Event) (bool, error) {
	if event.Type == watch.Error {
		// 遇到错误时继续等待——我们预期
		// 服务端会关闭 watch
		err := apierrors.FromObject(event.Object)
		fmt.Fprintf(j.errOut, "error: An error occurred while waiting for the condition to be satisfied: %v", err)
		return false, nil
	}
	if event.Type == watch.Deleted {
		// 这会层层返回，导致再次执行 get，并沿调用链返回 false
		return false, nil
	}
	// 事件中的 runtime Object 可以安全地断言为 Unstructed，
	// 因为我们使用的是 dynamic client
	obj := event.Object.(*unstructured.Unstructured)
	return j.checkCondition(obj)
}

// checkCondition 使用 JSONPath 解析器解析从 API server 收到的 JSON，
// 并检查它是否符合期望的条件
func (j JSONPathWait) checkCondition(obj *unstructured.Unstructured) (bool, error) {
	queryObj := obj.UnstructuredContent()
	parseResults, err := j.jsonPathParser.FindResults(queryObj)
	if err != nil {
		return false, err
	}
	if len(parseResults) == 0 || len(parseResults[0]) == 0 {
		return false, nil
	}
	if err := verifyParsedJSONPath(parseResults); err != nil {
		return false, err
	}
	if j.matchAnyValue {
		return true, nil
	}
	isConditionMet, err := compareResults(parseResults[0][0], j.jsonPathValue)
	if err != nil {
		return false, err
	}
	return isConditionMet, nil
}

// verifyParsedJSONPath 验证从 API server 收到的 JSON 是否有效。
// 它只接受单个 JSON
func verifyParsedJSONPath(results [][]reflect.Value) error {
	if len(results) > 1 {
		return errors.New("given jsonpath expression matches more than one list")
	}
	if len(results[0]) > 1 {
		return errors.New("given jsonpath expression matches more than one value")
	}
	return nil
}

// compareResults 会将 JSONPath 解析器解析结果中的 reflect.Value，
// 与给定值所提供的期望值进行比较
//
// 由于数据来自 unstructured，它只能是基本类型、
// map[string]interface{} 或 []interface{}。
// 我们不支持后两种类型，而是依赖 fmt 处理到字符串的转换，
// 并将结果与用户输入比较
func compareResults(r reflect.Value, expectedVal string) (bool, error) {
	switch r.Interface().(type) {
	case map[string]interface{}, []interface{}:
		return false, errors.New("jsonpath leads to a nested object or list which is not supported")
	}
	s := fmt.Sprintf("%v", r.Interface())
	return strings.TrimSpace(s) == strings.TrimSpace(expectedVal), nil
}
