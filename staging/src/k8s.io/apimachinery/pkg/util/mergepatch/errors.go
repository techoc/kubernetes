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

package mergepatch

import (
	"errors"
	"fmt"
	"reflect"
)

// 这些哨兵错误用于区分 patch 的失败原因，调用方可据此选择不同的响应
// （例如 precondition 失败返回 409/412）。
var (
	ErrBadJSONDoc                           = errors.New("invalid JSON document")
	ErrNoListOfLists                        = errors.New("lists of lists are not supported")
	ErrBadPatchFormatForPrimitiveList       = errors.New("invalid patch format of primitive list")
	ErrBadPatchFormatForRetainKeys          = errors.New("invalid patch format of retainKeys")
	ErrBadPatchFormatForSetElementOrderList = errors.New("invalid patch format of setElementOrder list")
	ErrPatchContentNotMatchRetainKeys       = errors.New("patch content doesn't match retainKeys list")
	ErrUnsupportedStrategicMergePatchFormat = errors.New("strategic merge patch format is not supported")
)

// ErrNoMergeKey 表示 map 元素缺少类型定义声明的 merge key，无法确定匹配对象。
func ErrNoMergeKey(m map[string]interface{}, k string) error {
	return fmt.Errorf("map: %v does not contain declared merge key: %s", m, k)
}

// ErrBadArgType 报告实际类型的完整类型名（而非 kind），便于定位具体的不匹配。
func ErrBadArgType(expected, actual interface{}) error {
	return fmt.Errorf("expected a %s, but received a %s",
		reflect.TypeOf(expected),
		reflect.TypeOf(actual))
}

// ErrBadArgKind 与 ErrBadArgType 类似，但只比较 kind 且显式处理 nil，
// 因为 nil 没有 Type 可打印。
func ErrBadArgKind(expected, actual interface{}) error {
	var expectedKindString, actualKindString string
	if expected == nil {
		expectedKindString = "nil"
	} else {
		expectedKindString = reflect.TypeOf(expected).Kind().String()
	}
	if actual == nil {
		actualKindString = "nil"
	} else {
		actualKindString = reflect.TypeOf(actual).Kind().String()
	}
	return fmt.Errorf("expected a %s, but received a %s", expectedKindString, actualKindString)
}

// ErrBadPatchType 表示 patch 中出现了无法识别的指令类型。
func ErrBadPatchType(t interface{}, m map[string]interface{}) error {
	return fmt.Errorf("unknown patch type: %s in map: %v", t, m)
}

// IsPreconditionFailed returns true if the provided error indicates
// a precondition failed.
func IsPreconditionFailed(err error) bool {
	_, ok := err.(ErrPreconditionFailed)
	return ok
}

// ErrPreconditionFailed 携带失败的目标对象内容，便于用户定位。
type ErrPreconditionFailed struct {
	message string
}

// NewErrPreconditionFailed 构造 precondition 失败错误。
func NewErrPreconditionFailed(target map[string]interface{}) ErrPreconditionFailed {
	s := fmt.Sprintf("precondition failed for: %v", target)
	return ErrPreconditionFailed{s}
}

func (err ErrPreconditionFailed) Error() string {
	return err.message
}

// ErrConflict 表示 patch 与并发修改冲突，错误信息同时包含 patch 和当前差异。
type ErrConflict struct {
	message string
}

// NewErrConflict 构造冲突错误，同时输出 patch 内容和从原始到当前的变更，
// 让用户能直接看出冲突来源。
func NewErrConflict(patch, current string) ErrConflict {
	s := fmt.Sprintf("patch:\n%s\nconflicts with changes made from original to current:\n%s\n", patch, current)
	return ErrConflict{s}
}

func (err ErrConflict) Error() string {
	return err.message
}

// IsConflict returns true if the provided error indicates
// a conflict between the patch and the current configuration.
func IsConflict(err error) bool {
	_, ok := err.(ErrConflict)
	return ok
}
