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

package runtime

import (
	"fmt"
	"reflect"
	"strings"

	"k8s.io/apimachinery/pkg/runtime/schema"
)

// notRegisteredErr 表示某个 GVK、Go 类型或转换目标未在 scheme 中注册。
// 各字段按错误场景按需填写，Error() 据此选择最贴切的提示文案。
type notRegisteredErr struct {
	schemeName string
	gvk        schema.GroupVersionKind
	target     GroupVersioner
	t          reflect.Type
}

// NewNotRegisteredErrForKind 构造“该 GVK 未注册”的错误。
func NewNotRegisteredErrForKind(schemeName string, gvk schema.GroupVersionKind) error {
	return &notRegisteredErr{schemeName: schemeName, gvk: gvk}
}

// NewNotRegisteredErrForType 构造“该 Go 类型未注册”的错误。
func NewNotRegisteredErrForType(schemeName string, t reflect.Type) error {
	return &notRegisteredErr{schemeName: schemeName, t: t}
}

// NewNotRegisteredErrForTarget 构造“该类型无法转换为目标版本”的错误。
func NewNotRegisteredErrForTarget(schemeName string, t reflect.Type, target GroupVersioner) error {
	return &notRegisteredErr{schemeName: schemeName, t: t, target: target}
}

// NewNotRegisteredGVKErrForTarget 构造“该 GVK 无法转换为目标版本”的错误。
func NewNotRegisteredGVKErrForTarget(schemeName string, gvk schema.GroupVersionKind, target GroupVersioner) error {
	return &notRegisteredErr{schemeName: schemeName, gvk: gvk, target: target}
}

// Error 按已填充的字段组合出最具体的错误信息；判定顺序从“类型+目标”到“仅 GVK”。
func (k *notRegisteredErr) Error() string {
	if k.t != nil && k.target != nil {
		return fmt.Sprintf("%v is not suitable for converting to %q in scheme %q", k.t, k.target, k.schemeName)
	}
	nullGVK := schema.GroupVersionKind{}
	if k.gvk != nullGVK && k.target != nil {
		return fmt.Sprintf("%q is not suitable for converting to %q in scheme %q", k.gvk.GroupVersion(), k.target, k.schemeName)
	}
	if k.t != nil {
		return fmt.Sprintf("no kind is registered for the type %v in scheme %q", k.t, k.schemeName)
	}
	if len(k.gvk.Kind) == 0 {
		return fmt.Sprintf("no version %q has been registered in scheme %q", k.gvk.GroupVersion(), k.schemeName)
	}
	if k.gvk.Version == APIVersionInternal {
		return fmt.Sprintf("no kind %q is registered for the internal version of group %q in scheme %q", k.gvk.Kind, k.gvk.Group, k.schemeName)
	}

	return fmt.Sprintf("no kind %q is registered for version %q in scheme %q", k.gvk.Kind, k.gvk.GroupVersion(), k.schemeName)
}

// IsNotRegisteredError 判断错误是否表示给定对象或输入数据未注册。
// 采用类型断言而非 errors.Is，因为这是包内私有错误类型，无需链式比较。
func IsNotRegisteredError(err error) bool {
	if err == nil {
		return false
	}
	_, ok := err.(*notRegisteredErr)
	return ok
}

// missingKindErr 表示解码输入缺少 Kind 字段。
type missingKindErr struct {
	data string
}

// NewMissingKindErr 构造缺少 Kind 的错误，data 为原始输入片段，便于定位。
func NewMissingKindErr(data string) error {
	return &missingKindErr{data}
}

func (k *missingKindErr) Error() string {
	return fmt.Sprintf("Object 'Kind' is missing in '%s'", k.data)
}

// IsMissingKind 判断错误是否表示对象缺少 'Kind' 字段。
func IsMissingKind(err error) bool {
	if err == nil {
		return false
	}
	_, ok := err.(*missingKindErr)
	return ok
}

// missingVersionErr 表示解码输入缺少 apiVersion 字段。
type missingVersionErr struct {
	data string
}

// NewMissingVersionErr 构造缺少 apiVersion 的错误，data 为原始输入片段。
func NewMissingVersionErr(data string) error {
	return &missingVersionErr{data}
}

func (k *missingVersionErr) Error() string {
	return fmt.Sprintf("Object 'apiVersion' is missing in '%s'", k.data)
}

// IsMissingVersion 判断错误是否表示对象缺少 'Version'（apiVersion）字段。
func IsMissingVersion(err error) bool {
	if err == nil {
		return false
	}
	_, ok := err.(*missingVersionErr)
	return ok
}

// strictDecodingError is a base error type that is returned by a strict Decoder such
// as UniversalStrictDecoder.
// 严格解码会收集所有未知字段、重复字段等违规项，统一包装为此类型返回。
type strictDecodingError struct {
	errors []error
}

// NewStrictDecodingError creates a new strictDecodingError object.
func NewStrictDecodingError(errors []error) error {
	return &strictDecodingError{
		errors: errors,
	}
}

// Error 把所有子错误用逗号拼接，保留每一项的具体信息。
func (e *strictDecodingError) Error() string {
	var s strings.Builder
	s.WriteString("strict decoding error: ")
	for i, err := range e.errors {
		if i != 0 {
			s.WriteString(", ")
		}
		s.WriteString(err.Error())
	}
	return s.String()
}

// Errors 返回全部严格解码违规项，调用方需保持返回切片的只读约定。
func (e *strictDecodingError) Errors() []error {
	return e.errors
}

// IsStrictDecodingError 判断错误是否为严格解码错误（可能含多条违规）。
func IsStrictDecodingError(err error) bool {
	if err == nil {
		return false
	}
	_, ok := err.(*strictDecodingError)
	return ok
}

// AsStrictDecodingError 把错误转换为具体的严格解码错误类型，
// 以便调用方取出并逐条检查违规项。
func AsStrictDecodingError(err error) (*strictDecodingError, bool) {
	if err == nil {
		return nil, false
	}
	strictErr, ok := err.(*strictDecodingError)
	return strictErr, ok
}
