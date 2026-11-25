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
	"bytes"
	"encoding/json"
	"fmt"

	ejson "github.com/exponent-io/jsonpath"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	utilerrors "k8s.io/apimachinery/pkg/util/errors"
	"k8s.io/cli-runtime/pkg/resource"
	"k8s.io/klog/v2"
)

// Schema 是能够对序列化为字节数组的 API 对象执行校验的接口。
type Schema interface {
	ValidateBytes(data []byte) error
}

// NullSchema 始终通过字节校验。
type NullSchema struct{}

// 对 NullSchema 而言，ValidateBytes 永远不会失败。
func (NullSchema) ValidateBytes(data []byte) error { return nil }

// NoDoubleKeySchema 是不允许出现重复键的 schema。
type NoDoubleKeySchema struct{}

// ValidateBytes 执行字节校验。
func (NoDoubleKeySchema) ValidateBytes(data []byte) error {
	var list []error
	if err := validateNoDuplicateKeys(data, "metadata", "labels"); err != nil {
		list = append(list, err)
	}
	if err := validateNoDuplicateKeys(data, "metadata", "annotations"); err != nil {
		list = append(list, err)
	}
	return utilerrors.NewAggregate(list)
}

func validateNoDuplicateKeys(data []byte, path ...string) error {
	r := ejson.NewDecoder(bytes.NewReader(data))
	// 这是 Go 不够友好的地方：'path ...string' 传入后实际是 []string，
	// 而 SeekTo 接受的是 ...interface{}，所以不能直接把 path 传进去，
	// 必须先复制一份。*叹气*
	ifacePath := []interface{}{}
	for ix := range path {
		ifacePath = append(ifacePath, path[ix])
	}
	found, err := r.SeekTo(ifacePath...)
	if err != nil {
		return err
	}
	if !found {
		return nil
	}
	seen := map[string]bool{}
	for {
		tok, err := r.Token()
		if err != nil {
			return err
		}
		switch t := tok.(type) {
		case json.Delim:
			if t.String() == "}" {
				return nil
			}
		case ejson.KeyString:
			if seen[string(t)] {
				return fmt.Errorf("duplicate key: %s", string(t))
			}
			seen[string(t)] = true
		}
	}
}

// ConjunctiveSchema 封装了一个 schema 列表。
type ConjunctiveSchema []Schema

// ValidateBytes 按照 ConjunctiveSchema 逐个校验字节。
func (c ConjunctiveSchema) ValidateBytes(data []byte) error {
	var list []error
	schemas := []Schema(c)
	for ix := range schemas {
		if err := schemas[ix].ValidateBytes(data); err != nil {
			list = append(list, err)
		}
	}
	return utilerrors.NewAggregate(list)
}

func NewParamVerifyingSchema(s Schema, verifier resource.Verifier, directive string) Schema {
	return &paramVerifyingSchema{
		schema:    s,
		verifier:  verifier,
		directive: directive,
	}
}

// 仅当 apiserver 不支持 fieldValidation 查询参数时，
// paramVerifyingSchema 才执行校验，因为此时
// 会由服务端校验取代客户端校验。
type paramVerifyingSchema struct {
	schema    Schema
	verifier  resource.Verifier
	directive string
}

// ValidateBytes 按照 ParamVerifyingSchema 的规则校验字节
func (c *paramVerifyingSchema) ValidateBytes(data []byte) error {
	obj, err := parse(data)
	if err != nil {
		return err
	}

	gvk, errs := getObjectKind(obj)
	if errs != nil {
		return utilerrors.NewAggregate(errs)
	}

	err = c.verifier.HasSupport(gvk)
	if resource.IsParamUnsupportedError(err) {
		switch c.directive {
		case metav1.FieldValidationStrict:
			return c.schema.ValidateBytes(data)
		case metav1.FieldValidationWarn:
			klog.Warningf("cannot perform warn validation if server-side field validation is unsupported, skipping validation")
		default:
			// 不会执行到这里
			klog.Warningf("unexpected field validation directive: %s, skipping validation", c.directive)
		}
		return nil
	}
	return err
}
