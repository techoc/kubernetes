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

package internal

import (
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"sigs.k8s.io/structured-merge-diff/v6/fieldpath"
	"sigs.k8s.io/structured-merge-diff/v6/value"
)

// 路径元素是 FieldsV1 的序列化单元，格式为 "<type>:<content>"，
// 其中 type 取值为 f/v/i/k，分别对应字段名、字段值、数组下标和 list-map 键。
const (
	//Field 表示此路径元素的内容是字段名。
	Field = "f"

	//Value 表示此路径元素的内容是字段值。
	Value = "v"

	//Index 表示此路径元素的内容是数组索引。
	Index = "i"

	//Key 表示此路径元素的内容是键值映射。
	Key = "k"

	//Separator 分隔路径元素的类型与内容。
	Separator = ":"
)

// NewPathElement 解析序列化后的路径元素。
//
// 只按第一个冒号切分，因为内容本身可能含冒号（如 "v:foo:bar" 的值是 "foo:bar"）。
// 未知类型返回空元素而不报错，为未来扩展留出前向兼容空间。
func NewPathElement(s string) (fieldpath.PathElement, error) {
	split := strings.SplitN(s, Separator, 2)
	if len(split) < 2 {
		return fieldpath.PathElement{}, fmt.Errorf("missing colon: %v", s)
	}
	switch split[0] {
	case Field:
		return fieldpath.PathElement{
			FieldName: &split[1],
		}, nil
	case Value:
		val, err := value.FromJSON([]byte(split[1]))
		if err != nil {
			return fieldpath.PathElement{}, err
		}
		return fieldpath.PathElement{
			Value: &val,
		}, nil
	case Index:
		i, err := strconv.Atoi(split[1])
		if err != nil {
			return fieldpath.PathElement{}, err
		}
		return fieldpath.PathElement{
			Index: &i,
		}, nil
	case Key:
		// key 的内容是一个 JSON 对象，每个成员都要转成 value.Value。
		kv := map[string]json.RawMessage{}
		err := json.Unmarshal([]byte(split[1]), &kv)
		if err != nil {
			return fieldpath.PathElement{}, err
		}
		fields := value.FieldList{}
		for k, v := range kv {
			b, err := json.Marshal(v)
			if err != nil {
				return fieldpath.PathElement{}, err
			}
			val, err := value.FromJSON(b)
			if err != nil {
				return fieldpath.PathElement{}, err
			}

			fields = append(fields, value.Field{
				Name:  k,
				Value: val,
			})
		}
		return fieldpath.PathElement{
			Key: &fields,
		}, nil
	default:
		//忽略未知的键类型。
		return fieldpath.PathElement{}, nil
	}
}

// PathElementString 序列化路径元素。
//
// 序列化是 NewPathElement 的逆操作，供 FieldsV1 输出使用；
// 同时设置了多个字段的 PathElement 会被判定为非法。
func PathElementString(pe fieldpath.PathElement) (string, error) {
	switch {
	case pe.FieldName != nil:
		return Field + Separator + *pe.FieldName, nil
	case pe.Key != nil:
		// 先转成 RawMessage 再统一 Marshal，避免重复编码与转义差异。
		kv := map[string]json.RawMessage{}
		for _, k := range *pe.Key {
			b, err := value.ToJSON(k.Value)
			if err != nil {
				return "", err
			}
			m := json.RawMessage{}
			err = json.Unmarshal(b, &m)
			if err != nil {
				return "", err
			}
			kv[k.Name] = m
		}
		b, err := json.Marshal(kv)
		if err != nil {
			return "", err
		}
		return Key + ":" + string(b), nil
	case pe.Value != nil:
		b, err := value.ToJSON(*pe.Value)
		if err != nil {
			return "", err
		}
		return Value + ":" + string(b), nil
	case pe.Index != nil:
		return Index + ":" + strconv.Itoa(*pe.Index), nil
	default:
		return "", errors.New("Invalid type of path element")
	}
}
