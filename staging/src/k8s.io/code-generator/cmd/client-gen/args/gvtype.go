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

package args

import (
	"flag"
	"fmt"
	"strings"

	"k8s.io/code-generator/cmd/client-gen/types"
)

// gvTypeValue 实现 flag.Value，把形如 group/version/type 的重复参数累积成
// GroupVersion → 类型名列表的映射，供 client-gen 决定为哪些类型生成客户端。
type gvTypeValue struct {
	gvToTypes *map[types.GroupVersion][]string
	changed   bool // 是否已经由命令行覆盖过默认值；用于首写时清空默认映射
}

// NewGVTypesValue 创建 flag 值；def 非空时作为默认值预先解析。
// 解析失败直接 panic，因为默认值写死在代码里，错误属于编程错误。
func NewGVTypesValue(gvToTypes *map[types.GroupVersion][]string, def []string) *gvTypeValue {
	gvt := new(gvTypeValue)
	gvt.gvToTypes = gvToTypes
	if def != nil {
		if err := gvt.set(def); err != nil {
			panic(err)
		}
	}
	return gvt
}

var _ flag.Value = &gvTypeValue{}

// set 把一组 group/version/type 字符串并入映射。
// 首次调用（未 changed）时清空映射，使命令行参数完全取代默认值而不是叠加。
func (s *gvTypeValue) set(vs []string) error {
	if !s.changed {
		*s.gvToTypes = map[types.GroupVersion][]string{}
	}

	for _, input := range vs {
		gvString, typeStr, err := parseGroupVersionType(input)
		if err != nil {
			return err
		}
		gv, err := types.ToGroupVersion(gvString)
		if err != nil {
			return err
		}
		types, ok := (*s.gvToTypes)[gv]
		if !ok {
			types = []string{}
		}
		types = append(types, typeStr)
		(*s.gvToTypes)[gv] = types
	}

	return nil
}

// Set 实现 flag.Value；单个 flag 可以携带逗号分隔的多个值。
func (s *gvTypeValue) Set(val string) error {
	vs, err := readAsCSV(val)
	if err != nil {
		return err
	}
	if err := s.set(vs); err != nil {
		return err
	}
	s.changed = true
	return nil
}

// Type 实现 flag.Value，声明该参数可重复出现。
func (s *gvTypeValue) Type() string {
	return "stringSlice"
}

// String 实现 flag.Value；输出顺序由 map 迭代决定，仅用于帮助文本，不保证稳定。
func (s *gvTypeValue) String() string {
	strs := make([]string, 0, len(*s.gvToTypes))
	for gv, ts := range *s.gvToTypes {
		for _, t := range ts {
			strs = append(strs, gv.Group.String()+"/"+gv.Version.String()+"/"+t)
		}
	}
	str, _ := writeAsCSV(strs)
	return "[" + str + "]"
}

// parseGroupVersionType 解析 group/version/type 或 group/type 两种形式。
// 两段形式里第一段是 group，version 留空，表示该 group 的未版本化（internal）类型；
// 这与其他 client-gen flag（如 --clientset-name）的约定一致。
func parseGroupVersionType(gvtString string) (gvString string, typeStr string, err error) {
	invalidFormatErr := fmt.Errorf("invalid value: %s, should be of the form group/version/type", gvtString)
	subs := strings.Split(gvtString, "/")
	length := len(subs)
	switch length {
	case 2:
		// gvtString of the form group/type, e.g. api/Service,extensions/ReplicaSet
		return subs[0] + "/", subs[1], nil
	case 3:
		return strings.Join(subs[:length-1], "/"), subs[length-1], nil
	default:
		return "", "", invalidFormatErr
	}
}
