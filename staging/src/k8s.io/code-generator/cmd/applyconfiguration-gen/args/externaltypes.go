/*
Copyright 2021 The Kubernetes Authors.

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
	"bytes"
	"encoding/csv"
	"flag"
	"fmt"
	"strings"

	"k8s.io/gengo/v2/types"
)

// externalApplyConfigurationValue 实现 flag.Value，用于解析
// --external-applyconfigurations 参数。externals 指向调用方持有的映射，
// 使其可以直接累积，无需额外合并步骤。
type externalApplyConfigurationValue struct {
	externals *map[types.Name]string
}

// NewExternalApplyConfigurationValue 创建参数值解析器，并立即应用默认值。
// 默认值非法时 panic：这些值来自生成器自身参数，错误应当尽早暴露，而不是
// 在后续生成阶段静默地产生错误映射。
func NewExternalApplyConfigurationValue(externals *map[types.Name]string, def []string) *externalApplyConfigurationValue {
	val := new(externalApplyConfigurationValue)
	val.externals = externals
	if def != nil {
		if err := val.set(def); err != nil {
			panic(err)
		}
	}
	return val
}

// 编译期断言：确保该类型可被 pflag/flag 当作值类型接受。
var _ flag.Value = &externalApplyConfigurationValue{}

// set 将一组 "pkg.Type:applyconfig-pkg" 映射写入目标 map。
// 重复类型直接报错而非后写覆盖，避免生成结果依赖参数顺序。
func (s *externalApplyConfigurationValue) set(vs []string) error {
	for _, input := range vs {
		typ, pkg, err := parseExternalMapping(input)
		if err != nil {
			return err
		}
		if _, ok := (*s.externals)[typ]; ok {
			return fmt.Errorf("duplicate type found in --external-applyconfigurations: %v", typ)
		}
		(*s.externals)[typ] = pkg
	}

	return nil
}

// Set 实现 flag.Value；允许单次参数携带逗号分隔的多条映射。
func (s *externalApplyConfigurationValue) Set(val string) error {
	vs, err := readAsCSV(val)
	if err != nil {
		return err
	}
	if err := s.set(vs); err != nil {
		return err
	}

	return nil
}

// Type 返回 flag 包展示的类型名。
func (s *externalApplyConfigurationValue) Type() string {
	return "string"
}

// String 以 CSV 形式回显当前映射，便于 flag 打印默认值。
// 排序并非必须；这里保持 map 遍历顺序，输出仅用于展示。
func (s *externalApplyConfigurationValue) String() string {
	var strs []string
	for k, v := range *s.externals {
		strs = append(strs, fmt.Sprintf("%s.%s:%s", k.Package, k.Name, v))
	}
	str, _ := writeAsCSV(strs)
	return "[" + str + "]"
}

// readAsCSV 将参数值拆为多条记录；空串视为空列表。
func readAsCSV(val string) ([]string, error) {
	if val == "" {
		return []string{}, nil
	}
	stringReader := strings.NewReader(val)
	csvReader := csv.NewReader(stringReader)
	return csvReader.Read()
}

// writeAsCSV 是 readAsCSV 的逆操作，用于 String() 回显。
func writeAsCSV(vals []string) (string, error) {
	b := &bytes.Buffer{}
	w := csv.NewWriter(b)
	err := w.Write(vals)
	if err != nil {
		return "", err
	}
	w.Flush()
	return strings.TrimSuffix(b.String(), "\n"), nil
}

// parseExternalMapping 解析 "<package>.<typeName>:<applyconfiguration-package>"。
// 冒号恰好一个：类型名/包名本身不允许含冒号，多出的分隔符视为笔误。
func parseExternalMapping(mapping string) (typ types.Name, pkg string, err error) {
	parts := strings.Split(mapping, ":")
	if len(parts) != 2 {
		return types.Name{}, "", fmt.Errorf("expected string of the form <package>.<typeName>:<applyconfiguration-package> but got %s", mapping)
	}
	packageTypeStr := parts[0]
	pkg = parts[1]
	// // 按最后一个点分割：k8s.io 等合法包名自身也含点
	lastDot := strings.LastIndex(packageTypeStr, ".")
	if lastDot == -1 || lastDot == len(packageTypeStr)-1 {
		return types.Name{}, "", fmt.Errorf("expected package and type of the form <package>.<typeName> but got %s", packageTypeStr)
	}
	structPkg := packageTypeStr[:lastDot]
	structType := packageTypeStr[lastDot+1:]

	return types.Name{Package: structPkg, Name: structType}, pkg, nil
}
