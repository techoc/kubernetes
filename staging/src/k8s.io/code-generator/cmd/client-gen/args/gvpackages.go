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
	"bytes"
	"encoding/csv"
	"flag"
	"path"
	"sort"
	"strings"

	"k8s.io/code-generator/cmd/client-gen/generators/util"
	"k8s.io/code-generator/cmd/client-gen/types"
)

// inputBasePathValue 实现 flag.Value，用于接收 --input-base 参数。
// 该参数表示所有 group-version 输入包所在的公共导入前缀，解析 group 时会与
// 相对路径拼接成完整的 Go import path。
type inputBasePathValue struct {
	builder *groupVersionsBuilder
}

var _ flag.Value = &inputBasePathValue{}

// NewInputBasePathValue 构造 inputBasePathValue 并立即应用默认值 def，
// 这样即使用户不传 --input-base 也能得到可用的 import 前缀。
func NewInputBasePathValue(builder *groupVersionsBuilder, def string) *inputBasePathValue {
	v := &inputBasePathValue{
		builder: builder,
	}
	v.Set(def)
	return v
}

// Set 更新 import 基础路径，并触发 builder 重新计算 group-version 列表。
func (s *inputBasePathValue) Set(val string) error {
	s.builder.importBasePath = val
	return s.builder.update()
}

// Type 返回 flag 包用于展示的类型名。
func (s *inputBasePathValue) Type() string {
	return "string"
}

// String 返回当前 import 基础路径，供 flag 打印默认值。
func (s *inputBasePathValue) String() string {
	return s.builder.importBasePath
}

// gvPackagesValue 实现 flag.Value，用于接收 --input 参数（逗号分隔的 group/version 列表）。
// 与 inputBasePathValue 一样，它只是把原始字符串收集起来，最终由 builder 统一解析。
type gvPackagesValue struct {
	builder *groupVersionsBuilder
	groups  []string
	changed bool
}

// NewGVPackagesValue 构造 gvPackagesValue；def 非空时先应用默认的 group-version 列表。
func NewGVPackagesValue(builder *groupVersionsBuilder, def []string) *gvPackagesValue {
	gvp := new(gvPackagesValue)
	gvp.builder = builder
	if def != nil {
		if err := gvp.set(def); err != nil {
			panic(err)
		}
	}
	return gvp
}

var _ flag.Value = &gvPackagesValue{}

// set 合并传入的 group-version 列表并触发重建。
// 首次调用（默认值）覆盖 groups，之后的调用（用户显式传入）追加，
// 从而允许命令行多次 --input 叠加。
func (s *gvPackagesValue) set(vs []string) error {
	if s.changed {
		s.groups = append(s.groups, vs...)
	} else {
		s.groups = append([]string(nil), vs...)
	}

	s.builder.groups = s.groups
	return s.builder.update()
}

// Set 将 flag 传入的 CSV 字符串解析为切片，再交给 set。
func (s *gvPackagesValue) Set(val string) error {
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

// Type 返回 flag 包用于展示的类型名。
func (s *gvPackagesValue) Type() string {
	return "stringSlice"
}

// String 以 CSV 形式返回已收集的 group-version，供 flag 打印默认值。
func (s *gvPackagesValue) String() string {
	str, _ := writeAsCSV(s.groups)
	return "[" + str + "]"
}

// groupVersionsBuilder 把 --input 的原始条目（可能形如 "apps/v1" 或
// "k8s.io/api/apps/v1"）解析成结构化的 types.GroupVersions 列表，
// 并按 group 归并同一 group 下的多个版本。
type groupVersionsBuilder struct {
	value          *[]types.GroupVersions
	groups         []string
	importBasePath string
}

// NewGroupVersionsBuilder 构造 builder，结果写回到 groups 指向的切片。
func NewGroupVersionsBuilder(groups *[]types.GroupVersions) *groupVersionsBuilder {
	return &groupVersionsBuilder{
		value: groups,
	}
}

// update 重新解析全部输入并重写输出切片。
// 注意 group 的顺序必须稳定（按 group 名排序），否则生成文件的导入顺序
// 会随 map 遍历顺序变化，导致代码生成结果不可复现。
func (p *groupVersionsBuilder) update() error {
	var seenGroups = make(map[types.Group]*types.GroupVersions)
	for _, v := range p.groups {
		pth, gvString := util.ParsePathGroupVersion(v)
		gv, err := types.ToGroupVersion(gvString)
		if err != nil {
			return err
		}

		// 版本包路径 = import 前缀 + 输入中携带的相对路径 + group + version。
		versionPkg := types.PackageVersion{Package: path.Join(p.importBasePath, pth, gv.Group.NonEmpty(), gv.Version.String()), Version: gv.Version}
		if group, ok := seenGroups[gv.Group]; ok {
			// 同一 group 的多个版本归并到同一个 GroupVersions 中。
			vers := group.Versions
			vers = append(vers, versionPkg)
			seenGroups[gv.Group].Versions = vers
		} else {
			seenGroups[gv.Group] = &types.GroupVersions{
				PackageName: gv.Group.NonEmpty(),
				Group:       gv.Group,
				Versions:    []types.PackageVersion{versionPkg},
			}
		}
	}

	// 排序后再写入，保证生成顺序与输入顺序无关。
	var groupNames []string
	for groupName := range seenGroups {
		groupNames = append(groupNames, groupName.String())
	}
	sort.Strings(groupNames)
	*p.value = []types.GroupVersions{}
	for _, groupName := range groupNames {
		*p.value = append(*p.value, *seenGroups[types.Group(groupName)])
	}

	return nil
}

// readAsCSV 把逗号分隔的参数解析为字符串切片；空串视为空列表。
func readAsCSV(val string) ([]string, error) {
	if val == "" {
		return []string{}, nil
	}
	stringReader := strings.NewReader(val)
	csvReader := csv.NewReader(stringReader)
	return csvReader.Read()
}

// writeAsCSV 把切片编码成 CSV 字符串，并去掉 csv.Writer 附加的换行。
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
