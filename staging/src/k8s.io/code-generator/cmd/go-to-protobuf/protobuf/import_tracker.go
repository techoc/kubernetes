/*
Copyright 2015 The Kubernetes Authors.

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

package protobuf

import (
	"k8s.io/gengo/v2/namer"
	"k8s.io/gengo/v2/types"
)

// ImportTracker 是 protobuf 生成专用的 import 追踪器。
// 默认实现按 Go 包名去重，而 .proto 的 import 是文件级（如 gogo.proto），
// 因此这里把“类型名”替换为包名、并直接输出路径，避免生成重复或错误的 import。
type ImportTracker struct {
	namer.DefaultImportTracker
}

// NewImportTracker 构造一个仅收集 Protobuf 类型、并按包名去重的追踪器。
func NewImportTracker(local types.Name, typesToAdd ...*types.Type) *ImportTracker {
	tracker := namer.NewDefaultImportTracker(local)
	// 只关心 Protobuf 类型：Go 内建类型会被映射成 proto 标量，无需 import。
	tracker.IsInvalidType = func(t *types.Type) bool { return t.Kind != types.Protobuf }
	// proto import 的键是 package，而不是类型名。
	tracker.LocalName = func(name types.Name) string { return name.Package }
	// 生成 .proto 时只写路径，不加 Go 别名。
	tracker.PrintImport = func(path, name string) string { return path }

	tracker.AddTypes(typesToAdd...)
	return &ImportTracker{
		DefaultImportTracker: tracker,
	}
}

// AddNullable 注册 nullable 扩展，确保生成代码能导入对应的 gogo.proto。
func (tracker *ImportTracker) AddNullable() {
	tracker.AddType(&types.Type{
		Kind: types.Protobuf,
		Name: types.Name{
			Name:    "nullable",
			Package: "gogoproto",
			Path:    "github.com/gogo/protobuf/gogoproto/gogo.proto",
		},
	})
}
