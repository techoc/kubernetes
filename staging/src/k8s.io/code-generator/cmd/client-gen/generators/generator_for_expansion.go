/*
Copyright 2016 The Kubernetes Authors.

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

package generators

import (
	"io"
	"os"
	"path/filepath"
	"strings"

	"k8s.io/gengo/v2/generator"
	"k8s.io/gengo/v2/types"
)

// genExpansion 为一个 group client（如 ExtensionsClient）生成 expansion 文件。
// 每个类型对应一个空的 Expansion 接口，供使用者在不改生成代码的前提下
// 通过同名文件补充自定义方法。
type genExpansion struct {
	generator.GoGenerator
	groupPackagePath string
	// group 内的类型列表。
	types []*types.Type
}

// Filter 保证每个 group 只调用一次 GenerateType：仅在首个类型上返回 true。
func (g *genExpansion) Filter(c *generator.Context, t *types.Type) bool {
	return len(g.types) == 0 || t == g.types[0]
}

// GenerateType 为尚未手写 expansion 文件的类型输出 Expansion 接口。
// 若同目录已存在 <type>_expansion.go，则跳过，避免与手写文件重复声明。
func (g *genExpansion) GenerateType(c *generator.Context, t *types.Type, w io.Writer) error {
	sw := generator.NewSnippetWriter(w, c, "$", "$")
	for _, t := range g.types {
		// 检查手写文件是否存在；文件名约定为小写的 <type>_expansion.go。
		if _, err := os.Stat(filepath.Join(g.groupPackagePath, strings.ToLower(t.Name.Name+"_expansion.go"))); os.IsNotExist(err) {
			sw.Do(expansionInterfaceTemplate, t)
		}
	}
	return sw.Error()
}

// expansionInterfaceTemplate 生成空的扩展接口声明。
var expansionInterfaceTemplate = `
type $.|public$Expansion interface {}
`
