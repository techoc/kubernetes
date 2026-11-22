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

// Package generators 实现 lister-gen 的代码生成器。
// 生成的 lister 提供只读缓存访问，并预留 expansion 接口供手写扩展。
package generators

import (
	"io"
	"os"
	"path/filepath"
	"strings"

	"k8s.io/gengo/v2/generator"
	"k8s.io/gengo/v2/types"
	"k8s.io/klog/v2"

	"k8s.io/code-generator/cmd/client-gen/generators/util"
)

// expansionGenerator produces a file for a expansion interfaces.
//
// expansion 接口用于在不修改生成代码的前提下给 lister 增加自定义方法。
type expansionGenerator struct {
	generator.GoGenerator
	outputPath string
	types      []*types.Type
}

// We only want to call GenerateType() once per group.
//
// 以第一个类型作为哨兵：gengo 按类型回调，但整个文件只应渲染一次。
func (g *expansionGenerator) Filter(c *generator.Context, t *types.Type) bool {
	return t == g.types[0]
}

// GenerateType 为每个类型产出 expansion 接口。
//
// 若同目录下已存在手写的 <type>_expansion.go，则跳过该类型，避免覆盖
// 用户的自定义方法；这是 expansion 机制的核心约定。
func (g *expansionGenerator) GenerateType(c *generator.Context, t *types.Type, w io.Writer) error {
	sw := generator.NewSnippetWriter(w, c, "$", "$")
	for _, t := range g.types {
		tags := util.MustParseClientGenTags(append(t.SecondClosestCommentLines, t.CommentLines...))
		manualFile := filepath.Join(g.outputPath, strings.ToLower(t.Name.Name+"_expansion.go"))
		if _, err := os.Stat(manualFile); err == nil {
			klog.V(4).Infof("file %q exists, not generating", manualFile)
		} else if os.IsNotExist(err) {
			sw.Do(expansionInterfaceTemplate, t)
			// cluster-scoped 类型没有 NamespaceLister，也就无需其 expansion 接口。
			if !tags.NonNamespaced {
				sw.Do(namespacedExpansionInterfaceTemplate, t)
			}
		} else {
			return err
		}
	}
	return sw.Error()
}

var expansionInterfaceTemplate = `
// $.|public$ListerExpansion allows custom methods to be added to
// $.|public$Lister.
type $.|public$ListerExpansion interface {}
`

var namespacedExpansionInterfaceTemplate = `
// $.|public$NamespaceListerExpansion allows custom methods to be added to
// $.|public$NamespaceLister.
type $.|public$NamespaceListerExpansion interface {}
`
