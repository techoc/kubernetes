/*
Copyright 2019 The Kubernetes Authors.

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

package resource

import (
	"bytes"

	"sigs.k8s.io/kustomize/api/krusty"
	"sigs.k8s.io/kustomize/kyaml/filesys"
)

// KustomizeVisitor 处理 kustomization.yaml 文件。
type KustomizeVisitor struct {
	mapper *mapper
	schema ContentValidator
	// 预期包含 kustomization 文件的目录。
	dirPath string
	// 包含 dirPath 的文件系统。
	fSys filesys.FileSystem
	// 保存 kustomize build 的结果，供测试检查。
	yml []byte
}

// Visit 把 kustomize build 的结果交给 StreamVisitor。
//
// 复用 StreamVisitor 处理构建产物，使 -k 与 -f 在后续解析、校验、
// 遍历行为上完全一致；目录本身作为 base path 传给 StreamVisitor，
// 以便相对路径与错误信息中的位置可正确解析。
func (v *KustomizeVisitor) Visit(fn VisitorFunc) error {
	kOpts := krusty.MakeDefaultOptions()
	// 使用 legacy 排序以兼容旧版 kubectl 的 -k 输出顺序，避免改变 apply 顺序。
	kOpts.Reorder = krusty.ReorderOptionLegacy
	k := krusty.MakeKustomizer(kOpts)
	m, err := k.Run(v.fSys, v.dirPath)
	if err != nil {
		return err
	}
	v.yml, err = m.AsYaml()
	if err != nil {
		return err
	}
	sv := NewStreamVisitor(
		bytes.NewReader(v.yml), v.mapper, v.dirPath, v.schema)
	return sv.Visit(fn)
}
