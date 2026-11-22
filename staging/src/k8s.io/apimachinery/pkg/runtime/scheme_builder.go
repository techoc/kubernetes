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

package runtime

// SchemeBuilder 收集向 scheme 注册内容的函数。
// 这样生成代码可以编译而不必显式引用生成的类型；每个会产生生成的深拷贝或转换函数的包
// 都应声明一个 SchemeBuilder。
type SchemeBuilder []func(*Scheme) error

// AddToScheme 把所有已保存的函数依次应用到 scheme。
// 返回非 nil 错误表示某个函数失败，且注册过程就此中止（已执行的部分不会回滚）。
func (sb *SchemeBuilder) AddToScheme(s *Scheme) error {
	// 顺序执行：注册函数之间可能存在依赖（如先注册类型再注册转换），因此不能并行或乱序。
	for _, f := range *sb {
		if err := f(s); err != nil {
			return err
		}
	}
	return nil
}

// Register 向列表追加一个 scheme 初始化函数。
func (sb *SchemeBuilder) Register(funcs ...func(*Scheme) error) {
	for _, f := range funcs {
		*sb = append(*sb, f)
	}
}

// NewSchemeBuilder 替你调用 Register，返回一个已包含给定函数的 SchemeBuilder。
func NewSchemeBuilder(funcs ...func(*Scheme) error) SchemeBuilder {
	var sb SchemeBuilder
	sb.Register(funcs...)
	return sb
}
