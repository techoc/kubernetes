/*
Copyright 2024 The Kubernetes Authors.

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

// 本文件定义 csaupgrade 的选项。

package csaupgrade

// Option 以函数选项模式配置升级行为。
type Option func(*options)

// Subresource set the subresource to upgrade from CSA to SSA.
//
// 子资源（如 status）有独立的字段集与属主，必须单独迁移。
func Subresource(s string) Option {
	return func(opts *options) {
		opts.subresource = s
	}
}

// options 保存 Option 应用后的最终配置；空 subresource 表示主资源。
type options struct {
	subresource string
}
