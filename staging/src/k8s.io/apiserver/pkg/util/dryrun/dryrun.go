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

// Package dryrun 提供对 DryRun 选项的判定。单独成包是为了让 registry
// 等底层代码无需依赖 metav1 选项类型。
package dryrun

// IsDryRun 判断 DryRun flag 是否表示真正的 dry-run。
//
// 约定：空切片（未指定）表示真实执行；只要包含任意值即视为 dry-run。
// 具体合法值（All 等）由 API 校验负责，此处不做值检查。
func IsDryRun(flag []string) bool {
	return len(flag) > 0
}
