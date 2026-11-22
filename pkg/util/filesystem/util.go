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

package filesystem

import (
	"path/filepath"
)

// IsPathClean 判断路径是否已经是规范形式。
// 先把斜杠统一转换为系统分隔符，再比较 Clean 前后的结果，
// 避免不同平台分隔符差异导致误判。
func IsPathClean(path string) bool {
	return filepath.ToSlash(filepath.Clean(path)) == filepath.ToSlash(path)
}
