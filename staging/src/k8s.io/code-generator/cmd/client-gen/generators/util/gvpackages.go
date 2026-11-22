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

package util

import "strings"

// ParsePathGroupVersion 把 "path/to/pkg/group/version" 拆成导入路径与
// "group/version" 两段。约定最后两段分别是 group 和 version，因此段数不足三段时
// 无法区分路径，整体按 groupVersion 返回、路径为空。
//
// 注意：这是按字符串后缀切分，不校验语义；"k8s.io/api/core/v1" 得到
// ("k8s.io/api", "core/v1")，而 "v1" 得到 ("", "v1")。
func ParsePathGroupVersion(pgvString string) (gvPath string, gvString string) {
	subs := strings.Split(pgvString, "/")
	length := len(subs)
	switch length {
	case 0, 1, 2:
		return "", pgvString
	default:
		return strings.Join(subs[:length-2], "/"), strings.Join(subs[length-2:], "/")
	}
}
