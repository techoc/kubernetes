//go:build linux

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

package util

import "os"

// lchown 修改指定文件的数字 uid 和 gid。
// 如果文件是符号链接，修改的是链接本身而不是它指向的目标。
// 卷内容可能包含符号链接，跟随链接会误改不该动的文件，因此这里必须用 Lchown。
func (w *AtomicWriter) lchown(name string, uid, gid int) error {
	return os.Lchown(name, uid, gid)
}
