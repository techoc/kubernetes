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

// Package dump 是 k8s.io/utils/dump 的兼容转发层，
// 保留旧函数名以平滑迁移，新代码应直接使用 k8s.io/utils/dump。
package dump

import (
	"k8s.io/utils/dump"
)

// Deprecated: 请改用 k8s.io/utils/dump.Pretty。
//
//go:fix inline
func Pretty(a interface{}) string {
	return dump.Pretty(a)
}

// Deprecated: 请改用 k8s.io/utils/dump.ForHash。
//
//go:fix inline
func ForHash(a interface{}) string {
	return dump.ForHash(a)
}

// Deprecated: 请改用 k8s.io/utils/dump.OneLine。
//
//go:fix inline
func OneLine(a interface{}) string {
	return dump.OneLine(a)
}
