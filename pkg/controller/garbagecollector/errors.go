/*
Copyright 2017 The Kubernetes Authors.

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

package garbagecollector

import (
	"fmt"
)

// restMappingError 表示无法获取某个 GroupVersionKind 的 REST 映射。
// GC 依赖 REST 映射确定如何删除对象；映射失败通常意味着该类型已从集群中移除。
type restMappingError struct {
	kind    string // 失败的 Kind
	version string // 失败的 API 版本
}

// Error 实现 error 接口，给出简洁的映射失败描述。
func (r *restMappingError) Error() string {
	versionKind := fmt.Sprintf("%s/%s", r.version, r.kind)
	return fmt.Sprintf("unable to get REST mapping for %s.", versionKind)
}

// Message 在错误描述之外补充人工处置建议：
// 若该类型确实已不存在，所有者引用将成为无法解析的悬空引用，
// 需要管理员手动清理引用该类型对象的 ownerReferences。
func (r *restMappingError) Message() string {
	versionKind := fmt.Sprintf("%s/%s", r.version, r.kind)
	errMsg := fmt.Sprintf("unable to get REST mapping for %s. ", versionKind)
	errMsg += fmt.Sprintf(" If %s is an invalid resource, then you should manually remove ownerReferences that refer %s objects.", versionKind, versionKind)
	return errMsg
}

// newRESTMappingError 构造映射失败错误，封装出错的 Kind 与版本。
func newRESTMappingError(kind, version string) *restMappingError {
	return &restMappingError{kind: kind, version: version}
}
