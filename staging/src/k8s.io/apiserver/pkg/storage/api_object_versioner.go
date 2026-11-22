/*
Copyright 2014 The Kubernetes Authors.

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

package storage

import (
	"fmt"
	"strconv"

	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/validation/field"
)

// APIObjectVersioner implements versioning and extracting etcd node information
// for objects that have an embedded ObjectMeta or ListMeta field.
//
// 该实现把 etcd 的 revision（uint64）编码为十进制字符串放入 resourceVersion，
// 因而可以直接做数值比较；不适用于使用其他版本语义的存储。
type APIObjectVersioner struct{}

// UpdateObject implements Versioner
// resourceVersion 为 0 表示“尚未持久化”，写入空字符串而不是 "0"。
func (a APIObjectVersioner) UpdateObject(obj runtime.Object, resourceVersion uint64) error {
	accessor, err := meta.Accessor(obj)
	if err != nil {
		return err
	}
	versionString := ""
	if resourceVersion != 0 {
		versionString = strconv.FormatUint(resourceVersion, 10)
	}
	accessor.SetResourceVersion(versionString)
	return nil
}

// UpdateList implements Versioner
// 列表必须带非零版本；同时写入 continue token 与剩余数量以支持分页。
func (a APIObjectVersioner) UpdateList(obj runtime.Object, resourceVersion uint64, nextKey string, count *int64) error {
	if resourceVersion == 0 {
		return fmt.Errorf("illegal resource version from storage: %d", resourceVersion)
	}
	listAccessor, err := meta.ListAccessor(obj)
	if err != nil || listAccessor == nil {
		return err
	}
	versionString := strconv.FormatUint(resourceVersion, 10)
	listAccessor.SetResourceVersion(versionString)
	listAccessor.SetContinue(nextKey)
	listAccessor.SetRemainingItemCount(count)
	return nil
}

// PrepareObjectForStorage clears resourceVersion and selfLink prior to writing to etcd.
// 这两个字段由服务器管理；若不清除，客户端可伪造版本号破坏 CAS 语义。
func (a APIObjectVersioner) PrepareObjectForStorage(obj runtime.Object) error {
	accessor, err := meta.Accessor(obj)
	if err != nil {
		return err
	}
	accessor.SetResourceVersion("")
	accessor.SetSelfLink("")
	return nil
}

// ObjectResourceVersion implements Versioner
// 空 resourceVersion 视为 0（对象尚未持久化）。
func (a APIObjectVersioner) ObjectResourceVersion(obj runtime.Object) (uint64, error) {
	accessor, err := meta.Accessor(obj)
	if err != nil {
		return 0, err
	}
	version := accessor.GetResourceVersion()
	if len(version) == 0 {
		return 0, nil
	}
	return strconv.ParseUint(version, 10, 64)
}

// ParseResourceVersion takes a resource version argument and converts it to
// the etcd version. For watch we should pass to helper.Watch(). Because resourceVersion is
// an opaque value, the default watch behavior for non-zero watch is to watch
// the next value (if you pass "1", you will see updates from "2" onwards).
//
// 非法值返回 Invalid 错误而非 500，使客户端得到可操作的 4xx 响应。
func (a APIObjectVersioner) ParseResourceVersion(resourceVersion string) (uint64, error) {
	if resourceVersion == "" || resourceVersion == "0" {
		return 0, nil
	}
	version, err := strconv.ParseUint(resourceVersion, 10, 64)
	if err != nil {
		return 0, NewInvalidError(field.ErrorList{
			// Validation errors are supposed to return version-specific field
			// paths, but this is probably close enough.
			field.Invalid(field.NewPath("resourceVersion"), resourceVersion, err.Error()),
		})
	}
	return version, nil
}

// Versioner implements Versioner
var _ Versioner = APIObjectVersioner{}

// CompareResourceVersion compares etcd resource versions.  Outside this API they are all strings,
// but etcd resource versions are special, they're actually ints, so we can easily compare them.
//
// 解析失败视为编程错误（对象未按 Versioner 写入）并 panic，避免静默给出错误排序。
func (a APIObjectVersioner) CompareResourceVersion(lhs, rhs runtime.Object) int {
	lhsVersion, err := a.ObjectResourceVersion(lhs)
	if err != nil {
		// coder error
		panic(err)
	}
	rhsVersion, err := a.ObjectResourceVersion(rhs)
	if err != nil {
		// coder error
		panic(err)
	}

	if lhsVersion == rhsVersion {
		return 0
	}
	if lhsVersion < rhsVersion {
		return -1
	}

	return 1
}
