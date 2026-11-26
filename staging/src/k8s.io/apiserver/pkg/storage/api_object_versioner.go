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

// APIObjectVersioner 为内嵌 ObjectMeta 或 ListMeta 字段的对象实现版本控制，并从 etcd 节点提取信息。
//
// 该实现把 etcd 的 revision（uint64）编码为十进制字符串放入 resourceVersion，
// 因而可以直接做数值比较；不适用于使用其他版本语义的存储。
type APIObjectVersioner struct{}

// UpdateObject 实现 Versioner 接口。
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

// UpdateList 实现 Versioner 接口。
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

// PrepareObjectForStorage 在写入 etcd 前清除 resourceVersion 和 selfLink。
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

// ObjectResourceVersion 实现 Versioner 接口。
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

// ParseResourceVersion 把资源版本参数转换为 etcd 版本。watch 场景会把它传给 helper.Watch()。由于 resourceVersion 是不透明值，
// 非零 watch 默认监听下一个版本（传入 "1" 时，会看到从 "2" 开始的更新）。
//
// 非法值返回 Invalid 错误而非 500，使客户端得到可操作的 4xx 响应。
func (a APIObjectVersioner) ParseResourceVersion(resourceVersion string) (uint64, error) {
	if resourceVersion == "" || resourceVersion == "0" {
		return 0, nil
	}
	version, err := strconv.ParseUint(resourceVersion, 10, 64)
	if err != nil {
		return 0, NewInvalidError(field.ErrorList{
			// 校验错误本应返回版本特定的字段路径，但这里已经足够接近。
			field.Invalid(field.NewPath("resourceVersion"), resourceVersion, err.Error()),
		})
	}
	return version, nil
}

// Versioner 实现 Versioner 接口。
var _ Versioner = APIObjectVersioner{}

// CompareResourceVersion 比较两个 etcd 资源版本。在本 API 之外它们都是字符串，
// 但 etcd 的资源版本实际上是整数，因此可以直接比较。
//
// 解析失败视为编程错误（对象未按 Versioner 写入）并 panic，避免静默给出错误排序。
func (a APIObjectVersioner) CompareResourceVersion(lhs, rhs runtime.Object) int {
	lhsVersion, err := a.ObjectResourceVersion(lhs)
	if err != nil {
		// 解码器错误
		panic(err)
	}
	rhsVersion, err := a.ObjectResourceVersion(rhs)
	if err != nil {
		// 解码器错误
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
