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

package errors

import (
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apiserver/pkg/storage"
)

// InterpretListError 把检索操作中
// 的通用错误转为相应的 API 错误。
//
// 映射原则：存储层的“键不存在”对应 404，“不可达/超时”对应 503 并建议
// 客户端稍后重试；无法识别的错误原样返回，避免掩盖真实故障。
func InterpretListError(err error, qualifiedResource schema.GroupResource) error {
	switch {
	case storage.IsNotFound(err):
		return errors.NewNotFound(qualifiedResource, "")
	case storage.IsUnreachable(err), storage.IsRequestTimeout(err):
		return errors.NewServerTimeout(qualifiedResource, "list", 2) // TODO: 做成可配置或在更高层处理
	case storage.IsInternalError(err):
		return errors.NewInternalError(err)
	case storage.IsCorruptObject(err):
		return errors.NewInternalError(err)
	default:
		return err
	}
}

// InterpretGetError 把检索操作中的通用错误
// 的通用错误转为相应的 API 错误。
//
// get 与 list 的区别仅在于 404 需要带上对象名。
func InterpretGetError(err error, qualifiedResource schema.GroupResource, name string) error {
	switch {
	case storage.IsNotFound(err):
		return errors.NewNotFound(qualifiedResource, name)
	case storage.IsUnreachable(err):
		return errors.NewServerTimeout(qualifiedResource, "get", 2) // TODO: 做成可配置或在更高层处理
	case storage.IsInternalError(err):
		return errors.NewInternalError(err)
	case storage.IsCorruptObject(err):
		return errors.NewInternalError(err)
	default:
		return err
	}
}

// InterpretCreateError 把创建操作中的通用错误
// 的通用错误转为相应的 API 错误。
//
// 已存在对应 409 AlreadyExists；create 阶段出现 conflict 也归入此类。
func InterpretCreateError(err error, qualifiedResource schema.GroupResource, name string) error {
	switch {
	case storage.IsExist(err):
		return errors.NewAlreadyExists(qualifiedResource, name)
	case storage.IsUnreachable(err):
		return errors.NewServerTimeout(qualifiedResource, "create", 2) // TODO: 做成可配置或在更高层处理
	case storage.IsInternalError(err):
		return errors.NewInternalError(err)
	case storage.IsCorruptObject(err):
		return errors.NewInternalError(err)
	default:
		return err
	}
}

// InterpretUpdateError 把更新操作中的通用错误
// 的通用错误转为相应的 API 错误。
//
// conflict/exist/invalid 都映射为 409 Conflict：它们都表示“基于当前状态
// 无法完成写入，客户端需重新读取后重试”。
func InterpretUpdateError(err error, qualifiedResource schema.GroupResource, name string) error {
	switch {
	case storage.IsConflict(err), storage.IsExist(err), storage.IsInvalidObj(err):
		return errors.NewConflict(qualifiedResource, name, err)
	case storage.IsUnreachable(err):
		return errors.NewServerTimeout(qualifiedResource, "update", 2) // TODO: 做成可配置或在更高层处理
	case storage.IsNotFound(err):
		return errors.NewNotFound(qualifiedResource, name)
	case storage.IsInternalError(err):
		return errors.NewInternalError(err)
	case storage.IsCorruptObject(err):
		return errors.NewInternalError(err)
	default:
		return err
	}
}

// InterpretDeleteError 把删除操作中的通用错误
// 的通用错误转为相应的 API 错误。
//
// 注意 delete 的判定顺序：先 404 再 conflict，因为“对象已不存在”是删除
// 场景最常见的并发结果，应优先给出 404 而非 409。
func InterpretDeleteError(err error, qualifiedResource schema.GroupResource, name string) error {
	switch {
	case storage.IsNotFound(err):
		return errors.NewNotFound(qualifiedResource, name)
	case storage.IsUnreachable(err):
		return errors.NewServerTimeout(qualifiedResource, "delete", 2) // TODO: 做成可配置或在更高层处理
	case storage.IsConflict(err), storage.IsExist(err), storage.IsInvalidObj(err):
		return errors.NewConflict(qualifiedResource, name, err)
	case storage.IsInternalError(err):
		return errors.NewInternalError(err)
	case storage.IsCorruptObject(err):
		return errors.NewInternalError(err)
	default:
		return err
	}
}

// InterpretWatchError 把 watch 操作中的通用错误
// 的通用错误转为相应的 API 错误。
//
// watch 的 InvalidError 映射为 422（Invalid），因为请求本身合法，但
// 指定的选择条件对存储层无效。
func InterpretWatchError(err error, resource schema.GroupResource, name string) error {
	switch {
	case storage.IsInvalidError(err):
		invalidError, _ := err.(storage.InvalidError)
		return errors.NewInvalid(schema.GroupKind{Group: resource.Group, Kind: resource.Resource}, name, invalidError.Errs)
	case storage.IsInternalError(err):
		return errors.NewInternalError(err)
	case storage.IsCorruptObject(err):
		return errors.NewInternalError(err)
	default:
		return err
	}
}
