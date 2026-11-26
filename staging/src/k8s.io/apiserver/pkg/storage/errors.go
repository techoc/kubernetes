/*
Copyright 2015 The Kubernetes Authors.

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

// 本文件集中定义 StorageError 及其判定函数。

package storage

import (
	"errors"
	"fmt"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/validation/field"
)

var (
	ErrResourceVersionSetOnCreate = errors.New("resourceVersion should not be set on objects to be created")
	ErrStorageNotReady            = errors.New("storage not ready")
)

// 存储错误的机器可读分类。ErrCode 使用 iota+1，使零值不会被误判为
// 某个合法错误码（零值 StorageError 不应匹配任何判定函数）。
const (
	ErrCodeKeyNotFound int = iota + 1
	ErrCodeKeyExists
	ErrCodeResourceVersionConflicts
	ErrCodeInvalidObj
	ErrCodeUnreachable
	ErrCodeTimeout
	ErrCodeCorruptObj
)

var errCodeToMessage = map[int]string{
	ErrCodeKeyNotFound:              "key not found",
	ErrCodeKeyExists:                "key exists",
	ErrCodeResourceVersionConflicts: "resource version conflicts",
	ErrCodeInvalidObj:               "invalid object",
	ErrCodeUnreachable:              "server unreachable",
	ErrCodeTimeout:                  "request timeout",
	ErrCodeCorruptObj:               "corrupt object",
}

func NewKeyNotFoundError(key string, rv int64) *StorageError {
	return &StorageError{
		Code:            ErrCodeKeyNotFound,
		Key:             key,
		ResourceVersion: rv,
	}
}

func NewKeyExistsError(key string, rv int64) *StorageError {
	return &StorageError{
		Code:            ErrCodeKeyExists,
		Key:             key,
		ResourceVersion: rv,
	}
}

func NewResourceVersionConflictsError(key string, rv int64) *StorageError {
	return &StorageError{
		Code:            ErrCodeResourceVersionConflicts,
		Key:             key,
		ResourceVersion: rv,
	}
}

func NewUnreachableError(key string, rv int64) *StorageError {
	return &StorageError{
		Code:            ErrCodeUnreachable,
		Key:             key,
		ResourceVersion: rv,
	}
}

// NewTimeoutError 构造超时错误；msg 会作为附加错误保留，便于上层日志定位。
func NewTimeoutError(key, msg string) *StorageError {
	return &StorageError{
		Code: ErrCodeTimeout,
		Key:  key,
		err:  errors.New(msg),
	}
}

func NewInvalidObjError(key, msg string) *StorageError {
	return &StorageError{
		Code: ErrCodeInvalidObj,
		Key:  key,
		err:  errors.New(msg),
	}
}

// NewCorruptObjError 返回一个表示对象损坏的 StorageError：
// a) 从存储取回的对象数据无法按给定 err 转换；
// b) 给定对象无法按给定 err 解码。
func NewCorruptObjError(key string, err error) *StorageError {
	return &StorageError{
		Code: ErrCodeCorruptObj,
		Key:  key,
		err:  err,
	}
}

// StorageError 是存储层错误的统一表示；Code 决定上层如何映射为 API 错误。
type StorageError struct {
	Code            int
	Key             string
	ResourceVersion int64

	// 内部错误
	err error
}

// Unwrap 暴露内部错误，使 errors.Is/As 可以穿透。
func (e *StorageError) Unwrap() error { return e.err }

// Error 输出包含所有定位信息；errCodeToMessage 缺失时 Message 为空，
// 但仍保留数字 Code 便于排查。
func (e *StorageError) Error() string {
	return fmt.Sprintf("StorageError: %s, Code: %d, Key: %s, ResourceVersion: %d, AdditionalErrorMsg: %v",
		errCodeToMessage[e.Code], e.Code, e.Key, e.ResourceVersion, e.err)
}

// IsNotFound 仅当 err 是 "key" 未找到错误时返回 true。
func IsNotFound(err error) bool {
	return isErrCode(err, ErrCodeKeyNotFound)
}

// IsExist 仅当 err 是 "key" 已存在错误时返回 true。
func IsExist(err error) bool {
	return isErrCode(err, ErrCodeKeyExists)
}

// IsUnreachable 仅当 err 表示无法访问服务端时返回 true。
func IsUnreachable(err error) bool {
	return isErrCode(err, ErrCodeUnreachable)
}

// IsConflict 仅当 err 是写冲突时返回 true。
func IsConflict(err error) bool {
	return isErrCode(err, ErrCodeResourceVersionConflicts)
}

// IsRequestTimeout 仅当 err 表示请求已超时时返回 true。
func IsRequestTimeout(err error) bool {
	return isErrCode(err, ErrCodeTimeout)
}

// IsInvalidObj 仅当 err 是 invalid 错误时返回 true。
func IsInvalidObj(err error) bool {
	return isErrCode(err, ErrCodeInvalidObj)
}

// IsCorruptObject 仅当满足以下任一条件时返回 true：
// a) 从存储取回的对象数据无法转换；或
// b) 给定对象无法正确解码。
//
// 与 isErrCode 不同，这里使用 errors.As 以支持被包装的错误链。
func IsCorruptObject(err error) bool {
	if err == nil {
		return false
	}
	var storageErr *StorageError
	if !errors.As(err, &storageErr) {
		return false
	}

	return storageErr.Code == ErrCodeCorruptObj
}

func isErrCode(err error, code int) bool {
	if err == nil {
		return false
	}
	if e, ok := err.(*StorageError); ok {
		return e.Code == code
	}
	return false
}

// InvalidError 表示存储包中因 API 对象非法而产生的错误。
// 它汇总了所有字段级校验失败项。
type InvalidError struct {
	Errs field.ErrorList
}

// Error 聚合所有字段级错误，保证调用方能看到全部校验失败项。
func (e InvalidError) Error() string {
	return e.Errs.ToAggregate().Error()
}

// IsInvalidError 仅当 err 是 InvalidError 时返回 true。
func IsInvalidError(err error) bool {
	_, ok := err.(InvalidError)
	return ok
}

func NewInvalidError(errors field.ErrorList) InvalidError {
	return InvalidError{errors}
}

// InternalError 表示存储包内部产生的错误，而非底层存储后端（如 etcd）的错误。
// 而非来自底层存储后端（如 etcd）。
type InternalError struct {
	Reason string

	// 保留内部错误以便维护错误树、支持正确的错误检查，
	// 同时保持向后兼容。
	//
	// Reason 是公开的稳定文本，err 仅用于错误链，不参与 Error() 输出。
	err error
}

// Error 只返回 Reason，保持与历史输出格式兼容。
func (e InternalError) Error() string {
	return e.Reason
}

// IsInternalError 仅当 err 是 InternalError 时返回 true。
func (e InternalError) Unwrap() error { return e.err }

// IsInternalError 仅当 err 是 InternalError 时返回 true。
func IsInternalError(err error) bool {
	_, ok := err.(InternalError)
	return ok
}

func NewInternalError(err error) InternalError {
	return InternalError{Reason: err.Error(), err: err}
}

var tooLargeResourceVersionCauseMsg = "Too large resource version"

// NewTooLargeResourceVersionError 返回一个超时错误，附带给定 retrySeconds，
// 用于请求的最小 resourceVersion 大于当前可用的最大 resourceVersion 的场景。
//
// 使用 Timeout + 特定 Cause 而不是自定义错误类型：这样既能让通用客户端
// 识别为可重试，又能让 watch 缓存逻辑通过 IsTooLargeResourceVersion
// 精确区分“版本尚未追上”与真正的超时。
func NewTooLargeResourceVersionError(minimumResourceVersion, currentRevision uint64, retrySeconds int) error {
	err := apierrors.NewTimeoutError(fmt.Sprintf("Too large resource version: %d, current: %d", minimumResourceVersion, currentRevision), retrySeconds)
	err.ErrStatus.Details.Causes = []metav1.StatusCause{
		{
			Type:    metav1.CauseTypeResourceVersionTooLarge,
			Message: tooLargeResourceVersionCauseMsg,
		},
	}
	return err
}

// IsTooLargeResourceVersion 判断错误是否为 TooLargeResourceVersion 错误。
func IsTooLargeResourceVersion(err error) bool {
	if !apierrors.IsTimeout(err) {
		return false
	}
	return apierrors.HasStatusCause(err, metav1.CauseTypeResourceVersionTooLarge)
}
