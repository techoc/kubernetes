/*
Copyright 2016 The Kubernetes Authors.

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

package etcd3

import (
	goerrors "errors"
	"net/http"

	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apiserver/pkg/storage"

	etcdrpc "go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
)

// interpretWatchError 把 etcd 的 watch 错误翻译成 API 语义错误。
// 中文说明：ErrCompacted 必须映射为 410 Gone（ResourceExpired），客户端据此重建
// watch 并回到最新版本；未识别的错误原样返回，避免把未知故障伪装成正常语义。
func interpretWatchError(err error) error {
	switch {
	case err == etcdrpc.ErrCompacted:
		return errors.NewResourceExpired("The resourceVersion for the provided watch is too old.")
	}

	// 损坏对象被删除属于存储层内部问题，用 StoreReadError 告知客户端重试无益。
	var corruptobjDeletedErr *corruptObjectDeletedError
	if goerrors.As(err, &corruptobjDeletedErr) {
		return &errors.StatusError{
			ErrStatus: metav1.Status{
				Status:  metav1.StatusFailure,
				Code:    http.StatusInternalServerError,
				Reason:  metav1.StatusReasonStoreReadError,
				Message: corruptobjDeletedErr.Error(),
			},
		}
	}

	return err
}

// 这些消息是面向用户的可操作提示：告诉客户端如何恢复（去掉/更换 continue 参数）。
const (
	expired         string = "The resourceVersion for the provided list is too old."
	continueExpired string = "The provided continue parameter is too old " +
		"to display a consistent list result. You can start a new list without " +
		"the continue parameter."
	inconsistentContinue string = "The provided continue parameter is too old " +
		"to display a consistent list result. You can start a new list without " +
		"the continue parameter, or use the continue token in this response to " +
		"retrieve the remainder of the results. Continuing with the provided " +
		"token results in an inconsistent list - objects that were created, " +
		"modified, or deleted between the time the first chunk was returned " +
		"and now may show up in the list."
)

// interpretListError 把 list 阶段的 compaction 错误翻译为 ResourceExpired。
// 中文说明：分页场景下不能只报错——要给出可继续的新 token，否则大列表在中途
// compaction 后永远无法完成。
func interpretListError(err error, paging bool, continueKey, keyPrefix string) error {
	switch {
	case err == etcdrpc.ErrCompacted:
		if paging {
			return handleCompactedErrorForPaging(continueKey, keyPrefix)
		}
		return errors.NewResourceExpired(expired)
	}
	return err
}

func handleCompactedErrorForPaging(continueKey, keyPrefix string) error {
	// continueToken.ResoureVersion=-1 means that the apiserver can
	// continue the list at the latest resource version. We don't use rv=0
	// for this purpose to distinguish from a bad token that has empty rv.
	// 中文说明：-1 编码“从最新版本继续”，与空 rv（非法 token）区分开。
	newToken, err := storage.EncodeContinue(continueKey, keyPrefix, -1)
	if err != nil {
		utilruntime.HandleError(err)
		return errors.NewResourceExpired(continueExpired)
	}
	statusError := errors.NewResourceExpired(inconsistentContinue)
	statusError.ErrStatus.ListMeta.Continue = newToken
	return statusError
}
