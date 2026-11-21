/*
Copyright 2019 The Kubernetes Authors.

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

package protectionutil

import (
	"slices"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// IsDeletionCandidate 判断对象是否处于删除流程中且仍带有指定 finalizer。
// 两个条件缺一不可：没有删除时间戳说明用户并未请求删除；没有该 finalizer
// 说明本控制器无需参与清理，避免误触发其他控制器的删除逻辑。
func IsDeletionCandidate(obj metav1.Object, finalizer string) bool {
	return obj.GetDeletionTimestamp() != nil && slices.Contains(obj.GetFinalizers(), finalizer)
}

// NeedToAddFinalizer 判断是否需要给对象添加指定 finalizer。
// 已在删除中的对象不能再添加 finalizer（apiserver 会拒绝），
// 而已持有该 finalizer 时重复添加只会产生无意义的更新请求。
func NeedToAddFinalizer(obj metav1.Object, finalizer string) bool {
	return obj.GetDeletionTimestamp() == nil && !slices.Contains(obj.GetFinalizers(), finalizer)
}
