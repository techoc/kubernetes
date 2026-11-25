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

package request

import (
	"context"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apiserver/pkg/authentication/user"
)

// key 类型未导出，用于避免与其他包的 context key 冲突；这是 Go context 的推荐做法，
// 不要改成字符串常量。
type key int

const (
	// namespaceKey 是请求命名空间在 context 中的 key。
	namespaceKey key = iota

	// userKey 是请求用户在 context 中的 key。
	userKey
)

// NewContext 为请求处理流程实例化一个基础 context。
// 这里用 context.TODO() 而不是 Background()，表示该 context 尚未绑定
// 具体请求；过滤器链路随后会用真实请求的 context 替换它。
func NewContext() context.Context {
	return context.TODO()
}

// NewDefaultContext 为默认命名空间中的请求处理流程实例化一个基础 context。
func NewDefaultContext() context.Context {
	return WithNamespace(NewContext(), metav1.NamespaceDefault)
}

// WithValue 返回 parent 的副本，其中与 key 关联的值被设置为 val。
func WithValue(parent context.Context, key interface{}, val interface{}) context.Context {
	return context.WithValue(parent, key, val)
}

// WithNamespace 返回 parent 的副本，其中 namespace 值已被设置。
func WithNamespace(parent context.Context, namespace string) context.Context {
	return WithValue(parent, namespaceKey, namespace)
}

// NamespaceFrom 返回 ctx 上 namespace key 的值。
// 第二个返回值区分“显式设置为空”与“从未设置”，调用方据此决定
// 是否回退到默认值。
func NamespaceFrom(ctx context.Context) (string, bool) {
	namespace, ok := ctx.Value(namespaceKey).(string)
	return namespace, ok
}

// NamespaceValue 返回 ctx 上 namespace key 的值，不存在时返回空字符串。
func NamespaceValue(ctx context.Context) string {
	namespace, _ := NamespaceFrom(ctx)
	return namespace
}

// WithUser 返回 parent 的副本，其中 user 值已被设置。
func WithUser(parent context.Context, user user.Info) context.Context {
	return WithValue(parent, userKey, user)
}

// UserFrom 返回 ctx 上 user key 的值。
func UserFrom(ctx context.Context) (user.Info, bool) {
	user, ok := ctx.Value(userKey).(user.Info)
	return user, ok
}
