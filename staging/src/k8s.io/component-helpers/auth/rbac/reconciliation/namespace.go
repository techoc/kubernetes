/*
Copyright 2018 The Kubernetes Authors.

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

package reconciliation

import (
	"context"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	utilerrors "k8s.io/apimachinery/pkg/util/errors"
	corev1client "k8s.io/client-go/kubernetes/typed/core/v1"
)

// tryEnsureNamespace 尽力确保 namespace 存在：先查后建，并忽略 Forbidden 错误。
// 这里吞掉 Forbidden 是刻意的：权限模型允许只授予角色和角色绑定的写权限，
// 若把命名空间不可读也当成致命错误，这类受限用户就完全用不了对账流程。
// 其他意外错误（例如服务端不可达）仍要向上抛，避免掩盖真实故障。
func tryEnsureNamespace(client corev1client.NamespaceInterface, namespace string) error {
	_, getErr := client.Get(context.TODO(), namespace, metav1.GetOptions{})
	if getErr == nil {
		return nil
	}

	// NotFound 是接下来要创建的正常前提，Forbidden 代表权限受限只能继续尝试；
	// 其余错误说明这次读取本身就不可信，不应再盲目发起创建。
	if fatalGetErr := utilerrors.FilterOut(getErr, apierrors.IsNotFound, apierrors.IsForbidden); fatalGetErr != nil {
		return fatalGetErr
	}

	ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: namespace}}
	_, createErr := client.Create(context.TODO(), ns, metav1.CreateOptions{})

	// AlreadyExists 可能来自创建与查询之间的竞态，Forbidden 是预期中的权限受限；
	// 两者都算目标已达成，不值得让调用方重试。
	return utilerrors.FilterOut(createErr, apierrors.IsAlreadyExists, apierrors.IsForbidden)
}
