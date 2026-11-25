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

package apiclient

import (
	"context"
	"encoding/json"
	"time"

	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/strategicpatch"
	"k8s.io/apimachinery/pkg/util/wait"
	clientset "k8s.io/client-go/kubernetes"

	kubeadmapi "k8s.io/kubernetes/cmd/kubeadm/app/apis/kubeadm"
	"k8s.io/kubernetes/cmd/kubeadm/app/constants"
	"k8s.io/kubernetes/cmd/kubeadm/app/util/errors"
)

// objectMutator 修改给定的运行时对象，并可返回错误。
type objectMutator[T runtime.Object] func(T) error

// apiCallRetryInterval 保存 apiCallRetryInterval 的本地副本，供测试使用。
var apiCallRetryInterval = constants.KubernetesAPICallRetryInterval

type kubernetesInterface[T kubernetesObject] interface {
	Create(ctx context.Context, obj T, opts metav1.CreateOptions) (T, error)
	Get(ctx context.Context, name string, opts metav1.GetOptions) (T, error)
	Update(ctx context.Context, obj T, opts metav1.UpdateOptions) (T, error)
}

type kubernetesObject interface {
	runtime.Object
	metav1.Object
}

// CreateOrUpdate 在目标资源不存在时创建运行时对象，已存在时更新它。
func CreateOrUpdate[T kubernetesObject](client kubernetesInterface[T], obj T) error {
	var lastError error
	err := wait.PollUntilContextTimeout(context.Background(),
		apiCallRetryInterval, kubeadmapi.GetActiveTimeouts().KubernetesAPICall.Duration,
		true, func(_ context.Context) (bool, error) {
			// API 调用使用后台 context，避免调用方收到其未预期的 context 相关错误。
			ctx := context.Background()
			if _, err := client.Create(ctx, obj, metav1.CreateOptions{}); err != nil {
				if !apierrors.IsAlreadyExists(err) {
					lastError = errors.Wrapf(err, "unable to create %T", obj)
					return false, nil
				}
				if _, err := client.Update(ctx, obj, metav1.UpdateOptions{}); err != nil {
					lastError = errors.Wrapf(err, "unable to update %T", obj)
					return false, nil
				}
			}
			return true, nil
		})
	if err == nil {
		return nil
	}
	return lastError
}

// CreateOrMutate 尝试创建对象；资源已存在时，从集群获取最新版本，
// 调用 mutator 回调修改后再更新。该函数可应对冲突：若对象在刷新与更新之间被服务端修改，
// 将重试。
func CreateOrMutate[T kubernetesObject](client kubernetesInterface[T], obj T, mutator objectMutator[T]) error {
	var lastError error
	err := wait.PollUntilContextTimeout(context.Background(),
		apiCallRetryInterval, kubeadmapi.GetActiveTimeouts().KubernetesAPICall.Duration,
		true, func(_ context.Context) (bool, error) {
			// API 调用使用后台 context，避免调用方收到其未预期的 context 相关错误。
			ctx := context.Background()
			if _, err := client.Create(ctx, obj, metav1.CreateOptions{}); err != nil {
				lastError = err
				if apierrors.IsAlreadyExists(err) {
					lastError = mutate(ctx, client, metav1.ObjectMeta{Namespace: obj.GetNamespace(), Name: obj.GetName()}, mutator)
					return lastError == nil, nil
				}
				return false, nil
			}
			return true, nil
		})
	if err == nil {
		return nil
	}
	return lastError
}

// mutate 根据对象元数据（命名空间和名称）获取资源，调用 mutator 回调修改后更新。
// 该函数可应对冲突：若对象在刷新与更新之间被服务端修改，将重试。
func mutate[T kubernetesObject](ctx context.Context, client kubernetesInterface[T], meta metav1.ObjectMeta, mutator objectMutator[T]) error {
	obj, err := client.Get(ctx, meta.Name, metav1.GetOptions{})
	if err != nil {
		return errors.Wrapf(err, "unable to get %T", obj)
	}
	if err = mutator(obj); err != nil {
		return errors.Wrapf(err, "unable to mutate %T", obj)
	}
	_, err = client.Update(ctx, obj, metav1.UpdateOptions{})
	return err
}

// CreateOrRetain 在目标资源不存在时创建运行时对象，已存在时保留它。
func CreateOrRetain[T kubernetesObject](client kubernetesInterface[T], obj T, name string) error {
	var lastError error
	err := wait.PollUntilContextTimeout(context.Background(),
		apiCallRetryInterval, kubeadmapi.GetActiveTimeouts().KubernetesAPICall.Duration,
		true, func(_ context.Context) (bool, error) {
			// API 调用使用后台 context，避免调用方收到其未预期的 context 相关错误。
			ctx := context.Background()
			if _, err := client.Get(ctx, name, metav1.GetOptions{}); err != nil {
				if !apierrors.IsNotFound(err) {
					lastError = errors.Wrapf(err, "unable to get %T", obj)
					return false, nil
				}
				if _, err := client.Create(ctx, obj, metav1.CreateOptions{}); err != nil {
					lastError = errors.Wrapf(err, "unable to create %T", obj)
					return false, nil
				}
			}
			return true, nil
		})
	if err == nil {
		return nil
	}
	return lastError
}

// PatchNodeOnce 在指定名称的 Node 对象上执行 patchFn。
func PatchNodeOnce(client clientset.Interface, nodeName string, patchFn func(*v1.Node), lastError *error) func(context.Context) (bool, error) {
	return func(_ context.Context) (bool, error) {
		// 先获取 Node 对象。
		ctx := context.Background()
		n, err := client.CoreV1().Nodes().Get(ctx, nodeName, metav1.GetOptions{})
		if err != nil {
			*lastError = err
			return false, nil // return false, nil // 任意错误均重试
		}

		// Node 刚出现时可能没有标签，因此等待其获得主机名标签。
		if _, found := n.ObjectMeta.Labels[v1.LabelHostname]; !found {
			return false, nil
		}

		oldData, err := json.Marshal(n)
		if err != nil {
			*lastError = errors.Wrapf(err, "failed to marshal unmodified node %q into JSON", n.Name)
			return false, *lastError
		}

		// 执行修改函数。
		patchFn(n)

		newData, err := json.Marshal(n)
		if err != nil {
			*lastError = errors.Wrapf(err, "failed to marshal modified node %q into JSON", n.Name)
			return false, *lastError
		}

		patchBytes, err := strategicpatch.CreateTwoWayMergePatch(oldData, newData, v1.Node{})
		if err != nil {
			*lastError = errors.Wrap(err, "failed to create two way merge patch")
			return false, *lastError
		}

		if _, err := client.CoreV1().Nodes().Patch(ctx, n.Name, types.StrategicMergePatchType, patchBytes, metav1.PatchOptions{}); err != nil {
			*lastError = errors.Wrapf(err, "error patching Node %q", n.Name)
			return false, nil
		}

		return true, nil
	}
}

// PatchNode 使用 patchFn 作为实际修改逻辑来修补 Node。
// 重试由 wait 包提供。
func PatchNode(client clientset.Interface, nodeName string, patchFn func(*v1.Node)) error {
	var lastError error
	err := wait.PollUntilContextTimeout(context.Background(),
		apiCallRetryInterval, kubeadmapi.GetActiveTimeouts().KubernetesAPICall.Duration,
		true, PatchNodeOnce(client, nodeName, patchFn, &lastError))
	if err == nil {
		return nil
	}
	return lastError
}

// GetConfigMapWithShortRetry 使用给定客户端获取 ConfigMap；遇到意外错误时短暂重试。
// 主要用于 API 无法及时提供 ConfigMap 时回退到默认值的场景。
func GetConfigMapWithShortRetry(client clientset.Interface, namespace, name string) (*v1.ConfigMap, error) {
	var cm *v1.ConfigMap
	var lastError error
	err := wait.PollUntilContextTimeout(context.Background(),
		time.Millisecond*50, time.Millisecond*350,
		true, func(_ context.Context) (bool, error) {
			var err error
			// 此处有意传入新的 context，使 API 调用不受父 context 的短超时影响而突然返回。
			cm, err = client.CoreV1().ConfigMaps(namespace).Get(context.Background(), name, metav1.GetOptions{})
			if err == nil {
				return true, nil
			}
			lastError = err
			return false, nil
		})
	if err == nil {
		return cm, nil
	}
	return nil, lastError
}
