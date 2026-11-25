/*
Copyright 2024 The Kubernetes Authors.

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

package wait

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/cli-runtime/pkg/resource"
	"k8s.io/client-go/tools/cache"
	watchtools "k8s.io/client-go/tools/watch"
	"k8s.io/kubectl/pkg/util/interrupt"
)

// IsDeleted 是用于等待某对象被删除的条件函数
func IsDeleted(ctx context.Context, info *resource.Info, o *WaitOptions) (runtime.Object, bool, error) {
	if len(info.Name) == 0 {
		return info.Object, false, fmt.Errorf("resource name must be provided")
	}

	gottenObj, initObjGetErr := o.DynamicClient.Resource(info.Mapping.Resource).Namespace(info.Namespace).Get(ctx, info.Name, metav1.GetOptions{})
	if apierrors.IsNotFound(initObjGetErr) {
		return info.Object, true, nil
	}
	if initObjGetErr != nil {
		// TODO 如有需要，这里可以做得更精巧一些
		return info.Object, false, initObjGetErr
	}
	resourceLocation := ResourceLocation{
		GroupResource: info.Mapping.Resource.GroupResource(),
		Namespace:     gottenObj.GetNamespace(),
		Name:          gottenObj.GetName(),
	}
	if uid, ok := o.UIDMap[resourceLocation]; ok {
		if gottenObj.GetUID() != uid {
			return gottenObj, true, nil
		}
	}

	endTime := time.Now().Add(o.Timeout)
	timeout := time.Until(endTime)
	errWaitTimeoutWithName := extendErrWaitTimeout(wait.ErrorInterrupted(nil), info) // nolint:staticcheck // SA1019
	if o.Timeout == 0 {
		// 如果 timeout 为零，则只检查一次对象是否存在
		if gottenObj == nil {
			return nil, true, nil
		}
		return gottenObj, false, fmt.Errorf("condition not met for %s", info.ObjectName())
	}
	if timeout < 0 {
		// 时间已用尽
		return info.Object, false, errWaitTimeoutWithName
	}

	fieldSelector := fields.OneTermEqualSelector("metadata.name", info.Name).String()
	lw := cache.ToListWatcherWithWatchListSemantics(&cache.ListWatch{
		ListFunc: func(options metav1.ListOptions) (runtime.Object, error) {
			options.FieldSelector = fieldSelector
			return o.DynamicClient.Resource(info.Mapping.Resource).Namespace(info.Namespace).List(ctx, options)
		},
		WatchFunc: func(options metav1.ListOptions) (watch.Interface, error) {
			options.FieldSelector = fieldSelector
			return o.DynamicClient.Resource(info.Mapping.Resource).Namespace(info.Namespace).Watch(ctx, options)
		},
	}, o.DynamicClient)

	// 该函数用于刷新缓存，避免对已消失的资源等待至超时
	preconditionFunc := func(store cache.Store) (bool, error) {
		obj, exists, err := store.Get(&metav1.ObjectMeta{Namespace: info.Namespace, Name: info.Name})
		if err != nil {
			return true, err
		}
		if !exists {
			// 由于我们正等待其消失，如果它已不存在就直接返回
			return true, nil
		}
		acc, err := meta.Accessor(obj)
		if err != nil {
			return true, err
		}
		if uid, ok := o.UIDMap[resourceLocation]; ok {
			if acc.GetUID() != uid {
				return true, nil
			}
		}
		return false, nil
	}

	intrCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	intr := interrupt.New(nil, cancel)
	err := intr.Run(func() error {
		_, err := watchtools.UntilWithSync(intrCtx, lw, &unstructured.Unstructured{}, preconditionFunc, Wait{errOut: o.ErrOut}.IsDeleted)
		if errors.Is(err, context.DeadlineExceeded) {
			return errWaitTimeoutWithName
		}
		return err
	})
	if err != nil {
		if wait.Interrupted(err) { // nolint:staticcheck // SA1019
			return gottenObj, false, errWaitTimeoutWithName
		}
		return gottenObj, false, err
	}

	return gottenObj, true, nil
}

// Wait 提供处理 watch 的辅助方法，包括错误处理。
type Wait struct {
	errOut io.Writer
}

// 如果对象已被删除，IsDeleted 返回 true。它会打印遇到的所有错误。
func (w Wait) IsDeleted(event watch.Event) (bool, error) {
	switch event.Type {
	case watch.Error:
		// 遇到错误时继续等待——如果错误不可恢复，
		// 服务端会关闭 watch。
		err := apierrors.FromObject(event.Object)
		fmt.Fprintf(w.errOut, "error: An error occurred while waiting for the object to be deleted: %v", err)
		return false, nil
	case watch.Deleted:
		return true, nil
	default:
		return false, nil
	}
}
