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
	"strings"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
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

// ConditionalWait 持有检查 API 状态条件所需的信息
type ConditionalWait struct {
	conditionName   string
	conditionStatus string
	// 发生错误时会写入 errOut
	errOut io.Writer
}

// IsConditionMet 是用于等待某个 API 条件得到满足的条件函数
func (w ConditionalWait) IsConditionMet(ctx context.Context, info *resource.Info, o *WaitOptions) (runtime.Object, bool, error) {
	return getObjAndCheckCondition(ctx, info, o, w.isConditionMet, w.checkCondition)
}

func (w ConditionalWait) checkCondition(obj *unstructured.Unstructured) (bool, error) {
	conditions, found, err := unstructured.NestedSlice(obj.Object, "status", "conditions")
	if err != nil {
		return false, err
	}
	if !found {
		return false, nil
	}
	for _, conditionUncast := range conditions {
		condition, ok := conditionUncast.(map[string]interface{})
		if !ok {
			continue
		}
		name, found, err := unstructured.NestedString(condition, "type")
		if !found || err != nil || !strings.EqualFold(name, w.conditionName) {
			continue
		}
		status, found, err := unstructured.NestedString(condition, "status")
		if !found || err != nil {
			continue
		}
		generation, found, _ := unstructured.NestedInt64(obj.Object, "metadata", "generation")
		if found {
			observedGeneration, found := getObservedGeneration(obj, condition)
			if found && observedGeneration < generation {
				return false, nil
			}
		}
		return strings.EqualFold(status, w.conditionStatus), nil
	}

	return false, nil
}

func (w ConditionalWait) isConditionMet(event watch.Event) (bool, error) {
	if event.Type == watch.Error {
		// 遇到错误时继续等待——我们预期
		// 服务端会关闭 watch
		err := apierrors.FromObject(event.Object)
		fmt.Fprintf(w.errOut, "error: An error occurred while waiting for the condition to be satisfied: %v", err)
		return false, nil
	}
	if event.Type == watch.Deleted {
		// 这会层层返回，导致再次执行 get，并沿调用链返回 false
		return false, nil
	}
	obj := event.Object.(*unstructured.Unstructured)
	return w.checkCondition(obj)
}

type isCondMetFunc func(event watch.Event) (bool, error)
type checkCondFunc func(obj *unstructured.Unstructured) (bool, error)

// getObjAndCheckCondition 会向 API server 发起 List 查询以获取对象，并使用 check 函数检查条件是否满足。
// 如果条件未满足，它会向服务端发起 Watch 查询，并传入 condMet 函数
func getObjAndCheckCondition(ctx context.Context, info *resource.Info, o *WaitOptions, condMet isCondMetFunc, check checkCondFunc) (runtime.Object, bool, error) {
	if len(info.Name) == 0 {
		return info.Object, false, fmt.Errorf("resource name must be provided")
	}

	endTime := time.Now().Add(o.Timeout)
	timeout := time.Until(endTime)
	errWaitTimeoutWithName := extendErrWaitTimeout(wait.ErrorInterrupted(nil), info) // nolint:staticcheck // SA1019
	if o.Timeout == 0 {
		// 如果 timeout 为零，只获取一次对象并检查
		gottenObj, initObjGetErr := o.DynamicClient.Resource(info.Mapping.Resource).Namespace(info.Namespace).Get(ctx, info.Name, metav1.GetOptions{})
		if initObjGetErr != nil {
			return nil, false, initObjGetErr
		}
		if gottenObj == nil {
			return nil, false, fmt.Errorf("condition not met for %s", info.ObjectName())
		}
		conditionCheck, err := check(gottenObj)
		if err != nil {
			return gottenObj, false, err
		}
		if !conditionCheck {
			return gottenObj, false, fmt.Errorf("condition not met for %s", info.ObjectName())
		}
		return gottenObj, true, nil
	}
	if timeout < 0 {
		// 时间已用尽
		return info.Object, false, errWaitTimeoutWithName
	}

	mapping := info.ResourceMapping() // used to pass back meaningful errors if object disappears
	fieldSelector := fields.OneTermEqualSelector("metadata.name", info.Name).String()
	lw := cache.ToListWatcherWithWatchListSemantics(&cache.ListWatch{
		ListWithContextFunc: func(ctx context.Context, options metav1.ListOptions) (runtime.Object, error) {
			options.FieldSelector = fieldSelector
			return o.DynamicClient.Resource(info.Mapping.Resource).Namespace(info.Namespace).List(ctx, options)
		},
		WatchFuncWithContext: func(ctx context.Context, options metav1.ListOptions) (watch.Interface, error) {
			options.FieldSelector = fieldSelector
			return o.DynamicClient.Resource(info.Mapping.Resource).Namespace(info.Namespace).Watch(ctx, options)
		},
	}, o.DynamicClient)

	// 该函数用于刷新缓存，避免对已消失的资源等待至超时
	preconditionFunc := func(store cache.Store) (bool, error) {
		_, exists, err := store.Get(&metav1.ObjectMeta{Namespace: info.Namespace, Name: info.Name})
		if err != nil {
			return true, err
		}
		if !exists {
			return true, apierrors.NewNotFound(mapping.Resource.GroupResource(), info.Name)
		}

		return false, nil
	}

	intrCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	var result runtime.Object
	intr := interrupt.New(nil, cancel)
	err := intr.Run(func() error {
		ev, err := watchtools.UntilWithSync(intrCtx, lw, &unstructured.Unstructured{}, preconditionFunc, watchtools.ConditionFunc(condMet))
		if ev != nil {
			result = ev.Object
		}
		if errors.Is(err, context.DeadlineExceeded) {
			return errWaitTimeoutWithName
		}
		return err
	})
	if err != nil {
		if wait.Interrupted(err) { // nolint:staticcheck // SA1019
			return result, false, errWaitTimeoutWithName
		}
		return result, false, err
	}

	return result, true, nil
}

func extendErrWaitTimeout(err error, info *resource.Info) error {
	return fmt.Errorf("%w on %s/%s", err, info.Mapping.Resource.Resource, info.Name)
}

func getObservedGeneration(obj *unstructured.Unstructured, condition map[string]interface{}) (int64, bool) {
	conditionObservedGeneration, found, _ := unstructured.NestedInt64(condition, "observedGeneration")
	if found {
		return conditionObservedGeneration, true
	}
	statusObservedGeneration, found, _ := unstructured.NestedInt64(obj.Object, "status", "observedGeneration")
	return statusObservedGeneration, found
}
