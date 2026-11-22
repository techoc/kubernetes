/*
Copyright 2021 The Kubernetes Authors.

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

package fieldmanager

import (
	"context"
	"fmt"
	"os"
	"reflect"
	"strconv"
	"sync"
	"time"

	"k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/conversion"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apiserver/pkg/endpoints/metrics"
	"k8s.io/klog/v2"
)

var (
	// avoidTimestampEqualities 是忽略 managedFields 时间戳的等价比较器；
	// 只在首次调用时构建，之后只读，因此无需加锁。
	avoidTimestampEqualities     conversion.Equalities
	initAvoidTimestampEqualities sync.Once
)

// getAvoidTimestampEqualities 惰性构建比较器。可通过环境变量
// KUBE_APISERVER_AVOID_NOOP_SSA_TIMESTAMP_UPDATES 关闭该优化（设为 false），
// 以便出现问题时快速回退，无需重新发版。
func getAvoidTimestampEqualities() conversion.Equalities {
	initAvoidTimestampEqualities.Do(func() {
		if avoidNoopTimestampUpdatesString, exists := os.LookupEnv("KUBE_APISERVER_AVOID_NOOP_SSA_TIMESTAMP_UPDATES"); exists {
			if ret, err := strconv.ParseBool(avoidNoopTimestampUpdatesString); err == nil && !ret {
				// 保持 avoidTimestampEqualities 为空，调用方据此跳过优化。
				return
			} else {
				klog.Errorf("failed to parse envar KUBE_APISERVER_AVOID_NOOP_SSA_TIMESTAMP_UPDATES: %v", err)
			}
		}

		var eqs = equality.Semantic.Copy()
		err := eqs.AddFuncs(
			func(a, b metav1.ManagedFieldsEntry) bool {
				// 忽略 timestamp 后深度相等，则两条 managed fields 视为等价。
				a.Time = nil
				b.Time = nil
				return reflect.DeepEqual(a, b)
			},
			func(a, b unstructured.Unstructured) bool {
				// 先把 managed fields 转成结构化类型并复用上面的函数比较，
				// 然后在忽略 managedFields 路径的前提下比较其余 unstructured 数据。
				if !avoidTimestampEqualities.DeepEqual(a.GetManagedFields(), b.GetManagedFields()) {
					return false
				}
				return equalIgnoringValueAtPath(a.Object, b.Object, []string{"metadata", "managedFields"})
			},
		)

		if err != nil {
			panic(fmt.Errorf("failed to instantiate semantic equalities: %w", err))
		}

		avoidTimestampEqualities = eqs
	})
	return avoidTimestampEqualities
}

// equalIgnoringValueAtPath 比较两个值，但跳过 path 指向的子树。
// 任何一方不是 map 时视为可忽略（无法继续下探），这是为了让比较只关注
// 确实可比较的结构化部分，而不是把类型差异当成对象变化。
func equalIgnoringValueAtPath(a, b any, path []string) bool {
	if len(path) == 0 { // 已到达需要忽略的值
		return true
	}
	aMap, aOk := a.(map[string]any)
	bMap, bOk := b.(map[string]any)
	if !aOk || !bOk {
		// 无法下探到非 map，忽略
		return true
	}
	if len(aMap) != len(bMap) {
		return false
	}
	pathHead := path[0]
	for k, aVal := range aMap {
		bVal, ok := bMap[k]
		if !ok {
			return false
		}
		if k == pathHead {
			if !equalIgnoringValueAtPath(aVal, bVal, path[1:]) {
				return false
			}
		} else if !avoidTimestampEqualities.DeepEqual(aVal, bVal) {
			return false
		}
	}
	return true
}

// IgnoreManagedFieldsTimestampsTransformer 在对象的非 managed 部分等价时，
// 撤销 managedFields 中的时间戳更新，避免无意义的 ResourceVersion 递增。
func IgnoreManagedFieldsTimestampsTransformer(
	_ context.Context,
	newObj runtime.Object,
	oldObj runtime.Object,
) (res runtime.Object, err error) {
	equalities := getAvoidTimestampEqualities()
	// 优化被环境变量关闭时直接返回。
	if len(equalities.Equalities) == 0 {
		return newObj, nil
	}

	outcome := "unequal_objects_fast"
	start := time.Now()
	err = nil
	res = nil

	defer func() {
		if err != nil {
			outcome = "error"
		}

		metrics.RecordTimestampComparisonLatency(outcome, time.Since(start))
	}()

	// 若 managedFields 去掉时间戳后未变，且对象其余部分也未变，
	// 则回滚 managedFields 中的时间戳变更（防止 ResourceVersion 无谓递增）。
	//
	// 流程：
	// 先快速检查 managedFields（忽略时间戳）是否未变；若未变再做完整的慢速检查。
	//
	// 多数真正修改对象的请求会在“managedFields 忽略时间戳”检查处失败并提前返回；
	// 只有 managedFields 恰好只在时间戳上不同、而对象本身有差异时，才进入慢速路径。
	oldAccessor, err := meta.Accessor(oldObj)
	if err != nil {
		return nil, fmt.Errorf("failed to acquire accessor for oldObj: %v", err)
	}

	accessor, err := meta.Accessor(newObj)
	if err != nil {
		return nil, fmt.Errorf("failed to acquire accessor for newObj: %v", err)
	}

	oldManagedFields := oldAccessor.GetManagedFields()
	newManagedFields := accessor.GetManagedFields()

	if len(oldManagedFields) != len(newManagedFields) {
		// 有任何 managed fields 条目增删就提前返回。
		// 需要保留用户预期：即使写入的字段值没变，也应体现为该字段的 manager。
		return newObj, nil
	} else if len(newManagedFields) == 0 {
		// managedFields 为空时该转换没有意义
		return newObj, nil
	}

	// 只有至少一个时间戳变化、其余部分相等时该转换才有意义。
	// 没有任何时间戳变化时提前返回。
	allTimesUnchanged := true
	for i, e := range newManagedFields {
		if !e.Time.Equal(oldManagedFields[i].Time) {
			allTimesUnchanged = false
			break
		}
	}

	if allTimesUnchanged {
		return newObj, nil
	}

	eqFn := equalities.DeepEqual
	if _, ok := newObj.(*unstructured.Unstructured); ok {
		// unstructured 使用严格等价：nil 与空值必须区分，否则会掩盖真实的字段差异
		eqFn = equalities.DeepEqualWithNilDifferentFromEmpty
	}

	// 先比较 managed fields：若不等，if 短路，不会进入整对象比较的慢路径；
	// 若相等，再对整个对象做慢速比较。
	if !eqFn(oldManagedFields, newManagedFields) {
		return newObj, nil
	}

	if eqFn(newObj, oldObj) {
		// 去掉变化的时间戳，使 etcd 看不到任何变化。
		//
		// 已知 newManagedFields 除时间戳外与 oldManagedFields 逐项相等，
		// 因此直接用旧值覆盖可能已变化的新时间戳。
		for idx := 0; idx < len(oldManagedFields); idx++ {
			newManagedFields[idx].Time = oldManagedFields[idx].Time
		}

		accessor.SetManagedFields(newManagedFields)
		outcome = "equal_objects"
		return newObj, nil
	}

	outcome = "unequal_objects_slow"
	return newObj, nil
}
