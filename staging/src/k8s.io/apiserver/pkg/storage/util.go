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

package storage

import (
	"fmt"
	"strings"
	"sync/atomic"

	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/validate/content"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

// SimpleUpdateFunc 是只关心对象本身的更新函数，不涉及 response meta。
type SimpleUpdateFunc func(runtime.Object) (runtime.Object, error)

// SimpleUpdate 将 SimpleUpdateFunc 转换为完整的 UpdateFunc。
// 适配为完整的 UpdateFunc，第二个返回值（TTL）恒为 nil，
// 让存储层沿用对象自身的过期语义，而不是由调用方强制指定。
func SimpleUpdate(fn SimpleUpdateFunc) UpdateFunc {
	return func(input runtime.Object, _ ResponseMeta) (runtime.Object, *uint64, error) {
		out, err := fn(input)
		return out, nil, err
	}
}

// NamespaceKeyFunc 生成 etcd 键：<prefix><namespace>/<name>。
// prefix 必须以 "/" 结尾，否则拼接会得到歧义键（如 /registry/a 与 /registry/ab）。
// 名称经 IsPathSegmentName 校验，防止 "/" 或 "." 之类的路径片段破坏键空间层级，
// 这是一道安全边界：非法名称若进入键路径，可能越权访问其他资源。
func NamespaceKeyFunc(prefix string, obj runtime.Object) (string, error) {
	if !strings.HasSuffix(prefix, "/") {
		return "", fmt.Errorf("prefix should have '/' suffix")
	}
	meta, err := meta.Accessor(obj)
	if err != nil {
		return "", err
	}
	name := meta.GetName()
	if msgs := content.IsPathSegmentName(name); len(msgs) != 0 {
		return "", fmt.Errorf("invalid name: %v", msgs)
	}
	return prefix + meta.GetNamespace() + "/" + name, nil
}

// NoNamespaceKeyFunc 生成集群级（非命名空间）对象的 etcd 键：<prefix><name>。
// 与 NamespaceKeyFunc 同样校验名称的路径片段合法性，理由相同。
func NoNamespaceKeyFunc(prefix string, obj runtime.Object) (string, error) {
	if !strings.HasSuffix(prefix, "/") {
		return "", fmt.Errorf("prefix should have '/' suffix")
	}
	meta, err := meta.Accessor(obj)
	if err != nil {
		return "", err
	}
	name := meta.GetName()
	if msgs := content.IsPathSegmentName(name); len(msgs) != 0 {
		return "", fmt.Errorf("invalid name: %v", msgs)
	}
	return prefix + name, nil
}

// HighWaterMark 是线程安全的对象，用于跟踪某个量出现过的最大值。
// 用于统计「历史最大」类指标（如最大 goroutine 数、最大缓存对象数）。
// 实现为 int64 并依赖原子操作，因此可零值使用，无需显式初始化。
type HighWaterMark int64

// Update 当且仅当 'current' 是出现过的最高值时返回 true。
// 采用 CAS 循环而非「读-比较-写」：并发更新时可能有多个 goroutine
// 同时观察到旧值，CAS 失败者重试即可保证最终记录的是真正的最大值。
// 返回值语义是「本次更新是否刷新了记录」，调用方据此决定是否打日志，避免刷屏。
func (hwm *HighWaterMark) Update(current int64) bool {
	for {
		old := atomic.LoadInt64((*int64)(hwm))
		if current <= old {
			return false
		}
		if atomic.CompareAndSwapInt64((*int64)(hwm), old, current) {
			return true
		}
	}
}

// AnnotateInitialEventsEndBookmark 为给定对象添加特殊注解，标记初始事件已发送。
// 注意：本函数假定 obj 的 annotation 字段是引用类型（即 map）。
// 这是 WatchList 语义的标记：bookmark 对象带上该注解后，
// 客户端据此确认「初始事件已全部收到」，可安全切换到增量 watch。
// 注解 map 为 nil 时先分配，因为 SetAnnotations 只接受非 nil 引用；
// 这里依赖注解字段是 map（引用类型）的约定，否则修改不会反映到原对象。
func AnnotateInitialEventsEndBookmark(obj runtime.Object) error {
	objMeta, err := meta.Accessor(obj)
	if err != nil {
		return err
	}
	objAnnotations := objMeta.GetAnnotations()
	if objAnnotations == nil {
		objAnnotations = map[string]string{}
	}
	objAnnotations[metav1.InitialEventsAnnotationKey] = "true"
	objMeta.SetAnnotations(objAnnotations)
	return nil
}

// HasInitialEventsEndBookmarkAnnotation 检查标记初始事件已发送的特殊注解是否存在。
// 只认字面值 "true"，其他值（包括 "false"）一律视为未标记，
// 这样客户端不会被误触发切换逻辑。
func HasInitialEventsEndBookmarkAnnotation(obj runtime.Object) (bool, error) {
	objMeta, err := meta.Accessor(obj)
	if err != nil {
		return false, err
	}
	objAnnotations := objMeta.GetAnnotations()
	return objAnnotations[metav1.InitialEventsAnnotationKey] == "true", nil
}
