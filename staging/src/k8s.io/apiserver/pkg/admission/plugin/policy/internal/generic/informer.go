/*
Copyright 2022 The Kubernetes Authors.

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

package generic

import (
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/cache"
)

// 编译期断言：informer 必须满足 Informer 接口。
var _ Informer[runtime.Object] = informer[runtime.Object]{}

// informer 把一个类型擦除的 SharedIndexInformer 包装成泛型 Informer。
// 内嵌 lister 复用同一个 indexer，保证读取与事件通知看到同一份缓存。
type informer[T runtime.Object] struct {
	cache.SharedIndexInformer
	lister[T]
}

// NewInformer 围绕类型擦除的 cache.SharedIndexInformer 创建泛型 informer。
// 调用方必须保证泛型参数 T 与 SharedIndexInformer 中实际存储的对象类型一致，
// 因为后续取值会做强制类型转换；不一致会在运行时 panic，且没有任何编译期保护。
func NewInformer[T runtime.Object](informe cache.SharedIndexInformer) Informer[T] {
	return informer[T]{
		SharedIndexInformer: informe,
		lister:              NewLister[T](informe.GetIndexer()),
	}
}
