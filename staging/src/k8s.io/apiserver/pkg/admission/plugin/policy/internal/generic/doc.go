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

// Package generic contains a typed wrapper over cache SharedIndexInformer
// and Lister (maybe eventually should have a home there?)
//
// This interface is being experimented with as an easier way to write controllers
// with a bit less boilerplate.
//
// Informer/Lister classes are thin wrappers providing a type-safe interface
// over regular interface{}-based Informers/Listers
//
// Controller[T] provides a reusable way to reconcile objects out of an informer
// using the tried and true controller design pattern found all over k8s
// codebase based upon syncFunc/reconcile
//
// 中文说明：本包用泛型为 cache.SharedIndexInformer / Lister 提供类型安全的薄封装，
// 避免每个控制器重复写类型断言与样板代码。Controller[T] 封装了 k8s 中常见的
// syncFunc/reconcile 控制器模式，便于在 admission policy 控制器之间复用。
package generic
