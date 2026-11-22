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

// Package scale provides a polymorphic scale client capable of fetching
// and updating Scale for any resource which implements the `scale` subresource,
// as long as that subresource operates on a version of scale convertable to
// autoscaling.Scale.
//
// 中文概述：本包面向“任意实现了 scale 子资源的资源”提供统一客户端。调用方只给出
// GroupResource，包内通过 RESTMapper 找到首选版本，并用 ScaleKindResolver 把结果
// 归一化为 autoscaling/v1.Scale，因此调用方无需感知 extensions/v1beta1、
// apps/v1beta1 等历史版本差异。
package scale
