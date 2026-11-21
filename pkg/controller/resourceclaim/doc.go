/*
Copyright 2020 The Kubernetes Authors.

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

// Package resourceclaim 实现了
// https://github.com/kubernetes/enhancements/tree/master/keps/sig-node/3063-dynamic-resource-allocation
// 中描述的控制器的控制器部分。
//
// 该控制器脱胎于通用临时卷（generic ephemeral volume）控制器。
package resourceclaim
