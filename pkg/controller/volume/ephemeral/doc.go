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

// Package ephemeral 实现通用临时卷 KEP 中控制器的部分：
// https://github.com/kubernetes/enhancements/tree/master/keps/sig-storage/1698-generic-ephemeral-volumes
//
// 该控制器的实现由 expand 控制器衍生而来。
package ephemeral
