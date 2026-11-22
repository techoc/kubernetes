/*
Copyright 2025 The Kubernetes Authors.

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

// +k8s:deepcopy-gen=package

// Package v1 contains the configuration API for metrics.
//
// The intention is to only have a single version of this API, potentially with
// new fields added over time in a backwards-compatible manner. Fields for
// alpha or beta features are allowed as long as they are defined so that not
// changing the defaults leaves those features disabled.
//
// The "v1" package name is just a reminder that API compatibility rules apply,
// not an indication of the stability of all features covered by it.
//
// NOTE: Component owners are advised to rely on `k8s.io/component-base/metrics` to operate upon
// `k8s.io/component-base/metrics/api/v1.MetricsConfiguration` as the former contains functions to apply and validate
// the configuration, which in turn rely on members of the same package, which cannot be moved,
// or imported (cyclic dependency) here.
//
// 中文补充：本包只定义配置数据结构，不包含校验与生效逻辑；
// 后者位于 k8s.io/component-base/metrics，以免形成循环依赖。

package v1
