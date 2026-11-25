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

package slis

import (
	"k8s.io/component-base/metrics"
)

var (
	// Registry 暴露 SLI 注册表，使各组件可以添加自己的 SLI。
	//
	// 独立于全局 legacyregistry：SLI 只应出现在专用的 /metrics/slis 端点，
	// 避免与普通指标混在一起。
	Registry = metrics.NewKubeRegistry()
)
