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

package register

import (
	"k8s.io/apimachinery/pkg/util/runtime"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/controller-manager/pkg/features"
)

// init 中把 controller-manager 专有的 feature gate 注册到 apiserver 的默认 gate 集合；
// 注册失败说明存在重复定义等致命问题，用 Must 直接 panic。
func init() {
	runtime.Must(features.SetupCurrentKubernetesSpecificFeatureGates(utilfeature.DefaultMutableFeatureGate))
}
