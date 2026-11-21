/*
Copyright 2019 The Kubernetes Authors.

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

package v1alpha1

import (
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kubectrlmgrconfigv1alpha1 "k8s.io/kube-controller-manager/config/v1alpha1"
)

// RecommendedDefaultSAControllerConfiguration 为 SAControllerConfiguration
// 设置推荐默认值。这些默认值可能会随 API 版本变化。此函数刻意不注册为
// scheme 中普通的 `SetDefaults_Foo` 函数，以便该类型的使用者可以为自己
// 嵌套的配置设置任意默认值。若强制使用这里的默认值，会带来问题：scheme
// 中的 defaulting 是转换过程的一部分，使用者将无法便捷地关闭它。
// 因此，若想使用此默认逻辑，请在本类型的包装结构体的 `SetDefaults_` 方法中调用它。
func RecommendedDefaultSAControllerConfiguration(obj *kubectrlmgrconfigv1alpha1.SAControllerConfiguration) {
	// 零值表示未显式配置，此时用并发同步数 5 作为推荐默认值。
	if obj.ConcurrentSATokenSyncs == 0 {
		obj.ConcurrentSATokenSyncs = 5
	}
}

// RecommendedDefaultLegacySATokenCleanerConfiguration 为
// LegacySATokenCleanerConfiguration 设置推荐默认值。
func RecommendedDefaultLegacySATokenCleanerConfiguration(obj *kubectrlmgrconfigv1alpha1.LegacySATokenCleanerConfiguration) {
	zero := metav1.Duration{}
	// 零值说明未配置清理周期；默认一年，给旧令牌留出足够长的生命周期，
	// 避免过早清理仍可能被使用的令牌。
	if obj.CleanUpPeriod == zero {
		obj.CleanUpPeriod = metav1.Duration{Duration: 365 * 24 * time.Hour}
	}
}
