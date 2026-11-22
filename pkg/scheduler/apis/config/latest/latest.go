/*
Copyright 2021 The Kubernetes Authors.

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

// Package latest 提供构造最新版调度器组件配置的入口。
package latest

import (
	"k8s.io/component-base/config/v1alpha1"
	v1 "k8s.io/kube-scheduler/config/v1"
	"k8s.io/kubernetes/pkg/scheduler/apis/config"
	"k8s.io/kubernetes/pkg/scheduler/apis/config/scheme"
)

// Default 创建最新版（带版本号的）类型的默认配置。
// 每当调度器组件配置版本升级时，都需要同步更新此函数。
func Default() (*config.KubeSchedulerConfiguration, error) {
	// 先填带版本号的类型：默认值只在该类型的 scheme 上注册。
	versionedCfg := v1.KubeSchedulerConfiguration{}
	versionedCfg.DebuggingConfiguration = *v1alpha1.NewRecommendedDebuggingConfiguration()

	scheme.Scheme.Default(&versionedCfg)
	cfg := config.KubeSchedulerConfiguration{}
	if err := scheme.Scheme.Convert(&versionedCfg, &cfg, nil); err != nil {
		return nil, err
	}
	// 不在 pkg/scheduler/apis/config/{version}/conversion.go 中设置该字段，
	// 因为转换过程中 API machinery 会将其清空。更多细节见
	// KubeSchedulerConfiguration 内部类型的定义。
	cfg.TypeMeta.APIVersion = v1.SchemeGroupVersion.String()
	return &cfg, nil
}
