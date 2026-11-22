/*
Copyright 2018 The Kubernetes Authors.

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

// Package scheme 提供 kubescheduler 配置 API 的运行时 Scheme 与编解码器。
//
// Scheme 把内部版本与外部版本注册的 API 类型集中起来，Codecs 则据此
// 完成配置的严格编解码，供调度器启动时解析 KubeSchedulerConfiguration 使用。
package scheme

import (
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	config "k8s.io/kubernetes/pkg/scheduler/apis/config"
	configv1 "k8s.io/kubernetes/pkg/scheduler/apis/config/v1"
)

var (
	// Scheme 是注册了所有 kubescheduler API 类型的运行时 Scheme。
	Scheme = runtime.NewScheme()

	// Codecs 提供该 Scheme 的编解码器。
	// 启用严格模式：配置里出现未知字段会直接报错，避免拼写错误被静默忽略。
	Codecs = serializer.NewCodecFactory(Scheme, serializer.EnableStrict)
)

func init() {
	AddToScheme(Scheme)
}

// AddToScheme 用所有已知的 kubescheduler API 版本构建 Scheme。
// 设置版本优先级后，v1 成为默认的外部版本。
func AddToScheme(scheme *runtime.Scheme) {
	utilruntime.Must(config.AddToScheme(scheme))
	utilruntime.Must(configv1.AddToScheme(scheme))
	utilruntime.Must(scheme.SetVersionPriority(
		configv1.SchemeGroupVersion,
	))
}
