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

package config

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// SAControllerConfiguration 描述 ServiceAccountController（ServiceAccount 控制器）的配置参数。
type SAControllerConfiguration struct {
	// serviceAccountKeyFile 是 PEM 编码的 RSA 私钥文件名，
	// 用于签发 ServiceAccount token。
	ServiceAccountKeyFile string
	// concurrentSATokenSyncs 是允许并发执行的 ServiceAccount token 同步操作数。
	ConcurrentSATokenSyncs int32
	// rootCAFile 指向根 CA 证书文件，该 CA 会被写入 ServiceAccount 的 token secret；
	// 必须是有效的 PEM 编码 CA 证书包。
	RootCAFile string
}

// LegacySATokenCleanerConfiguration 描述旧版 ServiceAccount token 清理器的配置。
type LegacySATokenCleanerConfiguration struct {
	// CleanUpPeriod 是自动生成的 ServiceAccount token 自上次使用起，
	// 经过多长时间后可以删除。
	CleanUpPeriod metav1.Duration
}
