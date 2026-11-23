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

package v1beta1

import (
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/pod-security-admission/api"
)

// addDefaultingFuncs 把本版本的默认值函数接入 scheme。
// 仅用于 scheme 初始化，不直接处理配置对象；RegisterDefaults 由生成器提供。
func addDefaultingFuncs(scheme *runtime.Scheme) error {
	return RegisterDefaults(scheme)
}

// SetDefaults_PodSecurityDefaults 在字段留空时填入最宽松的默认值：
// 未显式配置策略时保持 privileged + latest，避免因版本差异意外拒绝用户已有的 Pod。
func SetDefaults_PodSecurityDefaults(obj *PodSecurityDefaults) {
	// 三个动作分别填默认级别：强制、告警、审计各自独立，
	// 用户可只配置其中一部分而无需被迫补齐其余字段。
	if len(obj.Enforce) == 0 {
		obj.Enforce = string(api.LevelPrivileged)
	}
	if len(obj.Warn) == 0 {
		obj.Warn = string(api.LevelPrivileged)
	}
	if len(obj.Audit) == 0 {
		obj.Audit = string(api.LevelPrivileged)
	}

	// 版本与级别成对补齐；留空时选 latest，让策略随集群升级自动跟进最新规则。
	if len(obj.EnforceVersion) == 0 {
		obj.EnforceVersion = string(api.VersionLatest)
	}
	if len(obj.WarnVersion) == 0 {
		obj.WarnVersion = string(api.VersionLatest)
	}
	if len(obj.AuditVersion) == 0 {
		obj.AuditVersion = string(api.VersionLatest)
	}
}
