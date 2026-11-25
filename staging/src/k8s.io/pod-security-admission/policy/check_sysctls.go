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

package policy

import (
	"strings"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/pod-security-admission/api"
)

/*

sysctl 可能禁用安全机制，也可能影响节点上的全部容器，
因此除明确允许的“安全”子集外一律禁止。

只有已按容器或 Pod 命名空间隔离，且不会影响同一节点上其他 Pod 或进程的
sysctl 才视为安全。

**受限字段：**
spec.securityContext.sysctls[*].name

**允许值：**
'kernel.shm_rmid_forced'
'net.ipv4.ip_local_port_range'
'net.ipv4.tcp_syncookies'
'net.ipv4.ping_group_range'
'net.ipv4.ip_unprivileged_port_start'
'net.ipv4.ip_local_reserved_ports'
'net.ipv4.tcp_keepalive_time'
'net.ipv4.tcp_fin_timeout'
'net.ipv4.tcp_keepalive_intvl'
'net.ipv4.tcp_keepalive_probes'
'net.ipv4.tcp_rmem'
'net.ipv4.tcp_wmem'
'net.ipv4.tcp_slow_start_after_idle'
'net.ipv4.tcp_notsent_lowat'

*/

// 白名单按版本递增而非一次到位：sysctl 的安全性判定依赖当时内核与运行时的隔离能力，
// 旧策略必须保持原样，否则收紧级别的老集群会突然放行新 sysctl。
func init() {
	addCheck(CheckSysctls)
}

// CheckSysctls 返回基线级检查，从 1.0 起限制可设置的 sysctl。
func CheckSysctls() Check {
	return Check{
		ID:    "sysctls",
		Level: api.LevelBaseline,
		Versions: []VersionedCheck{
			{
				MinimumVersion: api.MajorMinorVersion(1, 0),
				CheckPod:       sysctlsV1Dot0,
			},
			{
				MinimumVersion: api.MajorMinorVersion(1, 27),
				CheckPod:       sysctlsV1Dot27,
			}, {
				MinimumVersion: api.MajorMinorVersion(1, 29),
				CheckPod:       sysctlsV1Dot29,
			},
			{
				MinimumVersion: api.MajorMinorVersion(1, 32),
				CheckPod:       sysctlsV1Dot32,
			},
			{
				MinimumVersion: api.MajorMinorVersion(1, 37),
				CheckPod:       sysctlsV1Dot37,
			},
		},
	}
}

// 每个版本在上一版基础上并集扩展，而不是另起一份完整名单；重复项会让版本差异难以审阅，
// 也容易在新增 sysctl 时漏更新旧版本。
var (
	sysctlsAllowedV1Dot0 = sets.NewString(
		"kernel.shm_rmid_forced",
		"net.ipv4.ip_local_port_range",
		"net.ipv4.tcp_syncookies",
		"net.ipv4.ping_group_range",
		"net.ipv4.ip_unprivileged_port_start",
	)
	sysctlsAllowedV1Dot27 = sysctlsAllowedV1Dot0.Union(sets.NewString(
		"net.ipv4.ip_local_reserved_ports",
	))
	sysctlsAllowedV1Dot29 = sysctlsAllowedV1Dot27.Union(sets.NewString(
		"net.ipv4.tcp_keepalive_time",
		"net.ipv4.tcp_fin_timeout",
		"net.ipv4.tcp_keepalive_intvl",
		"net.ipv4.tcp_keepalive_probes",
	))
	sysctlsAllowedV1Dot32 = sysctlsAllowedV1Dot29.Union(sets.NewString(
		"net.ipv4.tcp_rmem",
		"net.ipv4.tcp_wmem",
	))
	sysctlsAllowedV1Dot37 = sysctlsAllowedV1Dot32.Union(sets.NewString(
		"net.ipv4.tcp_slow_start_after_idle",
		"net.ipv4.tcp_notsent_lowat",
	))
)

// 各版本函数只是绑定不同的白名单；CheckPod 签名统一，策略框架才能在运行时按最低版本挑选实现。
func sysctlsV1Dot0(podMetadata *metav1.ObjectMeta, podSpec *corev1.PodSpec) CheckResult {
	return sysctls(podMetadata, podSpec, sysctlsAllowedV1Dot0)
}

func sysctlsV1Dot27(podMetadata *metav1.ObjectMeta, podSpec *corev1.PodSpec) CheckResult {
	return sysctls(podMetadata, podSpec, sysctlsAllowedV1Dot27)
}

func sysctlsV1Dot29(podMetadata *metav1.ObjectMeta, podSpec *corev1.PodSpec) CheckResult {
	return sysctls(podMetadata, podSpec, sysctlsAllowedV1Dot29)
}

func sysctlsV1Dot32(podMetadata *metav1.ObjectMeta, podSpec *corev1.PodSpec) CheckResult {
	return sysctls(podMetadata, podSpec, sysctlsAllowedV1Dot32)
}

func sysctlsV1Dot37(podMetadata *metav1.ObjectMeta, podSpec *corev1.PodSpec) CheckResult {
	return sysctls(podMetadata, podSpec, sysctlsAllowedV1Dot37)
}

func sysctls(podMetadata *metav1.ObjectMeta, podSpec *corev1.PodSpec, sysctls_allowed_set sets.String) CheckResult {
	// 收集全部违规项而非遇错即返回，拒绝信息里才能一次列出所有 sysctl，省去用户逐个重试。
	var forbiddenSysctls []string

	if podSpec.SecurityContext != nil {
		for _, sysctl := range podSpec.SecurityContext.Sysctls {
			if !sysctls_allowed_set.Has(sysctl.Name) {
				forbiddenSysctls = append(forbiddenSysctls, sysctl.Name)
			}
		}
	}

	if len(forbiddenSysctls) > 0 {
		return CheckResult{
			Allowed:         false,
			ForbiddenReason: "forbidden sysctls",
			ForbiddenDetail: strings.Join(forbiddenSysctls, ", "),
		}
	}
	return CheckResult{Allowed: true}
}
