/*
Copyright 2016 The Kubernetes Authors.

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

// Package imagepolicy 包含一个准入控制器，用于配置一个 webhook，策略决策委托给它处理。
package imagepolicy

import (
	"fmt"
	"time"

	"k8s.io/klog/v2"
)

const (
	defaultRetryBackoff = time.Duration(500) * time.Millisecond
	minRetryBackoff     = time.Duration(1)
	maxRetryBackoff     = time.Duration(5) * time.Minute
	defaultAllowTTL     = time.Duration(5) * time.Minute
	defaultDenyTTL      = time.Duration(30) * time.Second
	minAllowTTL         = time.Duration(1) * time.Second
	maxAllowTTL         = time.Duration(30) * time.Minute
	minDenyTTL          = time.Duration(1) * time.Second
	maxDenyTTL          = time.Duration(30) * time.Minute
	useDefault          = time.Duration(0)  // 使用默认 TTL 的哨兵值
	disableTTL          = time.Duration(-1) // 禁用 TTL 的哨兵值
)

// imagePolicyWebhookConfig 保存 imagePolicyWebhook 的配置数据。
type imagePolicyWebhookConfig struct {
	KubeConfigFile string        `json:"kubeConfigFile"`
	AllowTTL       time.Duration `json:"allowTTL"`
	DenyTTL        time.Duration `json:"denyTTL"`
	RetryBackoff   time.Duration `json:"retryBackoff"`
	DefaultAllow   bool          `json:"defaultAllow"`
}

// AdmissionConfig 保存各准入控制器的配置数据。
type AdmissionConfig struct {
	ImagePolicyWebhook imagePolicyWebhookConfig `json:"imagePolicy"`
}

// normalizeWebhookConfig 将 webhook 配置中的时长统一归一化：先校验范围，再处理哨兵值。
func normalizeWebhookConfig(config *imagePolicyWebhookConfig) (err error) {
	config.RetryBackoff, err = normalizeConfigDuration("backoff", time.Millisecond, config.RetryBackoff, minRetryBackoff, maxRetryBackoff, defaultRetryBackoff)
	if err != nil {
		return err
	}
	config.AllowTTL, err = normalizeConfigDuration("allow cache", time.Second, config.AllowTTL, minAllowTTL, maxAllowTTL, defaultAllowTTL)
	if err != nil {
		return err
	}
	config.DenyTTL, err = normalizeConfigDuration("deny cache", time.Second, config.DenyTTL, minDenyTTL, maxDenyTTL, defaultDenyTTL)
	return err
}

// normalizeConfigDuration 将反序列化得到的时长按 scale 换算，校验其落在 [min, max] 内，
// 并识别禁用/默认值哨兵。
func normalizeConfigDuration(name string, scale, value, min, max, defaultValue time.Duration) (time.Duration, error) {
	// -1 表示禁用该项配置。
	if value == disableTTL {
		klog.V(2).Infof("image policy webhook %s disabled", name)
		return time.Duration(0), nil
	}

	// 0 表示使用默认值。
	if value == useDefault {
		klog.V(2).Infof("image policy webhook %s using default value", name)
		return defaultValue, nil
	}

	// 反序列化得到的数值单位是纳秒，按 scale 换算到目标单位（毫秒或秒）。
	value *= scale

	// 换算后再校验，保证配置值在允许范围内。
	if value < min || value > max {
		return value, fmt.Errorf("valid value is between %v and %v, got %v", min, max, value)
	}
	return value, nil
}
