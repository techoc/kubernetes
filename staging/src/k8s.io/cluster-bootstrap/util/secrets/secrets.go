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

// Package secrets 提供从 bootstrap token Secret 中读取、校验数据的小工具。
//
// 这里刻意不包含任何写操作：控制器负责 Secret 生命周期，认证插件只读。
// 所有时间相关的判断都接收显式 currentTime，便于测试并避免依赖调用时刻。
package secrets

import (
	"regexp"
	"strings"
	"time"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/cluster-bootstrap/token/api"
	legacyutil "k8s.io/cluster-bootstrap/token/util"
	"k8s.io/klog/v2"
)

var (
	// 匹配 `bootstrap-token-<6 位 ID>`，用 QuoteMeta 保护前缀中的元字符；
	// 锚定首尾，防止名称中夹带额外片段。
	secretNameRe = regexp.MustCompile(`^` + regexp.QuoteMeta(api.BootstrapTokenSecretPrefix) + `([a-z0-9]{6})$`)
)

// GetData 返回 Secret 中指定键的字符串值；Data 为 nil 或键不存在时返回空串。
func GetData(secret *v1.Secret, key string) string {
	// 统一返回空串，让调用方无需区分“缺少键”和“缺少 Data”。
	if secret.Data == nil {
		return ""
	}
	if val, ok := secret.Data[key]; ok {
		return string(val)
	}
	return ""
}

// HasExpired 判断 bootstrap token Secret 在 currentTime 是否已经过期。
func HasExpired(secret *v1.Secret, currentTime time.Time) bool {
	_, expired := GetExpiration(secret, currentTime)

	return expired
}

// GetExpiration 返回距离过期的剩余时间和是否已过期。
// 未设置过期时间时返回 (0, false)；时间格式非法时返回 (0, true)，使畸形值无法延长凭据寿命。
func GetExpiration(secret *v1.Secret, currentTime time.Time) (timeRemaining time.Duration, isExpired bool) {
	expiration := GetData(secret, api.BootstrapTokenExpirationKey)
	if len(expiration) == 0 {
		return 0, false
	}
	expTime, err := time.Parse(time.RFC3339, expiration)
	if err != nil {
		klog.V(3).Infof("Unparseable expiration time (%s) in %s/%s Secret: %v. Treating as expired.",
			expiration, secret.Namespace, secret.Name, err)
		return 0, true
	}

	// 只有严格大于 0 才算未过期；等于 0 的边界视为已过期。
	timeRemaining = expTime.Sub(currentTime)
	if timeRemaining <= 0 {
		klog.V(3).Infof("Expired bootstrap token in %s/%s Secret: %v",
			secret.Namespace, secret.Name, expiration)
		return 0, true
	}
	return timeRemaining, false
}

// ParseName 从 `bootstrap-token-<id>` 形式的 Secret 名称中提取公开 ID。
// 名称不合法时返回空 ID 和 false；捕获组确保结果不夹带前缀。
func ParseName(name string) (secretID string, ok bool) {
	r := secretNameRe.FindStringSubmatch(name)
	if r == nil {
		return "", false
	}
	return r[1], true
}

// GetGroups 读取并校验 bootstrap token Secret 的附加组，返回去重、排序后的组列表。
// 默认组 system:bootstrappers 始终存在；任一附加组非法都会整体报错，不静默丢弃。
func GetGroups(secret *v1.Secret) ([]string, error) {
	// 默认组无条件加入。
	groups := sets.NewString(api.BootstrapDefaultGroup)

	// 未配置附加组时仅返回默认组。
	extraGroupsString := GetData(secret, api.BootstrapTokenExtraGroupsKey)
	if extraGroupsString == "" {
		return groups.List(), nil
	}

	// 逐个拒绝非法组名，防止配置错误被解释为权限不足或越权。
	for _, group := range strings.Split(extraGroupsString, ",") {
		if err := legacyutil.ValidateBootstrapGroupName(group); err != nil {
			return nil, err
		}
		groups.Insert(group)
	}

	// sets.String.List 同时保证去重与稳定排序。
	return groups.List(), nil
}
