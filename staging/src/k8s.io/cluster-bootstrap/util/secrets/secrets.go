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

// GetData returns the string value for the given key in the specified Secret
// If there is an error or if the key doesn't exist, an empty string is returned.
func GetData(secret *v1.Secret, key string) string {
	// Data 为 nil 等价于所有键都不存在；返回空串让调用方无需区分
	// “缺少键”和“缺少 Data”。
	if secret.Data == nil {
		return ""
	}
	if val, ok := secret.Data[key]; ok {
		return string(val)
	}
	return ""
}

// HasExpired will identify whether the secret expires
func HasExpired(secret *v1.Secret, currentTime time.Time) bool {
	_, expired := GetExpiration(secret, currentTime)

	return expired
}

// GetExpiration checks if the secret expires
// isExpired indicates if the secret is already expired.
// timeRemaining indicates how long until it does expire.
// if the secret has no expiration timestamp, returns 0, false.
// if there is an error parsing the secret's expiration timestamp, returns 0, true.
//
// 时间解析失败按过期处理（fail-closed），避免畸形的时间戳意外延长 token 的
// 可用期。
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

// ParseName parses the name of the secret to extract the secret ID.
//
// 名称形如 `bootstrap-token-<id>`；正则的捕获组保证返回的是纯 ID，
// 名称不合法时 ok 为 false 而不是返回空串加 nil 错误。
func ParseName(name string) (secretID string, ok bool) {
	r := secretNameRe.FindStringSubmatch(name)
	if r == nil {
		return "", false
	}
	return r[1], true
}

// GetGroups loads and validates the bootstrapapi.BootstrapTokenExtraGroupsKey
// key from the bootstrap token secret, returning a list of group names or an
// error if any of the group names are invalid.
//
// 结果始终包含默认组 system:bootstrappers；附加组逐个校验，任一非法则
// 整体返回错误（不静默丢弃），以免配置错误被掩盖成权限缺失或越权。
func GetGroups(secret *v1.Secret) ([]string, error) {
	// always include the default group
	groups := sets.NewString(api.BootstrapDefaultGroup)

	// grab any extra groups and if there are none, return just the default
	extraGroupsString := GetData(secret, api.BootstrapTokenExtraGroupsKey)
	if extraGroupsString == "" {
		return groups.List(), nil
	}

	// validate the names of the extra groups
	for _, group := range strings.Split(extraGroupsString, ",") {
		if err := legacyutil.ValidateBootstrapGroupName(group); err != nil {
			return nil, err
		}
		groups.Insert(group)
	}

	// return the result as a deduplicated, sorted list
	return groups.List(), nil
}
