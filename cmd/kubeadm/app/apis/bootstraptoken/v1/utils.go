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

package v1

import (
	"fmt"
	"sort"
	"strings"
	"time"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	bootstrapapi "k8s.io/cluster-bootstrap/token/api"
	bootstraputil "k8s.io/cluster-bootstrap/token/util"
	bootstrapsecretutil "k8s.io/cluster-bootstrap/util/secrets"

	"k8s.io/kubernetes/cmd/kubeadm/app/util/errors"
)

const (
	// 当令牌与 'BootstrapTokenPattern' 匹配时，名称中包含 'Submatch' 的
	// 正则表达式函数返回的已验证子字符串数量为 3。
	// 子匹配 0 匹配整个表达式，子匹配 1 匹配第一个括号内的子表达式，依此类推。
	// 例如：
	// result := bootstraputil.BootstrapTokenRegexp.FindStringSubmatch("abcdef.1234567890123456")
	// result == []string{"abcdef.1234567890123456","abcdef","1234567890123456"}
	// len(result) == 3
	validatedSubstringsSize = 3
)

// MarshalJSON 实现 json.Marshaler 接口。
func (bts BootstrapTokenString) MarshalJSON() ([]byte, error) {
	return []byte(fmt.Sprintf(`"%s"`, bts.String())), nil
}

// UnmarshalJSON 实现 json.Unmarshaller 接口。
func (bts *BootstrapTokenString) UnmarshalJSON(b []byte) error {
	// 如果令牌表示为 ""，直接返回，不产生错误。
	if len(b) == 0 {
		return nil
	}

	// 移除 JSON 解析器带入的多余 " 字符。
	token := strings.Replace(string(b), `"`, ``, -1)
	// 将字符串令牌转换为 BootstrapTokenString 对象。
	newbts, err := NewBootstrapTokenString(token)
	if err != nil {
		return err
	}
	bts.ID = newbts.ID
	bts.Secret = newbts.Secret
	return nil
}

// String 返回 BootstrapTokenString 的字符串表示形式。
func (bts BootstrapTokenString) String() string {
	if len(bts.ID) > 0 && len(bts.Secret) > 0 {
		return bootstraputil.TokenFromIDAndSecret(bts.ID, bts.Secret)
	}
	return ""
}

// NewBootstrapTokenString 将给定的字符串形式引导令牌转换为用于
// 序列化、反序列化和内部处理的 BootstrapTokenString 对象，并自动验证令牌格式是否正确。
func NewBootstrapTokenString(token string) (*BootstrapTokenString, error) {
	substrs := bootstraputil.BootstrapTokenRegexp.FindStringSubmatch(token)
	if len(substrs) != validatedSubstringsSize {
		return nil, errors.Errorf("the bootstrap token %q was not of the form %q", token, bootstrapapi.BootstrapTokenPattern)
	}

	return &BootstrapTokenString{ID: substrs[1], Secret: substrs[2]}, nil
}

// NewBootstrapTokenStringFromIDAndSecret 是 NewBootstrapTokenString 的包装函数，
// 允许调用方分别指定 ID 和 Secret。
func NewBootstrapTokenStringFromIDAndSecret(id, secret string) (*BootstrapTokenString, error) {
	return NewBootstrapTokenString(bootstraputil.TokenFromIDAndSecret(id, secret))
}

// BootstrapTokenToSecret 将给定的 BootstrapToken 对象转换为可提交给 API Server 存储的 Secret 表示形式。
func BootstrapTokenToSecret(bt *BootstrapToken) *v1.Secret {
	return &v1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      bootstraputil.BootstrapTokenSecretName(bt.Token.ID),
			Namespace: metav1.NamespaceSystem,
		},
		Type: bootstrapapi.SecretTypeBootstrapToken,
		Data: encodeTokenSecretData(bt, time.Now()),
	}
}

// encodeTokenSecretData 接收令牌对象和当前时间，返回 Secret 的 .Data 字段。
// 传入 now 是为了便于单元测试。
func encodeTokenSecretData(token *BootstrapToken, now time.Time) map[string][]byte {
	data := map[string][]byte{
		bootstrapapi.BootstrapTokenIDKey:     []byte(token.Token.ID),
		bootstrapapi.BootstrapTokenSecretKey: []byte(token.Token.Secret),
	}

	if len(token.Description) > 0 {
		data[bootstrapapi.BootstrapTokenDescriptionKey] = []byte(token.Description)
	}

	// 如果出于某种原因同时设置了 token.TTL 和 token.Expires
	// （验证逻辑要求二者互斥，因此不应出现这种情况），
	// token.Expires 的优先级更高，如下面的逻辑所示。
	if token.Expires != nil {
		// 据此设置过期时间的格式。
		// TODO: 这或许应当作为 bootstraputil 中的辅助函数？
		expirationString := token.Expires.Time.UTC().Format(time.RFC3339)
		data[bootstrapapi.BootstrapTokenExpirationKey] = []byte(expirationString)

	} else if token.TTL != nil && token.TTL.Duration > 0 {
		// 仅在未设置 .Expires 时，TTL 才可能生效。
		// 将当前时间加上指定时长，并据此设置过期时间的格式。
		expirationString := now.Add(token.TTL.Duration).UTC().Format(time.RFC3339)
		data[bootstrapapi.BootstrapTokenExpirationKey] = []byte(expirationString)
	}

	for _, usage := range token.Usages {
		data[bootstrapapi.BootstrapTokenUsagePrefix+usage] = []byte("true")
	}

	if len(token.Groups) > 0 {
		data[bootstrapapi.BootstrapTokenExtraGroupsKey] = []byte(strings.Join(token.Groups, ","))
	}
	return data
}

// BootstrapTokenFromSecret 从给定的 Secret 返回 BootstrapToken 对象。
func BootstrapTokenFromSecret(secret *v1.Secret) (*BootstrapToken, error) {
	// 从 Secret 数据中获取令牌 ID 字段。
	tokenID := bootstrapsecretutil.GetData(secret, bootstrapapi.BootstrapTokenIDKey)
	if len(tokenID) == 0 {
		return nil, errors.Errorf("bootstrap Token Secret has no token-id data: %s", secret.Name)
	}

	// 确保命名符合约定。
	if secret.Name != bootstraputil.BootstrapTokenSecretName(tokenID) {
		return nil, errors.Errorf("bootstrap token name is not of the form '%s(token-id)'. Actual: %q. Expected: %q",
			bootstrapapi.BootstrapTokenSecretPrefix, secret.Name, bootstraputil.BootstrapTokenSecretName(tokenID))
	}

	tokenSecret := bootstrapsecretutil.GetData(secret, bootstrapapi.BootstrapTokenSecretKey)
	if len(tokenSecret) == 0 {
		return nil, errors.Errorf("bootstrap Token Secret has no token-secret data: %s", secret.Name)
	}

	// 根据 ID 和 Secret 创建 BootstrapTokenString 对象。
	bts, err := NewBootstrapTokenStringFromIDAndSecret(tokenID, tokenSecret)
	if err != nil {
		return nil, errors.Wrap(err, "bootstrap Token Secret is invalid and couldn't be parsed")
	}

	// 从 Secret 中获取描述（如果存在）。
	description := bootstrapsecretutil.GetData(secret, bootstrapapi.BootstrapTokenDescriptionKey)

	// 过期时间可选；未指定时表示令牌永不过期。
	secretExpiration := bootstrapsecretutil.GetData(secret, bootstrapapi.BootstrapTokenExpirationKey)
	var expires *metav1.Time
	if len(secretExpiration) > 0 {
		expTime, err := time.Parse(time.RFC3339, secretExpiration)
		if err != nil {
			return nil, errors.Wrapf(err, "can't parse expiration time of bootstrap token %q", secret.Name)
		}
		expires = &metav1.Time{Time: expTime}
	}

	// 从 Secret 数据构建用途字符串切片。
	var usages []string
	for k, v := range secret.Data {
		// 跳过所有不含此前缀的字段。
		if !strings.HasPrefix(k, bootstrapapi.BootstrapTokenUsagePrefix) {
			continue
		}
		// 跳过未将此用途设置为 true 的字段。
		if string(v) != "true" {
			continue
		}
		usages = append(usages, strings.TrimPrefix(k, bootstrapapi.BootstrapTokenUsagePrefix))
	}
	// 仅在切片非空时排序。
	if usages != nil {
		sort.Strings(usages)
	}

	// 从 Secret 获取额外组信息。
	// 采用这种处理方式，使没有条目时 .Groups 为 nil，而不是空切片或仅含 "" 字符串的切片。
	var groups []string
	groupsString := bootstrapsecretutil.GetData(secret, bootstrapapi.BootstrapTokenExtraGroupsKey)
	g := strings.Split(groupsString, ",")
	if len(g) > 0 && len(g[0]) > 0 {
		groups = g
	}

	return &BootstrapToken{
		Token:       bts,
		Description: description,
		Expires:     expires,
		Usages:      usages,
		Groups:      groups,
	}, nil
}
