/*
Copyright 2017 The Kubernetes Authors.

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

package hash

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"

	"k8s.io/api/core/v1"
)

// 返回 ConfigMap 的哈希。
// 计算时考虑 Data、Kind 和 Name。
func ConfigMapHash(cm *v1.ConfigMap) (string, error) {
	encoded, err := encodeConfigMap(cm)
	if err != nil {
		return "", err
	}
	h, err := encodeHash(hash(encoded))
	if err != nil {
		return "", err
	}
	return h, nil
}

// 返回 Secret 的哈希。
// 计算时考虑 Data、Kind、Name 和 Type。
func SecretHash(sec *v1.Secret) (string, error) {
	encoded, err := encodeSecret(sec)
	if err != nil {
		return "", err
	}
	h, err := encodeHash(hash(encoded))
	if err != nil {
		return "", err
	}
	return h, nil
}

// 编码 ConfigMap。
// 计算时考虑 Data、Kind 和 Name。
func encodeConfigMap(cm *v1.ConfigMap) (string, error) {
	// json.Marshal 按键稳定排序，保证编码结果一致。
	m := map[string]interface{}{
		"kind": "ConfigMap",
		"name": cm.Name,
		"data": cm.Data,
	}
	if cm.Immutable != nil {
		m["immutable"] = *cm.Immutable
	}
	if len(cm.BinaryData) > 0 {
		m["binaryData"] = cm.BinaryData
	}
	data, err := json.Marshal(m)
	if err != nil {
		return "", err
	}
	return string(data), nil
}

// 编码 Secret。
// 计算时考虑 Data、Kind、Name 和 Type。
func encodeSecret(sec *v1.Secret) (string, error) {
	m := map[string]interface{}{
		"kind": "Secret",
		"type": sec.Type,
		"name": sec.Name,
		"data": sec.Data,
	}
	if sec.Immutable != nil {
		m["immutable"] = *sec.Immutable
	}
	// json.Marshal 按键稳定排序，保证编码结果一致。
	data, err := json.Marshal(m)
	if err != nil {
		return "", err
	}
	return string(data), nil
}

// 从十六进制哈希串中取前 40 位（每字符 4 位），
// 再把元音及类似元音的十六进制字符映射为辅音，
// 避免拼出不雅词汇（理论上没有元音就很难形成脏话）。
// 由于原串是十六进制，其中只可能出现元音 'a' 和 'e'。
// 随意选取 GenerateName 字符集中的一些辅音作为映射目标。
// See: https://github.com/kubernetes/apimachinery/blob/dc1f89aff9a7509782bde3b68824c8043a3e58cc/pkg/util/rand/rand.go#L75
// 十六进制串不足 10 个字符时返回错误。
func encodeHash(hex string) (string, error) {
	if len(hex) < 10 {
		return "", fmt.Errorf("the hex string must contain at least 10 characters")
	}
	enc := []rune(hex[:10])
	for i := range enc {
		switch enc[i] {
		case '0':
			enc[i] = 'g'
		case '1':
			enc[i] = 'h'
		case '3':
			enc[i] = 'k'
		case 'a':
			enc[i] = 'm'
		case 'e':
			enc[i] = 't'
		}
	}
	return string(enc), nil
}

// 用 sha256 对 `data` 哈希，返回十六进制串。
func hash(data string) string {
	return fmt.Sprintf("%x", sha256.Sum256([]byte(data)))
}
