/*
Copyright 2022 The Kubernetes Authors.

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

package storage

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"path"
	"strings"
)

var (
	// 以下是 continue token 的各种无效输入错误；它们会作为 4xx 返回给客户端，
	// 提示客户端应重新发起一次 list 而不是继续翻页。
	ErrInvalidStartRV             = errors.New("continue key is not valid: incorrect encoded start resourceVersion (version meta.k8s.io/v1)")
	ErrEmptyStartKey              = errors.New("continue key is not valid: encoded start key empty (version meta.k8s.io/v1)")
	ErrGenericInvalidKey          = errors.New("continue key is not valid")
	ErrUnrecognizedEncodedVersion = errors.New("continue key is not valid: server does not recognize this encoded version")
)

// continueToken 是编码续传 token 状态的简单结构。
// TODO: 改变编码格式版本后，必须等所有其他 server 升级后才能使用新版本编码
//
// 这是公开 API 结构体，不能改动。
//
// 该结构会经 base64+JSON 暴露给客户端，字段名与语义属于 API 契约，
// 只能通过新增版本（APIVersion 字段）来演进。
type continueToken struct {
	APIVersion      string `json:"v"`
	ResourceVersion int64  `json:"rv"`
	StartKey        string `json:"start"`
}

// DecodeContinue 把编码后的 predicate 转为带版本的结构体。
// TODO: 返回类型化错误，告诉客户端必须重新 LIST
//
// keyPrefix 是本次 list 的存储前缀；返回的 fromKey 已重新拼上该前缀，
// 因此调用方可以直接从该 key 之后继续读取。
func DecodeContinue(continueValue, keyPrefix string) (fromKey string, rv int64, err error) {
	data, err := base64.RawURLEncoding.DecodeString(continueValue)
	if err != nil {
		return "", 0, fmt.Errorf("%w: %v", ErrGenericInvalidKey, err)
	}
	var c continueToken
	if err := json.Unmarshal(data, &c); err != nil {
		return "", 0, fmt.Errorf("%w: %v", ErrGenericInvalidKey, err)
	}
	switch c.APIVersion {
	case "meta.k8s.io/v1":
		if c.ResourceVersion == 0 {
			return "", 0, ErrInvalidStartRV
		}
		if len(c.StartKey) == 0 {
			return "", 0, ErrEmptyStartKey
		}
		// 防御客户端的路径穿越攻击：path.Clean 保证 startKey 不能指向
		// 更高层级，拼接键前缀后得到的续传键必然完全限定，
		// 不会越过 keyPrefix。
		//
		// 具体做法：先补上前导 "/" 再 Clean，若 Clean 改变了字符串（例如含 ".."）
		// 就拒绝；最后去掉前导 "/" 再拼接 keyPrefix，确保结果始终位于 keyPrefix 之下。
		key := c.StartKey
		if !strings.HasPrefix(key, "/") {
			key = "/" + key
		}
		cleaned := path.Clean(key)
		if cleaned != key {
			return "", 0, fmt.Errorf("%w: %v", ErrGenericInvalidKey, c.StartKey)
		}
		return keyPrefix + cleaned[1:], c.ResourceVersion, nil
	default:
		return "", 0, fmt.Errorf("%w %v", ErrUnrecognizedEncodedVersion, c.APIVersion)
	}
}

// EncodeContinue 返回当前查询的编码续传字符串。
//
// 写入的 StartKey 会去掉 keyPrefix，减小 token 体积；解码时再拼回，
// 因此 token 与具体的存储前缀（例如 /registry/pods）绑定。
func EncodeContinue(key, keyPrefix string, resourceVersion int64) (string, error) {
	nextKey := strings.TrimPrefix(key, keyPrefix)
	if nextKey == key {
		return "", fmt.Errorf("unable to encode next field: the key and key prefix do not match")
	}
	out, err := json.Marshal(&continueToken{APIVersion: "meta.k8s.io/v1", ResourceVersion: resourceVersion, StartKey: nextKey})
	if err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(out), nil
}

// PrepareContinueToken 为分页请求准备可选参数，
//
// 客户端可用这些参数在服务端仍有数据时获取剩余结果。
//
// remainingItemCount 只在没有字段/标签选择器时给出：etcd 返回的计数包含不满足
// 谓词（predicate）的对象，直接暴露会误导客户端，因此宁可返回 nil。
func PrepareContinueToken(keyLastItem, keyPrefix string, resourceVersion int64, itemsCount int64, hasMoreItems bool, opts ListOptions) (string, *int64, error) {
	var remainingItemCount *int64
	var continueValue string
	var err error

	if hasMoreItems {
		// 指示客户端从最后一个键之后开始查询。
		// 追加 NUL 字节是因为 etcd 的 range 从 key 开始 inclusive，NUL 保证排在
		// 所有以 key 为前缀的键之后，从而实现“从下一项开始”。
		continueValue, err = EncodeContinue(keyLastItem+"\x00", keyPrefix, resourceVersion)
		if err != nil {
			return "", remainingItemCount, err
		}
		// etcd 的响应计数包含不匹配 predicate 的对象。
		// 选择器非空时计数不准，因此返回 nil。
		// 仅当 predicate 为空时才设置 remainingItemCount。
		if opts.Predicate.Empty() {
			remainingItems := itemsCount - opts.Predicate.Limit
			remainingItemCount = &remainingItems
		}
	}
	return continueValue, remainingItemCount, err
}
