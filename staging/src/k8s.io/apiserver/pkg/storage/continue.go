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

// continueToken is a simple structured object for encoding the state of a continue token.
// TODO: if we change the version of the encoded from, we can't start encoding the new version
// until all other servers are upgraded (i.e. we need to support rolling schema)
// This is a public API struct and cannot change.
//
// 该结构会经 base64+JSON 暴露给客户端，字段名与语义属于 API 契约，
// 只能通过新增版本（APIVersion 字段）来演进。
type continueToken struct {
	APIVersion      string `json:"v"`
	ResourceVersion int64  `json:"rv"`
	StartKey        string `json:"start"`
}

// DecodeContinue transforms an encoded predicate from into a versioned struct.
// TODO: return a typed error that instructs clients that they must relist
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
		// defend against path traversal attacks by clients - path.Clean will ensure that startKey cannot
		// be at a higher level of the hierarchy, and so when we append the key prefix we will end up with
		// continue start key that is fully qualified and cannot range over anything less specific than
		// keyPrefix.
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

// EncodeContinue returns a string representing the encoded continuation of the current query.
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

// PrepareContinueToken prepares optional
// parameters for retrieving additional results for a paginated request.
//
// This function sets up parameters that a client can use to fetch the remaining results
// from the server if they are available.
//
// remainingItemCount 只在没有字段/标签选择器时给出：etcd 返回的计数包含不满足
// 谓词（predicate）的对象，直接暴露会误导客户端，因此宁可返回 nil。
func PrepareContinueToken(keyLastItem, keyPrefix string, resourceVersion int64, itemsCount int64, hasMoreItems bool, opts ListOptions) (string, *int64, error) {
	var remainingItemCount *int64
	var continueValue string
	var err error

	if hasMoreItems {
		// Instruct the client to begin querying from immediately after the last key.
		// 追加 NUL 字节是因为 etcd 的 range 从 key 开始 inclusive，NUL 保证排在
		// 所有以 key 为前缀的键之后，从而实现“从下一项开始”。
		continueValue, err = EncodeContinue(keyLastItem+"\x00", keyPrefix, resourceVersion)
		if err != nil {
			return "", remainingItemCount, err
		}
		// Etcd response counts in objects that do not match the pred.
		// Instead of returning inaccurate count for non-empty selectors, we return nil.
		// We only set remainingItemCount if the predicate is empty.
		if opts.Predicate.Empty() {
			remainingItems := itemsCount - opts.Predicate.Limit
			remainingItemCount = &remainingItems
		}
	}
	return continueValue, remainingItemCount, err
}
