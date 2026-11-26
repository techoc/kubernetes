/*
Copyright 2014 The Kubernetes Authors.

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

// Package names 提供对象名称生成策略：在用户给出的 generateName 前缀后附加
// 随机后缀，保证名称唯一且符合 Kubernetes 的 63 字符长度限制。
package names

import (
	"fmt"

	utilrand "k8s.io/apimachinery/pkg/util/rand"
)

// NameGenerator 为对象生成名字。某些后端可能有更多信息
// 可用于指导命名，此接口隐藏这些细节。
type NameGenerator interface {
	// GenerateName 基于 base 名字加随机后缀生成合法名字。
	//
	// 生成器需知道合法名字的最大长度。
	GenerateName(base string) string
}

// simpleNameGenerator 生成随机名字。
type simpleNameGenerator struct{}

// SimpleNameGenerator 返回名字加 5 位字母数字随机后缀。
//
// 结果保证不超过标准 Kubernetes 名字长度（63 字符）。
var SimpleNameGenerator NameGenerator = simpleNameGenerator{}

const (
	// TODO: 让非 core 资源也能使用自己的命名规则。
	// 63 是 DNS-1123 label 的最大长度；5 位随机后缀在冲突概率与可读性之间折中。
	maxNameLength          = 63
	randomLength           = 5
	MaxGeneratedNameLength = maxNameLength - randomLength
)

// GenerateName 在 base 后拼接 5 位随机字符。超长的 base 会被截断到
// MaxGeneratedNameLength，确保结果不超过 63 字符。
func (simpleNameGenerator) GenerateName(base string) string {
	if len(base) > MaxGeneratedNameLength {
		base = base[:MaxGeneratedNameLength]
	}
	return fmt.Sprintf("%s%s", base, utilrand.String(randomLength))
}
