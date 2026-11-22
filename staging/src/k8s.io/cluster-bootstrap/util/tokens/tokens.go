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

// Package tokens 负责把 "<id>.<secret>" 形式的字符串拆解为 token 的两部分。
//
// 这是面向调用方的轻量解析入口（较 token/util 更细分）；它只做格式匹配，
// 不访问集群，也不验证 token 是否真实存在或未过期。
package tokens

import (
	"fmt"
	"regexp"

	"k8s.io/cluster-bootstrap/token/api"
)

var (
	bootstrapTokenRe = regexp.MustCompile(api.BootstrapTokenPattern)
)

// ParseToken tries and parse a valid token from a string.
// A token ID and token secret are returned in case of success, an error otherwise.
//
// 使用锚定整串的正则捕获两组；len 检查同时兜住格式错误，避免越界访问。
func ParseToken(s string) (tokenID, tokenSecret string, err error) {
	split := bootstrapTokenRe.FindStringSubmatch(s)
	if len(split) != 3 {
		return "", "", fmt.Errorf("token [%q] was not of form [%q]", s, api.BootstrapTokenPattern)
	}
	return split[1], split[2], nil
}
