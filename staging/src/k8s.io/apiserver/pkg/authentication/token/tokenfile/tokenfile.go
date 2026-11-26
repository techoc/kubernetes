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

// Package tokenfile 实现基于静态令牌文件的认证器，主要用于测试与本地开发。
// 令牌映射在构造时一次性载入内存，之后只读，因此可安全并发使用。
package tokenfile

import (
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"strings"

	"k8s.io/apiserver/pkg/authentication/authenticator"
	"k8s.io/apiserver/pkg/authentication/user"
	"k8s.io/klog/v2"
)

// TokenAuthenticator 以明文令牌为键查找预置用户信息。
// 注意令牌是明文存储的，仅适用于测试；生产环境应使用投射令牌或 OIDC 等方案。
type TokenAuthenticator struct {
	tokens map[string]*user.DefaultInfo
}

// New 返回一个针对单个 token 的 TokenAuthenticator
func New(tokens map[string]*user.DefaultInfo) *TokenAuthenticator {
	return &TokenAuthenticator{
		tokens: tokens,
	}
}

// NewCSV 返回一个从 CSV 文件填充的 TokenAuthenticator。
// CSV 文件记录格式必须为 "token,username,useruid"
// 可选的第 4 列是逗号分隔的用户组。格式错误会直接返回错误，
// 因为令牌文件损坏属于部署配置问题，静默跳过会导致意外的认证失败。
func NewCSV(path string) (*TokenAuthenticator, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	recordNum := 0
	tokens := make(map[string]*user.DefaultInfo)
	reader := csv.NewReader(file)
	// 允许每行字段数不同：组是可选的，且组名内部的逗号已由引号处理。
	reader.FieldsPerRecord = -1
	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		// 前三列（令牌、用户名、UID）是必需的，缺失时无法构造合法身份。
		if len(record) < 3 {
			return nil, fmt.Errorf("token file '%s' must have at least 3 columns (token, user name, user uid), found %d", path, len(record))
		}

		recordNum++
		// 空令牌若被载入，会与匿名请求的空令牌匹配，造成严重安全风险，因此跳过并告警。
		if record[0] == "" {
			klog.Warningf("empty token has been found in token file '%s', record number '%d'", path, recordNum)
			continue
		}

		obj := &user.DefaultInfo{
			Name: record[1],
			UID:  record[2],
		}
		// 重复令牌不报错，后者覆盖前者；告警提示文件存在冗余或冲突。
		if _, exist := tokens[record[0]]; exist {
			klog.Warningf("duplicate token has been found in token file '%s', record number '%d'", path, recordNum)
		}
		tokens[record[0]] = obj

		if len(record) >= 4 {
			obj.Groups = strings.Split(record[3], ",")
		}
	}

	return &TokenAuthenticator{
		tokens: tokens,
	}, nil
}

// AuthenticateToken 实现 authenticator.Token 接口。
// 未知令牌返回 (nil, false, nil) 表示“本认证器不认领”，让链上的其他认证器有机会处理；
// 这里不会产生 error，因为静态令牌表不存在临时性故障。
func (a *TokenAuthenticator) AuthenticateToken(ctx context.Context, value string) (*authenticator.Response, bool, error) {
	user, ok := a.tokens[value]
	if !ok {
		return nil, false, nil
	}
	return &authenticator.Response{User: user}, true, nil
}
