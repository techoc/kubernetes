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

package nodeidentifier

import (
	"strings"

	"k8s.io/apiserver/pkg/authentication/user"
)

// NewDefaultNodeIdentifier 返回默认的 NodeIdentifier 实现：
// 当用户所属组包含 system:nodes 且用户名符合 system:node:<nodeName> 格式时，
// 返回 isNode=true，并同时给出解析出的 nodeName。
func NewDefaultNodeIdentifier() NodeIdentifier {
	return defaultNodeIdentifier{}
}

// defaultNodeIdentifier 是 NodeIdentifier 的默认实现。
type defaultNodeIdentifier struct{}

// nodeUserNamePrefix 是 system:node:<nodeName> 形式用户名的固定前缀。
const nodeUserNamePrefix = "system:node:"

// NodeIdentity 判断用户是否为节点身份：
// 需要同时满足“用户名带 system:node: 前缀”和“所属组包含 system:nodes”，
// 两个条件都满足时从用户名中截出节点名返回。
func (defaultNodeIdentifier) NodeIdentity(u user.Info) (string, bool) {
	// 先确认用户非空且用户名带节点前缀，否则不可能是节点身份。
	if u == nil {
		return "", false
	}

	userName := u.GetName()
	if !strings.HasPrefix(userName, nodeUserNamePrefix) {
		return "", false
	}

	// 用户名前缀只是必要条件，还必须属于 system:nodes 组，
	// 避免普通用户通过构造用户名伪装成节点。
	isNode := false
	for _, g := range u.GetGroups() {
		if g == user.NodesGroup {
			isNode = true
			break
		}
	}
	if !isNode {
		return "", false
	}

	nodeName := strings.TrimPrefix(userName, nodeUserNamePrefix)
	return nodeName, true
}
