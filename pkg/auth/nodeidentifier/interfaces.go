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
	"k8s.io/apiserver/pkg/authentication/user"
)

// NodeIdentifier 从用户信息中判定节点身份。
type NodeIdentifier interface {
	// NodeIdentity 根据给定的 user.Info 判定节点信息。
	// nodeName 是与该 user.Info 关联的 Node API 对象名；若无法确定具体节点则为空。
	// isNode 表示该 user.Info 是否是签发给某个节点的身份。
	NodeIdentity(user.Info) (nodeName string, isNode bool)
}
