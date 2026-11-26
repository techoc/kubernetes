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

package user

// Info 描述已通过系统认证的用户。
type Info interface {
	// GetName 返回在所有活跃用户中唯一标识该用户的名字。
	// 中的其他活跃用户区分开来。
	GetName() string
	// GetUID 返回特定用户的唯一标识；用户被删除后若有同名新用户加入，该值会变化。
	// 若该用户被移除、系统中又新增了同名用户，该值会变化。
	//
	GetUID() string
	// GetGroups 返回用户所属组的名字。
	GetGroups() []string

	// GetExtra 携带认证器附加的额外信息，例如 token 的 scopes。
	// 认证器认为有用的信息，例如 token 的 scopes。
	// 键应按使用它的认证器或认证器/授权器组合加命名空间，
	// 以免不同组件之间的键冲突。
	// 例如使用 "example.org/foo" 而不是 "foo"。
	// 使用 map[string][]string 是为了能序列化进 SubjectAccessReviewSpec.authorization.k8s.io，
	// 从而支持授权委托流程。
	// delegation flows
	// 为了在 impersonation 流程中无损往返，这些键必须是小写。
	// 必须是小写。
	GetExtra() map[string][]string
}

// DefaultInfo 是一个简单的用户信息交换对象，供实现 UserInfo 接口的组件使用。
// 该类型不校验字段，使用方需自行保证 Name 非空且 Extra 的键为小写。
type DefaultInfo struct {
	Name   string
	UID    string
	Groups []string
	Extra  map[string][]string
}

// GetName 返回用户名。
func (i *DefaultInfo) GetName() string {
	return i.Name
}

// GetUID 返回用户唯一标识；为空表示认证器无法提供稳定 ID。
func (i *DefaultInfo) GetUID() string {
	return i.UID
}

// GetGroups 返回用户所属组列表。
func (i *DefaultInfo) GetGroups() []string {
	return i.Groups
}

// GetExtra 返回认证器附加的扩展信息（如 token 的 scopes）。
func (i *DefaultInfo) GetExtra() map[string][]string {
	return i.Extra
}

const (
	// 知名的用户与组名：
	// 这些名字是授权策略的稳定契约，不得修改；修改会破坏所有基于内置 ClusterRole 的授权。
	SystemPrivilegedGroup = "system:masters"
	NodesGroup            = "system:nodes"
	MonitoringGroup       = "system:monitoring"
	AllUnauthenticated    = "system:unauthenticated"
	AllAuthenticated      = "system:authenticated"

	Anonymous     = "system:anonymous"
	APIServerUser = "system:apiserver"

	// Kubernetes 核心进程身份。
	KubeProxy             = "system:kube-proxy"
	KubeControllerManager = "system:kube-controller-manager"
	KubeScheduler         = "system:kube-scheduler"

	// CredentialIDKey 是用户 "extra" 中用于携带该身份凭据唯一标识的键。
	// 值由各认证器写入，用于审计与准入策略区分同一用户的不同凭据。
	CredentialIDKey = "authentication.kubernetes.io/credential-id"
)
