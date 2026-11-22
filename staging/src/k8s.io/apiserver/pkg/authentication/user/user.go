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

// Info describes a user that has been authenticated to the system.
type Info interface {
	// GetName returns the name that uniquely identifies this user among all
	// other active users.
	GetName() string
	// GetUID returns a unique value for a particular user that will change
	// if the user is removed from the system and another user is added with
	// the same name.
	GetUID() string
	// GetGroups returns the names of the groups the user is a member of
	GetGroups() []string

	// GetExtra can contain any additional information that the authenticator
	// thought was interesting.  One example would be scopes on a token.
	// Keys in this map should be namespaced to the authenticator or
	// authenticator/authorizer pair making use of them.
	// For instance: "example.org/foo" instead of "foo"
	// This is a map[string][]string because it needs to be serializeable into
	// a SubjectAccessReviewSpec.authorization.k8s.io for proper authorization
	// delegation flows
	// In order to faithfully round-trip through an impersonation flow, these keys
	// MUST be lowercase.
	GetExtra() map[string][]string
}

// DefaultInfo provides a simple user information exchange object
// for components that implement the UserInfo interface.
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
	// well-known user and group names
	// 这些名字是授权策略的稳定契约，不得修改；修改会破坏所有基于内置 ClusterRole 的授权。
	SystemPrivilegedGroup = "system:masters"
	NodesGroup            = "system:nodes"
	MonitoringGroup       = "system:monitoring"
	AllUnauthenticated    = "system:unauthenticated"
	AllAuthenticated      = "system:authenticated"

	Anonymous     = "system:anonymous"
	APIServerUser = "system:apiserver"

	// core kubernetes process identities
	KubeProxy             = "system:kube-proxy"
	KubeControllerManager = "system:kube-controller-manager"
	KubeScheduler         = "system:kube-scheduler"

	// CredentialIDKey is the key used in a user's "extra" to specify the unique
	// identifier for this identity document).
	// 值由各认证器写入，用于审计与准入策略区分同一用户的不同凭据。
	CredentialIDKey = "authentication.kubernetes.io/credential-id"
)
