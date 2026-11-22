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

package authorizer

// ResourceRuleInfo 描述一条针对资源型请求的授权规则。
// 使用接口而非具体结构，是为了让不同来源的规则（RBAC PolicyRule、
// 聚合授权器返回值等）都能被同一套逻辑消费而不引入循环依赖。
type ResourceRuleInfo interface {
	// GetVerbs returns a list of kubernetes resource API verbs.
	GetVerbs() []string
	// GetAPIGroups return the names of the APIGroup that contains the resources.
	GetAPIGroups() []string
	// GetResources return a list of resources the rule applies to.
	GetResources() []string
	// GetResourceNames return a white list of names that the rule applies to.
	GetResourceNames() []string
}

// DefaultResourceRuleInfo 是 ResourceRuleInfo 的默认实现，仅做字段读取。
type DefaultResourceRuleInfo struct {
	Verbs         []string
	APIGroups     []string
	Resources     []string
	ResourceNames []string
}

// GetVerbs 返回规则允许的资源型 API 动词。
func (i *DefaultResourceRuleInfo) GetVerbs() []string {
	return i.Verbs
}

// GetAPIGroups 返回规则覆盖的 API 组。
func (i *DefaultResourceRuleInfo) GetAPIGroups() []string {
	return i.APIGroups
}

// GetResources 返回规则覆盖的资源列表。
func (i *DefaultResourceRuleInfo) GetResources() []string {
	return i.Resources
}

// GetResourceNames 返回规则限制的资源名白名单；为空表示不限具体名称。
func (i *DefaultResourceRuleInfo) GetResourceNames() []string {
	return i.ResourceNames
}

// NonResourceRuleInfo 描述一条针对非资源 URL 路径的授权规则。
type NonResourceRuleInfo interface {
	// GetVerbs returns a list of kubernetes resource API verbs.
	GetVerbs() []string
	// GetNonResourceURLs return a set of partial urls that a user should have access to.
	GetNonResourceURLs() []string
}

// DefaultNonResourceRuleInfo 是 NonResourceRuleInfo 的默认实现。
type DefaultNonResourceRuleInfo struct {
	Verbs           []string
	NonResourceURLs []string
}

// GetVerbs 返回规则允许的 HTTP 动词（小写）。
func (i *DefaultNonResourceRuleInfo) GetVerbs() []string {
	return i.Verbs
}

// GetNonResourceURLs 返回允许访问的 URL 路径前缀集合。
func (i *DefaultNonResourceRuleInfo) GetNonResourceURLs() []string {
	return i.NonResourceURLs
}
