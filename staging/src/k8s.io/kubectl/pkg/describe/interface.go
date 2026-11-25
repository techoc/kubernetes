/*
Copyright 2018 The Kubernetes Authors.

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

package describe

import (
	"fmt"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/cli-runtime/pkg/genericclioptions"
)

const (
	// LoadBalancerWidth 是我们描述负载均衡器时使用的宽度
	LoadBalancerWidth = 16

	// LabelNodeRolePrefix 是节点角色的标签前缀
	// 此处为临时拷贝，待其合入 core 后移除：https://github.com/kubernetes/kubernetes/pull/39112
	LabelNodeRolePrefix = "node-role.kubernetes.io/"

	// NodeLabelRole 指定节点的角色
	NodeLabelRole = "kubernetes.io/role"
)

// DescriberFunc 提供了一种显示指定 RESTMapping 类型的方式
type DescriberFunc func(restClientGetter genericclioptions.RESTClientGetter, mapping *meta.RESTMapping) (ResourceDescriber, error)

// ResourceDescriber 为指定资源生成输出，若无法生成则返回错误。
// 实现者通常会封装从远程服务器获取指定对象
// 的逻辑。
type ResourceDescriber interface {
	Describe(namespace, name string, describerSettings DescriberSettings) (output string, err error)
}

// DescriberSettings 保存每个对象 describe 器的显示配置，
// 用于控制打印内容。
type DescriberSettings struct {
	ShowEvents bool
	ChunkSize  int64
}

// ObjectDescriber 是用于显示任意对象及其附加信息的接口。
// 当对象已在手（在磁盘上或已获取）时使用。
// 实现者可以忽略 extra 中传入的附加信息，
// 也可以默认使用它。如果找不到合适的 describe 器，
// ObjectDescriber 可以返回 ErrNoDescriber。
type ObjectDescriber interface {
	DescribeObject(object interface{}, extra ...interface{}) (output string, err error)
}

// ErrNoDescriber 是一个结构化错误，表示所提供的一个或多个对象
// 无法被 describe。
type ErrNoDescriber struct {
	Types []string
}

// Error 实现 error 接口。
func (e ErrNoDescriber) Error() string {
	return fmt.Sprintf("no describer has been defined for %v", e.Types)
}
