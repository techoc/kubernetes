/*
Copyright 2025 The Kubernetes Authors.

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

package translator

import (
	"strings"

	v1 "k8s.io/api/core/v1"
	"k8s.io/kubernetes/pkg/controller/volume/selinuxwarning/internal/parse"
	"k8s.io/kubernetes/pkg/volume/util"
)

// ControllerSELinuxTranslator 是可在 kube-controller-manager（KCM）中使用的
// SELinuxLabelTranslator 实现。
// 真正的 SELinuxLabelTranslator 能从操作系统默认值（/etc/selinux/*）补全
// SELinuxOptions 中的空字段。但 KCM 常以容器运行，无法访问宿主机的
// /etc/selinux；即便能访问，KCM 所在发行版也可能与实际工作节点不同。
// 因此这里根本不尝试补全默认值，只使用传入 SELinuxOptions 中已填写的字段。
type ControllerSELinuxTranslator struct{}

// 编译时断言 ControllerSELinuxTranslator 满足 SELinuxLabelTranslator 接口。
var _ util.SELinuxLabelTranslator = &ControllerSELinuxTranslator{}

// SELinuxEnabled 返回是否启用 SELinux。
// 本控制器只有在被显式启用时才会运行，因此假定所有节点都启用了 SELinux。
func (c *ControllerSELinuxTranslator) SELinuxEnabled() bool {
	return true
}

// SELinuxOptionsToFileLabel 将 SELinuxOptions 转换为文件标签字符串。
// kube-controller-manager 无法访问节点上 /etc/selinux 中的默认值，因此只拼接
// 已有字段，不尝试补全缺失部分。
func (c *ControllerSELinuxTranslator) SELinuxOptionsToFileLabel(opts *v1.SELinuxOptions) (string, error) {
	if opts == nil {
		return "", nil
	}
	parts := []string{
		opts.User,
		opts.Role,
		opts.Type,
		opts.Level,
	}
	label := strings.Join(parts, ":")
	if label == ":::" {
		// 全空的 SELinuxOptions 与 nil 视为等价。
		return "", nil
	}
	return label, nil
}

// Conflicts 判断两个 SELinux 标签是否冲突。
// 这两个标签必须由上面的 SELinuxOptionsToFileLabel 生成（本函数要求标签中
// 元素个数严格固定）。由于本转换器无法从操作系统补全缺失的标签组件，前三个
// 组件可以为空：此时空组件与真实 SELinux 标签比较不会造成冲突，函数返回
// false（因为检测不到冲突）。最后一个组件（level）始终参与比较，因为它不会
// 被操作系统补全。
// 例如 "system_u:system_r:container_t:s0:c1,c2" 与 ":::s0:c1,c2" 不冲突，
// 因为运行该 Pod 的节点可能把 ":::s0:c1,c2" 展开为前者；但前者与
// ":::s0:c98,c99" 冲突；而 ":::s0:c1,c2" 与 "" 或 ":::" 冲突，因为 level
// 永远不会被操作系统补全。
func (c *ControllerSELinuxTranslator) Conflicts(labelA, labelB string) bool {
	return c.ConflictsParsed(parse.ParseSELinuxLabel(labelA), parse.ParseSELinuxLabel(labelB))
}

// ConflictsParsed 判断两个预先解析好的 SELinux 标签是否冲突。
// 这是 Conflicts 的优化版本，直接操作已切分的标签，避免在热路径（如指标
// 采集）中反复分配字符串。partsA 和 partsB 必须是 4 元素数组，格式为
// [user, role, type, level]。
func (c *ControllerSELinuxTranslator) ConflictsParsed(partsA, partsB [4]string) bool {
	// 逐组件比较。
	for i := range 4 {
		if partsA[i] == partsB[i] {
			continue
		}
		if i == 3 {
			// 最后一个组件（level）必须始终匹配。
			return true
		}
		// i < 3：空组件无法比较，视为通配。
		if partsA[i] == "" {
			continue
		}
		if partsB[i] == "" {
			continue
		}
		// 组件不相等且都非空，判定为冲突。
		return true
	}

	return false
}
