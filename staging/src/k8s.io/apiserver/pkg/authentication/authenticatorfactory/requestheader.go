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

package authenticatorfactory

import (
	"k8s.io/apiserver/pkg/authentication/request/headerrequest"
	"k8s.io/apiserver/pkg/server/dynamiccertificates"
)

// RequestHeaderConfig 描述「前置代理（front proxy）」认证方式的配置。
// 该模式下 API Server 不直接校验用户凭证，而是信任一个已认证的代理，
// 由代理通过 HTTP 头传递用户名、UID、用户组与额外属性。安全边界完全依赖 mTLS：
// 只有当连接对端证书由 CAContentProvider 中的 CA 签发、且（若配置）客户端 CN 在
// AllowedClientNames 内时，这些头部才被视为可信。因此 CAContentProvider 必须随
// 证书轮换动态更新（它被设计为可返回不同内容），否则轮换后代理会被拒绝。
// 所有头部名比较均大小写不敏感，与 HTTP 头语义一致。
type RequestHeaderConfig struct {
	// UsernameHeaders 按顺序（大小写不敏感）查找用于身份的头部，首个有值的头部胜出。
	// 顺序决定代理迁移期的兼容性。
	UsernameHeaders headerrequest.StringSliceProvider
	// UIDHeaders 按顺序（大小写不敏感）查找用于身份 UID 的头部，首个有值的头部胜出。
	// 与用户名同理，仅取首个非空值；UID 用于审计与 RBAC 主体区分。
	UIDHeaders headerrequest.StringSliceProvider
	// GroupHeaders 查找组名（大小写不敏感）的头部，所有值都会被使用。
	// 与其他字段不同，这里会收集所有匹配头部的所有值——
	// 代理常把大量组拆成多行发送，聚合后才是完整组集合。
	GroupHeaders headerrequest.StringSliceProvider
	// ExtraHeaderPrefixes 按前缀（大小写不敏感）匹配头部，用于填充 user.Info.Extra，
	// 所有匹配头部的所有值都会被加入。
	// 前缀匹配允许一个代理携带任意数量的扩展属性（如 scope-*），
	// 键名由去掉前缀后的部分构成；同样聚合所有值。
	ExtraHeaderPrefixes headerrequest.StringSliceProvider
	// CAContentProvider 是用 mTLS 校验传入连接的选项。通常指向用于校验前置代理身份的
	// CA bundle 文件，也可以按需产出不同的选项。
	// 这是信任根。之所以用 Provider 而非静态 bundle，是为了支持 CA 轮换时无需重启进程。
	CAContentProvider dynamiccertificates.CAContentProvider
	// AllowedClientNames 是执行认证的前置代理可以出示的 common name 列表，为空表示接受任意值。
	// 空列表表示「任何由受信 CA 签发的证书都接受」，通常过宽；
	// 生产环境应显式限定代理的 CN，防止同一 CA 下的其他客户端冒充代理注入头部。
	AllowedClientNames headerrequest.StringSliceProvider
}
