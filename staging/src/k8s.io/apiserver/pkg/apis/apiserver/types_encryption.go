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

package apiserver

import (
	"fmt"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

/*
EncryptionConfiguration 保存加密提供方的完整配置。
可用通配符指定需要加密的资源。
用 '*.<group>' 加密某组内所有资源，'*.*' 加密所有资源；
'*.' 表示核心组内的所有资源。'*.*' 连 apiserver 启动后新增的
自定义资源也会加密。
同一资源列表内或不同条目间的通配符若相互重叠则不允许，
因为重叠部分的配置不会生效。
资源列表按顺序处理，靠前的列表优先。

为什么顺序重要：解密时必须能用写入时的同一个 provider 读回数据，因此
配置是“读优先”的有序列表——排在前面的 provider 先尝试解密，而写入始终
使用列表中第一个 provider。

Example:

	kind: EncryptionConfiguration
	apiVersion: apiserver.config.k8s.io/v1
	resources:
	- resources:
	  - events
	  providers:
	  - identity: {}  # do not encrypt events even though *.* is specified below
	- resources:
	  - secrets
	  - configmaps
	  - pandas.awesome.bears.example
	  providers:
	  - aescbc:
	      keys:
	      - name: key1
	        secret: c2VjcmV0IGlzIHNlY3VyZQ==
	- resources:
	  - '*.apps'
	  providers:
	  - aescbc:
	      keys:
	      - name: key2
	        secret: c2VjcmV0IGlzIHNlY3VyZSwgb3IgaXMgaXQ/Cg==
	- resources:
	  - '*.*'
	  providers:
	  - aescbc:
	      keys:
	      - name: key3
	        secret: c2VjcmV0IGlzIHNlY3VyZSwgSSB0aGluaw==
*/
type EncryptionConfiguration struct {
	metav1.TypeMeta
	// resources 是资源列表及其对应的加密提供方
	//
	// 每个条目独立匹配一组资源；列表顺序即解密尝试顺序。
	Resources []ResourceConfiguration
}

// ResourceConfiguration 保存单个资源的配置
//
// 资源名使用 "resource" 或 "resource.group" 形式（注意不是 GVK），
// 与 RBAC 中的资源表示保持一致。
type ResourceConfiguration struct {
	// resources 是需要加密的 Kubernetes 资源列表。资源名取自 group/version/resource
	// 中的 `resource` 或 `resource.group`，例如自定义资源
	// pandas.awesome.bears.example 的 group 为 awesome.bears.example、resource 为 pandas。
	// 用 '*.*' 加密所有资源，用 '*.<group>' 加密指定组内所有资源，
	// 例如 '*.awesome.bears.example' 加密 awesome.bears.example 组的所有资源，
	Resources []string
	// '*.' 则加密核心组（如 pods、configmaps 等）的所有资源。
	// providers 是用于读写磁盘资源的 transformer 列表，
	Providers []ProviderConfiguration
}

// ProviderConfiguration 保存加密提供方的配置
// 各字段互斥：同一 provider 条目中只能设置一个 transformer，校验在
// validation 包中完成。
type ProviderConfiguration struct {
	// aesgcm 是 AES-GCM transformer 的配置
	AESGCM *AESConfiguration
	// aescbc 是 AES-CBC transformer 的配置
	AESCBC *AESConfiguration
	// secretbox 是基于 Secretbox 的 transformer 的配置
	Secretbox *SecretboxConfiguration
	// identity 是 identity transformer 的空配置
	Identity *IdentityConfiguration
	// kms 包含基于 KMS 的 envelope transformer 的名称、缓存大小和配置文件路径
	KMS *KMSConfiguration
}

// AESConfiguration 保存 AES transformer 的 API 配置
type AESConfiguration struct {
	// keys 是用于创建 AES transformer 的密钥列表。
	Keys []Key
}

// SecretboxConfiguration 保存 Secretbox transformer 的 API 配置
type SecretboxConfiguration struct {
	// keys 是用于创建 Secretbox transformer 的密钥列表。
	Keys []Key
}

// Key 包含 transformer 所用密钥的名称与秘密值
type Key struct {
	// name 是数据写入磁盘时标识密钥的名称
	Name string
	// secret 是实际密钥，以 base64 编码
	Secret string
}

// String 以日志安全的方式实现 Stringer 接口
// 密钥材料绝不能进日志：这里只输出名称，Secret 固定替换为 [REDACTED]。
// 所有打印 Key 的地方都应依赖该方法，而不是 fmt 结构体。
func (k Key) String() string {
	return fmt.Sprintf("Name: %s, Secret: [REDACTED]", k.Name)
}

// IdentityConfiguration 是空结构体，用于在 provider 配置中启用 identity transformer
type IdentityConfiguration struct{}

// KMSConfiguration 包含基于 KMS 的 envelope transformer 的名称、缓存大小和配置文件路径
// KMS 负责保护 DEK，真正的数据加密由本地 AES 完成；因此 KMS 不可用时
// 已有数据仍可读取（只要 DEK 已缓存），但新写入会失败。
type KMSConfiguration struct {
	// KeyManagementService 的 apiVersion
	// +optional
	APIVersion string
	// name 是使用的 KMS 插件名称
	Name string
	// cachesize 是内存中缓存 secret 的最大数量，默认 1000。
	// 设为负值可禁用缓存。此字段只允许用于 KMS v1 provider。
	// +optional
	CacheSize *int32
	// endpoint 是 gRPC 服务器监听地址，例如 "unix:///var/run/kms-provider.sock"。
	Endpoint string
	// 调用 kms-plugin 的 gRPC 超时时间（如 5s），默认 3 秒。
	// +optional
	Timeout *metav1.Duration
}
