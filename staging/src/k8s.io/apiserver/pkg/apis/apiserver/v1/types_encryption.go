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

package v1

import (
	"fmt"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

/*
EncryptionConfiguration 保存加密 provider 的完整配置。
支持用通配符指定需要加密的资源：'*.<group>' 加密某组内全部资源，'*.*' 加密全部资源。
'*.' 可用于核心组，'*.*' 甚至覆盖 API Server 启动后新增的自定义资源。
同一资源列表内或不同条目之间的通配符不得重叠，否则部分配置不会生效。
资源列表按顺序处理，靠前的条目优先。

示例：

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
	// resources 是资源列表，每项带有对应的加密 provider。列表按顺序匹配，
	// 靠前的条目优先；这保证了“个别资源例外、其余走默认”的配置写法可表达。
	Resources []ResourceConfiguration `json:"resources"`
}

// ResourceConfiguration 保存按资源配置的条目。一个条目把一组资源绑定到有序的
// provider 链；链中第一个是写入使用的 provider，其余用于读取历史数据
// （轮换密钥时的关键约束）。
type ResourceConfiguration struct {
	// resources 是需要加密的 Kubernetes 资源列表。资源名来自 group/version/resource 中的 `resource` 或 `resource.group`。
	// 例如 pandas.awesome.bears.example 是自定义资源，'group': awesome.bears.example，'resource': pandas。
	// 使用 '*.*' 加密所有资源，'*.<group>' 加密特定组内的所有资源。
	// 例如 '*.awesome.bears.example' 加密 awesome.bears.example 组内的所有资源。
	// 例如 '*.' 加密核心组的所有资源（如 pods、configmaps 等）。
	Resources []string `json:"resources"`
	// providers 是用于读写磁盘上资源的转换器列表。
	// 例如 aesgcm、aescbc、secretbox、identity、kms。
	Providers []ProviderConfiguration `json:"providers"`
}

// ProviderConfiguration 保存所配置的加密 provider 的配置。每个字段对应一种
// provider，同一结构内通常只设置一个；由校验逻辑保证互斥，而不是靠类型系统。
type ProviderConfiguration struct {
	// aesgcm 是 AES-GCM 转换器的配置。
	AESGCM *AESConfiguration `json:"aesgcm,omitempty"`
	// aescbc 是 AES-CBC 转换器的配置。
	AESCBC *AESConfiguration `json:"aescbc,omitempty"`
	// secretbox 是基于 Secretbox 的转换器的配置。
	Secretbox *SecretboxConfiguration `json:"secretbox,omitempty"`
	// identity 是 identity 转换器的（空）配置。
	Identity *IdentityConfiguration `json:"identity,omitempty"`
	// kms 包含基于 KMS 的 envelope 转换器的名称、缓存大小和配置文件路径。
	KMS *KMSConfiguration `json:"kms,omitempty"`
}

// AESConfiguration 包含 AES transformer 的 API 配置。密钥长度校验发生在
// provider 构建阶段，配置 API 层只做结构描述。
type AESConfiguration struct {
	// keys 是用于创建 AES 转换器的密钥列表。
	// AES-CBC 的密钥必须为 32 字节；AES-GCM 为 16、24 或 32 字节。
	Keys []Key `json:"keys"`
}

// SecretboxConfiguration 包含 Secretbox 转换器的 API 配置。
type SecretboxConfiguration struct {
	// keys 是用于创建 Secretbox 转换器的密钥列表。
	// 每个密钥必须为 32 字节。
	Keys []Key `json:"keys"`
}

// Key 包含为 transformer 提供的密钥的名称与密文。name 用于标识轮换中的某一代
// 密钥，必须稳定且唯一；secret 是 base64 编码的原始密钥材料。
type Key struct {
	// name 是数据落盘时使用的密钥名称。
	Name string `json:"name"`
	// secret 是 base64 编码的实际密钥。
	Secret string `json:"secret"`
}

// String 以日志安全的方式实现 Stringer 接口。刻意隐藏 secret，
// 避免配置对象被日志或错误信息打印时泄露密钥。
func (k Key) String() string {
	return fmt.Sprintf("Name: %s, Secret: [REDACTED]", k.Name)
}

// IdentityConfiguration 是空结构体，用于在 provider 配置中允许 identity transformer。
// 它作为显式“不加密”的标记，与字段缺失（未配置）在语义上区分开。
type IdentityConfiguration struct{}

// KMSConfiguration 包含基于 KMS 的 envelope 转换器的名称、缓存大小和配置文件路径。
type KMSConfiguration struct {
	// apiVersion 是 KeyManagementService 的 apiVersion。
	// +optional
	//
	// 留空时按实现默认版本处理，便于配置在版本升级间复用。
	APIVersion string `json:"apiVersion"`
	// name 是要使用的 KMS 插件名称。
	Name string `json:"name"`
	// cachesize 是内存中最多缓存的 secret 数量，默认值为 1000。
	// 设置为负数可禁用缓存。此字段仅允许用于 KMS v1 provider。
	// +optional
	CacheSize *int32 `json:"cachesize,omitempty"`
	// endpoint 是 gRPC server 的监听地址，例如 "unix:///var/run/kms-provider.sock"。
	Endpoint string `json:"endpoint"`
	// 调用 kms-plugin 的 gRPC 超时时间（如 5s），默认为 3 秒。
	// +optional
	Timeout *metav1.Duration `json:"timeout,omitempty"`
}
