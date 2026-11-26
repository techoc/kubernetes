/*
Copyright 2016 The Kubernetes Authors.

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

package apiregistration

import metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// APIServiceList 是 APIService 对象的列表。
// 资源列表类型之所以存在，是因为 APIService 是集群级资源，列表接口需要统一的类型元数据和分页信息；
// 它的 Items 保存的是对象值而非指针，以保持与其他 k8s 列表类型一致的序列化形态。
type APIServiceList struct {
	metav1.TypeMeta
	metav1.ListMeta

	Items []APIService
}

// ServiceReference 是对 Service.legacy.k8s.io 的引用。
type ServiceReference struct {
	// Namespace 是该 Service 所在的命名空间。
	Namespace string
	// Name 是该 Service 的名字。
	Name string
	// 若指定，则为承载该服务的 Service 上的端口；为向后兼容默认为 443；
	// `port` 应为合法端口号（1-65535，含端点）。
	// +optional
	// 内部类型用 int32 零值表示“未指定”，对外版本则多用指针区分；
	// 默认值机制在进入内部类型前把缺省端口归一到 443。
	Port int32
}

// APIServiceSpec 包含定位并与 server 通信所需的信息。
// 仅支持 https，但可以关闭证书校验。
type APIServiceSpec struct {
	// Service 是对该 API server 对应 Service 的引用，必须在端口 443 上通信。
	// 若 Service 为 nil，表示该 API groupversion 由本 server 在本地处理，
	// 调用将直接委托给普通 handler 链完成。
	// +optional
	// 把 Service 留空作为“本地组”的标记，避免为内置 API 再维护一份自指的服务引用；
	// 代理层据此走委托链而非发起真正的网络转发。
	Service *ServiceReference
	// Group 是该 server 承载的 API 组名。
	Group string
	// Version 是该 server 承载的 API 版本，例如 "v1"。
	Version string

	// InsecureSkipTLSVerify 关闭与该 server 通信时的 TLS 证书校验。
	// 强烈不建议这样做，应改用 CABundle。
	InsecureSkipTLSVerify bool
	// CABundle 是 PEM 编码的 CA 包，用于校验 API server 的服务证书；未指定时使用 apiserver 上的系统信任根。
	// +listType=atomic
	// +optional
	// CA 包视为原子整体而非可合并列表，因为它表示一份完整的信任根集合，
	// 逐元素合并会产生难以推理的中间信任状态。
	CABundle []byte

	// GroupPriorityMinimum 是该组至少应具有的优先级。优先级越高，客户端越偏好该组。
	// 注意同一组的其他版本可能声明更高的 GroupPriorityMinimum，从而使整组获得更高优先级。
	// 主排序依据 GroupPriorityMinimum，数值从大到小（20 排在 10 前）。
	// 次排序依据对象名称的字母序比较（v1.bar 排在 v1.foo 前）。
	// 建议把 *.k8s.io（extensions 除外）设在 18000 左右，
	// PaaS（OpenShift、Deis）建议放在 2000 多的区间。
	// 组级优先级与版本级优先级分开，是为了让同一组内的所有版本共享一次“组排序”，
	// 再在组内按较小粒度的 VersionPriority 决定首选版本。
	GroupPriorityMinimum int32

	// VersionPriority 控制该 API 版本在其组内的排序，必须大于 0。
	// 主排序依据 VersionPriority，从大到小（20 排在 10 前）。
	// 由于只影响组内顺序，数值可以很小，大概在 10 量级即可。
	// 版本优先级相同时，用版本字符串计算组内顺序。
	// 若版本字符串是 “kube-like”，它排在非 “kube-like” 版本字符串之前，后者按字典序排列。
	// “Kube-like” 版本以 “v” 开头，后跟一个数字（主版本），
	// 再可选地跟字符串 “alpha” 或 “beta” 和另一个数字（次版本）。这些版本先按
	// GA > beta > alpha 排序（GA 指没有 beta/alpha 后缀的版本），再比较主版本、次版本。一个排序后的版本示例：
	// v10, v2, v1, v11beta2, v10beta3, v3beta1, v12alpha1, v11alpha2, foo1, foo10.
	// 对“kube 风格”版本号做专门解析，是因为纯字典序会把 v10 排在 v2 之前；
	// 识别 GA/beta/alpha 后，才能让稳定版本天然优先于预发布版本。
	VersionPriority int32
}

// ConditionStatus 表示 condition 的状态（true、false 或 unknown）。
type ConditionStatus string

// 以下是合法的 condition 状态。“ConditionTrue” 表示资源处于该状态；
// “ConditionFalse” 表示资源不处于该状态；“ConditionUnknown” 表示 kubernetes
// 无法判断资源是否处于该状态。将来还可以添加其他中间状态，例如 ConditionDegraded。
// 条件状态保留 Unknown 三态，是为了区分“明确不可用”和“控制器还没法判断”，
// 这样状态同步方不会因为一次探测失败就武断地下结论。
const (
	ConditionTrue    ConditionStatus = "True"
	ConditionFalse   ConditionStatus = "False"
	ConditionUnknown ConditionStatus = "Unknown"
)

// APIServiceConditionType 是 APIServiceCondition.Type 的合法取值。
type APIServiceConditionType string

const (
	// Available 表示 Service 存在且可达。
	Available APIServiceConditionType = "Available"
)

// APIServiceCondition 描述 APIService 的 condition。
type APIServiceCondition struct {
	// Type 是 condition 的类型。
	Type APIServiceConditionType
	// Status 是 condition 的状态，可取 True、False 或 Unknown。
	Status ConditionStatus
	// LastTransitionTime 是 condition 上次发生状态转换的时间。
	LastTransitionTime metav1.Time
	// Reason 是上次状态转换的原因，使用单个 CamelCase 单词。
	Reason string
	// Message 是描述最近一次状态转换细节的、人类可读的消息。
	// 把 Reason（机器可读）与 Message（人可读）拆开，是因为两者消费方不同：
	// 控制器按 Reason 做判断，人则看 Message 排查问题。
	Message string
}

// APIServiceStatus 包含从 API server 推导出的信息。
type APIServiceStatus struct {
	// APIService 当前的服务状态。
	// +listType=map
	// +listMapKey=type
	// 指令 +listType=map 与 +listMapKey=type 一起使用，使得不同控制器并发更新时
	// 按条件类型做合并，而不是相互覆盖整份条件列表。
	Conditions []APIServiceCondition
}

// +genclient
// +genclient:nonNamespaced
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// APIService 表示某个特定 GroupVersion 的服务器，Name 必须形如 “version.group”。
// 对象名固定为“版本.组”的形式，是为了让名字本身就能被解析出 Group/Version，
// 从而免去在spec之外再存一份重复标识。
// 生成指令说明：+genclient:nonNamespaced 说明它是集群级资源：聚合层面对的是整个集群的 API 组，
// 不存在按命名空间隔离的意义。
type APIService struct {
	metav1.TypeMeta
	metav1.ObjectMeta

	// Spec 包含定位并与服务端通信所需的信息。
	// Spec 描述期望的静态配置，由用户/安装器提供。
	Spec APIServiceSpec
	// Status 包含从 API server 派生出的信息。
	// Status 描述控制器探测出来的运行状态，与 Spec 分开存放，
	// 这样状态更新不会意外改动用户声明的配置。
	Status APIServiceStatus
}
