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

package wardle

import metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// FlunderList 是 Flunder 对象的列表。
// 列表类型供 list 接口与序列化使用，内部版本与外部版本各有一份。
type FlunderList struct {
	metav1.TypeMeta
	metav1.ListMeta

	Items []Flunder
}

// ReferenceType 表示对象引用的目标类型。
// 用类型名区分两种引用，避免只靠字段名判断语义。
type ReferenceType string

const (
	// FlunderReferenceType 表示引用指向 Flunder。
	// 指向同命名空间的 Flunder。
	FlunderReferenceType = ReferenceType("Flunder")
	// FischerReferenceType 表示引用指向 Fischer。
	// Fischer 是集群级资源，引用时不带命名空间。
	FischerReferenceType = ReferenceType("Fischer")
)

// FlunderSpec 定义 Flunder 的期望状态。
// 两个引用字段语义互斥，校验逻辑要保证只有一个被设置。
type FlunderSpec struct {
	// FlunderReference 是另一个 Flunder 的名字，与 FischerReference 互斥。
	// 这里只存名字，跨对象引用不做强约束。
	FlunderReference string
	// FischerReference 是一个 Fischer 的名字，与 FlunderReference 互斥。
	// 同上，名字本身不保证对象一定存在。
	FischerReference string
	// ReferenceType 显式记录引用目标的类型。
	// 消费方无须回退去猜两个引用字段哪个非空。
	ReferenceType ReferenceType
}

// FlunderStatus 保存 Flunder 的观测状态。
// 目前没有状态字段，先占位以便后续扩展不动对象结构。
type FlunderStatus struct {
}

// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// Flunder 是包含 Spec 和 Status 的示例资源。
// Spec 由用户提交，Status 留给控制器回写，职责不要混在一起。
type Flunder struct {
	metav1.TypeMeta
	metav1.ObjectMeta

	// Spec 保存用户声明的期望状态。
	Spec FlunderSpec
	// Status 预留给控制器回写观测状态。
	Status FlunderStatus
}

// +genclient
// +genclient:nonNamespaced
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// Fischer 是保存禁用 Flunder 名字名单的示例资源。
// 集群级资源：禁用名单要跨命名空间生效。
type Fischer struct {
	metav1.TypeMeta
	metav1.ObjectMeta

	// DisallowedFlunders 保存被禁止使用的 Flunder 名字。
	// 先写规则后建对象也允许，因此这里不校验名字是否真实存在。
	DisallowedFlunders []string
}

// +genclient:nonNamespaced
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// FischerList 是 Fischer 对象的列表。
// 与 Fischer 一样按集群级资源处理，列表不需要命名空间字段。
type FischerList struct {
	metav1.TypeMeta
	metav1.ListMeta

	// Items 保存 Fischer 对象列表。
	// 元素用值类型，和生成代码的深拷贝实现保持一致。
	Items []Fischer
}
