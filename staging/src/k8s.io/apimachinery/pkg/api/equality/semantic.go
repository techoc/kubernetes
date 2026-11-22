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

// Package equality 提供语义化的深比较：不按 Go 内存表示，而按“用户可见的语义”
// 比较对象，使经过编解码往返、内部表示不同但语义相同的对象被判为相等。
package equality

import (
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/conversion"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/labels"
)

// Semantic 对 API 对象执行语义化的深相等比较。
// 例如：apiequality.Semantic.DeepEqual(aPod, aPodWithNonNilButEmptyMaps) == true
//
// 每种类型都显式定制比较策略，覆盖 reflect.DeepEqual 无法正确判定的情况：
// 数字的书写形式、时间的时区、选择器的规范化形式等。
var Semantic = conversion.EqualitiesOrDie(
	func(a, b resource.Quantity) bool {
		// 只比较数值，忽略书写格式；未初始化的 Quantity 与 0 视为等价。
		// TODO: 若日后确认格式一致性重要，可以安全地改为同时比较格式。
		return a.Cmp(b) == 0
	},
	func(a, b metav1.MicroTime) bool {
		// 统一到 UTC 再比较，消除时区差异造成的假性不等。
		return a.UTC() == b.UTC()
	},
	func(a, b metav1.Time) bool {
		// 同上：同一时刻的不同时区表示应视为相等。
		return a.UTC() == b.UTC()
	},
	func(a, b metav1.FieldsV1) bool {
		// 字段集合以原始字节存储，按内容逐字节比较即可。
		return a.Equal(b)
	},
	func(a, b labels.Selector) bool {
		// 选择器按规范化后的字符串形式比较，忽略解析过程中产生的等价差异。
		return a.String() == b.String()
	},
	func(a, b fields.Selector) bool {
		// 同上：字段选择器以字符串形式比较。
		return a.String() == b.String()
	},
)
