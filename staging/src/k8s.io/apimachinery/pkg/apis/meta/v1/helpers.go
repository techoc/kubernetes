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

package v1

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"

	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/labels"
	cbor "k8s.io/apimachinery/pkg/runtime/serializer/cbor/direct"
	"k8s.io/apimachinery/pkg/selection"
	"k8s.io/apimachinery/pkg/types"
	utiljson "k8s.io/apimachinery/pkg/util/json"
)

// LabelSelectorAsSelector 将 API 类型 LabelSelector 转换为实现 labels.Selector 的内部选择器。
// 修改此函数时，必须同步检查 pkg/labels/selector.go 中的选择器实现，确保两边语义一致。
func LabelSelectorAsSelector(ps *LabelSelector) (labels.Selector, error) {
	if ps == nil {
		// nil 选择器在 API 语义中表示"不匹配任何对象"，与空选择器（匹配全部）相反。
		return labels.Nothing(), nil
	}
	if len(ps.MatchLabels)+len(ps.MatchExpressions) == 0 {
		// 显式给出的空选择器表示"匹配全部"。
		return labels.Everything(), nil
	}
	requirements := make([]labels.Requirement, 0, len(ps.MatchLabels)+len(ps.MatchExpressions))
	// MatchLabels 等价于一组 Equals 表达式。
	for k, v := range ps.MatchLabels {
		r, err := labels.NewRequirement(k, selection.Equals, []string{v})
		if err != nil {
			return nil, err
		}
		requirements = append(requirements, *r)
	}
	for _, expr := range ps.MatchExpressions {
		var op selection.Operator
		// API 枚举到内部运算符的映射；未知值直接报错，避免静默放宽选择条件。
		switch expr.Operator {
		case LabelSelectorOpIn:
			op = selection.In
		case LabelSelectorOpNotIn:
			op = selection.NotIn
		case LabelSelectorOpExists:
			op = selection.Exists
		case LabelSelectorOpDoesNotExist:
			op = selection.DoesNotExist
		default:
			return nil, fmt.Errorf("%q is not a valid label selector operator", expr.Operator)
		}
		// 复制 Values，防止生成的 Requirement 与调用方共享底层数组。
		r, err := labels.NewRequirement(expr.Key, op, append([]string(nil), expr.Values...))
		if err != nil {
			return nil, err
		}
		requirements = append(requirements, *r)
	}
	selector := labels.NewSelector()
	selector = selector.Add(requirements...)
	return selector, nil
}

// LabelSelectorAsMap 将 API 类型 LabelSelector 转换为普通的 map[string]string。
// Exists、DoesNotExist、NotIn，以及包含多个值的 In 无法用简单键值映射表达，因此会返回错误。
func LabelSelectorAsMap(ps *LabelSelector) (map[string]string, error) {
	if ps == nil {
		return nil, nil
	}
	selector := map[string]string{}
	for k, v := range ps.MatchLabels {
		selector[k] = v
	}
	for _, expr := range ps.MatchExpressions {
		switch expr.Operator {
		case LabelSelectorOpIn:
			if len(expr.Values) != 1 {
				// 只能表达单值等式，多值 In 会丢失语义。
				return selector, fmt.Errorf("operator %q without a single value cannot be converted into the old label selector format", expr.Operator)
			}
			// 如果该表达式覆盖了之前 MatchLabels 中相同 key 的值，当前行为是以后出现的值为准。
			selector[expr.Key] = expr.Values[0]
		case LabelSelectorOpNotIn, LabelSelectorOpExists, LabelSelectorOpDoesNotExist:
			// 这些运算符无法用 map 表示；返回已转换的部分以便调用方诊断。
			return selector, fmt.Errorf("operator %q cannot be converted into the old label selector format", expr.Operator)
		default:
			return selector, fmt.Errorf("%q is not a valid selector operator", expr.Operator)
		}
	}
	return selector, nil
}

// ParseToLabelSelector 将选择器字符串解析为 API 类型 LabelSelector。
// 修改此函数时，必须同步检查 pkg/labels/selector.go 中的解析器，确保语法和语义一致。
func ParseToLabelSelector(selector string) (*LabelSelector, error) {
	reqs, err := labels.ParseToRequirements(selector)
	if err != nil {
		return nil, fmt.Errorf("couldn't parse the selector string \"%s\": %v", selector, err)
	}

	// 即使没有条件也初始化 map/slice，保证序列化结果是 {} / [] 而不是 null。
	labelSelector := &LabelSelector{
		MatchLabels:      map[string]string{},
		MatchExpressions: []LabelSelectorRequirement{},
	}
	for _, req := range reqs {
		var op LabelSelectorOperator
		switch req.Operator() {
		case selection.Equals, selection.DoubleEquals:
			// 单值等式回填到 MatchLabels，与 API 中的惯用表示一致。
			vals := req.Values()
			if vals.Len() != 1 {
				return nil, fmt.Errorf("equals operator must have exactly one value")
			}
			val, ok := vals.PopAny()
			if !ok {
				return nil, fmt.Errorf("equals operator has exactly one value but it cannot be retrieved")
			}
			labelSelector.MatchLabels[req.Key()] = val
			continue
		case selection.In:
			op = LabelSelectorOpIn
		case selection.NotIn:
			op = LabelSelectorOpNotIn
		case selection.Exists:
			op = LabelSelectorOpExists
		case selection.DoesNotExist:
			op = LabelSelectorOpDoesNotExist
		case selection.GreaterThan, selection.LessThan:
			// 单独列出这两个操作符，明确表示它们在 LabelSelector API 中被有意禁止。
			// 字符串选择器支持数值比较，但 LabelSelector API 不支持，因此显式拒绝。
			return nil, fmt.Errorf("%q isn't supported in label selectors", req.Operator())
		default:
			return nil, fmt.Errorf("%q is not a valid label selector operator", req.Operator())
		}
		labelSelector.MatchExpressions = append(labelSelector.MatchExpressions, LabelSelectorRequirement{
			Key:      req.Key(),
			Operator: op,
			Values:   req.Values().List(),
		})
	}
	return labelSelector, nil
}

// SetAsLabelSelector 将 labels.Set 转换为 API 类型 LabelSelector。
func SetAsLabelSelector(ls labels.Set) *LabelSelector {
	if ls == nil {
		// 保留 nil 与空 map 的区别：前者表示"未设置"。
		return nil
	}

	selector := &LabelSelector{
		MatchLabels: make(map[string]string, len(ls)),
	}
	for label, value := range ls {
		selector.MatchLabels[label] = value
	}

	return selector
}

// FormatLabelSelector 将 LabelSelector 转换为便于展示的选择器字符串。
//
// 仅用于日志/错误信息展示：转换失败时返回 "<error>"，空选择器返回 "<none>"，
// 避免在展示路径上再抛出错误。
func FormatLabelSelector(labelSelector *LabelSelector) string {
	selector, err := LabelSelectorAsSelector(labelSelector)
	if err != nil {
		return "<error>"
	}

	l := selector.String()
	if len(l) == 0 {
		l = "<none>"
	}
	return l
}

// ExtractGroupVersions 从 APIGroupList 中收集所有 group/version 字符串。
func ExtractGroupVersions(l *APIGroupList) []string {
	var groupVersions []string
	for _, g := range l.Groups {
		for _, gv := range g.Versions {
			groupVersions = append(groupVersions, gv.GroupVersion)
		}
	}
	return groupVersions
}

// HasAnnotation 判断 ObjectMeta.Annotations 中是否存在指定注解键。
func HasAnnotation(obj ObjectMeta, ann string) bool {
	_, found := obj.Annotations[ann]
	return found
}

// SetMetaDataAnnotation 设置指定注解键和值；原有同名键会被覆盖。
//
// 若 map 为 nil 则先初始化，使调用方无需关心是否已有注解。
func SetMetaDataAnnotation(obj *ObjectMeta, ann string, value string) {
	if obj.Annotations == nil {
		obj.Annotations = make(map[string]string)
	}
	obj.Annotations[ann] = value
}

// HasLabel 判断 ObjectMeta.Labels 中是否存在指定标签键。
func HasLabel(obj ObjectMeta, label string) bool {
	_, found := obj.Labels[label]
	return found
}

// SetMetaDataLabel 设置指定标签键和值；原有同名键会被覆盖。
//
// 若 map 为 nil 则先初始化。
func SetMetaDataLabel(obj *ObjectMeta, label string, value string) {
	if obj.Labels == nil {
		obj.Labels = make(map[string]string)
	}
	obj.Labels[label] = value
}

// SingleObject 创建只针对一个对象的 ListOptions，通过 metadata.name 字段选择器限定对象，
// 并从该对象当前 ResourceVersion 开始 watch。
//
// 通过 metadata.name 字段选择器把 list/watch 限定到单个对象，并带上其
// resourceVersion 以从已知点开始 watch。
func SingleObject(meta ObjectMeta) ListOptions {
	return ListOptions{
		FieldSelector:   fields.OneTermEqualSelector("metadata.name", meta.Name).String(),
		ResourceVersion: meta.ResourceVersion,
	}
}

// NewDeleteOptions 创建 DeleteOptions，指定资源在 grace 秒后删除。grace 为 0 表示立即删除；
// 如果希望使用服务器默认宽限期，应直接使用 &metav1.DeleteOptions{}，不要调用此函数。
func NewDeleteOptions(grace int64) *DeleteOptions {
	return &DeleteOptions{GracePeriodSeconds: &grace}
}

// NewPreconditionDeleteOptions 创建带 UID 前置条件的 DeleteOptions，确保删除目标 UID 与预期一致。
//
// UID 前置条件让删除只对特定对象生效，避免在"读取-删除"之间对象被重建后误删。
func NewPreconditionDeleteOptions(uid string) *DeleteOptions {
	u := types.UID(uid)
	p := Preconditions{UID: &u}
	return &DeleteOptions{Preconditions: &p}
}

// NewUIDPreconditions 创建仅设置 UID 前置条件的 Preconditions。
func NewUIDPreconditions(uid string) *Preconditions {
	u := types.UID(uid)
	return &Preconditions{UID: &u}
}

// NewRVDeletionPrecondition 创建带 ResourceVersion 前置条件的 DeleteOptions，防止删除期间对象发生变化。
//
// 资源版本前置条件用于实现乐观并发：若对象在此期间被修改，删除会失败。
func NewRVDeletionPrecondition(rv string) *DeleteOptions {
	p := Preconditions{ResourceVersion: &rv}
	return &DeleteOptions{Preconditions: &p}
}

// HasObjectMetaSystemFieldValues 判断 ObjectMeta 中是否已有系统填充字段的值（创建时间或 UID）。
//
// 用于判断对象是否已被持久化：创建时间与 UID 都由系统在写入时填充。
func HasObjectMetaSystemFieldValues(meta Object) bool {
	return !meta.GetCreationTimestamp().Time.IsZero() ||
		len(meta.GetUID()) != 0
}

// ResetObjectMetaForStatus 将 status 更新对象的元数据重置为已存在对象的元数据。
// 该保护需要由支持 status 子资源的新资源显式采用。
//
// 原因：/status 子资源不应能修改元数据。把元数据字段（labels、annotations、ownerRefs、
// finalizers 等）覆盖为已有对象的值，可以防止客户端通过 status 更新绕过校验。
// 注意 managedFields 不在此列：它需要在 status 更新中被重新计算。
func ResetObjectMetaForStatus(meta, existingMeta Object) {
	meta.SetDeletionTimestamp(existingMeta.GetDeletionTimestamp())
	meta.SetGeneration(existingMeta.GetGeneration())
	meta.SetSelfLink(existingMeta.GetSelfLink())
	meta.SetLabels(existingMeta.GetLabels())
	meta.SetAnnotations(existingMeta.GetAnnotations())
	meta.SetFinalizers(existingMeta.GetFinalizers())
	meta.SetOwnerReferences(existingMeta.GetOwnerReferences())
	// managedFields 必须保留，因为 status 更新期间它已经被修改，用于跟踪本次更新实际变更的字段。
	//meta.SetManagedFields(existingMeta.GetManagedFields())
}

// MarshalJSON 实现 json.Marshaler。该方法可能通过值或指针调用，因此接收者使用值类型，
// 确保两种调用形式都能正确触发自定义序列化。
func (f FieldsV1) MarshalJSON() ([]byte, error) {
	raw := f.GetRawBytes()
	if len(raw) == 0 {
		return []byte("null"), nil
	}
	if f.getContentType() == fieldsV1InvalidOrValidCBORObject {
		// 存储的是 CBOR，需要转码为 JSON。先解码为通用结构再序列化，
		// 以兼容任意合法的 map 内容。
		var u map[string]interface{}
		if err := cbor.Unmarshal(raw, &u); err != nil {
			return nil, fmt.Errorf("metav1.FieldsV1 cbor invalid: %w", err)
		}
		return utiljson.Marshal(u)
	}
	return raw, nil
}

// UnmarshalJSON 实现 json.Unmarshaler。
//
// 只保存原始字节而不解析：FieldsV1 的内容通常原样透传给客户端，延迟解析可以
// 减少开销，也允许保留未知/非法内容。
func (f *FieldsV1) UnmarshalJSON(b []byte) error {
	if f == nil {
		return errors.New("metav1.FieldsV1: UnmarshalJSON on nil pointer")
	}
	if !bytes.Equal(b, []byte("null")) {
		f.SetRawBytes(b)
	}
	return nil
}

var _ json.Marshaler = FieldsV1{}
var _ json.Unmarshaler = &FieldsV1{}

// MarshalCBOR 把 FieldsV1 编码为 CBOR；若原始字节是 JSON，则先转码。
func (f FieldsV1) MarshalCBOR() ([]byte, error) {
	raw := f.GetRawBytes()
	if len(raw) == 0 {
		return cbor.Marshal(nil)
	}
	if f.getContentType() == fieldsV1InvalidOrValidJSONObject {
		var u map[string]interface{}
		if err := utiljson.Unmarshal(raw, &u); err != nil {
			return nil, fmt.Errorf("metav1.FieldsV1 json invalid: %w", err)
		}
		return cbor.Marshal(u)
	}
	return raw, nil
}

// cborNull 是 CBOR 的 null 简单值；UnmarshalCBOR 用它识别"显式 null"。
var cborNull = []byte{0xf6}

// UnmarshalCBOR 保存原始 CBOR 字节，null 则保持零值。
func (f *FieldsV1) UnmarshalCBOR(b []byte) error {
	if f == nil {
		return errors.New("metav1.FieldsV1: UnmarshalCBOR on nil pointer")
	}
	if !bytes.Equal(b, cborNull) {
		f.SetRawBytes(b)
	}
	return nil
}

// FieldsV1 的内容类型判定结果。
const (
	// fieldsV1InvalidOrEmpty 表示 FieldsV1 没有原始字节，或原始字节无法被识别为任何受支持的合法编码。
	fieldsV1InvalidOrEmpty = iota

	// fieldsV1InvalidOrValidJSONObject 表示原始字节的首字节符合 JSON 对象或 null 的编码特征；
	// 该判断只识别可能的编码格式，不代表整个 JSON 内容已经通过完整校验。
	fieldsV1InvalidOrValidJSONObject

	// fieldsV1InvalidOrValidCBORObject 表示原始字节的首字节符合 CBOR map 或 null 的编码特征；
	// 该判断只识别可能的编码格式，不代表整个 CBOR 内容已经通过完整校验。
	fieldsV1InvalidOrValidCBORObject
)

// getContentType 根据 Raw 的首字节，判断其属于空值/未知、JSON 对象候选，还是 CBOR 对象候选。
//
// Raw 只允许为空、null 或对象（map）；JSON 字符串、数字、布尔值和数组均不符合 FieldsV1 约定。
// 对非空且符合约定的内容，首字节可以无歧义地区分 JSON 与 CBOR 编码。
//
// JSON 对象候选的首字节可以是四种 JSON 空白字符、null 的首字母 n 或左花括号 {，
// 对应十六进制值 0x09、0x0a、0x0d、0x20、0x6e、0x7b。CBOR 对象候选可以以 null 简单值、
// 主类型为 map 的首字节，或带 tag 的 map 首字节开头，对应 0xf6、0xa0..0xbf、0xc6..0xdb。
// 两组首字节范围互不重叠，因此可用于快速识别编码格式。
//
// 注意这些常量名都是 "InvalidOr..."：只看首字节无法确认整个值是否合法，只能确认
// "可能是哪种编码"。真正的合法性在转码时由解码器判定。
func (f FieldsV1) getContentType() int {
	reader := f.GetRawReader()
	if reader.Size() > 0 {
		var buf [1]byte
		if _, err := reader.Read(buf[:]); err != nil {
			return fieldsV1InvalidOrEmpty
		}
		p := buf[0]
		switch p {
		case 'n', '{', '\t', '\r', '\n', ' ':
			return fieldsV1InvalidOrValidJSONObject
		case 0xf6: // null
			return fieldsV1InvalidOrValidCBORObject
		default:
			if p >= 0xa0 && p <= 0xbf /* map */ || p >= 0xc6 && p <= 0xdb /* tag */ {
				return fieldsV1InvalidOrValidCBORObject
			}
		}
	}
	return fieldsV1InvalidOrEmpty
}
