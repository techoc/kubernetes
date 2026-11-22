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

// Package fuzzer 为 metav1/metav1beta1 类型注册随机化函数，供 roundtrip 测试使用。
// 目的是生成“能通过序列化往返”的随机值，避免因不可往返的构造导致测试误报。
package fuzzer

import (
	"fmt"
	"math/rand"
	"sort"
	"strconv"
	"strings"

	"sigs.k8s.io/randfill"

	apitesting "k8s.io/apimachinery/pkg/api/apitesting"
	"k8s.io/apimachinery/pkg/api/apitesting/fuzzer"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	metav1beta1 "k8s.io/apimachinery/pkg/apis/meta/v1beta1"
	"k8s.io/apimachinery/pkg/runtime"
	runtimeserializer "k8s.io/apimachinery/pkg/runtime/serializer"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
)

// genericFuzzerFuncs 提供与具体 API 组无关的基础类型随机化函数。
func genericFuzzerFuncs(codecs runtimeserializer.CodecFactory) []interface{} {
	return []interface{}{
		func(q *resource.Quantity, c randfill.Continue) {
			// 用有限范围内的整数构造 Quantity，避免极端数值在往返中改变表示形式。
			*q = *resource.NewQuantity(c.Int63n(1000), resource.DecimalExponent)
		},
		func(j *int, c randfill.Continue) {
			*j = int(c.Int31())
		},
		func(j **int, c randfill.Continue) {
			// 一半概率为 nil、一半概率为随机值，以覆盖可选字段的两种状态。
			if c.Bool() {
				i := int(c.Int31())
				*j = &i
			} else {
				*j = nil
			}
		},
		func(j *runtime.TypeMeta, c randfill.Continue) {
			// 必须定制 TypeMeta 的随机化：内存中 APIVersion 与 Kind 必须保持为空，
			// 它们只在序列化时由编解码器根据 GVK 填充。
			j.APIVersion = ""
			j.Kind = ""
		},
		func(j *runtime.Object, c randfill.Continue) {
			// TODO: uncomment when round trip starts from a versioned object
			// 目前总是生成 Unknown 对象，保证原始字节在往返中不被改写。
			if true { // c.Bool() {
				*j = &runtime.Unknown{
					// We do not set TypeMeta here because it is not carried through a round trip
					// 这里不设置 TypeMeta，因为往返过程不会保留它。
					Raw:         []byte(`{"apiVersion":"unknown.group/unknown","kind":"Something","someKey":"someValue"}`),
					ContentType: runtime.ContentTypeJSON,
				}
			} else {
				types := []runtime.Object{&metav1.Status{}, &metav1.APIGroup{}}
				t := types[c.Rand.Intn(len(types))]
				c.Fill(t)
				*j = t
			}
		},
		func(r *runtime.RawExtension, c randfill.Continue) {
			// Pick an arbitrary type and fuzz it
			// 任选一种类型填充，再编码为原始字节。
			types := []runtime.Object{&metav1.Status{}, &metav1.APIGroup{}}
			obj := types[c.Rand.Intn(len(types))]
			c.Fill(obj)

			// Find a codec for converting the object to raw bytes.  This is necessary for the
			// api version and kind to be correctly set be serialization.
			// 找到能把对象编码为原始字节的 codec，序列化时才能正确填入 apiVersion 与 kind。
			var codec = apitesting.TestCodec(codecs, metav1.SchemeGroupVersion)

			// Convert the object to raw bytes
			// 把对象编码为原始字节。
			bytes, err := runtime.Encode(codec, obj)
			if err != nil {
				panic(fmt.Sprintf("Failed to encode object: %v", err))
			}

			// strip trailing newlines which do not survive roundtrips
			// 去掉末尾换行，它们在往返中不会被保留。
			for len(bytes) >= 1 && bytes[len(bytes)-1] == 10 {
				bytes = bytes[:len(bytes)-1]
			}

			// Set the bytes field on the RawExtension
			// 写入 RawExtension 的字节字段。
			r.Raw = bytes
		},
	}
}

// taken from randfill (nee gofuzz) internals for RandString
// charRange 表示一个闭区间字符范围，用于从区间内随机选取 rune。
type charRange struct {
	first, last rune
}

// choose 在区间 [first, last] 内均匀随机选取一个 rune。
func (c *charRange) choose(r *rand.Rand) rune {
	count := int64(c.last - c.first + 1)
	ch := c.first + rune(r.Int63n(count))

	return ch
}

// randomLabelPart 生成合法的随机标签值或标签键的 name 部分。
// canBeEmpty 为 true 时长度可为 0，否则长度至少为 1；
// 首尾字符限制在字母数字，中间允许 . - _，与标签语法保持一致。
func randomLabelPart(c randfill.Continue, canBeEmpty bool) string {
	validStartEnd := []charRange{{'0', '9'}, {'a', 'z'}, {'A', 'Z'}}
	validMiddle := []charRange{{'0', '9'}, {'a', 'z'}, {'A', 'Z'},
		{'.', '.'}, {'-', '-'}, {'_', '_'}}

	partLen := c.Rand.Intn(64) // len is [0, 63]
	if !canBeEmpty {
		partLen = c.Rand.Intn(63) + 1 // len is [1, 63]
	}

	runes := make([]rune, partLen)
	if partLen == 0 {
		return string(runes)
	}

	runes[0] = validStartEnd[c.Rand.Intn(len(validStartEnd))].choose(c.Rand)
	for i := range runes[1:] {
		runes[i+1] = validMiddle[c.Rand.Intn(len(validMiddle))].choose(c.Rand)
	}
	runes[len(runes)-1] = validStartEnd[c.Rand.Intn(len(validStartEnd))].choose(c.Rand)

	return string(runes)
}

// randomDNSLabel 生成合法的随机 DNS label（小写字母数字，中间允许连字符）。
func randomDNSLabel(c randfill.Continue) string {
	validStartEnd := []charRange{{'0', '9'}, {'a', 'z'}}
	validMiddle := []charRange{{'0', '9'}, {'a', 'z'}, {'-', '-'}}

	partLen := c.Rand.Intn(63) + 1 // len is [1, 63]
	runes := make([]rune, partLen)

	runes[0] = validStartEnd[c.Rand.Intn(len(validStartEnd))].choose(c.Rand)
	for i := range runes[1:] {
		runes[i+1] = validMiddle[c.Rand.Intn(len(validMiddle))].choose(c.Rand)
	}
	runes[len(runes)-1] = validStartEnd[c.Rand.Intn(len(validStartEnd))].choose(c.Rand)

	return string(runes)
}

// randomLabelKey 生成合法的标签键，可能带 DNS 子域名前缀（如 "example.com/name"）。
func randomLabelKey(c randfill.Continue) string {
	namePart := randomLabelPart(c, false)
	prefixPart := ""

	usePrefix := c.Bool()
	if usePrefix {
		// we can fit, with dots, at most 3 labels in the 253 allotted characters
		// 在 253 字符的限额内，加上点号最多可容纳 3 个 label 段。
		prefixPartsLen := c.Rand.Intn(2) + 1
		prefixParts := make([]string, prefixPartsLen)
		for i := range prefixParts {
			prefixParts[i] = randomDNSLabel(c)
		}
		prefixPart = strings.Join(prefixParts, ".") + "/"
	}

	return prefixPart + namePart
}

// v1FuzzerFuncs 提供 metav1 各类型的随机化函数。
func v1FuzzerFuncs(codecs runtimeserializer.CodecFactory) []interface{} {

	return []interface{}{
		func(j *metav1.TypeMeta, c randfill.Continue) {
			// 同 genericFuzzerFuncs：内存中 TypeMeta 必须留空。
			j.APIVersion = ""
			j.Kind = ""
		},
		func(j *metav1.ObjectMeta, c randfill.Continue) {
			c.FillNoCustom(j)

			// ResourceVersion 必须是十进制字符串形式；UID 用随机字符串填充。
			j.ResourceVersion = strconv.FormatUint(c.Uint64(), 10)
			j.UID = types.UID(c.String(0))

			// Fuzzing sec and nsec in a smaller range (uint32 instead of int64),
			// so that the result Unix time is a valid date and can be parsed into RFC3339 format.
			// 用 uint32 范围生成秒/纳秒，保证结果时间能表示为合法 RFC3339 日期。
			var sec, nsec uint32
			c.Fill(&sec)
			c.Fill(&nsec)
			j.CreationTimestamp = metav1.Unix(int64(sec), int64(nsec)).Rfc3339Copy()

			if j.DeletionTimestamp != nil {
				c.Fill(&sec)
				c.Fill(&nsec)
				t := metav1.Unix(int64(sec), int64(nsec)).Rfc3339Copy()
				j.DeletionTimestamp = &t
			}

			// 空的集合在往返后会变成 nil，这里提前归一化；
			// 同时删除空字符串键，避免非法标签/注解键。
			if len(j.Labels) == 0 {
				j.Labels = nil
			} else {
				delete(j.Labels, "")
			}
			if len(j.Annotations) == 0 {
				j.Annotations = nil
			} else {
				delete(j.Annotations, "")
			}
			if len(j.OwnerReferences) == 0 {
				j.OwnerReferences = nil
			}
			if len(j.Finalizers) == 0 {
				j.Finalizers = nil
			}
		},
		func(j *metav1.ResourceVersionMatch, c randfill.Continue) {
			// 只从合法枚举值中取值。
			matches := []metav1.ResourceVersionMatch{"", metav1.ResourceVersionMatchExact, metav1.ResourceVersionMatchNotOlderThan}
			*j = matches[c.Rand.Intn(len(matches))]
		},
		func(j *metav1.ListMeta, c randfill.Continue) {
			j.ResourceVersion = strconv.FormatUint(c.Uint64(), 10)
			j.SelfLink = c.String(0) //nolint:staticcheck // SA1019 backwards compatibility
		},
		func(j *metav1.LabelSelector, c randfill.Continue) {
			c.FillNoCustom(j)
			// we can't have an entirely empty selector, so force
			// use of MatchExpression if necessary
			// 完全空的 selector 不合法，必要时强制生成 MatchExpression。
			if len(j.MatchLabels) == 0 && len(j.MatchExpressions) == 0 {
				j.MatchExpressions = make([]metav1.LabelSelectorRequirement, c.Rand.Intn(2)+1)
			}

			if j.MatchLabels != nil {
				fuzzedMatchLabels := make(map[string]string, len(j.MatchLabels))
				for i := 0; i < len(j.MatchLabels); i++ {
					fuzzedMatchLabels[randomLabelKey(c)] = randomLabelPart(c, true)
				}
				j.MatchLabels = fuzzedMatchLabels
			}

			validOperators := []metav1.LabelSelectorOperator{
				metav1.LabelSelectorOpIn,
				metav1.LabelSelectorOpNotIn,
				metav1.LabelSelectorOpExists,
				metav1.LabelSelectorOpDoesNotExist,
			}

			if j.MatchExpressions != nil {
				// NB: the label selector parser code sorts match expressions by key, and
				// sorts and deduplicates the values, so we need to make sure ours are
				// sorted and deduplicated as well here to preserve round-trip comparison.
				// In practice, not sorting doesn't hurt anything...
				// 注意：选择器解析器会按 key 排序表达式，并对值排序去重；
				// 为保持往返比较一致，这里也做同样的排序与去重。
				for i := range j.MatchExpressions {
					req := metav1.LabelSelectorRequirement{}
					c.Fill(&req)
					req.Key = randomLabelKey(c)
					req.Operator = validOperators[c.Rand.Intn(len(validOperators))]
					if req.Operator == metav1.LabelSelectorOpIn || req.Operator == metav1.LabelSelectorOpNotIn {
						if len(req.Values) == 0 {
							// we must have some values here, so randomly choose a short length
							// In/NotIn 必须至少有一个值，随机取一个短长度。
							req.Values = make([]string, c.Rand.Intn(2)+1)
						}
						for i := range req.Values {
							req.Values[i] = randomLabelPart(c, true)
						}
						req.Values = sets.List(sets.New(req.Values...))
					} else {
						// Exists/DoesNotExist 不允许携带值。
						req.Values = nil
					}
					j.MatchExpressions[i] = req
				}

				sort.Slice(j.MatchExpressions, func(a, b int) bool { return j.MatchExpressions[a].Key < j.MatchExpressions[b].Key })
			}
		},
		func(j *metav1.ManagedFieldsEntry, c randfill.Continue) {
			c.FillNoCustom(j)
			// FieldsV1 的随机字节无法保证可往返，统一置空。
			j.FieldsV1 = nil
		},
	}
}

// v1beta1FuzzerFuncs 提供 metav1beta1 各类型的随机化函数。
func v1beta1FuzzerFuncs(codecs runtimeserializer.CodecFactory) []interface{} {
	return []interface{}{
		func(r *metav1beta1.TableOptions, c randfill.Continue) {
			c.FillNoCustom(r)
			// NoHeaders is not serialized to the wire but is allowed within the versioned
			// type because we don't use meta internal types in the client and API server.
			// NoHeaders 不会序列化到线上，但允许存在于 versioned 类型中，
			// 因为客户端与 API server 不使用 meta 内部类型。
			r.NoHeaders = false
		},
		func(r *metav1beta1.TableRow, c randfill.Continue) {
			c.Fill(&r.Object)
			c.Fill(&r.Conditions)
			// 空切片往返后会变成 nil，提前归一化。
			if len(r.Conditions) == 0 {
				r.Conditions = nil
			}
			n := c.Intn(10)
			if n > 0 {
				r.Cells = make([]interface{}, n)
			}
			// 每个 cell 随机选择一种 JSON 可表示的类型，覆盖表格输出可能出现的值形态。
			for i := range r.Cells {
				t := c.Intn(6)
				switch t {
				case 0:
					r.Cells[i] = c.String(0)
				case 1:
					r.Cells[i] = c.Int63()
				case 2:
					r.Cells[i] = c.Bool()
				case 3:
					x := map[string]interface{}{}
					for j := c.Intn(10) + 1; j >= 0; j-- {
						x[c.String(0)] = c.String(0)
					}
					r.Cells[i] = x
				case 4:
					x := make([]interface{}, c.Intn(10))
					for i := range x {
						x[i] = c.Int63()
					}
					r.Cells[i] = x
				default:
					r.Cells[i] = nil
				}
			}
		},
	}
}

// Funcs 汇总所有随机化函数，供 apimachinery 的 roundtrip 测试注册使用。
var Funcs = fuzzer.MergeFuzzerFuncs(
	genericFuzzerFuncs,
	v1FuzzerFuncs,
	v1beta1FuzzerFuncs,
)
