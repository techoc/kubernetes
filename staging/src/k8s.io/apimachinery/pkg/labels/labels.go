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

package labels

import (
	"fmt"
	"sort"
	"strings"

	"k8s.io/apimachinery/pkg/util/validation/field"
)

// Labels 允许把标签的展示与存储方式解耦。
type Labels interface {
	// Has 返回给定标签是否存在。
	Has(label string) (exists bool)

	// Get 返回给定标签的值。
	Get(label string) (value string)

	// Lookup 返回给定标签的值（若存在）以及是否存在。
	Lookup(label string) (value string, exists bool)
}

// Set 是 label:value 的 map，实现 Labels 接口。
type Set map[string]string

// String 以人类可读形式返回所有标签。
// 其格式恰好与 ParseSelector 接受的输入一致。
func (ls Set) String() string {
	selector := make([]string, 0, len(ls))
	for key, value := range ls {
		selector = append(selector, key+"="+value)
	}
	// 排序以保证输出确定。
	sort.StringSlice(selector).Sort()
	return strings.Join(selector, ",")
}

// Has 返回 map 中是否存在给定标签。
func (ls Set) Has(label string) bool {
	_, exists := ls[label]
	return exists
}

// Get 返回 map 中给定标签的值。
func (ls Set) Get(label string) string {
	return ls[label]
}

// Lookup 返回给定标签的值（若存在）以及是否存在。
func (ls Set) Lookup(label string) (string, bool) {
	val, exists := ls[label]
	return val, exists
}

// AsSelector 把标签转换为选择器。它不做任何校验，因此若 Set 含非法值，
// 服务端会拒绝该请求。
func (ls Set) AsSelector() Selector {
	return SelectorFromSet(ls)
}

// AsValidatedSelector 把标签转换为选择器。
// Set 会在客户端校验，从而尽早发现错误。
func (ls Set) AsValidatedSelector() (Selector, error) {
	return ValidatedSelectorFromSet(ls)
}

// AsSelectorPreValidated 把标签转换为选择器，但假定标签已校验，因此不做校验。
// 根据我们的测量，在高并发关键路径上这样明显更快。
// 注意：该方法会复制 Set；若 Set 不可变，可考虑用 ValidatedSetSelector 包装，
// 后者不复制。
func (ls Set) AsSelectorPreValidated() Selector {
	return SelectorFromValidatedSet(ls)
}

// FormatLabels 把标签 map 转换为普通字符串；空 map 输出 "<none>"。
func FormatLabels(labelMap map[string]string) string {
	l := Set(labelMap).String()
	if l == "" {
		l = "<none>"
	}
	return l
}

// Conflicts 接收两个 map，若存在相同 key 但值不同则返回 true，其余情况返回 false。
func Conflicts(labels1, labels2 Set) bool {
	// 遍历较小的 map 以减少比较次数。
	small := labels1
	big := labels2
	if len(labels2) < len(labels1) {
		small = labels2
		big = labels1
	}

	for k, v := range small {
		if val, match := big[k]; match {
			if val != v {
				return true
			}
		}
	}

	return false
}

// Merge 合并给定的 map，不检查冲突。冲突时第二个 map（labels2）优先。
func Merge(labels1, labels2 Set) Set {
	mergedMap := Set{}

	for k, v := range labels1 {
		mergedMap[k] = v
	}
	for k, v := range labels2 {
		mergedMap[k] = v
	}
	return mergedMap
}

// Equals 判断给定的两个 map 是否相等。
func Equals(labels1, labels2 Set) bool {
	if len(labels1) != len(labels2) {
		return false
	}

	for k, v := range labels1 {
		value, ok := labels2[k]
		if !ok {
			return false
		}
		if value != v {
			return false
		}
	}
	return true
}

// ConvertSelectorToLabelsMap 把选择器字符串转换为标签 map，并校验键和值。
// 只支持 key=value 形式，逗号分隔；空字符串返回空 map。
func ConvertSelectorToLabelsMap(selector string, opts ...field.PathOption) (Set, error) {
	labelsMap := Set{}

	if len(selector) == 0 {
		return labelsMap, nil
	}

	labels := strings.Split(selector, ",")
	for _, label := range labels {
		l := strings.Split(label, "=")
		if len(l) != 2 {
			return labelsMap, fmt.Errorf("invalid selector: %s", l)
		}
		key := strings.TrimSpace(l[0])
		if err := validateLabelKey(key, field.ToPath(opts...)); err != nil {
			return labelsMap, err
		}
		value := strings.TrimSpace(l[1])
		if err := validateLabelValue(key, value, field.ToPath(opts...)); err != nil {
			return labelsMap, err
		}
		labelsMap[key] = value
	}
	return labelsMap, nil
}
