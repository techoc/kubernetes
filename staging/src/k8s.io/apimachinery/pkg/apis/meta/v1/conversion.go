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

package v1

import (
	"fmt"
	"net/url"
	"strconv"
	"strings"

	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/conversion"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
)

// Convert_Pointer_float64_To_float64 把可选 float64 转为必填值；nil 视为零值。
// 注意转换不可逆：显式的 0 与 nil 在结果中无法区分，这是 v1<->internal 的历史约定。
func Convert_Pointer_float64_To_float64(in **float64, out *float64, s conversion.Scope) error {
	if *in == nil {
		*out = 0
		return nil
	}
	*out = float64(**in)
	return nil
}

// Convert_float64_To_Pointer_float64 把必填 float64 转为指针；总是分配新对象，
// 避免 out 与调用方共享同一块内存。
func Convert_float64_To_Pointer_float64(in *float64, out **float64, s conversion.Scope) error {
	temp := float64(*in)
	*out = &temp
	return nil
}

// Convert_Pointer_int32_To_int32 见 float64 版本：nil 折叠为 0。
func Convert_Pointer_int32_To_int32(in **int32, out *int32, s conversion.Scope) error {
	if *in == nil {
		*out = 0
		return nil
	}
	*out = int32(**in)
	return nil
}

// Convert_int32_To_Pointer_int32 总是产生非 nil 指针（即使值是 0）。
func Convert_int32_To_Pointer_int32(in *int32, out **int32, s conversion.Scope) error {
	temp := int32(*in)
	*out = &temp
	return nil
}

// Convert_Pointer_int64_To_int64 见 float64 版本：nil 折叠为 0。
func Convert_Pointer_int64_To_int64(in **int64, out *int64, s conversion.Scope) error {
	if *in == nil {
		*out = 0
		return nil
	}
	*out = int64(**in)
	return nil
}

// Convert_int64_To_Pointer_int64 总是产生非 nil 指针。
func Convert_int64_To_Pointer_int64(in *int64, out **int64, s conversion.Scope) error {
	temp := int64(*in)
	*out = &temp
	return nil
}

// Convert_Pointer_int64_To_int 把 int64 收窄为平台相关的 int；nil 折叠为 0。
func Convert_Pointer_int64_To_int(in **int64, out *int, s conversion.Scope) error {
	if *in == nil {
		*out = 0
		return nil
	}
	*out = int(**in)
	return nil
}

// Convert_int_To_Pointer_int64 把 int 扩宽为 int64 指针。
func Convert_int_To_Pointer_int64(in *int, out **int64, s conversion.Scope) error {
	temp := int64(*in)
	*out = &temp
	return nil
}

// Convert_Pointer_string_To_string 把可选字符串转为必填；nil 折叠为空串。
// 空串与 nil 的语义差异在转换后丢失。
func Convert_Pointer_string_To_string(in **string, out *string, s conversion.Scope) error {
	if *in == nil {
		*out = ""
		return nil
	}
	*out = **in
	return nil
}

// Convert_string_To_Pointer_string 把必填字符串转为指针。
// in 为 nil 时分配指向空串的新指针；否则直接共享输入指针，不做复制。
func Convert_string_To_Pointer_string(in *string, out **string, s conversion.Scope) error {
	if in == nil {
		stringVar := ""
		*out = &stringVar
		return nil
	}
	*out = in
	return nil
}

// Convert_Pointer_bool_To_bool 把可选布尔转为必填；nil 折叠为 false。
// 这正是 tri-state 布尔无法在内部形态中表达的经典限制。
func Convert_Pointer_bool_To_bool(in **bool, out *bool, s conversion.Scope) error {
	if *in == nil {
		*out = false
		return nil
	}
	*out = **in
	return nil
}

// Convert_bool_To_Pointer_bool 把 bool 转为指针；nil 输入生成指向 false 的新指针。
func Convert_bool_To_Pointer_bool(in *bool, out **bool, s conversion.Scope) error {
	if in == nil {
		boolVar := false
		*out = &boolVar
		return nil
	}
	*out = in
	return nil
}

// +k8s:conversion-fn=drop
//
// Convert_v1_TypeMeta_To_v1_TypeMeta 故意不复制 TypeMeta：在内部转换中
// APIVersion/Kind 由序列化层决定，保留它们会让对象携带错误的类型信息。
func Convert_v1_TypeMeta_To_v1_TypeMeta(in, out *TypeMeta, s conversion.Scope) error {
	// 明确不复制这两个字段：内部对象的类型信息由 scheme/serializer 决定。
	return nil
}

// +k8s:conversion-fn=copy-only
//
// Convert_v1_ListMeta_To_v1_ListMeta 是纯拷贝，因为 ListMeta 没有指针字段需要逐项处理。
func Convert_v1_ListMeta_To_v1_ListMeta(in, out *ListMeta, s conversion.Scope) error {
	*out = *in
	return nil
}

// +k8s:conversion-fn=copy-only
//
// Convert_v1_DeleteOptions_To_v1_DeleteOptions 纯拷贝；Preconditions 等指针字段
// 在 v1 同版本互转中共享即可。
func Convert_v1_DeleteOptions_To_v1_DeleteOptions(in, out *DeleteOptions, s conversion.Scope) error {
	*out = *in
	return nil
}

// +k8s:conversion-fn=copy-only
//
// Convert_intstr_IntOrString_To_intstr_IntOrString 纯拷贝，IntOrString 是值类型。
func Convert_intstr_IntOrString_To_intstr_IntOrString(in, out *intstr.IntOrString, s conversion.Scope) error {
	*out = *in
	return nil
}

// Convert_Pointer_intstr_IntOrString_To_intstr_IntOrString 把可选 IntOrString 转为值；
// nil 折叠为零值（Type=0, IntVal=0）。
func Convert_Pointer_intstr_IntOrString_To_intstr_IntOrString(in **intstr.IntOrString, out *intstr.IntOrString, s conversion.Scope) error {
	if *in == nil {
		*out = intstr.IntOrString{} // 零值。
		return nil
	}
	*out = **in // 复制。
	return nil
}

// Convert_intstr_IntOrString_To_Pointer_intstr_IntOrString 把值转为新分配的指针。
func Convert_intstr_IntOrString_To_Pointer_intstr_IntOrString(in *intstr.IntOrString, out **intstr.IntOrString, s conversion.Scope) error {
	temp := *in // 复制。
	*out = &temp
	return nil
}

// +k8s:conversion-fn=copy-only
//
// Convert_v1_Time_To_v1_Time 只能浅拷贝：time.Time 含未导出字段，无法逐字段深拷贝。
func Convert_v1_Time_To_v1_Time(in *Time, out *Time, s conversion.Scope) error {
	// time.Time 包含未导出字段，无法在这里逐字段深拷贝，因此采用结构体浅拷贝。
	*out = *in
	return nil
}

// +k8s:conversion-fn=copy-only
//
// Convert_v1_MicroTime_To_v1_MicroTime 同样因为 time.Time 的未导出字段只能浅拷贝。
func Convert_v1_MicroTime_To_v1_MicroTime(in *MicroTime, out *MicroTime, s conversion.Scope) error {
	// time.Time 包含未导出字段，无法在这里逐字段深拷贝，因此采用结构体浅拷贝。
	*out = *in
	return nil
}

// Convert_Pointer_v1_Duration_To_v1_Duration 把可选 Duration 转为值；nil 折叠为零时长。
func Convert_Pointer_v1_Duration_To_v1_Duration(in **Duration, out *Duration, s conversion.Scope) error {
	if *in == nil {
		*out = Duration{} // 零时长。
		return nil
	}
	*out = **in // 复制。
	return nil
}

// Convert_v1_Duration_To_Pointer_v1_Duration 把值转为新分配的指针。
func Convert_v1_Duration_To_Pointer_v1_Duration(in *Duration, out **Duration, s conversion.Scope) error {
	temp := *in //copy
	*out = &temp
	return nil
}

// Convert_Slice_string_To_v1_Time 将 URL 查询参数中的字符串转换为 metav1.Time。
//
// 查询参数以 []string 承载，这里只取第一个值；空切片按空串解析。
func Convert_Slice_string_To_v1_Time(in *[]string, out *Time, s conversion.Scope) error {
	str := ""
	if len(*in) > 0 {
		str = (*in)[0]
	}
	return out.UnmarshalQueryParameter(str)
}

// Convert_Slice_string_To_Pointer_v1_Time 是上面函数返回指针的版本；
// 解析失败时直接把错误上抛，由调用方决定如何响应。
func Convert_Slice_string_To_Pointer_v1_Time(in *[]string, out **Time, s conversion.Scope) error {
	if in == nil {
		return nil
	}
	str := ""
	if len(*in) > 0 {
		str = (*in)[0]
	}
	temp := Time{}
	if err := temp.UnmarshalQueryParameter(str); err != nil {
		return err
	}
	*out = &temp
	return nil
}

// Convert_string_To_labels_Selector 把查询参数中的 selector 字符串解析为 labels.Selector；
// 语法错误直接返回，避免把非法 selector 带入后续过滤逻辑。
func Convert_string_To_labels_Selector(in *string, out *labels.Selector, s conversion.Scope) error {
	selector, err := labels.Parse(*in)
	if err != nil {
		return err
	}
	*out = selector
	return nil
}

// Convert_string_To_fields_Selector 与 labels 版本对应，解析字段选择器。
func Convert_string_To_fields_Selector(in *string, out *fields.Selector, s conversion.Scope) error {
	selector, err := fields.ParseSelector(*in)
	if err != nil {
		return err
	}
	*out = selector
	return nil
}

// Convert_labels_Selector_To_string 把 selector 序列化回字符串。
func Convert_labels_Selector_To_string(in *labels.Selector, out *string, s conversion.Scope) error {
	if *in == nil {
		return nil
	}
	*out = (*in).String()
	return nil
}

// Convert_fields_Selector_To_string 把字段 selector 序列化回字符串。
func Convert_fields_Selector_To_string(in *fields.Selector, out *string, s conversion.Scope) error {
	if *in == nil {
		return nil
	}
	*out = (*in).String()
	return nil
}

// +k8s:conversion-fn=copy-only
//
// Convert_resource_Quantity_To_resource_Quantity 纯拷贝；Quantity 内部缓存不可逐字段复制。
func Convert_resource_Quantity_To_resource_Quantity(in *resource.Quantity, out *resource.Quantity, s conversion.Scope) error {
	*out = *in
	return nil
}

// Convert_Map_string_To_string_To_v1_LabelSelector 把简单的 key=value map 转为 LabelSelector；
// 每个键值对作为一条 MatchLabels 条件追加，已有条件会保留。
func Convert_Map_string_To_string_To_v1_LabelSelector(in *map[string]string, out *LabelSelector, s conversion.Scope) error {
	if in == nil {
		return nil
	}
	for labelKey, labelValue := range *in {
		AddLabelToSelector(out, labelKey, labelValue)
	}
	return nil
}

// Convert_v1_LabelSelector_To_Map_string_To_string 是反向转换；
// 当 selector 表达式无法表示为简单 map 时由 LabelSelectorAsMap 返回错误。
func Convert_v1_LabelSelector_To_Map_string_To_string(in *LabelSelector, out *map[string]string, s conversion.Scope) error {
	var err error
	*out, err = LabelSelectorAsMap(in)
	return err
}

// 该转换用于端口转发场景，目标端口类型为 int32。
//
// 端口号上限为 16 位（0-65535），因此用 ParseUint(..., 16) 而非 32；
// 负数或越界都会返回错误，而不是截断。
func Convert_Slice_string_To_Slice_int32(in *[]string, out *[]int32, s conversion.Scope) error {
	for _, s := range *in {
		for _, v := range strings.Split(s, ",") {
			x, err := strconv.ParseUint(v, 10, 16)
			if err != nil {
				return fmt.Errorf("cannot convert to []int32: %v", err)
			}
			*out = append(*out, int32(x))
		}
	}
	return nil
}

// 即使参数缺失也返回非 nil 指针，保证调用方拿到明确的"空策略"而不是 nil。
func Convert_Slice_string_To_Pointer_v1_DeletionPropagation(in *[]string, out **DeletionPropagation, s conversion.Scope) error {
	var str string
	if len(*in) > 0 {
		str = (*in)[0]
	} else {
		str = ""
	}
	temp := DeletionPropagation(str)
	*out = &temp
	return nil
}

// 参数缺失时保持 out 的零值不变，避免覆盖调用方预设的默认策略。
func Convert_Slice_string_To_v1_IncludeObjectPolicy(in *[]string, out *IncludeObjectPolicy, s conversion.Scope) error {
	if len(*in) > 0 {
		*out = IncludeObjectPolicy((*in)[0])
	}
	return nil
}

// 先让自动生成的转换处理通用字段，再单独拼装 Preconditions：
// uid/resourceVersion 在 query 中是平铺的，而在结构体里嵌套于 Preconditions。
func Convert_url_Values_To_v1_DeleteOptions(in *url.Values, out *DeleteOptions, s conversion.Scope) error {
	if err := autoConvert_url_Values_To_v1_DeleteOptions(in, out, s); err != nil {
		return err
	}

	uid := types.UID("")
	if values, ok := (*in)["uid"]; ok && len(values) > 0 {
		uid = types.UID(values[0])
	}

	resourceVersion := ""
	if values, ok := (*in)["resourceVersion"]; ok && len(values) > 0 {
		resourceVersion = values[0]
	}

	// 只有确实提供了值才创建 Preconditions，避免无意义地生成空对象。
	if len(uid) > 0 || len(resourceVersion) > 0 {
		if out.Preconditions == nil {
			out.Preconditions = &Preconditions{}
		}
		if len(uid) > 0 {
			out.Preconditions.UID = &uid
		}
		if len(resourceVersion) > 0 {
			out.Preconditions.ResourceVersion = &resourceVersion
		}
	}
	return nil
}

// 参数缺失时保持零值（即未设置），合法性交由 ValidateListOptions 判断。
func Convert_Slice_string_To_v1_ResourceVersionMatch(in *[]string, out *ResourceVersionMatch, s conversion.Scope) error {
	if len(*in) > 0 {
		*out = ResourceVersionMatch((*in)[0])
	}
	return nil
}
