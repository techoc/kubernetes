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

// Package table 提供把任意对象或列表转换为 metav1.TableRow 的辅助函数，
// 供服务端在返回 Table 响应时复用统一的名称/年龄列。
package table

import (
	"time"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/duration"
)

// MetaToTableRow converts a list or object into one or more table rows. The provided rowFn is invoked for
// each accessed item, with name and age being passed to each.
//
// 对列表递归展开（列表项本身也可能是列表），从而兼容 meta.EachListItem
// 支持的嵌套结构；对单个对象则通过 meta.Accessor 取元数据。
// 返回的每行都携带原始对象，便于调用方在 Table 响应中做 selfLink/object 引用。
func MetaToTableRow(obj runtime.Object, rowFn func(obj runtime.Object, m metav1.Object, name, age string) ([]interface{}, error)) ([]metav1.TableRow, error) {
	// 列表类型不能直接取 Accessor，必须逐项展开；预分配容量只是减少扩容次数。
	if meta.IsListType(obj) {
		rows := make([]metav1.TableRow, 0, 16)
		err := meta.EachListItem(obj, func(obj runtime.Object) error {
			nestedRows, err := MetaToTableRow(obj, rowFn)
			if err != nil {
				return err
			}
			rows = append(rows, nestedRows...)
			return nil
		})
		if err != nil {
			return nil, err
		}
		return rows, nil
	}

	// 单个对象：先取元数据，名称和年龄由公共逻辑计算，其余单元格交给 rowFn。
	rows := make([]metav1.TableRow, 0, 1)
	m, err := meta.Accessor(obj)
	if err != nil {
		return nil, err
	}
	row := metav1.TableRow{
		// 保留对象本身，Table 序列化时会转成 object 字段。
		Object: runtime.RawExtension{Object: obj},
	}
	row.Cells, err = rowFn(obj, m, m.GetName(), ConvertToHumanReadableDateType(m.GetCreationTimestamp()))
	if err != nil {
		return nil, err
	}
	rows = append(rows, row)
	return rows, nil
}

// ConvertToHumanReadableDateType returns the elapsed time since timestamp in
// human-readable approximation.
//
// 零值时间不参与 time.Since 计算，避免把 1970 年当作创建时间而输出巨大的年龄。
func ConvertToHumanReadableDateType(timestamp metav1.Time) string {
	if timestamp.IsZero() {
		return "<unknown>"
	}
	return duration.HumanDuration(time.Since(timestamp.Time))
}
