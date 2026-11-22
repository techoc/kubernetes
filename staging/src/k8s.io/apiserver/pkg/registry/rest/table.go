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

package rest

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	genericapirequest "k8s.io/apiserver/pkg/endpoints/request"
)

// defaultTableConvertor 把任意带元数据的对象转成只有 Name/Created At 两列的
// 默认 Table；资源可以注册更丰富的转换器来覆盖它。
type defaultTableConvertor struct {
	defaultQualifiedResource schema.GroupResource
}

// NewDefaultTableConvertor creates a default convertor; the provided resource is used for error messages
// if no resource info can be determined from the context passed to ConvertToTable.
func NewDefaultTableConvertor(defaultQualifiedResource schema.GroupResource) TableConvertor {
	return defaultTableConvertor{defaultQualifiedResource: defaultQualifiedResource}
}

// swaggerMetadataDescriptions 复用 metav1 的字段描述，保证 Table 列说明与
// OpenAPI 文档一致；只计算一次，避免每次转换都重复生成。

var swaggerMetadataDescriptions = metav1.ObjectMeta{}.SwaggerDoc()

// ConvertToTable 把单个对象或列表转成 Table：列表逐项生成行，同时把
// resourceVersion/continue 等分页元数据带到 Table 上。列定义可被
// TableOptions.NoHeaders 抑制（客户端已缓存表头时）。
func (c defaultTableConvertor) ConvertToTable(ctx context.Context, object runtime.Object, tableOptions runtime.Object) (*metav1.Table, error) {
	var table metav1.Table
	// fn 为单个对象生成一行；无法取得元数据说明该资源不适合默认转换。
	fn := func(obj runtime.Object) error {
		m, err := meta.Accessor(obj)
		if err != nil {
			// 优先使用请求上下文中的 group/resource 生成更准确的错误信息，
			// 退化到构造时提供的默认资源。
			resource := c.defaultQualifiedResource
			if info, ok := genericapirequest.RequestInfoFrom(ctx); ok {
				resource = schema.GroupResource{Group: info.APIGroup, Resource: info.Resource}
			}
			return errNotAcceptable{resource: resource}
		}
		table.Rows = append(table.Rows, metav1.TableRow{
			Cells:  []interface{}{m.GetName(), m.GetCreationTimestamp().Time.UTC().Format(time.RFC3339)},
			Object: runtime.RawExtension{Object: obj},
		})
		return nil
	}
	// 列表走 EachListItem，避免为本包引入各资源列表类型的具体知识。
	switch {
	case meta.IsListType(object):
		if err := meta.EachListItem(object, fn); err != nil {
			return nil, err
		}
	default:
		if err := fn(object); err != nil {
			return nil, err
		}
	}
	// 列表对象提供分页信息；单对象只有 resourceVersion。
	if m, err := meta.ListAccessor(object); err == nil {
		table.ResourceVersion = m.GetResourceVersion()
		table.Continue = m.GetContinue()
		table.RemainingItemCount = m.GetRemainingItemCount()
	} else {
		if m, err := meta.CommonAccessor(object); err == nil {
			table.ResourceVersion = m.GetResourceVersion()
		}
	}
	if opt, ok := tableOptions.(*metav1.TableOptions); !ok || !opt.NoHeaders {
		table.ColumnDefinitions = []metav1.TableColumnDefinition{
			{Name: "Name", Type: "string", Format: "name", Description: swaggerMetadataDescriptions["name"]},
			{Name: "Created At", Type: "date", Description: swaggerMetadataDescriptions["creationTimestamp"]},
		}
	}
	return &table, nil
}

// errNotAcceptable indicates the resource doesn't support Table conversion
// errNotAcceptable 实现 APIStatus，使 handler 能返回规范化的 406 响应。
type errNotAcceptable struct {
	resource schema.GroupResource
}

// Error 返回给客户端的可读原因，包含资源名以便定位。
func (e errNotAcceptable) Error() string {
	return fmt.Sprintf("the resource %s does not support being converted to a Table", e.resource)
}

// Status 把错误转换为 metav1.Status，StatusReason 使用字符串而非预定义常量
// 以保持与历史响应兼容。
func (e errNotAcceptable) Status() metav1.Status {
	return metav1.Status{
		Status:  metav1.StatusFailure,
		Code:    http.StatusNotAcceptable,
		Reason:  metav1.StatusReason("NotAcceptable"),
		Message: e.Error(),
	}
}
