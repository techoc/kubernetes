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

// Package storage 提供将 Kubernetes 资源对象转换为 metav1.Table 的功能。
// 主要用于 API Server 的 table subresource（如 `kubectl get --output=custom-columns`），
// 让服务器端直接返回表格数据而非客户端转换。
package storage

import (
	"context"
	"fmt"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/kubernetes/pkg/printers"
)

// TableConvertor 将 runtime 对象转换为 metav1.Table，内部使用 printers.TableGenerator。
// 嵌入 TableGenerator 接口，复用其表格生成逻辑。
type TableConvertor struct {
	printers.TableGenerator
}

// ConvertToTable 将给定对象转换为 metav1.Table。
// 仅从 *metav1.TableOptions 读取 NoHeaders（nil 按 false 处理），其他选项类型直接报错；
// 服务端表格转换始终启用 Wide，以保留注册的全部列。
func (c TableConvertor) ConvertToTable(ctx context.Context, obj runtime.Object, tableOptions runtime.Object) (*metav1.Table, error) {
	noHeaders := false
	if tableOptions != nil {
		switch t := tableOptions.(type) {
		case *metav1.TableOptions:
			if t != nil {
				noHeaders = t.NoHeaders
			}
		default:
			return nil, fmt.Errorf("unrecognized type %T for table options, can't display tabular output", tableOptions)
		}
	}
	return c.TableGenerator.GenerateTable(obj, printers.GenerateOptions{Wide: true, NoHeaders: noHeaders})
}
