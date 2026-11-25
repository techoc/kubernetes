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

package explain

import (
	"fmt"
	"io"
	"strings"

	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/util/jsonpath"
	"k8s.io/kube-openapi/pkg/util/proto"
)

type fieldsPrinter interface {
	PrintFields(proto.Schema) error
}

// jsonPathParse 返回我们要处理的内部节点列表
func jsonPathParse(in string) ([]jsonpath.Node, error) {
	// 以防万一，移除末尾的句点
	in = strings.TrimSuffix(in, ".")

	// 定义初始的 jsonpath 解析器
	jpp, err := jsonpath.Parse("user", "{."+in+"}")
	if err != nil {
		return nil, err
	}

	// 由于 jsonpath 库的工作方式，解析器的结构为 [][]NodeList，
	// 这意味着我们需要先取得外层节点列表，确认其长度仅为 1，再取得内层节点列表，
	// 此后才能查看各个节点本身。
	outerNodeList := jpp.Root.Nodes
	if len(outerNodeList) != 1 {
		return nil, fmt.Errorf("must pass in 1 jsonpath string")
	}

	// 根节点始终是列表节点，因此此类型断言是安全的
	return outerNodeList[0].(*jsonpath.ListNode).Nodes, nil
}

// SplitAndParseResourceRequest 将用户输入拆分为模型和字段
func SplitAndParseResourceRequest(inResource string, mapper meta.RESTMapper) (schema.GroupVersionResource, []string, error) {
	inResourceNodeList, err := jsonPathParse(inResource)
	if err != nil {
		return schema.GroupVersionResource{}, nil, err
	}

	if inResourceNodeList[0].Type() != jsonpath.NodeField {
		return schema.GroupVersionResource{}, nil, fmt.Errorf("invalid jsonpath syntax, first node must be field node")
	}
	resource := inResourceNodeList[0].(*jsonpath.FieldNode).Value
	gvr, err := mapper.ResourceFor(schema.GroupVersionResource{Resource: resource})
	if err != nil {
		return schema.GroupVersionResource{}, nil, err
	}

	var fieldsPath []string
	for _, node := range inResourceNodeList[1:] {
		if node.Type() != jsonpath.NodeField {
			return schema.GroupVersionResource{}, nil, fmt.Errorf("invalid jsonpath syntax, all nodes must be field nodes")
		}
		fieldsPath = append(fieldsPath, node.(*jsonpath.FieldNode).Value)
	}

	return gvr, fieldsPath, nil
}

// SplitAndParseResourceRequestWithMatchingPrefix 将用户输入拆分为模型和字段，
// 同时选择 (resource, group) 前缀与该资源匹配的 gvr
func SplitAndParseResourceRequestWithMatchingPrefix(inResource string, mapper meta.RESTMapper) (gvr schema.GroupVersionResource, fieldsPath []string, err error) {
	inResourceNodeList, err := jsonPathParse(inResource)
	if err != nil {
		return schema.GroupVersionResource{}, nil, err
	}

	// 从 jsonpath 的第一个节点获取资源
	if inResourceNodeList[0].Type() != jsonpath.NodeField {
		return schema.GroupVersionResource{}, nil, fmt.Errorf("invalid jsonpath syntax, first node must be field node")
	}
	resource := inResourceNodeList[0].(*jsonpath.FieldNode).Value

	gvrs, err := mapper.ResourcesFor(schema.GroupVersionResource{Resource: resource})
	if err != nil {
		return schema.GroupVersionResource{}, nil, err
	}

	for _, gvrItem := range gvrs {
		// 查找第一个 gr 前缀与所请求资源匹配的 gvr
		groupResource := gvrItem.GroupResource().String()
		if strings.HasPrefix(inResource, groupResource) {
			resourceSuffix := inResource[len(groupResource):]
			var fieldsPath []string
			if len(resourceSuffix) > 0 {
				// 为资源后缀定义另一个 jsonpath 解析器
				resourceSuffixNodeList, err := jsonPathParse(resourceSuffix)
				if err != nil {
					return schema.GroupVersionResource{}, nil, err
				}

				if len(resourceSuffixNodeList) > 0 {
					nodeList := resourceSuffixNodeList[1:]
					for _, node := range nodeList {
						if node.Type() != jsonpath.NodeField {
							return schema.GroupVersionResource{}, nil, fmt.Errorf("invalid jsonpath syntax, first node must be field node")
						}
						fieldsPath = append(fieldsPath, node.(*jsonpath.FieldNode).Value)
					}
				}
			}
			return gvrItem, fieldsPath, nil
		}
	}

	// 如果没有匹配项，则取第一个（优先级最高的）gvr
	fieldsPath = []string{}
	if len(gvrs) > 0 {
		gvr = gvrs[0]

		fieldsPathNodeList, err := jsonPathParse(inResource)
		if err != nil {
			return schema.GroupVersionResource{}, nil, err
		}

		for _, node := range fieldsPathNodeList[1:] {
			if node.Type() != jsonpath.NodeField {
				return schema.GroupVersionResource{}, nil, fmt.Errorf("invalid jsonpath syntax, first node must be field node")
			}
			fieldsPath = append(fieldsPath, node.(*jsonpath.FieldNode).Value)
		}
	}

	return gvr, fieldsPath, nil
}

// PrintModelDescription 打印指定模型或点路径的描述。
// 如果 recursive 为真，嵌套在该 schema 字段中的所有组件都将被
// 打印。
func PrintModelDescription(fieldsPath []string, w io.Writer, schema proto.Schema, gvk schema.GroupVersionKind, recursive bool, maxDepth int) error {
	fieldName := ""
	if len(fieldsPath) != 0 {
		fieldName = fieldsPath[len(fieldsPath)-1]
	}

	// 沿 fieldsPath 向下查找我们要解释的内容
	schema, err := LookupSchemaForField(schema, fieldsPath)
	if err != nil {
		return err
	}
	b := fieldsPrinterBuilder{Recursive: recursive, MaxDepth: maxDepth}
	f := &Formatter{Writer: w, Wrap: 80}
	return PrintModel(fieldName, f, b, schema, gvk)
}
