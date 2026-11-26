/*
Copyright 2023 The Kubernetes Authors.

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

package openapi3

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/openapi"
	"k8s.io/kube-openapi/pkg/spec3"
)

// Root 定义针对 OpenAPI V3 根文档的操作。根文档是一个索引：把每个
// GroupVersion 的 API Server 相对路径映射到其 OpenAPI 文档的相对路径。
// 单 GroupVersion（如 apps/v1）的示例：
//
//	"apis/apps/v1": {
//	    "ServerRelativeURL": "/openapi/v3/apis/apps/v1?hash=<HASH>"
//	}
//
// Root 封装 OpenAPI V3 根文档。根文档只是索引：它给出每个 GroupVersion
// 对应的文档 URL，具体 schema 需要再按该 URL 单独拉取。拆分为多文档是为了
// 支持按 GroupVersion 缓存与按需加载。
type Root interface {
	// GroupVersions 返回所有存在 OpenAPI V3 文档的 GroupVersion；获取或解析根
	// 文档失败时返回错误。
	//
	// 返回的列表按 GroupVersion 字符串排序，便于结果稳定可比较。
	GroupVersions() ([]schema.GroupVersion, error)
	// GVSpec 以 *spec3.OpenAPI 指针返回某 GroupVersion 下所有资源的规范；
	// 获取或解析根文档或该 GV 文档失败时返回错误。
	GVSpec(gv schema.GroupVersion) (*spec3.OpenAPI, error)
	// GVSpecAsMap 以非结构化字节返回某 GroupVersion 下所有资源的规范；获取或解析
	// 根文档或该 GV 文档失败时返回错误。
	//
	// 用于调用方不想绑定 spec3 类型、希望自行遍历或做部分读取的场景。
	GVSpecAsMap(gv schema.GroupVersion) (map[string]interface{}, error)
}

// root 实现 Root 接口，封装获取与解析 OpenAPI V3 根文档所需的字段。
// 用于获取、存储解析后 OpenAPI V3 根文档的字段。
type root struct {
	// 用于获取 OpenAPI V3 文档的 OpenAPI 客户端。
	client openapi.Client
}

// 编译期断言：root 实现 Root 接口。
var _ Root = &root{}

// NewRoot 用传入的 rest 客户端创建实现 Root 接口的结构。
func NewRoot(client openapi.Client) Root {
	return &root{client: client}
}

// GroupVersions 从根文档的路径集合推导出所有可用的 GroupVersion。
func (r *root) GroupVersions() ([]schema.GroupVersion, error) {
	paths, err := r.client.Paths()
	if err != nil {
		return nil, err
	}
	// 无法解析为 GroupVersion 的路径直接忽略，例如不带版本号的路径。
	gvs := make([]schema.GroupVersion, 0, len(paths))
	for gvAPIPath := range paths {
		gv, err := pathToGroupVersion(gvAPIPath)
		if err != nil {
			// 忽略无法解析为 GroupVersion 的路径。
			// 根文档可能包含非 GroupVersion 形式的路径，直接跳过而非整体失败。
			continue
		}
		gvs = append(gvs, gv)
	}
	// 按字母序排列 GroupVersion，保证结果稳定可测。
	sort.Slice(gvs, func(i, j int) bool {
		return gvs[i].String() < gvs[j].String()
	})
	return gvs, nil
}

// GVSpec 拉取并解析指定 GroupVersion 的 OpenAPI 文档。
func (r *root) GVSpec(gv schema.GroupVersion) (*spec3.OpenAPI, error) {
	openAPISchemaBytes, err := r.retrieveGVBytes(gv)
	if err != nil {
		return nil, err
	}
	// 把下载得到的 Group/Version 字节解析到 spec3.OpenAPI 结构体。
	var parsedV3Schema spec3.OpenAPI
	err = json.Unmarshal(openAPISchemaBytes, &parsedV3Schema)
	return &parsedV3Schema, err
}

// GVSpecAsMap 拉取指定 GroupVersion 的 OpenAPI 文档并解析为通用 map。
func (r *root) GVSpecAsMap(gv schema.GroupVersion) (map[string]interface{}, error) {
	gvOpenAPIBytes, err := r.retrieveGVBytes(gv)
	if err != nil {
		return nil, err
	}
	// 把下载得到的 Group/Version 字节解析为非结构化 map[string]interface{}。
	var gvMap map[string]interface{}
	err = json.Unmarshal(gvOpenAPIBytes, &gvMap)
	return gvMap, err
}

// retrieveGVBytes 以非结构化字节返回指定 GroupVersion 的 schema；下载失败或
// 该 GV 不被支持时返回错误。
//
// 每次调用都会重新获取根文档路径，因此该方法不缓存解析结果；
// 缓存策略交由 openapi.Client 的实现负责。
func (r *root) retrieveGVBytes(gv schema.GroupVersion) ([]byte, error) {
	paths, err := r.client.Paths()
	if err != nil {
		return nil, err
	}
	apiPath := gvToAPIPath(gv)
	gvOpenAPI, found := paths[apiPath]
	if !found {
		return nil, &GroupVersionNotFoundError{gv: gv}
	}
	return gvOpenAPI.Schema(runtime.ContentTypeJSON)
}

// gvToAPIPath 把 GroupVersion 映射为 API Server 的相对 URL，例如：
//
//	GroupVersion{Group: "apps", Version: "v1"} -> "apis/apps/v1".
//
// 注意 core 组（Group 为空）使用 "api/<version>" 而非 "apis//<version>"。
func gvToAPIPath(gv schema.GroupVersion) string {
	var resourcePath string
	if len(gv.Group) == 0 {
		resourcePath = fmt.Sprintf("api/%s", gv.Version)
	} else {
		resourcePath = fmt.Sprintf("apis/%s/%s", gv.Group, gv.Version)
	}
	return resourcePath
}

// pathToGroupVersion 是辅助函数，把传入的相对 URL 解析为 GroupVersion。
//
//	Example: apis/apps/v1 -> GroupVersion{Group: "apps", Version: "v1"}
//	Example: api/v1 -> GroupVersion{Group: "", Version: "v1"}
func pathToGroupVersion(path string) (schema.GroupVersion, error) {
	var gv schema.GroupVersion
	parts := strings.Split(path, "/")
	if len(parts) < 2 {
		return gv, fmt.Errorf("Unable to parse api relative path: %s", path)
	}
	apiPrefix := parts[0]
	if apiPrefix == "apis" {
		// 例如：apis/apps（不带版本号）。
		if len(parts) < 3 {
			return gv, fmt.Errorf("Group without Version not allowed")
		}
		gv.Group = parts[1]
		gv.Version = parts[2]
	} else if apiPrefix == "api" {
		gv.Version = parts[1]
	} else {
		return gv, fmt.Errorf("Unable to parse api relative path: %s", path)
	}
	return gv, nil
}

// 把 GroupVersion 未出现在 OpenAPI V3 端点路径中封装为错误。
// 出现在 OpenAPI V3 端点路径中。
//
// GroupVersionNotFoundError 表示请求的 GroupVersion 未出现在根文档中，
// 通常意味着服务器未开启该 API 组或版本已被移除。
type GroupVersionNotFoundError struct {
	gv schema.GroupVersion
}

// Error 实现 error 接口。
func (r *GroupVersionNotFoundError) Error() string {
	return fmt.Sprintf("GroupVersion (%v) not found as OpenAPI V3 path", r.gv)
}
