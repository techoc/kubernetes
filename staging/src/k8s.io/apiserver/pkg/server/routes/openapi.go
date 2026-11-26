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

package routes

import (
	"strings"

	restful "github.com/emicklei/go-restful/v3"
	"k8s.io/klog/v2"

	"k8s.io/apiserver/pkg/server/mux"
	builder2 "k8s.io/kube-openapi/pkg/builder"
	"k8s.io/kube-openapi/pkg/builder3"
	"k8s.io/kube-openapi/pkg/common"
	"k8s.io/kube-openapi/pkg/common/restfuladapter"
	"k8s.io/kube-openapi/pkg/handler"
	"k8s.io/kube-openapi/pkg/handler3"
	"k8s.io/kube-openapi/pkg/spec3"
	"k8s.io/kube-openapi/pkg/validation/spec"
)

// OpenAPI 为每个 web service 安装 spec 端点。
// v2 与 v3 共用同一份配置（Config/V3Config），但构建器不同。
// 把配置放在结构体上而非函数参数，是为了让 server 在装配阶段一次性决定
// 命名策略、安全定义等，之后所有版本保持一致。
type OpenAPI struct {
	Config   *common.Config
	V3Config *common.OpenAPIV3Config
}

// InstallV2 把 SwaggerUI web service 添加到给定的 mux。
// v2 spec 在安装时一次性构建并注册。构建失败直接 Fatal：
// 没有可用 spec 的 apiserver 无法启动，与其带病运行不如立即暴露配置错误。
// PruneDefaults 去除与默认值重复的描述，显著减小 spec 体积。
func (oa OpenAPI) InstallV2(c *restful.Container, mux *mux.PathRecorderMux) (*handler.OpenAPIService, *spec.Swagger) {
	spec, err := builder2.BuildOpenAPISpecFromRoutes(restfuladapter.AdaptWebServices(c.RegisteredWebServices()), oa.Config)
	if err != nil {
		klog.Fatalf("Failed to build open api spec for root: %v", err)
	}
	spec.Definitions = handler.PruneDefaults(spec.Definitions)
	openAPIVersionedService := handler.NewOpenAPIService(spec)
	openAPIVersionedService.RegisterOpenAPIVersionedService("/openapi/v2", mux)

	return openAPIVersionedService, spec
}

// InstallV3 把 RegisteredWebServices 中定义的静态 group/version 加入 OpenAPI v3 spec。
// 这只覆盖通过 go-restful 提供的内置资源；CRD 与聚合 API 通过独立的代码路径
// 发布各自的 OpenAPI v3 spec。
// v3 以「每个 group/version 一个文档」的形式发布，客户端按 GV 拉取，
// 避免像 v2 那样一次性下载全量 spec。单组构建失败只记日志并跳过，
// 不阻断其余组的发布——部分可用优于整体不可用。
func (oa OpenAPI) InstallV3(c *restful.Container, mux *mux.PathRecorderMux) *handler3.OpenAPIService {
	openAPIVersionedService := handler3.NewOpenAPIService()
	err := openAPIVersionedService.RegisterOpenAPIV3VersionedService("/openapi/v3", mux)
	if err != nil {
		klog.Fatalf("Failed to register versioned open api spec for root: %v", err)
	}

	grouped := make(map[string][]*restful.WebService)

	for _, t := range c.RegisteredWebServices() {
		// 去掉名字中的 "/" 前缀。
		gvName := t.RootPath()[1:]
		grouped[gvName] = []*restful.WebService{t}
	}

	for gv, ws := range grouped {
		spec, err := builder3.BuildOpenAPISpecFromRoutes(restfuladapter.AdaptWebServices(ws), oa.V3Config)
		if err != nil {
			klog.Errorf("Failed to build OpenAPI v3 for group %s, %q", gv, err)
			continue
		}
		// 仅对能解析出 group/version 的内置路径做 GVK 过滤。
		if group, version, ok := groupVersionFromPath(gv); ok {
			filterScopedGVKs(spec, group, version)
		}
		openAPIVersionedService.UpdateGroupVersion(gv, spec)
	}
	return openAPIVersionedService
}

// groupVersionFromPath 从形如 "apis/apps/v1" 或 "api/v1" 的根路径中提取 API group 和 version。
// core 组（/api/v1）没有 group 段，返回空 group；这是 v3 spec 中
// 识别「core/v1」条目的依据，因此不能用 GroupVersion.String() 简单切分。
func groupVersionFromPath(path string) (group, version string, ok bool) {
	// "api/v1" → ("", "v1", true)
	// "apis/apps/v1" → ("apps", "v1", true)
	// "apis/networking.k8s.io/v1" → ("networking.k8s.io", "v1", true)
	parts := strings.SplitN(path, "/", 4)
	switch {
	case len(parts) < 2:
		return "", "", false
	case parts[0] == "api" && len(parts) == 2:
		return "", parts[1], true
	case parts[0] == "apis" && len(parts) == 3:
		return parts[1], parts[2], true
	default:
		return "", "", false
	}
}

// crossRegisteredKinds 列出 AddToGroupVersion（位于
// k8s.io/apimachinery/pkg/apis/meta/v1）注册进每个 API group 的 kind。
// 在按 group/version 划分的 v3 spec 中，只有这些类型会过滤其
// x-kubernetes-group-version-kind 列表。
// 这些是元类型而非真实资源，被注册进所有组属于实现细节；
// 白名单化可避免误伤真正跨组共享的类型（如 Scale）。
var crossRegisteredKinds = map[string]bool{
	"WatchEvent":    true,
	"DeleteOptions": true,
}

// filterScopedGVKs 收窄 AddToGroupVersion 交叉注册进每个 API group 的元类型
// （WatchEvent 与 DeleteOptions）上的 x-kubernetes-group-version-kind。
// 若不过滤，每份按 group/version 划分的 v3 spec 会为这些类型携带约 60 个 GVK 条目。
// 该过滤只保留与本 spec 的 group/version 匹配的条目，外加规范的 core/v1 条目。
//
// 只有 crossRegisteredKinds 中列出的元类型会被过滤。有意注册到特定组的其他类型
// （如 autoscaling/v1 的 Scale 注册进 apps/v1 和 core/v1）保持原样。
//
// 这只影响上面 InstallV3 生成的内置资源 spec。
// CRD 与聚合 API 不受影响，因为它们通过独立路径发布 spec。
// 这是体积优化，不是语义变更——过滤后每条目的 GVK 仍指向合法版本，
// 只是不再声称该类型同时属于所有组。保留 core/v1 条目是因为客户端常以它作为规范引用。
// 若过滤后为空（理论上不应发生），删除扩展并告警，避免输出非法空数组。
func filterScopedGVKs(s *spec3.OpenAPI, group, version string) {
	if s == nil || s.Components == nil {
		return
	}
	for _, schema := range s.Components.Schemas {
		if schema == nil {
			continue
		}
		ext, ok := schema.Extensions["x-kubernetes-group-version-kind"]
		if !ok {
			continue
		}
		gvks, ok := ext.([]interface{})
		if !ok || len(gvks) <= 1 {
			continue
		}
		if !isCrossRegisteredKind(gvks) {
			continue
		}
		var filtered []interface{}
		for _, item := range gvks {
			m, ok := item.(map[string]interface{})
			if !ok {
				continue
			}
			g, ok := m["group"].(string)
			if !ok {
				continue
			}
			v, ok := m["version"].(string)
			if !ok {
				continue
			}
			if (g == group && v == version) || (g == "" && v == "v1") {
				filtered = append(filtered, item)
			}
		}
		if len(filtered) > 0 {
			schema.Extensions["x-kubernetes-group-version-kind"] = filtered
		} else {
			klog.Warningf("Unexpected: filtering x-kubernetes-group-version-kind for %s/%s produced no matches", group, version)
			delete(schema.Extensions, "x-kubernetes-group-version-kind")
		}
	}
}

// isCrossRegisteredKind 通过检查首个条目的 kind，判断一个 GVK 列表是否代表
// crossRegisteredKinds 中的某个元类型。
// 同一 schema 的所有 GVK 条目共享同一个 kind，因此只看首项即可，
// 无需遍历；这依赖 openapi 生成器对同类型条目 kind 一致的约定。
func isCrossRegisteredKind(gvks []interface{}) bool {
	if len(gvks) == 0 {
		return false
	}
	m, ok := gvks[0].(map[string]interface{})
	if !ok {
		return false
	}
	kind, _ := m["kind"].(string)
	return crossRegisteredKinds[kind]
}
