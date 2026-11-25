/*
Copyright 2021 The Kubernetes Authors.

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

package aggregator

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/emicklei/go-restful/v3"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apiserver/pkg/endpoints/metrics"
	"k8s.io/apiserver/pkg/server"
	"k8s.io/apiserver/pkg/server/mux"
	"k8s.io/apiserver/pkg/server/routes"
	"k8s.io/klog/v2"
	v1 "k8s.io/kube-aggregator/pkg/apis/apiregistration/v1"
	"k8s.io/kube-openapi/pkg/common"
	"k8s.io/kube-openapi/pkg/handler3"
	"k8s.io/kube-openapi/pkg/openapiconv"

	v2aggregator "k8s.io/kube-aggregator/pkg/controllers/openapi/aggregator"
)

// 更新一个从未注册过的 APIService 属于调用方错误，而不是"稍后重试就好"的瞬态故障。
// 用哨兵错误单独标识这种情况，调用方就能把它和真正的下载失败区分开。
var ErrAPIServiceNotFound = errors.New("resource not found")

// SpecProxier 将 OpenAPI V3 请求代理到各自对应的 APIService。
// 这个接口把“按 APIService 管理文档”和“对外提供 HTTP 服务”解耦：
// 控制器只依赖这组方法，不需要知道内部是 map 还是别的存储。
// 更新方法带 error 而删除方法不带，是因为更新要真正去后端拉文档，
// 可能失败；删除只是本地状态变更，没有可失败的步骤。
type SpecProxier interface {
	AddUpdateAPIService(handler http.Handler, apiService *v1.APIService)
	// UpdateAPIServiceSpec updates the APIService. It returns ErrAPIServiceNotFound if the APIService doesn't exist.
	UpdateAPIServiceSpec(apiServiceName string) error
	RemoveAPIServiceSpec(apiServiceName string)
	GetAPIServiceNames() []string
}

// 本地委托链里的 handler 没有对应的 APIService 对象，只能凭空造一个名字。
// 用专用前缀是为了让它们能和真实的 APIService 区分开：
// 控制器据此知道这些名字不该拿去查 APIService API。
// 后面用递增编号区分同一条链上的不同层，补零只是让这些内部名字看起来整齐。
const (
	aggregatorUser                = "system:aggregator"
	specDownloadTimeout           = 60 * time.Second
	localDelegateChainNamePrefix  = "k8s_internal_local_delegation_chain_"
	localDelegateChainNamePattern = localDelegateChainNamePrefix + "%010d"
	openAPIV2Converter            = "openapiv2converter"
)

// IsLocalAPIService returns true for local specs from delegates.
func IsLocalAPIService(apiServiceName string) bool {
	return strings.HasPrefix(apiServiceName, localDelegateChainNamePrefix)
}

// GetAPIServiceNames returns the names of APIServices recorded in apiServiceInfo.
// We use this function to pass the names of local APIServices to the controller in this package,
// so that the controller can periodically sync the OpenAPI spec from delegation API servers.
func (s *specProxier) GetAPIServiceNames() []string {
	s.rwMutex.RLock()
	defer s.rwMutex.RUnlock()

	names := make([]string, 0, len(s.apiServiceInfo))
	for key := range s.apiServiceInfo {
		names = append(names, key)
	}
	return names
}

// BuildAndRegisterAggregator 注册 OpenAPI 聚合器 handler；该函数不是线程安全的，
// 因为它只在启动阶段被调用。
// 启动时要把三类来源塞进同一张表：聚合器自身的类型、委托链上的各层 handler、
// 以及负责把 V2 文档转成 V3 的兜底服务。它们都不是来自 APIService API，
// 所以这里用内部名字伪装成 APIService，后续就能复用同一套同步与聚合逻辑。
func BuildAndRegisterAggregator(downloader Downloader, delegationTarget server.DelegationTarget, aggregatorService *restful.Container, openAPIConfig *common.OpenAPIV3Config, pathHandler common.PathHandlerByGroupVersion) (SpecProxier, error) {
	s := &specProxier{
		apiServiceInfo: map[string]*openAPIV3APIServiceInfo{},
		downloader:     downloader,
	}

	if aggregatorService != nil && openAPIConfig != nil {
		// 让 aggregator 暴露的 native 类型对聚合 OpenAPI 可见
		// （常规 handler 已被 skipOpenAPIInstallation 选项关闭）。
		// 通用 server 的默认 OpenAPI handler 被关掉了，这里单独装一个 V3 路由，
		// 目的只是把自己的类型文档收进来，而不是对外提供服务。
		aggregatorLocalServiceName := "k8s_internal_local_kube_aggregator_types"
		v3Mux := mux.NewPathRecorderMux(aggregatorLocalServiceName)
		_ = routes.OpenAPI{
			V3Config: openAPIConfig,
		}.InstallV3(aggregatorService, v3Mux)

		s.AddUpdateAPIService(v3Mux, &v1.APIService{
			ObjectMeta: metav1.ObjectMeta{
				Name: aggregatorLocalServiceName,
			},
		})
		s.UpdateAPIServiceSpec(aggregatorLocalServiceName)
	}

	// 委托链是一条链表，从当前层一路往下走；每层 handler 可能提供不同的
	// group/version，必须逐层注册，不能只取最内层，否则聚合文档会缺东西。
	i := 1
	for delegate := delegationTarget; delegate != nil; delegate = delegate.NextDelegate() {
		handler := delegate.UnprotectedHandler()
		if handler == nil {
			continue
		}

		apiServiceName := fmt.Sprintf(localDelegateChainNamePattern, i)
		localAPIService := v1.APIService{}
		localAPIService.Name = apiServiceName
		s.AddUpdateAPIService(handler, &localAPIService)
		s.UpdateAPIServiceSpec(apiServiceName)
		i++
	}

	handler := handler3.NewOpenAPIService()
	s.openAPIV2ConverterHandler = handler
	openAPIV2ConverterMux := mux.NewPathRecorderMux(openAPIV2Converter)
	s.openAPIV2ConverterHandler.RegisterOpenAPIV3VersionedService("/openapi/v3", openAPIV2ConverterMux)
	openAPIV2ConverterAPIService := v1.APIService{}
	openAPIV2ConverterAPIService.Name = openAPIV2Converter
	s.AddUpdateAPIService(openAPIV2ConverterMux, &openAPIV2ConverterAPIService)
	s.register(pathHandler)

	return s, nil
}

// AddUpdateAPIService adds or updates the api service. It is thread safe.
func (s *specProxier) AddUpdateAPIService(handler http.Handler, apiservice *v1.APIService) {
	s.rwMutex.Lock()
	defer s.rwMutex.Unlock()
	// 若 APIService 正在被更新，则复用已有的结构体。
	// 保留同一个结构体指针是有意为之：上面缓存的 etag 等状态要留着，
	// 下次同步才能带上 If-None-Match 走 304 分支，不必重新下载整份文档。
	if apiServiceInfo, ok := s.apiServiceInfo[apiservice.Name]; ok {
		apiServiceInfo.apiService = *apiservice
		apiServiceInfo.handler = handler
		return
	}
	s.apiServiceInfo[apiservice.Name] = &openAPIV3APIServiceInfo{
		apiService: *apiservice,
		handler:    handler,
	}
}

// V3 文档的路径形如 apis/<group>/<version>，而 APIService 里 Group 和 Version
// 是分开存的；这个函数把两者拼成 V3 发现文档使用的 key。
// 两个字段都为空时返回空串，对应核心组的特殊路径。
func getGroupVersionStringFromAPIService(apiService v1.APIService) string {
	if apiService.Spec.Group == "" && apiService.Spec.Version == "" {
		return ""
	}
	return "apis/" + apiService.Spec.Group + "/" + apiService.Spec.Version
}

// UpdateAPIServiceSpec updates all the OpenAPI v3 specs that the APIService serves.
// It is thread safe.
func (s *specProxier) UpdateAPIServiceSpec(apiServiceName string) error {
	s.rwMutex.Lock()
	defer s.rwMutex.Unlock()
	return s.updateAPIServiceSpecLocked(apiServiceName)
}

// 同步分两条路：优先拿 V3 发现文档，拿不到（404）才退回 V2 下载再转换。
// 一旦发现文档存在就记住这个事实，后续同步不再重复探测；
// 反过来，一旦判定为旧后端就一直走 V2，直到该 APIService 被重新注册。
// 调用方必须持有写锁：这里会改写 etag、discovery 和转换器状态。
func (s *specProxier) updateAPIServiceSpecLocked(apiServiceName string) error {
	apiService, exists := s.apiServiceInfo[apiServiceName]
	if !exists {
		return ErrAPIServiceNotFound
	}

	if !apiService.isLegacyAPIService {
		gv, httpStatus, err := s.downloader.OpenAPIV3Root(apiService.handler)
		if err != nil {
			return err
		}
		// 404 说明后端只支持 V2 文档，这不是错误，而是切换到兼容路径的信号。
		if httpStatus == http.StatusNotFound {
			apiService.isLegacyAPIService = true
		} else {
			s.apiServiceInfo[apiServiceName].discovery = gv
			return nil
		}
	}

	newDownloader := v2aggregator.Downloader{}
	v2Spec, etag, httpStatus, err := newDownloader.Download(apiService.handler, apiService.etag)
	if err != nil {
		return err
	}
	apiService.etag = etag
	// 304 表示后端文档没变，此时 etag 已刷新但内容无需重算；
	// 只有真正返回 200 才做有损的 V2→V3 转换，并顺带刷新转换器自己的发现文档——
	// 因为客户端拿到的 V3 文档最终是由那个 handler 提供的。
	if httpStatus == http.StatusOK {
		v3Spec := openapiconv.ConvertV2ToV3(v2Spec)
		s.openAPIV2ConverterHandler.UpdateGroupVersion(getGroupVersionStringFromAPIService(apiService.apiService), v3Spec)
		s.updateAPIServiceSpecLocked(openAPIV2Converter)
	}
	return nil
}

type specProxier struct {
	// mutex protects all members of this struct.
	rwMutex sync.RWMutex

	// OpenAPI V3 specs by APIService name
	apiServiceInfo map[string]*openAPIV3APIServiceInfo

	// For downloading the OpenAPI v3 specs from apiservices
	downloader Downloader

	openAPIV2ConverterHandler *handler3.OpenAPIService
}

var _ SpecProxier = &specProxier{}

type openAPIV3APIServiceInfo struct {
	apiService v1.APIService
	handler    http.Handler
	discovery  *handler3.OpenAPIV3Discovery

	// 以下字段仅在 /openapi/v3 端点不是由某个 APIService 提供时使用。
	// Legacy APIService 表示该 APIService 不支持 OpenAPI V3，
	// 聚合器会下载它的 OpenAPI V2 文档、（有损地）转换为 V3 后对外提供。
	// etag 记录“上次拿到的文档版本”：带上它后端才能用 304 回答“没变化”，
	// 省掉每次同步都重新下载整份 V2 文档的开销。
	etag               string
	isLegacyAPIService bool
}

// RemoveAPIServiceSpec removes an api service from the OpenAPI map. If it does not exist, no error is returned.
// It is thread safe.
func (s *specProxier) RemoveAPIServiceSpec(apiServiceName string) {
	s.rwMutex.Lock()
	defer s.rwMutex.Unlock()
	if apiServiceInfo, ok := s.apiServiceInfo[apiServiceName]; ok {
		// 如果这个 APIService 的文档是由 V2 转换来的，还得把它从转换器里删掉，
		// 否则转换器会继续对外提供一份已经下线的 group/version 文档。
		s.openAPIV2ConverterHandler.DeleteGroupVersion(getGroupVersionStringFromAPIService(apiServiceInfo.apiService))
		_ = s.updateAPIServiceSpecLocked(openAPIV2Converter)
		delete(s.apiServiceInfo, apiServiceName)
	}
}

// 根发现文档是各 APIService 发现结果的并集。这里按 key 直接覆盖，
// 同名 group/version 冲突时谁赢取决于 map 遍历顺序；正常情况下
// 同一个 GroupVersion 只有一个权威 APIService，所以没做额外仲裁。
func (s *specProxier) getOpenAPIV3Root() handler3.OpenAPIV3Discovery {
	s.rwMutex.RLock()
	defer s.rwMutex.RUnlock()

	merged := handler3.OpenAPIV3Discovery{
		Paths: make(map[string]handler3.OpenAPIV3DiscoveryGroupVersion),
	}

	for _, apiServiceInfo := range s.apiServiceInfo {
		if apiServiceInfo.discovery == nil {
			continue
		}

		for key, item := range apiServiceInfo.discovery.Paths {
			merged.Paths[key] = item
		}
	}
	return merged
}

// handleDiscovery 是 OpenAPI V3 Discovery 的 handler。
// 用 ServeContent 而不是直接 Write，是为了让它顺手处理条件请求：
// 客户端带上 If-Modified-Since 时有机会拿到 304，不必重复下载整份发现文档。
func (s *specProxier) handleDiscovery(w http.ResponseWriter, r *http.Request) {
	merged := s.getOpenAPIV3Root()
	j, err := json.Marshal(&merged)
	if err != nil {
		w.WriteHeader(500)
		klog.Errorf("failed to created merged OpenAPIv3 discovery response: %s", err.Error())
		return
	}

	http.ServeContent(w, r, "/openapi/v3", time.Now(), bytes.NewReader(j))
}

// handleGroupVersion 是特定 group/version 的 OpenAPI V3 handler。
// 这里不自行构造文档，而是把请求直接转给对应 APIService 的 handler：
// 具体 schema 只有后端最清楚，聚合器只负责路由。
// 找到匹配就立刻返回，没找到才落到 404。
func (s *specProxier) handleGroupVersion(w http.ResponseWriter, r *http.Request) {
	s.rwMutex.RLock()
	defer s.rwMutex.RUnlock()

	// TODO: Import this logic from kube-openapi instead of duplicating
	// URLs for OpenAPI V3 have the format /openapi/v3/<groupversionpath>
	// SplitAfterN with 4 yields ["", "openapi", "v3", <groupversionpath>]
	url := strings.SplitAfterN(r.URL.Path, "/", 4)
	targetGV := url[3]

	for _, apiServiceInfo := range s.apiServiceInfo {
		if apiServiceInfo.discovery == nil {
			continue
		}

		for key := range apiServiceInfo.discovery.Paths {
			if targetGV == key {
				apiServiceInfo.handler.ServeHTTP(w, r)
				return
			}
		}
	}
	// No group-versions match the desired request
	w.WriteHeader(404)
}

// Register 注册 OpenAPI V3 Discovery 与 GroupVersion 的 handler。
// 精确路径挂发现文档，前缀路径挂具体 group/version 文档。
// 两个 handler 都套了 metrics 包装，调用方拿到的是同一个可观测的路由入口。
func (s *specProxier) register(handler common.PathHandlerByGroupVersion) {
	handler.Handle("/openapi/v3", metrics.InstrumentHandlerFunc("GET",
		/* group = */ "",
		/* version = */ "",
		/* resource = */ "",
		/* subresource = */ "openapi/v3",
		/* scope = */ "",
		/* component = */ "",
		/* deprecated */ false,
		/* removedRelease */ "",
		http.HandlerFunc(s.handleDiscovery)))
	handler.HandlePrefix("/openapi/v3/", metrics.InstrumentHandlerFunc("GET",
		/* group = */ "",
		/* version = */ "",
		/* resource = */ "",
		/* subresource = */ "openapi/v3/",
		/* scope = */ "",
		/* component = */ "",
		/* deprecated */ false,
		/* removedRelease */ "",
		http.HandlerFunc(s.handleGroupVersion)))
}
