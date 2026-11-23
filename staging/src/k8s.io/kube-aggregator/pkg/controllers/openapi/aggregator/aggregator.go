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

package aggregator

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"time"

	restful "github.com/emicklei/go-restful/v3"

	"k8s.io/apiserver/pkg/server"
	"k8s.io/klog/v2"
	v1 "k8s.io/kube-aggregator/pkg/apis/apiregistration/v1"
	"k8s.io/kube-openapi/pkg/aggregator"
	"k8s.io/kube-openapi/pkg/builder"
	"k8s.io/kube-openapi/pkg/cached"
	"k8s.io/kube-openapi/pkg/common"
	"k8s.io/kube-openapi/pkg/common/restfuladapter"
	"k8s.io/kube-openapi/pkg/handler"
	"k8s.io/kube-openapi/pkg/validation/spec"
)

// 单独定义哨兵错误，是为了让调用方能用 errors.Is 区分“不存在”与其他失败，
// 而不是去匹配错误消息文本。
var ErrAPIServiceNotFound = errors.New("resource not found")

// SpecAggregator calls out to http handlers of APIServices and merges specs. It keeps state of the last
// known specs including the http etag.
// 接口把“注册/更新/删除单个 APIService”与“对外提供合并后的 spec”解耦：
// 控制器负责前三者，OpenAPI handler 只消费合并结果，双方不需要了解彼此的内部结构。
type SpecAggregator interface {
	AddUpdateAPIService(apiService *v1.APIService, handler http.Handler) error
	// UpdateAPIServiceSpec updates the APIService. It returns ErrAPIServiceNotFound if the APIService doesn't exist.
	UpdateAPIServiceSpec(apiServiceName string) error
	RemoveAPIService(apiServiceName string)
}

const (
	aggregatorUser                = "system:aggregator"
	specDownloadTimeout           = time.Minute
	localDelegateChainNamePattern = "k8s_internal_local_delegation_chain_%010d"

	// A randomly generated UUID to differentiate local and remote eTags.
	// 本地生成的 ETag 使用固定前缀以区别于远端 APIService 的 ETag；
	// 二者若相同，合并后的 ETag 就无法反映真实变化，缓存会误判。
	locallyGeneratedEtagPrefix = "\"6E8F849B434D4B98A569B9D7718876E9-"
)

// openAPISpecInfo is used to store OpenAPI specs.
// The apiService object is used to sort specs with their priorities.
type openAPISpecInfo struct {
	apiService v1.APIService
	// spec is the cached OpenAPI spec
	spec cached.LastSuccess[*spec.Swagger]

	// The downloader is used only for non-local apiservices to
	// re-update the spec every so often.
	// Calling Get() is not thread safe and should only be called by a single
	// thread via the openapi controller.
	// 本地 spec 是静态的，不需要下载器；只有远端 APIService 才需要周期性刷新。
	downloader CacheableDownloader
}

// 把所有可变状态放进同一个结构体并用一把锁保护，是因为 APIService 的增删改
// 与合并 spec 的读取可能并发发生，粗粒度锁换取的是实现简单和一致性。
type specAggregator struct {
	// mutex protects the specsByAPIServiceName map and its contents.
	mutex sync.Mutex

	// Map of API Services' OpenAPI specs by their name
	// 用 APIService 名字做键，便于控制器按名字做增量更新，而不必重建整张表。
	specsByAPIServiceName map[string]*openAPISpecInfo

	// provided for dynamic OpenAPI spec
	openAPIVersionedService *handler.OpenAPIService

	downloader *Downloader
}

// 把“本地聚合器自身的 spec”和“委托链上各 handler 的 spec”统一塞进同一张表，
// 后续合并逻辑就无需再区分本地与远端，只需按优先级排序后逐个合并。
func buildAndRegisterSpecAggregatorForLocalServices(downloader *Downloader, aggregatorSpec *spec.Swagger, delegationHandlers []http.Handler, pathHandler common.PathHandler) *specAggregator {
	s := &specAggregator{
		downloader:            downloader,
		specsByAPIServiceName: map[string]*openAPISpecInfo{},
	}
	// 聚合器自身的 spec 永不变化，用静态缓存即可，省去无谓的重新下载。
	cachedAggregatorSpec := cached.Static(aggregatorSpec, "never-changes")
	// 从 0 开始编号，为聚合器自身保留一个槽位，委托链依次编号 1、2……
	s.addLocalSpec(fmt.Sprintf(localDelegateChainNamePattern, 0), cachedAggregatorSpec)
	for i, handler := range delegationHandlers {
		name := fmt.Sprintf(localDelegateChainNamePattern, i+1)

		spec := NewCacheableDownloader(name, downloader, handler)
		s.addLocalSpec(name, spec)
	}

	// 用 Lazy 版本注册，是为了在请求到来时才真正构建合并 spec，
	// 这样启动阶段无需等待所有远端 spec 下载完成。
	s.openAPIVersionedService = handler.NewOpenAPIServiceLazy(s.buildMergeSpecLocked())
	s.openAPIVersionedService.RegisterOpenAPIVersionedService("/openapi/v2", pathHandler)
	return s
}

// BuildAndRegisterAggregator registered OpenAPI aggregator handler. This function is not thread safe as it only being called on startup.
func BuildAndRegisterAggregator(downloader *Downloader, delegationTarget server.DelegationTarget, webServices []*restful.WebService,
	config *common.Config, pathHandler common.PathHandler) (SpecAggregator, error) {

	// 聚合器自身的路由也要产生一份 OpenAPI spec，作为合并的基线；
	// 这样即使没有注册任何远端 APIService，/openapi/v2 也能返回内置 API 的定义。
	aggregatorOpenAPISpec, err := builder.BuildOpenAPISpecFromRoutes(restfuladapter.AdaptWebServices(webServices), config)
	if err != nil {
		return nil, err
	}
	// 剥掉 default 字段，避免聚合器侧的默认值与远端 spec 中的定义冲突。
	aggregatorOpenAPISpec.Definitions = handler.PruneDefaults(aggregatorOpenAPISpec.Definitions)

	var delegationHandlers []http.Handler

	for delegate := delegationTarget; delegate != nil; delegate = delegate.NextDelegate() {
		handler := delegate.UnprotectedHandler()
		if handler == nil {
			continue
		}
		// ignore errors for the empty delegate we attach at the end the chain
		// atm the empty delegate returns 503 when the server hasn't been fully initialized
		// and the spec downloader only silences 404s
		// 链尾的空委托没有列出任何路径，下载它的 spec 只会得到 503，
		// 而下载器只忽略 404，所以这里直接跳过以免产生无意义的警告。
		if len(delegate.ListedPaths()) == 0 && delegate.NextDelegate() == nil {
			continue
		}
		delegationHandlers = append(delegationHandlers, handler)
	}
	s := buildAndRegisterSpecAggregatorForLocalServices(downloader, aggregatorOpenAPISpec, delegationHandlers, pathHandler)
	return s, nil
}

// 本地项用一个空 APIService 占占位，因为本地 spec 不参与远端优先级比较，
// 只要键存在、spec 可读即可；这也让本方法可以直接复用同一张 specsByAPIServiceName。
func (s *specAggregator) addLocalSpec(name string, cachedSpec cached.Value[*spec.Swagger]) {
	service := v1.APIService{}
	service.Name = name
	info := &openAPISpecInfo{
		apiService: service,
	}
	info.spec.Store(cachedSpec)
	s.specsByAPIServiceName[name] = info
}

// buildMergeSpecLocked creates a new cached mergeSpec from the list of cached specs.
// map 遍历顺序不稳定，必须先按优先级排序，才能保证合并结果（尤其是冲突消解和 ETag）稳定。
// 返回的 cached.Value 把合并计算延迟到真正需要时，避免在每次 APIService 变动时都同步重算。
func (s *specAggregator) buildMergeSpecLocked() cached.Value[*spec.Swagger] {
	apiServices := make([]*v1.APIService, 0, len(s.specsByAPIServiceName))
	for k := range s.specsByAPIServiceName {
		apiServices = append(apiServices, &s.specsByAPIServiceName[k].apiService)
	}
	sortByPriority(apiServices)
	// 按排好序的顺序收集缓存，索引位置即合并优先级。
	caches := make([]cached.Value[*spec.Swagger], len(apiServices))
	for i, apiService := range apiServices {
		caches[i] = &(s.specsByAPIServiceName[apiService.Name].spec)
	}

	return cached.MergeList(func(results []cached.Result[*spec.Swagger]) (*spec.Swagger, string, error) {
		var merged *spec.Swagger
		etags := make([]string, 0, len(results))
		for _, specInfo := range results {
			result, etag, err := specInfo.Get()
			if err != nil {
				// APIService name and err message will be included in
				// the error message as part of decorateError
				// 单个 APIService 拉取失败不应让整份 spec 不可用，记日志后跳过即可；
				// cached 层会保留上次成功的 spec 供后续使用。
				klog.Warning(err)
				continue
			}
			if merged == nil {
				merged = &spec.Swagger{}
				*merged = *result
				// Paths, Definitions and parameters are set by
				// MergeSpecsIgnorePathConflictRenamingDefinitionsAndParameters
				// 先以第一个 spec 为基底，再清空这三块由合并函数专门处理的字段，
				// 避免它们被基底内容“抢占”而使后续合并失去冲突检测的机会。
				merged.Paths = nil
				merged.Definitions = nil
				merged.Parameters = nil
			}
			etags = append(etags, etag)
			if err := aggregator.MergeSpecsIgnorePathConflictRenamingDefinitionsAndParameters(merged, result); err != nil {
				return nil, "", fmt.Errorf("failed to build merge specs: %v", err)
			}
		}
		// Printing the etags list is stable because it is sorted.
		// 合并后的 ETag 由各 spec ETag 列表的哈希生成，只要任一来源变化就会改变；
		// 顺序来自前面的优先级排序，因此相同输入总能得到相同 ETag。
		return merged, fmt.Sprintf("%x", sha256.Sum256([]byte(fmt.Sprintf("%#v", etags)))), nil
	}, caches)
}

// updateServiceLocked updates the spec cache by downloading the latest
// version of the spec.
// 下载得到的完整 spec 需要按该 APIService 声明的 group/version 过滤路径，
// 否则一个聚合 API 会意外暴露后端服务 spec 中属于其他组的路径。
func (s *specAggregator) updateServiceLocked(name string) error {
	specInfo, exists := s.specsByAPIServiceName[name]
	if !exists {
		// 返回哨兵错误，让调用方据此区分“对象已删除”与“下载失败”。
		return ErrAPIServiceNotFound
	}
	result, etag, err := specInfo.downloader.Get()
	filteredResult := cached.Transform[*spec.Swagger](func(result *spec.Swagger, etag string, err error) (*spec.Swagger, string, error) {
		if err != nil {
			return nil, "", err
		}
		group := specInfo.apiService.Spec.Group
		version := specInfo.apiService.Spec.Version
		return aggregator.FilterSpecByPathsWithoutSideEffects(result, []string{"/apis/" + group + "/" + version + "/"}), etag, nil
	}, cached.Result[*spec.Swagger]{Value: result, Etag: etag, Err: err})
	specInfo.spec.Store(filteredResult)
	return err
}

// UpdateAPIServiceSpec updates the api service. It is thread safe.
func (s *specAggregator) UpdateAPIServiceSpec(apiServiceName string) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	return s.updateServiceLocked(apiServiceName)
}

// AddUpdateAPIService adds the api service. It is thread safe. If the
// apiservice already exists, it will be updated.
// 本地 APIService（Service 为空）在构造阶段已经注册过，这里直接忽略，
// 避免为内置 API 建立多余的远端下载器。
func (s *specAggregator) AddUpdateAPIService(apiService *v1.APIService, handler http.Handler) error {
	if apiService.Spec.Service == nil {
		return nil
	}
	s.mutex.Lock()
	defer s.mutex.Unlock()

	existingSpec, exists := s.specsByAPIServiceName[apiService.Name]
	if !exists {
		specInfo := &openAPISpecInfo{
			apiService: *apiService,
			downloader: NewCacheableDownloader(apiService.Name, s.downloader, handler),
		}
		specInfo.spec.Store(cached.Result[*spec.Swagger]{Err: fmt.Errorf("spec for apiservice %s is not yet available", apiService.Name)})
		s.specsByAPIServiceName[apiService.Name] = specInfo
		// 只有新增/删除才改变合并项集合，需要重建合并 spec；
		// 单纯的属性更新不影响集合结构，无需重建。
		s.openAPIVersionedService.UpdateSpecLazy(s.buildMergeSpecLocked())
	} else {
		// 已存在时只更新元数据和 handler，缓存中的 spec 保持不变，
		// 等下一轮下载周期再刷新，避免每次 APIService 状态更新都触发全量重下。
		existingSpec.apiService = *apiService
		existingSpec.downloader.UpdateHandler(handler)
	}

	return nil
}

// RemoveAPIService removes an api service from OpenAPI aggregation. If it does not exist, no error is returned.
// It is thread safe.
func (s *specAggregator) RemoveAPIService(apiServiceName string) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	if _, exists := s.specsByAPIServiceName[apiServiceName]; !exists {
		return
	}
	delete(s.specsByAPIServiceName, apiServiceName)
	// Re-create the mergeSpec for the new list of apiservices
	// 删除后合并项集合发生变化，必须重建合并 spec，否则已删除 API 的路径仍会出现。
	s.openAPIVersionedService.UpdateSpecLazy(s.buildMergeSpecLocked())
}
