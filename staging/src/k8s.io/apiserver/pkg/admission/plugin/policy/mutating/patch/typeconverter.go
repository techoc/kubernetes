/*
Copyright 2024 The Kubernetes Authors.

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

package patch

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/managedfields"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/openapi"
	"k8s.io/kube-openapi/pkg/spec3"
)

// TypeConverterManager 按 GVK 提供类型转换器，并在后台持续刷新 CRD 的 schema 缓存。
//
// 内置类型使用静态转换器（编译期 schema，零网络开销）；
// CRD 等动态类型需要从 /openapi/v3 拉取 schema，因此必须有缓存与失效机制，
// 否则每次 apply 都会触发一次网络往返，成本不可接受。
type TypeConverterManager interface {
	// GetTypeConverter 返回适用于给定 GVK 的类型转换器，
	// 返回 nil 表示当前无法为该校验/转换提供 schema，
	// 调用方需按失败处理，而不是假定存在转换器。
	GetTypeConverter(gvk schema.GroupVersionKind) managedfields.TypeConverter
	// Run 启动后台轮询；ctx 取消后退出。
	Run(ctx context.Context)
}

// NewTypeConverterManager 构造管理器；缓存为空，首次使用时按需拉取。
func NewTypeConverterManager(
	staticTypeConverter managedfields.TypeConverter,
	openapiClient openapi.Client,
) TypeConverterManager {
	return &typeConverterManager{
		staticTypeConverter: staticTypeConverter,
		openapiClient:       openapiClient,
		typeConverterMap:    make(map[schema.GroupVersion]typeConverterCacheEntry),
		lastFetchedPaths:    make(map[schema.GroupVersion]openapi.GroupVersion),
	}
}

// typeConverterCacheEntry 把转换器与其生成时的 schema 版本绑定；
// 版本变化即意味着缓存失效，二者必须一起比较。
type typeConverterCacheEntry struct {
	typeConverter managedfields.TypeConverter
	entry         openapi.GroupVersion
}

// typeConverterManager 用于确保 openapi 模型始终拥有最新的 schema 与类型转换器。
// 它应被配置为对原生类型使用静态类型转换器，并按需通过网络为 CRD/其他类型
// 拉取 schema（尽量避免不必要的网络调用）。
//
// 并发模型为「读多写少」：GetTypeConverter 用 RLock 快路径，
// 只有真正重建转换器时才升级为写锁；Run 的轮询只用写锁替换版本表。
// 注意网络请求发生在锁外，避免阻塞其他 GVK 的读取。
type typeConverterManager struct {
	// schemaCache 用于缓存给定 GVK 的 schema
	staticTypeConverter managedfields.TypeConverter

	// discoveryClient 用于获取给定 GVK 的 schema
	openapiClient openapi.Client

	lock sync.RWMutex

	typeConverterMap map[schema.GroupVersion]typeConverterCacheEntry
	lastFetchedPaths map[schema.GroupVersion]openapi.GroupVersion
}

// Run 每 5 秒轮询一次 /openapi/v3 的路径列表，记录每个 GV 的 schema 版本。
// 底层依赖 e-tag/304，因此轮询本身开销很小；这里不直接拉取 schema，
// 只更新「版本号」映射，真正的重建推迟到下一次 GetTypeConverter。
// 出错时返回 (false, nil) 继续轮询，因为 schema 服务可能短暂不可用。
func (t *typeConverterManager) Run(ctx context.Context) {
	// 每 5 秒刷新一次 OpenAPI schema 列表，以得知哪些 schema 已失效。
	// 底层应会使用 e-tag 做增量判断
	_ = wait.PollUntilContextCancel(ctx, 5*time.Second, true, func(_ context.Context) (done bool, err error) {
		paths, err := t.openapiClient.Paths()
		if err != nil {
			utilruntime.HandleError(fmt.Errorf("failed to fetch openapi paths: %w", err))
			return false, nil
		}

		// /openapi/v3 端点返回一组路径，其 ServerRelativeURL 在 schema 每次更新时
		// 都会变化。因此我们轮询 /openapi/v3 拿到每个 schema 的「版本号」，
		// 版本号与拉取时不一致就使缓存失效。
		parsedPaths := make(map[schema.GroupVersion]openapi.GroupVersion, len(paths))
		for path, entry := range paths {
			// 只关心 API 路径；排除 openapi 自身等非资源条目。
			if !strings.HasPrefix(path, "apis/") && !strings.HasPrefix(path, "api/") {
				continue
			}
			path = strings.TrimPrefix(path, "apis/")
			path = strings.TrimPrefix(path, "api/")

			gv, err := schema.ParseGroupVersion(path)
			if err != nil {
				utilruntime.HandleError(fmt.Errorf("failed to parse group version %q: %w", path, err))
				return false, nil
			}

			parsedPaths[gv] = entry
		}

		// 整体替换而非增量合并：不存在的 GV 应从缓存视图中消失。
		t.lock.Lock()
		defer t.lock.Unlock()
		t.lastFetchedPaths = parsedPaths
		return false, nil
	})
}

// GetTypeConverter 返回适用于 gvk 的转换器，必要时拉取 schema 并重建。
// 先用静态转换器尝试「转换一个空对象」来探测其是否支持该 GVK——
// 这是对缺少存在性查询接口的临时绕行；成功即说明静态转换器覆盖了该类型，
// 从而避免任何网络调用。
// 缓存键是 GroupVersion 而非 GVK：同一 GV 下所有 kind 共享一份 schema。
func (t *typeConverterManager) GetTypeConverter(gvk schema.GroupVersionKind) managedfields.TypeConverter {
	// 检查静态类型转换器是否处理该 GVK
	if t.staticTypeConverter != nil {
		//!TODO: 为 type converter 增加存在性检查能力
		// 暂时用绕行方案：尝试获取空对象的类型化版本，看是否报错
		stub := &unstructured.Unstructured{}
		stub.SetGroupVersionKind(gvk)

		if _, err := t.staticTypeConverter.ObjectToTyped(stub); err == nil {
			return t.staticTypeConverter
		}
	}

	gv := gvk.GroupVersion()

	existing, entry, err := func() (managedfields.TypeConverter, openapi.GroupVersion, error) {
		t.lock.RLock()
		defer t.lock.RUnlock()

		// 若静态类型转换器不支持该 schema，则向 discovery 请求 schema
		entry, ok := t.lastFetchedPaths[gv]
		if !ok {
			// 拿不到 schema 就无能为力
			return nil, nil, fmt.Errorf("no schema for %v", gvk)
		}

		// ServerRelativeURL 内含内容哈希/版本，变化即表示 schema 已更新。
		if existing, ok := t.typeConverterMap[gv]; ok && existing.entry.ServerRelativeURL() == entry.ServerRelativeURL() {
			// 若已有该 GVK 的类型转换器，直接返回
			return existing.typeConverter, existing.entry, nil
		}

		return nil, entry, nil
	}()
	if err != nil {
		utilruntime.HandleError(err)
		return nil
	} else if existing != nil {
		return existing
	}

	// 以下网络与解析工作在锁外进行，避免阻塞其他 GVK 的读取。
	schBytes, err := entry.Schema(runtime.ContentTypeJSON)
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("failed to get schema for %v: %w", gvk, err))
		return nil
	}

	var sch spec3.OpenAPI
	if err := json.Unmarshal(schBytes, &sch); err != nil {
		utilruntime.HandleError(fmt.Errorf("failed to unmarshal schema for %v: %w", gvk, err))
		return nil
	}

	// schema 已变化或尚无对应条目，为该 GV 生成新的类型转换器
	tc, err := managedfields.NewTypeConverter(sch.Components.Schemas, false)
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("failed to create type converter for %v: %w", gvk, err))
		return nil
	}

	t.lock.Lock()
	defer t.lock.Unlock()

	// 并发重建时后写者覆盖先写者，但两者基于同一 entry 版本，结果等价。
	t.typeConverterMap[gv] = typeConverterCacheEntry{
		typeConverter: tc,
		entry:         entry,
	}

	return tc
}
