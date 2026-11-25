/*
Copyright The Kubernetes Authors.

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

package ktesting

import (
	"context"
	"fmt"
	"testing"
	"time"

	apiextensions "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset"
	"k8s.io/client-go/discovery/cached/memory"
	"k8s.io/client-go/dynamic"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/restmapper"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/test/utils/ktesting"
)

type data struct {
	restConfig    *rest.Config
	restMapper    *restmapper.DeferredDiscoveryRESTMapper
	client        clientset.Interface
	dynamic       dynamic.Interface
	apiextensions apiextensions.Interface
}

type dataKeyType struct{}

var dataKey dataKeyType

func get(ctx context.Context) data {
	c := ctx.Value(dataKey)
	if c == nil {
		return data{}
	}
	return *c.(*data)
}

func set(tCtx ktesting.TContext, data data) TContext {
	return TContext{tCtx.WithValue(dataKey, &data)}
}

// TContext 是 [ktesting.TContext] 的直接替代品，并增加了对 client-go 的支持。
//
// 新增的方法可用于获取多种 client-go 类型的实例，无需将它们作为单独的
// 参数一路传递到测试辅助函数的调用链中。
//
// 所有原型中包含 [ktesting.TContext] 的方法都会被重写，改为使用 TContext
// 工作。这确保新增的方法始终可被访问。
//
// 调用需要 [ktesting.TContext] 的函数时，传入此处嵌入的 TContext 字段即可。
type TContext struct {
	ktesting.TContext
}

// RESTConfig 返回 rest 客户端配置的副本，其中 UserAgent 设置为包含当前测试名称；
// 如果不可用则返回 nil。可通过 [Client]、[Dynamic]、[APIExtensions]
// 获取使用此配置的多个类型化客户端。
func (tCtx TContext) RESTConfig() *rest.Config {
	return rest.CopyConfig(get(tCtx).restConfig)
}

func (tCtx TContext) RESTMapper() *restmapper.DeferredDiscoveryRESTMapper {
	return get(tCtx).restMapper
}
func (tCtx TContext) Client() clientset.Interface            { return get(tCtx).client }
func (tCtx TContext) Dynamic() dynamic.Interface             { return get(tCtx).dynamic }
func (tCtx TContext) APIExtensions() apiextensions.Interface { return get(tCtx).apiextensions }

// WithRESTConfig 使用该配置创建的新客户端初始化所有 client-go 客户端。
// 当前测试名称会包含在 UserAgent 中。
func WithRESTConfig(tCtx ktesting.TContext, cfg *rest.Config) TContext {
	cfg = rest.CopyConfig(cfg)
	cfg.UserAgent = fmt.Sprintf("%s -- %s", rest.DefaultKubernetesUserAgent(), tCtx.Name())

	var data data
	data.restConfig = cfg
	data.client = clientset.NewForConfigOrDie(cfg)
	data.dynamic = dynamic.NewForConfigOrDie(cfg)
	data.apiextensions = apiextensions.NewForConfigOrDie(cfg)
	cachedDiscovery := memory.NewMemCacheClient(data.client.Discovery())
	data.restMapper = restmapper.NewDeferredDiscoveryRESTMapper(cachedDiscovery)
	return set(tCtx, data)
}

func (tCtx TContext) WithRESTConfig(cfg *rest.Config) TContext {
	return WithRESTConfig(tCtx.TContext, cfg)
}

// WithClients 使用已有的配置和客户端。
func WithClients(tCtx ktesting.TContext, cfg *rest.Config, mapper *restmapper.DeferredDiscoveryRESTMapper, client clientset.Interface, dynamic dynamic.Interface, apiextensions apiextensions.Interface) TContext {
	return set(tCtx, data{
		restConfig:    cfg,
		restMapper:    mapper,
		client:        client,
		dynamic:       dynamic,
		apiextensions: apiextensions,
	})
}

func (tCtx TContext) WithClients(cfg *rest.Config, mapper *restmapper.DeferredDiscoveryRESTMapper, client clientset.Interface, dynamic dynamic.Interface, apiextensions apiextensions.Interface) TContext {
	return WithClients(tCtx.TContext, cfg, mapper, client, dynamic, apiextensions)
}

// See [ktesting.TB].
type TB = ktesting.TB

// See [ktesting.ContextTB].
type ContextTB = ktesting.ContextTB

// See [ktesting.WithStep].
func (tCtx TContext) WithStep(step string) TContext {
	return TContext{tCtx.TContext.WithStep(step)}
}

// See [ktesting.Step].
func (tCtx TContext) Step(step string, cb func(tCtx TContext)) {
	tCtx.TContext.Step(step, func(tCtx ktesting.TContext) {
		cb(TContext{tCtx})
	})
}

// See [ktesting.Init].
func Init(tb ktesting.TB, opts ...ktesting.InitOption) TContext {
	return TContext{ktesting.Init(tb, opts...)}
}

// See [ktesting.InitCtx].
func InitCtx(ctx context.Context, tb ktesting.TB, opts ...ktesting.InitOption) TContext {
	return TContext{ktesting.InitCtx(ctx, tb, opts...)}
}

// See [ktesting.WithContext].
func (tCtx TContext) WithContext(ctx context.Context) TContext {
	return TContext{tCtx.TContext.WithContext(ctx)}
}

// See [ktesting.WithValue].
func (tCtx TContext) WithValue(key, val any) TContext {
	return TContext{tCtx.TContext.WithValue(key, val)}
}

// See [ktesting.WithNamespace].
func (tCtx TContext) WithNamespace(namespace string) TContext {
	return TContext{tCtx.TContext.WithNamespace(namespace)}
}

// See [ktesting.WithCancel].
func (tCtx TContext) WithCancel() TContext {
	return TContext{tCtx.TContext.WithCancel()}
}

// See [ktesting.WithoutCancel].
func (tCtx TContext) WithoutCancel() TContext {
	return TContext{tCtx.TContext.WithoutCancel()}
}

// See [ktesting.WithTimeout].
func (tCtx TContext) WithTimeout(timeout time.Duration, timeoutCause string) TContext {
	return TContext{tCtx.TContext.WithTimeout(timeout, timeoutCause)}
}

// See [ktesting.WithLogger].
func (tCtx TContext) WithLogger(logger klog.Logger) TContext {
	return TContext{tCtx.TContext.WithLogger(logger)}
}

// See [ktesting.CleanupCtx].
func (tCtx TContext) CleanupCtx(cb func(tCtx TContext)) {
	tCtx.Helper()
	tCtx.TContext.CleanupCtx(func(tCtx ktesting.TContext) {
		cb(TContext{tCtx})
	})
}

// See [ktesting.WithError].
func (tCtx TContext) WithError(err *error) (TContext, func()) {
	baseCtx, finalize := tCtx.TContext.WithError(err)
	return TContext{baseCtx}, finalize
}

// See [ktesting.Run].
func (tCtx TContext) Run(name string, cb func(tCtx TContext)) bool {
	return tCtx.TContext.Run(name, func(tCtx ktesting.TContext) {
		cb(TContext{tCtx})
	})
}

// See [ktesting.SyncTest].
func (tCtx TContext) SyncTest(name string, cb func(tCtx TContext)) bool {
	return tCtx.TContext.SyncTest(name, func(tCtx ktesting.TContext) {
		cb(TContext{tCtx})
	})
}

// See [ktesting.SetDefaultVerbosity].
func SetDefaultVerbosity(v int) {
	ktesting.SetDefaultVerbosity(v)
}

// See [ktesting.NewTestContext].
func NewTestContext(tb testing.TB) (klog.Logger, context.Context) {
	logger, ctx := ktesting.NewTestContext(tb)
	return logger, TContext{ctx.(ktesting.TContext)}
}
