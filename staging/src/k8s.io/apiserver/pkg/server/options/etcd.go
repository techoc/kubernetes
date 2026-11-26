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

package options

import (
	"context"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/spf13/pflag"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/apiserver/pkg/registry/generic"
	genericregistry "k8s.io/apiserver/pkg/registry/generic/registry"
	"k8s.io/apiserver/pkg/server"
	"k8s.io/apiserver/pkg/server/healthz"
	"k8s.io/apiserver/pkg/server/options/encryptionconfig"
	encryptionconfigcontroller "k8s.io/apiserver/pkg/server/options/encryptionconfig/controller"
	encryptionconfigmetrics "k8s.io/apiserver/pkg/server/options/encryptionconfig/metrics"
	serverstorage "k8s.io/apiserver/pkg/server/storage"
	"k8s.io/apiserver/pkg/storage/etcd3/metrics"
	"k8s.io/apiserver/pkg/storage/storagebackend"
	storagefactory "k8s.io/apiserver/pkg/storage/storagebackend/factory"
	storagevalue "k8s.io/apiserver/pkg/storage/value"
	"k8s.io/klog/v2"
)

// EtcdOptions 汇总 etcd 后端、加密配置与 watch cache 的启动选项。
type EtcdOptions struct {
	StorageConfig                           storagebackend.Config
	EncryptionProviderConfigFilepath        string
	EncryptionProviderConfigAutomaticReload bool

	EtcdServersOverrides []string

	// 要把 protobuf 作为存储格式，只需把它设为
	// "application/vnd.kubernetes.protobuf"。
	DefaultStorageMediaType string
	DeleteCollectionWorkers int
	EnableGarbageCollection bool

	// 把 EnableWatchCache 设为 false 可禁用所有 watch cache。
	EnableWatchCache bool
	// WatchCacheSizes 表示对给定资源的覆盖配置。
	WatchCacheSizes []string

	// SkipHealthEndpoints 为 true 时，Apply 方法不再设置健康检查端点。
	// 这样可多次调用 Apply 而不重复注册这些端点。
	SkipHealthEndpoints bool
}

// storageTypes 是当前支持的存储后端集合；只剩 etcd3。
var storageTypes = sets.NewString(
	storagebackend.StorageTypeETCD3,
)

// NewEtcdOptions 以给定后端配置为基础填充默认值。
// 默认 JSON 存储、1 个 DeleteCollection worker、启用 GC 与 watch cache。
func NewEtcdOptions(backendConfig *storagebackend.Config) *EtcdOptions {
	options := &EtcdOptions{
		StorageConfig:           *backendConfig,
		DefaultStorageMediaType: "application/json",
		DeleteCollectionWorkers: 1,
		EnableGarbageCollection: true,
		EnableWatchCache:        true,
	}
	// 对象计数指标默认每分钟轮询一次，避免对 etcd 造成额外压力。
	options.StorageConfig.CountMetricPollPeriod = time.Minute
	return options
}

// storageMediaTypes 是允许作为存储格式的媒体类型。
var storageMediaTypes = sets.New(
	runtime.ContentTypeJSON,
	runtime.ContentTypeYAML,
	runtime.ContentTypeProtobuf,
)

// Validate 校验必填项与选项间依赖关系，返回全部错误而不是遇到第一个就返回，
// 便于运维一次修正多个配置问题。
func (s *EtcdOptions) Validate() []error {
	if s == nil {
		return nil
	}

	allErrors := []error{}
	if len(s.StorageConfig.Transport.ServerList) == 0 {
		allErrors = append(allErrors, fmt.Errorf("--etcd-servers must be specified"))
	}

	if s.StorageConfig.Type != storagebackend.StorageTypeUnset && !storageTypes.Has(s.StorageConfig.Type) {
		allErrors = append(allErrors, fmt.Errorf("--storage-backend invalid, allowed values: %s. If not specified, it will default to 'etcd3'", strings.Join(storageTypes.List(), ", ")))
	}

	if _, err := ParseEtcdServersOverrides(s.EtcdServersOverrides); err != nil {
		allErrors = append(allErrors, err)
	}

	if len(s.EncryptionProviderConfigFilepath) == 0 && s.EncryptionProviderConfigAutomaticReload {
		allErrors = append(allErrors, fmt.Errorf("--encryption-provider-config-automatic-reload must be set with --encryption-provider-config"))
	}

	if s.DefaultStorageMediaType != "" && !storageMediaTypes.Has(s.DefaultStorageMediaType) {
		allErrors = append(allErrors, fmt.Errorf("--storage-media-type %q invalid, allowed values: %s", s.DefaultStorageMediaType, strings.Join(sets.List(storageMediaTypes), ", ")))
	}

	return allErrors
}

// AddFlags 为指定 APIServer 向给定 FlagSet 注册 etcd 存储相关标志。
//
// 每个标志直接绑定到 EtcdOptions 的对应字段；多数标志由各 apiserver 二进制
// 自行暴露，因此默认值必须与 NewEtcdOptions 保持一致。
func (s *EtcdOptions) AddFlags(fs *pflag.FlagSet) {
	if s == nil {
		return
	}

	fs.StringSliceVar(&s.EtcdServersOverrides, "etcd-servers-overrides", s.EtcdServersOverrides, ""+
		"Per-resource etcd servers overrides, comma separated. The individual override "+
		"format: group/resource#servers, where servers are URLs, semicolon separated. "+
		"Note that this applies only to resources compiled into this server binary. "+
		`e.g. "/pods#http://etcd4:2379;http://etcd5:2379,/events#http://etcd6:2379"`)

	fs.StringVar(&s.DefaultStorageMediaType, "storage-media-type", s.DefaultStorageMediaType, ""+
		"The media type to use to store objects in storage. "+
		"Some resources or storage backends may only support a specific media type and will ignore this setting. "+
		"Supported media types: [application/json, application/yaml, application/vnd.kubernetes.protobuf]")
	fs.IntVar(&s.DeleteCollectionWorkers, "delete-collection-workers", s.DeleteCollectionWorkers,
		"Number of workers spawned for DeleteCollection call. These are used to speed up namespace cleanup.")

	fs.BoolVar(&s.EnableGarbageCollection, "enable-garbage-collector", s.EnableGarbageCollection, ""+
		"Enables the generic garbage collector. MUST be synced with the corresponding flag "+
		"of the kube-controller-manager.")

	fs.BoolVar(&s.EnableWatchCache, "watch-cache", s.EnableWatchCache,
		"Enable watch caching in the apiserver")

	defaultWatchCacheSize := 100
	fs.IntVar(&defaultWatchCacheSize, "default-watch-cache-size", defaultWatchCacheSize,
		"Default watch cache size. If zero, watch cache will be disabled for resources that do not have a default watch size set.")

	fs.MarkDeprecated("default-watch-cache-size",
		"Watch caches are sized automatically. This flag is no-op and it will be removed in a future version.")

	fs.StringSliceVar(&s.WatchCacheSizes, "watch-cache-sizes", s.WatchCacheSizes, ""+
		"Watch cache size settings for some resources (pods, nodes, etc.), comma separated. "+
		"The individual setting format: resource[.group]#size, where resource is lowercase plural (no version), "+
		"group is omitted for resources of apiVersion v1 (the legacy core API) and included for others, "+
		"and size is a number. This option is only meaningful for resources built into the apiserver, "+
		"not ones defined by CRDs or aggregated from external servers, and is only consulted if the "+
		"watch-cache is enabled. The only meaningful size setting to supply here is zero, which means to "+
		"disable watch caching for the associated resource; all non-zero values are equivalent and mean "+
		"to not disable watch caching for that resource")

	fs.StringVar(&s.StorageConfig.Type, "storage-backend", s.StorageConfig.Type,
		"The storage backend for persistence. Options: 'etcd3' (default).")

	fs.StringSliceVar(&s.StorageConfig.Transport.ServerList, "etcd-servers", s.StorageConfig.Transport.ServerList,
		"List of etcd servers to connect with (scheme://ip:port), comma separated.")

	fs.StringVar(&s.StorageConfig.Prefix, "etcd-prefix", s.StorageConfig.Prefix,
		"The prefix to prepend to all resource paths in etcd.")

	fs.StringVar(&s.StorageConfig.Transport.KeyFile, "etcd-keyfile", s.StorageConfig.Transport.KeyFile,
		"SSL key file used to secure etcd communication.")

	fs.StringVar(&s.StorageConfig.Transport.CertFile, "etcd-certfile", s.StorageConfig.Transport.CertFile,
		"SSL certification file used to secure etcd communication.")

	fs.StringVar(&s.StorageConfig.Transport.TrustedCAFile, "etcd-cafile", s.StorageConfig.Transport.TrustedCAFile,
		"SSL Certificate Authority file used to secure etcd communication.")

	fs.StringVar(&s.EncryptionProviderConfigFilepath, "encryption-provider-config", s.EncryptionProviderConfigFilepath,
		"The file containing configuration for encryption providers to be used for storing secrets in etcd")

	fs.BoolVar(&s.EncryptionProviderConfigAutomaticReload, "encryption-provider-config-automatic-reload", s.EncryptionProviderConfigAutomaticReload,
		"Determines if the file set by --encryption-provider-config should be automatically reloaded if the disk contents change. "+
			"Setting this to true disables the ability to uniquely identify distinct KMS plugins via the API server healthz endpoints.")

	fs.DurationVar(&s.StorageConfig.CompactionInterval, "etcd-compaction-interval", s.StorageConfig.CompactionInterval,
		"The interval of compaction requests. If 0, the compaction request from apiserver is disabled.")

	fs.DurationVar(&s.StorageConfig.CountMetricPollPeriod, "etcd-count-metric-poll-period", s.StorageConfig.CountMetricPollPeriod, ""+
		"Frequency of polling etcd for number of resources per type. 0 disables the metric collection.")

	fs.DurationVar(&s.StorageConfig.DBMetricPollInterval, "etcd-db-metric-poll-interval", s.StorageConfig.DBMetricPollInterval,
		"The interval of requests to poll etcd and update metric. 0 disables the metric collection")

	fs.DurationVar(&s.StorageConfig.HealthcheckTimeout, "etcd-healthcheck-timeout", s.StorageConfig.HealthcheckTimeout,
		"The timeout to use when checking etcd health.")

	fs.DurationVar(&s.StorageConfig.ReadycheckTimeout, "etcd-readycheck-timeout", s.StorageConfig.ReadycheckTimeout,
		"The timeout to use when checking etcd readiness")

	fs.Int64Var(&s.StorageConfig.LeaseManagerConfig.ReuseDurationSeconds, "lease-reuse-duration-seconds", s.StorageConfig.LeaseManagerConfig.ReuseDurationSeconds,
		"The time in seconds that each lease is reused. A lower value could avoid large number of objects reusing the same lease. Notice that a too small value may cause performance problems at storage layer.")
}

// ApplyTo 修改传入的 server.Config，绝不修改接收者（EtcdOptions）。
//
// 复制一份 StorageConfig 再补 tracker，避免把 server 级状态写回共享的 options。
func (s *EtcdOptions) ApplyTo(c *server.Config) error {
	if s == nil {
		return nil
	}

	storageConfigCopy := s.StorageConfig
	if storageConfigCopy.StorageObjectCountTracker == nil {
		storageConfigCopy.StorageObjectCountTracker = c.StorageObjectCountTracker
	}

	return s.ApplyWithStorageFactoryTo(&SimpleStorageFactory{StorageConfig: storageConfigCopy}, c)
}

// ApplyWithStorageFactoryTo 修改传入的 server.Config，绝不修改接收者（EtcdOptions）。
//
// 顺序有讲究：先注册健康检查与加密 transformer，最后才设置 RESTOptionsGetter，
// 确保 REST 层初始化时 transformer 与 monitor 已就绪。
func (s *EtcdOptions) ApplyWithStorageFactoryTo(factory serverstorage.StorageFactory, c *server.Config) error {
	if s == nil {
		return nil
	}

	if !s.SkipHealthEndpoints {
		if err := s.addEtcdHealthEndpoint(c); err != nil {
			return err
		}
	}

	// 设置加密。
	if err := s.maybeApplyResourceTransformers(c); err != nil {
		return err
	}

	monitorCache, err := newMonitorCache(factory, c.DrainedNotify())
	if err != nil {
		return err
	}
	metrics.SetStorageMonitorGetter(monitorCache.get)

	c.RESTOptionsGetter = s.CreateRESTOptionsGetter(factory, c.ResourceTransformers)
	return nil
}

// monitorCache 惰性创建并缓存各后端的存储监控器。
// 惰性是为了把创建推迟到首次指标采集，避免启动阶段就建立额外连接。
type monitorCache struct {
	mu          sync.RWMutex
	initialized bool
	closed      bool
	monitors    []metrics.Monitor
	factory     serverstorage.StorageFactory
	stopCh      <-chan struct{}
}

// createMonitor 可在测试中替换，以注入假的监控实现。
var createMonitor = storagefactory.CreateMonitor

// newMonitorCache 要求 stopCh 非空，否则无法在退出时释放监控器。
func newMonitorCache(factory serverstorage.StorageFactory, stopCh <-chan struct{}) (*monitorCache, error) {
	if stopCh == nil {
		return nil, fmt.Errorf("stopCh is required for monitor cache cleanup")
	}
	cache := &monitorCache{
		factory: factory,
		stopCh:  stopCh,
	}
	return cache, nil
}

// get 返回缓存的监控器；首次调用走写锁初始化。
// 读路径使用 RLock，指标采集是高频操作，不能每次都拿写锁。
func (c *monitorCache) get() ([]metrics.Monitor, error) {
	// 快速路径：用读锁检查是否已初始化。
	c.mu.RLock()
	if c.closed {
		c.mu.RUnlock()
		return nil, fmt.Errorf("monitor cache is closed")
	}
	if c.initialized {
		result := c.monitors
		c.mu.RUnlock()
		return result, nil
	}
	c.mu.RUnlock()

	// 慢速路径：用写锁初始化。
	return c.initialize()
}

// initialize 在写锁下创建全部监控器；任一失败则回滚已创建的部分，避免资源泄漏。
func (c *monitorCache) initialize() ([]metrics.Monitor, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return nil, fmt.Errorf("monitor cache is closed")
	}
	if c.initialized {
		return c.monitors, nil
	}

	var monitors []metrics.Monitor
	for _, cfg := range c.factory.Configs() {
		m, err := createMonitor(cfg)
		if err != nil {
			for _, already := range monitors {
				already.Close() //nolint:errcheck
			}
			return nil, err
		}
		monitors = append(monitors, m)
	}
	c.monitors = monitors
	c.initialized = true

	go func() {
		<-c.stopCh
		c.close()
	}()

	return c.monitors, nil
}

// close 幂等地关闭所有监控器，由 server drained 信号触发。
func (c *monitorCache) close() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return
	}
	c.closed = true
	for _, m := range c.monitors {
		m.Close() //nolint:errcheck
	}
	c.monitors = nil
}

// CreateRESTOptionsGetter 构造 REST 选项工厂；若配置了加密 transformer，
// 则用装饰器把按资源选择的 transformer 注入到每个存储配置中。
func (s *EtcdOptions) CreateRESTOptionsGetter(factory serverstorage.StorageFactory, resourceTransformers storagevalue.ResourceTransformers) generic.RESTOptionsGetter {
	if resourceTransformers != nil {
		factory = &transformerStorageFactory{
			delegate:             factory,
			resourceTransformers: resourceTransformers,
		}
	}
	return &StorageFactoryRestOptionsFactory{Options: *s, StorageFactory: factory}
}

// maybeApplyResourceTransformers 按配置装配静态或动态加密 transformer。
// 自动重载模式需要且仅需要一个健康检查，用于暴露 KMS 插件状态；
// 该检查只加入 healthz/readyz，不加入 livez，避免 KMS 抖动导致 Pod 被重启。
func (s *EtcdOptions) maybeApplyResourceTransformers(c *server.Config) (err error) {
	if c.ResourceTransformers != nil {
		return nil
	}
	if len(s.EncryptionProviderConfigFilepath) == 0 {
		return nil
	}

	ctxServer := wait.ContextForChannel(c.DrainedNotify())
	ctxTransformers, closeTransformers := context.WithCancel(ctxServer)
	defer func() {
		// 出错时关闭部分已初始化的 transformer（如果有）。
		if err != nil {
			closeTransformers()
		}
	}()

	encryptionConfiguration, err := encryptionconfig.LoadEncryptionConfig(ctxTransformers, s.EncryptionProviderConfigFilepath, s.EncryptionProviderConfigAutomaticReload, c.APIServerID)
	if err != nil {
		return err
	}
	encryptionconfigmetrics.RecordEncryptionConfigLastConfigInfo(c.APIServerID, encryptionConfiguration.EncryptionFileContentHash)

	if s.EncryptionProviderConfigAutomaticReload {
		// reload=true 时始终只有 1 个健康检查。
		if len(encryptionConfiguration.HealthChecks) != 1 {
			return fmt.Errorf("failed to start kms encryption config hot reload controller. only 1 health check should be available when reload is enabled")
		}

		// 动态 transformer 接管 transformer 本身及其取消逻辑。
		dynamicTransformers := encryptionconfig.NewDynamicTransformers(encryptionConfiguration.Transformers, encryptionConfiguration.HealthChecks[0], closeTransformers, encryptionConfiguration.KMSCloseGracePeriod)

		// 添加 post start hook 以启动热重载控制器。
		// 在此添加 hook 可确保它只被配置一次。
		err = c.AddPostStartHook(
			"start-encryption-provider-config-automatic-reload",
			func(_ server.PostStartHookContext) error {
				dynamicEncryptionConfigController := encryptionconfigcontroller.NewDynamicEncryptionConfiguration(
					"encryption-provider-config-automatic-reload-controller",
					s.EncryptionProviderConfigFilepath,
					dynamicTransformers,
					encryptionConfiguration.EncryptionFileContentHash,
					c.APIServerID,
				)

				go dynamicEncryptionConfigController.Run(ctxServer)

				return nil
			},
		)
		if err != nil {
			return fmt.Errorf("failed to add post start hook for kms encryption config hot reload controller: %w", err)
		}

		c.ResourceTransformers = dynamicTransformers
		if !s.SkipHealthEndpoints {
			addHealthChecksWithoutLivez(c, dynamicTransformers)
		}
	} else {
		c.ResourceTransformers = encryptionconfig.StaticTransformers(encryptionConfiguration.Transformers)
		if !s.SkipHealthEndpoints {
			addHealthChecksWithoutLivez(c, encryptionConfiguration.HealthChecks...)
		}
	}

	return nil
}

// addHealthChecksWithoutLivez 只注册到 healthz 与 readyz：
// 存储/加密依赖不可用时应停止接流量，但不该触发容器重启。
func addHealthChecksWithoutLivez(c *server.Config, healthChecks ...healthz.HealthChecker) {
	c.HealthzChecks = append(c.HealthzChecks, healthChecks...)
	c.ReadyzChecks = append(c.ReadyzChecks, healthChecks...)
}

// addEtcdHealthEndpoint 注册 etcd 健康与就绪检查，并为每个服务器覆盖配置
// 生成独立的检查项。
func (s *EtcdOptions) addEtcdHealthEndpoint(c *server.Config) error {
	healthCheck, err := storagefactory.CreateHealthCheck(s.StorageConfig, c.DrainedNotify())
	if err != nil {
		return err
	}
	c.AddHealthChecks(healthz.NamedGroupedCheck("etcd", "etcd", func(r *http.Request) error {
		return healthCheck()
	}))

	readyCheck, err := storagefactory.CreateReadyCheck(s.StorageConfig, c.DrainedNotify())
	if err != nil {
		return err
	}
	c.AddReadyzChecks(healthz.NamedGroupedCheck("etcd-readiness", "etcd-readiness", func(r *http.Request) error {
		return readyCheck()
	}))

	if len(s.EtcdServersOverrides) != 0 {
		// 多个 override 的服务器顺序可能不同。
		// 例如：["apps/deployments#s2.example.com;s1.example.com","apps/replicasets#s1.example.com;s2.example.com"]
		overrides, err := ParseEtcdServersOverrides(s.EtcdServersOverrides)
		if err != nil {
			return err
		}
		// 多个 override 可能指向相同的服务器。
		// 例如：["apps/deployments#s1.example.com;s2.example.com","apps/replicasets#s1.example.com;s2.example.com"]
		serversSets := sets.NewString()
		for _, override := range overrides {
			sortedServers := make([]string, len(override.Servers))
			// 使用复制的 slice，避免通过 SetEtcdLocation 修改调用方持有的原始 slice。
			copy(sortedServers, override.Servers)
			sort.Strings(sortedServers)
			serversKeyStr := strings.Join(sortedServers, ";")
			if serversSets.Has(serversKeyStr) {
				continue
			}
			serversSets.Insert(serversKeyStr)

			sc := s.StorageConfig
			sc.Transport.ServerList = override.Servers

			healthCheck, err := storagefactory.CreateHealthCheck(sc, c.DrainedNotify())
			if err != nil {
				return err
			}
			c.AddHealthChecks(healthz.NamedGroupedCheck(fmt.Sprintf("etcd-override-%d", len(serversSets)-1), "etcd", func(r *http.Request) error {
				return healthCheck()
			}))

			readyCheck, err := storagefactory.CreateReadyCheck(sc, c.DrainedNotify())
			if err != nil {
				return err
			}
			c.AddReadyzChecks(healthz.NamedGroupedCheck(fmt.Sprintf("etcd-override-readiness-%d", len(serversSets)-1), "etcd-readiness", func(r *http.Request) error {
				return readyCheck()
			}))
		}
	}
	return nil
}

// StorageFactoryRestOptionsFactory 把 StorageFactory 适配成 generic.RESTOptionsGetter。
type StorageFactoryRestOptionsFactory struct {
	Options        EtcdOptions
	StorageFactory serverstorage.StorageFactory
}

// GetRESTOptions 为单个资源生成存储选项，并根据 watch cache 配置选择装饰器。
func (f *StorageFactoryRestOptionsFactory) GetRESTOptions(resource schema.GroupResource, example runtime.Object) (generic.RESTOptions, error) {
	storageConfig, err := f.StorageFactory.NewConfig(resource, example)
	if err != nil {
		return generic.RESTOptions{}, fmt.Errorf("unable to find storage destination for %v, due to %v", resource, err.Error())
	}

	ret := generic.RESTOptions{
		StorageConfig:             storageConfig,
		Decorator:                 generic.UndecoratedStorage,
		DeleteCollectionWorkers:   f.Options.DeleteCollectionWorkers,
		EnableGarbageCollection:   f.Options.EnableGarbageCollection,
		ResourcePrefix:            f.StorageFactory.ResourcePrefix(resource),
		CountMetricPollPeriod:     f.Options.StorageConfig.CountMetricPollPeriod,
		StorageObjectCountTracker: f.Options.StorageConfig.StorageObjectCountTracker,
	}

	if ret.StorageObjectCountTracker == nil {
		ret.StorageObjectCountTracker = storageConfig.StorageObjectCountTracker
	}

	if f.Options.EnableWatchCache {
		sizes, err := ParseWatchCacheSizes(f.Options.WatchCacheSizes)
		if err != nil {
			return generic.RESTOptions{}, err
		}
		size, ok := sizes[resource]
		if ok && size > 0 {
			klog.Warningf("Dropping watch-cache-size for %v - watchCache size is now dynamic", resource)
		}
		if ok && size <= 0 {
			klog.V(3).InfoS("Not using watch cache", "resource", resource)
			ret.Decorator = generic.UndecoratedStorage
		} else {
			klog.V(3).InfoS("Using watch cache", "resource", resource)
			ret.Decorator = genericregistry.StorageWithCacher()
		}
	}

	return ret, nil
}

// ParseWatchCacheSizes 把缓存大小值列表转换为 group resource 到请求大小的映射。
//
// 格式为 "resource[.group]#size"；负数没有意义，直接报错。
func ParseWatchCacheSizes(cacheSizes []string) (map[schema.GroupResource]int, error) {
	watchCacheSizes := make(map[schema.GroupResource]int)
	for _, c := range cacheSizes {
		tokens := strings.Split(c, "#")
		if len(tokens) != 2 {
			return nil, fmt.Errorf("invalid value of watch cache size: %s", c)
		}

		size, err := strconv.Atoi(tokens[1])
		if err != nil {
			return nil, fmt.Errorf("invalid size of watch cache size: %s", c)
		}
		if size < 0 {
			return nil, fmt.Errorf("watch cache size cannot be negative: %s", c)
		}
		watchCacheSizes[schema.ParseGroupResource(tokens[0])] = size
	}
	return watchCacheSizes, nil
}

// WriteWatchCacheSizes 把缓存大小值映射转换为字符串规格列表。
func WriteWatchCacheSizes(watchCacheSizes map[schema.GroupResource]int) ([]string, error) {
	var cacheSizes []string

	for resource, size := range watchCacheSizes {
		if size < 0 {
			return nil, fmt.Errorf("watch cache size cannot be negative for resource %s", resource)
		}
		cacheSizes = append(cacheSizes, fmt.Sprintf("%s#%d", resource.String(), size))
	}
	return cacheSizes, nil
}

var _ serverstorage.StorageFactory = &SimpleStorageFactory{}

// SimpleStorageFactory 提供 StorageFactory 实现，适用于不同资源基本共享同一存储配置
// （由给定 storagebackend.Config 定义）的场景。它假定资源存储在完全由 schema.GroupResource 决定的路径上。
//
// 需要灵活性及按资源覆盖的用户应改用 DefaultStorageFactory。
type SimpleStorageFactory struct {
	StorageConfig storagebackend.Config
}

// NewConfig 返回资源对应的存储配置；路径由 GroupResource 唯一决定。
func (s *SimpleStorageFactory) NewConfig(resource schema.GroupResource, example runtime.Object) (*storagebackend.ConfigForResource, error) {
	return s.StorageConfig.ForResource(resource), nil
}

// ResourcePrefix 返回资源在 etcd 中的键前缀（group/resource）。
func (s *SimpleStorageFactory) ResourcePrefix(resource schema.GroupResource) string {
	return resource.Group + "/" + resource.Resource
}

// Configs 返回底层存储配置列表。
func (s *SimpleStorageFactory) Configs() []storagebackend.Config {
	return serverstorage.Configs(s.StorageConfig)
}

// Backends 返回后端描述；当前实现不会调用它，但需满足接口。
func (s *SimpleStorageFactory) Backends() []serverstorage.Backend {
	// 不应有任何代码调用此方法，但仍提供可用实现。
	return serverstorage.Backends(s.StorageConfig)
}

var _ serverstorage.StorageFactory = &transformerStorageFactory{}

// transformerStorageFactory 为每个资源注入对应的加密 transformer，
// 其余行为全部委托给底层 factory。
type transformerStorageFactory struct {
	delegate             serverstorage.StorageFactory
	resourceTransformers storagevalue.ResourceTransformers
}

// NewConfig 复制配置后再替换 transformer，避免修改底层 factory 共享的对象。
func (t *transformerStorageFactory) NewConfig(resource schema.GroupResource, example runtime.Object) (*storagebackend.ConfigForResource, error) {
	config, err := t.delegate.NewConfig(resource, example)
	if err != nil {
		return nil, err
	}

	configCopy := *config
	resourceConfig := configCopy.Config
	resourceConfig.Transformer = t.resourceTransformers.TransformerForResource(resource)
	configCopy.Config = resourceConfig

	return &configCopy, nil
}

// ResourcePrefix 委托给底层 factory。
func (t *transformerStorageFactory) ResourcePrefix(resource schema.GroupResource) string {
	return t.delegate.ResourcePrefix(resource)
}

// Configs 委托给底层 factory。
func (t *transformerStorageFactory) Configs() []storagebackend.Config {
	return t.delegate.Configs()
}

// Backends 委托给底层 factory。
func (t *transformerStorageFactory) Backends() []serverstorage.Backend {
	return t.delegate.Backends()
}

// EtcdServerOverride 表示某个资源使用独立 etcd 集群的配置。
type EtcdServerOverride struct {
	GroupResource schema.GroupResource
	Servers       []string
}

// errOverridesInvalid 是 etcd 服务器覆盖格式错误的统一错误。
var errOverridesInvalid = fmt.Errorf("--etcd-servers-overrides invalid, must be of format: group/resource#servers, where servers are URLs, semicolon separated")

// ParseEtcdServersOverrides 解析 "group/resource#url;url" 形式的覆盖列表。
// 任何格式偏差都返回统一错误，避免部分解析导致难以察觉的配置遗漏。
func ParseEtcdServersOverrides(etcdServersOverrides []string) ([]EtcdServerOverride, error) {
	var overrides []EtcdServerOverride
	for _, override := range etcdServersOverrides {
		tokens := strings.Split(override, "#")
		if len(tokens) != 2 {
			return nil, errOverridesInvalid
		}
		apiresource := strings.Split(tokens[0], "/")
		if len(apiresource) != 2 {
			return nil, errOverridesInvalid
		}
		servers := strings.Split(tokens[1], ";")
		for _, server := range servers {
			if len(server) == 0 {
				return nil, errOverridesInvalid
			}
		}
		overrides = append(overrides, EtcdServerOverride{
			GroupResource: schema.GroupResource{Group: apiresource[0], Resource: apiresource[1]},
			Servers:       servers,
		})
	}
	return overrides, nil
}
