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

package storage

import (
	"crypto/tls"
	"crypto/x509"
	"os"
	"strings"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apiserver/pkg/storage/storagebackend"
	"k8s.io/klog/v2"
)

// Backend 描述存储服务器，其中的信息应足以
// 进行健康检查。
type Backend struct {
	// 存储后端的 URL，如 https://etcd.domain:2379
	Server string
	// 所需的 TLS 配置
	TLSConfig *tls.Config
}

// StorageFactory 是定位给定 GroupResource 存储位置的接口
type StorageFactory interface {
	// New 查找给定 group 与 resource 的存储目标。若该 group
	// 未配置存储目标，则返回错误。
	NewConfig(groupResource schema.GroupResource, example runtime.Object) (*storagebackend.ConfigForResource, error)

	// ResourcePrefix 返回 GroupResource 被覆盖的资源前缀
	// 这允许不同原生类型的资源共存，并集中控制
	// etcd 目录的结构
	ResourcePrefix(groupResource schema.GroupResource) string

	// Configs 获取所有已注册存储目标的配置。
	Configs() []storagebackend.Config

	// Backends 获取所有已注册存储目标的所有后端。
	// 用于获取所有实例以进行健康检查。
	// Deprecated: Use Configs instead
	//
	// 已弃用：请改用 Configs，再按需自行构造健康检查所需的连接信息。
	Backends() []Backend
}

// DefaultStorageFactory 接受 GroupResource 并返回其存储接口。结果包括：
// 1. 合并后的 etcd 配置，包括认证、服务器地址、前缀
// 2. 存储使用的资源编码：要存储为的 group/version/kind
// 3. 共存默认值：某些资源（如 hpa）通过多个 API 暴露，它们必须在 1 和 2 上保持一致
type DefaultStorageFactory struct {
	// StorageConfig 描述一般如何创建存储后端。
	// 其认证信息将用于每一个返回的 storage.Interface。
	StorageConfig storagebackend.Config

	// Overrides 按 GroupResource 保存差异化存储配置；键可以是具体资源，也可以是 "*" 组别名。
	Overrides map[schema.GroupResource]groupResourceOverrides

	// DefaultResourcePrefixes 为历史资源保留固定的 etcd 目录名，避免升级后数据"搬家"。
	DefaultResourcePrefixes map[schema.GroupResource]string

	// DefaultMediaType 是用于存储资源的媒体类型。未设置时使用 "application/json"。
	DefaultMediaType string

	// DefaultSerializer 用于为 storage.Interface 创建编码器与解码器。
	DefaultSerializer runtime.StorageSerializer

	// ResourceEncodingConfig 描述如何编码特定的 GroupVersionResource
	ResourceEncodingConfig ResourceEncodingConfig

	// APIResourceConfigSource 指示*存储*是否启用，而非 API 是否启用
	// 这与资源启用是两回事，配置方式留给调用方决定。
	//
	APIResourceConfigSource APIResourceConfigSource

	// newStorageCodecFn 存在是为了在单元测试中被覆盖。
	newStorageCodecFn func(opts StorageCodecConfig) (codec runtime.Codec, encodeVersioner runtime.GroupVersioner, err error)
}

// groupResourceOverrides 是单个 GroupResource 的存储覆盖项。
type groupResourceOverrides struct {
	// etcdLocation 包含用于特定 GroupResource 的"特殊"位置列表
	// 请求给定 GroupResource 的 storage.Interface 时，这些位置会覆盖在 StorageConfig 之上
	etcdLocation []string
	// etcdPrefix 是 GroupResource 的基础存储位置。
	etcdPrefix string
	// etcdResourcePrefix 是在 `etcdPrefix` 下存储特定类型的位置。
	// 为空时使用默认映射；若默认映射也没有对应条目，则使用
	// 资源的小写名称（不含组名）。
	etcdResourcePrefix string
	// mediaType 是希望选择的 serializer。为空时选择默认值。
	mediaType string
	// serializer 包含 GroupResource 的"特殊" serializer 列表。Resource=* 表示对整个组生效
	serializer runtime.StorageSerializer
	// cohabitatingResources 记录哪些资源必须存储在一起。当同一组概念
	// 有多种暴露方式时会出现这种情况，例如 autoscaling.HPA 与 extensions.HPA。
	// 切片的顺序很重要！它决定了查找存储位置的优先级。
	cohabitatingResources []schema.GroupResource
	// encoderDecoratorFn 是可选的，可在序列化前包装传入的 encoder。
	encoderDecoratorFn func(runtime.Encoder) runtime.Encoder
	// decoderDecoratorFn 是可选的，可包装传入的 decoder（添加新的 decoder）。返回
	// decoder 的顺序即尝试解码的优先级。
	decoderDecoratorFn func([]runtime.Decoder) []runtime.Decoder
}

// Apply 用覆盖配置中不为空的字段改写传入的 config 与 options。
//
// 零值字段表示"不覆盖"，因此无法用空字符串显式清空某项配置。
func (o groupResourceOverrides) Apply(config *storagebackend.Config, options *StorageCodecConfig) {
	if len(o.etcdLocation) > 0 {
		config.Transport.ServerList = o.etcdLocation
	}
	if len(o.etcdPrefix) > 0 {
		config.Prefix = o.etcdPrefix
	}

	if len(o.mediaType) > 0 {
		options.StorageMediaType = o.mediaType
	}
	if o.serializer != nil {
		options.StorageSerializer = o.serializer
	}
	if o.encoderDecoratorFn != nil {
		options.EncoderDecoratorFn = o.encoderDecoratorFn
	}
	if o.decoderDecoratorFn != nil {
		options.DecoderDecoratorFn = o.decoderDecoratorFn
	}
}

var _ StorageFactory = &DefaultStorageFactory{}

// AllResources 是"整个组"覆盖项的伪资源名。
const AllResources = "*"

// NewDefaultStorageFactory 构造默认存储工厂；未指定媒体类型时回退到 JSON。
func NewDefaultStorageFactory(
	config storagebackend.Config,
	defaultMediaType string,
	defaultSerializer runtime.StorageSerializer,
	resourceEncodingConfig ResourceEncodingConfig,
	resourceConfig APIResourceConfigSource,
	specialDefaultResourcePrefixes map[schema.GroupResource]string,
) *DefaultStorageFactory {
	if len(defaultMediaType) == 0 {
		defaultMediaType = runtime.ContentTypeJSON
	}
	return &DefaultStorageFactory{
		StorageConfig:           config,
		Overrides:               map[schema.GroupResource]groupResourceOverrides{},
		DefaultMediaType:        defaultMediaType,
		DefaultSerializer:       defaultSerializer,
		ResourceEncodingConfig:  resourceEncodingConfig,
		APIResourceConfigSource: resourceConfig,
		DefaultResourcePrefixes: specialDefaultResourcePrefixes,

		newStorageCodecFn: NewStorageCodec,
	}
}

// SetEtcdLocation 为指定资源设置独立的 etcd 端点列表。
func (s *DefaultStorageFactory) SetEtcdLocation(groupResource schema.GroupResource, location []string) {
	overrides := s.Overrides[groupResource]
	overrides.etcdLocation = location
	s.Overrides[groupResource] = overrides
}

// SetEtcdPrefix 为指定资源覆盖基础 key 前缀。
func (s *DefaultStorageFactory) SetEtcdPrefix(groupResource schema.GroupResource, prefix string) {
	overrides := s.Overrides[groupResource]
	overrides.etcdPrefix = prefix
	s.Overrides[groupResource] = overrides
}

// SetResourceEtcdPrefix 只设置资源级前缀，不动根目录；最终路径形如 `etcdPrefix/resourceEtcdPrefix`。
func (s *DefaultStorageFactory) SetResourceEtcdPrefix(groupResource schema.GroupResource, prefix string) {
	overrides := s.Overrides[groupResource]
	overrides.etcdResourcePrefix = prefix
	s.Overrides[groupResource] = overrides
}

// SetSerializer 为指定资源配置专用媒体类型与序列化器。
func (s *DefaultStorageFactory) SetSerializer(groupResource schema.GroupResource, mediaType string, serializer runtime.StorageSerializer) {
	overrides := s.Overrides[groupResource]
	overrides.mediaType = mediaType
	overrides.serializer = serializer
	s.Overrides[groupResource] = overrides
}

// AddCohabitatingResources 把多个资源关联到同一存储位置，切片顺序即查找优先级。
func (s *DefaultStorageFactory) AddCohabitatingResources(groupResources ...schema.GroupResource) {
	for _, groupResource := range groupResources {
		overrides := s.Overrides[groupResource]
		overrides.cohabitatingResources = groupResources
		s.Overrides[groupResource] = overrides
	}
}

// AddSerializationChains 为指定资源安装编解码装饰器（例如用于 protobuf/加密包装）。
func (s *DefaultStorageFactory) AddSerializationChains(encoderDecoratorFn func(runtime.Encoder) runtime.Encoder, decoderDecoratorFn func([]runtime.Decoder) []runtime.Decoder, groupResources ...schema.GroupResource) {
	for _, groupResource := range groupResources {
		overrides := s.Overrides[groupResource]
		overrides.encoderDecoratorFn = encoderDecoratorFn
		overrides.decoderDecoratorFn = decoderDecoratorFn
		s.Overrides[groupResource] = overrides
	}
}

// getAllResourcesAlias 返回组级别的通配键。
func getAllResourcesAlias(resource schema.GroupResource) schema.GroupResource {
	return schema.GroupResource{Group: resource.Group, Resource: AllResources}
}

// getStorageGroupResource 在共存资源中选择实际承载存储的那个：优先挑启用中的组，
// 保证被弃用的 API 组仍能读写同一份数据。
func (s *DefaultStorageFactory) getStorageGroupResource(groupResource schema.GroupResource) schema.GroupResource {
	for _, potentialStorageResource := range s.Overrides[groupResource].cohabitatingResources {
		// TODO(deads2k/liggitt): 确认新集群是否曾把共存资源存到别的位置。
		if s.APIResourceConfigSource.AnyResourceForGroupEnabled(potentialStorageResource.Group) {
			return potentialStorageResource
		}
	}

	return groupResource
}

// NewConfig 查找给定 group/resource 的存储目标；group 未配置存储目标时报错。
func (s *DefaultStorageFactory) NewConfig(groupResource schema.GroupResource, example runtime.Object) (*storagebackend.ConfigForResource, error) {
	chosenStorageResource := s.getStorageGroupResource(groupResource)

	// 必须操作副本，不能改动共享配置。
	// 必须复制：后续 Apply 会修改配置，不能污染工厂中共享的 StorageConfig。
	storageConfig := s.StorageConfig
	codecConfig := StorageCodecConfig{
		StorageMediaType:  s.DefaultMediaType,
		StorageSerializer: s.DefaultSerializer,
	}

	// 组级覆盖先应用，资源级覆盖后应用，后者优先级更高。
	if override, ok := s.Overrides[getAllResourcesAlias(chosenStorageResource)]; ok {
		override.Apply(&storageConfig, &codecConfig)
	}
	if override, ok := s.Overrides[chosenStorageResource]; ok {
		override.Apply(&storageConfig, &codecConfig)
	}

	var err error
	if backwardCompatibleInterface, ok := s.ResourceEncodingConfig.(CompatibilityResourceEncodingConfig); ok {
		// 兼容模式按对象示例选择存储版本，用于平滑迁移单个资源。
		codecConfig.StorageVersion, err = backwardCompatibleInterface.BackwardCompatibileStorageEncodingFor(chosenStorageResource, example)
		if err != nil {
			return nil, err
		}
	} else {
		codecConfig.StorageVersion, err = s.ResourceEncodingConfig.StorageEncodingFor(chosenStorageResource)
		if err != nil {
			return nil, err
		}
	}

	// 内存版本按请求的资源（而非存储资源）决定，确保返回给客户端的版本符合该 API 端点。
	codecConfig.MemoryVersion, err = s.ResourceEncodingConfig.InMemoryEncodingFor(groupResource)
	if err != nil {
		return nil, err
	}

	codecConfig.Config = storageConfig

	storageConfig.Codec, storageConfig.EncodeVersioner, err = s.newStorageCodecFn(codecConfig)
	if err != nil {
		return nil, err
	}
	klog.V(3).Infof("storing %v in %v, reading as %v from %#v", groupResource, codecConfig.StorageVersion, codecConfig.MemoryVersion, codecConfig.Config)

	return storageConfig.ForResource(groupResource), nil
}

// Configs 实现 StorageFactory 接口。
func (s *DefaultStorageFactory) Configs() []storagebackend.Config {
	return configs(s.StorageConfig, s.Overrides)
}

// Configs 返回所有已注册存储目标的配置。
func Configs(storageConfig storagebackend.Config) []storagebackend.Config {
	return configs(storageConfig, nil)
}

// 连同 group/resource 覆盖项的配置一起返回。
func configs(storageConfig storagebackend.Config, grOverrides map[schema.GroupResource]groupResourceOverrides) []storagebackend.Config {
	configs := []storagebackend.Config{storageConfig}

	for _, override := range grOverrides {
		if len(override.etcdLocation) == 0 {
			continue
		}
		// 复制一份再应用覆盖，避免污染基础配置。
		newConfig := storageConfig
		override.Apply(&newConfig, &StorageCodecConfig{})
		newConfig.Transport.ServerList = override.etcdLocation
		configs = append(configs, newConfig)
	}
	return configs
}

// Backends 实现 StorageFactory 接口。
func (s *DefaultStorageFactory) Backends() []Backend {
	return backends(s.StorageConfig, s.Overrides)
}

// Backends 返回所有已注册存储目标的后端，用于健康检查。
// Deprecated: Validate health by passing storagebackend.Config directly to storagefactory.CreateProber.
//
// 已弃用：此处返回的 TLS 配置仅宽松地用于连通性探测，不代表实际存储客户端的安全设置。
func Backends(storageConfig storagebackend.Config) []Backend {
	return backends(storageConfig, nil)
}

func backends(storageConfig storagebackend.Config, grOverrides map[schema.GroupResource]groupResourceOverrides) []Backend {
	servers := sets.NewString(storageConfig.Transport.ServerList...)

	for _, overrides := range grOverrides {
		servers.Insert(overrides.etcdLocation...)
	}

	tlsConfig := &tls.Config{
		InsecureSkipVerify: true,
	}
	if len(storageConfig.Transport.CertFile) > 0 && len(storageConfig.Transport.KeyFile) > 0 {
		cert, err := tls.LoadX509KeyPair(storageConfig.Transport.CertFile, storageConfig.Transport.KeyFile)
		if err != nil {
			klog.Errorf("failed to load key pair while getting backends: %s", err)
		} else {
			tlsConfig.Certificates = []tls.Certificate{cert}
		}
	}
	if len(storageConfig.Transport.TrustedCAFile) > 0 {
		if caCert, err := os.ReadFile(storageConfig.Transport.TrustedCAFile); err != nil {
			klog.Errorf("failed to read ca file while getting backends: %s", err)
		} else {
			caPool := x509.NewCertPool()
			caPool.AppendCertsFromPEM(caCert)
			tlsConfig.RootCAs = caPool
			// 配置了可信 CA 才做证书校验，否则保持跳过（仅用于探测）。
			tlsConfig.InsecureSkipVerify = false
		}
	}

	backends := []Backend{}
	for server := range servers {
		backends = append(backends, Backend{
			Server: server,
			// 各后端不能共享同一个 TLSConfig，否则会并发竞争。
			// For more details see: https://pr.k8s.io/59338
			TLSConfig: tlsConfig.Clone(),
		})
	}
	return backends
}

// ResourcePrefix 计算资源在 etcd 中的目录名，优先级：精确资源覆盖 > 组覆盖 > 特殊默认 > 小写资源名。
func (s *DefaultStorageFactory) ResourcePrefix(groupResource schema.GroupResource) string {
	chosenStorageResource := s.getStorageGroupResource(groupResource)
	groupOverride := s.Overrides[getAllResourcesAlias(chosenStorageResource)]
	exactResourceOverride := s.Overrides[chosenStorageResource]

	etcdResourcePrefix := s.DefaultResourcePrefixes[chosenStorageResource]
	if len(groupOverride.etcdResourcePrefix) > 0 {
		etcdResourcePrefix = groupOverride.etcdResourcePrefix
	}
	if len(exactResourceOverride.etcdResourcePrefix) > 0 {
		etcdResourcePrefix = exactResourceOverride.etcdResourcePrefix
	}
	if len(etcdResourcePrefix) == 0 {
		etcdResourcePrefix = strings.ToLower(chosenStorageResource.Resource)
	}

	return etcdResourcePrefix
}
