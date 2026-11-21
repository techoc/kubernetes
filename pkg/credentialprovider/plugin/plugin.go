/*
Copyright 2020 The Kubernetes Authors.

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

package plugin

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"golang.org/x/crypto/cryptobyte"
	"golang.org/x/sync/singleflight"

	authenticationv1 "k8s.io/api/authentication/v1"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	"k8s.io/apimachinery/pkg/runtime/serializer/json"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/client-go/tools/cache"
	"k8s.io/klog/v2"
	credentialproviderapi "k8s.io/kubelet/pkg/apis/credentialprovider"
	"k8s.io/kubelet/pkg/apis/credentialprovider/install"
	credentialproviderv1 "k8s.io/kubelet/pkg/apis/credentialprovider/v1"
	credentialproviderv1alpha1 "k8s.io/kubelet/pkg/apis/credentialprovider/v1alpha1"
	credentialproviderv1beta1 "k8s.io/kubelet/pkg/apis/credentialprovider/v1beta1"
	"k8s.io/kubernetes/pkg/credentialprovider"
	"k8s.io/kubernetes/pkg/features"
	kubeletconfig "k8s.io/kubernetes/pkg/kubelet/apis/config"
	kubeletconfigv1 "k8s.io/kubernetes/pkg/kubelet/apis/config/v1"
	kubeletconfigv1alpha1 "k8s.io/kubernetes/pkg/kubelet/apis/config/v1alpha1"
	kubeletconfigv1beta1 "k8s.io/kubernetes/pkg/kubelet/apis/config/v1beta1"
	"k8s.io/utils/clock"
)

const (
	// globalCacheKey 用于缓存不绑定仓库或镜像的凭证，尖括号形式避免与真实名称冲突。
	globalCacheKey     = "<global>"
	cachePurgeInterval = time.Minute * 15
)

var (
	scheme = runtime.NewScheme()
	codecs = serializer.NewCodecFactory(scheme, serializer.EnableStrict)

	apiVersions = map[string]schema.GroupVersion{
		credentialproviderv1alpha1.SchemeGroupVersion.String(): credentialproviderv1alpha1.SchemeGroupVersion,
		credentialproviderv1beta1.SchemeGroupVersion.String():  credentialproviderv1beta1.SchemeGroupVersion,
		credentialproviderv1.SchemeGroupVersion.String():       credentialproviderv1.SchemeGroupVersion,
	}
)

// GetServiceAccountFunc 按命名空间和名称查询 ServiceAccount。
type GetServiceAccountFunc func(namespace, name string) (*v1.ServiceAccount, error)

// getServiceAccountTokenFunc 为指定 ServiceAccount 创建令牌。
type getServiceAccountTokenFunc func(namespace, name string, tr *authenticationv1.TokenRequest) (*authenticationv1.TokenRequest, error)

// cacheKeyParams 汇总用于生成缓存键的上下文信息。
type cacheKeyParams struct {
	namespace          string
	serviceAccountName string
	serviceAccountUID  types.UID
	saAnnotations      map[string]string
	podName            string
	podUID             types.UID
	saTokenHash        string
	cacheType          kubeletconfig.ServiceAccountTokenCacheType
}

// cacheKeyBuilder 用带长度前缀的字节串拼接，避免字段内容拼接歧义。
type cacheKeyBuilder struct {
	builder *cryptobyte.Builder
}

// newCacheKeyBuilder 创建缓存键构造器。
func newCacheKeyBuilder() *cacheKeyBuilder {
	return &cacheKeyBuilder{builder: cryptobyte.NewBuilder(nil)}
}

// addString 追加带长度前缀的字符串。
func (c *cacheKeyBuilder) addString(value string) *cacheKeyBuilder {
	c.builder.AddUint16LengthPrefixed(func(b *cryptobyte.Builder) {
		b.AddBytes([]byte(value))
	})
	return c
}

// addAnnotations 按键排序后追加注解，保证生成的键稳定。
func (c *cacheKeyBuilder) addAnnotations(annotations map[string]string) *cacheKeyBuilder {
	keys := sets.StringKeySet(annotations).List()
	c.builder.AddUint32(uint32(len(keys)))
	for _, k := range keys {
		c.builder.AddUint16LengthPrefixed(func(b *cryptobyte.Builder) {
			b.AddBytes([]byte(k))
		})
		c.builder.AddUint16LengthPrefixed(func(b *cryptobyte.Builder) {
			b.AddBytes([]byte(annotations[k]))
		})
	}
	return c
}

// build 输出最终缓存键。
func (c *cacheKeyBuilder) build() (string, error) {
	keyBytes, err := c.builder.Bytes()
	if err != nil {
		return "", err
	}
	return string(keyBytes), nil
}

// init 安装凭证插件 API 与 kubelet 配置类型。
func init() {
	install.Install(scheme)
	kubeletconfig.AddToScheme(scheme)
	kubeletconfigv1alpha1.AddToScheme(scheme)
	kubeletconfigv1beta1.AddToScheme(scheme)
	kubeletconfigv1.AddToScheme(scheme)
}

// RegisterCredentialProviderPlugins 由 kubelet 调用，按配置文件注册外部凭证插件。
func RegisterCredentialProviderPlugins(pluginConfigPath, pluginBinDir string,
	getServiceAccountToken getServiceAccountTokenFunc,
	getServiceAccount GetServiceAccountFunc,
) error {
	if _, err := os.Stat(pluginBinDir); err != nil {
		if os.IsNotExist(err) {
			return fmt.Errorf("plugin binary directory %s did not exist", pluginBinDir)
		}

		return fmt.Errorf("error inspecting binary directory %s: %w", pluginBinDir, err)
	}

	credentialProviderConfig, configHash, err := readCredentialProviderConfig(pluginConfigPath)
	if err != nil {
		return err
	}

	saTokenForCredentialProvidersFeatureEnabled := utilfeature.DefaultFeatureGate.Enabled(features.KubeletServiceAccountTokenForCredentialProviders)
	if errs := validateCredentialProviderConfig(credentialProviderConfig, saTokenForCredentialProvidersFeatureEnabled); len(errs) > 0 {
		return fmt.Errorf("failed to validate credential provider config: %v", errs.ToAggregate())
	}

	// 注册发生在配置校验通过之后，避免暴露无效配置的状态。
	registerMetrics()

	// 配置哈希作为 metrics 标签暴露，便于监控配置变更。
	recordCredentialProviderConfigHash(configHash)

	for _, provider := range credentialProviderConfig.Providers {
		// LookPath 可处理 Windows 的 .exe 后缀，并同时完成可执行文件存在性检查。
		pluginBin, err := exec.LookPath(filepath.Join(pluginBinDir, provider.Name))
		if err != nil {
			if errors.Is(err, os.ErrNotExist) || errors.Is(err, exec.ErrNotFound) {
				return fmt.Errorf("plugin binary executable %s did not exist", pluginBin)
			}

			return fmt.Errorf("error inspecting binary executable %s: %w", pluginBin, err)
		}

		plugin, err := newPluginProvider(pluginBinDir, provider, getServiceAccountToken, getServiceAccount)
		if err != nil {
			return fmt.Errorf("error initializing plugin provider %s: %w", provider.Name, err)
		}

		registerCredentialProviderPlugin(provider.Name, plugin)
	}

	return nil
}

// newPluginProvider 根据配置创建插件提供者。
func newPluginProvider(pluginBinDir string, provider kubeletconfig.CredentialProvider,
	getServiceAccountToken getServiceAccountTokenFunc,
	getServiceAccount GetServiceAccountFunc,
) (*pluginProvider, error) {
	mediaType := "application/json"
	info, ok := runtime.SerializerInfoForMediaType(codecs.SupportedMediaTypes(), mediaType)
	if !ok {
		return nil, fmt.Errorf("unsupported media type %q", mediaType)
	}

	gv, ok := apiVersions[provider.APIVersion]
	if !ok {
		return nil, fmt.Errorf("invalid apiVersion: %q", provider.APIVersion)
	}

	clock := clock.RealClock{}
	return &pluginProvider{
		name:                 provider.Name,
		clock:                clock,
		matchImages:          provider.MatchImages,
		cache:                cache.NewExpirationStore(cacheKeyFunc, &cacheExpirationPolicy{clock: clock}),
		defaultCacheDuration: provider.DefaultCacheDuration.Duration,
		lastCachePurge:       clock.Now(),
		plugin: &execPlugin{
			name:         provider.Name,
			apiVersion:   provider.APIVersion,
			encoder:      codecs.EncoderForVersion(info.Serializer, gv),
			pluginBinDir: pluginBinDir,
			args:         provider.Args,
			envVars:      provider.Env,
			environ:      os.Environ,
		},
		serviceAccountProvider: newServiceAccountProvider(provider, getServiceAccount, getServiceAccountToken),
	}, nil
}

// pluginProvider 是基于外部插件的 DockerConfigProvider 实现。
type pluginProvider struct {
	name  string
	clock clock.Clock

	sync.Mutex

	group singleflight.Group

	// matchImages 定义插件适用的镜像 URL；不匹配的镜像不会从该插件获取凭证。
	matchImages []string

	// cache 保存凭证，过期时间由插件响应中的缓存时长决定。
	cache cache.Store
	// defaultCacheDuration 是插件未返回缓存时长时使用的默认内存缓存时长。
	defaultCacheDuration time.Duration

	// plugin 是凭证插件的 exec 实现。
	plugin Plugin

	// lastCachePurge 记录上次清理过期缓存的时间。
	lastCachePurge time.Time

	// serviceAccountProvider 在需要时处理 ServiceAccount 令牌。
	serviceAccountProvider *serviceAccountProvider
}

// serviceAccountProvider 在需要时获取 ServiceAccount 及其绑定令牌。
type serviceAccountProvider struct {
	audience                             string
	requireServiceAccount                bool
	getServiceAccountFunc                GetServiceAccountFunc
	getServiceAccountTokenFunc           getServiceAccountTokenFunc
	requiredServiceAccountAnnotationKeys []string
	optionalServiceAccountAnnotationKeys []string
	cacheType                            kubeletconfig.ServiceAccountTokenCacheType
}

// newServiceAccountProvider 按配置构造 ServiceAccount 提供者；未配置 audience 或特性关闭时返回 nil。
func newServiceAccountProvider(
	provider kubeletconfig.CredentialProvider,
	getServiceAccount GetServiceAccountFunc,
	getServiceAccountToken getServiceAccountTokenFunc,
) *serviceAccountProvider {
	featureGateEnabled := utilfeature.DefaultFeatureGate.Enabled(features.KubeletServiceAccountTokenForCredentialProviders)
	serviceAccountTokenAudienceSet := provider.TokenAttributes != nil && len(provider.TokenAttributes.ServiceAccountTokenAudience) > 0

	if !featureGateEnabled || !serviceAccountTokenAudienceSet {
		return nil
	}

	return &serviceAccountProvider{
		audience:                             provider.TokenAttributes.ServiceAccountTokenAudience,
		requireServiceAccount:                *provider.TokenAttributes.RequireServiceAccount,
		getServiceAccountFunc:                getServiceAccount,
		getServiceAccountTokenFunc:           getServiceAccountToken,
		requiredServiceAccountAnnotationKeys: provider.TokenAttributes.RequiredServiceAccountAnnotationKeys,
		optionalServiceAccountAnnotationKeys: provider.TokenAttributes.OptionalServiceAccountAnnotationKeys,
		cacheType:                            provider.TokenAttributes.CacheType,
	}
}

// requiredAnnotationNotFoundError 表示缺少必需的 ServiceAccount 注解。
type requiredAnnotationNotFoundError string

// Error 实现 error 接口。
func (e requiredAnnotationNotFoundError) Error() string {
	return fmt.Sprintf("required annotation %s not found", string(e))
}

// isRequiredAnnotationNotFoundError 判断错误是否由缺少必需注解引起。
func isRequiredAnnotationNotFoundError(err error) bool {
	var requiredAnnotationNotFoundErr requiredAnnotationNotFoundError
	return errors.As(err, &requiredAnnotationNotFoundErr)
}

// getServiceAccountData 返回 ServiceAccount UID 及插件所需注解。
// 缺失任一必需注解时不调用插件；可选注解仅在存在时加入 saAnnotations。
func (s *serviceAccountProvider) getServiceAccountData(namespace, name string) (types.UID, map[string]string, error) {
	sa, err := s.getServiceAccountFunc(namespace, name)
	if err != nil {
		return "", nil, err
	}

	saAnnotations := make(map[string]string, len(s.requiredServiceAccountAnnotationKeys)+len(s.optionalServiceAccountAnnotationKeys))
	for _, k := range s.requiredServiceAccountAnnotationKeys {
		val, ok := sa.Annotations[k]
		// 缺少必需注解视为工作负载未选择该凭证流程，跳过插件调用。
		if !ok {
			return "", nil, requiredAnnotationNotFoundError(k)
		}
		saAnnotations[k] = val
	}

	for _, k := range s.optionalServiceAccountAnnotationKeys {
		if val, ok := sa.Annotations[k]; ok {
			saAnnotations[k] = val
		}
	}

	return sa.UID, saAnnotations, nil
}

// getServiceAccountToken 为 ServiceAccount 创建绑定到 Pod 的令牌。
func (s *serviceAccountProvider) getServiceAccountToken(podNamespace, podName, serviceAccountName string, serviceAccountUID, podUID types.UID) (string, error) {
	tr, err := s.getServiceAccountTokenFunc(podNamespace, serviceAccountName, &authenticationv1.TokenRequest{
		ObjectMeta: metav1.ObjectMeta{
			UID: serviceAccountUID,
		},
		Spec: authenticationv1.TokenRequestSpec{
			Audiences: []string{s.audience},
			// 不显式设置过期时间，沿用 TokenRequestSpec 的默认值。
			BoundObjectRef: &authenticationv1.BoundObjectReference{
				APIVersion: "v1",
				Kind:       "Pod",
				Name:       podName,
				UID:        podUID,
			},
		},
	})

	if err != nil {
		return "", err
	}

	return tr.Status.Token, nil
}

// cacheEntry 是存入 cache.Store 的缓存对象。
type cacheEntry struct {
	key         string
	credentials credentialprovider.DockerConfig
	expiresAt   time.Time
}

// cacheKeyFunc 从缓存对象中提取键。
func cacheKeyFunc(obj interface{}) (string, error) {
	key := obj.(*cacheEntry).key
	return key, nil
}

// cacheExpirationPolicy 按 expiresAt 时间戳判断缓存是否过期。
type cacheExpirationPolicy struct {
	clock clock.Clock
}

// IsExpired 判断当前时间是否超过插件响应指定的缓存过期时间。
func (c *cacheExpirationPolicy) IsExpired(entry *cache.TimestampedEntry) bool {
	return c.clock.Now().After(entry.Obj.(*cacheEntry).expiresAt)
}

// perPodPluginProvider 把共享的 pluginProvider 与单次请求的 Pod 信息绑定，
// 使插件在 SA 令牌模式下能获取 Pod 的 ServiceAccount 并生成绑定令牌。
type perPodPluginProvider struct {
	provider *pluginProvider

	podNamespace string
	podName      string
	podUID       types.UID

	serviceAccountName string
}

// provideWithCoordinates 返回凭证及可用的 ServiceAccount 坐标；坐标为 nil 表示未使用 SA 上下文。
func (p *perPodPluginProvider) provideWithCoordinates(image string) (credentialprovider.DockerConfig, *credentialprovider.ServiceAccountCoordinates) {
	credentials, coordinates, err := p.provider.provide(image, p.podNamespace, p.podName, p.podUID, p.serviceAccountName)
	if err == nil {
		return credentials, coordinates
	}

	// 取凭证失败时记录日志并返回空配置，避免单个插件阻塞镜像拉取。
	if p.provider.serviceAccountProvider != nil {
		klog.ErrorS(err, "Failed to provide credentials for image", "provider", p.provider.name, "image", image, "pod", klog.KRef(p.podNamespace, p.podName), "podUID", p.podUID, "serviceAccount", klog.KRef(p.podNamespace, p.serviceAccountName))
	} else {
		klog.ErrorS(err, "Failed to provide credentials for image", "provider", p.provider.name, "image", image)
	}

	return credentialprovider.DockerConfig{}, nil
}

// provide 从缓存或 exec 插件获取凭证；未使用 SA 上下文时返回的坐标为 nil。
func (p *pluginProvider) provide(image, podNamespace, podName string, podUID types.UID, serviceAccountName string) (credentialprovider.DockerConfig, *credentialprovider.ServiceAccountCoordinates, error) {
	// 镜像不匹配任何 matchImages 时不调用插件。
	if !p.isImageAllowed(image) {
		return credentialprovider.DockerConfig{}, nil, nil
	}

	var serviceAccountUID types.UID
	var serviceAccountToken string
	var saAnnotations map[string]string
	var err error
	var serviceAccountCacheKey string
	var serviceAccountTokenHash string
	var serviceAccountCoordinates *credentialprovider.ServiceAccountCoordinates

	if p.serviceAccountProvider != nil {
		if len(serviceAccountName) == 0 && p.serviceAccountProvider.requireServiceAccount {
			klog.V(5).InfoS("Service account name is empty", "provider", p.name, "image", image, "pod", klog.KRef(podNamespace, podName), "podUID", podUID)
			return credentialprovider.DockerConfig{}, nil, nil
		}

		// 允许无 SA 调用时继续（例如静态 Pod），此时不生成令牌。
		if len(serviceAccountName) > 0 {
			if serviceAccountUID, saAnnotations, err = p.serviceAccountProvider.getServiceAccountData(podNamespace, serviceAccountName); err != nil {
				if isRequiredAnnotationNotFoundError(err) {
					// 必需注解可视为工作负载的显式选择；缺失时跳过插件，并降低日志级别以免刷屏。
					klog.V(5).ErrorS(err, "Failed to get service account data", "provider", p.name, "image", image, "pod", klog.KRef(podNamespace, podName), "podUID", podUID, "serviceAccount", klog.KRef(podNamespace, serviceAccountName))
					return credentialprovider.DockerConfig{}, nil, nil
				}

				return credentialprovider.DockerConfig{}, nil, fmt.Errorf("failed to get service account: %w", err)
			}

			if serviceAccountToken, err = p.serviceAccountProvider.getServiceAccountToken(podNamespace, podName, serviceAccountName, serviceAccountUID, podUID); err != nil {
				return credentialprovider.DockerConfig{}, nil, fmt.Errorf("failed to get service account token: %w", err)
			}
			serviceAccountTokenHash = getHashIfNotEmpty(serviceAccountToken)

			c := cacheKeyParams{
				namespace:          podNamespace,
				serviceAccountName: serviceAccountName,
				serviceAccountUID:  serviceAccountUID,
				saAnnotations:      saAnnotations,
				podName:            podName,
				podUID:             podUID,
				saTokenHash:        serviceAccountTokenHash,
				cacheType:          p.serviceAccountProvider.cacheType,
			}

			serviceAccountCacheKey, err = generateServiceAccountCacheKey(c)
			if err != nil {
				return credentialprovider.DockerConfig{}, nil, fmt.Errorf("error generating service account cache key: %w", err)
			}

			serviceAccountCoordinates = &credentialprovider.ServiceAccountCoordinates{
				UID:       string(serviceAccountUID),
				Namespace: podNamespace,
				Name:      serviceAccountName,
			}
		}
	}

	// 命中缓存即可跳过昂贵的插件调用。
	cachedConfig, found, errCache := p.getCachedCredentials(image, serviceAccountCacheKey)
	if errCache != nil {
		return credentialprovider.DockerConfig{}, nil, fmt.Errorf("failed to get cached docker config: %w", errCache)
	}

	if found {
		return cachedConfig, serviceAccountCoordinates, nil
	}

	// singleflight 合并同一镜像的并发插件调用。由于此时尚不知道 cacheKeyType，
	// 若插件按 registry/global 缓存，foo.bar.registry 与 foo.bar.registry/image1 等仍可能各自触发执行。
	// SA 令牌模式下键还包含 SA 命名空间、名称、UID 与注解生成的 serviceAccountCacheKey。
	singleFlightKey := image
	if p.serviceAccountProvider != nil && len(serviceAccountName) > 0 {
		// SA 令牌模式下键包含令牌与注解；即使同一工作负载拉取同一镜像，不同请求也可能无法合并。
		if singleFlightKey, err = generateSingleFlightKey(image, serviceAccountTokenHash, saAnnotations); err != nil {
			return credentialprovider.DockerConfig{}, nil, fmt.Errorf("error generating singleflight key: %w", err)
		}
	}
	res, err, _ := p.group.Do(singleFlightKey, func() (interface{}, error) {
		return p.plugin.ExecPlugin(context.Background(), image, serviceAccountToken, saAnnotations)
	})

	if err != nil {
		return credentialprovider.DockerConfig{}, nil, fmt.Errorf("failed to get credential from external registry credential provider: %w", err)
	}

	response, ok := res.(*credentialproviderapi.CredentialProviderResponse)
	if !ok {
		return credentialprovider.DockerConfig{}, nil, fmt.Errorf("invalid response type returned by external credential provider: %T", res)
	}
	if len(serviceAccountToken) > 0 && p.serviceAccountProvider.cacheType != kubeletconfig.TokenServiceAccountTokenCacheType {
		// 非 Token 缓存类型不允许把服务账号令牌原样当密码返回，避免凭证扩散。
		for _, authConfig := range response.Auth {
			if authConfig.Password == serviceAccountToken {
				return credentialprovider.DockerConfig{}, nil, fmt.Errorf("credential provider plugin returned the service account token as the password which is not allowed when service account cache type is not set to 'Token'")
			}
		}
	}

	var cacheKey string
	switch cacheKeyType := response.CacheKeyType; cacheKeyType {
	case credentialproviderapi.ImagePluginCacheKeyType:
		cacheKey = image
	case credentialproviderapi.RegistryPluginCacheKeyType:
		registry := parseRegistry(image)
		cacheKey = registry
	case credentialproviderapi.GlobalPluginCacheKeyType:
		cacheKey = globalCacheKey
	default:
		return credentialprovider.DockerConfig{}, nil, fmt.Errorf("credential provider plugin did not return a valid cacheKeyType: %s", cacheKeyType)
	}

	dockerConfig := make(credentialprovider.DockerConfig, len(response.Auth))
	for matchImage, authConfig := range response.Auth {
		dockerConfig[matchImage] = credentialprovider.DockerConfigEntry{
			Username: authConfig.Username,
			Password: authConfig.Password,
		}
	}

	// 显式 0 表示完全禁用缓存。
	if response.CacheDuration != nil && response.CacheDuration.Duration == 0 {
		return dockerConfig, serviceAccountCoordinates, nil
	}

	var expiresAt time.Time
	// 未指定缓存时长时使用配置的默认值。
	if response.CacheDuration == nil {
		if p.defaultCacheDuration == 0 {
			return dockerConfig, serviceAccountCoordinates, nil
		}
		expiresAt = p.clock.Now().Add(p.defaultCacheDuration)
	} else {
		expiresAt = p.clock.Now().Add(response.CacheDuration.Duration)
	}

	cacheKey, err = generateCacheKey(cacheKey, serviceAccountCacheKey)
	if err != nil {
		return credentialprovider.DockerConfig{}, nil, fmt.Errorf("error generating cache key: %w", err)
	}

	cachedEntry := &cacheEntry{
		key:         cacheKey,
		credentials: dockerConfig,
		expiresAt:   expiresAt,
	}

	if err := p.cache.Add(cachedEntry); err != nil {
		// 缓存写入失败不影响本次凭证返回，仅记录错误。
		return dockerConfig, serviceAccountCoordinates, fmt.Errorf("failed to add credentials to cache: %w", err)
	}

	return dockerConfig, serviceAccountCoordinates, nil
}

// isImageAllowed 判断镜像是否命中插件配置的 matchImages。
func (p *pluginProvider) isImageAllowed(image string) bool {
	for _, matchImage := range p.matchImages {
		if matched, _ := credentialprovider.URLsMatchStr(matchImage, image); matched {
			return true
		}
	}

	return false
}

// getCachedCredentials 按镜像、仓库、全局优先级查找缓存凭证。
func (p *pluginProvider) getCachedCredentials(image, serviceAccountCacheKey string) (credentialprovider.DockerConfig, bool, error) {
	p.Lock()
	// 定期触发 List 以清理过期项；否则这些条目只会在被访问时才删除。
	if p.clock.Now().After(p.lastCachePurge.Add(cachePurgeInterval)) {
		// 过期项仅在 Get/List 时清理，故周期性调用 List 回收不会再被访问的条目。
		_ = p.cache.List()
		p.lastCachePurge = p.clock.Now()
	}
	p.Unlock()

	baseCacheKeyCandidates := []string{
		image,
		parseRegistry(image),
		globalCacheKey,
	}

	// 按镜像、仓库、全局的顺序查找，优先使用最具体的缓存。
	for _, baseCacheKey := range baseCacheKeyCandidates {
		if config, found, err := p.lookupCredentialsInCache(baseCacheKey, serviceAccountCacheKey); err != nil {
			return nil, false, err
		} else if found {
			return config, true, nil
		}
	}

	return nil, false, nil
}

// lookupCredentialsInCache 用基础键与 SA 缓存键执行一次缓存查询。
func (p *pluginProvider) lookupCredentialsInCache(baseKey, serviceAccountCacheKey string) (credentialprovider.DockerConfig, bool, error) {
	cacheKey, err := generateCacheKey(baseKey, serviceAccountCacheKey)
	if err != nil {
		return nil, false, fmt.Errorf("error generating cache key: %w", err)
	}

	obj, found, err := p.cache.GetByKey(cacheKey)
	if err != nil {
		return nil, false, err
	}

	if found {
		return obj.(*cacheEntry).credentials, true, nil
	}

	return nil, false, nil
}

// Plugin 封装 ExecPlugin 调用，便于测试时无需真正启动外部进程。
type Plugin interface {
	ExecPlugin(ctx context.Context, image, serviceAccountToken string, serviceAccountAnnotations map[string]string) (*credentialproviderapi.CredentialProviderResponse, error)
}

// execPlugin 是 Plugin 的 exec 实现，按配置中的名称在 kubelet 的插件目录中查找可执行文件。
type execPlugin struct {
	name         string
	apiVersion   string
	encoder      runtime.Encoder
	args         []string
	envVars      []kubeletconfig.ExecEnvVar
	pluginBinDir string
	environ      func() []string
}

// ExecPlugin 按配置中的参数与环境变量执行插件二进制：
//
//	$ ENV_NAME=ENV_VALUE <plugin-name> args[0] args[1] <<<request
//
// 插件从 stdin 接收 CredentialProviderRequest，并通过 stdout 返回 CredentialProviderResponse。
func (e *execPlugin) ExecPlugin(ctx context.Context, image, serviceAccountToken string, serviceAccountAnnotations map[string]string) (*credentialproviderapi.CredentialProviderResponse, error) {
	klog.V(5).InfoS("Getting image credentials from external exec plugin", "pluginName", e.name, "image", image)

	authRequest := &credentialproviderapi.CredentialProviderRequest{Image: image, ServiceAccountToken: serviceAccountToken, ServiceAccountAnnotations: serviceAccountAnnotations}
	data, err := e.encodeRequest(authRequest)
	if err != nil {
		return nil, fmt.Errorf("failed to encode auth request: %w", err)
	}

	stdout := &bytes.Buffer{}
	stderr := &bytes.Buffer{}
	stdin := bytes.NewBuffer(data)

	// 统一设置 1 分钟超时：既给插件重试留出余量，又防止进程永久挂起。
	ctx, cancel := context.WithTimeout(ctx, 1*time.Minute)
	defer cancel()

	cmd := exec.CommandContext(ctx, filepath.Join(e.pluginBinDir, e.name), e.args...)
	cmd.Stdout, cmd.Stderr, cmd.Stdin = stdout, stderr, stdin

	var configEnvVars []string
	for _, v := range e.envVars {
		configEnvVars = append(configEnvVars, fmt.Sprintf("%s=%s", v.Name, v.Value))
	}

	// 合并系统环境变量与配置中的变量，缺失系统变量会导致插件执行失败
	// （见 https://github.com/kubernetes/kubernetes/issues/102750），也符合配置规范。
	cmd.Env = mergeEnvVars(e.environ(), configEnvVars)

	if err = e.runPlugin(ctx, cmd, image); err != nil {
		return nil, fmt.Errorf("%w: %s", err, stderr.String())
	}

	data = stdout.Bytes()
	// 校验响应的 apiVersion，防止误用其他版本的响应。
	gvk, err := json.DefaultMetaFactory.Interpret(data)
	if err != nil {
		return nil, fmt.Errorf("error reading GVK from response: %w", err)
	}

	if gvk.GroupVersion().String() != e.apiVersion {
		return nil, fmt.Errorf("apiVersion from credential plugin response did not match expected apiVersion:%s, actual apiVersion:%s", e.apiVersion, gvk.GroupVersion().String())
	}

	response, err := e.decodeResponse(data)
	if err != nil {
		// 不包装 err，以免错误信息泄露响应中的凭证。
		return nil, errors.New("error decoding credential provider plugin response from stdout")
	}

	return response, nil
}

// runPlugin 执行插件二进制并记录耗时与错误指标。
func (e *execPlugin) runPlugin(ctx context.Context, cmd *exec.Cmd, image string) error {
	startTime := time.Now()
	defer func() {
		kubeletCredentialProviderPluginDuration.WithLabelValues(e.name).Observe(time.Since(startTime).Seconds())
	}()

	err := cmd.Run()
	if ctx.Err() != nil {
		kubeletCredentialProviderPluginErrors.WithLabelValues(e.name).Inc()
		return fmt.Errorf("error execing credential provider plugin %s for image %s: %w", e.name, image, ctx.Err())
	}
	if err != nil {
		kubeletCredentialProviderPluginErrors.WithLabelValues(e.name).Inc()
		return fmt.Errorf("error execing credential provider plugin %s for image %s: %w", e.name, image, err)
	}
	return nil
}

// encodeRequest 把内部 CredentialProviderRequest 编码为 JSON。
func (e *execPlugin) encodeRequest(request *credentialproviderapi.CredentialProviderRequest) ([]byte, error) {
	data, err := runtime.Encode(e.encoder, request)
	if err != nil {
		return nil, fmt.Errorf("error encoding request: %w", err)
	}

	return data, nil
}

// decodeResponse 把响应数据解码为内部 CredentialProviderResponse 类型。
func (e *execPlugin) decodeResponse(data []byte) (*credentialproviderapi.CredentialProviderResponse, error) {
	obj, gvk, err := codecs.UniversalDecoder().Decode(data, nil, nil)
	if err != nil {
		return nil, err
	}

	if gvk.Kind != "CredentialProviderResponse" {
		return nil, fmt.Errorf("failed to decode CredentialProviderResponse, unexpected Kind: %q", gvk.Kind)
	}

	if gvk.Group != credentialproviderapi.GroupName {
		return nil, fmt.Errorf("failed to decode CredentialProviderResponse, unexpected Group: %s", gvk.Group)
	}

	if internalResponse, ok := obj.(*credentialproviderapi.CredentialProviderResponse); ok {
		return internalResponse, nil
	}

	return nil, fmt.Errorf("unable to convert %T to *CredentialProviderResponse", obj)
}

// parseRegistry 提取镜像的仓库主机名（含端口）。
func parseRegistry(image string) string {
	imageParts := strings.Split(image, "/")
	return imageParts[0]
}

// mergedEnvVars 用凭证插件配置的环境变量覆盖系统同名变量，允许用户显式改写系统值。
func mergeEnvVars(sysEnvVars, credProviderVars []string) []string {
	mergedEnvVars := sysEnvVars
	mergedEnvVars = append(mergedEnvVars, credProviderVars...)
	return mergedEnvVars
}

// generateServiceAccountCacheKey 按 SA 上下文生成缓存键；Token 模式额外纳入令牌哈希。
func generateServiceAccountCacheKey(params cacheKeyParams) (string, error) {
	builder := newCacheKeyBuilder().
		addString(params.namespace).
		addString(params.serviceAccountName).
		addString(string(params.serviceAccountUID)).
		addAnnotations(params.saAnnotations)

	if params.cacheType == kubeletconfig.TokenServiceAccountTokenCacheType {
		builder = builder.addString("token").addString(params.saTokenHash)
	}

	return builder.build()
}

// generateCacheKey 把基础键与 SA 缓存键拼接为最终键。
func generateCacheKey(baseKey, serviceAccountCacheKey string) (string, error) {
	builder := newCacheKeyBuilder().
		addString(baseKey).
		addString(serviceAccountCacheKey)

	return builder.build()
}

// generateSingleFlightKey 生成合并并发插件调用所用的键。
func generateSingleFlightKey(image, saTokenHash string, saAnnotations map[string]string) (string, error) {
	builder := newCacheKeyBuilder().
		addString(image).
		addString(saTokenHash).
		addAnnotations(saAnnotations)

	return builder.build()
}

// getHashIfNotEmpty 在数据非空时返回其 SHA256。
func getHashIfNotEmpty(data string) string {
	if len(data) > 0 {
		return fmt.Sprintf("sha256:%x", sha256.Sum256([]byte(data)))
	}
	return ""
}
