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

package apiserver

import (
	"context"
	"crypto/tls"
	"fmt"
	"net/http"
	"time"

	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	"k8s.io/apimachinery/pkg/runtime/serializer/cbor"
	utilnet "k8s.io/apimachinery/pkg/util/net"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/apiserver/pkg/admission"
	"k8s.io/apiserver/pkg/authorization/authorizer"
	"k8s.io/apiserver/pkg/endpoints/discovery/aggregated"
	openapinamer "k8s.io/apiserver/pkg/endpoints/openapi"
	genericfeatures "k8s.io/apiserver/pkg/features"
	peerreconcilers "k8s.io/apiserver/pkg/reconcilers"
	genericapiserver "k8s.io/apiserver/pkg/server"
	"k8s.io/apiserver/pkg/server/egressselector"
	"k8s.io/apiserver/pkg/server/filters"
	serverstorage "k8s.io/apiserver/pkg/server/storage"
	"k8s.io/apiserver/pkg/util/compatibility"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/apiserver/pkg/util/openapi"
	utilpeerproxy "k8s.io/apiserver/pkg/util/peerproxy"
	"k8s.io/apiserver/pkg/util/proxy"
	"k8s.io/client-go/dynamic"
	clientgoinformers "k8s.io/client-go/informers"
	clientgoclientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/keyutil"
	basecompatibility "k8s.io/component-base/compatibility"
	metricsfeatures "k8s.io/component-base/metrics/features"
	aggregatorapiserver "k8s.io/kube-aggregator/pkg/apiserver"
	openapicommon "k8s.io/kube-openapi/pkg/common"
	"k8s.io/kubernetes/pkg/api/legacyscheme"
	controlplaneadmission "k8s.io/kubernetes/pkg/controlplane/apiserver/admission"
	"k8s.io/kubernetes/pkg/controlplane/apiserver/options"
	"k8s.io/kubernetes/pkg/controlplane/controller/clusterauthenticationtrust"
	"k8s.io/kubernetes/pkg/kubeapiserver"
	"k8s.io/kubernetes/pkg/kubeapiserver/authorizer/modes"
	rbacrest "k8s.io/kubernetes/pkg/registry/rbac/rest"
	"k8s.io/kubernetes/pkg/serviceaccount"
)

// Config 定义 master（控制面）的配置。
type Config struct {
	Generic *genericapiserver.Config
	Extra
}

// Extra 保存控制面特有的、非通用 API 服务器的配置项。
type Extra struct {
	ClusterAuthenticationInfo clusterauthenticationtrust.ClusterAuthenticationInfo

	APIResourceConfigSource serverstorage.APIResourceConfigSource
	StorageFactory          serverstorage.StorageFactory
	EventTTL                time.Duration

	EnableLogsSupport bool
	ProxyTransport    *http.Transport

	// EndpointSliceGetter 用于查找 EndpointSlice 以进行 IP 路由；为 nil 时不可用。
	EndpointSliceGetter proxy.EndpointSliceGetter

	// PeerProxy 在 kube-apiserver 之间为无法本地处理的请求提供代理通道。
	PeerProxy utilpeerproxy.Interface
	// PeerEndpointReconcileInterval 定义 etcd 中对等端点租约的重建频率。
	PeerEndpointReconcileInterval time.Duration
	// PeerEndpointLeaseReconciler 更新对等端点租约。
	PeerEndpointLeaseReconciler peerreconcilers.PeerEndpointLeaseReconciler
	// PeerAdvertiseAddress 是本 apiserver 通告给对等节点的 IP，供版本偏差导致对端无法本地服务时路由请求；未设置时使用 AdvertiseAddress/BindAddress。
	PeerAdvertiseAddress peerreconcilers.PeerAdvertiseAddress

	ServiceAccountIssuer                serviceaccount.TokenGenerator
	ServiceAccountMaxExpiration         time.Duration
	ServiceAccountExtendedMaxExpiration time.Duration
	ExtendExpiration                    bool

	// 以下字段用于 ServiceAccount issuer 发现。
	ServiceAccountIssuerURL        string
	ServiceAccountJWKSURI          string
	ServiceAccountPublicKeysGetter serviceaccount.PublicKeysGetter

	SystemNamespaces []string

	VersionedInformers clientgoinformers.SharedInformerFactory

	// 协调式 Leader Election 的定时器配置。
	CoordinatedLeadershipLeaseDuration time.Duration
	CoordinatedLeadershipRenewDeadline time.Duration
	CoordinatedLeadershipRetryPeriod   time.Duration
}

// BuildGenericConfig 根据通用控制面选项生成 genericapiserver.Config，该配置通常由多个委托 apiserver 共享。
func BuildGenericConfig(
	s options.CompletedOptions,
	schemes []*runtime.Scheme,
	resourceConfig *serverstorage.ResourceConfig,
	getOpenAPIDefinitions func(ref openapicommon.ReferenceCallback) map[string]openapicommon.OpenAPIDefinition,
	informerName *cache.InformerName,
) (
	genericConfig *genericapiserver.Config,
	versionedInformers clientgoinformers.SharedInformerFactory,
	storageFactory *serverstorage.DefaultStorageFactory,
	lastErr error,
) {
	codecs := legacyscheme.Codecs
	if utilfeature.DefaultFeatureGate.Enabled(genericfeatures.CBORServingAndStorage) {
		codecs = serializer.NewCodecFactory(legacyscheme.Scheme, serializer.WithSerializer(cbor.NewSerializerInfo))
	}
	genericConfig = genericapiserver.NewConfig(codecs)
	genericConfig.Flagz = s.Flagz
	genericConfig.MergedResourceConfig = resourceConfig

	if lastErr = s.GenericServerRunOptions.ApplyTo(genericConfig); lastErr != nil {
		return
	}

	if lastErr = s.SecureServing.ApplyToConfig(genericConfig); lastErr != nil {
		return
	}

	// 自通信使用 protobuf：并非所有通用 apiserver 都支持 protobuf，因此需在 kube-apiserver 显式设置。
	genericConfig.LoopbackClientConfig.ContentConfig.ContentType = "application/vnd.kubernetes.protobuf"
	// 本地回环走快速网络，禁用压缩以减少开销。
	genericConfig.LoopbackClientConfig.DisableCompression = true

	kubeClientConfig := genericConfig.LoopbackClientConfig
	clientgoExternalClient, err := clientgoclientset.NewForConfig(kubeClientConfig)
	if err != nil {
		lastErr = fmt.Errorf("failed to create real external clientset: %w", err)
		return
	}
	trim := func(obj interface{}) (interface{}, error) {
		if accessor, err := meta.Accessor(obj); err == nil && accessor.GetManagedFields() != nil {
			accessor.SetManagedFields(nil)
		}
		return obj, nil
	}
	versionedInformers = clientgoinformers.NewSharedInformerFactoryWithOptions(
		clientgoExternalClient,
		10*time.Minute,
		clientgoinformers.WithTransform(trim),
		clientgoinformers.WithInformerName(informerName),
	)

	if lastErr = s.Features.ApplyTo(genericConfig, clientgoExternalClient, versionedInformers); lastErr != nil {
		return
	}
	if lastErr = s.APIEnablement.ApplyTo(genericConfig, resourceConfig, legacyscheme.Scheme); lastErr != nil {
		return
	}
	if lastErr = s.EgressSelector.ApplyTo(genericConfig); lastErr != nil {
		return
	}
	if lastErr = s.Traces.ApplyTo(genericConfig.EgressSelector, genericConfig); lastErr != nil {
		return
	}
	// 过滤掉被禁用特性带来的 OpenAPI 定义变更。
	getOpenAPIDefinitions = openapi.GetOpenAPIDefinitionsWithoutDisabledFeatures(getOpenAPIDefinitions)
	namer := openapinamer.NewDefinitionNamer(schemes...)
	genericConfig.OpenAPIConfig = genericapiserver.DefaultOpenAPIConfig(getOpenAPIDefinitions, namer)
	genericConfig.OpenAPIConfig.Info.Title = "Kubernetes"
	genericConfig.OpenAPIV3Config = genericapiserver.DefaultOpenAPIV3Config(getOpenAPIDefinitions, namer)
	genericConfig.OpenAPIV3Config.Info.Title = "Kubernetes"

	genericConfig.LongRunningFunc = filters.BasicLongRunningRequestCheck(
		sets.NewString("watch", "proxy"),
		sets.NewString("attach", "exec", "proxy", "log", "portforward"),
	)

	if genericConfig.EgressSelector != nil {
		s.Etcd.StorageConfig.Transport.EgressLookup = genericConfig.EgressSelector.Lookup
	}
	s.Etcd.StorageConfig.Transport.TracerProvider = genericConfig.TracerProvider

	storageFactoryConfig := kubeapiserver.NewStorageFactoryConfigEffectiveVersion(genericConfig.EffectiveVersion)
	storageFactoryConfig.APIResourceConfig = genericConfig.MergedResourceConfig
	storageFactoryConfig.DefaultResourceEncoding.SetEffectiveVersion(genericConfig.EffectiveVersion)
	storageFactory, lastErr = storageFactoryConfig.Complete(s.Etcd).New()
	if lastErr != nil {
		return
	}
	// storageFactory 的 StorageConfig 从 etcd 选项拷贝而来，其中 StorageObjectCountTracker 仍为 nil，需从 genericConfig 补齐。
	storageFactory.StorageConfig.StorageObjectCountTracker = genericConfig.StorageObjectCountTracker
	if lastErr = s.Etcd.ApplyWithStorageFactoryTo(storageFactory, genericConfig); lastErr != nil {
		return
	}

	ctx := wait.ContextForChannel(genericConfig.DrainedNotify())

	// Authentication.ApplyTo 依赖已应用的 OpenAPIConfig 与 EgressSelector，必须在它们之后调用。
	if lastErr = s.Authentication.ApplyTo(ctx, &genericConfig.Authentication, genericConfig.SecureServing, genericConfig.EgressSelector, genericConfig.OpenAPIConfig, genericConfig.OpenAPIV3Config, clientgoExternalClient, versionedInformers, genericConfig.APIServerID); lastErr != nil {
		return
	}

	var enablesRBAC bool
	genericConfig.Authorization.Authorizer, genericConfig.RuleResolver, enablesRBAC, err = BuildAuthorizer(
		ctx,
		s,
		genericConfig.EgressSelector,
		genericConfig.APIServerID,
		versionedInformers,
	)
	if err != nil {
		lastErr = fmt.Errorf("invalid authorization config: %w", err)
		return
	}
	if s.Authorization != nil && !enablesRBAC {
		genericConfig.DisabledPostStartHooks.Insert(rbacrest.PostStartHookName)
	}

	lastErr = s.Audit.ApplyTo(genericConfig)
	if lastErr != nil {
		return
	}

	genericConfig.AggregatedDiscoveryGroupManager = aggregated.NewResourceManager("apis")

	return
}

// BuildAuthorizer 构造授权器；若 s 中未配置授权，则返回 (nil, nil, false, nil)。
func BuildAuthorizer(ctx context.Context, s options.CompletedOptions, egressSelector *egressselector.EgressSelector, apiserverID string, versionedInformers clientgoinformers.SharedInformerFactory) (authorizer.Authorizer, authorizer.RuleResolver, bool, error) {
	authorizationConfig, err := s.Authorization.ToAuthorizationConfig(versionedInformers)
	if err != nil {
		return nil, nil, false, err
	}
	if authorizationConfig == nil {
		return nil, nil, false, nil
	}

	if egressSelector != nil {
		egressDialer, err := egressSelector.Lookup(egressselector.ControlPlane.AsNetworkContext())
		if err != nil {
			return nil, nil, false, err
		}
		authorizationConfig.CustomDial = egressDialer
	}

	enablesRBAC := false
	for _, a := range authorizationConfig.AuthorizationConfiguration.Authorizers {
		if string(a.Type) == modes.ModeRBAC {
			enablesRBAC = true
			break
		}
	}

	authorizer, ruleResolver, err := authorizationConfig.New(ctx, apiserverID)

	return authorizer, ruleResolver, enablesRBAC, err
}

// CreateConfig 根据通用控制面选项创建通用 Kube API 的配置。
func CreateConfig(
	opts options.CompletedOptions,
	genericConfig *genericapiserver.Config,
	versionedInformers clientgoinformers.SharedInformerFactory,
	endpointSliceGetter proxy.EndpointSliceGetter,
	storageFactory *serverstorage.DefaultStorageFactory,
	serviceResolver aggregatorapiserver.ServiceResolver,
	additionalInitializers []admission.PluginInitializer,
) (
	*Config,
	[]admission.PluginInitializer,
	error,
) {
	proxyTransport := CreateProxyTransport()

	metricsfeatures.ApplyFeatureGates(utilfeature.DefaultFeatureGate)
	opts.Metrics.Apply()
	serviceaccount.RegisterMetrics()

	config := &Config{
		Generic: genericConfig,
		Extra: Extra{
			APIResourceConfigSource: storageFactory.APIResourceConfigSource,
			StorageFactory:          storageFactory,
			EventTTL:                opts.EventTTL,
			EnableLogsSupport:       opts.EnableLogsHandler,
			ProxyTransport:          proxyTransport,
			SystemNamespaces:        opts.SystemNamespaces,

			ServiceAccountIssuer:                opts.ServiceAccountIssuer,
			ServiceAccountMaxExpiration:         opts.ServiceAccountTokenMaxExpiration,
			ServiceAccountExtendedMaxExpiration: opts.Authentication.ServiceAccounts.MaxExtendedExpiration,
			ExtendExpiration:                    opts.Authentication.ServiceAccounts.ExtendExpiration,

			VersionedInformers: versionedInformers,

			EndpointSliceGetter: endpointSliceGetter,

			CoordinatedLeadershipLeaseDuration: opts.CoordinatedLeadershipLeaseDuration,
			CoordinatedLeadershipRenewDeadline: opts.CoordinatedLeadershipRenewDeadline,
			CoordinatedLeadershipRetryPeriod:   opts.CoordinatedLeadershipRetryPeriod,
		},
	}

	if utilfeature.DefaultFeatureGate.Enabled(genericfeatures.UnknownVersionInteroperabilityProxy) {
		var err error
		config.PeerEndpointLeaseReconciler, err = CreatePeerEndpointLeaseReconciler(*genericConfig, storageFactory)
		if err != nil {
			return nil, nil, err
		}
		if opts.PeerCAFile != "" {
			leaseInformer := versionedInformers.Coordination().V1().Leases()
			config.PeerProxy, err = BuildPeerProxy(
				leaseInformer,
				genericConfig.LoopbackClientConfig,
				opts.ProxyClientCertFile,
				opts.ProxyClientKeyFile, opts.PeerCAFile,
				opts.PeerAdvertiseAddress,
				genericConfig.APIServerID,
				config.Extra.PeerEndpointLeaseReconciler,
				config.Generic.Serializer,
				config.Generic.EgressSelector,
			)
			if err != nil {
				return nil, nil, err
			}
		}
	}

	clientCAProvider, err := opts.Authentication.ClientCert.GetClientCAContentProvider()
	if err != nil {
		return nil, nil, err
	}
	config.ClusterAuthenticationInfo.ClientCA = clientCAProvider

	requestHeaderConfig, err := opts.Authentication.RequestHeader.ToAuthenticationRequestHeaderConfig()
	if err != nil {
		return nil, nil, err
	}
	if requestHeaderConfig != nil {
		config.ClusterAuthenticationInfo.RequestHeaderCA = requestHeaderConfig.CAContentProvider
		config.ClusterAuthenticationInfo.RequestHeaderAllowedNames = requestHeaderConfig.AllowedClientNames
		config.ClusterAuthenticationInfo.RequestHeaderExtraHeaderPrefixes = requestHeaderConfig.ExtraHeaderPrefixes
		config.ClusterAuthenticationInfo.RequestHeaderGroupHeaders = requestHeaderConfig.GroupHeaders
		config.ClusterAuthenticationInfo.RequestHeaderUsernameHeaders = requestHeaderConfig.UsernameHeaders
		config.ClusterAuthenticationInfo.RequestHeaderUIDHeaders = requestHeaderConfig.UIDHeaders
	}

	// 设置准入插件初始化器，供后续 ApplyTo 使用。
	genericAdmissionConfig := controlplaneadmission.Config{
		ExternalInformers:    versionedInformers,
		LoopbackClientConfig: genericConfig.LoopbackClientConfig,
		APIResourceConfig:    storageFactory.APIResourceConfigSource,
	}
	genericInitializers, err := genericAdmissionConfig.New(proxyTransport, genericConfig.EgressSelector, serviceResolver, genericConfig.TracerProvider)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create admission plugin initializer: %w", err)
	}
	clientgoExternalClient, err := clientgoclientset.NewForConfig(genericConfig.LoopbackClientConfig)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create real client-go external client: %w", err)
	}
	dynamicExternalClient, err := dynamic.NewForConfig(genericConfig.LoopbackClientConfig)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create real dynamic external client: %w", err)
	}
	err = opts.Admission.ApplyTo(
		genericConfig,
		versionedInformers,
		clientgoExternalClient,
		dynamicExternalClient,
		utilfeature.DefaultFeatureGate,
		compatibility.DefaultComponentGlobalsRegistry.EffectiveVersionFor(basecompatibility.DefaultKubeComponent),
		append(genericInitializers, additionalInitializers...)...,
	)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to apply admission: %w", err)
	}

	if len(opts.Authentication.ServiceAccounts.KeyFiles) > 0 {
		// 从配置的密钥文件加载并注入公钥。
		var pubKeys []any
		for _, f := range opts.Authentication.ServiceAccounts.KeyFiles {
			keys, err := keyutil.PublicKeysFromFile(f)
			if err != nil {
				return nil, nil, fmt.Errorf("failed to parse key file %q: %w", f, err)
			}
			pubKeys = append(pubKeys, keys...)
		}
		keysGetter, err := serviceaccount.StaticPublicKeysGetter(pubKeys)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to set up public service account keys: %w", err)
		}
		config.ServiceAccountPublicKeysGetter = keysGetter
	} else if opts.Authentication.ServiceAccounts.ExternalPublicKeysGetter != nil {
		config.ServiceAccountPublicKeysGetter = opts.Authentication.ServiceAccounts.ExternalPublicKeysGetter
	}

	config.ServiceAccountIssuerURL = opts.Authentication.ServiceAccounts.Issuers[0]
	config.ServiceAccountJWKSURI = opts.Authentication.ServiceAccounts.JWKSURI

	return config, genericInitializers, nil
}

// CreateProxyTransport 创建连接节点所需的拨号传输层。
func CreateProxyTransport() *http.Transport {
	var proxyDialerFn utilnet.DialFunc
	// 到 Pod 与 Service 的代理基于 IP，无法校验主机名，故跳过 TLS 验证。
	proxyTLSClientConfig := &tls.Config{InsecureSkipVerify: true}
	proxyTransport := utilnet.SetTransportDefaults(&http.Transport{
		DialContext:     proxyDialerFn,
		TLSClientConfig: proxyTLSClientConfig,
	})
	return proxyTransport
}
