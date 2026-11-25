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

package encryptionconfig

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"net/http"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	utilerrors "k8s.io/apimachinery/pkg/util/errors"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/uuid"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/apiserver/pkg/apis/apiserver"
	apiserverv1 "k8s.io/apiserver/pkg/apis/apiserver/v1"
	"k8s.io/apiserver/pkg/apis/apiserver/validation"
	"k8s.io/apiserver/pkg/endpoints/request"
	"k8s.io/apiserver/pkg/features"
	"k8s.io/apiserver/pkg/server/healthz"
	"k8s.io/apiserver/pkg/server/options/encryptionconfig/metrics"
	storagevalue "k8s.io/apiserver/pkg/storage/value"
	aestransformer "k8s.io/apiserver/pkg/storage/value/encrypt/aes"
	"k8s.io/apiserver/pkg/storage/value/encrypt/envelope"
	envelopekmsv2 "k8s.io/apiserver/pkg/storage/value/encrypt/envelope/kmsv2"
	kmstypes "k8s.io/apiserver/pkg/storage/value/encrypt/envelope/kmsv2/v2"
	envelopemetrics "k8s.io/apiserver/pkg/storage/value/encrypt/envelope/metrics"
	"k8s.io/apiserver/pkg/storage/value/encrypt/identity"
	"k8s.io/apiserver/pkg/storage/value/encrypt/secretbox"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/klog/v2"
	kmsservice "k8s.io/kms/pkg/service"
)

const (
	aesCBCTransformerPrefixV1    = "k8s:enc:aescbc:v1:"
	aesGCMTransformerPrefixV1    = "k8s:enc:aesgcm:v1:"
	secretboxTransformerPrefixV1 = "k8s:enc:secretbox:v1:"
	kmsTransformerPrefixV1       = "k8s:enc:kms:v1:"
	kmsTransformerPrefixV2       = "k8s:enc:kms:v2:"

	// 这些常量与 KMS v2 插件状态轮询逻辑以及 DEK/seed 生成逻辑的行为方式相关。
	// 特别是，positive interval 与 max TTL 密切相关：二者之差定义了最坏情况下的窗口，
	// 即写入用的 DEK/seed 可能因插件进入错误状态而过期的时间窗口。
	// 最坏情况窗口除以 negative interval，即定义了在 DEK/seed 过期、写入开始失败之前，
	// 服务器尝试恢复到健康状态的最少次数。
	//
	// 目前这些取值保持较小且硬编码，以支持在容忍一定插件停机时间的同时，
	// 仍能执行"被动"存储迁移。
	//
	// 在当前方案下，用户可以更新其插件使用的 key ID，然后把迁移调度到 3 + N + M 分钟之后，
	// 其中 N 是插件加载新配置所需的时间，M 是留给 API server 处理配置的额外缓冲。
	// 届时，要么迁移到新密钥，要么在迁移过程中收到错误，二者必居其一。
	//
	// 如果 API server 无限期地依赖最后一次 DEK/seed 运行，用户就需要在开始迁移前
	// 主动检查它是否已观测到新的 key ID——否则它可能仍在使用旧的 DEK/seed，
	// 存储迁移的结果就不会像他们以为的那样。
	kmsv2PluginHealthzPositiveInterval = 1 * time.Minute
	kmsv2PluginHealthzNegativeInterval = 10 * time.Second
	kmsv2PluginWriteDEKSourceMaxTTL    = 3 * time.Minute

	kmsPluginHealthzNegativeTTL = 3 * time.Second
	kmsPluginHealthzPositiveTTL = 20 * time.Second
	kmsAPIVersionV1             = "v1"
	kmsAPIVersionV2             = "v2"
	// 这个名称用于两个不同的 healthz 端点：
	// - 当使用了一个或多个 KMS v2 插件且没有使用 KMS v1 插件时
	//   此时，所有 v2 插件通过这一个端点探测
	// - 当启用了加密配置自动重载时
	//   此时，所有 KMS 插件通过这一个端点探测
	//   即使没有配置任何 KMS 插件，该端点也会存在（此时它是空操作）
	kmsReloadHealthCheckName = "kms-providers"
)

var codecs serializer.CodecFactory

// 这个映射让我们可以在测试中切换 KMSv2KDF 特性的启用状态，因为从 v1.29 开始
// 该特性开关已锁定为 true。注意：终端用户无法设置它。
// KDF 启用状态按 KMS provider 分别跟踪，以便测试可以并行运行。
//
// 按 KMS 名称分键而非全局开关，是为了让并行集成测试互不干扰。
var kdfEnabledPerKMS sync.Map // map[string]bool, KMS name -> KDF enabled

// SetKDFForTests 仅供测试中切换 KMSv2KDF 特性开关使用；调用方必须保证所有测试中的 KMS provider 名称互不相同。
// 返回的函数用于恢复默认行为（删除该条目的记录）。
func SetKDFForTests(kmsName string, b bool) func() {
	if len(kmsName) == 0 { // 保证 GetKDF("") 返回默认值
		panic("empty KMS name used in test")
	}
	if _, loaded := kdfEnabledPerKMS.LoadOrStore(kmsName, b); loaded {
		panic("duplicate KMS name used in test")
	}
	return func() { kdfEnabledPerKMS.Delete(kmsName) }
}

// 应当使用本函数来判断 KMSv2KDF 特性是否启用，而不是从 DefaultFeatureGate 读取，
// 因为从 v1.29 开始该特性开关已锁定为 true。
// 为了让集成测试可以并行运行，只要所有 provider 使用互不相同的名称，
// 这个"特性开关"就可以按 KMS provider 分别设置。
func GetKDF(kmsName string) bool {
	kdfEnabled, ok := kdfEnabledPerKMS.Load(kmsName)
	if !ok {
		return true // 显式配置缺失，但 KDF 默认启用
	}
	return kdfEnabled.(bool) // 如果存入了非 bool 值，这里会 panic，而这种事绝不应发生
}

// init 初始化配置编解码器并注册所有加密相关指标；指标必须先注册，
// 否则后续访问会因重复注册而 panic。
func init() {
	configScheme := runtime.NewScheme()
	utilruntime.Must(apiserver.AddToScheme(configScheme))
	utilruntime.Must(apiserverv1.AddToScheme(configScheme))
	codecs = serializer.NewCodecFactory(configScheme, serializer.EnableStrict)
	envelopemetrics.RegisterMetrics()
	storagevalue.RegisterMetrics()
	metrics.RegisterMetrics()
}

// kmsPluginHealthzResponse 缓存最近一次探针结果及其时间，用于 TTL 内的短路。
type kmsPluginHealthzResponse struct {
	err      error
	received time.Time
}

// kmsPluginProbe 是 KMS v1 的健康探针：通过 encrypt/decrypt "ping" 验证链路。
type kmsPluginProbe struct {
	name         string
	ttl          time.Duration
	service      envelope.Service
	lastResponse *kmsPluginHealthzResponse
	l            *sync.Mutex
}

// kmsv2PluginProbe 是 KMS v2 的健康探针：调用 Status 并根据 key ID 维护 DEK/seed 状态。
type kmsv2PluginProbe struct {
	// state 用 atomic 发布，后台轮询与存储读写可并发访问。
	state        atomic.Pointer[envelopekmsv2.State]
	name         string
	ttl          time.Duration
	service      kmsservice.Service
	lastResponse *kmsPluginHealthzResponse
	l            *sync.Mutex
	apiServerID  string
	version      string
}

// kmsHealthChecker 把多个探针聚合成单个健康检查端点，避免逐个暴露。
type kmsHealthChecker []healthz.HealthChecker

// Name 返回聚合端点的固定名称。
func (k kmsHealthChecker) Name() string {
	return kmsReloadHealthCheckName
}

// Check 串行执行全部探针并合并错误；串行是有意的，避免对 KMS 插件造成突发压力。
func (k kmsHealthChecker) Check(req *http.Request) error {
	var errs []error

	for i := range k {
		checker := k[i]
		if err := checker.Check(req); err != nil {
			errs = append(errs, fmt.Errorf("%s: %w", checker.Name(), err))
		}
	}

	return utilerrors.Reduce(utilerrors.NewAggregate(errs))
}

// toHealthzCheck 把 v1 探针适配为带序号名称的 healthz 检查。
func (h *kmsPluginProbe) toHealthzCheck(idx int) healthz.HealthChecker {
	return healthz.NamedCheck(fmt.Sprintf("kms-provider-%d", idx), func(r *http.Request) error {
		return h.check()
	})
}

// toHealthzCheck 把 v2 探针适配为 healthz 检查，透传请求上下文用于取消。
func (h *kmsv2PluginProbe) toHealthzCheck(idx int) healthz.HealthChecker {
	return healthz.NamedCheck(fmt.Sprintf("kms-provider-%d", idx), func(r *http.Request) error {
		return h.check(r.Context())
	})
}

// EncryptionConfiguration 表示已解析并归一化的 apiserver 加密配置。
type EncryptionConfiguration struct {
	// Transformers 是用于加密和解密数据的 value.Transformer 列表。
	Transformers map[schema.GroupResource]storagevalue.Transformer

	// HealthChecks 是用于检查加密 provider 健康状况的 healthz.HealthChecker 列表。
	HealthChecks []healthz.HealthChecker

	// EncryptionFileContentHash 是加密配置文件内容的哈希；只保留哈希而不保留原文，避免明文密钥长期驻留内存。
	EncryptionFileContentHash string

	// KMSCloseGracePeriod 是在关闭旧 transformer 之前等待的时长。
	// 我们使用一个比请求超时时间更长的时长，等待所有在途请求完成。
	KMSCloseGracePeriod time.Duration
}

// LoadEncryptionConfig 解析并校验 filepath 指定的加密配置。
// 它可能启动多个 goroutine，其生命周期由 ctx 控制。
// 若返回错误，调用方负责取消 ctx，以清理可能已启动的 goroutine。
// 如果 reload 为 true，或使用了 KMS v2 插件且没有 KMS v1 插件，返回的健康检查器切片长度始终为 1。
func LoadEncryptionConfig(ctx context.Context, filepath string, reload bool, apiServerID string) (*EncryptionConfiguration, error) {
	config, contentHash, err := loadConfig(filepath, reload)
	if err != nil {
		return nil, fmt.Errorf("error while parsing file: %w", err)
	}

	transformers, kmsHealthChecks, kmsUsed, err := getTransformerOverridesAndKMSPluginHealthzCheckers(ctx, config, apiServerID)
	if err != nil {
		return nil, fmt.Errorf("error while building transformers: %w", err)
	}

	// 动态重载或仅使用 v2 时统一暴露一个聚合端点，避免配置变更导致健康检查名称漂移。
	if reload || (kmsUsed.v2Used && !kmsUsed.v1Used) {
		kmsHealthChecks = []healthz.HealthChecker{kmsHealthChecker(kmsHealthChecks)}
	}

	// KMSCloseGracePeriod 是在关闭旧 transformer 之前等待的时长。
	// 计算方式如下：
	// 1. 将所有 KMS 插件的 timeout 求和（v1 与 v2 的差异见 kmsPrefixTransformer）。
	// 2. 将总和乘以 2（留出一些缓冲）。
	// 之所以对所有 timeout 求和，是因为 kmsHealthChecker() 会串行执行全部健康检查。
	return &EncryptionConfiguration{
		Transformers:              transformers,
		HealthChecks:              kmsHealthChecks,
		EncryptionFileContentHash: contentHash,
		KMSCloseGracePeriod:       2 * kmsUsed.kmsTimeoutSum,
	}, nil
}

// getTransformerOverridesAndKMSPluginHealthzCheckers 基于给定配置创建 transformer 集合和 KMS healthz 检查。
// 它可能启动多个 goroutine，其生命周期由 ctx 控制。
// 若返回错误，调用方负责取消 ctx，以清理可能已启动的 goroutine。
func getTransformerOverridesAndKMSPluginHealthzCheckers(ctx context.Context, config *apiserver.EncryptionConfiguration, apiServerID string) (map[schema.GroupResource]storagevalue.Transformer, []healthz.HealthChecker, *kmsState, error) {
	var kmsHealthChecks []healthz.HealthChecker
	transformers, probes, kmsUsed, err := getTransformerOverridesAndKMSPluginProbes(ctx, config, apiServerID)
	if err != nil {
		return nil, nil, nil, err
	}
	// 下标用于生成稳定的检查名（kms-provider-N）。
	for i := range probes {
		probe := probes[i]
		kmsHealthChecks = append(kmsHealthChecks, probe.toHealthzCheck(i))
	}

	return transformers, kmsHealthChecks, kmsUsed, nil
}

// healthChecker 抽象“可转换为 healthz 检查”的探针。
type healthChecker interface {
	toHealthzCheck(idx int) healthz.HealthChecker
}

// getTransformerOverridesAndKMSPluginProbes 基于给定配置创建 transformer 集合和 KMS 探针。
// 它可能启动多个 goroutine，其生命周期由 ctx 控制。
// 若返回错误，调用方负责取消 ctx，以清理可能已启动的 goroutine。
func getTransformerOverridesAndKMSPluginProbes(ctx context.Context, config *apiserver.EncryptionConfiguration, apiServerID string) (map[schema.GroupResource]storagevalue.Transformer, []healthChecker, *kmsState, error) {
	resourceToPrefixTransformer := map[schema.GroupResource][]storagevalue.PrefixTransformer{}
	var probes []healthChecker
	var kmsUsed kmsState

	// 遍历配置中的每个条目
	for _, resourceConfig := range config.Resources {
		transformers, p, used, err := prefixTransformersAndProbes(ctx, resourceConfig, apiServerID)
		if err != nil {
			return nil, nil, nil, err
		}
		kmsUsed.accumulate(used)

		// 对每个资源，创建要使用的 provider 列表
		for _, resource := range resourceConfig.Resources {
			gr := schema.ParseGroupResource(resource)

			// 检查该资源是否被 *.group 规则遮蔽
			anyResourceInGroup := schema.GroupResource{Group: gr.Group, Resource: "*"}
			if _, masked := resourceToPrefixTransformer[anyResourceInGroup]; masked {
				// 较早的规则已为 *.group 配置了 transformer，遮蔽了本规则：后续更具体的规则将永不生效，这属于不允许的配置，必须报错。
				return nil, nil, nil, fmt.Errorf("resource %q is masked by earlier rule %q", grYAMLString(gr), grYAMLString(anyResourceInGroup))
			}

			if _, masked := resourceToPrefixTransformer[anyGroupAnyResource]; masked {
				// 较早的规则已为 *.* 配置了 transformer，遮蔽了本规则：
				// 这属于不允许的配置，返回错误
				return nil, nil, nil, fmt.Errorf("resource %q is masked by earlier rule %q", grYAMLString(gr), grYAMLString(anyGroupAnyResource))
			}

			resourceToPrefixTransformer[gr] = append(resourceToPrefixTransformer[gr], transformers...)
		}

		probes = append(probes, p...)
	}

	transformers := make(map[schema.GroupResource]storagevalue.Transformer, len(resourceToPrefixTransformer))
	for gr, transList := range resourceToPrefixTransformer {
		// 按配置顺序尝试各前缀；都不匹配时报错，而不是静默返回明文。
		transformers[gr] = storagevalue.NewPrefixTransformers(fmt.Errorf("no matching prefix found"), transList...)
	}

	return transformers, probes, &kmsUsed, nil
}

// check 针对 KMS-Plugin 的 gRPC 端点对测试数据执行加密和解密。
func (h *kmsPluginProbe) check() error {
	h.l.Lock()
	defer h.l.Unlock()

	// TTL 内直接复用上次结果（无论成功失败），避免每个健康检查请求都打到插件。
	if (time.Since(h.lastResponse.received)) < h.ttl {
		return h.lastResponse.err
	}

	p, err := h.service.Encrypt([]byte("ping"))
	if err != nil {
		h.lastResponse = &kmsPluginHealthzResponse{err: err, received: time.Now()}
		h.ttl = kmsPluginHealthzNegativeTTL
		return fmt.Errorf("failed to perform encrypt section of the healthz check for KMS Provider %s, error: %w", h.name, err)
	}

	// 解密刚加密的数据，验证加解密链路完整（包括密钥可用性）。
	if _, err := h.service.Decrypt(p); err != nil {
		h.lastResponse = &kmsPluginHealthzResponse{err: err, received: time.Now()}
		h.ttl = kmsPluginHealthzNegativeTTL
		return fmt.Errorf("failed to perform decrypt section of the healthz check for KMS Provider %s, error: %w", h.name, err)
	}

	h.lastResponse = &kmsPluginHealthzResponse{err: nil, received: time.Now()}
	// 成功时使用较长的 TTL，减少对插件的调用频率。
	h.ttl = kmsPluginHealthzPositiveTTL
	return nil
}

// check 使用 Status() 方法获取 KMSv2-Plugin 的 healthz 状态。
func (h *kmsv2PluginProbe) check(ctx context.Context) error {
	h.l.Lock()
	defer h.l.Unlock()

	if time.Since(h.lastResponse.received) < h.ttl {
		return h.lastResponse.err
	}

	p, err := h.service.Status(ctx)
	if err != nil {
		h.lastResponse = &kmsPluginHealthzResponse{err: err, received: time.Now()}
		h.ttl = kmsPluginHealthzNegativeTTL
		return fmt.Errorf("failed to perform status section of the healthz check for KMS Provider %s, error: %w", h.name, err)
	}

	if err := h.isKMSv2ProviderHealthyAndMaybeRotateDEK(ctx, p); err != nil {
		h.lastResponse = &kmsPluginHealthzResponse{err: err, received: time.Now()}
		h.ttl = kmsPluginHealthzNegativeTTL
		return err
	}

	h.lastResponse = &kmsPluginHealthzResponse{err: nil, received: time.Now()}
	h.ttl = kmsPluginHealthzPositiveTTL
	return nil
}

// rotateDEKOnKeyIDChange 在 Status 返回的 key ID 与当前 state 不匹配时，尝试轮转到新的 DEK/seed。
// 若轮转成功，新的 DEK/seed 和 keyID 会覆盖现有 state。
// 轮转过程中一旦发生任何失败（包括 Status 与 Encrypt 调用返回的 key ID 不一致），
// 现有 state 会被保留，并在其过期前继续可用于加密（系统会尝试"惯性滑行"）。
// 若 Status 返回的 key ID 与当前 state 匹配，则延长当前 state 的过期时间，不执行轮转。
func (h *kmsv2PluginProbe) rotateDEKOnKeyIDChange(ctx context.Context, statusKeyID, uid string) error {
	// 这里不检查 ValidateEncryptCapability，因为复用在不健康期间被标记为过期的旧密钥是可以的。
	// 只要 key ID 与我们的预期一致，这里就不需要轮转。
	state, errState := h.getCurrentState()

	// 所有情况下都允许无限期读取
	// 只要没有错误，就允许无限期写入
	// 存在错误时，仅允许写入到从现在起 kmsv2PluginWriteDEKSourceMaxTTL 之后
	// 我们在发起网络调用之前就开始计时，因为 kmsv2PluginWriteDEKSourceMaxTTL 是上限
	expirationTimestamp := envelopekmsv2.GetNowFunc(h.name)().Add(kmsv2PluginWriteDEKSourceMaxTTL)

	// 动态检查是使用 KDF seed 派生 DEK，还是只用单个 DEK
	// 这个开关只可能在测试中变化，但该检查开销足够低，可以每次都做
	// 这样无需重启 API server 就能轻松覆盖两种模式
	// TODO 集成测试验证该切换动态生效
	useSeed := GetKDF(h.name)
	stateUseSeed := state.EncryptedObjectEncryptedDEKSourceType == kmstypes.EncryptedDEKSourceType_HKDF_SHA256_XNONCE_AES_GCM_SEED

	// state 有效，且 Status 的 keyID 与生成该 DEK/seed 时相比没有变化，因此无需轮转
	// 只需把当前 state 的过期时间按复用间隔向后顺延
	// useSeed 只可能在运行期的测试中变化，所以在这里检查它，以便轻松覆盖两种模式
	if errState == nil && state.EncryptedObjectKeyID == statusKeyID && stateUseSeed == useSeed {
		state.ExpirationTimestamp = expirationTimestamp
		h.state.Store(&state)
		return nil
	}

	transformer, encObject, cacheKey, errGen := envelopekmsv2.GenerateTransformer(ctx, uid, h.service, useSeed)

	if encObject == nil {
		encObject = &kmstypes.EncryptedObject{} // 避免 nil panic
	}

	// 正常路径，应当是最常见的情况
	// TODO 也许可以加上成功指标？
	if errGen == nil && encObject.KeyID == statusKeyID {
		// 只有 Status 与 Encrypt 返回同一 key ID 时才发布新状态，
		// 否则视为轮换未完成，继续使用旧状态。
		h.state.Store(&envelopekmsv2.State{
			Transformer:                           transformer,
			EncryptedObjectKeyID:                  encObject.KeyID,
			EncryptedObjectEncryptedDEKSource:     encObject.EncryptedDEKSource,
			EncryptedObjectAnnotations:            encObject.Annotations,
			EncryptedObjectEncryptedDEKSourceType: encObject.EncryptedDEKSourceType,
			UID:                                   uid,
			ExpirationTimestamp:                   expirationTimestamp,
			CacheKey:                              cacheKey,
			KMSProviderName:                       h.name,
		})

		// 从逻辑上讲新 state 不可能无效，但为保险起见仍检查一次
		_, errGen = h.getCurrentState()
		if errGen == nil {
			klogV6 := klog.V(6)
			if klogV6.Enabled() {
				// 只输出 key ID 的哈希，避免把密钥标识写入日志。
				klogV6.InfoS("successfully rotated DEK",
					"uid", uid,
					"useSeed", useSeed,
					"newKeyIDHash", envelopekmsv2.GetHashIfNotEmpty(encObject.KeyID),
					"oldKeyIDHash", envelopekmsv2.GetHashIfNotEmpty(state.EncryptedObjectKeyID),
					"expirationTimestamp", expirationTimestamp.Format(time.RFC3339),
				)
			}
			return nil
		}
	}

	//nolint:errorlint // 打印 <nil> 错误是有意为之
	return fmt.Errorf("failed to rotate DEK uid=%q, useSeed=%v, errState=%v, errGen=%v, statusKeyIDHash=%q, encryptKeyIDHash=%q, stateKeyIDHash=%q, expirationTimestamp=%s",
		uid, useSeed, errState, errGen, envelopekmsv2.GetHashIfNotEmpty(statusKeyID), envelopekmsv2.GetHashIfNotEmpty(encObject.KeyID), envelopekmsv2.GetHashIfNotEmpty(state.EncryptedObjectKeyID), state.ExpirationTimestamp.Format(time.RFC3339))
}

// getCurrentState 返回最近一次 status 和 encrypt 调用产生的最新 state。
// 若返回的 error 为 nil，该 state 对读请求而言永久有效。
// 对于写请求，调用方还必须检查 state.ValidateEncryptCapability 不报错。
func (h *kmsv2PluginProbe) getCurrentState() (envelopekmsv2.State, error) {
	state := *h.state.Load()

	if state.Transformer == nil {
		return envelopekmsv2.State{}, fmt.Errorf("got unexpected nil transformer")
	}

	// 用一个占位的非空 EncryptedData 走一遍校验，确保状态字段自洽。
	encryptedObjectCopy := kmstypes.EncryptedObject{
		KeyID:                  state.EncryptedObjectKeyID,
		EncryptedDEKSource:     state.EncryptedObjectEncryptedDEKSource,
		Annotations:            state.EncryptedObjectAnnotations,
		EncryptedDEKSourceType: state.EncryptedObjectEncryptedDEKSourceType,
		EncryptedData:          []byte{0}, // 任意非空值，用于通过校验
	}
	if err := envelopekmsv2.ValidateEncryptedObject(&encryptedObjectCopy); err != nil {
		return envelopekmsv2.State{}, fmt.Errorf("got invalid EncryptedObject: %w", err)
	}

	if state.ExpirationTimestamp.IsZero() {
		return envelopekmsv2.State{}, fmt.Errorf("got unexpected zero expirationTimestamp")
	}

	if len(state.CacheKey) == 0 {
		return envelopekmsv2.State{}, fmt.Errorf("got unexpected empty cacheKey")
	}

	return state, nil
}

// isKMSv2ProviderHealthyAndMaybeRotateDEK 校验 Status 响应并触发必要的 DEK 轮换。
func (h *kmsv2PluginProbe) isKMSv2ProviderHealthyAndMaybeRotateDEK(ctx context.Context, response *kmsservice.StatusResponse) error {
	var errs []error
	if response.Healthz != "ok" {
		errs = append(errs, fmt.Errorf("got unexpected healthz status: %s", response.Healthz))
	}
	if response.Version != envelopekmsv2.KMSAPIVersionv2 && response.Version != envelopekmsv2.KMSAPIVersionv2beta1 {
		errs = append(errs, fmt.Errorf("expected KMSv2 API version %s, got %s", envelopekmsv2.KMSAPIVersionv2, response.Version))
	} else {
		// 在首次收到 status 响应时记录版本号
		if len(h.version) == 0 {
			h.version = response.Version
		}
		// 插件在运行期切换 API 版本会让客户端语义不一致，视为不健康。
		if h.version != response.Version {
			errs = append(errs, fmt.Errorf("KMSv2 API version should not change after the initial status response version %s, got %s", h.version, response.Version))
		}
	}

	if errCode, err := envelopekmsv2.ValidateKeyID(response.KeyID); err != nil {
		envelopemetrics.RecordInvalidKeyIDFromStatus(h.name, string(errCode))
		// KeyID 可能敏感，只记录哈希。
		errs = append(errs, fmt.Errorf("got invalid KMSv2 KeyID hash %q: %w", envelopekmsv2.GetHashIfNotEmpty(response.KeyID), err))
	} else {
		envelopemetrics.RecordKeyIDFromStatus(h.name, response.KeyID, h.apiServerID)
		// 无条件追加，因为下面会过滤掉 nil 错误
		errs = append(errs, h.rotateDEKOnKeyIDChange(ctx, response.KeyID, string(uuid.NewUUID())))
	}

	if err := utilerrors.Reduce(utilerrors.NewAggregate(errs)); err != nil {
		return fmt.Errorf("kmsv2 Provider %s is not healthy, error: %w", h.name, err)
	}
	return nil
}

// loadConfig 解析 filepath 处的加密配置文件，返回解析后的配置及文件哈希。
func loadConfig(filepath string, reload bool) (*apiserver.EncryptionConfiguration, string, error) {
	data, contentHash, err := loadDataAndHash(filepath)
	if err != nil {
		return nil, "", fmt.Errorf("error while loading file: %w", err)
	}

	// 严格模式解码：拒绝未知字段，避免拼写错误导致安全配置被静默忽略。
	configObj, gvk, err := codecs.UniversalDecoder().Decode(data, nil, nil)
	if err != nil {
		return nil, "", fmt.Errorf("error decoding encryption provider configuration file %q: %w", filepath, err)
	}
	config, ok := configObj.(*apiserver.EncryptionConfiguration)
	if !ok {
		return nil, "", fmt.Errorf("got unexpected config type: %v", gvk)
	}

	return config, contentHash, validation.ValidateEncryptionConfiguration(config, reload).ToAggregate()
}

// loadDataAndHash 读取文件内容并计算哈希；空文件视为错误。
func loadDataAndHash(filepath string) ([]byte, string, error) {
	data, err := os.ReadFile(filepath)
	if err != nil {
		return nil, "", fmt.Errorf("error reading encryption provider configuration file %q: %w", filepath, err)
	}
	if len(data) == 0 {
		return nil, "", fmt.Errorf("encryption provider configuration file %q is empty", filepath)
	}

	return data, computeEncryptionConfigHash(data), nil
}

// GetEncryptionConfigHash 读取 filepath 处的加密配置文件并返回文件哈希。
// 它不会尝试解码或加载配置，可作为判断文件是否变更的廉价检查。
func GetEncryptionConfigHash(filepath string) (string, error) {
	_, contentHash, err := loadDataAndHash(filepath)
	return contentHash, err
}

// prefixTransformersAndProbes 基于给定的资源配置创建 transformer 集合和 KMS 探针。
// 它可能启动多个 goroutine，其生命周期由 ctx 控制。
// 若返回错误，调用方负责取消 ctx，以清理可能已启动的 goroutine。
func prefixTransformersAndProbes(ctx context.Context, config apiserver.ResourceConfiguration, apiServerID string) ([]storagevalue.PrefixTransformer, []healthChecker, *kmsState, error) {
	var transformers []storagevalue.PrefixTransformer
	var probes []healthChecker
	var kmsUsed kmsState

	for _, provider := range config.Providers {
		var (
			transformer    storagevalue.PrefixTransformer
			transformerErr error
			probe          healthChecker
			used           *kmsState
		)

		switch {
		case provider.AESGCM != nil:
			// 推荐默认：AEAD，性能与完整性兼得。
			transformer, transformerErr = aesPrefixTransformer(provider.AESGCM, aestransformer.NewGCMTransformer, aesGCMTransformerPrefixV1)

		case provider.AESCBC != nil:
			// CBC 仅用于兼容读取历史数据。
			cbcTransformer := func(block cipher.Block) (storagevalue.Transformer, error) {
				return aestransformer.NewCBCTransformer(block), nil
			}
			transformer, transformerErr = aesPrefixTransformer(provider.AESCBC, cbcTransformer, aesCBCTransformerPrefixV1)

		case provider.Secretbox != nil:
			transformer, transformerErr = secretboxPrefixTransformer(provider.Secretbox)

		case provider.KMS != nil:
			transformer, probe, used, transformerErr = kmsPrefixTransformer(ctx, provider.KMS, apiServerID)
			if transformerErr == nil {
				probes = append(probes, probe)
				kmsUsed.accumulate(used)
			}

		case provider.Identity != nil:
			// identity 即“不加密但校验”，用于迁移期间保留明文读写能力。
			transformer = storagevalue.PrefixTransformer{
				Transformer: identity.NewEncryptCheckTransformer(),
				Prefix:      []byte{},
			}

		default:
			return nil, nil, nil, errors.New("provider does not contain any of the expected providers: KMS, AESGCM, AESCBC, Secretbox, Identity")
		}

		if transformerErr != nil {
			return nil, nil, nil, transformerErr
		}

		transformers = append(transformers, transformer)
	}

	return transformers, probes, &kmsUsed, nil
}

// blockTransformerFunc 抽象“由 cipher.Block 构造 Transformer”的差异（GCM 会失败，CBC 不会）。
type blockTransformerFunc func(cipher.Block) (storagevalue.Transformer, error)

// aesPrefixTransformer 支持多密钥轮换：每个密钥有独立前缀，第一个密钥用于写，全部密钥可用于读。
func aesPrefixTransformer(config *apiserver.AESConfiguration, fn blockTransformerFunc, prefix string) (storagevalue.PrefixTransformer, error) {
	var result storagevalue.PrefixTransformer

	if len(config.Keys) == 0 {
		return result, fmt.Errorf("aes provider has no valid keys")
	}
	// 先整体校验再构造，避免构造到一半才发现后面的密钥非法。
	for _, key := range config.Keys {
		if key.Name == "" {
			return result, fmt.Errorf("key with invalid name provided")
		}
		if key.Secret == "" {
			return result, fmt.Errorf("key %v has no provided secret", key.Name)
		}
	}

	keyTransformers := []storagevalue.PrefixTransformer{}

	for _, keyData := range config.Keys {
		key, err := base64.StdEncoding.DecodeString(keyData.Secret)
		if err != nil {
			return result, fmt.Errorf("could not obtain secret for named key %s: %w", keyData.Name, err)
		}
		block, err := aes.NewCipher(key)
		if err != nil {
			return result, fmt.Errorf("error while creating cipher for named key %s: %w", keyData.Name, err)
		}
		transformer, err := fn(block)
		if err != nil {
			return result, fmt.Errorf("error while creating transformer for named key %s: %w", keyData.Name, err)
		}

		// 为该密钥创建新的 PrefixTransformer：前缀包含密钥名，读取时据此分流到正确密钥；写入始终使用第一个。
		keyTransformers = append(keyTransformers,
			storagevalue.PrefixTransformer{
				Transformer: transformer,
				Prefix:      []byte(keyData.Name + ":"),
			})
	}

	// 创建可在这组密钥之间选择的 prefixTransformer
	keyTransformer := storagevalue.NewPrefixTransformers(
		fmt.Errorf("no matching key was found for the provided AES transformer"), keyTransformers...)

	// 创建 PrefixTransformer，稍后它会与其他 provider 一起被放入列表
	result = storagevalue.PrefixTransformer{
		Transformer: keyTransformer,
		Prefix:      []byte(prefix),
	}
	return result, nil
}

// secretboxPrefixTransformer 与 AES 版本同构，但密钥长度固定为 32 字节（256 位）。
func secretboxPrefixTransformer(config *apiserver.SecretboxConfiguration) (storagevalue.PrefixTransformer, error) {
	var result storagevalue.PrefixTransformer

	if len(config.Keys) == 0 {
		return result, fmt.Errorf("secretbox provider has no valid keys")
	}
	for _, key := range config.Keys {
		if key.Name == "" {
			return result, fmt.Errorf("key with invalid name provided")
		}
		if key.Secret == "" {
			return result, fmt.Errorf("key %v has no provided secret", key.Name)
		}
	}

	keyTransformers := []storagevalue.PrefixTransformer{}

	for _, keyData := range config.Keys {
		key, err := base64.StdEncoding.DecodeString(keyData.Secret)
		if err != nil {
			return result, fmt.Errorf("could not obtain secret for named key %s: %s", keyData.Name, err)
		}

		// 长度不符直接报错，而不是截断或补零（那会静默降低安全性）。
		if len(key) != 32 {
			return result, fmt.Errorf("expected key size 32 for secretbox provider, got %v", len(key))
		}

		keyArray := [32]byte{}
		copy(keyArray[:], key)

		// 为该密钥创建新的 PrefixTransformer
		keyTransformers = append(keyTransformers,
			storagevalue.PrefixTransformer{
				Transformer: secretbox.NewSecretboxTransformer(keyArray),
				Prefix:      []byte(keyData.Name + ":"),
			})
	}

	// 创建可在这组密钥之间选择的 prefixTransformer
	keyTransformer := storagevalue.NewPrefixTransformers(
		fmt.Errorf("no matching key was found for the provided Secretbox transformer"), keyTransformers...)

	// 创建 PrefixTransformer，稍后它会与其他 provider 一起被放入列表
	result = storagevalue.PrefixTransformer{
		Transformer: keyTransformer,
		Prefix:      []byte(secretboxTransformerPrefixV1),
	}
	return result, nil
}

var (
	// 创建 kms service 的工厂，便于编写测试。
	envelopeServiceFactory = envelope.NewGRPCService

	// 创建 kmsv2 service 的工厂。导出是为了集成测试使用。
	EnvelopeKMSv2ServiceFactory = envelopekmsv2.NewGRPCService
)

// kmsState 汇总一组资源使用的 KMS 版本与超时，用于计算关闭旧 transformer 的宽限期。
type kmsState struct {
	v1Used, v2Used bool
	kmsTimeoutSum  time.Duration
}

// accumulate 通过以下方式汇总 KMS 状态：
//   - 确定正在使用哪些 KMS 插件版本
//   - 计算 kmsTimeoutSum，它被用作 transformTracker.kmsCloseGracePeriod
//     DynamicTransformers.Set 会在配置重载后等待该时长，再关闭旧 transformer
func (s *kmsState) accumulate(other *kmsState) {
	s.v1Used = s.v1Used || other.v1Used
	s.v2Used = s.v2Used || other.v2Used
	s.kmsTimeoutSum += other.kmsTimeoutSum
}

// kmsPrefixTransformer 基于给定的 KMS 配置创建 KMS transformer 和探针。
// 它可能启动多个 goroutine，其生命周期由 ctx 控制。
// 若返回错误，调用方负责取消 ctx，以清理可能已启动的 goroutine。
func kmsPrefixTransformer(ctx context.Context, config *apiserver.KMSConfiguration, apiServerID string) (storagevalue.PrefixTransformer, healthChecker, *kmsState, error) {
	kmsName := config.Name
	switch config.APIVersion {
	case kmsAPIVersionV1:
		// KMSv1 已弃用：默认关闭，必须显式开启特性门控才能使用。
		if !utilfeature.DefaultFeatureGate.Enabled(features.KMSv1) {
			return storagevalue.PrefixTransformer{}, nil, nil, fmt.Errorf("KMSv1 is deprecated and will only receive security updates going forward. Use KMSv2 instead.  Set --feature-gates=KMSv1=true to use the deprecated KMSv1 feature.")
		}
		klog.InfoS("KMSv1 is deprecated and will only receive security updates going forward. Use KMSv2 instead.")

		envelopeService, err := envelopeServiceFactory(ctx, config.Endpoint, config.Timeout.Duration)
		if err != nil {
			return storagevalue.PrefixTransformer{}, nil, nil, fmt.Errorf("could not configure KMSv1-Plugin's probe %q, error: %w", kmsName, err)
		}

		probe := &kmsPluginProbe{
			name:         kmsName,
			ttl:          kmsPluginHealthzNegativeTTL,
			service:      envelopeService,
			l:            &sync.Mutex{},
			lastResponse: &kmsPluginHealthzResponse{},
		}

		transformer := envelopePrefixTransformer(config, envelopeService, kmsTransformerPrefixV1)

		return transformer, probe, &kmsState{
			v1Used: true,
			// v1 的健康检查会执行加密和解密。由于二者是串行操作，超时时间按两倍计算。
			kmsTimeoutSum: 2 * config.Timeout.Duration,
		}, nil

	case kmsAPIVersionV2:
		envelopeService, err := EnvelopeKMSv2ServiceFactory(ctx, config.Endpoint, config.Name, config.Timeout.Duration)
		if err != nil {
			return storagevalue.PrefixTransformer{}, nil, nil, fmt.Errorf("could not configure KMSv2-Plugin's probe %q, error: %w", kmsName, err)
		}

		probe := &kmsv2PluginProbe{
			name:         kmsName,
			ttl:          kmsPluginHealthzNegativeTTL,
			service:      envelopeService,
			l:            &sync.Mutex{},
			lastResponse: &kmsPluginHealthzResponse{},
			apiServerID:  apiServerID,
		}
		// 先初始化状态，保证 Load 永远可用：放入仅含 provider 名的空状态，避免 transformer 构造后读到 nil。
		probe.state.Store(&envelopekmsv2.State{
			KMSProviderName: kmsName,
		})

		primeAndProbeKMSv2(ctx, probe, kmsName)
		transformer := storagevalue.PrefixTransformer{
			Transformer: envelopekmsv2.NewEnvelopeTransformer(envelopeService, kmsName, probe.getCurrentState, apiServerID),
			Prefix:      []byte(kmsTransformerPrefixV2 + kmsName + ":"),
		}

		return transformer, probe, &kmsState{
			v2Used:        true,
			kmsTimeoutSum: config.Timeout.Duration,
		}, nil

	default:
		return storagevalue.PrefixTransformer{}, nil, nil, fmt.Errorf("could not configure KMS plugin %q, unsupported KMS API version %q", kmsName, config.APIVersion)
	}
}

// primeAndProbeKMSv2 启动后台轮询：尽快拿到首个 key ID/DEK，并持续在 key 变更时轮换。
// 插件不可用不会阻断 apiserver 启动，但写入会在 DEK 过期后失败。
func primeAndProbeKMSv2(ctx context.Context, probe *kmsv2PluginProbe, kmsName string) {
	runProbeCheckAndLog := func(ctx context.Context, depth int) error {
		if err := probe.check(ctx); err != nil {
			klog.VDepth(1+depth, 2).ErrorS(err, "kms plugin failed health check probe", "name", kmsName)
			return err
		}
		return nil
	}

	blockAndProbeFastUntilSuccess := func(ctx context.Context) {
		_ = wait.PollUntilWithContext(
			ctx,
			kmsv2PluginHealthzNegativeInterval,
			func(ctx context.Context) (bool, error) {
				return runProbeCheckAndLog(ctx, 1) == nil, nil
			},
		)
	}

	// 在插件健康且服务器启动时即可用的正常路径下，
	// 就地运行一次 check 来预热 keyID 和 DEK（这也能避免单元测试偶发失败）
	errPrime := runProbeCheckAndLog(ctx, 0)

	// 如果首次预热尝试失败，立刻在后台开始尝试恢复到有效状态，
	// 以避免外部 healthz 检查器被配置为忽略 KMS healthz 端点时出现启动缓慢。
	// 由于我们需要支持插件与 API server 异步启动，此错误并非致命错误。
	if errPrime != nil {
		go blockAndProbeFastUntilSuccess(ctx) // 用独立 goroutine 以避免阻塞
	}

	// 确保插件的 key ID 处于 reasonably 新鲜的状态
	// 同时确保我们的 DEK 与该 key ID 保持同步（一旦过期，服务器将拒绝所有写入）
	// 如果这个后台循环停止运行，服务器将在 kmsv2PluginWriteDEKSourceMaxTTL 之后无法工作
	go wait.PollUntilWithContext(
		ctx,
		kmsv2PluginHealthzPositiveInterval,
		func(ctx context.Context) (bool, error) {
			if err := runProbeCheckAndLog(ctx, 0); err == nil {
				return false, nil
			}

			// TODO 为"失败后更快轮询"补充集成测试
			// 一旦失败，阻塞外层轮询，就地启动更快的新轮询
			// 以此降低 DEK 在瞬时故障期间过期的概率
			blockAndProbeFastUntilSuccess(ctx)

			return false, nil
		})
}

// envelopePrefixTransformer 构造 KMS v1 信封转换器：DEK 由 KMS 加密，数据用 DEK 加密。
func envelopePrefixTransformer(config *apiserver.KMSConfiguration, envelopeService envelope.Service, prefix string) storagevalue.PrefixTransformer {
	baseTransformerFunc := func(block cipher.Block) (storagevalue.Transformer, error) {
		gcm, err := aestransformer.NewGCMTransformer(block)
		if err != nil {
			return nil, err
		}

		// v1.24：仅用 AES-CBC 写入，但读取支持 AES-CBC 和 AES-GCM（以便过渡到 AES-GCM）
		// v1.25：仅用 AES-GCM 写入，但读取支持 AES-GCM，并为向后兼容回退到 AES-CBC
		// TODO(aramase): v1.25 之后：在实现存储迁移自动化之前，不能移除 CBC 读取支持。
		// 也可以在 release note 中硬性要求用户执行存储迁移。
		// 第一个元素负责加密（GCM），CBC 仅用于读取历史数据。
		return unionTransformers{gcm, aestransformer.NewCBCTransformer(block)}, nil
	}

	return storagevalue.PrefixTransformer{
		Transformer: envelope.NewEnvelopeTransformer(envelopeService, int(*config.CacheSize), baseTransformerFunc),
		Prefix:      []byte(prefix + config.Name + ":"),
	}
}

// unionTransformers 依次尝试多个 transformer，第一个成功者胜出。
type unionTransformers []storagevalue.Transformer

// TransformFromStorage 返回 stale=true 表示数据是用非首选算法读出的，
// 从而促使上层重写为当前格式。
func (u unionTransformers) TransformFromStorage(ctx context.Context, data []byte, dataCtx storagevalue.Context) (out []byte, stale bool, err error) {
	var errs []error
	for i := range u {
		transformer := u[i]
		result, stale, err := transformer.TransformFromStorage(ctx, data, dataCtx)
		if err != nil {
			errs = append(errs, err)
			continue
		}
		// 当 i != 0 时，说明数据是用较新的 transformer 从存储中解出来的，
		// 即使数据内容没有变化，我们也要向 etcd 发起一次写入
		return result, stale || i != 0, nil
	}
	if err := utilerrors.Reduce(utilerrors.NewAggregate(errs)); err != nil {
		return nil, false, err
	}
	return nil, false, fmt.Errorf("unionTransformers: unable to transform from storage")
}

// TransformToStorage 始终使用首选（第一个）transformer 写入。
func (u unionTransformers) TransformToStorage(ctx context.Context, data []byte, dataCtx storagevalue.Context) (out []byte, err error) {
	return u[0].TransformToStorage(ctx, data, dataCtx)
}

// computeEncryptionConfigHash 返回以字节读入的加密配置文件的预期哈希。
// 跟踪文件变更时使用哈希而非原始文件内容，以避免在 transformer 之外于内存中持有任何加密密钥。
func computeEncryptionConfigHash(data []byte) string {
	return fmt.Sprintf("sha256:%x", sha256.Sum256(data))
}

// 编译期断言：动态 transformer 同时是资源转换器与健康检查器。
var _ storagevalue.ResourceTransformers = &DynamicTransformers{}
var _ healthz.HealthChecker = &DynamicTransformers{}

// DynamicTransformers 持有可由单一外部角色（通常是控制器）动态更新的 transformers。
// 该结构必须避免加锁（读写锁也不行），因为它位于所有存储调用的关键路径上。
// 因此用 atomic.Value 整体替换快照，读路径零锁。
type DynamicTransformers struct {
	transformTracker *atomic.Value
}

// transformTracker 是不可变快照：更新时整体替换，避免读者看到半更新的状态。
type transformTracker struct {
	transformerOverrides  map[schema.GroupResource]storagevalue.Transformer
	kmsPluginHealthzCheck healthz.HealthChecker
	closeTransformers     context.CancelFunc
	kmsCloseGracePeriod   time.Duration
}

// NewDynamicTransformers 返回 transformers、KMS provider 的健康检查，以及关闭 transformers 的能力。
func NewDynamicTransformers(
	transformerOverrides map[schema.GroupResource]storagevalue.Transformer,
	kmsPluginHealthzCheck healthz.HealthChecker,
	closeTransformers context.CancelFunc,
	kmsCloseGracePeriod time.Duration,
) *DynamicTransformers {
	dynamicTransformers := &DynamicTransformers{
		transformTracker: &atomic.Value{},
	}

	tracker := &transformTracker{
		transformerOverrides:  transformerOverrides,
		kmsPluginHealthzCheck: kmsPluginHealthzCheck,
		closeTransformers:     closeTransformers,
		kmsCloseGracePeriod:   kmsCloseGracePeriod,
	}
	dynamicTransformers.transformTracker.Store(tracker)

	return dynamicTransformers
}

// Check 实现了 healthz.HealthChecker
func (d *DynamicTransformers) Check(req *http.Request) error {
	return d.transformTracker.Load().(*transformTracker).kmsPluginHealthzCheck.Check(req)
}

// Name 实现了 healthz.HealthChecker
func (d *DynamicTransformers) Name() string {
	return kmsReloadHealthCheckName
}

// TransformerForResource 返回给定资源对应的 transformer。
func (d *DynamicTransformers) TransformerForResource(resource schema.GroupResource) storagevalue.Transformer {
	// 返回轻量代理而非当前 transformer：后续 Set 后仍能取到最新实现。
	return &resourceTransformer{
		resource:         resource,
		transformTracker: d.transformTracker,
	}
}

// Set 设置 transformer 覆盖项。本方法不是 goroutine 安全的，在整个对象的生命周期内，必须只能由同一个、单一的调用方调用。
func (d *DynamicTransformers) Set(
	transformerOverrides map[schema.GroupResource]storagevalue.Transformer,
	closeTransformers context.CancelFunc,
	kmsPluginHealthzCheck healthz.HealthChecker,
	kmsCloseGracePeriod time.Duration,
) {
	// 存入新值
	newTransformTracker := &transformTracker{
		transformerOverrides:  transformerOverrides,
		closeTransformers:     closeTransformers,
		kmsPluginHealthzCheck: kmsPluginHealthzCheck,
		kmsCloseGracePeriod:   kmsCloseGracePeriod,
	}

	// 更新 transformer 覆盖项
	oldTransformTracker := d.transformTracker.Swap(newTransformTracker).(*transformTracker)

	// 等待足够长的时间，让 grpc 请求处理完所有在途请求后，再关闭旧 transformer。
	// 启动这个 goroutine 时，新 transformer 已经设置完毕，新请求将使用它们。
	// 如果服务器在 sleep 期间开始关闭，transformer 也会被正确地提前关闭，因为它们的生命周期绑定在 api-server 的 drain 通知器上。
	go func() {
		// 延迟关闭是必要的：旧 transformer 可能仍被在读请求持有。
		time.Sleep(oldTransformTracker.kmsCloseGracePeriod)
		oldTransformTracker.closeTransformers()
	}()
}

// 编译期断言：resourceTransformer 实现 Transformer。
var _ storagevalue.Transformer = &resourceTransformer{}

// resourceTransformer 每次调用都从最新快照解析实际 transformer，
// 使配置热加载对已注册的存储层透明。
type resourceTransformer struct {
	resource         schema.GroupResource
	transformTracker *atomic.Value
}

// TransformFromStorage 委托给最新的 transformer。
func (r *resourceTransformer) TransformFromStorage(ctx context.Context, data []byte, dataCtx storagevalue.Context) ([]byte, bool, error) {
	return r.transformer().TransformFromStorage(ctx, data, dataCtx)
}

// TransformToStorage 委托给最新的 transformer。
func (r *resourceTransformer) TransformToStorage(ctx context.Context, data []byte, dataCtx storagevalue.Context) ([]byte, error) {
	return r.transformer().TransformToStorage(ctx, data, dataCtx)
}

// transformer 按资源名在最新快照中查找匹配项。
func (r *resourceTransformer) transformer() storagevalue.Transformer {
	return transformerFromOverrides(r.transformTracker.Load().(*transformTracker).transformerOverrides, r.resource)
}

// 编译期断言：StaticTransformers 实现 ResourceTransformers。
var _ storagevalue.ResourceTransformers = &StaticTransformers{}

// StaticTransformers 是启动期固定不变的资源到 transformer 映射。
type StaticTransformers map[schema.GroupResource]storagevalue.Transformer

// TransformerForResource 按资源查找，遵循同样的通配回退规则。
func (s StaticTransformers) TransformerForResource(resource schema.GroupResource) storagevalue.Transformer {
	return transformerFromOverrides(s, resource)
}

// anyGroupAnyResource 是最终兜底的 "*.*" 规则键。
var anyGroupAnyResource = schema.GroupResource{
	Group:    "*",
	Resource: "*",
}

// transformerFromOverrides 按 精确资源 -> group/* -> *.* -> identity 的顺序回退，
// 并统一包一层 requestInfoTransformer 以便审计/指标拿到资源信息。
func transformerFromOverrides(transformerOverrides map[schema.GroupResource]storagevalue.Transformer, resource schema.GroupResource) (out storagevalue.Transformer) {
	defer func() { out = newRequestInfoTransformer(resource, out) }()

	if transformer := transformerOverrides[resource]; transformer != nil {
		return transformer
	}

	if transformer := transformerOverrides[schema.GroupResource{
		Group:    resource.Group,
		Resource: "*",
	}]; transformer != nil {
		return transformer
	}

	if transformer := transformerOverrides[anyGroupAnyResource]; transformer != nil {
		return transformer
	}

	// 无匹配规则时保持明文（但会校验写入数据），兼容未配置加密的集群。
	return identity.NewEncryptCheckTransformer()
}

// grYAMLString 生成配置中使用的资源字符串，core 组的 "*" 表示为 "*."。
func grYAMLString(gr schema.GroupResource) string {
	if gr.Group == "" && gr.Resource == "*" {
		return "*."
	}

	return gr.String()
}

// 编译期断言：requestInfoTransformer 实现 Transformer。
var _ storagevalue.Transformer = &requestInfoTransformer{}

// requestInfoTransformer 为底层 transformer 注入资源信息，
// 使 KMS/指标能在没有 HTTP 上下文的场景下识别资源。
type requestInfoTransformer struct {
	baseValueCtx context.Context
	delegate     storagevalue.Transformer
}

func newRequestInfoTransformer(resource schema.GroupResource, delegate storagevalue.Transformer) *requestInfoTransformer {
	return &requestInfoTransformer{
		baseValueCtx: request.WithRequestInfo(context.Background(), &request.RequestInfo{IsResourceRequest: true, APIGroup: resource.Group, Resource: resource.Resource}),
		delegate:     delegate,
	}
}

// TransformFromStorage 使用合并后的上下文调用底层实现。
func (l *requestInfoTransformer) TransformFromStorage(ctx context.Context, data []byte, dataCtx storagevalue.Context) ([]byte, bool, error) {
	return l.delegate.TransformFromStorage(l.withBaseValueCtx(ctx), data, dataCtx)
}

// TransformToStorage 使用合并后的上下文调用底层实现。
func (l *requestInfoTransformer) TransformToStorage(ctx context.Context, data []byte, dataCtx storagevalue.Context) ([]byte, error) {
	return l.delegate.TransformToStorage(l.withBaseValueCtx(ctx), data, dataCtx)
}

// withBaseValueCtx 合并请求上下文与预置资源信息；真实请求上下文优先。
func (l *requestInfoTransformer) withBaseValueCtx(ctx context.Context) context.Context {
	return &joinValueContext{Context: ctx, baseValueCtx: l.baseValueCtx}
}

// joinValueContext 实现上下文值回退：先查请求上下文，再查预置上下文。
type joinValueContext struct {
	context.Context
	baseValueCtx context.Context
}

// Value 按上述回退顺序查找 key。
func (j *joinValueContext) Value(key any) any {
	if val := j.Context.Value(key); val != nil {
		return val
	}
	return j.baseValueCtx.Value(key)
}
