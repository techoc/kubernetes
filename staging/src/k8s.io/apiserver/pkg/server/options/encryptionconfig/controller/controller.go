/*
Copyright 2022 The Kubernetes Authors.

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

// Package controller 实现加密配置文件的自动热加载：周期性比对文件哈希，
// 发现变化后加载新配置、校验新 KMS 插件健康，再原子替换 transformers。
// 校验失败时保留旧配置，避免一次错误编辑导致数据不可读。
package controller

import (
	"context"
	"fmt"
	"net/http"
	"sync"
	"time"

	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/apiserver/pkg/server/healthz"
	"k8s.io/apiserver/pkg/server/options/encryptionconfig"
	"k8s.io/apiserver/pkg/server/options/encryptionconfig/metrics"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/klog/v2"
)

// workqueueKey 是处理加密配置文件变更时使用的固定（dummy）队列 key；
// 队列只需要“有一个待处理信号”，因此固定使用同一个 key。
const workqueueKey = "key"

// EncryptionConfigFileChangePollDuration 对外暴露，便于集成测试加快重载节奏。
//
// 采用轮询而非 fsnotify：配置文件常通过 ConfigMap 挂载，其更新是目录
// 级的原子替换，inotify 行为在不同文件系统上不可靠。
var EncryptionConfigFileChangePollDuration = time.Minute

// DynamicEncryptionConfigContent 可动态响应加密配置文件的变更。
//
// 字段 getEncryptionConfigHash / loadEncryptionConfig 以函数形式保存，
// 便于测试替换为受控实现。
type DynamicEncryptionConfigContent struct {
	name string

	// filePath 是要读取的文件路径。
	filePath string

	// lastLoadedEncryptionConfigHash 保存上次成功读取的加密配置内容。
	lastLoadedEncryptionConfigHash string

	// 处理加密配置变化的队列。
	queue workqueue.TypedRateLimitingInterface[string]

	// 配置变化时 dynamicTransformers 更新 transformer。
	dynamicTransformers *encryptionconfig.DynamicTransformers

	// apiserver 的身份标识
	apiServerID string

	// 测试时可替换
	getEncryptionConfigHash func(ctx context.Context, filepath string) (string, error)
	loadEncryptionConfig    func(ctx context.Context, filepath string, reload bool, apiServerID string) (*encryptionconfig.EncryptionConfiguration, error)
}

func init() {
	metrics.RegisterMetrics()
}

// NewDynamicEncryptionConfiguration 返回动态响应加密配置变化的控制器。
func NewDynamicEncryptionConfiguration(
	name, filePath string,
	dynamicTransformers *encryptionconfig.DynamicTransformers,
	configContentHash string,
	apiServerID string,
) *DynamicEncryptionConfigContent {
	return &DynamicEncryptionConfigContent{
		name:                           name,
		filePath:                       filePath,
		lastLoadedEncryptionConfigHash: configContentHash,
		dynamicTransformers:            dynamicTransformers,
		queue: workqueue.NewTypedRateLimitingQueueWithConfig(
			workqueue.DefaultTypedControllerRateLimiter[string](),
			workqueue.TypedRateLimitingQueueConfig[string]{Name: name},
		),
		apiServerID: apiServerID,
		getEncryptionConfigHash: func(_ context.Context, filepath string) (string, error) {
			return encryptionconfig.GetEncryptionConfigHash(filepath)
		},
		loadEncryptionConfig: encryptionconfig.LoadEncryptionConfig,
	}
}

// Run 启动控制器并阻塞直到 ctx 被取消。
//
// 两个 goroutine：一个在 ctx 取消时关闭队列，另一个消费队列；主 goroutine
// 负责按间隔投递触发信号，并在退出前等待二者结束。
func (d *DynamicEncryptionConfigContent) Run(ctx context.Context) {
	defer utilruntime.HandleCrashWithContext(ctx)

	klog.InfoS("Starting controller", "name", d.name)
	defer klog.InfoS("Shutting down controller", "name", d.name)

	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer utilruntime.HandleCrashWithContext(ctx)
		defer wg.Done()
		defer d.queue.ShutDown()
		<-ctx.Done()
	}()

	wg.Add(1)
	go func() {
		defer utilruntime.HandleCrashWithContext(ctx)
		defer wg.Done()
		d.runWorker(ctx)
	}()

	// 此函数通过向队列放入哑键来轮询配置变化。
	// runWorker 取出哑键并处理变化。
	// ctx 取消时 goroutine 退出。
	_ = wait.PollUntilContextCancel(
		ctx,
		EncryptionConfigFileChangePollDuration,
		true,
		func(ctx context.Context) (bool, error) {
			// 向队列添加哑项以触发文件内容处理。
			d.queue.Add(workqueueKey)

			// 返回 false 以继续轮询。
			return false, nil
		},
	)

	wg.Wait()
}

// runWorker 处理文件内容
func (d *DynamicEncryptionConfigContent) runWorker(ctx context.Context) {
	for d.processNextWorkItem(ctx) {
	}
}

// 队列有消息时 processNextWorkItem 处理文件内容。
func (d *DynamicEncryptionConfigContent) processNextWorkItem(serverCtx context.Context) bool {
	// 这里的 key 是触发文件内容处理的队列哑项。
	key, quit := d.queue.Get()
	if quit {
		return false
	}
	defer d.queue.Done(key)

	d.processWorkItem(serverCtx, key)

	return true
}

func (d *DynamicEncryptionConfigContent) processWorkItem(serverCtx context.Context, workqueueKey string) {
	// 注意：这里的 workqueueKey 参数遮蔽了同名常量；它只用于出错时重新
	// 入队，值始终是常量 "key"。
	var (
		updatedEffectiveConfig  bool
		err                     error
		encryptionConfiguration *encryptionconfig.EncryptionConfiguration
		configChanged           bool
	)

	// 取得 context，以便在出错或下次重载时关闭新的 transformer
	// serverCtx 绑定 apiserver 生命周期，关停时总能关闭 transformer
	ctx, closeTransformers := context.WithCancel(serverCtx)

	defer func() {
		// TODO: workqueue 指标能帮上忙吗？

		if !updatedEffectiveConfig {
			// 若新构造的 transformers 未被采用（因出错或配置未变化），必须关闭以避免泄漏：
			// 新 transformers 可能已启动 KMS 连接，未启用时必须关闭。
			closeTransformers()
		}

		if updatedEffectiveConfig && err == nil {
			metrics.RecordEncryptionConfigAutomaticReloadSuccess(d.apiServerID, encryptionConfiguration.EncryptionFileContentHash)
		}

		if err != nil {
			metrics.RecordEncryptionConfigAutomaticReloadFailure(d.apiServerID)
			utilruntime.HandleError(fmt.Errorf("error processing encryption config file %s: %v", d.filePath, err))
			// 把哑项放回队列以触发文件内容处理。
			d.queue.AddRateLimited(workqueueKey)
		}
	}()

	encryptionConfiguration, configChanged, err = d.processEncryptionConfig(ctx)
	if err != nil {
		return
	}
	if !configChanged {
		return
	}

	if len(encryptionConfiguration.HealthChecks) != 1 {
		err = fmt.Errorf("unexpected number of healthz checks: %d. Should have only one", len(encryptionConfiguration.HealthChecks))
		return
	}
	// 获取全部新 KMS 插件的 healthz 检查。
	// 先健康检查再切换：若新配置的 KMS 不可用，立即切换会让后续写入
	// 失败甚至不可用，因此失败时保持旧配置继续服务。
	if err = d.validateNewTransformersHealth(ctx, encryptionConfiguration.HealthChecks[0], encryptionConfiguration.KMSCloseGracePeriod); err != nil {
		return
	}

	// 更新 transformer。
	// reload=true 时必须始终有一个 healthz 检查。
	d.dynamicTransformers.Set(
		encryptionConfiguration.Transformers,
		closeTransformers,
		encryptionConfiguration.HealthChecks[0],
		encryptionConfiguration.KMSCloseGracePeriod,
	)

	// 更新成功后更新本地保存的近期配置内容。
	d.lastLoadedEncryptionConfigHash = encryptionConfiguration.EncryptionFileContentHash
	klog.V(2).InfoS("Loaded new kms encryption config content", "name", d.name)

	updatedEffectiveConfig = true
}

// processEncryptionConfig 处理文件中的下一份加密配置内容。
//
// 两次哈希比较：加载前避免无谓解析，加载后再比一次以规避 TOCTOU
// （文件在读取过程中又被替换）。
func (d *DynamicEncryptionConfigContent) processEncryptionConfig(ctx context.Context) (
	_ *encryptionconfig.EncryptionConfiguration,
	configChanged bool,
	_ error,
) {
	contentHash, err := d.getEncryptionConfigHash(ctx, d.filePath)
	if err != nil {
		return nil, false, err
	}

	// 比较加密配置与当前是否不同；相同则不做处理。
	if contentHash == d.lastLoadedEncryptionConfigHash {
		klog.V(4).InfoS("Encryption config has not changed (before load)", "name", d.name)
		return nil, false, nil
	}

	// 此分支仅在 reload=true 时执行，因此显式传入 true。
	encryptionConfiguration, err := d.loadEncryptionConfig(ctx, d.filePath, true, d.apiServerID)
	if err != nil {
		return nil, false, err
	}

	// 再次比较配置与当前是否不同（避免 TOCTOU）；相同则不做处理。
	if encryptionConfiguration.EncryptionFileContentHash == d.lastLoadedEncryptionConfigHash {
		klog.V(4).InfoS("Encryption config has not changed (after load)", "name", d.name)
		return nil, false, nil
	}

	return encryptionConfiguration, true, nil
}

// 单元测试可降低 minKMSPluginCloseGracePeriod 以加快健康检查轮询
var minKMSPluginCloseGracePeriod = 10 * time.Second

// validateNewTransformersHealth 轮询 KMS 插件的 healthz，直到通过或超时。
// 轮询而非单次检查，是因为插件进程可能比 apiserver 晚就绪。
func (d *DynamicEncryptionConfigContent) validateNewTransformersHealth(
	ctx context.Context,
	kmsPluginHealthzCheck healthz.HealthChecker,
	kmsPluginCloseGracePeriod time.Duration,
) error {
	// 测试新 transformer 是否健康
	var healthCheckError error

	if kmsPluginCloseGracePeriod < minKMSPluginCloseGracePeriod {
		kmsPluginCloseGracePeriod = minKMSPluginCloseGracePeriod
	}

	// 务必确保立即执行的 Check 不会挂住：外层再套一层超时，防止单次 Check 卡住导致整个 reload 永久阻塞。
	var cancel context.CancelFunc
	ctx, cancel = context.WithTimeout(ctx, kmsPluginCloseGracePeriod)
	defer cancel()

	pollErr := wait.PollImmediateWithContext(ctx, 100*time.Millisecond, kmsPluginCloseGracePeriod, func(ctx context.Context) (bool, error) {
		// 构造发往健康检查端点的假 HTTP GET 请求
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, fmt.Sprintf("/healthz/%s", kmsPluginHealthzCheck.Name()), nil)
		if err != nil {
			return false, err
		}

		healthCheckError = kmsPluginHealthzCheck.Check(req)
		return healthCheckError == nil, nil
	})
	if pollErr != nil {
		return fmt.Errorf("health check for new transformers failed, polling error %v: %w", pollErr, healthCheckError)
	}
	klog.V(2).InfoS("Health check succeeded")
	return nil
}
