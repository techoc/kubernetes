/*
Copyright 2019 The Kubernetes Authors.

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

package dynamiccertificates

import (
	"bytes"
	"context"
	"crypto/x509"
	"errors"
	"fmt"
	"os"
	"sync/atomic"
	"time"

	"github.com/fsnotify/fsnotify"
	"k8s.io/client-go/util/cert"

	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/klog/v2"
)

// 该周期被 configmap 变体用于兜底轮询；暴露为变量便于集成测试调小刷新间隔。
var FileRefreshDuration = 1 * time.Minute

// ControllerRunner 抽象出“可单次同步、也可持续运行”的控制器，便于调用方选择同步预热或后台运行。
type ControllerRunner interface {
	// RunOnce 只运行一次同步循环，用于启动前预热，避免对外暴露尚未加载的证书内容。
	RunOnce(ctx context.Context) error

	// Run 应以 go .Run 方式调用，阻塞运行直至 ctx 结束；workers 是工作协程数，本包实现会忽略该参数（始终单 worker）。
	Run(ctx context.Context, workers int)
}

// DynamicFileCAContent 提供可随文件内容变化动态更新的 CAContentProvider，文件变化时重建 x509 校验池并通知下游监听者；同时实现 authenticator 接口以提供 verify options。
// 读取走 atomic.Value 因而无锁，写入由单 worker 串行化以保证一致性。
type DynamicFileCAContent struct {
	// name 是 purpose::filename 形式的唯一标识，用于日志、指标与队列命名。
	name string

	// filename 是待读取的 CA 文件路径；每次重载都重新读盘，因此文件可被原地替换。
	filename string

	// caBundle 保存 *caBundleAndVerifier，即最近一次读取到的非空文件内容；使用 atomic.Value 让读取方无需加锁即可拿到一致快照。
	caBundle atomic.Value

	// listeners 在 CA 内容真正变化时被 Enqueue，由各上层控制器自行决定重新加载的时机。
	listeners []Listener

	// queue 至多只有一个 workItemKey，但借用 workqueue 获得失败退避与重试语义。
	queue workqueue.TypedRateLimitingInterface[string]
}

// 编译期接口断言：DynamicFileCAContent 需同时满足 Notifier、CAContentProvider 与 ControllerRunner。
var _ Notifier = &DynamicFileCAContent{}
var _ CAContentProvider = &DynamicFileCAContent{}
var _ ControllerRunner = &DynamicFileCAContent{}

// caBundleAndVerifier 把原始 CA 字节与由其构建的校验参数绑定在一起，
// 确保任一次读取得到的字节和校验池始终来自同一份文件内容。
type caBundleAndVerifier struct {
	caBundle      []byte
	verifyOptions x509.VerifyOptions
}

// NewDynamicCAContentFromFile 基于文件名创建会自动重载内容的 CAContentProvider，并在返回前完成首次加载（快速失败）。
func NewDynamicCAContentFromFile(purpose, filename string) (*DynamicFileCAContent, error) {
	if len(filename) == 0 {
		// 没有文件名就无从监听，直接报错，避免返回一个永远为空的提供者。
		return nil, fmt.Errorf("missing filename for ca bundle")
	}
	// 用 purpose 区分不同用途的 CA，日志、指标与队列名都依赖它来定位来源。
	name := fmt.Sprintf("%s::%s", purpose, filename)

	// 队列名带上 purpose，便于排查重试来自哪个 CA。
	ret := &DynamicFileCAContent{
		name:     name,
		filename: filename,
		queue: workqueue.NewTypedRateLimitingQueueWithConfig(
			workqueue.DefaultTypedControllerRateLimiter[string](),
			workqueue.TypedRateLimitingQueueConfig[string]{Name: fmt.Sprintf("DynamicCABundle-%s", purpose)},
		),
	}
	if err := ret.loadCABundle(); err != nil {
		// 首次加载失败则构造失败，调用方不必处理“半初始化”状态。
		return nil, err
	}

	return ret, nil
}

// AddListener 注册变更回调；listeners 无锁，因而必须在 Run 之前调用。
func (c *DynamicFileCAContent) AddListener(listener Listener) {
	c.listeners = append(c.listeners, listener)
}

// loadCABundle 重新读取文件；内容未变时直接返回，避免无谓地重建校验池并惊动下游。
func (c *DynamicFileCAContent) loadCABundle() error {
	caBundle, err := os.ReadFile(c.filename)
	if err != nil {
		return err
	}
	if len(caBundle) == 0 {
		// 空文件通常意味着写入尚未完成或配置错误；保留旧内容比切换到空值更安全。
		return fmt.Errorf("missing content for CA bundle %q", c.Name())
	}

	// 字节相同则无需重建校验池，也无需通知监听者。
	if !c.hasCAChanged(caBundle) {
		return nil
	}

	// 先完成解析再原子替换：读取方要么看到旧值，要么看到完整可用的新值。
	caBundleAndVerifier, err := newCABundleAndVerifier(c.Name(), caBundle)
	if err != nil {
		return err
	}
	c.caBundle.Store(caBundleAndVerifier)
	klog.V(2).InfoS("Loaded a new CA Bundle and Verifier", "name", c.Name())

	// 通知下游（如 client CA 认证器）取用新内容。
	for _, listener := range c.listeners {
		listener.Enqueue()
	}

	return nil
}

// hasCAChanged 以字节比较判断是否需要重载；类型异常时保守地视为“已变化”。
func (c *DynamicFileCAContent) hasCAChanged(caBundle []byte) bool {
	uncastExisting := c.caBundle.Load()
	if uncastExisting == nil {
		// 首次加载，必然视为变化。
		return true
	}

	// 检查是否有变化。若值相同则不处理。
	existing, ok := uncastExisting.(*caBundleAndVerifier)
	if !ok {
		// 理论上不会发生；返回 true 让上层重新解析，避免静默沿用错误状态。
		return true
	}
	if !bytes.Equal(existing.caBundle, caBundle) {
		return true
	}

	return false
}

// RunOnce 只加载一次，供同步预热使用。
func (c *DynamicFileCAContent) RunOnce(ctx context.Context) error {
	return c.loadCABundle()
}

// Run 启动 worker 与文件 watch 两个后台循环，并阻塞至 ctx 取消。
func (c *DynamicFileCAContent) Run(ctx context.Context, workers int) {
	defer utilruntime.HandleCrashWithContext(ctx)
	defer c.queue.ShutDown()

	klog.InfoS("Starting controller", "name", c.name)
	defer klog.InfoS("Shutting down controller", "name", c.name)

	// 单 worker 即可：同一时刻只应有一个加载者，避免并发重建校验池。
	go wait.Until(c.runWorker, time.Second, ctx.Done())

	// watch 本身可能失败（例如文件被删除），外层用 wait.Until 周期性重建；
	// 即使 watch 失效，worker 仍会按队列退避重试，不会永久停止刷新。
	go wait.Until(func() {
		if err := c.watchCAFile(ctx.Done()); err != nil {
			klog.ErrorS(err, "Failed to watch CA file, will retry later")
		}
	}, time.Minute, ctx.Done())

	<-ctx.Done()
}

// watchCAFile 监听文件系统事件；返回错误后由上层重建 watch。
func (c *DynamicFileCAContent) watchCAFile(stopCh <-chan struct{}) error {
	// 先入队一次，保证即使 watch 创建失败也能通过队列重试加载。
	c.queue.Add(workItemKey)

	w, err := fsnotify.NewWatcher()
	if err != nil {
		return fmt.Errorf("error creating fsnotify watcher: %v", err)
	}
	defer w.Close()

	if err = w.Add(c.filename); err != nil {
		return fmt.Errorf("error adding watch for file %s: %v", c.filename, err)
	}
	// watch 建立前可能已有更新，补一次检查覆盖该窗口。
	c.queue.Add(workItemKey)

	for {
		select {
		case e := <-w.Events:
			// Kubernetes 中配置文件常通过 symlink + 原子 rename 更新，需要重置 watch。
			if err := c.handleWatchEvent(e, w); err != nil {
				return err
			}
		case err := <-w.Errors:
			// 无法继续可靠监听，返回让上层重建。
			return fmt.Errorf("received fsnotify error: %v", err)
		case <-stopCh:
			return nil
		}
	}
}

// handleWatchEvent 对任意事件都触发一次重载；Remove/Rename 后必须重建 watch，因为底层 inode 可能已更换。
func (c *DynamicFileCAContent) handleWatchEvent(e fsnotify.Event, w *fsnotify.Watcher) error {
	// 应在重启 watch（若适用）之后执行，以确保不会漏掉任何文件事件。
	defer c.queue.Add(workItemKey)
	if !e.Has(fsnotify.Remove) && !e.Has(fsnotify.Rename) {
		return nil
	}
	// ErrNonExistentWatch 说明 watch 已不存在，属于预期竞态，无需告警。
	if err := w.Remove(c.filename); err != nil && !errors.Is(err, fsnotify.ErrNonExistentWatch) {
		klog.InfoS("Failed to remove file watch, it may have been deleted", "file", c.filename, "err", err)
	}
	// 重新 Add 以跟随新文件（例如 symlink 指向的 inode 已变化）。
	if err := w.Add(c.filename); err != nil {
		return fmt.Errorf("error adding watch for file %s: %v", c.filename, err)
	}
	return nil
}

// runWorker 串行消费队列，直到队列被关闭。
func (c *DynamicFileCAContent) runWorker() {
	for c.processNextWorkItem() {
	}
}

// processNextWorkItem 处理一条队列项；返回 false 表示队列已关闭。
func (c *DynamicFileCAContent) processNextWorkItem() bool {
	dsKey, quit := c.queue.Get()
	if quit {
		return false
	}
	defer c.queue.Done(dsKey)

	err := c.loadCABundle()
	if err == nil {
		// 成功后 Forget，避免退避计数影响下一次故障的首次重试间隔。
		c.queue.Forget(dsKey)
		return true
	}

	// 失败按 rate limiter 退避重试，保证文件暂时不可读时能够恢复。
	utilruntime.HandleError(fmt.Errorf("%v failed with : %v", dsKey, err))
	c.queue.AddRateLimited(dsKey)

	return true
}

// Name 返回 purpose::filename 标识，供监听者与日志区分来源。
func (c *DynamicFileCAContent) Name() string {
	return c.name
}

// CurrentCABundleContent 提供 CA bundle 的字节内容；其假定构造时已完成首次加载，因此可直接断言类型。
func (c *DynamicFileCAContent) CurrentCABundleContent() (cabundle []byte) {
	return c.caBundle.Load().(*caBundleAndVerifier).caBundle
}

// VerifyOptions 返回与 authenticator 兼容的、基于当前 CA 的校验参数；未加载时 ok=false，调用方应据此拒绝认证。
func (c *DynamicFileCAContent) VerifyOptions() (x509.VerifyOptions, bool) {
	uncastObj := c.caBundle.Load()
	if uncastObj == nil {
		return x509.VerifyOptions{}, false
	}

	return uncastObj.(*caBundleAndVerifier).verifyOptions, true
}

// newVerifyOptions 从文件创建新的校验函数：读取内容后即构建，失败即返回；若传入空的 CA 文件则返回 nil 函数。
// 这里在读取时一次性构建好 verifyOptions，使认证路径无需重复解析 PEM。
func newCABundleAndVerifier(name string, caBundle []byte) (*caBundleAndVerifier, error) {
	if len(caBundle) == 0 {
		// 空内容无法构建 RootPool，提前返回明确错误。
		return nil, fmt.Errorf("missing content for CA bundle %q", name)
	}

	// 在此包装 x509 verifier：CA 内容无法解析成 PEM 证书时随即失败，避免把坏内容发布出去。
	var err error
	verifyOptions := defaultVerifyOptions()
	verifyOptions.Roots, err = cert.NewPoolFromBytes(caBundle)
	if err != nil {
		return nil, fmt.Errorf("error loading CA bundle for %q: %v", name, err)
	}

	return &caBundleAndVerifier{
		caBundle:      caBundle,
		verifyOptions: verifyOptions,
	}, nil
}

// defaultVerifyOptions 返回使用系统根证书与当前时间、且要求证书适用于客户端认证（x509.ExtKeyUsageClientAuth）的 VerifyOptions。
// 实现上仅显式限定 KeyUsages；CurrentTime 与 Intermediates 留空，由 crypto/x509 使用默认值。
func defaultVerifyOptions() x509.VerifyOptions {
	return x509.VerifyOptions{
		KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	}
}
