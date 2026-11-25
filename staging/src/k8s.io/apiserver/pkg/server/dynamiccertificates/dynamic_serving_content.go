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
	"context"
	"crypto/tls"
	"fmt"
	"os"
	"sync/atomic"
	"time"

	"github.com/fsnotify/fsnotify"

	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/klog/v2"
)

// 它监听证书/私钥文件变化并原子替换内存中的内容，供 TLS 握手并发读取。
type DynamicCertKeyPairContent struct {
	name string

	// certFile is the name of the certificate file to read.
	certFile string
	// keyFile is the name of the key file to read.
	keyFile string

	// certKeyPair 保存 *certKeyContent，即最近一次读取到的证书与私钥的非空内容；用 atomic.Value 发布，读路径无需锁，写入的是不可变快照。
	certKeyPair atomic.Value

	listeners []Listener

	// queue only ever has one item, but it has nice error handling backoff/retry semantics
	queue workqueue.TypedRateLimitingInterface[string]
}

// 编译期断言：同时实现内容提供者与控制器运行接口。
var _ CertKeyContentProvider = &DynamicCertKeyPairContent{}
var _ ControllerRunner = &DynamicCertKeyPairContent{}

// NewDynamicServingContentFromFiles 基于证书与私钥文件名返回一个动态的 CertKeyContentProvider。
// 构造时立即加载一次，确保启动即暴露文件缺失或证书不匹配的问题。
func NewDynamicServingContentFromFiles(purpose, certFile, keyFile string) (*DynamicCertKeyPairContent, error) {
	if len(certFile) == 0 || len(keyFile) == 0 {
		return nil, fmt.Errorf("missing filename for serving cert")
	}
	name := fmt.Sprintf("%s::%s::%s", purpose, certFile, keyFile)

	ret := &DynamicCertKeyPairContent{
		name:     name,
		certFile: certFile,
		keyFile:  keyFile,
		queue: workqueue.NewTypedRateLimitingQueueWithConfig(
			workqueue.DefaultTypedControllerRateLimiter[string](),
			workqueue.TypedRateLimitingQueueConfig[string]{Name: fmt.Sprintf("DynamicCABundle-%s", purpose)},
		),
	}
	if err := ret.loadCertKeyPair(); err != nil {
		return nil, err
	}

	return ret, nil
}

// AddListener 注册在服务证书内容变化时收到通知的监听者；必须在 Run 之前注册，运行期并发修改 listeners 不受支持。
func (c *DynamicCertKeyPairContent) AddListener(listener Listener) {
	c.listeners = append(c.listeners, listener)
}

// loadCertKeyPair determines the next set of content for the file.
func (c *DynamicCertKeyPairContent) loadCertKeyPair() error {
	cert, err := os.ReadFile(c.certFile)
	if err != nil {
		return err
	}
	key, err := os.ReadFile(c.keyFile)
	if err != nil {
		return err
	}
	if len(cert) == 0 || len(key) == 0 {
		return fmt.Errorf("missing content for serving cert %q", c.Name())
	}

	// 确保私钥与证书匹配且均合法：这一步同时校验配对关系；文件更新可能先后到达，不匹配时保留旧内容。
	_, err = tls.X509KeyPair(cert, key)
	if err != nil {
		return err
	}

	newCertKey := &certKeyContent{
		cert: cert,
		key:  key,
	}

	// 检查内容是否变化，相同则什么都不做：未变化时不触发 listener，避免无谓的 TLS 配置重建（会中断连接池）。
	existing, ok := c.certKeyPair.Load().(*certKeyContent)
	if ok && existing != nil && existing.Equal(newCertKey) {
		return nil
	}

	c.certKeyPair.Store(newCertKey)
	klog.V(2).InfoS("Loaded a new cert/key pair", "name", c.Name())

	for _, listener := range c.listeners {
		listener.Enqueue()
	}

	return nil
}

// RunOnce runs a single sync loop
func (c *DynamicCertKeyPairContent) RunOnce(ctx context.Context) error {
	return c.loadCertKeyPair()
}

// Run starts the controller and blocks until context is killed.
func (c *DynamicCertKeyPairContent) Run(ctx context.Context, workers int) {
	defer utilruntime.HandleCrashWithContext(ctx)
	defer c.queue.ShutDown()

	klog.InfoS("Starting controller", "name", c.name)
	defer klog.InfoS("Shutting down controller", "name", c.name)

	// 单 worker 即可：加载是幂等的，且需要串行化 listener 通知。
	go wait.Until(c.runWorker, time.Second, ctx.Done())

	// 启动监听证书与私钥文件的循环，直至 stopCh 关闭；定时重试可覆盖 fsnotify 失效（例如文件被整体替换、inotify 丢失）的情况。
	go wait.Until(func() {
		if err := c.watchCertKeyFile(ctx.Done()); err != nil {
			klog.ErrorS(err, "Failed to watch cert and key file, will retry later")
		}
	}, time.Minute, ctx.Done())

	<-ctx.Done()
}

// watchCertKeyFile 用 fsnotify 监听两个文件；watch 建立前先入队一次以兜底。
func (c *DynamicCertKeyPairContent) watchCertKeyFile(stopCh <-chan struct{}) error {
	// Trigger a check here to ensure the content will be checked periodically even if the following watch fails.
	c.queue.Add(workItemKey)

	w, err := fsnotify.NewWatcher()
	if err != nil {
		return fmt.Errorf("error creating fsnotify watcher: %v", err)
	}
	defer w.Close()

	if err := w.Add(c.certFile); err != nil {
		return fmt.Errorf("error adding watch for file %s: %v", c.certFile, err)
	}
	if err := w.Add(c.keyFile); err != nil {
		return fmt.Errorf("error adding watch for file %s: %v", c.keyFile, err)
	}
	// Trigger a check in case the file is updated before the watch starts.
	c.queue.Add(workItemKey)

	for {
		select {
		case e := <-w.Events:
			if err := c.handleWatchEvent(e, w); err != nil {
				return err
			}
		case err := <-w.Errors:
			return fmt.Errorf("received fsnotify error: %v", err)
		case <-stopCh:
			return nil
		}
	}
}

// handleWatchEvent triggers reloading the cert and key file, and restarts a new watch if it's a Remove or Rename event.
// If one file is updated before the other, the loadCertKeyPair method will catch the mismatch and will not apply the
// change. When an event of the other file is received, it will trigger reloading the files again and the new content
// will be loaded and used.
func (c *DynamicCertKeyPairContent) handleWatchEvent(e fsnotify.Event, w *fsnotify.Watcher) error {
	// 该操作应在重挂 watch（如适用）之后执行，以确保不遗漏文件事件；defer 保证“重挂 watch 失败”时也仍会触发一次重载。
	defer c.queue.Add(workItemKey)
	if !e.Has(fsnotify.Remove) && !e.Has(fsnotify.Rename) {
		return nil
	}
	// kubelet/容器场景常用“rename 后重建”方式更新文件，需重新挂到新 inode 上。
	if err := w.Remove(e.Name); err != nil {
		klog.InfoS("Failed to remove file watch, it may have been deleted", "file", e.Name, "err", err)
	}
	if err := w.Add(e.Name); err != nil {
		return fmt.Errorf("error adding watch for file %s: %v", e.Name, err)
	}
	return nil
}

// runWorker 持续处理队列直到队列关闭。
func (c *DynamicCertKeyPairContent) runWorker() {
	for c.processNextWorkItem() {
	}
}

// processNextWorkItem 处理一个重载请求；失败时按速率限制重试，成功则 Forget。
func (c *DynamicCertKeyPairContent) processNextWorkItem() bool {
	dsKey, quit := c.queue.Get()
	if quit {
		return false
	}
	defer c.queue.Done(dsKey)

	err := c.loadCertKeyPair()
	if err == nil {
		c.queue.Forget(dsKey)
		return true
	}

	utilruntime.HandleError(fmt.Errorf("%v failed with : %v", dsKey, err))
	c.queue.AddRateLimited(dsKey)

	return true
}

// Name is just an identifier
func (c *DynamicCertKeyPairContent) Name() string {
	return c.name
}

// CurrentCertKeyContent provides cert and key byte content
func (c *DynamicCertKeyPairContent) CurrentCertKeyContent() ([]byte, []byte) {
	// 构造时已保证至少加载过一次，因此这里的类型断言是安全的。
	certKeyContent := c.certKeyPair.Load().(*certKeyContent)
	return certKeyContent.cert, certKeyContent.key
}
