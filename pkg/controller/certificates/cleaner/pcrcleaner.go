/*
Copyright 2025 The Kubernetes Authors.

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

package cleaner

import (
	"context"
	"fmt"
	"time"

	certsv1 "k8s.io/api/certificates/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	certinformersv1 "k8s.io/client-go/informers/certificates/v1"
	"k8s.io/client-go/kubernetes"
	certlistersv1 "k8s.io/client-go/listers/certificates/v1"
	"k8s.io/klog/v2"
	"k8s.io/utils/clock"
	"k8s.io/utils/ptr"
)

// PCRCleanerController 负责垃圾回收超过 30 分钟的 PodCertificateRequest。
//
// PodCertificateRequest 是短生命周期的签发请求：一旦证书已经签发、或者请求
// 长期无人处理，它留在集群里就没有价值了。控制器通过定期轮询 informer 缓存，
// 把创建时间早于阈值的请求删掉，避免对象无限累积。
type PCRCleanerController struct {
	client          kubernetes.Interface
	pcrLister       certlistersv1.PodCertificateRequestLister
	clock           clock.PassiveClock
	threshold       time.Duration
	pollingInterval time.Duration
}

// NewPCRCleanerController 构造一个 PCRCleanerController。
//
// 从 informer 取得的是 Lister 而非 informer 本身，这样控制器只读取本地缓存，
// 不会给 apiserver 增加额外的 List 请求；clock 作为依赖注入，便于测试里精确
// 控制“请求是否已过期”的时间判断。
func NewPCRCleanerController(
	client kubernetes.Interface,
	pcrLister certinformersv1.PodCertificateRequestInformer,
	clock clock.PassiveClock,
	threshold time.Duration,
	pollingInterval time.Duration,
) *PCRCleanerController {
	return &PCRCleanerController{
		client:          client,
		pcrLister:       pcrLister.Lister(),
		clock:           clock,
		threshold:       threshold,
		pollingInterval: pollingInterval,
	}
}

// Run 启动清理控制器的主循环，直到 ctx 结束才返回。
//
// 这里不使用 workers 参数：清理逻辑只是遍历本地缓存并发出删除请求，属于
// 轻量操作，单个 worker 配合轮询间隔已经足够；wait.UntilWithContext 保证
// 每轮 worker 之间按 pollingInterval 间隔执行，并在 ctx 取消时立即退出。
func (c *PCRCleanerController) Run(ctx context.Context, workers int) {
	defer utilruntime.HandleCrash()

	logger := klog.FromContext(ctx)
	logger.Info("Starting PodCertificateRequest cleaner controller")
	defer logger.Info("Shutting down PodCertificateRequest cleaner controller")

	wait.UntilWithContext(ctx, c.worker, c.pollingInterval)
}

// worker 执行一轮清理：列出缓存中的全部 PodCertificateRequest，逐个判断是否过期。
//
// 从 Lister 读取的是本地缓存，因此 List 理论上不会失败，但保留错误处理可以
// 防御缓存异常；单个对象处理失败只记录日志并继续，避免一条坏数据阻塞整轮清理。
func (c *PCRCleanerController) worker(ctx context.Context) {
	pcrs, err := c.pcrLister.List(labels.Everything())
	if err != nil {
		utilruntime.HandleErrorWithContext(ctx, err, "Unable to list PodCertificateRequests")
		return
	}
	for _, pcr := range pcrs {
		if err := c.handle(ctx, pcr); err != nil {
			utilruntime.HandleErrorWithContext(ctx, err, "Error while attempting to clean PodCertificateRequest", "pcr", pcr.ObjectMeta.Namespace+"/"+pcr.ObjectMeta.Name)
		}
	}
}

// handle 判断单个 PodCertificateRequest 是否过期，过期则删除。
//
// 用“创建时间 + 阈值”与当前时间比较，而不是依据状态字段，是因为即使签发流程
// 中途失败或卡住，请求也不该永久留存；删除时带上 UID 前置条件，防止在缓存
// 与服务端状态不一致时误删掉同名但已被重建的新对象。
func (c PCRCleanerController) handle(ctx context.Context, pcr *certsv1.PodCertificateRequest) error {
	// 尚未达到阈值就跳过，保留请求直到下一轮轮询再次检查。
	if c.clock.Now().Before(pcr.ObjectMeta.CreationTimestamp.Time.Add(c.threshold)) {
		return nil
	}

	opts := metav1.DeleteOptions{
		Preconditions: &metav1.Preconditions{
			UID: ptr.To(pcr.ObjectMeta.UID),
		},
	}

	err := c.client.CertificatesV1().PodCertificateRequests(pcr.ObjectMeta.Namespace).Delete(ctx, pcr.ObjectMeta.Name, opts)
	if k8serrors.IsNotFound(err) {
		// 对象已经不存在说明清理目标已达成，别人抢先删掉也无需报错重试。
		return nil
	} else if err != nil {
		return fmt.Errorf("unable to delete PodCertificateRequest %q: %w", pcr.ObjectMeta.Namespace+"/"+pcr.ObjectMeta.Name, err)
	}

	return nil
}
