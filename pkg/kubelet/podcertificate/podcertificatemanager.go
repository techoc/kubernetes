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

package podcertificate

import (
	"bytes"
	"context"
	"crypto"
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	mathrand "math/rand/v2"
	"sync"
	"time"

	certificatesv1 "k8s.io/api/certificates/v1"
	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	certinformersv1 "k8s.io/client-go/informers/certificates/v1"
	coreinformersv1 "k8s.io/client-go/informers/core/v1"
	"k8s.io/client-go/kubernetes"
	certlistersv1 "k8s.io/client-go/listers/certificates/v1"
	corelistersv1 "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/klog/v2"
	"k8s.io/utils/clock"
)

// PodManager 是 pod.Manager 的本地包装接口。
type PodManager interface {
	GetPodByUID(uid types.UID) (*corev1.Pod, bool)
	GetPods() []*corev1.Pod
}

// Manager 抽象出 kubelet 与卷宿主提供 Pod 证书功能所需的能力。
type Manager interface {
	// TrackPod 在每次有新 Pod 被分配到节点时由 kubelet 调用。
	TrackPod(ctx context.Context, pod *corev1.Pod)
	// ForgetPod 在每次有 Pod 从节点移除时由 kubelet 调用。
	ForgetPod(ctx context.Context, pod *corev1.Pod)

	// GetPodCertificateCredentialBundle 由卷宿主调用，用于获取指定 Pod 证书卷的凭据包。
	GetPodCertificateCredentialBundle(ctx context.Context, namespace, podName, podUID, volumeName string, sourceIndex int) (privKey []byte, certChain []byte, err error)

	// MetricReport 返回该管理器当前 Pod 证书状态的快照。
	MetricReport() *MetricReport
}

// MetricReport 包含 Pod 证书投影卷来源当前状态的指标。
type MetricReport struct {
	PodCertificateStates map[SignerAndState]int
}

// SignerAndState 表示签名者名称与 Pod 证书状态的组合。
type SignerAndState struct {
	SignerName string
	State      string
}

// 超过该时长（含抖动）后，可以认为我们创建但未出现在 informer 中的 PCR 已被删除。
const assumeDeletedThreshold = 10 * time.Minute

// 自证书 BeginRefreshAt 起超过该时长后，视为证书刷新已逾期。
const refreshOverdueDuration = 10 * time.Minute

// IssuingManager 是 Manager 的主要实现。
//
// 核心结构是一个工作队列：调度到本节点的每个非镜像 Pod 中，每个
// PodCertificateProjection 都以 podname/volumename/sourceindex 三元组作为一条队列项。
// 每当 PodCertificateRequest 或 Pod 发生相关变化时，就把可能受影响的
// PodCertificateProjection 重新加入队列。
//
// 状态不会跨重启保留：kubelet 或节点重启后，所有 PodCertificateProjection
// 都会被加入队列并立即刷新。
//
// 刷新通过周期性把所有 PodCertificateProjection 重新加入队列来实现。
type IssuingManager struct {
	kc kubernetes.Interface

	podManager PodManager

	recorder record.EventRecorder

	projectionQueue workqueue.TypedRateLimitingInterface[projectionKey]

	pcrInformer cache.SharedIndexInformer
	pcrLister   certlistersv1.PodCertificateRequestLister

	nodeInformer cache.SharedIndexInformer
	nodeLister   corelistersv1.NodeLister
	nodeName     types.NodeName

	clock clock.PassiveClock

	// lock 保护 credStore。
	lock      sync.Mutex
	credStore map[projectionKey]*projectionRecord
}

type projectionKey struct {
	Namespace   string
	PodName     string
	PodUID      string
	VolumeName  string
	SourceIndex int
}

type projectionRecord struct {
	// lock 保护 projectionRecord 的所有字段。
	lock sync.Mutex

	// 该投影的状态机：
	//
	//
	//                         ┌─────────────────┐
	//                         ▼                 │
	// fresh ────► wait ────► fresh ──────► waitrefresh
	//               │                           │
	//               ├──────► denied ◄───────────┤
	//               │                           │
	//               └──────► failed ◄───────────┘
	curState credState
}

// credState 是所有投影记录状态的接口类型。
type credState interface {
	getCredBundle() (privKey, certChain []byte, err error)
	metricsState(now time.Time) string
}

// credStateInitial 表示尚未发起证书请求的初始状态。
type credStateInitial struct {
}

// getCredBundle 返回尚未签发证书的错误。
func (c *credStateInitial) getCredBundle() ([]byte, []byte, error) {
	return nil, nil, fmt.Errorf("credential bundle is not issued yet")
}

// metricsState 返回 not_yet_issued 指标状态。
func (c *credStateInitial) metricsState(_ time.Time) string {
	return "not_yet_issued"
}

// credStateWait 表示已创建 PCR，正在等待其进入终态。
type credStateWait struct {
	privateKey []byte
	pcrName    string
	// 若到达该时间且 PCR 仍未出现在 informer 中，则认为它已被删除。
	pcrAbandonAt time.Time
}

// getCredBundle 返回尚未签发证书的错误。
func (c *credStateWait) getCredBundle() ([]byte, []byte, error) {
	return nil, nil, fmt.Errorf("credential bundle is not issued yet")
}

// metricsState 返回 not_yet_issued 指标状态。
func (c *credStateWait) metricsState(_ time.Time) string {
	return "not_yet_issued"
}

// credStateDenied 表示 PCR 被永久拒绝。
type credStateDenied struct {
	Reason  string
	Message string
}

// getCredBundle 返回拒绝原因与消息。
func (c *credStateDenied) getCredBundle() ([]byte, []byte, error) {
	return nil, nil, fmt.Errorf("PodCertificateRequest was permanently denied: reason=%q message=%q", c.Reason, c.Message)
}

// metricsState 返回 denied 指标状态。
func (c *credStateDenied) metricsState(_ time.Time) string {
	return "denied"
}

// credStateFailed 表示 PCR 永久失败。
type credStateFailed struct {
	Reason  string
	Message string
}

// getCredBundle 返回失败原因与消息。
func (c *credStateFailed) getCredBundle() ([]byte, []byte, error) {
	return nil, nil, fmt.Errorf("PodCertificateRequest was permanently failed: reason=%q message=%q", c.Reason, c.Message)
}

// metricsState 返回 failed 指标状态。
func (c *credStateFailed) metricsState(_ time.Time) string {
	return "failed"
}

// credStateFresh 表示证书已签发且可用，直到到达刷新时间。
type credStateFresh struct {
	privateKey                       []byte
	certChain                        []byte
	beginRefreshAt                   time.Time
	notAfter                         time.Time
	eventEmittedForOverdueForRefresh bool
	eventEmittedForExpiration        bool
}

// getCredBundle 返回已签发的私钥与证书链。
func (c *credStateFresh) getCredBundle() ([]byte, []byte, error) {
	return c.privateKey, c.certChain, nil
}

// metricsState 根据当前时间返回 fresh、overdue_for_refresh 或 expired。
func (c *credStateFresh) metricsState(now time.Time) string {
	if now.After(c.notAfter) {
		return "expired"
	}
	if now.After(c.beginRefreshAt.Add(refreshOverdueDuration)) {
		return "overdue_for_refresh"
	}
	return "fresh"
}

// credStateWaitRefresh 表示已发起刷新请求，等待新证书签发。
type credStateWaitRefresh struct {
	privateKey                       []byte
	certChain                        []byte
	beginRefreshAt                   time.Time
	notAfter                         time.Time
	eventEmittedForOverdueForRefresh bool
	eventEmittedForExpiration        bool

	refreshPrivateKey []byte
	refreshPCRName    string
	// 若到达该时间且 PCR 仍未出现在 informer 中，则认为它已被删除。
	refreshPCRAbandonAt time.Time
}

// getCredBundle 返回旧证书，直到刷新完成。
func (c *credStateWaitRefresh) getCredBundle() ([]byte, []byte, error) {
	return c.privateKey, c.certChain, nil
}

// metricsState 根据旧证书时间返回 fresh、overdue_for_refresh 或 expired。
func (c *credStateWaitRefresh) metricsState(now time.Time) string {
	if now.After(c.notAfter) {
		return "expired"
	}
	if now.After(c.beginRefreshAt.Add(refreshOverdueDuration)) {
		return "overdue_for_refresh"
	}
	return "fresh"
}

var _ Manager = (*IssuingManager)(nil)

// NewIssuingManager 创建 IssuingManager，并注册 PCR informer 事件处理以尽快重新驱动相关投影。
func NewIssuingManager(kc kubernetes.Interface, podManager PodManager, recorder record.EventRecorder, pcrInformer certinformersv1.PodCertificateRequestInformer, nodeInformer coreinformersv1.NodeInformer, nodeName types.NodeName, clock clock.WithTicker) *IssuingManager {
	m := &IssuingManager{
		kc: kc,

		podManager:      podManager,
		recorder:        recorder,
		projectionQueue: workqueue.NewTypedRateLimitingQueue(workqueue.DefaultTypedControllerRateLimiter[projectionKey]()),

		pcrInformer:  pcrInformer.Informer(),
		pcrLister:    pcrInformer.Lister(),
		nodeInformer: nodeInformer.Informer(),
		nodeLister:   nodeInformer.Lister(),
		nodeName:     nodeName,
		clock:        clock,

		credStore: map[projectionKey]*projectionRecord{},
	}

	// 为 PodCertificateRequest 注册 informer 事件处理：任何情况下都只是
	// 把对应的 PodCertificateProjection 重新加入队列处理。
	// 从正确性上这并非必需，因为队列的退避重试最终也会触发检查；
	// 但一旦证书签发就能立即感知，对用户来说体验更好。
	m.pcrInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj any) {
			pcr := obj.(*certificatesv1.PodCertificateRequest)
			m.queueAllProjectionsForPod(pcr.Spec.PodUID)
		},
		UpdateFunc: func(old, new any) {
			pcr := new.(*certificatesv1.PodCertificateRequest)
			m.queueAllProjectionsForPod(pcr.Spec.PodUID)
		},
		DeleteFunc: func(obj any) {
			pcr := obj.(*certificatesv1.PodCertificateRequest)
			m.queueAllProjectionsForPod(pcr.Spec.PodUID)
		},
	})

	return m
}

// queueAllProjectionsForPod 将指定 Pod 的所有 PodCertificate 投影来源加入工作队列。
func (m *IssuingManager) queueAllProjectionsForPod(uid types.UID) {
	pod, ok := m.podManager.GetPodByUID(uid)
	if !ok {
		return
	}

	for _, v := range pod.Spec.Volumes {
		if v.Projected == nil {
			continue
		}

		for sourceIndex, source := range v.Projected.Sources {
			if source.PodCertificate == nil {
				continue
			}

			key := projectionKey{
				Namespace:   pod.ObjectMeta.Namespace,
				PodName:     pod.ObjectMeta.Name,
				PodUID:      string(pod.ObjectMeta.UID),
				VolumeName:  v.Name,
				SourceIndex: sourceIndex,
			}
			m.projectionQueue.Add(key)
		}
	}
}

// Run 等待缓存同步，周期性触发刷新扫描并运行投影处理循环，直到上下文取消。
func (m *IssuingManager) Run(ctx context.Context) {
	logger := klog.FromContext(ctx)
	logger.Info("podcertificate.IssuingManager starting up")
	// informer 缓存未同步时状态判断不可靠，因此先等待同步。
	if !cache.WaitForCacheSync(ctx.Done(), m.pcrInformer.HasSynced, m.nodeInformer.HasSynced) {
		return
	}

	// 周期扫描用于把到期需要刷新的投影重新入队。
	go wait.JitterUntilWithContext(ctx, m.runRefreshPass, 1*time.Minute, 1.0, false)
	go wait.UntilWithContext(ctx, m.runProjectionProcessor, time.Second)
	<-ctx.Done()

	m.projectionQueue.ShutDown()

	logger.Info("podcertificate.IssuingManager shut down")
}

// runProjectionProcessor 串行消费投影队列。
func (m *IssuingManager) runProjectionProcessor(ctx context.Context) {
	for m.processNextProjection(ctx) {
	}
}

// processNextProjection 处理一个队列项，失败时按限速重新入队。
func (m *IssuingManager) processNextProjection(ctx context.Context) bool {
	key, quit := m.projectionQueue.Get()
	if quit {
		return false
	}
	defer m.projectionQueue.Done(key)

	err := m.handleProjection(ctx, key)
	if err != nil {
		utilruntime.HandleErrorWithContext(ctx, err, "while handling podCertificate projected volume source", "namespace", key.Namespace, "pod", key.PodName, "volume", key.VolumeName, "sourceIndex", key.SourceIndex)
		m.projectionQueue.AddRateLimited(key)
		return true
	}

	m.projectionQueue.Forget(key)
	return true
}

// handleProjection 根据投影记录的当前状态推进状态机：创建 PCR、等待终态或发起刷新。
func (m *IssuingManager) handleProjection(ctx context.Context, key projectionKey) error {
	// 返回 nil 表示该工作项已成功处理，应从队列中移除。

	logger := klog.FromContext(ctx)

	pod, ok := m.podManager.GetPodByUID(types.UID(key.PodUID))
	if !ok {
		// 找不到 Pod 说明它已被删除：清理与其关联的内部状态，并返回 nil 以便队列忘记该项。
		m.cleanupCredStoreForPod(key.Namespace, key.PodName, key.PodUID)

		return nil
	}

	var source *corev1.PodCertificateProjection
	for _, vol := range pod.Spec.Volumes {
		if vol.Name == key.VolumeName && vol.Projected != nil {
			for i, volumeSource := range vol.Projected.Sources {
				if i == key.SourceIndex && volumeSource.PodCertificate != nil {
					source = volumeSource.PodCertificate
				}
			}
		}
	}
	if source == nil {
		// 无论重试多少次都无法解决该问题，记录日志并返回 nil。
		utilruntime.HandleErrorWithContext(ctx, nil, "pod does not contain the named podCertificate projected volume source", "key", key)
		return nil
	}

	var rec *projectionRecord
	func() {
		m.lock.Lock()
		defer m.lock.Unlock()

		rec = m.credStore[key]

		if rec == nil {
			rec = &projectionRecord{
				curState: &credStateInitial{},
			}
			m.credStore[key] = rec
		}
	}()

	// 在函数剩余部分持有该记录的锁。
	rec.lock.Lock()
	defer rec.lock.Unlock()

	switch state := rec.curState.(type) {
	case *credStateInitial:
		// 尚未开始首次签发，需要创建 PCR 并记录到 credStore。

		// 获取 ServiceAccount 以得知其 UID。理想情况下 kubelet 应有中心组件，
		// 通过单项 watch 跟踪节点上 Pod 关联的所有 ServiceAccount。
		serviceAccount, err := m.kc.CoreV1().ServiceAccounts(pod.ObjectMeta.Namespace).Get(ctx, pod.Spec.ServiceAccountName, metav1.GetOptions{})
		if err != nil {
			return fmt.Errorf("while fetching service account: %w", err)
		}

		node, err := m.nodeLister.Get(string(m.nodeName))
		if err != nil {
			return fmt.Errorf("while getting node object from local cache: %w", err)
		}

		privKey, pcr, err := m.createPodCertificateRequest(
			ctx,
			pod.ObjectMeta.Namespace,
			pod.ObjectMeta.Name, pod.ObjectMeta.UID,
			pod.Spec.ServiceAccountName, serviceAccount.ObjectMeta.UID,
			m.nodeName, node.ObjectMeta.UID,
			source.SignerName, source.KeyType, source.MaxExpirationSeconds, source.UserAnnotations,
		)
		if err != nil {
			return fmt.Errorf("while creating initial PodCertificateRequest: %w", err)
		}

		rec.curState = &credStateWait{
			privateKey:   privKey,
			pcrName:      pcr.ObjectMeta.Name,
			pcrAbandonAt: pcr.ObjectMeta.CreationTimestamp.Time.Add(assumeDeletedThreshold + jitterDuration()),
		}

		// 返回 nil 将投影从工作队列移除；待 PodCertificateRequest 出现在 informer
		// 缓存并经过状态更新后，它会再次被加入队列。
		logger.V(4).Info("PodCertificateRequest created, moving to credStateWait", "key", key, "pcr", pcr.ObjectMeta.Namespace+"/"+pcr.ObjectMeta.Name)
		return nil

	case *credStateWait:
		// 正在处理首次签发：PCR 已创建，等待其进入终态。

		pcr, err := m.pcrLister.PodCertificateRequests(key.Namespace).Get(state.pcrName)
		if k8serrors.IsNotFound(err) && m.clock.Now().After(state.pcrAbandonAt) {
			// "Not Found" 可能是 informer 延迟，也可能是 PCR 被删除。
			// 前者应继续等待，后者应重建 PCR。准确区分需要 resourceVersion 有序，
			// 且 lister 能报告其见过的最高版本。在此之前，若距记录创建 PCR 已过 10 分钟，
			// 则假定属于后者，回到 credStateInitial 以创建新的 PCR。
			rec.curState = &credStateInitial{}
			return fmt.Errorf("PodCertificateRequest %q appears to have been deleted", key.Namespace+"/"+state.pcrName)
		} else if err != nil {
			return fmt.Errorf("while getting PodCertificateRequest %q: %w", key.Namespace+"/"+state.pcrName, err)
		}

		// 若 PodCertificateRequest 已进入终态，则相应更新状态机。
		for _, cond := range pcr.Status.Conditions {
			switch cond.Type {
			case certificatesv1.PodCertificateRequestConditionTypeDenied:
				rec.curState = &credStateDenied{
					Reason:  cond.Reason,
					Message: cond.Message,
				}
				logger.V(4).Info("PodCertificateRequest denied, moving to credStateDenied", "key", key, "pcr", pcr.ObjectMeta.Namespace+"/"+pcr.ObjectMeta.Name)
				eventMessage := fmt.Sprintf("PodCertificateRequest %s was denied, reason=%q, message=%q", pcr.ObjectMeta.Namespace+"/"+pcr.ObjectMeta.Name, cond.Reason, cond.Message)
				m.recorder.Eventf(pod, corev1.EventTypeWarning, certificatesv1.PodCertificateRequestConditionTypeDenied, cond.Reason, eventMessage)
				return nil
			case certificatesv1.PodCertificateRequestConditionTypeFailed:
				rec.curState = &credStateFailed{
					Reason:  cond.Reason,
					Message: cond.Message,
				}
				logger.V(4).Info("PodCertificateRequest failed, moving to credStateFailed", "key", key, "pcr", pcr.ObjectMeta.Namespace+"/"+pcr.ObjectMeta.Name)
				eventMessage := fmt.Sprintf("PodCertificateRequest %s failed, reason=%q, message=%q", pcr.ObjectMeta.Namespace+"/"+pcr.ObjectMeta.Name, cond.Reason, cond.Message)
				m.recorder.Eventf(pod, corev1.EventTypeWarning, certificatesv1.PodCertificateRequestConditionTypeFailed, cond.Reason, eventMessage)
				return nil
			case certificatesv1.PodCertificateRequestConditionTypeIssued:
				rec.curState = &credStateFresh{
					privateKey:     state.privateKey,
					certChain:      cleanCertificateChain([]byte(pcr.Status.CertificateChain)),
					beginRefreshAt: pcr.Status.BeginRefreshAt.Time.Add(jitterDuration()),
					notAfter:       pcr.Status.NotAfter.Time,
				}
				logger.V(4).Info("PodCertificateRequest issued, moving to credStateFresh", "key", key, "pcr", pcr.ObjectMeta.Namespace+"/"+pcr.ObjectMeta.Name)
				return nil
			}
		}

		// 没有变化：请求仍处于 pending。返回 nil 将投影从队列移除，
		// 待 PodCertificateRequest 更新时会再次被驱动。
		logger.V(4).Info("PodCertificateRequest not in terminal state, remaining in credStateWait", "key", key, "pcr", pcr.ObjectMeta.Namespace+"/"+pcr.ObjectMeta.Name)
		return nil

	case *credStateDenied:
		// 无需处理；这是 Pod 的永久错误状态。
		logger.V(4).Info("staying in credStateDenied", "key", key)
		return nil

	case *credStateFailed:
		// 无需处理；这是 Pod 的永久错误状态。
		logger.V(4).Info("staying in credStateFailed", "key", key)
		return nil

	case *credStateFresh:
		// 在到达刷新时间前不做任何操作，然后创建新 PCR 并切换到 credStateWaitRefresh。

		if m.clock.Now().Before(state.beginRefreshAt) {
			// 尚未到刷新时间，不做任何操作。
			return nil
		}

		logger.V(4).Info("Time to refresh", "key", key)

		// 当前时间已超过最近签发证书 `beginRefreshAt` 10 分钟以上，但状态尚未标记为刷新逾期。
		if m.clock.Now().After(state.beginRefreshAt.Add(refreshOverdueDuration)) && !state.eventEmittedForOverdueForRefresh {
			logger.V(4).Info("Refresh overdue", "key", key)
			m.recorder.Eventf(pod, corev1.EventTypeWarning, "CertificateOverdueForRefresh", "PodCertificate refresh overdue")
			state.eventEmittedForOverdueForRefresh = true
		}

		// 当前时间已超过最近签发证书的 `notAfter`，但状态尚未标记为已过期。
		if m.clock.Now().After(state.notAfter) && !state.eventEmittedForExpiration {
			logger.V(4).Info("Certificates expired", "key", key)
			m.recorder.Eventf(pod, corev1.EventTypeWarning, "CertificateExpired", "PodCertificate expired")
			state.eventEmittedForExpiration = true
		}

		// 获取 ServiceAccount 以得知其 UID。理想情况下 kubelet 应有中心组件，
		// 通过单项 watch 跟踪节点上 Pod 关联的所有 ServiceAccount。
		serviceAccount, err := m.kc.CoreV1().ServiceAccounts(pod.ObjectMeta.Namespace).Get(ctx, pod.Spec.ServiceAccountName, metav1.GetOptions{})
		if err != nil {
			return fmt.Errorf("while fetching service account: %w", err)
		}

		node, err := m.nodeLister.Get(string(m.nodeName))
		if err != nil {
			return fmt.Errorf("while getting node object from local cache: %w", err)
		}

		privKey, pcr, err := m.createPodCertificateRequest(
			ctx,
			pod.ObjectMeta.Namespace,
			pod.ObjectMeta.Name, pod.ObjectMeta.UID,
			pod.Spec.ServiceAccountName, serviceAccount.ObjectMeta.UID,
			m.nodeName, node.ObjectMeta.UID,
			source.SignerName, source.KeyType, source.MaxExpirationSeconds, source.UserAnnotations,
		)
		if err != nil {
			return fmt.Errorf("while creating refresh PodCertificateRequest: %w", err)
		}

		rec.curState = &credStateWaitRefresh{
			privateKey:                       state.privateKey,
			certChain:                        state.certChain,
			beginRefreshAt:                   state.beginRefreshAt,
			notAfter:                         state.notAfter,
			eventEmittedForOverdueForRefresh: state.eventEmittedForOverdueForRefresh,
			eventEmittedForExpiration:        state.eventEmittedForExpiration,

			refreshPrivateKey:   privKey,
			refreshPCRName:      pcr.ObjectMeta.Name,
			refreshPCRAbandonAt: pcr.ObjectMeta.CreationTimestamp.Time.Add(assumeDeletedThreshold + jitterDuration()),
		}

		// 返回 nil 将投影从工作队列移除；待 PodCertificateRequest 出现在 informer
		// 缓存并经过状态更新后，它会再次被加入队列。
		logger.V(4).Info("PodCertificateRequest created, moving to credStateWaitRefresh", "key", key, "pcr", pcr.ObjectMeta.Namespace+"/"+pcr.ObjectMeta.Name)
		return nil

	case *credStateWaitRefresh:
		// 检查刷新用的 PodCertificateRequest。
		pcr, err := m.pcrLister.PodCertificateRequests(key.Namespace).Get(state.refreshPCRName)
		if k8serrors.IsNotFound(err) && m.clock.Now().After(state.refreshPCRAbandonAt) {
			// "Not Found" 可能是 informer 延迟，也可能是 PCR 被删除。
			// 前者应继续等待，后者应重建 PCR。准确区分需要 resourceVersion 有序，
			// 且 lister 能报告其见过的最高版本。在此之前，若距记录创建 PCR 已过 10 分钟，
			// 则假定属于后者，保持 credStateFresh 以便再次创建 PCR。
			rec.curState = &credStateFresh{
				privateKey:                       state.privateKey,
				certChain:                        state.certChain,
				beginRefreshAt:                   state.beginRefreshAt,
				notAfter:                         state.notAfter,
				eventEmittedForOverdueForRefresh: state.eventEmittedForOverdueForRefresh,
				eventEmittedForExpiration:        state.eventEmittedForExpiration,
			}
			return fmt.Errorf("PodCertificateRequest appears to have been deleted")
		} else if err != nil {
			return fmt.Errorf("while getting PodCertificateRequest %q: %w", key.Namespace+"/"+state.refreshPCRName, err)
		}

		// 若 PodCertificateRequest 已进入终态，则相应更新状态机。
		for _, cond := range pcr.Status.Conditions {
			switch cond.Type {
			case certificatesv1.PodCertificateRequestConditionTypeDenied:
				rec.curState = &credStateDenied{
					Reason:  cond.Reason,
					Message: cond.Message,
				}
				logger.V(4).Info("PodCertificateRequest denied, moving to credStateDenied", "key", key, "pcr", pcr.ObjectMeta.Namespace+"/"+pcr.ObjectMeta.Name)
				eventMessage := fmt.Sprintf("PodCertificateRequest %s was denied, reason=%q, message=%q", pcr.ObjectMeta.Namespace+"/"+pcr.ObjectMeta.Name, cond.Reason, cond.Message)
				m.recorder.Eventf(pod, corev1.EventTypeWarning, certificatesv1.PodCertificateRequestConditionTypeDenied, cond.Reason, eventMessage)
				return nil
			case certificatesv1.PodCertificateRequestConditionTypeFailed:
				rec.curState = &credStateFailed{
					Reason:  cond.Reason,
					Message: cond.Message,
				}
				logger.V(4).Info("PodCertificateRequest failed, moving to credStateFailed", "key", key, "pcr", pcr.ObjectMeta.Namespace+"/"+pcr.ObjectMeta.Name)
				eventMessage := fmt.Sprintf("PodCertificateRequest %s failed, reason=%q, message=%q", pcr.ObjectMeta.Namespace+"/"+pcr.ObjectMeta.Name, cond.Reason, cond.Message)
				m.recorder.Eventf(pod, corev1.EventTypeWarning, certificatesv1.PodCertificateRequestConditionTypeFailed, cond.Reason, eventMessage)
				return nil
			case certificatesv1.PodCertificateRequestConditionTypeIssued:
				rec.curState = &credStateFresh{
					privateKey:     state.refreshPrivateKey,
					certChain:      cleanCertificateChain([]byte(pcr.Status.CertificateChain)),
					beginRefreshAt: pcr.Status.BeginRefreshAt.Time.Add(jitterDuration()),
					notAfter:       pcr.Status.NotAfter.Time,
				}
				logger.V(4).Info("PodCertificateRequest issued, moving to credStateFresh", "key", key, "pcr", pcr.ObjectMeta.Namespace+"/"+pcr.ObjectMeta.Name)
				return nil
			}
		}

		// 没有变化：请求仍处于 pending。返回 nil 将投影从队列移除，
		// 待 PodCertificateRequest 更新时会再次被驱动。
		logger.V(4).Info("PodCertificateRequest not in terminal state, remaining in credStateWaitRefresh", "key", key, "pcr", pcr.ObjectMeta.Namespace+"/"+pcr.ObjectMeta.Name)

		// 当前时间已超过最近签发证书 `beginRefreshAt` 10 分钟以上，但状态尚未标记为刷新逾期。
		if m.clock.Now().After(state.beginRefreshAt.Add(refreshOverdueDuration)) && !state.eventEmittedForOverdueForRefresh {
			logger.V(4).Info("Refresh overdue", "key", key)
			m.recorder.Eventf(pod, corev1.EventTypeWarning, "CertificateOverdueForRefresh", "PodCertificate refresh overdue")
			state.eventEmittedForOverdueForRefresh = true
		}

		// 当前时间已超过最近签发证书的 `notAfter`，但状态尚未标记为已过期。
		if m.clock.Now().After(state.notAfter) && !state.eventEmittedForExpiration {
			logger.V(4).Info("Certificates expired", "key", key)
			m.recorder.Eventf(pod, corev1.EventTypeWarning, "CertificateExpired", "PodCertificate expired")
			state.eventEmittedForExpiration = true
		}
		return nil
	}

	return nil
}

// jitterDuration 返回 0 到 5 分钟之间的随机抖动，避免多个
// PodCertificateProjection 同时创建 PCR。
func jitterDuration() time.Duration {
	return time.Duration(mathrand.Int64N(5 * 60 * 1_000_000_000))
}

// runRefreshPass 把节点上每个非镜像 Pod 重新加入卷来源处理队列。
func (m *IssuingManager) runRefreshPass(ctx context.Context) {
	allPods := m.podManager.GetPods()
	for _, pod := range allPods {
		m.queueAllProjectionsForPod(pod.ObjectMeta.UID)
	}
}

// TrackPod 将 Pod 的 podCertificate 投影卷来源加入处理队列。
// TrackPod 将 Pod 的 PodCertificate 投影卷来源加入处理队列。
func (m *IssuingManager) TrackPod(ctx context.Context, pod *corev1.Pod) {
	m.queueAllProjectionsForPod(pod.ObjectMeta.UID)
}

// cleanupCredStoreForPod 删除指定 Pod 的所有 credStore 条目。
// cleanupCredStoreForPod 删除指定 Pod 的所有凭据记录。
func (m *IssuingManager) cleanupCredStoreForPod(namespace, podName, podUID string) {
	m.lock.Lock()
	defer m.lock.Unlock()

	for k := range m.credStore {
		if k.Namespace == namespace && k.PodName == podName && k.PodUID == podUID {
			delete(m.credStore, k)
		}
	}
}

// ForgetPod 清理指定 Pod 的所有证书凭据。
//
// pod worker 会发现 Pod 已不存在，并清除其待处理与现存的凭据。
// ForgetPod 立即清理该 Pod 的凭据缓存，避免已删除 Pod 的状态残留。
func (m *IssuingManager) ForgetPod(ctx context.Context, pod *corev1.Pod) {
	// 立即清理该 Pod 的 credStore 条目，避免竞态。
	m.cleanupCredStoreForPod(pod.Namespace, pod.Name, string(pod.UID))
}

// createPodCertificateRequest 创建一个 PodCertificateRequest。
// createPodCertificateRequest 生成密钥与 PKCS#10 请求，并向 API 创建 PodCertificateRequest。
func (m *IssuingManager) createPodCertificateRequest(
	ctx context.Context,
	namespace string,
	podName string, podUID types.UID,
	serviceAccountName string, serviceAccountUID types.UID,
	nodeName types.NodeName, nodeUID types.UID,
	signerName, keyType string, maxExpirationSeconds *int32,
	userAnnotations map[string]string,
) ([]byte, *certificatesv1.PodCertificateRequest, error) {
	privateKey, pkcs10Req, err := generateKeyAndProof(keyType)
	if err != nil {
		return nil, nil, fmt.Errorf("while generating keypair: %w", err)
	}

	keyPEM, err := pemEncodeKey(privateKey)
	if err != nil {
		return nil, nil, fmt.Errorf("while PEM-encoding private key: %w", err)
	}

	req := &certificatesv1.PodCertificateRequest{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:    namespace,
			GenerateName: "req-",
			OwnerReferences: []metav1.OwnerReference{
				{
					APIVersion: "v1",
					Kind:       "Pod",
					Name:       podName,
					UID:        podUID,
				},
			},
		},
		Spec: certificatesv1.PodCertificateRequestSpec{
			SignerName:                signerName,
			PodName:                   podName,
			PodUID:                    podUID,
			ServiceAccountName:        serviceAccountName,
			ServiceAccountUID:         serviceAccountUID,
			NodeName:                  nodeName,
			NodeUID:                   nodeUID,
			MaxExpirationSeconds:      maxExpirationSeconds,
			StubPKCS10Request:         pkcs10Req,
			UnverifiedUserAnnotations: userAnnotations,
		},
	}

	req, err = m.kc.CertificatesV1().PodCertificateRequests(namespace).Create(ctx, req, metav1.CreateOptions{})
	if err != nil {
		return nil, nil, fmt.Errorf("while creating on API: %w", err)
	}

	return keyPEM, req, nil
}

// GetPodCertificateCredentialBundle 返回指定投影来源的私钥与证书链。
func (m *IssuingManager) GetPodCertificateCredentialBundle(ctx context.Context, namespace, podName, podUID, volumeName string, sourceIndex int) ([]byte, []byte, error) {
	credKey := projectionKey{
		Namespace:   namespace,
		PodName:     podName,
		PodUID:      podUID,
		VolumeName:  volumeName,
		SourceIndex: sourceIndex,
	}

	var rec *projectionRecord
	func() {
		m.lock.Lock()
		defer m.lock.Unlock()
		rec = m.credStore[credKey]
	}()

	if rec == nil {
		return nil, nil, fmt.Errorf("no credentials yet for key=%v", credKey)
	}

	rec.lock.Lock()
	defer rec.lock.Unlock()

	return rec.curState.getCredBundle()
}

// MetricReport 汇总所有 Pod 的 PodCertificate 投影状态计数。
func (m *IssuingManager) MetricReport() *MetricReport {
	report := &MetricReport{
		PodCertificateStates: map[SignerAndState]int{},
	}

	// 遍历所有 Pod 及其 podCertificate 投影卷来源，而不是遍历 credStore，
	// 以便使用投影来源中的 SignerName。
	allPods := m.podManager.GetPods()
	for _, pod := range allPods {
		for _, v := range pod.Spec.Volumes {
			if v.Projected == nil {
				continue
			}

			for sourceIndex, source := range v.Projected.Sources {
				if source.PodCertificate == nil {
					continue
				}

				key := projectionKey{
					Namespace:   pod.ObjectMeta.Namespace,
					PodName:     pod.ObjectMeta.Name,
					PodUID:      string(pod.ObjectMeta.UID),
					VolumeName:  v.Name,
					SourceIndex: sourceIndex,
				}

				var rec *projectionRecord
				func() {
					m.lock.Lock()
					defer m.lock.Unlock()
					rec = m.credStore[key]
				}()
				if rec == nil {
					continue
				}

				func() {
					rec.lock.Lock()
					defer rec.lock.Unlock()

					metricsKey := SignerAndState{
						SignerName: source.PodCertificate.SignerName,
						State:      rec.curState.metricsState(m.clock.Now()),
					}
					report.PodCertificateStates[metricsKey]++
				}()

			}
		}
	}

	return report
}

// generateKeyAndProof 按 keyType 生成私钥并构造 stub PKCS#10 请求。
func generateKeyAndProof(keyType string) (crypto.PrivateKey, []byte, error) {
	var privKey crypto.PrivateKey

	switch keyType {
	case "RSA3072":
		priv, err := rsa.GenerateKey(rand.Reader, 3072)
		if err != nil {
			return nil, nil, fmt.Errorf("while generating RSA 3072 key: %w", err)
		}
		privKey = priv
	case "RSA4096":
		priv, err := rsa.GenerateKey(rand.Reader, 4096)
		if err != nil {
			return nil, nil, fmt.Errorf("while generating RSA 4096 key: %w", err)
		}
		privKey = priv
	case "ECDSAP256":
		priv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
		if err != nil {
			return nil, nil, fmt.Errorf("while generating ECDSA P256 key: %w", err)
		}
		privKey = priv
	case "ECDSAP384":
		priv, err := ecdsa.GenerateKey(elliptic.P384(), rand.Reader)
		if err != nil {
			return nil, nil, fmt.Errorf("while generating ECDSA P384 key: %w", err)
		}
		privKey = priv
	case "ECDSAP521":
		priv, err := ecdsa.GenerateKey(elliptic.P521(), rand.Reader)
		if err != nil {
			return nil, nil, fmt.Errorf("while generating ECDSA P521 key: %w", err)
		}
		privKey = priv
	case "ED25519":
		_, priv, err := ed25519.GenerateKey(rand.Reader)
		if err != nil {
			return nil, nil, fmt.Errorf("while generating Ed25519 key: %w", err)
		}
		privKey = priv
	default:
		return nil, nil, fmt.Errorf("unknown key type %q", keyType)
	}

	tmpl := &x509.CertificateRequest{}
	pkcs10Req, err := x509.CreateCertificateRequest(rand.Reader, tmpl, privKey)
	if err != nil {
		return nil, nil, fmt.Errorf("while generating stub PKCS#10 request: %w", err)
	}

	return privKey, pkcs10Req, nil
}

// pemEncodeKey 将私钥编码为 PKCS#8 PEM。
func pemEncodeKey(key crypto.PrivateKey) ([]byte, error) {
	keyDER, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		return nil, fmt.Errorf("while marshaling key to PKCS#8: %w", err)
	}

	return pem.EncodeToMemory(&pem.Block{
		Type:  "PRIVATE KEY",
		Bytes: keyDER,
	}), nil
}

// 确保从证书链中丢弃所有块间数据与块头，只保留证书本身。
func cleanCertificateChain(in []byte) []byte {
	outChain := &bytes.Buffer{}

	rest := in
	var b *pem.Block
	for {
		b, rest = pem.Decode(rest)
		if b == nil {
			break
		}

		cleanedBlock := &pem.Block{
			Type:  "CERTIFICATE",
			Bytes: b.Bytes,
		}
		outChain.Write(pem.EncodeToMemory(cleanedBlock))
	}

	return outChain.Bytes()
}

// NoOpManager 是 Manager 的空实现，只返回错误，
// 用于静态/分离（static/detached）模式的 kubelet。
type NoOpManager struct{}

var _ Manager = (*NoOpManager)(nil)

// TrackPod 为空实现。
func (m *NoOpManager) TrackPod(ctx context.Context, pod *corev1.Pod) {
}

// ForgetPod 为空实现。
func (m *NoOpManager) ForgetPod(ctx context.Context, pod *corev1.Pod) {
}

// GetPodCertificateCredentialBundle 始终返回未实现错误。
func (m *NoOpManager) GetPodCertificateCredentialBundle(ctx context.Context, namespace, podName, podUID, volumeName string, sourceIndex int) ([]byte, []byte, error) {
	return nil, nil, fmt.Errorf("unimplemented")
}

// MetricReport 返回空报告。
func (m *NoOpManager) MetricReport() *MetricReport {
	return &MetricReport{}
}
