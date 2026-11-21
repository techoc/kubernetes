/*
Copyright The Kubernetes Authors.

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

package pods

import (
	"context"
	"errors"
	"sort"
	"strings"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/klog/v2"
	podsv1alpha1 "k8s.io/kubelet/pkg/apis/pods/v1alpha1"
	"k8s.io/kubernetes/pkg/kubelet/config"
	"k8s.io/kubernetes/pkg/kubelet/metrics"
	"k8s.io/kubernetes/pkg/kubelet/pod"
	podstatus "k8s.io/kubernetes/pkg/kubelet/status"
)

// PodWatchEvent 描述一次供 watch 客户端消费的 Pod 事件。
type PodWatchEvent struct {
	Type watch.EventType
	UID  types.UID
	Pod  *v1.Pod
}

// broadcaster 将 Pod 事件扇出给所有已注册的 watch 客户端。
type broadcaster struct {
	lock     sync.RWMutex
	clients  map[chan PodWatchEvent]struct{}
	incoming chan PodWatchEvent
}

// NewBroadcaster 创建并启动事件广播器；ctx 取消时它会关闭所有客户端通道。
func NewBroadcaster(ctx context.Context) *broadcaster {
	// 内部通道使用带缓冲设计，吸收短时的事件突发，避免广播调用方被阻塞。
	b := &broadcaster{
		clients:  make(map[chan PodWatchEvent]struct{}),
		incoming: make(chan PodWatchEvent, 1000),
	}
	go b.run(ctx)
	return b
}

// run 是广播器的事件循环：串行地从 incoming 读取事件并分发给各客户端。
func (b *broadcaster) run(ctx context.Context) {
	logger := klog.FromContext(ctx)
	for {
		select {
		case <-ctx.Done():
			// 服务关闭时主动关闭所有客户端通道，促使正在 watch 的调用方退出。
			b.lock.Lock()
			for client := range b.clients {
				delete(b.clients, client)
				close(client)
			}
			b.lock.Unlock()
			return
		case event, ok := <-b.incoming:
			if !ok {
				return
			}
			b.lock.RLock()
			// 先收集需要剔除的客户端，避免在遍历 map 时修改它——那将需要写锁，
			// 并阻塞其他读锁持有者。
			var clientsToDrop []chan PodWatchEvent

			for client := range b.clients {
				select {
				case client <- event:
				case <-ctx.Done():
					b.lock.RUnlock()
					return
				default:
					// 客户端消费过慢导致通道满：丢弃该客户端，防止慢客户端拖垮广播器。
					logger.Info("Watch client channel is full, dropping client.")
					metrics.PodWatchEventsDroppedTotal.Inc()
					clientsToDrop = append(clientsToDrop, client)
				}
			}
			b.lock.RUnlock()

			if len(clientsToDrop) > 0 {
				b.lock.Lock()
				for _, client := range clientsToDrop {
					if _, ok := b.clients[client]; ok {
						delete(b.clients, client)
						close(client)
					}
				}
				b.lock.Unlock()
			}
		}
	}
}

// Register 将一个新的客户端通道注册到广播器。
func (b *broadcaster) Register(logger klog.Logger, client chan PodWatchEvent) {
	b.lock.Lock()
	defer b.lock.Unlock()
	b.clients[client] = struct{}{}
	logger.Info("Registered new watch client", "totalClients", len(b.clients))
}

// Unregister 从广播器移除客户端并关闭其通道；重复注销是安全的。
func (b *broadcaster) Unregister(logger klog.Logger, client chan PodWatchEvent) {
	b.lock.Lock()
	defer b.lock.Unlock()
	if _, ok := b.clients[client]; ok {
		delete(b.clients, client)
		close(client)
	}
	logger.Info("Unregistered watch client", "totalClients", len(b.clients))
}

// Broadcast 将事件投递到内部通道，由 run 异步分发；内部缓冲满时丢弃事件。
func (b *broadcaster) Broadcast(logger klog.Logger, event PodWatchEvent) {
	select {
	case b.incoming <- event:
	default:
		// 正常情况下不会发生，因为 run() 会剔除消费过慢的客户端。
		logger.Info("Broadcaster internal buffer full, dropping event.")
		metrics.PodWatchEventsDroppedTotal.Inc()
	}
}

// PodsServer 是提供 Pod 信息的 gRPC 服务端。
type PodsServer struct {
	podsv1alpha1.UnimplementedPodsServer
	podManager     pod.Manager
	statusProvider podstatus.PodStatusProvider
	broadcaster    *broadcaster
	sourcesReady   config.SourcesReady
}

// NewPodsServer 创建一个供生产环境使用的 PodsServer。
func NewPodsServer(broadcaster *broadcaster, podManager pod.Manager, statusProvider podstatus.PodStatusProvider, sourcesReady config.SourcesReady) *PodsServer {
	return &PodsServer{
		podManager:     podManager,
		statusProvider: statusProvider,
		broadcaster:    broadcaster,
		sourcesReady:   sourcesReady,
	}
}

// NewPodsServerForTest 创建一个供测试使用的 PodsServer，不设置 sourcesReady。
func NewPodsServerForTest(broadcaster *broadcaster, podManager pod.Manager, statusProvider podstatus.PodStatusProvider) *PodsServer {
	return &PodsServer{
		podManager:     podManager,
		statusProvider: statusProvider,
		broadcaster:    broadcaster,
	}
}

// OnPodUpdated 在 Pod 的 spec 与 status 已一致时被调用，向 watcher 广播更新事件。
func (s *PodsServer) OnPodUpdated(logger klog.Logger, pod *v1.Pod, status v1.PodStatus, isAdded bool) {
	eventType := watch.Modified
	if isAdded {
		eventType = watch.Added
	}
	// 复制一份 Pod 并附带最新状态，避免广播出去的对象被其他 goroutine 修改。
	podCopy := *pod
	podCopy.Status = status
	s.broadcaster.Broadcast(logger, PodWatchEvent{Type: eventType, UID: pod.UID, Pod: &podCopy})
	logger.Info("Pod update broadcasted", "podUID", pod.UID, "type", eventType)
}

// OnPodRemoved 在 Pod 被移除时被调用，向 watcher 广播删除事件。
func (s *PodsServer) OnPodRemoved(logger klog.Logger, pod *v1.Pod) {
	// 删除事件只需携带身份信息，构造最小 Pod 以减少传输与序列化开销。
	minimalPod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      pod.Name,
			Namespace: pod.Namespace,
			UID:       pod.UID,
		},
	}
	s.broadcaster.Broadcast(logger, PodWatchEvent{Type: watch.Deleted, UID: pod.UID, Pod: minimalPod})
	logger.Info("Pod removed broadcasted", "podUID", pod.UID)
}

// validateReady 校验 kubelet 的所有配置源是否已就绪；未就绪时返回 FailedPrecondition。
func (s *PodsServer) validateReady() error {
	if s.sourcesReady != nil && !s.sourcesReady.AllReady() {
		return status.Error(codes.FailedPrecondition, "Kubelet is initializing")
	}
	return nil
}

// getEffectivePodStatus 返回 Pod 的有效状态：优先使用 statusManager 的本地计算结果，
// 缺失时回退到 podManager 中的状态，并防止向客户端暴露未经本地验证的 Ready 状态。
func (s *PodsServer) getEffectivePodStatus(p *v1.Pod) v1.PodStatus {
	if s.statusProvider != nil {
		if podStatus, ok := s.statusProvider.GetPodStatus(p.UID); ok {
			return podStatus
		}
	}

	// 回退逻辑：若 statusManager 尚未计算出本地状态（如刚启动或重启的空窗期），
	// 则使用 podManager 中的状态；Phase 为空时默认视为 Pending。
	status := *p.Status.DeepCopy()
	if status.Phase == "" {
		status.Phase = v1.PodPending
	}

	// 若 APIServer 中的对象声称 PodReady=True，但 statusManager 尚无本地缓存状态，
	// 则将 PodReady 覆写为 False（ContainersNotReady），避免向客户端提供未经本地验证的 Ready 状态。
	for i, c := range status.Conditions {
		if c.Type == v1.PodReady && c.Status == v1.ConditionTrue {
			status.Conditions[i].Status = v1.ConditionFalse
			status.Conditions[i].Reason = "ContainersNotReady"
			status.Conditions[i].Message = "Kubelet is initializing pod containers"
			break
		}
	}

	return status
}

// ListPods 返回当前 kubelet 管理的全部 Pod。
func (s *PodsServer) ListPods(ctx context.Context, req *podsv1alpha1.ListPodsRequest) (*podsv1alpha1.ListPodsResponse, error) {
	logger := klog.FromContext(ctx)
	logger.Info("ListPods called")

	if err := s.validateReady(); err != nil {
		return nil, err
	}

	// TODO: Implement filtering based on req.Filter, pagination with req.PageToken and req.PageSize
	podsToReturn := s.podManager.GetPods()
	// 按 UID 排序，保证返回顺序稳定，便于客户端比对。
	sort.Slice(podsToReturn, func(i, j int) bool {
		return podsToReturn[i].UID < podsToReturn[j].UID
	})

	protoPods := make([][]byte, len(podsToReturn))
	for i, p := range podsToReturn {
		// 复制后填充有效状态再序列化，避免修改 podManager 中的原始对象。
		podCopy := *p
		podCopy.Status = s.getEffectivePodStatus(p)
		podBytes, err := podCopy.Marshal()
		if err != nil {
			return nil, status.Errorf(codes.Internal, "failed to marshal pod: %v", err)
		}
		protoPods[i] = podBytes
	}

	return &podsv1alpha1.ListPodsResponse{Pods: protoPods}, nil
}

// GetPod 按 UID 返回单个 Pod。
func (s *PodsServer) GetPod(ctx context.Context, req *podsv1alpha1.GetPodRequest) (*podsv1alpha1.GetPodResponse, error) {
	logger := klog.FromContext(ctx)
	logger.Info("GetPod called", "podUID", req.PodUID)

	if err := s.validateReady(); err != nil {
		return nil, err
	}

	// 转换为内部 UID 类型后再查找，保证与 podManager 的索引键一致。
	podUID := types.UID(req.PodUID)
	pod, ok := s.podManager.GetPodByUID(podUID)
	if !ok {
		return nil, status.Errorf(codes.NotFound, "pod with UID %s not found", req.PodUID)
	}

	podCopy := *pod
	podCopy.Status = s.getEffectivePodStatus(pod)
	podBytes, err := podCopy.Marshal()
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to marshal pod: %v", err)
	}

	return &podsv1alpha1.GetPodResponse{Pod: podBytes}, nil
}

// WatchPods 以流式 RPC 的方式向客户端推送 Pod 事件。
func (s *PodsServer) WatchPods(req *podsv1alpha1.WatchPodsRequest, stream podsv1alpha1.Pods_WatchPodsServer) error {
	// 记录客户端地址，仅用于日志排查。
	clientAddr := "unknown"
	if p, ok := peer.FromContext(stream.Context()); ok {
		clientAddr = p.Addr.String()
	}
	logger := klog.FromContext(stream.Context())
	logger.Info("WatchPods called", "client", clientAddr)

	if err := s.validateReady(); err != nil {
		return err
	}

	// 为每个 watch 客户端分配带缓冲的通道，缓冲量用于吸收客户端发送慢时的积压。
	clientChannel := make(chan PodWatchEvent, 100)
	s.broadcaster.Register(logger, clientChannel)
	// 连接结束时务必注销并关闭通道，防止广播器向已失效客户端发送事件。
	defer func() {
		s.broadcaster.Unregister(logger, clientChannel)
		logger.Info("Watch client disconnected", "client", clientAddr)
	}()

	// 先发送初始的 ADDED 事件，让客户端获得全量快照。
	initialPods := s.podManager.GetPods()
	sort.Slice(initialPods, func(i, j int) bool {
		return initialPods[i].UID < initialPods[j].UID
	})

	for _, p := range initialPods {
		podCopy := *p
		podCopy.Status = s.getEffectivePodStatus(p)
		podBytes, err := podCopy.Marshal()
		if err != nil {
			logger.Error(err, "Error marshalling initial watch event pod")
			metrics.PodWatchEventsDroppedTotal.Inc()
			return status.Errorf(codes.Internal, "error marshalling initial watch event pod: %v", err)
		}
		if err := stream.Send(&podsv1alpha1.WatchPodsEvent{
			Type: podsv1alpha1.EventType_ADDED,
			Pod:  podBytes,
		}); err != nil {
			logger.Error(err, "Error sending initial watch event")
			return err
		}
	}

	// 发送 INITIAL_SYNC_COMPLETE 事件，告知客户端初始全量列表已发送完毕。
	if err := stream.Send(&podsv1alpha1.WatchPodsEvent{
		Type: podsv1alpha1.EventType_INITIAL_SYNC_COMPLETE,
	}); err != nil {
		logger.Error(err, "Error sending initial sync watch event")
		return err
	}

	for {
		select {
		case <-stream.Context().Done():
			logger.Info("Watch context cancelled", "client", clientAddr)
			return stream.Context().Err()
		case event, ok := <-clientChannel:
			if !ok {
				// 通道被广播器关闭通常意味着该客户端消费过慢被剔除。
				logger.Info("Watch client channel closed", "client", clientAddr)
				return status.Errorf(codes.ResourceExhausted, "watch client channel closed")
			}
			var podToMarshal *v1.Pod
			if event.Pod != nil {
				podToMarshal = event.Pod
			} else if event.Type != watch.Bookmark {
				// 事件未携带 Pod 时回退到 podManager 中查找。
				// 当前实现下不应发生，保留该逻辑以增强健壮性。
				p, ok := s.podManager.GetPodByUID(event.UID)
				if ok {
					podToMarshal = p
				} else {
					logger.Info("Pod not found in manager during watch event processing", "uid", event.UID, "type", event.Type)
					continue
				}
			}

			var podBytes []byte
			if podToMarshal != nil {
				var err error
				podBytes, err = podToMarshal.Marshal()
				if err != nil {
					logger.Error(err, "Error marshalling watch event pod")
					metrics.PodWatchEventsDroppedTotal.Inc()
					return status.Errorf(codes.Internal, "error marshalling watch event pod: %v", err)
				}
			}

			// 未知事件类型只记录日志并跳过，避免单个坏事件中断整个 watch 流。
			eventType, err := convertWatchEventType(event.Type)
			if err != nil {
				logger.Error(err, "Unknown watch event type")
				continue
			}

			if err := stream.Send(&podsv1alpha1.WatchPodsEvent{
				Type: eventType,
				Pod:  podBytes,
			}); err != nil {
				logger.Error(err, "Error sending watch event to client", "client", clientAddr)
				return err
			}
		}
	}
}

// convertWatchEventType 将内部 watch 事件类型转换为 gRPC API 的事件类型。
func convertWatchEventType(watchType watch.EventType) (podsv1alpha1.EventType, error) {
	switch watchType {
	case watch.Added:
		return podsv1alpha1.EventType_ADDED, nil
	case watch.Modified:
		return podsv1alpha1.EventType_MODIFIED, nil
	case watch.Deleted:
		return podsv1alpha1.EventType_DELETED, nil
	case watch.Bookmark:
		return podsv1alpha1.EventType_INITIAL_SYNC_COMPLETE, nil
	default:
		return podsv1alpha1.EventType_UNSPECIFIED, status.Errorf(codes.Internal, "unknown watch event type: %v", watchType)
	}
}

// statusCodeString 将错误映射为 gRPC 状态码字符串，供指标标签使用。
func statusCodeString(err error) string {
	if err == nil {
		return codes.OK.String()
	}
	if errors.Is(err, context.Canceled) {
		return codes.Canceled.String()
	}
	return status.Code(err).String()
}

// MetricsUnaryServerInterceptor 是一个 gRPC 拦截器，为一元 RPC 记录请求指标。
func MetricsUnaryServerInterceptor(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
	// 从完整方法名解析出版本与方法名，作为指标标签的维度。
	version, method := parseFullMethod(info.FullMethod)

	resp, err := handler(ctx, req)
	statusCode := statusCodeString(err)

	metrics.PodRequestsTotal.WithLabelValues(version, statusCode).Inc()

	switch method {
	case "ListPods":
		metrics.PodRequestsList.WithLabelValues(version, statusCode).Inc()
	case "GetPod":
		metrics.PodRequestsGet.WithLabelValues(version, statusCode).Inc()
	}

	return resp, err
}

// MetricsStreamServerInterceptor 是一个 gRPC 拦截器，为流式 RPC 记录请求指标。
func MetricsStreamServerInterceptor(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	version, method := parseFullMethod(info.FullMethod)

	err := handler(srv, ss)
	statusCode := statusCodeString(err)

	metrics.PodRequestsTotal.WithLabelValues(version, statusCode).Inc()

	if method == "WatchPods" {
		metrics.PodRequestsWatch.WithLabelValues(version, statusCode).Inc()
	}

	return err
}

// parseFullMethod 从 gRPC 完整方法名中解析出版本与方法名，例如
// "/v1alpha1.Pods/ListPods" 解析为 "v1alpha1" 与 "ListPods"。
func parseFullMethod(fullMethod string) (string, string) {
	parts := strings.Split(strings.TrimPrefix(fullMethod, "/"), "/")
	if len(parts) != 2 {
		return "unknown", "unknown"
	}
	service := parts[0] // 例如 "v1alpha1.Pods"
	method := parts[1]  // 例如 "ListPods"

	serviceParts := strings.Split(service, ".")
	version := serviceParts[0] // 例如 "v1alpha1"
	return version, method
}
