/*
Copyright 2018 The Kubernetes Authors.

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

package logs

import (
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"

	"k8s.io/client-go/util/workqueue"
	"k8s.io/klog/v2"

	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	internalapi "k8s.io/cri-api/pkg/apis"
	runtimeapi "k8s.io/cri-api/pkg/apis/runtime/v1"
	kubecontainer "k8s.io/kubernetes/pkg/kubelet/container"
	"k8s.io/utils/clock"
)

const (
	// timestampFormat 是轮转日志时间戳后缀的格式。
	// 参考 https://golang.org/pkg/time/#Time.Format。
	timestampFormat = "20060102-150405"
	// compressSuffix 是压缩日志文件的后缀。
	compressSuffix = ".gz"
	// tmpSuffix 是临时文件的后缀。
	tmpSuffix = ".tmp"
)

// ContainerLogManager 管理所有容器日志的生命周期。
//
// 实现是线程安全的。
type ContainerLogManager interface {
	// Start 启动容器日志管理器。
	Start(ctx context.Context)
	// Clean 删除指定容器的所有日志。
	Clean(ctx context.Context, containerID string) error
}

// LogRotatePolicy 是容器日志轮转策略，适用于 kubelet 管理的所有容器。
type LogRotatePolicy struct {
	// MaxSize 是容器日志文件触发轮转的大小上限（字节）。
	// 取负值表示禁用容器日志轮转。
	MaxSize int64
	// MaxFiles 是允许保留的日志文件数量上限。
	// 若轮转产生超额文件，则删除最旧的文件。
	MaxFiles int
}

// GetAllLogs 获取指定容器日志对应的全部在用（已轮转/已压缩）日志文件。
// 返回结果按从旧到新的顺序排列。
func GetAllLogs(log string) ([]string, error) {
	// 用通配模式匹配所有轮转文件。
	pattern := fmt.Sprintf("%s.*", log)
	logs, err := filepath.Glob(pattern)
	if err != nil {
		return nil, fmt.Errorf("failed to list all log files with pattern %q: %w", pattern, err)
	}
	// 过滤掉不再使用的文件，并排序以保证返回顺序稳定。
	inuse, _ := filterUnusedLogs(logs)
	sort.Strings(inuse)
	return append(inuse, log), nil
}

// parseMaxSize 将带单位的数量字符串解析为以字节为单位的 int64 上限值。
func parseMaxSize(size string) (int64, error) {
	quantity, err := resource.ParseQuantity(size)
	if err != nil {
		return 0, err
	}
	maxSize, ok := quantity.AsInt64()
	if !ok {
		return 0, fmt.Errorf("invalid max log size")
	}
	return maxSize, nil
}

// containerLogManager 是 ContainerLogManager 的默认实现。
type containerLogManager struct {
	runtimeService   internalapi.RuntimeService
	osInterface      kubecontainer.OSInterface
	policy           LogRotatePolicy
	clock            clock.Clock
	mutex            sync.Mutex
	queue            workqueue.TypedRateLimitingInterface[string]
	maxWorkers       int
	monitoringPeriod metav1.Duration
}

// NewContainerLogManager 创建一个新的容器日志管理器。
func NewContainerLogManager(runtimeService internalapi.RuntimeService, osInterface kubecontainer.OSInterface, maxSize string, maxFiles int, maxWorkers int, monitorInterval metav1.Duration) (ContainerLogManager, error) {
	if maxFiles <= 1 {
		return nil, fmt.Errorf("invalid MaxFiles %d, must be > 1", maxFiles)
	}
	parsedMaxSize, err := parseMaxSize(maxSize)
	if err != nil {
		return nil, fmt.Errorf("failed to parse container log max size %q: %w", maxSize, err)
	}
	// 负数表示禁用容器日志轮转，直接返回空实现的 stub。
	if parsedMaxSize < 0 {
		return NewStubContainerLogManager(), nil
	}
	// 使用配置的轮转策略构造真正的日志管理器。
	return &containerLogManager{
		osInterface:    osInterface,
		runtimeService: runtimeService,
		policy: LogRotatePolicy{
			MaxSize:  parsedMaxSize,
			MaxFiles: maxFiles,
		},
		clock:      clock.RealClock{},
		mutex:      sync.Mutex{},
		maxWorkers: maxWorkers,
		queue: workqueue.NewTypedRateLimitingQueueWithConfig(
			workqueue.DefaultTypedControllerRateLimiter[string](),
			workqueue.TypedRateLimitingQueueConfig[string]{Name: "kubelet_log_rotate_manager"},
		),
		monitoringPeriod: monitorInterval,
	}, nil
}

// Start 启动容器日志管理器：拉起多个处理 worker 并周期性触发日志轮转。
func (c *containerLogManager) Start(ctx context.Context) {
	logger := klog.FromContext(ctx)
	logger.Info("Initializing container log rotate workers", "workers", c.maxWorkers, "monitorPeriod", c.monitoringPeriod)
	// 启动固定数量的 worker 并发消费轮转队列，提高多容器场景下的吞吐。
	for i := 0; i < c.maxWorkers; i++ {
		worker := i + 1
		go c.processQueueItems(ctx, worker)
	}
	// 启动一个 goroutine，周期性扫描并轮转容器日志。
	go wait.Forever(func() {
		if err := c.rotateLogs(ctx); err != nil {
			logger.Error(err, "Failed to rotate container logs")
		}
	}, c.monitoringPeriod.Duration)
}

// Clean 删除指定容器的所有日志（包括已轮转的日志）。
func (c *containerLogManager) Clean(ctx context.Context, containerID string) error {
	// 与轮转流程共用互斥锁，避免清理与轮转同时操作同一批文件。
	c.mutex.Lock()
	defer c.mutex.Unlock()
	resp, err := c.runtimeService.ContainerStatus(ctx, containerID, false)
	if err != nil {
		return fmt.Errorf("failed to get container status %q: %w", containerID, err)
	}
	if resp.GetStatus() == nil {
		return fmt.Errorf("container status is nil for %q", containerID)
	}
	// 以日志路径为前缀匹配主日志及所有轮转文件。
	pattern := fmt.Sprintf("%s*", resp.GetStatus().GetLogPath())
	logs, err := c.osInterface.Glob(pattern)
	if err != nil {
		return fmt.Errorf("failed to list all log files with pattern %q: %w", pattern, err)
	}

	for _, l := range logs {
		if err := c.osInterface.Remove(l); err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("failed to remove container %q log %q: %w", containerID, l, err)
		}
	}

	return nil
}

// processQueueItems 循环消费轮转队列，直到队列关闭或上下文结束。
func (c *containerLogManager) processQueueItems(ctx context.Context, worker int) {
	logger := klog.FromContext(ctx)
	logger.V(4).Info("Starting container log rotation worker", "workerID", worker)
	for c.processContainer(ctx, worker) {
	}
	logger.V(4).Info("Terminating container log rotation worker", "workerID", worker)
}

// rotateLogs 列出所有运行中的容器，并将其加入轮转队列。
func (c *containerLogManager) rotateLogs(ctx context.Context) error {
	logger := klog.FromContext(ctx)
	// 与 Clean 共用互斥锁，保证容器列表快照与清理操作不会互相干扰。
	c.mutex.Lock()
	defer c.mutex.Unlock()
	logger.V(4).Info("Starting container log rotation sequence")
	// TODO(#59998): Use kubelet pod cache.
	containers, err := c.runtimeService.ListContainers(ctx, &runtimeapi.ContainerFilter{})
	if err != nil {
		return fmt.Errorf("failed to list containers: %w", err)
	}
	for _, container := range containers {
		// 只轮转运行中容器的日志。非运行容器不会产生新输出，
		// 保留一个空的“最新日志”没有意义。
		if container.GetState() != runtimeapi.ContainerState_CONTAINER_RUNNING {
			continue
		}
		// 先判断日志级别是否启用，避免在关闭时仍构造可能开销很大的标签参数。
		if v := logger.V(4); v.Enabled() {
			logger.V(4).Info("Adding new entry to the queue for processing", "id", container.GetId(), "name", container.Metadata.GetName(), "labels", container.GetLabels())
		}
		c.queue.Add(container.GetId())
	}
	return nil
}

// processContainer 处理队列中的单个容器：按需轮转其日志。
func (c *containerLogManager) processContainer(ctx context.Context, worker int) (ok bool) {
	key, quit := c.queue.Get()
	if quit {
		return false
	}
	defer func() {
		c.queue.Done(key)
		c.queue.Forget(key)
	}()
	// 返回值默认为 true，保证队列处理循环持续进行。
	ok = true
	id := key
	logger := klog.FromContext(ctx)
	resp, err := c.runtimeService.ContainerStatus(ctx, id, false)
	if err != nil {
		logger.Error(err, "Failed to get container status", "worker", worker, "containerID", id)
		return
	}
	if resp.GetStatus() == nil {
		logger.Error(err, "Container status is nil", "worker", worker, "containerID", id)
		return
	}
	// 获取日志文件当前大小，判断是否达到轮转阈值。
	path := resp.GetStatus().GetLogPath()
	info, err := c.osInterface.Stat(path)

	if err != nil {
		if !os.IsNotExist(err) {
			logger.Error(err, "Failed to stat container log", "worker", worker, "containerID", id, "path", path)
			return
		}

		// 日志文件丢失时请求运行时重新打开日志，随后重新获取文件信息。
		if err = c.runtimeService.ReopenContainerLog(ctx, id); err != nil {
			logger.Error(err, "Container log doesn't exist, reopen container log failed", "worker", worker, "containerID", id, "path", path)
			return
		}

		info, err = c.osInterface.Stat(path)
		if err != nil {
			logger.Error(err, "Failed to stat container log after reopen", "worker", worker, "containerID", id, "path", path)
			return
		}
	}
	// 未达到大小阈值则跳过本轮轮转。
	if info.Size() < c.policy.MaxSize {
		logger.V(7).Info("log file doesn't need to be rotated", "worker", worker, "containerID", id, "path", path, "currentSize", info.Size(), "maxSize", c.policy.MaxSize)
		return
	}

	if err := c.rotateLog(ctx, id, path); err != nil {
		logger.Error(err, "Failed to rotate log for container", "worker", worker, "containerID", id, "path", path, "currentSize", info.Size(), "maxSize", c.policy.MaxSize)
		return
	}
	return
}

// rotateLog 执行一次完整的轮转：清理废弃文件、删除超额文件、压缩旧日志，
// 最后轮转当前日志文件。
func (c *containerLogManager) rotateLog(ctx context.Context, id, log string) error {
	// 用通配模式匹配所有轮转文件。
	pattern := fmt.Sprintf("%s.*", log)
	logs, err := filepath.Glob(pattern)
	if err != nil {
		return fmt.Errorf("failed to list all log files with pattern %q: %w", pattern, err)
	}

	logs, err = c.cleanupUnusedLogs(logs)
	if err != nil {
		return fmt.Errorf("failed to cleanup logs: %w", err)
	}

	logs, err = c.removeExcessLogs(logs)
	if err != nil {
		return fmt.Errorf("failed to remove excess logs: %w", err)
	}

	// 压缩尚未压缩的旧日志文件。
	for _, l := range logs {
		if strings.HasSuffix(l, compressSuffix) {
			continue
		}
		if err := c.compressLog(l); err != nil {
			return fmt.Errorf("failed to compress log %q: %w", l, err)
		}
	}

	if err := c.rotateLatestLog(ctx, id, log); err != nil {
		return fmt.Errorf("failed to rotate log %q: %w", log, err)
	}

	return nil
}

// cleanupUnusedLogs 清理此前轮转失败遗留的临时文件或已废弃的日志文件。
func (c *containerLogManager) cleanupUnusedLogs(logs []string) ([]string, error) {
	inuse, unused := filterUnusedLogs(logs)
	for _, l := range unused {
		if err := c.osInterface.Remove(l); err != nil {
			return nil, fmt.Errorf("failed to remove unused log %q: %w", l, err)
		}
	}
	return inuse, nil
}

// filterUnusedLogs 将日志分为两组：第一组为在用日志，第二组为未使用日志。
func filterUnusedLogs(logs []string) (inuse []string, unused []string) {
	for _, l := range logs {
		if isInUse(l, logs) {
			inuse = append(inuse, l)
		} else {
			unused = append(unused, l)
		}
	}
	return inuse, unused
}

// isInUse 判断某个容器日志文件是否仍在使用。
func isInUse(l string, logs []string) bool {
	// 所有临时文件都不在使用。
	if strings.HasSuffix(l, tmpSuffix) {
		return false
	}
	// 所有压缩日志都在使用。
	if strings.HasSuffix(l, compressSuffix) {
		return true
	}
	// 已经存在对应压缩文件的原始日志不再使用。
	for _, another := range logs {
		if l+compressSuffix == another {
			return false
		}
	}
	return true
}

// removeExcessLogs 删除旧日志，确保日志文件总数不超过 MaxFiles。
func (c *containerLogManager) removeExcessLogs(logs []string) ([]string, error) {
	// 按从旧到新的顺序排序日志文件。
	sort.Strings(logs)
	// 容器会创建新的日志文件，我们还会轮转最新的日志文件。
	// 除这两个文件外，最多可保留 MaxFiles-2 个轮转日志。
	// 通过删除旧文件保留 MaxFiles-2 个文件。
	// 必须从旧到新删除，以免破坏正在进行的 `kubectl logs`。
	maxRotatedFiles := c.policy.MaxFiles - 2
	if maxRotatedFiles < 0 {
		maxRotatedFiles = 0
	}
	i := 0
	for ; i < len(logs)-maxRotatedFiles; i++ {
		if err := c.osInterface.Remove(logs[i]); err != nil {
			return nil, fmt.Errorf("failed to remove old log %q: %w", logs[i], err)
		}
	}
	logs = logs[i:]
	return logs, nil
}

// compressLog 使用 gzip 将日志压缩为 log.gz。
func (c *containerLogManager) compressLog(log string) error {
	logInfo, err := os.Stat(log)
	if err != nil {
		return fmt.Errorf("failed to stat log file: %w", err)
	}
	r, err := c.osInterface.Open(log)
	if err != nil {
		return fmt.Errorf("failed to open log %q: %w", log, err)
	}
	defer r.Close()
	// 先写入临时文件，成功后再重命名，避免产生不完整的 .gz 文件。
	tmpLog := log + tmpSuffix
	f, err := c.osInterface.OpenFile(tmpLog, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, logInfo.Mode())
	if err != nil {
		return fmt.Errorf("failed to create temporary log %q: %w", tmpLog, err)
	}
	defer func() {
		// 尽力清理临时文件，失败也不影响主流程。
		c.osInterface.Remove(tmpLog)
	}()
	defer f.Close()
	w := gzip.NewWriter(f)
	defer w.Close()
	if _, err := io.Copy(w, r); err != nil {
		return fmt.Errorf("failed to compress %q to %q: %w", log, tmpLog, err)
	}
	// 重命名前必须先关闭压缩归档，否则在 Windows 上会报错。
	w.Close()
	f.Close()
	compressedLog := log + compressSuffix
	if err := c.osInterface.Rename(tmpLog, compressedLog); err != nil {
		return fmt.Errorf("failed to rename %q to %q: %w", tmpLog, compressedLog, err)
	}
	// 删除旧的未压缩日志文件。
	r.Close()
	if err := c.osInterface.Remove(log); err != nil {
		return fmt.Errorf("failed to remove log %q after compress: %w", log, err)
	}
	return nil
}

// rotateLatestLog 轮转最新的日志文件但不做压缩，
// 以便容器继续写入、日志采集器（如 fluentd）仍能读完旧内容。
func (c *containerLogManager) rotateLatestLog(ctx context.Context, id, log string) error {
	logger := klog.FromContext(ctx)
	timestamp := c.clock.Now().Format(timestampFormat)
	rotated := fmt.Sprintf("%s.%s", log, timestamp)
	if err := c.osInterface.Rename(log, rotated); err != nil {
		return fmt.Errorf("failed to rotate log %q to %q: %w", log, rotated, err)
	}
	if err := c.runtimeService.ReopenContainerLog(ctx, id); err != nil {
		// 将已轮转的日志改名回原路径，以便下一轮重新尝试轮转。
		// 如果此时 kubelet 重启，原日志将会丢失。
		if renameErr := c.osInterface.Rename(rotated, log); renameErr != nil {
			// 这不应发生。
			// 若发生则上报错误，因为原日志将丢失。
			logger.Error(renameErr, "Failed to rename rotated log", "rotatedLog", rotated, "newLog", log, "containerID", id)
		}
		return fmt.Errorf("failed to reopen container log %q: %w", id, err)
	}
	return nil
}
