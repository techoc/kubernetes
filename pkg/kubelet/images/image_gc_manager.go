/*
Copyright 2015 The Kubernetes Authors.

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

package images

import (
	"context"
	goerrors "errors"
	"fmt"
	"math"
	"sort"
	"strings"
	"sync"
	"time"

	"go.opentelemetry.io/otel/trace"

	v1 "k8s.io/api/core/v1"
	"k8s.io/klog/v2"

	"k8s.io/apimachinery/pkg/util/errors"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/wait"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/client-go/tools/record"
	statsapi "k8s.io/kubelet/pkg/apis/stats/v1alpha1"
	"k8s.io/kubernetes/pkg/features"
	"k8s.io/kubernetes/pkg/kubelet/container"
	"k8s.io/kubernetes/pkg/kubelet/events"
	"k8s.io/kubernetes/pkg/kubelet/metrics"
	"k8s.io/kubernetes/pkg/kubelet/util/sliceutils"
)

// instrumentationScope 是 OpenTelemetry 的 instrumentation scope 名称
const instrumentationScope = "k8s.io/kubernetes/pkg/kubelet/images"

// 当 RuntimeClassInImageCriAPI 特性门控启用时，imageRecord 以
// imageId-RuntimeHandler 元组作为索引
const imageIndexTupleFormat = "%s,%s"

// ImageGarbageCollectedTotalReason* 说明某镜像在
// `image_garbage_collected_total` 指标中被垃圾回收的原因。
const (
	ImageGarbageCollectedTotalReasonAge   = "age"
	ImageGarbageCollectedTotalReasonSpace = "space"
)

// PostImageGCHook 允许外部调用方对 GC 回收事件做出响应。
// `remainingImages` 是垃圾回收结束后仍保留在系统上的镜像列表。
type PostImageGCHook func(ctx context.Context, remainingImages []string, gcStart time.Time)

// StatsProvider 是获取镜像垃圾回收期间所用统计信息的接口。
type StatsProvider interface {
	// ImageFsStats 返回镜像文件系统的统计信息。
	ImageFsStats(ctx context.Context) (*statsapi.FsStats, *statsapi.FsStats, error)
}

// ImageGCManager 是管理所有镜像生命周期的接口。
// 实现必须是线程安全的。
type ImageGCManager interface {
	// 执行垃圾回收策略。若无法按策略释放足够空间，则返回错误。
	GarbageCollect(ctx context.Context, beganGC time.Time) error

	// Start 异步启动镜像垃圾回收。
	Start(ctx context.Context)

	GetImageList() ([]container.Image, error)

	// DeleteUnusedImages 删除所有未被使用的镜像。
	DeleteUnusedImages(ctx context.Context) error
}

// ImageGCPolicy 是镜像垃圾回收的策略。
// 策略定义了垃圾回收运行的允许区间。
type ImageGCPolicy struct {
	// 任何高于该阈值的用量都会触发垃圾回收。
	// 这是允许的最高用量。
	HighThresholdPercent int

	// 任何低于该阈值的用量都不会触发垃圾回收。
	// 这是尝试回收到的最低阈值。
	LowThresholdPercent int

	// 镜像可被垃圾回收的最小存活时间。
	MinAge time.Duration

	// 镜像可被垃圾回收的最大存活时间，无论磁盘用量如何。
	// 当前受 MaximumImageGCAge 特性门控和 Kubelet 配置约束。
	// 若为 0，则该特性被禁用。
	MaxAge time.Duration
}

type realImageGCManager struct {
	// 容器运行时
	runtime container.Runtime

	// 镜像及其使用情况的记录，按 ImageId 索引。
	// 当 RuntimeClassInImageCriAPI 特性门控启用时，imageRecords 以
	// 来自 ListImages() 调用的 (imageId,runtimeHandler) 元组标识。
	// 若容器运行时在 ListImages() 的响应中未指定 runtimeHandler，
	// 则仅以 imageID 作为该 map 的索引。
	imageRecords     map[string]*imageRecord
	imageRecordsLock sync.Mutex

	// 当前使用的镜像垃圾回收策略。
	policy ImageGCPolicy

	// statsProvider 提供镜像垃圾回收期间所用的统计信息。
	statsProvider StatsProvider

	// 用于记录 Kubernetes 事件的 Recorder。
	recorder record.EventRecorder

	// 指向本节点的引用。
	nodeRef *v1.ObjectReference

	// imageCache 缓存最新的镜像列表。
	imageCache imageCache

	postGCHooks []PostImageGCHook

	// tracer 用于记录 span。
	tracer trace.Tracer
}

// imageCache 缓存 ListImages 的最新结果。
type imageCache struct {
	// sync.Mutex 是保护镜像缓存的互斥锁。
	sync.Mutex
	// images 是缓存的镜像列表。
	images []container.Image
}

// set 对输入列表排序并更新镜像缓存。
// 该函数接管列表的所有权，调用后不应再引用该列表。
func (i *imageCache) set(images []container.Image) {
	i.Lock()
	defer i.Unlock()
	// 镜像列表在被读取并用于 setNodeStatusImages 时需要已排序。
	// 这里选择在写入时排序而非读取时排序，因为镜像缓存是读多写少的。
	sort.Sort(sliceutils.ByImageSize(images))
	i.images = images
}

// get 从镜像缓存中获取镜像列表。
// 注意：get() 的调用方不应对返回的列表做可能与其他读取者产生数据竞争的
// 修改操作（例如就地排序返回的列表）。
func (i *imageCache) get() []container.Image {
	i.Lock()
	defer i.Unlock()
	return i.images
}

// imageRecord 保存所跟踪镜像的相关信息。
type imageRecord struct {
	// 拉取该镜像时使用的 runtime handler
	runtimeHandlerUsedToPullImage string
	// 该镜像首次被检测到的时间。
	firstDetected time.Time

	// 最近一次看到该镜像被使用的时间。
	lastUsed time.Time

	// 镜像的大小（字节）。
	size int64

	// 镜像的 pinned 状态
	pinned bool
}

// NewImageGCManager 实例化一个新的 ImageGCManager 对象，并校验策略合法性。
func NewImageGCManager(runtime container.Runtime, statsProvider StatsProvider, postGCHooks []PostImageGCHook, recorder record.EventRecorder, nodeRef *v1.ObjectReference, policy ImageGCPolicy, tracerProvider trace.TracerProvider) (ImageGCManager, error) {
	// 校验策略：阈值必须落在 [0,100]，且低阈值不能高于高阈值。
	if policy.HighThresholdPercent < 0 || policy.HighThresholdPercent > 100 {
		return nil, fmt.Errorf("invalid HighThresholdPercent %d, must be in range [0-100]", policy.HighThresholdPercent)
	}
	if policy.LowThresholdPercent < 0 || policy.LowThresholdPercent > 100 {
		return nil, fmt.Errorf("invalid LowThresholdPercent %d, must be in range [0-100]", policy.LowThresholdPercent)
	}
	if policy.LowThresholdPercent > policy.HighThresholdPercent {
		return nil, fmt.Errorf("LowThresholdPercent %d can not be higher than HighThresholdPercent %d", policy.LowThresholdPercent, policy.HighThresholdPercent)
	}
	tracer := tracerProvider.Tracer(instrumentationScope)
	im := &realImageGCManager{
		runtime:       runtime,
		policy:        policy,
		imageRecords:  make(map[string]*imageRecord),
		statsProvider: statsProvider,
		recorder:      recorder,
		nodeRef:       nodeRef,
		postGCHooks:   postGCHooks,
		tracer:        tracer,
	}

	return im, nil
}

// Start 启动两个后台协程：一个周期性地检测镜像（识别在用镜像、更新镜像记录），
// 另一个周期性地刷新镜像缓存。
func (im *realImageGCManager) Start(ctx context.Context) {
	logger := klog.FromContext(ctx)
	go wait.UntilWithContext(ctx, func(ctx context.Context) {
		_, err := im.detectImages(ctx, time.Now())
		if err != nil {
			logger.Info("Failed to monitor images", "err", err)
		}
	}, 5*time.Minute)

	// 启动协程周期性地更新镜像缓存。
	go wait.UntilWithContext(ctx, func(ctx context.Context) {
		images, err := im.runtime.ListImages(ctx)
		if err != nil {
			logger.Info("Failed to update image list", "err", err)
		} else {
			im.imageCache.set(images)
		}
	}, 30*time.Second)

}

// GetImageList 获取本节点上的镜像列表。
func (im *realImageGCManager) GetImageList() ([]container.Image, error) {
	return im.imageCache.get(), nil
}

// detectImages 从容器运行时获取镜像与 pod 列表，统计正在使用的镜像，
// 更新镜像记录（新增、刷新 lastUsed/size/pinned、删除已不存在的镜像），
// 并返回在用镜像的 key 集合。
func (im *realImageGCManager) detectImages(ctx context.Context, detectTime time.Time) (sets.Set[string], error) {
	logger := klog.FromContext(ctx)
	isRuntimeClassInImageCriAPIEnabled := utilfeature.DefaultFeatureGate.Enabled(features.RuntimeClassInImageCriAPI)
	imagesInUse := sets.New[string]()

	images, err := im.runtime.ListImages(ctx)
	if err != nil {
		return imagesInUse, err
	}
	pods, err := im.runtime.GetPods(ctx, true)
	if err != nil {
		return imagesInUse, err
	}

	// 构造容器正在使用的镜像集合。
	for _, pod := range pods {
		for _, container := range pod.Containers {
			if err := im.handleImageVolumes(ctx, imagesInUse, container, pod, images); err != nil {
				return imagesInUse, err
			}

			if !isRuntimeClassInImageCriAPIEnabled {
				logger.V(5).Info("Container uses image", "pod", klog.KRef(pod.Namespace, pod.Name), "containerName", container.Name, "containerImage", container.Image, "imageID", container.ImageID, "imageRef", container.ImageRef)
				imagesInUse.Insert(container.ImageID)
			} else {
				imageKey := getImageTuple(container.ImageID, container.ImageRuntimeHandler)
				logger.V(5).Info("Container uses image", "pod", klog.KRef(pod.Namespace, pod.Name), "containerName", container.Name, "containerImage", container.Image, "imageID", container.ImageID, "imageRef", container.ImageRef, "imageKey", imageKey)
				imagesInUse.Insert(imageKey)
			}
		}
	}

	// 加入新镜像并记录正在被使用的镜像。
	now := time.Now()
	currentImages := sets.New[string]()
	im.imageRecordsLock.Lock()
	defer im.imageRecordsLock.Unlock()
	for _, image := range images {
		imageKey := image.ID
		if !isRuntimeClassInImageCriAPIEnabled {
			logger.V(5).Info("Adding image ID to currentImages", "imageID", imageKey)
		} else {
			imageKey = getImageTuple(image.ID, image.Spec.RuntimeHandler)
			logger.V(5).Info("Adding image ID with runtime class to currentImages", "imageKey", imageKey, "runtimeHandler", image.Spec.RuntimeHandler)
		}

		currentImages.Insert(imageKey)

		// 新镜像，将其首次检测时间设为当前时间。
		if _, ok := im.imageRecords[imageKey]; !ok {
			logger.V(5).Info("Image ID is new", "imageID", imageKey, "runtimeHandler", image.Spec.RuntimeHandler)
			im.imageRecords[imageKey] = &imageRecord{
				firstDetected:                 detectTime,
				runtimeHandlerUsedToPullImage: image.Spec.RuntimeHandler,
			}
		}

		// 若镜像正在被使用，则将 lastUsed 更新为当前时间。
		if isImageUsed(imageKey, imagesInUse) {
			logger.V(5).Info("Setting Image ID lastUsed", "imageID", imageKey, "lastUsed", now)
			im.imageRecords[imageKey].lastUsed = now
		}

		logger.V(5).Info("Image ID has size", "imageID", imageKey, "size", image.Size)
		im.imageRecords[imageKey].size = image.Size

		logger.V(5).Info("Image ID is pinned", "imageID", imageKey, "pinned", image.Pinned)
		im.imageRecords[imageKey].pinned = image.Pinned
	}

	// 从记录中移除已不存在的旧镜像。
	for image := range im.imageRecords {
		if !currentImages.Has(image) {
			logger.V(5).Info("Image ID is no longer present; removing from imageRecords", "imageID", image)
			delete(im.imageRecords, image)
		}
	}

	return imagesInUse, nil
}

// handleImageVolumes 确保把 image 卷视为在用的镜像。
func (im *realImageGCManager) handleImageVolumes(ctx context.Context, imagesInUse sets.Set[string], container *container.Container, pod *container.Pod, images []container.Image) error {
	logger := klog.FromContext(ctx)
	if !utilfeature.DefaultFeatureGate.Enabled(features.ImageVolume) {
		return nil
	}

	status, err := im.runtime.GetContainerStatus(ctx, pod.ID, container.ID)
	if err != nil {
		return fmt.Errorf("get container status: %w", err)
	}

	for _, mount := range status.Mounts {
		for _, image := range images {
			if mount.Image != nil && mount.Image.Image == image.ID {
				logger.V(5).Info("Container uses image as mount", "pod", klog.KRef(pod.Namespace, pod.Name), "containerName", container.Name, "imageID", image.ID)
				imagesInUse.Insert(image.ID)
			}
		}
	}

	return nil
}

// GarbageCollect 按策略执行镜像垃圾回收：先处理按驱逐顺序排好的过期镜像，
// 再在超过高阈值时释放足够空间回到低阈值；空间不足则上报事件并返回错误。
func (im *realImageGCManager) GarbageCollect(ctx context.Context, beganGC time.Time) error {
	ctx, otelSpan := im.tracer.Start(ctx, "Images/GarbageCollect")
	logger := klog.FromContext(ctx)
	defer otelSpan.End()

	freeTime := time.Now()
	images, err := im.imagesInEvictionOrder(ctx, freeTime)
	if err != nil {
		return err
	}

	images, err = im.freeOldImages(ctx, images, freeTime, beganGC)
	if err != nil {
		return err
	}

	// 获取承载镜像的磁盘的用量统计。
	fsStats, _, err := im.statsProvider.ImageFsStats(ctx)
	if err != nil {
		return err
	}

	var capacity, available int64
	if fsStats.CapacityBytes != nil {
		capacity = int64(*fsStats.CapacityBytes)
	}
	if fsStats.AvailableBytes != nil {
		available = int64(*fsStats.AvailableBytes)
	}

	if available > capacity {
		logger.Info("Availability is larger than capacity", "available", available, "capacity", capacity)
		available = capacity
	}

	// 校验容量有效性：容量为 0 时视为非法。
	if capacity == 0 {
		err := goerrors.New("invalid capacity 0 on image filesystem")
		im.recorder.Eventf(im.nodeRef, v1.EventTypeWarning, events.InvalidDiskCapacity, "%s", err.Error())
		return err
	}

	// 若超过高阈值，则释放足够空间，使使用量回落到低阈值。
	usagePercent := 100 - int(available*100/capacity)
	if usagePercent >= im.policy.HighThresholdPercent {
		amountToFree := capacity*int64(100-im.policy.LowThresholdPercent)/100 - available
		logger.Info("Disk usage on image filesystem is over the high threshold, trying to free bytes down to the low threshold", "usage", usagePercent, "highThreshold", im.policy.HighThresholdPercent, "amountToFree", amountToFree, "lowThreshold", im.policy.LowThresholdPercent)
		remainingImages, freed, err := im.freeSpace(ctx, amountToFree, freeTime, images)
		if err != nil {
			// 删除镜像失败，例如文件系统只读。
			return err
		}

		im.runPostGCHooks(ctx, remainingImages, freeTime)

		if freed < amountToFree {
			// 这通常意味着磁盘被非容器镜像的内容占满，
			// 例如日志、卷或其他文件。但也可能是正在使用的镜像数量或体积异常大。
			message := fmt.Sprintf("Insufficient free disk space on the node's image filesystem (%d%% of %s used). "+
				"Failed to free sufficient space by deleting unused images (freed %d bytes). "+
				"Investigate disk usage, as it could be used by active images, logs, volumes, or other data.",
				usagePercent, formatSize(capacity), freed)
			im.recorder.Eventf(im.nodeRef, v1.EventTypeWarning, events.FreeDiskSpaceFailed, "%s", message)
			return fmt.Errorf("%s", message)
		}
	}

	return nil
}

// runPostGCHooks 依次调用所有 GC 后置钩子，通知其本次回收后的剩余镜像。
func (im *realImageGCManager) runPostGCHooks(ctx context.Context, remainingImages []string, gcStartTime time.Time) {
	for _, h := range im.postGCHooks {
		h(ctx, remainingImages, gcStartTime)
	}
}

// freeOldImages 删除 lastUsed 超过 MaxAge 的镜像。
// 若自 Kubelet 启动以来尚未达到 MaxAge，则直接跳过，避免过早回收。
func (im *realImageGCManager) freeOldImages(ctx context.Context, images []evictionInfo, freeTime, beganGC time.Time) ([]evictionInfo, error) {
	if im.policy.MaxAge == 0 {
		return images, nil
	}

	// 等待自 Kubelet 启动以来达到 MaxAge，
	// 否则可能过早地对镜像做垃圾回收。
	if freeTime.Sub(beganGC) <= im.policy.MaxAge {
		return images, nil
	}
	var deletionErrors []error
	logger := klog.FromContext(ctx)
	remainingImages := make([]evictionInfo, 0)
	for _, image := range images {
		logger.V(5).Info("Evaluating image ID for possible garbage collection based on image age", "imageID", image.id)
		// 判断镜像是否比 MaxAge 更旧。
		if freeTime.Sub(image.lastUsed) > im.policy.MaxAge {
			if err := im.freeImage(ctx, image, ImageGarbageCollectedTotalReasonAge); err != nil {
				deletionErrors = append(deletionErrors, err)
				remainingImages = append(remainingImages, image)
				continue
			}
			continue
		}
		remainingImages = append(remainingImages, image)
	}
	if len(deletionErrors) > 0 {
		return remainingImages, fmt.Errorf("wanted to free images older than %v, encountered errors in image deletion: %v", im.policy.MaxAge, errors.NewAggregate(deletionErrors))
	}
	return remainingImages, nil
}

// DeleteUnusedImages 删除所有未被使用的镜像，释放全部可回收空间，
// 完成后触发 GC 后置钩子。
func (im *realImageGCManager) DeleteUnusedImages(ctx context.Context) error {
	logger := klog.FromContext(ctx)
	logger.Info("Attempting to delete unused images")
	freeTime := time.Now()

	images, err := im.imagesInEvictionOrder(ctx, freeTime)
	if err != nil {
		return err
	}

	remainingImages, _, err := im.freeSpace(ctx, math.MaxInt64, freeTime, images)
	if err != nil {
		return err
	}

	im.runPostGCHooks(ctx, remainingImages, freeTime)
	return nil
}

// freeSpace 尝试在磁盘上释放 bytesToFree 字节的镜像空间。
//
// 返回清理后仍保留的镜像、释放的字节数以及发生的错误。释放字节数始终会返回。
// 注意：错误可能为 nil，而实际释放的字节数可能小于 bytesToFree。
func (im *realImageGCManager) freeSpace(ctx context.Context, bytesToFree int64, freeTime time.Time, images []evictionInfo) ([]string, int64, error) {
	// 逐个删除未使用的镜像，直到释放出足够的空间。
	var deletionErrors []error
	logger := klog.FromContext(ctx)
	spaceFreed := int64(0)
	var imagesLeft []string
	for _, image := range images {
		logger.V(5).Info("Evaluating image ID for possible garbage collection based on disk usage", "imageID", image.id, "runtimeHandler", image.runtimeHandlerUsedToPullImage)
		// 当前正在使用的镜像被赋予了更新的 lastUsed。
		if image.lastUsed.Equal(freeTime) || image.lastUsed.After(freeTime) {
			imagesLeft = append(imagesLeft, image.id)
			logger.V(5).Info("Image ID was used too recently, not eligible for garbage collection", "imageID", image.id, "lastUsed", image.lastUsed, "freeTime", freeTime)
			continue
		}

		// 若镜像还不够老则避免回收：
		// 这种镜像可能刚被拉取下来，马上就会被容器使用。
		if freeTime.Sub(image.firstDetected) < im.policy.MinAge {
			imagesLeft = append(imagesLeft, image.id)
			logger.V(5).Info("Image ID's age is less than the policy's minAge, not eligible for garbage collection", "imageID", image.id, "age", freeTime.Sub(image.firstDetected), "minAge", im.policy.MinAge)
			continue
		}

		if err := im.freeImage(ctx, image, ImageGarbageCollectedTotalReasonSpace); err != nil {
			deletionErrors = append(deletionErrors, err)
			imagesLeft = append(imagesLeft, image.id)
			continue
		}
		spaceFreed += image.size

		if spaceFreed >= bytesToFree {
			break
		}
	}

	if len(deletionErrors) > 0 {
		return nil, spaceFreed, fmt.Errorf("wanted to free %d bytes, but freed %d bytes space with errors in image deletion: %w", bytesToFree, spaceFreed, errors.NewAggregate(deletionErrors))
	}
	return imagesLeft, spaceFreed, nil
}

// freeImage 从容器运行时删除指定镜像，成功后从 imageRecords 中移除记录，
// 并按 reason 递增镜像回收指标。
func (im *realImageGCManager) freeImage(ctx context.Context, image evictionInfo, reason string) error {
	isRuntimeClassInImageCriAPIEnabled := utilfeature.DefaultFeatureGate.Enabled(features.RuntimeClassInImageCriAPI)
	// 删除镜像。即使出错也继续执行后续处理。
	var err error
	logger := klog.FromContext(ctx)
	logger.Info("Removing image to free bytes", "imageID", image.id, "size", image.size, "runtimeHandler", image.runtimeHandlerUsedToPullImage)
	err = im.runtime.RemoveImage(ctx, container.ImageSpec{Image: image.id, RuntimeHandler: image.runtimeHandlerUsedToPullImage})
	if err != nil {
		return err
	}

	imageKey := image.id
	if isRuntimeClassInImageCriAPIEnabled {
		imageKey = getImageTuple(image.id, image.runtimeHandlerUsedToPullImage)
	}

	im.imageRecordsLock.Lock()
	delete(im.imageRecords, imageKey)
	im.imageRecordsLock.Unlock()

	metrics.ImageGarbageCollectedTotal.WithLabelValues(reason).Inc()
	return err
}

// imagesInEvictionOrder 查询全部镜像记录，整理成 evictionInfo 切片，
// 按最近使用时间排序，忽略被运行时 pin 住的镜像。
func (im *realImageGCManager) imagesInEvictionOrder(ctx context.Context, freeTime time.Time) ([]evictionInfo, error) {
	isRuntimeClassInImageCriAPIEnabled := utilfeature.DefaultFeatureGate.Enabled(features.RuntimeClassInImageCriAPI)
	imagesInUse, err := im.detectImages(ctx, freeTime)
	if err != nil {
		return nil, err
	}

	im.imageRecordsLock.Lock()
	defer im.imageRecordsLock.Unlock()
	logger := klog.FromContext(ctx)

	// 按驱逐顺序收集所有镜像。
	images := make([]evictionInfo, 0, len(im.imageRecords))
	for image, record := range im.imageRecords {
		if isImageUsed(image, imagesInUse) {
			logger.V(5).Info("Image ID is being used", "imageID", image)
			continue
		}
		// 检查镜像是否被 pin 住，被 pin 的镜像不做垃圾回收
		if record.pinned {
			logger.V(5).Info("Image is pinned, skipping garbage collection", "imageID", image)
			continue

		}
		if !isRuntimeClassInImageCriAPIEnabled {
			images = append(images, evictionInfo{
				id:          image,
				imageRecord: *record,
			})
		} else {
			imageID := getImageIDFromTuple(image)
			// 确保 imageID 合法，否则跳过该镜像
			if imageID == "" {
				im.recorder.Eventf(im.nodeRef, v1.EventTypeWarning, events.InvalidImageID, "ImageID is not valid, skipping, image: %v", image)
				continue
			}
			images = append(images, evictionInfo{
				id:          imageID,
				imageRecord: *record,
			})
		}
	}
	sort.Sort(byLastUsedAndDetected(images))
	return images, nil
}

// formatSize 把给定字节数转换为人类可读的字符串。
func formatSize(sizeBytes int64) string {
	const (
		KiB = 1024
		MiB = 1024 * KiB
		GiB = 1024 * MiB
		TiB = 1024 * GiB
	)

	size := float64(sizeBytes)

	switch {
	case size < KiB:
		return fmt.Sprintf("%d B", int64(size))
	case size < MiB:
		return fmt.Sprintf("%.1f KiB", size/KiB)
	case size < GiB:
		return fmt.Sprintf("%.1f MiB", size/MiB)
	case size < TiB:
		return fmt.Sprintf("%.1f GiB", size/GiB)
	default:
		return fmt.Sprintf("%.1f TiB", size/TiB)
	}
}

// 当 RuntimeClassInImageCriAPI 特性门控启用时，imageRecords 以
// 来自 ListImages() 调用的 (imageId,runtimeHandler) 元组标识。
// 若容器运行时在 ListImages() 的响应中未指定 runtimeHandler，则只返回 imageID。
func getImageTuple(imageID, runtimeHandler string) string {
	if runtimeHandler == "" {
		return imageID
	}
	return fmt.Sprintf(imageIndexTupleFormat, imageID, runtimeHandler)
}

// getImageIDFromTuple 从镜像元组中取出 imageID。
func getImageIDFromTuple(image string) string {
	imageTuples := strings.Split(image, ",")
	return imageTuples[0]
}

// evictionInfo 是参与驱逐排序的镜像条目，由镜像 ID 与镜像记录组成。
type evictionInfo struct {
	id string
	imageRecord
}

// byLastUsedAndDetected 实现 sort.Interface，按 lastUsed 对 evictionInfo 排序。
type byLastUsedAndDetected []evictionInfo

func (ev byLastUsedAndDetected) Len() int      { return len(ev) }
func (ev byLastUsedAndDetected) Swap(i, j int) { ev[i], ev[j] = ev[j], ev[i] }
func (ev byLastUsedAndDetected) Less(i, j int) bool {
	// 按最近使用时间排序，相同时按首次检测时间排序。
	if ev[i].lastUsed.Equal(ev[j].lastUsed) {
		return ev[i].firstDetected.Before(ev[j].firstDetected)
	}
	return ev[i].lastUsed.Before(ev[j].lastUsed)
}

// isImageUsed 判断镜像是否在在用镜像集合中。
func isImageUsed(imageID string, imagesInUse sets.Set[string]) bool {
	// 检查镜像 ID。
	if _, ok := imagesInUse[imageID]; ok {
		return true
	}
	return false
}
