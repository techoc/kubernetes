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
	"fmt"
	goruntime "runtime"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	noopoteltrace "go.opentelemetry.io/otel/trace/noop"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/client-go/tools/record"
	featuregatetesting "k8s.io/component-base/featuregate/testing"
	statsapi "k8s.io/kubelet/pkg/apis/stats/v1alpha1"
	"k8s.io/kubernetes/pkg/features"
	"k8s.io/kubernetes/pkg/kubelet/container"
	containertest "k8s.io/kubernetes/pkg/kubelet/container/testing"
	stats "k8s.io/kubernetes/pkg/kubelet/server/stats"
	statstest "k8s.io/kubernetes/pkg/kubelet/server/stats/testing"
	"k8s.io/kubernetes/test/utils/ktesting"
	testingclock "k8s.io/utils/clock/testing"
	"k8s.io/utils/ptr"
)

var zero time.Time
var sandboxImage = "registry.k8s.io/pause-amd64:latest"

// newRealImageGCManager 构造使用假运行时的镜像 GC 管理器，便于注入镜像列表与策略。
func newRealImageGCManager(policy ImageGCPolicy, mockStatsProvider stats.Provider) (*realImageGCManager, *containertest.FakeRuntime) {
	fakeRuntime := &containertest.FakeRuntime{}
	return &realImageGCManager{
		runtime:       fakeRuntime,
		policy:        policy,
		imageRecords:  make(map[string]*imageRecord),
		statsProvider: mockStatsProvider,
		recorder:      &record.FakeRecorder{},
		tracer:        noopoteltrace.NewTracerProvider().Tracer(""),
	}, fakeRuntime
}

// 以下访问器用于线程安全测试。
// imageRecordsLen 在锁保护下返回镜像记录数量。
func (im *realImageGCManager) imageRecordsLen() int {
	im.imageRecordsLock.Lock()
	defer im.imageRecordsLock.Unlock()
	return len(im.imageRecords)
}

// getImageRecord 在锁保护下复制并返回指定镜像记录。
func (im *realImageGCManager) getImageRecord(name string) (*imageRecord, bool) {
	im.imageRecordsLock.Lock()
	defer im.imageRecordsLock.Unlock()
	v, ok := im.imageRecords[name]
	vCopy := *v
	return &vCopy, ok
}

// getImageRecordWithRuntimeHandlerInImageCriAPIFeatureGate 按镜像名与 runtime handler 的组合键查找记录。
func (im *realImageGCManager) getImageRecordWithRuntimeHandlerInImageCriAPIFeatureGate(name, runtimeHandler string) (*imageRecord, bool) {
	im.imageRecordsLock.Lock()
	defer im.imageRecordsLock.Unlock()
	imageKey := getImageTuple(name, runtimeHandler)
	v, ok := im.imageRecords[imageKey]
	vCopy := *v
	return &vCopy, ok
}

// 返回指定 ID 的镜像 ID。
// imageID 返回测试镜像的 ID。
func imageID(id int) string {
	return fmt.Sprintf("image-%d", id)
}

// 返回指定 ID 的镜像名称。
// imageName 返回测试镜像的名称。
func imageName(id int) string {
	return imageID(id) + "-name"
}

// 构造指定 ID 的镜像。
// makeImage 构造指定 ID 与大小的镜像。
func makeImage(id int, size int64) container.Image {
	return container.Image{
		ID:   imageID(id),
		Size: size,
	}
}

// 构造指定 ID 的镜像。
// makeImageWithRuntimeHandler 构造带 runtime handler 的镜像；空 handler 表示未使用 RuntimeClass。
func makeImageWithRuntimeHandler(id int, size int64, runtimeHandler string) container.Image {
	if runtimeHandler == "" {
		return container.Image{
			ID:   imageID(id),
			Size: size,
		}
	} else {
		return container.Image{
			ID:   imageID(id),
			Size: size,
			Spec: container.ImageSpec{
				RuntimeHandler: runtimeHandler,
			},
		}
	}
}

// 构造指定 ID 的容器，并让它使用同 ID 的镜像。
// makeContainer 构造使用同名镜像的容器，用于标记镜像正在被使用。
func makeContainer(id int) *container.Container {
	return &container.Container{
		ID:      container.ContainerID{Type: "test", ID: fmt.Sprintf("container-%d", id)},
		Image:   imageName(id),
		ImageID: imageID(id),
	}
}

// TestDetectImagesInitialDetect 验证首次探测会记录所有镜像，并区分正在使用与未使用的镜像。
func TestDetectImagesInitialDetect(t *testing.T) {
	ctx := ktesting.Init(t)
	mockStatsProvider := statstest.NewMockProvider(t)

	manager, fakeRuntime := newRealImageGCManager(ImageGCPolicy{}, mockStatsProvider)
	fakeRuntime.ImageList = []container.Image{
		makeImage(0, 1024),
		makeImage(1, 2048),
		makeImage(2, 2048),
	}
	fakeRuntime.AllPodList = []*containertest.FakePod{
		{Pod: &container.Pod{
			Containers: []*container.Container{
				{
					ID:      container.ContainerID{Type: "test", ID: fmt.Sprintf("container-%d", 1)},
					ImageID: imageID(1),
					// 故意不设置镜像名，模拟无名镜像。
				},
				{
					ID:      container.ContainerID{Type: "test", ID: fmt.Sprintf("container-%d", 2)},
					Image:   imageName(2),
					ImageID: imageID(2),
				},
			},
		}},
	}

	startTime := time.Now().Add(-time.Millisecond)
	_, err := manager.detectImages(ctx, zero)
	assert := assert.New(t)
	require.NoError(t, err)
	assert.Equal(3, manager.imageRecordsLen())
	noContainer, ok := manager.getImageRecord(imageID(0))
	require.True(t, ok)
	assert.Equal(zero, noContainer.firstDetected)
	assert.Equal(zero, noContainer.lastUsed)
	withContainerUsingNoNameImage, ok := manager.getImageRecord(imageID(1))
	require.True(t, ok)
	assert.Equal(zero, withContainerUsingNoNameImage.firstDetected)
	assert.True(withContainerUsingNoNameImage.lastUsed.After(startTime))
	withContainer, ok := manager.getImageRecord(imageID(2))
	require.True(t, ok)
	assert.Equal(zero, withContainer.firstDetected)
	assert.True(withContainer.lastUsed.After(startTime))
}

// TestDetectImagesInitialDetectWithRuntimeHandlerInImageCriAPIFeatureGate 验证开启 RuntimeClassInImageCriAPI 后，镜像记录按镜像名与 runtime handler 区分。
func TestDetectImagesInitialDetectWithRuntimeHandlerInImageCriAPIFeatureGate(t *testing.T) {
	featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.RuntimeClassInImageCriAPI, true)
	testRuntimeHandler := "test-runtimeHandler"
	ctx := ktesting.Init(t)
	mockStatsProvider := statstest.NewMockProvider(t)

	manager, fakeRuntime := newRealImageGCManager(ImageGCPolicy{}, mockStatsProvider)
	fakeRuntime.ImageList = []container.Image{
		makeImageWithRuntimeHandler(0, 1024, testRuntimeHandler),
		makeImageWithRuntimeHandler(1, 2048, testRuntimeHandler),
		makeImageWithRuntimeHandler(2, 2048, ""),
	}
	fakeRuntime.AllPodList = []*containertest.FakePod{
		{Pod: &container.Pod{
			Containers: []*container.Container{
				{
					ID:      container.ContainerID{Type: "test", ID: fmt.Sprintf("container-%d", 1)},
					ImageID: imageID(1),
					// 故意不设置镜像名，模拟无名镜像。
					ImageRuntimeHandler: testRuntimeHandler,
				},
				{
					ID:      container.ContainerID{Type: "test", ID: fmt.Sprintf("container-%d", 2)},
					Image:   imageName(2),
					ImageID: imageID(2),
					// 不设置 runtime handler，用于模拟以下场景：
					// 特性门控 RuntimeHandlerInImageCriApi 已开启，但容器运行时尚未实现
					// KEP 4216，即运行时响应中不会携带 runtimeHandler 字符串，
					// 因此该字段为空。
				},
			},
		}},
	}

	startTime := time.Now().Add(-time.Millisecond)
	_, err := manager.detectImages(ctx, zero)
	assert := assert.New(t)
	require.NoError(t, err)
	assert.Equal(3, manager.imageRecordsLen())
	noContainer, ok := manager.getImageRecordWithRuntimeHandlerInImageCriAPIFeatureGate(imageID(0), testRuntimeHandler)
	require.True(t, ok)
	assert.Equal(zero, noContainer.firstDetected)
	assert.Equal(testRuntimeHandler, noContainer.runtimeHandlerUsedToPullImage)
	assert.Equal(zero, noContainer.lastUsed)
	withContainerUsingNoNameImage, ok := manager.getImageRecordWithRuntimeHandlerInImageCriAPIFeatureGate(imageID(1), testRuntimeHandler)
	require.True(t, ok)
	assert.Equal(zero, withContainerUsingNoNameImage.firstDetected)
	assert.True(withContainerUsingNoNameImage.lastUsed.After(startTime))
	assert.Equal(testRuntimeHandler, withContainerUsingNoNameImage.runtimeHandlerUsedToPullImage)
	withContainer, ok := manager.getImageRecordWithRuntimeHandlerInImageCriAPIFeatureGate(imageID(2), "")
	require.True(t, ok)
	assert.Equal(zero, withContainer.firstDetected)
	assert.True(withContainer.lastUsed.After(startTime))
	assert.Equal("", withContainer.runtimeHandlerUsedToPullImage)
}

// TestDetectImagesWithNewImage 验证新镜像的 firstDetected 时间，同时保留已有镜像的使用状态。
func TestDetectImagesWithNewImage(t *testing.T) {
	ctx := ktesting.Init(t)
	mockStatsProvider := statstest.NewMockProvider(t)

	// 初始只有两个镜像。
	manager, fakeRuntime := newRealImageGCManager(ImageGCPolicy{}, mockStatsProvider)
	fakeRuntime.ImageList = []container.Image{
		makeImage(0, 1024),
		makeImage(1, 2048),
	}
	fakeRuntime.AllPodList = []*containertest.FakePod{
		{Pod: &container.Pod{
			Containers: []*container.Container{
				makeContainer(1),
			},
		}},
	}

	_, err := manager.detectImages(ctx, zero)
	assert := assert.New(t)
	require.NoError(t, err)
	assert.Equal(2, manager.imageRecordsLen())

	// 新增一个镜像。
	fakeRuntime.ImageList = []container.Image{
		makeImage(0, 1024),
		makeImage(1, 1024),
		makeImage(2, 1024),
	}

	detectedTime := zero.Add(time.Second)
	startTime := time.Now().Add(-time.Millisecond)
	_, err = manager.detectImages(ctx, detectedTime)
	require.NoError(t, err)
	assert.Equal(3, manager.imageRecordsLen())
	noContainer, ok := manager.getImageRecord(imageID(0))
	require.True(t, ok)
	assert.Equal(zero, noContainer.firstDetected)
	assert.Equal(zero, noContainer.lastUsed)
	assert.Equal("", noContainer.runtimeHandlerUsedToPullImage)
	withContainer, ok := manager.getImageRecord(imageID(1))
	require.True(t, ok)
	assert.Equal(zero, withContainer.firstDetected)
	assert.True(withContainer.lastUsed.After(startTime))
	assert.Equal("", noContainer.runtimeHandlerUsedToPullImage)
	newContainer, ok := manager.getImageRecord(imageID(2))
	require.True(t, ok)
	assert.Equal(detectedTime, newContainer.firstDetected)
	assert.Equal(zero, noContainer.lastUsed)
	assert.Equal("", noContainer.runtimeHandlerUsedToPullImage)
}

// TestDeleteUnusedImagesExemptSandboxImage 验证标记为 pinned 的 sandbox 镜像不会被删除。
func TestDeleteUnusedImagesExemptSandboxImage(t *testing.T) {
	ctx := ktesting.Init(t)
	mockStatsProvider := statstest.NewMockProvider(t)

	manager, fakeRuntime := newRealImageGCManager(ImageGCPolicy{}, mockStatsProvider)
	fakeRuntime.ImageList = []container.Image{
		{
			ID:     sandboxImage,
			Size:   1024,
			Pinned: true,
		},
	}

	err := manager.DeleteUnusedImages(ctx)
	assert := assert.New(t)
	assert.Len(fakeRuntime.ImageList, 1)
	require.NoError(t, err)
}

// TestDeletePinnedImage 验证同一镜像的 pinned 与普通记录共存时只删除普通记录。
func TestDeletePinnedImage(t *testing.T) {
	ctx := ktesting.Init(t)
	mockStatsProvider := statstest.NewMockProvider(t)

	manager, fakeRuntime := newRealImageGCManager(ImageGCPolicy{}, mockStatsProvider)
	fakeRuntime.ImageList = []container.Image{
		{
			ID:     sandboxImage,
			Size:   1024,
			Pinned: true,
		},
		{
			ID:   sandboxImage,
			Size: 1024,
		},
	}

	err := manager.DeleteUnusedImages(ctx)
	assert := assert.New(t)
	assert.Len(fakeRuntime.ImageList, 1)
	require.NoError(t, err)
}

// TestDoNotDeletePinnedImage 验证回收空间时会跳过 pinned 镜像。
func TestDoNotDeletePinnedImage(t *testing.T) {
	ctx := ktesting.Init(t)
	mockStatsProvider := statstest.NewMockProvider(t)

	manager, fakeRuntime := newRealImageGCManager(ImageGCPolicy{}, mockStatsProvider)
	fakeRuntime.ImageList = []container.Image{
		{
			ID:     "1",
			Size:   1024,
			Pinned: true,
		},
		{
			ID:   "2",
			Size: 1024,
		},
	}

	assert := assert.New(t)
	getImagesAndFreeSpace(ctx, t, assert, manager, fakeRuntime, 4096, 1024, 1, time.Now())
}

// TestDeleteUnPinnedImage 验证非 pinned 镜像可以被回收。
func TestDeleteUnPinnedImage(t *testing.T) {
	ctx := ktesting.Init(t)
	mockStatsProvider := statstest.NewMockProvider(t)

	manager, fakeRuntime := newRealImageGCManager(ImageGCPolicy{}, mockStatsProvider)
	fakeRuntime.ImageList = []container.Image{
		{
			ID:     "1",
			Size:   1024,
			Pinned: false,
		},
		{
			ID:   "2",
			Size: 1024,
		},
	}

	assert := assert.New(t)
	getImagesAndFreeSpace(ctx, t, assert, manager, fakeRuntime, 2048, 2048, 0, time.Now())
}

// TestAllPinnedImages 验证所有镜像都 pinned 时无法回收任何空间。
func TestAllPinnedImages(t *testing.T) {
	ctx := ktesting.Init(t)
	mockStatsProvider := statstest.NewMockProvider(t)

	manager, fakeRuntime := newRealImageGCManager(ImageGCPolicy{}, mockStatsProvider)
	fakeRuntime.ImageList = []container.Image{
		{
			ID:     "1",
			Size:   1024,
			Pinned: true,
		},
		{
			ID:     "2",
			Size:   1024,
			Pinned: true,
		},
	}

	assert := assert.New(t)
	getImagesAndFreeSpace(ctx, t, assert, manager, fakeRuntime, 2048, 0, 2, time.Now())
}

// TestDetectImagesContainerStopped 验证容器停止后镜像的 lastUsed 不会被重置。
func TestDetectImagesContainerStopped(t *testing.T) {
	ctx := ktesting.Init(t)
	mockStatsProvider := statstest.NewMockProvider(t)

	manager, fakeRuntime := newRealImageGCManager(ImageGCPolicy{}, mockStatsProvider)
	fakeRuntime.ImageList = []container.Image{
		makeImage(0, 1024),
		makeImage(1, 2048),
	}
	fakeRuntime.AllPodList = []*containertest.FakePod{
		{Pod: &container.Pod{
			Containers: []*container.Container{
				makeContainer(1),
			},
		}},
	}

	_, err := manager.detectImages(ctx, zero)
	assert := assert.New(t)
	require.NoError(t, err)
	assert.Equal(2, manager.imageRecordsLen())
	withContainer, ok := manager.getImageRecord(imageID(1))
	require.True(t, ok)

	// 模拟容器已停止。
	fakeRuntime.AllPodList = []*containertest.FakePod{}
	_, err = manager.detectImages(ctx, time.Now())
	require.NoError(t, err)
	assert.Equal(2, manager.imageRecordsLen())
	container1, ok := manager.getImageRecord(imageID(0))
	require.True(t, ok)
	assert.Equal(zero, container1.firstDetected)
	assert.Equal(zero, container1.lastUsed)
	container2, ok := manager.getImageRecord(imageID(1))
	require.True(t, ok)
	assert.Equal(zero, container2.firstDetected)
	assert.True(container2.lastUsed.Equal(withContainer.lastUsed))
}

// TestDetectImagesWithRemovedImages 验证运行时中已不存在的镜像会从记录中移除。
func TestDetectImagesWithRemovedImages(t *testing.T) {
	ctx := ktesting.Init(t)
	mockStatsProvider := statstest.NewMockProvider(t)

	manager, fakeRuntime := newRealImageGCManager(ImageGCPolicy{}, mockStatsProvider)
	fakeRuntime.ImageList = []container.Image{
		makeImage(0, 1024),
		makeImage(1, 2048),
	}
	fakeRuntime.AllPodList = []*containertest.FakePod{
		{Pod: &container.Pod{
			Containers: []*container.Container{
				makeContainer(1),
			},
		}},
	}

	_, err := manager.detectImages(ctx, zero)
	assert := assert.New(t)
	require.NoError(t, err)
	assert.Equal(2, manager.imageRecordsLen())

	// 模拟两个镜像都被移除。
	fakeRuntime.ImageList = []container.Image{}
	_, err = manager.detectImages(ctx, time.Now())
	require.NoError(t, err)
	assert.Equal(0, manager.imageRecordsLen())
}

// TestFreeSpaceImagesInUseContainersAreIgnored 验证仍被容器使用的镜像不会被回收。
func TestFreeSpaceImagesInUseContainersAreIgnored(t *testing.T) {
	ctx := ktesting.Init(t)
	mockStatsProvider := statstest.NewMockProvider(t)

	manager, fakeRuntime := newRealImageGCManager(ImageGCPolicy{}, mockStatsProvider)
	fakeRuntime.ImageList = []container.Image{
		makeImage(0, 1024),
		makeImage(1, 2048),
	}
	fakeRuntime.AllPodList = []*containertest.FakePod{
		{Pod: &container.Pod{
			Containers: []*container.Container{
				makeContainer(1),
			},
		}},
	}

	assert := assert.New(t)
	getImagesAndFreeSpace(ctx, t, assert, manager, fakeRuntime, 2048, 1024, 1, time.Now())
}

// TestDeleteUnusedImagesRemoveAllUnusedImages 验证 DeleteUnusedImages 会删除所有未使用的镜像。
func TestDeleteUnusedImagesRemoveAllUnusedImages(t *testing.T) {
	ctx := ktesting.Init(t)
	mockStatsProvider := statstest.NewMockProvider(t)

	manager, fakeRuntime := newRealImageGCManager(ImageGCPolicy{}, mockStatsProvider)
	fakeRuntime.ImageList = []container.Image{
		makeImage(0, 1024),
		makeImage(1, 2048),
		makeImage(2, 2048),
	}
	fakeRuntime.AllPodList = []*containertest.FakePod{
		{Pod: &container.Pod{
			Containers: []*container.Container{
				makeContainer(2),
			},
		}},
	}

	err := manager.DeleteUnusedImages(ctx)
	assert := assert.New(t)
	require.NoError(t, err)
	assert.Len(fakeRuntime.ImageList, 1)
}

// TestDeleteUnusedImagesLimitByImageLiveTime 验证未达到最小存活时间的镜像不会被删除。
func TestDeleteUnusedImagesLimitByImageLiveTime(t *testing.T) {
	ktesting.Init(t).SyncTest("", testDeleteUnusedImagesLimitByImageLiveTime)
}

// testDeleteUnusedImagesLimitByImageLiveTime 是该测试的实际测试体。
func testDeleteUnusedImagesLimitByImageLiveTime(tCtx ktesting.TContext) {
	defer tCtx.Cancel("test completed")
	t := tCtx.TB()
	mockStatsProvider := statstest.NewMockProvider(t)

	manager, fakeRuntime := newRealImageGCManager(ImageGCPolicy{
		MinAge: time.Second * 3, // 将 minAge 设为 3 秒。
	}, mockStatsProvider)
	fakeRuntime.ImageList = []container.Image{
		makeImage(0, 1024),
		makeImage(1, 2048),
		makeImage(2, 2048),
	}
	fakeRuntime.AllPodList = []*containertest.FakePod{
		{Pod: &container.Pod{
			Containers: []*container.Container{
				makeContainer(2),
			},
		}},
	}
	// 开始探测镜像。
	manager.Start(tCtx)
	// 尝试删除镜像，但镜像还不够旧，因此不会删除。
	err := manager.DeleteUnusedImages(tCtx)
	assert := assert.New(t)
	require.NoError(t, err)
	assert.Len(fakeRuntime.ImageList, 3)
	// 等待 3 秒，使镜像达到可删除的年龄。
	time.Sleep(time.Second * 3)
	err = manager.DeleteUnusedImages(tCtx)
	require.NoError(t, err)
	assert.Len(fakeRuntime.ImageList, 1)
}

// TestFreeSpaceRemoveByLeastRecentlyUsed 验证按最近最少使用顺序回收镜像。
func TestFreeSpaceRemoveByLeastRecentlyUsed(t *testing.T) {
	ctx := ktesting.Init(t)
	mockStatsProvider := statstest.NewMockProvider(t)

	manager, fakeRuntime := newRealImageGCManager(ImageGCPolicy{}, mockStatsProvider)
	fakeRuntime.ImageList = []container.Image{
		makeImage(0, 1024),
		makeImage(1, 2048),
	}
	fakeRuntime.AllPodList = []*containertest.FakePod{
		{Pod: &container.Pod{
			Containers: []*container.Container{
				makeContainer(0),
				makeContainer(1),
			},
		}},
	}

	// 让镜像 1 比镜像 0 更近使用。
	_, err := manager.detectImages(ctx, zero)
	require.NoError(t, err)
	fakeRuntime.AllPodList = []*containertest.FakePod{
		{Pod: &container.Pod{
			Containers: []*container.Container{
				makeContainer(1),
			},
		}},
	}
	// detectImages 使用 time.Now() 更新镜像的 lastUsed 字段。
	// 在 Windows 上连续调用 time.Now() 可能返回相同时间戳，
	// 从而导致第二个镜像看起来并不比第一个更新。
	// 因此用 time.Sleep 让时间戳前进来避免该问题。
	if goruntime.GOOS == "windows" {
		time.Sleep(time.Millisecond)
	}
	_, err = manager.detectImages(ctx, time.Now())
	require.NoError(t, err)
	fakeRuntime.AllPodList = []*containertest.FakePod{
		{Pod: &container.Pod{
			Containers: []*container.Container{},
		}},
	}
	_, err = manager.detectImages(ctx, time.Now())
	require.NoError(t, err)
	require.Equal(t, 2, manager.imageRecordsLen())

	// 把删除时间设在一分钟后，使镜像首次检测时间与删除时间不同。
	assert := assert.New(t)
	getImagesAndFreeSpace(ctx, t, assert, manager, fakeRuntime, 1024, 1024, 1, time.Now().Add(time.Minute))
}

// TestFreeSpaceTiesBrokenByDetectedTime 验证 lastUsed 相同时用检测时间打破平局。
func TestFreeSpaceTiesBrokenByDetectedTime(t *testing.T) {
	ctx := ktesting.Init(t)
	mockStatsProvider := statstest.NewMockProvider(t)

	manager, fakeRuntime := newRealImageGCManager(ImageGCPolicy{}, mockStatsProvider)
	fakeRuntime.ImageList = []container.Image{
		makeImage(0, 1024),
	}
	fakeRuntime.AllPodList = []*containertest.FakePod{
		{Pod: &container.Pod{
			Containers: []*container.Container{
				makeContainer(0),
			},
		}},
	}

	// 让镜像 1 更晚被检测到，但与镜像 0 同时使用。
	_, err := manager.detectImages(ctx, zero)
	require.NoError(t, err)
	fakeRuntime.ImageList = []container.Image{
		makeImage(0, 1024),
		makeImage(1, 2048),
	}
	_, err = manager.detectImages(ctx, time.Now())
	require.NoError(t, err)
	fakeRuntime.AllPodList = []*containertest.FakePod{}
	_, err = manager.detectImages(ctx, time.Now())
	require.NoError(t, err)
	require.Equal(t, 2, manager.imageRecordsLen())

	assert := assert.New(t)
	getImagesAndFreeSpace(ctx, t, assert, manager, fakeRuntime, 1024, 2048, 1, time.Now())
}

// TestGarbageCollectBelowLowThreshold 验证磁盘使用率低于低水位时不触发回收。
func TestGarbageCollectBelowLowThreshold(t *testing.T) {
	ctx := ktesting.Init(t)
	policy := ImageGCPolicy{
		HighThresholdPercent: 90,
		LowThresholdPercent:  80,
	}
	mockStatsProvider := statstest.NewMockProvider(t)
	manager, _ := newRealImageGCManager(policy, mockStatsProvider)

	// 预期使用率 40%。
	imageStats := &statsapi.FsStats{
		AvailableBytes: ptr.To(uint64(600)),
		CapacityBytes:  ptr.To(uint64(1000)),
	}
	mockStatsProvider.EXPECT().ImageFsStats(mock.Anything).Return(imageStats, imageStats, nil)

	assert.NoError(t, manager.GarbageCollect(ctx, time.Now()))
}

// TestGarbageCollectCadvisorFailure 验证获取镜像文件系统统计失败时返回错误。
func TestGarbageCollectCadvisorFailure(t *testing.T) {
	ctx := ktesting.Init(t)
	policy := ImageGCPolicy{
		HighThresholdPercent: 90,
		LowThresholdPercent:  80,
	}
	mockStatsProvider := statstest.NewMockProvider(t)
	manager, _ := newRealImageGCManager(policy, mockStatsProvider)

	mockStatsProvider.EXPECT().ImageFsStats(mock.Anything).Return(&statsapi.FsStats{}, &statsapi.FsStats{}, fmt.Errorf("error"))
	require.Error(t, manager.GarbageCollect(ctx, time.Now()))
}

// TestGarbageCollectBelowSuccess 验证使用率超过高水位时会回收到低水位。
func TestGarbageCollectBelowSuccess(t *testing.T) {
	ctx := ktesting.Init(t)
	policy := ImageGCPolicy{
		HighThresholdPercent: 90,
		LowThresholdPercent:  80,
	}

	mockStatsProvider := statstest.NewMockProvider(t)
	manager, fakeRuntime := newRealImageGCManager(policy, mockStatsProvider)

	// 预期使用率 95%，且大部分空间可被释放。
	imageFs := &statsapi.FsStats{
		AvailableBytes: ptr.To(uint64(50)),
		CapacityBytes:  ptr.To(uint64(1000)),
	}
	mockStatsProvider.EXPECT().ImageFsStats(mock.Anything).Return(imageFs, imageFs, nil)
	fakeRuntime.ImageList = []container.Image{
		makeImage(0, 450),
	}

	assert.NoError(t, manager.GarbageCollect(ctx, time.Now()))
}

// TestGarbageCollectNotEnoughFreed 验证释放空间不足时返回错误并发送事件。
func TestGarbageCollectNotEnoughFreed(t *testing.T) {
	ctx := ktesting.Init(t)
	policy := ImageGCPolicy{
		HighThresholdPercent: 90,
		LowThresholdPercent:  80,
	}
	mockStatsProvider := statstest.NewMockProvider(t)
	fakeRuntime := &containertest.FakeRuntime{}
	recorder := record.NewFakeRecorder(1)
	manager := &realImageGCManager{
		runtime:       fakeRuntime,
		policy:        policy,
		imageRecords:  make(map[string]*imageRecord),
		statsProvider: mockStatsProvider,
		recorder:      recorder,
		tracer:        noopoteltrace.NewTracerProvider().Tracer(""),
	}

	// 初始状态：使用率 95%，其中 5% 是可回收的垃圾镜像。
	capacity := uint64(10 * 1024 * 1024 * 1024)
	available := capacity / 20 // 5% available, 95% usage
	imageFs := &statsapi.FsStats{
		AvailableBytes: ptr.To(available),
		CapacityBytes:  ptr.To(capacity),
	}
	mockStatsProvider.EXPECT().ImageFsStats(mock.Anything).Return(imageFs, imageFs, nil)
	// 该镜像未被使用，可以删除；但它的体积小于需要释放的空间。
	imageSize := int64(500 * 1024 * 1024) // 500 MiB
	fakeRuntime.ImageList = []container.Image{
		makeImage(0, imageSize),
	}

	err := manager.GarbageCollect(ctx, time.Now())
	require.Error(t, err)

	// 确认已发送警告事件。
	expectedEvent := "Warning FreeDiskSpaceFailed Insufficient free disk space on the node's image filesystem" +
		" (95% of 10.0 GiB used). Failed to free sufficient space by deleting unused images (freed 524288000 bytes)." +
		" Investigate disk usage, as it could be used by active images, logs, volumes, or other data."
	select {
	case event := <-recorder.Events:
		assert.Equal(t, expectedEvent, event)
	default:
		t.Errorf("expected a 'FreeDiskSpaceFailed' event")
	}
}

// TestGarbageCollectImageNotOldEnough 验证未达到 MinAge 的镜像不会被回收。
func TestGarbageCollectImageNotOldEnough(t *testing.T) {
	ctx := ktesting.Init(t)
	policy := ImageGCPolicy{
		HighThresholdPercent: 90,
		LowThresholdPercent:  80,
		MinAge:               time.Minute * 1,
	}
	fakeRuntime := &containertest.FakeRuntime{}
	mockStatsProvider := statstest.NewMockProvider(t)
	manager := &realImageGCManager{
		runtime:       fakeRuntime,
		policy:        policy,
		imageRecords:  make(map[string]*imageRecord),
		statsProvider: mockStatsProvider,
		recorder:      &record.FakeRecorder{},
	}

	fakeRuntime.ImageList = []container.Image{
		makeImage(0, 1024),
		makeImage(1, 2048),
	}
	// 一个镜像正在使用，另一个还不够旧
	fakeRuntime.AllPodList = []*containertest.FakePod{
		{Pod: &container.Pod{
			Containers: []*container.Container{
				makeContainer(1),
			},
		}},
	}

	fakeClock := testingclock.NewFakeClock(time.Now())
	t.Log(fakeClock.Now())
	_, err := manager.detectImages(ctx, fakeClock.Now())
	require.NoError(t, err)
	require.Equal(t, 2, manager.imageRecordsLen())
	// 一个镜像在使用、另一个不够旧，因此没有释放空间。
	assert := assert.New(t)
	getImagesAndFreeSpace(ctx, t, assert, manager, fakeRuntime, 1024, 0, 2, fakeClock.Now())

	// 时钟前进 minAge，随后应有 1 个镜像被回收。
	fakeClock.Step(policy.MinAge)
	getImagesAndFreeSpace(ctx, t, assert, manager, fakeRuntime, 1024, 1024, 1, fakeClock.Now())
}

// getImagesAndFreeSpace 按淘汰顺序计算并执行空间释放，同时断言释放量与剩余镜像数。
func getImagesAndFreeSpace(ctx context.Context, t *testing.T, assert *assert.Assertions, im *realImageGCManager, fakeRuntime *containertest.FakeRuntime, spaceToFree, expectedSpaceFreed int64, imagesLen int, freeTime time.Time) {
	images, err := im.imagesInEvictionOrder(ctx, freeTime)
	require.NoError(t, err)
	_, spaceFreed, err := im.freeSpace(ctx, spaceToFree, freeTime, images)
	require.NoError(t, err)
	assert.EqualValues(expectedSpaceFreed, spaceFreed)
	assert.Len(fakeRuntime.ImageList, imagesLen)
}

// TestGarbageCollectImageTooOld 验证超过 MaxAge 的镜像会被强制回收。
func TestGarbageCollectImageTooOld(t *testing.T) {
	ctx := ktesting.Init(t)
	policy := ImageGCPolicy{
		HighThresholdPercent: 90,
		LowThresholdPercent:  80,
		MinAge:               0,
		MaxAge:               time.Minute * 1,
	}
	fakeRuntime := &containertest.FakeRuntime{}
	mockStatsProvider := statstest.NewMockProvider(t)
	manager := &realImageGCManager{
		runtime:       fakeRuntime,
		policy:        policy,
		imageRecords:  make(map[string]*imageRecord),
		statsProvider: mockStatsProvider,
		recorder:      &record.FakeRecorder{},
	}

	fakeRuntime.ImageList = []container.Image{
		makeImage(0, 1024),
		makeImage(1, 2048),
	}
	// 一个镜像正在使用，另一个还不够旧
	fakeRuntime.AllPodList = []*containertest.FakePod{
		{Pod: &container.Pod{
			Containers: []*container.Container{
				makeContainer(1),
			},
		}},
	}

	fakeClock := testingclock.NewFakeClock(time.Now())
	t.Log(fakeClock.Now())
	images, err := manager.imagesInEvictionOrder(ctx, fakeClock.Now())
	require.NoError(t, err)
	require.Len(t, images, 1)
	// 模拟 Pod 刚使用过该镜像，但 Pod 已被回收。
	images[0].lastUsed = fakeClock.Now()

	// 第一轮 GC 不应回收剩余镜像，因为它最近才被使用。
	assert := assert.New(t)
	oldStartTime := fakeClock.Now()
	images, err = manager.freeOldImages(ctx, images, oldStartTime, oldStartTime)
	require.NoError(t, err)
	assert.Len(images, 1)
	assert.Len(fakeRuntime.ImageList, 2)

	// 时钟前进到超过 maxAge 后，应有 1 个镜像被回收。
	fakeClock.Step(policy.MaxAge + 1)
	images, err = manager.freeOldImages(ctx, images, fakeClock.Now(), oldStartTime)
	require.NoError(t, err)
	assert.Empty(images)
	assert.Len(fakeRuntime.ImageList, 1)
}

// TestGarbageCollectImageMaxAgeDisabled 验证 MaxAge 为 0 时禁用按最大存活时间回收。
func TestGarbageCollectImageMaxAgeDisabled(t *testing.T) {
	ctx := ktesting.Init(t)
	policy := ImageGCPolicy{
		HighThresholdPercent: 90,
		LowThresholdPercent:  80,
		MinAge:               0,
		MaxAge:               0,
	}
	fakeRuntime := &containertest.FakeRuntime{}
	mockStatsProvider := statstest.NewMockProvider(t)
	manager := &realImageGCManager{
		runtime:       fakeRuntime,
		policy:        policy,
		imageRecords:  make(map[string]*imageRecord),
		statsProvider: mockStatsProvider,
		recorder:      &record.FakeRecorder{},
	}

	assert := assert.New(t)
	fakeRuntime.ImageList = []container.Image{
		makeImage(0, 1024),
		makeImage(1, 2048),
	}
	assert.Len(fakeRuntime.ImageList, 2)
	// 一个镜像正在使用，另一个还不够旧
	fakeRuntime.AllPodList = []*containertest.FakePod{
		{Pod: &container.Pod{
			Containers: []*container.Container{
				makeContainer(1),
			},
		}},
	}

	fakeClock := testingclock.NewFakeClock(time.Now())
	t.Log(fakeClock.Now())
	images, err := manager.imagesInEvictionOrder(ctx, fakeClock.Now())
	require.NoError(t, err)
	require.Len(t, images, 1)
	assert.Len(fakeRuntime.ImageList, 2)

	oldStartTime := fakeClock.Now()

	// 第一轮 GC 不应回收剩余镜像，因为它最近才被使用。
	images, err = manager.freeOldImages(ctx, images, oldStartTime, oldStartTime)
	require.NoError(t, err)
	assert.Len(images, 1)
	assert.Len(fakeRuntime.ImageList, 2)

	// 大幅推进时钟后，镜像仍不应被回收。
	// 参见 https://stackoverflow.com/questions/25065055/what-is-the-maximum-time-time-in-go
	fakeClock.SetTime(time.Unix(1<<63-62135596801, 999999999))
	images, err = manager.freeOldImages(ctx, images, fakeClock.Now(), oldStartTime)
	require.NoError(t, err)
	assert.Len(images, 1)
	assert.Len(fakeRuntime.ImageList, 2)
}

// TestValidateImageGCPolicy 验证镜像 GC 策略的阈值范围以及高低水位关系。
func TestValidateImageGCPolicy(t *testing.T) {
	testCases := []struct {
		name          string
		imageGCPolicy ImageGCPolicy
		expectErr     string
	}{
		{
			name: "Test for LowThresholdPercent < HighThresholdPercent",
			imageGCPolicy: ImageGCPolicy{
				HighThresholdPercent: 2,
				LowThresholdPercent:  1,
			},
		},
		{
			name: "Test for HighThresholdPercent < 0,",
			imageGCPolicy: ImageGCPolicy{
				HighThresholdPercent: -1,
			},
			expectErr: "invalid HighThresholdPercent -1, must be in range [0-100]",
		},
		{
			name: "Test for HighThresholdPercent > 100",
			imageGCPolicy: ImageGCPolicy{
				HighThresholdPercent: 101,
			},
			expectErr: "invalid HighThresholdPercent 101, must be in range [0-100]",
		},
		{
			name: "Test for LowThresholdPercent < 0",
			imageGCPolicy: ImageGCPolicy{
				LowThresholdPercent: -1,
			},
			expectErr: "invalid LowThresholdPercent -1, must be in range [0-100]",
		},
		{
			name: "Test for LowThresholdPercent > 100",
			imageGCPolicy: ImageGCPolicy{
				LowThresholdPercent: 101,
			},
			expectErr: "invalid LowThresholdPercent 101, must be in range [0-100]",
		},
		{
			name: "Test for LowThresholdPercent > HighThresholdPercent",
			imageGCPolicy: ImageGCPolicy{
				HighThresholdPercent: 1,
				LowThresholdPercent:  2,
			},
			expectErr: "LowThresholdPercent 2 can not be higher than HighThresholdPercent 1",
		},
	}

	for _, tc := range testCases {
		if _, err := NewImageGCManager(nil, nil, nil, nil, nil, tc.imageGCPolicy, noopoteltrace.NewTracerProvider()); err != nil {
			if err.Error() != tc.expectErr {
				t.Errorf("[%s:]Expected err:%v, but got:%v", tc.name, tc.expectErr, err.Error())
			}
		}
	}
}
