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
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/kubernetes/pkg/kubelet/container"
	"k8s.io/kubernetes/test/utils/ktesting"

	runtimeapi "k8s.io/cri-api/pkg/apis/runtime/v1"
	critest "k8s.io/cri-api/pkg/apis/testing"
	testingclock "k8s.io/utils/clock/testing"
)

// TestGetAllLogs 验证 GetAllLogs 能列出主日志、轮转日志与压缩日志，
// 并排除未轮转的孤立日志和临时日志。
func TestGetAllLogs(t *testing.T) {
	dir, err := os.MkdirTemp("", "test-get-all-logs")
	require.NoError(t, err)
	defer os.RemoveAll(dir)
	testLogs := []string{
		"test-log.11111111-111111.gz",
		"test-log",
		"test-log.00000000-000000.gz",
		"test-log.19900322-000000.gz",
		"test-log.19900322-111111.gz",
		"test-log.19880620-000000", // unused log
		"test-log.19880620-000000.gz",
		"test-log.19880620-111111.gz",
		"test-log.20180101-000000",
		"test-log.20180101-000000.tmp", // temporary log
	}
	expectLogs := []string{
		"test-log.00000000-000000.gz",
		"test-log.11111111-111111.gz",
		"test-log.19880620-000000.gz",
		"test-log.19880620-111111.gz",
		"test-log.19900322-000000.gz",
		"test-log.19900322-111111.gz",
		"test-log.20180101-000000",
		"test-log",
	}
	// 创建全部测试文件，覆盖轮转、压缩、孤立与临时文件等形态。
	for i := range testLogs {
		f, err := os.Create(filepath.Join(dir, testLogs[i]))
		require.NoError(t, err)
		f.Close()
	}
	// 以基础日志名查询，期望返回按文件名排序的完整日志列表。
	got, err := GetAllLogs(filepath.Join(dir, "test-log"))
	assert.NoError(t, err)
	for i := range expectLogs {
		expectLogs[i] = filepath.Join(dir, expectLogs[i])
	}
	assert.Equal(t, expectLogs, got)
}

// TestRotateLogs 验证按大小与文件数策略轮转日志的完整流程：
// 运行中的容器按需轮转，已退出的容器只清理超额日志，压缩文件保持不变。
func TestRotateLogs(t *testing.T) {
	tCtx := ktesting.Init(t)
	dir, err := os.MkdirTemp("", "test-rotate-logs")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	const (
		testMaxFiles = 3
		testMaxSize  = 10
	)
	// 使用假时钟与假运行时服务，使轮转时间戳与 ReopenContainerLog 调用可控。
	now := time.Now()
	f := critest.NewFakeRuntimeService()
	c := &containerLogManager{
		runtimeService: f,
		policy: LogRotatePolicy{
			MaxSize:  testMaxSize,
			MaxFiles: testMaxFiles,
		},
		osInterface: container.RealOS{},
		clock:       testingclock.NewFakeClock(now),
		mutex:       sync.Mutex{},
		queue: workqueue.NewTypedRateLimitingQueueWithConfig(
			workqueue.DefaultTypedControllerRateLimiter[string](),
			workqueue.TypedRateLimitingQueueConfig[string]{Name: "kubelet_log_rotate_manager"},
		),
		maxWorkers:       10,
		monitoringPeriod: v1.Duration{Duration: 10 * time.Second},
	}
	testLogs := []string{
		"test-log-1",
		"test-log-2",
		"test-log-3",
		"test-log-4",
		"test-log-3.00000000-000001",
		"test-log-3.00000000-000000.gz",
	}
	testContent := []string{
		"short",
		"longer than 10 bytes",
		"longer than 10 bytes",
		"longer than 10 bytes",
		"the length doesn't matter",
		"the length doesn't matter",
	}
	// 写入不同长度的内容，以便触发按大小轮转。
	for i := range testLogs {
		f, err := os.Create(filepath.Join(dir, testLogs[i]))
		require.NoError(t, err)
		_, err = f.Write([]byte(testContent[i]))
		require.NoError(t, err)
		f.Close()
	}
	testContainers := []*critest.FakeContainer{
		{
			ContainerStatus: runtimeapi.ContainerStatus{
				Id:      "container-not-need-rotate",
				State:   runtimeapi.ContainerState_CONTAINER_RUNNING,
				LogPath: filepath.Join(dir, testLogs[0]),
			},
		},
		{
			ContainerStatus: runtimeapi.ContainerStatus{
				Id:      "container-need-rotate",
				State:   runtimeapi.ContainerState_CONTAINER_RUNNING,
				LogPath: filepath.Join(dir, testLogs[1]),
			},
		},
		{
			ContainerStatus: runtimeapi.ContainerStatus{
				Id:      "container-has-excess-log",
				State:   runtimeapi.ContainerState_CONTAINER_RUNNING,
				LogPath: filepath.Join(dir, testLogs[2]),
			},
		},
		{
			ContainerStatus: runtimeapi.ContainerStatus{
				Id:      "container-is-not-running",
				State:   runtimeapi.ContainerState_CONTAINER_EXITED,
				LogPath: filepath.Join(dir, testLogs[3]),
			},
		},
	}
	// 注册假容器，模拟运行中与已退出两类容器。
	f.SetFakeContainers(testContainers)

	// 在启动 worker 之前先把待处理项放入队列，避免 worker 启动时队列为空而直接退出。
	require.NoError(t, c.rotateLogs(tCtx))

	// 启动一个协程轮询队列：队列清空后关闭队列，使阻塞的 processQueueItems 返回。
	// 轮询间隔取较小值，尽量减少测试中的额外等待。
	go func() {
		pollTimeoutCtx, cancel := context.WithTimeout(tCtx, 10*time.Second)
		defer cancel()
		err = wait.PollUntilContextCancel(pollTimeoutCtx, 5*time.Millisecond, false, func(ctx context.Context) (done bool, err error) {
			return c.queue.Len() == 0, nil
		})
		if err != nil {
			t.Errorf("unexpected error %v", err)
		}
		c.queue.ShutDown()
	}()
	// 该调用会阻塞，依赖上面的协程在队列清空后关闭队列来触发返回。
	c.processQueueItems(tCtx, 1)

	// 校验轮转结果：目录中应恰好保留 5 个文件，名称与预期一一对应。
	timestamp := now.Format(timestampFormat)
	logs, err := os.ReadDir(dir)
	require.NoError(t, err)
	assert.Len(t, logs, 5)
	assert.Equal(t, testLogs[0], logs[0].Name())
	assert.Equal(t, testLogs[1]+"."+timestamp, logs[1].Name())
	assert.Equal(t, testLogs[4]+compressSuffix, logs[2].Name())
	assert.Equal(t, testLogs[2]+"."+timestamp, logs[3].Name())
	assert.Equal(t, testLogs[3], logs[4].Name())
}

// TestClean 验证 Clean 会删除指定容器对应的主日志及其所有轮转日志。
func TestClean(t *testing.T) {
	tCtx := ktesting.Init(t)
	dir, err := os.MkdirTemp("", "test-clean")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	const (
		testMaxFiles = 3
		testMaxSize  = 10
	)
	// 使用假时钟与假运行时服务，使轮转时间戳与 ReopenContainerLog 调用可控。
	now := time.Now()
	f := critest.NewFakeRuntimeService()
	c := &containerLogManager{
		runtimeService: f,
		policy: LogRotatePolicy{
			MaxSize:  testMaxSize,
			MaxFiles: testMaxFiles,
		},
		osInterface: container.RealOS{},
		clock:       testingclock.NewFakeClock(now),
		mutex:       sync.Mutex{},
		queue: workqueue.NewTypedRateLimitingQueueWithConfig(
			workqueue.DefaultTypedControllerRateLimiter[string](),
			workqueue.TypedRateLimitingQueueConfig[string]{Name: "kubelet_log_rotate_manager"},
		),
		maxWorkers:       10,
		monitoringPeriod: v1.Duration{Duration: 10 * time.Second},
	}
	testLogs := []string{
		"test-log-1",
		"test-log-2",
		"test-log-3",
		"test-log-2.00000000-000000.gz",
		"test-log-2.00000000-000001",
		"test-log-3.00000000-000000.gz",
		"test-log-3.00000000-000001",
	}
	for i := range testLogs {
		f, err := os.Create(filepath.Join(dir, testLogs[i]))
		require.NoError(t, err)
		f.Close()
	}
	testContainers := []*critest.FakeContainer{
		{
			ContainerStatus: runtimeapi.ContainerStatus{
				Id:      "container-1",
				State:   runtimeapi.ContainerState_CONTAINER_RUNNING,
				LogPath: filepath.Join(dir, testLogs[0]),
			},
		},
		{
			ContainerStatus: runtimeapi.ContainerStatus{
				Id:      "container-2",
				State:   runtimeapi.ContainerState_CONTAINER_RUNNING,
				LogPath: filepath.Join(dir, testLogs[1]),
			},
		},
		{
			ContainerStatus: runtimeapi.ContainerStatus{
				Id:      "container-3",
				State:   runtimeapi.ContainerState_CONTAINER_EXITED,
				LogPath: filepath.Join(dir, testLogs[2]),
			},
		},
	}
	f.SetFakeContainers(testContainers)

	// container-3 已退出，清理后其主日志与轮转日志都应被删除。
	err = c.Clean(tCtx, "container-3")
	require.NoError(t, err)

	logs, err := os.ReadDir(dir)
	require.NoError(t, err)
	assert.Len(t, logs, 4)
	assert.Equal(t, testLogs[0], logs[0].Name())
	assert.Equal(t, testLogs[1], logs[1].Name())
	assert.Equal(t, testLogs[3], logs[2].Name())
	assert.Equal(t, testLogs[4], logs[3].Name())
}

// TestCleanupUnusedLog 验证 cleanupUnusedLogs 会删除未在候选列表中引用的日志，
// 同时保留正在使用的日志与压缩日志，并返回保留下来的日志列表。
func TestCleanupUnusedLog(t *testing.T) {
	dir, err := os.MkdirTemp("", "test-cleanup-unused-log")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	testLogs := []string{
		"test-log-1",     // regular log
		"test-log-1.tmp", // temporary log
		"test-log-2",     // unused log
		"test-log-2.gz",  // compressed log
	}

	for i := range testLogs {
		testLogs[i] = filepath.Join(dir, testLogs[i])
		f, err := os.Create(testLogs[i])
		require.NoError(t, err)
		f.Close()
	}

	// 仅依赖真实文件系统操作，无需运行时服务或策略配置。
	c := &containerLogManager{
		osInterface: container.RealOS{},
	}
	got, err := c.cleanupUnusedLogs(testLogs)
	require.NoError(t, err)
	assert.Len(t, got, 2)
	assert.Equal(t, []string{testLogs[0], testLogs[3]}, got)

	logs, err := os.ReadDir(dir)
	require.NoError(t, err)
	assert.Len(t, logs, 2)
	assert.Equal(t, testLogs[0], filepath.Join(dir, logs[0].Name()))
	assert.Equal(t, testLogs[3], filepath.Join(dir, logs[1].Name()))
}

// TestRemoveExcessLog 验证 removeExcessLogs 按 MaxFiles 上限删除多余的轮转日志，
// 覆盖上限等于、大于文件数等边界情况。
func TestRemoveExcessLog(t *testing.T) {
	for desc, test := range map[string]struct {
		max    int
		expect []string
	}{
		"MaxFiles equal to 2": {
			max:    2,
			expect: []string{},
		},
		"MaxFiles more than 2": {
			max:    3,
			expect: []string{"test-log-4"},
		},
		"MaxFiles more than log file number": {
			max:    6,
			expect: []string{"test-log-1", "test-log-2", "test-log-3", "test-log-4"},
		},
	} {
		t.Logf("TestCase %q", desc)
		dir, err := os.MkdirTemp("", "test-remove-excess-log")
		require.NoError(t, err)
		defer os.RemoveAll(dir)

		// 故意打乱创建顺序，验证删除逻辑依赖轮转序号而非创建顺序。
		testLogs := []string{"test-log-3", "test-log-1", "test-log-2", "test-log-4"}

		for i := range testLogs {
			testLogs[i] = filepath.Join(dir, testLogs[i])
			f, err := os.Create(testLogs[i])
			require.NoError(t, err)
			f.Close()
		}

		c := &containerLogManager{
			policy:      LogRotatePolicy{MaxFiles: test.max},
			osInterface: container.RealOS{},
		}
		got, err := c.removeExcessLogs(testLogs)
		require.NoError(t, err)
		require.Len(t, got, len(test.expect))
		for i, name := range test.expect {
			assert.Equal(t, name, filepath.Base(got[i]))
		}

		logs, err := os.ReadDir(dir)
		require.NoError(t, err)
		require.Len(t, logs, len(test.expect))
		for i, name := range test.expect {
			assert.Equal(t, name, logs[i].Name())
		}
	}
}

// TestCompressLog 验证 compressLog 能正确压缩日志文件、保留文件权限模式，
// 并清理原始文件与临时文件；对不存在的日志应返回错误。
func TestCompressLog(t *testing.T) {
	dir, err := os.MkdirTemp("", "test-compress-log")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	// 创建待压缩的测试日志并写入内容。
	testFile, err := os.CreateTemp(dir, "test-rotate-latest-log")
	require.NoError(t, err)
	defer testFile.Close()
	testContent := "test log content"
	_, err = testFile.Write([]byte(testContent))
	require.NoError(t, err)
	testFile.Close()

	testLog := testFile.Name()
	testLogInfo, err := os.Stat(testLog)
	assert.NoError(t, err)
	c := &containerLogManager{osInterface: container.RealOS{}}
	// 压缩后应生成 .gz 文件，且文件模式与原文件一致。
	require.NoError(t, c.compressLog(testLog))
	testLogCompressInfo, err := os.Stat(testLog + compressSuffix)
	assert.NoError(t, err, "log should be compressed")
	if testLogInfo.Mode() != testLogCompressInfo.Mode() {
		t.Errorf("compressed and uncompressed test log file modes do not match")
	}
	if err := c.compressLog("test-unknown-log"); err == nil {
		t.Errorf("compressing unknown log should return error")
	}
	_, err = os.Stat(testLog + tmpSuffix)
	assert.Error(t, err, "temporary log should be renamed")
	_, err = os.Stat(testLog)
	assert.Error(t, err, "original log should be removed")
}

// TestRotateLatestLog 验证 rotateLatestLog 的正常轮转，以及 ReopenContainerLog
// 失败时回滚恢复原始日志的行为。
func TestRotateLatestLog(t *testing.T) {
	tCtx := ktesting.Init(t)
	dir, err := os.MkdirTemp("", "test-rotate-latest-log")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	for desc, test := range map[string]struct {
		runtimeError   error
		maxFiles       int
		expectError    bool
		expectOriginal bool
		expectRotated  bool
	}{
		"should successfully rotate log when MaxFiles is 2": {
			maxFiles:       2,
			expectError:    false,
			expectOriginal: false,
			expectRotated:  true,
		},
		"should restore original log when ReopenContainerLog fails": {
			runtimeError:   fmt.Errorf("random error"),
			maxFiles:       2,
			expectError:    true,
			expectOriginal: true,
			expectRotated:  false,
		},
	} {
		t.Logf("TestCase %q", desc)
		// 每个用例使用独立的假时钟与假运行时服务，避免用例间状态串扰。
		now := time.Now()
		f := critest.NewFakeRuntimeService()
		c := &containerLogManager{
			runtimeService: f,
			policy:         LogRotatePolicy{MaxFiles: test.maxFiles},
			osInterface:    container.RealOS{},
			clock:          testingclock.NewFakeClock(now),
			mutex:          sync.Mutex{},
			queue: workqueue.NewTypedRateLimitingQueueWithConfig(
				workqueue.DefaultTypedControllerRateLimiter[string](),
				workqueue.TypedRateLimitingQueueConfig[string]{Name: "kubelet_log_rotate_manager"},
			),
			maxWorkers:       10,
			monitoringPeriod: v1.Duration{Duration: 10 * time.Second},
		}
		// 注入运行时错误，模拟 ReopenContainerLog 调用失败。
		if test.runtimeError != nil {
			f.InjectError("ReopenContainerLog", test.runtimeError)
		}
		testFile, err := os.CreateTemp(dir, "test-rotate-latest-log")
		require.NoError(t, err)
		testFile.Close()
		defer testFile.Close()
		testLog := testFile.Name()
		rotatedLog := fmt.Sprintf("%s.%s", testLog, now.Format(timestampFormat))
		// 校验轮转后原始文件与轮转文件的存在性是否符合预期。
		err = c.rotateLatestLog(tCtx, "test-id", testLog)
		assert.Equal(t, test.expectError, err != nil)
		_, err = os.Stat(testLog)
		assert.Equal(t, test.expectOriginal, err == nil)
		_, err = os.Stat(rotatedLog)
		assert.Equal(t, test.expectRotated, err == nil)
		assert.NoError(t, f.AssertCalls([]string{"ReopenContainerLog"}))
	}
}
