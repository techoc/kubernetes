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

package pullmanager

import (
	"fmt"
	"runtime"
	"sync"
	"testing"

	"github.com/google/uuid"
	"k8s.io/apimachinery/pkg/util/rand"
	kubeletconfig "k8s.io/kubernetes/pkg/kubelet/apis/config"
	"k8s.io/kubernetes/test/utils/ktesting"
)

// namedAccessor 把一个可读名称与 accessor 构造函数绑定，便于 benchmark 子项命名。
type namedAccessor struct {
	name         string
	accessorInit func(b testing.TB) PullRecordsAccessor
}

// recordAccessorBenchmark 描述一次读基准：accessor、缓存记录数、并发倍数和是否命中。
type recordAccessorBenchmark struct {
	namedInit             namedAccessor
	recordsInCache        int
	concurrencyMultiplier int
	cacheHit              bool
}

// benchmarkedCheckFunc 是基准循环中执行的具体操作，并校验结果符合预期。
type benchmarkedCheckFunc func(b *testing.B, pullManager PullManager, imgRef string)

// BenchmarkPullRecordsAccessorsCacheHit 测量缓存命中时读取 pulled record 的吞吐。
func BenchmarkPullRecordsAccessorsCacheHit(b *testing.B) {
	const expectHit = true
	benchmarkAllPullAccessorsRead(b, expectHit, directRecordReadFunc(expectHit))
}

// BenchmarkPullRecordsAccessorCacheMiss 测量缓存未命中时的读取开销。
func BenchmarkPullRecordsAccessorCacheMiss(b *testing.B) {
	const expectHit = false
	benchmarkAllPullAccessorsRead(b, expectHit, directRecordReadFunc(expectHit))
}

// BenchmarkPullManagerMustAttemptImagePullFalse 测量无需重新认证时的 MustAttemptImagePull 开销。
func BenchmarkPullManagerMustAttemptImagePullFalse(b *testing.B) {
	const expectHit = true
	benchmarkAllPullAccessorsRead(b, expectHit, mustAttemptPullReadFunc(expectHit))
}

// BenchmarkPullManagerMustAttemptImagePullTrue 测量需要重新认证时的 MustAttemptImagePull 开销。
func BenchmarkPullManagerMustAttemptImagePullTrue(b *testing.B) {
	const expectHit = false
	benchmarkAllPullAccessorsRead(b, expectHit, mustAttemptPullReadFunc(expectHit))
}

// directRecordReadFunc 直接调用 recordsAccessor 读取记录，校验命中状态与错误。
func directRecordReadFunc(expectHit bool) benchmarkedCheckFunc {
	return func(b *testing.B, pullManager PullManager, imgRef string) {
		_, exists, err := pullManager.recordsAccessor.GetImagePulledRecord(imgRef)
		if exists != expectHit || err != nil {
			b.Fatalf("no error expected (got %v); expected record to exist %t, got %t)", err, expectHit, exists)
		}
	}
}

// mustAttemptPullReadFunc 通过 MustAttemptImagePull 间接读取记录，覆盖凭据判定路径。
func mustAttemptPullReadFunc(expectHit bool) benchmarkedCheckFunc {
	return func(b *testing.B, pullManager PullManager, imgRef string) {
		tCtx := ktesting.Init(b)
		mustPull, err := pullManager.MustAttemptImagePull(tCtx, "test.repo/org/"+imgRef, imgRef, func() ([]kubeletconfig.ImagePullSecret, *kubeletconfig.ImagePullServiceAccount, error) {
			return nil, nil, nil
		})
		if mustPull != !expectHit || err != nil {
			b.Fatalf("no error expected (got %v); MustAttemptImagePull() expected %t, got %t", err, !expectHit, mustPull)
		}
	}
}

// BenchmarkPullManagerWriteImagePullIntent 测量写入 ImagePullIntent 的开销，
// 这是最接近直接写盘的方法。不能绕过 PullManager 直接写：
// FSAccessor 非线程安全，并行测试会出错。
func BenchmarkPullManagerWriteImagePullIntent(b *testing.B) {
	logger, _ := ktesting.NewTestContext(b)
	benchmarkAllPullAccessorsWrite(b, func(b *testing.B, pullManager PullManager, imgRef string) {
		if err := pullManager.RecordPullIntent(logger, "test.repo/org/"+imgRef); err != nil {
			b.Fatalf("failed to write a record: %v", err)
		}
	})
}

// BenchmarkPullManagerWriteIfNotChanged 测量内容未变化时跳过写入的路径。
func BenchmarkPullManagerWriteIfNotChanged(b *testing.B) {
	benchmarkAllPullAccessorsWrite(b, func(b *testing.B, pullManager PullManager, imgRef string) {
		tCtx := ktesting.Init(b)
		if err := pullManager.writePulledRecordIfChanged(
			tCtx,
			"test.repo/org/"+imgRef,
			imgRef,
			&kubeletconfig.ImagePullCredentials{NodePodsAccessible: true},
		); err != nil {
			b.Fatalf("failed to write a record: %v", err)
		}
	})
}

// benchmarkAllPullAccessorsWrite 对文件系统 accessor 和内存缓存 accessor，
// 在多种并发倍数下运行写入基准。
func benchmarkAllPullAccessorsWrite(b *testing.B, writerRunner benchmarkedCheckFunc) {
	imageRequests := make([]string, 0, generatedTestRequestsNum)
	for range generatedTestRequestsNum {
		imageRequests = append(imageRequests, uuid.NewString())
	}

	createFSAccessor := namedAccessor{"FileSystemAccessor", setupFSRecordsAccessor}
	// 0 表示串行执行，其余值作为 b.SetParallelism 的倍数。
	const cacheSize = 500
	createMemCachedAccessor := namedAccessor{
		"InMemoryCache",
		func(b testing.TB) PullRecordsAccessor {
			return setupInMemRecordsAccessor(b, cacheSize, false)
		},
	}

	concurrencyMultipliers := []int{0, 1, 2, 8, 16}
	for _, namedInit := range []namedAccessor{
		createFSAccessor,
		createMemCachedAccessor,
	} {
		for _, concurrency := range concurrencyMultipliers {
			b.Run(fmt.Sprintf("Type=%s/ConcurrencyPerCPU=%d", namedInit.name, concurrency), func(b *testing.B) {
				// accessor 必须在叶子 b.Run() 内构造：/tmp 下会有大量文件写入，
				// 需要在下一个基准开始前完成临时目录清理。
				pullManger := PullManager{
					recordsAccessor:     namedInit.accessorInit(b),
					imagePolicyEnforcer: AlwaysVerifyImagePullPolicy(),
					imageService:        nil, // only needed during init, we're skipping that here
					intentAccessors:     NewStripedLockSet(int32(runtime.NumCPU())),
					intentCounters:      &sync.Map{},
					pulledAccessors:     NewStripedLockSet(int32(runtime.NumCPU())),
				}
				runBenchmark(b, pullManger, imageRequests, concurrency, writerRunner)
			})
		}
	}

}

// benchmarkAllPullAccessorsRead 组合 accessor 类型、记录数和并发倍数运行读取基准。
func benchmarkAllPullAccessorsRead(b *testing.B, hit bool, readerRunner benchmarkedCheckFunc) {
	// 对比文件系统缓存与“内存缓存为权威源”两种模式。
	recordsKepts := []int{50, 100, 200, 500, 2000}
	concurrencyMultipliers := []int{0, 1, 2, 8, 16}

	createFSAccessor := func(_ int) namedAccessor {
		return namedAccessor{"FileSystemAccessor", setupFSRecordsAccessor}
	}
	createMemCachedAuthoritativeAccessor := func(cacheSize int) namedAccessor {
		return namedAccessor{
			"InMemoryCache-Authoritative",
			func(b testing.TB) PullRecordsAccessor {
				return setupInMemRecordsAccessor(b, cacheSize, true)
			},
		}
	}

	for _, accessorInit := range []func(int) namedAccessor{
		createFSAccessor,
		createMemCachedAuthoritativeAccessor,
	} {
		for _, concurrency := range concurrencyMultipliers {
			for _, recordsKept := range recordsKepts {
				benchmarkPullAccessorCacheRead(b,
					recordAccessorBenchmark{
						namedInit:             accessorInit(recordsKept),
						recordsInCache:        recordsKept,
						concurrencyMultiplier: concurrency,
						cacheHit:              hit,
					},
					readerRunner,
				)
			}
		}
	}

	// 测量内存缓存性能随 总记录数/缓存容量 比例的变化。
	const nonAuthoritativeRecordsNumber = 2_000
	for _, concurrency := range concurrencyMultipliers {
		for _, cacheSize := range []int{2000, 1000, 500, 250, 125} {
			benchmarkPullAccessorCacheRead(b,
				recordAccessorBenchmark{
					namedInit: namedAccessor{
						fmt.Sprintf("InMemoryCache-NonAuthoritative/Records-CacheSize-Ratio=%d", nonAuthoritativeRecordsNumber/cacheSize),
						func(b testing.TB) PullRecordsAccessor { return setupInMemRecordsAccessor(b, cacheSize, false) },
					},
					recordsInCache:        nonAuthoritativeRecordsNumber,
					concurrencyMultiplier: concurrency,
					cacheHit:              hit,
				},
				readerRunner,
			)
		}
	}
}

const (
	generatedTestRequestsNum = 4_000
)

// benchmarkPullAccessorCacheRead 构造指定记录数的缓存后执行读取基准。
func benchmarkPullAccessorCacheRead(b *testing.B, bc recordAccessorBenchmark, benchmarkedCheck benchmarkedCheckFunc) {
	logger, _ := ktesting.NewTestContext(b)
	b.Run(fmt.Sprintf("Type=%s/RecordsStored=%d/ConcurrencyPerCPU=%d", bc.namedInit.name, bc.recordsInCache, bc.concurrencyMultiplier), func(b *testing.B) {
		// 生成预置记录和请求序列：命中模式从已有记录中取样，未命中模式使用随机 UUID。
		genRecords, genRequests := generateRecordsAndRequests(bc.recordsInCache, bc.cacheHit)

		// accessor 必须在叶子 b.Run() 内构造，确保临时目录在下一个基准前清理。
		accessor := bc.namedInit.accessorInit(b)
		for _, r := range genRecords {
			if err := accessor.WriteImagePulledRecord(logger, r); err != nil {
				b.Fatalf("failed to prepare cache - write error: %v", err)
			}
		}

		pullManager := &PullManager{
			recordsAccessor:     accessor,
			imagePolicyEnforcer: AlwaysVerifyImagePullPolicy(),
			imageService:        nil, // only needed during init, we're skipping that here
			intentAccessors:     NewStripedLockSet(int32(runtime.NumCPU())),
			intentCounters:      &sync.Map{},
			pulledAccessors:     NewStripedLockSet(int32(runtime.NumCPU())),
		}

		runBenchmark(b, *pullManager, genRequests, bc.concurrencyMultiplier, benchmarkedCheck)
	})
}

// runBenchmark 按并发倍数选择串行循环或 RunParallel，并统一统计分配。
func runBenchmark(b *testing.B, pullManger PullManager, generatedImgRefs []string, concurrencyMultiplier int, bf func(b *testing.B, pullManager PullManager, imgRef string)) {
	b.ReportAllocs()
	if concurrencyMultiplier == 0 {
		var idx uint
		for b.Loop() {
			imgRef := generatedImgRefs[idx%generatedTestRequestsNum]
			bf(b, pullManger, imgRef)
			idx++
		}
		return
	}

	// 并发倍数大于 0 时使用并行基准，每个 goroutine 从随机位置开始取请求。
	b.SetParallelism(concurrencyMultiplier)
	b.ResetTimer()
	b.RunParallel(func(p *testing.PB) {
		idx := uint(rand.IntnRange(0, generatedTestRequestsNum))
		for p.Next() {
			imgRef := generatedImgRefs[idx%generatedTestRequestsNum]
			bf(b, pullManger, imgRef)
			idx++
		}
	})
	b.StopTimer()

}

// generateRecordsAndRequests 生成预置记录与请求序列，用于控制缓存命中率。
func generateRecordsAndRequests(recordsNum int, generateHits bool) ([]*kubeletconfig.ImagePulledRecord, []string) {
	generatedPulledRecords := make([]*kubeletconfig.ImagePulledRecord, 0, recordsNum)
	for range recordsNum {
		imgRef := uuid.NewString()
		generatedPulledRecords = append(generatedPulledRecords, &kubeletconfig.ImagePulledRecord{
			ImageRef: string(imgRef),
			CredentialMapping: map[string]kubeletconfig.ImagePullCredentials{
				"test.repo/org/" + imgRef: {NodePodsAccessible: true},
			},
		})
	}

	generatedCacheRequests := make([]string, 0, generatedTestRequestsNum)
	for range generatedTestRequestsNum {
		var requestRef string
		// 命中模式复用已有记录，未命中模式使用全新 UUID。
		if generateHits {
			requestRef = generatedPulledRecords[rand.IntnRange(0, recordsNum)].ImageRef
		} else {
			requestRef = uuid.NewString()
		}
		generatedCacheRequests = append(generatedCacheRequests, requestRef)
	}

	return generatedPulledRecords, generatedCacheRequests
}

// setupFSRecordsAccessor 在测试临时目录上创建文件系统 accessor。
func setupFSRecordsAccessor(t testing.TB) PullRecordsAccessor {
	t.Helper()
	logger, _ := ktesting.NewTestContext(t)

	tempDir := t.TempDir()
	accessor, err := NewFSPullRecordsAccessor(logger, tempDir)
	if err != nil {
		t.Fatalf("failed to setup filesystem pull records accessor: %v", err)
	}
	return accessor
}

// setupInMemRecordsAccessor 在文件系统 accessor 之上包一层内存缓存，
// 并按需把内存缓存设为权威源。
func setupInMemRecordsAccessor(t testing.TB, cacheSize int, authoritative bool) PullRecordsAccessor {
	logger, _ := ktesting.NewTestContext(t)
	t.Helper()

	fsAccessor := setupFSRecordsAccessor(t)
	memcacheAccessor := NewCachedPullRecordsAccessor(logger, fsAccessor, int32(cacheSize), int32(cacheSize), int32(runtime.NumCPU()))
	gotMeteredAccessor, ok := memcacheAccessor.(*meteringRecordsAccessor)
	if !ok {
		t.Fatalf("the tested accessor must be a metered records accessor, got %T", memcacheAccessor)
	}
	inMemAccessor, ok := gotMeteredAccessor.sizeExposedPullRecordsAccessor.(*cachedPullRecordsAccessor)
	if !ok {
		t.Fatalf("the metered accessor's delegate is not an inMemAccesor: %T", gotMeteredAccessor.sizeExposedPullRecordsAccessor)
	}
	// 直接写入底层缓存开关，模拟权威/非权威两种部署模式。
	inMemAccessor.intents.authoritative.Store(authoritative)
	inMemAccessor.pulledRecords.authoritative.Store(authoritative)

	return memcacheAccessor
}
