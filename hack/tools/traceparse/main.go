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

// traceparse 汇总一个或多个 Go 执行跟踪文件（由 go test -trace 或
// scheduler_perf 的 -perf-trace 生成）中的 GC 活动。
//
// 用法：
//
//	traceparse <trace-file> [<trace-file> ...]
//
// 对每个文件输出：
//   - GC 周期数
//   - GC 并发标记阶段的总耗时
//   - GC mark-assist 事件的次数与总耗时（分配速度超过后台标记时，
//     goroutine 会被打断以协助 GC）
//   - stop-the-world 暂停的次数与总耗时，按类型细分
//   - 运行期间的平均存活堆大小
package main

import (
	"fmt"
	"os"
	"sort"
	"time"

	"golang.org/x/exp/trace"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintf(os.Stderr, "usage: traceparse <trace-file> [<trace-file> ...]\n")
		os.Exit(1)
	}

	rc := 0
	for _, path := range os.Args[1:] {
		if err := summarize(path); err != nil {
			fmt.Fprintf(os.Stderr, "%s: %v\n", path, err)
			rc = 1
		}
	}
	os.Exit(rc)
}

func summarize(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()

	r, err := trace.NewReader(f)
	if err != nil {
		return fmt.Errorf("NewReader: %w", err)
	}

	// rangeDurs 累计各命名 range 事件的时钟耗时。
	// rangeCounts 记录每个 range 被进入的次数。
	// rangeStart 保存各 range 名称最近一次 begin 事件的时间戳（后写覆盖）。
	// GC 阶段这类 range 互不重叠，覆盖无妨；"GC mark assist" 在不同
	// goroutine 上可以重叠，但每个 goroutine 用 goroutine ID 单独记录，
	// 因此各自的耗时仍能正确累计。
	type inFlight struct {
		name string
		t    trace.Time
	}
	goroutineRanges := map[trace.GoID]inFlight{} // per-goroutine open ranges
	procRanges := map[trace.ProcID]inFlight{}    // proc-scoped open ranges (GC sweep)
	singletonRange := map[string]trace.Time{}    // non-goroutine, non-proc ranges (GC mark phase)

	rangeDurs := map[string]time.Duration{}
	rangeCounts := map[string]int{}

	var heapLiveTotal uint64
	var heapLiveSamples int

	for {
		ev, err := r.ReadEvent()
		if err != nil {
			break
		}
		switch ev.Kind() {
		case trace.EventRangeBegin:
			ra := ev.Range()
			switch ra.Scope.Kind {
			case trace.ResourceGoroutine:
				goroutineRanges[ra.Scope.Goroutine()] = inFlight{ra.Name, ev.Time()}
				rangeCounts[ra.Name]++
			case trace.ResourceProc:
				procRanges[ra.Scope.Proc()] = inFlight{ra.Name, ev.Time()}
				rangeCounts[ra.Name]++
			default:
				singletonRange[ra.Name] = ev.Time()
				rangeCounts[ra.Name]++
			}
		case trace.EventRangeEnd:
			ra := ev.Range()
			switch ra.Scope.Kind {
			case trace.ResourceGoroutine:
				if inf, ok := goroutineRanges[ra.Scope.Goroutine()]; ok {
					rangeDurs[inf.name] += time.Duration(ev.Time() - inf.t)
					delete(goroutineRanges, ra.Scope.Goroutine())
				}
			case trace.ResourceProc:
				if inf, ok := procRanges[ra.Scope.Proc()]; ok {
					rangeDurs[inf.name] += time.Duration(ev.Time() - inf.t)
					delete(procRanges, ra.Scope.Proc())
				}
			default:
				if start, ok := singletonRange[ra.Name]; ok {
					rangeDurs[ra.Name] += time.Duration(ev.Time() - start)
					delete(singletonRange, ra.Name)
				}
			}
		case trace.EventMetric:
			m := ev.Metric()
			// /memory/classes/heap/objects:bytes 表示存活对象与尚未被 GC 清理的
			// 死对象占用的内存。完整的运行时指标及其定义参见
			// https://pkg.go.dev/runtime/metrics。
			if m.Name == "/memory/classes/heap/objects:bytes" {
				heapLiveTotal += m.Value.Uint64()
				heapLiveSamples++
			}
		}
	}

	// 按总耗时降序排列，便于阅读。
	type entry struct {
		name  string
		count int
		dur   time.Duration
	}
	var entries []entry
	for name, dur := range rangeDurs {
		entries = append(entries, entry{name, rangeCounts[name], dur})
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].dur > entries[j].dur })

	fmt.Printf("=== %s ===\n", path)
	fmt.Printf("%-52s  %9s  %s\n", "range", "count", "total duration")
	fmt.Printf("%-52s  %9s  %s\n", "-----", "-----", "--------------")
	for _, e := range entries {
		fmt.Printf("%-52s  %9d  %v\n", e.name, e.count, e.dur)
	}
	if heapLiveSamples > 0 {
		fmt.Printf("\navg live heap: %d MB  (%d samples)\n", heapLiveTotal/uint64(heapLiveSamples)/1024/1024, heapLiveSamples)
	}
	fmt.Println()
	return nil
}
