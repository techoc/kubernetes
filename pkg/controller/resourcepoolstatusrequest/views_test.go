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

package resourcepoolstatusrequest

import (
	"strings"
	"testing"

	resourcev1 "k8s.io/api/resource/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/utils/ptr"
)

// qty 解析数量字符串，Test 中大量构造资源量时的简写辅助函数。
func qty(s string) resource.Quantity { return resource.MustParse(s) }

// counterSet 把字符串形式的计数器值转成 CounterSet，便于表驱动用例紧凑表达。
func counterSet(name string, counters map[string]string) resourcev1.CounterSet {
	c := make(map[string]resourcev1.Counter, len(counters))
	for k, v := range counters {
		c[k] = resourcev1.Counter{Value: qty(v)}
	}
	return resourcev1.CounterSet{Name: name, Counters: c}
}

// consumes 构造设备对某个 CounterSet 的消耗声明，值同样用字符串简写。
func consumes(set string, counters map[string]string) resourcev1.DeviceCounterConsumption {
	c := make(map[string]resourcev1.Counter, len(counters))
	for k, v := range counters {
		c[k] = resourcev1.Counter{Value: qty(v)}
	}
	return resourcev1.DeviceCounterConsumption{CounterSet: set, Counters: c}
}

// inUseSet 把设备名列表转成 inUse 集合，表示这些设备已被占用。
func inUseSet(names ...string) map[string]struct{} {
	s := make(map[string]struct{}, len(names))
	for _, n := range names {
		s[n] = struct{}{}
	}
	return s
}

// partitionDevice 构造一个已解析分区属性与分区类型、并声明计数器消耗的设备，
// 模拟控制器在调用视图函数前完成的预处理结果。
func partitionDevice(name, partitionType string, cost ...resourcev1.DeviceCounterConsumption) deviceRecord {
	return deviceRecord{
		name:             name,
		partitionAttr:    "gpu.example.com/profile",
		partitionType:    partitionType,
		hasPartitionType: true,
		consumesCounters: cost,
	}
}

// TestComputePartitionSummary 验证分区汇总：按分区类型统计可分配数量，
// 覆盖计数器容量、设备数量上限、占用设备扣减与属性缺失/成本冲突等边界。
func TestComputePartitionSummary(t *testing.T) {
	// 构造 80Gi 的计数器集：Full 消耗 80Gi，Half 消耗 40Gi。
	sc := []resourcev1.CounterSet{counterSet("gpu-0", map[string]string{"memory": "80Gi"})}

	testCases := map[string]struct {
		// devices 是待汇总的设备记录。
		devices []deviceRecord
		// inUse 是被占用的设备名集合，用于计算剩余可分配量。
		inUse map[string]struct{}
		// want 是每种分区类型期望的可分配数量。
		want map[string]int32 // type -> allocatable
		// total 是每种分区类型的设备总数。
		total map[string]int32 // type -> total
		// wantErr 非空时期望错误信息以该前缀开头。
		wantErr string
	}{
		// 空闲设备按各类型独立以 80Gi 基线计算：Full 1 个、Half 2 个。
		"all-fresh-independent-per-type": {
			devices: []deviceRecord{
				partitionDevice("full-0", "Full", consumes("gpu-0", map[string]string{"memory": "80Gi"})),
				partitionDevice("half-0", "Half", consumes("gpu-0", map[string]string{"memory": "40Gi"})),
				partitionDevice("half-1", "Half", consumes("gpu-0", map[string]string{"memory": "40Gi"})),
			},
			inUse: inUseSet(),
			// 各类型独立对照 80Gi 基线计算可分配数量。
			want:  map[string]int32{"Full": 1, "Half": 2},
			total: map[string]int32{"Full": 1, "Half": 2},
		},
		// 已占用的 full-0 耗尽计数器，两种类型都无剩余额度。
		"in-use-full-consumes-counter": {
			devices: []deviceRecord{
				partitionDevice("full-0", "Full", consumes("gpu-0", map[string]string{"memory": "80Gi"})),
				partitionDevice("half-0", "Half", consumes("gpu-0", map[string]string{"memory": "40Gi"})),
				partitionDevice("half-1", "Half", consumes("gpu-0", map[string]string{"memory": "40Gi"})),
			},
			inUse: inUseSet("full-0"),
			// full-0 被占用后计数器被耗尽，两种类型都无可用额度。
			want:  map[string]int32{"Full": 0, "Half": 0},
			total: map[string]int32{"Full": 1, "Half": 2},
		},
		// 计数器容量足够 10 个 Full，但只有 3 个空闲设备，取二者较小值。
		"fresh-device-clamp-binds-before-counter": {
			devices: []deviceRecord{
				partitionDevice("full-0", "Full", consumes("gpu-1", map[string]string{"memory": "80Gi"})),
				partitionDevice("full-1", "Full", consumes("gpu-1", map[string]string{"memory": "80Gi"})),
				partitionDevice("full-2", "Full", consumes("gpu-1", map[string]string{"memory": "80Gi"})),
			},
			inUse: inUseSet(),
			// 800Gi 计数器可容纳 10 个 Full，但空闲设备只有 3 个，取较小值。
			want:  map[string]int32{"Full": 3},
			total: map[string]int32{"Full": 3},
		},
		// 消耗计数器但只解析出分组属性、缺少属性值的设备会被标记错误；
		// 非分区设备缺少该属性则会被跳过，两者行为不同。
		"missing-attribute": {
			devices: []deviceRecord{
				partitionDevice("full-0", "Full", consumes("gpu-0", map[string]string{"memory": "80Gi"})),
				{
					name:             "mystery",
					partitionAttr:    "gpu.example.com/profile",
					consumesCounters: []resourcev1.DeviceCounterConsumption{consumes("gpu-0", map[string]string{"memory": "40Gi"})},
					hasPartitionType: false,
				},
			},
			inUse:   inUseSet(),
			wantErr: prefixPartitionTypeMissing,
		},
		// 同一分区类型内成本不一致（40Gi 与 50Gi）应报错。
		"cost-mismatch-within-type": {
			devices: []deviceRecord{
				partitionDevice("half-0", "Half", consumes("gpu-0", map[string]string{"memory": "40Gi"})),
				partitionDevice("half-1", "Half", consumes("gpu-0", map[string]string{"memory": "50Gi"})),
			},
			inUse:   inUseSet(),
			wantErr: prefixPartitionCostMismatch,
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			in := poolViewInput{
				driver:   "gpu.example.com",
				poolName: "pool-0",
				devices:  tc.devices,
				// 只有名为 clamp 的用例使用 800Gi 基线，以验证设备数量上限的钳制。
				sharedCounters: chooseCounters(name, sc),
				inUse:          tc.inUse,
			}
			got, gotErr := computePartitionSummary(in)
			// 期望出错时只校验错误前缀，不要求完全一致，便于定位错误类别。
			if tc.wantErr != "" {
				if !strings.HasPrefix(gotErr, tc.wantErr) {
					t.Fatalf("want error prefix %q, got %q", tc.wantErr, gotErr)
				}
				return
			}
			if gotErr != "" {
				t.Fatalf("unexpected error: %s", gotErr)
			}
			// 逐类型比对可分配数与总数；未列入 want/total 的类型不在本用例校验范围内。
			for _, p := range got {
				if want, ok := tc.want[p.Type]; ok && ptr.Deref(p.Allocatable, 0) != want {
					t.Errorf("type %s: allocatable = %d, want %d", p.Type, ptr.Deref(p.Allocatable, 0), want)
				}
				if want, ok := tc.total[p.Type]; ok && ptr.Deref(p.Total, 0) != want {
					t.Errorf("type %s: total = %d, want %d", p.Type, ptr.Deref(p.Total, 0), want)
				}
			}
			// 最后校验分区类型数量，防止多返回未被断言的类型。
			if len(got) != len(tc.total) {
				t.Errorf("got %d partition types, want %d", len(got), len(tc.total))
			}
		})
	}
}

// chooseCounters 只给验证设备数量钳制的用例提供 800Gi 基线。
func chooseCounters(name string, dflt []resourcev1.CounterSet) []resourcev1.CounterSet {
	if strings.Contains(name, "clamp") {
		return []resourcev1.CounterSet{counterSet("gpu-1", map[string]string{"memory": "800Gi"})}
	}
	return dflt
}

// 设备同时消耗多个计数器集时，可分配数量受限于最紧的那个计数器。
func TestComputePartitionSummary_MultiCounterSet(t *testing.T) {
	sc := []resourcev1.CounterSet{
		counterSet("mem", map[string]string{"memory": "160Gi"}),
		counterSet("cores", map[string]string{"sm": "8"}),
	}
	full := func(name string) deviceRecord {
		return partitionDevice(name, "Full",
			consumes("mem", map[string]string{"memory": "80Gi"}),
			consumes("cores", map[string]string{"sm": "8"}),
		)
	}
	in := poolViewInput{
		driver: "gpu.example.com", poolName: "p", sharedCounters: sc,
		devices: []deviceRecord{full("full-0"), full("full-1")},
		inUse:   inUseSet(),
	}
	got, err := computePartitionSummary(in)
	if err != "" {
		t.Fatalf("unexpected error: %s", err)
	}
	// 内存计数器可容纳 2 个 Full，但 sm 计数器只够 1 个，故取最小值 1。
	if len(got) != 1 || ptr.Deref(got[0].Allocatable, 0) != 1 {
		t.Errorf("allocatable = %+v, want 1 (bound by the sm counter)", got)
	}
}

// 设备既是分区类型又可共享时，computePoolViews 应同时返回分区视图与共享视图。
func TestComputePoolViews_Hybrid(t *testing.T) {
	in := poolViewInput{
		driver: "gpu.example.com", poolName: "p",
		sharedCounters: []resourcev1.CounterSet{counterSet("gpu-0", map[string]string{"memory": "80Gi"})},
		devices: []deviceRecord{{
			name:             "full-0",
			partitionAttr:    "gpu.example.com/profile",
			partitionType:    "Full",
			hasPartitionType: true,
			consumesCounters: []resourcev1.DeviceCounterConsumption{consumes("gpu-0", map[string]string{"memory": "80Gi"})},
			allowMultiple:    true,
			capacity:         map[resourcev1.QualifiedName]resourcev1.DeviceCapacity{"memory": {Value: qty("80Gi")}},
		}},
		inUse: inUseSet(),
	}
	ps, sh, err := computePoolViews(in)
	if err != "" {
		t.Fatalf("unexpected error: %s", err)
	}
	// 两种视图都应生成，且 FullyAvailableDevices 计入该共享设备。
	if len(ps) != 1 || sh == nil {
		t.Errorf("hybrid: want partitionSummary set and shareableSummary set; got ps=%d sh=%v", len(ps), sh)
	}
	if sh != nil && ptr.Deref(sh.FullyAvailableDevices, 0) != 1 {
		t.Errorf("shareable fullyAvailable = %d, want 1", ptr.Deref(sh.FullyAvailableDevices, 0))
	}
}

// TestComputeShareableSummary 验证共享设备汇总：区分完全/部分可用设备，
// 并正确累加每种容量的 total、consumed 与 available。
func TestComputeShareableSummary(t *testing.T) {
	devices := []deviceRecord{
		{
			name:          "shareable-0",
			allowMultiple: true,
			capacity:      map[resourcev1.QualifiedName]resourcev1.DeviceCapacity{"memory": {Value: qty("40Gi")}},
		},
		{
			name:          "shareable-1",
			allowMultiple: true,
			capacity:      map[resourcev1.QualifiedName]resourcev1.DeviceCapacity{"memory": {Value: qty("40Gi")}},
		},
		// 非共享设备应被忽略，不参与共享视图统计。
		{name: "plain", allowMultiple: false},
	}
	in := poolViewInput{
		driver:   "gpu.example.com",
		poolName: "pool-0",
		devices:  devices,
		// shareable-0 已被占用，故一个完全可用、一个部分可用。
		inUse:            inUseSet("shareable-0"),
		consumedCapacity: map[resourcev1.QualifiedName]resource.Quantity{"memory": qty("10Gi")},
	}

	got, err := computeShareableSummary(in)
	if err != "" {
		t.Fatalf("unexpected error: %s", err)
	}
	// 只要存在共享设备，就应返回非空汇总。
	if got == nil {
		t.Fatal("expected a shareable summary")
	}
	// 被占用设备之外仍有 1 台完全可用的共享设备。
	if ptr.Deref(got.FullyAvailableDevices, 0) != 1 {
		t.Errorf("fullyAvailableDevices = %d, want 1", ptr.Deref(got.FullyAvailableDevices, 0))
	}
	// 占用设备本身计为部分可用。
	if ptr.Deref(got.PartiallyAvailableDevices, 0) != 1 {
		t.Errorf("partiallyAvailableDevices = %d, want 1", ptr.Deref(got.PartiallyAvailableDevices, 0))
	}
	// 只声明了 memory 一种容量。
	if len(got.Capacity) != 1 {
		t.Fatalf("want 1 capacity key, got %d", len(got.Capacity))
	}
	cap := got.Capacity[0]
	if cap.Total.String() != "80Gi" || cap.Consumed.String() != "10Gi" || cap.Available.Cmp(qty("70Gi")) != 0 {
		t.Errorf("capacity = %+v, want total=80Gi consumed=10Gi available=70Gi", cap)
	}
}

// 共享设备声明多种容量时，每种容量跨设备分别累加并独立上报，
// 结果按名称排序。
func TestComputeShareableSummary_MultipleCapacities(t *testing.T) {
	devices := []deviceRecord{
		{
			name:          "shareable-0",
			allowMultiple: true,
			capacity: map[resourcev1.QualifiedName]resourcev1.DeviceCapacity{
				"memory": {Value: qty("40Gi")},
				"cores":  {Value: qty("4")},
			},
		},
		{
			name:          "shareable-1",
			allowMultiple: true,
			capacity: map[resourcev1.QualifiedName]resourcev1.DeviceCapacity{
				"memory": {Value: qty("40Gi")},
				"cores":  {Value: qty("4")},
			},
		},
	}
	in := poolViewInput{
		driver:   "gpu.example.com",
		poolName: "pool-0",
		devices:  devices,
		inUse:    inUseSet("shareable-0"),
		consumedCapacity: map[resourcev1.QualifiedName]resource.Quantity{
			"memory": qty("10Gi"),
			"cores":  qty("1"),
		},
	}

	got, err := computeShareableSummary(in)
	if err != "" {
		t.Fatalf("unexpected error: %s", err)
	}
	if got == nil {
		t.Fatal("expected a shareable summary")
	}
	// 两台共享设备中一台被占用，完全/部分可用各 1 台。
	if ptr.Deref(got.FullyAvailableDevices, 0) != 1 || ptr.Deref(got.PartiallyAvailableDevices, 0) != 1 {
		t.Errorf("full/partial = %d/%d, want 1/1", ptr.Deref(got.FullyAvailableDevices, 0), ptr.Deref(got.PartiallyAvailableDevices, 0))
	}
	// 按容量名建索引，便于逐个断言而不依赖返回顺序。
	byKey := map[string][3]string{}
	for _, c := range got.Capacity {
		byKey[c.Name] = [3]string{c.Total.String(), c.Consumed.String(), c.Available.String()}
	}
	if want := [3]string{"8", "1", "7"}; byKey["cores"] != want {
		t.Errorf("cores {total,consumed,available} = %v, want %v", byKey["cores"], want)
	}
	if want := [3]string{"80Gi", "10Gi", "70Gi"}; byKey["memory"] != want {
		t.Errorf("memory {total,consumed,available} = %v, want %v", byKey["memory"], want)
	}
}

// 没有共享设备时应返回 nil 汇总且不报错，避免下游生成空视图。
func TestComputeShareableSummary_NoShareableDevices(t *testing.T) {
	in := poolViewInput{devices: []deviceRecord{{name: "plain"}}, inUse: inUseSet()}
	got, err := computeShareableSummary(in)
	if err != "" || got != nil {
		t.Errorf("want nil summary and no error, got %+v / %q", got, err)
	}
}

// 解析出分组属性才会生成分区视图；没有分组属性的可分区池不生成分区视图
// （采用方案 2，不回退到 counterSets）。
func TestComputePoolViews_PartitionAttributeGatesView(t *testing.T) {
	sc := []resourcev1.CounterSet{counterSet("gpu-0", map[string]string{"memory": "80Gi"})}

	// 有分组属性时应生成 partitionSummary。
	typed := poolViewInput{
		driver: "gpu.example.com", poolName: "p", sharedCounters: sc,
		devices: []deviceRecord{partitionDevice("d0", "Full", consumes("gpu-0", map[string]string{"memory": "80Gi"}))},
		inUse:   inUseSet(),
	}
	ps, _, err := computePoolViews(typed)
	if err != "" {
		t.Fatalf("typed: unexpected error %s", err)
	}
	if len(ps) == 0 {
		t.Errorf("typed pool: want partitionSummary set, got ps=%d", len(ps))
	}

	// 没有分组属性则不生成分区视图。
	noAttr := poolViewInput{
		driver: "gpu.example.com", poolName: "p", sharedCounters: sc,
		devices: []deviceRecord{{name: "d0", consumesCounters: []resourcev1.DeviceCounterConsumption{consumes("gpu-0", map[string]string{"memory": "10Gi"})}}},
		inUse:   inUseSet(),
	}
	ps2, _, err := computePoolViews(noAttr)
	if err != "" {
		t.Fatalf("no-attr: unexpected error %s", err)
	}
	if ps2 != nil {
		t.Errorf("no-attr pool: want no partition view, got ps=%d", len(ps2))
	}
}

// TestResolveDevicePartitions 验证分区属性/类型的解析优先级：
// 设备 slice 声明优先于请求默认值；不同设备声明不同属性时各自保留而非判冲突。
func TestResolveDevicePartitions(t *testing.T) {
	const driver = "gpu.example.com"
	// 该设备同时带 profile=Full 与 shape=Small，两种属性都能解析出类型。
	deviceAttrs := map[resourcev1.QualifiedName]resourcev1.DeviceAttribute{
		"gpu.example.com/profile": {StringValue: ptr.To("Full")},
		"gpu.example.com/shape":   {StringValue: ptr.To("Small")},
	}
	testCases := map[string]struct {
		// requestAttr 是请求侧给出的默认分组属性。
		requestAttr *string
		devices     []deviceRecord
		// wantAttr 是每台设备期望解析出的分区属性。
		wantAttr []string // resolved partitionAttr per device
		// wantType 是每台设备期望解析出的分区类型，空串表示未解析。
		wantType []string // resolved partitionType per device ("" when unresolved)
	}{
		// 设备自己声明的 profile 优先于请求默认 shape。
		"slice declaration wins over request default": {
			requestAttr: ptr.To("gpu.example.com/shape"),
			devices:     []deviceRecord{{name: "d0", partitionAttr: "gpu.example.com/profile", attributes: deviceAttrs}},
			wantAttr:    []string{"gpu.example.com/profile"},
			wantType:    []string{"Full"},
		},
		// 设备未声明时由请求默认 shape 补全。
		"request default fills devices when no slice declares": {
			requestAttr: ptr.To("gpu.example.com/shape"),
			devices:     []deviceRecord{{name: "d0", attributes: deviceAttrs}},
			wantAttr:    []string{"gpu.example.com/shape"},
			wantType:    []string{"Small"},
		},
		// 只要有一台设备声明了属性，请求默认值就整体失效。
		"request default ignored when any slice declares": {
			requestAttr: ptr.To("gpu.example.com/shape"),
			devices: []deviceRecord{
				{name: "d0", partitionAttr: "gpu.example.com/profile", attributes: deviceAttrs},
				{name: "d1", attributes: deviceAttrs},
			},
			wantAttr: []string{"gpu.example.com/profile", ""},
			wantType: []string{"Full", ""},
		},
		// liggitt 提出的场景：不同 slice 声明不同属性是合理的，
		// 每台设备保留各自的声明，不应被标记为冲突。
		"differing slice attributes are each kept": {
			devices: []deviceRecord{
				{name: "d0", partitionAttr: "gpu.example.com/profile", attributes: deviceAttrs},
				{name: "d1", partitionAttr: "gpu.example.com/shape", attributes: deviceAttrs},
			},
			wantAttr: []string{"gpu.example.com/profile", "gpu.example.com/shape"},
			wantType: []string{"Full", "Small"},
		},
		// slice 与请求都没有指定属性时保持未解析状态。
		"neither slice nor request names one": {
			devices:  []deviceRecord{{name: "d0", attributes: deviceAttrs}},
			wantAttr: []string{""},
			wantType: []string{""},
		},
	}
	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			in := poolViewInput{driver: driver, devices: tc.devices}
			resolveDevicePartitions(tc.requestAttr, &in)
			for i := range in.devices {
				d := in.devices[i]
				if d.partitionAttr != tc.wantAttr[i] {
					t.Errorf("device %d: partitionAttr = %q, want %q", i, d.partitionAttr, tc.wantAttr[i])
				}
				gotType := ""
				// 未解析的设备其 hasPartitionType 为 false，断言时按空类型处理。
				if d.hasPartitionType {
					gotType = d.partitionType
				}
				if gotType != tc.wantType[i] {
					t.Errorf("device %d: type = %q (has=%v), want %q", i, d.partitionType, d.hasPartitionType, tc.wantType[i])
				}
			}
		})
	}
}

// TestResolvePartitionType 验证属性名的域解析规则：裸名按驱动域匹配，
// 非字符串属性视为不存在，显式键优先于裸键以避免 map 遍历顺序影响结果。
func TestResolvePartitionType(t *testing.T) {
	attrs := map[resourcev1.QualifiedName]resourcev1.DeviceAttribute{
		"profile":                     {StringValue: ptr.To("Full")},
		"other.com/x":                 {StringValue: ptr.To("ignore")},
		"gpu.example.com/numeric-key": {IntValue: ptr.To(int64(3))},
	}
	// 裸名 profile 按驱动域解析为 gpu.example.com/profile。
	if v, ok := resolvePartitionType("gpu.example.com", attrs, "gpu.example.com/profile"); !ok || v != "Full" {
		t.Errorf("driver-domain resolution: got (%q,%v), want (Full,true)", v, ok)
	}
	// 非字符串属性视为不存在。
	if _, ok := resolvePartitionType("gpu.example.com", attrs, "gpu.example.com/numeric-key"); ok {
		t.Error("non-string attribute should resolve as absent")
	}
	// 未知属性视为不存在。
	if _, ok := resolvePartitionType("gpu.example.com", attrs, "gpu.example.com/missing"); ok {
		t.Error("missing attribute should resolve as absent")
	}
	// 外部域属性名只能按显式键匹配，不能按裸名匹配。
	if v, ok := resolvePartitionType("gpu.example.com", attrs, "other.com/x"); !ok || v != "ignore" {
		t.Errorf("foreign-domain resolution: got (%q,%v), want (ignore,true)", v, ok)
	}
	// 当同一属性的显式键与裸键同时存在时，显式键优先，
	// 以保证结果不依赖 map 迭代顺序。
	both := map[resourcev1.QualifiedName]resourcev1.DeviceAttribute{
		"profile":                 {StringValue: ptr.To("Bare")},
		"gpu.example.com/profile": {StringValue: ptr.To("Explicit")},
	}
	// 重复多次调用以覆盖 map 迭代顺序，确保结果稳定。
	for range 20 {
		if v, ok := resolvePartitionType("gpu.example.com", both, "gpu.example.com/profile"); !ok || v != "Explicit" {
			t.Fatalf("colliding keys: got (%q,%v), want (Explicit,true)", v, ok)
		}
	}
}
