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
	"fmt"
	"slices"
	"sort"
	"strings"

	resourcev1 "k8s.io/api/resource/v1"
	resourcev1alpha3 "k8s.io/api/resource/v1alpha3"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/utils/ptr"
)

// maxStatusListItems 与状态列表上的 +k8s:maxItems=32 标记一致。
// 超过该限制会导致状态校验失败，因此视图选择报告 validationError 而不是截断列表。
const maxStatusListItems = 32

// 机器可识别的 pool validationError 消息前缀。
const (
	// prefixPoolIncomplete 是唯一的瞬时错误；syncRequest 遇到它会重新入队。
	prefixPoolIncomplete = "PoolIncomplete:"

	prefixPartitionTypeMissing    = "PartitionTypeMissing:"
	prefixPartitionCostMismatch   = "PartitionCostMismatch:"
	prefixPartitionSummaryOverCap = "PartitionSummaryOverCap:"
	prefixShareableOverCap        = "ShareableSummaryOverCap:"
)

// deviceRecord 是计算高级视图所使用的单设备数据。
type deviceRecord struct {
	name string
	// partitionAttr 是为该设备解析出的分组属性：优先取设备自身 slice 声明的
	// PartitionTypeAttribute，未声明时取请求级别的默认值；两者都没有时为空。
	partitionAttr string
	// partitionType 是该设备 partitionAttr 属性的值；
	// 当设备缺少该字符串属性时 hasPartitionType 为 false。
	partitionType    string
	hasPartitionType bool
	consumesCounters []resourcev1.DeviceCounterConsumption
	allowMultiple    bool
	capacity         map[resourcev1.QualifiedName]resourcev1.DeviceCapacity
	// attributes 使得在得知 partitionAttr 后可以解析分区类型。
	attributes map[resourcev1.QualifiedName]resourcev1.DeviceAttribute
}

// sliceDeviceRecords 为单个 slice 构建逐设备记录，以该 slice 自身的
// PartitionTypeAttribute 作为初始值（未声明时为空）。
func sliceDeviceRecords(slice *resourcev1.ResourceSlice) []deviceRecord {
	var sliceAttr string
	if slice.Spec.PartitionTypeAttribute != nil {
		sliceAttr = string(*slice.Spec.PartitionTypeAttribute)
	}
	recs := make([]deviceRecord, 0, len(slice.Spec.Devices))
	for i := range slice.Spec.Devices {
		d := &slice.Spec.Devices[i]
		recs = append(recs, deviceRecord{
			name:             d.Name,
			partitionAttr:    sliceAttr,
			consumesCounters: d.ConsumesCounters,
			allowMultiple:    d.AllowMultipleAllocations != nil && *d.AllowMultipleAllocations,
			capacity:         d.Capacity,
			attributes:       d.Attributes,
		})
	}
	return recs
}

// resolvePartitionType 返回设备上属性 fqn 的字符串值。
// 属性键的域名默认取驱动名（与 CEL 选择器一致），因此驱动域名下的属性
// 既可以显式声明也可以裸名声明；两者都存在时显式形式优先，保证结果确定。
// 属性不存在或不是字符串时返回 ("", false)。
func resolvePartitionType(driver string, attrs map[resourcev1.QualifiedName]resourcev1.DeviceAttribute, fqn string) (string, bool) {
	attr, ok := attrs[resourcev1.QualifiedName(fqn)]
	if !ok {
		if bare, cut := strings.CutPrefix(fqn, driver+"/"); cut {
			attr, ok = attrs[resourcev1.QualifiedName(bare)]
		}
	}
	if !ok || attr.StringValue == nil {
		return "", false
	}
	return *attr.StringValue, true
}

// poolViewInput 承载计算单个完整池的高级视图所需的全部信息。
type poolViewInput struct {
	driver   string
	poolName string
	devices  []deviceRecord
	// sharedCounters 合并自池中所有 slice 的共享计数器（名称在池内唯一）。
	sharedCounters []resourcev1.CounterSet
	// inUse 保存至少被一个非 AdminAccess 声明占用的设备名。
	inUse map[string]struct{}
	// consumedCapacity 是各键在非 AdminAccess 声明上的累计消耗量。
	consumedCapacity map[resourcev1.QualifiedName]resource.Quantity
}

// resolveDevicePartitions 按照各设备的分组属性解析其分区类型。
// 设备自身 slice 的声明优先；不同 slice 可能声明不同属性，因此混合分区的池
// 按设备而非按池分组。请求中的属性仅作为全池兜底：只有当池中没有任何 slice
// 声明分组属性时才使用，以兼容尚未自行声明的驱动。
func resolveDevicePartitions(requestAttr *string, in *poolViewInput) {
	poolDeclares := false
	for i := range in.devices {
		if in.devices[i].partitionAttr != "" {
			poolDeclares = true
			break
		}
	}
	for i := range in.devices {
		d := &in.devices[i]
		if d.partitionAttr == "" {
			// 池内已有 slice 声明属性时，不应让请求级默认值污染其他设备；
			// 两者都没有时该设备没有分组属性，直接跳过。
			if poolDeclares || requestAttr == nil {
				continue
			}
			d.partitionAttr = *requestAttr
		}
		d.partitionType, d.hasPartitionType = resolvePartitionType(in.driver, d.attributes, d.partitionAttr)
	}
}

// computePoolViews 返回池的分区视图和可共享视图。
// 无法计算的视图为 nil；validationError 保存带前缀的第一个结构性错误。
// 无论是否存在 validationError，基础设备计数仍然有效。
// 对可分区但未解析出分组属性的池不产生分区视图（请求和驱动都没有指定）。
func computePoolViews(in poolViewInput) (partitionSummary []resourcev1alpha3.PartitionTypeStatus, shareable *resourcev1alpha3.ShareableSummaryStatus, validationError string) {
	hasCounters := len(in.sharedCounters) > 0

	grouped := false
	for i := range in.devices {
		if in.devices[i].partitionAttr != "" {
			grouped = true
			break
		}
	}

	switch {
	case grouped && !hasCounters:
		validationError = fmt.Sprintf("%s pool %s/%s declares partitionTypeAttribute but publishes no sharedCounters",
			prefixPartitionTypeMissing, in.driver, in.poolName)
	case grouped:
		partitionSummary, validationError = computePartitionSummary(in)
	}

	// 可共享视图与分区视图相互独立；只保留第一个错误。
	sh, shErr := computeShareableSummary(in)
	if validationError == "" {
		validationError = shErr
	}
	if shErr == "" {
		shareable = sh
	}
	if validationError != "" {
		// 出错时丢弃构建到一半的分区视图，改为标记该池存在校验错误。
		partitionSummary = nil
	}
	return partitionSummary, shareable, validationError
}

// computePartitionSummary 按（分组属性，分区类型）对可分区（消耗计数器）设备分组，
// 报告每组的设备总数以及按当前共享计数器消耗情况还能容纳的新增设备数。
// 不消耗计数器的设备不属于分区，会被跳过；没有解析出分组属性的设备同样跳过。
func computePartitionSummary(in poolViewInput) ([]resourcev1alpha3.PartitionTypeStatus, string) {
	type groupKey struct{ attr, typ string }
	type group struct {
		total   int32
		fresh   []deviceRecord
		costKey string
	}
	groups := map[groupKey]*group{}
	for i := range in.devices {
		d := in.devices[i]
		if len(d.consumesCounters) == 0 || d.partitionAttr == "" {
			continue
		}
		if !d.hasPartitionType {
			return nil, fmt.Sprintf("%s device %q in pool %s/%s does not carry partition type attribute %q",
				prefixPartitionTypeMissing, d.name, in.driver, in.poolName, d.partitionAttr)
		}
		k := groupKey{d.partitionAttr, d.partitionType}
		g := groups[k]
		if g == nil {
			g = &group{}
			groups[k] = g
		}
		key := consumesCountersKey(d.consumesCounters)
		if g.total == 0 {
			g.costKey = key
		} else if key != g.costKey {
			// 同组设备必须消耗相同的计数器，否则无法用统一成本计算可分配数。
			return nil, fmt.Sprintf("%s devices of partition type %q in pool %s/%s consume different counters",
				prefixPartitionCostMismatch, d.partitionType, in.driver, in.poolName)
		}
		g.total++
		if _, used := in.inUse[d.name]; !used {
			g.fresh = append(g.fresh, d)
		}
	}

	// 扣除已占用设备的消耗后得到剩余额度。各组独立基于该基线计算：
	// allocatable[G] = min(fresh[G], floor(avail/cost[G]))。
	baseline := availableCounters(in.sharedCounters, in.devices, in.inUse)

	result := make([]resourcev1alpha3.PartitionTypeStatus, 0, len(groups))
	for k, g := range groups {
		// 每组都从基线的深拷贝出发，避免一组的贪心分配影响其他组。
		avail := cloneCounters(baseline)
		allocatable := int32(0)
		for _, d := range g.fresh {
			if deviceFits(d.consumesCounters, avail) {
				allocatable++
				deductCounters(d.consumesCounters, avail)
			}
		}
		result = append(result, resourcev1alpha3.PartitionTypeStatus{
			Attribute:   k.attr,
			Type:        k.typ,
			Total:       ptr.To(g.total),
			Allocatable: ptr.To(allocatable),
		})
	}
	// 排序使状态输出稳定，避免触发不必要的状态更新。
	sort.Slice(result, func(i, j int) bool {
		if result[i].Attribute != result[j].Attribute {
			return result[i].Attribute < result[j].Attribute
		}
		return result[i].Type < result[j].Type
	})

	if len(result) > maxStatusListItems {
		return nil, fmt.Sprintf("%s pool %s/%s has %d partition types, exceeding the maximum of %d",
			prefixPartitionSummaryOverCap, in.driver, in.poolName, len(result), maxStatusListItems)
	}
	return result, ""
}

// computeShareableSummary 为包含 allowMultipleAllocations 设备的池生成聚合容量视图。
// 池中没有可共享设备时返回 nil。
func computeShareableSummary(in poolViewInput) (*resourcev1alpha3.ShareableSummaryStatus, string) {
	var full, partial int32
	total := map[resourcev1.QualifiedName]resource.Quantity{}
	any := false
	for i := range in.devices {
		d := in.devices[i]
		if !d.allowMultiple {
			continue
		}
		any = true
		// 被占用的可共享设备记为部分可用，否则记为完全可用。
		if _, used := in.inUse[d.name]; used {
			partial++
		} else {
			full++
		}
		for key, capacity := range d.capacity {
			cur := total[key].DeepCopy()
			cur.Add(capacity.Value)
			total[key] = cur
		}
	}
	if !any {
		return nil, ""
	}

	keys := make([]resourcev1.QualifiedName, 0, len(total))
	for key := range total {
		keys = append(keys, key)
	}
	// 排序保证容量列表的输出顺序确定。
	slices.Sort(keys)

	capacities := make([]resourcev1alpha3.ShareableCapacityStatus, 0, len(keys))
	for _, key := range keys {
		t := total[key]
		cons := resource.Quantity{}
		if q, ok := in.consumedCapacity[key]; ok {
			cons = q.DeepCopy()
		}
		avail := nonNegativeDiff(t, cons)
		capacities = append(capacities, resourcev1alpha3.ShareableCapacityStatus{
			Name:      string(key),
			Total:     &t,
			Consumed:  &cons,
			Available: &avail,
		})
	}
	if len(capacities) > maxStatusListItems {
		return nil, fmt.Sprintf("%s pool %s/%s has %d shareable capacity keys, exceeding the maximum of %d",
			prefixShareableOverCap, in.driver, in.poolName, len(capacities), maxStatusListItems)
	}

	return &resourcev1alpha3.ShareableSummaryStatus{
		FullyAvailableDevices:     ptr.To(full),
		PartiallyAvailableDevices: ptr.To(partial),
		Capacity:                  capacities,
	}, ""
}

// availableCounters 返回各 [计数器集][计数器] 的容量减去已占用设备的消耗后的余量。
// 每个已占用设备只扣一次计数器（而不是按声明数扣除），与调度器的行为一致。
func availableCounters(sharedCounters []resourcev1.CounterSet, devices []deviceRecord, inUse map[string]struct{}) map[string]map[string]resource.Quantity {
	avail := map[string]map[string]resource.Quantity{}
	for _, cs := range sharedCounters {
		m := avail[cs.Name]
		if m == nil {
			m = map[string]resource.Quantity{}
			avail[cs.Name] = m
		}
		for name, c := range cs.Counters {
			m[name] = c.Value.DeepCopy()
		}
	}
	deductInUse(avail, devices, inUse)
	return avail
}

// deductInUse 从 avail 中扣除每个已占用设备的计数器消耗。
func deductInUse(avail map[string]map[string]resource.Quantity, devices []deviceRecord, inUse map[string]struct{}) {
	for i := range devices {
		d := devices[i]
		if _, used := inUse[d.name]; !used {
			continue
		}
		deductCounters(d.consumesCounters, avail)
	}
}

// deviceFits 判断 avail 中的余量是否满足某设备的计数器开销。
func deviceFits(cost []resourcev1.DeviceCounterConsumption, avail map[string]map[string]resource.Quantity) bool {
	for _, cc := range cost {
		set := avail[cc.CounterSet]
		for name, c := range cc.Counters {
			have, ok := set[name]
			if !ok {
				return false
			}
			if have.Cmp(c.Value) < 0 {
				return false
			}
		}
	}
	return true
}

// deductCounters 就地从 avail 中扣除某设备的计数器开销。
func deductCounters(cost []resourcev1.DeviceCounterConsumption, avail map[string]map[string]resource.Quantity) {
	for _, cc := range cost {
		set := avail[cc.CounterSet]
		if set == nil {
			continue
		}
		for name, c := range cc.Counters {
			have, ok := set[name]
			if !ok {
				continue
			}
			have.Sub(c.Value)
			set[name] = have
		}
	}
}

// cloneCounters 深拷贝 [计数器集][计数器] 数量映射，
// 使按类型的贪心计算不会修改共享的基线数据。
func cloneCounters(src map[string]map[string]resource.Quantity) map[string]map[string]resource.Quantity {
	out := make(map[string]map[string]resource.Quantity, len(src))
	for set, counters := range src {
		m := make(map[string]resource.Quantity, len(counters))
		for name, q := range counters {
			m[name] = q.DeepCopy()
		}
		out[set] = m
	}
	return out
}

// consumesCountersKey 构造标识某设备计数器开销的稳定字符串，
// 用于检测同一分区类型内开销不一致的情况。
func consumesCountersKey(cost []resourcev1.DeviceCounterConsumption) string {
	parts := make([]string, 0, len(cost))
	for _, cc := range cost {
		names := make([]string, 0, len(cc.Counters))
		for name := range cc.Counters {
			names = append(names, name)
		}
		// 排序 map 键，确保相同开销生成相同字符串。
		sort.Strings(names)
		for _, name := range names {
			q := cc.Counters[name]
			parts = append(parts, cc.CounterSet+"/"+name+"="+q.Value.String())
		}
	}
	sort.Strings(parts)
	return strings.Join(parts, ",")
}

// nonNegativeDiff 以新的 Quantity 返回 max(0, a-b)，并保留 a 的表示格式。
func nonNegativeDiff(a, b resource.Quantity) resource.Quantity {
	out := a.DeepCopy()
	out.Sub(b)
	if out.Sign() < 0 {
		return *resource.NewQuantity(0, a.Format)
	}
	return out
}
