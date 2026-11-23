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

package nodedeclaredfeatures

import (
	"fmt"
	"math/bits"
	"slices"
)

// FeatureSet 表示节点特征的集合。
//
// 用类型别名直接复用 bitmap 的存储与位运算能力：特征集合只关心“哪些特征存在”，
// 位图表示让跨节点的比较和子集判断保持廉价。别名（而非新类型）让 bits 等底层操作
// 无需重复包装。
type FeatureSet = bitmap

// FeatureMapper 负责把特征名映射到 FeatureSet 中的位下标。
//
// 字符串名字便于配置和展示，位位置便于集合运算。FeatureMapper 把名字按字典序固定下来，
// 使同一个名字在不同节点、不同进程里总是映射到同一个位；registeredFeatures 既是名字表，
// 也是位下标的来源。
type FeatureMapper struct {
	// registeredFeatures 是排序后的特征名表，其下标就是 FeatureSet 的位序号。
	registeredFeatures []string
}

// NewFeatureMapper 根据已知特征名列表构造 FeatureMapper。
//
// 先复制再排序，避免改动调用方传入的切片，也避免依赖外部之后不再修改它。
// 排序是位编号稳定的前提：输入顺序变了，底层的位位置就不能再互相对照。
func NewFeatureMapper(features []string) *FeatureMapper {
	sortedFeatures := slices.Clone(features)
	slices.Sort(sortedFeatures)
	return &FeatureMapper{sortedFeatures}
}

// NewFeatureSet 返回一个空集合，其位数与已注册特征数一致。
//
// 位图位数必须等于已注册特征数，这样位下标和 registeredFeatures 的下标一一对应。
// 用不同大小构造的集合之间无法做差集/子集判断。
func (m *FeatureMapper) NewFeatureSet() FeatureSet {
	return FeatureSet(newBitmap(len(m.registeredFeatures)))
}

// MapSorted 把已排序的特征名切片映射成 FeatureSet。
//
// 这是严格模式：任何未知特征都要报错，避免把错拼的名字悄悄当成“没有这个特征”。
func (m *FeatureMapper) MapSorted(sortedFeatures []string) (FeatureSet, error) {
	return m.mapSorted(sortedFeatures, false)
}

// MustMapSorted 是 MapSorted 的便捷封装，遇到错误直接 panic。
//
// 给那些数据源可信、且调用方本来就无法处理错误的场景（例如测试或静态构造）用，
// 省去到处写 if err 的噪音。
func (m *FeatureMapper) MustMapSorted(sortedFeatures []string) FeatureSet {
	s, err := m.MapSorted(sortedFeatures)
	if err != nil {
		panic(err)
	}
	return s
}

// TryMap 把已排序的特征名切片映射成 FeatureSet，并忽略未知特征。
//
// 宽松模式用于读取可能来自更新版本节点的数据：未知特征只是看不到，
// 不应让整个集合构造失败。代价是拼错的名字会被吞掉，调用方自行取舍。
func (m *FeatureMapper) TryMap(sortedFeatures []string) FeatureSet {
	fs, _ := m.mapSorted(sortedFeatures, true)
	return fs
}

// mapSorted 用归并扫描替代逐个查表：两个切片都已有序，遇到相同名字就置位并各前进一格，
// 否则推进字典序较小的那一边。这样一次遍历就能同时完成匹配和未知特征的识别。
// 严格模式下发现只在输入里出现的名字立刻报错；扫描结束后输入仍有剩余同样说明它们未知。
func (m *FeatureMapper) mapSorted(sortedFeatures []string, ignoreUnknown bool) (FeatureSet, error) {
	s := m.NewFeatureSet()

	if len(sortedFeatures) == 0 {
		return s, nil
	}

	i, j := 0, 0
	for i < len(sortedFeatures) && j < len(m.registeredFeatures) {
		if sortedFeatures[i] == m.registeredFeatures[j] {
			s.Set(j)
			i++
			j++
		} else if sortedFeatures[i] < m.registeredFeatures[j] {
			if !ignoreUnknown {
				return s, fmt.Errorf("unknown feature %s", sortedFeatures[i])
			}
			i++
		} else {
			j++
		}
	}

	if !ignoreUnknown && i < len(sortedFeatures) {
		return s, fmt.Errorf("unknown feature %s", sortedFeatures[i])
	}
	return s, nil
}

// Unmap 返回 FeatureSet 中已置位特征的名字（按位序，即字典序）。
//
// 是 MapSorted 的逆操作：把位图还原成名字列表用于展示或跨组件传递。
// 集合大小不符意味着它来自另一套特征表，位下标的含义已失效，必须先拒绝。
// 逐个取最低位的 1 再清掉，可以跳过所有空位，输出顺序由下标决定，天然有序。
func (m *FeatureMapper) Unmap(s FeatureSet) ([]string, error) {
	if s.size != len(m.registeredFeatures) {
		return nil, fmt.Errorf("feature size mismatch! s.size=%d; len(m.registeredFeatures)=%d", s.size, len(m.registeredFeatures))
	}

	var keys []string
	for i, w := range s.words {
		for w != 0 {
			t := bits.TrailingZeros64(w) // 找到最低位 1 的位置。
			index := i*64 + t
			keys = append(keys, m.registeredFeatures[index])
			w &^= 1 << uint(t) // 清掉刚处理的最低位 1。
		}
	}
	return keys, nil
}
