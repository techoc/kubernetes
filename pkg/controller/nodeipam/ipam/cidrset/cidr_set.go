/*
Copyright 2016 The Kubernetes Authors.

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

package cidrset

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math/big"
	"math/bits"
	"net"
	"sync"
)

// CidrSet 管理一个集群 CIDR 地址池，可将它划分为若干子网块供节点分配。
// 内部用大整数位图跟踪每个子网的占用情况，并通过互斥锁保证并发安全。
type CidrSet struct {
	sync.Mutex
	// clusterCIDR 是分配给集群的 CIDR 范围。
	clusterCIDR *net.IPNet
	// clusterMaskSize 是集群掩码位数，缓存以避免重复调用 clusterCIDR.Mask.Size()。
	clusterMaskSize int
	// nodeMask 是分配给节点的子网掩码。
	nodeMask net.IPMask
	// nodeMaskSize 是节点掩码位数，缓存以避免重复调用 nodeMask.Size()。
	nodeMaskSize int
	// maxCIDRs 是可分配子网块的最大数量。
	maxCIDRs int
	// allocatedCIDRs 是当前已分配的子网块数量。
	allocatedCIDRs int
	// nextCandidate 指向下一次分配时优先检查的候选块索引，减少线性扫描开销。
	nextCandidate int
	// used 是跟踪子网块分配情况的位图。
	used big.Int
	// label 用于在指标中区分不同的 CIDR 集合。
	label string
}

const (
	// 子网掩码位数最多只能比集群掩码位数大 16。
	// TODO: 参见 https://github.com/kubernetes/kubernetes/issues/44918
	// 受未压缩位图实现限制，clusterSubnetMaxDiff 被限制为 16；
	// 因此 IPv6 集群的子网掩码至少为 48 位（IPv6 默认掩码为 64 位）。
	clusterSubnetMaxDiff = 16
	// halfIPv6Len 是 IPv6 地址长度的一半，用于分高低 64 位处理。
	halfIPv6Len = net.IPv6len / 2
)

var (
	// ErrCIDRRangeNoCIDRsRemaining 表示可分配范围内已无剩余 CIDR。
	ErrCIDRRangeNoCIDRsRemaining = errors.New(
		"CIDR allocation failed; there are no remaining CIDRs left to allocate in the accepted range")
	// ErrCIDRSetSubNetTooBig 表示节点子网掩码相对集群掩码过大。
	ErrCIDRSetSubNetTooBig = errors.New(
		"New CIDR set failed; the node CIDR size is too big")
)

// NewCIDRSet 创建一个新的 CidrSet，并校验子网掩码大小、注册相关指标。
func NewCIDRSet(clusterCIDR *net.IPNet, subNetMaskSize int) (*CidrSet, error) {
	clusterMask := clusterCIDR.Mask
	clusterMaskSize, bits := clusterMask.Size()

	if (clusterCIDR.IP.To4() == nil) && (subNetMaskSize-clusterMaskSize > clusterSubnetMaxDiff) {
		return nil, ErrCIDRSetSubNetTooBig
	}

	// 注册 CIDR 集合的监控指标，仅首次调用生效。
	registerCidrsetMetrics()

	maxCIDRs := getMaxCIDRs(subNetMaskSize, clusterMaskSize)
	cidrSet := &CidrSet{
		clusterCIDR:     clusterCIDR,
		nodeMask:        net.CIDRMask(subNetMaskSize, bits),
		clusterMaskSize: clusterMaskSize,
		maxCIDRs:        maxCIDRs,
		nodeMaskSize:    subNetMaskSize,
		label:           clusterCIDR.String(),
	}
	cidrSetMaxCidrs.WithLabelValues(cidrSet.label).Set(float64(maxCIDRs))

	return cidrSet, nil
}

// indexToCIDRBlock 将块索引转换为对应的 CIDR 子网，区分 IPv4 与 IPv6 计算。
func (s *CidrSet) indexToCIDRBlock(index int) *net.IPNet {
	var ip []byte
	switch /*v4 or v6*/ {
	case s.clusterCIDR.IP.To4() != nil:
		{
			j := uint32(index) << uint32(32-s.nodeMaskSize)
			ipInt := (binary.BigEndian.Uint32(s.clusterCIDR.IP)) | j
			ip = make([]byte, net.IPv4len)
			binary.BigEndian.PutUint32(ip, ipInt)
		}
	case s.clusterCIDR.IP.To16() != nil:
		{
			// leftClusterIP      |     rightClusterIP
			// 2001:0DB8:1234:0000:0000:0000:0000:0000
			const v6NBits = 128
			const halfV6NBits = v6NBits / 2
			leftClusterIP := binary.BigEndian.Uint64(s.clusterCIDR.IP[:halfIPv6Len])
			rightClusterIP := binary.BigEndian.Uint64(s.clusterCIDR.IP[halfIPv6Len:])

			ip = make([]byte, net.IPv6len)

			if s.nodeMaskSize <= halfV6NBits {
				// 子网在高 64 位内即可区分，只需修改左半部分。
				leftClusterIP |= uint64(index) << uint(halfV6NBits-s.nodeMaskSize)
			} else {
				if s.clusterMaskSize < halfV6NBits {
					// 计算索引中有多少位会溢出到左半部分。
					btl := uint(s.nodeMaskSize - halfV6NBits)
					indexMaxBit := uint(64 - bits.LeadingZeros64(uint64(index)))
					if indexMaxBit > btl {
						leftClusterIP |= uint64(index) >> btl
					}
				}
				// 右半部分的计算方式不变：子网掩码大于 64 位时会同时影响左右两侧。
				rightClusterIP |= uint64(index) << uint(v6NBits-s.nodeMaskSize)
			}
			binary.BigEndian.PutUint64(ip[:halfIPv6Len], leftClusterIP)
			binary.BigEndian.PutUint64(ip[halfIPv6Len:], rightClusterIP)
		}
	}
	return &net.IPNet{
		IP:   ip,
		Mask: s.nodeMask,
	}
}

// AllocateNext 分配下一个空闲的 CIDR 子网块并标记为占用。
// 从 nextCandidate 开始环形扫描位图，找到空闲块后更新游标并同步指标。
func (s *CidrSet) AllocateNext() (*net.IPNet, error) {
	s.Lock()
	defer s.Unlock()

	if s.allocatedCIDRs == s.maxCIDRs {
		return nil, ErrCIDRRangeNoCIDRsRemaining
	}
	candidate := s.nextCandidate
	var i int
	for i = 0; i < s.maxCIDRs; i++ {
		// 环形扫描，最多遍历一圈即可确定是否还有空闲块。
		if s.used.Bit(candidate) == 0 {
			break
		}
		candidate = (candidate + 1) % s.maxCIDRs
	}

	s.nextCandidate = (candidate + 1) % s.maxCIDRs
	s.used.SetBit(&s.used, candidate, 1)
	s.allocatedCIDRs++
	// 更新分配次数、每次分配的探测次数与使用率指标。
	cidrSetAllocations.WithLabelValues(s.label).Inc()
	cidrSetAllocationTriesPerRequest.WithLabelValues(s.label).Observe(float64(i))
	cidrSetUsage.WithLabelValues(s.label).Set(float64(s.allocatedCIDRs) / float64(s.maxCIDRs))

	return s.indexToCIDRBlock(candidate), nil
}

// getBeginningAndEndIndices 计算给定 CIDR 覆盖的块索引范围 [begin, end]。
// 先校验该 CIDR 与集群 CIDR 有交集，避免越界释放或占用其他集群的地址。
func (s *CidrSet) getBeginningAndEndIndices(cidr *net.IPNet) (begin, end int, err error) {
	if cidr == nil {
		return -1, -1, fmt.Errorf("error getting indices for cluster cidr %v, cidr is nil", s.clusterCIDR)
	}
	begin, end = 0, s.maxCIDRs-1
	cidrMask := cidr.Mask
	maskSize, _ := cidrMask.Size()
	var ipSize int

	if !s.clusterCIDR.Contains(cidr.IP.Mask(s.clusterCIDR.Mask)) && !cidr.Contains(s.clusterCIDR.IP.Mask(cidr.Mask)) {
		return -1, -1, fmt.Errorf("cidr %v is out the range of cluster cidr %v", cidr, s.clusterCIDR)
	}

	if s.clusterMaskSize < maskSize {

		ipSize = net.IPv4len
		if cidr.IP.To4() == nil {
			ipSize = net.IPv6len
		}
		begin, err = s.getIndexForIP(cidr.IP.Mask(s.nodeMask))
		if err != nil {
			return -1, -1, err
		}
		ip := make([]byte, ipSize)
		if cidr.IP.To4() != nil {
			ipInt := binary.BigEndian.Uint32(cidr.IP) | (^binary.BigEndian.Uint32(cidr.Mask))
			binary.BigEndian.PutUint32(ip, ipInt)
		} else {
			// ipIntLeft          |         ipIntRight
			// 2001:0DB8:1234:0000:0000:0000:0000:0000
			ipIntLeft := binary.BigEndian.Uint64(cidr.IP[:net.IPv6len/2]) | (^binary.BigEndian.Uint64(cidr.Mask[:net.IPv6len/2]))
			ipIntRight := binary.BigEndian.Uint64(cidr.IP[net.IPv6len/2:]) | (^binary.BigEndian.Uint64(cidr.Mask[net.IPv6len/2:]))
			binary.BigEndian.PutUint64(ip[:net.IPv6len/2], ipIntLeft)
			binary.BigEndian.PutUint64(ip[net.IPv6len/2:], ipIntRight)
		}
		end, err = s.getIndexForIP(net.IP(ip).Mask(s.nodeMask))
		if err != nil {
			return -1, -1, err
		}
	}
	return begin, end, nil
}

// Release 释放给定的 CIDR 范围，将其对应的块在位图中清零。
// 只对实际占用的块递减计数，避免重复释放导致计数错误。
func (s *CidrSet) Release(cidr *net.IPNet) error {
	begin, end, err := s.getBeginningAndEndIndices(cidr)
	if err != nil {
		return err
	}
	s.Lock()
	defer s.Unlock()
	for i := begin; i <= end; i++ {
		// 仅当位状态真正变化时才调整计数，防止重复释放导致重复计数。
		if s.used.Bit(i) != 0 {
			s.used.SetBit(&s.used, i, 0)
			s.allocatedCIDRs--
			cidrSetReleases.WithLabelValues(s.label).Inc()
		}
	}

	cidrSetUsage.WithLabelValues(s.label).Set(float64(s.allocatedCIDRs) / float64(s.maxCIDRs))
	return nil
}

// Occupy 将给定 CIDR 范围标记为已占用。
// 对已占用的块是幂等操作，用于从外部状态恢复时重建位图。
func (s *CidrSet) Occupy(cidr *net.IPNet) (err error) {
	begin, end, err := s.getBeginningAndEndIndices(cidr)
	if err != nil {
		return err
	}
	s.Lock()
	defer s.Unlock()
	for i := begin; i <= end; i++ {
		// 仅当位状态真正变化时才调整计数，防止重复占用导致重复计数。
		if s.used.Bit(i) == 0 {
			s.used.SetBit(&s.used, i, 1)
			s.allocatedCIDRs++
			cidrSetAllocations.WithLabelValues(s.label).Inc()
		}
	}

	cidrSetUsage.WithLabelValues(s.label).Set(float64(s.allocatedCIDRs) / float64(s.maxCIDRs))
	return nil
}

// getIndexForIP 将 IP 地址映射为其所属子网块的索引。
// IPv4 直接用整数位移计算，IPv6 用 big.Int 处理 128 位异或与右移。
func (s *CidrSet) getIndexForIP(ip net.IP) (int, error) {
	if ip.To4() != nil {
		cidrIndex := (binary.BigEndian.Uint32(s.clusterCIDR.IP) ^ binary.BigEndian.Uint32(ip.To4())) >> uint32(32-s.nodeMaskSize)
		if cidrIndex >= uint32(s.maxCIDRs) {
			return 0, fmt.Errorf("CIDR: %v/%v is out of the range of CIDR allocator", ip, s.nodeMaskSize)
		}
		return int(cidrIndex), nil
	}
	if ip.To16() != nil {
		bigIP := big.NewInt(0).SetBytes(s.clusterCIDR.IP)
		bigIP = bigIP.Xor(bigIP, big.NewInt(0).SetBytes(ip))
		cidrIndexBig := bigIP.Rsh(bigIP, uint(net.IPv6len*8-s.nodeMaskSize))
		cidrIndex := cidrIndexBig.Uint64()
		if cidrIndex >= uint64(s.maxCIDRs) {
			return 0, fmt.Errorf("CIDR: %v/%v is out of the range of CIDR allocator", ip, s.nodeMaskSize)
		}
		return int(cidrIndex), nil
	}

	return 0, fmt.Errorf("invalid IP: %v", ip)
}

// getMaxCIDRs 返回把 clusterMaskSize 的集群 CIDR 按 subNetMaskSize 划分子网后可得到的最大块数。
func getMaxCIDRs(subNetMaskSize, clusterMaskSize int) int {
	return 1 << uint32(subNetMaskSize-clusterMaskSize)
}
