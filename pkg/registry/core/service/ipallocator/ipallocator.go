/*
Copyright 2023 The Kubernetes Authors.

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

package ipallocator

import (
	"context"
	"errors"
	"fmt"
	"math"
	"math/big"
	"math/rand"
	"net"
	"net/netip"
	"sync/atomic"
	"time"

	networkingv1 "k8s.io/api/networking/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	networkingv1informers "k8s.io/client-go/informers/networking/v1"
	networkingv1client "k8s.io/client-go/kubernetes/typed/networking/v1"
	networkingv1listers "k8s.io/client-go/listers/networking/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/klog/v2"
	api "k8s.io/kubernetes/pkg/apis/core"
	netutils "k8s.io/utils/net"
	utiltrace "k8s.io/utils/trace"
)

const ControllerName = "ipallocator.k8s.io"

// Allocator 管理单个 CIDR 内的 IP 地址，使用 IPAddress API 对象记录地址分配关系，并通过 Informer 缓存提供查询能力。
type Allocator struct {
	cidr          *net.IPNet
	prefix        netip.Prefix
	firstAddress  netip.Addr   // 地址范围内的首个 IP 地址。
	offsetAddress netip.Addr   // 分隔上方动态区和下方预留区的 IP 地址。
	lastAddress   netip.Addr   // 地址范围内的最后一个 IP 地址。
	family        api.IPFamily // 该地址范围所属的 IP 协议族。

	rangeOffset int    // 划分地址范围，使动态分配优先使用上方区间。
	size        uint64 // 将可分配 IP 总数限制在 maxInt64 以内。

	client          networkingv1client.NetworkingV1Interface
	ipAddressLister networkingv1listers.IPAddressLister
	ipAddressSynced cache.InformerSynced
	// ready 表示分配器是否可以分配新 IP；只有关联的 ServiceCIDR 就绪后才允许分配。
	ready atomic.Bool

	// metrics 负责记录分配器指标，也可以禁用。
	metrics     metricsRecorderInterface
	metricLabel string

	rand *rand.Rand
}

var _ Interface = &Allocator{}

// NewIPAllocator 创建单一 CIDR 的 ClusterIP 分配器，使用 IPAddress 对象跟踪已分配地址，并通过 Informer 缓存查询占用状态。
func NewIPAllocator(
	cidr *net.IPNet,
	client networkingv1client.NetworkingV1Interface,
	ipAddressInformer networkingv1informers.IPAddressInformer,
) (*Allocator, error) {
	prefix, err := netip.ParsePrefix(cidr.String())
	if err != nil {
		return nil, err
	}

	if prefix.Addr().Is6() && prefix.Bits() < 64 {
		return nil, fmt.Errorf("shortest allowed prefix length for service CIDR is 64, got %d", prefix.Bits())
	}

	// TODO：相关 utils/net 方法可用后，改为复用该实现。
	size := hostsPerNetwork(cidr)
	var family api.IPFamily
	if netutils.IsIPv6CIDR(cidr) {
		family = api.IPv6Protocol
	} else {
		family = api.IPv4Protocol
	}
	// 缓存首地址、偏移地址和末地址后，可在搜索循环中直接使用 netip.Addr 迭代器，
	// 避免反复进行 IP 地址格式转换。
	// 不分配网络地址（通常以 .0 结尾）。
	ipFirst := prefix.Masked().Addr().Next()
	// IPv6 没有广播地址，此处使用地址范围末地址作为最后一个边界。
	ipLast, err := broadcastAddress(prefix)
	if err != nil {
		return nil, err
	}
	// IPv4 不分配网络广播地址。
	if family == api.IPv4Protocol {
		ipLast = ipLast.Prev()
	}
	// KEP-3070：为 Service IP 地址的动态分配和静态分配预留不同的地址区间。
	// 计算动态分配区与静态预留区之间的偏移边界。
	rangeOffset := calculateRangeOffset(cidr)
	offsetAddress, err := addOffsetAddress(ipFirst, uint64(rangeOffset))
	if err != nil {
		return nil, err
	}
	a := &Allocator{
		cidr:            cidr,
		prefix:          prefix,
		firstAddress:    ipFirst,
		lastAddress:     ipLast,
		rangeOffset:     rangeOffset,
		offsetAddress:   offsetAddress,
		size:            size,
		family:          family,
		client:          client,
		ipAddressLister: ipAddressInformer.Lister(),
		ipAddressSynced: ipAddressInformer.Informer().HasSynced,
		metrics:         &emptyMetricsRecorder{}, // 默认禁用指标记录。
		metricLabel:     cidr.String(),
		rand:            rand.New(rand.NewSource(time.Now().UnixNano())),
	}
	a.ready.Store(true)
	return a, nil
}

func (a *Allocator) createIPAddress(name string, svc *api.Service, scope string) error {
	ipAddress := networkingv1.IPAddress{
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
			Labels: map[string]string{
				networkingv1.LabelIPAddressFamily: string(a.IPFamily()),
				networkingv1.LabelManagedBy:       ControllerName,
			},
		},
		Spec: networkingv1.IPAddressSpec{
			ParentRef: serviceToRef(svc),
		},
	}
	_, err := a.client.IPAddresses().Create(context.Background(), &ipAddress, metav1.CreateOptions{})
	if err != nil {
		// 更新指标
		a.metrics.incrementAllocationErrors(a.metricLabel, scope)
		if apierrors.IsAlreadyExists(err) {
			return ErrAllocated
		}
		return err
	}
	// 更新指标
	a.metrics.incrementAllocations(a.metricLabel, scope)
	a.metrics.setAllocated(a.metricLabel, a.Used())
	a.metrics.setAvailable(a.metricLabel, a.Free())
	return nil
}

// Allocate 尝试预留指定 IP。IP 不在范围内时返回 ErrNotInRange，已被占用时返回 ErrAllocated，地址池耗尽时返回 ErrFull。
// 该方法仅供测试使用；由于创建 IPAddress 对象需要 Service 引用，未提供引用时可能创建失败。
func (a *Allocator) Allocate(ip net.IP) error {
	return a.AllocateService(nil, ip)
}

// AllocateService 尝试为 Service 预留指定 IP，并创建记录所有权关系的 IPAddress 对象。
// IP 不在范围内时返回 ErrNotInRange，已被占用时返回 ErrAllocated，地址池耗尽时返回 ErrFull。
func (a *Allocator) AllocateService(svc *api.Service, ip net.IP) error {
	return a.allocateService(svc, ip, dryRunFalse)
}

// allocateService 预留指定 IP；启用 IPAddress API 时同时创建引用 Service 的对象，并在失败时避免位图与对象状态不一致。
func (a *Allocator) allocateService(svc *api.Service, ip net.IP, dryRun bool) error {
	if !a.ready.Load() || !a.ipAddressSynced() {
		return ErrNotReady
	}
	addr, err := netip.ParseAddr(ip.String())
	if err != nil {
		return err
	}

	// 检查地址是否位于可分配范围内。
	if addr.Less(a.firstAddress) || // 请求地址小于子网的首个可分配地址。
		a.lastAddress.Less(addr) { // 子网最后一个可分配地址小于请求地址。
		if !dryRun {
			// 更新指标
			a.metrics.incrementAllocationErrors(a.metricLabel, "static")
		}
		return &ErrNotInRange{ip, a.prefix.String()}
	}
	if dryRun {
		return nil
	}
	start := time.Now()
	err = a.createIPAddress(ip.String(), svc, "static")
	if err != nil {
		return err
	}
	a.metrics.setLatency(a.metricLabel, time.Since(start))
	return nil
}

// AllocateNext 返回一个尚未分配的 IP。该方法仅供测试使用；由于创建 IPAddress 对象需要 Service 引用，未提供引用时可能创建失败。
func (a *Allocator) AllocateNext() (net.IP, error) {
	return a.AllocateNextService(nil)
}

// AllocateNextService 为 Service 自动分配一个尚未占用的 IP，并创建对应的 IPAddress 对象。
func (a *Allocator) AllocateNextService(svc *api.Service) (net.IP, error) {
	return a.allocateNextService(svc, dryRunFalse)
}

// allocateNextService 从 CIDR 中选择下一个可用地址，跳过网络/广播/保留地址，并按需要创建 IPAddress 对象。
// 较大的子网会按 a.rangeOffset 划分为两个区间：先从上方区间随机起点搜索，耗尽后再回退到下方区间。
func (a *Allocator) allocateNextService(svc *api.Service, dryRun bool) (net.IP, error) {
	if !a.ready.Load() || !a.ipAddressSynced() {
		return nil, ErrNotReady
	}
	if dryRun {
		// 不必额外查找空闲地址：该检查存在竞争窗口，进一步传递状态也无法保证后续分配成功。
		return a.CIDR().IP, nil
	}

	trace := utiltrace.New("allocate dynamic ClusterIP address")
	defer trace.LogIfLong(500 * time.Millisecond)
	start := time.Now()

	// rand.Int63n 在 n <= 0 时会 panic，因此从 uint64 转为 int64 前必须避免该情况。
	rangeSize := a.size - uint64(a.rangeOffset)
	var offset uint64
	switch {
	case rangeSize >= math.MaxInt64:
		offset = a.rand.Uint64()
		// a.offsetAddress + offset 不应超出 64 位 CIDR 的地址范围。
		if math.MaxUint64-offset < uint64(a.rangeOffset) {
			offset -= uint64(a.rangeOffset)
		}
	case rangeSize == 0:
		return net.IP{}, ErrFull
	default:
		offset = uint64(a.rand.Int63n(int64(rangeSize)))
	}
	iterator := ipIterator(a.offsetAddress, a.lastAddress, offset)
	ip, err := a.allocateFromRange(iterator, svc)
	if err == nil {
		a.metrics.setLatency(a.metricLabel, time.Since(start))
		return ip, nil
	}
	// 上方区间已满时，尝试从下方区间分配。
	if errors.Is(err, ErrFull) && a.rangeOffset != 0 {
		offset = uint64(a.rand.Intn(a.rangeOffset))
		iterator = ipIterator(a.firstAddress, a.offsetAddress.Prev(), offset)
		ip, err = a.allocateFromRange(iterator, svc)
		if err == nil {
			a.metrics.setLatency(a.metricLabel, time.Since(start))
			return ip, nil
		}
	}
	// 没有可用 IP。
	if err == nil {
		klog.Info("[SHOULD NOT HAPPEN] - No IP Allocated and no error")
		err = ErrFull
	}
	a.metrics.incrementAllocationErrors(a.metricLabel, "dynamic")
	return net.IP{}, err
}

// ipIterator 按指定 offset 在首尾地址之间迭代，支持随机起点并在遍历到末尾时回绕。
// 遍历结束时返回无效地址。
func ipIterator(first netip.Addr, last netip.Addr, offset uint64) func() netip.Addr {
	// IP 地址不支持取模运算，需要手动回绕。
	modulo := func(addr netip.Addr) netip.Addr {
		if addr.Compare(last) == 1 {
			return first
		}
		return addr
	}
	next := func(addr netip.Addr) netip.Addr {
		return modulo(addr.Next())
	}
	start, err := addOffsetAddress(first, offset)
	if err != nil {
		return func() netip.Addr { return netip.Addr{} }
	}
	start = modulo(start)
	ip := start
	seen := false
	return func() netip.Addr {
		value := ip
		// 判断是首次还是最后一次迭代。
		if value == start {
			if seen {
				return netip.Addr{}
			}
			seen = true
		}
		ip = next(ip)
		return value
	}

}

// allocateFromRange 从包含首尾地址的区间内分配空闲 IP。
// TODO：当前采用线性搜索，仍有优化空间。
func (a *Allocator) allocateFromRange(iterator func() netip.Addr, svc *api.Service) (net.IP, error) {
	for {
		ip := iterator()
		if !ip.IsValid() {
			break
		}
		name := ip.String()
		_, err := a.ipAddressLister.Get(name)
		// IP 已存在时继续尝试下一个地址。
		if err == nil {
			continue
		}
		if !apierrors.IsNotFound(err) {
			klog.Infof("unexpected error: %v", err)
			continue
		}
		// 缓存中不存在该地址，尝试分配。
		err = a.createIPAddress(name, svc, "dynamic")
		// TOCTOU 竞争可能导致其他实例先分配该地址；此时忽略冲突并尝试下一个 IP，其他错误则终止分配。
		// 参考：https://issues.k8s.io/135333
		if err != nil {
			if errors.Is(err, ErrAllocated) {
				klog.Infof("mid-air collision trying to allocate IP %s that already exists, retrying ...", name)
				continue
			}
			klog.Infof("can not create IPAddress %s: %v", name, err)
			return nil, err
		}
		return ip.AsSlice(), nil
	}
	return net.IP{}, ErrFull
}

// Release 将 IP 归还地址池。释放未分配或超出范围的 IP 时不执行任何操作，也不会返回错误。
func (a *Allocator) Release(ip net.IP) error {
	return a.release(ip, dryRunFalse)
}

// release 释放位图占用并删除对应 IPAddress 记录；DryRun 只模拟结果而不修改实际状态。
func (a *Allocator) release(ip net.IP, dryRun bool) error {
	if dryRun {
		return nil
	}
	name := ip.String()
	// 不依赖缓存状态直接删除 IPAddress；为保持兼容性忽略删除错误。
	err := a.client.IPAddresses().Delete(context.Background(), name, metav1.DeleteOptions{})
	if err == nil {
		// 更新指标
		a.metrics.setAllocated(a.metricLabel, a.Used())
		a.metrics.setAvailable(a.metricLabel, a.Free())
		return nil
	}
	klog.Infof("error releasing ip %s : %v", name, err)
	return nil
}

// ForEach 对每个已分配的 IP 执行指定函数，仅为满足 Allocator 接口而实现。
func (a *Allocator) ForEach(f func(net.IP)) {
	ipLabelSelector := labels.Set(map[string]string{
		networkingv1.LabelIPAddressFamily: string(a.IPFamily()),
		networkingv1.LabelManagedBy:       ControllerName,
	}).AsSelectorPreValidated()
	ips, err := a.ipAddressLister.List(ipLabelSelector)
	if err != nil {
		return
	}
	for _, ip := range ips {
		f(netutils.ParseIPSloppy(ip.Name))
	}
}

func (a *Allocator) CIDR() net.IPNet {
	return *a.cidr
}

// 该方法仅供测试使用。
func (a *Allocator) Has(ip net.IP) bool {
	// 将 IP 转换为 IPAddress 对象名称。
	name := ip.String()
	ipAddress, err := a.client.IPAddresses().Get(context.Background(), name, metav1.GetOptions{})
	if err != nil || len(ipAddress.Name) == 0 {
		return false
	}
	return true
}

func (a *Allocator) IPFamily() api.IPFamily {
	return a.family
}

// 该统计方法仅供测试使用，并假设该 IP 协议族只对应一个分配器。
func (a *Allocator) Used() int {
	ipLabelSelector := labels.Set(map[string]string{
		networkingv1.LabelIPAddressFamily: string(a.IPFamily()),
		networkingv1.LabelManagedBy:       ControllerName,
	}).AsSelectorPreValidated()
	ips, err := a.ipAddressLister.List(ipLabelSelector)
	if err != nil {
		return 0
	}

	// 只统计属于当前分配器 CIDR 的 IP。
	count := 0
	for _, ipAddress := range ips {
		// 将 IP 地址字符串解析为 netip.Addr。
		ip, err := netip.ParseAddr(ipAddress.Name)
		if err != nil {
			continue
		}
		// 只统计可解析且位于当前分配器 CIDR 范围内的 IP。
		if a.prefix.Contains(ip) {
			count++
		}
	}
	return count
}

// 该统计方法仅供测试使用，并假设该 IP 协议族只对应一个分配器。
func (a *Allocator) Free() int {
	used := a.Used()

	// 防止整数溢出：a.size 超过 int 最大值时改用 MaxInt。
	if a.size > math.MaxInt {
		// 此时 used 必然小于 MaxInt，不会返回负值。
		return math.MaxInt - used
	}

	size := int(a.size)

	// 数据不一致时避免返回负值。
	if used > size {
		// 通常表示数据不一致，记录警告并返回 0。
		klog.Warningf("IP allocator inconsistency detected: used (%d) > size (%d) for CIDR %s",
			used, size, a.cidr.String())
		return 0
	}

	return size - used
}

// Destroy 释放分配器资源；当前实现无需额外清理。
func (a *Allocator) Destroy() {
}

// DryRun 返回不持久化修改的试运行分配器。
func (a *Allocator) DryRun() Interface {
	return dryRunAllocator{a}
}

// EnableMetrics 启用指标记录。
func (a *Allocator) EnableMetrics() {
	registerMetrics()
	a.metrics = &metricsRecorder{}
}

// dryRunAllocator 包装真实分配器但不持久化修改，供 Service dry-run 流程验证地址是否可用。
type dryRunAllocator struct {
	real *Allocator
}

func (dry dryRunAllocator) Allocate(ip net.IP) error {
	return dry.real.allocateService(nil, ip, dryRunTrue)

}

func (dry dryRunAllocator) AllocateNext() (net.IP, error) {
	return dry.real.allocateNextService(nil, dryRunTrue)
}

func (dry dryRunAllocator) Release(ip net.IP) error {
	return dry.real.release(ip, dryRunTrue)
}

func (dry dryRunAllocator) ForEach(cb func(net.IP)) {
	dry.real.ForEach(cb)
}

func (dry dryRunAllocator) CIDR() net.IPNet {
	return dry.real.CIDR()
}

func (dry dryRunAllocator) IPFamily() api.IPFamily {
	return dry.real.IPFamily()
}

func (dry dryRunAllocator) DryRun() Interface {
	return dry
}

func (dry dryRunAllocator) Has(ip net.IP) bool {
	return dry.real.Has(ip)
}

func (dry dryRunAllocator) Destroy() {
}

func (dry dryRunAllocator) EnableMetrics() {
}

// addOffsetAddress 在 IPv4/IPv6 地址的整数表示上加偏移，并检测地址位宽溢出。
// TODO：迁移到 k8s.io/utils/net；功能与当前 AddIPOffset() 相同，但使用 netip.Addr 而非 net.IP。
func addOffsetAddress(address netip.Addr, offset uint64) (netip.Addr, error) {
	addressBytes := address.AsSlice()
	addressBig := big.NewInt(0).SetBytes(addressBytes)
	r := big.NewInt(0).Add(addressBig, big.NewInt(0).SetUint64(offset)).Bytes()
	// r 的长度必须为 4 或 16 字节，具体取决于 IP 协议族。
	// big.Int 转字节数组时会丢弃前导零，因此需要手动补齐。
	lenDiff := len(addressBytes) - len(r)
	if lenDiff > 0 {
		r = append(make([]byte, lenDiff), r...)
	} else if lenDiff < 0 {
		return netip.Addr{}, fmt.Errorf("invalid address %v", r)
	}
	addr, ok := netip.AddrFromSlice(r)
	if !ok {
		return netip.Addr{}, fmt.Errorf("invalid address %v", r)
	}
	return addr, nil
}

// hostsPerNetwork 返回一个子网中可用的主机数量。
// 上限受 uint64 位宽限制。
// 主机数按如下公式计算：
// IPv4：2^x – 2，不含网络地址和广播地址；
// IPv6：2^x - 1，不含网络地址。
// 其中 x 为子网中主机位的位数。
func hostsPerNetwork(subnet *net.IPNet) uint64 {
	ones, bits := subnet.Mask.Size()
	// 这里检查是否超出 int64 的范围
	if bits-ones >= 64 {
		return math.MaxUint64
	}
	max := uint64(1) << uint(bits-ones)
	// 不使用网络的 ".0" 地址，
	if max == 0 {
		return 0
	}
	max--
	if netutils.IsIPv4CIDR(subnet) {
		// 也不使用 IPv4 网络的广播地址
		if max == 0 {
			return 0
		}
		max--
	}
	return max
}

// broadcastAddress 返回子网的广播地址。
// 求法是把子网中所有主机位置为 1，例如：
// 网络 192.168.0.0/24：网络位 24，主机位 32 - 24 = 8
// 广播地址 192.168.0.255
func broadcastAddress(subnet netip.Prefix) (netip.Addr, error) {
	base := subnet.Masked().Addr()
	bytes := base.AsSlice()
	// 取出子网的主机位
	n := 8*len(bytes) - subnet.Bits()
	// 把所有主机位置为 1
	for i := len(bytes) - 1; i >= 0 && n > 0; i-- {
		if n >= 8 {
			bytes[i] = 0xff
			n -= 8
		} else {
			mask := ^uint8(0) >> (8 - n)
			bytes[i] |= mask
			break
		}
	}

	addr, ok := netip.AddrFromSlice(bytes)
	if !ok {
		return netip.Addr{}, fmt.Errorf("invalid address %v", bytes)
	}
	return addr, nil
}

// serviceToRef 构造 Service 的 ParentRef
func serviceToRef(svc *api.Service) *networkingv1.ParentReference {
	if svc == nil {
		return nil
	}

	return &networkingv1.ParentReference{
		Group:     "",
		Resource:  "services",
		Namespace: svc.Namespace,
		Name:      svc.Name,
	}
}
