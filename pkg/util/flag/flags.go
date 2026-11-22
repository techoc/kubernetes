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

package flag

import (
	"fmt"
	"net"
	"sort"
	"strconv"
	"strings"

	"github.com/spf13/pflag"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	utilnet "k8s.io/apimachinery/pkg/util/net"
	corev1helper "k8s.io/kubernetes/pkg/apis/core/v1/helper"
	kubeletconfig "k8s.io/kubernetes/pkg/kubelet/apis/config"
	utiltaints "k8s.io/kubernetes/pkg/util/taints"
	netutils "k8s.io/utils/net"
)

// TODO(mikedanese): 移除命令行 flag 时一并删除这些 flag 包装类型

var (
	_ pflag.Value = &IPVar{}
	_ pflag.Value = &IPPortVar{}
	_ pflag.Value = &PortRangeVar{}
	_ pflag.Value = &ReservedMemoryVar{}
	_ pflag.Value = &RegisterWithTaintsVar{}
)

// IPVar 用于校验表示 IP 的命令行选项，实现了 pflag.Value 接口。
type IPVar struct {
	Val *string
}

// Set 解析并设置命令行 flag 的值
func (v *IPVar) Set(s string) error {
	if len(s) == 0 {
		v.Val = nil
		return nil
	}
	if netutils.ParseIPSloppy(s) == nil {
		return fmt.Errorf("%q is not a valid IP address", s)
	}
	if v.Val == nil {
		// 这是调用方的编程错误，直接 panic 即可
		panic("the string pointer passed into IPVar should not be nil")
	}
	*v.Val = s
	return nil
}

// String 返回命令行 flag 当前的值
func (v *IPVar) String() string {
	if v.Val == nil {
		return ""
	}
	return *v.Val
}

// Type 返回命令行 flag 的类型名称
func (v *IPVar) Type() string {
	return "ip"
}

// IPPortVar 用于校验表示 IP 和端口的命令行选项，实现了 pflag.Value 接口。
type IPPortVar struct {
	Val *string
}

// Set 解析并设置命令行 flag 的值
func (v *IPPortVar) Set(s string) error {
	if len(s) == 0 {
		v.Val = nil
		return nil
	}

	if v.Val == nil {
		// 这是调用方的编程错误，直接 panic 即可
		panic("the string pointer passed into IPPortVar should not be nil")
	}

	// IP 和 IP:port 两种格式都合法。
	// 先尝试按纯 IP 解析。
	if netutils.ParseIPSloppy(s) != nil {
		*v.Val = s
		return nil
	}

	// 按 IP 解析失败，再按 IP:port 解析。
	host, port, err := net.SplitHostPort(s)
	if err != nil {
		return fmt.Errorf("%q is not in a valid format (ip or ip:port): %v", s, err)
	}
	if netutils.ParseIPSloppy(host) == nil {
		return fmt.Errorf("%q is not a valid IP address", host)
	}
	if _, err := netutils.ParsePort(port, true); err != nil {
		return fmt.Errorf("%q is not a valid number", port)
	}
	*v.Val = s
	return nil
}

// String 返回命令行 flag 当前的值
func (v *IPPortVar) String() string {
	if v.Val == nil {
		return ""
	}
	return *v.Val
}

// Type 返回命令行 flag 的类型名称
func (v *IPPortVar) Type() string {
	return "ipport"
}

// PortRangeVar 用于校验表示端口范围的命令行选项，实现了 pflag.Value 接口。
type PortRangeVar struct {
	Val *string
}

// Set 解析并设置命令行 flag 的值
func (v PortRangeVar) Set(s string) error {
	if _, err := utilnet.ParsePortRange(s); err != nil {
		return fmt.Errorf("%q is not a valid port range: %v", s, err)
	}
	if v.Val == nil {
		// 这是调用方的编程错误，直接 panic 即可
		panic("the string pointer passed into PortRangeVar should not be nil")
	}
	*v.Val = s
	return nil
}

// String 返回命令行 flag 当前的值
func (v PortRangeVar) String() string {
	if v.Val == nil {
		return ""
	}
	return *v.Val
}

// Type 返回命令行 flag 的类型名称
func (v PortRangeVar) Type() string {
	return "port-range"
}

// ReservedMemoryVar 用于校验表示预留内存的命令行选项，实现了 pflag.Value 接口。
type ReservedMemoryVar struct {
	Value       *[]kubeletconfig.MemoryReservation
	initialized bool // 第一次调用 Set 后置为 true
}

// Set 解析并设置命令行 flag 的值
func (v *ReservedMemoryVar) Set(s string) error {
	if v.Value == nil {
		return fmt.Errorf("no target (nil pointer to *[]MemoryReservation)")
	}

	if s == "" {
		v.Value = nil
		return nil
	}

	if !v.initialized || *v.Value == nil {
		*v.Value = make([]kubeletconfig.MemoryReservation, 0)
		v.initialized = true
	}

	if s == "" {
		return nil
	}

	numaNodeReservations := strings.Split(s, ";")
	for _, reservation := range numaNodeReservations {
		numaNodeReservation := strings.Split(reservation, ":")
		if len(numaNodeReservation) != 2 {
			return fmt.Errorf("the reserved memory has incorrect format, expected numaNodeID:type=quantity[,type=quantity...], got %s", reservation)
		}
		memoryTypeReservations := strings.Split(numaNodeReservation[1], ",")
		if len(memoryTypeReservations) < 1 {
			return fmt.Errorf("the reserved memory has incorrect format, expected numaNodeID:type=quantity[,type=quantity...], got %s", reservation)
		}
		numaNodeID, err := strconv.Atoi(numaNodeReservation[0])
		if err != nil {
			return fmt.Errorf("failed to convert the NUMA node ID, expected integer, got %s", numaNodeReservation[0])
		}

		memoryReservation := kubeletconfig.MemoryReservation{
			NumaNode: int32(numaNodeID),
			Limits:   map[v1.ResourceName]resource.Quantity{},
		}

		for _, memoryTypeReservation := range memoryTypeReservations {
			limit := strings.Split(memoryTypeReservation, "=")
			if len(limit) != 2 {
				return fmt.Errorf("the reserved limit has incorrect value, expected type=quantity, got %s", memoryTypeReservation)
			}

			resourceName := v1.ResourceName(limit[0])
			if resourceName != v1.ResourceMemory && !corev1helper.IsHugePageResourceName(resourceName) {
				return fmt.Errorf("memory type conversion error, unknown type: %q", resourceName)
			}

			q, err := resource.ParseQuantity(limit[1])
			if err != nil {
				return fmt.Errorf("failed to parse the quantity: %s", limit[1])
			}

			memoryReservation.Limits[v1.ResourceName(limit[0])] = q
		}
		*v.Value = append(*v.Value, memoryReservation)
	}
	return nil
}

// String 返回命令行 flag 当前的值
func (v *ReservedMemoryVar) String() string {
	if v == nil || v.Value == nil {
		return ""
	}

	var slices []string
	for _, reservedMemory := range *v.Value {
		var limits []string
		for resourceName, q := range reservedMemory.Limits {
			limits = append(limits, fmt.Sprintf("%s=%s", resourceName, q.String()))
		}

		sort.Strings(limits)
		slices = append(slices, fmt.Sprintf("%d:%s", reservedMemory.NumaNode, strings.Join(limits, ",")))
	}

	sort.Strings(slices)
	return strings.Join(slices, ",")
}

// Type 返回命令行 flag 的类型名称
func (v *ReservedMemoryVar) Type() string {
	return "reserved-memory"
}

// RegisterWithTaintsVar 用于校验表示节点注册时所带 taints 的命令行选项，
// 实现了 pflag.Value 接口。
type RegisterWithTaintsVar struct {
	Value *[]v1.Taint
}

// Set 解析并设置命令行 flag 的值
func (t RegisterWithTaintsVar) Set(s string) error {
	if t.Value == nil {
		return fmt.Errorf("no target (nil pointer to []v1.Taint)")
	}

	if len(s) == 0 {
		*t.Value = nil
		return nil
	}
	sts := strings.Split(s, ",")
	corev1Taints, _, err := utiltaints.ParseTaints(sts)
	if err != nil {
		return err
	}
	var taints []v1.Taint
	for _, ct := range corev1Taints {
		taints = append(taints, v1.Taint{Key: ct.Key, Value: ct.Value, Effect: ct.Effect})
	}
	*t.Value = taints
	return nil
}

// String 返回命令行 flag 当前的值
func (t RegisterWithTaintsVar) String() string {
	if t.Value == nil || len(*t.Value) == 0 {
		return ""
	}
	var taints []string
	for _, taint := range *t.Value {
		taints = append(taints, fmt.Sprintf("%s=%s:%s", taint.Key, taint.Value, taint.Effect))
	}
	return strings.Join(taints, ",")
}

// Type 返回命令行 flag 的类型名称
func (t RegisterWithTaintsVar) Type() string {
	return "[]v1.Taint"
}
