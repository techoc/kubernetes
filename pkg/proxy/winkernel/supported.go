//go:build windows

/*
Copyright 2017 The Kubernetes Authors.

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

package winkernel

import (
	"fmt"

	"github.com/Microsoft/hnslib"

	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/klog/v2"
	kubefeatures "k8s.io/kubernetes/pkg/features"
)

// KernelCompatTester 用于判断当前 Windows 内核是否具备运行内核模式 proxier 所需的能力。
type KernelCompatTester interface {
	IsCompatible() error
}

// CanUseWinKernelProxier 判断是否应该使用内核模式 Proxier，而不是传统的 userspace Proxier；
// 判断依据是 Windows 内核版本以及所需内核特性是否存在。
func CanUseWinKernelProxier(kcompat KernelCompatTester) (bool, error) {
	// 先让兼容性检查器验证内核版本与所需能力，避免在不兼容的系统上启用内核模式。
	if err := kcompat.IsCompatible(); err != nil {
		return false, err
	}
	return true, nil
}

// WindowsKernelCompatTester 是 KernelCompatTester 的生产实现，通过 HNS 请求判断内核是否兼容。
type WindowsKernelCompatTester struct{}

// IsCompatible 检查 HNS 是否支持策略列表请求；缺少该能力时内核模式代理无法工作。
func (lkct WindowsKernelCompatTester) IsCompatible() error {
	_, err := hnslib.HNSListPolicyListRequest()
	if err != nil {
		return fmt.Errorf("Windows kernel is not compatible for Kernel mode")
	}
	return nil
}

// StackCompatTester 用于判断内核与指定网络是否具备双栈能力。
type StackCompatTester interface {
	DualStackCompatible(networkName string) bool
}

// DualStackCompatTester 是 StackCompatTester 的生产实现，基于 HCN/HNS 查询系统与网络能力。
type DualStackCompatTester struct{}

// DualStackCompatible 判断指定网络能否以双栈模式工作；任一前置检查失败都回退到单栈。
func (t DualStackCompatTester) DualStackCompatible(networkName string) bool {
	hcnImpl := newHcnImpl()
	// 由于早期版本存在 bug，首个能正确检查双栈支持的 hnslib 标签是 v0.8.22；因此必须显式调用该检查。
	if err := hcnImpl.Ipv6DualStackSupported(); err != nil {
		// HCN 查询自身版本（该调用内部先做这件事，再解析是否支持双栈）理论上也可能失败，
		// 但至少从已知情况看，唯一可能的场景是主机早于 1803、当时还不存在 HCN。
		// 若真遇到这种情况，hnslib 理应返回可识别的已知错误供比对；
		// 因此这里把"非该已知错误"的 err 视为"特性不受支持"。
		// 鉴于 k8s 支持的最低版本是 ws2019（1809），基本可以断定此处错误表示主机不支持双栈，
		// 而不是查询本身失败。所以只记 Info 日志告知用户正在回退，不按错误处理。
		klog.InfoS("This version of Windows does not support dual-stack, falling back to single-stack", "err", err.Error())
		return false
	}

	// 再检查网络是否为 overlay：overlay 模式下还需要结合特性门控判断。
	hns, _ := newHostNetworkService(hcnImpl)
	networkName, err := getNetworkName(networkName)
	if err != nil {
		klog.ErrorS(err, "Unable to determine dual-stack status, falling back to single-stack")
		return false
	}
	networkInfo, err := getNetworkInfo(hns, networkName)
	if err != nil {
		klog.ErrorS(err, "Unable to determine dual-stack status, falling back to single-stack")
		return false
	}

	if utilfeature.DefaultFeatureGate.Enabled(kubefeatures.WinOverlay) && isOverlay(networkInfo) {
		// Windows 上的 overlay（VXLAN）网络目前不支持双栈，必须回退到单栈。
		klog.InfoS("Winoverlay does not support dual-stack, falling back to single-stack")
		return false
	}

	return true
}
