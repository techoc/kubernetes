/*
Copyright 2024 The Kubernetes Authors.

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

package swap

import (
	"bytes"
	"context"
	"errors"
	"os"
	sysruntime "runtime"
	"strings"
	"sync"

	inuserns "github.com/moby/sys/userns"
	"k8s.io/apimachinery/pkg/util/version"
	"k8s.io/klog/v2"
	utilkernel "k8s.io/kubernetes/pkg/util/kernel"
	"k8s.io/mount-utils"
)

// 缓存探测结果：sync.Once 保证昂贵的内核能力探测只执行一次。
var (
	tmpfsNoswapOptionSupported        bool
	tmpfsNoswapOptionAvailabilityOnce sync.Once
	swapOn                            bool
	swapOnErr                         error
	swapOnOnce                        sync.Once
)

// TmpfsNoswapOption 是 tmpfs 的 noswap 挂载选项。
const TmpfsNoswapOption = "noswap"

// IsTmpfsNoswapOptionSupported 判断当前环境是否支持 tmpfs 的 noswap 选项。
func IsTmpfsNoswapOptionSupported(mounter mount.Interface, mountPath string) bool {
	// TODO: it needs to be replaced by a proper context in the future
	ctx := context.TODO()
	logger := klog.FromContext(ctx)
	isTmpfsNoswapOptionSupportedHelper := func() bool {
		// Windows 没有 tmpfs，直接判定不支持。
		if sysruntime.GOOS == "windows" {
			return false
		}

		if inuserns.RunningInUserNS() {
			// 非特权 user namespace 中无法关闭 tmpfs 的 swap，参见：
			// https://github.com/torvalds/linux/blob/v6.8/mm/shmem.c#L4004-L4011
			// https://github.com/kubernetes/kubernetes/issues/125137
			logger.Info("Running under a user namespace - tmpfs noswap is not supported")
			return false
		}

		// 内核版本足够新时直接返回支持，无需实际挂载测试。
		kernelVersion, err := utilkernel.GetVersion()
		if err != nil {
			logger.Error(err, "cannot determine kernel version, unable to determine is tmpfs noswap is supported")
			return false
		}

		if kernelVersion.AtLeast(version.MustParseGeneric(utilkernel.TmpfsNoswapSupportKernelVersion)) {
			return true
		}

		// 未指定挂载路径时回退到系统临时目录。
		if mountPath == "" {
			logger.Error(errors.New("mount path is empty, falling back to /tmp"), "")
		}

		// 创建临时目录实际挂载一次，验证内核是否接受 noswap 选项。
		mountPath, err = os.MkdirTemp(mountPath, "tmpfs-noswap-test-")
		if err != nil {
			logger.Info("error creating dir to test if tmpfs noswap is enabled. Assuming not supported", "mount path", mountPath, "error", err)
			return false
		}

		// 无论测试成败都清理临时目录，避免残留挂载点。
		defer func() {
			err = os.RemoveAll(mountPath)
			if err != nil {
				logger.Error(err, "error removing test tmpfs dir", "mount path", mountPath)
			}
		}()

		// 绕过 systemd 直接测试内核行为，避免 systemd 对挂载选项的额外处理干扰结果。
		err = mounter.MountSensitiveWithoutSystemd("tmpfs", mountPath, "tmpfs", []string{TmpfsNoswapOption}, nil)
		if err != nil {
			logger.Info("error mounting tmpfs with the noswap option. Assuming not supported", "error", err)
			return false
		}

		err = mounter.Unmount(mountPath)
		if err != nil {
			logger.Error(err, "error unmounting test tmpfs dir", "mount path", mountPath)
		}

		return true
	}

	// 探测结果缓存后复用，后续调用不再重复挂载测试。
	tmpfsNoswapOptionAvailabilityOnce.Do(func() {
		tmpfsNoswapOptionSupported = isTmpfsNoswapOptionSupportedHelper()
	})

	return tmpfsNoswapOptionSupported
}

// isSwapOnAccordingToProcSwaps 接收 /proc/swaps 的内容，判断系统是否启用了 swap。
func isSwapOnAccordingToProcSwaps(logger klog.Logger, procSwapsContent []byte) bool {
	// 去掉末尾换行，避免把空行误判为 swap 条目。
	procSwapsContent = bytes.TrimSpace(procSwapsContent) // extra trailing \n
	procSwapsStr := string(procSwapsContent)
	procSwapsLines := strings.Split(procSwapsStr, "\n")

	// /proc/swaps 只有表头时说明没有 swap；多于一行即存在 swap 设备。
	isSwapOn := len(procSwapsLines) > 1
	if isSwapOn {
		logger.Info("Swap is on", "/proc/swaps contents", procSwapsStr)
	}

	return isSwapOn
}

// IsSwapOn 通过读取 /proc/swaps 判断系统是否启用 swap。
// 文件不存在时视为未启用；其他读取错误原样返回。Windows 上始终假定未启用 swap。
func IsSwapOn() (bool, error) {
	isSwapOnHelper := func() (bool, error) {
		// Windows 没有 /proc/swaps，直接返回 false。
		if sysruntime.GOOS == "windows" {
			return false, nil
		}

		// TODO: it needs to be replaced by a proper context in the future
		ctx := context.TODO()
		logger := klog.FromContext(ctx)

		// Linux 下 swap 状态由 /proc/swaps 暴露。
		const swapFilePath = "/proc/swaps"
		procSwapsContent, err := os.ReadFile(swapFilePath)
		if err != nil {
			if os.IsNotExist(err) {
				logger.Info("File does not exist, assuming that swap is disabled", "path", swapFilePath)
				return false, nil
			}

			return false, err
		}

		return isSwapOnAccordingToProcSwaps(logger, procSwapsContent), nil
	}

	// 结果只探测一次并缓存，避免重复读取 proc 文件。
	swapOnOnce.Do(func() {
		swapOn, swapOnErr = isSwapOnHelper()
	})

	return swapOn, swapOnErr
}
