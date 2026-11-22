//go:build linux

/*
Copyright 2015 The Kubernetes Authors.

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

package oom

import (
	"fmt"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"time"

	cmutil "k8s.io/kubernetes/pkg/kubelet/cm/util"

	"k8s.io/klog/v2"
)

// NewOOMAdjuster 创建一个使用 Linux 真实实现的 OOMAdjuster，
// 其中 PID 列举和 OOM 分数写入均通过 Linux 具体实现注入。
func NewOOMAdjuster() *OOMAdjuster {
	oomAdjuster := &OOMAdjuster{
		pidLister:        getPids,
		ApplyOOMScoreAdj: applyOOMScoreAdj,
	}
	oomAdjuster.ApplyOOMScoreAdjContainer = oomAdjuster.applyOOMScoreAdjContainer
	return oomAdjuster
}

func getPids(cgroupName string) ([]int, error) {
	return cmutil.GetPids(filepath.Join("/", cgroupName))
}

// applyOOMScoreAdj 将 value 写入 /proc/<pid>/oom_score_adj；pid 为 0 时表示当前进程自身。
// 若 pid 不存在，返回 os.ErrNotExist。
// 写入失败时最多重试两次，以容忍进程刚退出或 /proc 项尚不稳定的短暂竞争。
func applyOOMScoreAdj(pid int, oomScoreAdj int) error {
	if pid < 0 {
		return fmt.Errorf("invalid PID %d specified for oom_score_adj", pid)
	}

	var pidStr string
	if pid == 0 {
		pidStr = "self"
	} else {
		pidStr = strconv.Itoa(pid)
	}

	maxTries := 2
	oomScoreAdjPath := path.Join("/proc", pidStr, "oom_score_adj")
	value := strconv.Itoa(oomScoreAdj)
	klog.V(4).Infof("attempting to set %q to %q", oomScoreAdjPath, value)
	var err error
	for i := 0; i < maxTries; i++ {
		err = os.WriteFile(oomScoreAdjPath, []byte(value), 0700)
		if err != nil {
			if os.IsNotExist(err) {
				klog.V(2).Infof("%q does not exist", oomScoreAdjPath)
				return os.ErrNotExist
			}

			klog.V(3).Info(err)
			time.Sleep(100 * time.Millisecond)
			continue
		}
		return nil
	}
	if err != nil {
		klog.V(2).Infof("failed to set %q to %q: %v", oomScoreAdjPath, value, err)
	}
	return err
}

// applyOOMScoreAdjContainer 为 cgroup cgroupName 中的所有进程写入 oom_score_adj。
// 它会持续重试，直到 cgroup 的进程列表稳定或达到 maxTries 次尝试。
func (oomAdjuster *OOMAdjuster) applyOOMScoreAdjContainer(cgroupName string, oomScoreAdj, maxTries int) error {
	adjustedProcessSet := make(map[int]bool)
	for i := 0; i < maxTries; i++ {
		continueAdjusting := false
		pidList, err := oomAdjuster.pidLister(cgroupName)
		if err != nil {
			if os.IsNotExist(err) {
				// 容器已经不存在，无需再调整其进程的 OOM 分数。
				return os.ErrNotExist
			}
			continueAdjusting = true
			klog.V(10).Infof("Error getting process list for cgroup %s: %+v", cgroupName, err)
		} else if len(pidList) == 0 {
			klog.V(10).Infof("Pid list is empty")
			continueAdjusting = true
		} else {
			for _, pid := range pidList {
				if !adjustedProcessSet[pid] {
					klog.V(10).Infof("pid %d needs to be set", pid)
					if err = oomAdjuster.ApplyOOMScoreAdj(pid, oomScoreAdj); err == nil {
						adjustedProcessSet[pid] = true
					} else if err == os.ErrNotExist {
						continue
					} else {
						klog.V(10).Infof("cannot adjust oom score for pid %d - %v", pid, err)
						continueAdjusting = true
					}
					// 调整过程中进程可能随时退出，这类错误无需中断整体流程。
				}
			}
		}
		if !continueAdjusting {
			return nil
		}
		// 存在微小竞争：进程可能在我们写入 OOM 分数前刚 fork 出子进程，
		// 子进程复制了父进程的旧分数；随后我们更新了父进程，但子进程的 PID
		// 短时间内还没出现在 cgroup.procs 中，导致本函数返回时子进程分数未更新。
		// 概率极低，暂不处理。
	}
	return fmt.Errorf("exceeded maxTries, some processes might not have desired OOM score")
}
