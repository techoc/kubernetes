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

// Package exec 实现 Exec 探针，通过在容器内执行命令来检查健康状态。
//
// Exec 探针在容器内运行指定命令，根据退出码判断健康状态：
//   - 退出码 0：成功（Success）
//   - 退出码非 0：失败（Failure）
//   - 命令执行出错（如超时、无法启动）：返回 Unknown 或 Failure
//
// 输出限制为 10KB，防止命令输出过大占用内存。
package exec

import (
	"bytes"
	"errors"

	remote "k8s.io/cri-client/pkg"
	"k8s.io/kubernetes/pkg/kubelet/util/ioutils"
	"k8s.io/kubernetes/pkg/probe"

	"k8s.io/klog/v2"
	"k8s.io/utils/exec"
)

const (
	// maxReadLength 是命令输出读取的最大长度（10KB）
	// 限制输出大小以防止内存占用过大，同时保留足够的调试信息
	maxReadLength = 10 * 1 << 10 // 10KB
)

// New 创建 Exec 探针。
func New() Prober {
	return execProber{}
}

// Prober 是 Exec 探针接口，定义通过执行命令进行容器就绪/存活检查的方法。
type Prober interface {
	// Probe 执行命令并返回结果、命令输出和错误
	Probe(e exec.Cmd) (probe.Result, string, error)
}

type execProber struct{}

// Probe 在容器内执行命令以检查存活/就绪状态。
//
// 返回结果、命令输出和错误：
//   - 退出码 0：Success
//   - 退出码非 0：Failure
//   - 命令超时：Failure
//   - 其他执行错误（如无法启动命令）：Unknown
func (pr execProber) Probe(e exec.Cmd) (probe.Result, string, error) {
	var dataBuffer bytes.Buffer
	// 限制输出写入长度，最多 10KB
	writer := ioutils.LimitWriter(&dataBuffer, maxReadLength)

	// 将 stdout 和 stderr 都重定向到限制写入器
	e.SetStderr(writer)
	e.SetStdout(writer)
	err := e.Start()
	if err == nil {
		err = e.Wait()
	}
	data := dataBuffer.Bytes()

	klog.V(4).Infof("Exec probe response: %q", string(data))
	if err != nil {
		exit, ok := err.(exec.ExitError)
		if ok {
			// 命令执行完成但返回退出码
			if exit.ExitStatus() == 0 {
				return probe.Success, string(data), nil
			}
			// 非零退出码表示探针失败
			return probe.Failure, string(data), nil
		}

		// 命令超时是明确的失败信号
		if errors.Is(err, remote.ErrCommandTimedOut) {
			return probe.Failure, err.Error(), nil
		}

		// 其他执行错误（如命令不存在、权限不足）返回 Unknown
		return probe.Unknown, "", err
	}
	return probe.Success, string(data), nil
}
