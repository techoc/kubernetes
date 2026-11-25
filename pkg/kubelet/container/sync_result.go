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

package container

import (
	"errors"
	"fmt"
	"time"

	utilerrors "k8s.io/apimachinery/pkg/util/errors"
)

// BackoffError 用于包装需要向 kubelet 指定退避时长的错误。
type BackoffError struct {
	error       error
	backoffTime time.Time
}

// NewBackoffError 包装一个错误，并携带 kubelet 应等待的重试时间。
func NewBackoffError(err error, backoffTime time.Time) *BackoffError {
	return &BackoffError{
		error:       err,
		backoffTime: backoffTime,
	}
}

// Error 透传底层错误的文本，使 BackoffError 对调用方保持透明。
func (b *BackoffError) Error() string {
	return b.error.Error()
}

// Unwrap 暴露底层错误，使 errors.Is/As 能穿透 BackoffError。
func (b *BackoffError) Unwrap() error {
	return b.error
}

// BackoffTime 返回退避到期时间，调用方据此安排下次重试。
func (e *BackoffError) BackoffTime() time.Time {
	return e.backoffTime
}

// MinBackoffExpiration 递归遍历错误树（包括 Aggregate 与 Unwrap 链），
// 返回所有 BackoffError 中最早的退避到期时间；未找到时 ok 为 false。
func MinBackoffExpiration(err error) (time.Time, bool) {
	var ae utilerrors.Aggregate
	var be *BackoffError
	switch {
	case errors.As(err, &be):
		return be.BackoffTime(), true
	case errors.As(err, &ae):
		// 取最早的到期时间而非第一个遇到的：聚合错误中的多个退避应尽快重试。
		var min time.Time
		found := false
		for _, e := range ae.Errors() {
			if backoff, ok := MinBackoffExpiration(e); ok {
				if !found || backoff.Before(min) {
					min = backoff
					found = true
				}
			}
		}
		return min, found
	default:
		if e := errors.Unwrap(err); e != nil {
			return MinBackoffExpiration(e)
		}
		return time.Time{}, false
	}
}

// TODO(random-liu): 需要更好地组织运行时错误，以便内省。

// ErrCrashLoopBackOff 表示容器已终止且 kubelet 正在退避重启。
var ErrCrashLoopBackOff = errors.New("CrashLoopBackOff")

var (
	// ErrContainerNotFound 表示 kubelet 管理的 Pod 中找不到指定名称的容器。
	ErrContainerNotFound = errors.New("no matching container")
)

var (
	// ErrRunContainer 表示运行时未能启动 Pod 的任一容器。
	ErrRunContainer = errors.New("RunContainerError")
	// ErrKillContainer 表示运行时未能停止 Pod 的任一容器。
	ErrKillContainer = errors.New("KillContainerError")
	// ErrCreatePodSandbox 表示运行时未能为 Pod 创建 sandbox。
	ErrCreatePodSandbox = errors.New("CreatePodSandboxError")
	// ErrConfigPodSandbox 表示运行时无法从 Pod 获取 sandbox 配置。
	ErrConfigPodSandbox = errors.New("ConfigPodSandboxError")
	// ErrKillPodSandbox 表示运行时未能停止 Pod 的 sandbox。
	ErrKillPodSandbox = errors.New("KillPodSandboxError")
	// ErrResizePodInPlace 表示运行时未能原地调整 Pod 资源。
	ErrResizePodInPlace = errors.New("ResizePodInPlaceError")
	// ErrRemoveContainer 表示运行时未能删除容器。
	ErrRemoveContainer = errors.New("RemoveContainerError")
)

// SyncAction 表示 SyncPod()/KillPod() 中的动作类型，
// 目前只涉及容器启停与网络配置/拆除。
type SyncAction string

const (
	// StartContainer 动作。
	StartContainer SyncAction = "StartContainer"
	// KillContainer 动作。
	KillContainer SyncAction = "KillContainer"
	// InitContainer 动作。
	InitContainer SyncAction = "InitContainer"
	// CreatePodSandbox 动作。
	CreatePodSandbox SyncAction = "CreatePodSandbox"
	// ConfigPodSandbox 动作。
	ConfigPodSandbox SyncAction = "ConfigPodSandbox"
	// KillPodSandbox 动作。
	KillPodSandbox SyncAction = "KillPodSandbox"
	// ResizePodInPlace 表示 Pod 内任意容器被原地调整资源（不重启）。
	ResizePodInPlace SyncAction = "ResizePodInPlace"
	// RemoveContainer 动作。
	RemoveContainer SyncAction = "RemoveContainer"
)

// SyncResult 记录单个同步动作的结果。
type SyncResult struct {
	// 结果对应的动作。
	Action SyncAction
	// 动作目标，目前只可能是：
	//  * Container：目标为容器名；
	//  * Network：目标暂无意义，暂用 Pod 全名占位。
	Target interface{}
	// 简要错误原因。
	Error error
	// 人类可读的错误信息。
	Message string
}

// NewSyncResult 创建携带指定动作与目标的 SyncResult。
func NewSyncResult(action SyncAction, target interface{}) *SyncResult {
	return &SyncResult{Action: action, Target: target}
}

// Fail 标记该动作失败，并记录错误与可读信息。
func (r *SyncResult) Fail(err error, msg string) {
	r.Error, r.Message = err, msg
}

// PodSyncResult 汇总 SyncPod()/KillPod() 的全部动作结果。
type PodSyncResult struct {
	// 各同步动作的结果。
	SyncResults []*SyncResult
	// SyncPod()/KillPod() 自身产生、且未包含在 SyncResults 中的错误。
	SyncError error
}

// AddSyncResult 追加若干动作结果。
func (p *PodSyncResult) AddSyncResult(result ...*SyncResult) {
	p.SyncResults = append(p.SyncResults, result...)
}

// AddPodSyncResult 合并另一个 PodSyncResult：动作结果追加，SyncError 直接覆盖。
func (p *PodSyncResult) AddPodSyncResult(result PodSyncResult) {
	p.AddSyncResult(result.SyncResults...)
	p.SyncError = result.SyncError
}

// Fail 记录 SyncPod()/KillPod() 本身的错误。
func (p *PodSyncResult) Fail(err error) {
	p.SyncError = err
}

// Error 把所有动作错误与 SyncError 聚合成一个错误，便于调用方一次性上报。
func (p *PodSyncResult) Error() error {
	// 聚合而非返回首个错误，避免后续动作的失败信息被丢弃。
	errlist := []error{}
	if p.SyncError != nil {
		errlist = append(errlist, fmt.Errorf("failed to SyncPod: %w", p.SyncError))
	}
	for _, result := range p.SyncResults {
		if result.Error != nil {
			errlist = append(errlist, fmt.Errorf("failed to %q for %q with %w: %q", result.Action, result.Target,
				result.Error, result.Message))
		}
	}
	return utilerrors.NewAggregate(errlist)
}
