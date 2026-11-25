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

package leaderelection

import (
	"context"
	"os"
	"time"

	"k8s.io/apimachinery/pkg/util/uuid"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/leaderelection"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
	"k8s.io/klog/v2"
)

// LeaderElectionTimers 保存协调式选主的租约定时器配置。
type LeaderElectionTimers struct {
	LeaseDuration time.Duration
	RenewDeadline time.Duration
	RetryPeriod   time.Duration
}

// NewRunner 构造选主成功后运行的 worker 函数。
type NewRunner func() (func(ctx context.Context, workers int), error)

// RunWithLeaderElection 在选主成功后运行给定的 runner。
// newRunnerFn 可能被调用多次，每次应返回另一个控制器实例的 Run 方法。
// 获取主机名失败或上下文结束时返回；选主或 runner 启动失败会按 RetryPeriod 重试。

func RunWithLeaderElection(ctx context.Context, config *rest.Config, newRunnerFn NewRunner, timers LeaderElectionTimers) {
	hostname, err := os.Hostname()
	if err != nil {
		klog.Infof("Error parsing hostname: %v", err)
		return
	}
	identity := hostname + "_" + string(uuid.NewUUID())

	wait.UntilWithContext(ctx, func(ctx context.Context) {
		callbacks := leaderelection.LeaderCallbacks{
			OnStartedLeading: func(ctx context.Context) {
				var err error
				run, err := newRunnerFn()
				if err != nil {
					klog.Infof("Error creating runner: %v", err)
					return
				}
				run(ctx, 1)
			},
			OnStoppedLeading: func() {
			},
		}

		rl, err := resourcelock.NewFromKubeconfig(
			"leases",
			"kube-system",
			controllerName,
			resourcelock.ResourceLockConfig{
				Identity: identity,
			},
			config,
			10,
		)
		if err != nil {
			klog.Infof("Error creating resourcelock: %v", err)
			return
		}

		le, err := leaderelection.NewLeaderElector(leaderelection.LeaderElectionConfig{
			Lock:            rl,
			LeaseDuration:   timers.LeaseDuration,
			RenewDeadline:   timers.RenewDeadline,
			RetryPeriod:     timers.RetryPeriod,
			Callbacks:       callbacks,
			Name:            controllerName,
			ReleaseOnCancel: true,
		})
		if err != nil {
			klog.Infof("Error creating leader elector: %v", err)
			return
		}
		le.Run(ctx)
	}, timers.RetryPeriod)
}
