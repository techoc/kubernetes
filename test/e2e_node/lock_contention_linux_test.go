//go:build linux

/*
Copyright 2021 The Kubernetes Authors.

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

package e2enode

import (
	"context"
	"time"

	"golang.org/x/sys/unix"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	"k8s.io/kubernetes/test/e2e/feature"
	"k8s.io/kubernetes/test/e2e/framework"
	e2enode "k8s.io/kubernetes/test/e2e/framework/node"
)

const contentionLockFile = "/var/run/kubelet.lock"

// Kubelet Lock contention 测试锁竞争特性。
// 具有破坏性，因为测试中会重启 kubelet。
// Feature:LockContention，因为我们不希望该测试被其他
// 测试套件选中，因此使用唯一名称 "LockContention"。
var _ = SIGDescribe("Lock contention", framework.WithSlow(), framework.WithDisruptive(), feature.LockContention, func() {

	// 需要在 Kubelet 上设置 `--lock-file` 和 `--exit-on-lock-contention` flag。
	ginkgo.It("Kubelet should stop when the test acquires the lock on lock file and restart once the lock is released", func(ctx context.Context) {

		ginkgo.By("perform kubelet health check to check if kubelet is healthy and running.")
		// 运行测试前预防性检查 kubelet 是否健康。
		gomega.Expect(e2enode.HealthCheck(kubeletHealthCheckURL)).To(gomega.BeTrueBecause("expected kubelet to be in healthy state"))

		ginkgo.By("acquiring the lock on lock file i.e /var/run/kubelet.lock")
		// 打开文件意图获取锁，模拟另一个 kubelet（self-hosted）
		// 尝试启动的行为。发生锁竞争时，
		// 预期正在运行的 kubelet 必须终止，并等待
		// 锁文件上的锁被释放。
		// Kubelet 使用与此处相同的方式获取锁文件上的锁：
		// https://github.com/kubernetes/kubernetes/blob/9d2b361ebc7ef28f7cb75596ef40b7c239732d37/cmd/kubelet/app/server.go#L512-#L523
		// Acquire 的函数定义在这里：
		// https://github.com/kubernetes/kubernetes/blob/9d2b361ebc7ef28f7cb75596ef40b7c239732d37/pkg/util/flock/flock_unix.go#L26
		fd, err := unix.Open(contentionLockFile, unix.O_CREAT|unix.O_RDWR|unix.O_CLOEXEC, 0600)
		framework.ExpectNoError(err)
		// 延迟释放锁，以防测试失败、没走到释放步骤。
		// 这样确保一定会释放锁。
		defer func() {
			err = unix.Flock(fd, unix.LOCK_UN)
			framework.ExpectNoError(err)
		}()
		// 获取锁。
		err = unix.Flock(fd, unix.LOCK_EX)
		framework.ExpectNoError(err)

		ginkgo.By("verifying the kubelet is not healthy as there was a lock contention.")
		// 拿到锁后，检查 kubelet 是否健康。
		// 它应该不健康，因为锁竞争会迫使 kubelet 停止。
		gomega.Eventually(ctx, func() bool {
			return e2enode.HealthCheck(kubeletHealthCheckURL)
		}, 10*time.Second, time.Second).Should(gomega.BeFalseBecause("expected kubelet to not be in healthy state"))
	})
})
