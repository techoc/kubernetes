/*
Copyright 2019 The Kubernetes Authors.

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

package node

import (
	"context"
	"time"

	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/kubernetes/test/e2e/framework"
	e2ekubectl "k8s.io/kubernetes/test/e2e/framework/kubectl"
)

// WaitForSSHTunnels 等待与 busybox Pod 建立 SSH 隧道。
func WaitForSSHTunnels(ctx context.Context, namespace string) {
	framework.Logf("Waiting for SSH tunnels to establish")
	e2ekubectl.RunKubectl(namespace, "run", "ssh-tunnel-test",
		"--image=busybox",
		"--restart=Never",
		"--command", "--",
		"echo", "Hello")
	defer e2ekubectl.RunKubectl(namespace, "delete", "pod", "ssh-tunnel-test")

	// 最多等待一分钟让新的 SSH 隧道建立
	wait.PollUntilContextTimeout(ctx, 5*time.Second, time.Minute, true, func(ctx context.Context) (bool, error) {
		_, err := e2ekubectl.RunKubectl(namespace, "logs", "ssh-tunnel-test")
		return err == nil, nil
	})
}
