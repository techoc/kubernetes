/*
Copyright 2022 The Kubernetes Authors.

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

package apimachinery

import (
	"context"
	"crypto/sha256"
	"encoding/base32"
	"errors"
	"fmt"
	"net"
	"strings"
	"time"

	"github.com/onsi/gomega"
	"golang.org/x/crypto/cryptobyte"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/kubernetes/test/e2e/feature"
	"k8s.io/kubernetes/test/e2e/framework"
	e2enode "k8s.io/kubernetes/test/e2e/framework/node"
	e2essh "k8s.io/kubernetes/test/e2e/framework/ssh"
	admissionapi "k8s.io/pod-security-admission/api"
)

func getControlPlaneHostname(ctx context.Context, node *v1.Node) (string, error) {
	nodeAddresses := e2enode.GetAddresses(node, v1.NodeExternalIP)
	if len(nodeAddresses) == 0 {
		return "", errors.New("no valid addresses to use for SSH")
	}

	controlPlaneAddress := nodeAddresses[0]

	host := controlPlaneAddress + ":" + e2essh.SSHPort
	result, err := e2essh.SSH(ctx, "hostname", host, framework.TestContext.Provider)
	if err != nil {
		return "", err
	}

	if result.Code != 0 {
		return "", fmt.Errorf("encountered non-zero exit code when running hostname command: %d", result.Code)
	}

	return strings.TrimSpace(result.Stdout), nil
}

// restartAPIServer 尝试重启某个节点上的 kube-apiserver
func restartAPIServer(ctx context.Context, node *v1.Node) error {
	nodeAddresses := e2enode.GetAddresses(node, v1.NodeExternalIP)
	if len(nodeAddresses) == 0 {
		return errors.New("no valid addresses to use for SSH")
	}

	controlPlaneAddress := nodeAddresses[0]
	cmd := "pidof kube-apiserver | xargs sudo kill"
	framework.Logf("Restarting kube-apiserver via ssh, running: %v", cmd)
	result, err := e2essh.SSH(ctx, cmd, net.JoinHostPort(controlPlaneAddress, e2essh.SSHPort), framework.TestContext.Provider)
	if err != nil || result.Code != 0 {
		e2essh.LogResult(result)
		return fmt.Errorf("couldn't restart kube-apiserver: %w", err)
	}
	return nil
}

// 本测试要求 apiserver 设置 --feature-gates=APIServerIdentity=true
var _ = SIGDescribe("kube-apiserver identity", feature.APIServerIdentity, func() {
	f := framework.NewDefaultFramework("kube-apiserver-identity")
	f.NamespacePodSecurityLevel = admissionapi.LevelPrivileged

	f.It("kube-apiserver identity should persist after restart", f.WithDisruptive(), f.WithProvider("gce"), func(ctx context.Context) {
		client := f.ClientSet

		var controlPlaneNodes []v1.Node
		nodes, err := client.CoreV1().Nodes().List(context.TODO(), metav1.ListOptions{})
		framework.ExpectNoError(err)

		for _, node := range nodes.Items {
			if _, ok := node.Labels["node-role.kubernetes.io/control-plane"]; ok {
				controlPlaneNodes = append(controlPlaneNodes, node)
				continue
			}

			if _, ok := node.Labels["node-role.kubernetes.io/master"]; ok {
				controlPlaneNodes = append(controlPlaneNodes, node)
				continue
			}

			for _, taint := range node.Spec.Taints {
				if taint.Key == "node-role.kubernetes.io/master" {
					controlPlaneNodes = append(controlPlaneNodes, node)
					break
				}

				if taint.Key == "node-role.kubernetes.io/control-plane" {
					controlPlaneNodes = append(controlPlaneNodes, node)
					break
				}
			}
		}

		leases, err := client.CoordinationV1().Leases(metav1.NamespaceSystem).List(context.TODO(), metav1.ListOptions{
			LabelSelector: "apiserver.kubernetes.io/identity=kube-apiserver",
		})
		framework.ExpectNoError(err)
		gomega.Expect(leases.Items).To(gomega.HaveLen(len(controlPlaneNodes)), "unexpected number of leases")

		for _, node := range controlPlaneNodes {
			hostname, err := getControlPlaneHostname(ctx, &node)
			framework.ExpectNoError(err)

			b := cryptobyte.NewBuilder(nil)
			b.AddUint16LengthPrefixed(func(b *cryptobyte.Builder) {
				b.AddBytes([]byte(hostname))
			})
			b.AddUint16LengthPrefixed(func(b *cryptobyte.Builder) {
				b.AddBytes([]byte("kube-apiserver"))
			})

			hashData, err := b.Bytes()
			framework.ExpectNoError(err)
			hash := sha256.Sum256(hashData)
			leaseName := "apiserver-" + strings.ToLower(base32.StdEncoding.WithPadding(base32.NoPadding).EncodeToString(hash[:16]))

			lease, err := client.CoordinationV1().Leases(metav1.NamespaceSystem).Get(context.TODO(), leaseName, metav1.GetOptions{})
			framework.ExpectNoError(err)
			oldHolderIdentity := lease.Spec.HolderIdentity
			lastRenewedTime := lease.Spec.RenewTime

			err = restartAPIServer(ctx, &node)
			framework.ExpectNoError(err)

			err = wait.PollImmediate(time.Second, wait.ForeverTestTimeout, func() (bool, error) {
				lease, err = client.CoordinationV1().Leases(metav1.NamespaceSystem).Get(context.TODO(), leaseName, metav1.GetOptions{})
				if err != nil {
					return false, nil
				}

				// 重启后预期只有 holder identity 发生变化
				newHolderIdentity := lease.Spec.HolderIdentity
				if newHolderIdentity == oldHolderIdentity {
					return false, nil
				}

				// holder identity 变化后至少等待一次 lease 心跳
				if !lease.Spec.RenewTime.After(lastRenewedTime.Time) {
					return false, nil
				}

				return true, nil

			})
			framework.ExpectNoError(err, "holder identity did not change after a restart")
		}

		// 只要 kube-apiserver 的 hostname 不变，重启就不应产生新的 Lease 对象。
		// 检查重启 kube-apiserver 后 Lease 对象数量保持不变。
		leases, err = client.CoordinationV1().Leases(metav1.NamespaceSystem).List(context.TODO(), metav1.ListOptions{
			LabelSelector: "apiserver.kubernetes.io/identity=kube-apiserver",
		})
		framework.ExpectNoError(err)
		gomega.Expect(leases.Items).To(gomega.HaveLen(len(controlPlaneNodes)), "unexpected number of leases")
	})
})
