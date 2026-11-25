/*
Copyright 2023 The Kubernetes Authors.

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

package prober

import (
	"fmt"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/tools/record"
	kubepod "k8s.io/kubernetes/pkg/kubelet/pod"
	"k8s.io/kubernetes/pkg/kubelet/prober/results"
	"k8s.io/kubernetes/pkg/kubelet/status"
	statustest "k8s.io/kubernetes/pkg/kubelet/status/testing"
	kubeletutil "k8s.io/kubernetes/pkg/kubelet/util"
	"k8s.io/kubernetes/test/utils/ktesting"
	"k8s.io/utils/ptr"
)

// TCP socket 释放前会经历 TIME-WAIT 状态（默认 60 秒），
// 期间 conntrack 条目和临时端口会被占用 60 秒，即使探针可能不到 1 秒就完成。
// 如果探针速率高于操作系统回收端口的速率，就会消耗大量临时端口或 conntrack 条目。
// 这些测试验证经过一段时间后探针仍能正常工作；如果探针没有及时关闭 socket，就会开始失败。
// 测试会创建一个 TCP 或 HTTP 服务器来模拟 Pod，创建 1 个 Pod、每个 Pod 600 个假容器，
// 并为每个容器运行一个探针（所有探针与 kubelet 一样来自同一进程，且指向同一个 IP:port），
// 以验证临时端口没有被耗尽。
//
// 普通 Linux 系统默认端口范围中，每个 (srcIP,srcPort:dstIP:dstPort:Proto) 元组有 28321 个可用临时端口：
// /proc/sys/net/ipv4/ip_local_port_range 32768 60999
// 1 个 Pod x 600 容器/Pod x 1 探针/容器 x 1 请求/秒 = 600 请求/秒
// 600 请求/秒 x 59 秒 = 35400
// 测试应在一分钟内耗尽临时端口并开始出现连接失败。
// 参考：https://github.com/kubernetes/kubernetes/issues/89898#issuecomment-1383207322

// TestTCPPortExhaustion 验证持续探测不会耗尽临时端口或 conntrack 条目。
func TestTCPPortExhaustion(t *testing.T) {
	// 该测试会在短时间内建立大量连接，在资源受限环境中容易抖动，因此默认跳过；
	// 保留它用于手动验证或对探针新改动进行实验。
	t.Skip("skipping TCP port exhaustion tests")

	const (
		numTestPods   = 1
		numContainers = 600
	)

	tests := []struct {
		name string
		http bool // 可以是 TCP 或 HTTP
	}{
		{"TCP", false},
		{"HTTP", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tCtx := ktesting.Init(t)
			podManager := kubepod.NewBasicPodManager()
			podStartupLatencyTracker := kubeletutil.NewPodStartupLatencyTracker()
			m := NewManager(
				status.NewManager(&fake.Clientset{}, podManager, &statustest.FakePodDeletionSafetyProvider{}, podStartupLatencyTracker),
				results.NewManager(),
				results.NewManager(),
				results.NewManager(),
				nil, // 执行器
				&record.FakeRecorder{},
			).(*manager)
			defer cleanup(t, m)

			now := time.Now()
			fakePods := make([]*fakePod, numTestPods)
			for i := 0; i < numTestPods; i++ {
				fake, err := newFakePod(tt.http)
				if err != nil {
					t.Fatalf("unexpected error creating fake pod: %v", err)
				}
				defer fake.stop()
				handler := fake.probeHandler()
				fakePods[i] = fake

				pod := v1.Pod{
					ObjectMeta: metav1.ObjectMeta{
						UID:       types.UID(fmt.Sprintf("pod%d", i)),
						Name:      fmt.Sprintf("pod%d", i),
						Namespace: "test",
					},
					Spec: v1.PodSpec{},
					Status: v1.PodStatus{
						Phase:  v1.PodPhase(v1.PodReady),
						PodIPs: []v1.PodIP{{IP: "127.0.0.1"}},
					},
				}
				for j := 0; j < numContainers; j++ {
					// 为简化测试只使用存活探针，它们的初始状态就是成功。
					pod.Spec.Containers = append(pod.Spec.Containers, v1.Container{
						Name:          fmt.Sprintf("container%d", j),
						LivenessProbe: newProbe(handler),
					})
					pod.Status.ContainerStatuses = append(pod.Status.ContainerStatuses, v1.ContainerStatus{
						Name:        fmt.Sprintf("container%d", j),
						ContainerID: fmt.Sprintf("pod%d://container%d", i, j),
						State: v1.ContainerState{
							Running: &v1.ContainerStateRunning{
								StartedAt: metav1.Now(),
							},
						},
						Started: ptr.To(true),
					})
				}
				podManager.AddPod(&pod)
				m.statusManager.SetPodStatus(tCtx.Logger(), &pod, pod.Status)
				m.AddPod(tCtx, &pod)
			}
			t.Logf("Adding %d pods with %d containers each in %v", numTestPods, numContainers, time.Since(now))

			ctx := tCtx.WithTimeout(59*time.Second, "timeout 59 Second")
			defer ctx.Cancel("TestTCPPortExhaustion completed")
			var wg sync.WaitGroup

			wg.Add(1)
			go func() {
				defer wg.Done()
				for {
					var result results.Update
					var probeType string
					select {
					case result = <-m.startupManager.Updates():
						probeType = "startup"
					case result = <-m.livenessManager.Updates():
						probeType = "liveness"
					case result = <-m.readinessManager.Updates():
						probeType = "readiness"
					case <-ctx.Done():
						return
					}
					switch result.Result.String() {
					// 任何一个探针失败都说明临时端口耗尽，测试应失败。
					case "Failure":
						t.Errorf("Failure %s on contantinerID: %v Pod %v", probeType, result.ContainerID, result.PodUID)
					case "UNKNOWN": // 启动探针
						t.Logf("UNKNOWN state for %v", result)
					default:
					}
				}
			}()
			wg.Wait()

			// 记录每个 Pod 接收到的连接数，便于调试测试失败。
			for _, pod := range fakePods {
				n := pod.connections()
				t.Logf("Number of connections %d", n)
			}

		})
	}

}

// newProbe 构造一个 1 秒超时、1 秒周期、失败阈值 3 的探针。
func newProbe(handler v1.ProbeHandler) *v1.Probe {
	return &v1.Probe{
		ProbeHandler:     handler,
		TimeoutSeconds:   1,
		PeriodSeconds:    1,
		SuccessThreshold: 1,
		FailureThreshold: 3,
	}
}

// newFakePod 在随机端口上启动 TCP 或 HTTP 服务器来模拟 Pod。
func newFakePod(httpServer bool) (*fakePod, error) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, fmt.Errorf("failed to bind: %v", err)
	}
	f := &fakePod{ln: ln, http: httpServer}

	// 启动 HTTP 或 TCP 服务器，并统计接收到的连接数。
	if httpServer {
		var mu sync.Mutex
		visitors := map[string]struct{}{}
		go http.Serve(ln, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			mu.Lock()
			defer mu.Unlock()
			if _, ok := visitors[r.RemoteAddr]; !ok {
				atomic.AddInt64(&f.numConnection, 1)
				visitors[r.RemoteAddr] = struct{}{}
			}
		}))
	} else {
		go func() {
			for {
				conn, err := ln.Accept()
				if err != nil {
					// 监听器关闭时 Accept 返回错误，此时退出循环。
					return
				}
				atomic.AddInt64(&f.numConnection, 1)
				// 在处理连接的同时不阻塞 Accept 循环。
				go func(c net.Conn) {
					defer c.Close()
					// 读取数据但忽略错误，因为探针不会发送数据。
					buffer := make([]byte, 1024)
					c.Read(buffer)
					// 回写响应，让探针完成一次完整的连接。
					conn.Write([]byte("Hi back!\n"))
				}(conn)

			}
		}()
	}
	return f, nil

}

// fakePod 是测试用的假 Pod 服务器。
type fakePod struct {
	ln net.Listener
	// numConnection 通过原子操作累加，避免并发读写竞争。
	numConnection int64
	http          bool
}

// probeHandler 根据假 Pod 类型返回指向同一端口的 HTTP 或 TCP 探针处理器。
func (f *fakePod) probeHandler() v1.ProbeHandler {
	port := f.ln.Addr().(*net.TCPAddr).Port
	var handler v1.ProbeHandler
	if f.http {
		handler = v1.ProbeHandler{
			HTTPGet: &v1.HTTPGetAction{
				Host: "127.0.0.1",
				Port: intstr.FromInt32(int32(port)),
			},
		}
	} else {
		handler = v1.ProbeHandler{
			TCPSocket: &v1.TCPSocketAction{
				Host: "127.0.0.1",
				Port: intstr.FromInt32(int32(port)),
			},
		}
	}
	return handler
}

// stop 关闭监听器，结束假 Pod 服务器。
func (f *fakePod) stop() {
	f.ln.Close()
}

// connections 返回迄今接收到的连接数。
func (f *fakePod) connections() int {
	return int(atomic.LoadInt64(&f.numConnection))
}
