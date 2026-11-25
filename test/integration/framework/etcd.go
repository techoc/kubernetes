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

package framework

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"go.uber.org/goleak"
	"google.golang.org/grpc/grpclog"

	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/util/env"
)

const installEtcd = `
Cannot find etcd, cannot run integration tests
Please see https://git.k8s.io/community/contributors/devel/sig-testing/integration-tests.md#install-etcd-dependency for instructions.

You can use 'hack/install-etcd.sh' to install a copy in third_party/.

`

// getEtcdPath 返回 etcd 可执行文件的路径。
func getEtcdPath() (string, error) {
	return exec.LookPath("etcd")
}

// startEtcd 启动一个 etcd 实例。返回的函数会向 etcd 进程发信号并等待其退出。
func startEtcd(logger klog.Logger, forceCreate bool) (func(), error) {
	if !forceCreate {
		etcdURL := env.GetEnvAsStringOrFallback("KUBE_INTEGRATION_ETCD_URL", "http://127.0.0.1:2379")
		conn, err := net.Dial("tcp", strings.TrimPrefix(etcdURL, "http://"))
		if err == nil {
			klog.Infof("etcd already running at %s", etcdURL)
			_ = conn.Close()
			return func() {}, nil
		}
		logger.V(1).Info("could not connect to etcd", "err", err)
	}

	currentURL, stop, err := RunCustomEtcd(logger, "integration_test_etcd_data", nil)
	if err != nil {
		return nil, err
	}

	os.Setenv("KUBE_INTEGRATION_ETCD_URL", currentURL)

	return stop, nil
}

func init() {
	// 为集成测试关闭 etcd 日志。
	// 如需详细日志，可将其注释掉。
	// 这必须在任何使用 gRPC 的 goroutine 活跃之前完成。在 init 中执行是安全的，
	// 不过这也会影响不使用 RunCustomEtcd 的测试
	// （以前正是在那里执行此操作）。
	grpclog.SetLoggerV2(grpclog.NewLoggerV2(io.Discard, io.Discard, os.Stderr))
}

// hiddenEtcdMessages 包含因我们运行 etcd 的方式而产生的、
// 无需记录到所有测试日志中的消息。
var hiddenEtcdMessages = sets.New(
	"simple token is not cryptographically signed",
	"Running http and grpc server on single port. This is not recommended for production.",
)

// RunCustomEtcd 为测试目的启动一个自定义的 etcd 实例。
func RunCustomEtcd(logger klog.Logger, dataDir string, customFlags []string) (url string, stopFn func(), err error) {
	// TODO: 检查 etcd 版本是否有效。
	etcdPath, err := getEtcdPath()
	if err != nil {
		fmt.Fprint(os.Stderr, installEtcd)
		return "", nil, fmt.Errorf("could not find etcd in PATH: %v", err)
	}
	etcdDataDir, err := os.MkdirTemp(os.TempDir(), dataDir)
	if err != nil {
		return "", nil, fmt.Errorf("unable to make temp etcd data dir %s: %v", dataDir, err)
	}
	etcdSocketPath := path.Join(etcdDataDir, "etcd.sock")
	customURL := "unix://" + etcdSocketPath

	logger.V(2).Info("starting etcd", "url", customURL, "dataDir", etcdDataDir)
	ctx, cancel := context.WithCancel(context.Background())
	args := []string{
		"--data-dir",
		etcdDataDir,
		"--listen-client-urls",
		customURL,
		// 客户端本应通过此地址连接 etcd，但 https://github.com/etcd-io/etcd/pull/12469
		// 看来并不完整：在此传入 Unix 域 URL 会被 ectd 3.15.13 拒绝，报错为
		//    --advertise-client-urls "unix:///tmp/etcd.sock" must be "host:port" (missing port in address)
		//
		// 我们并不需要公布正确地址。为防万一真有组件使用该 URL 而连接到默认地址，
		// 这里设置了一个无效的 URL。
		"--advertise-client-urls",
		"http://127.0.0.111:0",
		// 使用 :0 让内核选择一个唯一端口。我们不关心具体是哪个端口，
		// 因为不会有其他 peer 来连接。
		"--listen-peer-urls",
		"http://127.0.0.1:0",
		"-log-level",
		"warn", // 设置为 info 或 debug 可获得更多日志
		"--quota-backend-bytes",
		strconv.FormatInt(8*1024*1024*1024, 10),
	}
	args = append(args, customFlags...)
	cmd := exec.CommandContext(ctx, etcdPath, args...)

	// 始终过滤 etcd 的输出。这样既能去掉已知的无害消息，
	// 又能将 etcd 日志纳入常规测试日志。
	reader, err := cmd.StderrPipe()
	if err != nil {
		cancel()
		return "", nil, fmt.Errorf("prepared etcd command's stderr pipe: %w", err)
	}
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		buffer := make([]byte, 100*1024)
	reading:
		for {
			n, err := reader.Read(buffer)
			// 实际中我们会拿到包裹在 os.Path 错误里的无类型 errors.errorString，
			// 因此只能退而求其次使用文本匹配。
			if errors.Is(err, io.EOF) || err != nil && err.Error() == "read |0: file already closed" {
				return
			}
			if err != nil {
				logger.Error(err, "read etcd output")
				return
			}
			if n == 0 {
				continue
			}
			dec := json.NewDecoder(bytes.NewBuffer(buffer[0:n]))
			for {
				// 尝试按 JSON 对象解析。如果拿到的不是 JSON 或不是对象，就直接输出剩余内容。
				var msg map[string]any
				err := dec.Decode(&msg)
				if err == io.EOF {
					// 全部处理完毕
					break
				}
				if err != nil {
					offset := int(dec.InputOffset())
					if offset < n {
						logger.Info("etcd output", "msg", string(buffer[offset:n]))
					}
					continue reading
				}

				// 跳过无害消息。
				msgStr, _ := msg["msg"].(string)
				if hiddenEtcdMessages.Has(msgStr) {
					continue
				}

				// 使用结构化日志记录。
				kvs := make([]any, 0, 2*len(msg))
				for key, value := range msg {
					kvs = append(kvs, key, value)
				}
				logger.Info("etcd output", kvs...)
			}
		}
	}()

	stop := func() {
		// 尝试让 etcd 优雅退出
		defer cancel()
		cmd.Process.Signal(syscall.SIGTERM)
		wg.Add(1)
		go func() {
			defer wg.Done()
			select {
			case <-ctx.Done():
				logger.V(6).Info("etcd exited gracefully, context cancelled")
			case <-time.After(5 * time.Second):
				logger.Info("etcd didn't exit in 5 seconds, killing it")
				cancel()
			}
		}()
		err := cmd.Wait()
		logger.V(2).Info("etcd exited", "err", err)
		// 通知 goroutine 我们已完成。
		cancel()
		wg.Wait()
		err = os.RemoveAll(etcdDataDir)
		if err != nil {
			logger.Info("Warning: error during etcd cleanup", "err", err)
		}
	}

	if err := cmd.Start(); err != nil {
		return "", nil, fmt.Errorf("failed to run etcd: %v", err)
	}

	var i int32 = 1
	const pollCount = int32(300)

	for i <= pollCount {
		conn, err := net.DialTimeout("unix", etcdSocketPath, 1*time.Second)
		if err == nil {
			conn.Close()
			break
		}

		if i == pollCount {
			stop()
			return "", nil, fmt.Errorf("could not start etcd")
		}

		time.Sleep(100 * time.Millisecond)
		i = i + 1
	}

	return customURL, stop, nil
}

// EtcdMain 在运行测试前启动一个 etcd 实例。
func EtcdMain(tests func() int) {
	// 如果指定了 -help 参数，则尽早退出。
	flag.Parse()

	// 必须在创建新 goroutine *之前* 调用。
	goleakOpts := IgnoreBackgroundGoroutines()

	goleakOpts = append(goleakOpts,
		// lumberjack 会泄漏一个 goroutine：
		// https://github.com/natefinch/lumberjack/issues/56 这会影响使用
		// --audit-log-path 的测试（例如
		// ./test/integration/apiserver/admissionwebhook/reinvocation_test.go）。
		// 在正常生产环境中这应该是无害的。这里无法判断测试是否使用了该路径，
		// 因此只能对所有测试都抑制这一泄漏报告。
		//
		// 实践中两种名字都出现过。
		goleak.IgnoreTopFunction("k8s.io/kubernetes/vendor/gopkg.in/natefinch/lumberjack%2ev2.(*Logger).millRun"),
		goleak.IgnoreTopFunction("gopkg.in/natefinch/lumberjack%2ev2.(*Logger).millRun"),
		// 如果连接关闭过程中出错，SPDY 会保留一个 goroutine 十分钟，
		// 这会被报告为泄漏（十分钟后该 goroutine 会被清理）。
		//
		// https://github.com/kubernetes/kubernetes/blob/master/vendor/github.com/moby/spdystream/connection.go#L732-L744
		//
		// 忽略这一泄漏报告。
		goleak.IgnoreTopFunction("github.com/moby/spdystream.(*Connection).shutdown"),
	)

	stop, err := startEtcd(klog.Background(), false)
	if err != nil {
		klog.Fatalf("cannot run integration tests: unable to start etcd: %v", err)
	}
	result := tests()
	stop() // 不要使用 defer。参见 os.Exit 文档。
	klog.StopFlushDaemon()

	if err := goleakFindRetry(goleakOpts...); err != nil {
		klog.ErrorS(err, "EtcdMain goroutine check")
		result = 1
	}

	os.Exit(result)
}

// GetEtcdURL 返回由 EtcdMain 或 StartEtcd 启动的 etcd 实例的 URL。
func GetEtcdURL() string {
	return env.GetEnvAsStringOrFallback("KUBE_INTEGRATION_ETCD_URL", "http://127.0.0.1:2379")
}

// StartEtcd 在测试内部启动一个 etcd 实例。若启动失败会中止测试，并在测试结束后自动清理。
// etcd 二进制文件的 stdout 和 stderr，以及 etcd 辅助代码的所有日志输出，
// 都会输出到提供的 logger。
//
// 与 EtcdMain 不同，StartEtcd 不会自动进行泄漏检查。
// 测试可以自行决定是否以及在哪里进行泄漏检查。
//
// 每次测试运行多次启动 etcd，而不是像 EtcdMain 那样只启动一次，
// 可以更好地隔离不同测试。
func StartEtcd(logger klog.Logger, tb testing.TB, forceCreate bool) {
	stop, err := startEtcd(logger, forceCreate)
	if err != nil {
		tb.Fatalf("unable to start etcd: %v", err)
	}
	tb.Cleanup(stop)
}
