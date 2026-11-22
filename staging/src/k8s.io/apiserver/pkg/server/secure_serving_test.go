//go:build go1.27

/*
Copyright The Kubernetes Authors.

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

package server

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net"
	"net/http"
	"strconv"
	"testing"
	"time"

	"k8s.io/apiserver/pkg/server/dynamiccertificates"
	certutil "k8s.io/client-go/util/cert"
	netutils "k8s.io/utils/net"
)

// TestSecureServingIdentityHeaders 验证在 HTTP/1.1 与 HTTP/2 下，
// apiserver 对请求头数量的上限处理：恰好等于上限可通过，超出则返回 431。
// 该文件仅在 Go 1.27+ 构建，对应 maxHeaderValueCount 等新 API。
func TestSecureServingIdentityHeaders(t *testing.T) {
	certPEM, keyPEM, err := certutil.GenerateSelfSignedCertKey("localhost", []net.IP{netutils.ParseIPSloppy("127.0.0.1")}, nil)
	if err != nil {
		t.Fatal(err)
	}
	cert, err := dynamiccertificates.NewStaticCertKeyContent("serving-cert", certPEM, keyPEM)
	if err != nil {
		t.Fatal(err)
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(certPEM) {
		t.Fatal("failed to add serving certificate to trust roots")
	}

	for _, tc := range []struct {
		name         string
		disableHTTP2 bool
		protocol     int
	}{
		{name: "HTTP/1.1", disableHTTP2: true, protocol: 1},
		{name: "HTTP/2", protocol: 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			stopCh := make(chan struct{})
			serving := &SecureServingInfo{Listener: listener, Cert: cert, DisableHTTP2: tc.disableHTTP2}
			// handler 回显收到的 impersonate 组数量，用于确认头部是否完整到达。
			stoppedCh, listenerStoppedCh, err := serving.Serve(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Group-Count", strconv.Itoa(len(r.Header.Values("Impersonate-Group"))))
				w.WriteHeader(http.StatusOK)
			}), 5*time.Second, stopCh)
			if err != nil {
				_ = listener.Close()
				t.Fatal(err)
			}
			t.Cleanup(func() {
				// 有序关闭：先通知停止，再等 listener 与 handler 退出。
				close(stopCh)
				<-listenerStoppedCh
				<-stoppedCh
			})

			transport := &http.Transport{
				TLSClientConfig:   &tls.Config{RootCAs: roots},
				ForceAttemptHTTP2: !tc.disableHTTP2,
			}
			defer transport.CloseIdleConnections()
			client := &http.Client{Transport: transport, Timeout: 10 * time.Second}
			// 三个边界值：普通数量、恰好等于上限、超过上限 1。
			for _, count := range []int{1000, maxHeaderValueCount - 50, maxHeaderValueCount + 1} {
				req, err := http.NewRequest(http.MethodGet, "https://"+listener.Addr().String(), nil)
				if err != nil {
					t.Fatal(err)
				}
				for i := range count {
					req.Header.Add("Impersonate-Group", fmt.Sprintf("group-%d", i))
				}
				resp, err := client.Do(req)
				if err != nil {
					t.Fatal(err)
				}
				if err := resp.Body.Close(); err != nil {
					t.Fatal(err)
				}
				// 协议版本必须与用例声明一致，确保测试的确实是目标 HTTP 栈。
				if resp.ProtoMajor != tc.protocol {
					t.Errorf("%d headers: protocol = %s, want HTTP/%d", count, resp.Proto, tc.protocol)
				}
				want := http.StatusOK
				if count > maxHeaderValueCount {
					want = http.StatusRequestHeaderFieldsTooLarge
				}
				if resp.StatusCode != want {
					t.Errorf("%d headers: status = %d, want %d", count, resp.StatusCode, want)
				}
				// 成功场景还需确认所有头部都被服务端接收，而非被截断。
				if want == http.StatusOK {
					if got := resp.Header.Get("Group-Count"); got != strconv.Itoa(count) {
						t.Errorf("%d headers: handler received %s group header values", count, got)
					}
				}
			}
		})
	}
}
