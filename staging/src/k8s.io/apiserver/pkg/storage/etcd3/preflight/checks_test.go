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

package preflight

import (
	"net/url"
	"testing"
	"time"

	utilwait "k8s.io/apimachinery/pkg/util/wait"
)

// TestParseServerURIGood 验证普通 etcd URL 能解析出正确的 host:port。
func TestParseServerURIGood(t *testing.T) {
	connURL, err := parseServerURI("https://127.0.0.1:2379")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	reference := "127.0.0.1:2379"
	if connURL.Host != reference {
		t.Fatalf("server uri was not parsed correctly, host %s was invalid", connURL.Host)
	}
}

// TestParseServerURIGoodUnix 验证 unix:// 形式的地址同样能解析出 host 部分。
func TestParseServerURIGoodUnix(t *testing.T) {
	connURL, err := parseServerURI("unix://127.0.0.1:21002112605")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	reference := "127.0.0.1:21002112605"
	if connURL.Host != reference {
		t.Fatalf("server uri was not parsed correctly, host %s was invalid", connURL.Host)
	}
}

// TestParseServerURIBad 验证非法 URI 会返回解析错误，而不是得到空 host。
func TestParseServerURIBad(t *testing.T) {
	_, err := parseServerURI("-invalid uri$@#%")
	if err == nil {
		t.Fatal("expected bad uri to raise parse error")
	}
}

// TestEtcdConnection 验证不可达地址会被判定为不可用。
func TestEtcdConnection(t *testing.T) {
	etcd := new(EtcdConnection)

	result := etcd.serverReachable(&url.URL{Host: "-not a real network address-", Scheme: "tcp"})
	if result {
		t.Fatal("checkConnection should not have succeeded")
	}
}

// TestCheckEtcdServersEmpty 验证 ServerList 为空时返回“不可用”而不是报错。
func TestCheckEtcdServersEmpty(t *testing.T) {
	etcd := new(EtcdConnection)
	result, err := etcd.CheckEtcdServers()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result {
		t.Fatal("CheckEtcdServers should not have succeeded")
	}
}

// TestCheckEtcdServersUri 验证列表中任一地址非法时应显式报错。
func TestCheckEtcdServersUri(t *testing.T) {
	etcd := new(EtcdConnection)
	etcd.ServerList = []string{"-invalid uri$@#%"}
	result, err := etcd.CheckEtcdServers()
	if err == nil {
		t.Fatalf("expected bad uri to raise parse error")
	}
	if result {
		t.Fatal("CheckEtcdServers should not have succeeded")
	}
}

// TestCheckEtcdServers 验证空字符串地址解析后不可达，返回“不可用”。
func TestCheckEtcdServers(t *testing.T) {
	etcd := new(EtcdConnection)
	etcd.ServerList = []string{""}
	result, err := etcd.CheckEtcdServers()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result {
		t.Fatal("CheckEtcdServers should not have succeeded")
	}
}

// TestPollCheckServer 验证用极短超时轮询不可达服务时最终返回超时错误。
func TestPollCheckServer(t *testing.T) {
	err := utilwait.PollImmediate(1*time.Microsecond,
		2*time.Microsecond,
		EtcdConnection{ServerList: []string{""}}.CheckEtcdServers)
	if err == nil {
		t.Fatal("expected check to time out")
	}
}
