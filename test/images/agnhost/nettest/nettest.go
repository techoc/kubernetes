/*
Copyright 2014 The Kubernetes Authors.

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

// 一个用于检查网络连通性的小型 web 服务器。
//
// 它会主动连接并通过 -service 标志传入的 service 中的每个 pod 通信，
// 并期望收到每个 pod 的回应。
//
// 它会在给定的 -port 上提供 web 服务。
//
// 访问 /read 可查看当前状态，访问 /quit 可关闭服务。
//
// 访问 /status 可查看 pass/running/fail 的判定结果（它实际上会返回这几个词之一）。
//
// /write 供其他网络测试 pod 用来上报连通性。

package nettest

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/spf13/cobra"

	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/version"
	clientset "k8s.io/client-go/kubernetes"
	restclient "k8s.io/client-go/rest"
)

var (
	port          int
	peerCount     int
	service       string
	namespace     string
	delayShutdown int
)

// CmdNettest 供 agnhost Cobra 使用。
var CmdNettest = &cobra.Command{
	Use:   "nettest",
	Short: "Starts a tiny web server for checking networking connectivity",
	Long: `Starts a web server for checking networking connectivity on the given "--port".

Will dial out to, and expect to hear from, every pod that is a member of the service
passed in the flag "--service".

The web server will have the following endpoints:

- "/read": to see the current state, or "/quit" to shut down.

- "/status": to see "pass/running/fail" determination. (literally, it will return
one of those words.)

- "/write": is used by other network test pods to register connectivity.`,
	Args: cobra.MaximumNArgs(0),
	Run:  main,
}

func init() {
	CmdNettest.Flags().IntVar(&port, "port", 8080, "Port number to serve at.")
	CmdNettest.Flags().IntVar(&peerCount, "peers", 8, "Must find at least this many peers for the test to pass.")
	CmdNettest.Flags().StringVar(&service, "service", "nettest", "Service to find other network test pods in.")
	CmdNettest.Flags().StringVar(&namespace, "namespace", "default", "Namespace of this pod. TODO: kubernetes should make this discoverable.")
	CmdNettest.Flags().IntVar(&delayShutdown, "delay-shutdown", 0, "Number of seconds to delay shutdown when receiving SIGTERM.")
}

// State 记录我们这个小型 http 服务器的内部状态。
// 它会通过 /read 端点原样返回。
type State struct {
	// Hostname 只设置一次且永不修改——读取它总是安全的。
	Hostname string

	// 以下字段在读写之前必须持有 lock。
	Sent                 map[string]int
	Received             map[string]int
	Errors               []string
	Log                  []string
	StillContactingPeers bool

	lock sync.Mutex
}

func (s *State) doneContactingPeers() {
	s.lock.Lock()
	defer s.lock.Unlock()
	s.StillContactingPeers = false
}

// serveStatus 返回 "pass"、"running" 或 "fail"。
func (s *State) serveStatus(w http.ResponseWriter, r *http.Request) {
	s.lock.Lock()
	defer s.lock.Unlock()
	if len(s.Sent) >= peerCount && len(s.Received) >= peerCount {
		fmt.Fprintf(w, "pass")
		return
	}
	if s.StillContactingPeers {
		fmt.Fprintf(w, "running")
		return
	}
	// 持有 lock 时不能调用 Logf，所以用 goroutine 延迟执行
	go s.Logf("Declaring failure for %s/%s with %d sent and %d received and %d peers", namespace, service, len(s.Sent), len(s.Received), peerCount)
	fmt.Fprintf(w, "fail")
}

// serveRead 写出经过 JSON 编码的状态
func (s *State) serveRead(w http.ResponseWriter, r *http.Request) {
	s.lock.Lock()
	defer s.lock.Unlock()
	w.WriteHeader(http.StatusOK)
	b, err := json.MarshalIndent(s, "", "\t")
	s.appendErr(err)
	_, err = w.Write(b)
	s.appendErr(err)
}

// WritePost 是发往 /write handler 的请求（经过 JSON 编码）应遵循的格式。
type WritePost struct {
	Source string
	Dest   string
}

// WriteResp 由 /write 返回
type WriteResp struct {
	Hostname string
}

// serveWrite 在我们的状态中记录这次联系。
func (s *State) serveWrite(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	s.lock.Lock()
	defer s.lock.Unlock()
	w.WriteHeader(http.StatusOK)
	var wp WritePost
	s.appendErr(json.NewDecoder(r.Body).Decode(&wp))
	if wp.Source == "" {
		s.appendErr(fmt.Errorf("%v: Got request with no source", s.Hostname))
	} else {
		if s.Received == nil {
			s.Received = map[string]int{}
		}
		s.Received[wp.Source]++
	}
	s.appendErr(json.NewEncoder(w).Encode(&WriteResp{Hostname: s.Hostname}))
}

// appendErr 在 err 不为 nil 时将其加入列表。调用时 s 必须处于加锁状态。
func (s *State) appendErr(err error) {
	if err != nil {
		s.Errors = append(s.Errors, err.Error())
	}
}

// Logf 向日志消息列表写入内容。调用时 s 不能处于加锁状态。
// 如果 s 的 Log 成员会超过 500 条消息，就会丢弃一条旧消息。
func (s *State) Logf(format string, args ...interface{}) {
	s.lock.Lock()
	defer s.lock.Unlock()
	s.Log = append(s.Log, fmt.Sprintf(format, args...))
	if len(s.Log) > 500 {
		s.Log = s.Log[1:]
	}
}

// 调用时 s 不能处于加锁状态
func (s *State) appendSuccessfulSend(toHostname string) {
	s.lock.Lock()
	defer s.lock.Unlock()
	if s.Sent == nil {
		s.Sent = map[string]int{}
	}
	s.Sent[toHostname]++
}

var (
	// 我们唯一的状态对象
	state State
)

func main(cmd *cobra.Command, args []string) {
	if service == "" {
		log.Fatal("Must provide -service flag.")
	}

	hostname, err := os.Hostname()
	if err != nil {
		log.Fatalf("Error getting hostname: %v", err)
	}

	if delayShutdown > 0 {
		termCh := make(chan os.Signal, 1)
		signal.Notify(termCh, syscall.SIGTERM)
		go func() {
			<-termCh
			log.Printf("Sleeping %d seconds before exit ...", delayShutdown)
			time.Sleep(time.Duration(delayShutdown) * time.Second)
			os.Exit(0)
		}()
	}

	state := State{
		Hostname:             hostname,
		StillContactingPeers: true,
	}

	go contactOthers(&state)

	http.HandleFunc("/quit", func(w http.ResponseWriter, r *http.Request) {
		os.Exit(0)
	})

	http.HandleFunc("/read", state.serveRead)
	http.HandleFunc("/write", state.serveWrite)
	http.HandleFunc("/status", state.serveStatus)

	go log.Fatal(http.ListenAndServe(fmt.Sprintf(":%d", port), nil))

	select {}
}

// 查找 service 中的所有同级 pod，并向它们的 /write handler 发送请求。
func contactOthers(state *State) {
	var (
		versionInfo *version.Info
		err         error
	)
	sleepTime := 5 * time.Second
	// 在大型集群中，获取所有 endpoint 的开销相当大。
	// 因此，我们将自己限制为平均每秒最多发送 10 个这样的请求
	if sleepTime < time.Duration(peerCount/10)*time.Second {
		sleepTime = time.Duration(peerCount/10) * time.Second
	}
	timeout := 5 * time.Minute
	// 类似地，我们需要增大超时时间，使其在大型集群中合理可用。
	if timeout < time.Duration(peerCount)*time.Second {
		timeout = time.Duration(peerCount) * time.Second
	}
	defer state.doneContactingPeers()

	config, err := restclient.InClusterConfig()
	if err != nil {
		log.Fatalf("Unable to create config; error: %v\n", err)
	}
	config.ContentType = "application/vnd.kubernetes.protobuf"
	client, err := clientset.NewForConfig(config)
	if err != nil {
		log.Fatalf("Unable to create client; error: %v\n", err)
	}

	// 在 <timeout> 之前尝试获取 server 版本；设置超时是因为
	// pod 的网络连通性可能不是立刻就可用。
	for start := time.Now(); time.Since(start) < timeout; time.Sleep(sleepTime) {
		// 通过获取 server 版本再次确认连接正常。
		if versionInfo, err = client.Discovery().ServerVersion(); err != nil {
			log.Printf("Unable to get server version: %v; retrying.\n", err)
		} else {
			log.Printf("Server version: %#v\n", versionInfo)
			break
		}
		time.Sleep(1 * time.Second)
	}

	if err != nil {
		log.Fatalf("Unable to contact Kubernetes: %v\n", err)
	}

	for start := time.Now(); time.Since(start) < timeout; time.Sleep(sleepTime) {
		eps := getWebserverEndpoints(client)
		if eps.Len() >= peerCount {
			break
		}
		state.Logf("%v/%v has %v endpoints (%v), which is less than %v as expected. Waiting for all endpoints to come up.", namespace, service, len(eps), eps.List(), peerCount)
	}

	// 重复执行此操作，以防新启动的 pod 加入 endpoint 列表时存在传播延迟。
	for range 15 {
		eps := getWebserverEndpoints(client)
		for ep := range eps {
			state.Logf("Attempting to contact %s", ep)
			contactSingle(ep, state)
		}
		time.Sleep(sleepTime)
	}
}

// getWebserverEndpoints 以 String 集合的形式返回 webserver 的 endpoint，
// 每个 endpoint 的格式类似 "http://{ip}:{port}"
func getWebserverEndpoints(client clientset.Interface) sets.String {
	endpoints, err := client.CoreV1().Endpoints(namespace).Get(context.TODO(), service, v1.GetOptions{})
	eps := sets.String{}
	if err != nil {
		state.Logf("Unable to read the endpoints for %v/%v: %v.", namespace, service, err)
		return eps
	}
	for _, ss := range endpoints.Subsets {
		for _, a := range ss.Addresses {
			for _, p := range ss.Ports {
				ipPort := net.JoinHostPort(a.IP, fmt.Sprint(p.Port))
				eps.Insert(fmt.Sprintf("http://%s", ipPort))
			}
		}
	}
	return eps
}

// contactSingle 连接地址 'e'，并尝试向其 /write 地址发送 POST 请求。
func contactSingle(e string, state *State) {
	body, err := json.Marshal(&WritePost{
		Dest:   e,
		Source: state.Hostname,
	})
	if err != nil {
		log.Fatalf("json marshal error: %v", err)
	}
	resp, err := http.Post(e+"/write", "application/json", bytes.NewReader(body))
	if err != nil {
		state.Logf("Warning: unable to contact the endpoint %q: %v", e, err)
		return
	}
	defer resp.Body.Close()

	body, err = io.ReadAll(resp.Body)
	if err != nil {
		state.Logf("Warning: unable to read response from '%v': '%v'", e, err)
		return
	}
	var wr WriteResp
	err = json.Unmarshal(body, &wr)
	if err != nil {
		state.Logf("Warning: unable to unmarshal response (%v) from '%v': '%v'", string(body), e, err)
		return
	}
	state.appendSuccessfulSend(wr.Hostname)
}
