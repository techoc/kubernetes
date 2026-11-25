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

package nosnat

import (
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"

	"github.com/spf13/cobra"
	"k8s.io/component-base/logs"
	netutils "k8s.io/utils/net"
)

// CmdNoSnatTest 供 agnhost Cobra 使用。
var CmdNoSnatTest = &cobra.Command{
	Use:   "no-snat-test",
	Short: "Creates the /checknosnat and /whoami endpoints",
	Long: `Serves the following endpoints on the given port (defaults to "8080").

- /whoami - returns the request's IP address.
- /checknosnat - queries  "ip/whoami" for each provided IP ("/checknosnat?ips=ip1,ip2"),
  and if all the response bodies match the "POD_IP" environment variable, it will return a 200 response, 500 otherwise.`,
	Args: cobra.MaximumNArgs(0),
	Run:  main,
}

var port string

func init() {
	CmdNoSnatTest.Flags().StringVar(&port, "port", "8080", "The port to serve /checknosnat and /whoami endpoints on.")
}

// ip = /whoami 查询的目标
// rip = 返回的 ip
// pip = 本 pod 的 ip
// nip = 本节点的 ip

type masqTester struct {
	Port string
}

func main(cmd *cobra.Command, args []string) {
	m := &masqTester{
		Port: port,
	}

	logs.InitLogs()
	defer logs.FlushLogs()

	if err := m.Run(); err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err)
		os.Exit(1)
	}
}

func (m *masqTester) Run() error {
	// pip 是当前 pod 的 IP，nip 是当前节点的 IP
	// 从环境变量中取出 pip 和 nip
	pip, ok := os.LookupEnv("POD_IP")
	if !ok {
		return fmt.Errorf("POD_IP env var was not present in the environment")
	}
	nip, ok := os.LookupEnv("NODE_IP")
	if !ok {
		return fmt.Errorf("NODE_IP env var was not present in the environment")
	}

	// 验证 pip 和 nip 是 IP 地址。
	if netutils.ParseIPSloppy(pip) == nil {
		return fmt.Errorf("POD_IP env var contained %q, which is not an IP address", pip)
	}
	if netutils.ParseIPSloppy(nip) == nil {
		return fmt.Errorf("NODE_IP env var contained %q, which is not an IP address", nip)
	}

	// 注册处理函数
	http.HandleFunc("/whoami", whoami)
	http.HandleFunc("/checknosnat", mkChecknosnat(pip, nip))

	// 启动服务器
	return http.ListenAndServe(":"+m.Port, nil)
}

type handler func(http.ResponseWriter, *http.Request)

func joinErrors(errs []error, sep string) string {
	strs := make([]string, len(errs))
	for i, err := range errs {
		strs[i] = err.Error()
	}
	return strings.Join(strs, sep)
}

// 使用当前位置的 pod 和节点 IP 构建 checknosnat 处理函数
func mkChecknosnat(pip string, nip string) handler {
	// 对每个提供的 ip 查询 /whoami，若所有响应体都匹配本 Pod 的 ip 则返回 200，否则返回 500
	return func(w http.ResponseWriter, req *http.Request) {
		errs := []error{}
		ips := strings.Split(req.URL.Query().Get("ips"), ",")
		for _, ip := range ips {
			if err := check(ip, pip, nip); err != nil {
				errs = append(errs, err)
			}
		}
		if len(errs) > 0 {
			w.WriteHeader(500)
			fmt.Fprintf(w, "%s", joinErrors(errs, ", "))
			return
		}
		w.WriteHeader(200)
	}
}

// 将 req.RemoteAddr 写入响应，req.RemoteAddr 是传入连接的地址
func whoami(w http.ResponseWriter, req *http.Request) {
	fmt.Fprintf(w, "%s", req.RemoteAddr)
}

// 查询 ip/whoami 并将响应与 pip 比较，用 nip 来区分 SNAT 与其他可能的失败模式
func check(ip string, pip string, nip string) error {
	url := fmt.Sprintf("http://%s/whoami", ip)
	resp, err := http.Get(url)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	rips := strings.Split(string(body), ":")
	if rips == nil || len(rips) == 0 {
		return fmt.Errorf("Invalid returned ip %q from %q", string(body), url)
	}
	rip := rips[0]
	if rip != pip {
		if rip == nip {
			return fmt.Errorf("Returned ip %q != my Pod ip %q, == my Node ip %q - SNAT", rip, pip, nip)
		}
		return fmt.Errorf("Returned ip %q != my Pod ip %q or my Node ip %q - SNAT to unexpected ip (possible SNAT through unexpected interface on the way into another node)", rip, pip, nip)
	}
	return nil
}
