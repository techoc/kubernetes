/*
Copyright 2018 The Kubernetes Authors.

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

package ssh

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/onsi/gomega"

	"golang.org/x/crypto/ssh"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/util/wait"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/kubernetes/test/e2e/framework"
)

const (
	// SSHPort 是 SSH 的 TCP 端口号
	SSHPort = "22"

	// pollNodeInterval 是轮询 pod 的间隔。
	pollNodeInterval = 2 * time.Second

	// singleCallTimeout 是单次 API 调用（如 'get' 或 'list'）的尝试时长，用于防止
	// 瞬时故障导致测试失败。
	singleCallTimeout = 5 * time.Minute

	// sshBastionEnvKey 是通过 bastion 运行 SSH 命令时使用的环境变量 key。
	sshBastionEnvKey = "KUBE_SSH_BASTION"
)

// GetSigner 返回指定 provider（"gce" 等）的 ssh.Signer，用于 SSH 到其节点。
func GetSigner(provider string) (ssh.Signer, error) {
	// 优先使用统一的 SSH key，覆盖所有 provider
	if path := os.Getenv("KUBE_SSH_KEY_PATH"); len(path) > 0 {
		return makePrivateKeySignerFromFile(path)
	}

	// 选择要使用的 key。在这里新增 provider 时，也请把它加入那些因 signer
	// 支持不足而被禁用的 SSH 测试。
	keyfile := ""
	switch provider {
	case "gce", "kubemark":
		keyfile = os.Getenv("GCE_SSH_KEY")
		if keyfile == "" {
			keyfile = os.Getenv("GCE_SSH_PRIVATE_KEY_FILE")
		}
		if keyfile == "" {
			keyfile = "google_compute_engine"
		}
	case "aws", "eks":
		keyfile = os.Getenv("AWS_SSH_KEY")
		if keyfile == "" {
			keyfile = "kube_aws_rsa"
		}
	case "local", "vsphere":
		keyfile = os.Getenv("LOCAL_SSH_KEY")
		if keyfile == "" {
			keyfile = "id_rsa"
		}
	case "skeleton":
		keyfile = os.Getenv("KUBE_SSH_KEY")
		if keyfile == "" {
			keyfile = "id_rsa"
		}
	case "azure":
		keyfile = os.Getenv("AZURE_SSH_KEY")
		if keyfile == "" {
			keyfile = "id_rsa"
		}
	default:
		return nil, fmt.Errorf("GetSigner(...) not implemented for %s", provider)
	}

	// 尊重用户给定的绝对路径；相对路径则默认位于 ~/.ssh
	if !filepath.IsAbs(keyfile) {
		keydir := filepath.Join(os.Getenv("HOME"), ".ssh")
		keyfile = filepath.Join(keydir, keyfile)
	}

	return makePrivateKeySignerFromFile(keyfile)
}

func makePrivateKeySignerFromFile(key string) (ssh.Signer, error) {
	buffer, err := os.ReadFile(key)
	if err != nil {
		return nil, fmt.Errorf("error reading SSH key %s: %w", key, err)
	}

	signer, err := ssh.ParsePrivateKey(buffer)
	if err != nil {
		return nil, fmt.Errorf("error parsing SSH key: %w", err)
	}

	return signer, err
}

// NodeSSHHosts 返回所有可调度节点上可通过 SSH 访问的主机名。
// 如果找不到外部 IP，则回退到内部 IP。如果无法为每个节点找到内部 IP，则返回错误。
func NodeSSHHosts(ctx context.Context, c clientset.Interface) ([]string, error) {
	nodelist := waitListSchedulableNodesOrDie(ctx, c)

	hosts := nodeAddresses(nodelist, v1.NodeExternalIP)
	// 如果并非所有节点都有 ExternalIP，则尝试回退到 InternalIP。
	if len(hosts) < len(nodelist.Items) {
		framework.Logf("No external IP address on nodes, falling back to internal IPs")
		hosts = nodeAddresses(nodelist, v1.NodeInternalIP)
	}
	if len(hosts) != len(nodelist.Items) {
		// 出了问题：所有可调度节点都应至少有一个 IP。
		return nil, fmt.Errorf(
			"only found %d IPs on nodes, but found %d nodes. Nodelist: %v",
			len(hosts), len(nodelist.Items), nodelist)
	}

	lenHosts := len(hosts)
	wg := &sync.WaitGroup{}
	wg.Add(lenHosts)
	sshHosts := make([]string, 0, lenHosts)
	var sshHostsLock sync.Mutex

	for _, host := range hosts {
		go func(host string) {
			defer wg.Done()
			if canConnect(host) {
				framework.Logf("Assuming SSH on host %s", host)
				sshHostsLock.Lock()
				sshHosts = append(sshHosts, net.JoinHostPort(host, SSHPort))
				sshHostsLock.Unlock()
			} else {
				framework.Logf("Skipping host %s because it does not run anything on port %s", host, SSHPort)
			}
		}(host)
	}
	wg.Wait()

	return sshHosts, nil
}

// 如果可以建立到 SSHPort 的网络连接，canConnect 返回 true。
func canConnect(host string) bool {
	if _, ok := os.LookupEnv(sshBastionEnvKey); ok {
		return true
	}
	hostPort := net.JoinHostPort(host, SSHPort)
	conn, err := net.DialTimeout("tcp", hostPort, 3*time.Second)
	if err != nil {
		framework.Logf("cannot dial %s: %v", hostPort, err)
		return false
	}
	conn.Close()
	return true
}

// Result 保存 SSH 命令的执行结果
type Result struct {
	User   string
	Host   string
	Cmd    string
	Stdout string
	Stderr string
	Code   int
}

// NodeExec 通过 SSH 在节点上执行给定 cmd。注意 nodeName 是可 SSH 的名称，
// 例如 framework.GetMasterHost() 返回的名称。由于涉及 SSH，这在不同云 provider 上
// 并不保证都能工作。
func NodeExec(ctx context.Context, nodeName, cmd, provider string) (Result, error) {
	return SSH(ctx, cmd, net.JoinHostPort(nodeName, SSHPort), provider)
}

// SSH 同步地 SSH 到 provider 上的节点并执行 cmd。如果 SSH 过程没有出错，
// 则返回 stdout、stderr 和退出码。
func SSH(ctx context.Context, cmd, host, provider string) (Result, error) {
	result := Result{Host: host, Cmd: cmd}

	// 获取 provider 的 signer。
	signer, err := GetSigner(provider)
	if err != nil {
		return result, fmt.Errorf("error getting signer for provider %s: %w", provider, err)
	}

	// 当 user 为空时 RunSSHCommand 会默认使用 Getenv("USER")，但这里也做一次默认值处理，
	// 以便日志更清晰。
	result.User = os.Getenv("KUBE_SSH_USER")
	if result.User == "" {
		result.User = os.Getenv("USER")
	}

	if bastion := os.Getenv(sshBastionEnvKey); len(bastion) > 0 {
		stdout, stderr, code, err := runSSHCommandViaBastion(ctx, cmd, result.User, bastion, host, signer)
		result.Stdout = stdout
		result.Stderr = stderr
		result.Code = code
		return result, err
	}

	stdout, stderr, code, err := runSSHCommand(ctx, cmd, result.User, host, signer)
	result.Stdout = stdout
	result.Stderr = stderr
	result.Code = code

	return result, err
}

// runSSHCommandViaBastion 返回以指定用户在 host 上执行 cmd 的 stdout、stderr 和退出码，
// 以及任何 SSH 层面的错误。
func runSSHCommand(ctx context.Context, cmd, user, host string, signer ssh.Signer) (string, string, int, error) {
	if user == "" {
		user = os.Getenv("USER")
	}
	// 配置 client，连接 server，打开 session。
	config := &ssh.ClientConfig{
		User:            user,
		Auth:            []ssh.AuthMethod{ssh.PublicKeys(signer)},
		HostKeyCallback: ssh.InsecureIgnoreHostKey(),
	}
	var client *ssh.Client
	err := framework.Gomega().Eventually(ctx, func() error {
		c, err := ssh.Dial("tcp", host, config)
		client = c
		return err
	}).WithPolling(5 * time.Second).WithTimeout(20 * time.Second).Should(gomega.Succeed())
	if err != nil {
		return "", "", 0, fmt.Errorf("error getting SSH client to %s@%s: %w", user, host, err)
	}
	defer client.Close()
	session, err := client.NewSession()
	if err != nil {
		return "", "", 0, fmt.Errorf("error creating session to %s@%s: %w", user, host, err)
	}
	defer session.Close()

	// 执行命令。
	code := 0
	var bout, berr bytes.Buffer
	session.Stdout, session.Stderr = &bout, &berr
	if err = session.Run(cmd); err != nil {
		// 检查命令是执行失败还是未完成。
		if exiterr, ok := err.(*ssh.ExitError); ok {
			// 如果得到 ExitError 且退出码非零，则认为 SSH 本身成功
			// （只是命令在 host 上执行失败）。
			if code = exiterr.ExitStatus(); code != 0 {
				err = nil
			}
		} else {
			// 发生了其他类型的错误（如 IOError），认为 SSH 失败。
			err = fmt.Errorf("failed running `%s` on %s@%s: %w", cmd, user, host, err)
		}
	}
	return bout.String(), berr.String(), code, err
}

// runSSHCommandViaBastion 返回以指定用户在 host 上执行 cmd 的 stdout、stderr 和退出码，
// 以及任何 SSH 层面的错误。它先通过 SSH proxy 连接 bastion，再借助该隧道连接远端 host。
// 与 sshutil.RunSSHCommand 类似，但针对测试基础设施的需求做了裁剪。
func runSSHCommandViaBastion(ctx context.Context, cmd, user, bastion, host string, signer ssh.Signer) (string, string, int, error) {
	// 配置 client，连接 server，打开 session。
	config := &ssh.ClientConfig{
		User:            user,
		Auth:            []ssh.AuthMethod{ssh.PublicKeys(signer)},
		HostKeyCallback: ssh.InsecureIgnoreHostKey(),
		Timeout:         150 * time.Second,
	}
	bastionClient, err := ssh.Dial("tcp", bastion, config)
	if err != nil {
		err = wait.PollUntilContextTimeout(ctx, 5*time.Second, 20*time.Second, false, func(ctx context.Context) (bool, error) {
			fmt.Printf("error dialing %s@%s: '%v', retrying\n", user, bastion, err)
			if bastionClient, err = ssh.Dial("tcp", bastion, config); err != nil {
				return false, err
			}
			return true, nil
		})
	}
	if err != nil {
		return "", "", 0, fmt.Errorf("error getting SSH client to %s@%s: %w", user, bastion, err)
	}
	defer bastionClient.Close()

	conn, err := bastionClient.Dial("tcp", host)
	if err != nil {
		return "", "", 0, fmt.Errorf("error dialing %s from bastion: %w", host, err)
	}
	defer conn.Close()

	ncc, chans, reqs, err := ssh.NewClientConn(conn, host, config)
	if err != nil {
		return "", "", 0, fmt.Errorf("error creating forwarding connection %s from bastion: %w", host, err)
	}
	client := ssh.NewClient(ncc, chans, reqs)
	defer client.Close()

	session, err := client.NewSession()
	if err != nil {
		return "", "", 0, fmt.Errorf("error creating session to %s@%s from bastion: %w", user, host, err)
	}
	defer session.Close()

	// 执行命令。
	code := 0
	var bout, berr bytes.Buffer
	session.Stdout, session.Stderr = &bout, &berr
	if err = session.Run(cmd); err != nil {
		// 检查命令是执行失败还是未完成。
		if exiterr, ok := err.(*ssh.ExitError); ok {
			// 如果得到 ExitError 且退出码非零，则认为 SSH 本身成功
			// （只是命令在 host 上执行失败）。
			if code = exiterr.ExitStatus(); code != 0 {
				err = nil
			}
		} else {
			// 发生了其他类型的错误（如 IOError），认为 SSH 失败。
			err = fmt.Errorf("failed running `%s` on %s@%s: %w", cmd, user, host, err)
		}
	}
	return bout.String(), berr.String(), code, err
}

// LogResult 记录执行结果日志
func LogResult(result Result) {
	remote := fmt.Sprintf("%s@%s", result.User, result.Host)
	framework.Logf("ssh %s: command:   %s", remote, result.Cmd)
	framework.Logf("ssh %s: stdout:    %q", remote, result.Stdout)
	framework.Logf("ssh %s: stderr:    %q", remote, result.Stderr)
	framework.Logf("ssh %s: exit code: %d", remote, result.Code)
}

// IssueSSHCommandWithResult 尝试执行 SSH 命令并返回执行结果
func IssueSSHCommandWithResult(ctx context.Context, cmd, provider string, node *v1.Node) (*Result, error) {
	framework.Logf("Getting external IP address for %s", node.Name)
	host := ""
	for _, a := range node.Status.Addresses {
		if a.Type == v1.NodeExternalIP && a.Address != "" {
			host = net.JoinHostPort(a.Address, SSHPort)
			break
		}
	}

	if host == "" {
		// 没找到外部 IP，退而求其次使用内部 IP
		for _, a := range node.Status.Addresses {
			if a.Type == v1.NodeInternalIP && a.Address != "" {
				host = net.JoinHostPort(a.Address, SSHPort)
				break
			}
		}
	}

	if host == "" {
		return nil, fmt.Errorf("couldn't find any IP address for node %s", node.Name)
	}

	framework.Logf("SSH %q on %s(%s)", cmd, node.Name, host)
	result, err := SSH(ctx, cmd, host, provider)
	LogResult(result)

	if result.Code != 0 || err != nil {
		return nil, fmt.Errorf("failed running %q: %v (exit code %d, stderr %v)",
			cmd, err, result.Code, result.Stderr)
	}

	return &result, nil
}

// IssueSSHCommand 尝试执行 SSH 命令
func IssueSSHCommand(ctx context.Context, cmd, provider string, node *v1.Node) error {
	_, err := IssueSSHCommandWithResult(ctx, cmd, provider, node)
	if err != nil {
		return err
	}
	return nil
}

// nodeAddresses 返回每个节点上指定类型的第一个地址。
func nodeAddresses(nodelist *v1.NodeList, addrType v1.NodeAddressType) []string {
	hosts := []string{}
	for _, n := range nodelist.Items {
		for _, addr := range n.Status.Addresses {
			if addr.Type == addrType && addr.Address != "" {
				hosts = append(hosts, addr.Address)
				break
			}
		}
	}
	return hosts
}

// waitListSchedulableNodes 封装了支持重试的节点列表查询。
func waitListSchedulableNodes(ctx context.Context, c clientset.Interface) (*v1.NodeList, error) {
	var nodes *v1.NodeList
	var err error
	if wait.PollUntilContextTimeout(ctx, pollNodeInterval, singleCallTimeout, true, func(ctx context.Context) (bool, error) {
		nodes, err = c.CoreV1().Nodes().List(ctx, metav1.ListOptions{FieldSelector: fields.Set{
			"spec.unschedulable": "false",
		}.AsSelector().String()})
		if err != nil {
			return false, err
		}
		return true, nil
	}) != nil {
		return nodes, err
	}
	return nodes, nil
}

// waitListSchedulableNodesOrDie 封装了支持重试的节点列表查询，失败时终止测试。
func waitListSchedulableNodesOrDie(ctx context.Context, c clientset.Interface) *v1.NodeList {
	nodes, err := waitListSchedulableNodes(ctx, c)
	if err != nil {
		expectNoError(err, "Non-retryable failure or timed out while listing nodes for e2e cluster.")
	}
	return nodes
}

// expectNoError 检查 err 是否非 nil，如果是则在记录错误后断言失败。
func expectNoError(err error, explain ...interface{}) {
	expectNoErrorWithOffset(1, err, explain...)
}

// expectNoErrorWithOffset 检查 err 是否非 nil，如果是则在调用方之上 "offset" 层记录错误并断言失败。
// （例如调用链 f -> g -> ExpectNoErrorWithOffset(1, ...) 时，错误会记为发生在 "f"）。
func expectNoErrorWithOffset(offset int, err error, explain ...interface{}) {
	if err != nil {
		framework.Logf("Unexpected error occurred: %v", err)
	}
	gomega.ExpectWithOffset(1+offset, err).NotTo(gomega.HaveOccurred(), explain...)
}
