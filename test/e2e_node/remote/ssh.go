/*
Copyright 2016 The Kubernetes Authors.

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

package remote

import (
	"fmt"
	"os"
	"os/exec"
	"os/user"
	"strings"
	"sync"

	"k8s.io/klog/v2"
)

var sshOptions = CommandLine.String("ssh-options", "", "Commandline options passed to ssh.")
var sshEnv = CommandLine.String("ssh-env", "", "Use predefined ssh options for environment.  Options: gce")
var sshKey = CommandLine.String("ssh-key", "", "Path to ssh private key.")
var sshUser = CommandLine.String("ssh-user", "", "Use predefined user for ssh.")

var sshOptionsMap map[string]string
var sshDefaultKeyMap map[string]string
var sshDefaultUserMap map[string]string

func init() {
	usr, err := user.Current()
	if err != nil {
		klog.Fatal(err)
	}
	sshOptionsMap = map[string]string{
		"gce": "-o UserKnownHostsFile=/dev/null -o IdentitiesOnly=yes -o CheckHostIP=no -o StrictHostKeyChecking=no -o ServerAliveInterval=30 -o LogLevel=ERROR",
		"aws": "-o UserKnownHostsFile=/dev/null -o IdentitiesOnly=yes -o CheckHostIP=no -o StrictHostKeyChecking=no -o ServerAliveInterval=30 -o LogLevel=ERROR",
	}
	defaultGceKey := os.Getenv("GCE_SSH_PRIVATE_KEY_FILE")
	if defaultGceKey == "" {
		defaultGceKey = fmt.Sprintf("%s/.ssh/google_compute_engine", usr.HomeDir)
	}
	sshDefaultKeyMap = map[string]string{
		"gce": defaultGceKey,
	}
	sshDefaultUserMap = map[string]string{
		"aws": "ec2-user",
	}
}

var hostnameIPOverrides = struct {
	sync.RWMutex
	m map[string]string
}{m: make(map[string]string)}

// AddHostnameIP 将 <hostname,ip> 对加入 hostnameIPOverrides map。
func AddHostnameIP(hostname, ip string) {
	hostnameIPOverrides.Lock()
	defer hostnameIPOverrides.Unlock()
	hostnameIPOverrides.m[hostname] = ip
}

var sshKeyOverrides = struct {
	sync.RWMutex
	m map[string]string
}{m: make(map[string]string)}

// AddSSHKey 将 <hostname,SSH 私钥路径> 对加入 sshKeyOverrides map
func AddSSHKey(hostname, keyFilePath string) {
	sshKeyOverrides.Lock()
	defer sshKeyOverrides.Unlock()
	sshKeyOverrides.m[hostname] = keyFilePath
}

// GetSSHUser 按以下顺序返回 ssh-user CLI flag、KUBE_SSH_USER 环境变量或默认 ssh 用户：
// 用于 ssh 环境。
func GetSSHUser() string {
	if *sshUser == "" {
		*sshUser = os.Getenv("KUBE_SSH_USER")
	}
	if *sshUser == "" {
		*sshUser = sshDefaultUserMap[*sshEnv]
	}
	return *sshUser
}

// GetHostnameOrIP 将主机名转换为 IP，并在必要时应用用户。
func GetHostnameOrIP(hostname string) string {
	hostnameIPOverrides.RLock()
	defer hostnameIPOverrides.RUnlock()
	host := hostname
	if ip, found := hostnameIPOverrides.m[hostname]; found {
		host = ip
	}

	sshUser := GetSSHUser()
	if sshUser != "" {
		host = fmt.Sprintf("%s@%s", sshUser, host)
	}
	return host
}

// getSSHCommand 处理正确的引号转义，使多个命令通过 ssh 在同一个 shell 中执行
func getSSHCommand(sep string, args ...string) string {
	return fmt.Sprintf("'%s'", strings.Join(args, sep))
}

// SSH 以 root 身份通过 runSSHCommand 执行 ssh 命令。`sudo` 确保所有命令
// 都由 root 执行，避免不同命令之间的权限不匹配。
func SSH(host string, cmd ...string) (string, error) {
	return runSSHCommand(host, "ssh", append([]string{GetHostnameOrIP(host), "--", "sudo"}, cmd...)...)
}

// SSHNoSudo 以普通用户身份通过 runSSHCommand 执行 ssh 命令。有时需要这样做，
// 例如创建目录以便之后用 scp 复制文件。
func SSHNoSudo(host string, cmd ...string) (string, error) {
	return runSSHCommand(host, "ssh", append([]string{GetHostnameOrIP(host), "--"}, cmd...)...)
}

// runSSHCommand 执行 ssh 或 scp 命令，并添加 --ssh-options 提供的 flag
func runSSHCommand(host, cmd string, args ...string) (string, error) {
	if key, err := getPrivateSSHKey(host); len(key) != 0 {
		if err != nil {
			klog.Errorf("private SSH key (%s) not found. Check if the SSH key is configured properly:, err: %v", key, err)
			return "", fmt.Errorf("private SSH key (%s) does not exist", key)
		}

		args = append([]string{"-i", key}, args...)
	}
	if env, found := sshOptionsMap[*sshEnv]; found {
		args = append(strings.Split(env, " "), args...)
	}
	if *sshOptions != "" {
		args = append(strings.Split(*sshOptions, " "), args...)
	}
	klog.Infof("Running the command %s, with args: %v", cmd, args)
	output, err := exec.Command(cmd, args...).CombinedOutput()
	if err != nil {
		klog.Errorf("failed to run SSH command: out: %s, err: %v", output, err)
		return string(output), fmt.Errorf("command [%s %s] failed with error: %w", cmd, strings.Join(args, " "), err)
	}
	return string(output), nil
}

// getPrivateSSHKey 返回 SSH 私钥的路径
func getPrivateSSHKey(host string) (string, error) {
	if *sshKey != "" {
		if _, err := os.Stat(*sshKey); err != nil {
			return *sshKey, err
		}

		return *sshKey, nil
	}

	sshKeyOverrides.Lock()
	defer sshKeyOverrides.Unlock()
	if key, ok := sshKeyOverrides.m[host]; ok {
		return key, nil
	}

	if key, found := sshDefaultKeyMap[*sshEnv]; found {
		if _, err := os.Stat(key); err != nil {
			return key, err
		}

		return key, nil
	}

	return "", nil
}
