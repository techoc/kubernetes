/*
Copyright 2015 The Kubernetes Authors.

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

// 用 `go build conformance.go` 构建二进制文件，然后在候选节点上运行 conformance 二进制文件。如果
// 在非 Linux 机器上编译，必须为主机交叉编译。
package main

import (
	"flag"
	"fmt"
	"net"
	"os/exec"
	"regexp"
	"strings"

	"errors"
	"os"
)

const success = "\033[0;32mSUCCESS\033[0m"
const failed = "\033[0;31mFAILED\033[0m"
const notConfigured = "\033[0;34mNOT CONFIGURED\033[0m"
const skipped = "\033[0;34mSKIPPED\033[0m"

var checkFlag = flag.String(
	"check", "all", "what to check for conformance.  One or more of all,container-runtime,daemons,dns,firewall,kernel")

func init() {
	// 将其设为 false 以撤销 util/logs.go 将其设为 true 的设置，防止 cadvisor 日志刷屏。
	// 当 util/logs.go 不再将该 flag 设为 true 后删除此项。
	flag.Set("logtostderr", "false")
}

// TODO: Should we write an e2e test for this?
func main() {
	flag.Parse()
	o := strings.Split(*checkFlag, ",")
	errs := check(o...)
	if len(errs) > 0 {
		os.Exit(1)
	} else {
		os.Exit(0)
	}
}

// check 返回检查给定组件时发现的错误，并防止错误输出到 stdout。
func check(options ...string) []error {
	errs := []error{}
	for _, c := range options {
		switch c {
		case "all":
			errs = appendNotNil(errs, kernel())
			errs = appendNotNil(errs, daemons())
			errs = appendNotNil(errs, firewall())
			errs = appendNotNil(errs, dns())
		case "daemons":
			errs = appendNotNil(errs, daemons())
		case "dns":
			errs = appendNotNil(errs, dns())
		case "firewall":
			errs = appendNotNil(errs, firewall())
		case "kernel":
			errs = appendNotNil(errs, kernel())
		default:
			fmt.Printf("Unrecognized option %s\n", c)
			errs = append(errs, fmt.Errorf("Unrecognized option %s", c))
		}
	}
	return errs
}

const kubeletClusterDNSRegexStr = `\/kubelet.*--cluster-dns=(\S+) `
const kubeletClusterDomainRegexStr = `\/kubelet.*--cluster-domain=(\S+)`

// dns 检查集群 DNS 是否配置正确，能否解析 kubernetes.default service
func dns() error {
	dnsRegex, err := regexp.Compile(kubeletClusterDNSRegexStr)
	if err != nil {
		// 这不应发生，只能通过修改代码来修复
		panic(err)
	}
	domainRegex, err := regexp.Compile(kubeletClusterDomainRegexStr)
	if err != nil {
		// 这不应发生，只能通过修改代码来修复
		panic(err)
	}

	h, err := net.LookupHost("kubernetes.default")
	if err == nil {
		return printSuccess("Dns Check (Optional): %s", success)
	}
	if len(h) > 0 {
		return printSuccess("Dns Check (Optional): %s", success)
	}

	kubecmd, err := exec.Command("ps", "aux").CombinedOutput()
	if err != nil {
		// 执行 ps aux 不应失败
		panic(err)
	}

	// 查找 dns flag 并解析其值
	dns := dnsRegex.FindStringSubmatch(string(kubecmd))
	if len(dns) < 2 {
		return printSuccess(
			"Dns Check (Optional): %s No hosts resolve to kubernetes.default.  kubelet will need to set "+
				"--cluster-dns and --cluster-domain when run", notConfigured)
	}

	// 查找 domain flag 并解析其值
	domain := domainRegex.FindStringSubmatch(string(kubecmd))
	if len(domain) < 2 {
		return printSuccess(
			"Dns Check (Optional): %s No hosts resolve to kubernetes.default.  kubelet will need to set "+
				"--cluster-dns and --cluster-domain when run", notConfigured)
	}

	// 用 kubelet 当前运行时的 flag 进行一次查询
	nsArgs := []string{"-q=a", fmt.Sprintf("kubernetes.default.%s", domain[1]), dns[1]}
	if err = exec.Command("nslookup", nsArgs...).Run(); err != nil {
		// 将其标记为失败，因为显然打算配置它，但配置方式有误
		return printError(
			"Dns Check (Optional): %s No hosts resolve to kubernetes.default  kubelet found, but cannot resolve "+
				"kubernetes.default using nslookup %s error: %v", failed, strings.Join(nsArgs, " "), err)
	}

	// 能用 kubelet 的 dns 和 domain 值解析 kubernetes.default
	return printSuccess("Dns Check (Optional): %s", success)
}

const cmdlineCGroupMemory = `cgroup_enable=memory`

// kernel 检查内核是否正确配置以支持所需的 cgroup 特性
func kernel() error {
	cmdline, err := os.ReadFile("/proc/cmdline")
	if err != nil {
		return printError("Kernel Command Line Check %s: Could not check /proc/cmdline", failed)
	}
	if !strings.Contains(string(cmdline), cmdlineCGroupMemory) {
		return printError("Kernel Command Line Check %s: cgroup_enable=memory not enabled in /proc/cmdline", failed)
	}
	return printSuccess("Kernel Command Line %s", success)
}

const iptablesInputRegexStr = `Chain INPUT \(policy DROP\)`
const iptablesForwardRegexStr = `Chain FORWARD \(policy DROP\)`

// firewall 检查 iptables 是否设置了会干扰流量的常见防火墙规则
func firewall() error {
	out, err := exec.Command("iptables", "-L", "INPUT").CombinedOutput()
	if err != nil {
		return printSuccess("Firewall IPTables Check %s: Could not run iptables", skipped)
	}
	inputRegex, err := regexp.Compile(iptablesInputRegexStr)
	if err != nil {
		// 这不应发生，只能通过修改代码来修复
		panic(err)
	}
	if inputRegex.Match(out) {
		return printError("Firewall IPTables Check %s: Found INPUT rule matching %s", failed, iptablesInputRegexStr)
	}

	// 检查 GCE forward 规则
	out, err = exec.Command("iptables", "-L", "FORWARD").CombinedOutput()
	if err != nil {
		return printSuccess("Firewall IPTables Check %s: Could not run iptables", skipped)
	}
	forwardRegex, err := regexp.Compile(iptablesForwardRegexStr)
	if err != nil {
		// 这不应发生，只能通过修改代码来修复
		panic(err)
	}
	if forwardRegex.Match(out) {
		return printError("Firewall IPTables Check %s: Found FORWARD rule matching %s", failed, iptablesInputRegexStr)
	}

	return printSuccess("Firewall IPTables Check %s", success)
}

// daemons 检查所需的节点程序是否在运行：kubelet 和 kube-proxy
func daemons() error {
	if exec.Command("pgrep", "-f", "kubelet").Run() != nil {
		return printError("Daemon Check %s: kubelet process not found", failed)
	}

	if exec.Command("pgrep", "-f", "kube-proxy").Run() != nil {
		return printError("Daemon Check %s: kube-proxy process not found", failed)
	}

	return printSuccess("Daemon Check %s", success)
}

// printError 将其参数格式化打印到控制台（以换行结尾），并返回
// 带相同字符串的 error
func printError(s string, args ...interface{}) error {
	es := fmt.Sprintf(s, args...)
	fmt.Println(es)
	return errors.New(es)
}

// printSuccess 将其参数格式化打印到控制台（以换行结尾）并返回 nil
func printSuccess(s string, args ...interface{}) error {
	fmt.Println(fmt.Sprintf(s, args...))
	return nil
}

// appendNotNil 仅在 err 非 nil 时将其追加到 errs
func appendNotNil(errs []error, err error) []error {
	if err != nil {
		return append(errs, err)
	}
	return errs
}
