/*
Copyright 2021 The Kubernetes Authors.

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

// Package node 实现 `e2e_node.test remote <flags>`，即
// 它被编译进 e2e_node.test。这样 release 包里
// 就不必再附带另一个二进制文件。只支持
// E2E suite 的远端执行。
//
// 当 flag 包含任一"使用或构建二进制"变体时，它由
// `kubetest2 noop -test=node -- <flags>` 调用。
// 这样无需 Kubernetes 源代码也能运行 E2E node 测试。
//
// 它提供 kubetest2 node tester 的命令行 flag（有些
// 改动，见下文），并映射到 `make test-e2e-node` 原本调用
// test/e2e_node/runner/remote 的方式。
//
// 它衍生自
// https://github.com/kubernetes-sigs/kubetest2/blob/master/pkg/testers/node/node.go
// （revision 558f16b589d15d031595af7d035330b8e87bcaaf）。
package node

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"time"

	"k8s.io/klog/v2"

	"k8s.io/kubernetes/test/e2e_node/remote"
	_ "k8s.io/kubernetes/test/e2e_node/remote/gce" // Register remote execution via GCE.
)

// Tester 刻意尽可能与
// https://github.com/kubernetes-sigs/kubetest2/blob/master/pkg/testers/node/node.go
// 保持一致，让命令行相同这一点更明显，
// 只是为二进制路径增加了几个 flag。
type Tester struct {
	// node tester 透传的 flag。
	GCPProject       string        `desc:"GCP Project to create VMs in."`
	GCPZone          string        `desc:"GCP Zone to create VMs in."`
	SkipRegex        string        `desc:"Regular expression of jobs to skip."`
	FocusRegex       string        `desc:"Regular expression of jobs to focus on."`
	TestArgs         string        `desc:"A space-separated list of arguments to pass to node e2e test."`
	LabelFilter      string        `desc:"Label filter arguments to be passed to ginkgo."`
	ImageConfigFile  string        `desc:"Path to a file containing image configuration."`
	Images           string        `desc:"List of images to use when creating instances separated by commas"`
	ImageProject     string        `desc:"A GCP Project containing an image to use when creating instances"`
	InstanceType     string        `desc:"Machine/Instance type to use on AWS/GCP"`
	InstanceMetadata string        `desc:"Instance Metadata to use for creating GCE instance"`
	UserDataFile     string        `desc:"User Data to use for creating EC2 instance"`
	Provider         string        `desc:"Cloud Provider to use for node tests. Valid options are ec2 and gce"`
	ImageConfigDir   string        `desc:"Path to image config files."`
	Parallelism      int           `desc:"The number of nodes to run in parallel."`
	RuntimeConfig    string        `desc:"The runtime configuration for the API server. Format: a list of key=value pairs."`
	Timeout          time.Duration `desc:"How long (in golang duration format) to wait for ginkgo tests to complete."`
	DeleteInstances  bool          `desc:"Where to delete instances after running the test"`
	NodeEnv          string        `desc:"Additional metadata keys to add to a gce instance"`

	// 预构建二进制支持的 flag。必需，不支持从源码构建。
	GinkgoBinary  string `desc:"Existing Ginkgo binary to be used on the target instead of building from source"`
	KubeletBinary string `desc:"Existing kubelet binary to be used on the target instead of building from source"`
	E2ENodeBinary string `desc:"Existing e2e-node.test binary to be used on the target instead of building from source"`

	// 由 node tester 确定的值的 flag。
	SSHUser string `desc:"Used for ssh into the remote node."`
	SSHKey  string `desc:"Used for ssh into the remote node."`
}

func NewDefaultTester() *Tester {
	return &Tester{
		Parallelism:     8,
		DeleteInstances: true,
	}
}

func (t *Tester) Execute(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet(`"e2e_node.test remote"`, flag.ExitOnError)

	// 把所有导出字段绑定到 flag。这取代了 github.com/octago/sflags/gen/gpflag。
	value := reflect.ValueOf(t).Elem()
	for field := range reflect.TypeOf(*t).Fields() {
		if !field.IsExported() {
			continue
		}
		name := fieldToFlagName(field.Name)
		desc := field.Tag.Get("desc")
		ptr := value.FieldByIndex(field.Index).Addr()
		switch ptr := ptr.Interface().(type) {
		case *string:
			fs.StringVar(ptr, name, *ptr, desc)
		case *int:
			fs.IntVar(ptr, name, *ptr, desc)
		case *bool:
			fs.BoolVar(ptr, name, *ptr, desc)
		case *time.Duration:
			fs.DurationVar(ptr, name, *ptr, desc)
		default:
			return fmt.Errorf("unsupported config field type %T", ptr)
		}
	}

	klog.InitFlags(fs)
	if err := fs.Parse(args); err != nil {
		return fmt.Errorf("parse flags: %v", err)
	}
	if err := t.validateFlags(); err != nil {
		return fmt.Errorf("validate flags: %v", err)
	}

	return t.Test()
}

// fieldToFlagName 转成小写，并在大小写边界之间
// 插入连字符。特例中，例如 "GCPProject" 变成
// "gcp-project"。
func fieldToFlagName(in string) string {
	parts := regexp.MustCompile(`[a-z0-9]+|[A-Z0-9]+`).FindAllString(in, -1)
	for i, part := range parts {
		if part[0] < 'A' || part[0] > 'Z' {
			continue
		}
		if l := len(part); l > 1 {
			// 在最后一个大写字符前拆分。
			part = part[:l-1] + "-" + part[l-1:]
		}
		if i > 0 {
			part = "-" + part
		}
		parts[i] = strings.ToLower(part)
	}
	return strings.Join(parts, "")
}

func (t *Tester) validateFlags() error {
	if t.GinkgoBinary == "" {
		return errors.New("required --ginkgo-binary path missing")
	}
	if t.KubeletBinary == "" {
		return errors.New("required --kubelet-binary path missing")
	}
	if t.E2ENodeBinary == "" {
		return errors.New("required --e2e-node-binary path missing")
	}
	if t.GCPZone == "" && t.Provider == "gce" {
		return errors.New("required --gcp-zone")
	}
	return nil
}

// configureRemote 通过 flag 配置 "remote" 包。
// 这对应 test/e2e_node/runner/remote/run_remote.go
// 被 hack/make-rules/test-e2e-node.sh 调用的方式。
func (t *Tester) configureRemote() (finalErr error) {
	for name, value := range map[string]string{
		"ssh-env": "gce", // Hard-coded as in https://github.com/kubernetes/kubernetes/blob/34341909b3e9a4854ab5d336b056b934bbbd9f16/hack/make-rules/test-e2e-node.sh#L218

		// 这些 flag 的定义见 test/e2e_node/remote/run_remote_suite.go 和 test/e2e_node/remote/gce/gce_runner.go，
		// 或查看 `go run ./test/e2e_node/runner/remote/ -help`。
		"project":           t.GCPProject,
		"zone":              t.GCPZone,
		"test_args":         t.TestArgs,
		"node-env":          t.NodeEnv,
		"delete-instances":  strconv.FormatBool(t.DeleteInstances),
		"image-config-file": t.ImageConfigFile,
		"image-config-dir":  t.ImageConfigDir,
		"image-project":     t.ImageProject,
		"images":            t.Images,
		"instance-metadata": t.InstanceMetadata,
		"instance-type":     t.InstanceType,
		"test-timeout":      t.Timeout.String(),

		"ginkgo-binary":   t.GinkgoBinary,
		"kubelet-binary":  t.KubeletBinary,
		"e2e-node-binary": t.E2ENodeBinary,

		"ssh-user": t.SSHUser,
		"ssh-key":  t.SSHKey,

		// remote runner 不使用，且不能设置，因为该 flag 只在命令中定义：
		//
		//为保持一致性、方便迁移 job，我们接受该参数。将来如果有需求，
		// 也许也会支持它。
		//
		//
		// "runtime-config":    t.RuntimeConfig,

		// 把多个不同参数合到一个参数里会因引号而有问题，
		// 但现有代码就是这么做的。更好的方案是提供 `ginkgo-flag`
		// 参数，每个 Ginkgo 参数用一次。
		//
		"ginkgo-flags": fmt.Sprintf("--nodes=%d --label-filter=%q --skip=%q --focus=%q", t.Parallelism, t.LabelFilter, t.SkipRegex, t.FocusRegex),
	} {
		if err := remote.CommandLine.Set(name, value); err != nil {
			return fmt.Errorf("set --%s to %q: %v", name, value, err)
		}
	}

	return nil
}

func (t *Tester) Test() error {
	if err := t.configureRemote(); err != nil {
		return fmt.Errorf("configure remote E2E node execution: %v", err)
	}

	suite, err := remote.GetTestSuite("default")
	if err != nil {
		return fmt.Errorf("error looking up testsuite [%v] - registered test suites [%v]", err, remote.GetTestSuiteKeys())
	}
	remote.RunRemoteTestSuite(suite)
	return nil
}

func Main(args []string) {
	// 没有 cancellation，被杀死时无需清理。
	ctx := context.Background()

	t := NewDefaultTester()
	if err := t.Execute(ctx, args); err != nil {
		fmt.Fprintf(os.Stderr, "\"%s remote\"  failed: %v\n", os.Args[0], err)
		os.Exit(1)
	}
}
