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

package storage

import (
	"context"
	"fmt"
	"net"
	"path"

	"time"

	"github.com/onsi/ginkgo/v2"
	v1 "k8s.io/api/core/v1"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/kubernetes/test/e2e/feature"
	"k8s.io/kubernetes/test/e2e/framework"
	e2enode "k8s.io/kubernetes/test/e2e/framework/node"
	e2epod "k8s.io/kubernetes/test/e2e/framework/pod"
	e2eskipper "k8s.io/kubernetes/test/e2e/framework/skipper"
	e2essh "k8s.io/kubernetes/test/e2e/framework/ssh"
	e2etestfiles "k8s.io/kubernetes/test/e2e/framework/testfiles"
	e2evolume "k8s.io/kubernetes/test/e2e/framework/volume"
	"k8s.io/kubernetes/test/e2e/storage/utils"
	admissionapi "k8s.io/pod-security-admission/api"
)

const (
	driverDir              = "test/e2e/testing-manifests/flexvolume/"
	defaultVolumePluginDir = "/usr/libexec/kubernetes/kubelet-plugins/volume/exec"
	// TODO: GCI 默认 flex volume 安装路径变化时，同步修改本处和 config-test.sh
	// 在 gci 上 root 是只读的，且 controller-manager 运行在容器中。假设
	// controller-manager 启动时 --flex-volume-plugin-dir 设为该路径
	//（参见 cluster/gce/config-test.sh）
	gciVolumePluginDir = "/home/kubernetes/flexvolume"
	detachTimeout      = 10 * time.Second
)

// testFlexVolume 测试使用给定 flexvolume driver 的 client Pod 能否
// 成功挂载并运行
func testFlexVolume(ctx context.Context, driver string, config e2evolume.TestConfig, f *framework.Framework) {
	tests := []e2evolume.Test{
		{
			Volume: v1.VolumeSource{
				FlexVolume: &v1.FlexVolumeSource{
					Driver: "k8s/" + driver,
				},
			},
			File: "index.html",
			// 必须与 examples/volumes/flexvolume/dummy(-attachable) domount 的内容一致
			ExpectedContent: "Hello from flexvolume!",
		},
	}
	e2evolume.TestVolumeClient(ctx, f, config, nil, "" /* fsType */, tests)
}

// installFlex 在节点上安装 filePath 处的 driver，若 'restart' 为 true 则重启
// kubelet。若 node 为 nil，则安装在 master 上，且 'restart' 为 true 时重启
// controller-manager。
func installFlex(ctx context.Context, c clientset.Interface, node *v1.Node, vendor, driver, filePath string) {
	flexDir := getFlexDir(c, node, vendor, driver)
	flexFile := path.Join(flexDir, driver)

	host := ""
	var err error
	if node != nil {
		host, err = e2enode.GetSSHExternalIP(node)
		if err != nil {
			host, err = e2enode.GetSSHInternalIP(node)
		}
	} else {
		instanceWithPort := framework.APIAddress()
		hostName := getHostFromHostPort(instanceWithPort)
		host = net.JoinHostPort(hostName, e2essh.SSHPort)
	}

	framework.ExpectNoError(err)

	cmd := fmt.Sprintf("sudo mkdir -p %s", flexDir)
	sshAndLog(ctx, cmd, host, true /*failOnError*/)

	data, err := e2etestfiles.Read(filePath)
	if err != nil {
		framework.Fail(err.Error())
	}
	cmd = fmt.Sprintf("sudo tee <<'EOF' %s\n%s\nEOF", flexFile, string(data))
	sshAndLog(ctx, cmd, host, true /*failOnError*/)

	cmd = fmt.Sprintf("sudo chmod +x %s", flexFile)
	sshAndLog(ctx, cmd, host, true /*failOnError*/)
}

func uninstallFlex(ctx context.Context, c clientset.Interface, node *v1.Node, vendor, driver string) {
	flexDir := getFlexDir(c, node, vendor, driver)

	host := ""
	var err error
	if node != nil {
		host, err = e2enode.GetSSHExternalIP(node)
		if err != nil {
			host, err = e2enode.GetSSHInternalIP(node)
		}
	} else {
		instanceWithPort := framework.APIAddress()
		hostName := getHostFromHostPort(instanceWithPort)
		host = net.JoinHostPort(hostName, e2essh.SSHPort)
	}

	if host == "" {
		framework.Failf("Error getting node ip : %v", err)
	}

	cmd := fmt.Sprintf("sudo rm -r %s", flexDir)
	sshAndLog(ctx, cmd, host, false /*failOnError*/)
}

func getFlexDir(c clientset.Interface, node *v1.Node, vendor, driver string) string {
	volumePluginDir := defaultVolumePluginDir
	if framework.ProviderIs("gce") {
		volumePluginDir = gciVolumePluginDir
	}
	flexDir := path.Join(volumePluginDir, fmt.Sprintf("/%s~%s/", vendor, driver))
	return flexDir
}

func sshAndLog(ctx context.Context, cmd, host string, failOnError bool) {
	result, err := e2essh.SSH(ctx, cmd, host, framework.TestContext.Provider)
	e2essh.LogResult(result)
	framework.ExpectNoError(err)
	if result.Code != 0 && failOnError {
		framework.Failf("%s returned non-zero, stderr: %s", cmd, result.Stderr)
	}
}

func getHostFromHostPort(hostPort string) string {
	// 尝试拆分 host 和 port
	var host string
	var err error
	if host, _, err = net.SplitHostPort(hostPort); err != nil {
		// 若 SplitHostPort 返回错误，则把整个 hostport 当作 host
		host = hostPort
	}
	return host
}

var _ = utils.SIGDescribe("Flexvolumes", framework.WithProvider("gce", "local"), func() {
	f := framework.NewDefaultFramework("flexvolume")
	f.NamespacePodSecurityLevel = admissionapi.LevelBaseline

	// 注意命名空间删除由 delete-namespace flag 处理

	var cs clientset.Interface
	var ns *v1.Namespace
	var node *v1.Node
	var config e2evolume.TestConfig
	var suffix string

	ginkgo.BeforeEach(func(ctx context.Context) {
		e2eskipper.SkipUnlessMasterOSDistroIs("debian", "ubuntu", "gci", "custom")
		e2eskipper.SkipUnlessNodeOSDistroIs("debian", "ubuntu", "gci", "custom")
		e2eskipper.SkipUnlessSSHKeyPresent()

		cs = f.ClientSet
		ns = f.Namespace
		var err error
		node, err = e2enode.GetRandomReadySchedulableNode(ctx, f.ClientSet)
		framework.ExpectNoError(err)
		config = e2evolume.TestConfig{
			Namespace:           ns.Name,
			Prefix:              "flex",
			ClientNodeSelection: e2epod.NodeSelection{Name: node.Name},
		}
		suffix = ns.Name
	})

	ginkgo.It("should be mountable when non-attachable", func(ctx context.Context) {
		driver := "dummy"
		driverInstallAs := driver + "-" + suffix

		ginkgo.By(fmt.Sprintf("installing flexvolume %s on node %s as %s", path.Join(driverDir, driver), node.Name, driverInstallAs))
		installFlex(ctx, cs, node, "k8s", driverInstallAs, path.Join(driverDir, driver))

		testFlexVolume(ctx, driverInstallAs, config, f)

		ginkgo.By(fmt.Sprintf("uninstalling flexvolume %s from node %s", driverInstallAs, node.Name))
		uninstallFlex(ctx, cs, node, "k8s", driverInstallAs)
	})

	f.It("should be mountable when attachable", feature.Flexvolumes, func(ctx context.Context) {
		driver := "dummy-attachable"
		driverInstallAs := driver + "-" + suffix

		ginkgo.By(fmt.Sprintf("installing flexvolume %s on node %s as %s", path.Join(driverDir, driver), node.Name, driverInstallAs))
		installFlex(ctx, cs, node, "k8s", driverInstallAs, path.Join(driverDir, driver))
		ginkgo.By(fmt.Sprintf("installing flexvolume %s on master as %s", path.Join(driverDir, driver), driverInstallAs))
		installFlex(ctx, cs, nil, "k8s", driverInstallAs, path.Join(driverDir, driver))

		testFlexVolume(ctx, driverInstallAs, config, f)

		// Detach 可能发生在 Pod 删除之后。等待一会儿再删除 driver。
		time.Sleep(detachTimeout)

		ginkgo.By(fmt.Sprintf("uninstalling flexvolume %s from node %s", driverInstallAs, node.Name))
		uninstallFlex(ctx, cs, node, "k8s", driverInstallAs)
		ginkgo.By(fmt.Sprintf("uninstalling flexvolume %s from master", driverInstallAs))
		uninstallFlex(ctx, cs, nil, "k8s", driverInstallAs)
	})
})
