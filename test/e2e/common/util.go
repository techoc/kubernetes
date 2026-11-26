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

package common

import (
	"bytes"
	"context"
	"fmt"
	"text/template"
	"time"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/wait"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/kubernetes/test/e2e/framework"
	e2erc "k8s.io/kubernetes/test/e2e/framework/rc"
	imageutils "k8s.io/kubernetes/test/utils/image"

	"github.com/onsi/ginkgo/v2"
)

// TODO: 清理这个文件。

// Suite 表示测试套件。
type Suite string

const (
	// E2E 表示 e2e 测试套件。
	E2E Suite = "e2e"
	// NodeE2E 表示 node e2e 测试套件。
	NodeE2E Suite = "node e2e"
)

// CurrentSuite 表示当前测试套件。
var CurrentSuite Suite

// PrePulledImages 是 e2e/common 测试中使用的镜像列表。这些镜像应在
// 测试开始前预拉取，以免测试因镜像拉取偶发失败。
// 目前只有 node e2e 测试和 E2E 测试使用它。
// 另见 ../../e2e_node/image_list.go 中的 updateImageAllowList()
// TODO(random-liu): 让镜像拉取 pod 也使用类似的机制。
var PrePulledImages = sets.NewString(
	imageutils.GetE2EImage(imageutils.AgnhostPrev),
	imageutils.GetE2EImage(imageutils.Agnhost),
	imageutils.GetE2EImage(imageutils.BusyBox),
	imageutils.GetE2EImage(imageutils.IpcUtils),
	imageutils.GetE2EImage(imageutils.Nginx),
	imageutils.GetE2EImage(imageutils.VolumeNFSServer),
	imageutils.GetE2EImage(imageutils.NonRoot),
)

// WindowsPrePulledImages 是 e2e/common 测试中使用的镜像列表。这些镜像应在
// 测试开始前预拉取，以免测试因镜像拉取偶发失败。这些镜像
// 还支持 Windows。目前只有 E2E 测试使用它们。
var WindowsPrePulledImages = sets.NewString(
	imageutils.GetE2EImage(imageutils.AgnhostPrev),
	imageutils.GetE2EImage(imageutils.Agnhost),
	imageutils.GetE2EImage(imageutils.BusyBox),
	imageutils.GetE2EImage(imageutils.Nginx),
)

type testImagesStruct struct {
	AgnhostPrevImage string
	AgnhostImage     string
	BusyBoxImage     string
	KittenImage      string
	NautilusImage    string
	NginxImage       string
	NginxNewImage    string
	PauseImage       string
}

var testImages testImagesStruct

func init() {
	testImages = testImagesStruct{
		imageutils.GetE2EImage(imageutils.AgnhostPrev),
		imageutils.GetE2EImage(imageutils.Agnhost),
		imageutils.GetE2EImage(imageutils.BusyBox),
		imageutils.GetE2EImage(imageutils.Kitten),
		imageutils.GetE2EImage(imageutils.Nautilus),
		imageutils.GetE2EImage(imageutils.Nginx),
		imageutils.GetE2EImage(imageutils.NginxNew),
		imageutils.GetE2EImage(imageutils.Pause),
	}
}

// SubstituteImageName 替换 content 中的镜像名称。
func SubstituteImageName(content string) string {
	contentWithImageName := new(bytes.Buffer)
	tmpl, err := template.New("imagemanifest").Parse(content)
	if err != nil {
		framework.Failf("Failed Parse the template: %v", err)
	}
	err = tmpl.Execute(contentWithImageName, testImages)
	if err != nil {
		framework.Failf("Failed executing template: %v", err)
	}
	return contentWithImageName.String()
}

func svcByName(name string, port int, selector map[string]string) *v1.Service {
	return &v1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
		},
		Spec: v1.ServiceSpec{
			Type:     v1.ServiceTypeNodePort,
			Selector: selector,
			Ports: []v1.ServicePort{{
				Port:       int32(port),
				TargetPort: intstr.FromInt32(int32(port)),
			}},
		},
	}
}

// NewSVCByName 用指定的 selector 创建 service。
func NewSVCByName(c clientset.Interface, ns, name string, selector map[string]string) error {
	const testPort = 9376
	_, err := c.CoreV1().Services(ns).Create(context.TODO(), svcByName(name, testPort, selector), metav1.CreateOptions{})
	return err
}

// NewRCByName 用指定标签集合作为 selector 创建 replication controller。
func NewRCByName(c clientset.Interface, ns, name string, replicas int32, gracePeriod *int64, containerArgs []string, rcLabels map[string]string) (*v1.ReplicationController, error) {
	ginkgo.By(fmt.Sprintf("creating replication controller %s", name))

	if containerArgs == nil {
		containerArgs = []string{"serve-hostname"}
	}

	return c.CoreV1().ReplicationControllers(ns).Create(context.TODO(), rcByNamePort(
		name, replicas, imageutils.GetE2EImage(imageutils.Agnhost), containerArgs, 9376, v1.ProtocolTCP, rcLabels, gracePeriod), metav1.CreateOptions{})
}

// RestartNodes 重启指定的节点。
func RestartNodes(c clientset.Interface, nodes []v1.Node) error {
	// 构建从 zone 到该 zone 内节点的映射。
	nodeNamesByZone := make(map[string][]string)
	for i := range nodes {
		node := &nodes[i]
		zone := framework.TestContext.CloudConfig.Zone
		if z, ok := node.Labels[v1.LabelFailureDomainBetaZone]; ok {
			zone = z
		} else if z, ok := node.Labels[v1.LabelTopologyZone]; ok {
			zone = z
		}
		nodeNamesByZone[zone] = append(nodeNamesByZone[zone], node.Name)
	}

	// 重启节点。
	for zone, nodeNames := range nodeNamesByZone {
		args := []string{
			"compute",
			fmt.Sprintf("--project=%s", framework.TestContext.CloudConfig.ProjectID),
			"instances",
			"reset",
		}
		args = append(args, nodeNames...)
		args = append(args, fmt.Sprintf("--zone=%s", zone))
		stdout, stderr, err := framework.RunCmd("gcloud", args...)
		if err != nil {
			return fmt.Errorf("error restarting nodes: %s\nstdout: %s\nstderr: %s", err, stdout, stderr)
		}
	}

	// 等待它们的 boot ID 发生变化。
	for i := range nodes {
		node := &nodes[i]
		if err := wait.Poll(30*time.Second, framework.RestartNodeReadyAgainTimeout, func() (bool, error) {
			newNode, err := c.CoreV1().Nodes().Get(context.TODO(), node.Name, metav1.GetOptions{})
			if err != nil {
				return false, fmt.Errorf("error getting node info after reboot: %w", err)
			}
			return node.Status.NodeInfo.BootID != newNode.Status.NodeInfo.BootID, nil
		}); err != nil {
			return fmt.Errorf("error waiting for node %s boot ID to change: %w", node.Name, err)
		}
	}
	return nil
}

// rcByNamePort 返回一个带指定名称和端口的 ReplicationController
func rcByNamePort(name string, replicas int32, image string, containerArgs []string, port int, protocol v1.Protocol,
	labels map[string]string, gracePeriod *int64) *v1.ReplicationController {

	return e2erc.ByNameContainer(name, replicas, labels, v1.Container{
		Name:  name,
		Image: image,
		Args:  containerArgs,
		Ports: []v1.ContainerPort{{ContainerPort: int32(port), Protocol: protocol}},
	}, gracePeriod)
}
