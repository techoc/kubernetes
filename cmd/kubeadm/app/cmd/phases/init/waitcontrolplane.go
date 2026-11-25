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

package phases

import (
	"fmt"
	"io"
	"time"

	v1 "k8s.io/api/core/v1"
	clientset "k8s.io/client-go/kubernetes"
	kubeletconfig "k8s.io/kubelet/config/v1beta1"

	"k8s.io/kubernetes/cmd/kubeadm/app/cmd/phases/workflow"
	"k8s.io/kubernetes/cmd/kubeadm/app/componentconfigs"
	"k8s.io/kubernetes/cmd/kubeadm/app/constants"
	"k8s.io/kubernetes/cmd/kubeadm/app/util/apiclient"
	dryrunutil "k8s.io/kubernetes/cmd/kubeadm/app/util/dryrun"
	"k8s.io/kubernetes/cmd/kubeadm/app/util/errors"
	staticpodutil "k8s.io/kubernetes/cmd/kubeadm/app/util/staticpod"
)

// NewWaitControlPlanePhase 是在控制平面和 etcd 阶段之后运行的隐藏阶段。
func NewWaitControlPlanePhase() workflow.Phase {
	phase := workflow.Phase{
		Name:  "wait-control-plane",
		Short: "Wait for the control plane to start",
		Run:   runWaitControlPlanePhase,
	}
	return phase
}

func runWaitControlPlanePhase(c workflow.RunData) error {
	data, ok := c.(InitData)
	if !ok {
		return errors.New("wait-control-plane phase invoked with an invalid data struct")
	}

	// dry-run 时打印生成的清单。
	// TODO: 考虑将此调用移到更合适的位置，例如隐藏阶段。
	if data.DryRun() {
		if err := dryrunutil.PrintFilesIfDryRunning(true /* 是否需要打印清单。 */, data.ManifestDir(), data.OutputWriter()); err != nil {
			return errors.Wrap(err, "error printing files on dryrun")
		}
	}

	client, err := data.Client()
	if err != nil {
		return errors.Wrap(err, "cannot obtain client without bootstrap")
	}

	waiter, err := newControlPlaneWaiter(data.DryRun(), 0, client, data.OutputWriter())
	if err != nil {
		return errors.Wrap(err, "error creating waiter")
	}

	fmt.Printf("[wait-control-plane] Waiting for the kubelet to boot up the control plane as static Pods"+
		" from directory %q\n",
		data.ManifestDir())

	waiter.SetTimeout(data.Cfg().Timeouts.KubeletHealthCheck.Duration)
	kubeletConfig := data.Cfg().ClusterConfiguration.ComponentConfigs[componentconfigs.KubeletGroup].Get()
	kubeletConfigTyped, ok := kubeletConfig.(*kubeletconfig.KubeletConfiguration)
	if !ok {
		return errors.New("could not convert the KubeletConfiguration to a typed object")
	}
	if err := waiter.WaitForKubelet(kubeletConfigTyped.HealthzBindAddress, *kubeletConfigTyped.HealthzPort); err != nil {
		apiclient.PrintKubeletErrorHelpScreen(data.OutputWriter())
		return errors.Wrap(err, "failed while waiting for the kubelet to start")
	}

	var podMap map[string]*v1.Pod
	waiter.SetTimeout(data.Cfg().Timeouts.ControlPlaneComponentHealthCheck.Duration)
	podMap, err = staticpodutil.ReadMultipleStaticPodsFromDisk(data.ManifestDir(),
		constants.ControlPlaneComponents...)
	if err == nil {
		err = waiter.WaitForControlPlaneComponents(podMap,
			data.Cfg().LocalAPIEndpoint.AdvertiseAddress)
	}
	if err != nil {
		apiclient.PrintControlPlaneErrorHelpScreen(data.OutputWriter(), data.Cfg().NodeRegistration.CRISocket)
		return errors.Wrap(err, "failed while waiting for the control plane to start")
	}

	return nil
}

// newControlPlaneWaiter 返回用于等待控制平面启动的新等待器。
func newControlPlaneWaiter(dryRun bool, timeout time.Duration, client clientset.Interface, out io.Writer) (apiclient.Waiter, error) {
	if dryRun {
		return dryrunutil.NewWaiter(), nil
	}

	return apiclient.NewKubeWaiter(client, timeout, out), nil
}
