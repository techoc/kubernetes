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

package apiclient

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"net/http"
	"strconv"
	"strings"
	"text/template"
	"time"

	"github.com/lithammer/dedent"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	utilerrors "k8s.io/apimachinery/pkg/util/errors"
	netutil "k8s.io/apimachinery/pkg/util/net"
	"k8s.io/apimachinery/pkg/util/wait"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/klog/v2"

	"k8s.io/kubernetes/cmd/kubeadm/app/constants"
	"k8s.io/kubernetes/cmd/kubeadm/app/util/errors"
)

const (
	// TODO: 所有组件支持后切换到 /livez，并删除 endpointHealthz 常量。
	// https://github.com/kubernetes/kubernetes/issues/118158
	endpointHealthz = "healthz"
	endpointLivez   = "livez"

	argPort        = "secure-port"
	argBindAddress = "bind-address"
	// 默认情况下，kubeadm 不为 kube-api-server 设置 --bind-address，
	// 因此应检查 --advertise-address。
	argAdvertiseAddress = "advertise-address"
)

var (
	controlPlaneFailTempl = template.Must(template.New("init").Parse(dedent.Dedent(`
	A control plane component may have crashed or exited when started by the container runtime.
	To troubleshoot, list all containers using your preferred container runtimes CLI.
	Here is one example how you may list all running Kubernetes containers by using crictl:
		- 'crictl --runtime-endpoint {{ .Socket }} ps -a | grep kube | grep -v pause'
		Once you have found the failing container, you can inspect its logs with:
		- 'crictl --runtime-endpoint {{ .Socket }} logs CONTAINERID'
`)))

	kubeletFailMsg = dedent.Dedent(`
	Unfortunately, an error has occurred, likely caused by:
		- The kubelet is not running
		- The kubelet is unhealthy due to a misconfiguration of the node in some way (required cgroups disabled)

	If you are on a systemd-powered system, you can try to troubleshoot the error with the following commands:
		- 'systemctl status kubelet'
		- 'journalctl -xeu kubelet'
`)
)

// Waiter 是等待 Kubernetes 中条件满足的接口。
type Waiter interface {
	// WaitForControlPlaneComponents 等待所有控制平面组件就绪。
	WaitForControlPlaneComponents(podMap map[string]*v1.Pod, apiServerAddress string) error
	// WaitForPodsWithLabel 等待 kube-system 命名空间中的 Pod 就绪。
	WaitForPodsWithLabel(kvLabel string) error
	// WaitForStaticPodSingleHash 获取控制平面静态 Pod 的 SHA-256 哈希。
	WaitForStaticPodSingleHash(nodeName string, component string) (string, error)
	// WaitForStaticPodHashChange 等待指定静态 Pod 组件的哈希更新，
	// 从而确认 kubelet 已重启该静态 Pod。
	WaitForStaticPodHashChange(nodeName, component, previousHash string) error
	// WaitForStaticPodControlPlaneHashes 获取控制平面静态 Pod 的 SHA-256 哈希。
	WaitForStaticPodControlPlaneHashes(nodeName string) (map[string]string, error)
	// WaitForKubelet 阻塞等待 kubelet 的 /healthz 端点返回 "ok"。
	WaitForKubelet(healthzAddress string, healthzPort int32) error
	// SetTimeout 将超时调整为指定时长。
	SetTimeout(timeout time.Duration)
}

// KubeWaiter 是基于 Kubernetes 客户端的 Waiter 实现。
type KubeWaiter struct {
	client  clientset.Interface
	timeout time.Duration
	writer  io.Writer
}

// NewKubeWaiter 返回与指定 Kubernetes 集群通信的新 Waiter。
func NewKubeWaiter(client clientset.Interface, timeout time.Duration, writer io.Writer) Waiter {
	return &KubeWaiter{
		client:  client,
		timeout: timeout,
		writer:  writer,
	}
}

// controlPlaneComponent 保存组件名称以及执行健康检查的 URL。
type controlPlaneComponent struct {
	name        string
	addressPort string
	endpoint    string
}

// getControlPlaneComponentAddressAndPort 解析静态 Pod 容器命令，提取指定参数的值。
func getControlPlaneComponentAddressAndPort(pod *v1.Pod, name string, args []string) ([]string, error) {
	var (
		values    = make([]string, len(args))
		container *v1.Container
	)

	if pod == nil {
		return values, errors.Errorf("got nil Pod for component %q", name)
	}

	for i, c := range pod.Spec.Containers {
		if len(c.Command) == 0 {
			continue
		}
		if c.Command[0] == name {
			container = &pod.Spec.Containers[i]
			break
		}
	}
	if container == nil {
		return values, errors.Errorf("the Pod has no container command starting with %q", name)
	}

	for _, line := range container.Command {
		for i, arg := range args {
			line = strings.TrimSpace(line)
			if !strings.HasPrefix(line, "--"+arg) && !strings.HasPrefix(line, "-"+arg) {
				continue
			}
			_, value, found := strings.Cut(line, "=")
			if !found {
				_, value, _ = strings.Cut(line, " ")
			}
			values[i] = value
		}
	}
	return values, nil
}

// getControlPlaneComponents 读取控制平面组件的静态 Pod，并返回 controlPlaneComponent 切片。
func getControlPlaneComponents(podMap map[string]*v1.Pod, addressAPIServer string) ([]controlPlaneComponent, error) {
	var (
		// 默认情况下，kubeadm 为 kube-controller-manager 和 kube-scheduler 设置
		// --bind-address=127.0.0.1。此值应与 get{Scheduler|ControllerManager}Command() 保持一致。
		addressKCM       = "127.0.0.1"
		addressScheduler = "127.0.0.1"

		portAPIServer = fmt.Sprintf("%d", constants.KubeAPIServerPort)
		portKCM       = fmt.Sprintf("%d", constants.KubeControllerManagerPort)
		portScheduler = fmt.Sprintf("%d", constants.KubeSchedulerPort)

		errs   []error
		result []controlPlaneComponent
	)

	type componentConfig struct {
		name        string
		args        []string
		defaultAddr string
		defaultPort string
		endpoint    string
	}

	components := []componentConfig{
		{
			name:        constants.KubeAPIServer,
			args:        []string{argAdvertiseAddress, argPort},
			defaultAddr: addressAPIServer,
			defaultPort: portAPIServer,
			endpoint:    endpointLivez,
		},
		{
			name:        constants.KubeControllerManager,
			args:        []string{argBindAddress, argPort},
			defaultAddr: addressKCM,
			defaultPort: portKCM,
			endpoint:    endpointHealthz,
		},
		{
			name:        constants.KubeScheduler,
			args:        []string{argBindAddress, argPort},
			defaultAddr: addressScheduler,
			defaultPort: portScheduler,
			endpoint:    endpointLivez,
		},
	}

	for _, component := range components {
		address, port := component.defaultAddr, component.defaultPort

		values, err := getControlPlaneComponentAddressAndPort(
			podMap[component.name],
			component.name,
			component.args,
		)
		if err != nil {
			errs = append(errs, err)
		}

		if len(values[0]) != 0 {
			address = values[0]
		}
		if len(values[1]) != 0 {
			port = values[1]
		}

		result = append(result, controlPlaneComponent{
			name:        component.name,
			addressPort: net.JoinHostPort(address, port),
			endpoint:    component.endpoint,
		})
	}

	if len(errs) > 0 {
		return nil, utilerrors.NewAggregate(errs)
	}
	return result, nil
}

// WaitForControlPlaneComponents 等待所有控制平面组件报告 "ok"。
func (w *KubeWaiter) WaitForControlPlaneComponents(podMap map[string]*v1.Pod, apiSeverAddress string) error {
	_, _ = fmt.Fprintf(w.writer, "[control-plane-check] Waiting for healthy control plane components."+
		" This can take up to %v\n", w.timeout)

	components, err := getControlPlaneComponents(podMap, apiSeverAddress)
	if err != nil {
		return errors.Wrap(err, "could not parse the address and port of all control plane components")
	}

	var errs []error
	errChan := make(chan error, len(components))

	for _, comp := range components {
		url := fmt.Sprintf("https://%s/%s", comp.addressPort, comp.endpoint)
		_, _ = fmt.Fprintf(w.writer, "[control-plane-check] Checking %s at %s\n", comp.name, url)

		go func(comp controlPlaneComponent) {
			tr := &http.Transport{
				TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
			}
			client := &http.Client{Transport: tr}
			start := time.Now()
			statusCode := 0
			var lastError error

			err := wait.PollUntilContextTimeout(
				context.Background(),
				constants.KubernetesAPICallRetryInterval,
				w.timeout,
				true, func(ctx context.Context) (bool, error) {
					// kube-apiserver 检查应使用 waiter 中的客户端，否则启用匿名认证时普通 HTTP 客户端可能失败。
					if comp.name == constants.KubeAPIServer {
						result := w.client.Discovery().RESTClient().
							Get().AbsPath(comp.endpoint).Do(ctx).StatusCode(&statusCode)
						if err := result.Error(); err != nil {
							lastError = errors.WithMessagef(err, "%s check failed at %s", comp.name, url)
							klog.V(5).Info(lastError)
							return false, nil
						}
					} else {
						resp, err := client.Get(url)
						if err != nil {
							lastError = errors.WithMessagef(err, "%s check failed at %s", comp.name, url)
							klog.V(5).Info(lastError)
							return false, nil
						}
						defer func() {
							_ = resp.Body.Close()
						}()
						statusCode = resp.StatusCode
					}

					if statusCode != http.StatusOK {
						lastError = errors.Errorf("%s check failed at %s with status: %d",
							comp.name, url, statusCode)
						klog.V(5).Info(lastError)
						return false, nil
					}

					return true, nil
				})
			if err != nil {
				_, _ = fmt.Fprintf(w.writer, "[control-plane-check] %s is not healthy after %v\n", comp.name, time.Since(start))
				errChan <- lastError
				return
			}
			_, _ = fmt.Fprintf(w.writer, "[control-plane-check] %s is healthy after %v\n", comp.name, time.Since(start))
			errChan <- nil
		}(comp)
	}

	for range components {
		if err := <-errChan; err != nil {
			errs = append(errs, err)
		}
	}
	return utilerrors.NewAggregate(errs)
}

// WaitForPodsWithLabel 查找具有指定标签的 Pod，并等待全部进入 running 状态。
func (w *KubeWaiter) WaitForPodsWithLabel(kvLabel string) error {

	lastKnownPodNumber := -1
	return wait.PollUntilContextTimeout(context.Background(),
		constants.KubernetesAPICallRetryInterval, w.timeout,
		true, func(_ context.Context) (bool, error) {
			listOpts := metav1.ListOptions{LabelSelector: kvLabel}
			pods, err := w.client.CoreV1().Pods(metav1.NamespaceSystem).List(context.Background(), listOpts)
			if err != nil {
				_, _ = fmt.Fprintf(w.writer, "[apiclient] Error getting Pods with label selector %q [%v]\n", kvLabel, err)
				return false, nil
			}

			if lastKnownPodNumber != len(pods.Items) {
				_, _ = fmt.Fprintf(w.writer, "[apiclient] Found %d Pods for label selector %s\n", len(pods.Items), kvLabel)
				lastKnownPodNumber = len(pods.Items)
			}

			if len(pods.Items) == 0 {
				return false, nil
			}

			for _, pod := range pods.Items {
				if pod.Status.Phase != v1.PodRunning {
					return false, nil
				}
			}

			return true, nil
		})
}

// WaitForKubelet 阻塞等待 kubelet 的 /healthz 端点返回 "ok"。
func (w *KubeWaiter) WaitForKubelet(healthzAddress string, healthzPort int32) error {
	var (
		lastError       error
		start           = time.Now()
		addrPort        = net.JoinHostPort(healthzAddress, strconv.Itoa(int(healthzPort)))
		healthzEndpoint = fmt.Sprintf("http://%s/healthz", addrPort)
	)

	if healthzPort == 0 {
		_, _ = fmt.Fprintln(w.writer, "[kubelet-check] Skipping the kubelet health check because the healthz port is set to 0")
		return nil
	}
	_, _ = fmt.Fprintf(w.writer, "[kubelet-check] Waiting for a healthy kubelet at %s. This can take up to %v\n",
		healthzEndpoint, w.timeout)

	formatError := func(cause string) error {
		return errors.Errorf("The HTTP call equal to 'curl -sSL %s' returned %s\n",
			healthzEndpoint, cause)
	}

	err := wait.PollUntilContextTimeout(
		context.Background(),
		constants.KubernetesAPICallRetryInterval,
		w.timeout,
		true, func(ctx context.Context) (bool, error) {
			client := &http.Client{Transport: netutil.SetOldTransportDefaults(&http.Transport{})}
			req, err := http.NewRequestWithContext(ctx, http.MethodGet, healthzEndpoint, nil)
			if err != nil {
				lastError = formatError(fmt.Sprintf("error: %v", err))
				return false, err
			}
			resp, err := client.Do(req)
			if err != nil {
				lastError = formatError(fmt.Sprintf("error: %v", err))
				return false, nil
			}
			defer func() {
				_ = resp.Body.Close()
			}()
			if resp.StatusCode != http.StatusOK {
				lastError = formatError(fmt.Sprintf("status code: %d", resp.StatusCode))
				return false, nil
			}

			return true, nil
		})
	if err != nil {
		_, _ = fmt.Fprintf(w.writer, "[kubelet-check] The kubelet is not healthy after %v\n", time.Since(start))
		return lastError
	}

	_, _ = fmt.Fprintf(w.writer, "[kubelet-check] The kubelet is healthy after %v\n", time.Since(start))
	return nil
}

// SetTimeout 将超时调整为指定时长。
func (w *KubeWaiter) SetTimeout(timeout time.Duration) {
	w.timeout = timeout
}

// WaitForStaticPodControlPlaneHashes 阻塞等待所有组件及其静态 Pod 的哈希映射，超时则返回。
func (w *KubeWaiter) WaitForStaticPodControlPlaneHashes(nodeName string) (map[string]string, error) {

	componentHash := ""
	var err, lastErr error
	mirrorPodHashes := map[string]string{}
	for _, component := range constants.ControlPlaneComponents {
		err = wait.PollUntilContextTimeout(context.Background(),
			constants.KubernetesAPICallRetryInterval, w.timeout,
			true, func(_ context.Context) (bool, error) {
				componentHash, err = getStaticPodSingleHash(w.client, nodeName, component)
				if err != nil {
					lastErr = err
					return false, nil
				}
				return true, nil
			})
		if err != nil {
			return nil, lastErr
		}
		mirrorPodHashes[component] = componentHash
	}

	return mirrorPodHashes, nil
}

// WaitForStaticPodSingleHash 阻塞等待单个组件及其静态 Pod 的哈希，超时则返回。
func (w *KubeWaiter) WaitForStaticPodSingleHash(nodeName string, component string) (string, error) {

	componentPodHash := ""
	var err, lastErr error
	err = wait.PollUntilContextTimeout(context.Background(),
		constants.KubernetesAPICallRetryInterval, w.timeout,
		true, func(_ context.Context) (bool, error) {
			componentPodHash, err = getStaticPodSingleHash(w.client, nodeName, component)
			if err != nil {
				lastErr = err
				return false, nil
			}
			return true, nil
		})

	if err != nil {
		err = lastErr
	}
	return componentPodHash, err
}

// WaitForStaticPodHashChange 阻塞等待镜像 Pod（即对应静态 Pod）的 UID 变化，超时则返回。
// 这同时意味着等待 kubelet 重启该静态 Pod。
func (w *KubeWaiter) WaitForStaticPodHashChange(nodeName, component, previousHash string) error {
	var err, lastErr error
	err = wait.PollUntilContextTimeout(context.Background(),
		constants.KubernetesAPICallRetryInterval, w.timeout,
		true, func(_ context.Context) (bool, error) {
			hash, err := getStaticPodSingleHash(w.client, nodeName, component)
			if err != nil {
				lastErr = err
				return false, nil
			}
			// 将 lastErr 置空，以便后续区分 getStaticPodSingleHash() 错误与超时错误。
			lastErr = nil
			// 应持续轮询，直到 UID 改变。
			if hash == previousHash {
				return false, nil
			}

			return true, nil
		})

	// lastError 非空表示 getStaticPodSingleHash() 出错；否则 err 非空表示轮询超时。
	if lastErr != nil {
		return lastErr
	}
	return errors.Wrapf(err, "static Pod hash for component %s on Node %s did not change after %v", component, nodeName, w.timeout)
}

// getStaticPodSingleHash 计算单个静态 Pod 资源的哈希。
func getStaticPodSingleHash(client clientset.Interface, nodeName string, component string) (string, error) {

	staticPodName := fmt.Sprintf("%s-%s", component, nodeName)
	staticPod, err := client.CoreV1().Pods(metav1.NamespaceSystem).Get(context.Background(), staticPodName, metav1.GetOptions{})
	if err != nil {
		return "", errors.Wrapf(err, "failed to obtain static Pod hash for component %s on Node %s", component, nodeName)
	}

	staticPodHash := staticPod.Annotations["kubernetes.io/config.hash"]
	return staticPodHash, nil
}

// PrintControlPlaneErrorHelpScreen 在等待控制平面组件出错时打印帮助信息。
func PrintControlPlaneErrorHelpScreen(outputWriter io.Writer, criSocket string) {
	context := struct {
		Socket string
	}{
		Socket: criSocket,
	}
	_ = controlPlaneFailTempl.Execute(outputWriter, context)
	_, _ = fmt.Fprintln(outputWriter, "")
}

// PrintKubeletErrorHelpScreen 在 kubelet 出错时打印帮助信息。
func PrintKubeletErrorHelpScreen(outputWriter io.Writer) {
	_, _ = fmt.Fprintln(outputWriter, kubeletFailMsg)
}
