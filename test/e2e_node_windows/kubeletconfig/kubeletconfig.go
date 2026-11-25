/*
Copyright The Kubernetes Authors.

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

package kubeletconfig

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"time"

	"github.com/onsi/gomega"
	"k8s.io/apimachinery/pkg/util/wait"
	kubeletconfigv1beta1 "k8s.io/kubelet/config/v1beta1"
	"k8s.io/kubernetes/pkg/cluster/ports"
	kubeletconfig "k8s.io/kubernetes/pkg/kubelet/apis/config"
	kubeletconfigscheme "k8s.io/kubernetes/pkg/kubelet/apis/config/scheme"
	kubeletconfigcodec "k8s.io/kubernetes/pkg/kubelet/kubeletconfig/util/codec"
	"k8s.io/kubernetes/test/e2e/framework"
	e2ekubectl "k8s.io/kubernetes/test/e2e/framework/kubectl"

	"sigs.k8s.io/yaml"
)

func getKubeletConfigFilePath() (string, error) {
	cwd, err := os.Getwd()
	if err != nil {
		return "", fmt.Errorf("failed to get current working directory: %w", err)
	}

	// 不要将文件命名为 "kubelet"，否则会覆盖 kubelet 二进制并造成困惑 :)
	return filepath.Join(cwd, "kubelet-config"), nil
}

// GetCurrentKubeletConfigFromFile 返回文件系统上的当前 kubelet 配置。
// 该方法只能与 e2e node 测试一起运行，即测试执行器与集群节点必须是同一台机器
func GetCurrentKubeletConfigFromFile() (*kubeletconfig.KubeletConfiguration, error) {
	kubeletConfigFilePath, err := getKubeletConfigFilePath()
	if err != nil {
		return nil, err
	}

	data, err := os.ReadFile(kubeletConfigFilePath)
	if err != nil {
		return nil, fmt.Errorf("failed to get the kubelet config from the file %q: %w", kubeletConfigFilePath, err)
	}

	var kubeletConfigV1Beta1 kubeletconfigv1beta1.KubeletConfiguration
	if err := yaml.Unmarshal(data, &kubeletConfigV1Beta1); err != nil {
		return nil, fmt.Errorf("failed to unmarshal the kubelet config: %w", err)
	}

	scheme, _, err := kubeletconfigscheme.NewSchemeAndCodecs()
	if err != nil {
		return nil, err
	}

	kubeletConfig := kubeletconfig.KubeletConfiguration{}
	err = scheme.Convert(&kubeletConfigV1Beta1, &kubeletConfig, nil)
	if err != nil {
		return nil, err
	}

	return &kubeletConfig, nil
}

// WriteKubeletConfigFile 更新文件系统上的 kubelet 配置。
// 该方法只能与 e2e node 测试一起运行，即测试执行器与集群节点必须是同一台机器
func WriteKubeletConfigFile(kubeletConfig *kubeletconfig.KubeletConfiguration) error {
	data, err := kubeletconfigcodec.EncodeKubeletConfig(kubeletConfig, kubeletconfigv1beta1.SchemeGroupVersion)
	if err != nil {
		return err
	}

	kubeletConfigFilePath, err := getKubeletConfigFilePath()
	if err != nil {
		return err
	}

	if err := os.WriteFile(kubeletConfigFilePath, data, 0644); err != nil {
		return fmt.Errorf("failed to write the kubelet file to %q: %w", kubeletConfigFilePath, err)
	}

	return nil
}

// GetCurrentKubeletConfig 获取指定节点当前的 Kubelet 配置
func GetCurrentKubeletConfig(ctx context.Context, nodeName, namespace string, useProxy bool, standaloneMode bool) (*kubeletconfig.KubeletConfiguration, error) {
	resp := pollConfigz(ctx, 5*time.Minute, 5*time.Second, nodeName, namespace, useProxy, standaloneMode)
	if len(resp) == 0 {
		return nil, fmt.Errorf("failed to fetch /configz from %q", nodeName)
	}
	kubeCfg, err := decodeConfigz(resp)
	if err != nil {
		return nil, err
	}
	return kubeCfg, nil
}

// 成功时返回 /configz 端点的 200 响应，失败时返回 nil
func pollConfigz(ctx context.Context, timeout time.Duration, pollInterval time.Duration, nodeName, namespace string, useProxy bool, standaloneMode bool) []byte {
	endpoint := ""
	if useProxy {
		// 启动本地代理，以便通过查询参数而非 body 参数发送优雅删除请求
		framework.Logf("Opening proxy to cluster")
		tk := e2ekubectl.NewTestKubeconfig(framework.TestContext.CertDir, framework.TestContext.Host, framework.TestContext.KubeConfig, framework.TestContext.KubeContext, framework.TestContext.KubectlPath, namespace)
		cmd := tk.KubectlCmd("proxy", "-p", "0")
		stdout, stderr, err := framework.StartCmdAndStreamOutput(cmd)
		framework.ExpectNoError(err)
		defer func() { _ = stdout.Close() }()
		defer func() { _ = stderr.Close() }()
		defer framework.TryKill(cmd)

		buf := make([]byte, 128)
		var n int
		n, err = stdout.Read(buf)
		framework.ExpectNoError(err)
		output := string(buf[:n])
		proxyRegexp := regexp.MustCompile("Starting to serve on 127.0.0.1:([0-9]+)")
		match := proxyRegexp.FindStringSubmatch(output)
		gomega.Expect(match).To(gomega.HaveLen(2))
		port, err := strconv.Atoi(match[1])
		framework.ExpectNoError(err)
		framework.Logf("http requesting node kubelet /configz")
		endpoint = fmt.Sprintf("http://127.0.0.1:%d/api/v1/nodes/%s/proxy/configz", port, nodeName)
	} else if !standaloneMode {
		endpoint = fmt.Sprintf("%s/api/v1/nodes/%s/proxy/configz", framework.TestContext.Host, framework.TestContext.NodeName)
	} else {
		endpoint = fmt.Sprintf("https://127.0.0.1:%d/configz", ports.KubeletPort)
	}
	tr := &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
	}
	client := &http.Client{Transport: tr}
	req, err := http.NewRequest(http.MethodGet, endpoint, nil)
	framework.ExpectNoError(err)
	if !useProxy {
		req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", framework.TestContext.BearerToken))
	}
	req.Header.Add("Accept", "application/json")

	var respBody []byte
	err = wait.PollUntilContextTimeout(ctx, pollInterval, timeout, true, func(ctx context.Context) (bool, error) {
		resp, err := client.Do(req)
		if err != nil {
			framework.Logf("Failed to get /configz, retrying. Error: %v", err)
			return false, nil
		}
		defer func() { _ = resp.Body.Close() }()

		if resp.StatusCode != http.StatusOK {
			framework.Logf("/configz response status not 200, retrying. Response was: %+v", resp)
			return false, nil
		}

		respBody, err = io.ReadAll(resp.Body)
		if err != nil {
			framework.Logf("failed to read body from /configz response, retrying. Error: %v", err)
			return false, nil
		}

		return true, nil
	})
	framework.ExpectNoError(err, "Failed to get successful response from /configz")

	return respBody
}

// 解码 /configz 的 HTTP 响应并返回 kubeletconfig.KubeletConfiguration（内部类型）。
func decodeConfigz(respBody []byte) (*kubeletconfig.KubeletConfiguration, error) {
	// 需要这个 hack 是因为 /configz 返回的结构如下：
	// {"kubeletconfig": {the JSON representation of kubeletconfigv1beta1.KubeletConfiguration}}
	type configzWrapper struct {
		ComponentConfig kubeletconfigv1beta1.KubeletConfiguration `json:"kubeletconfig"`
	}

	configz := configzWrapper{}
	kubeCfg := kubeletconfig.KubeletConfiguration{}

	err := json.Unmarshal(respBody, &configz)
	if err != nil {
		return nil, err
	}

	scheme, _, err := kubeletconfigscheme.NewSchemeAndCodecs()
	if err != nil {
		return nil, err
	}
	err = scheme.Convert(&configz.ComponentConfig, &kubeCfg, nil)
	if err != nil {
		return nil, err
	}

	return &kubeCfg, nil
}
