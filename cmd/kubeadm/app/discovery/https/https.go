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

package https

import (
	"io"
	"net/http"
	"time"

	netutil "k8s.io/apimachinery/pkg/util/net"
	"k8s.io/client-go/tools/clientcmd"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"

	"k8s.io/kubernetes/cmd/kubeadm/app/discovery/file"
	"k8s.io/kubernetes/cmd/kubeadm/app/util/errors"
)

// RetrieveValidatedConfigInfo 从指定的 HTTPS URL 下载发现用 kubeconfig，
// 并将其交给 file.ValidateConfigInfo 执行 cluster-info ConfigMap 校验，以完成发现流程。
// HTTPS 连接本身仅根据主机的默认 TLS 信任库验证；kubeadm 在此阶段不固定到调用方提供的 CA，
// 因此 kubeconfig 内容实际上可能从任意位置获取，只有在 file.ValidateConfigInfo 成功后才受到信任。
func RetrieveValidatedConfigInfo(httpsURL string, discoveryTimeout time.Duration) (*clientcmdapi.Config, error) {
	client := &http.Client{Transport: netutil.SetOldTransportDefaults(&http.Transport{})}
	response, err := client.Get(httpsURL)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()

	if response.StatusCode != http.StatusOK {
		return nil, errors.Errorf("error trying to fetch discovery kubeconfig over HTTPS from %s, received status %d", httpsURL, response.StatusCode)
	}

	kubeconfig, err := io.ReadAll(response.Body)
	if err != nil {
		return nil, err
	}

	config, err := clientcmd.Load(kubeconfig)
	if err != nil {
		return nil, err
	}
	return file.ValidateConfigInfo(config, discoveryTimeout)
}
