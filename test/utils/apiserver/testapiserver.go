/*
Copyright 2022 The Kubernetes Authors.

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

package apiserver

import (
	"path"
	"testing"

	"github.com/google/uuid"

	"k8s.io/apiserver/pkg/server/dynamiccertificates"
	etcdserver "k8s.io/apiserver/pkg/storage/etcd3/testserver"
	"k8s.io/apiserver/pkg/storage/storagebackend"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/util/cert"
	kubeapiservertesting "k8s.io/kubernetes/cmd/kube-apiserver/app/testing"
	"k8s.io/kubernetes/test/utils/kubeconfig"
)

// TestAPIServer 提供对正在运行的 apiserver 实例的访问。
type TestAPIServer struct {
	// ClientSet 已经初始化为以 admin 身份访问 apiserver。
	ClientSet clientset.Interface

	// KubeConfigFile 是一个 kube.config 文件的绝对路径，
	// 该文件授予对 apiserver 的 admin 访问权限。
	KubeConfigFile string
}

// StartAPIServer 在同一进程的后台运行 etcd 和 apiserver。
// 测试完成时，所有资源都会自动释放。如果启动失败，测试会被中止。
func StartAPITestServer(t *testing.T) TestAPIServer {
	cfg := etcdserver.NewTestConfig(t)
	etcdClient := etcdserver.RunEtcd(t, cfg)
	storageConfig := storagebackend.NewDefaultConfig(path.Join(uuid.New().String(), "registry"), nil)
	storageConfig.Transport.ServerList = etcdClient.Endpoints()

	server := kubeapiservertesting.StartTestServerOrDie(t, nil, []string{}, storageConfig)
	t.Cleanup(server.TearDownFn)

	clientSet := clientset.NewForConfigOrDie(server.ClientConfig)

	kubeConfigFile := writeKubeConfigForWardleServerToKASConnection(t, server.ClientConfig)

	return TestAPIServer{
		ClientSet:      clientSet,
		KubeConfigFile: kubeConfigFile,
	}
}

func writeKubeConfigForWardleServerToKASConnection(t *testing.T, kubeClientConfig *rest.Config) string {
	// 写出一个 kubeconfig，用于以委托认证方式启动其他 API server。注意，没有 in-cluster config。
	// loopback client config 使用了具有不同 SNI 的 loopback 证书。我们需要使用"真实"的
	// 证书，因此我们假设在单元测试期间不会被攻击，直接从我们启动的服务器上加载它。
	wardleToKASKubeClientConfig := rest.CopyConfig(kubeClientConfig)
	wardleToKASKubeClientConfig.ServerName = "" // 重置 SNI 以使用"真实"证书

	servingCerts, _, err := cert.GetServingCertificatesForURL(wardleToKASKubeClientConfig.Host, "")
	if err != nil {
		t.Fatal(err)
	}
	encodedServing, err := cert.EncodeCertificates(servingCerts...)
	if err != nil {
		t.Fatal(err)
	}
	wardleToKASKubeClientConfig.CAData = encodedServing

	for _, v := range servingCerts {
		t.Logf("Client: Server public key is %v\n", dynamiccertificates.GetHumanCertDetail(v))
	}
	certs, err := cert.ParseCertsPEM(wardleToKASKubeClientConfig.CAData)
	if err != nil {
		t.Fatal(err)
	}
	for _, curr := range certs {
		t.Logf("CA bundle %v\n", dynamiccertificates.GetHumanCertDetail(curr))
	}

	adminKubeConfig := kubeconfig.CreateKubeConfig(wardleToKASKubeClientConfig)
	tmpDir := t.TempDir()
	kubeConfigFile := path.Join(tmpDir, "kube.config")
	if err := clientcmd.WriteToFile(*adminKubeConfig, kubeConfigFile); err != nil {
		t.Fatal(err)
	}

	return kubeConfigFile
}
