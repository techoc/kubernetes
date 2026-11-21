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

package bootstrap

import (
	"context"
	"crypto"
	"crypto/rsa"
	"crypto/sha512"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"k8s.io/klog/v2"

	certificatesv1 "k8s.io/api/certificates/v1"
	"k8s.io/apimachinery/pkg/types"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	restclient "k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"
	"k8s.io/client-go/transport"
	certutil "k8s.io/client-go/util/cert"
	"k8s.io/client-go/util/certificate"
	"k8s.io/client-go/util/certificate/csr"
	"k8s.io/client-go/util/keyutil"
)

// tmpPrivateKeyFile 是在 CSR 成功前缓存私钥的临时文件名。
// 必须使用独立文件，因为 store.CurrentPath() 指向由 store 管理的符号链接。
const tmpPrivateKeyFile = "kubelet-client.key.tmp"

// LoadClientConfig 加载用于获取证书以及供用户使用的客户端配置。
// 若 bootstrapPath 为空，则只检查 kubeconfigPath。
// 若 bootstrapPath 非空且 kubeconfigPath 内容有效，certConfig 与 userConfig 都指向该文件。
// 否则基于 bootstrapPath 在磁盘上生成 kubeconfigPath，并指向 certDir 中的客户端证书，
// 从而保持历史上的 bootstrap 行为：后续重启时使用最新的客户端证书申请新证书，
// 而不是继续使用初始 token。
func LoadClientConfig(logger klog.Logger, kubeconfigPath, bootstrapPath, certDir string) (certConfig, userConfig *restclient.Config, err error) {
	if len(bootstrapPath) == 0 {
		clientConfig, err := loadRESTClientConfig(kubeconfigPath)
		if err != nil {
			return nil, nil, fmt.Errorf("unable to load kubeconfig: %v", err)
		}
		logger.V(2).Info("No bootstrapping requested, will use kubeconfig")
		return clientConfig, restclient.CopyConfig(clientConfig), nil
	}

	store, err := certificate.NewFileStore("kubelet-client", certDir, certDir, "", "")
	if err != nil {
		return nil, nil, fmt.Errorf("unable to build bootstrap cert store")
	}

	ok, err := isClientConfigStillValid(kubeconfigPath)
	if err != nil {
		return nil, nil, err
	}

	// 现有 kubeconfig 仍有效，直接复用，无需 bootstrap。
	if ok {
		clientConfig, err := loadRESTClientConfig(kubeconfigPath)
		if err != nil {
			return nil, nil, fmt.Errorf("unable to load kubeconfig: %v", err)
		}
		logger.V(2).Info("Current kubeconfig file contents are still valid, no bootstrap necessary")
		return clientConfig, restclient.CopyConfig(clientConfig), nil
	}

	bootstrapClientConfig, err := loadRESTClientConfig(bootstrapPath)
	if err != nil {
		return nil, nil, fmt.Errorf("unable to load bootstrap kubeconfig: %v", err)
	}

	// 使用 bootstrap 凭据申请证书，并把 kubeconfig 指向证书目录中的 PEM 文件。
	clientConfig := restclient.AnonymousClientConfig(bootstrapClientConfig)
	pemPath := store.CurrentPath()
	clientConfig.KeyFile = pemPath
	clientConfig.CertFile = pemPath
	if err := writeKubeconfigFromBootstrapping(clientConfig, kubeconfigPath, pemPath); err != nil {
		return nil, nil, err
	}
	logger.V(2).Info("Use the bootstrap credentials to request a cert, and set kubeconfig to point to the certificate dir")
	return bootstrapClientConfig, clientConfig, nil
}

// LoadClientCert 在 kubeconfigPath 文件不存在时为 kubelet 申请客户端证书。
// 它使用 bootstrapPath 中的 kubeconfig 向 API server 申请客户端证书；
// 成功后把引用所生成密钥与证书的 kubeconfig 写入 kubeconfigPath，
// 证书与密钥文件存放在 certDir 中。
func LoadClientCert(ctx context.Context, kubeconfigPath, bootstrapPath, certDir string, nodeName types.NodeName) error {
	logger := klog.FromContext(ctx)
	// kubeconfig 已存在且有效时直接返回，避免不必要的 CSR。
	ok, err := isClientConfigStillValid(kubeconfigPath)
	if err != nil {
		return err
	}
	if ok {
		logger.V(2).Info("Kubeconfig exists and is valid, skipping bootstrap", "path", kubeconfigPath)
		return nil
	}

	logger.V(2).Info("Using bootstrap kubeconfig to generate TLS client cert, key and kubeconfig file")

	bootstrapClientConfig, err := loadRESTClientConfig(bootstrapPath)
	if err != nil {
		return fmt.Errorf("unable to load bootstrap kubeconfig: %v", err)
	}

	bootstrapClient, err := clientset.NewForConfig(bootstrapClientConfig)
	if err != nil {
		return fmt.Errorf("unable to create certificates signing request client: %v", err)
	}

	store, err := certificate.NewFileStore("kubelet-client", certDir, certDir, "", "")
	if err != nil {
		return fmt.Errorf("unable to build bootstrap cert store")
	}

	var keyData []byte
	if cert, err := store.Current(); err == nil {
		if cert.PrivateKey != nil {
			keyData, err = keyutil.MarshalPrivateKeyToPEM(cert.PrivateKey)
			if err != nil {
				keyData = nil
			}
		}
	}
	// 在 CSR 成功之前把私钥缓存到独立文件中。
	// 必须是独立文件，因为 store.CurrentPath() 指向由 store 管理的符号链接。
	privKeyPath := filepath.Join(certDir, tmpPrivateKeyFile)
	if !verifyKeyData(keyData) {
		logger.V(2).Info("No valid private key and/or certificate found, reusing existing private key or creating a new one")
		// 注意：始终调用 LoadOrGenerateKeyFile，使 CSR 失败时下次启动可以复用私钥。
		keyData, _, err = keyutil.LoadOrGenerateKeyFile(privKeyPath)
		if err != nil {
			return err
		}
	}

	// 等待 API server 可用；超时只记录日志，后续 CSR 仍会尝试。
	if err := waitForServer(ctx, *bootstrapClientConfig, 1*time.Minute); err != nil {
		logger.Info("Error waiting for apiserver to come up", "err", err)
	}

	certData, err := requestNodeCertificate(ctx, bootstrapClient, keyData, nodeName)
	if err != nil {
		return err
	}
	if _, err := store.Update(certData, keyData); err != nil {
		return err
	}
	// 证书已写入存储，临时私钥文件可以清理；删除失败仅记录日志。
	if err := os.Remove(privKeyPath); err != nil && !os.IsNotExist(err) {
		logger.V(2).Info("Failed cleaning up private key file", "path", privKeyPath, "err", err)
	}

	return writeKubeconfigFromBootstrapping(bootstrapClientConfig, kubeconfigPath, store.CurrentPath())
}

// writeKubeconfigFromBootstrapping 基于 bootstrap 客户端配置生成 kubeconfig，
// 其中认证信息指向 pemPath 中的客户端证书。
func writeKubeconfigFromBootstrapping(bootstrapClientConfig *restclient.Config, kubeconfigPath, pemPath string) error {
	// 从 bootstrap 客户端配置中提取 CA 数据。
	caFile, caData := bootstrapClientConfig.CAFile, []byte{}
	if len(caFile) == 0 {
		caData = bootstrapClientConfig.CAData
	}

	// 构造最终写入的 kubeconfig。
	kubeconfigData := clientcmdapi.Config{
		// 基于 bootstrap kubeconfig 定义集群信息。
		Clusters: map[string]*clientcmdapi.Cluster{"default-cluster": {
			Server:                   bootstrapClientConfig.Host,
			InsecureSkipTLSVerify:    bootstrapClientConfig.Insecure,
			CertificateAuthority:     caFile,
			CertificateAuthorityData: caData,
		}},
		// 基于已获取的客户端证书定义认证信息。
		AuthInfos: map[string]*clientcmdapi.AuthInfo{"default-auth": {
			ClientCertificate: pemPath,
			ClientKey:         pemPath,
		}},
		// 定义连接认证信息与集群的 context，并将其设为默认。
		Contexts: map[string]*clientcmdapi.Context{"default-context": {
			Cluster:   "default-cluster",
			AuthInfo:  "default-auth",
			Namespace: "default",
		}},
		CurrentContext: "default-context",
	}

	// 序列化并写入磁盘。
	return clientcmd.WriteToFile(kubeconfigData, kubeconfigPath)
}

// loadRESTClientConfig 从指定路径加载 kubeconfig，并转换为 restclient 配置。
func loadRESTClientConfig(kubeconfig string) (*restclient.Config, error) {
	// 从给定路径加载结构化 kubeconfig 数据。
	loader := &clientcmd.ClientConfigLoadingRules{ExplicitPath: kubeconfig}
	loadedConfig, err := loader.Load()
	if err != nil {
		return nil, err
	}
	// 基于当前 context 把加载的数据展平为具体的 restclient.Config。
	return clientcmd.NewNonInteractiveClientConfig(
		*loadedConfig,
		loadedConfig.CurrentContext,
		&clientcmd.ConfigOverrides{},
		loader,
	).ClientConfig()
}

// isClientConfigStillValid 检查给定 kubeconfig 是否包含有效的客户端证书。
// 有效时返回 true；若 bootstrapping 应立即终止则返回错误。
func isClientConfigStillValid(kubeconfigPath string) (bool, error) {
	_, err := os.Stat(kubeconfigPath)
	if os.IsNotExist(err) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("error reading existing bootstrap kubeconfig %s: %v", kubeconfigPath, err)
	}
	bootstrapClientConfig, err := loadRESTClientConfig(kubeconfigPath)
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("unable to read existing bootstrap client config from %s: %v", kubeconfigPath, err))
		return false, nil
	}
	transportConfig, err := bootstrapClientConfig.TransportConfig()
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("unable to load transport configuration from existing bootstrap client config read from %s: %v", kubeconfigPath, err))
		return false, nil
	}
	// 该调用会顺带填充 transport 配置中的数据字段。
	if _, err := transport.TLSConfigFor(transportConfig); err != nil {
		utilruntime.HandleError(fmt.Errorf("unable to load TLS configuration from existing bootstrap client config read from %s: %v", kubeconfigPath, err))
		return false, nil
	}
	certs, err := certutil.ParseCertsPEM(transportConfig.TLS.CertData)
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("unable to load TLS certificates from existing bootstrap client config read from %s: %v", kubeconfigPath, err))
		return false, nil
	}
	if len(certs) == 0 {
		utilruntime.HandleError(fmt.Errorf("unable to read TLS certificates from existing bootstrap client config read from %s: %v", kubeconfigPath, err))
		return false, nil
	}
	now := time.Now()
	for _, cert := range certs {
		if now.After(cert.NotAfter) {
			utilruntime.HandleError(fmt.Errorf("part of the existing bootstrap client certificate in %s is expired: %v", kubeconfigPath, cert.NotAfter))
			return false, nil
		}
	}
	return true, nil
}

// verifyKeyData 判断给定数据是否看起来是合法的私钥。
func verifyKeyData(data []byte) bool {
	if len(data) == 0 {
		return false
	}
	_, err := keyutil.ParsePrivateKeyPEM(data)
	return err == nil
}

// waitForServer 轮询 API server 的 /healthz 端点，直到连接成功或超时。
func waitForServer(ctx context.Context, cfg restclient.Config, deadline time.Duration) error {
	cfg.NegotiatedSerializer = scheme.Codecs.WithoutConversion()
	cfg.Timeout = 1 * time.Second
	cli, err := restclient.UnversionedRESTClientFor(&cfg)
	if err != nil {
		return fmt.Errorf("couldn't create client: %v", err)
	}

	ctx, cancel := context.WithTimeout(ctx, deadline)
	defer cancel()

	// 带抖动地重试，避免所有节点同时冲击 API server。
	var connected bool
	wait.JitterUntil(func() {
		if _, err := cli.Get().AbsPath("/healthz").Do(ctx).Raw(); err != nil {
			klog.FromContext(ctx).Info("Failed to connect to apiserver", "err", err)
			return
		}
		cancel()
		connected = true
	}, 2*time.Second, 0.2, true, ctx.Done())

	if !connected {
		return errors.New("timed out waiting to connect to apiserver")
	}
	return nil
}

// requestNodeCertificate 为节点创建证书签名请求（CSR 的 Organization 与 CommonName
// 按节点证书的约定设置）并提交给 API server，随后 watch 该 CSR 的状态；
// 一旦被 API server 批准，就返回签发好的 PEM 编码证书。
// 若出错或 watch 超时则返回错误。该函数供节点上的组件（kubelet 与 kubeadm）使用。
func requestNodeCertificate(ctx context.Context, client clientset.Interface, privateKeyData []byte, nodeName types.NodeName) (certData []byte, err error) {
	subject := &pkix.Name{
		Organization: []string{"system:nodes"},
		CommonName:   "system:node:" + string(nodeName),
	}

	privateKey, err := keyutil.ParsePrivateKeyPEM(privateKeyData)
	if err != nil {
		return nil, fmt.Errorf("invalid private key for certificate request: %v", err)
	}
	csrData, err := certutil.MakeCSR(privateKey, subject, nil, nil)
	if err != nil {
		return nil, fmt.Errorf("unable to generate certificate request: %v", err)
	}

	usages := []certificatesv1.KeyUsage{
		certificatesv1.UsageDigitalSignature,
		certificatesv1.UsageClientAuth,
	}
	if _, ok := privateKey.(*rsa.PrivateKey); ok {
		usages = append(usages, certificatesv1.UsageKeyEncipherment)
	}

	// crypto.Signer 接口提供 Public() 方法以获取公钥。
	signer, ok := privateKey.(crypto.Signer)
	if !ok {
		return nil, fmt.Errorf("private key does not implement crypto.Signer")
	}

	name, err := digestedName(signer.Public(), subject, usages)
	if err != nil {
		return nil, err
	}

	reqName, reqUID, err := csr.RequestCertificate(client, csrData, name, certificatesv1.KubeAPIServerClientKubeletSignerName, nil, usages, privateKey)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithTimeout(ctx, 3600*time.Second)
	defer cancel()

	klog.FromContext(ctx).V(2).Info("Waiting for client certificate to be issued")
	return csr.WaitForCertificate(ctx, client, reqName, reqUID)
}

// digestedName 计算 CSR 摘要名称，应包含我们关心的所有 CSR 要素。
// 不能直接对序列化后的 CSR 做哈希：CSR 含有每次循环都会重新生成的随机填充，
// 且我们还会纳入并不在 CSR 中的 usages。
// 当节点证书新增字段或调整 ensureCompatible 时，需同步更新此函数。
func digestedName(publicKey interface{}, subject *pkix.Name, usages []certificatesv1.KeyUsage) (string, error) {
	hash := sha512.New512_256()

	// 用分隔符确保两个不同输入不会写入相同的哈希流。
	// 该分隔符不在 base64.URLEncoding 字符集中，因此不会产生溢出碰撞。
	// 若没有分隔符，'CN:foo,ORG:bar' 与 'CN:foob,ORG:ar' 会得到相同哈希。
	const delimiter = '|'
	encode := base64.RawURLEncoding.EncodeToString

	write := func(data []byte) {
		hash.Write([]byte(encode(data)))
		hash.Write([]byte{delimiter})
	}

	publicKeyData, err := x509.MarshalPKIXPublicKey(publicKey)
	if err != nil {
		return "", err
	}
	write(publicKeyData)

	write([]byte(subject.CommonName))
	for _, v := range subject.Organization {
		write([]byte(v))
	}
	for _, v := range usages {
		write([]byte(v))
	}

	return fmt.Sprintf("node-csr-%s", encode(hash.Sum(nil))), nil
}
