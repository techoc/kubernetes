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

package kubeconfig

import (
	"bytes"
	"context"
	"crypto"
	"crypto/x509"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"time"

	rbac "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"
	certutil "k8s.io/client-go/util/cert"
	"k8s.io/client-go/util/keyutil"
	"k8s.io/klog/v2"

	kubeadmapi "k8s.io/kubernetes/cmd/kubeadm/app/apis/kubeadm"
	kubeadmconstants "k8s.io/kubernetes/cmd/kubeadm/app/constants"
	certsphase "k8s.io/kubernetes/cmd/kubeadm/app/phases/certs"
	kubeadmutil "k8s.io/kubernetes/cmd/kubeadm/app/util"
	"k8s.io/kubernetes/cmd/kubeadm/app/util/errors"
	kubeconfigutil "k8s.io/kubernetes/cmd/kubeadm/app/util/kubeconfig"
	"k8s.io/kubernetes/cmd/kubeadm/app/util/pkiutil"
)

const (
	errInvalid = "invalid argument"
	errExist   = "file already exists"
)

// clientCertAuth 保存构建客户端证书所需的信息，用于在 kubeconfig 对象中提供认证信息。
type clientCertAuth struct {
	CAKey         crypto.Signer
	Organizations []string
}

// tokenAuth 保存在 kubeconfig 对象中使用令牌提供认证信息所需的数据。
type tokenAuth struct {
	Token string `datapolicy:"token"`
}

// kubeConfigSpec 保存构建 KubeConfig 对象所需的信息。
type kubeConfigSpec struct {
	CACert              *x509.Certificate
	APIServer           string
	ClientName          string
	ClientCertNotAfter  time.Time
	TokenAuth           *tokenAuth      `datapolicy:"token"`
	ClientCertAuth      *clientCertAuth `datapolicy:"security-key"`
	EncryptionAlgorithm kubeadmapi.EncryptionAlgorithmType
}

// CreateJoinControlPlaneKubeConfigFiles 创建 kubeadm join --control-plane 流程所需的 kubeconfig 文件，
// 以及供管理员和 kubeadm 使用的 admin kubeconfig 文件，并写入磁盘。
// 不得创建 kubelet.conf，因为该文件将由 kubelet TLS 引导流程创建并签名。
// 非外部 CA 模式下，已有 kubeconfig 仅在内容一致时复用，否则返回错误；外部 CA 模式跳过文件创建。
func CreateJoinControlPlaneKubeConfigFiles(outDir string, cfg *kubeadmapi.InitConfiguration) error {
	var externalCA bool
	caKeyPath := filepath.Join(cfg.CertificatesDir, kubeadmconstants.CAKeyName)
	if _, err := os.Stat(caKeyPath); os.IsNotExist(err) {
		externalCA = true
	}

	files := []string{
		kubeadmconstants.AdminKubeConfigFileName,
		kubeadmconstants.ControllerManagerKubeConfigFileName,
		kubeadmconstants.SchedulerKubeConfigFileName,
	}

	for _, file := range files {
		if externalCA {
			fmt.Printf("[kubeconfig] External CA mode: Using user provided %s\n", file)
			continue
		}
		if err := createKubeConfigFiles(outDir, cfg, file); err != nil {
			return err
		}
	}
	return nil
}

// CreateKubeConfigFile 创建 kubeconfig 文件。
// 已有文件仅在内容一致时复用，否则返回错误。
func CreateKubeConfigFile(kubeConfigFileName string, outDir string, cfg *kubeadmapi.InitConfiguration) error {
	klog.V(1).Infof("creating kubeconfig file for %s", kubeConfigFileName)
	return createKubeConfigFiles(outDir, cfg, kubeConfigFileName)
}

// createKubeConfigFiles 创建请求的全部 kubeconfig 文件。
// 已有文件仅在内容一致时复用，否则返回错误。
func createKubeConfigFiles(outDir string, cfg *kubeadmapi.InitConfiguration, kubeConfigFileNames ...string) error {

	// 获取按当前 InitConfiguration 填充的 KubeConfigSpecs
	specs, err := getKubeConfigSpecs(cfg)
	if err != nil {
		return err
	}

	for _, kubeConfigFileName := range kubeConfigFileNames {
		// 获取指定 kubeConfigFileName 的 KubeConfigSpec
		spec, exists := specs[kubeConfigFileName]
		if !exists {
			return errors.Errorf("couldn't retrieve KubeConfigSpec for %s", kubeConfigFileName)
		}

		// 构建 KubeConfig 对象
		config, err := buildKubeConfigFromSpec(spec, cfg.ClusterName)
		if err != nil {
			return err
		}

		// 如果 kubeconfig 不存在，写入磁盘
		if err = createKubeConfigFileIfNotExists(outDir, kubeConfigFileName, config); err != nil {
			return err
		}
	}

	return nil
}

// getKubeConfigSpecs 返回按当前 InitConfiguration 填充的全部 KubeConfigSpecs。
// 注意：此方法包含 kubeadm 创建 kubeconfig 文件所需的信息。
func getKubeConfigSpecs(cfg *kubeadmapi.InitConfiguration) (map[string]*kubeConfigSpec, error) {
	caCert, caKey, err := pkiutil.TryLoadCertAndKeyFromDisk(cfg.CertificatesDir, kubeadmconstants.CACertAndKeyBaseName)
	if os.IsNotExist(errors.Unwrap(err)) {
		return nil, errors.Wrap(err, "the CA files do not exist, please run `kubeadm init phase certs ca` to generate it")
	}
	if err != nil {
		return nil, errors.Wrap(err, "couldn't create a kubeconfig; the CA files couldn't be loaded")
	}
	// 校验有效期
	certsphase.CheckCertificatePeriodValidity(kubeadmconstants.CACertAndKeyBaseName, caCert)

	configs, err := getKubeConfigSpecsBase(cfg)
	if err != nil {
		return nil, err
	}
	for _, spec := range configs {
		spec.CACert = caCert
		spec.ClientCertAuth.CAKey = caKey
	}
	return configs, nil
}

// buildKubeConfigFromSpec 根据给定 kubeConfigSpec 创建 kubeconfig 对象。
func buildKubeConfigFromSpec(spec *kubeConfigSpec, clustername string) (*clientcmdapi.Config, error) {

	// 如果此 kubeconfig 应使用令牌
	if spec.TokenAuth != nil {
		// 创建使用令牌的 kubeconfig
		return kubeconfigutil.CreateWithToken(
			spec.APIServer,
			clustername,
			spec.ClientName,
			pkiutil.EncodeCertPEM(spec.CACert),
			spec.TokenAuth.Token,
		), nil
	}

	// 否则创建客户端证书
	clientCertConfig := newClientCertConfigFromKubeConfigSpec(spec)

	clientCert, clientKey, err := pkiutil.NewCertAndKey(spec.CACert, spec.ClientCertAuth.CAKey, &clientCertConfig)
	if err != nil {
		return nil, errors.Wrapf(err, "failure while creating %s client certificate", spec.ClientName)
	}

	encodedClientKey, err := keyutil.MarshalPrivateKeyToPEM(clientKey)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to marshal private key to PEM")
	}
	// 创建使用客户端证书的 kubeconfig
	return kubeconfigutil.CreateWithCerts(
		spec.APIServer,
		clustername,
		spec.ClientName,
		pkiutil.EncodeCertPEM(spec.CACert),
		encodedClientKey,
		pkiutil.EncodeCertPEM(clientCert),
	), nil
}

func newClientCertConfigFromKubeConfigSpec(spec *kubeConfigSpec) pkiutil.CertConfig {
	return pkiutil.CertConfig{
		Config: certutil.Config{
			CommonName:   spec.ClientName,
			Organization: spec.ClientCertAuth.Organizations,
			Usages:       []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
		},
		NotAfter:            spec.ClientCertNotAfter,
		EncryptionAlgorithm: spec.EncryptionAlgorithm,
	}
}

// validateKubeConfig 检查 kubeconfig 文件是否存在，以及 CA 和服务器 URL 是否符合预期。
func validateKubeConfig(outDir, filename string, config *clientcmdapi.Config) error {
	kubeConfigFilePath := filepath.Join(outDir, filename)

	if _, err := os.Stat(kubeConfigFilePath); err != nil {
		return err
	}

	// kubeconfig 已存在，检查其 CA 和服务器 URL 是否相同
	currentConfig, err := clientcmd.LoadFromFile(kubeConfigFilePath)
	if err != nil {
		return errors.Wrapf(err, "failed to load kubeconfig file %s that already exists on disk", kubeConfigFilePath)
	}

	expectedCtx, exists := config.Contexts[config.CurrentContext]
	if !exists {
		return errors.Errorf("failed to find expected context %s", config.CurrentContext)
	}
	expectedCluster := expectedCtx.Cluster
	currentCtx, exists := currentConfig.Contexts[currentConfig.CurrentContext]
	if !exists {
		return errors.Errorf("failed to find CurrentContext in Contexts of the kubeconfig file %s", kubeConfigFilePath)
	}
	currentCluster := currentCtx.Cluster
	if currentConfig.Clusters[currentCluster] == nil {
		return errors.Errorf("failed to find the given CurrentContext Cluster in Clusters of the kubeconfig file %s", kubeConfigFilePath)
	}

	// 比较前去除 CA 两端的空白。clientcmd.LoadFromFile() 仅解码 base64 CA 并原样放入 v1.Config；
	// 如果用户创建 kubeconfig 时使用的 CA 含多余空白，与生成的 v1.Config 比较会失败。
	caCurrent := bytes.TrimSpace(currentConfig.Clusters[currentCluster].CertificateAuthorityData)
	if len(caCurrent) == 0 {
		// 回退到从外部 CA 文件加载 CA 证书数据
		clusterCAFilePath := currentConfig.Clusters[currentCluster].CertificateAuthority
		if len(clusterCAFilePath) > 0 {
			clusterCABytes, err := os.ReadFile(clusterCAFilePath)
			if err != nil {
				klog.Warningf("failed to load CA cert from %q for kubeconfig %q, %v", clusterCAFilePath, kubeConfigFilePath, err)
			} else {
				caCurrent = bytes.TrimSpace(clusterCABytes)
			}
		}
	}
	caExpected := bytes.TrimSpace(config.Clusters[expectedCluster].CertificateAuthorityData)

	// 解析当前 CA 数据
	currentCACerts, err := certutil.ParseCertsPEM(caCurrent)
	if err != nil {
		return errors.Errorf("the kubeconfig file %q contains an invalid CA cert", kubeConfigFilePath)
	}

	// 解析预期 CA 数据
	expectedCACerts, err := certutil.ParseCertsPEM(caExpected)
	if err != nil {
		return errors.Errorf("the expected base64 encoded CA cert could not be parsed as a PEM:\n%s\n", caExpected)
	}

	// 仅使用当前 CA 证书列表中的第一项
	currentCaCert := currentCACerts[0]

	// 查找共同的信任锚
	trustAnchorFound := slices.ContainsFunc(expectedCACerts, currentCaCert.Equal)
	if !trustAnchorFound {
		return errors.Errorf("a kubeconfig file %q exists but does not contain a trusted CA in its current context's "+
			"cluster. Total CA certificates found: %d", kubeConfigFilePath, len(currentCACerts))
	}

	// 如果磁盘上的 API Server 位置与预期不符，输出警告
	if currentConfig.Clusters[currentCluster].Server != config.Clusters[expectedCluster].Server {
		klog.Warningf("a kubeconfig file %q exists already but has an unexpected API Server URL: expected: %s, got: %s",
			kubeConfigFilePath, config.Clusters[expectedCluster].Server, currentConfig.Clusters[currentCluster].Server)
	}

	return nil
}

// createKubeConfigFileIfNotExists 在给定路径无文件时将 KubeConfig 对象保存到文件。
// 如果已有 kubeconfig，kubeadm 会加载并比较现有配置与预期配置；内容一致时跳过写入。
// 如果文件内容过期或不是 kubeconfig，则返回错误。
func createKubeConfigFileIfNotExists(outDir, filename string, config *clientcmdapi.Config) error {
	kubeConfigFilePath := filepath.Join(outDir, filename)

	err := validateKubeConfig(outDir, filename, config)
	if err != nil {
		// 检查文件是否存在，不存在则写入磁盘
		if !os.IsNotExist(err) {
			return err
		}
		fmt.Printf("[kubeconfig] Writing %q kubeconfig file\n", filename)
		err = kubeconfigutil.WriteToDisk(kubeConfigFilePath, config)
		return errors.Wrapf(err, "failed to save kubeconfig file %q on disk", kubeConfigFilePath)
	}
	// kubeadm 对现有 kubeconfig 仅做此校验（信任客户端证书有效）：
	// 如果路径、CA 证书和服务器 URL 均相同，就认为文件一致，不再写入新文件。
	fmt.Printf("[kubeconfig] Using existing kubeconfig file: %q\n", kubeConfigFilePath)

	return nil
}

// WriteKubeConfigWithClientCert 将使用客户端证书认证的 kubeconfig 写入给定 Writer。
func WriteKubeConfigWithClientCert(out io.Writer, cfg *kubeadmapi.InitConfiguration, clientName string, organizations []string, notAfter time.Time) error {

	// 创建按当前 InitConfiguration 填充的 KubeConfigSpecs
	caCert, caKey, err := pkiutil.TryLoadCertAndKeyFromDisk(cfg.CertificatesDir, kubeadmconstants.CACertAndKeyBaseName)
	if err != nil {
		return errors.Wrap(err, "couldn't create a kubeconfig; the CA files couldn't be loaded")
	}
	// 校验有效期
	certsphase.CheckCertificatePeriodValidity(kubeadmconstants.CACertAndKeyBaseName, caCert)

	controlPlaneEndpoint, err := kubeadmutil.GetControlPlaneEndpoint(cfg.ControlPlaneEndpoint, &cfg.LocalAPIEndpoint)
	if err != nil {
		return err
	}

	spec := &kubeConfigSpec{
		ClientName: clientName,
		APIServer:  controlPlaneEndpoint,
		CACert:     caCert,
		ClientCertAuth: &clientCertAuth{
			CAKey:         caKey,
			Organizations: organizations,
		},
		ClientCertNotAfter:  notAfter,
		EncryptionAlgorithm: cfg.ClusterConfiguration.EncryptionAlgorithm,
	}

	return writeKubeConfigFromSpec(out, spec, cfg.ClusterName)
}

// WriteKubeConfigWithToken 将使用令牌认证的 kubeconfig 写入给定 Writer。
func WriteKubeConfigWithToken(out io.Writer, cfg *kubeadmapi.InitConfiguration, clientName, token string, notAfter time.Time) error {

	// 创建按当前 InitConfiguration 填充的 KubeConfigSpecs
	caCert, _, err := pkiutil.TryLoadCertAndKeyFromDisk(cfg.CertificatesDir, kubeadmconstants.CACertAndKeyBaseName)
	if err != nil {
		return errors.Wrap(err, "couldn't create a kubeconfig; the CA files couldn't be loaded")
	}
	// 校验有效期
	certsphase.CheckCertificatePeriodValidity(kubeadmconstants.CACertAndKeyBaseName, caCert)

	controlPlaneEndpoint, err := kubeadmutil.GetControlPlaneEndpoint(cfg.ControlPlaneEndpoint, &cfg.LocalAPIEndpoint)
	if err != nil {
		return err
	}

	spec := &kubeConfigSpec{
		ClientName: clientName,
		APIServer:  controlPlaneEndpoint,
		CACert:     caCert,
		TokenAuth: &tokenAuth{
			Token: token,
		},
		ClientCertNotAfter:  notAfter,
		EncryptionAlgorithm: cfg.ClusterConfiguration.EncryptionAlgorithm,
	}

	return writeKubeConfigFromSpec(out, spec, cfg.ClusterName)
}

// writeKubeConfigFromSpec 根据 kubeConfigSpec 创建 kubeconfig 对象并写入给定 Writer。
func writeKubeConfigFromSpec(out io.Writer, spec *kubeConfigSpec, clustername string) error {

	// 构建 KubeConfig 对象
	config, err := buildKubeConfigFromSpec(spec, clustername)
	if err != nil {
		return err
	}

	// 如果 kubeconfig 不存在，写入磁盘
	configBytes, err := clientcmd.Write(*config)
	if err != nil {
		return errors.Wrap(err, "failure while serializing admin kubeconfig")
	}

	fmt.Fprintln(out, string(configBytes))
	return nil
}

// ValidateKubeconfigsForExternalCA 使用 kubeadmapi.InitConfiguration 检查 kubeconfig 是否存在，以及 CA 和服务器 URL 是否符合预期。
func ValidateKubeconfigsForExternalCA(outDir string, cfg *kubeadmapi.InitConfiguration) error {
	// 创建包含目标 CA 和服务器 URL 的 kubeconfig，
	// 作为校验用户提供的 kubeconfig 文件的输入。
	caCert, intermediaries, err := pkiutil.TryLoadCertChainFromDisk(cfg.CertificatesDir, kubeadmconstants.CACertAndKeyBaseName)
	if err != nil {
		return errors.Wrapf(err, "the CA file couldn't be loaded")
	}

	// 将 caCert 和中间证书合并为一个数组
	caCertChain := append([]*x509.Certificate{caCert}, intermediaries...)

	// 校验有效期
	for _, cert := range caCertChain {
		certsphase.CheckCertificatePeriodValidity(kubeadmconstants.CACertAndKeyBaseName, cert)
	}

	// 校验用户为调度器和 Controller Manager 提供的 kubeconfig 文件
	localAPIEndpoint, err := kubeadmutil.GetLocalAPIEndpoint(&cfg.LocalAPIEndpoint)
	if err != nil {
		return err
	}

	caCertBytes, err := pkiutil.EncodeCertBundlePEM(caCertChain)
	if err != nil {
		return err
	}

	validationConfigLocal := kubeconfigutil.CreateBasic(localAPIEndpoint, "dummy", "dummy", caCertBytes)
	kubeConfigFileNamesLocal := []string{
		kubeadmconstants.ControllerManagerKubeConfigFileName,
		kubeadmconstants.SchedulerKubeConfigFileName,
	}

	for _, kubeConfigFileName := range kubeConfigFileNamesLocal {
		if err = validateKubeConfig(outDir, kubeConfigFileName, validationConfigLocal); err != nil {
			return errors.Wrapf(err, "the %s file does not exists or it is not valid", kubeConfigFileName)
		}
	}

	// 校验用户为 kubelet 和管理员提供的 kubeconfig 文件
	controlPlaneEndpoint, err := kubeadmutil.GetControlPlaneEndpoint(cfg.ControlPlaneEndpoint, &cfg.LocalAPIEndpoint)
	if err != nil {
		return err
	}

	validationConfigCPE := kubeconfigutil.CreateBasic(controlPlaneEndpoint, "dummy", "dummy", caCertBytes)
	kubeConfigFileNamesCPE := []string{
		kubeadmconstants.AdminKubeConfigFileName,
		kubeadmconstants.SuperAdminKubeConfigFileName,
		kubeadmconstants.KubeletKubeConfigFileName,
	}

	for _, kubeConfigFileName := range kubeConfigFileNamesCPE {
		if err = validateKubeConfig(outDir, kubeConfigFileName, validationConfigCPE); err != nil {
			return errors.Wrapf(err, "the %s file does not exists or it is not valid", kubeConfigFileName)
		}
	}

	return nil
}

func getKubeConfigSpecsBase(cfg *kubeadmapi.InitConfiguration) (map[string]*kubeConfigSpec, error) {
	controlPlaneEndpoint, err := kubeadmutil.GetControlPlaneEndpoint(cfg.ControlPlaneEndpoint, &cfg.LocalAPIEndpoint)
	if err != nil {
		return nil, err
	}
	localAPIEndpoint, err := kubeadmutil.GetLocalAPIEndpoint(&cfg.LocalAPIEndpoint)
	if err != nil {
		return nil, err
	}

	startTime := kubeadmutil.StartTimeUTC()
	notAfter := startTime.Add(kubeadmconstants.CertificateValidityPeriod)
	if cfg.ClusterConfiguration.CertificateValidityPeriod != nil {
		notAfter = startTime.Add(cfg.ClusterConfiguration.CertificateValidityPeriod.Duration)
	}

	return map[string]*kubeConfigSpec{
		kubeadmconstants.AdminKubeConfigFileName: {
			APIServer:  controlPlaneEndpoint,
			ClientName: "kubernetes-admin",
			ClientCertAuth: &clientCertAuth{
				Organizations: []string{kubeadmconstants.ClusterAdminsGroupAndClusterRoleBinding},
			},
			ClientCertNotAfter:  notAfter,
			EncryptionAlgorithm: cfg.ClusterConfiguration.EncryptionAlgorithm,
		},
		kubeadmconstants.SuperAdminKubeConfigFileName: {
			APIServer:  controlPlaneEndpoint,
			ClientName: "kubernetes-super-admin",
			ClientCertAuth: &clientCertAuth{
				Organizations: []string{kubeadmconstants.SystemPrivilegedGroup},
			},
			ClientCertNotAfter:  notAfter,
			EncryptionAlgorithm: cfg.ClusterConfiguration.EncryptionAlgorithm,
		},
		kubeadmconstants.KubeletKubeConfigFileName: {
			APIServer:  controlPlaneEndpoint,
			ClientName: fmt.Sprintf("%s%s", kubeadmconstants.NodesUserPrefix, cfg.NodeRegistration.Name),
			ClientCertAuth: &clientCertAuth{
				Organizations: []string{kubeadmconstants.NodesGroup},
			},
			ClientCertNotAfter:  notAfter,
			EncryptionAlgorithm: cfg.ClusterConfiguration.EncryptionAlgorithm,
		},
		kubeadmconstants.ControllerManagerKubeConfigFileName: {
			APIServer:           localAPIEndpoint,
			ClientName:          kubeadmconstants.ControllerManagerUser,
			ClientCertAuth:      &clientCertAuth{},
			ClientCertNotAfter:  notAfter,
			EncryptionAlgorithm: cfg.ClusterConfiguration.EncryptionAlgorithm,
		},
		kubeadmconstants.SchedulerKubeConfigFileName: {
			APIServer:           localAPIEndpoint,
			ClientName:          kubeadmconstants.SchedulerUser,
			ClientCertAuth:      &clientCertAuth{},
			ClientCertNotAfter:  notAfter,
			EncryptionAlgorithm: cfg.ClusterConfiguration.EncryptionAlgorithm,
		},
	}, nil
}

func createKubeConfigAndCSR(kubeConfigDir string, kubeadmConfig *kubeadmapi.InitConfiguration, name string, spec *kubeConfigSpec) error {
	if kubeConfigDir == "" {
		return errors.Errorf("%s: kubeConfigDir was empty", errInvalid)
	}
	if kubeadmConfig == nil {
		return errors.Errorf("%s: kubeadmConfig was nil", errInvalid)
	}
	if name == "" {
		return errors.Errorf("%s: name was empty", errInvalid)
	}
	if spec == nil {
		return errors.Errorf("%s: spec was nil", errInvalid)
	}
	kubeConfigPath := filepath.Join(kubeConfigDir, name)
	if _, err := os.Stat(kubeConfigPath); err == nil {
		return errors.Errorf("%s: kube config: %s", errExist, kubeConfigPath)
	} else if !os.IsNotExist(err) {
		return errors.Wrapf(err, "unexpected error while checking if file exists: %s", kubeConfigPath)
	}
	if pkiutil.CSROrKeyExist(kubeConfigDir, name) {
		return errors.Errorf("%s: csr: %s", errExist, kubeConfigPath)
	}

	clientCertConfig := newClientCertConfigFromKubeConfigSpec(spec)

	clientKey, err := pkiutil.NewPrivateKey(clientCertConfig.EncryptionAlgorithm)
	if err != nil {
		return err
	}
	clientCSR, err := pkiutil.NewCSR(clientCertConfig, clientKey)
	if err != nil {
		return err
	}

	encodedClientKey, err := keyutil.MarshalPrivateKeyToPEM(clientKey)
	if err != nil {
		return err
	}

	var (
		emptyCACert     []byte
		emptyClientCert []byte
	)

	// 创建使用客户端证书的 kubeconfig
	config := kubeconfigutil.CreateWithCerts(
		spec.APIServer,
		kubeadmConfig.ClusterName,
		spec.ClientName,
		emptyCACert,
		encodedClientKey,
		emptyClientCert,
	)

	if err := kubeconfigutil.WriteToDisk(kubeConfigPath, config); err != nil {
		return err
	}
	// 将 CSR 写入磁盘
	if err := pkiutil.WriteCSR(kubeConfigDir, name, clientCSR); err != nil {
		return err
	}
	return nil
}

// CreateDefaultKubeConfigsAndCSRFiles 用于 ExternalCA 模式，创建 kubeconfig 文件及同目录的 CSR 文件。
func CreateDefaultKubeConfigsAndCSRFiles(out io.Writer, kubeConfigDir string, kubeadmConfig *kubeadmapi.InitConfiguration) error {
	kubeConfigs, err := getKubeConfigSpecsBase(kubeadmConfig)
	if err != nil {
		return err
	}
	if out != nil {
		fmt.Fprintf(out, "generating keys and CSRs in %s\n", kubeConfigDir)
	}
	for name, spec := range kubeConfigs {
		if err := createKubeConfigAndCSR(kubeConfigDir, kubeadmConfig, name, spec); err != nil {
			return err
		}
		if out != nil {
			fmt.Fprintf(out, "  %s\n", name)
		}
	}
	return nil
}

// EnsureRBACFunc 定义可传给 EnsureAdminClusterRoleBinding() 的函数类型。
type EnsureRBACFunc func(context.Context, clientset.Interface, clientset.Interface, time.Duration, time.Duration) (clientset.Interface, error)

// EnsureAdminClusterRoleBinding 从 admin.conf 创建客户端；如果 super-admin.conf 存在，也据此创建客户端。
// 然后将客户端传给 EnsureAdminClusterRoleBindingImpl。
// 函数返回已正确配置 RBAC、可用的 admin.conf 客户端，或返回错误。
func EnsureAdminClusterRoleBinding(outDir string, lae *kubeadmapi.APIEndpoint, ensureRBACFunc EnsureRBACFunc) (clientset.Interface, error) {
	var (
		err                           error
		adminClient, superAdminClient clientset.Interface
	)

	// 从 admin.conf 创建客户端。
	kubeconfig, err := clientcmd.LoadFromFile(filepath.Join(outDir, kubeadmconstants.AdminKubeConfigFileName))
	if err != nil {
		return nil, err
	}
	kubeconfigutil.PointKubeConfigToLocalAPIEndpoint(kubeconfig, lae)
	adminClient, err = kubeconfigutil.ToClientSet(kubeconfig)
	if err != nil {
		return nil, err
	}

	// 从 super-admin.conf 创建客户端。
	superAdminPath := filepath.Join(outDir, kubeadmconstants.SuperAdminKubeConfigFileName)
	if _, err := os.Stat(superAdminPath); err == nil {
		kubeconfig, err := clientcmd.LoadFromFile(superAdminPath)
		if err != nil {
			return nil, err
		}
		kubeconfigutil.PointKubeConfigToLocalAPIEndpoint(kubeconfig, lae)
		superAdminClient, err = kubeconfigutil.ToClientSet(kubeconfig)
		if err != nil {
			return nil, err
		}
	}

	if ensureRBACFunc == nil {
		ensureRBACFunc = EnsureAdminClusterRoleBindingImpl
	}

	ctx := context.Background()
	return ensureRBACFunc(
		ctx, adminClient, superAdminClient,
		kubeadmconstants.KubernetesAPICallRetryInterval, kubeadmapi.GetActiveTimeouts().KubernetesAPICall.Duration,
	)
}

// EnsureAdminClusterRoleBindingImpl 先使用 adminClient 检查 ClusterRoleBinding kubeadm:cluster-admins 是否存在。
// 如果存在，说明 adminClient 可用；否则尝试使用 superAdminClient 创建该绑定。
func EnsureAdminClusterRoleBindingImpl(ctx context.Context, adminClient, superAdminClient clientset.Interface,
	retryInterval, retryTimeout time.Duration) (clientset.Interface, error) {

	klog.V(1).Infof("ensuring that the ClusterRoleBinding for the %s Group exists",
		kubeadmconstants.ClusterAdminsGroupAndClusterRoleBinding)

	var (
		err, lastError     error
		crbExists          bool
		clusterRoleBinding = &rbac.ClusterRoleBinding{
			ObjectMeta: metav1.ObjectMeta{
				Name: kubeadmconstants.ClusterAdminsGroupAndClusterRoleBinding,
			},
			RoleRef: rbac.RoleRef{
				APIGroup: rbac.GroupName,
				Kind:     "ClusterRole",
				Name:     "cluster-admin",
			},
			Subjects: []rbac.Subject{
				{
					Kind: rbac.GroupKind,
					Name: kubeadmconstants.ClusterAdminsGroupAndClusterRoleBinding,
				},
			},
		}
	)

	// 先使用 admin.conf 客户端创建 CRB。如果 admin.conf 中的用户绑定到内置超级用户组，即可成功；
	// 其他情况会返回错误。此处轮询用于确保 "kubeadm init" 流程中 API Server 可访问。
	err = wait.PollUntilContextTimeout(
		ctx,
		retryInterval,
		retryTimeout,
		true, func(ctx context.Context) (bool, error) {
			if _, err := adminClient.RbacV1().ClusterRoleBindings().Create(
				ctx,
				clusterRoleBinding,
				metav1.CreateOptions{},
			); err != nil {
				if apierrors.IsForbidden(err) {
					// 如果遇到禁止访问错误，说明 API Server 可访问，但 CRB 不存在，
					// 即 admin.conf 用户尚无权限创建授予自身权限的 RBAC。
					return true, nil
				} else if apierrors.IsAlreadyExists(err) {
					// 如果 CRB 存在，说明 admin.conf 已有正确权限，直接返回。
					crbExists = true
					return true, nil
				} else {
					// 遇到其他类型的错误时重试。
					lastError = errors.Wrap(err, "unable to create ClusterRoleBinding")
					return false, nil
				}
			}
			crbExists = true
			return true, nil
		})
	if err != nil {
		return nil, lastError
	}

	// CRB 已创建或原本存在，返回 admin.conf 客户端。
	if crbExists {
		return adminClient, nil
	}

	// 此时如果 superAdminClient 为 nil，无法继续创建 CRB，返回错误。
	if superAdminClient == nil {
		return nil, errors.Errorf("the ClusterRoleBinding for the %s Group is missing but there is no %s to create it",
			kubeadmconstants.ClusterAdminsGroupAndClusterRoleBinding,
			kubeadmconstants.SuperAdminKubeConfigFileName)
	}

	// 使用 super-admin.conf 客户端创建 ClusterRoleBinding。
	klog.V(1).Infof("creating the ClusterRoleBinding for the %s Group by using %s",
		kubeadmconstants.ClusterAdminsGroupAndClusterRoleBinding,
		kubeadmconstants.SuperAdminKubeConfigFileName)

	err = wait.PollUntilContextTimeout(
		ctx,
		retryInterval,
		retryTimeout,
		true, func(ctx context.Context) (bool, error) {
			if _, err := superAdminClient.RbacV1().ClusterRoleBindings().Create(
				ctx,
				clusterRoleBinding,
				metav1.CreateOptions{},
			); err != nil {
				lastError = err
				if apierrors.IsAlreadyExists(err) {
					klog.V(5).Infof("ClusterRoleBinding %s already exists.", kubeadmconstants.ClusterAdminsGroupAndClusterRoleBinding)
					return true, nil
				}
				// 遇到其他类型的错误时重试。
				return false, nil
			}
			return true, nil
		})
	if err != nil {
		return nil, errors.Wrapf(lastError, "unable to create the %s ClusterRoleBinding by using %s",
			kubeadmconstants.ClusterAdminsGroupAndClusterRoleBinding,
			kubeadmconstants.SuperAdminKubeConfigFileName)
	}

	// CRB 就位后，开始使用 admin.conf 客户端。
	return adminClient, nil
}
