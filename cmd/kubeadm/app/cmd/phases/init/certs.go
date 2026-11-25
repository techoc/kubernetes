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

package phases

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	kubeadmapi "k8s.io/kubernetes/cmd/kubeadm/app/apis/kubeadm"
	kubeadmscheme "k8s.io/kubernetes/cmd/kubeadm/app/apis/kubeadm/scheme"
	kubeadmapiv1 "k8s.io/kubernetes/cmd/kubeadm/app/apis/kubeadm/v1beta4"
	"k8s.io/kubernetes/cmd/kubeadm/app/cmd/options"
	"k8s.io/kubernetes/cmd/kubeadm/app/cmd/phases/workflow"
	cmdutil "k8s.io/kubernetes/cmd/kubeadm/app/cmd/util"
	kubeadmconstants "k8s.io/kubernetes/cmd/kubeadm/app/constants"
	certsphase "k8s.io/kubernetes/cmd/kubeadm/app/phases/certs"
	"k8s.io/kubernetes/cmd/kubeadm/app/util/errors"
	filesutil "k8s.io/kubernetes/cmd/kubeadm/app/util/files"
	"k8s.io/kubernetes/cmd/kubeadm/app/util/pkiutil"
)

var (
	saKeyLongDesc = fmt.Sprintf(cmdutil.LongDesc(`
		Generate the private key for signing service account tokens along with its public key, and save them into
		%s and %s files.

		If both files already exist, kubeadm skips the generation step and existing files will be used.
		`), kubeadmconstants.ServiceAccountPrivateKeyName, kubeadmconstants.ServiceAccountPublicKeyName)

	genericLongDesc = cmdutil.LongDesc(`
		Generate the %[1]s, and save them into %[2]s.crt and %[2]s.key files.%[3]s

		If both files already exist, kubeadm skips the generation step and existing files will be used.
		`)
)

// NewCertsPhase 返回证书阶段。
func NewCertsPhase() workflow.Phase {
	return workflow.Phase{
		Name:   "certs",
		Short:  "Certificate generation",
		Phases: newCertSubPhases(),
		Run:    runCerts,
	}
}

// newCertSubPhases 返回证书阶段的子阶段。
func newCertSubPhases() []workflow.Phase {
	subPhases := []workflow.Phase{}

	// 全部子阶段。
	allPhase := workflow.Phase{
		Name:           "all",
		Short:          "Generate all certificates",
		InheritFlags:   getCertPhaseFlags("all"),
		RunAllSiblings: true,
	}

	subPhases = append(subPhases, allPhase)

	// 此循环假定 GetDefaultCertList() 返回的证书列表中，
	// 每张证书之前都会列出为其签名的 CA。
	var lastCACert *certsphase.KubeadmCert
	for _, cert := range certsphase.GetDefaultCertList() {
		var phase workflow.Phase
		if cert.CAName == "" {
			phase = newCertSubPhase(cert, runCAPhase(cert))
			lastCACert = cert
		} else {
			phase = newCertSubPhase(cert, runCertPhase(cert, lastCACert))
		}
		subPhases = append(subPhases, phase)
	}

	// SA 创建公私钥对，完全不使用 x509。
	saPhase := workflow.Phase{
		Name:         "sa",
		Short:        "Generate a private key for signing service account tokens along with its public key",
		Long:         saKeyLongDesc,
		Run:          runCertsSa,
		InheritFlags: getCertPhaseFlags("sa"),
	}

	subPhases = append(subPhases, saPhase)

	return subPhases
}

func newCertSubPhase(certSpec *certsphase.KubeadmCert, run func(c workflow.RunData) error) workflow.Phase {
	phase := workflow.Phase{
		Name:  certSpec.Name,
		Short: fmt.Sprintf("Generate the %s", certSpec.LongName),
		Long: fmt.Sprintf(
			genericLongDesc,
			certSpec.LongName,
			certSpec.BaseName,
			getSANDescription(certSpec),
		),
		Run:          run,
		InheritFlags: getCertPhaseFlags(certSpec.Name),
	}
	return phase
}

func getCertPhaseFlags(name string) []string {
	flags := []string{
		options.CertificatesDir,
		options.CfgPath,
		options.KubernetesVersion,
		options.DryRun,
	}
	if name == "all" || name == "apiserver" {
		flags = append(flags,
			options.APIServerAdvertiseAddress,
			options.ControlPlaneEndpoint,
			options.APIServerCertSANs,
			options.NetworkingDNSDomain,
			options.NetworkingServiceSubnet,
		)
	}
	return flags
}

func getSANDescription(certSpec *certsphase.KubeadmCert) string {
	// 用于获取 SAN 证书的应用默认值后的配置。
	defaultConfig := &kubeadmapiv1.InitConfiguration{
		LocalAPIEndpoint: kubeadmapiv1.APIEndpoint{
			// GetAPIServerAltNames 在没有 AdvertiseAddress 时会报错；此处地址同样可用。
			AdvertiseAddress: "127.0.0.1",
		},
	}

	defaultInternalConfig := &kubeadmapi.InitConfiguration{}

	kubeadmscheme.Scheme.Default(defaultConfig)
	if err := kubeadmscheme.Scheme.Convert(defaultConfig, defaultInternalConfig, nil); err != nil {
		return ""
	}

	certConfig, err := certSpec.GetConfig(defaultInternalConfig)
	if err != nil {
		return ""
	}

	if len(certConfig.AltNames.DNSNames) == 0 && len(certConfig.AltNames.IPs) == 0 {
		return ""
	}
	// 此操作会修改 certConfig，但构造命令后不会再使用它。
	sans := []string{}

	for _, dnsName := range certConfig.AltNames.DNSNames {
		if dnsName != "" {
			sans = append(sans, dnsName)
		}
	}

	for _, ip := range certConfig.AltNames.IPs {
		sans = append(sans, ip.String())
	}
	return fmt.Sprintf("\n\nDefault SANs are %s", strings.Join(sans, ", "))
}

func runCertsSa(c workflow.RunData) error {
	data, ok := c.(InitData)
	if !ok {
		return errors.New("certs phase invoked with an invalid data struct")
	}

	// 外部 CA 模式下跳过服务账号密钥生成。
	if data.ExternalCA() {
		fmt.Printf("[certs] Using existing sa keys\n")
		return nil
	}

	// 创建新的服务账号密钥（或使用已有密钥）。
	return certsphase.CreateServiceAccountKeyAndPublicKeyFiles(data.CertificateWriteDir(), data.Cfg().ClusterConfiguration.EncryptionAlgorithm)
}

func runCerts(c workflow.RunData) error {
	data, ok := c.(InitData)
	if !ok {
		return errors.New("certs phase invoked with an invalid data struct")
	}

	fmt.Printf("[certs] Using certificateDir folder %q\n", data.CertificateWriteDir())
	return nil
}

func runCAPhase(ca *certsphase.KubeadmCert) func(c workflow.RunData) error {
	return func(c workflow.RunData) error {
		data, ok := c.(InitData)
		if !ok {
			return errors.New("certs phase invoked with an invalid data struct")
		}

		// 使用外部 etcd 时跳过 etcd 证书颁发机构生成。
		if data.Cfg().Etcd.External != nil && ca.Name == "etcd-ca" {
			fmt.Printf("[certs] External etcd mode: Skipping %s certificate authority generation\n", ca.BaseName)
			return nil
		}

		if cert, err := pkiutil.TryLoadCertFromDisk(data.CertificateDir(), ca.BaseName); err == nil {
			certsphase.CheckCertificatePeriodValidity(ca.BaseName, cert)
			srcCertPath, srcKeyPath := pkiutil.PathsForCertAndKey(data.CertificateDir(), ca.BaseName)
			dryRunCertPath, dryRunKeyPath := pkiutil.PathsForCertAndKey(data.CertificateWriteDir(), ca.BaseName)

			// 若 dry-run 前已存在 CA 证书，将其复制到 dry-run 目录供后续使用。
			if data.DryRun() {
				if err := os.MkdirAll(filepath.Dir(dryRunCertPath), os.FileMode(0700)); err != nil {
					return errors.Wrapf(err, "failed to create directory %s", filepath.Dir(dryRunCertPath))
				}
				err := filesutil.CopyFile(srcCertPath, dryRunCertPath)
				if err != nil {
					return errors.Wrapf(err, "could not copy %s to dry run directory %s", fmt.Sprintf("%s.crt", ca.BaseName), data.CertificateWriteDir())
				}
			}
			if _, err := pkiutil.TryLoadKeyFromDisk(data.CertificateDir(), ca.BaseName); err == nil {
				// 若 dry-run 前已存在 CA 密钥，将其复制到 dry-run 目录供后续使用。
				if data.DryRun() {
					err := filesutil.CopyFile(srcKeyPath, dryRunKeyPath)
					if err != nil {
						return errors.Wrapf(err, "could not copy %s to dry run directory %s", fmt.Sprintf("%s.key", ca.BaseName), data.CertificateWriteDir())
					}
				}
				fmt.Printf("[certs] Using existing %s certificate authority\n", ca.BaseName)
				return nil
			}
			fmt.Printf("[certs] Using existing %s keyless certificate authority\n", ca.BaseName)
			return nil
		}

		// dry-run 时将证书颁发机构写入临时目录，
		// 并延迟恢复到用户最初指定的路径。
		cfg := data.Cfg()
		cfg.CertificatesDir = data.CertificateWriteDir()
		defer func() { cfg.CertificatesDir = data.CertificateDir() }()

		// 创建新的证书颁发机构（或使用已有机构）。
		return certsphase.CreateCACertAndKeyFiles(ca, cfg)
	}
}

func runCertPhase(cert *certsphase.KubeadmCert, caCert *certsphase.KubeadmCert) func(c workflow.RunData) error {
	return func(c workflow.RunData) error {
		data, ok := c.(InitData)
		if !ok {
			return errors.New("certs phase invoked with an invalid data struct")
		}

		// 使用外部 etcd 时跳过 etcd 证书生成。
		if data.Cfg().Etcd.External != nil && cert.CAName == "etcd-ca" {
			fmt.Printf("[certs] External etcd mode: Skipping %s certificate generation\n", cert.BaseName)
			return nil
		}

		if certData, intermediates, err := pkiutil.TryLoadCertChainFromDisk(data.CertificateDir(), cert.BaseName); err == nil {
			certsphase.CheckCertificatePeriodValidity(cert.BaseName, certData)

			caCertData, err := pkiutil.TryLoadCertFromDisk(data.CertificateDir(), caCert.BaseName)
			if err != nil {
				return errors.Wrapf(err, "couldn't load CA certificate %s", caCert.Name)
			}

			certsphase.CheckCertificatePeriodValidity(caCert.BaseName, caCertData)

			if err := pkiutil.VerifyCertChain(certData, intermediates, caCertData); err != nil {
				return errors.Wrapf(err, "[certs] certificate %s not signed by CA certificate %s", cert.BaseName, caCert.BaseName)
			}

			fmt.Printf("[certs] Using existing %s certificate and key on disk\n", cert.BaseName)
			return nil
		}

		// dry-run 时将证书写入临时目录，
		// 并延迟恢复到用户最初指定的路径。
		cfg := data.Cfg()
		cfg.CertificatesDir = data.CertificateWriteDir()
		defer func() { cfg.CertificatesDir = data.CertificateDir() }()

		// 创建新证书（或使用已有证书）。
		return certsphase.CreateCertAndKeyFilesWithCA(cert, caCert, cfg)
	}
}
