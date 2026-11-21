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
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	utiltesting "k8s.io/client-go/util/testing"
	"k8s.io/kubernetes/test/utils/ktesting"

	"github.com/google/go-cmp/cmp"
	certificatesv1 "k8s.io/api/certificates/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes/fake"
	certificatesclient "k8s.io/client-go/kubernetes/typed/certificates/v1beta1"
	restclient "k8s.io/client-go/rest"
	clienttesting "k8s.io/client-go/testing"
	"k8s.io/client-go/util/certificate"
	"k8s.io/client-go/util/keyutil"
)

// copyFile 把 src 文件的内容原样复制到 dst。
func copyFile(src, dst string) (err error) {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	// defer 保证无论复制是否成功，文件句柄都会被关闭。
	defer in.Close()
	out, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer func() {
		cerr := out.Close()
		// 只有当前没有其他错误时才用关闭错误覆盖 err，避免丢失复制阶段的真实失败原因。
		if err == nil {
			err = cerr
		}
	}()
	_, err = io.Copy(out, in)
	return err
}

// TestLoadClientConfig 验证 LoadClientConfig 在 bootstrapPath 为空/已设置，
// 以及 kubeconfig 合法/非法的组合场景下，都能得到预期的证书配置和客户端配置。
func TestLoadClientConfig(t *testing.T) {
	// 在临时目录下创建工作目录，存放测试所需的证书和配置文件，保证各次运行互不干扰。
	fileDir := t.TempDir()
	// 把测试证书复制到临时目录，使 kubeconfig 中的相对路径只在本次测试内有效。
	copyFile("./testdata/mycertinvalid.crt", fileDir+"/mycertinvalid.crt")
	copyFile("./testdata/mycertvalid.crt", fileDir+"/mycertvalid.crt")
	copyFile("./testdata/mycertinvalid.key", fileDir+"/mycertinvalid.key")
	copyFile("./testdata/mycertvalid.key", fileDir+"/mycertvalid.key")
	testDataValid := []byte(`
apiVersion: v1
kind: Config
clusters:
- cluster:
    certificate-authority: ca-a.crt
    server: https://cluster-a.com
  name: cluster-a
- cluster:
    server: https://cluster-b.com
  name: cluster-b
contexts:
- context:
    cluster: cluster-a
    namespace: ns-a
    user: user-a
  name: context-a
- context:
    cluster: cluster-b
    namespace: ns-b
    user: user-b
  name: context-b
current-context: context-b
users:
- name: user-a
  user:
    client-certificate: mycertvalid.crt
    client-key: mycertvalid.key
- name: user-b
  user:
    client-certificate: mycertvalid.crt
    client-key: mycertvalid.key

`)
	filevalid, err := os.CreateTemp(fileDir, "kubeconfigvalid")
	if err != nil {
		t.Fatal(err)
	}
	// os.CreateTemp 已经打开文件，未关闭就删除会失败，因此必须显式关闭。
	defer filevalid.Close()
	os.WriteFile(filevalid.Name(), testDataValid, os.FileMode(0755))

	testDataInvalid := []byte(`
apiVersion: v1
kind: Config
clusters:
- cluster:
    certificate-authority: ca-a.crt
    server: https://cluster-a.com
  name: cluster-a
- cluster:
    server: https://cluster-b.com
  name: cluster-b
contexts:
- context:
    cluster: cluster-a
    namespace: ns-a
    user: user-a
  name: context-a
- context:
    cluster: cluster-b
    namespace: ns-b
    user: user-b
  name: context-b
current-context: context-b
users:
- name: user-a
  user:
    client-certificate: mycertinvalid.crt
    client-key: mycertinvalid.key
- name: user-b
  user:
    client-certificate: mycertinvalid.crt
    client-key: mycertinvalid.key

`)
	fileinvalid, err := os.CreateTemp(fileDir, "kubeconfiginvalid")
	if err != nil {
		t.Fatal(err)
	}
	defer fileinvalid.Close()
	os.WriteFile(fileinvalid.Name(), testDataInvalid, os.FileMode(0755))

	testDatabootstrap := []byte(`
apiVersion: v1
kind: Config
clusters:
- cluster:
    certificate-authority: ca-a.crt
    server: https://cluster-a.com
  name: cluster-a
- cluster:
    server: https://cluster-b.com
  name: cluster-b
contexts:
- context:
    cluster: cluster-a
    namespace: ns-a
    user: user-a
  name: context-a
- context:
    cluster: cluster-b
    namespace: ns-b
    user: user-b
  name: context-b
current-context: context-b
users:
- name: user-a
  user:
   token: mytoken-b
- name: user-b
  user:
   token: mytoken-b
`)
	fileboot, err := os.CreateTemp(fileDir, "kubeconfig")
	if err != nil {
		t.Fatal(err)
	}
	defer fileboot.Close()
	os.WriteFile(fileboot.Name(), testDatabootstrap, os.FileMode(0755))

	dir, err := os.MkdirTemp(fileDir, "k8s-test-certstore-current")
	if err != nil {
		t.Fatalf("Unable to create the test directory %q: %v", dir, err)
	}

	store, err := certificate.NewFileStore("kubelet-client", dir, dir, "", "")
	if err != nil {
		t.Errorf("unable to build bootstrap cert store")
	}

	tests := []struct {
		name                 string
		kubeconfigPath       string
		bootstrapPath        string
		certDir              string
		expectedCertConfig   *restclient.Config
		expectedClientConfig *restclient.Config
	}{
		{
			name:           "bootstrapPath is empty",
			kubeconfigPath: filevalid.Name(),
			bootstrapPath:  "",
			certDir:        dir,
			expectedCertConfig: &restclient.Config{
				Host: "https://cluster-b.com",
				TLSClientConfig: restclient.TLSClientConfig{
					CertFile: filepath.Join(fileDir, "mycertvalid.crt"),
					KeyFile:  filepath.Join(fileDir, "mycertvalid.key"),
				},
				BearerToken: "",
			},
			expectedClientConfig: &restclient.Config{
				Host: "https://cluster-b.com",
				TLSClientConfig: restclient.TLSClientConfig{
					CertFile: filepath.Join(fileDir, "mycertvalid.crt"),
					KeyFile:  filepath.Join(fileDir, "mycertvalid.key"),
				},
				BearerToken: "",
			},
		},
		{
			name:           "bootstrap path is set and the contents of kubeconfigPath are valid",
			kubeconfigPath: filevalid.Name(),
			bootstrapPath:  fileboot.Name(),
			certDir:        dir,
			expectedCertConfig: &restclient.Config{
				Host: "https://cluster-b.com",
				TLSClientConfig: restclient.TLSClientConfig{
					CertFile: filepath.Join(fileDir, "mycertvalid.crt"),
					KeyFile:  filepath.Join(fileDir, "mycertvalid.key"),
				},
				BearerToken: "",
			},
			expectedClientConfig: &restclient.Config{
				Host: "https://cluster-b.com",
				TLSClientConfig: restclient.TLSClientConfig{
					CertFile: filepath.Join(fileDir, "mycertvalid.crt"),
					KeyFile:  filepath.Join(fileDir, "mycertvalid.key"),
				},
				BearerToken: "",
			},
		},
		{
			name:           "bootstrap path is set and the contents of kubeconfigPath are not valid",
			kubeconfigPath: fileinvalid.Name(),
			bootstrapPath:  fileboot.Name(),
			certDir:        dir,
			expectedCertConfig: &restclient.Config{
				Host:            "https://cluster-b.com",
				TLSClientConfig: restclient.TLSClientConfig{},
				BearerToken:     "mytoken-b",
			},
			expectedClientConfig: &restclient.Config{
				Host: "https://cluster-b.com",
				TLSClientConfig: restclient.TLSClientConfig{
					CertFile: store.CurrentPath(),
					KeyFile:  store.CurrentPath(),
				},
				BearerToken: "",
			},
		},
	}

	logger, _ := ktesting.NewTestContext(t)
	// 逐个执行用例，直接比较 LoadClientConfig 的双返回值与预期配置，任何不一致都判失败。
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			certConfig, clientConfig, err := LoadClientConfig(logger, test.kubeconfigPath, test.bootstrapPath, test.certDir)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(certConfig, test.expectedCertConfig) {
				t.Errorf("Unexpected certConfig: %s", cmp.Diff(certConfig, test.expectedCertConfig))
			}
			if !reflect.DeepEqual(clientConfig, test.expectedClientConfig) {
				t.Errorf("Unexpected clientConfig: %s", cmp.Diff(clientConfig, test.expectedClientConfig))
			}
		})
	}
}

// TestLoadRESTClientConfig 验证 loadRESTClientConfig 能正确解析 kubeconfig，
// 并正确还原当前上下文的 CA 数据与 BearerToken。
func TestLoadRESTClientConfig(t *testing.T) {
	testData := []byte(`
apiVersion: v1
kind: Config
clusters:
- cluster:
    certificate-authority: ca-a.crt
    server: https://cluster-a.com
  name: cluster-a
- cluster:
    certificate-authority-data: VGVzdA==
    server: https://cluster-b.com
  name: cluster-b
contexts:
- context:
    cluster: cluster-a
    namespace: ns-a
    user: user-a
  name: context-a
- context:
    cluster: cluster-b
    namespace: ns-b
    user: user-b
  name: context-b
current-context: context-b
users:
- name: user-a
  user:
    token: mytoken-a
- name: user-b
  user:
    token: mytoken-b
`)
	f, err := os.CreateTemp("", "kubeconfig")
	if err != nil {
		t.Fatal(err)
	}
	defer utiltesting.CloseAndRemove(t, f)
	f.Write(testData)

	config, err := loadRESTClientConfig(f.Name())
	if err != nil {
		t.Fatal(err)
	}

	expectedConfig := &restclient.Config{
		Host: "https://cluster-b.com",
		TLSClientConfig: restclient.TLSClientConfig{
			CAData: []byte(`Test`),
		},
		BearerToken: "mytoken-b",
	}

	if !reflect.DeepEqual(config, expectedConfig) {
		t.Errorf("Unexpected config: %s", cmp.Diff(config, expectedConfig))
	}
}

// TestRequestNodeCertificateNoKeyData 验证传入空的私钥数据时，
// requestNodeCertificate 会报错且不返回证书数据。
func TestRequestNodeCertificateNoKeyData(t *testing.T) {
	tCtx := ktesting.Init(t)
	certData, err := requestNodeCertificate(tCtx, newClientset(fakeClient{}), []byte{}, "fake-node-name")
	if err == nil {
		t.Errorf("Got no error, wanted error an error because there was an empty private key passed in.")
	}
	if certData != nil {
		t.Errorf("Got cert data, wanted nothing as there should have been an error.")
	}
}

// TestRequestNodeCertificateErrorCreatingCSR 验证创建 CSR 失败时，
// requestNodeCertificate 返回错误且不返回证书数据。
func TestRequestNodeCertificateErrorCreatingCSR(t *testing.T) {
	tCtx := ktesting.Init(t)
	client := newClientset(fakeClient{
		failureType: createError,
	})
	privateKeyData, err := keyutil.MakeEllipticPrivateKeyPEM()
	if err != nil {
		t.Fatalf("Unable to generate a new private key: %v", err)
	}

	certData, err := requestNodeCertificate(tCtx, client, privateKeyData, "fake-node-name")
	if err == nil {
		t.Errorf("Got no error, wanted error an error because client.Create failed.")
	}
	if certData != nil {
		t.Errorf("Got cert data, wanted nothing as there should have been an error.")
	}
}

// TestRequestNodeCertificate 验证私钥合法且 CSR 创建成功时，
// requestNodeCertificate 能正常返回 CSR 数据。
func TestRequestNodeCertificate(t *testing.T) {
	tCtx := ktesting.Init(t)
	privateKeyData, err := keyutil.MakeEllipticPrivateKeyPEM()
	if err != nil {
		t.Fatalf("Unable to generate a new private key: %v", err)
	}

	certData, err := requestNodeCertificate(tCtx, newClientset(fakeClient{}), privateKeyData, "fake-node-name")
	if err != nil {
		t.Errorf("Got %v, wanted no error.", err)
	}
	if certData == nil {
		t.Errorf("Got nothing, expected a CSR.")
	}
}

// failureType 枚举 fakeClient 要模拟的失败场景。
type failureType int

// failureType 的枚举值。
const (
	// noError 表示所有操作都成功。
	noError failureType = iota
	// createError 表示创建 CSR 时返回错误。
	createError
	// certificateSigningRequestDenied 表示 CSR 被拒绝。
	certificateSigningRequestDenied
)

// fakeClient 是 CSR 接口的测试替身，通过 failureType 控制模拟成功、创建失败或被拒绝等行为。
type fakeClient struct {
	certificatesclient.CertificateSigningRequestInterface
	failureType failureType
}

// newClientset 构造一个 fake Clientset，按 failureType 注入创建、列出和监听 CSR 的反应器。
func newClientset(opts fakeClient) *fake.Clientset {
	f := fake.NewSimpleClientset()
	switch opts.failureType {
	case createError:
		// 模拟创建 CSR 直接失败，用于覆盖错误处理分支。
		f.PrependReactor("create", "certificatesigningrequests", func(action clienttesting.Action) (handled bool, ret runtime.Object, err error) {
			switch action.GetResource().Version {
			case "v1":
				return true, nil, fmt.Errorf("create error")
			default:
				return true, nil, apierrors.NewNotFound(certificatesv1.Resource("certificatesigningrequests"), "")
			}
		})
	default:
		// 默认模拟正常流程：创建与列出 CSR 均成功，并监听到一个已审批的 CSR。
		f.PrependReactor("create", "certificatesigningrequests", func(action clienttesting.Action) (handled bool, ret runtime.Object, err error) {
			switch action.GetResource().Version {
			case "v1":
				return true, &certificatesv1.CertificateSigningRequest{ObjectMeta: metav1.ObjectMeta{Name: "fake-certificate-signing-request-name", UID: "fake-uid"}}, nil
			default:
				return true, nil, apierrors.NewNotFound(certificatesv1.Resource("certificatesigningrequests"), "")
			}
		})
		f.PrependReactor("list", "certificatesigningrequests", func(action clienttesting.Action) (handled bool, ret runtime.Object, err error) {
			switch action.GetResource().Version {
			case "v1":
				return true, &certificatesv1.CertificateSigningRequestList{Items: []certificatesv1.CertificateSigningRequest{{ObjectMeta: metav1.ObjectMeta{Name: "fake-certificate-signing-request-name", UID: "fake-uid"}}}}, nil
			default:
				return true, nil, apierrors.NewNotFound(certificatesv1.Resource("certificatesigningrequests"), "")
			}
		})
		f.PrependWatchReactor("certificatesigningrequests", func(action clienttesting.Action) (handled bool, ret watch.Interface, err error) {
			switch action.GetResource().Version {
			case "v1":
				w := watch.NewFakeWithChanSize(1, false)
				w.Add(opts.generateCSR())
				w.Stop()
				return true, w, nil

			default:
				return true, nil, apierrors.NewNotFound(certificatesv1.Resource("certificatesigningrequests"), "")
			}
		})
	}
	return f
}

// generateCSR 构造一个用于测试的 CertificateSigningRequest：
// 被拒绝的场景只带拒绝条件，正常场景则带审批条件和签发的证书数据。
func (c fakeClient) generateCSR() runtime.Object {
	var condition certificatesv1.CertificateSigningRequestCondition
	var certificateData []byte
	if c.failureType == certificateSigningRequestDenied {
		// 被拒绝的 CSR 不携带证书数据，模拟审批未通过的情况。
		condition = certificatesv1.CertificateSigningRequestCondition{
			Type: certificatesv1.CertificateDenied,
		}
	} else {
		// 只有已审批的 CSR 才会带上签发的证书数据，模拟 kubelet 成功拿到新证书。
		condition = certificatesv1.CertificateSigningRequestCondition{
			Type: certificatesv1.CertificateApproved,
		}
		certificateData = []byte(`issued certificate`)
	}

	csr := certificatesv1.CertificateSigningRequest{
		ObjectMeta: metav1.ObjectMeta{
			UID: "fake-uid",
		},
		Status: certificatesv1.CertificateSigningRequestStatus{
			Conditions: []certificatesv1.CertificateSigningRequestCondition{
				condition,
			},
			Certificate: certificateData,
		},
	}
	return &csr
}
