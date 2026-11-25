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

package etcd

import (
	"context"
	"net"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"

	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	apiextensionsclientset "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	utilerrors "k8s.io/apimachinery/pkg/util/errors"
	"k8s.io/apimachinery/pkg/util/json"
	"k8s.io/apimachinery/pkg/util/wait"
	genericapiserveroptions "k8s.io/apiserver/pkg/server/options"
	"k8s.io/apiserver/pkg/util/compatibility"
	"k8s.io/apiserver/pkg/util/feature"
	cacheddiscovery "k8s.io/client-go/discovery/cached/memory"
	"k8s.io/client-go/dynamic"
	clientset "k8s.io/client-go/kubernetes"
	restclient "k8s.io/client-go/rest"
	"k8s.io/client-go/restmapper"
	utiltesting "k8s.io/client-go/util/testing"
	basecompatibility "k8s.io/component-base/compatibility"
	"k8s.io/component-base/featuregate"
	featuregatetesting "k8s.io/component-base/featuregate/testing"
	"k8s.io/kubernetes/cmd/kube-apiserver/app"
	"k8s.io/kubernetes/cmd/kube-apiserver/app/options"
	"k8s.io/kubernetes/test/integration"
	"k8s.io/kubernetes/test/integration/framework"
	"k8s.io/kubernetes/test/utils/ktesting"
	netutils "k8s.io/utils/net"

	// 安装所有 API
	_ "k8s.io/kubernetes/pkg/controlplane"
)

// 此密钥仅用于测试，不被认为是安全的。
const ecdsaPrivateKey = `-----BEGIN EC PRIVATE KEY-----
MHcCAQEEIEZmTmUhuanLjPA2CLquXivuwBDHTt5XYwgIr/kA1LtRoAoGCCqGSM49
AwEHoUQDQgAEH6cuzP8XuD5wal6wf9M6xDljTOPLX2i8uIp/C/ASqiIGUeeKQtX0
/IR3qCXyThP/dbCiHrF3v1cuhBOHY8CLVg==
-----END EC PRIVATE KEY-----`

// StartRealAPIServerOrDie 启动一个适用于需要每类资源各有一个的测试的 API server
func StartRealAPIServerOrDie(t *testing.T, configFuncs ...func(*options.ServerRunOptions)) *APIServer {
	tCtx := ktesting.Init(t)

	// 去掉子测试名称中的 "/"
	certDir, err := os.MkdirTemp("", strings.ReplaceAll(t.Name(), "/", ""))
	if err != nil {
		t.Fatal(err)
	}

	_, defaultServiceClusterIPRange, err := netutils.ParseCIDRSloppy("10.0.0.0/24")
	if err != nil {
		t.Fatal(err)
	}

	listener, _, err := genericapiserveroptions.CreateListener("tcp", "127.0.0.1:0", net.ListenConfig{})
	if err != nil {
		t.Fatal(err)
	}

	saSigningKeyFile, err := os.CreateTemp("/tmp", "insecure_test_key")
	if err != nil {
		t.Fatalf("create temp file failed: %v", err)
	}
	defer utiltesting.CloseAndRemove(t, saSigningKeyFile)
	if err = os.WriteFile(saSigningKeyFile.Name(), []byte(ecdsaPrivateKey), 0666); err != nil {
		t.Fatalf("write file %s failed: %v", saSigningKeyFile.Name(), err)
	}

	opts := options.NewServerRunOptions()
	// 如果测试中设置了 DefaultFeatureGate 的 EmulationVersion，需要将其传播到 apiserver 的 ComponentGlobalsRegistry。
	featureGate := feature.DefaultMutableFeatureGate.DeepCopy()
	effectiveVersion := compatibility.DefaultKubeEffectiveVersionForTest()
	effectiveVersion.SetEmulationVersion(featureGate.EmulationVersion())
	// 创建新的 ComponentGlobalsRegistry 实例，而不是使用 DefaultComponentGlobalsRegistry，以避免并行测试中的竞争。
	componentGlobalsRegistry := basecompatibility.NewComponentGlobalsRegistry()
	if err := componentGlobalsRegistry.Register(basecompatibility.DefaultKubeComponent, effectiveVersion, featureGate); err != nil {
		t.Fatal(err)
	}
	opts.GenericServerRunOptions.ComponentGlobalsRegistry = componentGlobalsRegistry

	opts.Options.SecureServing.Listener = listener
	opts.Options.SecureServing.ServerCert.CertDirectory = certDir
	opts.Options.ServiceAccountSigningKeyFile = saSigningKeyFile.Name()
	opts.Options.Etcd.StorageConfig.Transport.ServerList = []string{framework.GetEtcdURL()}
	opts.Options.Etcd.DefaultStorageMediaType = runtime.ContentTypeJSON // 强制使用 json，方便直接解读 etcd 中的结果
	opts.ServiceClusterIPRanges = defaultServiceClusterIPRange.String()
	opts.Options.Authentication.APIAudiences = []string{"https://foo.bar.example.com"}
	opts.Options.Authentication.ServiceAccounts.Issuers = []string{"https://foo.bar.example.com"}
	opts.Options.Authentication.ServiceAccounts.KeyFiles = []string{saSigningKeyFile.Name()}
	opts.Options.Authorization.Modes = []string{"RBAC"}
	opts.Options.Admission.GenericAdmission.DisablePlugins = []string{"ServiceAccount"}
	opts.Options.APIEnablement.RuntimeConfig["api/all"] = "true"
	for _, f := range configFuncs {
		f(opts)
	}

	// 如果 configFuncs 修改了本地的 ComponentGlobalsRegistry，
	// 需要将新的特性值拷回 DefaultFeatureGate，因为大多数特性检查仍使用 DefaultFeatureGate。
	if !featureGate.EmulationVersion().EqualTo(feature.DefaultMutableFeatureGate.EmulationVersion()) || !featureGate.MinCompatibilityVersion().EqualTo(feature.DefaultMutableFeatureGate.MinCompatibilityVersion()) {
		featuregatetesting.SetFeatureGateVersionsDuringTest(t, feature.DefaultMutableFeatureGate, effectiveVersion.EmulationVersion(), effectiveVersion.MinCompatibilityVersion())
	}
	featureOverrides := map[featuregate.Feature]bool{}
	for f := range feature.DefaultMutableFeatureGate.GetAll() {
		if featureGate.Enabled(f) != feature.DefaultFeatureGate.Enabled(f) {
			featureOverrides[f] = featureGate.Enabled(f)
		}
	}
	if len(featureOverrides) > 0 {
		featuregatetesting.SetFeatureGatesDuringTest(t, feature.DefaultFeatureGate, featureOverrides)
	}
	feature.DefaultMutableFeatureGate.AddMetrics()

	completedOptions, err := opts.Complete(tCtx)
	if err != nil {
		t.Fatal(err)
	}

	if errs := completedOptions.Validate(); len(errs) != 0 {
		t.Fatalf("failed to validate ServerRunOptions: %v", utilerrors.NewAggregate(errs))
	}

	// 在启动 API server 之前获取 etcd client
	rawClient, kvClient, err := integration.GetEtcdClients(completedOptions.Etcd.StorageConfig.Transport)
	if err != nil {
		t.Fatal(err)
	}

	// 确保从干净的状态开始
	if _, err := kvClient.Delete(context.Background(), "/registry/", clientv3.WithPrefix()); err != nil {
		t.Fatal(err)
	}

	config, err := app.NewConfig(completedOptions)
	if err != nil {
		t.Fatal(err)
	}
	completed, err := config.Complete()
	if err != nil {
		t.Fatal(err)
	}
	kubeAPIServer, err := app.CreateServerChain(completed)
	if err != nil {
		t.Fatal(err)
	}

	kubeClientConfig := restclient.CopyConfig(kubeAPIServer.GenericAPIServer.LoopbackClientConfig)

	// 我们会发出大量请求，别让它慢下来
	kubeClientConfig.QPS = 99999
	kubeClientConfig.Burst = 9999

	// 我们会请求所有资源，不要记录关于已废弃资源的警告
	restclient.SetDefaultWarningHandler(restclient.NoWarnings{})

	kubeClient := clientset.NewForConfigOrDie(kubeClientConfig)

	errCh := make(chan error)
	go func() {
		// 捕获此 goroutine 中发生的 panic，以便得到可理解的失败信息
		defer func() {
			if err := recover(); err != nil {
				t.Errorf("Unexpected panic trying to start API server: %#v", err)
			}
		}()
		defer close(errCh)

		prepared, err := kubeAPIServer.PrepareRun()
		if err != nil {
			errCh <- err
			return
		}
		if err := prepared.Run(tCtx); err != nil {
			errCh <- err
			t.Error(err)
			return
		}
	}()

	lastHealth := ""
	attempt := 0
	if err := wait.PollImmediate(time.Second, time.Minute, func() (done bool, err error) {
		select {
		case err := <-errCh:
			return false, err
		default:
		}

		// 等待 server 健康
		result := kubeClient.RESTClient().Get().AbsPath("/healthz").Do(context.TODO())
		content, _ := result.Raw()
		lastHealth = string(content)
		if errResult := result.Error(); errResult != nil {
			attempt++
			if attempt < 10 {
				t.Log("waiting for server to be healthy")
			} else {
				t.Log(errResult)
			}
			return false, nil
		}
		var status int
		result.StatusCode(&status)
		return status == http.StatusOK, nil
	}); err != nil {
		t.Log(lastHealth)
		t.Fatal(err)
	}

	// 创建 CRD，以确保自定义资源不会丢失
	CreateTestCRDs(t, apiextensionsclientset.NewForConfigOrDie(kubeClientConfig), false, GetCustomResourceDefinitionData()...)

	// 强制重置缓存的 discovery
	discoveryClient := cacheddiscovery.NewMemCacheClient(kubeClient.Discovery())
	restMapper := restmapper.NewDeferredDiscoveryRESTMapper(discoveryClient)
	restMapper.Reset()

	_, serverResources, err := kubeClient.Discovery().ServerGroupsAndResources()
	if err != nil {
		t.Fatal(err)
	}

	cleanup := func() {
		// 取消 apiserver 以停止它并自行清理，
		// 包括关闭其存储层。
		tCtx.Cancel("cleaning up")

		// 如果 apiserver 已启动，等待它
		// 完全关闭。
		err, ok := <-errCh
		if ok && err != nil {
			t.Error(err)
		}
		rawClient.Close()
		if err := os.RemoveAll(certDir); err != nil {
			t.Log(err)
		}
	}

	return &APIServer{
		Client:    kubeClient,
		Dynamic:   dynamic.NewForConfigOrDie(kubeClientConfig),
		Config:    kubeClientConfig,
		KV:        kvClient,
		Mapper:    restMapper,
		Resources: GetResources(t, serverResources),
		Cleanup:   cleanup,
	}
}

// APIServer 表示一个已运行且可供使用的 API server
// 必须 defer Cleanup 函数，以防止资源泄漏
type APIServer struct {
	Client    clientset.Interface
	Dynamic   dynamic.Interface
	Config    *restclient.Config
	KV        clientv3.KV
	Mapper    meta.RESTMapper
	Resources []Resource
	Cleanup   func()
}

// Resource 包含特定资源的 REST 映射信息，以及诸如是否支持 delete collection 等额外元数据
type Resource struct {
	Mapping             *meta.RESTMapping
	HasDeleteCollection bool
}

// GetResources 获取 serverResources 中支持 get 和 create 的对应 Resource
func GetResources(t *testing.T, serverResources []*metav1.APIResourceList) []Resource {
	var resources []Resource

	for _, discoveryGroup := range serverResources {
		for _, discoveryResource := range discoveryGroup.APIResources {
			// 这是一个子资源，跳过
			if strings.Contains(discoveryResource.Name, "/") {
				continue
			}
			hasCreate := false
			hasGet := false
			hasDeleteCollection := false
			for _, verb := range discoveryResource.Verbs {
				if verb == "get" {
					hasGet = true
				}
				if verb == "create" {
					hasCreate = true
				}
				if verb == "deletecollection" {
					hasDeleteCollection = true
				}
			}
			if !(hasCreate && hasGet) {
				continue
			}

			resourceGV, err := schema.ParseGroupVersion(discoveryGroup.GroupVersion)
			if err != nil {
				t.Fatal(err)
			}
			gvk := resourceGV.WithKind(discoveryResource.Kind)
			if len(discoveryResource.Group) > 0 || len(discoveryResource.Version) > 0 {
				gvk = schema.GroupVersionKind{
					Group:   discoveryResource.Group,
					Version: discoveryResource.Version,
					Kind:    discoveryResource.Kind,
				}
			}
			gvr := resourceGV.WithResource(discoveryResource.Name)

			resources = append(resources, Resource{
				Mapping: &meta.RESTMapping{
					Resource:         gvr,
					GroupVersionKind: gvk,
					Scope:            scope(discoveryResource.Namespaced),
				},
				HasDeleteCollection: hasDeleteCollection,
			})
		}
	}

	return resources
}

func scope(namespaced bool) meta.RESTScope {
	if namespaced {
		return meta.RESTScopeNamespace
	}
	return meta.RESTScopeRoot
}

// JSONToUnstructured 将 JSON stub 转换为 unstructured.Unstructured，
// 并返回可用于与其交互的 dynamic resource client
func JSONToUnstructured(stub, namespace string, mapping *meta.RESTMapping, dynamicClient dynamic.Interface) (dynamic.ResourceInterface, *unstructured.Unstructured, error) {
	typeMetaAdder := map[string]interface{}{}
	if err := json.Unmarshal([]byte(stub), &typeMetaAdder); err != nil {
		return nil, nil, err
	}

	// 我们不要求提供的数据中带有 GVK，因此在此补全。也可以提供，但那显得多余。
	typeMetaAdder["apiVersion"] = mapping.GroupVersionKind.GroupVersion().String()
	typeMetaAdder["kind"] = mapping.GroupVersionKind.Kind

	if mapping.Scope == meta.RESTScopeRoot {
		namespace = ""
	}

	return dynamicClient.Resource(mapping.Resource).Namespace(namespace), &unstructured.Unstructured{Object: typeMetaAdder}, nil
}

// CreateTestCRDs 创建给定的 CRD，任何失败都会导致测试 Fatal。
// 如果 skipCrdExistsInDiscovery 为 true，则仅通过 CRD 的 Status 检查 Established 条件。
// 如果 skipCrdExistsInDiscovery 为 false，则通过 discovery 检查 CRD，参见 CrdExistsInDiscovery。
func CreateTestCRDs(t *testing.T, client apiextensionsclientset.Interface, skipCrdExistsInDiscovery bool, crds ...*apiextensionsv1.CustomResourceDefinition) {
	for _, crd := range crds {
		createTestCRD(t, client, skipCrdExistsInDiscovery, crd)
	}
}

func createTestCRD(t *testing.T, client apiextensionsclientset.Interface, skipCrdExistsInDiscovery bool, crd *apiextensionsv1.CustomResourceDefinition) {
	if _, err := client.ApiextensionsV1().CustomResourceDefinitions().Create(context.TODO(), crd, metav1.CreateOptions{}); err != nil {
		t.Fatalf("Failed to create %s CRD; %v", crd.Name, err)
	}
	if skipCrdExistsInDiscovery {
		if err := waitForEstablishedCRD(client, crd.Name); err != nil {
			t.Fatalf("Failed to establish %s CRD; %v", crd.Name, err)
		}
		return
	}
	if err := wait.PollImmediate(500*time.Millisecond, wait.ForeverTestTimeout, func() (bool, error) {
		return CrdExistsInDiscovery(client, crd), nil
	}); err != nil {
		t.Fatalf("Failed to see %s in discovery: %v", crd.Name, err)
	}
}

func waitForEstablishedCRD(client apiextensionsclientset.Interface, name string) error {
	return wait.PollImmediate(500*time.Millisecond, wait.ForeverTestTimeout, func() (bool, error) {
		crd, err := client.ApiextensionsV1().CustomResourceDefinitions().Get(context.TODO(), name, metav1.GetOptions{})
		if err != nil {
			return false, err
		}
		for _, cond := range crd.Status.Conditions {
			switch cond.Type {
			case apiextensionsv1.Established:
				if cond.Status == apiextensionsv1.ConditionTrue {
					return true, nil
				}
			}
		}
		return false, nil
	})
}

// CrdExistsInDiscovery 检查给定 CRD 在所有 served 版本的 discovery 中是否存在。
func CrdExistsInDiscovery(client apiextensionsclientset.Interface, crd *apiextensionsv1.CustomResourceDefinition) bool {
	var versions []string
	for _, v := range crd.Spec.Versions {
		if v.Served {
			versions = append(versions, v.Name)
		}
	}
	for _, v := range versions {
		if !crdVersionExistsInDiscovery(client, crd, v) {
			return false
		}
	}
	return true
}

func crdVersionExistsInDiscovery(client apiextensionsclientset.Interface, crd *apiextensionsv1.CustomResourceDefinition, version string) bool {
	resourceList, err := client.Discovery().ServerResourcesForGroupVersion(crd.Spec.Group + "/" + version)
	if err != nil {
		return false
	}
	for _, resource := range resourceList.APIResources {
		if resource.Name == crd.Spec.Names.Plural {
			return true
		}
	}
	return false
}
