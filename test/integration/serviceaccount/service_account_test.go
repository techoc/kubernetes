/*
Copyright 2014 The Kubernetes Authors.

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

package serviceaccount

// 本文件测试发往 API server 对象的 HTTP 请求的认证以及（即将支持的）授权。
// 这里不使用 pkg/client/... 中的客户端，因为认证和授权
// 必须对 HTTP 接口的任意客户端都有效。

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	serviceaccountapiserver "k8s.io/apiserver/pkg/authentication/serviceaccount"
	"k8s.io/apiserver/pkg/authorization/authorizer"
	unionauthz "k8s.io/apiserver/pkg/authorization/union"
	clientinformers "k8s.io/client-go/informers"
	clientset "k8s.io/client-go/kubernetes"
	restclient "k8s.io/client-go/rest"
	"k8s.io/client-go/util/keyutil"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/cmd/kube-apiserver/app/options"
	"k8s.io/kubernetes/pkg/controller"
	serviceaccountcontroller "k8s.io/kubernetes/pkg/controller/serviceaccount"
	"k8s.io/kubernetes/pkg/controlplane"
	"k8s.io/kubernetes/pkg/controlplane/controller/legacytokentracking"
	"k8s.io/kubernetes/pkg/serviceaccount"
	serviceaccountadmission "k8s.io/kubernetes/plugin/pkg/admission/serviceaccount"
	"k8s.io/kubernetes/test/integration/framework"
	"k8s.io/kubernetes/test/utils/ktesting"
)

const (
	readOnlyServiceAccountName  = "ro"
	readWriteServiceAccountName = "rw"
)

func TestServiceAccountAutoCreate(t *testing.T) {
	tCtx := ktesting.Init(t)
	c, _, stopFunc, _, err := startServiceAccountTestServerAndWaitForCaches(tCtx, t)
	defer stopFunc()
	if err != nil {
		t.Fatalf("failed to setup ServiceAccounts server: %v", err)
	}

	ns := "test-service-account-creation"

	// 创建命名空间
	_, err = c.CoreV1().Namespaces().Create(tCtx, &v1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: ns}}, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("could not create namespace: %v", err)
	}

	// 获取 service account
	defaultUser, err := getServiceAccount(c, ns, "default", true)
	if err != nil {
		t.Fatalf("Default serviceaccount not created: %v", err)
	}

	// 删除 service account
	err = c.CoreV1().ServiceAccounts(ns).Delete(tCtx, defaultUser.Name, metav1.DeleteOptions{})
	if err != nil {
		t.Fatalf("Could not delete default serviceaccount: %v", err)
	}

	// 获取重新创建的 service account
	defaultUser2, err := getServiceAccount(c, ns, "default", true)
	if err != nil {
		t.Fatalf("Default serviceaccount not created: %v", err)
	}
	if defaultUser2.UID == defaultUser.UID {
		t.Fatalf("Expected different UID with recreated serviceaccount")
	}
}

func TestServiceAccountTokenAutoMount(t *testing.T) {
	tCtx := ktesting.Init(t)
	c, _, stopFunc, _, err := startServiceAccountTestServerAndWaitForCaches(tCtx, t)
	defer stopFunc()
	if err != nil {
		t.Fatalf("failed to setup ServiceAccounts server: %v", err)
	}

	ns := "auto-mount-ns"

	// 创建 "my" 命名空间
	_, err = c.CoreV1().Namespaces().Create(tCtx, &v1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: ns}}, metav1.CreateOptions{})
	if err != nil && !apierrors.IsAlreadyExists(err) {
		t.Fatalf("could not create namespace: %v", err)
	}

	// 要创建的 Pod
	protoPod := v1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "protopod"},
		Spec: v1.PodSpec{
			Containers: []v1.Container{
				{
					Name:  "container",
					Image: "container-image",
				},
			},
		},
	}

	createdPod, err := c.CoreV1().Pods(ns).Create(tCtx, &protoPod, metav1.CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}

	expectedServiceAccount := serviceaccountadmission.DefaultServiceAccountName
	if createdPod.Spec.ServiceAccountName != expectedServiceAccount {
		t.Fatalf("Expected %s, got %s", expectedServiceAccount, createdPod.Spec.ServiceAccountName)
	}
	if len(createdPod.Spec.Volumes) == 0 || createdPod.Spec.Volumes[0].Projected == nil {
		t.Fatal("Expected projected volume for service account token inserted")
	}
}

func TestServiceAccountTokenAuthentication(t *testing.T) {
	tCtx := ktesting.Init(t)
	c, config, stopFunc, _, err := startServiceAccountTestServerAndWaitForCaches(tCtx, t)
	defer stopFunc()
	if err != nil {
		t.Fatalf("failed to setup ServiceAccounts server: %v", err)
	}

	myns := "auth-ns"
	otherns := "other-ns"

	// 创建 "my" 命名空间
	_, err = c.CoreV1().Namespaces().Create(tCtx, &v1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: myns}}, metav1.CreateOptions{})
	if err != nil && !apierrors.IsAlreadyExists(err) {
		t.Fatalf("could not create namespace: %v", err)
	}

	// 创建 "other" 命名空间
	_, err = c.CoreV1().Namespaces().Create(tCtx, &v1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: otherns}}, metav1.CreateOptions{})
	if err != nil && !apierrors.IsAlreadyExists(err) {
		t.Fatalf("could not create namespace: %v", err)
	}

	// 在 myns 中创建 "ro" 用户
	roSA, err := c.CoreV1().ServiceAccounts(myns).Create(tCtx, &v1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Name: readOnlyServiceAccountName}}, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("Service Account not created: %v", err)
	}

	roTokenName := "ro-test-token"
	secret, err := createServiceAccountToken(c, roSA, myns, roTokenName)
	if err != nil {
		t.Fatalf("Secret not created: %v", err)
	}
	roClientConfig := *config
	roClientConfig.BearerToken = string(secret.Data[v1.ServiceAccountTokenKey])
	roClient := clientset.NewForConfigOrDie(&roClientConfig)
	doServiceAccountAPIRequests(t, roClient, myns, true, true, false)
	doServiceAccountAPIRequests(t, roClient, otherns, true, false, false)
	err = c.CoreV1().Secrets(myns).Delete(tCtx, roTokenName, metav1.DeleteOptions{})
	if err != nil {
		t.Fatalf("could not delete token: %v", err)
	}
	// 等待通过 watch 观察到删除操作并作出响应
	err = wait.PollImmediate(100*time.Millisecond, 30*time.Second, func() (bool, error) {
		_, err := roClient.CoreV1().Secrets(myns).List(tCtx, metav1.ListOptions{})
		if err == nil {
			t.Logf("token is still valid, waiting")
			return false, nil
		}
		if !apierrors.IsUnauthorized(err) {
			t.Logf("expected unauthorized error, got %v", err)
			return false, nil
		}
		return true, nil
	})
	if err != nil {
		t.Fatalf("waiting for token to be invalidated: %v", err)
	}
	doServiceAccountAPIRequests(t, roClient, myns, false, false, false)

	// 在 myns 中创建 "rw" 用户
	rwSA, err := c.CoreV1().ServiceAccounts(myns).Create(tCtx, &v1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Name: readWriteServiceAccountName}}, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("Service Account not created: %v", err)
	}
	rwTokenName := "rw-test-token"
	secret, err = createServiceAccountToken(c, rwSA, myns, rwTokenName)
	if err != nil {
		t.Fatalf("Secret not created: %v", err)
	}
	rwClientConfig := *config
	rwClientConfig.BearerToken = string(secret.Data[v1.ServiceAccountTokenKey])
	rwClient := clientset.NewForConfigOrDie(&rwClientConfig)
	doServiceAccountAPIRequests(t, rwClient, myns, true, true, true)
	doServiceAccountAPIRequests(t, rwClient, otherns, true, false, false)
}

func TestLegacyServiceAccountTokenTracking(t *testing.T) {
	tCtx := ktesting.Init(t)
	c, config, stopFunc, _, err := startServiceAccountTestServerAndWaitForCaches(tCtx, t)
	defer stopFunc()
	if err != nil {
		t.Fatalf("failed to setup ServiceAccounts server: %v", err)
	}

	// 创建 service account
	myns := "auth-ns"
	_, err = c.CoreV1().Namespaces().Create(tCtx, &v1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: myns}}, metav1.CreateOptions{})
	if err != nil && !apierrors.IsAlreadyExists(err) {
		t.Fatalf("could not create namespace: %v", err)
	}
	mysa, err := c.CoreV1().ServiceAccounts(myns).Create(tCtx, &v1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Name: readOnlyServiceAccountName}}, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("Service Account not created: %v", err)
	}
	manualSecretName := "manual-token"
	manualSecret, err := createServiceAccountToken(c, mysa, myns, manualSecretName)
	if err != nil {
		t.Fatalf("Secret not created: %v", err)
	}

	// 手动伪造一个自动创建的 token
	autoSecretName := "auto-token"
	autoSecret, err := createServiceAccountToken(c, mysa, myns, autoSecretName)
	if err != nil {
		t.Fatalf("Secret not created: %v", err)
	}
	if err := addReferencedServiceAccountToken(c, myns, readOnlyServiceAccountName, autoSecret); err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name            string
		secretName      string
		secretTokenData string

		expectWarning bool
	}{
		{
			name:            "manually created legacy token",
			secretName:      manualSecretName,
			secretTokenData: string(manualSecret.Data[v1.ServiceAccountTokenKey]),
		},
		{
			name:            "auto created legacy token",
			secretName:      autoSecretName,
			secretTokenData: string(autoSecret.Data[v1.ServiceAccountTokenKey]),
			expectWarning:   true,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			myConfig := *config
			wh := &warningHandler{}
			myConfig.WarningHandler = wh
			myConfig.BearerToken = string(test.secretTokenData)
			roClient := clientset.NewForConfigOrDie(&myConfig)
			dateBefore := time.Now().UTC().Format(dateFormat)

			var wg sync.WaitGroup
			concurrency := 5
			for range concurrency {
				wg.Add(1)
				go func() {
					doServiceAccountAPIRequests(t, roClient, myns, true, true, false)
					wg.Done()
				}()
			}
			wg.Wait()
			dateAfter := time.Now().UTC().Format(dateFormat)
			liveSecret, err := c.CoreV1().Secrets(myns).Get(tCtx, test.secretName, metav1.GetOptions{})
			if err != nil {
				t.Fatalf("Could not get secret: %v", err)
			}

			// doServiceAccountAPIRequests 会发起 4 个 API 请求
			if test.expectWarning && len(wh.warnings) != 4*concurrency {
				t.Fatalf("Expect %d warnings, got %d", 4*concurrency, len(wh.warnings))
			}
			if !test.expectWarning && len(wh.warnings) != 0 {
				t.Fatalf("Don't expect warnings, got %d", len(wh.warnings))
			}

			// 通过认证的旧版 token 应具有预期的 annotation 和 label。
			date, ok := liveSecret.GetLabels()[serviceaccount.LastUsedLabelKey]
			if !ok {
				t.Fatalf("Secret wasn't labeled with %q", serviceaccount.LastUsedLabelKey)
			}
			if date != dateBefore || date != dateAfter {
				t.Fatalf("Secret was labeled with wrong date: %q", date)
			}
		})
	}

	// configmap 应带有 'since' 时间戳而存在。
	if err = wait.PollImmediate(time.Millisecond*10, wait.ForeverTestTimeout, func() (bool, error) {
		dateBefore := time.Now().UTC().Format("2006-01-02")
		configMap, err := c.CoreV1().ConfigMaps(metav1.NamespaceSystem).Get(tCtx, legacytokentracking.ConfigMapName, metav1.GetOptions{})
		if err != nil {
			return false, fmt.Errorf("failed to get %q configmap, err %w", legacytokentracking.ConfigMapDataKey, err)
		}
		dateAfter := time.Now().UTC().Format("2006-01-02")
		date, ok := configMap.Data[legacytokentracking.ConfigMapDataKey]
		if !ok {
			return false, fmt.Errorf("configMap doesn't contain key %q", legacytokentracking.ConfigMapDataKey)
		}
		if date != dateBefore || date != dateAfter {
			return false, fmt.Errorf("configMap contains a wrong date %q", date)
		}
		return true, nil
	}); err != nil {
		t.Fatal(err)
	}
}

// startServiceAccountTestServerAndWaitForCaches 返回一个已启动的 server。
// 调用方有责任确保返回的 stopFunc 被调用。
func startServiceAccountTestServerAndWaitForCaches(ctx context.Context, t *testing.T) (clientset.Interface, *restclient.Config, func(), clientinformers.SharedInformerFactory, error) {
	var serviceAccountKey interface{}

	ctx, cancel := context.WithCancel(ctx)

	// 建立一个 API server
	rootClientset, clientConfig, tearDownFn := framework.StartTestServer(ctx, t, framework.TestServerSetup{
		ModifyServerRunOptions: func(opts *options.ServerRunOptions) {
			var err error
			serviceAccountKey, err = keyutil.PrivateKeyFromFile(opts.ServiceAccountSigningKeyFile)
			if err != nil {
				t.Fatal(err)
			}
		},
		ModifyServerConfig: func(config *controlplane.Config) {
			// 建立一个桩 authorizer：
			// 1. "root" 用户可以做任何事
			// 2. 名为 "ro" 的 ServiceAccount 可以在其命名空间内执行只读操作
			// 3. 名为 "rw" 的 ServiceAccount 可以在其命名空间内执行任何操作
			authorizer := authorizer.AuthorizerFunc(func(ctx context.Context, attrs authorizer.Attributes) (authorizer.Decision, string, error) {
				username := ""
				if user := attrs.GetUser(); user != nil {
					username = user.GetName()
				}
				ns := attrs.GetNamespace()

				// 如果用户是 service account……
				if serviceAccountNamespace, serviceAccountName, err := serviceaccountapiserver.SplitUsername(username); err == nil {
					// 将它们限制在自己的命名空间内
					if serviceAccountNamespace == ns {
						switch serviceAccountName {
						case readOnlyServiceAccountName:
							if attrs.IsReadOnly() {
								return authorizer.DecisionAllow, "", nil
							}
						case readWriteServiceAccountName:
							return authorizer.DecisionAllow, "", nil
						}
					}
				}

				return authorizer.DecisionNoOpinion, fmt.Sprintf("User %s is denied (ns=%s, readonly=%v, resource=%s)", username, ns, attrs.IsReadOnly(), attrs.GetResource()), nil
			})
			authz, err := unionauthz.New(
				unionauthz.NamedAuthorizer{AuthorizerName: "default", Authorizer: config.ControlPlane.Generic.Authorization.Authorizer},
				unionauthz.NamedAuthorizer{AuthorizerName: "service-account-test", Authorizer: authorizer},
			)
			if err != nil {
				t.Fatalf("unionauthz.New: %v", err)
			}
			config.ControlPlane.Generic.Authorization.Authorizer = authz
		},
	})

	stop := func() {
		cancel()
		tearDownFn()
	}

	informers := clientinformers.NewSharedInformerFactory(rootClientset, controller.NoResyncPeriodFunc())

	// 启动 service account 和 service account token 控制器
	tokenGenerator, err := serviceaccount.JWTTokenGenerator(serviceaccount.LegacyIssuer, serviceAccountKey)
	if err != nil {
		return rootClientset, clientConfig, stop, informers, err
	}
	tokenController, err := serviceaccountcontroller.NewTokensController(
		klog.FromContext(ctx),
		informers.Core().V1().ServiceAccounts(),
		informers.Core().V1().Secrets(),
		rootClientset,
		serviceaccountcontroller.TokensControllerOptions{
			TokenGenerator: tokenGenerator,
		},
	)
	if err != nil {
		return rootClientset, clientConfig, stop, informers, err
	}
	go tokenController.Run(ctx, 1)
	logger := klog.FromContext(ctx)
	serviceAccountController, err := serviceaccountcontroller.NewServiceAccountsController(
		logger,
		informers.Core().V1().ServiceAccounts(),
		informers.Core().V1().Namespaces(),
		rootClientset,
		serviceaccountcontroller.DefaultServiceAccountsControllerOptions(),
	)
	if err != nil {
		return rootClientset, clientConfig, stop, informers, err
	}
	informers.Start(ctx.Done())
	go serviceAccountController.Run(ctx, 5)

	// 由于该方法在单独的 goroutine 中启动控制器，
	// 而测试不会检查 /readyz，因此测试无法判断
	// 何时可以安全地调用 server 且请求不会被拒绝，
	// 所以我们等待缓存完成同步。
	informers.WaitForCacheSync(ctx.Done())

	return rootClientset, clientConfig, stop, informers, nil
}

func getServiceAccount(c clientset.Interface, ns string, name string, shouldWait bool) (*v1.ServiceAccount, error) {
	if !shouldWait {
		return c.CoreV1().ServiceAccounts(ns).Get(context.TODO(), name, metav1.GetOptions{})
	}

	var user *v1.ServiceAccount
	var err error
	err = wait.Poll(time.Second, 10*time.Second, func() (bool, error) {
		user, err = c.CoreV1().ServiceAccounts(ns).Get(context.TODO(), name, metav1.GetOptions{})
		if apierrors.IsNotFound(err) {
			return false, nil
		}
		if err != nil {
			return false, err
		}
		return true, nil
	})
	return user, err
}

func createServiceAccountToken(c clientset.Interface, sa *v1.ServiceAccount, ns string, name string) (*v1.Secret, error) {
	secret := &v1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: ns,
			Annotations: map[string]string{
				v1.ServiceAccountNameKey: sa.GetName(),
				v1.ServiceAccountUIDKey:  string(sa.UID),
			},
		},
		Type: v1.SecretTypeServiceAccountToken,
		Data: map[string][]byte{},
	}
	secret, err := c.CoreV1().Secrets(ns).Create(context.TODO(), secret, metav1.CreateOptions{})
	if err != nil {
		return nil, fmt.Errorf("failed to create secret (%s:%s) %+v, err: %v", ns, secret.Name, *secret, err)
	}
	err = wait.Poll(time.Second, 10*time.Second, func() (bool, error) {
		if len(secret.Data[v1.ServiceAccountTokenKey]) != 0 {
			return false, nil
		}
		secret, err = c.CoreV1().Secrets(ns).Get(context.TODO(), name, metav1.GetOptions{})
		if err != nil {
			return true, fmt.Errorf("failed to get secret (%s:%s) %+v, err: %v", ns, secret.Name, *secret, err)
		}
		return true, nil
	})
	return secret, nil
}

func addReferencedServiceAccountToken(c clientset.Interface, ns string, name string, secret *v1.Secret) error {
	sa, err := c.CoreV1().ServiceAccounts(ns).Get(context.TODO(), name, metav1.GetOptions{})
	if err != nil {
		return err
	}
	sa.Secrets = append(sa.Secrets, v1.ObjectReference{
		APIVersion:      secret.APIVersion,
		Kind:            secret.Kind,
		Namespace:       secret.Namespace,
		Name:            secret.Name,
		ResourceVersion: secret.ResourceVersion,
	})
	if _, err = c.CoreV1().ServiceAccounts(ns).Update(context.TODO(), sa, metav1.UpdateOptions{}); err != nil {
		return err
	}
	return nil
}

type testOperation func() error

func doServiceAccountAPIRequests(t *testing.T, c clientset.Interface, ns string, authenticated bool, canRead bool, canWrite bool) {
	testSecret := &v1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "testSecret"},
		Data:       map[string][]byte{"test": []byte("data")},
	}

	readOps := []testOperation{
		func() error {
			_, err := c.CoreV1().Secrets(ns).List(context.TODO(), metav1.ListOptions{})
			return err
		},
		func() error {
			_, err := c.CoreV1().Pods(ns).List(context.TODO(), metav1.ListOptions{})
			return err
		},
	}
	writeOps := []testOperation{
		func() error {
			_, err := c.CoreV1().Secrets(ns).Create(context.TODO(), testSecret, metav1.CreateOptions{})
			return err
		},
		func() error {
			return c.CoreV1().Secrets(ns).Delete(context.TODO(), testSecret.Name, metav1.DeleteOptions{})
		},
	}

	for _, op := range readOps {
		err := op()
		unauthorizedError := apierrors.IsUnauthorized(err)
		forbiddenError := apierrors.IsForbidden(err)

		switch {
		case !authenticated && !unauthorizedError:
			t.Fatalf("expected unauthorized error, got %v", err)
		case authenticated && unauthorizedError:
			t.Fatalf("unexpected unauthorized error: %v", err)
		case authenticated && canRead && forbiddenError:
			t.Fatalf("unexpected forbidden error: %v", err)
		case authenticated && !canRead && !forbiddenError:
			t.Fatalf("expected forbidden error, got: %v", err)
		}
	}

	for _, op := range writeOps {
		err := op()
		unauthorizedError := apierrors.IsUnauthorized(err)
		forbiddenError := apierrors.IsForbidden(err)

		switch {
		case !authenticated && !unauthorizedError:
			t.Fatalf("expected unauthorized error, got %v", err)
		case authenticated && unauthorizedError:
			t.Fatalf("unexpected unauthorized error: %v", err)
		case authenticated && canWrite && forbiddenError:
			t.Fatalf("unexpected forbidden error: %v", err)
		case authenticated && !canWrite && !forbiddenError:
			t.Fatalf("expected forbidden error, got: %v", err)
		}
	}
}

type warningHandler struct {
	mu       sync.Mutex
	warnings []string
}

func (r *warningHandler) HandleWarningHeader(code int, agent string, message string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.warnings = append(r.warnings, message)
}
