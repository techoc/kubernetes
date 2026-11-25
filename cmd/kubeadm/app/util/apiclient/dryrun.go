/*
Copyright 2024 The Kubernetes Authors.

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

// Package apiclient 包含 Kubernetes API 客户端的包装逻辑。
package apiclient

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"

	"github.com/lithammer/dedent"

	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"
	clientsetscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/testing"
	"k8s.io/client-go/tools/clientcmd"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"
	bootstrapapi "k8s.io/cluster-bootstrap/token/api"

	"k8s.io/kubernetes/cmd/kubeadm/app/constants"
	kubeadmutil "k8s.io/kubernetes/cmd/kubeadm/app/util"
	"k8s.io/kubernetes/cmd/kubeadm/app/util/errors"
)

// DryRun 负责对一组不同的 API 客户端执行详细输出的试运行操作。
// 到达 DryRun.FakeClient() 的 REST 操作由伪客户端反应链依次处理：
//   - 记录操作。
//   - 除 GET 和 LIST 外，直接使用伪客户端存储写入；除非用户通过 PrependReactor() 或 AppendReactor() 添加了反应器。
//   - 尝试使用真实动态客户端执行 GET 或 LIST。
//   - 若失败，则从伪客户端存储读取对象；除非用户通过 PrependReactor() 或 AppendReactor() 添加了反应器。
type DryRun struct {
	fakeClient    *fake.Clientset
	client        clientset.Interface
	dynamicClient dynamic.Interface

	writer      io.Writer
	marshalFunc func(runtime.Object, schema.GroupVersion) ([]byte, error)
}

// NewDryRun 创建仅包含伪客户端的 DryRun。
func NewDryRun() *DryRun {
	d := &DryRun{}
	d.fakeClient = fake.NewSimpleClientset()
	d.addReactors()
	return d
}

// WithKubeConfigFile 接收文件路径，并创建真实 clientset 和动态客户端。
func (d *DryRun) WithKubeConfigFile(file string) error {
	config, err := clientcmd.LoadFromFile(file)
	if err != nil {
		return errors.Wrap(err, "failed to load kubeconfig")
	}
	return d.WithKubeConfig(config)
}

// WithKubeConfig 接收 kubeconfig，并创建真实 clientset 和动态客户端。
func (d *DryRun) WithKubeConfig(config *clientcmdapi.Config) error {
	restConfig, err := clientcmd.NewDefaultClientConfig(*config, &clientcmd.ConfigOverrides{}).ClientConfig()
	if err != nil {
		return errors.Wrap(err, "failed to create API client configuration from kubeconfig")
	}
	return d.WithRestConfig(restConfig)
}

// WithRestConfig 接收 rest.Config，并创建真实 clientset 和动态客户端。
func (d *DryRun) WithRestConfig(config *rest.Config) error {
	var err error

	d.client, err = clientset.NewForConfig(config)
	if err != nil {
		return err
	}

	d.dynamicClient, err = dynamic.NewForConfig(config)
	if err != nil {
		return err
	}

	return nil
}

// WithWriter 设置 DryRun 打印输出所用的 io.Writer。
func (d *DryRun) WithWriter(w io.Writer) *DryRun {
	d.writer = w
	return d
}

// WithMarshalFunction 设置 DryRun 的序列化函数。
func (d *DryRun) WithMarshalFunction(f func(runtime.Object, schema.GroupVersion) ([]byte, error)) *DryRun {
	d.marshalFunc = f
	return d
}

// WithDefaultMarshalFunction 将 DryRun 的序列化函数恢复为默认实现。
func (d *DryRun) WithDefaultMarshalFunction() *DryRun {
	d.WithMarshalFunction(kubeadmutil.MarshalToYaml)
	return d
}

// PrependReactor 在伪客户端 ReactorChain 的位置 1 插入反应器。
// 位置 0 保留给日志反应器：
// [ log, r, ... rest of the chain, default fake client reactor ]
func (d *DryRun) PrependReactor(r *testing.SimpleReactor) *DryRun {
	log := d.fakeClient.Fake.ReactionChain[0]
	chain := make([]testing.Reactor, len(d.fakeClient.Fake.ReactionChain)+1)
	chain[0] = log
	chain[1] = r
	copy(chain[2:], d.fakeClient.Fake.ReactionChain[1:])
	d.fakeClient.Fake.ReactionChain = chain
	return d
}

// AppendReactor 在伪客户端 ReactorChain 的 len-2 位置追加反应器。
// len-1 保留给默认伪客户端反应器。
// [ log, rest of the chain... , r, default fake client reactor ]
func (d *DryRun) AppendReactor(r *testing.SimpleReactor) *DryRun {
	sz := len(d.fakeClient.Fake.ReactionChain)
	def := d.fakeClient.Fake.ReactionChain[sz-1]
	d.fakeClient.Fake.ReactionChain[sz-1] = r
	d.fakeClient.Fake.ReactionChain = append(d.fakeClient.Fake.ReactionChain, def)
	return d
}

// Client 返回此 DryRun 的 clientset。
func (d *DryRun) Client() clientset.Interface {
	return d.client
}

// DynamicClient 返回此 DryRun 的动态客户端。
func (d *DryRun) DynamicClient() dynamic.Interface {
	return d.dynamicClient
}

// FakeClient 返回此 DryRun 的伪客户端。
func (d *DryRun) FakeClient() clientset.Interface {
	return d.fakeClient
}

// addRectors 默认由 NewDryRun 在创建伪客户端后调用，
// 在默认伪客户端反应器之前插入一组反应器。
func (d *DryRun) addReactors() {
	reactors := []testing.Reactor{
		// 添加反应器，记录到达伪客户端的所有请求。
		&testing.SimpleReactor{
			Verb:     "*",
			Resource: "*",
			Reaction: func(action testing.Action) (bool, runtime.Object, error) {
				d.LogAction(action)
				return false, nil, nil
			},
		},
		// 添加反应器，处理到达伪客户端的所有 GET 请求。
		// 该反应器调用真实动态客户端；无法处理对象时继续执行反应链。
		&testing.SimpleReactor{
			Verb:     "get",
			Resource: "*",
			Reaction: func(action testing.Action) (bool, runtime.Object, error) {
				getAction, ok := action.(testing.GetAction)
				if !ok {
					return true, nil, errors.New("cannot cast reactor action to GetAction")
				}

				handled, obj, err := d.handleGetAction(getAction)
				if err != nil {
					fmt.Fprintln(d.writer, "[dryrun] Real object does not exist. "+
						"Attempting to GET from followup reactors or from the fake client tracker")
					return false, nil, nil
				}

				d.LogObject(obj, action.GetResource().GroupVersion())
				return handled, obj, err
			},
		},
		// 添加反应器，处理到达伪客户端的所有 LIST 请求。
		// 该反应器调用真实动态客户端；无法处理对象时继续执行反应链。
		&testing.SimpleReactor{
			Verb:     "list",
			Resource: "*",
			Reaction: func(action testing.Action) (bool, runtime.Object, error) {
				listAction, ok := action.(testing.ListAction)
				if !ok {
					return true, nil, errors.New("cannot cast reactor action to ListAction")
				}

				handled, obj, err := d.handleListAction(listAction)
				if err != nil {
					fmt.Fprintln(d.writer, "[dryrun] Real object does not exist. "+
						"Attempting to LIST from followup reactors or from the fake client tracker")
					return false, nil, nil
				}

				d.LogObject(obj, action.GetResource().GroupVersion())
				return handled, obj, err
			},
		},
	}
	d.fakeClient.Fake.ReactionChain = append(reactors, d.fakeClient.Fake.ReactionChain...)
}

// handleGetAction 尝试使用动态客户端处理所有 GET 操作。
func (d *DryRun) handleGetAction(action testing.GetAction) (bool, runtime.Object, error) {
	if d.dynamicClient == nil {
		return false, nil, errors.New("dynamicClient is nil")
	}

	unstructuredObj, err := d.dynamicClient.
		Resource(action.GetResource()).
		Namespace(action.GetNamespace()).
		Get(context.Background(), action.GetName(), metav1.GetOptions{})
	if err != nil {
		return true, nil, err
	}

	newObj, err := d.decodeUnstructuredIntoAPIObject(action, unstructuredObj)
	if err != nil {
		return true, nil, err
	}

	return true, newObj, err
}

// handleListAction 尝试使用动态客户端处理所有 LIST 操作。
func (d *DryRun) handleListAction(action testing.ListAction) (bool, runtime.Object, error) {
	if d.dynamicClient == nil {
		return false, nil, errors.New("dynamicClient is nil")
	}

	listOpts := metav1.ListOptions{
		LabelSelector: action.GetListRestrictions().Labels.String(),
		FieldSelector: action.GetListRestrictions().Fields.String(),
	}

	unstructuredObj, err := d.dynamicClient.
		Resource(action.GetResource()).
		Namespace(action.GetNamespace()).
		List(context.Background(), listOpts)
	if err != nil {
		return true, nil, err
	}

	newObj, err := d.decodeUnstructuredIntoAPIObject(action, unstructuredObj)
	if err != nil {
		return true, nil, err
	}

	return true, newObj, err
}

// decodeUnstructuredIntoAPIObject 将非结构化对象解码为 API 对象。
func (d *DryRun) decodeUnstructuredIntoAPIObject(action testing.Action, obj runtime.Unstructured) (runtime.Object, error) {
	objBytes, err := json.Marshal(obj)
	if err != nil {
		return nil, err
	}
	newObj, err := runtime.Decode(clientsetscheme.Codecs.UniversalDecoder(action.GetResource().GroupVersion()), objBytes)
	if err != nil {
		return nil, err
	}
	return newObj, nil
}

// LogAction 记录操作详情，例如名称、对象和资源。
func (d *DryRun) LogAction(action testing.Action) {
	// actionWithName 是关联名称的操作的通用接口。
	type actionWithNameAndNamespace interface {
		testing.Action
		GetName() string
		GetNamespace() string
	}

	// actionWithObject 是关联对象的操作的通用接口。
	type actionWithObject interface {
		testing.Action
		GetObject() runtime.Object
	}

	group := action.GetResource().Group
	if len(group) == 0 {
		group = "core"
	}
	fmt.Fprintf(d.writer, "[dryrun] Would perform action %s on resource %q in API group \"%s/%s\"\n",
		strings.ToUpper(action.GetVerb()), action.GetResource().Resource, group, action.GetResource().Version)

	namedAction, ok := action.(actionWithNameAndNamespace)
	if ok {
		fmt.Fprintf(d.writer, "[dryrun] Resource name %q, namespace %q\n",
			namedAction.GetName(), namedAction.GetNamespace())
	}

	objAction, ok := action.(actionWithObject)
	if ok && objAction.GetObject() != nil {
		d.LogObject(objAction.GetObject(), objAction.GetResource().GroupVersion())
	}
}

// LogObject 序列化对象，然后输出到此 DryRun 的 io.Writer。
func (d *DryRun) LogObject(obj runtime.Object, gv schema.GroupVersion) {
	objBytes, err := d.marshalFunc(obj, gv)
	if err == nil {
		fmt.Fprintln(d.writer, "[dryrun] Attached object:")
		fmt.Fprintln(d.writer, string(objBytes))
	}
}

// HealthCheckJobReactor 返回处理升级预检 "CreateJob" 所用 Job 对象 GET 操作的反应器。
func (d *DryRun) HealthCheckJobReactor() *testing.SimpleReactor {
	return &testing.SimpleReactor{
		Verb:     "get",
		Resource: "jobs",
		Reaction: func(action testing.Action) (bool, runtime.Object, error) {
			a := action.(testing.GetAction)
			if !strings.HasPrefix(a.GetName(), "upgrade-health-check") || a.GetNamespace() != metav1.NamespaceSystem {
				return false, nil, nil
			}
			obj := getJob(a.GetName(), a.GetNamespace())
			d.LogObject(obj, action.GetResource().GroupVersion())
			return true, obj, nil
		},
	}
}

// PatchNodeReactor 返回处理 Node 对象通用 PATCH 操作的反应器。
func (d *DryRun) PatchNodeReactor() *testing.SimpleReactor {
	return &testing.SimpleReactor{
		Verb:     "patch",
		Resource: "nodes",
		Reaction: func(action testing.Action) (bool, runtime.Object, error) {
			a := action.(testing.PatchAction)
			obj := getNode(a.GetName())
			d.LogObject(obj, action.GetResource().GroupVersion())
			return true, obj, nil
		},
	}
}

// GetNodeReactor 返回处理 Node 对象通用 GET 操作的反应器。
func (d *DryRun) GetNodeReactor() *testing.SimpleReactor {
	return &testing.SimpleReactor{
		Verb:     "get",
		Resource: "nodes",
		Reaction: func(action testing.Action) (bool, runtime.Object, error) {
			a := action.(testing.GetAction)
			obj := getNode(a.GetName())
			d.LogObject(obj, action.GetResource().GroupVersion())
			return true, obj, nil
		},
	}
}

// GetClusterInfoReactor 返回处理节点引导所用 "cluster-info" ConfigMap 的 GET 操作的反应器。
func (d *DryRun) GetClusterInfoReactor() *testing.SimpleReactor {
	return &testing.SimpleReactor{
		Verb:     "get",
		Resource: "configmaps",
		Reaction: func(action testing.Action) (bool, runtime.Object, error) {
			a := action.(testing.GetAction)
			if a.GetName() != bootstrapapi.ConfigMapClusterInfo || a.GetNamespace() != metav1.NamespacePublic {
				return false, nil, nil
			}
			obj := getClusterInfoConfigMap()
			d.LogObject(obj, action.GetResource().GroupVersion())
			return true, obj, nil
		},
	}
}

// GetKubeadmConfigReactor 返回处理 "kubeadm-config" ConfigMap 的 GET 操作的反应器。
func (d *DryRun) GetKubeadmConfigReactor() *testing.SimpleReactor {
	return &testing.SimpleReactor{
		Verb:     "get",
		Resource: "configmaps",
		Reaction: func(action testing.Action) (bool, runtime.Object, error) {
			a := action.(testing.GetAction)
			if a.GetName() != constants.KubeadmConfigConfigMap || a.GetNamespace() != metav1.NamespaceSystem {
				return false, nil, nil
			}

			obj := getKubeadmConfigMap()
			d.LogObject(obj, action.GetResource().GroupVersion())
			return true, obj, nil
		},
	}
}

// GetKubeadmCertsReactor 返回处理 "kubeadm-certs" Secret 的 GET 操作的反应器。
func (d *DryRun) GetKubeadmCertsReactor() *testing.SimpleReactor {
	return &testing.SimpleReactor{
		Verb:     "get",
		Resource: "secrets",
		Reaction: func(action testing.Action) (bool, runtime.Object, error) {
			a := action.(testing.GetAction)
			if a.GetName() != constants.KubeadmCertsSecret || a.GetNamespace() != metav1.NamespaceSystem {
				return false, nil, nil
			}
			obj := getKubeadmCertsSecret()
			d.LogObject(obj, action.GetResource().GroupVersion())
			return true, obj, nil
		},
	}
}

// GetKubeletConfigReactor 返回处理 "kubelet-config" ConfigMap 的 GET 操作的反应器。
func (d *DryRun) GetKubeletConfigReactor() *testing.SimpleReactor {
	return &testing.SimpleReactor{
		Verb:     "get",
		Resource: "configmaps",
		Reaction: func(action testing.Action) (bool, runtime.Object, error) {
			a := action.(testing.GetAction)
			if a.GetName() != constants.KubeletBaseConfigurationConfigMap || a.GetNamespace() != metav1.NamespaceSystem {
				return false, nil, nil
			}
			obj := getKubeletConfigMap()
			d.LogObject(obj, action.GetResource().GroupVersion())
			return true, obj, nil
		},
	}
}

// GetKubeProxyConfigReactor 返回处理 "kube-proxy" ConfigMap 的 GET 操作的反应器。
func (d *DryRun) GetKubeProxyConfigReactor() *testing.SimpleReactor {
	return &testing.SimpleReactor{
		Verb:     "get",
		Resource: "configmaps",
		Reaction: func(action testing.Action) (bool, runtime.Object, error) {
			a := action.(testing.GetAction)
			if a.GetName() != constants.KubeProxyConfigMap || a.GetNamespace() != metav1.NamespaceSystem {
				return false, nil, nil
			}
			obj := getKubeProxyConfigMap()
			d.LogObject(obj, action.GetResource().GroupVersion())
			return true, obj, nil
		},
	}
}

// GetCoreDNSConfigReactor 返回处理 "coredns" ConfigMap 的 GET 操作的反应器。
func (d *DryRun) GetCoreDNSConfigReactor() *testing.SimpleReactor {
	return &testing.SimpleReactor{
		Verb:     "get",
		Resource: "configmaps",
		Reaction: func(action testing.Action) (bool, runtime.Object, error) {
			a := action.(testing.GetAction)
			if a.GetName() != constants.CoreDNSConfigMap || a.GetNamespace() != metav1.NamespaceSystem {
				return false, nil, nil
			}
			obj := getCoreDNSConfigMap()
			d.LogObject(obj, action.GetResource().GroupVersion())
			return true, obj, nil
		},
	}
}

// DeleteBootstrapTokenReactor 返回处理引导令牌 Secret 的 DELETE 操作的反应器。
func (d *DryRun) DeleteBootstrapTokenReactor() *testing.SimpleReactor {
	return &testing.SimpleReactor{
		Verb:     "delete",
		Resource: "secrets",
		Reaction: func(action testing.Action) (bool, runtime.Object, error) {
			a := action.(testing.DeleteAction)
			if !strings.HasPrefix(a.GetName(), bootstrapapi.BootstrapTokenSecretPrefix) || a.GetNamespace() != metav1.NamespaceSystem {
				return false, nil, nil
			}
			obj := &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{
					Name:      a.GetName(),
					Namespace: a.GetNamespace(),
				},
			}
			d.LogObject(obj, action.GetResource().GroupVersion())
			return true, obj, nil
		},
	}
}

// ListPodsReactor 返回处理 Pod LIST 操作的反应器。
func (d *DryRun) ListPodsReactor(nodeName string) *testing.SimpleReactor {
	return &testing.SimpleReactor{
		Verb:     "list",
		Resource: "pods",
		Reaction: func(action testing.Action) (bool, runtime.Object, error) {
			a := action.(testing.ListAction)
			if a.GetNamespace() != metav1.NamespaceSystem {
				return false, nil, nil
			}
			obj := getPodList(nodeName)
			d.LogObject(obj, action.GetResource().GroupVersion())
			return true, obj, nil
		},
	}
}

// ListDeploymentsReactor 返回处理 Deployment LIST 操作的反应器。
func (d *DryRun) ListDeploymentsReactor() *testing.SimpleReactor {
	return &testing.SimpleReactor{
		Verb:     "list",
		Resource: "deployments",
		Reaction: func(action testing.Action) (bool, runtime.Object, error) {
			a := action.(testing.ListAction)
			if a.GetNamespace() != metav1.NamespaceSystem {
				return false, nil, nil
			}
			obj := getDeploymentList()
			d.LogObject(obj, action.GetResource().GroupVersion())
			return true, obj, nil
		},
	}
}

// getJob 返回伪 Job 对象。
func getJob(namespace, name string) *batchv1.Job {
	return &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
		Status: batchv1.JobStatus{
			Conditions: []batchv1.JobCondition{
				{Type: batchv1.JobComplete},
			},
		},
	}
}

// getNode 返回伪 Node 对象。
func getNode(name string) *corev1.Node {
	return &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
			Labels: map[string]string{
				"kubernetes.io/hostname": name,
			},
		},
	}
}

// getConfigMap 返回伪 ConfigMap 对象。
func getConfigMap(namespace, name string, data map[string]string) *corev1.ConfigMap {
	return &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
		Data: data,
	}
}

// getSecret 返回伪 Secret 对象。
func getSecret(namespace, name string, data map[string][]byte) *corev1.Secret {
	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
		Data: data,
	}
}

// getClusterInfoConfigMap 返回伪 "cluster-info" ConfigMap。
func getClusterInfoConfigMap() *corev1.ConfigMap {
	kubeconfig := dedent.Dedent(`apiVersion: v1
clusters:
- cluster:
    certificate-authority-data: LS0tLS1CRUdJTiBDRVJUSUZJQ0FURS0tLS0tCk1JSURCVENDQWUyZ0F3SUJBZ0lJTkpmdGFCK09Xd0F3RFFZSktvWklodmNOQVFFTEJRQXdGVEVUTUJFR0ExVUUKQXhNS2EzVmlaWEp1WlhSbGN6QWVGdzB5TkRBM01EZ3hNalF3TURSYUZ3MHpOREEzTURZeE1qUTFNRFJhTUJVeApFekFSQmdOVkJBTVRDbXQxWW1WeWJtVjBaWE13Z2dFaU1BMEdDU3FHU0liM0RRRUJBUVVBQTRJQkR3QXdnZ0VLCkFvSUJBUURhUURkaWdPeVdpbTZXLzQ4bjNQSG9WZVZCU2lkNldjbmRFV3VVcTVnQldYZGx0OTk2aCtWbkl0bHQKOHpDaGwvb1I1V2ZSYVJDODA1WitvTW4vWThJR1ZRM3QxaG55SW1ZbjR3M3Z6UlhvdUdlQmVpdTJTU1ZqZ0J3agpYanliYk1DbXJBZEljYkllWm1INjZldjV6KzVZS21aUlVZYzNoRGFIcFhkMEVFblp5SlY1d2FaczBYTVFVSE03CmVxT1pBWko5L21PM05VQnBsdnJQbnBPTUs3a1NFUFBnNzVjVTdXSG9KSEZrZVlXNTkzZ3NnQ3MyQnRVdTY0Y3EKYlZYOWJpZ3JZZGRwWmtvRUtLeFU4SEl3SHNJNVY4Um9uM21LRkdsckUxN2IybC84Q3FtQXVPdnl6TEllaVFHWAplZ0lhUi9uUkhISUQ5QVRpNnRYOEVhRERwZXYvQWdNQkFBR2pXVEJYTUE0R0ExVWREd0VCL3dRRUF3SUNwREFQCkJnTlZIUk1CQWY4RUJUQURBUUgvTUIwR0ExVWREZ1FXQkJRWWFDRGU2eXVWcVNhV1Y3M3pMaldtR2hpYVR6QVYKQmdOVkhSRUVEakFNZ2dwcmRXSmxjbTVsZEdWek1BMEdDU3FHU0liM0RRRUJDd1VBQTRJQkFRQ2NlTW5uaVBhcQpHUkVqR3A2WFJTN1FFWW1RcmJFZCtUVjVKUTE4byttVzZvR3BaOENBM0pBSFFETk5QRW1QYzB5dVhEeE85QkZYCmJnMlhSSCtLTkozVzJKZlExVEgzVFhKRWF4Zkh1WDRtQjU5UkNsQzExNGtsV2RIeDFqN0FtRWt1eTZ0ZGpuYWkKZmh0U0dueEEwM2JwN2I4Z28zSWpXNE5wV1JOMVdHNTl2YTBKOEJIRmg3Q0RpZUxuK0RNdUk2M0Jna1kveTJzMApML2RtOVBmcWdVSzFBMy8wZGhDVjZiRUNqekEzSkJld21kSC8rVUJPeVkybUMwNVlQMzNkMHA5eXlrYmtkWE5xCkRONXlBc3ZNUC9PV0NuQjFlQlFUb2pNODJMU3F3dHZtbU1SNHRXYXVoOXVkVktHY2s0eEJaV3Vkcm5LRFVVWEkKUURNUFJnSkMvTng0Ci0tLS0tRU5EIENFUlRJRklDQVRFLS0tLS0K
    server: https://192.168.0.101:6443
  name: ""
contexts: null
current-context: ""
kind: Config
users: null
`)
	data := map[string]string{
		bootstrapapi.JWSSignatureKeyPrefix + "abcdef": "eyJhbGciOiJIUzI1NiIsImtpZCI6ImFiY2RlZiJ9..rh3cVKiU2mdt3CqHzC81sNE-4WQLRHMtXduHWfSbrIM",
		bootstrapapi.KubeConfigKey:                    kubeconfig,
	}
	return getConfigMap(metav1.NamespacePublic, bootstrapapi.ConfigMapClusterInfo, data)
}

// getKubeadmConfigMap 返回伪 "kubeadm-config" ConfigMap。
func getKubeadmConfigMap() *corev1.ConfigMap {
	ccData := fmt.Sprintf(`apiServer: {}
apiVersion: kubeadm.k8s.io/v1beta4
caCertificateValidityPeriod: 87600h0m0s
certificateValidityPeriod: 100h0m0s
certificatesDir: /etc/kubernetes/pki
clusterName: kubernetes
controllerManager:
  extraArgs:
  - name: cluster-signing-duration
    value: 24h
controlPlaneEndpoint: 192.168.0.101:6443
dns: {}
encryptionAlgorithm: RSA-2048
etcd:
  local:
    dataDir: /var/lib/etcd
imageRepository: registry.k8s.io
kind: ClusterConfiguration
kubernetesVersion: %s
networking:
  dnsDomain: cluster.local
  podSubnet: 192.168.0.0/16
  serviceSubnet: 10.96.0.0/12
proxy: {}
scheduler: {}
`, constants.MinimumControlPlaneVersion)

	data := map[string]string{
		constants.ClusterConfigurationKind: ccData,
	}
	return getConfigMap(metav1.NamespaceSystem, constants.KubeadmConfigConfigMap, data)
}

// getKubeadmCertsSecret 返回伪 "kubeadm-certs" Secret。
func getKubeadmCertsSecret() *corev1.Secret {
	// 试运行测试不关心证书内容，因此此处留空。
	data := map[string][]byte{
		constants.CACertName:                                   {},
		constants.CAKeyName:                                    {},
		constants.FrontProxyCACertName:                         {},
		constants.FrontProxyCAKeyName:                          {},
		constants.ServiceAccountPrivateKeyName:                 {},
		constants.ServiceAccountPublicKeyName:                  {},
		strings.ReplaceAll(constants.EtcdCACertName, "/", "-"): {},
		strings.ReplaceAll(constants.EtcdCAKeyName, "/", "-"):  {},
	}

	return getSecret(metav1.NamespaceSystem, constants.KubeadmCertsSecret, data)
}

// getKubeletConfigMap 返回伪 "kubelet-config" ConfigMap。
func getKubeletConfigMap() *corev1.ConfigMap {
	configData := `apiVersion: kubelet.config.k8s.io/v1beta1
authentication:
  anonymous:
    enabled: false
  webhook:
    enabled: true
  x509:
    clientCAFile: /etc/kubernetes/pki/ca.crt
authorization:
  mode: Webhook
cgroupDriver: systemd
clusterDNS:
- 10.96.0.10
clusterDomain: cluster.local
healthzBindAddress: 127.0.0.1
healthzPort: 10248
kind: KubeletConfiguration
memorySwap: {}
resolvConf: /run/systemd/resolve/resolv.conf
rotateCertificates: true
staticPodPath: /etc/kubernetes/manifests
`
	data := map[string]string{
		constants.KubeletBaseConfigurationConfigMapKey: configData,
	}
	return getConfigMap(metav1.NamespaceSystem, constants.KubeletBaseConfigurationConfigMap, data)
}

// getKubeProxyConfigMap 返回伪 "kube-proxy" ConfigMap。
func getKubeProxyConfigMap() *corev1.ConfigMap {
	configData := `apiVersion: kubeproxy.config.k8s.io/v1alpha1
bindAddress: 0.0.0.0
bindAddressHardFail: false
clientConnection:
  kubeconfig: /var/lib/kube-proxy/kubeconfig.conf
clusterCIDR: 192.168.0.0/16
kind: KubeProxyConfiguration
`
	kubeconfigData := `apiVersion: v1
kind: Config
clusters:
- cluster:
    certificate-authority: /var/run/secrets/kubernetes.io/serviceaccount/ca.crt
    server: https://192.168.0.101:6443
  name: default
contexts:
- context:
    cluster: default
    namespace: default
    user: default
  name: default
current-context: default
users:
- name: default
  user:
    tokenFile: /var/run/secrets/kubernetes.io/serviceaccount/token
`
	data := map[string]string{
		constants.KubeProxyConfigMapKey: configData,
		"kubeconfig.conf":               kubeconfigData,
	}
	return getConfigMap(metav1.NamespaceSystem, constants.KubeProxyConfigMap, data)
}

// getCoreDNSConfigMap 返回伪 "coredns" ConfigMap。
func getCoreDNSConfigMap() *corev1.ConfigMap {
	data := map[string]string{
		"Corefile": "",
	}
	return getConfigMap(metav1.NamespaceSystem, constants.CoreDNSConfigMap, data)
}

// getPod 返回伪 Pod。
func getPod(name, nodeName string) corev1.Pod {
	return corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name + "-" + nodeName,
			Namespace: metav1.NamespaceSystem,
			Labels: map[string]string{
				"component": name,
				"tier":      constants.ControlPlaneTier,
			},
			Annotations: map[string]string{
				constants.KubeAPIServerAdvertiseAddressEndpointAnnotationKey: "0.0.0.0:6443",
			},
		},
		Spec: corev1.PodSpec{
			NodeName: nodeName,
			Containers: []corev1.Container{
				{
					Name:  name,
					Image: "registry.k8s.io/" + name + ":v1.1.1",
				},
			},
		},
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning,
		},
	}
}

// getPodList 返回伪 Pod 列表。
func getPodList(nodeName string) *corev1.PodList {
	return &corev1.PodList{
		Items: []corev1.Pod{
			getPod(constants.KubeAPIServer, nodeName),
			getPod(constants.KubeControllerManager, nodeName),
			getPod(constants.KubeScheduler, nodeName),
			getPod(constants.Etcd, nodeName),
		},
	}
}

// getDeploymentList 返回伪 Deployment 列表。
func getDeploymentList() *appsv1.DeploymentList {
	return &appsv1.DeploymentList{
		Items: []appsv1.Deployment{
			{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: metav1.NamespaceSystem,
					Labels: map[string]string{
						"k8s-app": "kube-dns",
					},
				},
				Spec: appsv1.DeploymentSpec{
					Template: corev1.PodTemplateSpec{
						Spec: corev1.PodSpec{
							Containers: []corev1.Container{
								{
									Image: "registry.k8s.io/coredns/coredns:" + constants.CoreDNSVersion,
								},
							},
						},
					},
				},
			},
		},
	}
}
