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

package create

import (
	"net/http"
	"testing"

	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/cli-runtime/pkg/genericiooptions"
	"k8s.io/cli-runtime/pkg/resource"
	"k8s.io/client-go/rest/fake"
	cmdtesting "k8s.io/kubectl/pkg/cmd/testing"
	cmdutil "k8s.io/kubectl/pkg/cmd/util"
	"k8s.io/kubectl/pkg/scheme"
)

func TestExtraArgsFail(t *testing.T) {
	cmdtesting.InitTestErrorHandler(t)

	f := cmdtesting.NewTestFactory()
	defer f.Cleanup()

	c := NewCmdCreate(f, genericiooptions.NewTestIOStreamsDiscard())
	ioStreams, _, _, _ := genericiooptions.NewTestIOStreams()
	options := NewCreateOptions(ioStreams)
	if options.Complete(f, c, []string{"rc"}) == nil {
		t.Errorf("unexpected non-error")
	}
}

func TestCreateObject(t *testing.T) {
	// 注册 klog 的测试错误处理器：让 cmdutil.CheckErr 在失败时 panic 而不是 os.Exit，
	// 否则测试进程会直接退出、t.Fatal 无法生效
	cmdtesting.InitTestErrorHandler(t)

	// TestData() 提供一组预置测试对象（rc/svc/pod 等），这里取 rc.Items[0] 作为 POST 的预期响应
	_, _, rc := cmdtesting.TestData()
	rc.Items[0].Name = "redis-master-controller"

	// WithNamespace("test")：让 kubeconfig 解析固定返回 namespace "test"，
	// 与下方断言的 URL 路径 /namespaces/test/... 保持一致
	tf := cmdtesting.NewTestFactory().WithNamespace("test")
	defer tf.Cleanup()

	// LegacyCodec：把响应对象编码成 v1 的 wire format，模拟真实 apiserver 的响应体
	codec := scheme.Codecs.LegacyCodec(scheme.Scheme.PrioritizedVersionsAllGroups()...)

	// fake.RESTClient 替代真实 apiserver：CreateHTTPClient 用一个闭包充当 HTTP handler，
	// 按请求的 path+method 路由响应——不落库，也断言了实际发出的请求形态
	tf.UnstructuredClient = &fake.RESTClient{
		GroupVersion:         schema.GroupVersion{Version: "v1"},
		NegotiatedSerializer: resource.UnstructuredPlusDefaultContentConfig().NegotiatedSerializer,
		Client: fake.CreateHTTPClient(func(req *http.Request) (*http.Response, error) {
			// 只接受对 /namespaces/test/replicationcontrollers 的 POST（即 RunCreate 里的 resource.Helper.Create）
			// 其余请求直接 Fatal，能及时发现 Builder 发出的多余 discovery/mapping 请求
			switch p, m := req.URL.Path, req.Method; {
			case p == "/namespaces/test/replicationcontrollers" && m == http.MethodPost:
				// 返回 201 Created + 预置对象作为响应体
				return &http.Response{StatusCode: http.StatusCreated, Header: cmdtesting.DefaultHeader(), Body: cmdtesting.ObjBody(codec, &rc.Items[0])}, nil
			default:
				t.Fatalf("unexpected request: %#v\n%#v", req.URL, req)
				return nil, nil
			}
		}),
	}

	// NewTestIOStreams 用内存 buffer 捕获 stdout，便于断言命令输出
	ioStreams, _, buf, _ := genericiooptions.NewTestIOStreams()
	cmd := NewCmdCreate(tf, ioStreams)
	// 模拟用户传参：--filename 指向测试清单，--output name 只打印 "<type>/<name>"
	cmd.Flags().Set("filename", "../../../testdata/redis-master-controller.yaml")
	cmd.Flags().Set("output", "name")
	// args 传空切片：create 不接受位置参数（见 Complete 里的校验）
	cmd.Run(cmd, []string{})

	// 断言：输出 "replicationcontroller/redis-master-controller"。
	// 注意 info.Refresh 会用 fake 响应刷新 info.Object，所以打印的名字严格说来自响应体；
	// 测试在开头把 rc.Items[0].Name 也设成清单里的同名值，两侧保持一致。
	// （上游注释 "uses the name from the file, not the response" 的本意是验证
	// create 打印资源名而非随机生成名，此断言沿用至今。）
	if buf.String() != "replicationcontroller/redis-master-controller\n" {
		t.Errorf("unexpected output: %s", buf.String())
	}
}

func TestCreateMultipleObject(t *testing.T) {
	cmdtesting.InitTestErrorHandler(t)
	_, svc, rc := cmdtesting.TestData()

	tf := cmdtesting.NewTestFactory().WithNamespace("test")
	defer tf.Cleanup()

	codec := scheme.Codecs.LegacyCodec(scheme.Scheme.PrioritizedVersionsAllGroups()...)

	tf.UnstructuredClient = &fake.RESTClient{
		GroupVersion:         schema.GroupVersion{Version: "v1"},
		NegotiatedSerializer: resource.UnstructuredPlusDefaultContentConfig().NegotiatedSerializer,
		Client: fake.CreateHTTPClient(func(req *http.Request) (*http.Response, error) {
			switch p, m := req.URL.Path, req.Method; {
			case p == "/namespaces/test/services" && m == http.MethodPost:
				return &http.Response{StatusCode: http.StatusCreated, Header: cmdtesting.DefaultHeader(), Body: cmdtesting.ObjBody(codec, &svc.Items[0])}, nil
			case p == "/namespaces/test/replicationcontrollers" && m == http.MethodPost:
				return &http.Response{StatusCode: http.StatusCreated, Header: cmdtesting.DefaultHeader(), Body: cmdtesting.ObjBody(codec, &rc.Items[0])}, nil
			default:
				t.Fatalf("unexpected request: %#v\n%#v", req.URL, req)
				return nil, nil
			}
		}),
	}

	ioStreams, _, buf, _ := genericiooptions.NewTestIOStreams()
	cmd := NewCmdCreate(tf, ioStreams)
	cmd.Flags().Set("filename", "../../../testdata/redis-master-controller.yaml")
	cmd.Flags().Set("filename", "../../../testdata/frontend-service.yaml")
	cmd.Flags().Set("output", "name")
	cmd.Run(cmd, []string{})

	// Names should come from the REST response, NOT the files
	if buf.String() != "replicationcontroller/rc1\nservice/baz\n" {
		t.Errorf("unexpected output: %s", buf.String())
	}
}

func TestCreateDirectory(t *testing.T) {
	cmdtesting.InitTestErrorHandler(t)
	_, _, rc := cmdtesting.TestData()
	rc.Items[0].Name = "name"

	tf := cmdtesting.NewTestFactory().WithNamespace("test")
	defer tf.Cleanup()

	codec := scheme.Codecs.LegacyCodec(scheme.Scheme.PrioritizedVersionsAllGroups()...)

	tf.UnstructuredClient = &fake.RESTClient{
		GroupVersion:         schema.GroupVersion{Version: "v1"},
		NegotiatedSerializer: resource.UnstructuredPlusDefaultContentConfig().NegotiatedSerializer,
		Client: fake.CreateHTTPClient(func(req *http.Request) (*http.Response, error) {
			switch p, m := req.URL.Path, req.Method; {
			case p == "/namespaces/test/replicationcontrollers" && m == http.MethodPost:
				return &http.Response{StatusCode: http.StatusCreated, Header: cmdtesting.DefaultHeader(), Body: cmdtesting.ObjBody(codec, &rc.Items[0])}, nil
			default:
				t.Fatalf("unexpected request: %#v\n%#v", req.URL, req)
				return nil, nil
			}
		}),
	}

	ioStreams, _, buf, _ := genericiooptions.NewTestIOStreams()
	cmd := NewCmdCreate(tf, ioStreams)
	cmd.Flags().Set("filename", "../../../testdata/replace/legacy")
	cmd.Flags().Set("output", "name")
	cmd.Run(cmd, []string{})

	if buf.String() != "replicationcontroller/name\nreplicationcontroller/name\nreplicationcontroller/name\n" {
		t.Errorf("unexpected output: %s", buf.String())
	}
}

func TestEditOptionsInitializedByComplete(t *testing.T) {
	tf := cmdtesting.NewTestFactory().WithNamespace("test")
	defer tf.Cleanup()

	ioStreams, _, _, _ := genericiooptions.NewTestIOStreams()
	cmd := NewCmdCreate(tf, ioStreams)
	cmd.Flags().Set("filename", "../../../testdata/redis-master-controller.yaml") // nolint:errcheck

	t.Run("editOptions is nil when EditBeforeCreate is false", func(t *testing.T) {
		o := NewCreateOptions(ioStreams)
		o.FilenameOptions.Filenames = []string{"../../../testdata/redis-master-controller.yaml"}
		if err := o.Complete(tf, cmd, []string{}); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if o.editOptions != nil {
			t.Error("expected editOptions to be nil when EditBeforeCreate is false")
		}
	})

	t.Run("editOptions is initialized when EditBeforeCreate is true", func(t *testing.T) {
		o := NewCreateOptions(ioStreams)
		o.EditBeforeCreate = true
		o.FilenameOptions.Filenames = []string{"../../../testdata/redis-master-controller.yaml"}
		if err := o.Complete(tf, cmd, []string{}); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if o.editOptions == nil {
			t.Error("expected editOptions to be initialized when EditBeforeCreate is true")
		}
	})

	t.Run("RunCreate returns error when EditBeforeCreate is true but Complete was not called", func(t *testing.T) {
		o := NewCreateOptions(ioStreams)
		o.EditBeforeCreate = true
		err := o.RunCreate(tf)
		if err == nil {
			t.Fatal("expected error when editOptions is nil")
		}
		if err.Error() != "EditBeforeCreate requires edit options to be initialized via Complete()" {
			t.Errorf("unexpected error message: %v", err)
		}
	})
}

func TestMissingFilenameError(t *testing.T) {
	var errStr string
	var exitCode int
	cmdutil.BehaviorOnFatal(func(str string, code int) {
		if errStr == "" {
			errStr = str
			exitCode = code
		}
	})

	tf := cmdtesting.NewTestFactory().WithNamespace("test")
	defer tf.Cleanup()

	ioStreams, _, buf, _ := genericiooptions.NewTestIOStreams()
	cmd := NewCmdCreate(tf, ioStreams)
	cmd.Run(cmd, []string{})

	if buf.Len() > 0 {
		t.Errorf("unexpected output: %s", buf.String())
	}

	if len(errStr) == 0 {
		t.Errorf("unexpected non-error")
	} else if errStr != "error: must specify one of -f and -k" {
		t.Errorf("unexpected error: %s", errStr)
	}

	if exitCode != 1 {
		t.Errorf("unexpected exit code: %d", exitCode)
	}
}
