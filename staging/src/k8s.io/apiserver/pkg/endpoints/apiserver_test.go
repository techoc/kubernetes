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

package endpoints

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/emicklei/go-restful/v3"
	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/require"

	"k8s.io/apimachinery/pkg/api/apitesting"
	"k8s.io/apimachinery/pkg/api/apitesting/fuzzer"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metainternalversion "k8s.io/apimachinery/pkg/apis/meta/internalversion"
	metainternalversionscheme "k8s.io/apimachinery/pkg/apis/meta/internalversion/scheme"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	metav1beta1 "k8s.io/apimachinery/pkg/apis/meta/v1beta1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	"k8s.io/apimachinery/pkg/runtime/serializer/streaming"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/managedfields"
	"k8s.io/apimachinery/pkg/util/net"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/apiserver/pkg/admission"
	auditinternal "k8s.io/apiserver/pkg/apis/audit"
	"k8s.io/apiserver/pkg/apis/example"
	examplefuzzer "k8s.io/apiserver/pkg/apis/example/fuzzer"
	examplev1 "k8s.io/apiserver/pkg/apis/example/v1"
	"k8s.io/apiserver/pkg/audit"
	auditpolicy "k8s.io/apiserver/pkg/audit/policy"
	genericapifilters "k8s.io/apiserver/pkg/endpoints/filters"
	"k8s.io/apiserver/pkg/endpoints/handlers/negotiation"
	"k8s.io/apiserver/pkg/endpoints/handlers/responsewriters"
	"k8s.io/apiserver/pkg/endpoints/request"
	genericapitesting "k8s.io/apiserver/pkg/endpoints/testing"
	"k8s.io/apiserver/pkg/registry/rest"
)

// alwaysMutatingDeny 是一个拒绝一切修改的变更类准入插件，用于验证删除/更新/创建
// 路径都会经过变更准入。
type alwaysMutatingDeny struct{}

func (alwaysMutatingDeny) Admit(ctx context.Context, a admission.Attributes, o admission.ObjectInterfaces) (err error) {
	return admission.NewForbidden(a, errors.New("Mutating admission control is denying all modifications"))
}

func (alwaysMutatingDeny) Handles(operation admission.Operation) bool {
	return true
}

var _ admission.MutationInterface = &alwaysMutatingDeny{}

// alwaysValidatingDeny 是一个拒绝一切请求的校验类准入插件，用于验证各写路径
// 都会经过校验准入。
type alwaysValidatingDeny struct{}

func (alwaysValidatingDeny) Validate(ctx context.Context, a admission.Attributes, o admission.ObjectInterfaces) (err error) {
	return admission.NewForbidden(a, errors.New("Validating admission control is denying all modifications"))
}

func (alwaysValidatingDeny) Handles(operation admission.Operation) bool {
	return true
}

var _ admission.ValidationInterface = &alwaysValidatingDeny{}

// This creates fake API versions, similar to api/latest.go.
var testAPIGroup = "test.group"
var testAPIGroup2 = "test.group2"
var testInternalGroupVersion = schema.GroupVersion{Group: testAPIGroup, Version: runtime.APIVersionInternal}
var testGroupVersion = schema.GroupVersion{Group: testAPIGroup, Version: "version"}
var newGroupVersion = schema.GroupVersion{Group: testAPIGroup, Version: "version2"}
var testGroup2Version = schema.GroupVersion{Group: testAPIGroup2, Version: "version"}
var testInternalGroup2Version = schema.GroupVersion{Group: testAPIGroup2, Version: runtime.APIVersionInternal}
var prefix = "apis"

var grouplessGroupVersion = schema.GroupVersion{Group: "", Version: "v1"}
var grouplessInternalGroupVersion = schema.GroupVersion{Group: "", Version: runtime.APIVersionInternal}
var grouplessPrefix = "api"

var groupVersions = []schema.GroupVersion{grouplessGroupVersion, testGroupVersion, newGroupVersion}

var scheme = runtime.NewScheme()
var codecs = serializer.NewCodecFactory(scheme)

var codec = codecs.LegacyCodec(groupVersions...)
var testCodec = codecs.LegacyCodec(testGroupVersion)
var newCodec = codecs.LegacyCodec(newGroupVersion)
var parameterCodec = runtime.NewParameterCodec(scheme)

var accessor = meta.NewAccessor()
var namer runtime.Namer = accessor
var admissionControl admission.Interface

func init() {
	metav1.AddToGroupVersion(scheme, metav1.SchemeGroupVersion)

	// unnamed core group
	scheme.AddUnversionedTypes(grouplessGroupVersion, &metav1.Status{})
	metav1.AddToGroupVersion(scheme, grouplessGroupVersion)

	utilruntime.Must(example.AddToScheme(scheme))
	utilruntime.Must(examplev1.AddToScheme(scheme))
}

// addGrouplessTypes 注册无组（core 风格，/api 前缀）的测试类型与转换。
func addGrouplessTypes() {
	scheme.AddKnownTypes(grouplessGroupVersion,
		&genericapitesting.Simple{}, &genericapitesting.SimpleList{}, &metav1.ListOptions{},
		&metav1.DeleteOptions{}, &genericapitesting.SimpleGetOptions{}, &genericapitesting.SimpleRoot{})
	scheme.AddKnownTypes(grouplessInternalGroupVersion,
		&genericapitesting.Simple{}, &genericapitesting.SimpleList{},
		&genericapitesting.SimpleGetOptions{}, &genericapitesting.SimpleRoot{})

	utilruntime.Must(genericapitesting.RegisterConversions(scheme))
}

// addTestTypes 注册 test.group 的测试类型。SimpleXGSubresource 同时在
// test.group 与 test.group2 下注册，用于验证跨组子资源的行为。
func addTestTypes() {
	scheme.AddKnownTypes(testGroupVersion,
		&genericapitesting.Simple{}, &genericapitesting.SimpleList{},
		&metav1.DeleteOptions{}, &genericapitesting.SimpleGetOptions{}, &genericapitesting.SimpleRoot{},
		&genericapitesting.SimpleXGSubresource{})
	scheme.AddKnownTypes(testGroupVersion, &examplev1.Pod{})
	scheme.AddKnownTypes(testInternalGroupVersion,
		&genericapitesting.Simple{}, &genericapitesting.SimpleList{},
		&genericapitesting.SimpleGetOptions{}, &genericapitesting.SimpleRoot{},
		&genericapitesting.SimpleXGSubresource{})
	scheme.AddKnownTypes(testInternalGroupVersion, &example.Pod{})
	// Register SimpleXGSubresource in both testGroupVersion and testGroup2Version, and also their
	// their corresponding internal versions, to verify that the desired group version object is
	// served in the tests.
	scheme.AddKnownTypes(testGroup2Version, &genericapitesting.SimpleXGSubresource{})
	scheme.AddKnownTypes(testInternalGroup2Version, &genericapitesting.SimpleXGSubresource{})
	metav1.AddToGroupVersion(scheme, testGroupVersion)

	utilruntime.Must(genericapitesting.RegisterConversions(scheme))
}

// addNewTestTypes 注册 test.group/version2 的测试类型，用于多版本场景。
func addNewTestTypes() {
	scheme.AddKnownTypes(newGroupVersion,
		&genericapitesting.Simple{}, &genericapitesting.SimpleList{},
		&metav1.DeleteOptions{}, &genericapitesting.SimpleGetOptions{}, &genericapitesting.SimpleRoot{},
		&examplev1.Pod{},
	)
	metav1.AddToGroupVersion(scheme, newGroupVersion)

	utilruntime.Must(genericapitesting.RegisterConversions(scheme))
}

func init() {
	// Certain API objects are returned regardless of the contents of storage:
	// api.Status is returned in errors

	addGrouplessTypes()
	addTestTypes()
	addNewTestTypes()

	scheme.AddFieldLabelConversionFunc(grouplessGroupVersion.WithKind("Simple"),
		func(label, value string) (string, string, error) {
			return label, value, nil
		},
	)
	scheme.AddFieldLabelConversionFunc(testGroupVersion.WithKind("Simple"),
		func(label, value string) (string, string, error) {
			return label, value, nil
		},
	)
	scheme.AddFieldLabelConversionFunc(newGroupVersion.WithKind("Simple"),
		func(label, value string) (string, string, error) {
			return label, value, nil
		},
	)
}

// defaultAPIServer exposes nested objects for testability.
// defaultAPIServer 暴露内部的 restful.Container，便于测试检查已注册的路由与
// 生产媒体类型。
type defaultAPIServer struct {
	http.Handler
	container *restful.Container
}

// handleWithWarnings 在 handle 基础上叠加警告记录过滤器，用于字段校验警告的测试。
func handleWithWarnings(storage map[string]rest.Storage) http.Handler {
	return genericapifilters.WithWarningRecorder(handle(storage))
}

// uses the default settings
// handle 用默认准入配置装配一个测试用 apiserver handler。
func handle(storage map[string]rest.Storage) http.Handler {
	return handleInternal(storage, admissionControl, nil)
}

// handleInternal 安装无组、test.group/version、test.group/version2 三个 API 组，
// 并串联审计、请求截止时间、RequestInfo 与审计 ID 过滤器。
func handleInternal(storage map[string]rest.Storage, admissionControl admission.Interface, auditSink audit.Sink) http.Handler {
	container := restful.NewContainer()
	container.Router(restful.CurlyRouter{})
	mux := container.ServeMux

	template := APIGroupVersion{
		Storage: storage,

		Creater:         scheme,
		Convertor:       scheme,
		TypeConverter:   managedfields.NewDeducedTypeConverter(),
		UnsafeConvertor: runtime.UnsafeObjectConvertor(scheme),
		Defaulter:       scheme,
		Typer:           scheme,
		Namer:           namer,

		EquivalentResourceRegistry: runtime.NewEquivalentResourceRegistry(),

		ParameterCodec: parameterCodec,

		Admit: admissionControl,
	}

	// groupless v1 version
	{
		group := template
		group.Root = "/" + grouplessPrefix
		group.GroupVersion = grouplessGroupVersion
		group.OptionsExternalVersion = &grouplessGroupVersion
		group.Serializer = codecs
		if _, _, err := (&group).InstallREST(container); err != nil {
			panic(fmt.Sprintf("unable to install container %s: %v", group.GroupVersion, err))
		}
	}

	// group version 1
	{
		group := template
		group.Root = "/" + prefix
		group.GroupVersion = testGroupVersion
		group.OptionsExternalVersion = &testGroupVersion
		group.Serializer = codecs
		if _, _, err := (&group).InstallREST(container); err != nil {
			panic(fmt.Sprintf("unable to install container %s: %v", group.GroupVersion, err))
		}
	}

	// group version 2
	{
		group := template
		group.Root = "/" + prefix
		group.GroupVersion = newGroupVersion
		group.OptionsExternalVersion = &newGroupVersion
		group.Serializer = codecs
		if _, _, err := (&group).InstallREST(container); err != nil {
			panic(fmt.Sprintf("unable to install container %s: %v", group.GroupVersion, err))
		}
	}
	longRunningCheck := func(r *http.Request, requestInfo *request.RequestInfo) bool {
		// simplified long-running check
		return requestInfo.Verb == "watch" || requestInfo.Verb == "proxy"
	}
	fakeRuleEvaluator := auditpolicy.NewFakePolicyRuleEvaluator(auditinternal.LevelRequestResponse, nil)
	handler := genericapifilters.WithAudit(mux, auditSink, fakeRuleEvaluator, longRunningCheck)
	handler = genericapifilters.WithRequestDeadline(handler, auditSink, fakeRuleEvaluator, longRunningCheck, codecs, 60*time.Second)
	handler = genericapifilters.WithRequestInfo(handler, testRequestInfoResolver())
	handler = genericapifilters.WithAuditInit(handler)

	return &defaultAPIServer{handler, container}
}

// testRequestInfoResolver 返回测试用的 RequestInfo 解析器（/api 为无组前缀）。
func testRequestInfoResolver() *request.RequestInfoFactory {
	return &request.RequestInfoFactory{
		APIPrefixes:          sets.NewString("api", "apis"),
		GrouplessAPIPrefixes: sets.NewString("api"),
	}
}

// TestSimpleSetupRight 是编解码器的自检：Simple 经编码再解码必须保持不变。
func TestSimpleSetupRight(t *testing.T) {
	s := &genericapitesting.Simple{ObjectMeta: metav1.ObjectMeta{Name: "aName"}}
	wire, err := runtime.Encode(codec, s)
	if err != nil {
		t.Fatal(err)
	}
	s2, err := runtime.Decode(codec, wire)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(s, s2) {
		t.Fatalf("encode/decode broken:\n%#v\n%#v\n", s, s2)
	}
}

// TestSimpleOptionsSetupRight 验证 options 类型的编解码往返无损。
func TestSimpleOptionsSetupRight(t *testing.T) {
	s := &genericapitesting.SimpleGetOptions{}
	wire, err := runtime.Encode(codec, s)
	if err != nil {
		t.Fatal(err)
	}
	s2, err := runtime.Decode(codec, wire)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(s, s2) {
		t.Fatalf("encode/decode broken:\n%#v\n%#v\n", s, s2)
	}
}

// SimpleRESTStorage 是测试用的通用存储桩：记录收到的请求参数，并可注入错误
// 或自定义返回，从而在不依赖真实存储的情况下验证 handler 行为。
type SimpleRESTStorage struct {
	lock sync.Mutex

	errors map[string]error
	list   []genericapitesting.Simple
	item   genericapitesting.Simple

	updated *genericapitesting.Simple
	created *genericapitesting.Simple

	stream *SimpleStream

	deleted       string
	deleteOptions *metav1.DeleteOptions

	actualNamespace  string
	namespacePresent bool

	// These are set when Watch is called
	fakeWatch                  *watch.FakeWatcher
	requestedLabelSelector     labels.Selector
	requestedFieldSelector     fields.Selector
	requestedResourceVersion   string
	requestedResourceNamespace string

	expectedResourceNamespace string

	// If non-nil, called inside the WorkFunc when answering update, delete, create.
	// obj receives the original input to the update, delete, or create call.
	injectedFunction func(obj runtime.Object) (returnObj runtime.Object, err error)
}

func (storage *SimpleRESTStorage) NamespaceScoped() bool {
	return true
}

func (storage *SimpleRESTStorage) ConvertToTable(ctx context.Context, obj runtime.Object, tableOptions runtime.Object) (*metav1.Table, error) {
	return rest.NewDefaultTableConvertor(schema.GroupResource{Resource: "simple"}).ConvertToTable(ctx, obj, tableOptions)
}

func (storate *SimpleRESTStorage) GetSingularName() string {
	return "simple"
}

// List 记录收到的选择器并返回预设列表与错误。
func (storage *SimpleRESTStorage) List(ctx context.Context, options *metainternalversion.ListOptions) (runtime.Object, error) {
	storage.checkContext(ctx)
	result := &genericapitesting.SimpleList{
		ListMeta: metav1.ListMeta{
			ResourceVersion: "10",
		},
		Items: storage.list,
	}
	storage.requestedLabelSelector = labels.Everything()
	if options != nil && options.LabelSelector != nil {
		storage.requestedLabelSelector = options.LabelSelector
	}
	storage.requestedFieldSelector = fields.Everything()
	if options != nil && options.FieldSelector != nil {
		storage.requestedFieldSelector = options.FieldSelector
	}
	return result, storage.errors["list"]
}

// SimpleStream 实现 rest.Streamer，用于测试二进制/流式 GET 响应。
type SimpleStream struct {
	version     string
	accept      string
	contentType string
	err         error

	io.Reader
	closed bool
}

func (s *SimpleStream) Close() error {
	s.closed = true
	return nil
}

func (s *SimpleStream) GetObjectKind() schema.ObjectKind { return schema.EmptyObjectKind }
func (s *SimpleStream) DeepCopyObject() runtime.Object {
	panic("SimpleStream does not support DeepCopy")
}

func (s *SimpleStream) InputStream(_ context.Context, version, accept string) (io.ReadCloser, bool, string, error) {
	s.version = version
	s.accept = accept
	return s, false, s.contentType, s.err
}

// OutputConnect 是一个固定返回预设文本的 http.Handler，用于 connect 测试。
type OutputConnect struct {
	response string
}

func (h *OutputConnect) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	w.Write([]byte(h.response))
}

// Get 对 id=="binary" 返回流对象，其余返回预设 item。
func (storage *SimpleRESTStorage) Get(ctx context.Context, id string, options *metav1.GetOptions) (runtime.Object, error) {
	storage.checkContext(ctx)
	if id == "binary" {
		return storage.stream, storage.errors["get"]
	}
	return storage.item.DeepCopy(), storage.errors["get"]
}

func (storage *SimpleRESTStorage) checkContext(ctx context.Context) {
	storage.actualNamespace, storage.namespacePresent = request.NamespaceFrom(ctx)
}

// Delete 记录被删除的 id 与选项，并执行删除校验回调。
func (storage *SimpleRESTStorage) Delete(ctx context.Context, id string, deleteValidation rest.ValidateObjectFunc, options *metav1.DeleteOptions) (runtime.Object, bool, error) {
	storage.checkContext(ctx)
	storage.deleted = id
	storage.deleteOptions = options
	if err := storage.errors["delete"]; err != nil {
		return nil, false, err
	}
	if err := deleteValidation(ctx, &storage.item); err != nil {
		return nil, false, err
	}
	var obj runtime.Object = &metav1.Status{Status: metav1.StatusSuccess}
	var err error
	if storage.injectedFunction != nil {
		obj, err = storage.injectedFunction(&genericapitesting.Simple{ObjectMeta: metav1.ObjectMeta{Name: id}})
	}
	return obj, true, err
}

func (storage *SimpleRESTStorage) New() runtime.Object {
	return &genericapitesting.Simple{}
}

func (storage *SimpleRESTStorage) NewList() runtime.Object {
	return &genericapitesting.SimpleList{}
}

func (storage *SimpleRESTStorage) Destroy() {
}

// Create 记录创建的对象，并依次执行注入函数与创建校验。
func (storage *SimpleRESTStorage) Create(ctx context.Context, obj runtime.Object, createValidation rest.ValidateObjectFunc, options *metav1.CreateOptions) (runtime.Object, error) {
	storage.checkContext(ctx)
	storage.created = obj.(*genericapitesting.Simple)
	if err := storage.errors["create"]; err != nil {
		return nil, err
	}
	var err error
	if storage.injectedFunction != nil {
		obj, err = storage.injectedFunction(obj)
	}
	if err := createValidation(ctx, obj); err != nil {
		return nil, err
	}
	return obj, err
}

// Update 通过 objInfo 计算出新对象，再执行注入函数与更新校验。
func (storage *SimpleRESTStorage) Update(ctx context.Context, name string, objInfo rest.UpdatedObjectInfo, createValidation rest.ValidateObjectFunc, updateValidation rest.ValidateObjectUpdateFunc, forceAllowCreate bool, options *metav1.UpdateOptions) (runtime.Object, bool, error) {
	storage.checkContext(ctx)
	obj, err := objInfo.UpdatedObject(ctx, &storage.item)
	if err != nil {
		return nil, false, err
	}
	storage.updated = obj.(*genericapitesting.Simple)
	if err := storage.errors["update"]; err != nil {
		return nil, false, err
	}
	if storage.injectedFunction != nil {
		obj, err = storage.injectedFunction(obj)
	}
	if err := updateValidation(ctx, &storage.item, obj); err != nil {
		return nil, false, err
	}
	return obj, false, err
}

// Implement ResourceWatcher.
// Watch 记录 watch 参数并返回一个可供测试手动推送事件的 FakeWatcher。
func (storage *SimpleRESTStorage) Watch(ctx context.Context, options *metainternalversion.ListOptions) (watch.Interface, error) {
	storage.lock.Lock()
	defer storage.lock.Unlock()
	storage.checkContext(ctx)
	storage.requestedLabelSelector = labels.Everything()
	if options != nil && options.LabelSelector != nil {
		storage.requestedLabelSelector = options.LabelSelector
	}
	storage.requestedFieldSelector = fields.Everything()
	if options != nil && options.FieldSelector != nil {
		storage.requestedFieldSelector = options.FieldSelector
	}
	storage.requestedResourceVersion = ""
	if options != nil {
		storage.requestedResourceVersion = options.ResourceVersion
	}
	storage.requestedResourceNamespace = request.NamespaceValue(ctx)
	if err := storage.errors["watch"]; err != nil {
		return nil, err
	}
	storage.fakeWatch = watch.NewFake()
	return storage.fakeWatch, nil
}

func (storage *SimpleRESTStorage) Watcher() *watch.FakeWatcher {
	storage.lock.Lock()
	defer storage.lock.Unlock()
	return storage.fakeWatch
}

// Implement Connecter
// ConnecterRESTStorage 实现 rest.Connecter，记录 Connect 收到的参数与 responder。
type ConnecterRESTStorage struct {
	connectHandler http.Handler
	handlerFunc    func() http.Handler

	emptyConnectOptions    runtime.Object
	receivedConnectOptions runtime.Object
	receivedID             string
	receivedResponder      rest.Responder
	takesPath              string
}

// Implement Connecter
var _ = rest.Connecter(&ConnecterRESTStorage{})

func (s *ConnecterRESTStorage) New() runtime.Object {
	return &genericapitesting.Simple{}
}

func (s *ConnecterRESTStorage) Destroy() {
}

// Connect 记录 connect 参数，并返回预设或按需构造的 handler。
func (s *ConnecterRESTStorage) Connect(ctx context.Context, id string, options runtime.Object, responder rest.Responder) (http.Handler, error) {
	s.receivedConnectOptions = options
	s.receivedID = id
	s.receivedResponder = responder
	if s.handlerFunc != nil {
		return s.handlerFunc(), nil
	}
	return s.connectHandler, nil
}

func (s *ConnecterRESTStorage) ConnectMethods() []string {
	return []string{request.MethodGet, request.MethodPost, request.MethodPut, request.MethodDelete}
}

// NewConnectOptions 返回空 options；takesPath 非空时声明需要捕获 URL 尾部路径。
func (s *ConnecterRESTStorage) NewConnectOptions() (runtime.Object, bool, string) {
	if len(s.takesPath) > 0 {
		return s.emptyConnectOptions, true, s.takesPath
	}
	return s.emptyConnectOptions, false, ""
}

func (s *ConnecterRESTStorage) GetSingularName() string {
	return "simple"
}

// MetadataRESTStorage 在 SimpleRESTStorage 之上实现 rest.StorageMetadata，
// 用于验证自定义 Produces MIME 类型会反映到注册的路由中。
type MetadataRESTStorage struct {
	*SimpleRESTStorage
	types []string
}

func (m *MetadataRESTStorage) ProducesMIMETypes(method string) []string {
	return m.types
}

func (m *MetadataRESTStorage) ProducesObject(verb string) interface{} {
	return nil
}

var _ rest.StorageMetadata = &MetadataRESTStorage{}

// GetWithOptionsRESTStorage 实现 GetterWithOptions，并可选择把 URL 尾部路径
// 作为选项传入。
type GetWithOptionsRESTStorage struct {
	*SimpleRESTStorage
	optionsReceived runtime.Object
	takesPath       string
}

func (r *GetWithOptionsRESTStorage) Get(ctx context.Context, name string, options runtime.Object) (runtime.Object, error) {
	if _, ok := options.(*genericapitesting.SimpleGetOptions); !ok {
		return nil, fmt.Errorf("Unexpected options object: %#v", options)
	}
	r.optionsReceived = options
	return r.SimpleRESTStorage.Get(ctx, name, &metav1.GetOptions{})
}

func (r *GetWithOptionsRESTStorage) NewGetOptions() (runtime.Object, bool, string) {
	if len(r.takesPath) > 0 {
		return &genericapitesting.SimpleGetOptions{}, true, r.takesPath
	}
	return &genericapitesting.SimpleGetOptions{}, false, ""
}

var _ rest.GetterWithOptions = &GetWithOptionsRESTStorage{}

// GetWithOptionsRootRESTStorage 是集群级（非命名空间）的 GetterWithOptions 变体。
type GetWithOptionsRootRESTStorage struct {
	*SimpleTypedStorage
	optionsReceived runtime.Object
	takesPath       string
}

func (r *GetWithOptionsRootRESTStorage) GetSingularName() string {
	return "simple"
}

func (r *GetWithOptionsRootRESTStorage) NamespaceScoped() bool {
	return false
}

func (r *GetWithOptionsRootRESTStorage) Get(ctx context.Context, name string, options runtime.Object) (runtime.Object, error) {
	if _, ok := options.(*genericapitesting.SimpleGetOptions); !ok {
		return nil, fmt.Errorf("Unexpected options object: %#v", options)
	}
	r.optionsReceived = options
	return r.SimpleTypedStorage.Get(ctx, name, &metav1.GetOptions{})
}

func (r *GetWithOptionsRootRESTStorage) NewGetOptions() (runtime.Object, bool, string) {
	if len(r.takesPath) > 0 {
		return &genericapitesting.SimpleGetOptions{}, true, r.takesPath
	}
	return &genericapitesting.SimpleGetOptions{}, false, ""
}

var _ rest.GetterWithOptions = &GetWithOptionsRootRESTStorage{}

// NamedCreaterRESTStorage 实现带名称的创建接口（POST 到具体对象名）。
type NamedCreaterRESTStorage struct {
	*SimpleRESTStorage
	createdName string
}

func (storage *NamedCreaterRESTStorage) Create(ctx context.Context, name string, obj runtime.Object, createValidation rest.ValidateObjectFunc, options *metav1.CreateOptions) (runtime.Object, error) {
	storage.checkContext(ctx)
	storage.created = obj.(*genericapitesting.Simple)
	storage.createdName = name
	if err := storage.errors["create"]; err != nil {
		return nil, err
	}
	var err error
	if storage.injectedFunction != nil {
		obj, err = storage.injectedFunction(obj)
	}
	if err := createValidation(ctx, obj); err != nil {
		return nil, err
	}
	return obj, err
}

// SimpleTypedStorage 是只实现 Get 的极简存储桩，类型由 baseType 决定。
type SimpleTypedStorage struct {
	errors   map[string]error
	item     runtime.Object
	baseType runtime.Object

	actualNamespace  string
	namespacePresent bool
}

func (storage *SimpleTypedStorage) New() runtime.Object {
	return storage.baseType
}

func (storage *SimpleTypedStorage) Destroy() {
}

func (storage *SimpleTypedStorage) Get(ctx context.Context, id string, options *metav1.GetOptions) (runtime.Object, error) {
	storage.checkContext(ctx)
	return storage.item.DeepCopyObject(), storage.errors["get"]
}

func (storage *SimpleTypedStorage) checkContext(ctx context.Context) {
	storage.actualNamespace, storage.namespacePresent = request.NamespaceFrom(ctx)
}

func (storage *SimpleTypedStorage) GetSingularName() string {
	return "simple"
}

// bodyOrDie 读取并关闭响应体，读取失败直接 panic（仅用于测试断言消息）。
func bodyOrDie(response *http.Response) string {
	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)
	if err != nil {
		panic(err)
	}
	return string(body)
}

// extractBody 用全局 codec 解码响应体到 object，同时返回原始文本。
func extractBody(response *http.Response, object runtime.Object) (string, error) {
	return extractBodyDecoder(response, object, codec)
}

// extractBodyDecoder 与 extractBody 相同，但使用调用方指定的解码器。
func extractBodyDecoder(response *http.Response, object runtime.Object, decoder runtime.Decoder) (string, error) {
	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)
	if err != nil {
		return string(body), err
	}
	return string(body), runtime.DecodeInto(decoder, body, object)
}

// extractBodyObject 解码响应体为任意 runtime.Object，并返回原始文本。
func extractBodyObject(response *http.Response, decoder runtime.Decoder) (runtime.Object, string, error) {
	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)
	if err != nil {
		return nil, string(body), err
	}
	obj, err := runtime.Decode(decoder, body)
	return obj, string(body), err
}

// TestNotFound 覆盖路由未命中与不支持的方法：期望 404 或 405，而不是 500。
func TestNotFound(t *testing.T) {
	type T struct {
		Method string
		Path   string
		Status int
	}
	cases := map[string]T{
		// Positive checks to make sure everything is wired correctly
		"groupless GET root":       {request.MethodGet, "/" + grouplessPrefix + "/" + grouplessGroupVersion.Version + "/simpleroots", http.StatusOK},
		"groupless GET namespaced": {request.MethodGet, "/" + grouplessPrefix + "/" + grouplessGroupVersion.Version + "/namespaces/ns/simples", http.StatusOK},

		"groupless GET long prefix": {request.MethodGet, "/" + grouplessPrefix + "/", http.StatusNotFound},

		"groupless root PATCH method":                 {request.MethodPatch, "/" + grouplessPrefix + "/" + grouplessGroupVersion.Version + "/simpleroots", http.StatusMethodNotAllowed},
		"groupless root GET missing storage":          {request.MethodGet, "/" + grouplessPrefix + "/" + grouplessGroupVersion.Version + "/blah", http.StatusNotFound},
		"groupless root GET with extra segment":       {request.MethodGet, "/" + grouplessPrefix + "/" + grouplessGroupVersion.Version + "/simpleroots/bar/baz", http.StatusNotFound},
		"groupless root DELETE without extra segment": {request.MethodDelete, "/" + grouplessPrefix + "/" + grouplessGroupVersion.Version + "/simpleroots", http.StatusMethodNotAllowed},
		"groupless root DELETE with extra segment":    {request.MethodDelete, "/" + grouplessPrefix + "/" + grouplessGroupVersion.Version + "/simpleroots/bar/baz", http.StatusNotFound},
		"groupless root PUT without extra segment":    {request.MethodPut, "/" + grouplessPrefix + "/" + grouplessGroupVersion.Version + "/simpleroots", http.StatusMethodNotAllowed},
		"groupless root PUT with extra segment":       {request.MethodPut, "/" + grouplessPrefix + "/" + grouplessGroupVersion.Version + "/simpleroots/bar/baz", http.StatusNotFound},
		"groupless root watch missing storage":        {request.MethodGet, "/" + grouplessPrefix + "/" + grouplessGroupVersion.Version + "/watch/", http.StatusInternalServerError},

		"groupless namespaced PATCH method":                 {request.MethodPatch, "/" + grouplessPrefix + "/" + grouplessGroupVersion.Version + "/namespaces/ns/simples", http.StatusMethodNotAllowed},
		"groupless namespaced GET long prefix":              {request.MethodGet, "/" + grouplessPrefix + "/", http.StatusNotFound},
		"groupless namespaced GET missing storage":          {request.MethodGet, "/" + grouplessPrefix + "/" + grouplessGroupVersion.Version + "/blah", http.StatusNotFound},
		"groupless namespaced GET with extra segment":       {request.MethodGet, "/" + grouplessPrefix + "/" + grouplessGroupVersion.Version + "/namespaces/ns/simples/bar/baz", http.StatusNotFound},
		"groupless namespaced POST with extra segment":      {request.MethodPost, "/" + grouplessPrefix + "/" + grouplessGroupVersion.Version + "/namespaces/ns/simples/bar", http.StatusMethodNotAllowed},
		"groupless namespaced DELETE without extra segment": {request.MethodDelete, "/" + grouplessPrefix + "/" + grouplessGroupVersion.Version + "/namespaces/ns/simples", http.StatusMethodNotAllowed},
		"groupless namespaced DELETE with extra segment":    {request.MethodDelete, "/" + grouplessPrefix + "/" + grouplessGroupVersion.Version + "/namespaces/ns/simples/bar/baz", http.StatusNotFound},
		"groupless namespaced PUT without extra segment":    {request.MethodPut, "/" + grouplessPrefix + "/" + grouplessGroupVersion.Version + "/namespaces/ns/simples", http.StatusMethodNotAllowed},
		"groupless namespaced PUT with extra segment":       {request.MethodPut, "/" + grouplessPrefix + "/" + grouplessGroupVersion.Version + "/namespaces/ns/simples/bar/baz", http.StatusNotFound},
		"groupless namespaced watch missing storage":        {request.MethodGet, "/" + grouplessPrefix + "/" + grouplessGroupVersion.Version + "/watch/", http.StatusInternalServerError},
		"groupless namespaced watch with bad method":        {request.MethodPost, "/" + grouplessPrefix + "/" + grouplessGroupVersion.Version + "/watch/namespaces/ns/simples/bar", http.StatusMethodNotAllowed},
		"groupless namespaced watch param with bad method":  {request.MethodPost, "/" + grouplessPrefix + "/" + grouplessGroupVersion.Version + "/namespaces/ns/simples/bar?watch=true", http.StatusMethodNotAllowed},

		// Positive checks to make sure everything is wired correctly
		"GET root": {request.MethodGet, "/" + prefix + "/" + testGroupVersion.Group + "/" + testGroupVersion.Version + "/simpleroots", http.StatusOK},
		// TODO: JTL: "GET root item":       {request.MethodGet, "/" + prefix + "/" + testGroupVersion.Group + "/" + testGroupVersion.Version + "/simpleroots/bar", http.StatusOK},
		"GET namespaced": {request.MethodGet, "/" + prefix + "/" + testGroupVersion.Group + "/" + testGroupVersion.Version + "/namespaces/ns/simples", http.StatusOK},
		// TODO: JTL: "GET namespaced item": {request.MethodGet, "/" + prefix + "/" + testGroupVersion.Group + "/" + testGroupVersion.Version + "/namespaces/ns/simples/bar", http.StatusOK},

		"GET long prefix": {request.MethodGet, "/" + prefix + "/", http.StatusNotFound},

		"root PATCH method":           {request.MethodPatch, "/" + prefix + "/" + testGroupVersion.Group + "/" + testGroupVersion.Version + "/simpleroots", http.StatusMethodNotAllowed},
		"root GET missing storage":    {request.MethodGet, "/" + prefix + "/" + testGroupVersion.Group + "/" + testGroupVersion.Version + "/blah", http.StatusNotFound},
		"root GET with extra segment": {request.MethodGet, "/" + prefix + "/" + testGroupVersion.Group + "/" + testGroupVersion.Version + "/simpleroots/bar/baz", http.StatusNotFound},
		// TODO: JTL: "root POST with extra segment":      {request.MethodPost, "/" + prefix + "/" + testGroupVersion.Group + "/" + testGroupVersion.Version + "/simpleroots/bar", http.StatusMethodNotAllowed},
		"root DELETE without extra segment": {request.MethodDelete, "/" + prefix + "/" + testGroupVersion.Group + "/" + testGroupVersion.Version + "/simpleroots", http.StatusMethodNotAllowed},
		"root DELETE with extra segment":    {request.MethodDelete, "/" + prefix + "/" + testGroupVersion.Group + "/" + testGroupVersion.Version + "/simpleroots/bar/baz", http.StatusNotFound},
		"root PUT without extra segment":    {request.MethodPut, "/" + prefix + "/" + testGroupVersion.Group + "/" + testGroupVersion.Version + "/simpleroots", http.StatusMethodNotAllowed},
		"root PUT with extra segment":       {request.MethodPut, "/" + prefix + "/" + testGroupVersion.Group + "/" + testGroupVersion.Version + "/simpleroots/bar/baz", http.StatusNotFound},
		"root watch missing storage":        {request.MethodGet, "/" + prefix + "/" + testGroupVersion.Group + "/" + testGroupVersion.Version + "/watch/", http.StatusInternalServerError},
		// TODO: JTL: "root watch with bad method":        {request.MethodPost, "/" + prefix + "/" + testGroupVersion.Group + "/" + testGroupVersion.Version + "/watch/simpleroot/bar", http.StatusMethodNotAllowed},

		"namespaced PATCH method":                 {request.MethodPatch, "/" + prefix + "/" + testGroupVersion.Group + "/" + testGroupVersion.Version + "/namespaces/ns/simples", http.StatusMethodNotAllowed},
		"namespaced GET long prefix":              {request.MethodGet, "/" + prefix + "/", http.StatusNotFound},
		"namespaced GET missing storage":          {request.MethodGet, "/" + prefix + "/" + testGroupVersion.Group + "/" + testGroupVersion.Version + "/blah", http.StatusNotFound},
		"namespaced GET with extra segment":       {request.MethodGet, "/" + prefix + "/" + testGroupVersion.Group + "/" + testGroupVersion.Version + "/namespaces/ns/simples/bar/baz", http.StatusNotFound},
		"namespaced POST with extra segment":      {request.MethodPost, "/" + prefix + "/" + testGroupVersion.Group + "/" + testGroupVersion.Version + "/namespaces/ns/simples/bar", http.StatusMethodNotAllowed},
		"namespaced DELETE without extra segment": {request.MethodDelete, "/" + prefix + "/" + testGroupVersion.Group + "/" + testGroupVersion.Version + "/namespaces/ns/simples", http.StatusMethodNotAllowed},
		"namespaced DELETE with extra segment":    {request.MethodDelete, "/" + prefix + "/" + testGroupVersion.Group + "/" + testGroupVersion.Version + "/namespaces/ns/simples/bar/baz", http.StatusNotFound},
		"namespaced PUT without extra segment":    {request.MethodPut, "/" + prefix + "/" + testGroupVersion.Group + "/" + testGroupVersion.Version + "/namespaces/ns/simples", http.StatusMethodNotAllowed},
		"namespaced PUT with extra segment":       {request.MethodPut, "/" + prefix + "/" + testGroupVersion.Group + "/" + testGroupVersion.Version + "/namespaces/ns/simples/bar/baz", http.StatusNotFound},
		"namespaced watch missing storage":        {request.MethodGet, "/" + prefix + "/" + testGroupVersion.Group + "/" + testGroupVersion.Version + "/watch/", http.StatusInternalServerError},
		"namespaced watch with bad method":        {request.MethodPost, "/" + prefix + "/" + testGroupVersion.Group + "/" + testGroupVersion.Version + "/watch/namespaces/ns/simples/bar", http.StatusMethodNotAllowed},
		"namespaced watch param with bad method":  {request.MethodPost, "/" + prefix + "/" + testGroupVersion.Group + "/" + testGroupVersion.Version + "/namespaces/ns/simples/bar?watch=true", http.StatusMethodNotAllowed},
	}
	handler := handle(map[string]rest.Storage{
		"simples":     &SimpleRESTStorage{},
		"simpleroots": &SimpleRESTStorage{},
	})
	server := httptest.NewServer(handler)
	defer server.Close()
	client := http.Client{}
	for testName, test := range cases {
		t.Run(testName, func(t *testing.T) {
			ctx := t.Context()
			request, err := http.NewRequestWithContext(ctx, test.Method, server.URL+test.Path, nil)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			response, err := client.Do(request)
			if err != nil {
				t.Errorf("unexpected error: %v", err)
			}
			defer apitesting.Close(t, response.Body)
			if response.StatusCode != test.Status {
				t.Errorf("Expected %d for %s, Got %#v", test.Status, test.Method, response)
			}
		})
	}
}

// UnimplementedRESTStorage 不实现任何动词接口，用于验证“未实现的动词不会被注册”。
type UnimplementedRESTStorage struct{}

func (UnimplementedRESTStorage) NamespaceScoped() bool {
	return true
}

func (UnimplementedRESTStorage) New() runtime.Object {
	return &genericapitesting.Simple{}
}

func (UnimplementedRESTStorage) Destroy() {
}

func (UnimplementedRESTStorage) GetSingularName() string {
	return ""
}

// TestUnimplementedRESTStorage ensures that if a rest.Storage does not implement a given
// method, that it is literally not registered with the server.  In the past,
// we registered everything, and returned method not supported if it didn't support
// a verb.  Now we literally do not register a storage if it does not implement anything.
// TODO: in future, we should update proxy/redirect
func TestUnimplementedRESTStorage(t *testing.T) {
	type T struct {
		Method  string
		Path    string
		ErrCode int
	}
	cases := map[string]T{
		"groupless GET object":    {request.MethodGet, "/" + grouplessPrefix + "/" + grouplessGroupVersion.Version + "/foo/bar", http.StatusNotFound},
		"groupless GET list":      {request.MethodGet, "/" + grouplessPrefix + "/" + grouplessGroupVersion.Version + "/foo", http.StatusNotFound},
		"groupless POST list":     {request.MethodPost, "/" + grouplessPrefix + "/" + grouplessGroupVersion.Version + "/foo", http.StatusNotFound},
		"groupless PUT object":    {request.MethodPut, "/" + grouplessPrefix + "/" + grouplessGroupVersion.Version + "/foo/bar", http.StatusNotFound},
		"groupless DELETE object": {request.MethodDelete, "/" + grouplessPrefix + "/" + grouplessGroupVersion.Version + "/foo/bar", http.StatusNotFound},
		"groupless watch list":    {request.MethodGet, "/" + grouplessPrefix + "/" + grouplessGroupVersion.Version + "/watch/foo", http.StatusNotFound},
		"groupless watch object":  {request.MethodGet, "/" + grouplessPrefix + "/" + grouplessGroupVersion.Version + "/watch/foo/bar", http.StatusNotFound},
		"groupless proxy object":  {request.MethodGet, "/" + grouplessPrefix + "/" + grouplessGroupVersion.Version + "/proxy/foo/bar", http.StatusNotFound},

		"GET object":    {request.MethodGet, "/" + prefix + "/" + testGroupVersion.Group + "/" + testGroupVersion.Version + "/foo/bar", http.StatusNotFound},
		"GET list":      {request.MethodGet, "/" + prefix + "/" + testGroupVersion.Group + "/" + testGroupVersion.Version + "/foo", http.StatusNotFound},
		"POST list":     {request.MethodPost, "/" + prefix + "/" + testGroupVersion.Group + "/" + testGroupVersion.Version + "/foo", http.StatusNotFound},
		"PUT object":    {request.MethodPut, "/" + prefix + "/" + testGroupVersion.Group + "/" + testGroupVersion.Version + "/foo/bar", http.StatusNotFound},
		"DELETE object": {request.MethodDelete, "/" + prefix + "/" + testGroupVersion.Group + "/" + testGroupVersion.Version + "/foo/bar", http.StatusNotFound},
		"watch list":    {request.MethodGet, "/" + prefix + "/" + testGroupVersion.Group + "/" + testGroupVersion.Version + "/watch/foo", http.StatusNotFound},
		"watch object":  {request.MethodGet, "/" + prefix + "/" + testGroupVersion.Group + "/" + testGroupVersion.Version + "/watch/foo/bar", http.StatusNotFound},
		"proxy object":  {request.MethodGet, "/" + prefix + "/" + testGroupVersion.Group + "/" + testGroupVersion.Version + "/proxy/foo/bar", http.StatusNotFound},
	}
	handler := handle(map[string]rest.Storage{
		"foo": UnimplementedRESTStorage{},
	})
	server := httptest.NewServer(handler)
	defer server.Close()
	client := http.Client{}
	for testName, test := range cases {
		t.Run(testName, func(t *testing.T) {
			ctx := t.Context()
			request, err := http.NewRequestWithContext(ctx, test.Method, server.URL+test.Path, bytes.NewReader([]byte(`{"kind":"Simple","apiVersion":"version"}`)))
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			response, err := client.Do(request)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			defer apitesting.Close(t, response.Body)
			data, err := io.ReadAll(response.Body)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if response.StatusCode != test.ErrCode {
				t.Fatalf("expected %d for %s, Got %s", test.ErrCode, test.Method, string(data))
			}
		})
	}
}

// OnlyGetRESTStorage 只实现 Get/List，用于验证部分实现的存储仍会注册路由，
// 未实现的方法返回 405 而非 404。
type OnlyGetRESTStorage struct {
	UnimplementedRESTStorage
}

func (OnlyGetRESTStorage) Get(ctx context.Context, id string, options *metav1.GetOptions) (runtime.Object, error) {
	return nil, nil
}

func (OnlyGetRESTStorage) NewList() runtime.Object {
	return &genericapitesting.SimpleList{}
}

func (OnlyGetRESTStorage) List(ctx context.Context, options *metainternalversion.ListOptions) (runtime.Object, error) {
	return nil, nil
}

func (OnlyGetRESTStorage) ConvertToTable(ctx context.Context, object runtime.Object, tableOptions runtime.Object) (*metav1.Table, error) {
	return nil, nil
}

// TestSomeUnimplementedRESTStorage ensures that if a rest.Storage does
// not implement a given method, that it is literally not registered
// with the server. We need to have at least one verb supported inorder
// to get a MethodNotAllowed rather than NotFound error.
func TestSomeUnimplementedRESTStorage(t *testing.T) {
	type T struct {
		Method  string
		Path    string
		ErrCode int
	}

	cases := map[string]T{
		"groupless POST list":         {request.MethodPost, "/" + grouplessPrefix + "/" + grouplessGroupVersion.Version + "/namespaces/default/foo", http.StatusMethodNotAllowed},
		"groupless PUT object":        {request.MethodPut, "/" + grouplessPrefix + "/" + grouplessGroupVersion.Version + "/namespaces/default/foo/bar", http.StatusMethodNotAllowed},
		"groupless DELETE object":     {request.MethodDelete, "/" + grouplessPrefix + "/" + grouplessGroupVersion.Version + "/namespaces/default/foo/bar", http.StatusMethodNotAllowed},
		"groupless DELETE collection": {request.MethodDelete, "/" + grouplessPrefix + "/" + grouplessGroupVersion.Version + "/namespaces/default/foo", http.StatusMethodNotAllowed},
		"POST list":                   {request.MethodPost, "/" + prefix + "/" + testGroupVersion.Group + "/" + testGroupVersion.Version + "/namespaces/default/foo", http.StatusMethodNotAllowed},
		"PUT object":                  {request.MethodPut, "/" + prefix + "/" + testGroupVersion.Group + "/" + testGroupVersion.Version + "/namespaces/default/foo/bar", http.StatusMethodNotAllowed},
		"DELETE object":               {request.MethodDelete, "/" + prefix + "/" + testGroupVersion.Group + "/" + testGroupVersion.Version + "/namespaces/default/foo/bar", http.StatusMethodNotAllowed},
		"DELETE collection":           {request.MethodDelete, "/" + prefix + "/" + testGroupVersion.Group + "/" + testGroupVersion.Version + "/namespaces/default/foo", http.StatusMethodNotAllowed},
	}
	handler := handle(map[string]rest.Storage{
		"foo": OnlyGetRESTStorage{},
	})
	server := httptest.NewServer(handler)
	defer server.Close()
	client := http.Client{}
	for testName, test := range cases {
		t.Run(testName, func(t *testing.T) {
			ctx := t.Context()
			request, err := http.NewRequestWithContext(ctx, test.Method, server.URL+test.Path, bytes.NewReader([]byte(`{"kind":"Simple","apiVersion":"version"}`)))
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			response, err := client.Do(request)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			defer apitesting.Close(t, response.Body)
			data, err := io.ReadAll(response.Body)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if response.StatusCode != test.ErrCode {
				t.Fatalf("expected %d for %s, Got %s", test.ErrCode, test.Method, string(data))
			}
		})
	}
}

// TestList 验证 LIST 的命名空间来源（URL 路径优先，旧的 ?namespace 参数被忽略）
// 以及 label/field 选择器是否正确传给存储层。
func TestList(t *testing.T) {
	cases := []struct {
		url       string
		namespace string
		legacy    bool
		label     string
		field     string
	}{
		// Groupless API

		// legacy namespace param is ignored
		{
			url:       "/" + grouplessPrefix + "/" + grouplessGroupVersion.Version + "/simple?namespace=",
			namespace: "",
			legacy:    true,
		},
		{
			url:       "/" + grouplessPrefix + "/" + grouplessGroupVersion.Version + "/simple?namespace=other",
			namespace: "",
			legacy:    true,
		},
		{
			url:       "/" + grouplessPrefix + "/" + grouplessGroupVersion.Version + "/simple?namespace=other&labelSelector=a%3Db&fieldSelector=c%3Dd",
			namespace: "",
			legacy:    true,
			label:     "a=b",
			field:     "c=d",
		},
		// legacy api version is honored
		{
			url:       "/" + grouplessPrefix + "/" + grouplessGroupVersion.Version + "/simple",
			namespace: "",
			legacy:    true,
		},
		{
			url:       "/" + grouplessPrefix + "/" + grouplessGroupVersion.Version + "/namespaces/other/simple",
			namespace: "other",
			legacy:    true,
		},
		{
			url:       "/" + grouplessPrefix + "/" + grouplessGroupVersion.Version + "/namespaces/other/simple?labelSelector=a%3Db&fieldSelector=c%3Dd",
			namespace: "other",
			legacy:    true,
			label:     "a=b",
			field:     "c=d",
		},
		// list items across all namespaces
		{
			url:       "/" + grouplessPrefix + "/" + grouplessGroupVersion.Version + "/simple",
			namespace: "",
			legacy:    true,
		},
		// list items in a namespace in the path
		{
			url:       "/" + grouplessPrefix + "/" + grouplessGroupVersion.Version + "/namespaces/default/simple",
			namespace: "default",
		},
		{
			url:       "/" + grouplessPrefix + "/" + grouplessGroupVersion.Version + "/namespaces/other/simple",
			namespace: "other",
		},
		{
			url:       "/" + grouplessPrefix + "/" + grouplessGroupVersion.Version + "/namespaces/other/simple?labelSelector=a%3Db&fieldSelector=c%3Dd",
			namespace: "other",
			label:     "a=b",
			field:     "c=d",
		},
		// list items across all namespaces
		{
			url:       "/" + grouplessPrefix + "/" + grouplessGroupVersion.Version + "/simple",
			namespace: "",
		},

		// Group API

		// legacy namespace param is ignored
		{
			url:       "/" + prefix + "/" + testGroupVersion.Group + "/" + testGroupVersion.Version + "/simple?namespace=",
			namespace: "",
			legacy:    true,
		},
		{
			url:       "/" + prefix + "/" + testGroupVersion.Group + "/" + testGroupVersion.Version + "/simple?namespace=other",
			namespace: "",
			legacy:    true,
		},
		{
			url:       "/" + prefix + "/" + testGroupVersion.Group + "/" + testGroupVersion.Version + "/simple?namespace=other&labelSelector=a%3Db&fieldSelector=c%3Dd",
			namespace: "",
			legacy:    true,
			label:     "a=b",
			field:     "c=d",
		},
		// legacy api version is honored
		{
			url:       "/" + prefix + "/" + testGroupVersion.Group + "/" + testGroupVersion.Version + "/simple",
			namespace: "",
			legacy:    true,
		},
		{
			url:       "/" + prefix + "/" + testGroupVersion.Group + "/" + testGroupVersion.Version + "/namespaces/other/simple",
			namespace: "other",
			legacy:    true,
		},
		{
			url:       "/" + prefix + "/" + testGroupVersion.Group + "/" + testGroupVersion.Version + "/namespaces/other/simple?labelSelector=a%3Db&fieldSelector=c%3Dd",
			namespace: "other",
			legacy:    true,
			label:     "a=b",
			field:     "c=d",
		},
		// list items across all namespaces
		{
			url:       "/" + prefix + "/" + testGroupVersion.Group + "/" + testGroupVersion.Version + "/simple",
			namespace: "",
			legacy:    true,
		},
		// list items in a namespace in the path
		{
			url:       "/" + prefix + "/" + newGroupVersion.Group + "/" + newGroupVersion.Version + "/namespaces/default/simple",
			namespace: "default",
		},
		{
			url:       "/" + prefix + "/" + newGroupVersion.Group + "/" + newGroupVersion.Version + "/namespaces/other/simple",
			namespace: "other",
		},
		{
			url:       "/" + prefix + "/" + newGroupVersion.Group + "/" + newGroupVersion.Version + "/namespaces/other/simple?labelSelector=a%3Db&fieldSelector=c%3Dd",
			namespace: "other",
			label:     "a=b",
			field:     "c=d",
		},
		// list items across all namespaces
		{
			url:       "/" + prefix + "/" + newGroupVersion.Group + "/" + newGroupVersion.Version + "/simple",
			namespace: "",
		},
	}
	for testIndex, test := range cases {
		testName := fmt.Sprintf("%d", testIndex)
		t.Run(testName, func(t *testing.T) {
			ctx := t.Context()
			storage := map[string]rest.Storage{}
			simpleStorage := SimpleRESTStorage{expectedResourceNamespace: test.namespace}
			storage["simple"] = &simpleStorage
			var handler = handleInternal(storage, admissionControl, nil)
			server := httptest.NewServer(handler)
			defer server.Close()

			url := server.URL + test.url
			req, err := http.NewRequestWithContext(ctx, request.MethodGet, url, nil)
			require.NoError(t, err)
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			defer apitesting.Close(t, resp.Body)
			if resp.StatusCode != http.StatusOK {
				t.Errorf("unexpected status: %d from url %s, Expected: %d, %#v", resp.StatusCode, test.url, http.StatusOK, resp)
				body, err := io.ReadAll(resp.Body)
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				t.Logf("body: %s", string(body))
				return
			}
			if !simpleStorage.namespacePresent {
				t.Error("namespace not set")
			} else if simpleStorage.actualNamespace != test.namespace {
				t.Errorf("%q unexpected resource namespace: %s", test.url, simpleStorage.actualNamespace)
			}
			if simpleStorage.requestedLabelSelector == nil || simpleStorage.requestedLabelSelector.String() != test.label {
				t.Errorf("unexpected label selector: expected=%v got=%v", test.label, simpleStorage.requestedLabelSelector)
			}
			if simpleStorage.requestedFieldSelector == nil || simpleStorage.requestedFieldSelector.String() != test.field {
				t.Errorf("unexpected field selector: expected=%v got=%v", test.field, simpleStorage.requestedFieldSelector)
			}
		})
	}
}

// TestRequestsWithInvalidQuery 验证非法查询参数在进入存储前即返回 400。
func TestRequestsWithInvalidQuery(t *testing.T) {
	storage := map[string]rest.Storage{}

	storage["simple"] = &SimpleRESTStorage{expectedResourceNamespace: "default"}
	storage["withoptions"] = GetWithOptionsRESTStorage{}

	var handler = handleInternal(storage, admissionControl, nil)
	server := httptest.NewServer(handler)
	defer server.Close()

	for _, test := range []struct {
		postfix string
		method  string
	}{
		{"/simple?labelSelector=<invalid>", request.MethodGet},
		{"/simple/foo?gracePeriodSeconds=<invalid>", request.MethodDelete},
		// {"/simple?labelSelector=<value>", request.MethodDelete}, TODO: implement DeleteCollection in  SimpleRESTStorage
		// {"/simple/foo?export=<invalid>", request.MethodGet}, TODO: there is no invalid bool in conversion. Should we be more strict?
		// {"/simple/foo?resourceVersion=<invalid>", request.MethodGet}, TODO: there is no invalid resourceVersion. Should we be more strict?
		// {"/withoptions?labelSelector=<invalid>", request.MethodGet}, TODO: SimpleGetOptions is always valid. Add more validation that can fail.
	} {
		testName := fmt.Sprintf("%s:%s", test.method, test.postfix)
		t.Run(testName, func(t *testing.T) {
			ctx := t.Context()
			baseURL := server.URL + "/" + grouplessPrefix + "/" + grouplessGroupVersion.Version + "/namespaces/default"
			url := baseURL + test.postfix
			r, err := http.NewRequestWithContext(ctx, test.method, url, nil)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			resp, err := http.DefaultClient.Do(r)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			defer apitesting.Close(t, resp.Body)
			if resp.StatusCode != http.StatusBadRequest {
				t.Errorf("unexpected status: %d from url %s, Expected: %d, %#v", resp.StatusCode, url, http.StatusBadRequest, resp)
				body, err := io.ReadAll(resp.Body)
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				t.Logf("body: %s", string(body))
			}
		})
	}
}

// TestListCompression 验证 LIST 响应在客户端声明 gzip 时被压缩，且解压后内容正确。
func TestListCompression(t *testing.T) {
	cases := []struct {
		url            string
		namespace      string
		legacy         bool
		label          string
		field          string
		acceptEncoding string
	}{
		// list items in a namespace in the path
		{
			url:            "/" + grouplessPrefix + "/" + grouplessGroupVersion.Version + "/namespaces/default/simple",
			namespace:      "default",
			acceptEncoding: "",
		},
		{
			url:            "/" + grouplessPrefix + "/" + grouplessGroupVersion.Version + "/namespaces/default/simple",
			namespace:      "default",
			acceptEncoding: "gzip",
		},
	}
	for testIndex, test := range cases {
		t.Run(test.acceptEncoding, func(t *testing.T) {
			ctx := t.Context()
			storage := map[string]rest.Storage{}
			simpleStorage := SimpleRESTStorage{
				expectedResourceNamespace: test.namespace,
				list: []genericapitesting.Simple{
					{Other: strings.Repeat("0123456789abcdef", (128*1024/16)+1)},
				},
			}
			storage["simple"] = &simpleStorage
			var handler = handleInternal(storage, admissionControl, nil)

			handler = genericapifilters.WithRequestInfo(handler, newTestRequestInfoResolver())

			server := httptest.NewServer(handler)

			defer server.Close()

			req, err := http.NewRequestWithContext(ctx, request.MethodGet, server.URL+test.url, nil)
			if err != nil {
				t.Fatalf("%d: unexpected error: %v", testIndex, err)
			}
			// It's necessary to manually set Accept-Encoding here
			// to prevent http.DefaultClient from automatically
			// decoding responses
			req.Header.Set("Accept-Encoding", test.acceptEncoding)
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatalf("%d: unexpected error: %v", testIndex, err)
			}
			defer apitesting.Close(t, resp.Body)
			if resp.StatusCode != http.StatusOK {
				t.Errorf("unexpected status: %d from url %s, Expected: %d, %#v", resp.StatusCode, test.url, http.StatusOK, resp)
				body, err := io.ReadAll(resp.Body)
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				t.Logf("body: %s", string(body))
				return
			}
			if !simpleStorage.namespacePresent {
				t.Error("namespace not set")
			} else if simpleStorage.actualNamespace != test.namespace {
				t.Errorf("%q unexpected resource namespace: %s", test.url, simpleStorage.actualNamespace)
			}
			if simpleStorage.requestedLabelSelector == nil || simpleStorage.requestedLabelSelector.String() != test.label {
				t.Errorf("unexpected label selector: %v", simpleStorage.requestedLabelSelector)
			}
			if simpleStorage.requestedFieldSelector == nil || simpleStorage.requestedFieldSelector.String() != test.field {
				t.Errorf("unexpected field selector: %v", simpleStorage.requestedFieldSelector)
			}

			var decoder *json.Decoder
			if test.acceptEncoding == "gzip" {
				gzipReader, err := gzip.NewReader(resp.Body)
				if err != nil {
					t.Fatalf("unexpected error creating gzip reader: %v", err)
				}
				decoder = json.NewDecoder(gzipReader)
			} else {
				decoder = json.NewDecoder(resp.Body)
			}
			var itemOut genericapitesting.SimpleList
			err = decoder.Decode(&itemOut)
			if err != nil {
				t.Errorf("failed to read response body as SimpleList: %v", err)
			}
		})
	}
}

// TestLogs 对未注册的 /logs 路径发起请求，仅验证不会导致 panic（历史回归测试）。
func TestLogs(t *testing.T) {
	ctx := t.Context()
	handler := handle(map[string]rest.Storage{})
	server := httptest.NewServer(handler)
	defer server.Close()
	client := http.Client{}

	request, err := http.NewRequestWithContext(ctx, request.MethodGet, server.URL+"/logs", nil)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	response, err := client.Do(request)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	defer apitesting.Close(t, response.Body)

	body, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	t.Logf("Data: %s", string(body))
}

// TestErrorList 验证存储层返回错误时 LIST 映射为 500。
func TestErrorList(t *testing.T) {
	ctx := t.Context()
	storage := map[string]rest.Storage{}
	simpleStorage := SimpleRESTStorage{
		errors: map[string]error{"list": fmt.Errorf("test Error")},
	}
	storage["simple"] = &simpleStorage
	handler := handle(storage)
	server := httptest.NewServer(handler)
	defer server.Close()

	url := server.URL + "/" + prefix + "/" + testGroupVersion.Group + "/" + testGroupVersion.Version + "/simple"
	req, err := http.NewRequestWithContext(ctx, request.MethodGet, url, nil)
	require.NoError(t, err)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer apitesting.Close(t, resp.Body)

	if resp.StatusCode != http.StatusInternalServerError {
		t.Errorf("Unexpected status: %d, Expected: %d, %#v", resp.StatusCode, http.StatusInternalServerError, resp)
	}
}

// TestNonEmptyList 验证非空列表的序列化：条目数量与内容都应与存储一致。
func TestNonEmptyList(t *testing.T) {
	ctx := t.Context()
	storage := map[string]rest.Storage{}
	simpleStorage := SimpleRESTStorage{
		list: []genericapitesting.Simple{
			{
				ObjectMeta: metav1.ObjectMeta{Name: "something", Namespace: "other"},
				Other:      "foo",
			},
		},
	}
	storage["simple"] = &simpleStorage
	handler := handle(storage)
	server := httptest.NewServer(handler)
	defer server.Close()

	url := server.URL + "/" + prefix + "/" + testGroupVersion.Group + "/" + testGroupVersion.Version + "/simple"
	req, err := http.NewRequestWithContext(ctx, request.MethodGet, url, nil)
	require.NoError(t, err)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer apitesting.Close(t, resp.Body)

	if resp.StatusCode != http.StatusOK {
		t.Errorf("Unexpected status: %d, Expected: %d, %#v", resp.StatusCode, http.StatusOK, resp)
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		t.Logf("Data: %s", string(body))
	}

	var listOut genericapitesting.SimpleList
	body, err := extractBody(resp, &listOut)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	t.Log(body)

	if len(listOut.Items) != 1 {
		t.Errorf("Unexpected response: %#v", listOut)
		return
	}
	if listOut.Items[0].Other != simpleStorage.list[0].Other {
		t.Errorf("Unexpected data: %#v, %s", listOut.Items[0], string(body))
	}
}

// TestMetadata 验证 rest.StorageMetadata.ProducesMIMETypes 会影响注册路由的
// Produces 列表，确保自定义媒体类型被正确暴露。
func TestMetadata(t *testing.T) {
	simpleStorage := &MetadataRESTStorage{&SimpleRESTStorage{}, []string{"text/plain"}}
	h := handle(map[string]rest.Storage{"simple": simpleStorage})
	ws := h.(*defaultAPIServer).container.RegisteredWebServices()
	if len(ws) == 0 {
		t.Fatal("no web services registered")
	}
	matches := map[string]int{}
	for _, w := range ws {
		for _, r := range w.Routes() {
			t.Logf("%v %v %#v", r.Method, r.Path, r.Produces)
			s := strings.Join(r.Produces, ",")
			i := matches[s]
			matches[s] = i + 1
		}
	}
	cs := []func() bool{
		func() bool {
			return matches["text/plain,application/json,application/yaml,application/vnd.kubernetes.protobuf"] == 0
		},
		func() bool {
			return matches["application/json,application/yaml,application/vnd.kubernetes.protobuf,application/json;stream=watch,application/vnd.kubernetes.protobuf;stream=watch"] == 0
		},
		func() bool {
			return matches["application/json,application/yaml,application/vnd.kubernetes.protobuf"] == 0
		},
		func() bool {
			return len(matches) != 4
		},
	}
	for i, c := range cs {
		if c() {
			t.Errorf("[%d]unexpected mime types: %#v", i, matches)
		}
	}
}

// TestGet 验证单对象 GET 返回存储中的对象。
func TestGet(t *testing.T) {
	ctx := t.Context()
	storage := map[string]rest.Storage{}
	simpleStorage := SimpleRESTStorage{
		item: genericapitesting.Simple{
			Other: "foo",
		},
	}
	storage["simple"] = &simpleStorage
	handler := handle(storage)
	server := httptest.NewServer(handler)
	defer server.Close()

	url := server.URL + "/" + prefix + "/" + testGroupVersion.Group + "/" + testGroupVersion.Version + "/namespaces/default/simple/id"
	req, err := http.NewRequestWithContext(ctx, request.MethodGet, url, nil)
	require.NoError(t, err)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer apitesting.Close(t, resp.Body)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("unexpected response: %#v", resp)
	}
	var itemOut genericapitesting.Simple
	body, err := extractBody(resp, &itemOut)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	if itemOut.Name != simpleStorage.item.Name {
		t.Errorf("Unexpected data: %#v, expected %#v (%s)", itemOut, simpleStorage.item, string(body))
	}
}

// BenchmarkGet 测量 GET 热路径（含默认 gzip 协商）的开销。
func BenchmarkGet(b *testing.B) {
	ctx := b.Context()
	storage := map[string]rest.Storage{}
	simpleStorage := SimpleRESTStorage{
		item: genericapitesting.Simple{
			Other: "foo",
		},
	}
	storage["simple"] = &simpleStorage
	handler := handle(storage)
	server := httptest.NewServer(handler)
	defer server.Close()

	url := server.URL + "/" + prefix + "/" + testGroupVersion.Group + "/" + testGroupVersion.Version + "/namespaces/default/simple/id"

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Use a closure to execute defers before continuing
		func() {
			req, err := http.NewRequestWithContext(ctx, request.MethodGet, url, nil)
			require.NoError(b, err)
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				b.Fatalf("unexpected error: %v", err)
			}
			defer apitesting.Close(b, resp.Body)
			if resp.StatusCode != http.StatusOK {
				b.Fatalf("unexpected response: %#v", resp)
			}
			if _, err := io.Copy(io.Discard, resp.Body); err != nil {
				b.Fatalf("unable to read body")
			}
		}()
	}
	b.StopTimer()
}

// BenchmarkGetNoCompression 测量禁用压缩时的 GET 开销，用于对比压缩成本。
func BenchmarkGetNoCompression(b *testing.B) {
	ctx := b.Context()
	storage := map[string]rest.Storage{}
	simpleStorage := SimpleRESTStorage{
		item: genericapitesting.Simple{
			Other: "foo",
		},
	}
	storage["simple"] = &simpleStorage
	handler := handle(storage)
	server := httptest.NewServer(handler)
	defer server.Close()

	client := &http.Client{
		Transport: &http.Transport{
			DisableCompression: true,
		},
	}

	url := server.URL + "/" + prefix + "/" + testGroupVersion.Group + "/" + testGroupVersion.Version + "/namespaces/default/simple/id"

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Use a closure to execute defers before continuing
		func() {
			ctx, cancel := context.WithCancel(ctx)
			defer cancel()
			req, err := http.NewRequestWithContext(ctx, request.MethodGet, url, nil)
			if err != nil {
				b.Fatalf("unexpected error: %v", err)
			}
			resp, err := client.Do(req)
			if err != nil {
				b.Fatalf("unexpected error: %v", err)
			}
			defer apitesting.Close(b, resp.Body)
			if resp.StatusCode != http.StatusOK {
				b.Fatalf("unexpected response: %#v", resp)
			}
			if _, err := io.Copy(io.Discard, resp.Body); err != nil {
				b.Fatalf("unable to read body")
			}
		}()
	}
	b.StopTimer()
}

// TestGetCompression 验证 GET 在 Accept-Encoding: gzip 下返回可解压的压缩响应。
func TestGetCompression(t *testing.T) {
	storage := map[string]rest.Storage{}
	simpleStorage := SimpleRESTStorage{
		item: genericapitesting.Simple{
			Other: strings.Repeat("0123456789abcdef", (128*1024/16)+1),
		},
	}

	storage["simple"] = &simpleStorage
	handler := handle(storage)
	handler = genericapifilters.WithRequestInfo(handler, newTestRequestInfoResolver())
	server := httptest.NewServer(handler)
	defer server.Close()

	cases := []struct {
		acceptEncoding string
	}{
		{acceptEncoding: ""},
		{acceptEncoding: "gzip"},
	}

	for _, test := range cases {
		testName := fmt.Sprintf("Accept-Encoding:%s", test.acceptEncoding)
		t.Run(testName, func(t *testing.T) {
			ctx := t.Context()
			req, err := http.NewRequestWithContext(ctx, request.MethodGet, server.URL+"/"+prefix+"/"+testGroupVersion.Group+"/"+testGroupVersion.Version+"/namespaces/default/simple/id", nil)
			if err != nil {
				t.Fatalf("unexpected error creating request: %v", err)
			}
			// It's necessary to manually set Accept-Encoding here
			// to prevent http.DefaultClient from automatically
			// decoding responses
			req.Header.Set("Accept-Encoding", test.acceptEncoding)
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			defer apitesting.Close(t, resp.Body)
			if resp.StatusCode != http.StatusOK {
				t.Fatalf("unexpected response: %#v", resp)
			}
			var decoder *json.Decoder
			if test.acceptEncoding == "gzip" {
				gzipReader, err := gzip.NewReader(resp.Body)
				if err != nil {
					t.Fatalf("unexpected error creating gzip reader: %v", err)
				}
				decoder = json.NewDecoder(gzipReader)
			} else {
				decoder = json.NewDecoder(resp.Body)
			}
			var itemOut genericapitesting.Simple
			err = decoder.Decode(&itemOut)
			if err != nil {
				t.Errorf("unexpected error: %v", err)
			}
			body, err := io.ReadAll(resp.Body)
			if err != nil {
				t.Errorf("unexpected error reading body: %v", err)
			}

			if itemOut.Name != simpleStorage.item.Name {
				t.Errorf("Unexpected data: %#v, expected %#v (%s)", itemOut, simpleStorage.item, string(body))
			}
		})
	}
}

// TestGetPretty 验证 pretty 输出的触发条件：Accept 参数、?pretty 查询参数，
// 以及 curl/Wget/浏览器等 User-Agent。
func TestGetPretty(t *testing.T) {
	storage := map[string]rest.Storage{}
	simpleStorage := SimpleRESTStorage{
		item: genericapitesting.Simple{
			Other: "foo",
		},
	}
	storage["simple"] = &simpleStorage
	handler := handle(storage)
	server := httptest.NewServer(handler)
	defer server.Close()

	cases := []struct {
		accept    string
		userAgent string
		params    url.Values
		pretty    bool
	}{
		{accept: runtime.ContentTypeJSON},
		{accept: "application/json;pretty=0"},
		{accept: runtime.ContentTypeJSON, userAgent: "kubectl"},
		{accept: runtime.ContentTypeJSON, params: url.Values{"pretty": {"0"}}},

		{pretty: true, accept: runtime.ContentTypeJSON, userAgent: "curl"},
		{pretty: true, accept: runtime.ContentTypeJSON, userAgent: "Mozilla/5.0"},
		{pretty: true, accept: runtime.ContentTypeJSON, userAgent: "Wget"},
		{pretty: true, accept: "application/json;pretty=1"},
		{pretty: true, accept: runtime.ContentTypeJSON, params: url.Values{"pretty": {"1"}}},
		{pretty: true, accept: runtime.ContentTypeJSON, params: url.Values{"pretty": {"true"}}},
	}
	for _, test := range cases {
		testName := fmt.Sprintf("Accept:%s", test.accept)
		if test.userAgent != "" {
			testName += fmt.Sprintf(" User-Agent:%s", test.userAgent)
		}
		if len(test.params) > 0 {
			testName += fmt.Sprintf(" Params:%s", test.params.Encode())
		}
		t.Run(testName, func(t *testing.T) {
			u, err := url.Parse(server.URL + "/" + prefix + "/" + testGroupVersion.Group + "/" + testGroupVersion.Version + "/namespaces/default/simple/id")
			if err != nil {
				t.Fatal(err)
			}
			u.RawQuery = test.params.Encode()
			req := &http.Request{Method: request.MethodGet, URL: u}
			req.Header = http.Header{}
			req.Header.Set("Accept", test.accept)
			req.Header.Set("User-Agent", test.userAgent)
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatal(err)
			}
			if resp.StatusCode != http.StatusOK {
				t.Fatal(err)
			}
			var itemOut genericapitesting.Simple
			body, err := extractBody(resp, &itemOut)
			if err != nil {
				t.Fatal(err)
			}
			// to get stable ordering we need to use a go type
			unstructured := genericapitesting.Simple{}
			if err := json.Unmarshal([]byte(body), &unstructured); err != nil {
				t.Fatal(err)
			}
			var expect string
			if test.pretty {
				out, err := json.MarshalIndent(unstructured, "", "  ")
				if err != nil {
					t.Fatal(err)
				}
				expect = string(out)
			} else {
				out, err := json.Marshal(unstructured)
				if err != nil {
					t.Fatal(err)
				}
				expect = string(out) + "\n"
			}
			if expect != body {
				t.Errorf("body did not match expected:\n%s\n%s", body, expect)
			}
		})
	}
}

// TestGetTable 验证 Table 输出协商：不支持的版本返回 406，支持的版本返回
// 正确的列定义、单元格与内嵌对象。
func TestGetTable(t *testing.T) {
	now := metav1.Now()
	obj := genericapitesting.Simple{
		ObjectMeta: metav1.ObjectMeta{Name: "foo1", Namespace: "ns1", ResourceVersion: "10", CreationTimestamp: now, UID: types.UID("abcdef0123")},
		Other:      "foo",
	}

	m, err := meta.Accessor(&obj)
	if err != nil {
		t.Fatal(err)
	}
	var encodedV1Beta1Body []byte
	{
		partial := meta.AsPartialObjectMetadata(m)
		partial.GetObjectKind().SetGroupVersionKind(metav1beta1.SchemeGroupVersion.WithKind("PartialObjectMetadata"))
		encodedBody, err := runtime.Encode(metainternalversionscheme.Codecs.LegacyCodec(metav1beta1.SchemeGroupVersion), partial)
		if err != nil {
			t.Fatal(err)
		}
		// the codec includes a trailing newline that is not present during decode
		encodedV1Beta1Body = bytes.TrimSpace(encodedBody)
	}
	var encodedV1Body []byte
	{
		partial := meta.AsPartialObjectMetadata(m)
		partial.GetObjectKind().SetGroupVersionKind(metav1.SchemeGroupVersion.WithKind("PartialObjectMetadata"))
		encodedBody, err := runtime.Encode(metainternalversionscheme.Codecs.LegacyCodec(metav1.SchemeGroupVersion), partial)
		if err != nil {
			t.Fatal(err)
		}
		// the codec includes a trailing newline that is not present during decode
		encodedV1Body = bytes.TrimSpace(encodedBody)
	}

	metaDoc := metav1.ObjectMeta{}.SwaggerDoc()

	cases := []struct {
		accept     string
		params     url.Values
		pretty     bool
		expected   *metav1.Table
		statusCode int
		item       bool
	}{
		{
			accept:     "application/json;as=Table;v=v1alpha1;g=meta.k8s.io",
			statusCode: http.StatusNotAcceptable,
		},
		{
			accept:     runtime.ContentTypeProtobuf + ";as=Table;v=v1beta1;g=meta.k8s.io",
			statusCode: http.StatusNotAcceptable,
		},
		{
			accept:     runtime.ContentTypeProtobuf + ";as=Table;v=v1;g=meta.k8s.io",
			statusCode: http.StatusNotAcceptable,
		},

		{
			item:   true,
			accept: "application/json;as=Table;v=v1;g=meta.k8s.io",
			expected: &metav1.Table{
				TypeMeta: metav1.TypeMeta{Kind: "Table", APIVersion: "meta.k8s.io/v1"},
				ListMeta: metav1.ListMeta{ResourceVersion: "10"},
				ColumnDefinitions: []metav1.TableColumnDefinition{
					{Name: "Name", Type: "string", Format: "name", Description: metaDoc["name"]},
					{Name: "Created At", Type: "date", Description: metaDoc["creationTimestamp"]},
				},
				Rows: []metav1.TableRow{
					{Cells: []interface{}{"foo1", now.Time.UTC().Format(time.RFC3339)}, Object: runtime.RawExtension{Raw: encodedV1Body}},
				},
			},
		},
		{
			item:   true,
			accept: "application/json;as=Table;v=v1beta1;g=meta.k8s.io",
			expected: &metav1.Table{
				TypeMeta: metav1.TypeMeta{Kind: "Table", APIVersion: "meta.k8s.io/v1beta1"},
				ListMeta: metav1.ListMeta{ResourceVersion: "10"},
				ColumnDefinitions: []metav1.TableColumnDefinition{
					{Name: "Name", Type: "string", Format: "name", Description: metaDoc["name"]},
					{Name: "Created At", Type: "date", Description: metaDoc["creationTimestamp"]},
				},
				Rows: []metav1.TableRow{
					{Cells: []interface{}{"foo1", now.Time.UTC().Format(time.RFC3339)}, Object: runtime.RawExtension{Raw: encodedV1Beta1Body}},
				},
			},
		},
		{
			item: true,
			accept: strings.Join([]string{
				runtime.ContentTypeProtobuf + ";as=Table;v=v1beta1;g=meta.k8s.io",
				"application/json;as=Table;v=v1beta1;g=meta.k8s.io",
			}, ","),
			expected: &metav1.Table{
				TypeMeta: metav1.TypeMeta{Kind: "Table", APIVersion: "meta.k8s.io/v1beta1"},
				ListMeta: metav1.ListMeta{ResourceVersion: "10"},
				ColumnDefinitions: []metav1.TableColumnDefinition{
					{Name: "Name", Type: "string", Format: "name", Description: metaDoc["name"]},
					{Name: "Created At", Type: "date", Description: metaDoc["creationTimestamp"]},
				},
				Rows: []metav1.TableRow{
					{Cells: []interface{}{"foo1", now.Time.UTC().Format(time.RFC3339)}, Object: runtime.RawExtension{Raw: encodedV1Beta1Body}},
				},
			},
		},
		{
			item:   true,
			accept: "application/json;as=Table;v=v1beta1;g=meta.k8s.io",
			params: url.Values{"includeObject": []string{"Metadata"}},
			expected: &metav1.Table{
				TypeMeta: metav1.TypeMeta{Kind: "Table", APIVersion: "meta.k8s.io/v1beta1"},
				ListMeta: metav1.ListMeta{ResourceVersion: "10"},
				ColumnDefinitions: []metav1.TableColumnDefinition{
					{Name: "Name", Type: "string", Format: "name", Description: metaDoc["name"]},
					{Name: "Created At", Type: "date", Description: metaDoc["creationTimestamp"]},
				},
				Rows: []metav1.TableRow{
					{Cells: []interface{}{"foo1", now.Time.UTC().Format(time.RFC3339)}, Object: runtime.RawExtension{Raw: encodedV1Beta1Body}},
				},
			},
		},
		{
			accept: "application/json;as=Table;v=v1beta1;g=meta.k8s.io",
			params: url.Values{"includeObject": []string{"Metadata"}},
			expected: &metav1.Table{
				TypeMeta: metav1.TypeMeta{Kind: "Table", APIVersion: "meta.k8s.io/v1beta1"},
				ListMeta: metav1.ListMeta{ResourceVersion: "10"},
				ColumnDefinitions: []metav1.TableColumnDefinition{
					{Name: "Name", Type: "string", Format: "name", Description: metaDoc["name"]},
					{Name: "Created At", Type: "date", Description: metaDoc["creationTimestamp"]},
				},
				Rows: []metav1.TableRow{
					{Cells: []interface{}{"foo1", now.Time.UTC().Format(time.RFC3339)}, Object: runtime.RawExtension{Raw: encodedV1Beta1Body}},
				},
			},
		},
	}
	for testIndex, test := range cases {
		testName := fmt.Sprintf("%d", testIndex)
		t.Run(testName, func(t *testing.T) {
			storage := map[string]rest.Storage{}
			simpleStorage := SimpleRESTStorage{
				item: obj,
				list: []genericapitesting.Simple{obj},
			}
			storage["simple"] = &simpleStorage
			handler := handle(storage)
			server := httptest.NewServer(handler)
			defer server.Close()

			var id string
			if test.item {
				id = "/id"
			}
			u, err := url.Parse(server.URL + "/" + prefix + "/" + testGroupVersion.Group + "/" + testGroupVersion.Version + "/namespaces/default/simple" + id)
			if err != nil {
				t.Fatal(err)
			}
			u.RawQuery = test.params.Encode()
			req := &http.Request{Method: request.MethodGet, URL: u}
			req.Header = http.Header{}
			req.Header.Set("Accept", test.accept)
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatal(err)
			}
			if test.statusCode != 0 {
				if resp.StatusCode != test.statusCode {
					t.Errorf("unexpected response: %#v", resp)
				}
				obj, _, err := extractBodyObject(resp, unstructured.UnstructuredJSONScheme)
				if err != nil {
					t.Fatalf("unexpected body read error: %v", err)
				}
				gvk := schema.GroupVersionKind{Version: "v1", Kind: "Status"}
				if obj.GetObjectKind().GroupVersionKind() != gvk {
					t.Fatalf("unexpected error body: %#v", obj)
				}
				return
			}
			if resp.StatusCode != http.StatusOK {
				t.Errorf("unexpected response: %#v", resp)
			}
			var itemOut metav1.Table
			body, err := extractBody(resp, &itemOut)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(test.expected, &itemOut) {
				t.Log(body)
				t.Errorf("did not match: %s", cmp.Diff(test.expected, &itemOut))
			}
		})
	}
}

// TestWatchTable 验证 watch 事件也能按 Table 形式编码，且事件序列与预期一致。
func TestWatchTable(t *testing.T) {
	obj := genericapitesting.Simple{
		ObjectMeta: metav1.ObjectMeta{Name: "foo1", Namespace: "ns1", ResourceVersion: "10", CreationTimestamp: metav1.NewTime(time.Unix(1, 0)), UID: types.UID("abcdef0123")},
		Other:      "foo",
	}

	m, err := meta.Accessor(&obj)
	if err != nil {
		t.Fatal(err)
	}
	partial := meta.AsPartialObjectMetadata(m)
	partial.GetObjectKind().SetGroupVersionKind(metav1beta1.SchemeGroupVersion.WithKind("PartialObjectMetadata"))
	encodedBody, err := runtime.Encode(metainternalversionscheme.Codecs.LegacyCodec(metav1beta1.SchemeGroupVersion), partial)
	if err != nil {
		t.Fatal(err)
	}
	// the codec includes a trailing newline that is not present during decode
	encodedBody = bytes.TrimSpace(encodedBody)

	encodedBodyV1, err := runtime.Encode(metainternalversionscheme.Codecs.LegacyCodec(metav1.SchemeGroupVersion), partial)
	if err != nil {
		t.Fatal(err)
	}
	// the codec includes a trailing newline that is not present during decode
	encodedBodyV1 = bytes.TrimSpace(encodedBodyV1)

	metaDoc := metav1.ObjectMeta{}.SwaggerDoc()

	s := metainternalversionscheme.Codecs.SupportedMediaTypes()[0].Serializer

	cases := []struct {
		accept string
		params url.Values
		send   func(w *watch.FakeWatcher)

		expected    []*metav1.WatchEvent
		contentType string
		statusCode  int
		item        bool
	}{
		{
			accept:     "application/json;as=Table;v=v1alpha1;g=meta.k8s.io",
			statusCode: http.StatusNotAcceptable,
		},
		{
			accept: "application/json;as=Table;v=v1beta1;g=meta.k8s.io",
			send: func(w *watch.FakeWatcher) {
				w.Add(&obj)
			},
			expected: []*metav1.WatchEvent{
				{
					Type: "ADDED",
					Object: runtime.RawExtension{
						Raw: []byte(strings.TrimSpace(runtime.EncodeOrDie(s, &metav1.Table{
							TypeMeta: metav1.TypeMeta{Kind: "Table", APIVersion: "meta.k8s.io/v1beta1"},
							ListMeta: metav1.ListMeta{ResourceVersion: "10"},
							ColumnDefinitions: []metav1.TableColumnDefinition{
								{Name: "Name", Type: "string", Format: "name", Description: metaDoc["name"]},
								{Name: "Created At", Type: "date", Description: metaDoc["creationTimestamp"]},
							},
							Rows: []metav1.TableRow{
								{Cells: []interface{}{"foo1", time.Unix(1, 0).UTC().Format(time.RFC3339)}, Object: runtime.RawExtension{Raw: encodedBody}},
							},
						}))),
					},
				},
			},
		},
		{
			accept: "application/json;as=Table;v=v1beta1;g=meta.k8s.io",
			send: func(w *watch.FakeWatcher) {
				w.Add(&obj)
				w.Modify(&obj)
			},
			expected: []*metav1.WatchEvent{
				{
					Type: "ADDED",
					Object: runtime.RawExtension{
						Raw: []byte(strings.TrimSpace(runtime.EncodeOrDie(s, &metav1.Table{
							TypeMeta: metav1.TypeMeta{Kind: "Table", APIVersion: "meta.k8s.io/v1beta1"},
							ListMeta: metav1.ListMeta{ResourceVersion: "10"},
							ColumnDefinitions: []metav1.TableColumnDefinition{
								{Name: "Name", Type: "string", Format: "name", Description: metaDoc["name"]},
								{Name: "Created At", Type: "date", Description: metaDoc["creationTimestamp"]},
							},
							Rows: []metav1.TableRow{
								{Cells: []interface{}{"foo1", time.Unix(1, 0).UTC().Format(time.RFC3339)}, Object: runtime.RawExtension{Raw: encodedBody}},
							},
						}))),
					},
				},
				{
					Type: "MODIFIED",
					Object: runtime.RawExtension{
						Raw: []byte(strings.TrimSpace(runtime.EncodeOrDie(s, &metav1.Table{
							TypeMeta: metav1.TypeMeta{Kind: "Table", APIVersion: "meta.k8s.io/v1beta1"},
							ListMeta: metav1.ListMeta{ResourceVersion: "10"},
							Rows: []metav1.TableRow{
								{Cells: []interface{}{"foo1", time.Unix(1, 0).UTC().Format(time.RFC3339)}, Object: runtime.RawExtension{Raw: encodedBody}},
							},
						}))),
					},
				},
			},
		},
		{
			accept: "application/json;as=Table;v=v1;g=meta.k8s.io",
			send: func(w *watch.FakeWatcher) {
				w.Add(&obj)
				w.Modify(&obj)
			},
			expected: []*metav1.WatchEvent{
				{
					Type: "ADDED",
					Object: runtime.RawExtension{
						Raw: []byte(strings.TrimSpace(runtime.EncodeOrDie(s, &metav1.Table{
							TypeMeta: metav1.TypeMeta{Kind: "Table", APIVersion: "meta.k8s.io/v1"},
							ListMeta: metav1.ListMeta{ResourceVersion: "10"},
							ColumnDefinitions: []metav1.TableColumnDefinition{
								{Name: "Name", Type: "string", Format: "name", Description: metaDoc["name"]},
								{Name: "Created At", Type: "date", Description: metaDoc["creationTimestamp"]},
							},
							Rows: []metav1.TableRow{
								{Cells: []interface{}{"foo1", time.Unix(1, 0).UTC().Format(time.RFC3339)}, Object: runtime.RawExtension{Raw: encodedBodyV1}},
							},
						}))),
					},
				},
				{
					Type: "MODIFIED",
					Object: runtime.RawExtension{
						Raw: []byte(strings.TrimSpace(runtime.EncodeOrDie(s, &metav1.Table{
							TypeMeta: metav1.TypeMeta{Kind: "Table", APIVersion: "meta.k8s.io/v1"},
							ListMeta: metav1.ListMeta{ResourceVersion: "10"},
							Rows: []metav1.TableRow{
								{Cells: []interface{}{"foo1", time.Unix(1, 0).UTC().Format(time.RFC3339)}, Object: runtime.RawExtension{Raw: encodedBodyV1}},
							},
						}))),
					},
				},
			},
		},
	}
	for testIndex, test := range cases {
		testName := fmt.Sprintf("%d", testIndex)
		t.Run(testName, func(t *testing.T) {
			storage := map[string]rest.Storage{}
			simpleStorage := SimpleRESTStorage{
				item: obj,
				list: []genericapitesting.Simple{obj},
			}

			storage["simple"] = &simpleStorage
			handler := handle(storage)
			server := httptest.NewServer(handler)
			defer server.Close()

			var id string
			if test.item {
				id = "/id"
			}
			u, err := url.Parse(server.URL + "/" + prefix + "/" + testGroupVersion.Group + "/" + testGroupVersion.Version + "/namespaces/default/simple")
			if err != nil {
				t.Fatal(err)
			}
			if test.params == nil {
				test.params = url.Values{}
			}
			if test.item {
				test.params["fieldSelector"] = []string{fmt.Sprintf("metadata.name=%s", id)}
			}
			test.params["watch"] = []string{"1"}

			u.RawQuery = test.params.Encode()
			req := &http.Request{Method: request.MethodGet, URL: u}
			req.Header = http.Header{}
			req.Header.Set("Accept", test.accept)
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatal(err)
			}
			defer apitesting.Close(t, resp.Body)
			if test.statusCode != 0 {
				if resp.StatusCode != test.statusCode {
					t.Fatalf("unexpected response: %#v", resp)
				}
				obj, _, err := extractBodyObject(resp, unstructured.UnstructuredJSONScheme)
				if err != nil {
					t.Fatalf("unexpected body read error: %v", err)
				}
				gvk := schema.GroupVersionKind{Version: "v1", Kind: "Status"}
				if obj.GetObjectKind().GroupVersionKind() != gvk {
					t.Fatalf("unexpected error body: %#v", obj)
				}
				return
			}
			if resp.StatusCode != http.StatusOK {
				t.Fatalf("unexpected response: %#v", resp)
			}

			go func() {
				defer simpleStorage.fakeWatch.Stop()
				test.send(simpleStorage.fakeWatch)
			}()

			body, err := io.ReadAll(resp.Body)
			if err != nil {
				t.Fatal(err)
			}
			t.Logf("Body:\n%s", string(body))
			d := watcher(resp.Header.Get("Content-Type"), io.NopCloser(bytes.NewReader(body)))
			var actual []*metav1.WatchEvent
			for {
				var event metav1.WatchEvent
				_, _, err := d.Decode(nil, &event)
				if err == io.EOF {
					break
				}
				if err != nil {
					t.Fatal(err)
				}
				actual = append(actual, &event)
			}
			if !reflect.DeepEqual(test.expected, actual) {
				t.Fatalf("unexpected: %s", cmp.Diff(test.expected, actual))
			}
		})
	}
}

// watcher 根据响应的 Content-Type 构造流式解码器，用于解析 watch 事件流。
func watcher(mediaType string, r io.ReadCloser) streaming.Decoder {
	info, ok := runtime.SerializerInfoForMediaType(metainternalversionscheme.Codecs.SupportedMediaTypes(), mediaType)
	if !ok || info.StreamSerializer == nil {
		panic(info)
	}
	streamSerializer := info.StreamSerializer
	fr := streamSerializer.Framer.NewFrameReader(r)
	d := streaming.NewDecoder(fr, streamSerializer.Serializer)
	return d
}

// TestGetPartialObjectMetadata 验证 PartialObjectMetadata 的协商与降级：
// 不支持的版本返回 406，支持时返回只含元数据的对象，客户端偏好顺序决定版本。
func TestGetPartialObjectMetadata(t *testing.T) {
	now := metav1.Time{Time: metav1.Now().Rfc3339Copy().Local()}
	storage := map[string]rest.Storage{}
	simpleStorage := SimpleRESTStorage{
		item: genericapitesting.Simple{
			ObjectMeta: metav1.ObjectMeta{Name: "foo1", Namespace: "ns1", CreationTimestamp: now, UID: types.UID("abcdef0123")},
			Other:      "foo",
		},
		list: []genericapitesting.Simple{
			{
				ObjectMeta: metav1.ObjectMeta{Name: "foo1", Namespace: "ns1", CreationTimestamp: now, UID: types.UID("newer")},
				Other:      "foo",
			},
			{
				ObjectMeta: metav1.ObjectMeta{Name: "foo2", Namespace: "ns2", CreationTimestamp: now, UID: types.UID("older")},
				Other:      "bar",
			},
		},
	}
	storage["simple"] = &simpleStorage
	handler := handle(storage)
	server := httptest.NewServer(handler)
	defer server.Close()

	cases := []struct {
		accept     string
		params     url.Values
		pretty     bool
		list       bool
		expected   runtime.Object
		expectKind schema.GroupVersionKind
		statusCode int
	}{
		{
			accept:     "application/json;as=PartialObjectMetadata;v=v1alpha1;g=meta.k8s.io",
			statusCode: http.StatusNotAcceptable,
		},
		{
			accept:     "application/json;as=PartialObjectMetadata;v=v1alpha1;g=meta.k8s.io, application/json",
			expectKind: schema.GroupVersionKind{Kind: "Simple", Group: testGroupVersion.Group, Version: testGroupVersion.Version},
		},
		{
			accept:     "application/json;as=PartialObjectMetadata;v=v1beta1;g=meta.k8s.io, application/json",
			expectKind: schema.GroupVersionKind{Kind: "PartialObjectMetadata", Group: "meta.k8s.io", Version: "v1beta1"},
		},
		{
			list:       true,
			accept:     "application/json;as=PartialObjectMetadata;v=v1beta1;g=meta.k8s.io",
			statusCode: http.StatusNotAcceptable,
		},

		// verify preferred version overrides supported version
		{
			accept:     "application/json;as=PartialObjectMetadata;v=v1beta1;g=meta.k8s.io, application/json;as=PartialObjectMetadata;v=v1;g=meta.k8s.io, application/json",
			expectKind: schema.GroupVersionKind{Kind: "PartialObjectMetadata", Group: "meta.k8s.io", Version: "v1beta1"},
		},
		{
			accept:     "application/json;as=PartialObjectMetadata;v=v1;g=meta.k8s.io, application/json;as=PartialObjectMetadata;v=v1beta1;g=meta.k8s.io, application/json",
			expectKind: schema.GroupVersionKind{Kind: "PartialObjectMetadata", Group: "meta.k8s.io", Version: "v1"},
		},
		{
			accept:     "application/json;as=PartialObjectMetadata;v=v1beta1;g=meta.k8s.io, application/json;as=PartialObjectMetadata;v=v1;g=meta.k8s.io",
			expectKind: schema.GroupVersionKind{Kind: "PartialObjectMetadata", Group: "meta.k8s.io", Version: "v1beta1"},
		},
		{
			accept:     "application/json;as=PartialObjectMetadata;v=v1;g=meta.k8s.io, application/json;as=PartialObjectMetadata;v=v1beta1;g=meta.k8s.io",
			expectKind: schema.GroupVersionKind{Kind: "PartialObjectMetadata", Group: "meta.k8s.io", Version: "v1"},
		},

		{
			list:       true,
			accept:     "application/json;as=PartialObjectMetadata;v=v1alpha1;g=meta.k8s.io, application/json",
			expectKind: schema.GroupVersionKind{Kind: "SimpleList", Group: testGroupVersion.Group, Version: testGroupVersion.Version},
		},
		{
			list:       true,
			accept:     "application/json;as=PartialObjectMetadataList;v=v1beta1;g=meta.k8s.io, application/json",
			expectKind: schema.GroupVersionKind{Kind: "PartialObjectMetadataList", Group: "meta.k8s.io", Version: "v1beta1"},
		},
		{
			accept:     "application/json;as=PartialObjectMetadataList;v=v1beta1;g=meta.k8s.io",
			statusCode: http.StatusNotAcceptable,
		},
		{
			accept: "application/json;as=PartialObjectMetadata;v=v1beta1;g=meta.k8s.io",
			expected: &metav1beta1.PartialObjectMetadata{
				ObjectMeta: metav1.ObjectMeta{Name: "foo1", Namespace: "ns1", CreationTimestamp: now, UID: types.UID("abcdef0123")},
			},
			expectKind: schema.GroupVersionKind{Kind: "PartialObjectMetadata", Group: "meta.k8s.io", Version: "v1beta1"},
		},
		{
			accept: "application/json;as=PartialObjectMetadata;v=v1;g=meta.k8s.io",
			expected: &metav1.PartialObjectMetadata{
				ObjectMeta: metav1.ObjectMeta{Name: "foo1", Namespace: "ns1", CreationTimestamp: now, UID: types.UID("abcdef0123")},
			},
			expectKind: schema.GroupVersionKind{Kind: "PartialObjectMetadata", Group: "meta.k8s.io", Version: "v1"},
		},
		{
			list:   true,
			accept: "application/json;as=PartialObjectMetadataList;v=v1beta1;g=meta.k8s.io",
			expected: &metav1beta1.PartialObjectMetadataList{
				ListMeta: metav1.ListMeta{
					ResourceVersion: "10",
				},
				Items: []metav1beta1.PartialObjectMetadata{
					{
						TypeMeta:   metav1.TypeMeta{APIVersion: "meta.k8s.io/v1beta1", Kind: "PartialObjectMetadata"},
						ObjectMeta: metav1.ObjectMeta{Name: "foo1", Namespace: "ns1", CreationTimestamp: now, UID: types.UID("newer")},
					},
					{
						TypeMeta:   metav1.TypeMeta{APIVersion: "meta.k8s.io/v1beta1", Kind: "PartialObjectMetadata"},
						ObjectMeta: metav1.ObjectMeta{Name: "foo2", Namespace: "ns2", CreationTimestamp: now, UID: types.UID("older")},
					},
				},
			},
			expectKind: schema.GroupVersionKind{Kind: "PartialObjectMetadataList", Group: "meta.k8s.io", Version: "v1beta1"},
		},
	}
	for testIndex, test := range cases {
		testName := fmt.Sprintf("%d", testIndex)
		t.Run(testName, func(t *testing.T) {
			suffix := "/namespaces/default/simple/id"
			if test.list {
				suffix = "/namespaces/default/simple"
			}
			u, err := url.Parse(server.URL + "/" + prefix + "/" + testGroupVersion.Group + "/" + testGroupVersion.Version + suffix)
			if err != nil {
				t.Fatal(err)
			}
			u.RawQuery = test.params.Encode()
			req := &http.Request{Method: request.MethodGet, URL: u}
			req.Header = http.Header{}
			req.Header.Set("Accept", test.accept)
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatal(err)
			}
			defer apitesting.Close(t, resp.Body)
			if test.statusCode != 0 {
				if resp.StatusCode != test.statusCode {
					t.Errorf("unexpected response: %#v", resp)
				}
				obj, _, err := extractBodyObject(resp, unstructured.UnstructuredJSONScheme)
				if err != nil {
					t.Fatalf("unexpected body read error: %v", err)
				}
				gvk := schema.GroupVersionKind{Version: "v1", Kind: "Status"}
				if obj.GetObjectKind().GroupVersionKind() != gvk {
					t.Errorf("unexpected error body: %#v", obj)
				}
				return
			}
			if resp.StatusCode != http.StatusOK {
				t.Fatalf("invalid status: %#v\n%s", resp, bodyOrDie(resp))
			}
			body := ""
			if test.expected != nil {
				itemOut, d, err := extractBodyObject(resp, metainternalversionscheme.Codecs.LegacyCodec(metav1beta1.SchemeGroupVersion))
				if err != nil {
					t.Fatal(err)
				}
				if !reflect.DeepEqual(test.expected, itemOut) {
					t.Errorf("did not match: %s", cmp.Diff(test.expected, itemOut))
				}
				body = d
			} else {
				d, err := io.ReadAll(resp.Body)
				if err != nil {
					t.Fatal(err)
				}
				body = string(d)
			}
			obj := &unstructured.Unstructured{}
			if err := json.Unmarshal([]byte(body), obj); err != nil {
				t.Fatal(err)
			}
			if obj.GetObjectKind().GroupVersionKind() != test.expectKind {
				t.Errorf("unexpected kind: %#v", obj.GetObjectKind().GroupVersionKind())
			}
		})
	}
}

// TestGetBinary 验证流式（非结构化）GET：响应体原样透传，Content-Type 来自
// Streamer 而非内容协商。
func TestGetBinary(t *testing.T) {
	ctx := t.Context()
	simpleStorage := SimpleRESTStorage{
		stream: &SimpleStream{
			contentType: "text/plain",
			Reader:      bytes.NewBufferString("response data"),
		},
	}
	stream := simpleStorage.stream
	server := httptest.NewServer(handle(map[string]rest.Storage{"simple": &simpleStorage}))
	defer server.Close()

	req, err := http.NewRequestWithContext(ctx, request.MethodGet, server.URL+"/"+prefix+"/"+testGroupVersion.Group+"/"+testGroupVersion.Version+"/namespaces/default/simple/binary", nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	req.Header.Add("Accept", "text/other, */*")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer apitesting.Close(t, resp.Body)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("unexpected response: %#v", resp)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if !stream.closed || stream.version != testGroupVersion.String() || stream.accept != "text/other, */*" ||
		resp.Header.Get("Content-Type") != stream.contentType || string(body) != "response data" {
		t.Errorf("unexpected stream: %#v", stream)
	}
}

// validateSimpleGetOptionsParams 校验 SimpleGetOptions 生成的参数文档与预期一致。
func validateSimpleGetOptionsParams(t *testing.T, route *restful.Route) {
	// Validate name and description
	expectedParams := map[string]string{
		"param1":  "description for param1",
		"param2":  "description for param2",
		"atAPath": "",
	}
	for _, p := range route.ParameterDocs {
		data := p.Data()
		if desc, exists := expectedParams[data.Name]; exists {
			if desc != data.Description {
				t.Errorf("unexpected description for parameter %s: %s\n", data.Name, data.Description)
			}
			delete(expectedParams, data.Name)
		}
	}
	if len(expectedParams) > 0 {
		t.Errorf("did not find all expected parameters: %#v", expectedParams)
	}
}

// TestGetWithOptionsRouteParams 验证 GET 路由暴露了 options 的参数文档。
func TestGetWithOptionsRouteParams(t *testing.T) {
	storage := map[string]rest.Storage{}
	simpleStorage := GetWithOptionsRESTStorage{
		SimpleRESTStorage: &SimpleRESTStorage{},
	}
	storage["simple"] = &simpleStorage
	handler := handle(storage)
	ws := handler.(*defaultAPIServer).container.RegisteredWebServices()
	if len(ws) == 0 {
		t.Fatal("no web services registered")
	}
	routes := ws[0].Routes()
	for i := range routes {
		if routes[i].Method == request.MethodGet && routes[i].Operation == "readNamespacedSimple" {
			validateSimpleGetOptionsParams(t, &routes[i])
			break
		}
	}
}

// TestGetWithOptions 验证 URL 尾部路径（subpath）在各种形态下都能正确解析，
// 包括命名空间级/集群级以及作为子资源的情况。
func TestGetWithOptions(t *testing.T) {

	cases := []struct {
		name         string
		rootScoped   bool
		requestURL   string
		expectedPath string
	}{
		{
			name:         "basic",
			requestURL:   "/namespaces/default/simple/id?param1=test1&param2=test2",
			expectedPath: "",
		},
		{
			name:         "with root slash",
			requestURL:   "/namespaces/default/simple/id/?param1=test1&param2=test2",
			expectedPath: "/",
		},
		{
			name:         "with path",
			requestURL:   "/namespaces/default/simple/id/a/different/path?param1=test1&param2=test2",
			expectedPath: "/a/different/path",
		},
		{
			name:         "with path with trailing slash",
			requestURL:   "/namespaces/default/simple/id/a/different/path/?param1=test1&param2=test2",
			expectedPath: "/a/different/path/",
		},
		{
			name:         "as subresource",
			requestURL:   "/namespaces/default/simple/id/subresource/another/different/path?param1=test1&param2=test2",
			expectedPath: "/another/different/path",
		},
		{
			name:         "cluster-scoped basic",
			rootScoped:   true,
			requestURL:   "/simple/id?param1=test1&param2=test2",
			expectedPath: "",
		},
		{
			name:         "cluster-scoped basic with path",
			rootScoped:   true,
			requestURL:   "/simple/id/a/cluster/path?param1=test1&param2=test2",
			expectedPath: "/a/cluster/path",
		},
		{
			name:         "cluster-scoped basic as subresource",
			rootScoped:   true,
			requestURL:   "/simple/id/subresource/another/cluster/path?param1=test1&param2=test2",
			expectedPath: "/another/cluster/path",
		},
	}

	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			ctx := t.Context()
			simpleStorage := GetWithOptionsRESTStorage{
				SimpleRESTStorage: &SimpleRESTStorage{
					item: genericapitesting.Simple{
						Other: "foo",
					},
				},
				takesPath: "atAPath",
			}
			simpleRootStorage := GetWithOptionsRootRESTStorage{
				SimpleTypedStorage: &SimpleTypedStorage{
					baseType: &genericapitesting.SimpleRoot{}, // a root scoped type
					item: &genericapitesting.SimpleRoot{
						Other: "foo",
					},
				},
				takesPath: "atAPath",
			}

			storage := map[string]rest.Storage{}
			if test.rootScoped {
				storage["simple"] = &simpleRootStorage
				storage["simple/subresource"] = &simpleRootStorage
			} else {
				storage["simple"] = &simpleStorage
				storage["simple/subresource"] = &simpleStorage
			}
			handler := handle(storage)
			server := httptest.NewServer(handler)
			defer server.Close()

			url := server.URL + "/" + prefix + "/" + testGroupVersion.Group + "/" + testGroupVersion.Version + test.requestURL
			req, err := http.NewRequestWithContext(ctx, request.MethodGet, url, nil)
			require.NoError(t, err)
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatalf("%s: %v", test.name, err)
			}
			defer apitesting.Close(t, resp.Body)
			if resp.StatusCode != http.StatusOK {
				t.Fatalf("%s: unexpected response: %#v", test.name, resp)
			}

			var itemOut runtime.Object
			if test.rootScoped {
				itemOut = &genericapitesting.SimpleRoot{}
			} else {
				itemOut = &genericapitesting.Simple{}
			}
			body, err := extractBody(resp, itemOut)
			if err != nil {
				t.Fatalf("%s: %v", test.name, err)
			}
			if metadata, err := meta.Accessor(itemOut); err == nil {
				if metadata.GetName() != simpleStorage.item.Name {
					t.Fatalf("%s: Unexpected data: %#v, expected %#v (%s)", test.name, itemOut, simpleStorage.item, string(body))
				}
			} else {
				t.Errorf("Couldn't get name from %#v: %v", itemOut, err)
			}

			var opts *genericapitesting.SimpleGetOptions
			var ok bool
			if test.rootScoped {
				opts, ok = simpleRootStorage.optionsReceived.(*genericapitesting.SimpleGetOptions)
			} else {
				opts, ok = simpleStorage.optionsReceived.(*genericapitesting.SimpleGetOptions)

			}
			if !ok {
				t.Fatalf("Unexpected options object received: %#v", simpleStorage.optionsReceived)
			}
			if opts.Param1 != "test1" || opts.Param2 != "test2" {
				t.Fatalf("Did not receive expected options: %#v", opts)
			}
			if opts.Path != test.expectedPath {
				t.Fatalf("Unexpected path value. Expected: %s. Actual: %s.", test.expectedPath, opts.Path)
			}
		})
	}
}

// TestGetMissing 验证 NotFound 错误映射为 404。
func TestGetMissing(t *testing.T) {
	ctx := t.Context()
	storage := map[string]rest.Storage{}
	simpleStorage := SimpleRESTStorage{
		errors: map[string]error{"get": apierrors.NewNotFound(schema.GroupResource{Resource: "simples"}, "id")},
	}
	storage["simple"] = &simpleStorage
	handler := handle(storage)
	server := httptest.NewServer(handler)
	defer server.Close()

	url := server.URL + "/" + prefix + "/" + testGroupVersion.Group + "/" + testGroupVersion.Version + "/namespaces/default/simple/id"
	req, err := http.NewRequestWithContext(ctx, request.MethodGet, url, nil)
	require.NoError(t, err)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	defer apitesting.Close(t, resp.Body)
	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("Unexpected response %#v", resp)
	}
}

// TestGetRetryAfter 验证 ServerTimeout 错误会带出 Retry-After 响应头。
func TestGetRetryAfter(t *testing.T) {
	ctx := t.Context()
	storage := map[string]rest.Storage{}
	simpleStorage := SimpleRESTStorage{
		errors: map[string]error{"get": apierrors.NewServerTimeout(schema.GroupResource{Resource: "simples"}, "id", 2)},
	}
	storage["simple"] = &simpleStorage
	handler := handle(storage)
	server := httptest.NewServer(handler)
	defer server.Close()

	url := server.URL + "/" + prefix + "/" + testGroupVersion.Group + "/" + testGroupVersion.Version + "/namespaces/default/simple/id"
	req, err := http.NewRequestWithContext(ctx, request.MethodGet, url, nil)
	require.NoError(t, err)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	defer apitesting.Close(t, resp.Body)
	if resp.StatusCode != http.StatusInternalServerError {
		t.Errorf("Unexpected response %#v", resp)
	}
	if resp.Header.Get("Retry-After") != "2" {
		t.Errorf("Unexpected Retry-After header: %v", resp.Header)
	}
}

// TestConnect 验证 connect 子资源把请求交给存储返回的 handler，并透传响应体。
func TestConnect(t *testing.T) {
	ctx := t.Context()
	responseText := "Hello World"
	itemID := "theID"
	connectStorage := &ConnecterRESTStorage{
		connectHandler: &OutputConnect{
			response: responseText,
		},
	}
	storage := map[string]rest.Storage{
		"simple":         &SimpleRESTStorage{},
		"simple/connect": connectStorage,
	}
	handler := handle(storage)
	server := httptest.NewServer(handler)
	defer server.Close()

	url := server.URL + "/" + prefix + "/" + testGroupVersion.Group + "/" + testGroupVersion.Version + "/namespaces/default/simple/" + itemID + "/connect"
	req, err := http.NewRequestWithContext(ctx, request.MethodGet, url, nil)
	require.NoError(t, err)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	defer apitesting.Close(t, resp.Body)
	if resp.StatusCode != http.StatusOK {
		t.Errorf("unexpected response: %#v", resp)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if connectStorage.receivedID != itemID {
		t.Errorf("Unexpected item id. Expected: %s. Actual: %s.", itemID, connectStorage.receivedID)
	}
	if string(body) != responseText {
		t.Errorf("Unexpected response. Expected: %s. Actual: %s.", responseText, string(body))
	}
}

// TestConnectResponderObject 验证通过 Responder 返回对象时状态码与编码正确。
func TestConnectResponderObject(t *testing.T) {
	ctx := t.Context()
	itemID := "theID"
	simple := &genericapitesting.Simple{Other: "foo"}
	connectStorage := &ConnecterRESTStorage{}
	connectStorage.handlerFunc = func() http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			connectStorage.receivedResponder.Object(http.StatusCreated, simple)
		})
	}
	storage := map[string]rest.Storage{
		"simple":         &SimpleRESTStorage{},
		"simple/connect": connectStorage,
	}
	handler := handle(storage)
	server := httptest.NewServer(handler)
	defer server.Close()

	url := server.URL + "/" + prefix + "/" + testGroupVersion.Group + "/" + testGroupVersion.Version + "/namespaces/default/simple/" + itemID + "/connect"
	req, err := http.NewRequestWithContext(ctx, request.MethodGet, url, nil)
	require.NoError(t, err)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	defer apitesting.Close(t, resp.Body)
	if resp.StatusCode != http.StatusCreated {
		t.Errorf("unexpected response: %#v", resp)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if connectStorage.receivedID != itemID {
		t.Errorf("Unexpected item id. Expected: %s. Actual: %s.", itemID, connectStorage.receivedID)
	}
	obj, err := runtime.Decode(codec, body)
	if err != nil {
		t.Fatal(err)
	}
	if !apiequality.Semantic.DeepEqual(obj, simple) {
		t.Errorf("Unexpected response: %#v", obj)
	}
}

// TestConnectResponderError 验证通过 Responder 返回错误时映射为对应的状态码。
func TestConnectResponderError(t *testing.T) {
	ctx := t.Context()
	itemID := "theID"
	connectStorage := &ConnecterRESTStorage{}
	connectStorage.handlerFunc = func() http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			connectStorage.receivedResponder.Error(apierrors.NewForbidden(schema.GroupResource{Resource: "simples"}, itemID, errors.New("you are terminated")))
		})
	}
	storage := map[string]rest.Storage{
		"simple":         &SimpleRESTStorage{},
		"simple/connect": connectStorage,
	}
	handler := handle(storage)
	server := httptest.NewServer(handler)
	defer server.Close()

	url := server.URL + "/" + prefix + "/" + testGroupVersion.Group + "/" + testGroupVersion.Version + "/namespaces/default/simple/" + itemID + "/connect"
	req, err := http.NewRequestWithContext(ctx, request.MethodGet, url, nil)
	require.NoError(t, err)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	defer apitesting.Close(t, resp.Body)
	if resp.StatusCode != http.StatusForbidden {
		t.Errorf("unexpected response: %#v", resp)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if connectStorage.receivedID != itemID {
		t.Errorf("Unexpected item id. Expected: %s. Actual: %s.", itemID, connectStorage.receivedID)
	}
	obj, err := runtime.Decode(codec, body)
	if err != nil {
		t.Fatal(err)
	}
	if obj.(*metav1.Status).Code != http.StatusForbidden {
		t.Errorf("Unexpected response: %#v", obj)
	}
}

// TestConnectWithOptionsRouteParams 验证 connect 路由暴露 options 参数文档。
func TestConnectWithOptionsRouteParams(t *testing.T) {
	connectStorage := &ConnecterRESTStorage{
		connectHandler:      &OutputConnect{},
		emptyConnectOptions: &genericapitesting.SimpleGetOptions{},
	}
	storage := map[string]rest.Storage{
		"simple":         &SimpleRESTStorage{},
		"simple/connect": connectStorage,
	}
	handler := handle(storage)
	ws := handler.(*defaultAPIServer).container.RegisteredWebServices()
	if len(ws) == 0 {
		t.Fatal("no web services registered")
	}
	routes := ws[0].Routes()
	for i := range routes {
		switch routes[i].Operation {
		case "connectGetNamespacedSimpleConnect":
		case "connectPostNamespacedSimpleConnect":
		case "connectPutNamespacedSimpleConnect":
		case "connectDeleteNamespacedSimpleConnect":
			validateSimpleGetOptionsParams(t, &routes[i])

		}
	}
}

// TestConnectWithOptions 验证 connect 的查询参数被解析进 options。
func TestConnectWithOptions(t *testing.T) {
	ctx := t.Context()
	responseText := "Hello World"
	itemID := "theID"
	connectStorage := &ConnecterRESTStorage{
		connectHandler: &OutputConnect{
			response: responseText,
		},
		emptyConnectOptions: &genericapitesting.SimpleGetOptions{},
	}
	storage := map[string]rest.Storage{
		"simple":         &SimpleRESTStorage{},
		"simple/connect": connectStorage,
	}
	handler := handle(storage)
	server := httptest.NewServer(handler)
	defer server.Close()

	url := server.URL + "/" + prefix + "/" + testGroupVersion.Group + "/" + testGroupVersion.Version + "/namespaces/default/simple/" + itemID + "/connect?param1=value1&param2=value2"
	req, err := http.NewRequestWithContext(ctx, request.MethodGet, url, nil)
	require.NoError(t, err)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	defer apitesting.Close(t, resp.Body)
	if resp.StatusCode != http.StatusOK {
		t.Errorf("unexpected response: %#v", resp)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if connectStorage.receivedID != itemID {
		t.Errorf("Unexpected item id. Expected: %s. Actual: %s.", itemID, connectStorage.receivedID)
	}
	if string(body) != responseText {
		t.Errorf("Unexpected response. Expected: %s. Actual: %s.", responseText, string(body))
	}
	if connectStorage.receivedResponder == nil {
		t.Errorf("Unexpected responder")
	}
	opts, ok := connectStorage.receivedConnectOptions.(*genericapitesting.SimpleGetOptions)
	if !ok {
		t.Fatalf("Unexpected options type: %#v", connectStorage.receivedConnectOptions)
	}
	if opts.Param1 != "value1" && opts.Param2 != "value2" {
		t.Errorf("Unexpected options value: %#v", opts)
	}
}

// TestConnectWithOptionsAndPath 验证 connect 的 URL 尾部路径被写入 options.Path。
func TestConnectWithOptionsAndPath(t *testing.T) {
	ctx := t.Context()
	responseText := "Hello World"
	itemID := "theID"
	testPath := "/a/b/c/def"
	connectStorage := &ConnecterRESTStorage{
		connectHandler: &OutputConnect{
			response: responseText,
		},
		emptyConnectOptions: &genericapitesting.SimpleGetOptions{},
		takesPath:           "atAPath",
	}
	storage := map[string]rest.Storage{
		"simple":         &SimpleRESTStorage{},
		"simple/connect": connectStorage,
	}
	handler := handle(storage)
	server := httptest.NewServer(handler)
	defer server.Close()

	url := server.URL + "/" + prefix + "/" + testGroupVersion.Group + "/" + testGroupVersion.Version + "/namespaces/default/simple/" + itemID + "/connect" + testPath + "?param1=value1&param2=value2"
	req, err := http.NewRequestWithContext(ctx, request.MethodGet, url, nil)
	require.NoError(t, err)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	defer apitesting.Close(t, resp.Body)
	if resp.StatusCode != http.StatusOK {
		t.Errorf("unexpected response: %#v", resp)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if connectStorage.receivedID != itemID {
		t.Errorf("Unexpected item id. Expected: %s. Actual: %s.", itemID, connectStorage.receivedID)
	}
	if string(body) != responseText {
		t.Errorf("Unexpected response. Expected: %s. Actual: %s.", responseText, string(body))
	}
	opts, ok := connectStorage.receivedConnectOptions.(*genericapitesting.SimpleGetOptions)
	if !ok {
		t.Fatalf("Unexpected options type: %#v", connectStorage.receivedConnectOptions)
	}
	if opts.Param1 != "value1" && opts.Param2 != "value2" {
		t.Errorf("Unexpected options value: %#v", opts)
	}
	if opts.Path != testPath {
		t.Errorf("Unexpected path value. Expected: %s. Actual: %s.", testPath, opts.Path)
	}
}

// TestDelete 验证 DELETE 请求到达存储并带上正确的对象名。
func TestDelete(t *testing.T) {
	ctx := t.Context()
	storage := map[string]rest.Storage{}
	simpleStorage := SimpleRESTStorage{}
	ID := "id"
	storage["simple"] = &simpleStorage
	handler := handle(storage)
	server := httptest.NewServer(handler)
	defer server.Close()

	client := http.Client{}
	request, err := http.NewRequestWithContext(ctx, request.MethodDelete, server.URL+"/"+prefix+"/"+testGroupVersion.Group+"/"+testGroupVersion.Version+"/namespaces/default/simple/"+ID, nil)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	res, err := client.Do(request)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer apitesting.Close(t, res.Body)
	if res.StatusCode != http.StatusOK {
		t.Errorf("unexpected response: %#v", res)
	}
	if simpleStorage.deleted != ID {
		t.Errorf("Unexpected delete: %s, expected %s", simpleStorage.deleted, ID)
	}
}

// TestDeleteWithOptions 验证请求体中的 DeleteOptions 被解析并传给存储。
func TestDeleteWithOptions(t *testing.T) {
	ctx := t.Context()
	storage := map[string]rest.Storage{}
	simpleStorage := SimpleRESTStorage{}
	ID := "id"
	storage["simple"] = &simpleStorage
	handler := handle(storage)
	server := httptest.NewServer(handler)
	defer server.Close()

	grace := int64(300)
	item := &metav1.DeleteOptions{
		GracePeriodSeconds: &grace,
	}
	body, err := runtime.Encode(codec, item)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	client := http.Client{}
	request, err := http.NewRequestWithContext(ctx, request.MethodDelete, server.URL+"/"+prefix+"/"+testGroupVersion.Group+"/"+testGroupVersion.Version+"/namespaces/default/simple/"+ID, bytes.NewReader(body))
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	res, err := client.Do(request)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer apitesting.Close(t, res.Body)
	if res.StatusCode != http.StatusOK {
		t.Errorf("unexpected response: %s %#v", request.URL, res)
		s, err := io.ReadAll(res.Body)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		t.Log(string(s))
	}
	if simpleStorage.deleted != ID {
		t.Errorf("Unexpected delete: %s, expected %s", simpleStorage.deleted, ID)
	}
	simpleStorage.deleteOptions.GetObjectKind().SetGroupVersionKind(schema.GroupVersionKind{})
	if !apiequality.Semantic.DeepEqual(simpleStorage.deleteOptions, item) {
		t.Errorf("unexpected delete options: %s", cmp.Diff(simpleStorage.deleteOptions, item))
	}
}

// TestDeleteWithOptionsQuery 验证通过查询参数传入的 DeleteOptions 生效。
func TestDeleteWithOptionsQuery(t *testing.T) {
	ctx := t.Context()
	storage := map[string]rest.Storage{}
	simpleStorage := SimpleRESTStorage{}
	ID := "id"
	storage["simple"] = &simpleStorage
	handler := handle(storage)
	server := httptest.NewServer(handler)
	defer server.Close()

	grace := int64(300)
	item := &metav1.DeleteOptions{
		GracePeriodSeconds: &grace,
	}

	client := http.Client{}
	request, err := http.NewRequestWithContext(ctx, request.MethodDelete, server.URL+"/"+prefix+"/"+testGroupVersion.Group+"/"+testGroupVersion.Version+"/namespaces/default/simple/"+ID+"?gracePeriodSeconds="+strconv.FormatInt(grace, 10), nil)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	res, err := client.Do(request)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer apitesting.Close(t, res.Body)
	if res.StatusCode != http.StatusOK {
		t.Errorf("unexpected response: %s %#v", request.URL, res)
		s, err := io.ReadAll(res.Body)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		t.Log(string(s))
	}
	if simpleStorage.deleted != ID {
		t.Fatalf("Unexpected delete: %s, expected %s", simpleStorage.deleted, ID)
	}
	simpleStorage.deleteOptions.GetObjectKind().SetGroupVersionKind(schema.GroupVersionKind{})
	if !apiequality.Semantic.DeepEqual(simpleStorage.deleteOptions, item) {
		t.Errorf("unexpected delete options: %s", cmp.Diff(simpleStorage.deleteOptions, item))
	}
}

// TestDeleteWithOptionsQueryAndBody 验证查询参数与请求体同时存在时的优先级：
// 以请求体为准。
func TestDeleteWithOptionsQueryAndBody(t *testing.T) {
	ctx := t.Context()
	storage := map[string]rest.Storage{}
	simpleStorage := SimpleRESTStorage{}
	ID := "id"
	storage["simple"] = &simpleStorage
	handler := handle(storage)
	server := httptest.NewServer(handler)
	defer server.Close()

	grace := int64(300)
	item := &metav1.DeleteOptions{
		GracePeriodSeconds: &grace,
	}
	body, err := runtime.Encode(codec, item)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	client := http.Client{}
	request, err := http.NewRequestWithContext(ctx, request.MethodDelete, server.URL+"/"+prefix+"/"+testGroupVersion.Group+"/"+testGroupVersion.Version+"/namespaces/default/simple/"+ID+"?gracePeriodSeconds="+strconv.FormatInt(grace+10, 10), bytes.NewReader(body))
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	res, err := client.Do(request)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer apitesting.Close(t, res.Body)
	if res.StatusCode != http.StatusOK {
		t.Errorf("unexpected response: %s %#v", request.URL, res)
		s, err := io.ReadAll(res.Body)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		t.Log(string(s))
	}
	if simpleStorage.deleted != ID {
		t.Errorf("Unexpected delete: %s, expected %s", simpleStorage.deleted, ID)
	}
	simpleStorage.deleteOptions.GetObjectKind().SetGroupVersionKind(schema.GroupVersionKind{})
	if !apiequality.Semantic.DeepEqual(simpleStorage.deleteOptions, item) {
		t.Errorf("unexpected delete options: %s", cmp.Diff(simpleStorage.deleteOptions, item))
	}
}

// TestDeleteInvokesAdmissionControl 验证删除路径同时经过变更与校验准入。
func TestDeleteInvokesAdmissionControl(t *testing.T) {
	// TODO: remove mutating deny when we removed it from the endpoint implementation and ported all plugins
	for _, admit := range []admission.Interface{alwaysMutatingDeny{}, alwaysValidatingDeny{}} {
		testName := fmt.Sprintf("%T", admit)
		t.Run(testName, func(t *testing.T) {
			ctx := t.Context()
			storage := map[string]rest.Storage{}
			simpleStorage := SimpleRESTStorage{}
			ID := "id"
			storage["simple"] = &simpleStorage
			handler := handleInternal(storage, admit, nil)
			server := httptest.NewServer(handler)
			defer server.Close()

			client := http.Client{}
			request, err := http.NewRequestWithContext(ctx, request.MethodDelete, server.URL+"/"+prefix+"/"+testGroupVersion.Group+"/"+testGroupVersion.Version+"/namespaces/default/simple/"+ID, nil)
			if err != nil {
				t.Errorf("unexpected error: %v", err)
			}
			response, err := client.Do(request)
			if err != nil {
				t.Errorf("unexpected error: %v", err)
			}
			defer apitesting.Close(t, response.Body)
			if response.StatusCode != http.StatusForbidden {
				t.Errorf("Unexpected response %#v", response)
			}
		})
	}
}

// TestDeleteMissing 验证删除不存在的对象返回 404。
func TestDeleteMissing(t *testing.T) {
	ctx := t.Context()
	storage := map[string]rest.Storage{}
	ID := "id"
	simpleStorage := SimpleRESTStorage{
		errors: map[string]error{"delete": apierrors.NewNotFound(schema.GroupResource{Resource: "simples"}, ID)},
	}
	storage["simple"] = &simpleStorage
	handler := handle(storage)
	server := httptest.NewServer(handler)
	defer server.Close()

	client := http.Client{}
	request, err := http.NewRequestWithContext(ctx, request.MethodDelete, server.URL+"/"+prefix+"/"+testGroupVersion.Group+"/"+testGroupVersion.Version+"/namespaces/default/simple/"+ID, nil)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	response, err := client.Do(request)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	defer apitesting.Close(t, response.Body)
	if response.StatusCode != http.StatusNotFound {
		t.Errorf("Unexpected response %#v", response)
	}
}

// TestUpdate 验证 PUT 的更新流程；请求体允许省略 namespace，由 handler 补齐。
func TestUpdate(t *testing.T) {
	ctx := t.Context()
	storage := map[string]rest.Storage{}
	simpleStorage := SimpleRESTStorage{}
	ID := "id"
	storage["simple"] = &simpleStorage
	handler := handle(storage)
	server := httptest.NewServer(handler)
	defer server.Close()

	item := &genericapitesting.Simple{
		ObjectMeta: metav1.ObjectMeta{
			Name:      ID,
			Namespace: "", // update should allow the client to send an empty namespace
		},
		Other: "bar",
	}
	body, err := runtime.Encode(testCodec, item)
	if err != nil {
		// The following cases will fail, so die now
		t.Fatalf("unexpected error: %v", err)
	}

	client := http.Client{}
	request, err := http.NewRequestWithContext(ctx, request.MethodPut, server.URL+"/"+prefix+"/"+testGroupVersion.Group+"/"+testGroupVersion.Version+"/namespaces/default/simple/"+ID, bytes.NewReader(body))
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	response, err := client.Do(request)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	defer apitesting.Close(t, response.Body)
	dump, _ := httputil.DumpResponse(response, true)
	t.Log(string(dump))

	if simpleStorage.updated == nil || simpleStorage.updated.Name != item.Name {
		t.Errorf("Unexpected update value %#v, expected %#v.", simpleStorage.updated, item)
	}
}

// TestUpdateInvokesAdmissionControl 验证更新路径经过准入且拒绝时返回 403。
func TestUpdateInvokesAdmissionControl(t *testing.T) {
	for _, admit := range []admission.Interface{alwaysMutatingDeny{}, alwaysValidatingDeny{}} {
		testName := fmt.Sprintf("%T", admit)
		t.Run(testName, func(t *testing.T) {
			ctx := t.Context()

			storage := map[string]rest.Storage{}
			simpleStorage := SimpleRESTStorage{}
			ID := "id"
			storage["simple"] = &simpleStorage
			handler := handleInternal(storage, admit, nil)
			server := httptest.NewServer(handler)
			defer server.Close()

			item := &genericapitesting.Simple{
				ObjectMeta: metav1.ObjectMeta{
					Name:      ID,
					Namespace: metav1.NamespaceDefault,
				},
				Other: "bar",
			}
			body, err := runtime.Encode(testCodec, item)
			if err != nil {
				// The following cases will fail, so die now
				t.Fatalf("unexpected error: %v", err)
			}

			client := http.Client{}
			request, err := http.NewRequestWithContext(ctx, request.MethodPut, server.URL+"/"+prefix+"/"+testGroupVersion.Group+"/"+testGroupVersion.Version+"/namespaces/default/simple/"+ID, bytes.NewReader(body))
			if err != nil {
				t.Errorf("unexpected error: %v", err)
			}
			response, err := client.Do(request)
			if err != nil {
				t.Errorf("unexpected error: %v", err)
			}
			defer apitesting.Close(t, response.Body)
			dump, _ := httputil.DumpResponse(response, true)
			t.Log(string(dump))

			if response.StatusCode != http.StatusForbidden {
				t.Errorf("Unexpected response %#v", response)
			}
		})
	}
}

// TestUpdateRequiresMatchingName 验证 PUT 请求体必须带有与 URL 一致的对象名。
func TestUpdateRequiresMatchingName(t *testing.T) {
	ctx := t.Context()
	storage := map[string]rest.Storage{}
	simpleStorage := SimpleRESTStorage{}
	ID := "id"
	storage["simple"] = &simpleStorage
	handler := handle(storage)
	server := httptest.NewServer(handler)
	defer server.Close()

	item := &genericapitesting.Simple{
		Other: "bar",
	}
	body, err := runtime.Encode(testCodec, item)
	if err != nil {
		// The following cases will fail, so die now
		t.Fatalf("unexpected error: %v", err)
	}

	client := http.Client{}
	request, err := http.NewRequestWithContext(ctx, request.MethodPut, server.URL+"/"+prefix+"/"+testGroupVersion.Group+"/"+testGroupVersion.Version+"/namespaces/default/simple/"+ID, bytes.NewReader(body))
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	response, err := client.Do(request)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	defer apitesting.Close(t, response.Body)
	if response.StatusCode != http.StatusBadRequest {
		dump, _ := httputil.DumpResponse(response, true)
		t.Log(string(dump))
		t.Errorf("Unexpected response %#v", response)
	}
}

// TestUpdateAllowsMissingNamespace 验证更新时对象可省略 namespace。
func TestUpdateAllowsMissingNamespace(t *testing.T) {
	ctx := t.Context()
	storage := map[string]rest.Storage{}
	simpleStorage := SimpleRESTStorage{}
	ID := "id"
	storage["simple"] = &simpleStorage
	handler := handle(storage)
	server := httptest.NewServer(handler)
	defer server.Close()

	item := &genericapitesting.Simple{
		ObjectMeta: metav1.ObjectMeta{
			Name: ID,
		},
		Other: "bar",
	}
	body, err := runtime.Encode(testCodec, item)
	if err != nil {
		// The following cases will fail, so die now
		t.Fatalf("unexpected error: %v", err)
	}

	client := http.Client{}
	request, err := http.NewRequestWithContext(ctx, request.MethodPut, server.URL+"/"+prefix+"/"+testGroupVersion.Group+"/"+testGroupVersion.Version+"/namespaces/default/simple/"+ID, bytes.NewReader(body))
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	response, err := client.Do(request)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	defer apitesting.Close(t, response.Body)
	dump, _ := httputil.DumpResponse(response, true)
	t.Log(string(dump))

	if response.StatusCode != http.StatusOK {
		t.Errorf("Unexpected response %#v", response)
	}
}

// when the object name and namespace can't be retrieved, don't update.  It isn't safe.
func TestUpdateDisallowsMismatchedNamespaceOnError(t *testing.T) {
	ctx := t.Context()
	storage := map[string]rest.Storage{}
	simpleStorage := SimpleRESTStorage{}
	ID := "id"
	storage["simple"] = &simpleStorage
	handler := handle(storage)
	server := httptest.NewServer(handler)
	defer server.Close()

	item := &genericapitesting.Simple{
		ObjectMeta: metav1.ObjectMeta{
			Name:      ID,
			Namespace: "other", // does not match request
		},
		Other: "bar",
	}
	body, err := runtime.Encode(testCodec, item)
	if err != nil {
		// The following cases will fail, so die now
		t.Fatalf("unexpected error: %v", err)
	}

	client := http.Client{}
	request, err := http.NewRequestWithContext(ctx, request.MethodPut, server.URL+"/"+prefix+"/"+testGroupVersion.Group+"/"+testGroupVersion.Version+"/namespaces/default/simple/"+ID, bytes.NewReader(body))
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	response, err := client.Do(request)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	defer apitesting.Close(t, response.Body)
	dump, _ := httputil.DumpResponse(response, true)
	t.Log(string(dump))

	if simpleStorage.updated != nil {
		t.Errorf("Unexpected update value %#v.", simpleStorage.updated)
	}
}

// TestUpdatePreventsMismatchedNamespace 验证对象声明的 namespace 与 URL 不一致
// 时被拒绝（安全边界，防止跨命名空间写入）。
func TestUpdatePreventsMismatchedNamespace(t *testing.T) {
	ctx := t.Context()
	storage := map[string]rest.Storage{}
	simpleStorage := SimpleRESTStorage{}
	ID := "id"
	storage["simple"] = &simpleStorage
	handler := handle(storage)
	server := httptest.NewServer(handler)
	defer server.Close()

	item := &genericapitesting.Simple{
		ObjectMeta: metav1.ObjectMeta{
			Name:      ID,
			Namespace: "other",
		},
		Other: "bar",
	}
	body, err := runtime.Encode(testCodec, item)
	if err != nil {
		// The following cases will fail, so die now
		t.Fatalf("unexpected error: %v", err)
	}

	client := http.Client{}
	request, err := http.NewRequestWithContext(ctx, request.MethodPut, server.URL+"/"+prefix+"/"+testGroupVersion.Group+"/"+testGroupVersion.Version+"/namespaces/default/simple/"+ID, bytes.NewReader(body))
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	response, err := client.Do(request)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	defer apitesting.Close(t, response.Body)
	if response.StatusCode != http.StatusBadRequest {
		t.Errorf("Unexpected response %#v", response)
	}
}

// TestUpdateMissing 验证更新不存在的对象返回 404。
func TestUpdateMissing(t *testing.T) {
	ctx := t.Context()
	storage := map[string]rest.Storage{}
	ID := "id"
	simpleStorage := SimpleRESTStorage{
		errors: map[string]error{"update": apierrors.NewNotFound(schema.GroupResource{Resource: "simples"}, ID)},
	}
	storage["simple"] = &simpleStorage
	handler := handle(storage)
	server := httptest.NewServer(handler)
	defer server.Close()

	item := &genericapitesting.Simple{
		ObjectMeta: metav1.ObjectMeta{
			Name:      ID,
			Namespace: metav1.NamespaceDefault,
		},
		Other: "bar",
	}
	body, err := runtime.Encode(testCodec, item)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	client := http.Client{}
	request, err := http.NewRequestWithContext(ctx, request.MethodPut, server.URL+"/"+prefix+"/"+testGroupVersion.Group+"/"+testGroupVersion.Version+"/namespaces/default/simple/"+ID, bytes.NewReader(body))
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	response, err := client.Do(request)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	defer apitesting.Close(t, response.Body)
	if response.StatusCode != http.StatusNotFound {
		t.Errorf("Unexpected response %#v", response)
	}
}

// TestCreateNotFound 验证创建时存储返回 NotFound 会被原样映射。
func TestCreateNotFound(t *testing.T) {
	ctx := t.Context()
	handler := handle(map[string]rest.Storage{
		"simple": &SimpleRESTStorage{
			// storage.Create can fail with not found error in theory.
			// See https://pr.k8s.io/486#discussion_r15037092.
			errors: map[string]error{"create": apierrors.NewNotFound(schema.GroupResource{Resource: "simples"}, "id")},
		},
	})
	server := httptest.NewServer(handler)
	defer server.Close()
	client := http.Client{}

	simple := &genericapitesting.Simple{Other: "foo"}
	data, err := runtime.Encode(testCodec, simple)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	request, err := http.NewRequestWithContext(ctx, request.MethodPost, server.URL+"/"+prefix+"/"+testGroupVersion.Group+"/"+testGroupVersion.Version+"/namespaces/default/simple", bytes.NewBuffer(data))
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	response, err := client.Do(request)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	defer apitesting.Close(t, response.Body)
	if response.StatusCode != http.StatusNotFound {
		t.Errorf("Unexpected response %#v", response)
	}
}

// TestCreateChecksDecode 验证请求体类型与资源不匹配时返回 400。
func TestCreateChecksDecode(t *testing.T) {
	ctx := t.Context()
	handler := handle(map[string]rest.Storage{"simple": &SimpleRESTStorage{}})
	server := httptest.NewServer(handler)
	defer server.Close()
	client := http.Client{}

	simple := &example.Pod{}
	data, err := runtime.Encode(testCodec, simple)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	request, err := http.NewRequestWithContext(ctx, request.MethodPost, server.URL+"/"+prefix+"/"+testGroupVersion.Group+"/"+testGroupVersion.Version+"/namespaces/default/simple", bytes.NewBuffer(data))
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	response, err := client.Do(request)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	defer apitesting.Close(t, response.Body)
	if response.StatusCode != http.StatusBadRequest {
		t.Errorf("Unexpected response %#v", response)
	}
	b, err := io.ReadAll(response.Body)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	} else if !strings.Contains(string(b), "cannot be handled as a Simple") {
		t.Errorf("unexpected response: %s", string(b))
	}
}

// TestParentResourceIsRequired 验证注册子资源时其父资源也必须注册，否则
// InstallREST 应报错；同时确认命名空间级子资源能正确解析命名空间。
func TestParentResourceIsRequired(t *testing.T) {
	storage := &SimpleTypedStorage{
		baseType: &genericapitesting.SimpleRoot{}, // a root scoped type
		item:     &genericapitesting.SimpleRoot{},
	}
	group := &APIGroupVersion{
		Storage: map[string]rest.Storage{
			"simple/sub": storage,
		},
		Root:            "/" + prefix,
		Creater:         scheme,
		Convertor:       scheme,
		UnsafeConvertor: runtime.UnsafeObjectConvertor(scheme),
		Defaulter:       scheme,
		Typer:           scheme,
		Namer:           namer,

		EquivalentResourceRegistry: runtime.NewEquivalentResourceRegistry(),

		Admit: admissionControl,

		GroupVersion:           newGroupVersion,
		OptionsExternalVersion: &newGroupVersion,

		Serializer:     codecs,
		ParameterCodec: parameterCodec,
	}
	container := restful.NewContainer()
	if _, _, err := group.InstallREST(container); err == nil {
		t.Fatal("expected error")
	}

	storage = &SimpleTypedStorage{
		baseType: &genericapitesting.SimpleRoot{}, // a root scoped type
		item:     &genericapitesting.SimpleRoot{},
	}
	group = &APIGroupVersion{
		Storage: map[string]rest.Storage{
			"simple":     &SimpleRESTStorage{},
			"simple/sub": storage,
		},
		Root:            "/" + prefix,
		Creater:         scheme,
		Convertor:       scheme,
		UnsafeConvertor: runtime.UnsafeObjectConvertor(scheme),
		TypeConverter:   managedfields.NewDeducedTypeConverter(),
		Defaulter:       scheme,
		Typer:           scheme,
		Namer:           namer,

		EquivalentResourceRegistry: runtime.NewEquivalentResourceRegistry(),

		Admit: admissionControl,

		GroupVersion:           newGroupVersion,
		OptionsExternalVersion: &newGroupVersion,

		Serializer:     codecs,
		ParameterCodec: parameterCodec,
	}
	container = restful.NewContainer()
	if _, _, err := group.InstallREST(container); err != nil {
		t.Fatal(err)
	}

	handler := genericapifilters.WithRequestInfo(container, newTestRequestInfoResolver())

	// resource is NOT registered in the root scope
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, &http.Request{Method: request.MethodGet, URL: &url.URL{Path: "/" + prefix + "/simple/test/sub"}})
	if w.Code != http.StatusNotFound {
		t.Errorf("expected not found: %#v", w)
	}

	// resource is registered in the namespace scope
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, &http.Request{Method: request.MethodGet, URL: &url.URL{Path: "/" + prefix + "/" + newGroupVersion.Group + "/" + newGroupVersion.Version + "/namespaces/test/simple/test/sub"}})
	if w.Code != http.StatusOK {
		t.Fatalf("expected OK: %#v", w)
	}
	if storage.actualNamespace != "test" {
		t.Errorf("namespace should be set %#v", storage)
	}
}

// TestNamedCreaterWithName 验证带名称的创建接口收到 URL 中的名称。
func TestNamedCreaterWithName(t *testing.T) {
	ctx := t.Context()
	pathName := "helloworld"
	storage := &NamedCreaterRESTStorage{SimpleRESTStorage: &SimpleRESTStorage{}}
	handler := handle(map[string]rest.Storage{
		"simple":     &SimpleRESTStorage{},
		"simple/sub": storage,
	})
	server := httptest.NewServer(handler)
	defer server.Close()
	client := http.Client{}

	simple := &genericapitesting.Simple{Other: "foo"}
	data, err := runtime.Encode(testCodec, simple)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	request, err := http.NewRequestWithContext(ctx, request.MethodPost, server.URL+"/"+prefix+"/"+testGroupVersion.Group+"/"+testGroupVersion.Version+"/namespaces/default/simple/"+pathName+"/sub", bytes.NewBuffer(data))
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	response, err := client.Do(request)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	defer apitesting.Close(t, response.Body)
	if response.StatusCode != http.StatusCreated {
		t.Errorf("Unexpected response %#v", response)
	}
	if storage.createdName != pathName {
		t.Errorf("Did not get expected name in create context. Got: %s, Expected: %s", storage.createdName, pathName)
	}
}

// TestNamedCreaterWithoutName 验证带名称的创建接口在 URL 缺少名称时返回 400。
func TestNamedCreaterWithoutName(t *testing.T) {
	ctx := t.Context()
	storage := &NamedCreaterRESTStorage{
		SimpleRESTStorage: &SimpleRESTStorage{
			injectedFunction: func(obj runtime.Object) (runtime.Object, error) {
				time.Sleep(5 * time.Millisecond)
				return obj, nil
			},
		},
	}

	handler := handle(map[string]rest.Storage{"foo": storage})
	server := httptest.NewServer(handler)
	defer server.Close()
	client := http.Client{}

	simple := &genericapitesting.Simple{
		Other: "bar",
	}
	data, err := runtime.Encode(testCodec, simple)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	request, err := http.NewRequestWithContext(ctx, request.MethodPost, server.URL+"/"+prefix+"/"+testGroupVersion.Group+"/"+testGroupVersion.Version+"/namespaces/default/foo", bytes.NewBuffer(data))
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	wg := sync.WaitGroup{}
	wg.Add(1)
	var response *http.Response
	go func() {
		response, err = client.Do(request)
		wg.Done()
	}()
	wg.Wait()
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	defer apitesting.Close(t, response.Body)
	// empty name is not allowed for NamedCreater
	if response.StatusCode != http.StatusBadRequest {
		t.Errorf("Unexpected response %#v", response)
	}
}

// namePopulatorAdmissionControl 是校验类准入桩，用于断言准入看到的名称。
type namePopulatorAdmissionControl struct {
	t            *testing.T
	populateName string
}

func (npac *namePopulatorAdmissionControl) Validate(ctx context.Context, a admission.Attributes, o admission.ObjectInterfaces) (err error) {
	if a.GetName() != npac.populateName {
		npac.t.Errorf("Unexpected name: got %q, expected %q", a.GetName(), npac.populateName)
	}
	return nil
}

func (npac *namePopulatorAdmissionControl) Handles(operation admission.Operation) bool {
	return true
}

var _ admission.ValidationInterface = &namePopulatorAdmissionControl{}

// TestNamedCreaterWithGenerateName 验证 generateName 流程：存储层补全名称后，
// 校验准入看到的应是补全后的名称。
func TestNamedCreaterWithGenerateName(t *testing.T) {
	ctx := t.Context()
	populateName := "bar"
	storage := &SimpleRESTStorage{
		injectedFunction: func(obj runtime.Object) (runtime.Object, error) {
			time.Sleep(5 * time.Millisecond)
			if metadata, err := meta.Accessor(obj); err == nil {
				if len(metadata.GetName()) != 0 {
					t.Errorf("Unexpected name %q", metadata.GetName())
				}
				metadata.SetName(populateName)
			} else {
				return nil, err
			}
			return obj, nil
		},
	}

	ac := &namePopulatorAdmissionControl{
		t:            t,
		populateName: populateName,
	}

	handler := handleInternal(map[string]rest.Storage{"foo": storage}, ac, nil)
	server := httptest.NewServer(handler)
	defer server.Close()
	client := http.Client{}

	simple := &genericapitesting.Simple{
		Other: "bar",
	}
	data, err := runtime.Encode(testCodec, simple)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	request, err := http.NewRequestWithContext(ctx, request.MethodPost, server.URL+"/"+prefix+"/"+testGroupVersion.Group+"/"+testGroupVersion.Version+"/namespaces/default/foo", bytes.NewBuffer(data))
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	wg := sync.WaitGroup{}
	wg.Add(1)
	var response *http.Response
	go func() {
		response, err = client.Do(request)
		wg.Done()
	}()
	wg.Wait()
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	defer apitesting.Close(t, response.Body)
	if response.StatusCode != http.StatusCreated {
		t.Errorf("Unexpected status: %d, Expected: %d, %#v", response.StatusCode, http.StatusOK, response)
	}

	var itemOut genericapitesting.Simple
	body, err := extractBody(response, &itemOut)
	if err != nil {
		t.Errorf("unexpected error: %v %#v", err, response)
	}

	// Avoid comparing managed fields in expected result
	itemOut.ManagedFields = nil
	itemOut.GetObjectKind().SetGroupVersionKind(schema.GroupVersionKind{})
	simple.Name = populateName
	simple.Namespace = "default" // populated by create handler to match request URL
	if !reflect.DeepEqual(&itemOut, simple) {
		t.Errorf("Unexpected data: %#v, expected %#v (%s)", itemOut, simple, string(body))
	}
}

// TestUpdateChecksDecode 验证更新请求体类型不匹配时返回 400。
func TestUpdateChecksDecode(t *testing.T) {
	ctx := t.Context()
	handler := handle(map[string]rest.Storage{"simple": &SimpleRESTStorage{}})
	server := httptest.NewServer(handler)
	defer server.Close()
	client := http.Client{}

	simple := &example.Pod{}
	data, err := runtime.Encode(testCodec, simple)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	request, err := http.NewRequestWithContext(ctx, request.MethodPut, server.URL+"/"+prefix+"/"+testGroupVersion.Group+"/"+testGroupVersion.Version+"/namespaces/default/simple/bar", bytes.NewBuffer(data))
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	response, err := client.Do(request)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	defer apitesting.Close(t, response.Body)
	if response.StatusCode != http.StatusBadRequest {
		t.Errorf("Unexpected response %#v\n%s", response, readBodyOrDie(response.Body))
	}
	b, err := io.ReadAll(response.Body)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	} else if !strings.Contains(string(b), "cannot be handled as a Simple") {
		t.Errorf("unexpected response: %s", string(b))
	}
}

// TestCreate 验证基本创建流程与 201 响应。
func TestCreate(t *testing.T) {
	ctx := t.Context()
	storage := SimpleRESTStorage{
		injectedFunction: func(obj runtime.Object) (runtime.Object, error) {
			time.Sleep(5 * time.Millisecond)
			return obj, nil
		},
	}
	handler := handle(map[string]rest.Storage{"foo": &storage})
	server := httptest.NewServer(handler)
	defer server.Close()
	client := http.Client{}

	simple := &genericapitesting.Simple{
		Other: "bar",
	}
	data, err := runtime.Encode(testCodec, simple)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	request, err := http.NewRequestWithContext(ctx, request.MethodPost, server.URL+"/"+prefix+"/"+testGroupVersion.Group+"/"+testGroupVersion.Version+"/namespaces/default/foo", bytes.NewBuffer(data))
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	wg := sync.WaitGroup{}
	wg.Add(1)
	var response *http.Response
	go func() {
		response, err = client.Do(request)
		wg.Done()
	}()
	wg.Wait()
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	defer apitesting.Close(t, response.Body)

	var itemOut genericapitesting.Simple
	body, err := extractBody(response, &itemOut)
	if err != nil {
		t.Errorf("unexpected error: %v %#v", err, response)
	}

	// Avoid comparing managed fields in expected result
	itemOut.ManagedFields = nil
	itemOut.GetObjectKind().SetGroupVersionKind(schema.GroupVersionKind{})
	simple.Namespace = "default" // populated by create handler to match request URL
	if !reflect.DeepEqual(&itemOut, simple) {
		t.Errorf("Unexpected data: %#v, expected %#v (%s)", itemOut, simple, string(body))
	}
	if response.StatusCode != http.StatusCreated {
		t.Errorf("Unexpected status: %d, Expected: %d, %#v", response.StatusCode, http.StatusOK, response)
	}
}

// TestCreateYAML 验证以 YAML 提交并协商 YAML 响应的创建流程。
func TestCreateYAML(t *testing.T) {
	ctx := t.Context()
	storage := SimpleRESTStorage{
		injectedFunction: func(obj runtime.Object) (runtime.Object, error) {
			time.Sleep(5 * time.Millisecond)
			return obj, nil
		},
	}
	handler := handle(map[string]rest.Storage{"foo": &storage})
	server := httptest.NewServer(handler)
	defer server.Close()
	client := http.Client{}

	// yaml encoder
	simple := &genericapitesting.Simple{
		Other: "bar",
	}
	info, ok := runtime.SerializerInfoForMediaType(codecs.SupportedMediaTypes(), "application/yaml")
	if !ok {
		t.Fatal("No yaml serializer")
	}
	encoder := codecs.EncoderForVersion(info.Serializer, testGroupVersion)
	decoder := codecs.DecoderToVersion(info.Serializer, testInternalGroupVersion)

	data, err := runtime.Encode(encoder, simple)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	request, err := http.NewRequestWithContext(ctx, request.MethodPost, server.URL+"/"+prefix+"/"+testGroupVersion.Group+"/"+testGroupVersion.Version+"/namespaces/default/foo", bytes.NewBuffer(data))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	request.Header.Set("Accept", "application/yaml, application/json")
	request.Header.Set("Content-Type", "application/yaml")

	wg := sync.WaitGroup{}
	wg.Add(1)
	var response *http.Response
	go func() {
		response, err = client.Do(request)
		wg.Done()
	}()
	wg.Wait()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer apitesting.Close(t, response.Body)

	var itemOut genericapitesting.Simple
	body, err := extractBodyDecoder(response, &itemOut, decoder)
	if err != nil {
		t.Fatalf("unexpected error: %v %#v", err, response)
	}

	// Avoid comparing managed fields in expected result
	itemOut.ManagedFields = nil
	itemOut.GetObjectKind().SetGroupVersionKind(schema.GroupVersionKind{})
	simple.Namespace = "default" // populated by create handler to match request URL
	if !reflect.DeepEqual(&itemOut, simple) {
		t.Errorf("Unexpected data: %#v, expected %#v (%s)", itemOut, simple, string(body))
	}
	if response.StatusCode != http.StatusCreated {
		t.Errorf("Unexpected status: %d, Expected: %d, %#v", response.StatusCode, http.StatusOK, response)
	}
}

// TestCreateInNamespace 验证创建时对象命名空间由 URL 决定。
func TestCreateInNamespace(t *testing.T) {
	ctx := t.Context()
	storage := SimpleRESTStorage{
		injectedFunction: func(obj runtime.Object) (runtime.Object, error) {
			time.Sleep(5 * time.Millisecond)
			return obj, nil
		},
	}
	handler := handle(map[string]rest.Storage{"foo": &storage})
	server := httptest.NewServer(handler)
	defer server.Close()
	client := http.Client{}

	simple := &genericapitesting.Simple{
		Other: "bar",
	}
	data, err := runtime.Encode(testCodec, simple)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	request, err := http.NewRequestWithContext(ctx, request.MethodPost, server.URL+"/"+prefix+"/"+testGroupVersion.Group+"/"+testGroupVersion.Version+"/namespaces/other/foo", bytes.NewBuffer(data))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	wg := sync.WaitGroup{}
	wg.Add(1)
	var response *http.Response
	go func() {
		response, err = client.Do(request)
		wg.Done()
	}()
	wg.Wait()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer apitesting.Close(t, response.Body)

	var itemOut genericapitesting.Simple
	body, err := extractBody(response, &itemOut)
	if err != nil {
		t.Fatalf("unexpected error: %v\n%s", err, data)
	}

	// Avoid comparing managed fields in expected result
	itemOut.ManagedFields = nil
	itemOut.GetObjectKind().SetGroupVersionKind(schema.GroupVersionKind{})
	simple.Namespace = "other" // populated by create handler to match request URL
	if !reflect.DeepEqual(&itemOut, simple) {
		t.Errorf("Unexpected data: %#v, expected %#v (%s)", itemOut, simple, string(body))
	}
	if response.StatusCode != http.StatusCreated {
		t.Errorf("Unexpected status: %d, Expected: %d, %#v", response.StatusCode, http.StatusOK, response)
	}
}

// TestCreateInvokeAdmissionControl 验证创建路径经过准入且拒绝时返回 403。
func TestCreateInvokeAdmissionControl(t *testing.T) {
	for _, admit := range []admission.Interface{alwaysMutatingDeny{}, alwaysValidatingDeny{}} {
		testName := fmt.Sprintf("%T", admit)
		t.Run(testName, func(t *testing.T) {
			ctx := t.Context()
			storage := SimpleRESTStorage{
				injectedFunction: func(obj runtime.Object) (runtime.Object, error) {
					time.Sleep(5 * time.Millisecond)
					return obj, nil
				},
			}
			handler := handleInternal(map[string]rest.Storage{"foo": &storage}, admit, nil)
			server := httptest.NewServer(handler)
			defer server.Close()
			client := http.Client{}

			simple := &genericapitesting.Simple{
				Other: "bar",
			}
			data, err := runtime.Encode(testCodec, simple)
			if err != nil {
				t.Errorf("unexpected error: %v", err)
			}
			request, err := http.NewRequestWithContext(ctx, request.MethodPost, server.URL+"/"+prefix+"/"+testGroupVersion.Group+"/"+testGroupVersion.Version+"/namespaces/other/foo", bytes.NewBuffer(data))
			if err != nil {
				t.Errorf("unexpected error: %v", err)
			}

			var response *http.Response
			response, err = client.Do(request)
			if err != nil {
				t.Errorf("unexpected error: %v", err)
			}
			defer apitesting.Close(t, response.Body)
			if response.StatusCode != http.StatusForbidden {
				t.Errorf("Unexpected status: %d, Expected: %d, %#v", response.StatusCode, http.StatusForbidden, response)
			}
		})
	}
}

// expectAPIStatus 发起请求、断言状态码，并解码返回的 metav1.Status。
func expectAPIStatus(t *testing.T, method, url string, data []byte, code int) *metav1.Status {
	t.Helper()
	ctx := t.Context()
	client := http.Client{}
	request, err := http.NewRequestWithContext(ctx, method, url, bytes.NewBuffer(data))
	if err != nil {
		t.Fatalf("unexpected error %#v", err)
		return nil
	}
	response, err := client.Do(request)
	if err != nil {
		t.Fatalf("unexpected error on %s %s: %v", method, url, err)
		return nil
	}
	defer apitesting.Close(t, response.Body)
	var status metav1.Status
	body, err := extractBody(response, &status)
	if err != nil {
		t.Fatalf("unexpected error on %s %s: %v\nbody:\n%s", method, url, err, body)
		return nil
	}
	if code != response.StatusCode {
		t.Fatalf("Expected %s %s to return %d, Got %d: %v", method, url, code, response.StatusCode, body)
	}
	return &status
}

// TestDelayReturnsError 验证存储返回 AlreadyExists 时映射为 409 且带 Details。
func TestDelayReturnsError(t *testing.T) {
	storage := SimpleRESTStorage{
		injectedFunction: func(obj runtime.Object) (runtime.Object, error) {
			return nil, apierrors.NewAlreadyExists(schema.GroupResource{Resource: "foos"}, "bar")
		},
	}
	handler := handle(map[string]rest.Storage{"foo": &storage})
	server := httptest.NewServer(handler)
	defer server.Close()

	status := expectAPIStatus(t, request.MethodDelete, fmt.Sprintf("%s/"+prefix+"/"+testGroupVersion.Group+"/"+testGroupVersion.Version+"/namespaces/default/foo/bar", server.URL), nil, http.StatusConflict)
	if status.Status != metav1.StatusFailure || status.Message == "" || status.Details == nil || status.Reason != metav1.StatusReasonAlreadyExists {
		t.Errorf("Unexpected status %#v", status)
	}
}

// UnregisteredAPIObject 是未注册到 scheme 的类型，用于触发编码错误。
type UnregisteredAPIObject struct {
	Value string
}

func (obj *UnregisteredAPIObject) GetObjectKind() schema.ObjectKind {
	return schema.EmptyObjectKind
}
func (obj *UnregisteredAPIObject) DeepCopyObject() runtime.Object {
	if obj == nil {
		return nil
	}
	clone := *obj
	return &clone
}

// TestWriteJSONDecodeError 验证编码失败时返回 500 且错误信息包含类型未注册的提示。
func TestWriteJSONDecodeError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		responsewriters.WriteObjectNegotiated(codecs, negotiation.DefaultEndpointRestrictions, newGroupVersion, w, req, http.StatusOK, &UnregisteredAPIObject{"Undecodable"}, false)
	}))
	defer server.Close()
	// Decode error response behavior is dictated by
	// apiserver/pkg/endpoints/handlers/responsewriters/status.go::ErrorToAPIStatus().
	// Unless specific metav1.Status() parameters are implemented for the particular error in question, such that
	// the status code is defined, metav1 errors where error.status == metav1.StatusFailure
	// will throw a '500 Internal Server Error'. Non-metav1 type errors will always throw a '500 Internal Server Error'.
	status := expectAPIStatus(t, request.MethodGet, server.URL, nil, http.StatusInternalServerError)
	if status.Reason != metav1.StatusReasonUnknown {
		t.Errorf("unexpected reason %#v", status)
	}
	if !strings.Contains(status.Message, "no kind is registered for the type endpoints.UnregisteredAPIObject") {
		t.Errorf("unexpected message %#v", status)
	}
}

// marshalError 在序列化时总是返回错误，用于测试 WriteRawJSON 的错误处理。
type marshalError struct {
	err error
}

func (m *marshalError) MarshalJSON() ([]byte, error) {
	return []byte{}, m.err
}

// TestWriteRAWJSONMarshalError 验证 WriteRawJSON 在 Marshal 失败时返回 500。
func TestWriteRAWJSONMarshalError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		responsewriters.WriteRawJSON(http.StatusOK, &marshalError{errors.New("Undecodable")}, w)
	}))
	defer server.Close()
	client := http.Client{}
	resp, err := client.Get(server.URL)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	if resp.StatusCode != http.StatusInternalServerError {
		t.Errorf("unexpected status code %d", resp.StatusCode)
	}
}

// TestCreateTimeout 验证请求超时后返回 504 且原因为 Timeout。
func TestCreateTimeout(t *testing.T) {
	testOver := make(chan struct{})
	defer close(testOver)
	storage := SimpleRESTStorage{
		injectedFunction: func(obj runtime.Object) (runtime.Object, error) {
			// Eliminate flakes by ensuring the create operation takes longer than this test.
			<-testOver
			return obj, nil
		},
	}
	handler := handle(map[string]rest.Storage{
		"foo": &storage,
	})
	server := httptest.NewServer(handler)
	defer server.Close()

	simple := &genericapitesting.Simple{Other: "foo"}
	data, err := runtime.Encode(testCodec, simple)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	itemOut := expectAPIStatus(t, request.MethodPost, server.URL+"/"+prefix+"/"+testGroupVersion.Group+"/"+testGroupVersion.Version+"/namespaces/default/foo?timeout=4ms", data, http.StatusGatewayTimeout)
	if itemOut.Status != metav1.StatusFailure || itemOut.Reason != metav1.StatusReasonTimeout {
		t.Errorf("Unexpected status %#v", itemOut)
	}
}

// TestCreateChecksAPIVersion 验证请求体的 apiVersion 与路由不匹配时返回 400。
func TestCreateChecksAPIVersion(t *testing.T) {
	ctx := t.Context()
	handler := handle(map[string]rest.Storage{"simple": &SimpleRESTStorage{}})
	server := httptest.NewServer(handler)
	defer server.Close()
	client := http.Client{}

	simple := &genericapitesting.Simple{}
	//using newCodec and send the request to testVersion URL shall cause a discrepancy in apiVersion
	data, err := runtime.Encode(newCodec, simple)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	request, err := http.NewRequestWithContext(ctx, request.MethodPost, server.URL+"/"+prefix+"/"+testGroupVersion.Group+"/"+testGroupVersion.Version+"/namespaces/default/simple", bytes.NewBuffer(data))
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	response, err := client.Do(request)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	defer apitesting.Close(t, response.Body)
	if response.StatusCode != http.StatusBadRequest {
		t.Errorf("Unexpected response %#v", response)
	}
	b, err := io.ReadAll(response.Body)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	} else if !strings.Contains(string(b), "does not match the expected API version") {
		t.Errorf("unexpected response: %s", string(b))
	}
}

// TestCreateDefaultsAPIVersion 验证省略 apiVersion 时按路由的版本进行默认化。
func TestCreateDefaultsAPIVersion(t *testing.T) {
	ctx := t.Context()
	handler := handle(map[string]rest.Storage{"simple": &SimpleRESTStorage{}})
	server := httptest.NewServer(handler)
	defer server.Close()
	client := http.Client{}

	simple := &genericapitesting.Simple{}
	data, err := runtime.Encode(codec, simple)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	m := make(map[string]interface{})
	if err := json.Unmarshal(data, &m); err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	delete(m, "apiVersion")
	data, err = json.Marshal(m)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	request, err := http.NewRequestWithContext(ctx, request.MethodPost, server.URL+"/"+prefix+"/"+testGroupVersion.Group+"/"+testGroupVersion.Version+"/namespaces/default/simple", bytes.NewBuffer(data))
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	response, err := client.Do(request)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	defer apitesting.Close(t, response.Body)
	if response.StatusCode != http.StatusCreated {
		t.Errorf("unexpected status: %d, Expected: %d, %#v", response.StatusCode, http.StatusCreated, response)
	}
}

// TestUpdateChecksAPIVersion 验证更新时 apiVersion 不匹配会返回 400。
func TestUpdateChecksAPIVersion(t *testing.T) {
	ctx := t.Context()
	handler := handle(map[string]rest.Storage{"simple": &SimpleRESTStorage{}})
	server := httptest.NewServer(handler)
	defer server.Close()
	client := http.Client{}

	simple := &genericapitesting.Simple{ObjectMeta: metav1.ObjectMeta{Name: "bar"}}
	data, err := runtime.Encode(newCodec, simple)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	request, err := http.NewRequestWithContext(ctx, request.MethodPut, server.URL+"/"+prefix+"/"+testGroupVersion.Group+"/"+testGroupVersion.Version+"/namespaces/default/simple/bar", bytes.NewBuffer(data))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	response, err := client.Do(request)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer apitesting.Close(t, response.Body)
	if response.StatusCode != http.StatusBadRequest {
		t.Errorf("Unexpected response %#v", response)
	}
	b, err := io.ReadAll(response.Body)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	} else if !strings.Contains(string(b), "does not match the expected API version") {
		t.Errorf("unexpected response: %s", string(b))
	}
}

// runRequest is used by TestDryRun since it runs the test twice in a
// row with a slightly different URL (one has ?dryRun, one doesn't).
func runRequest(t testing.TB, path, verb string, data []byte, contentType string) *http.Response {
	ctx := t.Context()
	request, err := http.NewRequestWithContext(ctx, verb, path, bytes.NewBuffer(data))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if contentType != "" {
		request.Header.Set("Content-Type", contentType)
	}
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	return response
}

// SimpleRESTStorageWithDeleteCollection 在 SimpleRESTStorage 之上增加
// DeleteCollection，用于覆盖集合删除路径。
type SimpleRESTStorageWithDeleteCollection struct {
	SimpleRESTStorage
}

// Delete collection doesn't do much, but let us test this path.
func (storage *SimpleRESTStorageWithDeleteCollection) DeleteCollection(ctx context.Context, deleteValidation rest.ValidateObjectFunc, options *metav1.DeleteOptions, listOptions *metainternalversion.ListOptions) (runtime.Object, error) {
	storage.checkContext(ctx)
	return nil, nil
}

// shared vars used by both TestFieldValidation and BenchmarkFieldValidation
var (
	strictFieldValidation = "?fieldValidation=Strict"
	warnFieldValidation   = "?fieldValidation=Warn"
	ignoreFieldValidation = "?fieldValidation=Ignore"
)

// TestFieldValidation tests the create, update, and patch handlers for correctness when faced with field validation errors.
func TestFieldValidation(t *testing.T) {
	var (
		strictDecodingErr          = `strict decoding error: duplicate field \"other\", unknown field \"unknown\"`
		strictDecodingWarns        = []string{`duplicate field "other"`, `unknown field "unknown"`}
		strictDecodingErrYAML      = `strict decoding error: yaml: unmarshal errors:\n  line 5: key \"other\" already set in map, unknown field \"unknown\"`
		strictDecodingWarnsYAML    = []string{`line 5: key "other" already set in map`, `unknown field "unknown"`}
		strictDecodingErrYAMLPut   = `strict decoding error: yaml: unmarshal errors:\n  line 6: key \"other\" already set in map, unknown field \"unknown\"`
		strictDecodingWarnsYAMLPut = []string{`line 6: key "other" already set in map`, `unknown field "unknown"`}

		invalidJSONDataPost = []byte(`{"kind":"Simple", "apiVersion":"test.group/version", "metadata":{}, "other":"foo","other":"bar","unknown":"baz"}`)
		invalidYAMLDataPost = []byte(`apiVersion: test.group/version
kind: Simple
metadata: {}
other: foo
other: bar
unknown: baz`)

		invalidJSONDataPut = []byte(`{"kind":"Simple", "apiVersion":"test.group/version", "metadata":{"name":"id"}, "other":"foo","other":"bar","unknown":"baz"}`)
		invalidYAMLDataPut = []byte(`apiVersion: test.group/version
kind: Simple
metadata:
  name: id
other: foo
other: bar
unknown: baz`)

		invalidMergePatch = []byte(`{"labels":{"foo":"bar"}, "unknown": "foo", "other": "foo", "other": "bar"}`)
		invalidJSONPatch  = []byte(`
[
	{"op": "add", "path": "/unknown", "value": "foo"},
	{"op": "add", "path": "/other", "value": "foo"},
	{"op": "add", "path": "/other", "value": "bar"}
	]
	`)
		// note: duplicate fields in the patch itself
		// are dropped by the
		// evanphx/json-patch library and is expected.
		jsonPatchStrictDecodingErr   = `strict decoding error: unknown field \"unknown\"`
		jsonPatchStrictDecodingWarns = []string{`unknown field "unknown"`}

		invalidSMP = []byte(`{"unknown": "foo", "other":"foo", "other": "bar"}`)

		cases = []struct {
			name               string
			path               string
			verb               string
			data               []byte
			queryParams        string
			contentType        string
			expectedErr        string
			expectedWarns      []string
			expectedStatusCode int
		}{
			// Create
			{name: "post-strict-validation", path: "/namespaces/default/simples", verb: request.MethodPost, data: invalidJSONDataPost, queryParams: strictFieldValidation, expectedStatusCode: http.StatusBadRequest, expectedErr: strictDecodingErr},
			{name: "post-warn-validation", path: "/namespaces/default/simples", verb: request.MethodPost, data: invalidJSONDataPost, queryParams: warnFieldValidation, expectedStatusCode: http.StatusCreated, expectedWarns: strictDecodingWarns},
			{name: "post-ignore-validation", path: "/namespaces/default/simples", verb: request.MethodPost, data: invalidJSONDataPost, queryParams: ignoreFieldValidation, expectedStatusCode: http.StatusCreated},

			{name: "post-strict-validation-yaml", path: "/namespaces/default/simples", verb: request.MethodPost, data: invalidYAMLDataPost, queryParams: strictFieldValidation, contentType: "application/yaml", expectedStatusCode: http.StatusBadRequest, expectedErr: strictDecodingErrYAML},
			{name: "post-warn-validation-yaml", path: "/namespaces/default/simples", verb: request.MethodPost, data: invalidYAMLDataPost, queryParams: warnFieldValidation, contentType: "application/yaml", expectedStatusCode: http.StatusCreated, expectedWarns: strictDecodingWarnsYAML},
			{name: "post-ignore-validation-yaml", path: "/namespaces/default/simples", verb: request.MethodPost, data: invalidYAMLDataPost, queryParams: ignoreFieldValidation, contentType: "application/yaml", expectedStatusCode: http.StatusCreated},

			// Update
			{name: "put-strict-validation", path: "/namespaces/default/simples/id", verb: request.MethodPut, data: invalidJSONDataPut, queryParams: strictFieldValidation, expectedStatusCode: http.StatusBadRequest, expectedErr: strictDecodingErr},
			{name: "put-warn-validation", path: "/namespaces/default/simples/id", verb: request.MethodPut, data: invalidJSONDataPut, queryParams: warnFieldValidation, expectedStatusCode: http.StatusOK, expectedWarns: strictDecodingWarns},
			{name: "put-ignore-validation", path: "/namespaces/default/simples/id", verb: request.MethodPut, data: invalidJSONDataPut, queryParams: ignoreFieldValidation, expectedStatusCode: http.StatusOK},

			{name: "put-strict-validation-yaml", path: "/namespaces/default/simples/id", verb: request.MethodPut, data: invalidYAMLDataPut, queryParams: strictFieldValidation, contentType: "application/yaml", expectedStatusCode: http.StatusBadRequest, expectedErr: strictDecodingErrYAMLPut},
			{name: "put-warn-validation-yaml", path: "/namespaces/default/simples/id", verb: request.MethodPut, data: invalidYAMLDataPut, queryParams: warnFieldValidation, contentType: "application/yaml", expectedStatusCode: http.StatusOK, expectedWarns: strictDecodingWarnsYAMLPut},
			{name: "put-ignore-validation-yaml", path: "/namespaces/default/simples/id", verb: request.MethodPut, data: invalidYAMLDataPut, queryParams: ignoreFieldValidation, contentType: "application/yaml", expectedStatusCode: http.StatusOK},

			// MergePatch
			{name: "merge-patch-strict-validation", path: "/namespaces/default/simples/id", verb: request.MethodPatch, data: invalidMergePatch, queryParams: strictFieldValidation, contentType: "application/merge-patch+json; charset=UTF-8", expectedStatusCode: http.StatusUnprocessableEntity, expectedErr: strictDecodingErr},
			{name: "merge-patch-warn-validation", path: "/namespaces/default/simples/id", verb: request.MethodPatch, data: invalidMergePatch, queryParams: warnFieldValidation, contentType: "application/merge-patch+json; charset=UTF-8", expectedStatusCode: http.StatusOK, expectedWarns: strictDecodingWarns},
			{name: "merge-patch-ignore-validation", path: "/namespaces/default/simples/id", verb: request.MethodPatch, data: invalidMergePatch, queryParams: ignoreFieldValidation, contentType: "application/merge-patch+json; charset=UTF-8", expectedStatusCode: http.StatusOK},

			// JSON Patch
			{name: "json-patch-strict-validation", path: "/namespaces/default/simples/id", verb: request.MethodPatch, data: invalidJSONPatch, queryParams: strictFieldValidation, contentType: "application/json-patch+json; charset=UTF-8", expectedStatusCode: http.StatusUnprocessableEntity, expectedErr: jsonPatchStrictDecodingErr},
			{name: "json-patch-warn-validation", path: "/namespaces/default/simples/id", verb: request.MethodPatch, data: invalidJSONPatch, queryParams: warnFieldValidation, contentType: "application/json-patch+json; charset=UTF-8", expectedStatusCode: http.StatusOK, expectedWarns: jsonPatchStrictDecodingWarns},
			{name: "json-patch-ignore-validation", path: "/namespaces/default/simples/id", verb: request.MethodPatch, data: invalidJSONPatch, queryParams: ignoreFieldValidation, contentType: "application/json-patch+json; charset=UTF-8", expectedStatusCode: http.StatusOK},

			// SMP
			{name: "strategic-merge-patch-strict-validation", path: "/namespaces/default/simples/id", verb: request.MethodPatch, data: invalidSMP, queryParams: strictFieldValidation, contentType: "application/strategic-merge-patch+json; charset=UTF-8", expectedStatusCode: http.StatusUnprocessableEntity, expectedErr: strictDecodingErr},
			{name: "strategic-merge-patch-warn-validation", path: "/namespaces/default/simples/id", verb: request.MethodPatch, data: invalidSMP, queryParams: warnFieldValidation, contentType: "application/strategic-merge-patch+json; charset=UTF-8", expectedStatusCode: http.StatusOK, expectedWarns: strictDecodingWarns},
			{name: "strategic-merge-patch-ignore-validation", path: "/namespaces/default/simples/id", verb: request.MethodPatch, data: invalidSMP, queryParams: ignoreFieldValidation, contentType: "application/strategic-merge-patch+json; charset=UTF-8", expectedStatusCode: http.StatusOK},
		}
	)

	server := httptest.NewServer(handleWithWarnings(map[string]rest.Storage{
		"simples": &SimpleRESTStorageWithDeleteCollection{
			SimpleRESTStorage{
				item: genericapitesting.Simple{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "id",
						Namespace: "",
						UID:       "uid",
					},
					Other: "baz",
				},
			},
		},
		"simples/subsimple": &SimpleXGSubresourceRESTStorage{
			item: genericapitesting.SimpleXGSubresource{
				SubresourceInfo: "foo",
			},
			itemGVK: testGroup2Version.WithKind("SimpleXGSubresource"),
		},
	}))
	defer server.Close()
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			baseURL := server.URL + "/" + prefix + "/" + testGroupVersion.Group + "/" + testGroupVersion.Version
			response := runRequest(t, baseURL+test.path+test.queryParams, test.verb, test.data, test.contentType)
			defer apitesting.Close(t, response.Body)
			buf := new(bytes.Buffer)
			buf.ReadFrom(response.Body)

			if response.StatusCode != test.expectedStatusCode || !strings.Contains(buf.String(), test.expectedErr) {
				t.Fatalf("unexpected response: %#v, expected err: %#v", response, test.expectedErr)
			}

			warnings, _ := net.ParseWarningHeaders(response.Header["Warning"])
			if len(warnings) != len(test.expectedWarns) {
				t.Fatalf("unexpected number of warnings. Got count %d, expected %d. Got warnings %#v, expected %#v", len(warnings), len(test.expectedWarns), warnings, test.expectedWarns)

			}
			for i, warn := range warnings {
				if warn.Text != test.expectedWarns[i] {
					t.Fatalf("unexpected warning: %#v, expected warning: %#v", warn.Text, test.expectedWarns[i])
				}
			}
		})
	}
}

// BenchmarkFieldValidation benchmarks the create, update, and patch handlers for performance distinctions between
// strict, warn, and ignore field validation handling.
func BenchmarkFieldValidation(b *testing.B) {
	var (
		validJSONDataPost = []byte(`{"kind":"Simple", "apiVersion":"test.group/version", "metadata":{}, "other":"foo"}`)
		validYAMLDataPost = []byte(`apiVersion: test.group/version
kind: Simple
metadata: {}
other: foo`)

		validJSONDataPut = []byte(`{"kind":"Simple", "apiVersion":"test.group/version", "metadata":{"name":"id"}, "other":"bar"}`)
		validYAMLDataPut = []byte(`apiVersion: test.group/version
kind: Simple
metadata:
  name: id
other: bar`)

		validMergePatch = []byte(`{"labels":{"foo":"bar"}, "other": "bar"}`)
		validJSONPatch  = []byte(`
[
	{"op": "add", "path": "/other", "value": "bar"}
	]
	`)
		validSMP = []byte(`{"other": "bar"}`)

		fieldValidationBenchmarks = []struct {
			name               string
			path               string
			verb               string
			data               []byte
			queryParams        string
			contentType        string
			expectedStatusCode int
		}{
			// Create
			{name: "post-strict-validation", path: "/namespaces/default/simples", verb: request.MethodPost, data: validJSONDataPost, queryParams: strictFieldValidation, expectedStatusCode: http.StatusCreated},
			{name: "post-warn-validation", path: "/namespaces/default/simples", verb: request.MethodPost, data: validJSONDataPost, queryParams: warnFieldValidation, expectedStatusCode: http.StatusCreated},
			{name: "post-ignore-validation", path: "/namespaces/default/simples", verb: request.MethodPost, data: validJSONDataPost, queryParams: ignoreFieldValidation, expectedStatusCode: http.StatusCreated},

			{name: "post-strict-validation-yaml", path: "/namespaces/default/simples", verb: request.MethodPost, data: validYAMLDataPost, queryParams: strictFieldValidation, contentType: "application/yaml", expectedStatusCode: http.StatusCreated},
			{name: "post-warn-validation-yaml", path: "/namespaces/default/simples", verb: request.MethodPost, data: validYAMLDataPost, queryParams: warnFieldValidation, contentType: "application/yaml", expectedStatusCode: http.StatusCreated},
			{name: "post-ignore-validation-yaml", path: "/namespaces/default/simples", verb: request.MethodPost, data: validYAMLDataPost, queryParams: ignoreFieldValidation, contentType: "application/yaml", expectedStatusCode: http.StatusCreated},

			// Update
			{name: "put-strict-validation", path: "/namespaces/default/simples/id", verb: request.MethodPut, data: validJSONDataPut, queryParams: strictFieldValidation, expectedStatusCode: http.StatusOK},
			{name: "put-warn-validation", path: "/namespaces/default/simples/id", verb: request.MethodPut, data: validJSONDataPut, queryParams: warnFieldValidation, expectedStatusCode: http.StatusOK},
			{name: "put-ignore-validation", path: "/namespaces/default/simples/id", verb: request.MethodPut, data: validJSONDataPut, queryParams: ignoreFieldValidation, expectedStatusCode: http.StatusOK},

			{name: "put-strict-validation-yaml", path: "/namespaces/default/simples/id", verb: request.MethodPut, data: validYAMLDataPut, queryParams: strictFieldValidation, contentType: "application/yaml", expectedStatusCode: http.StatusOK},
			{name: "put-warn-validation-yaml", path: "/namespaces/default/simples/id", verb: request.MethodPut, data: validYAMLDataPut, queryParams: warnFieldValidation, contentType: "application/yaml", expectedStatusCode: http.StatusOK},
			{name: "put-ignore-validation-yaml", path: "/namespaces/default/simples/id", verb: request.MethodPut, data: validYAMLDataPut, queryParams: ignoreFieldValidation, contentType: "application/yaml", expectedStatusCode: http.StatusOK},

			// MergePatch
			{name: "merge-patch-strict-validation", path: "/namespaces/default/simples/id", verb: request.MethodPatch, data: validMergePatch, queryParams: strictFieldValidation, contentType: "application/merge-patch+json; charset=UTF-8", expectedStatusCode: http.StatusOK},
			{name: "merge-patch-warn-validation", path: "/namespaces/default/simples/id", verb: request.MethodPatch, data: validMergePatch, queryParams: warnFieldValidation, contentType: "application/merge-patch+json; charset=UTF-8", expectedStatusCode: http.StatusOK},
			{name: "merge-patch-ignore-validation", path: "/namespaces/default/simples/id", verb: request.MethodPatch, data: validMergePatch, queryParams: ignoreFieldValidation, contentType: "application/merge-patch+json; charset=UTF-8", expectedStatusCode: http.StatusOK},

			// JSON Patch
			{name: "json-patch-strict-validation", path: "/namespaces/default/simples/id", verb: request.MethodPatch, data: validJSONPatch, queryParams: strictFieldValidation, contentType: "application/json-patch+json; charset=UTF-8", expectedStatusCode: http.StatusOK},
			{name: "json-patch-warn-validation", path: "/namespaces/default/simples/id", verb: request.MethodPatch, data: validJSONPatch, queryParams: warnFieldValidation, contentType: "application/json-patch+json; charset=UTF-8", expectedStatusCode: http.StatusOK},
			{name: "json-patch-ignore-validation", path: "/namespaces/default/simples/id", verb: request.MethodPatch, data: validJSONPatch, queryParams: ignoreFieldValidation, contentType: "application/json-patch+json; charset=UTF-8", expectedStatusCode: http.StatusOK},

			// SMP
			{name: "strategic-merge-patch-strict-validation", path: "/namespaces/default/simples/id", verb: request.MethodPatch, data: validSMP, queryParams: strictFieldValidation, contentType: "application/strategic-merge-patch+json; charset=UTF-8", expectedStatusCode: http.StatusOK},
			{name: "strategic-merge-patch-warn-validation", path: "/namespaces/default/simples/id", verb: request.MethodPatch, data: validSMP, queryParams: warnFieldValidation, contentType: "application/strategic-merge-patch+json; charset=UTF-8", expectedStatusCode: http.StatusOK},
			{name: "strategic-merge-patch-ignore-validation", path: "/namespaces/default/simples/id", verb: request.MethodPatch, data: validSMP, queryParams: ignoreFieldValidation, contentType: "application/strategic-merge-patch+json; charset=UTF-8", expectedStatusCode: http.StatusOK},
		}
	)

	server := httptest.NewServer(handleWithWarnings(map[string]rest.Storage{
		"simples": &SimpleRESTStorageWithDeleteCollection{
			SimpleRESTStorage{
				item: genericapitesting.Simple{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "id",
						Namespace: "",
						UID:       "uid",
					},
					Other: "bar",
				},
			},
		},
		"simples/subsimple": &SimpleXGSubresourceRESTStorage{
			item: genericapitesting.SimpleXGSubresource{
				SubresourceInfo: "foo",
			},
			itemGVK: testGroup2Version.WithKind("SimpleXGSubresource"),
		},
	}))
	defer server.Close()
	for _, test := range fieldValidationBenchmarks {
		b.Run(test.name, func(b *testing.B) {
			for n := 0; n < b.N; n++ {
				baseURL := server.URL + "/" + prefix + "/" + testGroupVersion.Group + "/" + testGroupVersion.Version
				response := runRequest(b, baseURL+test.path+test.queryParams, test.verb, test.data, test.contentType)
				defer apitesting.Close(b, response.Body)
				if response.StatusCode != test.expectedStatusCode {
					b.Fatalf("unexpected status code: %d, expected: %d", response.StatusCode, test.expectedStatusCode)
				}
			}
		})
	}
}

// SimpleXGSubresourceRESTStorage 是一个跨组子资源存储：其返回对象的 GVK
// 与父资源不同，用于验证 GroupVersionKindProvider 的处理。
type SimpleXGSubresourceRESTStorage struct {
	item    genericapitesting.SimpleXGSubresource
	itemGVK schema.GroupVersionKind
}

var _ = rest.GroupVersionKindProvider(&SimpleXGSubresourceRESTStorage{})

func (storage *SimpleXGSubresourceRESTStorage) New() runtime.Object {
	return &genericapitesting.SimpleXGSubresource{}
}

func (storage *SimpleXGSubresourceRESTStorage) Destroy() {
}

func (storage *SimpleXGSubresourceRESTStorage) Get(ctx context.Context, id string, options *metav1.GetOptions) (runtime.Object, error) {
	return storage.item.DeepCopyObject(), nil
}

func (storage *SimpleXGSubresourceRESTStorage) Delete(ctx context.Context, name string, deleteValidation rest.ValidateObjectFunc, options *metav1.DeleteOptions) (runtime.Object, bool, error) {
	return nil, true, nil
}

func (storage *SimpleXGSubresourceRESTStorage) GroupVersionKind(containingGV schema.GroupVersion) schema.GroupVersionKind {
	return storage.itemGVK
}

func (storage *SimpleXGSubresourceRESTStorage) GetSingularName() string {
	return "simple"
}

// TestXGSubresource 验证跨组子资源返回的 apiVersion/kind 来自存储声明的 GVK，
// 而不是父资源的 GVK。
func TestXGSubresource(t *testing.T) {
	ctx := t.Context()
	container := restful.NewContainer()
	container.Router(restful.CurlyRouter{})
	mux := container.ServeMux

	itemID := "theID"
	subresourceStorage := &SimpleXGSubresourceRESTStorage{
		item: genericapitesting.SimpleXGSubresource{
			SubresourceInfo: "foo",
		},
		itemGVK: testGroup2Version.WithKind("SimpleXGSubresource"),
	}
	storage := map[string]rest.Storage{
		"simple":           &SimpleRESTStorage{},
		"simple/subsimple": subresourceStorage,
	}

	group := APIGroupVersion{
		Storage: storage,

		Creater:         scheme,
		Convertor:       scheme,
		TypeConverter:   managedfields.NewDeducedTypeConverter(),
		UnsafeConvertor: runtime.UnsafeObjectConvertor(scheme),
		Defaulter:       scheme,
		Typer:           scheme,
		Namer:           namer,

		EquivalentResourceRegistry: runtime.NewEquivalentResourceRegistry(),

		ParameterCodec: parameterCodec,

		Admit: admissionControl,

		Root:                   "/" + prefix,
		GroupVersion:           testGroupVersion,
		OptionsExternalVersion: &testGroupVersion,
		Serializer:             codecs,
	}

	if _, _, err := (&group).InstallREST(container); err != nil {
		panic(fmt.Sprintf("unable to install container %s: %v", group.GroupVersion, err))
	}

	server := newTestServer(defaultAPIServer{mux, container})
	defer server.Close()

	url := server.URL + "/" + prefix + "/" + testGroupVersion.Group + "/" + testGroupVersion.Version + "/namespaces/default/simple/" + itemID + "/subsimple"
	req, err := http.NewRequestWithContext(ctx, request.MethodGet, url, nil)
	require.NoError(t, err)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer apitesting.Close(t, resp.Body)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("unexpected response: %#v", resp)
	}
	var itemOut genericapitesting.SimpleXGSubresource
	body, err := extractBody(resp, &itemOut)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	// Test if the returned object has the expected group, version and kind
	// We are directly unmarshaling JSON here because TypeMeta cannot be decoded through the
	// installed decoders. TypeMeta cannot be decoded because it is added to the ignored
	// conversion type list in API scheme and hence cannot be converted from input type object
	// to output type object. So it's values don't appear in the decoded output object.
	decoder := json.NewDecoder(strings.NewReader(body))
	var itemFromBody genericapitesting.SimpleXGSubresource
	err = decoder.Decode(&itemFromBody)
	if err != nil {
		t.Errorf("unexpected JSON decoding error: %v", err)
	}
	if want := fmt.Sprintf("%s/%s", testGroup2Version.Group, testGroup2Version.Version); itemFromBody.APIVersion != want {
		t.Errorf("unexpected APIVersion got: %+v want: %+v", itemFromBody.APIVersion, want)
	}
	if itemFromBody.Kind != "SimpleXGSubresource" {
		t.Errorf("unexpected Kind got: %+v want: SimpleXGSubresource", itemFromBody.Kind)
	}

	if itemOut.Name != subresourceStorage.item.Name {
		t.Errorf("Unexpected data: %#v, expected %#v (%s)", itemOut, subresourceStorage.item, string(body))
	}
}

// readBodyOrDie 读取全部内容，失败即 panic（仅用于测试）。
func readBodyOrDie(r io.Reader) []byte {
	body, err := io.ReadAll(r)
	if err != nil {
		panic(err)
	}
	return body
}

// BenchmarkUpdateProtobuf measures the cost of processing an update on the server in proto
func BenchmarkUpdateProtobuf(b *testing.B) {
	ctx := b.Context()
	items := benchmarkItems(b)

	simpleStorage := &SimpleRESTStorage{}
	handler := handle(map[string]rest.Storage{"simples": simpleStorage})
	server := httptest.NewServer(handler)
	defer server.Close()
	client := http.Client{}

	dest, _ := url.Parse(server.URL)
	dest.Path = "/" + prefix + "/" + newGroupVersion.Group + "/" + newGroupVersion.Version + "/namespaces/foo/simples/bar"
	dest.RawQuery = ""

	info, _ := runtime.SerializerInfoForMediaType(codecs.SupportedMediaTypes(), "application/vnd.kubernetes.protobuf")
	e := codecs.EncoderForVersion(info.Serializer, newGroupVersion)
	data, err := runtime.Encode(e, &items[0])
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Use a closure to execute defers before continuing
		func() {
			request, err := http.NewRequestWithContext(ctx, request.MethodPut, dest.String(), bytes.NewReader(data))
			if err != nil {
				b.Fatalf("unexpected error: %v", err)
			}
			request.Header.Set("Accept", "application/vnd.kubernetes.protobuf")
			request.Header.Set("Content-Type", "application/vnd.kubernetes.protobuf")
			response, err := client.Do(request)
			if err != nil {
				b.Fatalf("unexpected error: %v", err)
			}
			defer apitesting.Close(b, response.Body)
			if response.StatusCode != http.StatusBadRequest {
				body, _ := io.ReadAll(response.Body)
				b.Fatalf("Unexpected response %#v\n%s", response, body)
			}
			_, _ = io.ReadAll(response.Body)
		}()
	}
	b.StopTimer()
}

// newTestServer 为 handler 套上 RequestInfo 过滤器后启动测试服务器。
func newTestServer(handler http.Handler) *httptest.Server {
	handler = genericapifilters.WithRequestInfo(handler, newTestRequestInfoResolver())
	return httptest.NewServer(handler)
}

// newTestRequestInfoResolver 返回另一份测试用 RequestInfo 解析器。
func newTestRequestInfoResolver() *request.RequestInfoFactory {
	return &request.RequestInfoFactory{
		APIPrefixes:          sets.NewString("api", "apis"),
		GrouplessAPIPrefixes: sets.NewString("api"),
	}
}

const benchmarkSeed = 100

// benchmarkItems 用固定随机种子生成可复现的基准测试数据。
func benchmarkItems(b *testing.B) []example.Pod {
	clientapiObjectFuzzer := fuzzer.FuzzerFor(examplefuzzer.Funcs, rand.NewSource(benchmarkSeed), codecs)
	items := make([]example.Pod, 3)
	for i := range items {
		clientapiObjectFuzzer.Fill(&items[i])
	}
	return items
}
