/*
Copyright 2020 The Kubernetes Authors.

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

package internal_test

import (
	"fmt"
	"reflect"
	"testing"

	apiequality "k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/managedfields/internal"
	"k8s.io/apimachinery/pkg/util/managedfields/managedfieldstest"
	yamlutil "k8s.io/apimachinery/pkg/util/yaml"
	"sigs.k8s.io/structured-merge-diff/v6/fieldpath"
	"sigs.k8s.io/structured-merge-diff/v6/merge"
	"sigs.k8s.io/yaml"
)

// testArgs 描述一个 apply 冲突场景：last-applied 注解（客户端 apply 遗留）、
// 已存在对象、请求 apply 的对象，以及期望的冲突字段集合。expectConflictSet
// 为 nil 表示期望成功。
type testArgs struct {
	lastApplied       []byte
	original          []byte
	applied           []byte
	fieldManager      string
	expectConflictSet *fieldpath.Set
}

// TestApplyUsingLastAppliedAnnotation tests that applying to an object
// created with the client-side apply last-applied annotation
// will not give conflicts
//
// 中文说明：复用同一 kubectl manager 时，只有 last-applied 中未声明的字段
// 变更才会冲突；不同 manager 则要把所有被他人改过的字段都视为冲突。
// 用例还覆盖 apiVersion 不匹配与非法 last-applied 的降级行为。
func TestApplyUsingLastAppliedAnnotation(t *testing.T) {
	f := managedfieldstest.NewTestFieldManager(fakeTypeConverter, schema.FromAPIVersionAndKind("apps/v1", "Deployment"))

	tests := []testArgs{
		{
			fieldManager: "kubectl",
			lastApplied: []byte(`
apiVersion: apps/v1
kind: Deployment
metadata:
  name: my-deployment
spec:
  replicas: 3
  selector:
    matchLabels:
      app: my-app
  template:
    metadata:
      labels:
        app: my-app
    spec:
      containers:
      - name: my-c
        image: my-image-v1
      - name: my-c2
        image: my-image2
`),
			original: []byte(`
apiVersion: apps/v1
kind: Deployment
metadata:
  name: my-deployment
  labels:
    app: my-app # missing from last-applied
spec:
  replicas: 100 # does not match last-applied
  selector:
    matchLabels:
      app: my-app
  template:
    metadata:
      labels:
        app: my-app
    spec:
      containers:
      - name: my-c
        image: my-image-v2 # does no match last-applied
      # note that second container in last-applied is missing
`),
			applied: []byte(`
# test conflicts due to fields not allowed by last-applied

apiVersion: apps/v1
kind: Deployment
metadata:
  name: my-deployment
  labels:
    app: my-new-label # NOT allowed: update label
spec:
  replicas: 333 # NOT allowed: update replicas
  selector:
    matchLabels:
      app: my-new-label # allowed: update label
  template:
    metadata:
      labels:
        app: my-new-label # allowed: update-label
    spec:
      containers:
      - name: my-c
        image: my-image-new # NOT allowed: update image
`),
			expectConflictSet: fieldpath.NewSet(
				fieldpath.MakePathOrDie("metadata", "labels", "app"),
				fieldpath.MakePathOrDie("spec", "replicas"),
				fieldpath.MakePathOrDie("spec", "template", "spec", "containers", fieldpath.KeyByFields("name", "my-c"), "image"),
			),
		},
		{
			fieldManager: "kubectl",
			lastApplied: []byte(`
apiVersion: apps/v1
kind: Deployment
metadata:
  name: my-deployment
  labels:
    app: my-app
spec:
  replicas: 3
  selector:
    matchLabels:
      app: my-app
  template:
    metadata:
      labels:
        app: my-app
    spec:
      containers:
      - name: my-c
        image: my-image
`),
			original: []byte(`
apiVersion: apps/v1
kind: Deployment
metadata:
  name: my-deployment
  labels:
    app: my-app
spec:
  replicas: 100 # does not match last applied
  selector:
    matchLabels:
      app: my-app
  template:
    metadata:
      labels:
        app: my-app
    spec:
      containers:
      - name: my-c
        image: my-image
`),
			applied: []byte(`
apiVersion: apps/v1
kind: Deployment
metadata:
  name: my-deployment
  labels:
    app: my-new-label
spec:
  replicas: 3 # expect conflict
  template:
    metadata:
      labels:
        app: my-app
    spec:
      containers:
      - name: my-c
        image: my-image
`),
			expectConflictSet: fieldpath.NewSet(
				fieldpath.MakePathOrDie("spec", "replicas"),
			),
		},
		{
			fieldManager: "kubectl",
			original: []byte(`
apiVersion: apps/v1
kind: Deployment
metadata:
  name: my-deployment
  labels:
    app: my-app
spec:
  replicas: 100
  selector:
    matchLabels:
      app: my-app
  template:
    metadata:
      labels:
        app: my-app
    spec:
      containers:
      - name: my-c
        image: my-image
`),
			applied: []byte(`
# applied object matches original

apiVersion: apps/v1
kind: Deployment
metadata:
  name: my-deployment
  labels:
    app: my-app
spec:
  replicas: 100
  selector:
    matchLabels:
      app: my-app
  template:
    metadata:
      labels:
        app: my-app
    spec:
      containers:
      - name: my-c
        image: my-image
`),
		},
		{
			fieldManager: "kubectl",
			original: []byte(`
apiVersion: apps/v1
kind: Deployment
metadata:
  name: my-deployment
  labels:
    app: my-app
spec:
  replicas: 3
  selector:
    matchLabels:
      app: my-app
  template:
    metadata:
      labels:
        app: my-app
    spec:
      containers:
      - name: my-c
        image: my-image
`),
			applied: []byte(`
# test allowed update with no conflicts

apiVersion: apps/v1
kind: Deployment
metadata:
  name: my-deployment
  labels:
    app: my-new-label # update label
spec:
  replicas: 333 # update replicas
  selector:
    matchLabels:
      app: my-new-label # update label
  template:
    metadata:
      labels:
        app: my-new-label # update-label
    spec:
      containers:
      - name: my-c
        image: my-image
`),
		},
		{
			fieldManager: "not_kubectl",
			lastApplied: []byte(`
# expect conflicts because field manager is NOT kubectl

apiVersion: apps/v1
kind: Deployment
metadata:
  name: my-deployment
  labels:
    app: my-app
spec:
  replicas: 3
  selector:
    matchLabels:
      app: my-app
  template:
    metadata:
      labels:
        app: my-app
    spec:
      containers:
      - name: my-c
        image: my-image-v1
`),
			original: []byte(`
apiVersion: apps/v1
kind: Deployment
metadata:
  name: my-deployment
  labels:
    app: my-app
spec:
  replicas: 100 # does not match last-applied
  selector:
    matchLabels:
      app: my-app
  template:
    metadata:
      labels:
        app: my-app
    spec:
      containers:
      - name: my-c
        image: my-image-v2 # does no match last-applied
`),
			applied: []byte(`
# test conflicts due to fields not allowed by last-applied

apiVersion: apps/v1
kind: Deployment
metadata:
  name: my-deployment
  labels:
    app: my-new-label # update label
spec:
  replicas: 333 # update replicas
  selector:
    matchLabels:
      app: my-new-label # update label
  template:
    metadata:
      labels:
        app: my-new-label # update-label
    spec:
      containers:
      - name: my-c
        image: my-image-new # update image
`),
			expectConflictSet: fieldpath.NewSet(
				fieldpath.MakePathOrDie("metadata", "labels", "app"),
				fieldpath.MakePathOrDie("spec", "replicas"),
				fieldpath.MakePathOrDie("spec", "selector"), // selector is atomic
				fieldpath.MakePathOrDie("spec", "template", "metadata", "labels", "app"),
				fieldpath.MakePathOrDie("spec", "template", "spec", "containers", fieldpath.KeyByFields("name", "my-c"), "image"),
			),
		},
		{
			fieldManager: "kubectl",
			original: []byte(`
apiVersion: apps/v1
kind: Deployment
metadata:
  name: my-deployment
  labels:
    app: my-app
spec:
  replicas: 3
  selector:
    matchLabels:
      app: my-app
  template:
    metadata:
      labels:
        app: my-app
    spec:
      containers:
      - name: my-c
        image: my-image
`),
			applied: []byte(`
# test allowed update with no conflicts

apiVersion: apps/v1
kind: Deployment
metadata:
  name: my-deployment
  labels:
    app: my-new-label
spec:
  replicas: 3
  selector:
    matchLabels:
      app: my-app
  template:
    metadata:
      labels:
        app: my-app
    spec:
      containers:
      - name: my-c
        image: my-new-image # update image
`),
		},
		{
			fieldManager: "not_kubectl",
			original: []byte(`
apiVersion: apps/v1
kind: Deployment
metadata:
  name: my-deployment
  labels:
    app: my-app
spec:
  replicas: 100
  selector:
    matchLabels:
      app: my-app
  template:
    metadata:
      labels:
        app: my-app
    spec:
      containers:
      - name: my-c
        image: my-image
`),
			applied: []byte(`

# expect changes to fail because field manager is not kubectl

apiVersion: apps/v1
kind: Deployment
metadata:
  name: my-deployment
  labels:
    app: my-new-label # update label
spec:
  replicas: 3 # update replicas
  selector:
    matchLabels:
      app: my-app
  template:
    metadata:
      labels:
        app: my-app
    spec:
      containers:
      - name: my-c
        image: my-new-image # update image
`),
			expectConflictSet: fieldpath.NewSet(
				fieldpath.MakePathOrDie("metadata", "labels", "app"),
				fieldpath.MakePathOrDie("spec", "replicas"),
				fieldpath.MakePathOrDie("spec", "template", "spec", "containers", fieldpath.KeyByFields("name", "my-c"), "image"),
			),
		},
		{
			fieldManager: "kubectl",
			original: []byte(`
apiVersion: apps/v1
kind: Deployment
metadata:
  name: my-deployment
spec:
  replicas: 3
`),
			applied: []byte(`
apiVersion: apps/v1
kind: Deployment
metadata:
  name: my-deployment
spec:
  replicas: 100 # update replicas
`),
		},
		{
			fieldManager: "kubectl",
			lastApplied: []byte(`
apiVersion: extensions/v1beta1
kind: Deployment
metadata:
  name: my-deployment
spec:
  replicas: 3
`),
			original: []byte(`
apiVersion: apps/v1 # expect conflict due to apiVersion mismatch with last-applied
kind: Deployment
metadata:
  name: my-deployment
spec:
  replicas: 3
`),
			applied: []byte(`
apiVersion: apps/v1
kind: Deployment
metadata:
  name: my-deployment
spec:
  replicas: 100 # update replicas
`),
			expectConflictSet: fieldpath.NewSet(
				fieldpath.MakePathOrDie("spec", "replicas"),
			),
		},
		{
			fieldManager: "kubectl",
			lastApplied: []byte(`
apiVerison: foo
kind: bar
spec: expect conflict due to invalid object
`),
			original: []byte(`
apiVersion: apps/v1
kind: Deployment
metadata:
  name: my-deployment
spec:
  replicas: 3
`),
			applied: []byte(`
apiVersion: apps/v1
kind: Deployment
metadata:
  name: my-deployment
spec:
  replicas: 100 # update replicas
`),
			expectConflictSet: fieldpath.NewSet(
				fieldpath.MakePathOrDie("spec", "replicas"),
			),
		},
		{
			fieldManager: "kubectl",
			// last-applied is empty
			lastApplied: []byte{},
			original: []byte(`
apiVersion: apps/v1
kind: Deployment
metadata:
  name: my-deployment
spec:
  replicas: 3
`),
			applied: []byte(`
apiVersion: apps/v1
kind: Deployment
metadata:
  name: my-deployment
spec:
  replicas: 100 # update replicas
`),
			expectConflictSet: fieldpath.NewSet(
				fieldpath.MakePathOrDie("spec", "replicas"),
			),
		},
	}

	testConflicts(t, f, tests)
}

// TestServiceApply 验证 Service 的 apply：selector 是原子字段，修改或清空会
// 以整体为冲突单位；同一 manager 可以清除 selector，不同 manager 不能。
func TestServiceApply(t *testing.T) {
	f := managedfieldstest.NewTestFieldManager(fakeTypeConverter, schema.FromAPIVersionAndKind("v1", "Service"))

	tests := []testArgs{
		{
			fieldManager: "kubectl",
			original: []byte(`
apiVersion: v1
kind: Service
metadata:
  name: test
spec:
  ports:
  - name: https
    port: 443
    protocol: TCP
    targetPort: 8443
  selector:
    old: test
`),
			applied: []byte(`
# All accepted while using the same field manager

apiVersion: v1
kind: Service
metadata:
  name: test
spec:
  ports:
  - name: https
    port: 443
    protocol: TCP
    targetPort: 8444
  selector:
    new: test
`),
		},
		{
			fieldManager: "kubectl",
			original: []byte(`
apiVersion: v1
kind: Service
metadata:
  name: test
spec:
  ports:
  - name: https
    port: 443
    protocol: TCP
    targetPort: 8443
  selector:
    old: test
`),
			applied: []byte(`
# Allowed to remove selectors while using the same field manager

apiVersion: v1
kind: Service
metadata:
  name: test
spec:
  ports:
  - name: https
    port: 443
    protocol: TCP
    targetPort: 8444
  selector: {}
`),
		},
		{
			fieldManager: "not_kubectl",
			original: []byte(`
apiVersion: v1
kind: Service
metadata:
  name: test
spec:
  ports:
  - name: https
    port: 443
    protocol: TCP # TODO: issue - this is a defaulted field, should not be required in a new spec
    targetPort: 8443
  selector:
    old: test
`),
			applied: []byte(`
# test selector update not allowed by last-applied

apiVersion: v1
kind: Service
metadata:
  name: test
spec:
  ports:
  - name: https
    port: 443
    protocol: TCP
    targetPort: 8444
  selector:
    new: test
`),
			expectConflictSet: fieldpath.NewSet(
				fieldpath.MakePathOrDie("spec", "selector"), // selector is atomic
				fieldpath.MakePathOrDie("spec", "ports", fieldpath.KeyByFields("port", 443, "protocol", "TCP"), "targetPort"),
			),
		},
	}

	testConflicts(t, f, tests)
}

// TestReplicationControllerApply 与 Service 类似：同一 manager 可更新
// replicas 与 selector，不同 manager 则两者都冲突。
func TestReplicationControllerApply(t *testing.T) {
	f := managedfieldstest.NewTestFieldManager(fakeTypeConverter, schema.FromAPIVersionAndKind("v1", "ReplicationController"))

	tests := []testArgs{
		{
			fieldManager: "kubectl",
			original: []byte(`
apiVersion: v1
kind: ReplicationController
metadata:
  name: test
spec:
  replicas: 0
  selector:
    old: test
`),
			applied: []byte(`
# All accepted while using the same field manager

apiVersion: v1
kind: ReplicationController
metadata:
  name: test
spec:
  replicas: 3
  selector:
    new: test
`),
		},
		{
			fieldManager: "not_kubectl",
			original: []byte(`
apiVersion: v1
kind: ReplicationController
metadata:
  name: test
spec:
  replicas: 0
  selector:
    old: test
`),
			applied: []byte(`
# test selector update not allowed by last-applied

apiVersion: v1
kind: ReplicationController
metadata:
  name: test
spec:
  replicas: 3
  selector:
    new: test
`),
			expectConflictSet: fieldpath.NewSet(
				fieldpath.MakePathOrDie("spec", "selector"), // selector is atomic
				fieldpath.MakePathOrDie("spec", "replicas"),
			),
		},
	}

	testConflicts(t, f, tests)
}

// TestPodApply 验证 Pod 的 nodeSelector/nodeName 更新：同一 manager 可自由
// 修改；不同 manager 修改原子字段 nodeSelector（含清空）或 nodeName 会冲突。
func TestPodApply(t *testing.T) {
	f := managedfieldstest.NewTestFieldManager(fakeTypeConverter, schema.FromAPIVersionAndKind("v1", "Pod"))

	tests := []testArgs{
		{
			fieldManager: "kubectl",
			original: []byte(`
apiVersion: v1
kind: Pod
metadata:
  name: test
  namespace: test
spec:
  containers:
  - args:
    - -v=2
    command:
    - controller
    image: some.registry/app:latest
    name: doJob
  nodeName: definetlyControlPlane
  nodeSelector:
    node-role.kubernetes.io/master: ""
`),
			applied: []byte(`
# All accepted while using the same field manager

apiVersion: v1
kind: Pod
metadata:
  name: test
  namespace: test
spec:
  containers:
  - args:
    - -v=2
    command:
    - controller
    image: some.registry/app:latest
    name: doJob
  nodeSelector:
    node-role.kubernetes.io/worker: ""
`),
		},
		{
			fieldManager: "not_kubectl",
			original: []byte(`
apiVersion: v1
kind: Pod
metadata:
  name: test
  namespace: test
spec:
  containers:
  - args:
    - -v=2
    command:
    - controller
    image: some.registry/app:latest
    name: doJob
  nodeName: definetlyControlPlane
  nodeSelector:
    node-role.kubernetes.io/master: ""
`),
			applied: []byte(`
# test selector update not allowed by last-applied

apiVersion: v1
kind: Pod
metadata:
  name: test
  namespace: test
spec:
  containers:
  - args:
    - -v=2
    command:
    - controller
    image: some.registry/app:latest
    name: doJob
  nodeName: definetlyControlPlane
  nodeSelector:
    node-role.kubernetes.io/master: ""
    otherNodeType: ""
`),
			expectConflictSet: fieldpath.NewSet(
				fieldpath.MakePathOrDie("spec", "nodeSelector"), // selector is atomic
			),
		},
		{
			fieldManager: "not_kubectl",
			original: []byte(`
apiVersion: v1
kind: Pod
metadata:
  name: test
  namespace: test
spec:
  containers:
  - args:
    - -v=2
    command:
    - controller
    image: some.registry/app:latest
    name: doJob
  nodeName: definetlyControlPlane
  nodeSelector:
    node-role.kubernetes.io/master: ""
`),
			applied: []byte(`
# purging selector not allowed for different manager

apiVersion: v1
kind: Pod
metadata:
  name: test
  namespace: test
spec:
  containers:
  - args:
    - -v=2
    command:
    - controller
    image: some.registry/app:latest
    name: doJob
  nodeName: another
  nodeSelector: {}
`),
			expectConflictSet: fieldpath.NewSet(
				fieldpath.MakePathOrDie("spec", "nodeSelector"), // selector is atomic
				fieldpath.MakePathOrDie("spec", "nodeName"),
			),
		},
		{
			fieldManager: "kubectl",
			original: []byte(`
apiVersion: v1
kind: Pod
metadata:
  name: test
  namespace: test
spec:
  containers:
  - args:
    - -v=2
    command:
    - controller
    image: some.registry/app:latest
    name: doJob
  nodeName: definetlyControlPlane
  nodeSelector:
    node-role.kubernetes.io/master: ""
`),
			applied: []byte(`
# same manager could purge nodeSelector

apiVersion: v1
kind: Pod
metadata:
  name: test
  namespace: test
spec:
  containers:
  - args:
    - -v=2
    command:
    - controller
    image: some.registry/app:latest
    name: doJob
  nodeName: another
  nodeSelector: {}
`),
		},
	}

	testConflicts(t, f, tests)
}

// testConflicts 是各用例共用的执行骨架：
//  1. 重置 manager，解析 original；
//  2. 未显式给出 lastApplied 时退化为 original（模拟客户端 apply 的默认行为）；
//  3. 先以 test_client_side_apply 做一次 Update 写入 last-applied；
//  4. 执行目标 manager 的 Apply，比对冲突集合；
//  5. 若预期冲突，再用 force=true 重试，确认 force 能接管全部冲突字段；
//  6. 最终 live 对象必须包含 applied 的所有字段。
func testConflicts(t *testing.T, f managedfieldstest.TestFieldManager, tests []testArgs) {
	for i, test := range tests {
		t.Run(fmt.Sprintf("test %d", i), func(t *testing.T) {
			f.Reset()

			originalObj := &unstructured.Unstructured{Object: map[string]interface{}{}}
			if err := yaml.Unmarshal(test.original, &originalObj.Object); err != nil {
				t.Errorf("error decoding YAML: %v", err)
			}

			if test.lastApplied == nil {
				test.lastApplied = test.original
			}
			if err := setLastAppliedFromEncoded(originalObj, test.lastApplied); err != nil {
				t.Errorf("failed to set last applied: %v", err)
			}

			if err := f.Update(originalObj, "test_client_side_apply"); err != nil {
				t.Errorf("failed to apply object: %v", err)
			}

			appliedObj := &unstructured.Unstructured{Object: map[string]interface{}{}}
			if err := yaml.Unmarshal(test.applied, &appliedObj.Object); err != nil {
				t.Errorf("error decoding YAML: %v", err)
			}

			err := f.Apply(appliedObj, test.fieldManager, false)

			if test.expectConflictSet == nil {
				if err != nil {
					t.Errorf("expected no error but got %v", err)
				}
			} else {
				if err == nil || !apierrors.IsConflict(err) {
					t.Errorf("expected to get conflicts but got %v", err)
				}

				// 构造期望的 ConflictError：每条冲突都归属于执行 Update 的
				// test_client_side_apply。
				expectedConflicts := merge.Conflicts{}
				test.expectConflictSet.Iterate(func(p fieldpath.Path) {
					expectedConflicts = append(expectedConflicts, merge.Conflict{
						Manager: fmt.Sprintf(`{"manager":"test_client_side_apply","operation":"Update","apiVersion":"%s"}`, f.APIVersion()),
						Path:    p,
					})
				})
				expectedConflictErr := internal.NewConflictError(expectedConflicts)
				if !reflect.DeepEqual(expectedConflictErr, err) {
					t.Errorf("expected to get\n%+v\nbut got\n%+v", expectedConflictErr, err)
				}

				// Yet force should resolve all conflicts
				err = f.Apply(appliedObj, test.fieldManager, true)
				if err != nil {
					t.Errorf("unexpected error during force ownership apply: %v", err)
				}

			}

			// Eventually resource should contain applied changes
			// 中文说明：DeepDerivative 只要求 applied 中出现的字段在 live 中一致，
			// 不反对 server 补充默认字段。
			if !apiequality.Semantic.DeepDerivative(appliedObj, f.Live()) {
				t.Errorf("expected equal resource: \n%#v, got: \n%#v", appliedObj, f.Live())
			}
		})
	}
}

// yamlToJSON 把 YAML 解码为 Unstructured 后再经 JSON scheme 重新编码，
// 保证与 API 接受的数据形态一致（字段名、类型均规范化）。
func yamlToJSON(y []byte) (string, error) {
	obj := &unstructured.Unstructured{Object: map[string]interface{}{}}
	if err := yaml.Unmarshal(y, &obj.Object); err != nil {
		return "", fmt.Errorf("error decoding YAML: %v", err)
	}
	serialization, err := runtime.Encode(unstructured.UnstructuredJSONScheme, obj)
	if err != nil {
		return "", fmt.Errorf("error encoding object: %v", err)
	}
	json, err := yamlutil.ToJSON(serialization)
	if err != nil {
		return "", fmt.Errorf("error converting to json: %v", err)
	}
	return string(json), nil
}

// setLastAppliedFromEncoded 把给定 YAML/JSON 写入对象的 last-applied 注解。
func setLastAppliedFromEncoded(obj runtime.Object, lastApplied []byte) error {
	lastAppliedJSON, err := yamlToJSON(lastApplied)
	if err != nil {
		return err
	}
	return internal.SetLastApplied(obj, lastAppliedJSON)
}
