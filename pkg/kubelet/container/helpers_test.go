/*
Copyright 2015 The Kubernetes Authors.

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

package container

import (
	"reflect"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/assert"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/record"
	runtimeapi "k8s.io/cri-api/pkg/apis/runtime/v1"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/test/utils/ktesting"
	"k8s.io/utils/ptr"
)

// TestEnvVarsToMap 校验 envVarsToMap 按名称正确建立环境变量映射。
func TestEnvVarsToMap(t *testing.T) {
	vars := []EnvVar{
		{
			Name:  "foo",
			Value: "bar",
		},
		{
			Name:  "zoo",
			Value: "baz",
		},
	}

	varMap := envVarsToMap(vars)

	if e, a := len(vars), len(varMap); e != a {
		t.Errorf("Unexpected map length; expected: %d, got %d", e, a)
	}

	if a := varMap["foo"]; a != "bar" {
		t.Errorf("Unexpected value of key 'foo': %v", a)
	}

	if a := varMap["zoo"]; a != "baz" {
		t.Errorf("Unexpected value of key 'zoo': %v", a)
	}
}

// TestExpandCommandAndArgs 校验 ExpandContainerCommandAndArgs 能对 command 与 args
// 中的 $(VAR) 引用做环境变量展开，覆盖仅 command、仅 args 与两者同时展开的场景。
func TestExpandCommandAndArgs(t *testing.T) {
	cases := []struct {
		name            string
		container       *v1.Container
		envs            []EnvVar
		expectedCommand []string
		expectedArgs    []string
	}{
		{
			name:      "none",
			container: &v1.Container{},
		},
		{
			name: "command expanded",
			container: &v1.Container{
				Command: []string{"foo", "$(VAR_TEST)", "$(VAR_TEST2)"},
			},
			envs: []EnvVar{
				{
					Name:  "VAR_TEST",
					Value: "zoo",
				},
				{
					Name:  "VAR_TEST2",
					Value: "boo",
				},
			},
			expectedCommand: []string{"foo", "zoo", "boo"},
		},
		{
			name: "args expanded",
			container: &v1.Container{
				Args: []string{"zap", "$(VAR_TEST)", "$(VAR_TEST2)"},
			},
			envs: []EnvVar{
				{
					Name:  "VAR_TEST",
					Value: "hap",
				},
				{
					Name:  "VAR_TEST2",
					Value: "trap",
				},
			},
			expectedArgs: []string{"zap", "hap", "trap"},
		},
		{
			name: "both expanded",
			container: &v1.Container{
				Command: []string{"$(VAR_TEST2)--$(VAR_TEST)", "foo", "$(VAR_TEST3)"},
				Args:    []string{"foo", "$(VAR_TEST)", "$(VAR_TEST2)"},
			},
			envs: []EnvVar{
				{
					Name:  "VAR_TEST",
					Value: "zoo",
				},
				{
					Name:  "VAR_TEST2",
					Value: "boo",
				},
				{
					Name:  "VAR_TEST3",
					Value: "roo",
				},
			},
			expectedCommand: []string{"boo--zoo", "foo", "roo"},
			expectedArgs:    []string{"foo", "zoo", "boo"},
		},
	}

	for _, tc := range cases {
		actualCommand, actualArgs := ExpandContainerCommandAndArgs(tc.container, tc.envs)

		if e, a := tc.expectedCommand, actualCommand; !reflect.DeepEqual(e, a) {
			t.Errorf("%v: unexpected command; expected %v, got %v", tc.name, e, a)
		}

		if e, a := tc.expectedArgs, actualArgs; !reflect.DeepEqual(e, a) {
			t.Errorf("%v: unexpected args; expected %v, got %v", tc.name, e, a)
		}

	}
}

// TestExpandVolumeMountsWithSubpath 校验 ExpandContainerVolumeMounts 对 SubPathExpr
// 的展开行为，包括无变量、缺变量、空变量值、嵌套引用等边界情况。
func TestExpandVolumeMountsWithSubpath(t *testing.T) {
	cases := []struct {
		name              string
		container         *v1.Container
		envs              []EnvVar
		expectedSubPath   string
		expectedMountPath string
		expectedOk        bool
	}{
		{
			name: "subpath with no expansion",
			container: &v1.Container{
				VolumeMounts: []v1.VolumeMount{{SubPathExpr: "foo"}},
			},
			expectedSubPath:   "foo",
			expectedMountPath: "",
			expectedOk:        true,
		},
		{
			name: "volumes with expanded subpath",
			container: &v1.Container{
				VolumeMounts: []v1.VolumeMount{{SubPathExpr: "foo/$(POD_NAME)"}},
			},
			envs: []EnvVar{
				{
					Name:  "POD_NAME",
					Value: "bar",
				},
			},
			expectedSubPath:   "foo/bar",
			expectedMountPath: "",
			expectedOk:        true,
		},
		{
			name: "volumes expanded with empty subpath",
			container: &v1.Container{
				VolumeMounts: []v1.VolumeMount{{SubPathExpr: ""}},
			},
			envs: []EnvVar{
				{
					Name:  "POD_NAME",
					Value: "bar",
				},
			},
			expectedSubPath:   "",
			expectedMountPath: "",
			expectedOk:        true,
		},
		{
			name: "volumes expanded with no envs subpath",
			container: &v1.Container{
				VolumeMounts: []v1.VolumeMount{{SubPathExpr: "/foo/$(POD_NAME)"}},
			},
			expectedSubPath:   "/foo/$(POD_NAME)",
			expectedMountPath: "",
			expectedOk:        false,
		},
		{
			name: "volumes expanded with leading environment variable",
			container: &v1.Container{
				VolumeMounts: []v1.VolumeMount{{SubPathExpr: "$(POD_NAME)/bar"}},
			},
			envs: []EnvVar{
				{
					Name:  "POD_NAME",
					Value: "foo",
				},
			},
			expectedSubPath:   "foo/bar",
			expectedMountPath: "",
			expectedOk:        true,
		},
		{
			name: "volumes with volume and subpath",
			container: &v1.Container{
				VolumeMounts: []v1.VolumeMount{{MountPath: "/foo", SubPathExpr: "$(POD_NAME)/bar"}},
			},
			envs: []EnvVar{
				{
					Name:  "POD_NAME",
					Value: "foo",
				},
			},
			expectedSubPath:   "foo/bar",
			expectedMountPath: "/foo",
			expectedOk:        true,
		},
		{
			name: "volumes with volume and no subpath",
			container: &v1.Container{
				VolumeMounts: []v1.VolumeMount{{MountPath: "/foo"}},
			},
			envs: []EnvVar{
				{
					Name:  "POD_NAME",
					Value: "foo",
				},
			},
			expectedSubPath:   "",
			expectedMountPath: "/foo",
			expectedOk:        true,
		},
		{
			name: "subpaths with empty environment variable",
			container: &v1.Container{
				VolumeMounts: []v1.VolumeMount{{SubPathExpr: "foo/$(POD_NAME)/$(ANNOTATION)"}},
			},
			envs: []EnvVar{
				{
					Name:  "ANNOTATION",
					Value: "",
				},
			},
			expectedSubPath:   "foo/$(POD_NAME)/$(ANNOTATION)",
			expectedMountPath: "",
			expectedOk:        false,
		},
		{
			name: "subpaths with missing env variables",
			container: &v1.Container{
				VolumeMounts: []v1.VolumeMount{{SubPathExpr: "foo/$(ODD_NAME)/$(POD_NAME)"}},
			},
			envs: []EnvVar{
				{
					Name:  "ODD_NAME",
					Value: "bar",
				},
			},
			expectedSubPath:   "foo/$(ODD_NAME)/$(POD_NAME)",
			expectedMountPath: "",
			expectedOk:        false,
		},
		{
			name: "subpaths with empty expansion",
			container: &v1.Container{
				VolumeMounts: []v1.VolumeMount{{SubPathExpr: "$()"}},
			},
			expectedSubPath:   "$()",
			expectedMountPath: "",
			expectedOk:        false,
		},
		{
			name: "subpaths with nested expandable envs",
			container: &v1.Container{
				VolumeMounts: []v1.VolumeMount{{SubPathExpr: "$(POD_NAME$(ANNOTATION))"}},
			},
			envs: []EnvVar{
				{
					Name:  "POD_NAME",
					Value: "foo",
				},
				{
					Name:  "ANNOTATION",
					Value: "bar",
				},
			},
			expectedSubPath:   "$(POD_NAME$(ANNOTATION))",
			expectedMountPath: "",
			expectedOk:        false,
		},
	}

	for _, tc := range cases {
		actualSubPath, err := ExpandContainerVolumeMounts(tc.container.VolumeMounts[0], tc.envs)
		ok := err == nil
		if e, a := tc.expectedOk, ok; !reflect.DeepEqual(e, a) {
			t.Errorf("%v: unexpected validation failure of subpath; expected %v, got %v", tc.name, e, a)
		}
		if !ok {
			// 若 ExpandContainerVolumeMounts 返回错误，则不必关心 actualSubPath 的值
			continue
		}
		if e, a := tc.expectedSubPath, actualSubPath; !reflect.DeepEqual(e, a) {
			t.Errorf("%v: unexpected subpath; expected %v, got %v", tc.name, e, a)
		}
		if e, a := tc.expectedMountPath, tc.container.VolumeMounts[0].MountPath; !reflect.DeepEqual(e, a) {
			t.Errorf("%v: unexpected mountpath; expected %v, got %v", tc.name, e, a)
		}
	}

}

// TestGetContainerSpec 校验 GetContainerSpec 能从普通容器、init 容器和临时容器中
// 按名称找到对应的容器 spec。
func TestGetContainerSpec(t *testing.T) {
	for _, tc := range []struct {
		name          string
		havePod       *v1.Pod
		haveName      string
		wantContainer *v1.Container
	}{
		{
			name: "regular container",
			havePod: &v1.Pod{
				Spec: v1.PodSpec{
					Containers: []v1.Container{
						{Name: "plain-ole-container"},
					},
					InitContainers: []v1.Container{
						{Name: "init-container"},
					},
				},
			},
			haveName:      "plain-ole-container",
			wantContainer: &v1.Container{Name: "plain-ole-container"},
		},
		{
			name: "init container",
			havePod: &v1.Pod{
				Spec: v1.PodSpec{
					Containers: []v1.Container{
						{Name: "plain-ole-container"},
					},
					InitContainers: []v1.Container{
						{Name: "init-container"},
					},
				},
			},
			haveName:      "init-container",
			wantContainer: &v1.Container{Name: "init-container"},
		},
		{
			name: "ephemeral container",
			havePod: &v1.Pod{
				Spec: v1.PodSpec{
					Containers: []v1.Container{
						{Name: "plain-ole-container"},
					},
					InitContainers: []v1.Container{
						{Name: "init-container"},
					},
					EphemeralContainers: []v1.EphemeralContainer{
						{EphemeralContainerCommon: v1.EphemeralContainerCommon{
							Name: "debug-container",
						}},
					},
				},
			},
			haveName:      "debug-container",
			wantContainer: &v1.Container{Name: "debug-container"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			gotContainer := GetContainerSpec(tc.havePod, tc.haveName)
			if diff := cmp.Diff(tc.wantContainer, gotContainer); diff != "" {
				t.Fatalf("GetContainerSpec for %q returned diff (-want +got):%v", tc.name, diff)
			}
		})
	}
}

// TestShouldContainerBeRestarted 校验在不同重启策略下 ShouldContainerBeRestarted
// 的判定结果，并验证已删除 pod 的容器一律不再重启。
func TestShouldContainerBeRestarted(t *testing.T) {
	logger, _ := ktesting.NewTestContext(t)
	pod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			UID:       "12345678",
			Name:      "foo",
			Namespace: "new",
		},
		Spec: v1.PodSpec{
			Containers: []v1.Container{
				{Name: "no-history"},
				{Name: "alive"},
				{Name: "succeed"},
				{Name: "failed"},
				{Name: "unknown"},
			},
		},
	}
	podStatus := &PodStatus{
		ID:        pod.UID,
		Name:      pod.Name,
		Namespace: pod.Namespace,
		ContainerStatuses: []*Status{
			{
				Name:  "alive",
				State: ContainerStateRunning,
			},
			{
				Name:     "succeed",
				State:    ContainerStateExited,
				ExitCode: 0,
			},
			{
				Name:     "failed",
				State:    ContainerStateExited,
				ExitCode: 1,
			},
			{
				Name:     "alive",
				State:    ContainerStateExited,
				ExitCode: 2,
			},
			{
				Name:  "unknown",
				State: ContainerStateUnknown,
			},
			{
				Name:     "failed",
				State:    ContainerStateExited,
				ExitCode: 3,
			},
		},
	}
	policies := []v1.RestartPolicy{
		v1.RestartPolicyNever,
		v1.RestartPolicyOnFailure,
		v1.RestartPolicyAlways,
	}

	// 验证各重启策略下的预期结果
	expected := map[string][]bool{
		"no-history": {true, true, true},
		"alive":      {false, false, false},
		"succeed":    {false, false, true},
		"failed":     {false, true, true},
		"unknown":    {true, true, true},
	}
	for _, c := range pod.Spec.Containers {
		for i, policy := range policies {
			pod.Spec.RestartPolicy = policy
			e := expected[c.Name][i]
			r := ShouldContainerBeRestarted(logger, &c, pod, podStatus)
			if r != e {
				t.Errorf("Restart for container %q with restart policy %q expected %t, got %t",
					c.Name, policy, e, r)
			}
		}
	}

	// 验证已删除的 pod
	pod.DeletionTimestamp = &metav1.Time{Time: time.Now()}
	expected = map[string][]bool{
		"no-history": {false, false, false},
		"alive":      {false, false, false},
		"succeed":    {false, false, false},
		"failed":     {false, false, false},
		"unknown":    {false, false, false},
	}
	for _, c := range pod.Spec.Containers {
		for i, policy := range policies {
			pod.Spec.RestartPolicy = policy
			e := expected[c.Name][i]
			r := ShouldContainerBeRestarted(logger, &c, pod, podStatus)
			if r != e {
				t.Errorf("Restart for container %q with restart policy %q expected %t, got %t",
					c.Name, policy, e, r)
			}
		}
	}
}

// TestHasPrivilegedContainer 校验 HasPrivilegedContainer 对普通容器与 init 容器
// 中 privileged 标志的识别。
func TestHasPrivilegedContainer(t *testing.T) {
	tests := map[string]struct {
		securityContext *v1.SecurityContext
		expected        bool
	}{
		"nil security context": {
			securityContext: nil,
			expected:        false,
		},
		"nil privileged": {
			securityContext: &v1.SecurityContext{},
			expected:        false,
		},
		"false privileged": {
			securityContext: &v1.SecurityContext{Privileged: ptr.To(false)},
			expected:        false,
		},
		"true privileged": {
			securityContext: &v1.SecurityContext{Privileged: ptr.To(true)},
			expected:        true,
		},
	}

	for k, v := range tests {
		pod := &v1.Pod{
			Spec: v1.PodSpec{
				Containers: []v1.Container{
					{SecurityContext: v.securityContext},
				},
			},
		}
		actual := HasPrivilegedContainer(pod)
		if actual != v.expected {
			t.Errorf("%s expected %t but got %t", k, v.expected, actual)
		}
	}
	// 同时验证 init 容器。
	for k, v := range tests {
		pod := &v1.Pod{
			Spec: v1.PodSpec{
				InitContainers: []v1.Container{
					{SecurityContext: v.securityContext},
				},
			},
		}
		actual := HasPrivilegedContainer(pod)
		if actual != v.expected {
			t.Errorf("%s expected %t but got %t", k, v.expected, actual)
		}
	}
}

// TestMakePortMappings 校验 MakePortMappings 生成端口映射时的去重规则：
// 同名端口忽略、不同地址族保留、相同容器端口与主机端口视为重复。
func TestMakePortMappings(t *testing.T) {
	logger, _ := ktesting.NewTestContext(t)
	port := func(name string, protocol v1.Protocol, containerPort, hostPort int32, ip string) v1.ContainerPort {
		return v1.ContainerPort{
			Name:          name,
			Protocol:      protocol,
			ContainerPort: containerPort,
			HostPort:      hostPort,
			HostIP:        ip,
		}
	}
	portMapping := func(protocol v1.Protocol, containerPort, hostPort int, ip string) PortMapping {
		return PortMapping{
			Protocol:      protocol,
			ContainerPort: containerPort,
			HostPort:      hostPort,
			HostIP:        ip,
		}
	}

	tests := []struct {
		container            *v1.Container
		expectedPortMappings []PortMapping
	}{
		{
			&v1.Container{
				Name: "fooContainer",
				Ports: []v1.ContainerPort{
					port("", v1.ProtocolTCP, 80, 8080, "127.0.0.1"),
					port("", v1.ProtocolTCP, 443, 4343, "192.168.0.1"),
					port("foo", v1.ProtocolUDP, 555, 5555, ""),
					// 重复项，应被忽略。
					port("foo", v1.ProtocolUDP, 888, 8888, ""),
					// 地址族不同的重复项，不应被忽略
					port("", v1.ProtocolTCP, 80, 8080, "::"),
					// 未指定地址族
					port("", v1.ProtocolTCP, 1234, 5678, ""),
				},
			},
			[]PortMapping{
				portMapping(v1.ProtocolTCP, 80, 8080, "127.0.0.1"),
				portMapping(v1.ProtocolTCP, 443, 4343, "192.168.0.1"),
				portMapping(v1.ProtocolUDP, 555, 5555, ""),
				portMapping(v1.ProtocolTCP, 80, 8080, "::"),
				portMapping(v1.ProtocolTCP, 1234, 5678, ""),
			},
		},
		{
			// 同一个容器端口可以映射到不同的主机端口
			&v1.Container{
				Name: "fooContainer",
				Ports: []v1.ContainerPort{
					port("", v1.ProtocolTCP, 443, 4343, "192.168.0.1"),
					port("", v1.ProtocolTCP, 4343, 4343, "192.168.0.1"),
				},
			},
			[]PortMapping{
				portMapping(v1.ProtocolTCP, 443, 4343, "192.168.0.1"),
				portMapping(v1.ProtocolTCP, 4343, 4343, "192.168.0.1"),
			},
		},
		{
			// 容器端口与主机端口都相同时不允许
			&v1.Container{
				Name: "fooContainer",
				Ports: []v1.ContainerPort{
					port("", v1.ProtocolTCP, 443, 4343, ""),
					port("", v1.ProtocolTCP, 443, 4343, ""),
				},
			},
			[]PortMapping{
				portMapping(v1.ProtocolTCP, 443, 4343, ""),
			},
		},
		{
			// 多网卡节点 —— 多 IP 场景
			&v1.Container{
				Name: "fooContainer",
				Ports: []v1.ContainerPort{
					port("", v1.ProtocolTCP, 443, 4343, "192.168.0.1"),
					port("", v1.ProtocolTCP, 443, 4343, "172.16.0.1"),
				},
			},
			[]PortMapping{
				portMapping(v1.ProtocolTCP, 443, 4343, "192.168.0.1"),
				portMapping(v1.ProtocolTCP, 443, 4343, "172.16.0.1"),
			},
		},
	}

	for i, tt := range tests {
		actual := MakePortMappings(logger, tt.container)
		assert.Equal(t, tt.expectedPortMappings, actual, "[%d]", i)
	}
}

// TestHashContainer 校验 HashContainer 对固定输入产生稳定的哈希值 —— 该值一旦变化
// 会导致已有容器被重建，因此不能被随意修改。
func TestHashContainer(t *testing.T) {
	testCases := []struct {
		name          string
		image         string
		args          []string
		containerPort int32
		expectedHash  uint64
	}{
		{
			name:  "test_container",
			image: "foo/image:v1",
			args: []string{
				"/bin/sh",
				"-c",
				"echo abc",
			},
			containerPort: int32(8001),
			expectedHash:  uint64(0x8e45cbd0),
		},
	}

	for _, tc := range testCases {
		container := v1.Container{
			Name:  tc.name,
			Image: tc.image,
			Args:  tc.args,
			Ports: []v1.ContainerPort{{ContainerPort: tc.containerPort}},
		}

		hashVal := HashContainer(&container)
		assert.Equal(t, tc.expectedHash, hashVal, "the hash value here should not be changed.")
	}
}

// TestShouldRecordEvent 校验 shouldRecordEvent 过滤隐式必需容器事件的逻辑，
// 并验证 typed nil 不会引发 panic。
func TestShouldRecordEvent(t *testing.T) {
	var innerEventRecorder = &innerEventRecorder{
		recorder: nil,
	}

	_, actual := innerEventRecorder.shouldRecordEvent(nil)
	assert.False(t, actual)

	var obj = &v1.ObjectReference{Namespace: "claimrefns", Name: "claimrefname"}

	_, actual = innerEventRecorder.shouldRecordEvent(obj)
	assert.True(t, actual)

	obj = &v1.ObjectReference{Namespace: "system", Name: "infra", FieldPath: "implicitly required container "}

	_, actual = innerEventRecorder.shouldRecordEvent(obj)
	assert.False(t, actual)

	var nilObj *v1.ObjectReference = nil
	_, actual = innerEventRecorder.shouldRecordEvent(nilObj)
	assert.False(t, actual, "should not panic if the typed nil was used, see https://github.com/kubernetes/kubernetes/issues/95552")
}

// TestHasWindowsHostProcessContainer 校验 HasWindowsHostProcessContainer 在 pod 级与
// 容器级 HostProcess 设置组合下的判定结果。
func TestHasWindowsHostProcessContainer(t *testing.T) {
	trueVar := true
	falseVar := false
	const containerName = "container"

	testCases := []struct {
		name           string
		podSpec        *v1.PodSpec
		expectedResult bool
	}{
		{
			name: "hostprocess not set anywhere",
			podSpec: &v1.PodSpec{
				Containers: []v1.Container{{
					Name: containerName,
				}},
			},
			expectedResult: false,
		},
		{
			name: "pod with hostprocess=false",
			podSpec: &v1.PodSpec{
				HostNetwork: true,
				SecurityContext: &v1.PodSecurityContext{
					WindowsOptions: &v1.WindowsSecurityContextOptions{
						HostProcess: &falseVar,
					},
				},
				Containers: []v1.Container{{
					Name: containerName,
				}},
			},
			expectedResult: false,
		},
		{
			name: "pod with hostprocess=true",
			podSpec: &v1.PodSpec{
				HostNetwork: true,
				SecurityContext: &v1.PodSecurityContext{
					WindowsOptions: &v1.WindowsSecurityContextOptions{
						HostProcess: &trueVar,
					},
				},
				Containers: []v1.Container{{
					Name: containerName,
				}},
			},
			expectedResult: true,
		},
		{
			name: "container with hostprocess=false",
			podSpec: &v1.PodSpec{
				HostNetwork: true,
				Containers: []v1.Container{{
					Name: containerName,
					SecurityContext: &v1.SecurityContext{
						WindowsOptions: &v1.WindowsSecurityContextOptions{
							HostProcess: &falseVar,
						},
					},
				}},
			},
			expectedResult: false,
		},
		{
			name: "container with hostprocess=true",
			podSpec: &v1.PodSpec{
				HostNetwork: true,
				Containers: []v1.Container{{
					Name: containerName,
					SecurityContext: &v1.SecurityContext{
						WindowsOptions: &v1.WindowsSecurityContextOptions{
							HostProcess: &trueVar,
						},
					},
				}},
			},
			expectedResult: true,
		},
		{
			name: "pod with hostprocess=false, container with hostprocess=true",
			podSpec: &v1.PodSpec{
				HostNetwork: true,
				SecurityContext: &v1.PodSecurityContext{
					WindowsOptions: &v1.WindowsSecurityContextOptions{
						HostProcess: &falseVar,
					},
				},
				Containers: []v1.Container{{
					Name: containerName,
					SecurityContext: &v1.SecurityContext{
						WindowsOptions: &v1.WindowsSecurityContextOptions{
							HostProcess: &trueVar,
						},
					},
				}},
			},
			expectedResult: true,
		},
		{
			name: "pod with hostprocess=true, container with hostprocess=flase",
			podSpec: &v1.PodSpec{
				HostNetwork: true,
				SecurityContext: &v1.PodSecurityContext{
					WindowsOptions: &v1.WindowsSecurityContextOptions{
						HostProcess: &trueVar,
					},
				},
				Containers: []v1.Container{{
					Name: containerName,
					SecurityContext: &v1.SecurityContext{
						WindowsOptions: &v1.WindowsSecurityContextOptions{
							HostProcess: &falseVar,
						},
					},
				}},
			},
			expectedResult: false,
		},
		{
			name: "containers with hostproces=mixed",
			podSpec: &v1.PodSpec{
				Containers: []v1.Container{
					{
						Name: containerName,
						SecurityContext: &v1.SecurityContext{
							WindowsOptions: &v1.WindowsSecurityContextOptions{
								HostProcess: &falseVar,
							},
						},
					},
					{
						Name: containerName,
						SecurityContext: &v1.SecurityContext{
							WindowsOptions: &v1.WindowsSecurityContextOptions{
								HostProcess: &trueVar,
							},
						},
					},
				},
			},
			expectedResult: true,
		},
		{
			name: "pod with hostProcess=false, containers with hostproces=mixed",
			podSpec: &v1.PodSpec{
				SecurityContext: &v1.PodSecurityContext{
					WindowsOptions: &v1.WindowsSecurityContextOptions{
						HostProcess: &falseVar,
					},
				},
				Containers: []v1.Container{
					{
						Name: containerName,
						SecurityContext: &v1.SecurityContext{
							WindowsOptions: &v1.WindowsSecurityContextOptions{
								HostProcess: &falseVar,
							},
						},
					},
					{
						Name: containerName,
						SecurityContext: &v1.SecurityContext{
							WindowsOptions: &v1.WindowsSecurityContextOptions{
								HostProcess: &trueVar,
							},
						},
					},
				},
			},
			expectedResult: true,
		},
		{
			name: "pod with hostProcess=true, containers with hostproces=mixed",
			podSpec: &v1.PodSpec{
				SecurityContext: &v1.PodSecurityContext{
					WindowsOptions: &v1.WindowsSecurityContextOptions{
						HostProcess: &trueVar,
					},
				},
				Containers: []v1.Container{
					{
						Name: containerName,
						SecurityContext: &v1.SecurityContext{
							WindowsOptions: &v1.WindowsSecurityContextOptions{
								HostProcess: &falseVar,
							},
						},
					},
					{
						Name: containerName,
						SecurityContext: &v1.SecurityContext{
							WindowsOptions: &v1.WindowsSecurityContextOptions{
								HostProcess: &trueVar,
							},
						},
					},
				},
			},
			expectedResult: true,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			pod := &v1.Pod{}
			pod.Spec = *testCase.podSpec
			result := HasWindowsHostProcessContainer(pod)
			assert.Equal(t, testCase.expectedResult, result)
		})
	}
}

// TestHashContainerWithoutResources 校验 HashContainer 在忽略资源与 resize 策略时
// 对同一 pod 不同资源组合产生相同哈希，且不修改原始容器对象。
func TestHashContainerWithoutResources(t *testing.T) {
	cpu100m := resource.MustParse("100m")
	cpu200m := resource.MustParse("200m")
	mem100M := resource.MustParse("100Mi")
	mem200M := resource.MustParse("200Mi")
	cpuPolicyRestartNotRequired := v1.ContainerResizePolicy{ResourceName: v1.ResourceCPU, RestartPolicy: v1.NotRequired}
	memPolicyRestartNotRequired := v1.ContainerResizePolicy{ResourceName: v1.ResourceMemory, RestartPolicy: v1.NotRequired}
	cpuPolicyRestartRequired := v1.ContainerResizePolicy{ResourceName: v1.ResourceCPU, RestartPolicy: v1.RestartContainer}
	memPolicyRestartRequired := v1.ContainerResizePolicy{ResourceName: v1.ResourceMemory, RestartPolicy: v1.RestartContainer}

	// testCase 描述容器的输入及其期望的哈希值。
	type testCase struct {
		name         string
		container    *v1.Container
		expectedHash uint64
	}

	tests := []testCase{
		{
			"Burstable pod with CPU policy restart required",
			&v1.Container{
				Name:  "foo",
				Image: "bar",
				Resources: v1.ResourceRequirements{
					Limits:   v1.ResourceList{v1.ResourceCPU: cpu200m, v1.ResourceMemory: mem200M},
					Requests: v1.ResourceList{v1.ResourceCPU: cpu100m, v1.ResourceMemory: mem100M},
				},
				ResizePolicy: []v1.ContainerResizePolicy{cpuPolicyRestartRequired, memPolicyRestartNotRequired},
			},
			0x11a6d6d6,
		},
		{
			"Burstable pod with memory policy restart required",
			&v1.Container{
				Name:  "foo",
				Image: "bar",
				Resources: v1.ResourceRequirements{
					Limits:   v1.ResourceList{v1.ResourceCPU: cpu200m, v1.ResourceMemory: mem200M},
					Requests: v1.ResourceList{v1.ResourceCPU: cpu100m, v1.ResourceMemory: mem100M},
				},
				ResizePolicy: []v1.ContainerResizePolicy{cpuPolicyRestartNotRequired, memPolicyRestartRequired},
			},
			0x11a6d6d6,
		},
		{
			"Guaranteed pod with CPU policy restart required",
			&v1.Container{
				Name:  "foo",
				Image: "bar",
				Resources: v1.ResourceRequirements{
					Limits:   v1.ResourceList{v1.ResourceCPU: cpu100m, v1.ResourceMemory: mem100M},
					Requests: v1.ResourceList{v1.ResourceCPU: cpu100m, v1.ResourceMemory: mem100M},
				},
				ResizePolicy: []v1.ContainerResizePolicy{cpuPolicyRestartRequired, memPolicyRestartNotRequired},
			},
			0x11a6d6d6,
		},
		{
			"Guaranteed pod with memory policy restart required",
			&v1.Container{
				Name:  "foo",
				Image: "bar",
				Resources: v1.ResourceRequirements{
					Limits:   v1.ResourceList{v1.ResourceCPU: cpu100m, v1.ResourceMemory: mem100M},
					Requests: v1.ResourceList{v1.ResourceCPU: cpu100m, v1.ResourceMemory: mem100M},
				},
				ResizePolicy: []v1.ContainerResizePolicy{cpuPolicyRestartNotRequired, memPolicyRestartRequired},
			},
			0x11a6d6d6,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			containerCopy := tc.container.DeepCopy()
			hash := HashContainer(tc.container)
			assert.Equal(t, tc.expectedHash, hash, "[%s]", tc.name)
			assert.Equal(t, containerCopy, tc.container, "[%s]", tc.name)
		})
	}
}

// TestHasAnyActiveRegularContainerStarted 校验 HasAnyActiveRegularContainerStarted 能
// 区分初始化中、已初始化、sandbox 重建等状态下是否存在活动普通容器。
func TestHasAnyActiveRegularContainerStarted(t *testing.T) {
	testCases := []struct {
		desc      string
		spec      *v1.PodSpec
		podStatus *PodStatus
		expected  bool
	}{
		{
			desc: "pod has no active container",
			spec: &v1.PodSpec{
				InitContainers: []v1.Container{
					{
						Name: "init",
					},
				},
				Containers: []v1.Container{
					{
						Name: "regular",
					},
				},
			},
			podStatus: &PodStatus{
				SandboxStatuses: []*runtimeapi.PodSandboxStatus{
					{
						Id:    "old",
						State: runtimeapi.PodSandboxState_SANDBOX_NOTREADY,
					},
				},
			},
			expected: false,
		},
		{
			desc: "pod is initializing",
			spec: &v1.PodSpec{
				InitContainers: []v1.Container{
					{
						Name: "init",
					},
				},
				Containers: []v1.Container{
					{
						Name: "regular",
					},
				},
			},
			podStatus: &PodStatus{
				SandboxStatuses: []*runtimeapi.PodSandboxStatus{
					{
						Id:    "current",
						State: runtimeapi.PodSandboxState_SANDBOX_READY,
					},
				},
				ActiveContainerStatuses: []*Status{
					{
						Name:  "init",
						State: ContainerStateRunning,
					},
				},
			},
			expected: false,
		},
		{
			desc: "pod has initialized",
			spec: &v1.PodSpec{
				InitContainers: []v1.Container{
					{
						Name: "init",
					},
				},
				Containers: []v1.Container{
					{
						Name: "regular",
					},
				},
			},
			podStatus: &PodStatus{
				SandboxStatuses: []*runtimeapi.PodSandboxStatus{
					{
						Id:    "current",
						State: runtimeapi.PodSandboxState_SANDBOX_READY,
					},
				},
				ActiveContainerStatuses: []*Status{
					{
						Name:  "init",
						State: ContainerStateExited,
					},
					{
						Name:  "regular",
						State: ContainerStateRunning,
					},
				},
			},
			expected: true,
		},
		{
			desc: "pod is re-initializing after the sandbox recreation",
			spec: &v1.PodSpec{
				InitContainers: []v1.Container{
					{
						Name: "init",
					},
				},
				Containers: []v1.Container{
					{
						Name: "regular",
					},
				},
			},
			podStatus: &PodStatus{
				SandboxStatuses: []*runtimeapi.PodSandboxStatus{
					{
						Id:    "current",
						State: runtimeapi.PodSandboxState_SANDBOX_READY,
					},
					{
						Id:    "old",
						State: runtimeapi.PodSandboxState_SANDBOX_NOTREADY,
					},
				},
				ActiveContainerStatuses: []*Status{
					{
						Name:  "init",
						State: ContainerStateRunning,
					},
				},
			},
			expected: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			actual := HasAnyActiveRegularContainerStarted(tc.spec, tc.podStatus)
			assert.Equal(t, tc.expected, actual)
		})
	}
}

// TestExpandContainerCommandOnlyStatic 校验仅静态环境变量展开：
// 缺失变量保持原样，不被替换。
func TestExpandContainerCommandOnlyStatic(t *testing.T) {
	testCases := []struct {
		name     string
		command  []string
		envs     []v1.EnvVar
		expected []string
	}{
		{
			name:     "no command",
			command:  nil,
			envs:     nil,
			expected: nil,
		},
		{
			name:    "static env expansion",
			command: []string{"echo", "$(FOO)", "$(BAR)"},
			envs: []v1.EnvVar{
				{Name: "FOO", Value: "foo-value"},
				{Name: "BAR", Value: "bar-value"},
			},
			expected: []string{"echo", "foo-value", "bar-value"},
		},
		{
			name:    "missing env variable",
			command: []string{"echo", "$(FOO)", "$(MISSING)"},
			envs: []v1.EnvVar{
				{Name: "FOO", Value: "foo-value"},
			},
			expected: []string{"echo", "foo-value", "$(MISSING)"},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			actual := ExpandContainerCommandOnlyStatic(tc.command, tc.envs)
			assert.Equal(t, tc.expected, actual, "ExpandContainerCommandOnlyStatic(%v, %v)", tc.command, tc.envs)
		})
	}
}

// fakeRecorder 是记录事件内容的测试用 EventRecorder 实现。
type fakeRecorder struct {
	events  []string
	fEvents []string
	aEvents []string
}

// WithLogger 返回自身，fakeRecorder 不使用 logger。
func (f *fakeRecorder) WithLogger(logger klog.Logger) record.EventRecorderLogger {
	return f
}

// Event 按 "类型:原因:消息" 格式记录事件。
func (f *fakeRecorder) Event(object runtime.Object, eventtype, reason, message string) {
	f.events = append(f.events, eventtype+":"+reason+":"+message)
}

// Eventf 记录格式化事件（不展开参数，便于断言模板本身）。
func (f *fakeRecorder) Eventf(object runtime.Object, eventtype, reason, messageFmt string, args ...interface{}) {
	f.fEvents = append(f.fEvents, eventtype+":"+reason+":"+messageFmt)
}

// AnnotatedEventf 记录带注解的格式化事件。
func (f *fakeRecorder) AnnotatedEventf(object runtime.Object, annotations map[string]string, eventtype, reason, messageFmt string, args ...interface{}) {
	f.aEvents = append(f.aEvents, eventtype+":"+reason+":"+messageFmt)
}

// TestFilterEventRecorder 校验 FilterEventRecorder 只放行非隐式容器的相关事件。
func TestFilterEventRecorder(t *testing.T) {
	recorder := &fakeRecorder{}
	filtered := FilterEventRecorder(recorder)

	// 记录一个正常事件
	obj := &v1.ObjectReference{FieldPath: "foo"}
	filtered.Event(obj, "Normal", "Reason", "Message")
	filtered.Eventf(obj, "Normal", "Reason", "MessageFmt")
	//nolint:forbidigo // Legacy usage
	filtered.AnnotatedEventf(obj, map[string]string{"a": "b"}, "Normal", "Reason", "MessageFmt")

	// 隐式容器的事件不应被记录
	implicit := &v1.ObjectReference{FieldPath: "implicitly required container foo"}
	filtered.Event(implicit, "Normal", "Reason", "Message")
	filtered.Eventf(implicit, "Normal", "Reason", "MessageFmt")
	//nolint:forbidigo // Legacy usage
	filtered.AnnotatedEventf(implicit, map[string]string{"a": "b"}, "Normal", "Reason", "MessageFmt")

	assert.Len(t, recorder.events, 1, "Expected only one event of each type to be recorded, got events: %v", recorder.events)
	assert.Len(t, recorder.fEvents, 1, "Expected only one event of each type to be recorded, got fEvents: %v", recorder.fEvents)
	assert.Len(t, recorder.aEvents, 1, "Expected only one event of each type to be recorded, got aEvents: %v", recorder.aEvents)
}

// TestIsHostNetworkPod 校验 IsHostNetworkPod 能正确识别 HostNetwork 设置。
func TestIsHostNetworkPod(t *testing.T) {
	pod := &v1.Pod{Spec: v1.PodSpec{HostNetwork: true}}
	assert.True(t, IsHostNetworkPod(pod), "expected true for HostNetwork pod")

	pod = &v1.Pod{Spec: v1.PodSpec{HostNetwork: false}}
	assert.False(t, IsHostNetworkPod(pod), "expected false for non-HostNetwork pod")
}

// TestConvertPodStatusToRunningPod 校验 ConvertPodStatusToRunningPod 只保留运行中的
// 容器，并把 sandbox 状态映射为容器状态。
func TestConvertPodStatusToRunningPod(t *testing.T) {
	podStatus := &PodStatus{
		ID:        "poduid",
		Name:      "podname",
		Namespace: "podns",
		ContainerStatuses: []*Status{
			{
				ID:    ContainerID{Type: "docker", ID: "c1"},
				Name:  "c1",
				Image: "img1",
				State: ContainerStateRunning,
			},
			{
				ID:    ContainerID{Type: "docker", ID: "c2"},
				Name:  "c2",
				Image: "img2",
				State: ContainerStateExited,
			},
		},
		SandboxStatuses: []*runtimeapi.PodSandboxStatus{
			{
				Id:    "sandbox1",
				State: runtimeapi.PodSandboxState_SANDBOX_READY,
			},
			{
				Id:    "sandbox2",
				State: runtimeapi.PodSandboxState_SANDBOX_NOTREADY,
			},
		},
	}
	runtimeName := "docker"
	runningPod := ConvertPodStatusToRunningPod(runtimeName, podStatus)
	assert.Equal(t, podStatus.ID, runningPod.ID, "ConvertPodStatusToRunningPod did not copy pod ID correctly")
	assert.Equal(t, podStatus.Name, runningPod.Name, "ConvertPodStatusToRunningPod did not copy pod Name correctly")
	assert.Equal(t, podStatus.Namespace, runningPod.Namespace, "ConvertPodStatusToRunningPod did not copy pod Namespace correctly")

	if assert.Len(t, runningPod.Containers, 1, "expected 1 running container, got %d", len(runningPod.Containers)) {
		assert.Equal(t, "c1", runningPod.Containers[0].Name, "expected running container name 'c1', got %q", runningPod.Containers[0].Name)
	}

	if assert.Len(t, runningPod.Sandboxes, 2, "expected 2 sandboxes, got %d", len(runningPod.Sandboxes)) {
		assert.Equal(t, "sandbox1", runningPod.Sandboxes[0].ID.ID, "expected sandbox1 to be running")
		assert.Equal(t, ContainerStateRunning, runningPod.Sandboxes[0].State, "expected sandbox1 to be running")
		assert.Equal(t, "sandbox2", runningPod.Sandboxes[1].ID.ID, "expected sandbox2 to be exited")
		assert.Equal(t, ContainerStateExited, runningPod.Sandboxes[1].State, "expected sandbox2 to be exited")
	}
}

// TestSandboxToContainerState 校验 sandbox 状态到容器状态的映射，包括未知值。
func TestSandboxToContainerState(t *testing.T) {
	testCases := []struct {
		name     string
		input    runtimeapi.PodSandboxState
		expected State
	}{
		{"ready", runtimeapi.PodSandboxState_SANDBOX_READY, ContainerStateRunning},
		{"notready", runtimeapi.PodSandboxState_SANDBOX_NOTREADY, ContainerStateExited},
		{"unknown", 99, ContainerStateUnknown},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			actual := SandboxToContainerState(tc.input)
			assert.Equal(t, tc.expected, actual, "SandboxToContainerState(%v)", tc.input)
		})
	}
}

// TestAllContainersAreWindowsHostProcess 校验 AllContainersAreWindowsHostProcess
// 仅在全部容器均为 HostProcess 时返回 true。
func TestAllContainersAreWindowsHostProcess(t *testing.T) {
	trueVar := true
	falseVar := false
	containerName := "container"

	testCases := []struct {
		name           string
		podSpec        *v1.PodSpec
		expectedResult bool
	}{
		{
			name: "all containers hostprocess true",
			podSpec: &v1.PodSpec{
				Containers: []v1.Container{{
					Name: containerName,
					SecurityContext: &v1.SecurityContext{
						WindowsOptions: &v1.WindowsSecurityContextOptions{
							HostProcess: &trueVar,
						},
					},
				}},
			},
			expectedResult: true,
		},
		{
			name: "one container hostprocess false",
			podSpec: &v1.PodSpec{
				Containers: []v1.Container{{
					Name: containerName,
					SecurityContext: &v1.SecurityContext{
						WindowsOptions: &v1.WindowsSecurityContextOptions{
							HostProcess: &falseVar,
						},
					},
				}},
			},
			expectedResult: false,
		},
		{
			name: "mixed containers",
			podSpec: &v1.PodSpec{
				Containers: []v1.Container{
					{
						Name: containerName,
						SecurityContext: &v1.SecurityContext{
							WindowsOptions: &v1.WindowsSecurityContextOptions{
								HostProcess: &trueVar,
							},
						},
					},
					{
						Name: containerName,
						SecurityContext: &v1.SecurityContext{
							WindowsOptions: &v1.WindowsSecurityContextOptions{
								HostProcess: &falseVar,
							},
						},
					},
				},
			},
			expectedResult: false,
		},
		{
			name: "no containers",
			podSpec: &v1.PodSpec{
				Containers: []v1.Container{},
			},
			expectedResult: true, // 按定义即为 true
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			pod := &v1.Pod{}
			pod.Spec = *tc.podSpec
			result := AllContainersAreWindowsHostProcess(pod)
			assert.Equal(t, tc.expectedResult, result)
		})
	}
}

// TestHasAnyRegularContainerStarted 校验 HasAnyRegularContainerStarted 只统计普通容器
// （不含 init 与临时容器），且已终止的普通容器也算已启动。
func TestHasAnyRegularContainerStarted(t *testing.T) {
	testCases := []struct {
		name          string
		spec          *v1.PodSpec
		statuses      []v1.ContainerStatus
		expectStarted bool
	}{
		{
			name: "no statuses",
			spec: &v1.PodSpec{
				Containers: []v1.Container{
					{Name: "regular"},
				},
			},
			statuses:      []v1.ContainerStatus{},
			expectStarted: false,
		},
		{
			name: "no regular containers started",
			spec: &v1.PodSpec{
				InitContainers: []v1.Container{
					{Name: "init"},
				},
				Containers: []v1.Container{
					{Name: "regular"},
				},
			},
			statuses: []v1.ContainerStatus{
				{
					Name: "init",
					State: v1.ContainerState{
						Running: &v1.ContainerStateRunning{},
					},
				},
			},
			expectStarted: false,
		},
		{
			name: "regular container running",
			spec: &v1.PodSpec{
				InitContainers: []v1.Container{
					{Name: "init"},
				},
				Containers: []v1.Container{
					{Name: "regular"},
				},
			},
			statuses: []v1.ContainerStatus{
				{
					Name: "init",
					State: v1.ContainerState{
						Terminated: &v1.ContainerStateTerminated{},
					},
				},
				{
					Name: "regular",
					State: v1.ContainerState{
						Running: &v1.ContainerStateRunning{},
					},
				},
			},
			expectStarted: true,
		},
		{
			name: "regular container terminated",
			spec: &v1.PodSpec{
				Containers: []v1.Container{
					{Name: "regular"},
				},
			},
			statuses: []v1.ContainerStatus{
				{
					Name: "regular",
					State: v1.ContainerState{
						Terminated: &v1.ContainerStateTerminated{},
					},
				},
			},
			expectStarted: true,
		},
		{
			name: "regular container waiting",
			spec: &v1.PodSpec{
				Containers: []v1.Container{
					{Name: "regular"},
				},
			},
			statuses: []v1.ContainerStatus{
				{
					Name: "regular",
					State: v1.ContainerState{
						Waiting: &v1.ContainerStateWaiting{},
					},
				},
			},
			expectStarted: false,
		},
		{
			name: "ephemeral container running",
			spec: &v1.PodSpec{
				Containers: []v1.Container{
					{Name: "regular"},
				},
				EphemeralContainers: []v1.EphemeralContainer{
					{EphemeralContainerCommon: v1.EphemeralContainerCommon{Name: "debug"}},
				},
			},
			statuses: []v1.ContainerStatus{
				{
					Name: "debug",
					State: v1.ContainerState{
						Running: &v1.ContainerStateRunning{},
					},
				},
			},
			expectStarted: false,
		},
		{
			name: "multiple regular containers",
			spec: &v1.PodSpec{
				Containers: []v1.Container{
					{Name: "regular1"},
					{Name: "regular2"},
				},
			},
			statuses: []v1.ContainerStatus{
				{
					Name: "regular1",
					State: v1.ContainerState{
						Waiting: &v1.ContainerStateWaiting{},
					},
				},
				{
					Name: "regular2",
					State: v1.ContainerState{
						Running: &v1.ContainerStateRunning{},
					},
				},
			},
			expectStarted: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			actual := HasAnyRegularContainerStarted(tc.spec, tc.statuses)
			assert.Equal(t, tc.expectStarted, actual, "HasAnyRegularContainerStarted(%v, %v)", tc.spec, tc.statuses)
		})
	}
}

// TestShouldAllContainersRestart 校验 ShouldAllContainersRestart 在 pod 条件标记、
// 退出码匹配重启规则等情形下的判定。
func TestShouldAllContainersRestart(t *testing.T) {
	restartPolicyNever := v1.ContainerRestartPolicyNever
	restartPolicyAlways := v1.ContainerRestartPolicyAlways
	restartRuleRestartAllContainers := v1.ContainerRestartRule{
		Action: v1.ContainerRestartRuleActionRestartAllContainers,
		ExitCodes: &v1.ContainerRestartRuleOnExitCodes{
			Operator: v1.ContainerRestartRuleOnExitCodesOpIn,
			Values:   []int32{42},
		},
	}
	RestartAllContainersCondition := v1.PodCondition{
		Type:   v1.AllContainersRestarting,
		Status: v1.ConditionTrue,
	}

	testcases := []struct {
		name         string
		pod          *v1.Pod
		podStatus    *PodStatus
		apiPodStatus *v1.PodStatus
		expected     bool
	}{
		{
			"pod marked with condition",
			&v1.Pod{
				Spec: v1.PodSpec{
					Containers: []v1.Container{
						{
							Name:          "regular",
							RestartPolicy: &restartPolicyNever,
						},
					},
				},
			},
			&PodStatus{
				ContainerStatuses: []*Status{
					{
						Name:  "regular",
						State: ContainerStateRunning,
					},
				},
			},
			&v1.PodStatus{
				Conditions: []v1.PodCondition{RestartAllContainersCondition},
			},
			true,
		},
		{
			"regular container exited with matching rules",
			&v1.Pod{
				Spec: v1.PodSpec{
					Containers: []v1.Container{
						{
							Name:               "regular",
							RestartPolicy:      &restartPolicyNever,
							RestartPolicyRules: []v1.ContainerRestartRule{restartRuleRestartAllContainers},
						},
					},
				},
			},
			&PodStatus{
				ContainerStatuses: []*Status{
					{
						Name:     "regular",
						State:    ContainerStateExited,
						ExitCode: 42,
					},
				},
			},
			nil,
			true,
		},
		{
			"init container exited with matching rules",
			&v1.Pod{
				Spec: v1.PodSpec{
					InitContainers: []v1.Container{
						{
							Name:               "init",
							RestartPolicy:      &restartPolicyNever,
							RestartPolicyRules: []v1.ContainerRestartRule{restartRuleRestartAllContainers},
						},
					},
				},
			},
			&PodStatus{
				ContainerStatuses: []*Status{
					{
						Name:     "init",
						State:    ContainerStateExited,
						ExitCode: 42,
					},
				},
			},
			nil,
			true,
		},
		{
			"sidecar container exited with matching rules",
			&v1.Pod{
				Spec: v1.PodSpec{
					InitContainers: []v1.Container{
						{
							Name:               "init",
							RestartPolicy:      &restartPolicyAlways,
							RestartPolicyRules: []v1.ContainerRestartRule{restartRuleRestartAllContainers},
						},
					},
				},
			},
			&PodStatus{
				ContainerStatuses: []*Status{
					{
						Name:     "init",
						State:    ContainerStateExited,
						ExitCode: 42,
					},
				},
			},
			nil,
			true,
		},
		{
			"container exited without rules",
			&v1.Pod{
				Spec: v1.PodSpec{
					Containers: []v1.Container{
						{
							Name: "regular",
						},
					},
				},
			},
			&PodStatus{
				ContainerStatuses: []*Status{
					{
						Name:     "regular",
						State:    ContainerStateExited,
						ExitCode: 1,
					},
				},
			},
			nil,
			false,
		},
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			actual := ShouldAllContainersRestart(tc.pod, tc.podStatus, tc.apiPodStatus)
			assert.Equal(t, tc.expected, actual, "ShouldAllContainersRestart(%v, %v)", tc.pod, tc.podStatus)
		})
	}
}
