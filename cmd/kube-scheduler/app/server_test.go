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

package app

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/spf13/pflag"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/version"
	"k8s.io/apiserver/pkg/util/feature"
	basecompatibility "k8s.io/component-base/compatibility"
	componentbaseconfig "k8s.io/component-base/config"
	"k8s.io/component-base/featuregate"
	configv1 "k8s.io/kube-scheduler/config/v1"
	fwk "k8s.io/kube-scheduler/framework"
	"k8s.io/kubernetes/cmd/kube-scheduler/app/options"
	"k8s.io/kubernetes/pkg/scheduler/apis/config"
	"k8s.io/kubernetes/pkg/scheduler/apis/config/testing/defaults"
)

// TestSetup 验证 Setup 能否把配置文件、默认 Profile 和树外插件注册选项组装成可用 Scheduler。
// 测试通过临时 HTTPS API 端点和 kubeconfig 隔离真实集群。
func TestSetup(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "scheduler-options")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		w.WriteHeader(200)
		w.Write([]byte(`ok`))
	}))
	defer server.Close()

	configKubeconfig := filepath.Join(tmpDir, "config.kubeconfig")
	if err := os.WriteFile(configKubeconfig, []byte(fmt.Sprintf(`
apiVersion: v1
kind: Config
clusters:
- cluster:
    insecure-skip-tls-verify: true
    server: %s
  name: default
contexts:
- context:
    cluster: default
    user: default
  name: default
current-context: default
users:
- name: default
  user:
    username: config
`, server.URL)), os.FileMode(0600)); err != nil {
		t.Fatal(err)
	}

	pluginConfigFilev1 := filepath.Join(tmpDir, "pluginv1.yaml")
	if err := os.WriteFile(pluginConfigFilev1, []byte(fmt.Sprintf(`
apiVersion: kubescheduler.config.k8s.io/v1
kind: KubeSchedulerConfiguration
clientConnection:
  kubeconfig: '%s'
profiles:
- plugins:
    multiPoint:
      enabled:
      - name: SchedulingGates
      - name: DefaultBinder
      - name: PrioritySort
      - name: DefaultPreemption
      - name: VolumeBinding
      - name: NodeResourcesFit
      - name: NodePorts
      - name: InterPodAffinity
      - name: TaintToleration
      - name: DynamicResources
      disabled:
      - name: "*"
    preFilter:
      disabled:
      - name: VolumeBinding
      - name: InterPodAffinity
    filter:
      disabled:
      - name: VolumeBinding
      - name: InterPodAffinity
      - name: TaintToleration
    score:
      disabled:
      - name: VolumeBinding
      - name: NodeResourcesFit
`, configKubeconfig)), os.FileMode(0600)); err != nil {
		t.Fatal(err)
	}

	simplifiedPluginConfigFilev1 := filepath.Join(tmpDir, "simplifiedPluginv1.yaml")
	if err := os.WriteFile(simplifiedPluginConfigFilev1, []byte(fmt.Sprintf(`
apiVersion: kubescheduler.config.k8s.io/v1
kind: KubeSchedulerConfiguration
clientConnection:
  kubeconfig: '%s'
profiles:
- schedulerName: simplified-scheduler
`, configKubeconfig)), os.FileMode(0600)); err != nil {
		t.Fatal(err)
	}

	// 树外插件 v1 配置
	outOfTreePluginConfigFilev1 := filepath.Join(tmpDir, "outOfTreePluginv1.yaml")
	if err := os.WriteFile(outOfTreePluginConfigFilev1, []byte(fmt.Sprintf(`
apiVersion: kubescheduler.config.k8s.io/v1
kind: KubeSchedulerConfiguration
clientConnection:
  kubeconfig: '%s'
profiles:
- plugins:
    preFilter:
      enabled:
      - name: Foo
    filter:
      enabled:
      - name: Foo
`, configKubeconfig)), os.FileMode(0600)); err != nil {
		t.Fatal(err)
	}

	// 多 Profile 配置
	multiProfilesConfig := filepath.Join(tmpDir, "multi-profiles.yaml")
	if err := os.WriteFile(multiProfilesConfig, []byte(fmt.Sprintf(`
apiVersion: kubescheduler.config.k8s.io/v1
kind: KubeSchedulerConfiguration
clientConnection:
  kubeconfig: '%s'
profiles:
- schedulerName: "profile-default-plugins"
- schedulerName: "profile-disable-all-filter-and-score-plugins"
  plugins:
    preFilter:
      disabled:
      - name: "*"
    filter:
      disabled:
      - name: "*"
    postFilter:
      disabled:
      - name: "*"
    preScore:
      disabled:
      - name: "*"
    score:
      disabled:
      - name: "*"
`, configKubeconfig)), os.FileMode(0600)); err != nil {
		t.Fatal(err)
	}

	// 未显式配置选主参数
	emptyLeaderElectionConfig := filepath.Join(tmpDir, "empty-leader-election-config.yaml")
	if err := os.WriteFile(emptyLeaderElectionConfig, []byte(fmt.Sprintf(`
apiVersion: kubescheduler.config.k8s.io/v1
kind: KubeSchedulerConfiguration
clientConnection:
  kubeconfig: '%s'
`, configKubeconfig)), os.FileMode(0600)); err != nil {
		t.Fatal(err)
	}

	// 显式指定租约时长的选主配置
	leaderElectionConfig := filepath.Join(tmpDir, "leader-election-config.yaml")
	if err := os.WriteFile(leaderElectionConfig, []byte(fmt.Sprintf(`
apiVersion: kubescheduler.config.k8s.io/v1
kind: KubeSchedulerConfiguration
clientConnection:
  kubeconfig: '%s'
leaderElection:
  leaseDuration: 1h
`, configKubeconfig)), os.FileMode(0600)); err != nil {
		t.Fatal(err)
	}

	testcases := []struct {
		name                 string
		flags                []string
		registryOptions      []Option
		wantPlugins          map[string]*config.Plugins
		wantLeaderElection   *componentbaseconfig.LeaderElectionConfiguration
		wantClientConnection *componentbaseconfig.ClientConnectionConfiguration
		wantErr              bool
		wantFeaturesGates    map[string]bool
	}{
		{
			name: "default config with beta feature explicitly enabled",
			flags: []string{
				"--kubeconfig", configKubeconfig,
				"--feature-gates=StorageCapacityScoring=true",
			},
			wantPlugins: map[string]*config.Plugins{
				"default-scheduler": defaults.ExpandedPluginsV1,
			},
		},
		{
			name: "default config with beta feature explicitly disabled",
			flags: []string{
				"--kubeconfig", configKubeconfig,
				"--feature-gates=StorageCapacityScoring=false",
			},
			wantPlugins: map[string]*config.Plugins{
				"default-scheduler": defaults.ExpandedPluginsV1,
			},
		},
		{
			name: "component configuration v1 with only scheduler name configured",
			flags: []string{
				"--config", simplifiedPluginConfigFilev1,
				"--kubeconfig", configKubeconfig,
			},
			wantPlugins: map[string]*config.Plugins{
				"simplified-scheduler": defaults.ExpandedPluginsV1,
			},
		},
		{
			name: "default config",
			flags: []string{
				"--kubeconfig", configKubeconfig,
			},
			wantPlugins: map[string]*config.Plugins{
				"default-scheduler": defaults.ExpandedPluginsV1,
			},
		},
		{
			name: "component configuration v1",
			flags: []string{
				"--config", pluginConfigFilev1,
				"--kubeconfig", configKubeconfig,
			},
			wantPlugins: map[string]*config.Plugins{
				"default-scheduler": func() *config.Plugins {
					plugins := defaults.ExpandedPluginsV1.DeepCopy()
					// 此配置会使 DynamicResources 在默认展开顺序中位于 DefaultPreemption 之后。
					plugins.PreEnqueue.Enabled[1], plugins.PreEnqueue.Enabled[2] = plugins.PreEnqueue.Enabled[2], plugins.PreEnqueue.Enabled[1]
					plugins.PostFilter.Enabled[0], plugins.PostFilter.Enabled[1] = plugins.PostFilter.Enabled[1], plugins.PostFilter.Enabled[0]
					plugins.PodGroupPostFilter.Enabled[0], plugins.PodGroupPostFilter.Enabled[1] = plugins.PodGroupPostFilter.Enabled[1], plugins.PodGroupPostFilter.Enabled[0]
					plugins.Filter.Enabled = []config.Plugin{
						{Name: "NodeResourcesFit"},
						{Name: "NodePorts"},
						{Name: "DynamicResources"},
					}
					plugins.PreFilter.Enabled = []config.Plugin{
						{Name: "NodeResourcesFit"},
						{Name: "NodePorts"},
						{Name: "TaintToleration"},
						{Name: "DynamicResources"},
					}
					plugins.PreScore.Enabled = []config.Plugin{
						{Name: "VolumeBinding"},
						{Name: "NodeResourcesFit"},
						{Name: "InterPodAffinity"},
						{Name: "TaintToleration"},
					}
					plugins.Score.Enabled = []config.Plugin{
						{Name: "InterPodAffinity", Weight: 1},
						{Name: "TaintToleration", Weight: 1},
						{Name: "DynamicResources", Weight: 1},
					}
					return plugins
				}(),
			},
		},
		{
			name: "out-of-tree component configuration v1",
			flags: []string{
				"--config", outOfTreePluginConfigFilev1,
				"--kubeconfig", configKubeconfig,
			},
			registryOptions: []Option{WithPlugin("Foo", newFoo)},
			wantPlugins: map[string]*config.Plugins{
				"default-scheduler": func() *config.Plugins {
					plugins := defaults.ExpandedPluginsV1.DeepCopy()
					plugins.PreFilter.Enabled = append(plugins.PreFilter.Enabled, config.Plugin{Name: "Foo"})
					plugins.Filter.Enabled = append(plugins.Filter.Enabled, config.Plugin{Name: "Foo"})
					return plugins
				}(),
			},
		},
		{
			name: "leader election CLI args, along with --config arg",
			flags: []string{
				"--leader-elect=false",
				"--leader-elect-lease-duration=2h", // 命令行参数优先于组件配置字段
				"--kubeconfig=foo",                 // 指定 --config 时忽略废弃命令行参数
				"--config", emptyLeaderElectionConfig,
			},
			wantLeaderElection: &componentbaseconfig.LeaderElectionConfiguration{
				LeaderElect:       false,                                    // 来自命令行参数
				LeaseDuration:     metav1.Duration{Duration: 2 * time.Hour}, // 来自命令行参数
				RenewDeadline:     metav1.Duration{Duration: 10 * time.Second},
				RetryPeriod:       metav1.Duration{Duration: 2 * time.Second},
				ResourceLock:      "leases",
				ResourceName:      configv1.SchedulerDefaultLockObjectName,
				ResourceNamespace: configv1.SchedulerDefaultLockObjectNamespace,
			},
			wantClientConnection: &componentbaseconfig.ClientConnectionConfiguration{
				Kubeconfig:  configKubeconfig,
				ContentType: "application/vnd.kubernetes.protobuf",
				QPS:         50,
				Burst:       100,
			},
		},
		{
			name: "leader election CLI args, without --config arg",
			flags: []string{
				"--leader-elect=false",
				"--leader-elect-lease-duration=2h",
				"--leader-elect-resource-namespace=default",
				"--kubeconfig", configKubeconfig, // 未指定 --config 时采用废弃命令行参数
			},
			wantLeaderElection: &componentbaseconfig.LeaderElectionConfiguration{
				LeaderElect:       false,                                    // 来自命令行参数
				LeaseDuration:     metav1.Duration{Duration: 2 * time.Hour}, // 来自命令行参数
				RenewDeadline:     metav1.Duration{Duration: 10 * time.Second},
				RetryPeriod:       metav1.Duration{Duration: 2 * time.Second},
				ResourceLock:      "leases",
				ResourceName:      configv1.SchedulerDefaultLockObjectName,
				ResourceNamespace: "default", // 来自命令行参数
			},
			wantClientConnection: &componentbaseconfig.ClientConnectionConfiguration{
				Kubeconfig:  configKubeconfig, // 来自废弃命令行参数
				ContentType: "application/vnd.kubernetes.protobuf",
				QPS:         50,
				Burst:       100,
			},
		},
		{
			name: "leader election settings specified by ComponentConfig only",
			flags: []string{
				"--config", leaderElectionConfig,
			},
			wantLeaderElection: &componentbaseconfig.LeaderElectionConfiguration{
				LeaderElect:       true,
				LeaseDuration:     metav1.Duration{Duration: 1 * time.Hour}, // 来自组件配置
				RenewDeadline:     metav1.Duration{Duration: 10 * time.Second},
				RetryPeriod:       metav1.Duration{Duration: 2 * time.Second},
				ResourceLock:      "leases",
				ResourceName:      configv1.SchedulerDefaultLockObjectName,
				ResourceNamespace: configv1.SchedulerDefaultLockObjectNamespace,
			},
		},
		{
			name: "leader election settings specified by CLI args and ComponentConfig",
			flags: []string{
				"--leader-elect=true",
				"--leader-elect-renew-deadline=5s",
				"--leader-elect-retry-period=1s",
				"--config", leaderElectionConfig,
			},
			wantLeaderElection: &componentbaseconfig.LeaderElectionConfiguration{
				LeaderElect:       true,
				LeaseDuration:     metav1.Duration{Duration: 1 * time.Hour},   // 来自组件配置
				RenewDeadline:     metav1.Duration{Duration: 5 * time.Second}, // 来自命令行参数
				RetryPeriod:       metav1.Duration{Duration: 1 * time.Second}, // 来自命令行参数
				ResourceLock:      "leases",
				ResourceName:      configv1.SchedulerDefaultLockObjectName,
				ResourceNamespace: configv1.SchedulerDefaultLockObjectNamespace,
			},
		},
		{
			name: "emulated version out of range",
			flags: []string{
				"--kubeconfig", configKubeconfig,
				"--emulated-version=1.28",
			},
			wantErr: true,
		},
		{
			name: "default feature gates at binary version",
			flags: []string{
				"--kubeconfig", configKubeconfig,
			},
			wantFeaturesGates: map[string]bool{"kubeA": true, "kubeB": false},
		},
		{
			name: "default feature gates at emulated version",
			flags: []string{
				"--kubeconfig", configKubeconfig,
				"--emulated-version=1.31",
			},
			wantFeaturesGates: map[string]bool{"kubeA": false, "kubeB": false},
		},
		{
			name: "set feature gates at emulated version",
			flags: []string{
				"--kubeconfig", configKubeconfig,
				"--emulated-version=1.31",
				"--feature-gates=kubeA=false,kubeB=true",
			},
			wantFeaturesGates: map[string]bool{"kubeA": false, "kubeB": true},
		},
		{
			name: "cannot set locked feature gate",
			flags: []string{
				"--kubeconfig", configKubeconfig,
				"--feature-gates=kubeA=false,kubeB=true",
			},
			wantErr: true,
		},
	}

	makeListener := func(t *testing.T) net.Listener {
		t.Helper()
		l, err := net.Listen("tcp", ":0")
		if err != nil {
			t.Fatal(err)
		}
		return l
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			componentGlobalsRegistry := basecompatibility.NewComponentGlobalsRegistry()
			// StorageCapacityScoring 自 v1.37 起默认启用，因此二进制版本至少设为 1.37。
			verKube := basecompatibility.NewEffectiveVersionFromString("1.37", "1.31", "1.31")
			fg := feature.DefaultFeatureGate.DeepCopy()
			utilruntime.Must(fg.AddVersioned(map[featuregate.Feature]featuregate.VersionedSpecs{
				"kubeA": {
					{Version: version.MustParse("1.30"), Default: false, PreRelease: featuregate.Beta},
					{Version: version.MustParse("1.32"), Default: true, LockToDefault: true, PreRelease: featuregate.GA},
				},
				"kubeB": {
					{Version: version.MustParse("1.31"), Default: false, PreRelease: featuregate.Alpha},
				},
			}))
			utilruntime.Must(componentGlobalsRegistry.Register(basecompatibility.DefaultKubeComponent, verKube, fg))

			fs := pflag.NewFlagSet("test", pflag.PanicOnError)
			opts := options.NewOptionsWithComponentGlobalsRegistry(componentGlobalsRegistry)

			// 使用动态 Listener 而非固定端口，避免并行测试冲突
			opts.SecureServing.Listener = makeListener(t)
			defer opts.SecureServing.Listener.Close()

			nfs := opts.Flags
			for _, f := range nfs.FlagSets {
				fs.AddFlagSet(f)
			}
			if err := fs.Parse(tc.flags); err != nil {
				t.Fatal(err)
			}

			// 使用动态 Listener 而非固定端口，避免并行测试冲突
			opts.SecureServing.Listener = makeListener(t)
			defer opts.SecureServing.Listener.Close()

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			_, sched, err := Setup(ctx, opts, tc.registryOptions...)
			if tc.wantErr {
				if err == nil {
					t.Fatal("expected Setup error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}

			if tc.wantPlugins != nil {
				gotPlugins := make(map[string]*config.Plugins)
				for n, p := range sched.Profiles {
					gotPlugins[n] = p.ListPlugins()
				}

				if diff := cmp.Diff(tc.wantPlugins, gotPlugins); diff != "" {
					t.Errorf("Unexpected plugins diff (-want, +got): %s", diff)
				}
			}

			if tc.wantLeaderElection != nil {
				gotLeaderElection := opts.ComponentConfig.LeaderElection
				if diff := cmp.Diff(*tc.wantLeaderElection, gotLeaderElection); diff != "" {
					t.Errorf("Unexpected leaderElection diff (-want, +got): %s", diff)
				}
			}

			if tc.wantClientConnection != nil {
				gotClientConnection := opts.ComponentConfig.ClientConnection
				if diff := cmp.Diff(*tc.wantClientConnection, gotClientConnection); diff != "" {
					t.Errorf("Unexpected clientConnection diff (-want, +got): %s", diff)
				}
			}
			for f, v := range tc.wantFeaturesGates {
				enabled := fg.Enabled(featuregate.Feature(f))
				if enabled != v {
					t.Errorf("expected featuregate.Enabled(%s)=%v, got %v", f, v, enabled)
				}
			}
		})
	}
}

// foo 模拟由集群外注册的调度插件。
type foo struct{}

var _ fwk.PreFilterPlugin = &foo{}
var _ fwk.FilterPlugin = &foo{}

func (*foo) Name() string {
	return "Foo"
}

func newFoo(_ context.Context, _ runtime.Object, _ fwk.Handle) (fwk.Plugin, error) {
	return &foo{}, nil
}

func (*foo) PreFilter(_ context.Context, _ fwk.CycleState, _ *v1.Pod, _ []fwk.NodeInfo) (*fwk.PreFilterResult, *fwk.Status) {
	return nil, nil
}

func (*foo) PreFilterExtensions() fwk.PreFilterExtensions {
	return nil
}

func (*foo) Filter(_ context.Context, _ fwk.CycleState, _ *v1.Pod, nodeInfo fwk.NodeInfo) *fwk.Status {
	return nil
}
