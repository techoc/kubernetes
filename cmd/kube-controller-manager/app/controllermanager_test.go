/*
Copyright 2023 The Kubernetes Authors.

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
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"

	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/version"
	"k8s.io/apiserver/pkg/server/healthz"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/component-base/featuregate"
	featuregatetesting "k8s.io/component-base/featuregate/testing"
	"k8s.io/klog/v2/ktesting"
	"k8s.io/kubernetes/cmd/kube-controller-manager/names"
	"k8s.io/kubernetes/pkg/features"
)

func TestControllerNamesConsistency(t *testing.T) {
	controllerNameRegexp := regexp.MustCompile("^[a-z]([-a-z]*[a-z])?$")

	for _, name := range KnownControllers() {
		if !controllerNameRegexp.MatchString(name) {
			t.Errorf("name consistency check failed: controller %q must consist of lower case alphabetic characters or '-', and must start and end with an alphabetic character", name)
		}
		if !strings.HasSuffix(name, "-controller") {
			t.Errorf("name consistency check failed: controller %q must have \"-controller\" suffix", name)
		}
	}
}

func TestControllerNamesDeclaration(t *testing.T) {
	declaredControllers := sets.NewString(
		names.ServiceAccountTokenController,
		names.EndpointsController,
		names.EndpointSliceController,
		names.EndpointSliceMirroringController,
		names.ReplicationControllerController,
		names.PodGarbageCollectorController,
		names.ResourceQuotaController,
		names.NamespaceController,
		names.ServiceAccountController,
		names.GarbageCollectorController,
		names.DaemonSetController,
		names.JobController,
		names.PodGroupProtectionController,
		names.DeploymentController,
		names.ReplicaSetController,
		names.HorizontalPodAutoscalerController,
		names.DisruptionController,
		names.StatefulSetController,
		names.CronJobController,
		names.CertificateSigningRequestSigningController,
		names.CertificateSigningRequestApprovingController,
		names.CertificateSigningRequestCleanerController,
		names.PodCertificateRequestCleanerController,
		names.TTLController,
		names.BootstrapSignerController,
		names.TokenCleanerController,
		names.NodeIpamController,
		names.NodeLifecycleController,
		names.TaintEvictionController,
		names.PersistentVolumeBinderController,
		names.PersistentVolumeAttachDetachController,
		names.PersistentVolumeExpanderController,
		names.ClusterRoleAggregationController,
		names.PersistentVolumeClaimProtectionController,
		names.PersistentVolumeProtectionController,
		names.VolumeAttributesClassProtectionController,
		names.TTLAfterFinishedController,
		names.RootCACertificatePublisherController,
		names.KubeAPIServerClusterTrustBundlePublisherController,
		names.EphemeralVolumeController,
		names.StorageVersionGarbageCollectorController,
		names.ResourceClaimController,
		names.DeviceTaintEvictionController,
		names.ResourcePoolStatusRequestController,
		names.LegacyServiceAccountTokenCleanerController,
		names.ValidatingAdmissionPolicyStatusController,
		names.ServiceCIDRController,
		names.StorageVersionMigratorController,
		names.SELinuxWarningController,
	)

	for _, name := range KnownControllers() {
		if !declaredControllers.Has(name) {
			t.Errorf("name declaration check failed: controller name %q should be declared in  \"controller_names.go\" and added to this test", name)
		}
	}
}

func TestNewControllerDescriptorsShouldNotPanic(t *testing.T) {
	NewControllerDescriptors()
}

func TestNewControllerDescriptorsAlwaysReturnsDescriptorsForAllControllers(t *testing.T) {
	featuregatetesting.SetFeatureGatesDuringTest(t, utilfeature.DefaultFeatureGate, featuregatetesting.FeatureOverrides{
		"AllAlpha": false,
		"AllBeta":  false,
	})

	controllersWithoutFeatureGates := KnownControllers()

	// 必须先启用 AllBeta，再启用 AllAlpha，以满足特性依赖。
	featuregatetesting.SetFeatureGatesDuringTest(t, utilfeature.DefaultFeatureGate, featuregatetesting.FeatureOverrides{
		"AllBeta":  true,
		"AllAlpha": true,
	})

	controllersWithFeatureGates := KnownControllers()

	if diff := cmp.Diff(controllersWithoutFeatureGates, controllersWithFeatureGates); diff != "" {
		t.Errorf("unexpected controllers after enabling feature gates, NewControllerDescriptors should always return all controller descriptors. Controllers should define required feature gates in ControllerDescriptor.requiredFeatureGates. Diff of returned controllers:\n%s", diff)
	}
}

func TestFeatureGatedControllersShouldNotDefineAliases(t *testing.T) {
	featureGateRegex := regexp.MustCompile("^([a-zA-Z0-9]+)")

	alphaFeatures := sets.NewString()
	for _, featureText := range utilfeature.DefaultFeatureGate.KnownFeatures() {
		// 单元测试不能使用可变 FeatureGate，因此从 KnownFeatures 解析。
		feature := featureGateRegex.FindString(featureText)
		if strings.Contains(featureText, string(featuregate.Alpha)) && feature != "AllAlpha" {
			alphaFeatures.Insert(feature)
		}

	}

	for name, controller := range NewControllerDescriptors() {
		if len(controller.GetAliases()) == 0 {
			continue
		}

		requiredFeatureGates := controller.GetRequiredFeatureGates()
		if len(requiredFeatureGates) == 0 {
			continue
		}

		// 不要在此列表新增控制器；唯一例外是本测试引入前已经存在的那个控制器。
		if name == names.ResourceClaimController {
			continue
		}

		areAllRequiredFeaturesAlpha := true
		for _, feature := range requiredFeatureGates {
			if !alphaFeatures.Has(string(feature)) {
				areAllRequiredFeaturesAlpha = false
				break
			}
		}

		if areAllRequiredFeaturesAlpha {
			t.Errorf("alias check failed: controller name %q should not be aliased as it is still guarded by alpha feature gates (%v) and thus should have only a canonical name", name, requiredFeatureGates)
		}
	}
}

// TestTaintEvictionControllerGating 验证只有启用 SeparateTaintEvictionController 时才能独立运行 taint-manager。
func TestTaintEvictionControllerGating(t *testing.T) {
	tests := []struct {
		name               string
		enableFeatureGate  bool
		expectInitFuncCall bool
	}{
		{
			name:               "standalone taint-eviction-controller should run when SeparateTaintEvictionController feature gate is enabled",
			enableFeatureGate:  true,
			expectInitFuncCall: true,
		},
		{
			name:               "standalone taint-eviction-controller should not run when SeparateTaintEvictionController feature gate is not enabled",
			enableFeatureGate:  false,
			expectInitFuncCall: false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			featuregatetesting.SetFeatureGateEmulationVersionDuringTest(t, utilfeature.DefaultFeatureGate, version.MustParse("1.33"))
			featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.SeparateTaintEvictionController, test.enableFeatureGate)
			_, ctx := ktesting.NewTestContext(t)
			ctx, cancel := context.WithCancel(ctx)
			defer cancel()

			controllerCtx := ControllerContext{}
			controllerCtx.ComponentConfig.Generic.Controllers = []string{names.TaintEvictionController}

			initFuncCalled := false

			taintEvictionControllerDescriptor := NewControllerDescriptors()[names.TaintEvictionController]
			taintEvictionControllerDescriptor.constructor = func(ctx context.Context, controllerContext ControllerContext, controllerName string) (Controller, error) {
				initFuncCalled = true
				return newControllerLoop(func(ctx context.Context) {}, controllerName), nil
			}

			var healthChecks mockHealthCheckAdder
			if err := runControllers(ctx, controllerCtx, map[string]*ControllerDescriptor{
				names.TaintEvictionController: taintEvictionControllerDescriptor,
			}, &healthChecks); err != nil {
				t.Errorf("starting a TaintEvictionController controller should not return an error")
			}
			if test.expectInitFuncCall != initFuncCalled {
				t.Errorf("TaintEvictionController init call check failed: expected=%v, got=%v", test.expectInitFuncCall, initFuncCalled)
			}
			hasHealthCheck := len(healthChecks.Checks) > 0
			expectHealthCheck := test.expectInitFuncCall
			if expectHealthCheck != hasHealthCheck {
				t.Errorf("TaintEvictionController healthCheck check failed: expected=%v, got=%v", expectHealthCheck, hasHealthCheck)
			}
		})
	}
}

func TestRunControllers(t *testing.T) {
	testCases := []struct {
		name                     string
		newController            func(ctx context.Context) Controller
		shutdownTimeout          time.Duration
		expectedCleanTermination bool
	}{
		{
			name: "clean shutdown",
			newController: func(testCtx context.Context) Controller {
				return newControllerLoop(func(ctx context.Context) {
					<-ctx.Done()
				}, "controller-A")
			},
			shutdownTimeout:          10 * time.Second,
			expectedCleanTermination: true,
		},
		{
			name: "shutdown timeout",
			newController: func(testCtx context.Context) Controller {
				return newControllerLoop(func(ctx context.Context) {
					<-testCtx.Done()
				}, "controller-A")
			},
			shutdownTimeout:          50 * time.Millisecond,
			expectedCleanTermination: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			_, ctx := ktesting.NewTestContext(t)
			controllerCtx := ControllerContext{}

			// testCtx 确保无法正常关闭的控制器在测试结束后仍能退出。
			testCtx, cancelTest := context.WithCancel(ctx)
			defer cancelTest()

			// ctx 用于等待控制器关闭并启动 RunControllers 的关闭超时；传入已取消上下文可立即触发超时。
			ctx, cancelController := context.WithCancel(ctx)
			cancelController()

			cleanShutdown := RunControllers(ctx, controllerCtx, []Controller{tc.newController(testCtx)}, 0, tc.shutdownTimeout)
			if cleanShutdown != tc.expectedCleanTermination {
				t.Errorf("expected clean shutdown %v, got %v", tc.expectedCleanTermination, cleanShutdown)
			}
		})
	}
}

type mockHealthCheckAdder struct {
	Checks []healthz.HealthChecker
}

func (m *mockHealthCheckAdder) AddHealthChecker(checks ...healthz.HealthChecker) {
	m.Checks = append(m.Checks, checks...)
}

func runControllers(
	ctx context.Context, controllerCtx ControllerContext,
	controllerDescriptors map[string]*ControllerDescriptor, healthzChecks HealthCheckAdder,
) error {
	controllers, err := BuildControllers(ctx, controllerCtx, controllerDescriptors, nil, healthzChecks)
	if err != nil {
		return err
	}
	RunControllers(ctx, controllerCtx, controllers, 0, 0)
	return nil
}
