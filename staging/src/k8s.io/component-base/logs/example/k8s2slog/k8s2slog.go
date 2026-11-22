//go:build go1.21

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

// Package main 演示如何通过 component-base 配置同时初始化 klog 与 slog：
// 在 Go 1.21+ 上，日志配置会同时作用于标准库 slog。
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"strings"

	"github.com/spf13/cobra"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/component-base/cli"
	"k8s.io/component-base/featuregate"
	"k8s.io/component-base/logs"
	logsapi "k8s.io/component-base/logs/api/v1"
	"k8s.io/klog/v2"

	_ "k8s.io/component-base/logs/json/register"
)

var featureGate = featuregate.NewFeatureGate()

func main() {
	// 日志格式相关 feature gate 必须在注册 flag 之前加入，否则解析会失败。
	runtime.Must(logsapi.AddFeatureGates(featureGate))
	command := NewLoggerCommand()
	code := cli.Run(command)
	os.Exit(code)
}

// NewLoggerCommand 构造演示命令，注册日志 flag 与 feature gate flag。
func NewLoggerCommand() *cobra.Command {
	c := logsapi.NewLoggingConfiguration()
	cmd := &cobra.Command{
		Run: func(cmd *cobra.Command, args []string) {
			// 该调用在 Go >= 1.21 时会同时配置 klog 与全局 slog logger。
			logs.InitLogs()
			if err := logsapi.ValidateAndApply(c, featureGate); err != nil {
				fmt.Fprintf(os.Stderr, "%v\n", err)
				os.Exit(1)
			}
			if len(args) > 0 {
				fmt.Fprintf(os.Stderr, "Unexpected additional command line arguments:\n    %s\n", strings.Join(args, "\n    "))
				os.Exit(1)
			}

			// 输出一些示例，展示 Kubernetes 特殊类型在 slog/klog 中的渲染方式。
			podRef := klog.KObj(&metav1.ObjectMeta{Name: "some-pod", Namespace: "some-namespace"})
			podRefs := klog.KObjSlice([]interface{}{
				&metav1.ObjectMeta{Name: "some-pod", Namespace: "some-namespace"},
				nil,
				&metav1.ObjectMeta{Name: "other-pod"},
			})
			slog.Info("slog.Info", "pod", podRef, "pods", podRefs)
			klog.InfoS("klog.InfoS", "pod", podRef, "pods", podRefs)
			klog.Background().Info("klog.Background+logr.Logger.Info")
			klog.FromContext(context.Background()).Info("klog.FromContext+logr.Logger.Info")
			slogLogger := slog.Default()
			slogLogger.Info("slog.Default+slog.Logger.Info")
		},
	}
	if err := logsapi.AddFeatureGates(featureGate); err != nil {
		// 正常情况下不会失败；重复注册才可能触发。
		panic(err)
	}
	featureGate.AddFlag(cmd.Flags())
	logsapi.AddFlags(c, cmd.Flags())
	return cmd
}
