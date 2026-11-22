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

// Package server 提供 apiextensions-apiserver 的命令行入口和启动逻辑。
// NewServerCommand 创建 cobra 命令，Run 执行完整的启动流程：
// 解析配置 → 创建 API Server → 启动服务和控制器。
package server

import (
	"context"
	"io"

	"github.com/spf13/cobra"

	"k8s.io/apiextensions-apiserver/pkg/cmd/server/options"
	genericapiserver "k8s.io/apiserver/pkg/server"
)

// NewServerCommand 创建 apiextensions-apiserver 的 cobra 命令。
// 命令流程：Complete → Validate → Run，符合 Kubernetes 标准 API Server 启动模式。
func NewServerCommand(ctx context.Context, out, errOut io.Writer) *cobra.Command {
	o := options.NewCustomResourceDefinitionsServerOptions(out, errOut)

	cmd := &cobra.Command{
		Short: "Launch an API extensions API server",
		Long:  "Launch an API extensions API server",
		PersistentPreRunE: func(*cobra.Command, []string) error {
			return o.ServerRunOptions.ComponentGlobalsRegistry.Set()
		},
		RunE: func(c *cobra.Command, args []string) error {
			if err := o.Complete(); err != nil {
				return err
			}
			if err := o.Validate(); err != nil {
				return err
			}
			if err := Run(c.Context(), o); err != nil {
				return err
			}
			return nil
		},
	}
	cmd.SetContext(ctx)

	fs := cmd.Flags()
	o.AddFlags(fs)
	return cmd
}

// Run 执行 apiextensions-apiserver 的完整启动流程：
// 1. 从选项生成 Config
// 2. 创建 API Server 实例（注册 CRD Storage、启动控制器）
// 3. 准备并运行服务器（监听端口、启动 HTTP 服务）
func Run(ctx context.Context, o *options.CustomResourceDefinitionsServerOptions) error {
	config, err := o.Config()
	if err != nil {
		return err
	}

	server, err := config.Complete().New(genericapiserver.NewEmptyDelegate())
	if err != nil {
		return err
	}
	return server.GenericAPIServer.PrepareRun().RunWithContext(ctx)
}
