/*
Copyright 2021 The Kubernetes Authors.

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

// Package grpc 实现 gRPC 健康检查探针，用于检查容器是否通过 gRPC Health Checking Protocol 响应。
//
// gRPC 探针使用 gRPC Health Checking Protocol (grpc.health.v1.Health) 向容器发送 Check 请求：
//   - 服务返回 SERVING 状态：返回 Success
//   - 服务返回其他状态或连接失败：返回 Failure
//
// 支持 TLS 和非 TLS 两种模式，使用 ProbeDialer 减少 TIME-WAIT 状态的 socket 残留。
package grpc

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	grpchealth "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"k8s.io/component-base/version"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/probe"
)

// ProbeOptions 配置 gRPC 探针的选项。
type ProbeOptions struct {
	// UseTLS 为 true 时使用 TLS 连接（跳过证书验证）
	UseTLS bool
}

// Prober 是 gRPC 探针接口，定义 gRPC 就绪/存活/启动检查的方法。
type Prober interface {
	// Probe 执行 gRPC 健康检查，返回结果、消息和错误
	Probe(host, service string, port int, timeout time.Duration, options ProbeOptions) (probe.Result, string, error)
}

type grpcProber struct {
}

// New 创建 gRPC 探针。
func New() Prober {
	return grpcProber{}
}

// Probe 执行 gRPC 健康检查，判断容器的存活/就绪/启动状态。
//
// 使用 gRPC Health Checking Protocol 向容器发送 Check 请求。
// 任何失败都视为探针失败，模仿 grpc_health_probe 工具的行为。
// err 返回值始终为 nil，错误信息通过 string 返回值传递。
func (p grpcProber) Probe(host, service string, port int, timeout time.Duration, options ProbeOptions) (probe.Result, string, error) {
	v := version.Get()

	// 根据配置选择传输凭证：TLS 跳过证书验证，非 TLS 使用 insecure
	var transportCreds credentials.TransportCredentials
	if options.UseTLS {
		transportCreds = credentials.NewTLS(&tls.Config{
			InsecureSkipVerify: true,
		})
	} else {
		transportCreds = insecure.NewCredentials()
	}

	opts := []grpc.DialOption{
		// 设置 User-Agent 以便服务端识别探针来源
		grpc.WithUserAgent(fmt.Sprintf("kube-probe/%s.%s", v.Major, v.Minor)),
		grpc.WithBlock(), // 阻塞直到连接成功或超时
		grpc.WithTransportCredentials(transportCreds),
		// 使用 ProbeDialer 减少 TIME-WAIT 状态的 socket 残留
		grpc.WithContextDialer(func(ctx context.Context, addr string) (net.Conn, error) {
			return probe.ProbeDialer().DialContext(ctx, "tcp", addr)
		}),
	}

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	addr := net.JoinHostPort(host, fmt.Sprintf("%d", port))
	conn, err := grpc.DialContext(ctx, addr, opts...)

	if err != nil {
		if err == context.DeadlineExceeded {
			klog.V(4).ErrorS(err, "failed to connect grpc service due to timeout", "addr", addr, "service", service, "timeout", timeout)
			return probe.Failure, fmt.Sprintf("timeout: failed to connect service %q within %v: %+v", addr, timeout, err), nil
		} else {
			klog.V(4).ErrorS(err, "failed to connect grpc service", "service", addr)
			return probe.Failure, fmt.Sprintf("error: failed to connect service at %q: %+v", addr, err), nil
		}
	}

	defer func() {
		_ = conn.Close()
	}()

	// 创建 gRPC Health Checking Protocol 客户端
	client := grpchealth.NewHealthClient(conn)

	// 发送健康检查请求
	resp, err := client.Check(metadata.NewOutgoingContext(ctx, make(metadata.MD)), &grpchealth.HealthCheckRequest{
		Service: service,
	})

	if err != nil {
		stat, ok := status.FromError(err)
		if ok {
			switch stat.Code() {
			case codes.Unimplemented:
				// 服务端未实现 gRPC Health Checking Protocol
				klog.V(4).ErrorS(err, "server does not implement the grpc health protocol (grpc.health.v1.Health)", "addr", addr, "service", service)
				return probe.Failure, fmt.Sprintf("error: this server does not implement the grpc health protocol (grpc.health.v1.Health): %s", stat.Message()), nil
			case codes.DeadlineExceeded:
				// RPC 请求在超时内未完成
				klog.V(4).ErrorS(err, "rpc request not finished within timeout", "addr", addr, "service", service, "timeout", timeout)
				return probe.Failure, fmt.Sprintf("timeout: health rpc did not complete within %v", timeout), nil
			default:
				klog.V(4).ErrorS(err, "rpc probe failed")
			}
		} else {
			klog.V(4).ErrorS(err, "health rpc probe failed")
		}

		return probe.Failure, fmt.Sprintf("error: health rpc probe failed: %+v", err), nil
	}

	// 只有 SERVING 状态视为健康，其他状态（NOT_SERVING、UNKNOWN 等）都视为失败
	if resp.GetStatus() != grpchealth.HealthCheckResponse_SERVING {
		return probe.Failure, fmt.Sprintf("service unhealthy (responded with %q)", resp.GetStatus().String()), nil
	}

	return probe.Success, fmt.Sprintf("service healthy"), nil
}
