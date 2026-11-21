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

package podresources

import (
	"context"
	"fmt"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"k8s.io/cri-client/pkg/util"
	v1 "k8s.io/kubelet/pkg/apis/podresources/v1"
	"k8s.io/kubelet/pkg/apis/podresources/v1alpha1"
)

// 注意：pod resources API 的使用方不应导入本包，
// 而应把相关函数复制到自己的项目中。

// GetV1alpha1Client 返回 PodResourcesLister gRPC 服务的客户端。
// 注意：该版本已废弃。
func GetV1alpha1Client(ctx context.Context, socket string, connectionTimeout time.Duration, maxMsgSize int) (v1alpha1.PodResourcesListerClient, *grpc.ClientConn, error) {
	addr, dialer, err := util.GetAddressAndDialer(socket)
	if err != nil {
		return nil, nil, err
	}
	// 为建立连接设置超时，避免 socket 不可用时无限阻塞。
	ctxT, cancel := context.WithTimeout(ctx, connectionTimeout)
	defer cancel()

	// 通过 unix dialer 连接，并按需设置最大接收消息大小。
	conn, err := grpc.DialContext(ctxT, addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(dialer),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(maxMsgSize)))
	if err != nil {
		return nil, nil, fmt.Errorf("error dialing socket %s: %v", socket, err)
	}
	return v1alpha1.NewPodResourcesListerClient(conn), conn, nil
}

// GetV1Client 返回 PodResourcesLister gRPC 服务的 v1 客户端。
func GetV1Client(ctx context.Context, socket string, connectionTimeout time.Duration, maxMsgSize int) (v1.PodResourcesListerClient, *grpc.ClientConn, error) {
	addr, dialer, err := util.GetAddressAndDialer(socket)
	if err != nil {
		return nil, nil, err
	}
	// 为建立连接设置超时，避免 socket 不可用时无限阻塞。
	ctxT, cancel := context.WithTimeout(ctx, connectionTimeout)
	defer cancel()

	// 通过 unix dialer 连接，并按需设置最大接收消息大小。
	conn, err := grpc.DialContext(ctxT, addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(dialer),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(maxMsgSize)))
	if err != nil {
		return nil, nil, fmt.Errorf("error dialing socket %s: %v", socket, err)
	}
	return v1.NewPodResourcesListerClient(conn), conn, nil
}
