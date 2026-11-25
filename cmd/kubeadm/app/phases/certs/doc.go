/*
Copyright 2016 The Kubernetes Authors.

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

package certs

/*

	阶段：证书

	输入：
		来自 InitConfiguration
			.API.AdvertiseAddress 是可选参数，可追加到 SAN IP 列表。
			.APIServer.CertSANs 是可选参数，可向 API Server 服务证书的 SAN 添加 DNS 名称和 IP。
			.Etcd.Local.ServerCertSANs 是可选参数，可向 etcd 服务证书的 SAN 添加 DNS 名称和 IP。
			.Etcd.Local.PeerCertSANs 是可选参数，可向 etcd 对等证书的 SAN 添加 DNS 名称和 IP。
			.Networking.DNSDomain 用于确定内部 Kubernetes 服务的 DNS 名称。
			.Networking.ServiceSubnet 用于确定内部 Kubernetes 服务指向的 IP。
			.CertificatesDir 用于确定所有证书的存储位置，必填。

	输出：
		文件写入 .CertificatesDir（默认 /etc/kubernetes/pki）：
		 - ca.crt
		 - ca.key
		 - apiserver.crt
		 - apiserver.key
		 - apiserver-kubelet-client.crt
		 - apiserver-kubelet-client.key
		 - apiserver-etcd-client.crt
		 - apiserver-etcd-client.key
		 - etcd/ca.crt
		 - etcd/ca.key
		 - etcd/server.crt
		 - etcd/server.key
		 - etcd/peer.crt
		 - etcd/peer.key
		 - etcd/healthcheck-client.crt
		 - etcd/healthcheck-client.key
		 - sa.pub
		 - sa.key
		 - front-proxy-ca.crt
		 - front-proxy-ca.key
		 - front-proxy-client.crt
		 - front-proxy-client.key

*/
