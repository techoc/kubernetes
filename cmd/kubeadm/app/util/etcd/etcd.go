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

package etcd

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"net/url"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"time"

	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	"go.etcd.io/etcd/client/pkg/v3/transport"
	clientv3 "go.etcd.io/etcd/client/v3"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	utilerrors "k8s.io/apimachinery/pkg/util/errors"
	"k8s.io/apimachinery/pkg/util/wait"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/klog/v2"

	kubeadmapi "k8s.io/kubernetes/cmd/kubeadm/app/apis/kubeadm"
	"k8s.io/kubernetes/cmd/kubeadm/app/constants"
	"k8s.io/kubernetes/cmd/kubeadm/app/util/errors"
)

const etcdTimeout = 2 * time.Second

// ErrNoMemberIDForPeerURL 表示无法从给定 peer URL 获取成员 ID。
var ErrNoMemberIDForPeerURL = errors.New("no member id found for peer URL")

// ClusterInterrogator 是获取 etcd 集群信息的接口。
type ClusterInterrogator interface {
	CheckClusterHealth() error
	WaitForClusterAvailable(retries int, retryInterval time.Duration) (bool, error)
	Sync() error
	ListMembers() ([]Member, error)
	AddMember(name string, peerAddrs string) ([]Member, error)
	AddMemberAsLearner(name string, peerAddrs string) ([]Member, error)
	MemberPromote(learnerID uint64) error
	GetMemberID(peerURL string) (uint64, error)
	RemoveMember(id uint64) ([]Member, error)
}

type etcdClient interface {
	// Close 关闭客户端的 etcd 连接。
	Close() error

	// Endpoints 列出客户端已注册的端点。
	Endpoints() []string

	// MemberList 列出当前集群成员。
	MemberList(ctx context.Context, opts ...clientv3.OpOption) (*clientv3.MemberListResponse, error)

	// MemberAdd 向集群添加成员。
	MemberAdd(ctx context.Context, peerAddrs []string) (*clientv3.MemberAddResponse, error)

	// MemberAddAsLearner 向集群添加 learner 成员。
	MemberAddAsLearner(ctx context.Context, peerAddrs []string) (*clientv3.MemberAddResponse, error)

	// MemberRemove 从集群移除已有成员。
	MemberRemove(ctx context.Context, id uint64) (*clientv3.MemberRemoveResponse, error)

	// MemberPromote 将成员从 raft learner（无投票权）提升为有投票权成员。
	MemberPromote(ctx context.Context, id uint64) (*clientv3.MemberPromoteResponse, error)

	// Status 获取端点状态。
	Status(ctx context.Context, endpoint string) (*clientv3.StatusResponse, error)

	// Sync 根据 etcd 成员列表中的已知端点同步客户端端点。
	Sync(ctx context.Context) error
}

// Client 保存 etcd 集群的连接参数。
type Client struct {
	Endpoints []string

	newEtcdClient func(endpoints []string) (etcdClient, error)

	listMembersFunc func(timeout time.Duration) (*clientv3.MemberListResponse, error)
}

type etcdMemberStatus struct {
	ep     string
	status *clientv3.StatusResponse
	// err 保存与 etcd 服务器通信时遇到的任意错误。
	err error
}

// New 创建新的 EtcdCluster 客户端。
func New(endpoints []string, ca, cert, key string) (*Client, error) {
	client := Client{Endpoints: endpoints}

	var err error
	var tlsConfig *tls.Config
	if ca != "" || cert != "" || key != "" {
		tlsInfo := transport.TLSInfo{
			CertFile:      cert,
			KeyFile:       key,
			TrustedCAFile: ca,
		}
		tlsConfig, err = tlsInfo.ClientConfig()
		if err != nil {
			return nil, err
		}
	}

	client.newEtcdClient = func(endpoints []string) (etcdClient, error) {
		return clientv3.New(clientv3.Config{
			Endpoints:   endpoints,
			DialTimeout: etcdTimeout,
			TLS:         tlsConfig,
		})
	}

	client.listMembersFunc = client.listMembers

	return &client, nil
}

// NewFromCluster 根据 etcd 成员列表中的端点创建客户端。先检查现有 etcd Pod，发现至少一个
// 可连接端点；创建后再通过成员列表 API 同步端点，因为该 API 是可用成员的权威来源。
func NewFromCluster(client clientset.Interface, certificatesDir string) (*Client, error) {
	// 检查现有 etcd Pod，发现至少一个可连接端点。

	// 获取 etcd 端点列表。
	endpoints, err := getEtcdEndpoints(client)
	if err != nil {
		return nil, err
	}
	klog.V(1).Infof("etcd endpoints read from pods: %s", strings.Join(endpoints, ","))

	// 创建 etcd 客户端。
	etcdClient, err := New(
		endpoints,
		filepath.Join(certificatesDir, constants.EtcdCACertName),
		filepath.Join(certificatesDir, constants.EtcdHealthcheckClientCertName),
		filepath.Join(certificatesDir, constants.EtcdHealthcheckClientKeyName),
	)
	if err != nil {
		return nil, errors.Wrapf(err, "error creating etcd client for %v endpoints", endpoints)
	}

	// 根据 etcd 成员列表同步客户端端点。
	err = etcdClient.Sync()
	if err != nil {
		return nil, errors.Wrap(err, "error syncing endpoints with etcd")
	}
	klog.V(1).Infof("update etcd endpoints: %s", strings.Join(etcdClient.Endpoints, ","))

	return etcdClient, nil
}

// getEtcdEndpoints 返回 etcd 端点列表。
func getEtcdEndpoints(client clientset.Interface) ([]string, error) {
	return getEtcdEndpointsWithRetry(client,
		constants.KubernetesAPICallRetryInterval, kubeadmapi.GetActiveTimeouts().KubernetesAPICall.Duration)
}

func getEtcdEndpointsWithRetry(client clientset.Interface, interval, timeout time.Duration) ([]string, error) {
	return getRawEtcdEndpointsFromPodAnnotation(client, interval, timeout)
}

// getRawEtcdEndpointsFromPodAnnotation 按给定退避策略，从 etcd Pod 注解获取端点列表。
func getRawEtcdEndpointsFromPodAnnotation(client clientset.Interface, interval, timeout time.Duration) ([]string, error) {
	etcdEndpoints := []string{}
	var lastErr error
	// 容忍 API 服务器或负载均衡器的短暂异常；静态 Pod 尚未镜像到 API 服务器时也等待其传播完成。
	err := wait.PollUntilContextTimeout(context.Background(), interval, timeout, true,
		func(_ context.Context) (bool, error) {
			var overallEtcdPodCount int
			if etcdEndpoints, overallEtcdPodCount, lastErr = getRawEtcdEndpointsFromPodAnnotationWithoutRetry(client); lastErr != nil {
				return false, nil
			}
			if len(etcdEndpoints) == 0 || overallEtcdPodCount != len(etcdEndpoints) {
				klog.V(4).Infof("found a total of %d etcd pods and the following endpoints: %v; retrying",
					overallEtcdPodCount, etcdEndpoints)
				return false, nil
			}
			return true, nil
		})
	if err != nil {
		const message = "could not retrieve the list of etcd endpoints"
		if lastErr != nil {
			return []string{}, errors.Wrap(lastErr, message)
		}
		return []string{}, errors.Wrap(err, message)
	}
	return etcdEndpoints, nil
}

// getRawEtcdEndpointsFromPodAnnotationWithoutRetry 从 etcd Pod 注解获取端点列表及全局 etcd Pod 数量。
// 调用方可据此区分“未找到端点”和“列出 Pod 后仍未找到端点”，从而跳过重试。
func getRawEtcdEndpointsFromPodAnnotationWithoutRetry(client clientset.Interface) ([]string, int, error) {
	klog.V(3).Infof("retrieving etcd endpoints from %q annotation in etcd Pods", constants.EtcdAdvertiseClientUrlsAnnotationKey)
	podList, err := client.CoreV1().Pods(metav1.NamespaceSystem).List(
		context.TODO(),
		metav1.ListOptions{
			LabelSelector: fmt.Sprintf("component=%s,tier=%s", constants.Etcd, constants.ControlPlaneTier),
		},
	)
	if err != nil {
		return []string{}, 0, err
	}
	etcdEndpoints := []string{}
	for _, pod := range podList.Items {
		podIsReady := false
		for _, c := range pod.Status.Conditions {
			if c.Type == corev1.PodReady && c.Status == corev1.ConditionTrue {
				podIsReady = true
				break
			}
		}
		if !podIsReady {
			klog.V(3).Infof("etcd pod %q is not ready", pod.ObjectMeta.Name)
		}
		etcdEndpoint, ok := pod.ObjectMeta.Annotations[constants.EtcdAdvertiseClientUrlsAnnotationKey]
		if !ok {
			klog.V(3).Infof("etcd Pod %q is missing the %q annotation; cannot infer etcd advertise client URL using the Pod annotation", pod.ObjectMeta.Name, constants.EtcdAdvertiseClientUrlsAnnotationKey)
			continue
		}
		etcdEndpoints = append(etcdEndpoints, etcdEndpoint)
	}
	return etcdEndpoints, len(podList.Items), nil
}

// Sync 根据 etcd 成员列表中的已知端点同步客户端端点。
func (c *Client) Sync() error {
	// 同步端点列表。
	var cli etcdClient
	var lastError error
	err := wait.PollUntilContextTimeout(context.Background(), constants.EtcdAPICallRetryInterval, kubeadmapi.GetActiveTimeouts().EtcdAPICall.Duration,
		true, func(_ context.Context) (bool, error) {
			var err error
			cli, err = c.newEtcdClient(c.Endpoints)
			if err != nil {
				lastError = err
				return false, nil
			}
			defer func() { _ = cli.Close() }()
			ctx, cancel := context.WithTimeout(context.Background(), etcdTimeout)
			err = cli.Sync(ctx)
			cancel()
			if err == nil {
				return true, nil
			}
			klog.V(5).Infof("Failed to sync etcd endpoints: %v", err)
			lastError = err
			return false, nil
		})
	if err != nil {
		return lastError
	}
	klog.V(1).Infof("etcd endpoints read from etcd: %s", strings.Join(cli.Endpoints(), ","))

	c.Endpoints = cli.Endpoints()
	return nil
}

// Member 定义 etcd 成员，避免将 github.com/coreos/etcd 依赖扩散到整个 kubeadm 代码库。
type Member struct {
	Name    string
	PeerURL string
}

func (c *Client) listMembersOnce() (*clientv3.MemberListResponse, error) {
	cli, err := c.newEtcdClient(c.Endpoints)
	if err != nil {
		return nil, err
	}
	defer func() { _ = cli.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), etcdTimeout)
	resp, err := cli.MemberList(ctx)
	cancel()
	if err == nil {
		return resp, nil
	}
	klog.V(5).Infof("Failed to get etcd member list: %v", err)
	return nil, err
}

func (c *Client) listMembers(timeout time.Duration) (*clientv3.MemberListResponse, error) {
	// 获取成员列表。
	var (
		err       error
		lastError error
		resp      *clientv3.MemberListResponse
	)

	if timeout == 0 {
		timeout = kubeadmapi.GetActiveTimeouts().EtcdAPICall.Duration
	}
	err = wait.PollUntilContextTimeout(context.Background(), constants.EtcdAPICallRetryInterval, timeout,
		true, func(_ context.Context) (bool, error) {
			resp, err = c.listMembersOnce()
			if err != nil {
				lastError = err
				return false, nil
			}
			return true, nil
		})
	if err != nil {
		if lastError != nil {
			err = lastError
		}
		return nil, errors.Wrapf(err, "timed out after %v waiting for the etcd member list", timeout)
	}
	return resp, nil
}

// GetMemberID 返回给定 peer URL 的成员 ID。
func (c *Client) GetMemberID(peerURL string) (uint64, error) {
	resp, err := c.listMembersFunc(0)
	if err != nil {
		return 0, err
	}

	for _, member := range resp.Members {
		if member.GetPeerURLs()[0] == peerURL {
			return member.GetID(), nil
		}
	}
	return 0, ErrNoMemberIDForPeerURL
}

// ListMembers 返回成员列表。
func (c *Client) ListMembers() ([]Member, error) {
	resp, err := c.listMembersFunc(0)
	if err != nil {
		return nil, err
	}

	ret := make([]Member, 0, len(resp.Members))
	for _, m := range resp.Members {
		ret = append(ret, Member{Name: m.Name, PeerURL: m.PeerURLs[0]})
	}
	return ret, nil
}

// RemoveMember 通知 etcd 集群移除已有成员。
func (c *Client) RemoveMember(id uint64) ([]Member, error) {
	cli, err := c.newEtcdClient(c.Endpoints)
	if err != nil {
		return nil, err
	}
	defer func() { _ = cli.Close() }()

	// 从集群移除已有成员。
	var (
		lastError   error
		respMembers []*etcdserverpb.Member
	)
	err = wait.PollUntilContextTimeout(context.Background(), constants.EtcdAPICallRetryInterval, kubeadmapi.GetActiveTimeouts().EtcdAPICall.Duration,
		true, func(_ context.Context) (bool, error) {
			ctx, cancel := context.WithTimeout(context.Background(), etcdTimeout)
			defer cancel()

			// 列出成员；成员不存在时立即返回。
			listResp, err := cli.MemberList(ctx)
			if err != nil {
				klog.V(5).Infof("Failed to check whether the member %s exists: %v", strconv.FormatUint(id, 16), err)
				lastError = err
				return false, nil
			}
			found := false
			for _, member := range listResp.Members {
				if member.GetID() == id {
					found = true
					break
				}
			}
			if !found {
				klog.V(5).Infof("Member %s was not found; assuming it was already removed", strconv.FormatUint(id, 16))
				respMembers = listResp.Members
				return true, nil
			}

			resp, err := cli.MemberRemove(ctx, id)
			if err == nil {
				respMembers = resp.Members
				return true, nil
			}
			if errors.Is(err, rpctypes.ErrMemberNotFound) {
				klog.V(5).Infof("Member was already removed, because member %s was not found", strconv.FormatUint(id, 16))
				listResp, err = cli.MemberList(ctx)
				if err == nil {
					respMembers = listResp.Members
					return true, nil
				}
			}
			klog.V(5).Infof("Failed to remove etcd member: %v", err)
			lastError = err
			return false, nil
		})
	if err != nil {
		return nil, lastError
	}

	// 返回更新后的 etcd 成员列表。
	ret := []Member{}
	for _, m := range respMembers {
		ret = append(ret, Member{Name: m.Name, PeerURL: m.PeerURLs[0]})
	}

	return ret, nil
}

// AddMember 向 etcd 集群添加成员。
func (c *Client) AddMember(name string, peerAddrs string) ([]Member, error) {
	return c.addMember(name, peerAddrs, false)
}

// AddMemberAsLearner 向 etcd 集群添加 learner 成员。
func (c *Client) AddMemberAsLearner(name string, peerAddrs string) ([]Member, error) {
	return c.addMember(name, peerAddrs, true)
}

// addMember 通知现有 etcd 集群有新成员加入并返回更新后的成员列表。
// 成员已存在时返回现有 etcd 成员列表。
func (c *Client) addMember(name string, peerAddrs string, isLearner bool) ([]Member, error) {
	// 先解析 peer 地址，以便稍后将客户端 URL 加入端点列表；
	// 首先执行可确保解析失败时不会向 etcd 集群添加成员。
	parsedPeerAddrs, err := url.Parse(peerAddrs)
	if err != nil {
		return nil, errors.Wrapf(err, "error parsing peer address %s", peerAddrs)
	}

	cli, err := c.newEtcdClient(c.Endpoints)
	if err != nil {
		return nil, err
	}
	defer func() { _ = cli.Close() }()

	// 向集群添加成员。
	var (
		lastError   error
		respMembers []*etcdserverpb.Member
		resp        *clientv3.MemberAddResponse
	)
	err = wait.PollUntilContextTimeout(context.Background(), constants.EtcdAPICallRetryInterval, kubeadmapi.GetActiveTimeouts().EtcdAPICall.Duration,
		true, func(_ context.Context) (bool, error) {
			ctx, cancel := context.WithTimeout(context.Background(), etcdTimeout)
			defer cancel()

			// 列出成员；成员已存在时立即返回。
			listResp, err := cli.MemberList(ctx)
			if err != nil {
				klog.V(5).Infof("Failed to check whether the member %q exists: %v", peerAddrs, err)
				lastError = err
				return false, nil
			}
			found := false
			for _, member := range listResp.Members {
				if member.GetPeerURLs()[0] == peerAddrs {
					found = true
					break
				}
			}
			if found {
				klog.V(5).Infof("The peer URL %q for the added etcd member already exists. Skipping etcd member addition", peerAddrs)
				respMembers = listResp.Members
				return true, nil
			}

			if isLearner {
				klog.V(1).Infof("[etcd] Adding etcd member %q as learner", peerAddrs)
				resp, err = cli.MemberAddAsLearner(ctx, []string{peerAddrs})
			} else {
				klog.V(1).Infof("[etcd] Adding etcd member %q", peerAddrs)
				resp, err = cli.MemberAdd(ctx, []string{peerAddrs})
			}
			if err == nil {
				respMembers = resp.Members
				return true, nil
			}

			// 错误表明 peer 已存在时提前退出。此时 resp 为 nil，
			// 因此调用 MemberList 获取全部成员后再返回。
			if errors.Is(err, rpctypes.ErrPeerURLExist) {
				klog.V(5).Info("The peer URL for the added etcd member already exists. Fetching the existing etcd members")
				listResp, err = cli.MemberList(ctx)
				if err == nil {
					respMembers = listResp.Members
					return true, nil
				}
			}

			klog.V(5).Infof("Failed to add etcd member: %v", err)
			lastError = err
			return false, nil
		})
	if err != nil {
		return nil, lastError
	}

	// 返回更新后的 etcd 成员列表。
	ret := []Member{}
	for _, m := range respMembers {
		// peer 地址匹配时即为待添加成员，使用传入函数的名称。
		if peerAddrs == m.PeerURLs[0] {
			ret = append(ret, Member{Name: name, PeerURL: peerAddrs})
			continue
		}
		// 否则处理 AddMembers 返回的其他已有 etcd 成员。
		memberName := m.Name
		// 并发加入时某些成员可能没有名称，此时使用成员 ID 作为名称。
		if len(memberName) == 0 {
			memberName = strconv.FormatUint(m.ID, 16)
		}
		ret = append(ret, Member{Name: memberName, PeerURL: m.PeerURLs[0]})
	}

	if !isLearner {
		// 将新成员的客户端地址加入端点列表。
		c.addEndpoint(GetClientURLByIP(parsedPeerAddrs.Hostname()))
	}

	return ret, nil
}

// getMemberStatus 返回指定成员 ID 的状态，包括是否为 learner 以及是否已启动。
func (c *Client) getMemberStatus(memberID uint64) (isLearner bool, started bool, err error) {
	resp, err := c.listMembersOnce()
	if err != nil {
		return false, false, err
	}

	var m *etcdserverpb.Member
	for _, member := range resp.Members {
		if member.ID == memberID {
			m = member
			break
		}
	}
	if m == nil {
		return false, false, fmt.Errorf("member %s not found", strconv.FormatUint(memberID, 16))
	}

	started = true
	// 没有表示“已启动”的字段；成员未启动时，Name 和 ClientURLs 为各自的零值。
	if len(m.Name) == 0 {
		started = false
	}

	return m.IsLearner, started, nil
}

// MemberPromote 将成员提升为投票成员。成员已有投票权时直接返回；
// 提升前等待成员启动。
func (c *Client) MemberPromote(learnerID uint64) error {
	var (
		lastError     error
		isLearner     bool
		isStarted     bool
		learnerIDUint = strconv.FormatUint(learnerID, 16)
	)

	klog.V(1).Infof("[etcd] Waiting for a learner to start: %s", learnerIDUint)

	err := wait.PollUntilContextTimeout(context.Background(), constants.EtcdAPICallRetryInterval, kubeadmapi.GetActiveTimeouts().EtcdAPICall.Duration,
		true, func(_ context.Context) (bool, error) {
			var err error
			isLearner, isStarted, err = c.getMemberStatus(learnerID)
			if err != nil {
				lastError = errors.WithMessagef(err, "failed to get member %s status", learnerIDUint)
				return false, nil
			}
			if !isLearner {
				klog.V(1).Infof("[etcd] Member %s was already promoted.", learnerIDUint)
				return true, nil
			}
			if !isStarted {
				klog.V(1).Infof("[etcd] Member %s is not started yet. Waiting for it to be started.", learnerIDUint)
				lastError = errors.Errorf("the etcd member %s is not started", learnerIDUint)
				return false, nil
			}
			return true, nil
		})
	if err != nil {
		return lastError
	}

	if !isLearner {
		return nil
	}

	klog.V(1).Infof("[etcd] Promoting a learner as a voting member: %s", learnerIDUint)

	cli, err := c.newEtcdClient(c.Endpoints)
	if err != nil {
		return err
	}

	defer func() { _ = cli.Close() }()

	// TODO: 应移除 etcd 客户端的警告日志。客户端因多种原因打印警告，包括：
	// 1. 尚未同步，无法提升
	// 2. context 超时
	// 3. peer URL 已存在
	// 客户端提供检查 etcd learner 是否可提升的方法后，可重新审视重试逻辑。
	var memberList []*etcdserverpb.Member
	err = wait.PollUntilContextTimeout(context.Background(), constants.EtcdAPICallRetryInterval, kubeadmapi.GetActiveTimeouts().EtcdAPICall.Duration,
		true, func(_ context.Context) (bool, error) {
			// 即使提升已在 etcd 端成功，MemberPromote 也可能返回短暂的客户端错误。
			// 再次提升前检查成员状态，确保重试保持幂等。
			resp, statusErr := c.listMembersOnce()
			if statusErr != nil {
				klog.V(5).Infof("[etcd] Failed to list members before promoting learner %s: %v", learnerIDUint, statusErr)
				lastError = statusErr
				return false, nil
			}

			for _, m := range resp.Members {
				if m.ID != learnerID {
					continue
				}

				if !m.IsLearner {
					klog.V(1).Infof("[etcd] Member %s is already a voting member, treating promotion as successful", learnerIDUint)
					memberList = resp.Members
					return true, nil
				}
				break
			}

			ctx, cancel := context.WithTimeout(context.Background(), etcdTimeout)
			defer cancel()
			promoteResp, err := cli.MemberPromote(ctx, learnerID)
			if err == nil {
				klog.V(1).Infof("[etcd] The learner was promoted as a voting member: %s", learnerIDUint)
				memberList = promoteResp.Members
				return true, nil
			}
			klog.V(5).Infof("[etcd] Promoting the learner %s failed: %v", learnerIDUint, err)
			lastError = err
			return false, nil
		})
	if err != nil {
		return lastError
	}

	for _, m := range memberList {
		if m.ID == learnerID {
			parsedPeerAddrs, err := url.Parse(m.PeerURLs[0])
			if err != nil {
				return errors.Wrapf(err, "error parsing peer address %s", m.PeerURLs[0])
			}
			c.addEndpoint(GetClientURLByIP(parsedPeerAddrs.Hostname()))
			break
		}
	}

	return nil
}

func (c *Client) addEndpoint(ep string) {
	if slices.Contains(c.Endpoints, ep) {
		return
	}
	c.Endpoints = append(c.Endpoints, ep)
}

// CheckClusterHealth 在状态为 Up 时返回 nil，状态为 Down 时返回错误。
func (c *Client) CheckClusterHealth() error {
	_, ok, err := c.getClusterStatus()
	if err != nil {
		klog.V(1).Infof("[etcd] cluster has quorum: %t; some members are not healthy: %v\n", ok, err)
	}
	if ok {
		return nil
	}
	return err
}

// getClusterStatus 检查成员健康状态，返回各成员状态、集群仲裁是否满足以及聚合成员错误。
// 布尔结果为真表示健康成员占多数（healthyCount > totalCount/2）。
// 状态请求失败或返回的健康状态含错误时，视为不健康成员。
func (c *Client) getClusterStatus() (map[string]*etcdMemberStatus, bool, error) {
	// 第一步：先获取集群状态。
	clusterStatus := make(map[string]*etcdMemberStatus)
	for _, ep := range c.Endpoints {
		// 获取成员状态。
		var lastError error
		var resp *clientv3.StatusResponse
		err := wait.PollUntilContextTimeout(context.Background(), constants.EtcdAPICallRetryInterval, kubeadmapi.GetActiveTimeouts().EtcdAPICall.Duration,
			true, func(_ context.Context) (bool, error) {
				cli, err := c.newEtcdClient(c.Endpoints)
				if err != nil {
					klog.V(5).Infof("Failed to create etcd client with %v: %v", c.Endpoints, err)
					lastError = err
					return false, nil
				}
				defer func() { _ = cli.Close() }()

				ctx, cancel := context.WithTimeout(context.Background(), etcdTimeout)
				resp, err = cli.Status(ctx, ep)
				cancel()
				if err == nil {
					return true, nil
				}
				klog.V(5).Infof("Failed to get etcd status for %s: %v", ep, err)
				lastError = err
				return false, nil
			})
		if err != nil {
			clusterStatus[ep] = &etcdMemberStatus{ep: ep, err: lastError}
		} else {
			clusterStatus[ep] = &etcdMemberStatus{ep: ep, status: resp}
		}
	}

	// 第二步：评估集群状态。
	totalCount, healthyCount := len(clusterStatus), 0
	var memberErrs []error

	for ep, epStatus := range clusterStatus {
		if epStatus.err != nil {
			memberErrs = append(memberErrs, errors.Wrapf(epStatus.err, "the status of member %s is not available", ep))
			continue
		}
		if len(epStatus.status.Errors) > 0 {
			memberErrs = append(memberErrs, errors.Errorf("member %s is not healthy: %s", ep, strings.Join(epStatus.status.Errors, ",")))
			continue
		}
		healthyCount++
	}

	err := utilerrors.NewAggregate(memberErrs)
	return clusterStatus, healthyCount > totalCount/2, err
}

// WaitForClusterAvailable 在重试后返回 etcd 集群是否健康；不健康时返回错误。
func (c *Client) WaitForClusterAvailable(retries int, retryInterval time.Duration) (bool, error) {
	for i := range retries {
		if i > 0 {
			klog.V(1).Infof("[etcd] Waiting %v until next retry\n", retryInterval)
			time.Sleep(retryInterval)
		}
		klog.V(2).Infof("[etcd] attempting to see if all cluster endpoints (%s) are available %d/%d", c.Endpoints, i+1, retries)
		_, ok, err := c.getClusterStatus()
		if err != nil {
			klog.V(1).Infof("[etcd] cluster has quorum: %t; some members are not healthy: %v\n", ok, err)
		}
		if ok {
			return true, nil
		}
	}
	return false, errors.New("timeout waiting for etcd cluster to be available")
}

// GetClientURL 使用配置的公告地址和控制器客户端端口创建 HTTPS URL。
func GetClientURL(localEndpoint *kubeadmapi.APIEndpoint) string {
	return "https://" + net.JoinHostPort(localEndpoint.AdvertiseAddress, strconv.Itoa(constants.EtcdListenClientPort))
}

// GetPeerURL 使用配置的公告地址和控制器 peer 端口创建 HTTPS URL。
func GetPeerURL(localEndpoint *kubeadmapi.APIEndpoint) string {
	return "https://" + net.JoinHostPort(localEndpoint.AdvertiseAddress, strconv.Itoa(constants.EtcdListenPeerPort))
}

// GetClientURLByIP 根据 IP 地址和客户端监听端口创建 HTTPS URL。
func GetClientURLByIP(ip string) string {
	return "https://" + net.JoinHostPort(ip, strconv.Itoa(constants.EtcdListenClientPort))
}
