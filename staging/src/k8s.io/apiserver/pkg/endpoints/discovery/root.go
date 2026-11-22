/*
Copyright 2017 The Kubernetes Authors.

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

package discovery

import (
	"context"
	"net/http"
	"sync"

	restful "github.com/emicklei/go-restful/v3"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	utilnet "k8s.io/apimachinery/pkg/util/net"
	"k8s.io/apiserver/pkg/endpoints/handlers/negotiation"
	"k8s.io/apiserver/pkg/endpoints/handlers/responsewriters"
)

// GroupManager is an interface that allows dynamic mutation of the existing webservice to handle
// API groups being added or removed.
type GroupManager interface {
	GroupLister

	AddGroup(apiGroup metav1.APIGroup)
	RemoveGroup(groupName string)
	ServeHTTP(resp http.ResponseWriter, req *http.Request)
	WebService() *restful.WebService
}

// GroupLister knows how to list APIGroups for discovery.
type GroupLister interface {
	// Groups returns APIGroups for discovery, filling in ServerAddressByClientCIDRs
	// based on data in req.
	Groups(ctx context.Context, req *http.Request) ([]metav1.APIGroup, error)
}

// rootAPIsHandler creates a webservice serving api group discovery.
// The list of APIGroups may change while the server is running because additional resources
// are registered or removed.  It is not safe to cache the values.
//
// 因此读路径统一加 RLock，写路径加 Lock；apiGroupNames 单独维护插入顺序，
// 因为 map 迭代顺序不稳定，而发现输出需要可预测的顺序。
type rootAPIsHandler struct {
	// addresses is used to build cluster IPs for discovery.
	addresses Addresses

	serializer runtime.NegotiatedSerializer

	// Map storing information about all groups to be exposed in discovery response.
	// The map is from name to the group.
	lock      sync.RWMutex
	apiGroups map[string]metav1.APIGroup
	// apiGroupNames preserves insertion order
	apiGroupNames []string
}

// NewRootAPIsHandler 构造 /apis 发现处理器，并沿用旧版空 APIVersion 的兼容序列化。
func NewRootAPIsHandler(addresses Addresses, serializer runtime.NegotiatedSerializer) *rootAPIsHandler {
	// Because in release 1.1, /apis returns response with empty APIVersion, we
	// use stripVersionNegotiatedSerializer to keep the response backwards
	// compatible.
	serializer = stripVersionNegotiatedSerializer{serializer}

	return &rootAPIsHandler{
		addresses:  addresses,
		serializer: serializer,
		apiGroups:  map[string]metav1.APIGroup{},
	}
}

// AddGroup 新增或更新一个 API 组；已存在时只覆盖内容，不重复追加顺序项。
func (s *rootAPIsHandler) AddGroup(apiGroup metav1.APIGroup) {
	s.lock.Lock()
	defer s.lock.Unlock()

	_, alreadyExists := s.apiGroups[apiGroup.Name]

	s.apiGroups[apiGroup.Name] = apiGroup
	if !alreadyExists {
		s.apiGroupNames = append(s.apiGroupNames, apiGroup.Name)
	}
}

// RemoveGroup 删除 API 组，并同步从顺序列表中移除，保持两个结构一致。
func (s *rootAPIsHandler) RemoveGroup(groupName string) {
	s.lock.Lock()
	defer s.lock.Unlock()

	delete(s.apiGroups, groupName)
	for i := range s.apiGroupNames {
		if s.apiGroupNames[i] == groupName {
			s.apiGroupNames = append(s.apiGroupNames[:i], s.apiGroupNames[i+1:]...)
			break
		}
	}
}

// Groups 返回当前所有 API 组的发现信息；结果按插入顺序排列。
func (s *rootAPIsHandler) Groups(ctx context.Context, req *http.Request) ([]metav1.APIGroup, error) {
	s.lock.RLock()
	defer s.lock.RUnlock()

	return s.groupsLocked(ctx, req), nil
}

// groupsLocked returns the APIGroupList discovery information for this handler.
// The caller must hold the lock before invoking this method to avoid data races.
func (s *rootAPIsHandler) groupsLocked(ctx context.Context, req *http.Request) []metav1.APIGroup {
	clientIP := utilnet.GetClientIP(req)
	serverCIDR := s.addresses.ServerAddressByClientCIDRs(clientIP)

	orderedGroups := []metav1.APIGroup{}
	for _, groupName := range s.apiGroupNames {
		orderedGroups = append(orderedGroups, s.apiGroups[groupName])
	}

	// 复制一份再写入 ServerAddressByClientCIDRs，避免修改缓存中的 APIGroup 对象。
	groups := make([]metav1.APIGroup, len(orderedGroups))
	for i := range orderedGroups {
		groups[i] = orderedGroups[i]
		groups[i].ServerAddressByClientCIDRs = serverCIDR
	}

	return groups
}

// ServeHTTP 输出 APIGroupList；整个序列化过程持有读锁，保证输出的一致性快照。
func (s *rootAPIsHandler) ServeHTTP(resp http.ResponseWriter, req *http.Request) {
	s.lock.RLock()
	defer s.lock.RUnlock()

	groupList := metav1.APIGroupList{Groups: s.groupsLocked(req.Context(), req)}

	responsewriters.WriteObjectNegotiated(s.serializer, negotiation.DefaultEndpointRestrictions, schema.GroupVersion{}, resp, req, http.StatusOK, &groupList, false)
}

// restfulHandle 把 go-restful 请求桥接回标准 net/http 处理逻辑。
func (s *rootAPIsHandler) restfulHandle(req *restful.Request, resp *restful.Response) {
	s.ServeHTTP(resp.ResponseWriter, req.Request)
}

// WebService returns a webservice serving api group discovery.
// Note: during the server runtime apiGroups might change.
func (s *rootAPIsHandler) WebService() *restful.WebService {
	mediaTypes, _ := negotiation.MediaTypesForSerializer(s.serializer)
	ws := new(restful.WebService)
	ws.Path(APIGroupPrefix)
	ws.Doc("get available API versions")
	ws.Route(ws.GET("/").To(s.restfulHandle).
		Doc("get available API versions").
		Operation("getAPIVersions").
		Produces(mediaTypes...).
		Consumes(mediaTypes...).
		Writes(metav1.APIGroupList{}))
	return ws
}
