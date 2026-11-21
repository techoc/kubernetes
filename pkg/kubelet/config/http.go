/*
Copyright 2014 The Kubernetes Authors.

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

package config

import (
	"bytes"
	"fmt"
	"net/http"
	"time"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	api "k8s.io/kubernetes/pkg/apis/core"

	"k8s.io/apimachinery/pkg/types"
	"k8s.io/klog/v2"
	utilio "k8s.io/utils/io"
)

// sourceURL 表示从 HTTP(S) URL 读取 Pod 配置的配置源。
type sourceURL struct {
	url         string
	header      http.Header
	nodeName    types.NodeName
	updates     chan<- sourceUpdate
	data        []byte
	failureLogs int
	client      *http.Client
}

// NewSourceURL 指定读取 Pod 配置的 URL，并周期性轮询其变化。
func NewSourceURL(logger klog.Logger, url string, header http.Header, nodeName types.NodeName, period time.Duration, updates chan<- sourceUpdate) {
	config := &sourceURL{
		url:      url,
		header:   header,
		nodeName: nodeName,
		updates:  updates,
		data:     nil,
		// 请求超时会触发重试；该客户端只用于读取传给 kubelet 的 manifest URL。
		client: &http.Client{Timeout: 10 * time.Second},
	}
	logger.V(1).Info("Watching URL", "URL", url)
	go wait.Until(func() { config.run(logger) }, period, wait.NeverStop)
}

// run 执行一次抓取，并对失败日志做限流，避免刷屏。
func (s *sourceURL) run(logger klog.Logger) {
	if err := s.extractFromURL(logger); err != nil {
		// 不要每分钟重复打印。前几条日志足以说明问题。
		if s.failureLogs < 3 {
			logger.Info("Failed to read pods from URL", "err", err)
		} else if s.failureLogs == 3 {
			logger.Info("Failed to read pods from URL. Dropping verbosity of this message to V(4)", "err", err)
		} else {
			logger.V(4).Info("Failed to read pods from URL", "err", err)
		}
		s.failureLogs++
	} else {
		if s.failureLogs > 0 {
			logger.Info("Successfully read pods from URL")
			s.failureLogs = 0
		}
	}
}

// applyDefaults 为从该 URL 读取的 Pod 补齐默认值。
func (s *sourceURL) applyDefaults(logger klog.Logger, pod *api.Pod) error {
	return applyDefaults(logger, pod, s.url, false, s.nodeName)
}

// extractFromURL 拉取 URL 内容并解析为单个 Pod 或 Pod 列表，再通过 updates 通道发布。
func (s *sourceURL) extractFromURL(logger klog.Logger) error {
	req, err := http.NewRequest("GET", s.url, nil)
	if err != nil {
		return err
	}
	req.Header = s.header
	resp, err := s.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	// 限制读取长度，防止恶意 URL 返回超大响应。
	data, err := utilio.ReadAtMost(resp.Body, maxConfigLength)
	if err != nil {
		return err
	}
	// 先检查状态码，非 200 的响应体不可信。
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("%v: %v", s.url, resp.Status)
	}
	if len(data) == 0 {
		// 发送空 PodList 更新，使 HTTPSource 被标记为已读取，避免误判为配置源消失。
		s.updates <- sourceUpdate{Pods: []*v1.Pod{}}
		return fmt.Errorf("zero-length data received from %v", s.url)
	}
	// 数据与上次相同则直接返回，避免重复发布更新。
	if bytes.Equal(data, s.data) {
		return nil
	}
	s.data = data

	// 先按单个 Pod 解析。
	parsed, pod, singlePodErr := tryDecodeSinglePod(logger, data, s.applyDefaults)
	if parsed {
		if singlePodErr != nil {
			// 解析成功但内容不可用。
			return singlePodErr
		}
		s.updates <- sourceUpdate{Pods: []*v1.Pod{pod}}
		return nil
	}

	// 单 Pod 解析失败，再按 Pod 列表解析。
	parsed, podList, multiPodErr := tryDecodePodList(logger, data, s.applyDefaults)
	if parsed {
		if multiPodErr != nil {
			// 解析成功但内容不可用。
			return multiPodErr
		}
		pods := make([]*v1.Pod, 0, len(podList.Items))
		for i := range podList.Items {
			pods = append(pods, &podList.Items[i])
		}
		s.updates <- sourceUpdate{Pods: pods}
		return nil
	}

	return fmt.Errorf("%v: received '%v', but couldn't parse as "+
		"single (%v) or multiple pods (%v)",
		s.url, string(data), singlePodErr, multiPodErr)
}
