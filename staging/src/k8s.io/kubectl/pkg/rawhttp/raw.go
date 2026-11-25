/*
Copyright 2019 The Kubernetes Authors.

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

package rawhttp

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"

	"k8s.io/cli-runtime/pkg/genericiooptions"
	"k8s.io/client-go/rest"
)

// RawPost 使用 REST client 以 POST 方式发送内容
func RawPost(restClient *rest.RESTClient, streams genericiooptions.IOStreams, url, filename string) error {
	return raw(restClient, streams, url, filename, "POST")
}

// RawPut 使用 REST client 以 PUT 方式发送内容
func RawPut(restClient *rest.RESTClient, streams genericiooptions.IOStreams, url, filename string) error {
	return raw(restClient, streams, url, filename, "PUT")
}

// RawGet 使用 REST client 以 GET 方式获取内容
func RawGet(restClient *rest.RESTClient, streams genericiooptions.IOStreams, url string) error {
	return raw(restClient, streams, url, "", "GET")
}

// RawDelete 使用 REST client 以 DELETE 方式删除内容
func RawDelete(restClient *rest.RESTClient, streams genericiooptions.IOStreams, url, filename string) error {
	return raw(restClient, streams, url, filename, "DELETE")
}

// raw 使用默认凭据向服务端提供的路径发起简单的 HTTP 请求。
func raw(restClient *rest.RESTClient, streams genericiooptions.IOStreams, url, filename, requestType string) error {
	var data io.Reader
	switch {
	case len(filename) == 0:
		data = bytes.NewBuffer([]byte{})

	case filename == "-":
		data = streams.In

	default:
		f, err := os.Open(filename)
		if err != nil {
			return err
		}
		defer f.Close()
		data = f
	}

	var request *rest.Request
	switch requestType {
	case "GET":
		request = restClient.Get().RequestURI(url)
	case "PUT":
		request = restClient.Put().RequestURI(url).Body(data)
	case "POST":
		request = restClient.Post().RequestURI(url).Body(data)
	case "DELETE":
		request = restClient.Delete().RequestURI(url).Body(data)

	default:
		return fmt.Errorf("unknown requestType: %q", requestType)
	}

	stream, err := request.Stream(context.TODO())
	if err != nil {
		return err
	}
	defer stream.Close()

	_, err = io.Copy(streams.Out, stream)
	if err != nil && err != io.EOF {
		return err
	}
	return nil
}
