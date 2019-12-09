package opensearch

import (
	"bytes"
	"context"
	"fmt"
	jsoniter "github.com/json-iterator/go"
	"github.com/tedux/aliyun-opensearch-go-sdk/credential"
	"io"
	"net/http"
	"strings"
	"sync"
)

const (
	searchApiPath = "/v3/openapi/apps/%s/search"
	verb          = "GET"
)

var (
	json = jsoniter.ConfigCompatibleWithStandardLibrary
)

type OpenSearch interface {
	Search(ctx context.Context, req SearchRequest) (resp *SearchResponse, err error)
}

type client struct {
	host    string
	appName string
	cred    credential.Credential
	http    *http.Client
	pool    sync.Pool
}

func New(host, appName, accessKeyId, accessKeySecret string, httpClient *http.Client) OpenSearch {
	if httpClient == nil {
		httpClient = http.DefaultClient
	}
	return &client{
		host:    host,
		appName: appName,
		cred:    credential.New(accessKeyId, accessKeySecret),
		http:    httpClient,
		pool: sync.Pool{
			New: func() interface{} {
				return bytes.NewBuffer(make([]byte, 1024*1024)) // 1 Mb
			},
		},
	}
}

func (c *client) Search(ctx context.Context, request SearchRequest) (response *SearchResponse, err error) {
	query, headers := buildQuery(c.appName, c.cred, request.Headers(), request.Params())
	reqUrl := c.host + query

	httpReq, err := http.NewRequestWithContext(ctx, verb, reqUrl, nil)
	if err != nil {
		return nil, err
	}
	for k, v := range headers {
		httpReq.Header.Add(k, v)
	}

	resp, err := c.http.Do(httpReq)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("error response, code: %v", resp.StatusCode)
	}
	defer func() { _ = resp.Body.Close() }()

	buffer := c.pool.Get().(*bytes.Buffer)
	defer func() {
		if buffer != nil {
			c.pool.Put(buffer)
			buffer = nil
		}
	}() // return buffer to pool

	buffer.Reset()
	_, err = io.Copy(buffer, resp.Body)
	if err != nil {
		return nil, fmt.Errorf("client io.copy failure error:%v", err)
	}

	response = &SearchResponse{}
	err = json.Unmarshal(buffer.Bytes(), response)
	if err != nil {
		return nil, err
	}
	return response, nil
}

func buildQuery(appName string, cred credential.Credential, httpHeader, httpParams map[string]string) (string, map[string]string) {
	uri := fmt.Sprintf(searchApiPath, appName)

	var paramList []string
	for k, v := range httpParams {
		paramList = append(paramList, encodeUrlQuery(k)+"="+encodeUrlQuery(v))
	}
	query := strings.Join(paramList, "&")

	requestHeader := NewHeader(httpHeader)
	requestHeader.Auth(verb, uri, httpParams, cred)

	return uri + "?" + query, requestHeader.ToMap()
}
