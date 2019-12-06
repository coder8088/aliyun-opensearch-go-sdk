package opensearch

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha1"
	"encoding/base64"
	"fmt"
	"github.com/json-iterator/go"
	"io"
	"math/rand"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
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
	host            string
	appName         string
	accessKeyId     string
	accessKeySecret string
	http            *http.Client
	pool            sync.Pool
}

func New(host, appName, accessKeyId, accessKeySecret string, httpClient *http.Client) OpenSearch {
	if httpClient == nil {
		httpClient = http.DefaultClient
	}
	return &client{
		host:            host,
		appName:         appName,
		accessKeyId:     accessKeyId,
		accessKeySecret: accessKeySecret,
		http:            httpClient,
		pool: sync.Pool{
			New: func() interface{} {
				return bytes.NewBuffer(make([]byte, 1024*1024)) // 512 Kb
			},
		},
	}
}

func (c *client) Search(ctx context.Context, request SearchRequest) (response *SearchResponse, err error) {
	query, headers := buildQuery(c.appName, c.accessKeyId, c.accessKeySecret, request.Headers(), request.Params())
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

func buildQuery(appName, accessKeyId, accessKeySecret string, httpHeaders, httpParams map[string]string) (string, map[string]string) {
	uri := fmt.Sprintf(searchApiPath, appName)

	var paramList []string
	for k, v := range httpParams {
		paramList = append(paramList, encodeUrlQuery(k)+"="+encodeUrlQuery(v))
	}
	query := strings.Join(paramList, "&")

	requestHeaders := buildRequestHeaders(uri, accessKeyId, accessKeySecret, httpHeaders, httpParams)

	return uri + "?" + query, requestHeaders
}

func buildRequestHeaders(uri, accessKeyId, accessKeySecret string, httpHeaders, httpParams map[string]string) map[string]string {
	// deep copy from http headers
	requestHeaders := requestHeaders{}
	for k, v := range httpHeaders {
		requestHeaders[k] = v
	}
	if _, ok := requestHeaders["Content-MD5"]; !ok {
		requestHeaders["Content-MD5"] = ""
	}
	if _, ok := requestHeaders["Content-Type"]; !ok {
		requestHeaders["Content-Type"] = "application/json"
	}
	if _, ok := requestHeaders["Date"]; !ok {
		requestHeaders["Date"] = formattedDateString()
	}
	if _, ok := requestHeaders["X-Opensearch-Nonce"]; !ok {
		requestHeaders["X-Opensearch-Nonce"] = nonce()
	}
	if _, ok := requestHeaders["Authorization"]; !ok {
		requestHeaders["Authorization"] = buildAuthorization(uri, accessKeyId, accessKeySecret, httpParams, requestHeaders)
	}

	/*for k, v := range requestHeaders {
		if len(v) == 0 {
			delete(requestHeaders, k)
		}
	}*/
	return requestHeaders
}

func buildAuthorization(uri, accessKeyId, accessKeySecret string, httpParams map[string]string, requestHeaders CanonicalizableHeaders) string {
	canonicalized := verb + "\n" +
		requestHeaders.Header("Content-MD5", "") + "\n" +
		requestHeaders.Header("Content-Type", "") + "\n" +
		requestHeaders.Header("Date", "") + "\n" +
		requestHeaders.Canonicalize() +
		canonicalizedResource(uri, httpParams)

	mac := hmac.New(sha1.New, []byte(accessKeySecret))
	mac.Write([]byte(canonicalized))
	signature := base64.StdEncoding.EncodeToString(mac.Sum(nil))

	return fmt.Sprintf("%s %s:%s", "OPENSEARCH", accessKeyId, signature)
}

func canonicalizedResource(uri string, httpParams map[string]string) string {
	result := strings.ReplaceAll(encodeUrlPath(uri), "%2F", "/")

	keys := SortedKeys(httpParams)
	params := make([]string, 0, len(keys))
	for _, k := range keys {
		if v, ok := httpParams[k]; ok && len(v) > 0 {
			params = append(params, encodeUrlQuery(k)+"="+encodeUrlQuery(v))
		}
	}

	return result + "?" + strings.Join(params, "&")
}

func formattedDateString() string {
	return time.Now().UTC().Format("2006-01-02T15:04:05Z")
}

func nonce() string {
	timestamp := time.Now().UTC().UnixNano()
	rand.Seed(timestamp)
	min := 100000
	max := 999999
	return fmt.Sprintf("%d%d", timestamp/100000000, rand.Intn(max-min)+min)
}

func encodeUrlPath(path string) string {
	if len(path) == 0 {
		return path
	}
	escaped := url.PathEscape(path)
	escaped = strings.ReplaceAll(escaped, "+", "%20")
	escaped = strings.ReplaceAll(escaped, "*", "%2A")
	return strings.ReplaceAll(escaped, "%7E", "~")
}

func encodeUrlQuery(query string) string {
	if len(query) == 0 {
		return query
	}
	escaped := url.QueryEscape(query)
	return strings.ReplaceAll(escaped, "+", "%20")
}
