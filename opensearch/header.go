package opensearch

import (
	"fmt"
	"github.com/tedux/aliyun-opensearch-go-sdk/credential"
	"math/rand"
	"net/url"
	"sort"
	"strings"
	"time"
)

type Header interface {
	Get(k, defaultVal string) string
	Set(k, v string)
	Auth(method, uri string, httpParams map[string]string, cred credential.Credential)
	ToMap() map[string]string
}

type header struct {
	httpHeader map[string]string
	m          map[string]string
}

func NewHeader(httpHeader map[string]string) Header {
	h := header{httpHeader: httpHeader, m: make(map[string]string, 8)}
	h.Set("Content-MD5", "")
	h.Set("Content-Type", "application/json")
	h.Set("Date", formattedDateString())
	h.Set("X-Opensearch-Nonce", nonce())
	for k, v := range httpHeader {
		h.Set(k, v)
	}
	return h
}

func (h header) Get(k, defaultVal string) string {
	if v, ok := h.m[k]; ok {
		return v
	}
	return defaultVal
}

func (h header) Set(k, v string) {
	h.m[k] = v
}

func (h header) Auth(method, uri string, httpParams map[string]string, cred credential.Credential) {
	canonicalized := method + "\n" +
		h.Get("Content-MD5", "") + "\n" +
		h.Get("Content-Type", "") + "\n" +
		h.Get("Date", "") + "\n" +
		h.canonicalize() +
		canonicalizedResource(uri, httpParams)

	signature := cred.Sign(canonicalized)
	authVal := fmt.Sprintf("%s %s:%s", "OPENSEARCH", cred.KeyId(), signature)
	h.Set("Authorization", authVal)
}

func (h header) ToMap() map[string]string {
	return h.m
}

func (h header) canonicalize() string {
	tmp := make(map[string]string)
	for k, v := range h.m {
		key := strings.TrimSpace(k)
		value := strings.TrimSpace(v)
		if strings.HasPrefix(key, "X-Opensearch-") && len(value) > 0 {
			tmp[key] = value
		}
	}

	if len(tmp) == 0 {
		return ""
	}

	result := ""
	keys := sortedKeys(tmp)
	for _, k := range keys {
		result += fmt.Sprintf("%s:%s\n", strings.ToLower(k), tmp[k])
	}

	return result
}

func canonicalizedResource(uri string, httpParams map[string]string) string {
	result := strings.ReplaceAll(encodeUrlPath(uri), "%2F", "/")

	keys := sortedKeys(httpParams)
	params := make([]string, 0, len(keys))
	for _, k := range keys {
		if v, ok := httpParams[k]; ok && len(v) > 0 {
			params = append(params, encodeUrlQuery(k)+"="+encodeUrlQuery(v))
		}
	}

	return result + "?" + strings.Join(params, "&")
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

func sortedKeys(m map[string]string) (keys []string) {
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return
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
