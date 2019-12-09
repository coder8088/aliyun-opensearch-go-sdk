package opensearch

import (
	"fmt"
	"strings"
)

type SearchRequest interface {
	Headers() map[string]string
	Params() map[string]string
}

type Headers interface {
	Header(key, defaultValue string) string
}

type Canonicalizer interface {
	Canonicalize() string
}

type CanonicalizableHeaders interface {
	Headers
	Canonicalizer
}

type requestHeaders map[string]string

func (h requestHeaders) Header(key, defaultValue string) string {
	if val, ok := h[key]; ok && len(val) > 0 {
		return val
	}
	return defaultValue
}

func (h requestHeaders) Canonicalize() string {
	headers := make(map[string]string)
	for k, v := range h {
		key := strings.TrimSpace(k)
		value := strings.TrimSpace(v)
		if strings.HasPrefix(key, "X-Opensearch-") && len(value) > 0 {
			headers[key] = value
		}
	}

	if len(headers) == 0 {
		return ""
	}

	result := ""
	keys := sortedKeys(headers)
	for _, k := range keys {
		result += fmt.Sprintf("%s:%s\n", strings.ToLower(k), headers[k])
	}

	return result
}

type SimpleSearchRequest struct {
	FetchFields []string
	Start       int
	Hits        int
	Kvpairs     string
	Query       string
	Filter      string
	SortFields  SortFields
}

type SortFields []SortField

type SortField struct {
	Field string
	Order string // INCREASE | DECREASE
}

func (sf *SortField) String() string {
	return fmt.Sprintf("%s:%s", sf.Field, sf.Order)
}

func (sfs SortFields) String() string {
	var arr []string
	for _, sf := range sfs {
		arr = append(arr, sf.String())
	}
	return strings.Join(arr, ";")
}

func (req *SimpleSearchRequest) Params() map[string]string {
	params := make(map[string]string)
	params["query"] = req.buildQueryClauses()
	if len(req.FetchFields) > 0 {
		params["fetch_fields"] = strings.Join(req.FetchFields, ";")
	}
	return params
}

func (req *SimpleSearchRequest) Headers() map[string]string {
	return map[string]string{}
}

func (req *SimpleSearchRequest) buildQueryClauses() string {
	clauses := []string{
		req.defaultConfigClause(),
		req.defaultQueryClause(),
		req.defaultSortClause(),
		req.defaultFilterClause(),
		req.defaultKvpairsClause(),
	}
	sb := strings.Builder{}
	for _, clause := range clauses {
		if len(clause) > 0 {
			sb.WriteString("&&" + clause)
		}
	}
	return strings.TrimLeft(sb.String(), "&&")
}

func (req *SimpleSearchRequest) defaultConfigClause() string {
	sb := strings.Builder{}
	sb.WriteString("config=")
	sb.WriteString(fmt.Sprintf("start:%d,", req.Start))
	sb.WriteString(fmt.Sprintf("hit:%d,", req.Hits))
	sb.WriteString("format:fulljson")
	return sb.String()
}

func (req *SimpleSearchRequest) defaultQueryClause() string {
	return "query=" + req.Query
}

func (req *SimpleSearchRequest) defaultSortClause() string {
	if len(req.SortFields) > 0 {
		sb := strings.Builder{}
		sb.WriteString("sort=")
		for _, sortField := range req.SortFields {
			sortStr := sortField.Field
			switch sortField.Order {
			case "INCREASE", "increase", "asc":
				sortStr = "+" + sortStr
			default:
				sortStr = "-" + sortStr
			}
			sb.WriteString(sortStr + ";")
		}
		return strings.TrimRight(sb.String(), ";")
	}
	return ""
}

func (req *SimpleSearchRequest) defaultFilterClause() string {
	if len(req.Filter) > 0 {
		return "filter=" + req.Filter
	}
	return ""
}

func (req *SimpleSearchRequest) defaultKvpairsClause() string {
	if len(req.Kvpairs) > 0 {
		return "kvpairs=" + req.Kvpairs
	}
	return ""
}

func (req *SimpleSearchRequest) String() string {
	return fmt.Sprintf(`{"fetch_fields": %#v, "start": %d, "hits": %d, "kvpairs": %#v, "query": %#v, "filter": %#v, "sort_fields": %#v}`,
		req.FetchFields, req.Start, req.Hits, req.Kvpairs, req.Query, req.Filter, req.SortFields.String())
}
