package opensearch

import (
	"fmt"
	"testing"
)

func TestClient_Search(t *testing.T) {
	client := New("", "", "", "", nil)
	req := &SimpleSearchRequest{
		Query: "",
		Hits:  3,
	}
	resp, err := client.Search(req)
	if err != nil {
		panic(err)
	}
	fmt.Println(resp.Status)
	resp.Print()
}
