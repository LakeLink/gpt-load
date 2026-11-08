package codexprivacy

import (
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"
)

func cookieRequest(t *testing.T, lease *CookieLease, target string) *http.Request {
	t.Helper()
	request, err := http.NewRequest(http.MethodGet, target, nil)
	if err != nil {
		t.Fatal(err)
	}
	request.Header["cookie"] = []string{"__oailb=foreign"}
	request.Header.Set("Cookie2", "foreign=other")
	if err := lease.Prepare(request); err != nil {
		t.Fatal(err)
	}
	if request.Header.Get("Cookie2") != "" || request.Header["cookie"] != nil {
		t.Fatal("incoming cookies survived final preparation")
	}
	return request
}
func cookieObserve(t *testing.T, lease *CookieLease, target string, values ...string) {
	t.Helper()
	destination, err := url.Parse(target)
	if err != nil {
		t.Fatal(err)
	}
	lease.Observe(destination, http.Header{"set-cookie": values})
}

func TestAccountCookieScopeAndLifecycle(t *testing.T) {
	now := time.Now().Truncate(time.Second)
	store := NewCookies()
	store.now = func() time.Time { return now }
	account := store.Lease("account-a", "target-a")
	other := store.Lease("account-b", "target-a")
	newTarget := store.Lease("account-a", "target-b")
	cookieObserve(t, account, "https://chatgpt.com/backend-api/codex/responses",
		"__oailb=route-a; Path=/backend-api; Secure; HttpOnly",
		"cf_clearance=domain-a; Domain=.chatgpt.com; Path=/; Max-Age=60; Secure",
		"__cf_bm=short; Max-Age=1; Path=/; Secure",
		"session_token=authentication; Path=/; Secure",
		"__cflb=wrong-host; Domain=evil.example; Path=/; Secure",
		"cf_chl_public=public; Domain=com; Path=/; Secure",
		"cf_chl_big="+strings.Repeat("x", cookieMaxBytes))
	matching := cookieRequest(t, account, "wss://chatgpt.com/backend-api/codex/responses").Header.Get("Cookie")
	if !strings.Contains(matching, "__oailb=route-a") || !strings.Contains(matching, "cf_clearance=domain-a") || !strings.Contains(matching, "__cf_bm=short") || strings.Contains(matching, "authentication") || strings.Contains(matching, "wrong-host") || strings.Contains(matching, "public") || strings.Contains(matching, "big") {
		t.Fatalf("matching cookie selection=%s", matching)
	}
	for _, test := range []struct {
		lease  *CookieLease
		target string
	}{{other, "https://chatgpt.com/backend-api"}, {newTarget, "https://chatgpt.com/backend-api"}, {account, "http://chatgpt.com/backend-api"}, {account, "https://api.openai.com/backend-api"}, {account, "https://chatgpt.com.evil.example/backend-api"}} {
		if got := cookieRequest(t, test.lease, test.target).Header.Get("Cookie"); got != "" {
			t.Fatalf("cookie crossed account/security scope: %s", got)
		}
	}
	if got := cookieRequest(t, account, "https://sub.chatgpt.com/unrelated").Header.Get("Cookie"); got != "cf_clearance=domain-a" {
		t.Fatalf("domain/path matching=%s", got)
	}
	now = now.Add(2 * time.Second)
	if got := cookieRequest(t, account, "https://chatgpt.com/backend-api").Header.Get("Cookie"); strings.Contains(got, "short") {
		t.Fatal("expired Max-Age cookie replayed")
	}
	cookieObserve(t, account, "wss://chatgpt.com/backend-api/codex/responses", "__oailb=; Path=/backend-api; Max-Age=0; Secure", "cf_clearance=; Domain=.chatgpt.com; Path=/; Expires=Thu, 01 Jan 1970 00:00:00 GMT")
	if got := cookieRequest(t, account, "https://chatgpt.com/backend-api").Header.Get("Cookie"); got != "" {
		t.Fatalf("deleted cookies replayed: %s", got)
	}
	cookieObserve(t, account, "https://chat.openai.com/path/sub", "cf_use_ob=default-path; Secure")
	if got := cookieRequest(t, account, "https://chat.openai.com/elsewhere").Header.Get("Cookie"); got != "" {
		t.Fatal("default path expanded")
	}
	if got := cookieRequest(t, account, "https://chat.openai.com/path/next").Header.Get("Cookie"); got != "cf_use_ob=default-path" {
		t.Fatalf("default path missing: %s", got)
	}
	now = now.Add(cookieLifetime + time.Second)
	replacement := store.Lease("account-a", "target-a")
	cookieObserve(t, account, "https://chatgpt.com/", "__oailb=late; Path=/; Secure")
	if got := cookieRequest(t, replacement, "https://chatgpt.com/").Header.Get("Cookie"); got != "" {
		t.Fatalf("late expired response populated replacement: %s", got)
	}
}

func TestCookieBoundsAndConcurrentCapture(t *testing.T) {
	store := NewCookies()
	store.capacity = 2
	account := store.Lease("a", "target")
	for i := range cookieCapacity + 8 {
		cookieObserve(t, account, "https://chatgpt.com/", fmt.Sprintf("cf_chl_%d=value; Path=/; Secure", i))
	}
	if got := len(cookieRequest(t, account, "https://chatgpt.com/").Cookies()); got != cookieCapacity {
		t.Fatalf("cookie bound=%d", got)
	}
	var workers sync.WaitGroup
	for i := range 16 {
		workers.Go(func() {
			for j := range 20 {
				cookieObserve(t, account, "https://chatgpt.com/", fmt.Sprintf("cf_chl_%d=worker-%d; Path=/; Secure", i, j))
				cookieRequest(t, account, "wss://chatgpt.com/")
			}
		})
	}
	workers.Wait()
	if got := len(cookieRequest(t, account, "https://chatgpt.com/").Cookies()); got > cookieCapacity {
		t.Fatalf("concurrent cookie bound=%d", got)
	}
	store.Lease("b", "target")
	store.Lease("c", "target")
	replacement := store.Lease("a", "target")
	cookieObserve(t, account, "https://chatgpt.com/", "__oailb=late; Secure; Path=/")
	if got := cookieRequest(t, replacement, "https://chatgpt.com/").Header.Get("Cookie"); got != "" {
		t.Fatal("evicted lease response repopulated owner")
	}
	cookieObserve(t, replacement, "https://chatgpt.com/", "__oailb=valid; Secure; Path=/")
	replacement.Close()
	fresh := store.Lease("a", "target")
	cookieObserve(t, replacement, "https://chatgpt.com/", "__oailb=late; Secure; Path=/")
	if got := cookieRequest(t, fresh, "https://chatgpt.com/").Header.Get("Cookie"); got != "" {
		t.Fatal("closed lease response repopulated owner")
	}
}
