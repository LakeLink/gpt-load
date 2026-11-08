package codexprivacy

import (
	"container/list"
	"net/http"
	"net/http/cookiejar"
	"net/url"
	"strings"
	"sync"
	"time"

	"golang.org/x/net/publicsuffix"
)

const (
	cookieOwnerCapacity = 256
	cookieCapacity      = 32
	cookieMaxBytes      = 4096
	cookieLifetime      = 24 * time.Hour
)

type storedCookie struct {
	key     string
	cookie  http.Cookie
	source  url.URL
	expires time.Time
}

type cookieOwner struct {
	key     string
	records []storedCookie
	jar     *cookiejar.Jar
	expires time.Time
	element *list.Element
}

// Cookies shares only allowlisted infrastructure cookies within an account and
// target security scope. Each partition has at most 32 cookies of 4096 bytes;
// at most 256 partitions survive. Session cookies and idle partitions expire
// after 24 hours. Access performs cleanup, with no background goroutine.
type Cookies struct {
	mu       sync.Mutex
	owners   map[string]*cookieOwner
	recent   list.List
	now      func() time.Time
	capacity int
}

func NewCookies() *Cookies {
	return &Cookies{owners: make(map[string]*cookieOwner), now: time.Now, capacity: cookieOwnerCapacity}
}

// CookieLease freezes the owner generation. An evicted or expired partition's
// late responses cannot populate its replacement.
type CookieLease struct {
	store *Cookies
	owner *cookieOwner
}

func (c *Cookies) Lease(owner, scope string) *CookieLease {
	c.mu.Lock()
	defer c.mu.Unlock()
	now := c.now()
	for _, entry := range c.owners {
		if !now.Before(entry.expires) {
			c.remove(entry)
		}
	}
	key := owner + "\x00" + scope
	if entry := c.owners[key]; entry != nil {
		entry.expires = now.Add(cookieLifetime)
		c.recent.MoveToFront(entry.element)
		return &CookieLease{c, entry}
	}
	for len(c.owners) >= c.capacity {
		c.remove(c.recent.Back().Value.(*cookieOwner))
	}
	entry := &cookieOwner{key: key, jar: newCookieJar(), expires: now.Add(cookieLifetime)}
	entry.element = c.recent.PushFront(entry)
	c.owners[key] = entry
	return &CookieLease{c, entry}
}

func (c *Cookies) remove(entry *cookieOwner) {
	delete(c.owners, entry.key)
	c.recent.Remove(entry.element)
}

func (lease *CookieLease) current(now time.Time) bool {
	return lease != nil && lease.store.owners[lease.owner.key] == lease.owner && now.Before(lease.owner.expires)
}

// Prepare replaces all incoming and intermediate cookie headers using only the
// selected account's cookies that match this final HTTPS/WSS destination.
func (lease *CookieLease) Prepare(request *http.Request) error {
	for name := range request.Header {
		if strings.EqualFold(name, "Cookie") || strings.EqualFold(name, "Cookie2") {
			delete(request.Header, name)
		}
	}
	if lease == nil {
		return nil
	}
	c := lease.store
	c.mu.Lock()
	defer c.mu.Unlock()
	now := c.now()
	if !lease.current(now) {
		return nil
	}
	destination := cookieURL(request.URL)
	if destination == nil {
		return nil
	}
	entry := lease.owner
	entry.expires = now.Add(cookieLifetime)
	c.recent.MoveToFront(entry.element)
	entry.rebuild(now)
	for _, cookie := range entry.jar.Cookies(destination) {
		request.AddCookie(cookie)
	}
	return nil
}

// Observe captures raw server-issued values before downstream response filtering.
func (lease *CookieLease) Observe(destination *url.URL, headers http.Header) {
	if lease == nil {
		return
	}
	destination = cookieURL(destination)
	if destination == nil {
		return
	}
	c := lease.store
	c.mu.Lock()
	defer c.mu.Unlock()
	now := c.now()
	if !lease.current(now) {
		return
	}
	entry := lease.owner
	entry.rebuild(now)
	var setCookies []string
	for name, values := range headers {
		if strings.EqualFold(name, "Set-Cookie") {
			setCookies = append(setCookies, values...)
		}
	}
	for _, raw := range setCookies {
		if len(raw) > cookieMaxBytes {
			continue
		}
		cookie, err := http.ParseSetCookie(raw)
		if err != nil || !allowedCookie(cookie.Name) || cookie.Valid() != nil {
			continue
		}
		record, ok := validatedCookie(destination, cookie, now)
		if !ok {
			continue
		}
		replaced := false
		for i, previous := range entry.records {
			if previous.key == record.key {
				if now.Before(record.expires) {
					entry.records[i] = record
				} else {
					entry.records = append(entry.records[:i], entry.records[i+1:]...)
				}
				replaced = true
				break
			}
		}
		if replaced || !now.Before(record.expires) {
			continue
		}
		if len(entry.records) >= cookieCapacity {
			entry.records = entry.records[1:]
		}
		entry.records = append(entry.records, record)
	}
	entry.rebuild(now)
}

// Close invalidates this partition, including any in-flight response captures.
func (lease *CookieLease) Close() {
	if lease == nil {
		return
	}
	c := lease.store
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.owners[lease.owner.key] == lease.owner {
		c.remove(lease.owner)
	}
}

func (entry *cookieOwner) rebuild(now time.Time) {
	entry.jar = newCookieJar()
	retained := entry.records[:0]
	for _, record := range entry.records {
		if !now.Before(record.expires) {
			continue
		}
		retained = append(retained, record)
		cookie := record.cookie
		// Expiry is frozen above, so rebuilds never extend Max-Age or session life.
		// The standard jar handles domain, path and Secure matching.
		cookie.MaxAge = 0
		cookie.Expires = time.Time{}
		entry.jar.SetCookies(&record.source, []*http.Cookie{&cookie})
	}
	entry.records = retained
}

func newCookieJar() *cookiejar.Jar {
	jar, _ := cookiejar.New(&cookiejar.Options{PublicSuffixList: publicsuffix.List})
	return jar
}

func validatedCookie(source *url.URL, cookie *http.Cookie, now time.Time) (storedCookie, bool) {
	copyCookie := *cookie
	path := cookie.Path
	if !strings.HasPrefix(path, "/") {
		path = source.Path
		if index := strings.LastIndex(path, "/"); index > 0 {
			path = path[:index]
		} else {
			path = "/"
		}
	}
	// Probe standard acceptance at the cookie's effective path. Use a session
	// value for validation so deletion and injected clocks share this same path.
	probe := *source
	probe.Path = path
	probe.RawPath = ""
	validation := *cookie
	validation.MaxAge = 0
	validation.Expires = time.Time{}
	jar := newCookieJar()
	jar.SetCookies(source, []*http.Cookie{&validation})
	if len(jar.Cookies(&probe)) == 0 {
		return storedCookie{}, false
	}
	domain := strings.TrimPrefix(strings.ToLower(cookie.Domain), ".")
	if domain == "" {
		domain = strings.ToLower(source.Hostname())
	}
	expires := now.Add(cookieLifetime)
	switch {
	case cookie.MaxAge < 0:
		expires = now
	case cookie.MaxAge > 0:
		// HTTP's parser limits Max-Age to platform int; avoid duration overflow.
		seconds := int64(cookie.MaxAge)
		if seconds > int64((1<<63-1)/int64(time.Second)) {
			seconds = int64((1<<63 - 1) / int64(time.Second))
		}
		expires = now.Add(time.Duration(seconds) * time.Second)
	case !cookie.Expires.IsZero():
		expires = cookie.Expires
	}
	return storedCookie{key: cookie.Name + "\x00" + domain + "\x00" + path, cookie: copyCookie, source: *source, expires: expires}, true
}

func cookieURL(source *url.URL) *url.URL {
	if source == nil {
		return nil
	}
	copyURL := *source
	if copyURL.Scheme == "wss" {
		copyURL.Scheme = "https"
	}
	if copyURL.Scheme != "https" {
		return nil
	}
	host := strings.ToLower(copyURL.Hostname())
	if host != "chatgpt.com" && host != "chat.openai.com" && host != "chatgpt-staging.com" && !strings.HasSuffix(host, ".chatgpt.com") && !strings.HasSuffix(host, ".chatgpt-staging.com") {
		return nil
	}
	return &copyURL
}

func allowedCookie(name string) bool {
	switch name {
	case "__cf_bm", "__cflb", "__cfruid", "__cfseq", "__cfwaitingroom", "__oailb", "_cfuvid", "cf_clearance", "cf_ob_info", "cf_use_ob":
		return true
	}
	return strings.HasPrefix(name, "cf_chl_")
}
