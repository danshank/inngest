package golang

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/inngest/inngestgo"
	"github.com/stretchr/testify/require"
)

func TestTraceContext_PreservesCallerTraceID(t *testing.T) {
	const callerTraceID = "deadbeefdeadbeefdeadbeefdeadbeef"
	const callerSpanID = "1111111111111111"
	const inboundTraceparent = "00-" + callerTraceID + "-" + callerSpanID + "-01"

	captured := make(chan string, 16)

	client := mustNewSDKClient(t, "trace-context-test")
	server := wrapSDKWithHeaderCapture(t, client, captured)
	defer server.Close()

	sdkHost := os.Getenv("INNGEST_SDK_HOST")
	if sdkHost == "" {
		sdkHost = "127.0.0.1"
	}
	sdkURL := fmt.Sprintf("http://%s:%d/", sdkHost, server.Port)
	u, _ := url.Parse(sdkURL)
	client.SetURL(u)

	_, err := inngestgo.CreateFunction(
		client,
		inngestgo.FunctionOpts{ID: "trace-echo"},
		inngestgo.EventTrigger("trace.context.test", nil),
		func(ctx context.Context, input inngestgo.Input[any]) (any, error) {
			return nil, nil
		},
	)
	require.NoError(t, err)
	registerSDK(t, server)

	req, err := http.NewRequest(
		http.MethodPost,
		DEV_URL+"/e/test",
		strings.NewReader(`{"name":"trace.context.test","data":{}}`),
	)
	require.NoError(t, err)
	req.Header.Set("content-type", "application/json")
	req.Header.Set("traceparent", inboundTraceparent)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	require.Equal(t, 200, resp.StatusCode)
	_ = resp.Body.Close()

	deadline := time.After(20 * time.Second)
	for {
		select {
		case tp := <-captured:
			if tp == "" {
				continue
			}
			parts := strings.Split(tp, "-")
			require.Lenf(t, parts, 4, "outbound traceparent malformed: %q", tp)
			require.Equalf(t, callerTraceID, parts[1],
				"inbound=%s outbound=%s", inboundTraceparent, tp)
			require.NotEqualf(t, callerSpanID, parts[2],
				"parent-id should be the executor's span ID, not the caller's: %q", parts[2])
			return
		case <-deadline:
			t.Fatalf("executor never invoked the SDK with a traceparent within 20s")
		}
	}
}

func mustNewSDKClient(t *testing.T, appID string) inngestgo.Client {
	t.Helper()
	_ = os.Setenv("INNGEST_DEV", DEV_URL)
	key := "test"
	c, err := inngestgo.NewClient(inngestgo.ClientOpts{
		AppID:       appID,
		EventKey:    &key,
		Logger:      slog.New(slog.DiscardHandler),
		RegisterURL: inngestgo.StrPtr(fmt.Sprintf("%s/fn/register", DEV_URL)),
	})
	require.NoError(t, err)
	return c
}

func wrapSDKWithHeaderCapture(t *testing.T, client inngestgo.Client, ch chan<- string) *HTTPServer {
	t.Helper()
	sdk := client.Serve()
	wrapped := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case ch <- r.Header.Get("traceparent"):
		default:
		}
		sdk.ServeHTTP(w, r)
	})
	return NewHTTPServer(wrapped)
}

func registerSDK(t *testing.T, server *HTTPServer) {
	t.Helper()
	req, err := http.NewRequest(http.MethodPut, server.LocalURL(), nil)
	require.NoError(t, err)
	req.Close = true
	resp, err := (&http.Client{
		Transport: &http.Transport{DisableKeepAlives: true},
	}).Do(req)
	require.NoError(t, err)
	require.Equal(t, 200, resp.StatusCode)
	_ = resp.Body.Close()
}
