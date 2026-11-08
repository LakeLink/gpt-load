package embedded

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

func TestCodexWSSessionObservesMidConnectionAndLateMessages(t *testing.T) {
	firstBody, secondBody := make(chan []byte, 1), make(chan []byte, 1)
	idle := make(chan struct{})
	var releaseIdleOnce sync.Once
	releaseIdle := func() { releaseIdleOnce.Do(func() { close(idle) }) }
	defer releaseIdle()
	idlePayload := []byte(" \r\n{\"type\":\"fixture.idle\",\"metadata\":true} \t")
	created := []byte(`{"type":"response.created","response":{"id":"resp_second","status":"in_progress"}}`)
	late := []byte(`{"type":"response.output_text.delta","response_id":"resp_first","delta":"late"}`)
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := (&websocket.Upgrader{}).Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		_, body, err := conn.ReadMessage()
		if err != nil {
			return
		}
		firstBody <- body
		if conn.WriteMessage(websocket.TextMessage, wsCompleted("resp_first")) != nil {
			return
		}
		_, body, err = conn.ReadMessage()
		if err != nil {
			return
		}
		secondBody <- body
		for _, payload := range [][]byte{created, late, wsCompleted("resp_second")} {
			if conn.WriteMessage(websocket.TextMessage, payload) != nil {
				return
			}
		}
		select {
		case <-idle:
		case <-r.Context().Done():
			return
		}
		if conn.WriteMessage(websocket.TextMessage, idlePayload) != nil {
			return
		}
		_, _, _ = conn.ReadMessage()
	}))
	defer upstream.Close()
	session := wsTestSession(t, upstream.URL)
	if _, err := session.ExecuteTurn(t.Context(), json.RawMessage(`{"model":"gpt-5","input":"first"}`), nil); err != nil {
		t.Fatal(err)
	}
	observed := make(chan ExchangeObservation, 16)
	ctx := WithCodexWSTurnObserver(t.Context(), func(fact ExchangeObservation) {
		fact.Body = append([]byte(nil), fact.Body...)
		observed <- fact
	})
	if _, err := session.ExecuteTurn(ctx, json.RawMessage(`{"model":"gpt-5","input":"second","previous_response_id":"resp_first"}`), nil); err != nil {
		t.Fatal(err)
	}
	releaseIdle()
	var facts []ExchangeObservation
	for {
		select {
		case fact := <-observed:
			facts = append(facts, fact)
			if string(fact.Body) == string(idlePayload) {
				goto receivedIdle
			}
		case <-time.After(time.Second):
			t.Fatal("idle message was not observed")
		}
	}
receivedIdle:
	if err := session.Close(); err != nil {
		t.Fatal(err)
	}
	for ends := 0; ends < 2; {
		select {
		case fact := <-observed:
			facts = append(facts, fact)
			if fact.Stream != nil && fact.Stream.Kind == "end" {
				ends++
			}
		case <-time.After(time.Second):
			t.Fatal("connection ends missing")
		}
	}
	sent := wsStreamFacts(facts, "request", "data")
	firstSent, secondSent := <-firstBody, <-secondBody
	if len(sent) != 1 || string(sent[0].Body) != string(secondSent) || sent[0].Stream.StreamSequence != 2 || sent[0].Stream.ByteOffset != uint64(len(firstSent)) || sent[0].Stream.WriteResult != "success" {
		t.Fatalf("serialized send or mid-connection counters changed: %+v", sent)
	}
	received := wsStreamFacts(facts, "response", "data")
	if len(received) != 4 {
		t.Fatalf("messages=%+v", received)
	}
	offset := uint64(len(wsCompleted("resp_first")))
	for i, payload := range [][]byte{created, late, wsCompleted("resp_second"), idlePayload} {
		fact := received[i]
		if string(fact.Body) != string(payload) || fact.Stream.StreamSequence != uint64(i+2) || fact.Stream.ByteOffset != offset || fact.Stream.BodySize != uint64(len(payload)) {
			t.Fatalf("raw message %d or counters changed: %+v", i, fact)
		}
		wantConnectionOnly := i == 1 || i == 3
		if fact.Stream.ConnectionOnly != wantConnectionOnly {
			t.Fatalf("message %d ownership=%+v", i, fact.Stream)
		}
		offset += uint64(len(payload))
	}
	for _, fact := range facts {
		if fact.Stream == nil || fact.Stream.Kind != "end" {
			continue
		}
		if !fact.Stream.ConnectionOnly || fact.Stream.EndReason != "closed" {
			t.Fatalf("end attribution/reason=%+v", fact.Stream)
		}
		if fact.Stream.Direction == "response" && (fact.Stream.LastStreamSequence != 5 || fact.Stream.TotalBodyBytes != offset) {
			t.Fatalf("response totals=%+v", fact.Stream)
		}
		if fact.Stream.Direction == "request" && (fact.Stream.LastStreamSequence != 2 || fact.Stream.TotalBodyBytes != uint64(len(firstSent)+len(secondSent))) {
			t.Fatalf("request totals=%+v", fact.Stream)
		}
	}
}
