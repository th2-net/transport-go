/*
 * Copyright 2023 Exactpro (Exactpro Systems Limited)
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package transport_test

import (
	"fmt"
	"github.com/th2-net/transport-go/pkg"
	"math/rand"
	"testing"
	"time"
)

func init() {
	rand.Seed(time.Now().Unix())
}

func payload(size int) []byte {
	ints := rand.Perm(size)
	b := make([]byte, len(ints))
	for i, el := range ints {
		b[i] = byte(el)
	}
	return b
}

func BenchmarkDecoding(b *testing.B) {
	for _, size := range []int{50, 100, 250, 500, 1000} {
		e := transport.NewEncoder(nil)
		e.EncodeRaw(transport.RawMessage{
			MessageId: transport.MessageId{
				SessionAlias: "test",
				Direction:    transport.IncomingDirection,
				Sequence:     int64(1),
				Timestamp:    transport.TimestampFromTime(time.Now()),
			},
			Body: payload(size),
		}, 1)
		batch := e.CompleteBatch("group", "book")
		b.ResetTimer()
		b.Run(fmt.Sprintf("S%d", size), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				testDecoder(b, batch)
			}
		})
	}
}

//go:noinline
func testDecoder(b *testing.B, batch []byte) any {
	d := transport.NewDecoder(batch)
	msg, _, err := d.NextMessage()
	if err != nil {
		b.Fatal(err)
	}
	consume(msg)
	d.Used(msg)
	return msg
}

func BenchmarkEncoding(b *testing.B) {
	for _, payloadSize := range []int{50, 100, 250, 500, 1000} {
		for _, initBuffPer := range []int{0, 50, 100, 200, 500} {
			data := payload(payloadSize)
			initBufSize := payloadSize * initBuffPer / 100
			initBuf := make([]byte, initBufSize)
			b.Run(fmt.Sprintf("Raw_Size_%d_Init_Buf_%d", payloadSize, initBufSize), func(b *testing.B) {
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					e := transport.NewEncoder(initBuf)
					testEncode(data, e)
				}
			})
		}
	}
}

//go:noinline
func testEncode(data []byte, e transport.Encoder) []byte {
	e.EncodeRaw(transport.RawMessage{
		MessageId: transport.MessageId{
			SessionAlias: "test",
			Direction:    transport.IncomingDirection,
			Sequence:     42,
			Timestamp:    transport.TimestampFromTime(time.Now()),
		},
		Protocol: "protocol",
		Body:     data,
	}, 1)
	batch := e.CompleteBatch("group", "book")
	consume(batch)
	return batch
}

//go:noinline
func consume(data any) {
	// do nothing
}
