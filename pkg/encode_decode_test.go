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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	transport "github.com/th2-net/transport-go/pkg"
)

func TestEncodeDecode(t *testing.T) {
	type holder struct {
		msg     any
		groupId int
	}
	testData := []struct {
		name     string
		messages []holder
	}{
		{
			name: "single raw",
			messages: []holder{
				{
					groupId: 0,
					msg: &transport.RawMessage{
						MessageId: transport.MessageId{
							SessionAlias: "alias1",
							Direction:    transport.IncomingDirection,
							Sequence:     42,
							Timestamp:    transport.TimestampFromTime(time.Now()),
						},
						Protocol: "protocol",
						Metadata: map[string]string{
							"key": "value",
						},
						Body: []byte("test_data"),
					},
				},
			},
		},
		{
			name: "single raw with event ID",
			messages: []holder{
				{
					groupId: 0,
					msg: &transport.RawMessage{
						MessageId: transport.MessageId{
							SessionAlias: "alias1",
							Direction:    transport.IncomingDirection,
							Sequence:     42,
							Timestamp:    transport.TimestampFromTime(time.Now()),
						},
						EventID:  transport.NewEventID("id", "book", "scope", transport.TimestampFromTime(time.Now())),
						Protocol: "protocol",
						Metadata: map[string]string{
							"key": "value",
						},
						Body: []byte("test_data"),
					},
				},
			},
		},
		{
			name: "several sub sequences",
			messages: []holder{
				{
					groupId: 0,
					msg: &transport.RawMessage{
						MessageId: transport.MessageId{
							SessionAlias: "alias1",
							Direction:    transport.IncomingDirection,
							Sequence:     42,
							Subsequence:  []int32{1, 2},
							Timestamp:    transport.TimestampFromTime(time.Now()),
						},
						Protocol: "protocol",
						Metadata: map[string]string{
							"key": "value",
						},
						Body: []byte("test_data"),
					},
				},
			},
		},
		{
			name: "single parsed",
			messages: []holder{
				{
					groupId: 0,
					msg: &transport.ParsedMessage{
						MessageId: transport.MessageId{
							SessionAlias: "alias1",
							Direction:    transport.IncomingDirection,
							Sequence:     42,
							Subsequence:  []int32{1},
							Timestamp:    transport.TimestampFromTime(time.Now()),
						},
						MessageType: "TestType",
						Protocol:    "protocol",
						Metadata: map[string]string{
							"key": "value",
						},
						Body: []byte("test_data"),
					},
				},
			},
		},
		{
			name: "single parsed with event ID",
			messages: []holder{
				{
					groupId: 0,
					msg: &transport.ParsedMessage{
						MessageId: transport.MessageId{
							SessionAlias: "alias1",
							Direction:    transport.IncomingDirection,
							Sequence:     42,
							Subsequence:  []int32{1},
							Timestamp:    transport.TimestampFromTime(time.Now()),
						},
						EventID:     transport.NewEventID("id", "book", "scope", transport.TimestampFromTime(time.Now())),
						MessageType: "TestType",
						Protocol:    "protocol",
						Metadata: map[string]string{
							"key": "value",
						},
						Body: []byte("test_data"),
					},
				},
			},
		},
		{
			name: "two parsed in one group",
			messages: []holder{
				{
					groupId: 0,
					msg: &transport.ParsedMessage{
						MessageId: transport.MessageId{
							SessionAlias: "alias1",
							Direction:    transport.IncomingDirection,
							Sequence:     42,
							Subsequence:  []int32{1},
							Timestamp:    transport.TimestampFromTime(time.Now()),
						},
						MessageType: "TestType1",
						Protocol:    "protocol",
						Metadata: map[string]string{
							"key": "value",
						},
						Body: []byte("test_data"),
					},
				},
				{
					groupId: 0,
					msg: &transport.ParsedMessage{
						MessageId: transport.MessageId{
							SessionAlias: "alias1",
							Direction:    transport.IncomingDirection,
							Sequence:     42,
							Subsequence:  []int32{2},
							Timestamp:    transport.TimestampFromTime(time.Now()),
						},
						MessageType: "TestType2",
						Protocol:    "protocol",
						Metadata: map[string]string{
							"key": "value",
						},
						Body: []byte("test_data"),
					},
				},
			},
		},
		{
			name: "two parsed",
			messages: []holder{
				{
					groupId: 0,
					msg: &transport.ParsedMessage{
						MessageId: transport.MessageId{
							SessionAlias: "alias1",
							Direction:    transport.IncomingDirection,
							Sequence:     42,
							Subsequence:  []int32{1},
							Timestamp:    transport.TimestampFromTime(time.Now()),
						},
						MessageType: "TestType",
						Protocol:    "protocol",
						Metadata: map[string]string{
							"key": "value",
						},
						Body: []byte("test_data"),
					},
				},
				{
					groupId: 1,
					msg: &transport.ParsedMessage{
						MessageId: transport.MessageId{
							SessionAlias: "alias1",
							Direction:    transport.IncomingDirection,
							Sequence:     42,
							Subsequence:  []int32{1},
							Timestamp:    transport.TimestampFromTime(time.Now()),
						},
						MessageType: "TestType",
						Protocol:    "protocol",
						Metadata: map[string]string{
							"key": "value",
						},
						Body: []byte("test_data"),
					},
				},
			},
		},
	}

	for _, td := range testData {
		t.Run(td.name, func(t *testing.T) {
			e := transport.NewEncoder(nil)
			for _, h := range td.messages {
				switch h.msg.(type) {
				case *transport.RawMessage:
					e.EncodeRaw(*h.msg.(*transport.RawMessage), h.groupId)
				case *transport.ParsedMessage:
					e.EncodeParsed(*h.msg.(*transport.ParsedMessage), h.groupId)
				default:
					t.Fatalf("unsupported message type %#v", h.msg)
				}
			}
			batch := e.CompleteBatch("group", "book")
			d := transport.NewDecoder(batch)
			expectedMessagesCount := len(td.messages)
			expectedIndex := 0
			for {
				msg, groupId, err := d.NextMessage()
				if err != nil {
					t.Fatalf("cannot decode message %v", err)
				}
				if msg == nil {
					break
				}
				if expectedIndex >= expectedMessagesCount {
					t.Fatalf("received more messages then expected")
				}
				expectedHolder := td.messages[expectedIndex]
				expectedIndex += 1
				expected := expectedHolder.msg
				if assert.IsType(t, expected, msg, "unexpected type") {
					assert.Equal(t, expected, msg, "unexpected message decoded")
					assert.Equal(t, expectedHolder.groupId, groupId)
				}
			}
			assert.Equal(t, "book", d.GetBook(), "unexpected book")
			assert.Equal(t, "group", d.GetGroup(), "unexpected group")
		})
	}
}
