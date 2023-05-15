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

package transport

import "time"

type RawMessage struct {
	MessageId MessageId
	Metadata  Metadata
	Protocol  Protocol
	Body      []byte
}

type ParsedMessage struct {
	MessageId   MessageId
	Metadata    Metadata
	Protocol    Protocol
	MessageType string
	CborBody    []byte
}

type MessageType = string

type Direction = byte

const (
	IncomingDirection Direction = 1
	OutgoingDirection Direction = 2
)

type MessageId struct {
	SessionAlias string
	Direction    Direction
	Sequence     int64
	Subsequence  []int32
	Timestamp    Timestamp
}

type Timestamp struct {
	seconds int64
	nano    int32
}

func (t Timestamp) Sec() int64 {
	return t.seconds
}

func (t Timestamp) Nano() int32 {
	return t.nano
}

func (t Timestamp) ToTime() time.Time {
	return time.Unix(t.seconds, int64(t.nano))
}

func TimestampFromTime(t time.Time) Timestamp {
	return Timestamp{
		seconds: t.Unix(),
		nano:    int32(t.Nanosecond()),
	}
}

type Metadata = map[string]string

type Protocol = string
