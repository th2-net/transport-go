/*
 * Copyright 2023-2025 Exactpro (Exactpro Systems Limited)
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

import (
	"fmt"
	"sync"
)

func NewDecoder(batchData []byte) Decoder {
	return Decoder{
		batch:     batchData,
		pos:       0,
		lastGroup: unsetGroup,
	}
}

var rawPool = sync.Pool{
	New: func() any {
		return &RawMessage{}
	},
}

var parsedPool = sync.Pool{
	New: func() any {
		return &ParsedMessage{
			MessageId: MessageId{},
		}
	},
}

func getParsed() *ParsedMessage {
	p := parsedPool.Get().(*ParsedMessage)
	p.Body = nil
	p.MessageId.Subsequence = nil
	p.Metadata = nil
	return p
}

func getRaw() *RawMessage {
	r := rawPool.Get().(*RawMessage)
	r.Body = nil
	r.Metadata = nil
	r.MessageId.Subsequence = nil
	return r
}

type Decoder struct {
	book      string
	group     string
	batch     []byte
	pos       int
	lastGroup int
}

func (d *Decoder) GetBook() string {
	return d.book
}

func (d *Decoder) GetGroup() string {
	return d.group
}

func (d *Decoder) data() []byte {
	return d.batch[d.pos:]
}

func (d *Decoder) Used(msg any) {
	switch msg.(type) {
	case *RawMessage:
		rawPool.Put(msg)
	case *ParsedMessage:
		parsedPool.Put(msg)
	default:
		panic(fmt.Sprintf("unsupported type %T", msg))
	}
}

/*
NextMessage returns a next message from the batch (either raw or parsed).
It also returns groupId that indicate to which group the message corresponds
*/
func (d *Decoder) NextMessage() (msg any, groupId int, err error) {
	for {
		groupId = d.lastGroup
		if d.pos == len(d.batch) {
			return
		}
		codecT, r := readType(d.data())
		if d.pos == 0 && codecT != groupBatchCodecType {
			return nil, 0, fmt.Errorf("batch does not start with %d element (actual: %d)",
				groupBatchCodecType, codecT)
		}
		d.pos += r
		partL, r := readLength(d.data())
		d.pos += r
		partBytes := d.batch[d.pos : d.pos+partL]
		switch codecT {
		case messageGroupCodecType:
			d.lastGroup += 1
			groupId = d.lastGroup
		case bookCodecType:
			d.book = string(partBytes)
			d.pos += partL
		case sessionGroupCodecType:
			d.group = string(partBytes)
			d.pos += partL
		case rawMessageCodecType:
			msg, err = decodeRawMessage(partBytes)
			d.pos += partL
			return
		case parsedMessageCodecType:
			msg, err = decodeParsedMessage(partBytes)
			d.pos += partL
			return
		}
	}
}

func decodeRawMessage(src []byte) (*RawMessage, error) {
	srcLen := len(src)
	var err error
	message := getRaw()
	pos := 0
	for {
		if pos == srcLen {
			break
		}
		codecT, r := readType(src[pos:])
		pos += r
		partL, r := readLength(src[pos:])
		pos += r
		partBytes := src[pos : pos+partL]
		switch codecT {
		case messageIdCodecType:
			message.MessageId, err = decodeMessageId(partBytes)
		case metadataCodecType:
			message.Metadata, _ = readMetadata(partBytes)
		case protocolCodecType:
			message.Protocol = string(partBytes)
		case rawBodyCodecType:
			message.Body = partBytes
		case eventIdCodecType:
			message.EventID = decodeEventId(partBytes)
		}
		pos += partL
		if err != nil {
			return nil, err
		}
	}
	return message, nil
}

func decodeParsedMessage(src []byte) (*ParsedMessage, error) {
	var err error
	message := getParsed()
	pos := 0
	for {
		if pos == len(src) {
			break
		}
		codecT, r := readType(src[pos:])
		pos += r
		partL, r := readLength(src[pos:])
		pos += r
		partBytes := src[pos : pos+partL]
		switch codecT {
		case messageIdCodecType:
			message.MessageId, err = decodeMessageId(partBytes)
		case metadataCodecType:
			message.Metadata, _ = readMetadata(partBytes)
		case protocolCodecType:
			message.Protocol = string(partBytes)
		case messageTypeCodecType:
			message.MessageType = string(partBytes)
		case parsedBodyCodecType:
			message.Body = partBytes
		case eventIdCodecType:
			message.EventID = decodeEventId(partBytes)
		}
		pos += partL
		if err != nil {
			return nil, err
		}
	}
	return message, nil
}

func decodeMessageId(src []byte) (MessageId, error) {
	id := MessageId{}
	pos := 0
	for {
		if pos == len(src) {
			break
		}
		codecT, r := readType(src[pos:])
		pos += r
		partL, r := readLength(src[pos:])
		pos += r
		partBytes := src[pos : pos+partL]
		switch codecT {
		case sessionAliasCodecType:
			id.SessionAlias = string(partBytes)
		case directionCodecType:
			switch src[pos] {
			case IncomingDirection:
				id.Direction = IncomingDirection
			case OutgoingDirection:
				id.Direction = OutgoingDirection
			default:
				return id, fmt.Errorf("unknown direction %d", src[pos])
			}
		case sequenceCodecType:
			id.Sequence, _ = readLong(partBytes)
		case subsequenceCodecType:
			id.Subsequence, _ = readIntCollection(partBytes)
		case timestampCodecType:
			id.Timestamp, _ = readTimestamp(partBytes)
		}
		pos += partL
	}
	return id, nil
}

func decodeEventId(src []byte) *EventID {
	id := new(EventID)
	pos := 0
	for {
		if pos == len(src) {
			break
		}
		codecT, r := readType(src[pos:])
		pos += r
		partL, r := readLength(src[pos:])
		pos += r
		partBytes := src[pos : pos+partL]
		switch codecT {
		case idCodecType:
			id.ID = string(partBytes)
		case bookCodecType:
			id.Book = string(partBytes)
		case scopeCodecType:
			id.Scope = string(partBytes)
		case timestampCodecType:
			id.Timestamp, _ = readTimestamp(partBytes)
		}
		pos += partL
	}
	return id
}
