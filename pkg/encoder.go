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

import "fmt"

const (
	unset_group      int = -1
	type_len_size    int = 1 + 4
	batch_start_size int = type_len_size * 2
	group_start_size int = type_len_size * 2
)

type Encoder struct {
	dst             []byte
	wrInx           int
	lastGroupId     int
	lastGroupOffset int
}

func NewEncoder(dst []byte) Encoder {
	return Encoder{
		dst:         dst,
		wrInx:       0,
		lastGroupId: unset_group,
	}
}

func (e *Encoder) Reset() {
	e.wrInx = 0
	e.lastGroupId = unset_group
	e.lastGroupOffset = 0
}

func (e *Encoder) IsEmpty() bool {
	return e.wrInx == 0
}

func (e *Encoder) CompleteBatch(group, book string) []byte {
	if e.IsEmpty() {
		e.initBatchAndGroup(unset_group)
	}
	e.writeGroupEnd()
	e.writeGroupListEnd()

	e.ensureWritable(type_len_size + len(book) + type_len_size + len(group))
	e.wrInx += writeBook(e.buf(), book)
	e.wrInx += writeGroup(e.buf(), group)
	e.writeBatchEnd()
	return e.dst[:e.wrInx]
}

func (e *Encoder) EncodeRaw(message RawMessage, groupId int) {
	e.initBatchAndGroup(groupId)
	e.writeRawMessage(message)
}

func (e *Encoder) SizeAfterEncodeRaw(group string, book string, message RawMessage, groupId int) int {
	return e.computeBaseSize(groupId) +
		type_len_size + // rew message
		computeMessageIdSize(message.MessageId) +
		computeMetadataSize(message.Metadata) +
		computeProtocolSize(message.Protocol) +
		computeBodySize(message.Body) +
		computeEventIdSize(message.EventID) +
		type_len_size + len(book) +
		type_len_size + len(group)
}

func (e *Encoder) EncodeParsed(message ParsedMessage, groupId int) {
	e.initBatchAndGroup(groupId)
	e.writeParsedMessage(message)
}

func (e *Encoder) SizeAfterEncodeParsed(group string, book string, message ParsedMessage, groupId int) int {
	return e.computeBaseSize(groupId) +
		type_len_size + // parsed message
		computeMessageIdSize(message.MessageId) +
		computeMetadataSize(message.Metadata) +
		computeProtocolSize(message.Protocol) +
		computeMessageTypeSize(message.MessageType) +
		computeBodySize(message.CborBody) +
		computeEventIdSize(message.EventID) +
		type_len_size + len(book) +
		type_len_size + len(group)
}

func (e *Encoder) initBatchAndGroup(groupId int) {
	if e.wrInx == 0 {
		e.writeBatchStart()
	}
	if e.lastGroupId == unset_group {
		e.writeGroupStart()
		e.lastGroupId = groupId
	}
	if e.lastGroupId != groupId {
		e.writeGroupEnd()
		e.lastGroupId = groupId
		e.writeGroupStart()
	}
}

func (e *Encoder) computeBaseSize(groupId int) int {
	size := e.wrInx
	if size == 0 {
		size += batch_start_size
	}
	if e.lastGroupId != groupId {
		size += group_start_size
	}
	return size
}

func (e *Encoder) buf() []byte {
	return e.dst[e.wrInx:]
}

func (e *Encoder) ensureWritable(size int) {
	capacity := len(e.dst)
	if capacity-e.wrInx < size {
		newSize := max(capacity*2, e.wrInx+size)
		cp := make([]byte, newSize)
		copied := copy(cp, e.dst)
		if copied != capacity {
			panic(fmt.Sprintf("not all bytes copied: %d but should %d", copied, capacity))
		}
		e.dst = cp
	}
}

func max(a, b int) int {
	if a < b {
		return b
	} else {
		return a
	}
}

func (e *Encoder) writeBatchStart() {
	e.ensureWritable(batch_start_size)
	e.wrInx += writeType(e.buf(), groupBatchCodecType)
	e.wrInx += writeLen(e.buf(), 0) // placeholder for batch length
	e.wrInx += writeType(e.buf(), groupListCodecType)
	e.wrInx += writeLen(e.buf(), 0) // placeholder for group list length
}

func (e *Encoder) writeGroupStart() {
	e.ensureWritable(group_start_size)
	e.lastGroupOffset = e.wrInx
	e.wrInx += writeType(e.buf(), messageGroupCodecType)
	e.wrInx += writeLen(e.buf(), 0) // placeholder for group
	e.wrInx += writeType(e.buf(), messageListCodecType)
	e.wrInx += writeLen(e.buf(), 0) // placeholder for messages list
}

func (e *Encoder) writeGroupEnd() {
	groupLenIndex := e.lastGroupOffset + 1
	messageListLenIndex := e.lastGroupOffset + type_len_size + 1
	_ = writeLen(e.dst[groupLenIndex:], e.wrInx-groupLenIndex-lengthSize)
	_ = writeLen(e.dst[messageListLenIndex:], e.wrInx-messageListLenIndex-lengthSize)
}

func (e *Encoder) writeGroupListEnd() {
	groupListLenIdx := type_len_size + 1
	_ = writeLen(e.dst[groupListLenIdx:], e.wrInx-groupListLenIdx-lengthSize)
}

func (e *Encoder) writeBatchEnd() {
	batchLenIdx := 1
	_ = writeLen(e.dst[batchLenIdx:], e.wrInx-batchLenIdx-lengthSize)
}

func (e *Encoder) writeRawMessage(message RawMessage) {
	e.ensureWritable(type_len_size)
	e.wrInx += writeType(e.buf(), rawMessageCodecType)
	lenIndex := e.wrInx
	e.wrInx += writeLen(e.buf(), 0) // placeholder for msg len

	e.writeMessageId(message.MessageId)
	e.writeMetadata(message.Metadata)
	e.writeProtocol(message.Protocol)
	e.writeBody(message.Body, rawBodyCodecType)
	e.writeEventId(message.EventID)

	_ = writeLen(e.dst[lenIndex:], e.wrInx-lenIndex-lengthSize)
}

func (e *Encoder) writeParsedMessage(message ParsedMessage) {
	e.ensureWritable(type_len_size)
	e.wrInx += writeType(e.buf(), parsedMessageCodecType)
	lenIndex := e.wrInx
	e.wrInx += writeLen(e.buf(), 0) // placeholder for msg len

	e.writeMessageId(message.MessageId)
	e.writeMetadata(message.Metadata)
	e.writeProtocol(message.Protocol)
	e.writeMessageType(message.MessageType)
	e.writeBody(message.CborBody, parsedBodyCodecType)
	e.writeEventId(message.EventID)

	_ = writeLen(e.dst[lenIndex:], e.wrInx-lenIndex-lengthSize)
}

func (e *Encoder) writeMessageId(id MessageId) {
	e.ensureWritable(type_len_size)
	e.wrInx += writeType(e.buf(), messageIdCodecType)
	lenIndex := e.wrInx
	e.wrInx += writeLen(e.buf(), 0) // placeholder for id len

	e.ensureWritable(type_len_size + len(id.SessionAlias))
	e.wrInx += writeString(e.buf(), sessionAliasCodecType, id.SessionAlias)
	e.ensureWritable(type_len_size + 1)
	e.wrInx += writeDirection(e.buf(), id.Direction)
	e.ensureWritable(type_len_size + 8)
	e.wrInx += writeLongValue(e.buf(), sequenceCodecType, id.Sequence)
	e.ensureWritable(type_len_size + len(id.Subsequence)*(type_len_size+4))
	e.wrInx += writeIntCollection(e.buf(), subsequenceCodecType, id.Subsequence)
	e.ensureWritable(type_len_size + 8 + 4)
	e.wrInx += writeTime(e.buf(), id.Timestamp)

	_ = writeLen(e.dst[lenIndex:], e.wrInx-lenIndex-lengthSize)
}

func computeMessageIdSize(id MessageId) int {
	return type_len_size + // message id
		type_len_size + len(id.SessionAlias) + // alias
		type_len_size + 1 + // direction
		type_len_size + 8 + // sequence
		type_len_size + len(id.Subsequence)*(type_len_size+4) + // subsequence
		type_len_size + 8 + 4 // timestamp
}

func (e *Encoder) writeMetadata(metadata Metadata) {
	e.ensureWritable(type_len_size)
	e.wrInx += writeType(e.buf(), metadataCodecType)
	lenIndex := e.wrInx
	e.wrInx += writeLen(e.buf(), 0) // placeholder for metadata len

	for k, v := range metadata {
		e.ensureWritable(type_len_size + len(k))
		e.wrInx += writeString(e.buf(), stringCodecType, k)

		e.ensureWritable(type_len_size + len(v))
		e.wrInx += writeString(e.buf(), stringCodecType, v)
	}

	_ = writeLen(e.dst[lenIndex:], e.wrInx-lenIndex-lengthSize)
}

func computeMetadataSize(metadata Metadata) int {
	size := type_len_size // metadata
	for k, v := range metadata {
		size += type_len_size + len(k) + // key
			type_len_size + len(v) // value
	}
	return size
}

func (e *Encoder) writeProtocol(protocol Protocol) {
	e.ensureWritable(type_len_size + len(protocol))
	e.wrInx += writeProtocol(e.buf(), protocol)
}

func computeProtocolSize(protocol Protocol) int {
	return type_len_size + len(protocol)
}

func (e *Encoder) writeBody(body []byte, bodyCodecType codecType) {
	bodyLen := len(body)
	e.ensureWritable(type_len_size + bodyLen)
	e.wrInx += writeType(e.buf(), bodyCodecType)
	e.wrInx += writeLen(e.buf(), bodyLen)
	copied := copy(e.buf(), body)
	if copied != bodyLen {
		panic(fmt.Sprintf("couldn't copy the whole body. BufCap: %d, wrIdx: %d, bodyLen: %d",
			cap(e.dst), e.wrInx, bodyLen))
	}
	e.wrInx += copied
}

func computeBodySize(body []byte) int {
	return type_len_size + len(body)
}

func (e *Encoder) writeMessageType(messageType string) {
	e.ensureWritable(type_len_size + len(messageType))
	e.wrInx += writeString(e.buf(), messageTypeCodecType, messageType)
}

func computeMessageTypeSize(messageType string) int {
	return type_len_size + len(messageType)
}

func (e *Encoder) writeEventId(id *EventID) {
	if id == nil {
		return
	}
	payloadLen := (type_len_size + len(id.ID)) +
		(type_len_size + len(id.Book)) +
		(type_len_size + len(id.Scope)) +
		(type_len_size + 8 + 4)
	totalLength := type_len_size + payloadLen
	e.ensureWritable(totalLength)
	e.wrInx += writeType(e.buf(), eventIdCodecType)
	e.wrInx += writeLen(e.buf(), payloadLen)

	e.wrInx += writeString(e.buf(), idCodecType, id.ID)
	e.wrInx += writeString(e.buf(), bookCodecType, id.Book)
	e.wrInx += writeString(e.buf(), scopeCodecType, id.Scope)
	e.wrInx += writeTime(e.buf(), id.Timestamp)
}

func computeEventIdSize(id *EventID) int {
	if id == nil {
		return 0
	}

	return type_len_size + // event id
		type_len_size + len(id.ID) + // id
		type_len_size + len(id.Book) + // book
		type_len_size + len(id.Scope) + // scope
		type_len_size + 8 + 4 // timestamp
}
