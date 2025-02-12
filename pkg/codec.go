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
	"encoding/binary"
	"fmt"
)

type codecType = uint8

type length = int

const (
	longCodecType          codecType = 1
	stringCodecType        codecType = 2
	intCodecType           codecType = 3
	messageIdCodecType     codecType = 10
	bookCodecType          codecType = 101
	sessionGroupCodecType  codecType = 102
	sessionAliasCodecType  codecType = 103
	directionCodecType     codecType = 104
	sequenceCodecType      codecType = 105
	subsequenceCodecType   codecType = 106
	timestampCodecType     codecType = 107
	metadataCodecType      codecType = 11
	protocolCodecType      codecType = 12
	messageTypeCodecType   codecType = 13
	idCodecType            codecType = 14
	scopeCodecType         codecType = 15
	eventIdCodecType       codecType = 16
	rawMessageCodecType    codecType = 20
	rawBodyCodecType       codecType = 21
	parsedMessageCodecType codecType = 30
	parsedBodyCodecType    codecType = 31
	messageGroupCodecType  codecType = 40
	messageListCodecType   codecType = 41
	groupBatchCodecType    codecType = 50
	groupListCodecType     codecType = 51

	lengthSize length = 4
)

func readType(src []byte) (codecType, int) {
	return codecType(src[0]), 1
}

func readLength(src []byte) (length, int) {
	l, r := readInt(src)
	return l, r
}

func readInt(src []byte) (int, int) {
	l := binary.LittleEndian.Uint32(src)
	return int(l), 4
}

func readLong(src []byte) (int64, int) {
	return int64(binary.LittleEndian.Uint64(src)), 8
}

func readLongValue(src []byte) (int64, int) {
	pos := 0
	t, r := readType(src[pos:])
	pos += r
	if t != longCodecType {
		panic(fmt.Sprintf("unexpected type for long value %d", t))
	}
	l, r := readLength(src[pos:])
	pos += r
	if l != 8 {
		panic(fmt.Sprintf("unexpected length for long %d", l))
	}
	value, r := readLong(src[pos:])
	return value, pos + r
}

func readIntValue(src []byte) (int32, int) {
	pos := 0
	t, r := readType(src[pos:])
	pos += r
	if t != intCodecType {
		panic(fmt.Sprintf("unexpected type for long value %d", t))
	}
	l, r := readLength(src[pos:])
	pos += r
	if l != 4 {
		panic(fmt.Sprintf("unexpected length for long %d", l))
	}
	value, r := readInt(src[pos:])
	return int32(value), pos + r
}

func readIntCollection(src []byte) ([]int32, int) {
	srcLen := len(src)
	if srcLen == 0 {
		return nil, 0
	}
	items := make([]int32, srcLen/(1+4+4))
	pos := 0
	i := 0
	for pos < srcLen {
		var r int
		items[i], r = readIntValue(src[pos:])
		i += 1
		pos += r
	}
	return items, srcLen
}

func readTimestamp(src []byte) (Timestamp, int) {
	sec, offset := readLong(src)
	nanos, nOffset := readInt(src[offset:])
	return Timestamp{seconds: sec, nano: int32(nanos)}, offset + nOffset
}

func readMetadata(src []byte) (Metadata, int) {
	srcLen := len(src)
	if srcLen == 0 {
		return nil, 0
	}
	readString := func(src []byte) (string, int) {
		pos := 0
		t, r := readType(src[pos:])
		pos += r
		if t != stringCodecType {
			panic(fmt.Sprintf("unexpected metadata part type: %d", t))
		}
		l, r := readLength(src[pos:])
		pos += r
		return string(src[pos : pos+l]), pos + l
	}
	meta := make(Metadata)
	pos := 0
	for pos < srcLen {
		key, r := readString(src[pos:])
		pos += r
		value, r := readString(src[pos:])
		pos += r
		meta[key] = value
	}
	return meta, srcLen
}

func writeType(dest []byte, t codecType) int {
	dest[0] = t
	return 1
}

func writeLen(dest []byte, len int) int {
	return writeInt(dest, len)
}

func writeInt(dest []byte, value int) int {
	binary.LittleEndian.PutUint32(dest, uint32(value))
	return 4
}

func writeLong(dest []byte, value int64) int {
	binary.LittleEndian.PutUint64(dest, uint64(value))
	return 8
}

func writeLongValue(dest []byte, t codecType, value int64) int {
	start := 0
	start += writeType(dest[start:], t)
	start += writeLen(dest[start:], 8)
	start += writeLong(dest[start:], value)
	return start
}

func writeIntValue(dest []byte, t codecType, value int32) int {
	start := 0
	start += writeType(dest[start:], t)
	start += writeLen(dest[start:], 4)
	start += writeInt(dest[start:], int(value))
	return start
}

func writeDirection(dest []byte, dir Direction) int {
	start := 0
	start += writeType(dest[start:], directionCodecType)
	start += writeLen(dest[start:], 1)
	dest[start] = byte(dir)
	return start + 1
}

func writeString(dest []byte, t codecType, value string) int {
	start := 0
	start += writeType(dest[start:], t)
	start += writeLen(dest[start:], len(value))
	start += copy(dest[start:], value)
	return start
}

func writeBook(dest []byte, book string) int {
	return writeString(dest, bookCodecType, book)
}

func writeGroup(dest []byte, group string) int {
	return writeString(dest, sessionGroupCodecType, group)
}

func writeProtocol(dest []byte, protocol Protocol) int {
	return writeString(dest, protocolCodecType, protocol)
}

func writeTime(dest []byte, time Timestamp) int {
	start := 0
	start += writeType(dest[start:], timestampCodecType)
	start += writeLen(dest[start:], 8+4)
	start += writeLong(dest[start:], time.seconds)
	start += writeInt(dest[start:], int(time.nano))
	return start
}

func writeIntCollection(dest []byte, t codecType, data []int32) int {
	start := 0
	start += writeType(dest[start:], t)
	lenIndex := start
	start += writeLen(dest[start:], 0)
	for _, v := range data {
		start += writeIntValue(dest[start:], intCodecType, v)
	}
	writeLen(dest[lenIndex:], start-lenIndex-lengthSize)
	return start
}
