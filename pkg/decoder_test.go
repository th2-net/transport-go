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
	"encoding/base64"
	"github.com/stretchr/testify/assert"
	"github.com/th2-net/transport-go/pkg"

	"testing"
)

func TestDecodesRawMessage(t *testing.T) {
	data := "MnYAAABlBAAAAGJvb2tmBQAAAGdyb3VwM14AAAAoWQAAAClUAAAAFE8AAAAKMwAAAGcFAAAAYWxpYXNoAQAAAAFpCAAAACoAAAAAAAAAagAAAABrDAAAAPqHR2QAAAAAIH5bDwsAAAAADAQAAAB0ZXN0FQQAAABkYXRh"
	batch, err := base64.StdEncoding.DecodeString(data)
	if err != nil {
		t.Fatal(err)
	}
	d := transport.NewDecoder(batch)
	msg, _, err := d.NextMessage()
	if err != nil {
		t.Fatal(err)
	}
	if assert.IsType(t, &transport.RawMessage{}, msg) {
		rawMsg := msg.(*transport.RawMessage)
		assert.Equal(t, []byte("data"), rawMsg.Body, "unexpected body")
		assert.Equal(t, "alias", rawMsg.MessageId.SessionAlias, "unexpected alias")
		assert.Equal(t, "test", rawMsg.Protocol, "unexpected protocol")
		assert.Equal(t, int64(42), rawMsg.MessageId.Sequence, "unexpected sequence")
	}

	msg, _, _ = d.NextMessage()
	if assert.Nil(t, msg, "not the last message") {
		assert.Equal(t, "book", d.GetBook(), "unexpected book")
		assert.Equal(t, "group", d.GetGroup(), "unexpected group")
	}
}