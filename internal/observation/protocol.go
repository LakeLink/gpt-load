// Package observation 提供通用进程外观测订阅。
package observation

import (
	"fmt"

	"github.com/ugorji/go/codec"
)

const Subprotocol = "gpt-load.observation.v1.msgpack"
const ControlReadLimit = 32 << 10

type Command struct {
	Type      string `codec:"type"`
	CommandID string `codec:"command_id"`
}

type Control struct {
	fatal         bool
	Type          string `codec:"type"`
	CommandID     string `codec:"command_id,omitempty"`
	BootID        string `codec:"boot_id,omitempty"`
	MaxEventBytes int    `codec:"max_event_bytes,omitempty"`
	Code          string `codec:"code,omitempty"`
	Message       string `codec:"message,omitempty"`
}

type Event struct {
	Type         string `codec:"type"`
	BootID       string `codec:"boot_id"`
	Seq          uint64 `codec:"seq"`
	ObservedAt   int64  `codec:"observed_at"`
	OriginalType string `codec:"original_type,omitempty"`
	BodyOmitted  bool   `codec:"body_omitted,omitempty"`
	Data         any    `codec:"data"`
}

func encode(value any) ([]byte, error) {
	var data []byte
	err := codec.NewEncoderBytes(&data, &codec.MsgpackHandle{WriteExt: true, PositiveIntUnsigned: true}).Encode(value)
	return data, err
}

func decodeCommand(data []byte) (Command, error) {
	command := Command{}
	if len(data) == 0 || data[0]&0xf0 != 0x80 && data[0] != 0xde && data[0] != 0xdf {
		return command, fmt.Errorf("control requires map")
	}
	handle := &codec.MsgpackHandle{WriteExt: true}
	handle.ErrorIfNoField = true
	decoder := codec.NewDecoderBytes(data, handle)
	if err := decoder.Decode(&command); err != nil {
		return command, err
	}
	if decoder.NumBytesRead() != len(data) {
		return command, fmt.Errorf("control requires one map")
	}
	return command, nil
}
