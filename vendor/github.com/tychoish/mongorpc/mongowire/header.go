package mongowire

import (
	"github.com/k0kubun/pp"
	"github.com/pkg/errors"
)

type MessageHeader struct {
	Size       int32 // total message size
	RequestID  int32
	ResponseTo int32
	OpCode     OpType
}

func (h *MessageHeader) WriteInto(buf []byte) {
	writeInt32(h.Size, buf, 0)
	writeInt32(h.RequestID, buf, 4)
	writeInt32(h.ResponseTo, buf, 8)
	writeInt32(int32(h.OpCode), buf, 12)
}

func (h *MessageHeader) Parse(body []byte) (Message, error) {
	var (
		m   Message
		err error
	)

	switch h.OpCode {
	case OP_REPLY:
		m, err = h.parseReplyMessage(body)
		pp.Print("OP_REPLY")
		pp.Print(m)
	case OP_UPDATE:
		m, err = h.parseUpdateMessage(body)
	case OP_INSERT:
		m, err = h.parseInsertMessage(body)
	case OP_QUERY:
		pp.Print("OP_QUERY")
		pp.Print(m)
		m, err = h.parseQueryMessage(body)
	case OP_GET_MORE:
		m, err = h.parseGetMoreMessage(body)
	case OP_DELETE:
		m, err = h.parseDeleteMessage(body)
	case OP_KILL_CURSORS:
		m, err = h.parseKillCursorsMessage(body)
	case OP_COMMAND:
		pp.Print("OP_COMMAND")
		pp.Print(m)
		m, err = h.parseCommandMessage(body)
	case OP_COMMAND_REPLY:
		m, err = h.parseCommandReplyMessage(body)
	default:
		return nil, errors.Errorf("unknown op code: %s", h.OpCode)
	}

	return m, errors.WithStack(err)
}
