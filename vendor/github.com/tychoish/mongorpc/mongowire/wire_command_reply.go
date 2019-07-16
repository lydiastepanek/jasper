package mongowire

import (
	"github.com/k0kubun/pp"
	"github.com/pkg/errors"
	"github.com/tychoish/mongorpc/bson"
)

func (m *CommandReplyMessage) HasResponse() bool     { return false }
func (m *CommandReplyMessage) Header() MessageHeader { return m.CommandReplyHeader }
func (m *CommandReplyMessage) Scope() *OpScope       { return nil }

func (m *CommandReplyMessage) Serialize() []byte {
	size := 16 /* header */
	size += int(m.CommandReply.Size)
	size += int(m.Metadata.Size)
	for _, d := range m.OutputDocs {
		size += int(d.Size)
	}
	m.CommandReplyHeader.Size = int32(size)
	pp.Print(m.CommandReplyHeader.Size)

	buf := make([]byte, size)
	m.CommandReplyHeader.WriteInto(buf)

	loc := 16

	m.CommandReply.Copy(&loc, buf)
	m.Metadata.Copy(&loc, buf)

	for _, d := range m.OutputDocs {
		d.Copy(&loc, buf)
	}

	pp.Print(m)
	return buf
}

func (h *MessageHeader) parseCommandReplyMessage(buf []byte) (Message, error) {
	rm := &CommandReplyMessage{
		CommandReplyHeader: *h,
	}

	var err error

	rm.CommandReply, err = bson.ParseSimple(buf)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	if len(buf) < int(rm.CommandReply.Size) {
		return nil, errors.New("invalid command message -- message length is too short")
	}
	buf = buf[rm.CommandReply.Size:]

	rm.Metadata, err = bson.ParseSimple(buf)
	if err != nil {
		return nil, err
	}
	if len(buf) < int(rm.Metadata.Size) {
		return nil, errors.New("invalid command message -- message length is too short")
	}
	buf = buf[rm.Metadata.Size:]

	for len(buf) > 0 {
		doc, err := bson.ParseSimple(buf)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		buf = buf[doc.Size:]
		rm.OutputDocs = append(rm.OutputDocs, doc)
	}

	return rm, nil
}
