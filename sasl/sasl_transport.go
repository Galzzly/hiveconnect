package sasl

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"

	"github.com/apache/thrift/lib/go/thrift"
	"github.com/pkg/errors"
)

const (
	START    = 1
	OK       = 2
	BAD      = 3
	ERROR    = 4
	COMPLETE = 5
)

type TSaslTransport struct {
	// service        string
	saslClient *Client
	tp         thrift.TTransport
	// tpFramed       thrift.TFramedTransport
	mechanism      string
	writeBuf       bytes.Buffer
	readBuf        bytes.Buffer
	buffer         [4]byte
	rawFrameSize   uint32
	frameSize      int
	maxLength      uint32
	principal      string
	OpeningContext context.Context
}

func NewTSaslTransport(trans thrift.TTransport, host string, mechanismName string,
	configuration map[string]string, maxLength uint32) (transport *TSaslTransport) {
	var mechanism Mechanism
	switch mechanismName {
	case "PLAIN":
		mechanism = NewPlainMechanism(configuration["username"], configuration["password"])
	case "GSSAPI":
		mechanism = NewGSSAPIMechanism(configuration["service"])
	case "DIGEST-MD5":
		mechanism = NewDigestMD5Mechanism(configuration["service"], configuration["username"], configuration["password"])
	default:
		panic("Mechanism not supported")
	}
	client := NewSaslClient(host, mechanism)
	transport = &TSaslTransport{
		saslClient:     client,
		tp:             trans,
		mechanism:      mechanismName,
		maxLength:      maxLength,
		principal:      configuration["principal"],
		OpeningContext: context.Background(),
	}

	return
}

func (t *TSaslTransport) IsOpen() bool {
	return t.tp.IsOpen() && t.saslClient.Complete()
}

func (t *TSaslTransport) Open() (err error) {
	if !t.tp.IsOpen() {
		if err = t.tp.Open(); err != nil {
			return
		}
	}
	if err = t.sendSaslMsg(t.OpeningContext, START, []byte(t.mechanism)); err != nil {
		return
	}

	processed, err := t.saslClient.Start()
	if err != nil {
		return
	}

	if err = t.sendSaslMsg(t.OpeningContext, OK, processed); err != nil {
		return
	}

l:
	for {
		status, challenge := t.recvSaslMsg(t.OpeningContext)
		switch status {
		case OK:
			processed, err = t.saslClient.Step(challenge)
			if err != nil {
				return
			}
			t.sendSaslMsg(t.OpeningContext, OK, processed)
		case COMPLETE:
			if !t.saslClient.Complete() {
				return thrift.NewTTransportException(thrift.NOT_OPEN, "Server erroneous responded SASL negotiation was complete")
			}
			break l
		default:
			return thrift.NewTTransportExceptionFromError((errors.Errorf("Bad SASL negotiation status: %d (%s)", status, challenge)))
		}
	}
	return
}

func (t *TSaslTransport) Close() error {
	t.saslClient.Dispose()
	return t.tp.Close()
}

func (t *TSaslTransport) Flush(ctx context.Context) error {
	wrappedBuf, err := t.saslClient.Encode(t.writeBuf.Bytes())
	if err != nil {
		return thrift.NewTTransportExceptionFromError(err)
	}

	t.writeBuf.Reset()

	size := len(wrappedBuf)
	buf := t.buffer[:4]
	binary.BigEndian.PutUint32(buf, uint32(size))
	if _, err = t.tp.Write(buf); err != nil {
		return thrift.NewTTransportExceptionFromError(err)
	}

	if size > 0 {
		if _, err := t.tp.Write(wrappedBuf); err != nil {
			return thrift.NewTTransportExceptionFromError(err)
		}
	}

	err = t.tp.Flush(ctx)
	return thrift.NewTTransportExceptionFromError(err)
}

func (t *TSaslTransport) Read(p []byte) (n int, err error) {
	if t.rawFrameSize == 0 && t.frameSize == 0 {
		t.rawFrameSize, err = t.readFrameHeader()
		if err != nil {
			return
		}
	}

	var got int
	if t.rawFrameSize > 0 {
		rawBuf := make([]byte, t.rawFrameSize)
		got, err = io.ReadFull(t.tp, rawBuf)
		if err != nil {
			return
		}
		t.rawFrameSize = t.rawFrameSize - uint32(got)

		var unwrappedBuf []byte
		unwrappedBuf, err = t.saslClient.Decode(rawBuf)
		if err != nil {
			return
		}
		t.frameSize = len(unwrappedBuf)
		t.readBuf.Write(unwrappedBuf)
	}

	got, err = t.readBuf.Read(p)
	t.frameSize = t.frameSize - got

	if t.frameSize < 0 {
		return 0, thrift.NewTTransportException(thrift.UNKNOWN_TRANSPORT_EXCEPTION, "Negative frame size")
	}
	return got, thrift.NewTTransportExceptionFromError(err)
}

func (t *TSaslTransport) RemainingBytes() uint64 {
	return uint64(t.frameSize)
}

func (t *TSaslTransport) Write(p []byte) (n int, err error) {
	n, err = t.writeBuf.Write(p)
	return n, thrift.NewTTransportExceptionFromError(err)
}

func (t *TSaslTransport) sendSaslMsg(ctx context.Context, stat uint8, msg []byte) (err error) {
	h := make([]byte, 5)
	h[0] = stat
	l := uint32(len(msg))
	binary.BigEndian.PutUint32(h[1:], l)

	if _, err = t.tp.Write(append(h[:], msg[:]...)); err != nil {
		return
	}

	if err = t.tp.Flush(ctx); err != nil {
		return
	}

	return
}

func (t *TSaslTransport) recvSaslMsg(ctx context.Context) (s int8, b []byte) {
	h := make([]byte, 5)
	if _, err := io.ReadFull(t.tp, h); err != nil {
		return ERROR, nil
	}

	s = int8(h[0])
	l := binary.BigEndian.Uint32(h[1:])

	if l > 0 {
		p := make([]byte, l)
		if _, err := io.ReadFull(t.tp, p); err != nil {
			return ERROR, nil
		}
		return s, p
	}
	return s, nil
}

func (t *TSaslTransport) readFrameHeader() (size uint32, err error) {
	buf := t.buffer[:4]
	if _, err := io.ReadFull(t.tp, buf); err != nil {
		return 0, err
	}
	size = binary.BigEndian.Uint32(buf)
	if size < 0 {
		return 0, thrift.NewTTransportException(thrift.UNKNOWN_TRANSPORT_EXCEPTION, fmt.Sprintf("Incorrect frame size (%d)", size))
	}
	if size > t.maxLength {
		return 0, thrift.NewTTransportException(thrift.UNKNOWN_TRANSPORT_EXCEPTION, fmt.Sprintf("Frame size is bigger than allowed, set configuration.MaxLength (%d)", size))
	}
	return size, nil
}
