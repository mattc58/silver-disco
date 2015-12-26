package cassandra

import (
	"bytes"
	"encoding/binary"
	"log"

	"net"
)

type Session struct {
	ProtocolVersion int
	Timeout         int
}
type SessionOption func(*Session)

func SessionTimeout(timeout int) SessionOption {
	return func(session *Session) {
		session.Timeout = timeout
	}
}

type QueryResult struct {
}

type QueryResults []QueryResult

func (session *Session) connect() error {
	frame, err := NewStartupFrame(0x04, 0x0, 0x01, NewStringPair("CQL_VERSION", "3.0.0"))
	if err != nil {
		log.Printf("Could not create startup frame:", err)
		return err
	}
	conn, err := net.Dial("tcp", "127.0.0.1:9042")
	if err != nil {
		log.Printf("Error connecting:%s", err)
		return err
	}

	frameBytes, err := frame.MarshalBinary()
	if err != nil {
		log.Printf("Could not serialize to binary:%s", err)
		return err
	}

	log.Printf("Sending frame %v", frameBytes)
	num, err := conn.Write(frameBytes)
	if err != nil {
		log.Printf("Could not write to socket:%s", err)
		return err
	}
	log.Printf("Wrote %d bytes", num)
	if num != len(frameBytes) {
		log.Printf("Error, only wrote %d of %d bytes", num, len(frameBytes))
		return err
	}

	b := make([]byte, 9)
	num, err = conn.Read(b)
	if err != nil {
		log.Printf("Error from read:%s", err)
		return err
	}
	log.Printf("Got back:%d bytes", num)

	resp := FrameHeader{}
	err = binary.Read(bytes.NewReader(b), binary.BigEndian, &resp)
	if err != nil {
		log.Printf("Error reading bytes into frame:%s", err)
		return nil
	}
	log.Printf("opcode = %x, %d", resp.Opcode, resp.Opcode)
	log.Printf("response=%v", resp)
	log.Printf("response length=%d", resp.Length)

	if resp.Length > 0 {
		b = make([]byte, resp.Length)
		num, err = conn.Read(b)
		if err != nil {
			log.Printf("Error from read:%s", err)
			return err
		}
		log.Printf("Body got back:%d bytes, %s", num, b)
	}

	err = conn.Close()
	if err != nil {
		log.Printf("Could not close connection:%s", err)
		return nil
	}

	return nil
}

func (session *Session) Execute(query string, options ...SessionOption) (QueryResults, error) {
	return nil, nil
}
