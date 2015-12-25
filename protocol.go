package cassandra

import (
	"bytes"
	"encoding/binary"
	"log"
)

type Frame struct {
	Version byte
	Flags   byte
	Stream  uint16
	Opcode  byte
	Length  uint32
}

type String struct {
	Length uint16
	Bytes  []byte
}

func NewString(val string) String {
	s := String{}
	s.Bytes = []byte(val)
	s.Length = uint16(len(s.Bytes))
	return s
}

type StringPair struct {
	Key   String
	Value String
}

func (sp *StringPair) AddPair(key string, value string) {
	sp.Key = NewString(key)
	sp.Value = NewString(value)
}

func NewStringPair(key string, value string) StringPair {
	sp := StringPair{}
	sp.AddPair(key, value)
	return sp
}

type StringMap struct {
	Num         uint16
	StringPairs []StringPair
}

func (sm *StringMap) AddStringPair(sp StringPair) {
	sm.StringPairs = append(sm.StringPairs, sp)
	sm.Num++
}

func (sm *StringMap) MarshalBinary() ([]byte, error) {
	log.Printf("In MarshalBinary, num=%d", sm.Num)
	buf := new(bytes.Buffer)
	err := binary.Write(buf, binary.BigEndian, sm.Num)
	if err != nil {
		log.Printf("Could not write num=%s", err)
		return nil, err
	}

	for _, pair := range sm.StringPairs {
		log.Printf("string pair %v", pair)
		binary.Write(buf, binary.BigEndian, pair.Key.Length)
		binary.Write(buf, binary.BigEndian, pair.Key.Bytes)

		binary.Write(buf, binary.BigEndian, pair.Value.Length)
		binary.Write(buf, binary.BigEndian, pair.Value.Bytes)
	}

	return buf.Bytes(), nil
}

func (sm *StringMap) UnmarshalBinary(data []byte) error {
	log.Printf("In Unmarshal")
	return nil
}
