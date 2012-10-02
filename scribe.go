package scribe

import (
	"errors"
	"fmt"
	"io"

	"github.com/samuel/go-thrift"
)

type ResultCode int32

var (
	ResultCodeOk       ResultCode = 0
	ResultCodeTryLater ResultCode = 1
)

func (rc ResultCode) String() string {
	switch rc {
	case ResultCodeOk:
		return "Ok"
	case ResultCodeTryLater:
		return "TryLater"
	}
	return fmt.Sprintf("Unknown(%d)", rc)
}

type LogEntry struct {
	Category string `thrift:"1,required"`
	Message  []byte `thrift:"2,required"`
}

type ScribeLogRequest struct {
	Messages []*LogEntry `thrift:"1,required"`
}

type ScribeLogResponse struct {
	Result ResultCode `thrift:"0,required"`
}

type ScribeService interface {
	Log([]*LogEntry) (ResultCode, error)
}

// LogEntry

func (e *LogEntry) String() string {
	return fmt.Sprintf("%+v", *e)
}

func (e *LogEntry) EncodeThrift(w io.Writer, p thrift.Protocol) error {
	if err := p.WriteStructBegin(w, "logEntry"); err != nil {
		return err
	}
	if err := p.WriteFieldBegin(w, "category", thrift.TypeString, 1); err != nil {
		return err
	}
	if err := p.WriteString(w, e.Category); err != nil {
		return err
	}
	if err := p.WriteFieldEnd(w); err != nil {
		return err
	}
	if err := p.WriteFieldBegin(w, "message", thrift.TypeString, 2); err != nil {
		return err
	}
	if err := p.WriteBytes(w, e.Message); err != nil {
		return err
	}
	if err := p.WriteFieldEnd(w); err != nil {
		return err
	}
	if err := p.WriteFieldStop(w); err != nil {
		return err
	}
	return p.WriteStructEnd(w)
}

// ScribeLogRequest

func (req *ScribeLogRequest) EncodeThrift(w io.Writer, p thrift.Protocol) error {
	if err := p.WriteStructBegin(w, ""); err != nil {
		return err
	}
	if err := p.WriteFieldBegin(w, "", thrift.TypeList, 1); err != nil {
		return err
	}
	if err := p.WriteListBegin(w, thrift.TypeStruct, len(req.Messages)); err != nil {
		return err
	}
	for _, e := range req.Messages {
		e.EncodeThrift(w, p)
	}
	if err := p.WriteListEnd(w); err != nil {
		return err
	}
	if err := p.WriteFieldEnd(w); err != nil {
		return err
	}
	if err := p.WriteFieldStop(w); err != nil {
		return err
	}
	return p.WriteStructEnd(w)
}

// ScribeLogResponse

func (res *ScribeLogResponse) DecodeThrift(r io.Reader, p thrift.Protocol) error {
	if err := p.ReadStructBegin(r); err != nil {
		return err
	}
	ftype, fid, err := p.ReadFieldBegin(r)
	if err != nil {
		return err
	}
	if ftype != thrift.TypeI32 {
		return errors.New("Invalid type")
	}
	if fid != 0 {
		return errors.New("Unknown field id")
	}
	val, err := p.ReadI32(r)
	if err != nil {
		return err
	}
	res.Result = ResultCode(val)
	if err := p.ReadFieldEnd(r); err != nil {
		return err
	}
	ftype, _, err = p.ReadFieldBegin(r)
	if err != nil {
		return err
	}
	if ftype != thrift.TypeStop {
		return errors.New("Invalid type")
	}
	return p.ReadStructEnd(r)
}
