package hiveserver

import (
	"context"
	"fmt"

	"github.com/apache/thrift/lib/go/thrift"
)

type TOpenSessionReq struct {
	ClientProtocol TProtocolVersion  `thrift:"client_protocol,1,required" db:"client_protocol" json:"client_protocol"`
	Username       *string           `thrift:"username,2" db:"username" json:"username,omitempty"`
	Password       *string           `thrift:"password,3" db:"password" json:"password,omitempty"`
	Configuration  map[string]string `thrift:"configuration,4" db:"configuration" json:"configuration,omitempty"`
}

type TOpenSessionResp struct {
	Status                *TStatus          `thrift:"status,1,required" db:"status" json:"status"`
	ServerProtocolVersion TProtocolVersion  `thrift:"serverProtocolVersion,2,required" db:"serverProtocolVersion" json:"serverProtocolVersion"`
	SessionHandle         *TSessionHandle   `thrift:"sessionHandle,3" db:"sessionHandle" json:"sessionHandle,omitempty"`
	Configuration         map[string]string `thrift:"configuration,4" db:"configuration" json:"configuration,omitempty"`
}

type TSessionHandle struct {
	SessionId *THandleIdentifier `thrift:"sessionId,1,required" db:"sessionId" json:"sessionId"`
}

type TCLIServiceOpenSessionArgs struct {
	Req *TOpenSessionReq `thrift:"req,1" db:"req" json:"req"`
}

type TCLIServiceOpenSessionResult struct {
	Success *TOpenSessionResp `thrift:"success,0" db:"success" json:"success,omitempty"`
}

func NewTOpenSessionReq() *TOpenSessionReq {
	return &TOpenSessionReq{
		ClientProtocol: 9,
	}
}

func (r *TOpenSessionReq) Read(ctx context.Context, p thrift.TProtocol) error {
	if _, err := p.ReadStructBegin(ctx); err != nil {
		return thrift.PrependError(fmt.Sprintf("%T read error: ", r), err)
	}

	var clientProtoSet = false

	for {
		_, fTypeId, fId, err := p.ReadFieldBegin(ctx)
		if err != nil {
			return thrift.PrependError(fmt.Sprintf("%T field %d read eror: ", r), err)
		}

		if fTypeId == thrift.STOP {
			break
		}
		switch fId {
		case 1:
			if fTypeId == thrift.I32 {
				if r.readField1(ctx, p); err != nil {
					return err
				}
				clientProtoSet = true
			} else {
				if err := p.Skip(ctx, fTypeId); err != nil {
					return err
				}
			}
		case 2:
			if fTypeId == thrift.STRING {
				if err := r.readField2(ctx, p); err != nil {
					return err
				}
			} else {
				if err := p.Skip(ctx, fTypeId); err != nil {
					return err
				}
			}
		case 3:
			if fTypeId == thrift.STRING {
				if err := r.readField3(ctx, p); err != nil {
					return err
				}
			} else {
				if err := p.Skip(ctx, fTypeId); err != nil {
					return err
				}
			}
		case 4:
			if fTypeId == thrift.MAP {
				if err := r.readField4(ctx, p); err != nil {
					return err
				}
			} else {
				if err := p.Skip(ctx, fTypeId); err != nil {
					return err
				}
			}
		default:
			if err := p.Skip(ctx, fTypeId); err != nil {
				return err
			}
		}
		if err := p.ReadFieldEnd(ctx); err != nil {
			return err
		}
	}
	if err := p.ReadStructEnd(ctx); err != nil {
		return err
	}
	if !clientProtoSet {
		return thrift.NewTProtocolExceptionWithType(thrift.INVALID_DATA, fmt.Errorf("Required field ClientProtocol is not set"))
	}
	return nil
}

func (r *TOpenSessionReq) readField1(ctx context.Context, p thrift.TProtocol) error {
	v, err := p.ReadI32(ctx)
	if err != nil {
		return thrift.PrependError("error reading field1: ", err)
	}

	r.ClientProtocol = TProtocolVersion(v)
	return nil
}

func (r *TOpenSessionReq) readField2(ctx context.Context, p thrift.TProtocol) error {
	v, err := p.ReadString(ctx)
	if err != nil {
		return thrift.PrependError("error reading field2: ", err)
	}
	r.Username = &v
	return nil
}

func (r *TOpenSessionReq) readField3(ctx context.Context, p thrift.TProtocol) error {
	v, err := p.ReadString(ctx)
	if err != nil {
		return thrift.PrependError("error reading field3: ", err)
	}
	r.Password = &v
	return nil
}

func (r *TOpenSessionReq) readField4(ctx context.Context, p thrift.TProtocol) error {
	_, _, size, err := p.ReadMapBegin(ctx)
	if err != nil {
		return thrift.PrependError("error reading map begin: ", err)
	}
	cMap := make(map[string]string, size)
	r.Configuration = cMap

	for i := 0; i < size; i++ {
		v, err := p.ReadString(ctx)
		if err != nil {
			return thrift.PrependError("error reading field0 ;", err)
		}

		r.Configuration[v] = v
	}
	if err := p.ReadMapEnd(ctx); err != nil {
		return thrift.PrependError("error reading map end: ", err)
	}
	return nil
}

func (r *TOpenSessionReq) Write(ctx context.Context, p thrift.TProtocol) error {
	if err := p.WriteStructBegin(ctx, "TOpenSessionReq"); err != nil {
		return thrift.PrependError(fmt.Sprintf("%T write struct begin error: ", r), err)
	}
	if r != nil {
		if err := r.writeField1(ctx, p); err != nil {
			return err
		}
		if err := r.writeField2(ctx, p); err != nil {
			return err
		}
		if err := r.writeField3(ctx, p); err != nil {
			return err
		}
		if err := r.writeField4(ctx, p); err != nil {
			return err
		}
	}
	if err := p.WriteFieldStop(ctx); err != nil {
		return thrift.PrependError("write field stop error: ", err)
	}
	if err := p.WriteStructEnd(ctx); err != nil {
		return thrift.PrependError("write struct end error: ", err)
	}

	return nil
}

func (r *TOpenSessionReq) writeField1(ctx context.Context, p thrift.TProtocol) error {

	return nil
}

func (r *TOpenSessionReq) writeField2(ctx context.Context, p thrift.TProtocol) error {

	return nil
}

func (r *TOpenSessionReq) writeField3(ctx context.Context, p thrift.TProtocol) error {

	return nil
}

func (r *TOpenSessionReq) writeField4(ctx context.Context, p thrift.TProtocol) error {

	return nil
}

func (a *TCLIServiceOpenSessionArgs) Read(ctx context.Context, p thrift.TProtocol) error {
	if _, err := p.ReadStructBegin(ctx); err != nil {
		return thrift.PrependError(fmt.Sprintf("%T read error: ", a), err)
	}

	for {
		_, fTypeId, fId, err := p.ReadFieldBegin(ctx)
		if err != nil {
			return thrift.PrependError(fmt.Sprintf("%T field %d read error: ", a), err)
		}

		if fTypeId == thrift.STOP {
			break
		}

		switch fId {
		case 1:
			if fTypeId == thrift.STRUCT {
				if err := a.readField(ctx, p); err != nil {
					return err
				}
			} else {
				if err := p.Skip(ctx, fTypeId); err != nil {
					return err
				}
			}
		default:
			if err := p.Skip(ctx, fTypeId); err != nil {
				return err
			}
		}
		if err := p.ReadFieldEnd(ctx); err != nil {
			return err
		}
	}

	if err := p.ReadStructEnd(ctx); err != nil {
		return thrift.PrependError(fmt.Sprintf("%T read struct end error: ", a), err)
	}

	return nil
}

func (a *TCLIServiceOpenSessionArgs) Write(ctx context.Context, p thrift.TProtocol) error {
	if err := p.WriteStructBegin(ctx, "OpenSession_args"); err != nil {
		return thrift.PrependError(fmt.Sprintf("%T write struct begin error: ", a), err)
	}

	if a != nil {
		if err := a.writeField(ctx, p); err != nil {
			return err
		}
	}
	if err := p.WriteFieldStop(ctx); err != nil {
		return thrift.PrependError(fmt.Sprintf("%T write field stop error: ", a), err)
	}
	if err := p.WriteStructEnd(ctx); err != nil {
		return thrift.PrependError(fmt.Sprintf("%T write struct end error: ", a), err)
	}

	return nil
}

func (a *TCLIServiceOpenSessionArgs) readField(ctx context.Context, p thrift.TProtocol) error {
	a.Req = &TOpenSessionReq{
		ClientProtocol: 9,
	}
	if err := a.Req.Read(ctx, p); err != nil {
		return thrift.PrependError(fmt.Sprintf("%T error reading struct: ", a), err)
	}
	return nil
}

func (a *TCLIServiceOpenSessionArgs) writeField(ctx context.Context, p thrift.TProtocol) error {
	if err := p.WriteFieldBegin(ctx, "req", thrift.STRUCT, 1); err != nil {
		return thrift.PrependError(fmt.Sprintf("%T write field begin error: req: ", a), err)
	}

	if err := a.Req.Write(ctx, p); err != nil {
		return thrift.PrependError(fmt.Sprintf("%T error writing struct: ", a), err)
	}
	if err := p.WriteFieldEnd(ctx); err != nil {
		return thrift.PrependError(fmt.Sprintf("%T write field end error: req: ", a), err)
	}
	return nil
}
