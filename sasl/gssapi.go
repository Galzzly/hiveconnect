package sasl

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"log"
	"os"

	gssapi "github.com/Galzzly/gssapi"
)

type GSSAPIMechanism struct {
	config           *MechanismConfig
	host             string
	user             string
	service          string
	negotiationStage int
	context          *GSSAPIContext
	qop              byte
	supportedQop     uint8
	serverMaxLength  int
	UserSelectQop    uint8
	MaxLength        int
}

type GSSAPIContext struct {
	DebugLog       bool
	RunAsService   bool
	ServiceName    string
	ServiceAddress string

	gssapi.Options

	*gssapi.Lib `json:"-"`
	// loadonce    sync.Once

	// credential     *gssapi.CredId
	token []byte
	// continueNeeded bool
	contextId  *gssapi.CtxId
	reqFlags   uint32
	availFlags uint32
}

func NewGSSAPIMechanism(service string) *GSSAPIMechanism {
	context := newGSSAPIContext()
	return &GSSAPIMechanism{
		config:           newDefaultConfig("GSSAPI"),
		service:          service,
		negotiationStage: 0,
		context:          context,
		supportedQop:     QOP_TO_FLAG[AUTH] | QOP_TO_FLAG[AUTH_CONF] | QOP_TO_FLAG[AUTH_INT],
		MaxLength:        DEFAULT_MAX_LENGTH,
		UserSelectQop:    QOP_TO_FLAG[AUTH] | QOP_TO_FLAG[AUTH_INT] | QOP_TO_FLAG[AUTH_CONF],
	}
}

func newGSSAPIContext() *GSSAPIContext {
	var context = &GSSAPIContext{
		reqFlags: uint32(gssapi.GSS_C_INTEG_FLAG) + uint32(gssapi.GSS_C_MUTUAL_FLAG) +
			uint32(gssapi.GSS_C_SEQUENCE_FLAG) + uint32(gssapi.GSS_C_CONF_FLAG),
	}

	prefix := "sasl-client"
	err := loadlib(context.DebugLog, prefix, context)
	if err != nil {
		log.Fatal(err)
	}

	j, _ := json.MarshalIndent(context, "", " ")
	context.Debug(fmt.Sprintf("Config: %s", string(j)))
	return context
}

func (m *GSSAPIMechanism) start() ([]byte, error) {
	return m.step(nil)
}

func (m *GSSAPIMechanism) step(challenge []byte) ([]byte, error) {
	var serviceHostQualified, fullServiceName string

	serviceHostQualified = os.Getenv("SERVICE_HOST_QUALIFIED")
	fullServiceName = m.service + "/" + serviceHostQualified
	if len(serviceHostQualified) == 0 {
		fullServiceName += m.host
	}

	switch {
	case m.negotiationStage == 0:
		err := initClientContext(m.context, fullServiceName, nil)
		m.negotiationStage = 1
		return m.context.token, err
	case m.negotiationStage == 1:
		err := initClientContext(m.context, fullServiceName, challenge)
		if err != nil {
			log.Fatal(err)
			return nil, err
		}

		if m.user == "" {
			return m.context.token, nil
		}

		if !m.context.integAvail() && !m.context.confAvail() {
			log.Println("Unable to establish a security layer, however authentication is still possible.")
		}
		m.negotiationStage = 2
		return m.context.token, nil
	case m.negotiationStage == 2:
		data, err := m.context.unwrap(challenge)
		if err != nil {
			return nil, err
		}
		if len(data) != 4 {
			return nil, fmt.Errorf("the decoded data should have length at this stage")
		}
		qopBits := data[0]
		data[0] = 0
		m.serverMaxLength = int(binary.BigEndian.Uint32(data))

		m.qop, err = m.selectQop(qopBits)
		if err != nil {
			m.MaxLength = 0
		}

		header := make([]byte, 4)
		maxLength := m.serverMaxLength
		if m.MaxLength < m.serverMaxLength {
			maxLength = m.MaxLength
		}

		headerInt := (uint(m.qop) << 24) | uint(maxLength)

		binary.BigEndian.PutUint32(header, uint32(headerInt))

		var name string
		if name = m.user; m.config.AuthorizationID != "" {
			name = m.config.AuthorizationID
		}
		out := append(header, []byte(name)...)
		wrappedOut, err := m.context.wrap(out, false)

		m.config.complete = true
		return wrappedOut, err
	}
	return nil, fmt.Errorf("error, should not get to this point")
}

func (m *GSSAPIMechanism) encode(outgoing []byte) ([]byte, error) {
	if m.qop == QOP_TO_FLAG[AUTH] {
		return outgoing, nil
	}
	var conf_flag bool = false
	if m.qop == QOP_TO_FLAG[AUTH_CONF] {
		conf_flag = true
	}
	return m.context.wrap(deepCopy(outgoing), conf_flag)
}

func (m *GSSAPIMechanism) decode(incoming []byte) ([]byte, error) {
	if m.qop == QOP_TO_FLAG[AUTH] {
		return incoming, nil
	}
	return m.context.unwrap(deepCopy(incoming))
}

func (m *GSSAPIMechanism) dispose() {
	m.context.dispose()
}

func (m *GSSAPIMechanism) selectQop(qopByte byte) (byte, error) {
	availableQops := m.UserSelectQop & m.supportedQop & qopByte
	for _, qop := range []byte{QOP_TO_FLAG[AUTH_CONF], QOP_TO_FLAG[AUTH_INT], QOP_TO_FLAG[AUTH]} {
		if qop&availableQops != 0 {
			return qop, nil
		}
	}

	return byte(0), fmt.Errorf("no qop available")
}

func (m *GSSAPIMechanism) getConfig() *MechanismConfig {
	return m.config
}

func (c *GSSAPIContext) wrap(original []byte, conf_flag bool) (wrapped []byte, err error) {
	if original == nil {
		return
	}

	_orig, err := c.MakeBufferBytes(original)
	defer _orig.Release()
	if err != nil {
		return nil, err
	}

	_, buf, err := c.contextId.Wrap(conf_flag, gssapi.GSS_C_QOP_DEFAULT, _orig)
	defer buf.Release()
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (c *GSSAPIContext) unwrap(original []byte) (unrwapped []byte, err error) {
	if original == nil {
		return
	}

	_orig, err := c.MakeBufferBytes(original)
	defer _orig.Release()
	if err != nil {
		return nil, err
	}

	buf, _, _, err := c.contextId.Unwrap(_orig)
	defer buf.Release()
	if err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

func (c *GSSAPIContext) dispose() error {
	if c.contextId != nil {
		return c.contextId.Unload()
	}
	return nil
}

func (c *GSSAPIContext) integAvail() bool {
	return c.availFlags&uint32(gssapi.GSS_C_INTEG_FLAG) != 0
}

func (c *GSSAPIContext) confAvail() bool {
	return c.availFlags&uint32(gssapi.GSS_C_CONF_FLAG) != 0
}

func deepCopy(original []byte) []byte {
	copied := make([]byte, len(original))
	// for i, c := range original {
	// 	copied[i] = c
	// }
	copy(copied, original)
	return copied
}

func initClientContext(context *GSSAPIContext, service string, intoken []byte) error {
	context.ServiceName = service

	var _token *gssapi.Buffer
	var err error
	_token = context.GSS_C_NO_BUFFER
	if intoken != nil {
		_token, err = context.MakeBufferBytes(intoken)
		defer _token.Release()
		if err != nil {
			return err
		}
	}

	prepName := prepareServiceName(context)
	defer prepName.Release()

	contextId, _, token, outFlags, _, err := context.InitSecContext(
		nil,
		context.contextId,
		prepName,
		context.GSS_MECH_KRB5,
		context.reqFlags,
		0,
		context.GSS_C_NO_CHANNEL_BINDINGS,
		_token)
	defer token.Release()
	if err != nil {
		log.Fatal(err)
	}

	context.token = token.Bytes()
	context.contextId = contextId
	context.availFlags = outFlags

	return nil
}

func prepareServiceName(context *GSSAPIContext) *gssapi.Name {
	if context.ServiceName == "" {
		log.Fatal("Need a service name to be provided")
	}

	nameBuf, err := context.MakeBufferString(context.ServiceName)
	defer nameBuf.Release()
	if err != nil {
		log.Fatal(err)
	}

	name, err := nameBuf.Name(context.GSS_KRB5_NT_PRINCIPAL_NAME)
	if err != nil {
		log.Fatal(err)
	}

	if name.String() != context.ServiceName {
		log.Fatalf("name: got %q, expected %q", name.String(), context.ServiceName)
	}

	return name
}
