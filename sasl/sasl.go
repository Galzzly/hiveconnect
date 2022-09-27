package sasl

import (
	"fmt"
	"log"
	"os"

	gssapi "github.com/Galzzly/gssapi"
)

const DEFAULT_MAX_LENGTH = 16384000

const (
	GSS_C_MANUAL_FLAG   uint32 = 2
	GSS_C_SEQUENCE_FLAG uint32 = 8
	GSS_C_CONF_FLAG     uint32 = 16
	GSS_C_INTEG_FLAG    uint32 = 32
)

var (
	AUTH      = "auth"
	AUTH_INT  = "auth-int"
	AUTH_CONF = "auth-conf"
)

var QOP_TO_FLAG = map[string]byte{
	AUTH:      1,
	AUTH_INT:  2,
	AUTH_CONF: 4,
}

type MechanismConfig struct {
	name               string
	score              int
	complete           bool
	hasInitialresponse bool
	allowsAnonymous    bool
	usesPlaintext      bool
	activeSafe         bool
	disctionarySafe    bool
	qop                []byte
	AuthorizationID    string
}

type Mechanism interface {
	start() ([]byte, error)
	step(challenge []byte) ([]byte, error)
	encode(outgoing []byte) ([]byte, error)
	decode(incoming []byte) ([]byte, error)
	dispose()
	getConfig() *MechanismConfig
}

func newDefaultConfig(name string) *MechanismConfig {
	return &MechanismConfig{
		name:               name,
		score:              0,
		complete:           false,
		hasInitialresponse: false,
		allowsAnonymous:    true,
		usesPlaintext:      true,
		activeSafe:         false,
		disctionarySafe:    false,
		qop:                nil,
		AuthorizationID:    "",
	}
}

func loadlib(debug bool, prefix string, context *GSSAPIContext) error {
	max := gssapi.Err + 1
	if debug {
		max = gssapi.MaxSeverity
	}
	pp := make([]gssapi.Printer, 0, max)
	for i := gssapi.Severity(0); i < max; i++ {
		p := log.New(os.Stderr,
			fmt.Sprintf("%s: %s\t", prefix, i),
			log.LstdFlags)
		pp = append(pp, p)
	}
	context.Options.Printers = pp

	lib, err := gssapi.Load(&context.Options)
	if err != nil {
		return err
	}
	context.Lib = lib
	return nil
}
