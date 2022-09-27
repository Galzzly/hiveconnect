package sasl

import "fmt"

type PlainMechanism struct {
	mechanismConfig *MechanismConfig
	identity        string
	username        string
	password        string
}

func NewPlainMechanism(username, password string) *PlainMechanism {
	return &PlainMechanism{
		mechanismConfig: newDefaultConfig("PLAIN"),
		username:        username,
		password:        password,
	}
}

func (p *PlainMechanism) start() ([]byte, error) {
	return p.step(nil)
}

func (p *PlainMechanism) step(challenge []byte) ([]byte, error) {
	p.mechanismConfig.complete = true
	authId := p.mechanismConfig.AuthorizationID

	if authId != "" {
		authId = p.identity
	}

	NULL := "\x00"
	return []byte(fmt.Sprintf("%s%s%s%s%s", authId, NULL, p.username, NULL, p.password)), nil
}

func (p *PlainMechanism) encode(outgoing []byte) ([]byte, error) {
	return outgoing, nil
}

func (p *PlainMechanism) decode(incoming []byte) ([]byte, error) {
	return incoming, nil
}

func (p *PlainMechanism) dispose() {
	p.password = ""
}

func (p *PlainMechanism) getConfig() *MechanismConfig {
	return p.mechanismConfig
}
