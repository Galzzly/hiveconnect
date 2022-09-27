package sasl

import (
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"math/rand"
	"strconv"
	"strings"
)

var randSeqChars = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")

type DigestMD5Mechanism struct {
	mechanismConfig *MechanismConfig
	service         string
	// identity        string
	username   string
	password   string
	host       string
	nonceCount int
	cnonce     string
	nonce      string
	keyHash    string
	auth       string
}

func NewDigestMD5Mechanism(service, username, password string) *DigestMD5Mechanism {
	return &DigestMD5Mechanism{
		mechanismConfig: newDefaultConfig("DIGEST-MD5"),
		service:         service,
		username:        username,
		password:        password,
	}
}

func (m *DigestMD5Mechanism) start() ([]byte, error) {
	return m.step(nil)
}

func (m *DigestMD5Mechanism) step(challenge []byte) ([]byte, error) {
	if challenge == nil {
		return nil, nil
	}

	c := parseChallenge(challenge)
	digestUri := m.service + "/" + m.host

	if _, ok := c["rspauth"]; ok {
		m.mechanismConfig.complete = true
		return nil, m.authenticate(digestUri, c)
	}

	m.nonce = c["nonce"]
	m.auth = c["qop"]
	if m.nonceCount == 0 {
		m.cnonce = randSeq(14)
	}
	m.nonceCount++

	a2String := "AUTHENTICATE:" + digestUri

	maxBuf := ""
	if m.auth != AUTH {
		a2String += ":00000000000000000000000000000000"
		maxBuf = ",maxbuf=16777215"
	}

	nc := fmt.Sprintf("%08x", m.nonceCount)

	resHash := m.getHash(digestUri, a2String, c)

	res := "qop=" + m.auth + ",realm=" + strconv.Quote(c["realm"]) + ",username=" +
		strconv.Quote(m.username) + ",nonce=" + strconv.Quote(m.nonce) + ",cnonce=" +
		strconv.Quote(m.cnonce) + ",nc=" + nc + ",digest-uri=" + strconv.Quote(digestUri) +
		",response=" + resHash + maxBuf

	return []byte(res), nil
}

func (m *DigestMD5Mechanism) encode(outgoing []byte) ([]byte, error) {
	return outgoing, nil
}

func (m *DigestMD5Mechanism) decode(incoming []byte) ([]byte, error) {
	return incoming, nil
}

func (m *DigestMD5Mechanism) dispose() {
	m.password = ""
}

func (m *DigestMD5Mechanism) getConfig() *MechanismConfig {
	return m.mechanismConfig
}

func (m *DigestMD5Mechanism) authenticate(digestUri string, challengeMap map[string]string) error {
	a2String := ":" + digestUri

	if m.auth != "auth" {
		a2String += ":00000000000000000000000000000000"
	}

	if m.getHash(digestUri, a2String, challengeMap) != challengeMap["rspauth"] {
		return fmt.Errorf("authentication error")
	}
	return nil
}

func (m *DigestMD5Mechanism) getHash(digestUri string, a2String string,
	challengeMap map[string]string) string {
	if m.keyHash == "" {
		x := m.username + ":" + challengeMap["realm"] + ":" + m.password
		byteKeyHash := md5.Sum([]byte(x))
		m.keyHash = string(byteKeyHash[:])
	}
	a1String := []string{
		m.keyHash,
		m.nonce,
		m.cnonce,
	}

	if len(m.mechanismConfig.AuthorizationID) != 0 {
		a1String = append(a1String, m.mechanismConfig.AuthorizationID)
	}

	h1 := md5.Sum([]byte(strings.Join(a1String, ":")))
	a1 := hex.EncodeToString(h1[:])

	h2 := md5.Sum([]byte(a2String))
	a2 := hex.EncodeToString(h2[:])

	nc := fmt.Sprintf("%08x", m.nonceCount)

	r := strings.ToLower(a1) + ":" + m.nonce + ":" + nc + ":" + m.cnonce + ":" +
		m.auth + ":" + strings.ToLower(a2)
	hr := md5.Sum([]byte(r))

	res := strings.ToLower(hex.EncodeToString(hr[:]))
	return string(res)
}

func parseChallenge(challenge []byte) map[string]string {
	s := string(challenge)

	c := make(map[string]string)

	for len(s) > 0 {
		eq := strings.Index(s, "=")
		key := s[:eq]
		s = s[eq+1:]
		isQuote := false
		search := ","
		if s[0:1] == "\"" {
			isQuote = true
			search = "\""
			s = s[1:]
		}
		co := strings.Index(s, search)
		if co == -1 {
			co = len(s)
		}
		v := s[:co]
		switch {
		case isQuote && len(s) > len(v)+1:
			s = s[co+2:]
		case co < len(s):
			s = s[co+1:]
		default:
			s = ""
		}
		c[key] = v
	}

	return c
}

func randSeq(n int) string {
	seq := make([]rune, n)
	for i := range seq {
		seq[i] = randSeqChars[rand.Intn(len(randSeqChars))]
	}

	return string(seq)
}
