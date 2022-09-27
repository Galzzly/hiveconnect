package sasl

type Client struct {
	host            string
	AuthorizationID string
	mechanism       Mechanism
}

func NewSaslClient(host string, mechanism Mechanism) *Client {
	mech, ok := mechanism.(*GSSAPIMechanism)
	if ok {
		mech.host = host
	}
	mechDigest, ok := mechanism.(*DigestMD5Mechanism)
	if ok {
		mechDigest.host = host
	}
	return &Client{
		host:      host,
		mechanism: mechanism,
	}
}

func (c *Client) Start() ([]byte, error) {
	return c.mechanism.start()
}

func (c *Client) Step(challenge []byte) ([]byte, error) {
	return c.mechanism.step(challenge)
}

func (c *Client) Complete() bool {
	return c.GetConfig().complete
}

func (c *Client) GetConfig() *MechanismConfig {
	return c.mechanism.getConfig()
}

func (c *Client) Encode(outgoing []byte) ([]byte, error) {
	return c.mechanism.encode(outgoing)
}

func (c *Client) Decode(incoming []byte) ([]byte, error) {
	return c.mechanism.decode(incoming)
}

func (c *Client) Dispose() {
	c.mechanism.dispose()
}
