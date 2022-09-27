package hiveserver

type THandleIdentifier struct {
	GUID   []byte `thrift:"guid,1,required" db:"guid" json:"guid"`
	Secret []byte `thrift:"secret,2,required" db:"secret" json:"secret"`
}
