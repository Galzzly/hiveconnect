package hiveserver

type TStatusCode int64

type TStatus struct {
	StatusCode   TStatusCode `thrift:"statusCode,1,required" db:"statusCode" json:"statusCode"`
	InfoMessages []string    `thrift:"infoMessages,2" db:"infoMessages" json:"infoMessages,omitempty"`
	SqlState     *string     `thrift:"sqlState,3" db:"sqlState" json:"sqlState,omitempty"`
	ErrorCode    *int32      `thrift:"errorCode,4" db:"errorCode" json:"errorCode,omitempty"`
	ErrorMessage *string     `thrift:"errorMessage,5" db:"errorMessage" json:"errorMessage,omitempty"`
}
