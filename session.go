package cassandra

type Session struct {
	ProtocolVersion int
	Timeout         int
}
type SessionOption func(*Session)

func SessionTimeout(timeout int) SessionOption {
	return func(session *Session) {
		session.Timeout = timeout
	}
}

type QueryResult struct {
}

type QueryResults []QueryResult

func (session *Session) Execute(query string, options ...SessionOption) (QueryResults, error) {
	return nil, nil
}
