package gocql

import (
	"fmt"
	"log"
	"runtime/debug"
	"strings"
	"sync"
	"time"

	//"code.brandscreen.net/go/performance"
)

type ISession interface {
	Close()

	Query(stmt string, values ...interface{}) *Query
	ExecuteBatch(batch *Batch) error
	ExecuteQuery(qry *Query) *Iter
}

type BSSession struct {
	cfg   ClusterConfig
	queue chan *Conn
}

func (me *BSSession) connect(addr string) {
	connConfig := ConnConfig{
		ProtoVersion:  me.cfg.ProtoVersion,
		CQLVersion:    me.cfg.CQLVersion,
		Timeout:       me.cfg.Timeout,
		NumStreams:    me.cfg.NumStreams,
		Compressor:    me.cfg.Compressor,
		Authenticator: me.cfg.Authenticator,
		Keepalive:     me.cfg.SocketKeepalive,
		SetupTimeout:  me.cfg.SetupTimeout,
	}

	for {
		conn, err := Connect(addr, connConfig, me)
		if err == nil {
			err = conn.UseKeyspace(me.cfg.Keyspace)
			if err == nil {
				me.queue <- conn
				log.Println("Init Connected to ", addr)
				break
			}
		}
		log.Println("Unable to connect to ", addr, " Error: ", err)
		time.Sleep(2 * time.Second)
	}

}

func (me *BSSession) ExecuteQuery(qry *Query) (itr *Iter) {
	var conn *Conn

	select {
	case conn = <-me.queue:
	default:
		itr = &Iter{err: ErrUnavailable}
		return
	}

	itr = conn.executeQuery(qry)
	if itr.err == nil {
		me.queue <- conn
		return
	}

	log.Println(itr.err)
	//reconnect
	conn.Close()
	go me.connect(conn.addr)
	return
}

func (me *BSSession) ExecuteBatch(batch *Batch) error {
	debug.PrintStack()
	log.Fatal("Execute Batch Not implemented")
	return nil
}

func (me *BSSession) Close() {
	debug.PrintStack()
	log.Fatal("Close Not implemented")
}

func (me *BSSession) HandleError(conn *Conn, err error, closed bool) {
	//log.Println(err)
	//debug.PrintStack()
	//log.Fatal("Handle Error Not implemented")
	conn.Close()
}

func (me *BSSession) HandleKeyspace(conn *Conn, keyspace string) {
	debug.PrintStack()
	log.Fatal("Handle Keyspace Not implemented")
}

func (me *BSSession) Query(stmt string, values ...interface{}) *Query {

	qry := &Query{
		stmt:    stmt,
		values:  values,
		cons:    me.cfg.Consistency,
		session: me,
		rt:      me.cfg.RetryPolicy,
	}
	return qry
}

func (cfg *ClusterConfig) BSInitSession() (*BSSession, error) {

	if len(cfg.Hosts) < 1 {
		return nil, ErrNoHosts
	}

	numConnections := cfg.NumConns * len(cfg.Hosts)
	session := &BSSession{
		cfg:   *cfg,
		queue: make(chan *Conn, numConnections),
	}

	var wg sync.WaitGroup
	wg.Add(numConnections)

	for i := 0; i < len(cfg.Hosts); i++ {
		addr := strings.TrimSpace(cfg.Hosts[i])
		if strings.Index(addr, ":") < 0 {
			addr = fmt.Sprintf("%s:%d", addr, cfg.DefaultPort)
		}
		for j := 0; j < cfg.NumConns; j++ {
			session.connect(addr)
			wg.Done()
		}
	}

	wg.Wait()
	log.Println("Queue len: ", len(session.queue))
	return session, nil

}
