package redislocks

import (
	"context"
	"math"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stvp/tempredis"
)

var servers []*tempredis.Server
var taken []bool
var mtx sync.Mutex
var cond = sync.NewCond(&mtx)

const ServerCount = 6
const StartPort = 51200

var timeoutOptions = TimeoutOptions{
	LockTimeout:          100 * time.Millisecond,
	AcquireTimeout:       50 * time.Millisecond,
	AcquireAttemptsLimit: math.MaxUint64,
	RetryInterval:        10 * time.Millisecond,
	RefreshInterval:      80 * time.Millisecond,
}

func TestMain(m *testing.M) {
	startServers()
	defer stopServers()

	m.Run()
}

func startServers() {
	mtx.Lock()
	defer mtx.Unlock()

	if len(servers) > 0 {
		panic("servers already started")
	}

	for i := 0; i < ServerCount; i++ {
		s, err := tempredis.Start(tempredis.Config{
			"port": strconv.Itoa(StartPort + i),
		})
		if err != nil {
			panic(err)
		}
		servers = append(servers, s)
		taken = append(taken, false)
	}
}

func stopServers() {
	mtx.Lock()
	defer mtx.Unlock()

	for _, s := range servers {
		_ = s.Term()
	}
	servers = nil
}

func getClient() (redis.UniversalClient, int) {
	mtx.Lock()
	defer mtx.Unlock()

	for {
		for i, t := range taken {
			if !t {
				taken[i] = true
				client := redis.NewClient(&redis.Options{
					Addr: "localhost:" + strconv.Itoa(StartPort+i),
				})
				// Flush DB just in case.
				_ = client.FlushDB(context.Background())
				return client, i
			}
		}
		// Unlocks mtx and waits for a signal.
		cond.Wait()
		// Locks mtx again.
	}
}

func returnClient(client redis.UniversalClient, i int) {
	// Flush DB and close the client.
	_ = client.FlushDB(context.Background())
	_ = client.Close()

	mtx.Lock()

	if 0 < i && i < len(taken) {
		taken[i] = false
	}

	mtx.Unlock()
	// Wakes up one goroutine waiting on cond.
	cond.Signal()
}
