package lock

import (
	"fmt"
	"math/rand"
	"time"

	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
)

type Lock struct {
	// IKVClerk is a go interface for k/v clerks: the interface hides
	// the specific Clerk type of ck but promises that ck supports
	// Put and Get.  The tester passes the clerk in when calling
	// MakeLock().
	ck kvtest.IKVClerk
	// You may add code here
	lockClientName string
	name           string
	mytoken        string
}

// The tester calls MakeLock() and passes in a k/v clerk; your code can
// perform a Put or Get by calling lk.ck.Put() or lk.ck.Get().
//
// Use l as the key to store the "lock state" (you would have to decide
// precisely what the lock state is).
func MakeLock(ck kvtest.IKVClerk, l string) *Lock {
	lk := &Lock{ck: ck}
	// You may add code here
	lk.lockClientName = kvtest.RandValue(8)
	return lk
}

func (lk *Lock) makeToken() string {
	return fmt.Sprintf("%s:%d", lk.lockClientName, time.Now().UnixNano())
}

func (lk *Lock) getRandomInt() int {
	// Random int in [0, n)
	x := rand.Int() % 10 // e.g. 0–9
	return x
}

func (lk *Lock) Acquire() {
	// Your code here
	for {
		v, ver, err := lk.ck.Get("lock/" + lk.name)
		if !(err == rpc.ErrNoKey || v == "Empty") {
			// fmt.Println("Try to Acquire, err, v", err, v)
			s := lk.getRandomInt()
			time.Sleep(time.Duration(s) * time.Millisecond)
			continue
		}
		tok := lk.makeToken()
		lk.mytoken = tok

		ok := lk.ck.Put("lock/"+lk.name, tok, ver)
		if ok != rpc.OK && ok != rpc.ErrMaybe {
			continue
		}
		// fmt.Println("Put lock")
		v2, ver2, err := lk.ck.Get("lock/" + lk.name)
		if err == rpc.OK && v2 == tok && ver == ver2-1 {
			// fmt.Println("Acquire Success")
			return
		}
		s := lk.getRandomInt()
		time.Sleep(time.Duration(s) * time.Millisecond)
	}
}

func (lk *Lock) Release() {
	// Your code here
	for {
		// fmt.Println("Try to Release")
		v, ver, err := lk.ck.Get("lock/" + lk.name)
		if err != rpc.OK {
			// fmt.Println("Try to Release err type:", err)
			continue
		}

		if v == lk.mytoken {
			err := lk.ck.Put("lock/"+lk.name, "Empty", ver)
			// fmt.Println("Try to Release err type:", err)
			if err == rpc.OK || err == rpc.ErrMaybe {
				// fmt.Println("Release success.")
				return
			} else {
				continue
			}
		}
	}
}
