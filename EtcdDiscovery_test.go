package etcdutil

import (
	"fmt"
	"testing"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

func TestEtcdDiscovery(t *testing.T) {
	cfg := clientv3.Config{
		Endpoints:   []string{"127.0.0.1:2381"},
		DialTimeout: 5 * time.Second,
	}
	if util, err := NewETCDUtil(cfg, 5, nil); err == nil {

		if err := util.PutWithLease("serverlist/", "{state:0}"); err != nil {
			fmt.Println(err.Error())
		}

		if handle, err := util.WatchKey("serverlist", true, func(eventType EventType, key string, value string) {
			fmt.Println("eventType ", eventType, " key ", key, " value ", value)
		}); err == nil {
			fmt.Println("WatchKey handle ", handle)
		} else {
			fmt.Println("WatchKey fail ", err.Error())
		}
		var i int = 30
		for {
			i -= 1
			if i < 0 {
				break
			}
			time.Sleep(time.Second * 1)
			if ret, err := util.Get("serverlist/"); err == nil {
				fmt.Println("get [serverlist/] info ", ret)
			} else {
				fmt.Print("get [serverlist/] info fail ", err.Error())
			}

		}

		util.Put("serverlist/", "{state:2}")
		util.Delete("serverlist/")

		if ret, err := util.Get("serverlist/"); err == nil {
			fmt.Println("get [serverlist/] info ", ret)
		} else {
			fmt.Print("get [serverlist/] info fail ", err.Error())
		}

		util.Close()
	} else {
		t.Log(err.Error())
	}
}
