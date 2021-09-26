//
// 2021-09-26
//
//
package etcdutil

import (
	"context"
	"errors"
	"sync"

	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// 侦听的事件返回类型
type EventType int8

const (
	EventType_Put    EventType = iota // 写入值事件
	EventType_Delete                  // 删除key事件
)

// etcd的简单封装
type ETCDUtil struct {
	cli *clientv3.Client // etcd的client

	leaseId clientv3.LeaseID // 租约编号
	ttl     int64            // 租约时间（秒）

	ctx    context.Context    // 退出上下文
	cancel context.CancelFunc // 退出函数

	watchCancelSN   int                        // 分配watchCancel编号
	watchCancelFuns map[int]context.CancelFunc // 管理全部退出watch的函数
	watchCancelMx   sync.Mutex                 // 保护临界

	wg sync.WaitGroup // 管理退出逻辑
}

//
// 创建ETCDUtil
//  conf 连接配置
//  ttl 租赁的超时时间（单位秒）
//  parentContext 依赖退出信号
func NewETCDUtil(conf clientv3.Config, ttl int64, parentContext context.Context) (util *ETCDUtil, err error) {

	util = &ETCDUtil{
		cli:     &clientv3.Client{},
		leaseId: 0,
		ttl:     ttl,
		ctx:     nil,
		cancel: func() {
		},
		watchCancelSN:   0,
		watchCancelFuns: map[int]context.CancelFunc{},
		watchCancelMx:   sync.Mutex{},
		wg:              sync.WaitGroup{},
	}

	if parentContext == nil {
		util.ctx, util.cancel = context.WithCancel(context.Background())
	} else {
		util.ctx, util.cancel = context.WithCancel(parentContext)
	}

	if cli, err := clientv3.New(conf); err != nil {
		return nil, err
	} else {
		util.cli = cli
	}

	if err := util.initLease(); err != nil {
		util.Close()
		return nil, err
	}

	return util, nil
}

// 关闭etcd的连接
func (util *ETCDUtil) Close() {

	// 给开启的全部协程都发送关闭信号
	util.cancel()
	util.WatchCancelAll()

	// 等待优雅的退出
	util.wg.Wait()

	// 将连接断开
	util.cli.Close()
}

// 初始化租赁
func (util *ETCDUtil) initLease() error {

	// 如果之前租赁过，将取消租赁
	util.revokeLease()

	if leaseRsp, err := util.cli.Lease.Grant(util.ctx, util.ttl); err == nil {
		util.leaseId = leaseRsp.ID
	} else {
		return err
	}

	if leaseRspChan, err := util.cli.Lease.KeepAlive(util.ctx, util.leaseId); err == nil {
		util.wg.Add(1)
		go func() {
			defer util.wg.Done()
			for {
				select {
				case _, ok := <-leaseRspChan:
					if !ok {
						// 如果万一超时了，也支持重新注册
						util.initLease()
						return
					}
				case <-util.ctx.Done():
					return
				}
			}
		}()
	} else {
		return err
	}

	return nil
}

// 取消租赁
func (util *ETCDUtil) revokeLease() {

	if util.leaseId != 0 {
		util.cli.Lease.Revoke(util.ctx, util.leaseId)
		util.leaseId = 0
	}

}

// 从etcd里面读取信息
func (util *ETCDUtil) Get(key string) (string, error) {

	if getRsp, err := util.cli.Get(context.Background(), key); err == nil {
		if len(getRsp.Kvs) > 0 {
			return string(getRsp.Kvs[0].Value), nil
		} else {
			return "", errors.New("not exists")
		}
	} else {
		return "", err
	}
}

// 通过前缀批量获取etcd中的信息
func (util *ETCDUtil) GetByPrefix(prefix string) (keys, values []string, e error) {

	if getRsp, err := util.cli.Get(context.Background(), prefix, clientv3.WithPrefix()); err != nil {
		e = err
		return
	} else {
		for _, ev := range getRsp.Kvs {
			keys = append(keys, string(ev.Key))
			values = append(values, string(ev.Value))
		}
		return
	}
}

// 将数据设置到etcd中
func (util *ETCDUtil) Put(key string, value string) error {
	_, err := util.cli.Put(context.Background(), key, value)
	return err
}

// 将数据写入etcd并且按照租赁
func (util *ETCDUtil) PutWithLease(key string, value string) error {
	_, err := util.cli.Put(context.Background(), key, value, clientv3.WithLease(util.leaseId))
	return err
}

// 将某个key从etcd中删除掉
func (util *ETCDUtil) Delete(key string) error {
	_, err := util.cli.Delete(context.Background(), key)
	return err
}

//
// 订阅某个key的修改（回调函数注意线程安全）
//  key 侦听的key
//  isPrefix key是否使用前缀模式
//  cb 回调函数
func (util *ETCDUtil) WatchKey(key string, isPrefix bool, cb func(eventType EventType, key string, value string)) (handle int, err error) {

	ctx, cancel := context.WithCancel(util.ctx)

	util.watchCancelMx.Lock()
	handle = util.watchCancelSN
	util.watchCancelFuns[handle] = cancel
	util.watchCancelSN += 1
	util.watchCancelMx.Unlock()

	var watchChan clientv3.WatchChan
	if isPrefix {
		watchChan = util.cli.Watch(ctx, key, clientv3.WithPrefix())
	} else {
		watchChan = util.cli.Watch(ctx, key)
	}

	util.wg.Add(1)
	go func() {
		defer util.wg.Done()
		for {
			select {
			case <-ctx.Done():
				return
			case watchEvent, ok := <-watchChan:
				if ok {
					for _, event := range watchEvent.Events {
						if event.Type == mvccpb.PUT {
							cb(EventType_Put, string(event.Kv.Key), string(event.Kv.Value))
						} else {
							cb(EventType_Delete, string(event.Kv.Key), string(event.Kv.Value))
						}
					}
				} else {
					// 管道断开了
					return
				}
			}
		}
	}()
	return
}

// 清理掉指定的协程
func (util *ETCDUtil) WatchCancel(handle int) {

	util.watchCancelMx.Lock()
	defer util.watchCancelMx.Unlock()

	if fun, ok := util.watchCancelFuns[handle]; ok {
		fun()
		delete(util.watchCancelFuns, handle)
	}

}

// 清理掉全部的watch协程
func (util *ETCDUtil) WatchCancelAll() {

	util.watchCancelMx.Lock()
	defer util.watchCancelMx.Unlock()

	for _, fun := range util.watchCancelFuns {
		fun()
	}

	util.watchCancelFuns = make(map[int]context.CancelFunc)

}
