package etcdutil

import "github.com/coreos/etcd/clientv3"

type ETCDUtil interface {
	cli       *clientv3.Client
	leaseId   clientv3.LeaseID
	lease     clientv3.Lease
}

// 
func NewETCDUtil(conf clientv3.Config)(util *ETCDUtil,err error) {

}
