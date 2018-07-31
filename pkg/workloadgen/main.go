/*
Copyright 2017 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package workloadgen

import (
	"fmt"
	"sync"
	"time"
	"flag"

	"github.com/golang/glog"

	"k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/informers"
	coreinformers "k8s.io/client-go/informers/core/v1"
	"k8s.io/client-go/kubernetes"
	corelisters "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"

	"github.com/kubernetes-incubator/kube-arbitrator/pkg/apis/utils"
	arbv1 "github.com/kubernetes-incubator/kube-arbitrator/pkg/apis/v1alpha1"
	"github.com/kubernetes-incubator/kube-arbitrator/pkg/client"
	"github.com/kubernetes-incubator/kube-arbitrator/pkg/client/clientset"
	arbinformers "github.com/kubernetes-incubator/kube-arbitrator/pkg/client/informers"
	informersv1 "github.com/kubernetes-incubator/kube-arbitrator/pkg/client/informers/v1"
	listersv1 "github.com/kubernetes-incubator/kube-arbitrator/pkg/client/listers/v1"
)

const (
	// QueueJobLabel label string for queuejob name
	QueueJobLabel string = "queuejob.kube-arbitrator.k8s.io"
)

// generate workload parameters
// - homogeneous vs heterogeneous
// - burst vs generate_load_to_reach_utilization_target
// - number of jobs
// - type of workload to create: replicaSet, QueueJob, XQueueJob

func main() {
	workloadtype := flag.Int("type", 0, "type of workload to run: 1 means heterogeneous and 0 means homogeneous")
	duration := flag.Int("load", -1, "target load we want to reach; -1 means a single burst of jobs")
	number := flag.Int("number", 100, "number of jobs to generate")
	settype := flag.String("settype", "replica", "type of set to create")
	
	flag.Parse()
	
	fmt.Printf("Workloadtype=%v load=%v number of jobs=%v Set Type=%v \n", workloadtype, duration, number, settype)
	
	context := initTestContext()
	defer cleanupTestContext(context)
	
	slot := oneCPU
	ctime := time.Now().Unix()
	
	// generate arrival rate to keep the load to target 
	// arrival rate = exponential distribution
	// l = util/service time, where util = number of occupied slots/capacity of slots ?
	// exponential with mean = 1/l ; I want l = x/minute; 
	// duration=10sec=1/6 ; service rate = 60/10 = 6 
	// lambda = 20/min ; util = l/service = 20/6 = 3.2 >> 1 ! ?
	
	lambda := 0.0
	if duration > -1 {
		lambda = duration/(60/(2*10))
	}
	
	ctime := time.Now().Unix()
	
	for i := 0; i < number; i++ {
			name := fmt.Sprintf("qj-%v", i)
			nreplicas := 2
			// for heterogeneous load
			if workloadtype == 1 {
				nreplicas = rand.Intn(4)
			}
			
			if duration > -1 {
				lambda = duration/(60/(nreplicas*10))
				nextarr := rand.ExpFloat64()/lambda
			}
			
			if settype == "xqj" {
				createXQueueJob(context, name, 2, 2, "busybox", slot)
			}
			if settype == "qj" {
				createQueueJob(context, name, 2, 2, "busybox", slot)
			}
			if settype == "replica" {
				createReplicaSet(context, name, 2, "busybox", slot)
			}
	}
	// wait for all?
	err := listXQueueJobs(context, 0)
	// wait all jobs to finish
	ftime := time.Now().Unix()
	diff := ftime - ctime
	fmt.Printf("Makespan of workload is %v\n", diff)
}
