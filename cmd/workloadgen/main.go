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

package main

import (
	"fmt"
	"time"
	"flag"
	"math/rand"

	"sync"
	"k8s.io/apimachinery/pkg/util/wait"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/golang/glog"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
	"github.com/kubernetes-incubator/kube-arbitrator/pkg/client/clientset"
	"k8s.io/api/core/v1"
	"k8s.io/client-go/informers"
	corev1informer "k8s.io/client-go/informers/core/v1"
	"k8s.io/client-go/kubernetes"
	corelisters "k8s.io/client-go/listers/core/v1"
)

const (
	// QueueJobLabel label string for queuejob name
	QueueJobLabel string = "queuejob.kube-arbitrator.k8s.io"
	XQueueJobLabel string = "xqueuejob.kube-arbitrator.k8s.io"
	RSJobLabel string = "rs.kube-arbitrator.k8s.io"

	// 10 seconds per job
	TimeInterval int64 = 10
)

type TimeStats struct {
	Start int64
	Running int64
	Completion int64
}

type Size struct {
	Min int
	Actual int
}

type GeneratorStats struct {
	// QJ name and number of pods running
	QJState map[string]*Size
	// RS name and number of pods running
	RSState map[string]*Size
	// XQJ name and number of pods running
	XQJState map[string]*Size
	// node and nb of slots allocated
	Allocated int64

	Capacity int64

	TimeStatsLock *sync.Mutex

	// marks the fact that the job is running and the timestamp
	QJRunning map[string]*TimeStats
	RSRunning map[string]*TimeStats
	XQJRunning map[string]*TimeStats
	
	Deleted map[string]bool

	DeletedCount int

	clients    *kubernetes.Clientset
        arbclients *clientset.Clientset

	allocmutex *sync.Mutex

	workloadtype string

	workloadnamespace string

        // A TTLCache of pod creates/deletes each rc expects to see

        // A store of pods, populated by the podController
        podStore    corelisters.PodLister
        podInformer corev1informer.PodInformer

        podSynced func() bool
}

func NewGeneratorStats(config *rest.Config, csize int32, wtype string, namespace string) *GeneratorStats {
	genstats := &GeneratorStats {
		QJState: make(map[string]*Size),
		RSState: make(map[string]*Size),
		XQJState: make(map[string]*Size),
		Allocated: 0,
		Capacity: int64(csize),
		allocmutex: &sync.Mutex{},
		TimeStatsLock : &sync.Mutex{},
		Deleted: make(map[string]bool),
		DeletedCount: 0,
		QJRunning: make(map[string]*TimeStats),
		RSRunning: make(map[string]*TimeStats),
		XQJRunning: make(map[string]*TimeStats),
		clients:            kubernetes.NewForConfigOrDie(config),
                arbclients:         clientset.NewForConfigOrDie(config),
		workloadtype: wtype,
		workloadnamespace: namespace,
	}

        // create informer for pod information
        genstats.podInformer = informers.NewSharedInformerFactory(genstats.clients, 0).Core().V1().Pods()
        genstats.podInformer.Informer().AddEventHandler(
                cache.FilteringResourceEventHandler{
                        FilterFunc: func(obj interface{}) bool {
                                switch t := obj.(type) {
                                case *v1.Pod:
                                        glog.V(4).Infof("filter pod name(%s) namespace(%s) status(%s)\n", t.Name, t.Namespace, t.Status.Phase)
                                        return true
                                default:
                                        return false
                                }
                        },
                        Handler: cache.ResourceEventHandlerFuncs{
                                AddFunc:    genstats.addPod,
                                UpdateFunc: genstats.updatePod,
                                DeleteFunc: genstats.deletePod,
                        },
                })

        genstats.podStore = genstats.podInformer.Lister()
        genstats.podSynced = genstats.podInformer.Informer().HasSynced

	return genstats
}

func buildConfig(master, kubeconfig string) (*rest.Config, error) {
	if master != "" || kubeconfig != "" {
		return clientcmd.BuildConfigFromFlags(master, kubeconfig)
	}
	return rest.InClusterConfig()
}

func (qjrPod *GeneratorStats) Run(stopCh <-chan struct{}) {
	go qjrPod.podInformer.Informer().Run(stopCh)
	go wait.Until(qjrPod.UtilizationSnapshot, 10*time.Second, stopCh)
	go wait.Until(qjrPod.JobRunandClear, 10*time.Second, stopCh)
}

func (qjrPod *GeneratorStats) JobRunandClear() {
	// check jobs that have the Running time stamp and put them in hashmap 
	// hashmap tells me wether jobs should be deleted or continue running
	qjrPod.allocmutex.Lock()
	defer qjrPod.allocmutex.Unlock()

	ctime := time.Now().Unix()
	fmt.Printf("Checking any jobs to delete \n")
	for name, job := range qjrPod.QJRunning {
		if job.Completion > ctime {
			fmt.Printf("Deleting Job %s \n", name)
			if qjrPod.workloadtype == "rs" {
			qjrPod.clients.ExtensionsV1beta1().ReplicaSets(qjrPod.workloadnamespace).Delete(name, &metav1.DeleteOptions{})
			}
			if qjrPod.workloadtype == "qj" {
			qjrPod.arbclients.ArbV1().QueueJobs(qjrPod.workloadnamespace).Delete(name, &metav1.DeleteOptions{})
			}
			if qjrPod.workloadtype == "xqj" {
			qjrPod.arbclients.ArbV1().XQueueJobs(qjrPod.workloadnamespace).Delete(name, &metav1.DeleteOptions{})
			}
			_, ok := qjrPod.Deleted[name]
			if !ok {
				qjrPod.Deleted[name] = true
	        		qjrPod.DeletedCount = qjrPod.DeletedCount + 1
			}
		}
	}
}

func (qjrPod *GeneratorStats) UtilizationSnapshot() {
	qjrPod.allocmutex.Lock()
        defer qjrPod.allocmutex.Unlock()

	alloc := qjrPod.Allocated
	capacity := qjrPod.Capacity 

	util := float64(alloc)/float64(capacity)
	fmt.Printf("%v Cluster utilization: %v Allocated %v Capacity %v \n", time.Now().Unix(), util, alloc, capacity)
}


// generate workload parameters
// - homogeneous vs heterogeneous
// - burst vs generate_load_to_reach_utilization_target
// - number of jobs
// - type of workload to create: replicaSet, QueueJob, XQueueJob

func (qjrPod *GeneratorStats) addPod(obj interface{}) {

	return
}

func (qjrPod *GeneratorStats) updatePod(old, obj interface{}) {
	var pod, oldpod *v1.Pod
	switch t := obj.(type) {
        case *v1.Pod:
                pod = t
        case cache.DeletedFinalStateUnknown:
                var ok bool
                pod, ok = t.Obj.(*v1.Pod)
                if !ok {
                        glog.Errorf("Cannot convert to *v1.Pod: %v", t.Obj)
                        return
                }
        default:
                glog.Errorf("Cannot convert to *v1.Pod: %v", t)
                return
        }

	switch t := old.(type) {
        case *v1.Pod: 
                oldpod = t
        case cache.DeletedFinalStateUnknown:
                var ok bool
                oldpod, ok = t.Obj.(*v1.Pod)
                if !ok {
                        glog.Errorf("Cannot convert to *v1.Pod: %v", t.Obj)
                        return
                }
        default:
                glog.Errorf("Cannot convert to *v1.Pod: %v", t)
                return
        }

        // update running pod counter for a QueueJob
        if len(pod.Labels) != 0 && len(pod.Labels[QueueJobLabel]) > 0 {
        	if oldpod.Status.Phase != pod.Status.Phase && pod.Status.Phase == v1.PodRunning{
			qjrPod.UpdateAllocated()
			name := pod.Labels[QueueJobLabel]
			qjrPod.QJState[name].Actual = qjrPod.QJState[name].Actual + 1
			if qjrPod.QJState[name].Actual >= qjrPod.QJState[name].Min {
				qjrPod.QJRunning[name].Running = time.Now().Unix()
				qjrPod.QJRunning[name].Completion = time.Now().Unix() + TimeInterval 
				fmt.Printf("Update for queuejob %s %v\n", name, qjrPod.QJState[name].Actual)
			}
		}
	}

	if len(pod.Labels) != 0 && len(pod.Labels[XQueueJobLabel]) > 0 {
                if oldpod.Status.Phase != pod.Status.Phase && pod.Status.Phase == v1.PodRunning{
                        qjrPod.UpdateAllocated()
			name := pod.Labels[XQueueJobLabel]
                        qjrPod.XQJState[name].Actual = qjrPod.XQJState[name].Actual + 1
                        if qjrPod.XQJState[name].Actual >= qjrPod.XQJState[name].Min {
                                qjrPod.XQJRunning[name].Running = time.Now().Unix()
				qjrPod.XQJRunning[name].Completion = time.Now().Unix() + TimeInterval
                        }
                }
        }

	if len(pod.Labels) != 0 && len(pod.Labels[RSJobLabel]) > 0 {
                if oldpod.Status.Phase != pod.Status.Phase && pod.Status.Phase == v1.PodRunning{
                        qjrPod.UpdateAllocated()
			name := pod.Labels[RSJobLabel]
                        qjrPod.RSState[name].Actual = qjrPod.RSState[name].Actual + 1
                        if qjrPod.RSState[name].Actual >= qjrPod.RSState[name].Min {
                                qjrPod.RSRunning[name].Running = time.Now().Unix()
				qjrPod.RSRunning[name].Completion = time.Now().Unix() + TimeInterval
                        }
                }
        }

	return
}

func (qjrPod *GeneratorStats) UpdateAllocated() {
	qjrPod.allocmutex.Lock()
        qjrPod.Allocated = qjrPod.Allocated + 1
        qjrPod.allocmutex.Unlock()
}

func (qjrPod *GeneratorStats) deletePod(obj interface{}) {
	var pod *v1.Pod
        switch t := obj.(type) {
        case *v1.Pod:
                pod = t
        case cache.DeletedFinalStateUnknown:
                var ok bool
                pod, ok = t.Obj.(*v1.Pod)
                if !ok {
                        glog.Errorf("Cannot convert to *v1.Pod: %v", t.Obj)
                        return
                }
        default:
                glog.Errorf("Cannot convert to *v1.Pod: %v", t)
                return
        }

	qjrPod.allocmutex.Lock()
        defer qjrPod.allocmutex.Unlock()

        if len(pod.Labels) != 0 && (len(pod.Labels[QueueJobLabel]) > 0 || len(pod.Labels[XQueueJobLabel]) > 0 || len(pod.Labels[RSJobLabel]) > 0) {
                qjrPod.Allocated = qjrPod.Allocated - 1
        }
}

func main() {
	workloadtype := flag.Int("type", 0, "type of workload to run: 1 means heterogeneous and 0 means homogeneous")
	duration := flag.Int("load", -1, "target load we want to reach; -1 means a single burst of jobs")
	number := flag.Int("number", 100, "number of jobs to generate")
	settype := flag.String("settype", "replica", "type of set to create")
	scheduler := flag.String("scheduler", "", "the scheduler name to use for the jobs")
	master := flag.String("master", "", "The address of the Kubernetes API server (overrides any value in kubeconfig)")
	kubeconfig := flag.String("kubeconfig", "/root/.kube/config", "Path to kubeconfig file with authorization and master location information.")

	flag.Parse()
	
	fmt.Printf("Workloadtype=%v load=%v number of jobs=%v Set Type=%v \n", *workloadtype, *duration, *number, *settype)
	
	config, err := buildConfig(*master, *kubeconfig)
	if err != nil {
		panic(err)
	}
	neverStop := make(chan struct{})

	context := initTestContext()
	defer cleanupTestContext(context)
	slot := oneCPU
	// generate arrival rate to keep the load to target 
	// arrival rate = exponential distribution
	// l = util/service time, where util = number of occupied slots/capacity of slots ?
	// exponential with mean = 1/l ; I want l = x/minute; 
	// duration=10sec=1/6 ; service rate = 60/10 = 6 
	// lambda = 20/min ; util = l/service = 20/6 = 3.2 >> 1 ! ?

	rep := clusterSize(context, oneCPU)
	fmt.Printf("Cluster capacity: %v \n", rep)

	genstats := NewGeneratorStats(config, clusterSize(context, oneCPU), *settype, "workload")
        genstats.Run(neverStop)

	lambda := 0.0
	if *duration > -1 {
		lambda = float64(*duration)/float64((60/(2*10)))
	}
	ctime := time.Now().Unix()

	for i := 0; i < *number; i++ {
		name := fmt.Sprintf("qj-%v", i)
		nreplicas := 2
		// for heterogeneous load
		if *workloadtype == 1 {
			nreplicas = rand.Intn(4)
		}
		
		if *duration > -1 {
			lambda = float64(*duration)/float64(60/(nreplicas*10))
			nextarr := rand.ExpFloat64()/lambda
			time.Sleep(time.Duration(nextarr)*time.Second)
		}
		
		fmt.Printf("Creating %s name %s resources %v %+v\n", *settype, name, nreplicas, slot)
	
		if *settype == "xqj" {
			qj := createXQueueJob(context, name, 2, 2, "nginx", *scheduler, slot)
			genstats.XQJState[name]= &Size {
					Min: 	qj.Spec.SchedSpec.MinAvailable,
					Actual: 0,
				}
			genstats.XQJRunning[name] = &TimeStats{
					Start: time.Now().Unix(),
					Running: -1,
				}
		}
		if *settype == "qj" {
			qj := createQueueJob(context, name, 2, 2, "nginx", *scheduler, slot)
			genstats.QJState[name]= &Size{
					Min:    qj.Spec.SchedSpec.MinAvailable,
                                        Actual: 0,
				}
			genstats.QJRunning[name] = &TimeStats{
                                        Start: time.Now().Unix(),
                                        Running: -1,
                                }
		}
		if *settype == "replica" {
			rs := createReplicaSet(context, name, 2, "nginx", *scheduler, slot)
			genstats.RSState[name]= &Size{
				Min: int(*rs.Spec.Replicas),
				Actual: 0,
			}
			genstats.RSRunning[name] = &TimeStats{
                                        Start: time.Now().Unix(),
                                        Running: -1,
                                }
		}
	}
	// wait for all?
	for genstats.DeletedCount < *number {
		time.Sleep(2*time.Second)
	}

	//if *settype == "xqj" {
	//	listXQueueJobs(context, 0)
	//}
	//if *settype == "qj" {
	//	listCompletedQueueJobs(context, *number)
	//}
	//if *settype == "replica" {
	//	listReplicaSets(context, 0)
	//}
	// wait all jobs to finish
	ftime := time.Now().Unix()
	diff := ftime - ctime
	fmt.Printf("Makespan of workload is %v\n", diff)
	// print all Stats
	total_wait := 0
	for name, stats := range genstats.XQJRunning {
		fmt.Printf("XQJ: %s Delay: %v\n", name, stats.Running - stats.Start)
		total_wait = total_wait + int(stats.Running - stats.Start)
	}
	for name, stats := range genstats.QJRunning {
                fmt.Printf("QJ: %s Delay: %v\n", name, stats.Running - stats.Start)
        	total_wait = total_wait + int(stats.Running - stats.Start)
	}
	for name, stats := range genstats.RSRunning {
                fmt.Printf("XQJ: %s Delay: %v\n", name, stats.Running - stats.Start)
        	total_wait = total_wait + int(stats.Running - stats.Start)
	}
	fmt.Printf("Total wait time: %v", total_wait)
}
