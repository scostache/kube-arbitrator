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
	"math"
	"sync"
	"strconv"
	"k8s.io/apimachinery/pkg/util/wait"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/golang/glog"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
	"github.com/kubernetes-incubator/kube-arbitrator/pkg/client/clientset"
	arbv1 "github.com/kubernetes-incubator/kube-arbitrator/pkg/apis/v1alpha1"
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
	RSJobLabel string = "rs.kube-arbitrator.k8s.io" // used for replica sets, stateful sets
)

type GeneratorConfig struct {
	TimeInterval int64 // max duration a job will have (seconds)
	MaxLearners int  // max number of workers a job will have
	Scheduler   string // scheduler to use
	SetType     string // type of workload replica|ss|qj|xqj
	Number	    int    // number of jobs to generate
        Duration    float64    // arrival rate; -1 for burst of jobs
        WorkloadType int   // type of workload: 1 - heterogeneous, 0 - homogeneous
	Namespace string   // namespace in which the workload will run

	EnableCompletion bool // if completion for the jobs is enable

	Capacity   int64
}

type TimeStats struct {
	Start int64
	Running int64
	Completion int64
	RealCompletion int64
	FirstPodRunning int64
	LastPodRunning int64
}

type Size struct {
	Min int
	Actual int

	FailedPods int
        DeletedPods int
        CompletedPods int
        StartedPods int
}

type GeneratorStats struct {
	gconfig *GeneratorConfig

	// TODO - make one structure for all types instead of keeping separate ones
	// QJ name and number of pods running
	QJState map[string]*Size
	// RS name and number of pods running
	RSState map[string]*Size
	// XQJ name and number of pods running
	XQJState map[string]*Size
	// node and nb of slots allocated
	Allocated int64

	TimeStatsLock *sync.Mutex

	// marks the fact that the job is running and the timestamp
	QJRunning map[string]*TimeStats
	RSRunning map[string]*TimeStats
	XQJRunning map[string]*TimeStats
	
	Deleted map[string]bool

	runningPods map[string]bool

	DeletedCount int

	clients    *kubernetes.Clientset
        arbclients *clientset.Clientset

	allocmutex *sync.Mutex

        // A store of pods, populated by the podController
        podStore    corelisters.PodLister
        podInformer corev1informer.PodInformer

        podSynced func() bool
}

func NewGeneratorStats(config *rest.Config, gconfig *GeneratorConfig) *GeneratorStats {
	genstats := &GeneratorStats {
		gconfig: gconfig,
		QJState: make(map[string]*Size),
		RSState: make(map[string]*Size),
		XQJState: make(map[string]*Size),
		Allocated: 0,
		allocmutex: &sync.Mutex{},
		TimeStatsLock : &sync.Mutex{},
		Deleted: make(map[string]bool),
		runningPods: make(map[string]bool),
		DeletedCount: 0,
		QJRunning: make(map[string]*TimeStats),
		RSRunning: make(map[string]*TimeStats),
		XQJRunning: make(map[string]*TimeStats),
		clients:            kubernetes.NewForConfigOrDie(config),
                arbclients:         clientset.NewForConfigOrDie(config),
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
	go wait.Until(qjrPod.UtilizationSnapshot, 5*time.Second, stopCh)
	go wait.Until(qjrPod.JobRunandClear, 1*time.Second, stopCh)
}

func (qjrPod *GeneratorStats) JobRunandClear() {
	// check jobs that have the Running time stamp and put them in hashmap 
	// hashmap tells me wether jobs should be deleted or continue running

	qjrPod.TimeStatsLock.Lock()
	defer qjrPod.TimeStatsLock.Unlock()

	qjrPod.allocmutex.Lock()
	defer qjrPod.allocmutex.Unlock()

	pending_queue := 0
        real_pending_queue := 0

	ctime := time.Now().Unix()
	fmt.Printf("Checking any jobs to delete \n")
	for name, job := range qjrPod.QJRunning {
		is_deleted := false

		queuejob, err := qjrPod.arbclients.ArbV1().QueueJobs(qjrPod.gconfig.Namespace).Get(name, metav1.GetOptions{})
                if err!= nil {
                                continue
                }
                if int32(queuejob.Status.Running) < int32(queuejob.Spec.SchedSpec.MinAvailable) {
                        real_pending_queue = real_pending_queue + 1
                }

		if qjrPod.gconfig.EnableCompletion {
			if int(queuejob.Status.Succeeded) >= queuejob.Spec.SchedSpec.MinAvailable {
				is_deleted = true
				job.RealCompletion = time.Now().Unix()
				fmt.Printf("%v Deleting QJ Job %s Completed at %v \n", ctime, name, job.RealCompletion)
				foreground := metav1.DeletePropagationForeground
                                err = qjrPod.arbclients.ArbV1().QueueJobs(qjrPod.gconfig.Namespace).Delete(name, &metav1.DeleteOptions{PropagationPolicy: &foreground})
			}
		} else {
		 _, ok := qjrPod.Deleted[name]
		if ctime - job.Completion > 0 && job.Completion > 0 && !ok {
			is_deleted = true
			fmt.Printf("QJ: %s Delay: %v\n", name, job.Running - job.Start)
			var err error
			fmt.Printf("%v Deleting QJ Job %s \n", ctime, name)
			if qjrPod.gconfig.SetType == "qj" {
				foreground := metav1.DeletePropagationForeground
				err = qjrPod.arbclients.ArbV1().QueueJobs(qjrPod.gconfig.Namespace).Delete(name, &metav1.DeleteOptions{PropagationPolicy: &foreground})
			}
			if err == nil {
				qjrPod.Deleted[name] = true
	        		qjrPod.DeletedCount = qjrPod.DeletedCount + 1
			}
			if err != nil {
				fmt.Printf("%+v\n", err)
			}
		}
		}
		if is_deleted {
			fmt.Printf("QJ: %s Delay: %v DeclaredCompletion %v ActualCompletion %v Failed %v Completed %v Deleted %v Started %v TimeToStart %v FirstPodRunning %v LastPodRunning %v \n", name, job.Running - job.Start, job.Completion - job.Running, job.RealCompletion - job.Running, qjrPod.QJState[name].FailedPods, qjrPod.QJState[name].CompletedPods, qjrPod.QJState[name].DeletedPods, qjrPod.QJRunning[name].Running, qjrPod.QJRunning[name].LastPodRunning-qjrPod.QJRunning[name].FirstPodRunning, qjrPod.QJRunning[name].FirstPodRunning, qjrPod.QJRunning[name].LastPodRunning)
		}
	}

	for name, job := range qjrPod.RSRunning {
                 _, ok := qjrPod.Deleted[name]
		is_deleted := false
                if ctime - job.Completion > 0 && job.Completion > 0 && !ok {
                        is_deleted = true
			var err error
                        if qjrPod.gconfig.SetType == "replica" {
			fmt.Printf("RS: %s Delay: %v\n", name, job.Running - job.Start)
			foreground := metav1.DeletePropagationForeground
			fmt.Printf("%v Deleting RS %s Completed at %v\n", ctime, name, job.Completion)
                        err  = qjrPod.clients.ExtensionsV1beta1().ReplicaSets(qjrPod.gconfig.Namespace).Delete(name, &metav1.DeleteOptions{PropagationPolicy: &foreground})
                        }
                        if qjrPod.gconfig.SetType == "statefulset" {
			fmt.Printf("SS: %s Delay: %v\n", name, job.Running - job.Start)
			foreground := metav1.DeletePropagationForeground
                        err  = qjrPod.clients.AppsV1().StatefulSets(qjrPod.gconfig.Namespace).Delete(name, &metav1.DeleteOptions{PropagationPolicy:&foreground})
                        }
                        if err == nil {
                                qjrPod.Deleted[name] = true
                                qjrPod.DeletedCount = qjrPod.DeletedCount + 1
                        }
                        if err != nil {
                                fmt.Printf("%+v\n", err)
                        }
                }
		if is_deleted {
                        fmt.Printf("QJ: %s Delay: %v DeclaredCompletion %v ActualCompletion %v Failed %v Completed %v Deleted %v Started %v \n", name, job.Running - job.Start, job.Completion - job.Running, job.RealCompletion - job.Running, qjrPod.RSState[name].FailedPods, qjrPod.RSState[name].CompletedPods, qjrPod.RSState[name].DeletedPods, qjrPod.XQJRunning[name].Running)
                }
        }

	for name, job := range qjrPod.XQJRunning {
		is_deleted := false
		 _, ok := qjrPod.Deleted[name]
		if ok {
			continue
		}
		 queuejob, err := qjrPod.arbclients.ArbV1().XQueueJobs(qjrPod.gconfig.Namespace).Get(name, metav1.GetOptions{})
                if err!= nil {
                                continue
                }
                if queuejob.Status.State == arbv1.QueueJobStateEnqueued {
                        pending_queue = pending_queue + 1
                }
                if int32(queuejob.Status.Running) < int32(queuejob.Spec.SchedSpec.MinAvailable) {
                        real_pending_queue = real_pending_queue + 1
                }

	       if qjrPod.gconfig.EnableCompletion {
                        if int(queuejob.Status.Succeeded) >= queuejob.Spec.SchedSpec.MinAvailable {
                                is_deleted = true
				job.RealCompletion = time.Now().Unix()
				fmt.Printf("%v Deleting XQJ Job %s Completed at %v \n", ctime, name, job.RealCompletion)
				foreground := metav1.DeletePropagationForeground
                                err = qjrPod.arbclients.ArbV1().XQueueJobs(qjrPod.gconfig.Namespace).Delete(name, &metav1.DeleteOptions{PropagationPolicy: &foreground})
                	}
		} else {
                if ctime - job.Completion > 0 && job.Completion > 0 && !ok {
			fmt.Printf("XQJ: %s Delay: %v\n", name, job.Running - job.Start)
                        var err error
                        fmt.Printf("%v Deleting XQJ Job %s Completed at %v \n", ctime, name, job.Completion )
                        if qjrPod.gconfig.SetType == "xqj" || qjrPod.gconfig.SetType == "xqjs" || qjrPod.gconfig.SetType == "xqjr" || qjrPod.gconfig.SetType == "xqjall" {
				is_deleted = true
				foreground := metav1.DeletePropagationForeground
                                err = qjrPod.arbclients.ArbV1().XQueueJobs(qjrPod.gconfig.Namespace).Delete(name, &metav1.DeleteOptions{PropagationPolicy:&foreground})
                        }
			if err == nil {
                                qjrPod.Deleted[name] = true
                                qjrPod.DeletedCount = qjrPod.DeletedCount + 1
                        }
                        if err != nil {
                                fmt.Printf("%+v\n", err)
                        }
                }
		}
		if is_deleted {
                        fmt.Printf("XQJ: %s Delay: %v DeclaredCompletion %v ActualCompletion %v Failed %v Completed %v Deleted %v Started %v Requested %v TimeToStart %v FirstPodRunning %v LastpodRunning %v StartTimeStamp %v RunningTimeStamp %v CompletionTimeStamp %v\n", name, job.Running - job.Start, job.Completion - job.Running, job.RealCompletion - job.Running, qjrPod.XQJState[name].FailedPods, qjrPod.XQJState[name].CompletedPods, qjrPod.XQJState[name].DeletedPods, qjrPod.XQJState[name].StartedPods, qjrPod.XQJState[name].Actual, qjrPod.XQJRunning[name].LastPodRunning-qjrPod.XQJRunning[name].FirstPodRunning, qjrPod.XQJRunning[name].FirstPodRunning, qjrPod.XQJRunning[name].LastPodRunning, job.Start, job.Running, job.RealCompletion)
                }	
	}
	fmt.Printf("Dumping Queues for jobs: PendingQueue=%v RealPendingQueue=%v\n", pending_queue, real_pending_queue)
}

func (qjrPod *GeneratorStats) UtilizationSnapshot() {

	capacity := qjrPod.gconfig.Capacity 

	qjrPod.TimeStatsLock.Lock()

	actualAllocated := 0
	for name,job := range qjrPod.QJRunning {
		_, ok := qjrPod.Deleted[name]
		if job.Completion > 0 && !ok {
			actualAllocated = actualAllocated + qjrPod.QJState[name].Actual
		}
		if job.Completion <=0 {
			qjrPod.QJState[name].Actual = 0
		}
	}
	for name,job := range qjrPod.XQJRunning {
                _, ok := qjrPod.Deleted[name]
                if job.Completion > 0 && !ok {
                        actualAllocated = actualAllocated + qjrPod.XQJState[name].Actual
                }
		if job.Completion <0 {
			qjrPod.XQJState[name].Actual = 0
		}
        }
	for name,job := range qjrPod.RSRunning {
                _, ok := qjrPod.Deleted[name]
                if job.Completion > 0 && !ok {
                        actualAllocated = actualAllocated + qjrPod.RSState[name].Actual
                }
        }

	qjrPod.TimeStatsLock.Unlock()

	sel := &metav1.LabelSelector{
                MatchLabels: map[string]string{
                },
        }
        selector, err := metav1.LabelSelectorAsSelector(sel)
        if err != nil {
                fmt.Errorf("couldn't convert QueueJob selector: %v", err)
        	return
	}

	pods, errt := qjrPod.podStore.Pods("").List(selector)
	if errt != nil {
		fmt.Printf("%+v", errt)
		return
	}

	alloc := 0

	for _, pod := range pods {
		if isPodActive(pod) {
			if len(pod.Labels) > 0 && (len(pod.Labels[QueueJobLabel]) > 0 || len(pod.Labels[XQueueJobLabel]) > 0 || len(pod.Labels[RSJobLabel]) > 0) {
				alloc = alloc + 1
				q, ok := qjrPod.XQJState[pod.Labels[XQueueJobLabel]]
				if ok {
					if qjrPod.XQJRunning[pod.Labels[XQueueJobLabel]].Completion <= 0 {
						creationTs := pod.CreationTimestamp.Unix()
						if creationTs < qjrPod.XQJRunning[pod.Labels[XQueueJobLabel]].FirstPodRunning {
                                                        qjrPod.XQJRunning[pod.Labels[XQueueJobLabel]].FirstPodRunning = creationTs
                                                }
						if pod.Status.StartTime != nil {
							runTs := pod.Status.StartTime.Unix()
							if runTs > qjrPod.XQJRunning[pod.Labels[XQueueJobLabel]].LastPodRunning {
								qjrPod.XQJRunning[pod.Labels[XQueueJobLabel]].LastPodRunning  = runTs
							}
						}
        	                        	q.Actual = q.Actual + 1
						if q.Actual >= q.Min {
                                        		qjrPod.XQJRunning[pod.Labels[XQueueJobLabel]].Running = time.Now().Unix()
							qjrPod.XQJRunning[pod.Labels[XQueueJobLabel]].Completion = time.Now().Unix() + 60 + int64(rand.Intn(int(qjrPod.gconfig.TimeInterval)))
                        				fmt.Printf("XQueuejob %s is running - running pods: %v started running at: %v completion at: %v\n", pod.Labels[XQueueJobLabel], qjrPod.XQJState[pod.Labels[XQueueJobLabel]].Actual, qjrPod.XQJRunning[pod.Labels[XQueueJobLabel]].Running, qjrPod.XQJRunning[pod.Labels[XQueueJobLabel]].Completion)
						}
					}
				}
				q, ok = qjrPod.QJState[pod.Labels[QueueJobLabel]]
                                if ok {
                                        if qjrPod.QJRunning[pod.Labels[QueueJobLabel]].Completion <= 0 {
                                                creationTs := pod.CreationTimestamp.Unix()
                                                if creationTs < qjrPod.QJRunning[pod.Labels[QueueJobLabel]].FirstPodRunning {
                                                        qjrPod.QJRunning[pod.Labels[QueueJobLabel]].FirstPodRunning = creationTs
                                                }
                                                if pod.Status.StartTime != nil {
                                                        runTs := pod.Status.StartTime.Unix()
                                                        if runTs > qjrPod.QJRunning[pod.Labels[QueueJobLabel]].LastPodRunning {
                                                                qjrPod.QJRunning[pod.Labels[QueueJobLabel]].LastPodRunning  = runTs
                                                        }
                                                }
                                                q.Actual = q.Actual + 1
                                                if q.Actual >= q.Min {
                                                        qjrPod.QJRunning[pod.Labels[QueueJobLabel]].Running = time.Now().Unix()
                                                        qjrPod.QJRunning[pod.Labels[QueueJobLabel]].Completion = time.Now().Unix() + 60 + int64(rand.Intn(int(qjrPod.gconfig.TimeInterval)))
                                                        fmt.Printf("Queuejob %s is running - running pods: %v started running at: %v completion at: %v\n", pod.Labels[QueueJobLabel], qjrPod.QJState[pod.Labels[QueueJobLabel]].Actual, qjrPod.QJRunning[pod.Labels[QueueJobLabel]].Running, qjrPod.QJRunning[pod.Labels[QueueJobLabel]].Completion)
                                                }
                                        }
                                }
                        }
		}
	}

	util := float64(alloc)/float64(capacity)
	fmt.Printf("%v Cluster utilization: %v Allocated %v Real %v Capacity %v \n", time.Now().Unix(), util, alloc, actualAllocated, capacity)
}

func isPodActive(p *v1.Pod) bool {
	return v1.PodSucceeded != p.Status.Phase &&
		v1.PodFailed != p.Status.Phase &&
		p.DeletionTimestamp == nil
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

	qjrPod.TimeStatsLock.Lock()
        defer qjrPod.TimeStatsLock.Unlock()

	if len(pod.Labels) != 0 &&  (len(pod.Labels[QueueJobLabel]) > 0 || len(pod.Labels[XQueueJobLabel]) > 0 || len(pod.Labels[RSJobLabel]) > 0) {
		if oldpod.Status.Phase != pod.Status.Phase && pod.Status.Phase == v1.PodRunning{
			qjrPod.runningPods[pod.Name] = true
			qjrPod.UpdateAllocated()
		}
	}

        // update running pod counter for a QueueJob
        if len(pod.Labels) != 0 && len(pod.Labels[QueueJobLabel]) > 0 && qjrPod.gconfig.SetType == "qj" {
        	if oldpod.Status.Phase != pod.Status.Phase && pod.Status.Phase == v1.PodRunning{
			name, ok := pod.Labels[QueueJobLabel]
			if !ok {
				return
			}
			qjrPod.QJState[name].Actual = qjrPod.QJState[name].Actual + 1
			qjrPod.QJState[name].StartedPods = qjrPod.QJState[name].StartedPods + 1
			if qjrPod.QJState[name].Actual >= qjrPod.QJState[name].Min {
				//qjrPod.QJRunning[name].Running = time.Now().Unix()
				//qjrPod.QJRunning[name].Completion = time.Now().Unix() + int64(30 + rand.Intn(int(qjrPod.gconfig.TimeInterval)))
				fmt.Printf("Queuejob %s is running - running pods: %v\n", name, qjrPod.QJState[name].Actual)
			}
		}
		if oldpod.Status.Phase != pod.Status.Phase && pod.Status.Phase == v1.PodSucceeded {
			name := pod.Labels[QueueJobLabel]
			qjrPod.QJState[name].CompletedPods = qjrPod.QJState[name].CompletedPods + 1
		}
		if oldpod.Status.Phase != pod.Status.Phase && pod.Status.Phase == v1.PodFailed {
                        name := pod.Labels[QueueJobLabel]
                        qjrPod.QJState[name].FailedPods = qjrPod.QJState[name].FailedPods + 1
                }
	}

	if len(pod.Labels) != 0 && len(pod.Labels[XQueueJobLabel]) > 0 && (qjrPod.gconfig.SetType == "xqj" || qjrPod.gconfig.SetType == "xqjs" || qjrPod.gconfig.SetType == "xqjr" || qjrPod.gconfig.SetType == "xqjall" ) {
                name, ok := pod.Labels[XQueueJobLabel]
                if !ok {
                                return
                }
		if oldpod.Status.Phase != pod.Status.Phase && pod.Status.Phase == v1.PodRunning{
			_, k := qjrPod.XQJState[name]
			if !k {
				fmt.Printf("Dumping data because qjrPod.XQJState is nil: %s %+v", name, pod.Labels)
				return
			}
			
                        //qjrPod.XQJState[name].Actual = qjrPod.XQJState[name].Actual + 1
			qjrPod.XQJState[name].StartedPods = qjrPod.XQJState[name].StartedPods + 1
                        if qjrPod.XQJState[name].Actual >= qjrPod.XQJState[name].Min {
                                //qjrPod.XQJRunning[name].Running = time.Now().Unix()
				//qjrPod.XQJRunning[name].Completion = time.Now().Unix() + int64(30 + rand.Intn(int(qjrPod.gconfig.TimeInterval)))
                        	//fmt.Printf("XQueuejob %s is running - running pods: %v started running at: %v completion at: %v\n", name, qjrPod.XQJState[name].Actual, qjrPod.XQJRunning[name].Running, qjrPod.XQJRunning[name].Completion)
			}
                }

		if oldpod.Status.Phase != pod.Status.Phase && pod.Status.Phase == v1.PodSucceeded {
                        name := pod.Labels[XQueueJobLabel]
                        qjrPod.XQJState[name].CompletedPods = qjrPod.XQJState[name].CompletedPods + 1
                }
                if oldpod.Status.Phase != pod.Status.Phase && pod.Status.Phase == v1.PodFailed {
                        name := pod.Labels[XQueueJobLabel]
                        qjrPod.XQJState[name].FailedPods = qjrPod.XQJState[name].FailedPods + 1
                }

        }

	if len(pod.Labels) != 0 && len(pod.Labels[RSJobLabel]) > 0 && (qjrPod.gconfig.SetType == "replica" || qjrPod.gconfig.SetType == "statefulset") {
                if oldpod.Status.Phase != pod.Status.Phase && pod.Status.Phase == v1.PodRunning{
			name := pod.Labels[RSJobLabel]
                        qjrPod.RSState[name].Actual = qjrPod.RSState[name].Actual + 1
			qjrPod.QJState[name].StartedPods = qjrPod.QJState[name].StartedPods + 1
                        if qjrPod.RSState[name].Actual >= qjrPod.RSState[name].Min {
                                qjrPod.RSRunning[name].Running = time.Now().Unix()
				qjrPod.RSRunning[name].Completion = time.Now().Unix() + qjrPod.gconfig.TimeInterval
                        	fmt.Printf("RS %s is running - running pods: %v\n", name, qjrPod.RSState[name].Actual)
			}
                }
		if oldpod.Status.Phase != pod.Status.Phase && pod.Status.Phase == v1.PodSucceeded {
                        name := pod.Labels[QueueJobLabel]
                        qjrPod.RSState[name].CompletedPods = qjrPod.RSState[name].CompletedPods + 1
                }
                if oldpod.Status.Phase != pod.Status.Phase && pod.Status.Phase == v1.PodFailed {
                        name := pod.Labels[QueueJobLabel]
                        qjrPod.RSState[name].FailedPods = qjrPod.RSState[name].FailedPods + 1
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

	qjrPod.TimeStatsLock.Lock()
        defer qjrPod.TimeStatsLock.Unlock()

	qjrPod.allocmutex.Lock()
        defer qjrPod.allocmutex.Unlock()

	isdel, ok := qjrPod.runningPods[pod.Name]
	if !ok || !isdel {
		return
	}

        if len(pod.Labels) != 0 && (len(pod.Labels[QueueJobLabel]) > 0 || len(pod.Labels[XQueueJobLabel]) > 0 || len(pod.Labels[RSJobLabel]) > 0) {
                qjrPod.Allocated = qjrPod.Allocated - 1
		if qjrPod.gconfig.SetType == "replica" || qjrPod.gconfig.SetType == "statefulset"{
        		name := pod.Labels[RSJobLabel]
			qjrPod.RSState[name].Actual = qjrPod.RSState[name].Actual - 1
			qjrPod.XQJState[name].DeletedPods = qjrPod.XQJState[name].DeletedPods + 1
		}
		if qjrPod.gconfig.SetType == "xqj" || qjrPod.gconfig.SetType == "xqjs" || qjrPod.gconfig.SetType == "xqjr" || qjrPod.gconfig.SetType == "xqjall"  {
                        name := pod.Labels[XQueueJobLabel]
			qjrPod.XQJState[name].Actual = qjrPod.XQJState[name].Actual - 1
                	qjrPod.XQJState[name].DeletedPods = qjrPod.XQJState[name].DeletedPods + 1
		}
		if qjrPod.gconfig.SetType == "qj" {
			name := pod.Labels[QueueJobLabel]
			qjrPod.QJState[name].Actual = qjrPod.QJState[name].Actual - 1
			qjrPod.XQJState[name].DeletedPods = qjrPod.XQJState[name].DeletedPods + 1
		}
	}
}

func main() {
	workloadtype := flag.Int("type", 0, "type of workload to run: 1 means heterogeneous and 0 means homogeneous")
	duration := flag.Float64("rate", -1, "mean arrival rate of the jobs (jobs/second); -1 means a single burst of jobs")
	number := flag.Int("number", 100, "number of jobs to generate")
	settype := flag.String("settype", "replica", "type of set to create replica|ss|qj|xqj|xqjs|xqjr|xqjall")
	scheduler := flag.String("scheduler", "kar-scheduler", "the scheduler name to use for the jobs")
	master := flag.String("master", "", "The address of the Kubernetes API server (overrides any value in kubeconfig)")
	kubeconfig := flag.String("kubeconfig", "/root/.kube/config", "Path to kubeconfig file with authorization and master location information.")
	maxlearners := flag.Int("nworker", 4, "max number of workers a job will have")
	timeInterval := flag.Int("runtime", 30, "max duration a job will have (seconds)")
	priorities := flag.Int("priorities", 0, "Number of priority classes")
	completion := flag.Bool("completion", false, "Enables deletion of job when pods are completed instead of looking at declared runtime")
	
	flag.Parse()

	fmt.Printf("Workloadtype=%v load=%v number of jobs=%v Set Type=%v scheduler %s\n", *workloadtype, *duration, *number, *settype, *scheduler)
	
	config, err := buildConfig(*master, *kubeconfig)
	if err != nil {
		panic(err)
	}
	neverStop := make(chan struct{})

	context := initTestContext(*priorities)
	defer cleanupTestContext(context, *priorities)
	slot := fourCPU
	// arrival rate = exponential distribution
	// l = util/service time, where util = number of occupied slots/capacity of slots ?
	// exponential with mean = 1/l ; I want l = x/minute; 
	// duration=10sec=1/6 ; service rate = 60/10 = 6 
	// lambda = 20/min ; util = l/service = 20/6 = 3.2 >> 1 ! ?

	rep := clusterSize(context, slot)
	fmt.Printf("Cluster capacity: %v \n", rep)

	gconfig := &GeneratorConfig {
        		TimeInterval: int64(*timeInterval),
			Scheduler: *scheduler,
			Capacity: int64(rep),
			Namespace: "workload",
			MaxLearners: *maxlearners,
			SetType: *settype,
			Number: *number,
			Duration: *duration,
			WorkloadType: *workloadtype,
			EnableCompletion: *completion,
                }

	genstats := NewGeneratorStats(config, gconfig)
        genstats.Run(neverStop)

	ctime := time.Now().Unix()

	for i := 0; i < *number; i++ {
		genstats.TimeStatsLock.Lock()
		name := fmt.Sprintf("qj-%v", i)
		nreplicas := *maxlearners
		// for heterogeneous load - uniform load
		if *workloadtype == 1 {
			nreplicas = rand.Intn(nreplicas)
			if nreplicas < 1 {
				nreplicas = 1
			}
		}
		
		if *duration > -1 {
			nextarr := rand.ExpFloat64()/float64(*duration)
			fmt.Printf("Waiting .... %v seconds\n", nextarr)
			time.Sleep(time.Duration(math.Ceil(nextarr*1000))*time.Millisecond)
		}
		

		priority := "default"
		priorityvalue := 0
		if *priorities > 0 {
			prange := rand.Intn(*priorities)
			priorityvalue = prange * 10
			priority = "priority-"+strconv.Itoa(prange)
		}
	
		jobruntime := -1
		if *completion {
			jobruntime = *timeInterval // TODO - change to variable runtime
		}

		fmt.Printf("Creating %s name %s priority %v runtime %v resources %v %+v\n", *settype, name, priority, jobruntime, nreplicas, slot)

		if *settype == "xqj" {
			qj := createXQueueJob(context, name, int32(nreplicas), int32(nreplicas), "nginx", priority, priorityvalue, jobruntime, *scheduler, slot)
			genstats.XQJState[name]= &Size {
					Min: 	qj.Spec.SchedSpec.MinAvailable,
					Actual: 0,
				}
			genstats.XQJRunning[name] = &TimeStats{
					Start: time.Now().Unix(),
					Running: -1,
					FirstPodRunning: 2147483647,
					LastPodRunning: -1,
				}
		}
		if *settype == "xqjs" {
                        qj := createXQueueJobwithStatefulSet(context, name, int32(nreplicas), int32(nreplicas), "busybox", priority, priorityvalue, jobruntime, *scheduler, slot)
                        genstats.XQJState[name]= &Size {
                                        Min:    qj.Spec.SchedSpec.MinAvailable,
                                        Actual: 0,
                                }
                        genstats.XQJRunning[name] = &TimeStats{
                                        Start: time.Now().Unix(),
                                        Running: -1,
					FirstPodRunning: 2147483647,
                                        LastPodRunning: -1,
                                }
                }
		if *settype == "xqjr" {
                        qj := createXQueueJobwithReplicaSet(context, name, int32(nreplicas), int32(nreplicas), "nginx", priority, priorityvalue, jobruntime, *scheduler, slot)
                        genstats.XQJState[name]= &Size {
                                        Min:    qj.Spec.SchedSpec.MinAvailable,
                                        Actual: 0,
                                }
                        genstats.XQJRunning[name] = &TimeStats{
                                        Start: time.Now().Unix(),
                                        Running: -1,
                                }
                }
		if *settype == "xqjall" {
                        qj := createXQueueJobwithMultipleResources(context, name, int32(nreplicas), int32(nreplicas), "nginx", priority, priorityvalue, jobruntime, *scheduler, slot)
                        genstats.XQJState[name]= &Size {
                                        Min:    qj.Spec.SchedSpec.MinAvailable,
                                        Actual: 0,
                                }
                        genstats.XQJRunning[name] = &TimeStats{
                                        Start: time.Now().Unix(),
                                        Running: -1,
                                }
                }


		if *settype == "qj" {
			qj := createQueueJob(context, name, int32(nreplicas), int32(nreplicas), "nginx", *scheduler, slot)
			genstats.QJState[name]= &Size{
					Min:    qj.Spec.SchedSpec.MinAvailable,
                                        Actual: 0,
				}
			genstats.QJRunning[name] = &TimeStats{
                                        Start: time.Now().Unix(),
                                        Running: -1,
					FirstPodRunning: 2147483647,
                                        LastPodRunning: -1,
                                }
		}
		if *settype == "replica" {
			rs := createReplicaSet(context, name, int32(nreplicas), "nginx", *scheduler, slot)
			genstats.RSState[name]= &Size{
				Min: int(*rs.Spec.Replicas),
				Actual: 0,
			}
			genstats.RSRunning[name] = &TimeStats{
                                        Start: time.Now().Unix(),
                                        Running: -1,
                                }
		}
		if *settype == "statefulset" {
                        rs := createStatefulSet(context, name, int32(nreplicas), "nginx", *scheduler, slot)
                        genstats.RSState[name]= &Size{
                                Min: int(*rs.Spec.Replicas),
                                Actual: 0,
                        }
                        genstats.RSRunning[name] = &TimeStats{
                                        Start: time.Now().Unix(),
                                        Running: -1,
                                }
                }
		genstats.TimeStatsLock.Unlock()
	}
	// wait for all?
	for genstats.DeletedCount < *number {
		time.Sleep(2*time.Second)
	}

	// wait all jobs to finish
	ftime := time.Now().Unix()
	diff := ftime - ctime
	fmt.Printf("Makespan of experiment is %v\n", diff)
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
                fmt.Printf("RS/SS: %s Delay: %v\n", name, stats.Running - stats.Start)
        	total_wait = total_wait + int(stats.Running - stats.Start)
	}
	fmt.Printf("Total wait time: %v", total_wait)
}
