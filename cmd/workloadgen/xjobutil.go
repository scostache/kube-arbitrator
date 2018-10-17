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
	json2 "encoding/json"
	"fmt"
	. "github.com/onsi/gomega"
	"time"
	"strconv"
	"k8s.io/apimachinery/pkg/runtime"
	appv1 "k8s.io/api/extensions/v1beta1"
	app1 "k8s.io/api/apps/v1"
	arbv1 "github.com/kubernetes-incubator/kube-arbitrator/pkg/apis/v1alpha1"
	"k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/util/wait"
)

var longPoll = 100*time.Minute

const (
	queueJobName = "xqueuejob.kube-arbitrator.k8s.io"
)

func createXQueueJob(context *context, name string, min, rep int32, img string, priority string, priorityvalue int, jruntime int, scheduler string, req v1.ResourceList) *arbv1.XQueueJob {
	cmd := make([]string, 0)
	cmd = append(cmd, "sleep")
	if jruntime > 0 {
		cmd = append(cmd, strconv.Itoa(jruntime))
	} else {
		cmd = append(cmd, "100000000")
	}
	podTemplate := v1.PodTemplate{
		ObjectMeta: metav1.ObjectMeta{
			Labels: map[string]string{queueJobName: name},
		},
		TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "PodTemplate"},
		Template: v1.PodTemplateSpec{
			ObjectMeta: metav1.ObjectMeta{
                        Labels: map[string]string{queueJobName: name, "app": name, "size": strconv.Itoa(int(rep))},
                	},
			Spec: v1.PodSpec{
				PriorityClassName: priority,
				SchedulerName: scheduler,
				RestartPolicy:     v1.RestartPolicyNever,
				Containers: []v1.Container{
					{
						Image:           img,
						Name:            name,
						Command:	 cmd,
						ImagePullPolicy: v1.PullIfNotPresent,
						Resources: v1.ResourceRequirements{
							Requests: req,
						},
					},
				},
			},
			},
	}

	pods := make([]arbv1.XQueueJobResource, 0)

	data, err := json2.Marshal(podTemplate)
	if err != nil {
		fmt.Errorf("I encode podTemplate %+v %+v", podTemplate, err)
	}

	rawExtension := runtime.RawExtension{Raw: json2.RawMessage(data)}

	podResource := arbv1.XQueueJobResource{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: context.namespace,
			Labels:    map[string]string{queueJobName: name},
		},
		Replicas:          rep,
		MinAvailable:      &min,
		AllocatedReplicas: 0,
		Priority:          float64(priorityvalue),
		Type:              arbv1.ResourceTypePod,
		Template:          rawExtension,
	}

	pods = append(pods, podResource)

	queueJob := &arbv1.XQueueJob{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: context.namespace,
			Labels:    map[string]string{queueJobName: name},
		},
		Spec: arbv1.XQueueJobSpec{
			Selector: &metav1.LabelSelector{
				MatchLabels: map[string]string{
					queueJobName: name,
				},
			},
			SchedSpec: arbv1.SchedulingSpecTemplate{
				MinAvailable: int(min),
			},
			AggrResources: arbv1.XQueueJobResourceList{
				Items: pods,
			},
			Priority: priorityvalue,
		},
		Status: arbv1.XQueueJobStatus{
			CanRun: false,
			State: arbv1.QueueJobStateEnqueued,
		},
	}

	queueJob, err2 := context.karclient.ArbV1().XQueueJobs(context.namespace).Create(queueJob)
	if err2 != nil {
		fmt.Printf("Error is: %v", err2)
	}

	return queueJob
}

func createXQueueJobwithMultipleResources(context *context, name string, min, rep int32, img string, priority string, priorityvalue int, jruntime int, scheduler string, req v1.ResourceList) *arbv1.XQueueJob {
	// a queuejob with two stateful sets and two services
	cmd := make([]string, 0)
        cmd = append(cmd, "sleep")
	if jruntime > 0 {
                cmd = append(cmd, strconv.Itoa(jruntime))
        } else {
                cmd = append(cmd, "100000000")
        }

	resources := make([]arbv1.XQueueJobResource, 0)
	// first set of the replicas
	rep1 := int32(1)
	name1 := name + "01"
	deployment1 := &app1.StatefulSet{
                ObjectMeta: metav1.ObjectMeta{
                        Name:      name1,
                        Namespace: context.namespace,
                },
                Spec: app1.StatefulSetSpec{
                        Replicas: &rep1,
                        Selector: &metav1.LabelSelector{
                                MatchLabels: map[string]string{
                                        "app" : name,
                                },
                        },
                        Template: v1.PodTemplateSpec{
                                ObjectMeta: metav1.ObjectMeta{
                                        Labels: map[string]string{RSJobLabel: name, XQueueJobLabel: name, "app": name, "size": strconv.Itoa(int(rep))},
                                },
                                Spec: v1.PodSpec{
                                        RestartPolicy: v1.RestartPolicyAlways,
                                        SchedulerName: scheduler,
					//PriorityClassName: priority,
                                        Containers: []v1.Container{
                                                {
                                                        Image:           img,
                                                        Name:            name,
                                                        Command:         cmd,
                                                        ImagePullPolicy: v1.PullIfNotPresent,
                                                        Resources: v1.ResourceRequirements{
                                                                Requests: req,
                                                        },
                                                },
                                        },
                                },
                        },
                },
        }
	data, err := json2.Marshal(deployment1)
        if err != nil {
                fmt.Errorf("I encode stateful set Template %+v %+v", deployment1, err)
        }
        rawExtension := runtime.RawExtension{Raw: json2.RawMessage(data)}

        resource := arbv1.XQueueJobResource{
                ObjectMeta: metav1.ObjectMeta{
                        Name:      name1,
                        Namespace: context.namespace,
                        Labels:    map[string]string{queueJobName: name},
                },
                Replicas:          1,
                MinAvailable:      &min,
                AllocatedReplicas: 0,
                Priority:          0.0,
                Type:              arbv1.ResourceTypeStatefulSet,
                Template:          rawExtension,
        }
        resources = append(resources, resource)
	// second set of the replicas
	name2 := name + "02"
	rep2 := rep - 1
	if rep2 > 0 {
		podTemplate := v1.PodTemplate{
        	        ObjectMeta: metav1.ObjectMeta{
                	        Labels: map[string]string{queueJobName: name},
                	},
                	TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "PodTemplate"},
                	Template: v1.PodTemplateSpec{
                        	ObjectMeta: metav1.ObjectMeta{
                        	Labels: map[string]string{queueJobName: name, "app": name, "size": strconv.Itoa(int(rep))},
                        },
                        Spec: v1.PodSpec{
                                PriorityClassName: priority,
                                SchedulerName: scheduler,
                                RestartPolicy:     v1.RestartPolicyNever,
                                Containers: []v1.Container{
                                        {
                                                Image:           img,
                                                Name:            name,
                                                Command:         cmd,
                                                ImagePullPolicy: v1.PullIfNotPresent,
                                                Resources: v1.ResourceRequirements{
                                                        Requests: req,
                                                },
                                        },
                                },
                        },
                        },
	        }

	deployment2 := &podTemplate
	
	data, err = json2.Marshal(deployment2)
        if err != nil {
                fmt.Errorf("I encode stateful set Template %+v %+v", deployment2, err)
        }
        rawExtension = runtime.RawExtension{Raw: json2.RawMessage(data)}

        resource = arbv1.XQueueJobResource{
                ObjectMeta: metav1.ObjectMeta{
                        Name:      name2,
                        Namespace: context.namespace,
                        Labels:    map[string]string{queueJobName: name},
                },
                Replicas:          1,
                MinAvailable:      &min,
                AllocatedReplicas: 0,
                Priority:          0.0,
                Type:              arbv1.ResourceTypePod,
                Template:          rawExtension,
        }
        resources = append(resources, resource)
	}

	serviceSpec := &v1.Service{
		TypeMeta: metav1.TypeMeta{
			Kind:       "Service",
			APIVersion: "v1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
			Labels: map[string]string{
				"app":         name,
				"service":     "dlaas-parameter-server",
			},
		},
		Spec: v1.ServiceSpec{
			Type:     v1.ServiceTypeClusterIP,
			Selector: map[string]string{"app": name},
			Ports: []v1.ServicePort{
				v1.ServicePort{
					Name:     "grpc",
					Protocol: v1.ProtocolTCP,
					Port:     8080,
				},
			},
		},
	}
	data, err = json2.Marshal(serviceSpec)
        if err != nil {
                fmt.Errorf("I encode stateful set Template %+v %+v", serviceSpec, err)
        }
        rawExtension = runtime.RawExtension{Raw: json2.RawMessage(data)}
                        
        resource = arbv1.XQueueJobResource{
                ObjectMeta: metav1.ObjectMeta{
                        Name:      name2,
                        Namespace: context.namespace,
                        Labels:    map[string]string{queueJobName: name},
                },
                Replicas:          1,
                MinAvailable:      &min,
                AllocatedReplicas: 0,
                Priority:          0.0,
                Type:              arbv1.ResourceTypeService,
                Template:          rawExtension,
        }
        resources = append(resources, resource)

	queueJob := &arbv1.XQueueJob{
                ObjectMeta: metav1.ObjectMeta{
                        Name:      name,
                        Namespace: context.namespace,
                        Labels:    map[string]string{queueJobName: name},
                },
                Spec: arbv1.XQueueJobSpec{
                        Selector: &metav1.LabelSelector{
                                MatchLabels: map[string]string{
                                        queueJobName: name,
                                },
                        },
                        SchedSpec: arbv1.SchedulingSpecTemplate{
                                MinAvailable: int(min),
                        },
                        AggrResources: arbv1.XQueueJobResourceList{
                                Items: resources,
                        },
                },
                Status: arbv1.XQueueJobStatus{
                        CanRun: false,
                        State: arbv1.QueueJobStateEnqueued,
                },
        }

        queueJob, err2 := context.karclient.ArbV1().XQueueJobs(context.namespace).Create(queueJob)
        if err2 != nil {
                fmt.Printf("Error is: %v", err2)
        }
	return queueJob
}

func createXQueueJobwithStatefulSet(context *context, name string, min, rep int32, img string, priority string, priorityvalue int, jruntime int, scheduler string, req v1.ResourceList) *arbv1.XQueueJob {
	cmd := make([]string, 0)
        cmd = append(cmd, "sleep")
	if jruntime > 0 {
                cmd = append(cmd, strconv.Itoa(jruntime))
        } else {
                cmd = append(cmd, "100000000")
        }

	deployment := &app1.StatefulSet{
                ObjectMeta: metav1.ObjectMeta{
                        Name:      name,
                        Namespace: context.namespace,
                },
                Spec: app1.StatefulSetSpec{
                        Replicas: &rep,
                        Selector: &metav1.LabelSelector{
                                MatchLabels: map[string]string{
                                        "app" : name,
                                },
                        },
                        Template: v1.PodTemplateSpec{
                                ObjectMeta: metav1.ObjectMeta{
                                        Labels: map[string]string{RSJobLabel: name, XQueueJobLabel: name, "app": name, "size": strconv.Itoa(int(rep))},
                                },
                                Spec: v1.PodSpec{
					PriorityClassName: priority,
                                        RestartPolicy: v1.RestartPolicyAlways,
                                        SchedulerName: scheduler,
                                        Containers: []v1.Container{
                                                {
                                                        Image:           img,
                                                        Name:            name,
							Command:	 cmd,
                                                        ImagePullPolicy: v1.PullIfNotPresent,
                                                        Resources: v1.ResourceRequirements{
                                                                Requests: req,
                                                        },
                                                },
                                        },
                                },
                        },
                },
        }

	resources := make([]arbv1.XQueueJobResource, 0)

	data, err := json2.Marshal(deployment)
	if err != nil {
		fmt.Errorf("I encode stateful set Template %+v %+v", deployment, err)
	}

	rawExtension := runtime.RawExtension{Raw: json2.RawMessage(data)}

	resource := arbv1.XQueueJobResource{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: context.namespace,
			Labels:    map[string]string{queueJobName: name},
		},
		Replicas:          1,
		MinAvailable:      &min,
		AllocatedReplicas: 0,
		Priority:          float64(priorityvalue),
		Type:              arbv1.ResourceTypeStatefulSet,
		Template:          rawExtension,
	}
	
	resources = append(resources, resource)

	queueJob := &arbv1.XQueueJob{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: context.namespace,
			Labels:    map[string]string{queueJobName: name},
		},
		Spec: arbv1.XQueueJobSpec{
			Selector: &metav1.LabelSelector{
				MatchLabels: map[string]string{
					queueJobName: name,
				},
			},
			SchedSpec: arbv1.SchedulingSpecTemplate{
				MinAvailable: int(min),
			},
			AggrResources: arbv1.XQueueJobResourceList{
				Items: resources,
			},
			Priority: priorityvalue,
		},
		Status: arbv1.XQueueJobStatus{
			CanRun: false,
			State: arbv1.QueueJobStateEnqueued,
		},
	}

	queueJob, err2 := context.karclient.ArbV1().XQueueJobs(context.namespace).Create(queueJob)
	if err2 != nil {
		fmt.Printf("Error is: %v", err2)
	}

	return queueJob

}


func createXQueueJobwithReplicaSet(context *context, name string, min, rep int32, img string, priority string, priorityvalue int, jruntime int, scheduler string, req v1.ResourceList) *arbv1.XQueueJob {
	deployment := &appv1.ReplicaSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: context.namespace,
		},
		Spec: appv1.ReplicaSetSpec{
			Replicas: &rep,
			Selector: &metav1.LabelSelector{
				MatchLabels: map[string]string{
					RSJobLabel: name,
				},
			},
			Template: v1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{RSJobLabel: name, XQueueJobLabel: name, "app": name, "size": strconv.Itoa(int(rep))},
				},
				Spec: v1.PodSpec{
					RestartPolicy: v1.RestartPolicyAlways,
					SchedulerName: scheduler,
					PriorityClassName: priority,
					Containers: []v1.Container{
						{
							Image:           img,
							Name:            name,
							ImagePullPolicy: v1.PullIfNotPresent,
							Resources: v1.ResourceRequirements{
								Requests: req,
							},
						},
					},
				},
			},
		},
	}

	resources := make([]arbv1.XQueueJobResource, 0)

        data, err := json2.Marshal(deployment)
        if err != nil {
                fmt.Errorf("I encode stateful set Template %+v %+v", deployment, err)
        }

        rawExtension := runtime.RawExtension{Raw: json2.RawMessage(data)}

        resource := arbv1.XQueueJobResource{
                ObjectMeta: metav1.ObjectMeta{
                        Name:      name,
                        Namespace: context.namespace,
                        Labels:    map[string]string{queueJobName: name},
                },
                Replicas:          1,
                MinAvailable:      &min,
                AllocatedReplicas: 0,
                Priority:          float64(priorityvalue),
                Type:              arbv1.ResourceTypeReplicaSet,
                Template:          rawExtension,
        }
        
        resources = append(resources, resource)

	queueJob := &arbv1.XQueueJob{
                ObjectMeta: metav1.ObjectMeta{
                        Name:      name,
                        Namespace: context.namespace,
                        Labels:    map[string]string{queueJobName: name},
                },
                Spec: arbv1.XQueueJobSpec{
                        Selector: &metav1.LabelSelector{
                                MatchLabels: map[string]string{
                                        queueJobName: name,
                                },
                        },
                        SchedSpec: arbv1.SchedulingSpecTemplate{
                                MinAvailable: int(min),
                        },
                        AggrResources: arbv1.XQueueJobResourceList{
                                Items: resources,
                        },
		Priority: priorityvalue,
                },
                Status: arbv1.XQueueJobStatus{
                        CanRun: false,
                        State: arbv1.QueueJobStateEnqueued,
                },
        }

        queueJob, err2 := context.karclient.ArbV1().XQueueJobs(context.namespace).Create(queueJob)
        if err2 != nil {
                fmt.Printf("Error is: %v", err2)
        }

        return queueJob
}

func xtaskReady(ctx *context, jobName string, taskNum int) wait.ConditionFunc {
	return func() (bool, error) {
		queueJob, err := ctx.karclient.ArbV1().XQueueJobs(ctx.namespace).Get(jobName, metav1.GetOptions{})
		pods, err := ctx.kubeclient.CoreV1().Pods(ctx.namespace).List(metav1.ListOptions{})
		Expect(err).NotTo(HaveOccurred())

		labelSelector := labels.SelectorFromSet(queueJob.Spec.Selector.MatchLabels)

		readyTaskNum := 0
		for _, pod := range pods.Items {
			if !labelSelector.Matches(labels.Set(pod.Labels)) {
				continue
			}
			if pod.Status.Phase == v1.PodRunning || pod.Status.Phase == v1.PodSucceeded {
				readyTaskNum++
			}
		}

		if taskNum < 0 {
			taskNum = queueJob.Spec.SchedSpec.MinAvailable
		}

		return taskNum <= readyTaskNum, nil
	}
}

func listXQueueJob(ctx *context, jobName string) error {
	_, err := ctx.karclient.ArbV1().XQueueJobs(ctx.namespace).Get(jobName, metav1.GetOptions{})
	return err
}

func listXTasks(ctx *context, nJobs int) wait.ConditionFunc {
	return func() (bool, error) {
		jobs, err := ctx.karclient.ArbV1().XQueueJobs(ctx.namespace).List(metav1.ListOptions{})
		Expect(err).NotTo(HaveOccurred())

		nJobs0 := len(jobs.Items)

		return nJobs0 == nJobs, nil
	}
}

func xtaskCreated(ctx *context, jobName string, taskNum int) wait.ConditionFunc {
	return func() (bool, error) {
		_, err := ctx.karclient.ArbV1().XQueueJobs(ctx.namespace).Get(jobName, metav1.GetOptions{})
		Expect(err).NotTo(HaveOccurred())
		return true, nil
	}
}

func listXQueueJobs(ctx *context, nJobs int) error {
	return wait.Poll(100*time.Millisecond, longPoll, listXTasks(ctx, nJobs))
}

func deleteXQueueJob(ctx *context, jobName string) error {
	foreground := metav1.DeletePropagationForeground
	return ctx.karclient.ArbV1().XQueueJobs(ctx.namespace).Delete(jobName, &metav1.DeleteOptions{
		PropagationPolicy: &foreground,
	})
}

func waitXJobReady(ctx *context, name string, taskNum int) error {
	return wait.Poll(100*time.Millisecond, oneMinute, xtaskReady(ctx, name, taskNum))
}

func waitXJobCreated(ctx *context, name string) error {
	return wait.Poll(100*time.Millisecond, oneMinute, xtaskCreated(ctx, name, -1))
}

func xjobNotReady(ctx *context, jobName string) wait.ConditionFunc {
	return func() (bool, error) {
		queueJob, err := ctx.karclient.ArbV1().XQueueJobs(ctx.namespace).Get(jobName, metav1.GetOptions{})
		Expect(err).NotTo(HaveOccurred())

		pods, err := ctx.kubeclient.CoreV1().Pods(ctx.namespace).List(metav1.ListOptions{})
		Expect(err).NotTo(HaveOccurred())

		labelSelector := labels.SelectorFromSet(queueJob.Spec.Selector.MatchLabels)

		pendingTaskNum := int32(0)
		for _, pod := range pods.Items {
			if !labelSelector.Matches(labels.Set(pod.Labels)) {
				continue
			}
			if pod.Status.Phase == v1.PodPending && len(pod.Spec.NodeName) == 0 {
				pendingTaskNum++
			}
		}

		return int(pendingTaskNum) >= int(queueJob.Spec.SchedSpec.MinAvailable), nil
	}
}

func waitXJobNotReady(ctx *context, name string) error {
	return wait.Poll(10*time.Second, oneMinute, xjobNotReady(ctx, name))
}
