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

package e2e

import (
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Job E2E Test", func() {
	It("Schedule Job", func() {
		context := initTestContext()
		defer cleanupTestContext(context)
		rep := clusterSize(context, oneCPU)

		_, pg := createJobEx(context, &jobSpec{
			name: "qj-1",
			tasks: []taskSpec{
				{
					img: "busybox",
					req: oneCPU,
					min: 2,
					rep: rep,
				},
			},
		})

		err := waitPodGroupReady(context, pg)
		Expect(err).NotTo(HaveOccurred())
	})

	It("Schedule Multiple Jobs", func() {
		context := initTestContext()
		defer cleanupTestContext(context)

		rep := clusterSize(context, oneCPU)

		job := &jobSpec{
			tasks: []taskSpec{
				{
					img: "busybox",
					req: oneCPU,
					min: 2,
					rep: rep,
				},
			},
		}

		job.name = "mqj-1"
		_, pg1 := createJobEx(context, job)
		job.name = "mqj-2"
		_, pg2 := createJobEx(context, job)
		job.name = "mqj-3"
		_, pg3 := createJobEx(context, job)

		err := waitPodGroupReady(context, pg1)
		Expect(err).NotTo(HaveOccurred())

		err = waitPodGroupReady(context, pg2)
		Expect(err).NotTo(HaveOccurred())

		err = waitPodGroupReady(context, pg3)
		Expect(err).NotTo(HaveOccurred())
	})

	It("Gang scheduling", func() {
		context := initTestContext()
		defer cleanupTestContext(context)
		rep := clusterSize(context, oneCPU)/2 + 1

		replicaset := createReplicaSet(context, "rs-1", rep, "nginx", oneCPU)
		err := waitReplicaSetReady(context, replicaset.Name)
		Expect(err).NotTo(HaveOccurred())

		job := &jobSpec{
			name:      "gang-qj",
			namespace: "test",
			tasks: []taskSpec{
				{
					img: "busybox",
					req: oneCPU,
					min: rep,
					rep: rep,
				},
			},
		}

		_, pg := createJobEx(context, job)
		err = waitPodGroupPending(context, pg)
		Expect(err).NotTo(HaveOccurred())

		err = waitPodGroupUnschedulable(context, pg)
		Expect(err).NotTo(HaveOccurred())

		err = deleteReplicaSet(context, replicaset.Name)
		Expect(err).NotTo(HaveOccurred())

		err = waitPodGroupReady(context, pg)
		Expect(err).NotTo(HaveOccurred())
	})

	It("Gang scheduling: Full Occupied", func() {
		context := initTestContext()
		defer cleanupTestContext(context)
		rep := clusterSize(context, oneCPU)

		job := &jobSpec{
			namespace: "test",
			tasks: []taskSpec{
				{
					img: "nginx",
					req: oneCPU,
					min: rep,
					rep: rep,
				},
			},
		}

		job.name = "gang-fq-qj1"
		_, pg1 := createJobEx(context, job)
		err := waitPodGroupReady(context, pg1)
		Expect(err).NotTo(HaveOccurred())

		job.name = "gang-fq-qj2"
		_, pg2 := createJobEx(context, job)
		err = waitPodGroupPending(context, pg2)
		Expect(err).NotTo(HaveOccurred())

		err = waitPodGroupReady(context, pg1)
		Expect(err).NotTo(HaveOccurred())
	})

	It("Preemption", func() {
		context := initTestContext()
		defer cleanupTestContext(context)

		slot := oneCPU
		rep := clusterSize(context, slot)

		job := &jobSpec{
			tasks: []taskSpec{
				{
					img: "nginx",
					req: slot,
					min: 1,
					rep: rep,
				},
			},
		}

		job.name = "preemptee-qj"
		_, pg1 := createJobEx(context, job)
		err := waitTasksReady(context, pg1, int(rep))
		Expect(err).NotTo(HaveOccurred())

		job.name = "preemptor-qj"
		_, pg2 := createJobEx(context, job)
		err = waitTasksReady(context, pg1, int(rep)/2)
		Expect(err).NotTo(HaveOccurred())

		err = waitTasksReady(context, pg2, int(rep)/2)
		Expect(err).NotTo(HaveOccurred())
	})

	It("Multiple Preemption", func() {
		context := initTestContext()
		defer cleanupTestContext(context)

		slot := oneCPU
		rep := clusterSize(context, slot)

		job := &jobSpec{
			tasks: []taskSpec{
				{
					img: "nginx",
					req: slot,
					min: 1,
					rep: rep,
				},
			},
		}

		job.name = "preemptee-qj"
		_, pg1 := createJobEx(context, job)
		err := waitTasksReady(context, pg1, int(rep))
		Expect(err).NotTo(HaveOccurred())

		job.name = "preemptor-qj1"
		_, pg2 := createJobEx(context, job)
		Expect(err).NotTo(HaveOccurred())

		job.name = "preemptor-qj2"
		_, pg3 := createJobEx(context, job)
		Expect(err).NotTo(HaveOccurred())

		err = waitTasksReady(context, pg1, int(rep)/3)
		Expect(err).NotTo(HaveOccurred())

		err = waitTasksReady(context, pg2, int(rep)/3)
		Expect(err).NotTo(HaveOccurred())

		err = waitTasksReady(context, pg3, int(rep)/3)
		Expect(err).NotTo(HaveOccurred())
	})

	It("Schedule BestEffort Job", func() {
		context := initTestContext()
		defer cleanupTestContext(context)

		slot := oneCPU
		rep := clusterSize(context, slot)

		job := &jobSpec{
			name: "test",
			tasks: []taskSpec{
				{
					img: "nginx",
					req: slot,
					min: 2,
					rep: rep,
				},
				{
					img: "nginx",
					min: 2,
					rep: rep / 2,
				},
			},
		}

		_, pg := createJobEx(context, job)

		err := waitPodGroupReady(context, pg)
		Expect(err).NotTo(HaveOccurred())
	})

	It("Statement", func() {
		context := initTestContext()
		defer cleanupTestContext(context)

		slot := oneCPU
		rep := clusterSize(context, slot)

		job := &jobSpec{
			namespace: "test",
			tasks: []taskSpec{
				{
					img: "nginx",
					req: slot,
					min: rep,
					rep: rep,
				},
			},
		}

		job.name = "st-qj-1"
		_, pg1 := createJobEx(context, job)
		err := waitPodGroupReady(context, pg1)
		Expect(err).NotTo(HaveOccurred())

		now := time.Now()

		job.name = "st-qj-2"
		_, pg2 := createJobEx(context, job)
		err = waitPodGroupUnschedulable(context, pg2)
		Expect(err).NotTo(HaveOccurred())

		// No preemption event
		evicted, err := podGroupEvicted(context, pg1, now)()
		Expect(err).NotTo(HaveOccurred())
		Expect(evicted).NotTo(BeTrue())
	})

	It("TaskPriority", func() {
		context := initTestContext()
		defer cleanupTestContext(context)

		slot := oneCPU
		rep := clusterSize(context, slot)

		replicaset := createReplicaSet(context, "rs-1", rep/2, "nginx", slot)
		err := waitReplicaSetReady(context, replicaset.Name)
		Expect(err).NotTo(HaveOccurred())

		_, pg := createJobEx(context, &jobSpec{
			name: "multi-pod-job",
			tasks: []taskSpec{
				{
					img: "nginx",
					pri: workerPriority,
					min: rep/2 - 1,
					rep: rep,
					req: slot,
				},
				{
					img: "nginx",
					pri: masterPriority,
					min: 1,
					rep: 1,
					req: slot,
				},
			},
		})

		expteced := map[string]int{
			masterPriority: 1,
			workerPriority: int(rep/2) - 1,
		}

		err = waitTasksReadyEx(context, pg, expteced)
		Expect(err).NotTo(HaveOccurred())
	})

	It("Try to fit unassigned task with different resource requests in one loop", func() {
		context := initTestContext()
		defer cleanupTestContext(context)

		slot := oneCPU
		rep := clusterSize(context, slot)
		minMemberOverride := int32(1)

		replicaset := createReplicaSet(context, "rs-1", rep-1, "nginx", slot)
		err := waitReplicaSetReady(context, replicaset.Name)
		Expect(err).NotTo(HaveOccurred())

		_, pg := createJobEx(context, &jobSpec{
			name: "multi-task-diff-resource-job",
			tasks: []taskSpec{
				{
					img: "nginx",
					pri: masterPriority,
					min: 1,
					rep: 1,
					req: twoCPU,
				},
				{
					img: "nginx",
					pri: workerPriority,
					min: 1,
					rep: 1,
					req: halfCPU,
				},
			},
			minMember: &minMemberOverride,
		})

		err = waitPodGroupPending(context, pg)
		Expect(err).NotTo(HaveOccurred())

		// task_1 has been scheduled
		err = waitTasksReady(context, pg, int(minMemberOverride))
		Expect(err).NotTo(HaveOccurred())
	})
})
