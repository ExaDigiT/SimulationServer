from enum import Enum

class JobStateEnum(str, Enum):
    """
    Slurm jobs pass through several states in the course of their execution. 
    The typical states are PENDING, RUNNING, SUSPENDED, COMPLETING, and COMPLETED.
    An explanation of each state follows.
    """

    completed = "COMPLETED"
    """
    CD COMPLETED
    Job has terminated all processes on all nodes with an exit code of zero.
    """

    pending = "PENDING"
    """
    PD PENDING
    Job is awaiting resource allocation.
    """

    running = "RUNNING"
    """
    R RUNNING
    Job currently has an allocation.
    """

    cancelled = "CANCELLED"
    """
    CA CANCELLED
    Job was explicitly cancelled by the user or system administrator. The job may or may not have been initiated.
    """

    completing = "COMPLETING"
    """
    CG COMPLETING
    Job is in the process of completing. Some processes on some nodes may still be active.
    """


    #######

    boot_fail = "BOOT_FAIL"
    """
    BF BOOT_FAIL
    Job terminated due to launch failure, typically due to a hardware failure (e.g. unable to boot the node or block and the job can not be requeued).
    """

    configuring = "CONFIGURING"
    """
    CF CONFIGURING
    Job has been allocated resources, but are waiting for them to become ready for use (e.g. booting).
    """
    
    deadline = "DEADLINE"
    """
    DL DEADLINE
    Job terminated on deadline.
    """

    failed = "FAILED"
    """
    F FAILED
    Job terminated with non-zero exit code or other failure condition.
    """

    node_fail = "NODE_FAIL"
    """
    NF NODE_FAIL
    Job terminated due to failure of one or more allocated nodes.
    """

    out_of_memory = "OUT_OF_MEMORY"
    """
    OOM OUT_OF_MEMORY
    Job experienced out of memory error.
    """

    preempted = "PREEMPTED"
    """
    PR PREEMPTED
    Job terminated due to preemption.
    """

    resv_del_hold = "RESV_DEL_HOLD"
    """
    RD RESV_DEL_HOLD
    Job is being held after requested reservation was deleted.
    """

    requeue_fed = "REQUEUE_FED"
    """
    RF REQUEUE_FED
    Job is being requeued by a federation.
    """

    requeue_hold = "REQUEUE_HOLD"
    """
    RH REQUEUE_HOLD
    Held job is being requeued.
    """

    requeued = "REQUEUED"
    """
    RQ REQUEUED
    Completing job is being requeued.
    """

    resizing = "RESIZING"
    """
    RS RESIZING
    Job is about to change size.
    """

    revoked = "REVOKED"
    """
    RV REVOKED
    Sibling was removed from cluster due to other cluster starting the job.
    """

    signaling = "SIGNALING"
    """
    SI SIGNALING
    Job is being signaled.
    """

    special_exit = "SPECIAL_EXIT"
    """
    SE SPECIAL_EXIT
    The job was requeued in a special state. This state can be set by users, typically in EpilogSlurmctld, if the job has terminated with a particular exit value.
    """

    stage_out = "STAGE_OUT"
    """
    SO STAGE_OUT
    Job is staging out files.
    """

    stopped = "STOPPED"
    """
    ST STOPPED
    Job has an allocation, but execution has been stopped with SIGSTOP signal. CPUS have been retained by this job.
    """

    suspended = "SUSPENDED"
    """
    S SUSPENDED
    Job has an allocation, but execution has been suspended and CPUs have been released for other jobs.
    """

    timeout = "TIMEOUT"
    """
    TO TIMEOUT
    Job terminated upon reaching its time limit.
    """
