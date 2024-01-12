declare module 'workerpool' {
    // Define options for creating a worker pool
    interface PoolOptions {
        minWorkers?: number | 'max';
        maxWorkers?: number;
        maxQueueSize?: number;
        workerType?: 'auto' | 'web' | 'process' | 'thread';
        workerTerminateTimeout?: number;
        forkArgs?: String[];
        forkOpts?: Object;
        workerOpts?: Object;
        workerThreadOpts?: Object;
        onCreateWorker?: (options: {
            forkArgs: String[];
            forkOpts: Object;
            workerOpts: Object;
            script: string;
        }) => Object;
        onTerminateWorker?: (options: {
            forkArgs: String[];
            forkOpts: Object;
            workerOpts: Object;
            script: string;
        }) => void;
    }

    // Define options for executing a task
    interface ExecOptions {
        on?: (payload: any) => void;
        transfer?: Object[];
    }

    // Define the type for a worker pool
    interface Pool {
        exec<T = any>(method: Function | string, params?: any[], options?: ExecOptions): Promise<T>;
        proxy<T = any>(): Promise<T>;
        stats(): PoolStats;
        terminate(force?: boolean, timeout?: number): Promise<void>;
    }

    // Define statistics provided by the pool
    interface PoolStats {
        totalWorkers: number;
        busyWorkers: number;
        idleWorkers: number;
        pendingTasks: number;
        activeTasks: number;
    }

    // Define the main workerpool functions
    function pool(script?: string, options?: PoolOptions): Pool;
    function worker(methods: Record<string, Function>, options?: { onTerminate?: (code?: number) => Promise<void> | void }): void;

    // Export the workerEmit function (for use within workers)
    function workerEmit(payload: any): unknown;

    // Expose additional properties
    const platform: 'node' | 'browser';
    const isMainThread: boolean;
    const cpus: number;
}

export = workerpool;
