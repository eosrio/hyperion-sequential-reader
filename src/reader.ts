import {RawData} from "ws";
import {EventEmitter} from "events";
import * as path from "path";
import {fileURLToPath} from "node:url";
import {cargo, queue, QueueObject} from "async";
import fetch from "node-fetch";
import * as process from "process";
import * as console from "console";

import {addOnBlockToABI, logLevelToInt, ThroughputMeasurer} from "./utils.js";
import {ActionDSMessage, ActionDSResponse, DeltaDSMessage, DeltaDSResponse} from "./ds-worker.js";
import {SharedObject, SharedObjectStore} from "./shm.js";
import {StateHistorySocket} from "./state-history.js";
import {OrderedSet} from "./orderedset.js";

import {ABI, ABIDecoder, APIClient, Serializer} from "@greymass/eosio";
import workerpool, {Pool} from "workerpool";

const HIST_TIME = 15 * 60 * 2;  // 15 minutes in blocks

export interface HyperionSequentialReaderOptions {
    shipApi: string;
    chainApi: string;
    poolSize: number;
    irreversibleOnly?: boolean;
    blockConcurrency?: number;
    blockHistorySize?: number;
    startBlock?: number;
    endBlock?: number;
    inputQueueLimit?: number;
    outputQueueLimit?: number;
    logLevel?: string;
    workerLogLevel?: string;
    maxPayloadMb?: number;
    actionWhitelist?: {[key: string]: string[]};  // key is code name, value is list of actions
    tableWhitelist?: {[key: string]: string[]};   // key is code name, value is list of tables,
    speedMeasureConf?: {
        windowSizeMs: number;
        deltaMs: number;
    };
}

export class HyperionSequentialReader {
    ship: StateHistorySocket;
    max_payload_mb: number;
    reconnectCount = 0;
    private connecting = false;
    private shipAbi?: ABI;
    private shipAbiReady = false;
    events = new EventEmitter();

    private dsPool: Pool;

    private sharedABIStore: SharedObjectStore<ABI.Def>;

    private contracts: Map<string, ABI> = new Map();
    private actionWhitelist: Map<string, string[]>;
    private tableWhitelist: Map<string, string[]>;

    startBlock: number;
    endBlock: number;

    // queues
    maxMessagesInFlight = 50;
    inputQueueLimit = 200;
    outputQueueLimit = 1000;
    blockConcurrency: number;
    inputQueue: QueueObject<any>;
    decodingQueue: QueueObject<any>;

    private pendingAck = 0;
    private paused = false;

    perfMetrics: ThroughputMeasurer;
    readonly speedMeasureWindowSize: number;
    readonly speedMeasureDeltaMs: number;
    private blocksSinceLastMeasure: number = 0;
    private _perfMetricTask;

    // block collector map
    blockCollector: Map<number, {
        ready: boolean,
        blockInfo: any,
        blockHeader: any,
        counters: {
            actions: number,
            deltas: number
        },
        createdAt?: bigint,
        proc_time_us?: number,
        targets: {
            actions: number,
            deltas: number
        }
        actions: any[],
        deltas: any[]
    }> = new Map();

    blockHistory: OrderedSet<number>;
    blockHistorySize: number;
    logLevel: string;

    api: APIClient;
    shipApi: string;
    // abiRequests: Record<string, boolean> = {};
    private actionRefMap: Map<number, any> = new Map();
    private deltaRefMap: Map<string, any> = new Map();
    private lastEmittedBlock: number;
    private nextBlockRequested = 0;
    private irreversibleOnly: boolean;

    onConnected: () => void = null;
    onDisconnect: () => void = null;
    onError: (err) => void = null;

    private _resumerTask;

    constructor(private options: HyperionSequentialReaderOptions) {

        this.logLevel = options.logLevel || 'warning';
        this.shipApi = options.shipApi;
        this.max_payload_mb = options.maxPayloadMb || 256;
        this.ship = new StateHistorySocket(this.shipApi, this.max_payload_mb);

        this.blockHistorySize = options.blockHistorySize || HIST_TIME;
        this.blockHistory = new OrderedSet<number>(this.blockHistorySize);

        // placeholder store
        this.sharedABIStore = new SharedObjectStore({bufferLengthBytes: 0});

        this.api = new APIClient({
            url: options.chainApi,
            fetch
        });

        this.createWorkers({
            poolSize: options.poolSize || 1,
            logLevel: options.workerLogLevel || 'warning'
        });

        this.irreversibleOnly = options.irreversibleOnly || false

        this.startBlock = options.startBlock || -1;
        this.lastEmittedBlock = this.startBlock - 1;
        this.nextBlockRequested = this.startBlock;
        this.endBlock = options.endBlock || -1;

        if (!options.endBlock && !options.startBlock) {
            this.blockConcurrency = 1;
        } else {
            this.blockConcurrency = options.blockConcurrency || 5;
        }

        if (options.inputQueueLimit) {
            this.inputQueueLimit = options.inputQueueLimit;
        }

        if (options.outputQueueLimit) {
            this.outputQueueLimit = options.outputQueueLimit;
        }

        if (options.actionWhitelist)
            this.actionWhitelist = new Map(Object.entries(options.actionWhitelist));

        if (options.tableWhitelist)
            this.tableWhitelist = new Map(Object.entries(options.tableWhitelist));

        this.speedMeasureDeltaMs = 1000;
        this.speedMeasureWindowSize = 10 * 1000;
        if (options.speedMeasureConf) {
            if (options.speedMeasureConf.windowSizeMs)
                this.speedMeasureWindowSize = options.speedMeasureConf.windowSizeMs;

            if (options.speedMeasureConf.deltaMs)
                this.speedMeasureDeltaMs = options.speedMeasureConf.deltaMs;
        }
        this.perfMetrics = new ThroughputMeasurer({windowSizeMs: this.speedMeasureWindowSize})
        this._perfMetricTask = setInterval(() => {
            this.perfMetrics.measure(this.blocksSinceLastMeasure);
            this.blocksSinceLastMeasure = 0;
        }, this.speedMeasureDeltaMs);

        // Initial Reading Queue
        this.inputQueue = cargo(async (tasks) => {
            try {
                await this.processInputQueue(tasks);
            } catch (e) {
                this.log('error', e);
                process.exit();
            }
        }, this.maxMessagesInFlight);

        // Parallel Decoding Queue
        this.decodingQueue = queue(async (task) => {
            await this.decodeShipData(task);
            // readerLog(`[${blockNum}] Decoding Queue: ${this.decodingQueue.length()} | Paused: ${this.paused}`);
            if ((this.decodingQueue.length() < this.inputQueueLimit && this.blockCollector.size < this.outputQueueLimit) && this.paused) {
                this.resumeReading();
            }
        }, this.blockConcurrency);

        // Check if output queue is whitin limits
        this._resumerTask = setInterval(() => {
            if (this.blockCollector.size < this.outputQueueLimit && this.paused) {
                this.resumeReading();
            }
        }, 1000);
    }

    get isShipAbiReady(): boolean {
        return this.shipAbiReady;
    }

    isActionRelevant(account: string, name: string): boolean {
        return (
            this.contracts.has(account) && (
                !this.actionWhitelist ||
                (this.actionWhitelist.has(account) &&
                 this.actionWhitelist.get(account).includes(name))
            )
        );
    }

    isDeltaRelevant(code: string, table: string): boolean {
        return (
            this.contracts.has(code) && (
                !this.tableWhitelist ||
                (this.tableWhitelist.has(code) &&
                 this.tableWhitelist.get(code).includes(table))
            )
        );
    }

    log(level: string, message?: any, ...optionalParams: any[]): void {
        if (logLevelToInt(this.logLevel) >= logLevelToInt(level))
            console.log(`[${(new Date()).toISOString().slice(0, -1)}][READER][${level.toUpperCase()}]`, message, ...optionalParams);
    }

    private async processInputQueue(tasks: any[]) {
        // this.log('info', `Tasks: ${tasks.length} | Decoding Queue: ${this.decodingQueue.length()}`);
        for (const task of tasks) {
            this.decodingQueue.push(task, null);
        }
        if ((this.decodingQueue.length() > this.inputQueueLimit || this.blockCollector.size >= this.outputQueueLimit) && !this.paused) {
            this.paused = true;
            this.pendingAck = tasks.length;
            this.inputQueue.pause();
            this.log('info', 'Reader paused!');
        } else {
            this.ackBlockRange(tasks.length);
        }
    }

    async start() {
        if (this.connecting)
            throw new Error('Reader already connecting');

        // check if target node is up & contains requested range
        await this.api.v1.chain.get_info();

        if (this.startBlock > 0)
            await this.api.v1.chain.get_block(this.startBlock);

        if (this.endBlock > 0)
            await this.api.v1.chain.get_block(this.endBlock);

        this.log('info', 'Node range check done!');
        this.log('info', `Connecting to ${this.shipApi}...`);
        this.connecting = true;

        this.ship.connect(
            (data: RawData) => {
                this.handleShipMessage(data as Buffer).catch((e) => this.log('error', e));
            },
            () => {
                this.connecting = false;
                this.shipAbiReady = false;
                if (this.onDisconnect)
                    this.onDisconnect();
            },
            (err) => {
                this.connecting = false;
                this.shipAbiReady = false;
                if (this.onError)
                    this.onError(err);
            },
            () => {
                this.connecting = false;
                if (this.onConnected)
                    this.onConnected();
            }
        );
    }

   async stop() {
        this.log('info', 'Stopping...');
        clearInterval(this._perfMetricTask);
        clearInterval(this._resumerTask);
        this.ship.close();
        this.shipAbiReady = false;
        this.blockHistory.clear();
        this.blockCollector.clear();
        await this.dsPool.terminate();
    }

    restart(ms: number = 3000) {
        this.log('info', 'Restarting...');
        this.ship.close();
        this.shipAbiReady = false;
        this.blockHistory.clear()
        this.blockCollector.clear()
        setTimeout(() => {
            this.reconnectCount++;
            this.startBlock = this.lastEmittedBlock + 1;
            this.nextBlockRequested = this.lastEmittedBlock;
            this.start();
        }, ms);
    }

    private send(param: (string | any)[]) {
        this.ship.send(Serializer.encode({
            type: 'request',
            object: param,
            abi: this.shipAbi
        }).array);
    }

    private ackBlockRange(size: number) {
        this.send(['get_blocks_ack_request_v0', {num_messages: size}]);
    }

    private async handleShipMessage(msg: Buffer) {
        if (!this.shipAbiReady) {
            this.loadShipAbi(msg);
            return;
        }
        const result = Serializer.decode({type: 'result', abi: this.shipAbi, data: msg});
        switch (result[0]) {
            case 'get_blocks_result_v0': {
                try {
                    this.inputQueue.push(result[1], null);
                } catch (e) {
                    this.log('error', '[decodeShipData]', e.message);
                    this.log('error', e);
                }
                break;
            }
            case 'get_status_result_v0': {
                const data = Serializer.objectify(result[1]) as any;
                this.log('info', `Head block: ${data.head.block_num}`);
                if (this.startBlock < 0) {
                    this.startBlock = (this.irreversibleOnly ? data.last_irreversible.block_num : data.head.block_num) + this.startBlock;
                } else {
                    if (this.irreversibleOnly && this.startBlock > data.last_irreversible.block_num)
                        throw new Error(`irreversibleOnly true but startBlock > ship LIB`);
                }
                const beginShipState = data.chain_state_begin_block;
                const endShipState = data.chain_state_end_block;

                if (this.startBlock <= beginShipState)
                    throw new Error(`Start block ${this.startBlock} not in chain_state, begin state: ${beginShipState} (must be +1 to startBlock)`);

                if (this.endBlock > endShipState)
                    throw new Error(`End block ${this.endBlock} not in chain_state, end state: ${endShipState}`);

                this.lastEmittedBlock = this.startBlock - 1;
                this.nextBlockRequested = this.startBlock;
                this.requestBlocks({
                    from: this.startBlock,
                    to: this.endBlock
                });
                break;
            }
        }
    }

    private loadShipAbi(data: Buffer) {
        this.log('info', `loading ship abi of size: ${data.length}`)
        const abi = JSON.parse(data.toString());
        this.shipAbi = ABI.from(abi);
        this.shipAbiReady = true;
        this.send(['get_status_request_v0', {}]);
        this.ackBlockRange(1);
    }

    private requestBlocks(param: { from: number; to: number }) {
        this.log('info', `Requesting blocks from ${param.from} to ${param.to}`);
        this.send(['get_blocks_request_v0', {
            start_block_num: param.from > 0 ? param.from - 1 : -1,
            end_block_num: param.to > 0 ? param.to + 1 : 0xffffffff,
            max_messages_in_flight: this.maxMessagesInFlight,
            have_positions: [],
            irreversible_only: this.irreversibleOnly,
            fetch_block: true,
            fetch_traces: true,
            fetch_deltas: true,
        }]);
    }

    private async decodeShipData(resultElement: any) {
        const blockInfo = Serializer.objectify({
            head: resultElement.head,
            last_irreversible: resultElement.last_irreversible,
            this_block: resultElement.this_block,
            prev_block: resultElement.prev_block
        });

        const prevBlockNum = blockInfo.prev_block.block_num;
        const prevBlockId = blockInfo.prev_block.block_id;

        const blockNum = blockInfo.this_block.block_num;
        const blockId = blockInfo.this_block.block_id;

        this.log('debug', '[decodeShipData]:');
        this.log('debug', `prev: #${prevBlockNum} - ${prevBlockId}`);
        this.log('debug', `this: #${blockNum} - ${blockId}`);

        // fork handling;
        if (this.blockHistory.has(blockNum)) {
            let i = blockNum;
            this.log('debug', 'FORK detected!');
            this.log('debug', 'blockHistory last 40 values before handling:');
            this.log('debug', this.blockHistory.queue.slice(-40));

            this.log('debug', `purging block collector from ${i}`);

            while(this.blockCollector.delete(i))
                i++;

            this.log('debug', `done, purged up to ${i}`);
            this.log('debug', `purging block history from ${blockNum}`);

            this.blockHistory.deleteFrom(blockNum);

            this.log('debug', 'blockHistory last 40 values after handling:');
            this.log('debug', this.blockHistory.queue.slice(-40));

            this.log('debug', `blockHistory has #${blockNum}? ${this.blockHistory.has(blockNum)}`);

            this.log('debug', 'done.');

            const lastNonForked = blockNum - 1;
            this.lastEmittedBlock = this.lastEmittedBlock > lastNonForked ? lastNonForked : this.lastEmittedBlock;
            this.nextBlockRequested = 0;
        }

        this.blockHistory.add(blockNum);

        if (resultElement.block && blockNum) {

            const block = Serializer.decode({
                type: 'signed_block',
                data: resultElement.block.array as Uint8Array,
                abi: this.shipAbi
            }) as any;

            const blockHeader = Serializer.objectify({
                timestamp: block.timestamp,
                producer: block.producer,
                confirmed: block.confirmed,
                previous: block.previous,
                transaction_mroot: block.transaction_mroot,
                action_mroot: block.action_mroot,
                schedule_version: block.schedule_version,
                new_producers: block.new_producers,
                header_extensions: block.header_extensions,
                producer_signature: block.producer_signature,
                block_extensions: block.block_extensions,
            });

            const extendedDeltas = [];
            if (resultElement.deltas) {
                const deltaArrays = Serializer.decode({
                    type: 'table_delta[]',
                    data: resultElement.deltas.array as Uint8Array,
                    abi: this.shipAbi
                }) as any[];


                // process deltas
                for (let deltaArray of deltaArrays) {

                    // make sure the ABI for the watched contracts is updated before other processing is done
                    if (deltaArray[1].name === 'account') {
                        const abiRows = deltaArray[1].rows.map(r => {
                            if (r.present && r.data.array) {
                                const decodedRow = Serializer.decode({
                                    type: 'account',
                                    data: r.data.array,
                                    abi: this.shipAbi
                                });
                                if (decodedRow[1].abi) {
                                    return Serializer.objectify(decodedRow[1]);
                                }
                            }
                            return null;
                        }).filter(r => r !== null);
                        abiRows.forEach((abiRow) => {
                            if (this.contracts.has(abiRow.name)) {
                                this.log('info', abiRow.name, `block_num: ${blockNum}`, abiRow.creation_date, `abi hex len: ${abiRow.abi.length}`);
                                if (abiRow.abi.length == 0)
                                    return;
                                console.time(`abiDecoding-${abiRow.name}-${blockNum}`);
                                const abiBin = new Uint8Array(Buffer.from(abiRow.abi, 'hex'));
                                const abi = ABI.fromABI(new ABIDecoder(abiBin));
                                console.timeEnd(`abiDecoding-${abiRow.name}-${blockNum}`);
                                this.addContract(abiRow.name, abi);
                            }
                        });
                    }


                    if (deltaArray[1].name === 'contract_row') {
                        deltaArray[1].rows.forEach((row: any, index: number) => {
                            const deltaRow = Serializer.decode({
                                data: row.data.array,
                                type: 'contract_row',
                                abi: this.shipAbi
                            })[1];
                            const deltaObj = Serializer.objectify(deltaRow);
                            if (this.isDeltaRelevant(deltaObj.code, deltaObj.table)) {
                                const extDelta = {
                                    present: row.present,
                                    ...deltaObj
                                };
                                const key = `${blockNum}:${index}`;
                                this.deltaRefMap.set(key, extDelta);
                                extendedDeltas.push(this.deltaRefMap.get(key));
                                const deltaDSParams: DeltaDSMessage = {
                                    shmRef: this.sharedABIStore.sharedMem,
                                    memMap: this.sharedABIStore.getMemoryMap(),
                                    index, blockId, blockNum,
                                    data: extDelta
                                };
                                this.dsPool.exec('processDelta', [deltaDSParams]).then((delta: DeltaDSResponse) => {
                                    this.collectDelta(delta);
                                }).catch((error) => {
                                    this.log('error', 'processDelta call errored out!');
                                    this.log('error', error.message);
                                    this.log('error', error.stack);
                                    throw new Error(error);
                                });
                            }
                        });
                    }
                }
            }

            const extendedActions = [];
            if (resultElement.traces) {
                const traces = Serializer.decode({
                    type: 'transaction_trace[]',
                    data: resultElement.traces.array as Uint8Array,
                    abi: this.shipAbi,
                    ignoreInvalidUTF8: true
                }) as any[];

                // process traces
                for (let trace of traces) {
                    const rt = Serializer.objectify(trace[1]);
                    if (!rt.partial || rt.partial.length < 2)
                        continue;

                    const partialTransaction = rt.partial[1];

                    for (const at of rt.action_traces) {
                        const actionTrace = at[1];
                        if (actionTrace.receipt === null) {
                            this.log('warning', `action trace with receipt null! maybe hard_fail'ed deferred tx? block: ${blockNum}`);
                            continue;
                        }
                        if (this.isActionRelevant(actionTrace.act.account, actionTrace.act.name))  {
                            const abiActionNames = [];
                            this.contracts.get(actionTrace.act.account).actions.forEach((obj) => {
                                abiActionNames.push(obj.name.toString());
                            });
                            if (!abiActionNames.includes(actionTrace.act.name)) {
                                this.log(
                                    'warning',
                                    `action ${actionTrace.act.name} not found in ${actionTrace.act.account}'s abi, ignoring tx ${rt.id}...`);
                                continue;
                            }
                            const gs = actionTrace.receipt[1].global_sequence;
                            const extAction = {
                                actionOrdinal: actionTrace.action_ordinal,
                                creatorActionOrdinal: actionTrace.creator_action_ordinal,
                                trxId: rt.id,
                                cpu: rt.cpu_usage_us,
                                net: rt.net_usage_words,
                                ram: actionTrace.account_ram_deltas,
                                receipt: actionTrace.receipt[1],
                                receiver: actionTrace.receiver,
                                console: actionTrace.console,
                                signatures: partialTransaction.signatures,
                                act: actionTrace.act
                            };
                            this.actionRefMap.set(gs, extAction);
                            extendedActions.push(this.actionRefMap.get(gs));
                            const actionDSParams: ActionDSMessage = {
                                shmRef: this.sharedABIStore.sharedMem,
                                memMap: this.sharedABIStore.getMemoryMap(),
                                index: gs, blockId, blockNum,
                                data: actionTrace.act
                            };
                            this.dsPool.exec('processAction', [actionDSParams]).then((action: ActionDSResponse) => {
                                this.collectAction(action);
                            }).catch((error) => {
                                this.log('error', 'process callAction errored out!');
                                this.log('error', error.message);
                                this.log('error', error.stack);
                                throw new Error(error);
                            });
                        }
                    }
                }
            }

            const nBlock = {
                ready: false,
                blockInfo,
                blockHeader,
                counters: {
                    actions: 0,
                    deltas: 0
                },
                targets: {
                    actions: extendedActions.length,
                    deltas: extendedDeltas.length,
                },
                deltas: extendedDeltas,
                actions: extendedActions,
                createdAt: process.hrtime.bigint()
            };

            this.blockCollector.set(blockNum, nBlock);

            if (extendedDeltas.length == 0 && extendedActions.length == 0)
                this.checkBlock(nBlock);
        }
    }

    createWorkers(param: { poolSize: number, logLevel: string }) {
        const __dirname = fileURLToPath(new URL('.', import.meta.url));
        const workerModule = path.join(__dirname, 'ds-worker.js');
        process.env.WORKER_LOG_LEVEL = param.logLevel;
        this.dsPool = workerpool.pool(
            workerModule, {
                minWorkers: param.poolSize,
                maxWorkers: param.poolSize,
                workerType: 'thread'
        });
        this.log('info', `Pool created with ${param.poolSize} workers`);
    }

    private updateSharedABIStore() {
        const objectMap: {[keys: string]: SharedObject<ABI.Def>} = {};
        for (const [account, abi] of this.contracts.entries())
            objectMap[account] = SharedObject.fromObject<ABI.Def>(abi.toJSON());

        this.sharedABIStore = SharedObjectStore.fromObjects<ABI.Def>(objectMap);
    }

    addContract(account: string, abi: ABI) {
        if (account == 'eosio')
            addOnBlockToABI(abi);

        this.contracts.set(account, abi);
        this.updateSharedABIStore();
    }

    addContracts(contracts: {account: string, abi: ABI}[]) {
        for (const contract of contracts) {
            if (contract.account == 'eosio')
                addOnBlockToABI(contract.abi);

            this.contracts.set(contract.account, contract.abi);
        }
        this.updateSharedABIStore();
    }

    private emitBlock(block) {
        delete block.ready;
        const blockNum = block.blockInfo.this_block.block_num;
        this.lastEmittedBlock = blockNum;
        this.blockCollector.delete(blockNum);
        if (this.nextBlockRequested === blockNum)
            this.nextBlockRequested = 0;

        this.events.emit('block', block);
        this.blocksSinceLastMeasure++;

        if (blockNum == this.endBlock) {
            this.log('info', `Finished reading range ${this.startBlock} to ${this.endBlock}`);
            this.stop().then(() => this.events.emit('stop'));
        }
    }

    ack() {
        const nextBlock = this.blockCollector.get(this.lastEmittedBlock + 1);
        if (nextBlock && nextBlock.ready)
            this.emitBlock(nextBlock);

        else
            this.nextBlockRequested = this.lastEmittedBlock + 1;
    }

    private collectAction(action: ActionDSResponse) {
        const refAction = this.actionRefMap.get(action.index);
        refAction.act.data = action.data.data;
        const block = this.blockCollector.get(action.blockNum);
        if (!block) {
            this.log('warning', 'collect delta called but block is undefined');
            return;
        }
        const blockId = block.blockInfo.this_block.block_id;
        if (blockId != action.blockId) {
            this.log(
                'warning',
                `discarding data due to fork on block #${action.blockNum}, data id: ${action.blockId}, collector id: ${blockId}`);
            return
        }

        block.counters.actions++;
        this.actionRefMap.delete(action.index);
        this.checkBlock(block);
    }

    private collectDelta(delta: DeltaDSResponse) {
        const key = `${delta.blockNum}:${delta.index}`;
        const refDelta = this.deltaRefMap.get(key);
        refDelta.value = delta.data.value;
        const block = this.blockCollector.get(delta.blockNum);
        if (!block) {
            this.log('warning', 'collect delta called but block is undefined');
            return;
        }
        const blockId = block.blockInfo.this_block.block_id;
        if (blockId != delta.blockId) {
            this.log(
                'warning',
                `discarding data due to fork on block #${delta.blockNum}, data id: ${delta.blockId}, collector id: ${blockId}`);
            return
        }
        block.counters.deltas++;
        this.deltaRefMap.delete(key);
        this.checkBlock(block);
    }

    private checkBlock(block) {
        if (block.counters.actions === block.targets.actions && block.counters.deltas === block.targets.deltas) {
            const elapsed = process.hrtime.bigint() - block.createdAt;
            block.proc_time_us = Number(elapsed / BigInt(1000));
            delete block.createdAt;
            delete block.counters;
            delete block.targets;
            block.ready = true;
            // check if this block can be emitted directly
            if (this.nextBlockRequested === block.blockInfo.this_block.block_num)
                this.emitBlock(block);
        }
    }

    private resumeReading() {
        this.inputQueue.resume();
        this.paused = false;
        this.ackBlockRange(this.pendingAck);
        this.log('info', 'Reader resumed!');
    }
}
