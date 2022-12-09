import WebSocket, {ErrorEvent, RawData} from "ws";
import {ABI, ABIDecoder, Serializer} from "@greymass/eosio";
import {EventEmitter} from "events";
import {Worker} from "worker_threads";
import * as path from "path";

export class HyperionSequentialReader {
    ws: WebSocket
    max_payload_mb = 256;
    private shipAbi?: ABI;
    private shipAbiReady = false;
    private shipInitStatus?: any;
    events = new EventEmitter();

    dsPool: Worker[] = [];
    private allowedContracts: Map<string, ABI> = new Map();
    private deltaCollector?: (value) => void;
    private traceCollector?: (value) => void;
    startBlock: number;
    endBlock: number;

    constructor(private shipUrl: string, options: {
        poolSize: number,
        startBlock?: number,
        endBlock?: number
    }) {
        this.createWorkers({
            poolSize: options.poolSize || 1
        });
        this.startBlock = options.startBlock || -1
        this.endBlock = options.endBlock || 0xffffffff
    }

    start() {
        console.log(`Connecting to ${this.shipUrl}...`);
        this.ws = new WebSocket(this.shipUrl, {
            perMessageDeflate: false,
            maxPayload: this.max_payload_mb * 1024 * 1024,
            handshakeTimeout: 5000,
        });
        this.ws.on('open', () => {
            console.log('Websocket connected!');
        });
        this.ws.on('message', (data: RawData) => {
            this.handleShipMessage(data as Buffer).catch(console.log);
        });
        this.ws.on('close', () => {
            console.log('Websocket disconnected!');
        });
        this.ws.on('error', (err: ErrorEvent) => {
            console.log(`${this.shipUrl} :: ${err.message}`);
        });
    }

    private send(param: (string | any)[]) {
        this.ws.send(Serializer.encode({
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
            case 'get_status_result_v0': {
                const data = Serializer.objectify(result[1]) as any;
                this.requestBlocks({
                    from: this.startBlock > 0 ? this.startBlock : data.head.block_num,
                    to: this.endBlock
                });
                this.shipInitStatus = data;
                break;
            }
            case 'get_blocks_result_v0': {
                try {
                    await this.decodeShipData(result[1]);
                } catch (e) {
                    console.log('[decodeShipData]', e.message);
                    console.log(e);
                }
                break;
            }
        }
    }

    private loadShipAbi(data: RawData) {
        const abi = JSON.parse(data.toString());
        this.shipAbi = ABI.from(abi);
        this.dsPool.forEach(value => {
            value.postMessage({
                event: 'set_ship_abi',
                data: {abi}
            });
        });
        this.shipAbiReady = true;
        this.send(['get_status_request_v0', {}]);
        this.ackBlockRange(1);
    }

    private requestBlocks(param: { from: number; to: number }) {
        this.send(['get_blocks_request_v0', {
            start_block_num: param.from,
            end_block_num: param.to,
            max_messages_in_flight: 1,
            have_positions: [],
            irreversible_only: false,
            fetch_block: true,
            fetch_traces: true,
            fetch_deltas: true,
        }]);
    }

    private async decodeShipData(resultElement: any) {
        // const tRef = process.hrtime.bigint();
        const blockInfo = Serializer.objectify({
            head: resultElement.head,
            last_irreversible: resultElement.last_irreversible,
            this_block: resultElement.this_block,
            prev_block: resultElement.prev_block
        });

        if (resultElement.block && resultElement.traces && resultElement.deltas) {

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

            const transactions = block.transactions as any[];
            const traces = Serializer.decode({
                type: 'transaction_trace[]',
                data: resultElement.traces.array as Uint8Array,
                abi: this.shipAbi
            }) as any[];
            const deltas = Serializer.decode({
                type: 'table_delta[]',
                data: resultElement.deltas.array as Uint8Array,
                abi: this.shipAbi
            }) as any[];
            const promises = [];
            // process deltas
            const deltaRows = [];
            for (let delta of deltas) {
                // make sure the ABI for the watched contracts is updated before other processing is done
                if (delta[1].name === 'account') {
                    const abiRows = delta[1].rows.map(r => {
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
                        if (this.allowedContracts.has(abiRow.name)) {
                            console.time('abiDecoding');
                            console.log(abiRow.name, abiRow.creation_date);
                            const abiBin = new Uint8Array(Buffer.from(abiRow.abi, 'hex'));
                            const abi = ABI.fromABI(new ABIDecoder(abiBin));
                            this.addContract(abiRow.name, abi);
                            console.timeEnd('abiDecoding');
                        }
                    });
                }

                if (delta[1].name === 'contract_row') {
                    let j = 0;
                    delta[1].rows.forEach((row: any, index: number) => {
                        this.dsPool[j].postMessage({
                            event: 'delta',
                            data: {
                                index,
                                present: row.present,
                                data: row.data.array
                            }
                        });
                        j++;
                        if (j > this.dsPool.length - 1) {
                            j = 0;
                        }
                    });
                    promises.push(this.collectDeltas(deltaRows, delta[1].rows.length));
                }
            }

            // process traces
            const actArray = [];
            let expectedTraces = 0;
            for (let trace of traces) {
                let j = 0;
                trace[1].action_traces.forEach((actionTrace: any, index: number) => {
                    const act = Serializer.objectify(actionTrace[1].act);
                    act.data = null;
                    if (this.allowedContracts.has(act.account)) {
                        expectedTraces++;
                        this.dsPool[j].postMessage({
                            event: 'action',
                            data: {
                                index,
                                act,
                                serializedData: actionTrace[1].act.data.array
                            }
                        });
                        j++;
                        if (j > this.dsPool.length - 1) {
                            j = 0;
                        }
                    }
                })
            }

            promises.push(this.collectTraces(actArray, expectedTraces));

            await Promise.all(promises);

            this.events.emit('block', {
                ...blockInfo,
                ...blockHeader,
                acts: actArray,
                contractRows: deltaRows
            });

            /*
            console.log('--------------');
            const decodeTime = process.hrtime.bigint() - tRef;
            console.log('Total Processing Time:', Number(decodeTime) / 1000000, 'ms')
            console.log(`Block ${blockInfo.head.block_num} | TRX: ${transactions.length} | Action Traces: ${actArray.length} | Deltas: ${deltaRows.length}`);
            const relativeTimePerBlockData = (Number(decodeTime / BigInt(1000)) / (transactions.length + actArray.length + deltaRows.length)).toFixed(2);
            console.log('Relative Processing Speed:', relativeTimePerBlockData, 'us/object');
             */

        }
    }

    private async collectTraces(actArray: any[], expectedLength: number) {
        //console.log(`Waiting for ${expectedLength} traces to be collected...`);
        await new Promise<void>((resolve) => {
            if (expectedLength === 0) {
                resolve();
                return;
            }
            this.traceCollector = (value) => {
                actArray.push(value.decodedAct);
                if (actArray.length === expectedLength) {
                    resolve();
                }
            };
        });
        actArray.sort((a, b) => a.index - b.index);
        //console.log(actArray.map(a => a.index));
    }

    private async collectDeltas(deltaRows: any[], expectedLength: number) {
        await new Promise<void>((resolve) => {
            if (expectedLength === 0) {
                resolve();
                return;
            }
            this.deltaCollector = (value) => {
                deltaRows.push(value.decodedDeltaRow);
                if (deltaRows.length === expectedLength) {
                    resolve();
                }
            };
        });
        deltaRows.sort((a, b) => a.index - b.index);
    }

    createWorkers(param: { poolSize: number }) {
        for (let i = 0; i < param.poolSize; i++) {
            const w = new Worker(path.resolve('dist/ds-worker.js'), {
                workerData: {
                    wIndex: i
                }
            });
            w.on("message", value => {
                this.handleWorkerMessage(value);
            });
            this.dsPool.push(w);
        }
    }

    private handleWorkerMessage(value: any) {
        switch (value.event) {
            case 'decoded_delta': {
                if (this.deltaCollector) {
                    this.deltaCollector(value);
                }
                break;
            }
            case 'decoded_action': {
                if (this.traceCollector) {
                    this.traceCollector(value);
                }
            }
        }
    }

    addContract(account: string, abi: ABI) {
        this.allowedContracts.set(account, abi);
        this.dsPool.forEach(value => {
            value.postMessage({
                event: 'set_abi',
                data: {account, abi: Serializer.objectify(abi)}
            });
        });
    }

    ack() {
        this.ackBlockRange(1);
    }
}
