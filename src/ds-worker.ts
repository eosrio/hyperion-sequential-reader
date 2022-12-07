import {parentPort, workerData} from "worker_threads";
import {ABI, Serializer} from "@greymass/eosio";

const contracts: Map<string, ABI> = new Map();
let shipAbi: ABI;

parentPort.on("message", value => {
    const data = value.data;
    switch (value.event) {
        case 'set_abi': {
            contracts.set(value.data.account, value.data.abi);
            break;
        }
        case 'set_ship_abi': {
            shipAbi = value.data.abi;
            break;
        }
        case 'delta': {
            const deltaRow = Serializer.decode({
                data: data.data,
                type: 'contract_row',
                abi: shipAbi
            });
            const decodedDeltaRow = {
                index: data.index,
                present: data.present,
                ...Serializer.objectify(deltaRow[1])
            };
            parentPort.postMessage({
                event: 'decoded_delta',
                wIndex: workerData.wIndex,
                decodedDeltaRow
            });
            break;
        }
        case 'action': {
            const act = data.act;
            const serializedData = data.serializedData;
            const abi = contracts.get(act.account);
            if (abi) {
                try {
                    act.data = Serializer.decode({
                        data: serializedData,
                        type: act.name,
                        abi
                    });
                    parentPort.postMessage({
                        event: 'decoded_action',
                        wIndex: workerData.wIndex,
                        decodedAct: {
                            index: data.index,
                            ...act
                        }
                    });
                } catch (e) {
                    console.log(e.message, act);
                }
            }
            break;
        }
    }
});
