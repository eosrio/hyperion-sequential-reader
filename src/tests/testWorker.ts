import {MemoryBounds, SharedObjectStore} from "../shm.js";
import {ABI} from "@greymass/eosio";
import workerpool from "workerpool";

function sosFetchObject(args: {
    sharedMem: SharedArrayBuffer;
    memoryMap: {[key: string]: MemoryBounds};
    key: string
}): ABI.Def {
    return SharedObjectStore.fromMemoryMap<ABI.Def>(
        args.sharedMem, args.memoryMap
    ).get(args.key);
};


workerpool.worker({sosFetchObject})