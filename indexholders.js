const fs = require("fs");
const path = require("path");
const { ethers } = require("ethers");

/////////////////////////////
// CONFIG
/////////////////////////////

const RPC_URL = "https://arc-testnet.g.alchemy.com/v2/o1k50yOLGXHrczBA8KDOf";

const NFT_CONTRACT = "0x9e05c6075f9e890fc515ef86091414c77036f8fa";

const NFT_CREATION_BLOCK = 9435462;

const BLOCK_BATCH = 2000;

// How many balanceOf calls to fire in parallel
const CONCURRENCY = 20;

const OUTPUT_FILE = path.resolve(__dirname, "holders.csv");

const TRANSFER_TOPIC = ethers.id("Transfer(address,address,uint256)");
const ZERO_ADDR = "0x0000000000000000000000000000000000000000";

/////////////////////////////
// PROVIDER + CONTRACT
/////////////////////////////

const provider = new ethers.JsonRpcProvider(RPC_URL);

const contract = new ethers.Contract(
    NFT_CONTRACT,
    ["function balanceOf(address) view returns (uint256)"],
    provider
);

/////////////////////////////
// SAFE LOG FETCH (auto-splits on RPC range errors)
/////////////////////////////

async function getLogs(fromBlock, toBlock) {
    try {
        return await provider.getLogs({
            address: NFT_CONTRACT,
            fromBlock,
            toBlock,
            topics: [TRANSFER_TOPIC]
        });
    } catch (err) {
        if (fromBlock === toBlock) throw err;
        const mid = Math.floor((fromBlock + toBlock) / 2);
        const [left, right] = await Promise.all([
            getLogs(fromBlock, mid),
            getLogs(mid + 1, toBlock)
        ]);
        return [...left, ...right];
    }
}

/////////////////////////////
// MAIN
/////////////////////////////

async function main() {

    const latestBlock = process.argv[2]
        ? Number(process.argv[2])
        : await provider.getBlockNumber();

    console.log(`Scanning blocks ${NFT_CREATION_BLOCK} -> ${latestBlock}`);

    /////////////////////////////
    // 1. COLLECT ALL ADDRESSES THAT EVER TOUCHED THE CONTRACT
    //    Both senders and receivers from every Transfer event.
    //    This is the complete set of wallets that could hold tokens.
    /////////////////////////////

    const addresses = new Set();

    for (
        let from = NFT_CREATION_BLOCK;
        from <= latestBlock;
        from += BLOCK_BATCH
    ) {
        const to = Math.min(from + BLOCK_BATCH - 1, latestBlock);
        process.stdout.write(`\rFetching logs ${from} -> ${to}   `);

        const logs = await getLogs(from, to);

        for (const log of logs) {
            const fromAddr = "0x" + log.topics[1].slice(26).toLowerCase();
            const toAddr   = "0x" + log.topics[2].slice(26).toLowerCase();

            // Exclude zero address (mint source / burn destination)
            if (fromAddr !== ZERO_ADDR) addresses.add(fromAddr);
            if (toAddr   !== ZERO_ADDR) addresses.add(toAddr);
        }
    }

    console.log(`\nUnique addresses found: ${addresses.size}`);

    /////////////////////////////
    // 2. QUERY balanceOf() FOR EVERY ADDRESS SIMULTANEOUSLY
    //    balanceOf() is the exact value stored on-chain -- same as ARC Scan.
    //    Print each result immediately as it comes back.
    //    Run CONCURRENCY workers in parallel for speed.
    /////////////////////////////

    const holders = [];
    const addrList = Array.from(addresses);
    let idx = 0;

    // Open CSV and write header immediately
    const writeStream = fs.createWriteStream(OUTPUT_FILE);
    writeStream.write("address,balance\n");

    async function worker() {
        while (idx < addrList.length) {
            const addr = addrList[idx++];

            try {
                const bal = await contract.balanceOf(addr);

                if (bal > 0n) {
                    // Print immediately as each result arrives
                    console.log(`${addr}  =>  ${bal.toString()}`);
                    holders.push([addr, bal]);
                }
            } catch {
                // RPC error for this address -- skip
            }
        }
    }

    // Fire all workers simultaneously
    await Promise.all(Array.from({ length: CONCURRENCY }, () => worker()));

    /////////////////////////////
    // 3. SORT: highest balance first, then alphabetical
    /////////////////////////////

    holders.sort(([addrA, balA], [addrB, balB]) => {
        if (balB !== balA) return Number(balB - balA);
        return addrA.localeCompare(addrB);
    });

    /////////////////////////////
    // 4. WRITE SORTED CSV
    /////////////////////////////

    for (const [addr, bal] of holders) {
        writeStream.write(`${addr},${bal}\n`);
    }

    await new Promise(resolve => writeStream.end(resolve));

    console.log(`\nSaved: ${OUTPUT_FILE}`);
    console.log(`Total holders: ${holders.length}`);
}

main().catch(console.error);
