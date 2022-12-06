const fetch = require('node-fetch');
import { EventHubProducerClient } from "@azure/event-hubs";
const { BlobServiceClient } = require('@azure/storage-blob');
import { ethers } from "ethers";

const abi = JSON.parse('[{"inputs":[],"stateMutability":"nonpayable","type":"constructor"},{"anonymous":false,"inputs":[{"indexed":false,"internalType":"uint256","name":"id","type":"uint256"},{"indexed":true,"internalType":"address","name":"from","type":"address"},{"indexed":true,"internalType":"address","name":"to","type":"address"},{"indexed":false,"internalType":"string","name":"article","type":"string"},{"indexed":false,"internalType":"string","name":"currency","type":"string"},{"indexed":false,"internalType":"string","name":"amount","type":"string"}],"name":"IssueInvoice","type":"event"},{"anonymous":false,"inputs":[{"indexed":false,"internalType":"uint256","name":"_id","type":"uint256"}],"name":"ItemUpdated","type":"event"},{"stateMutability":"payable","type":"fallback"},{"inputs":[{"internalType":"address","name":"","type":"address"}],"name":"accountantList","outputs":[{"internalType":"bool","name":"","type":"bool"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"adr","type":"address"}],"name":"addAccountant","outputs":[{"internalType":"address","name":"","type":"address"}],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"adr","type":"address"}],"name":"addSource","outputs":[{"internalType":"address","name":"","type":"address"}],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"","type":"address"}],"name":"fromList","outputs":[{"internalType":"bool","name":"","type":"bool"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"string","name":"name","type":"string"},{"internalType":"string","name":"amount","type":"string"},{"internalType":"string","name":"currency","type":"string"},{"internalType":"address","name":"from","type":"address"},{"internalType":"address","name":"to","type":"address"}],"name":"generate","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"uint256","name":"id","type":"uint256"}],"name":"getId","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"uint256","name":"id","type":"uint256"}],"name":"getItem","outputs":[{"components":[{"internalType":"uint256","name":"id","type":"uint256"},{"internalType":"uint64","name":"status","type":"uint64"},{"internalType":"string","name":"name","type":"string"},{"internalType":"string","name":"amount","type":"string"},{"internalType":"address","name":"from","type":"address"},{"internalType":"address","name":"to","type":"address"},{"internalType":"address","name":"accountant","type":"address"},{"internalType":"string","name":"receipt","type":"string"},{"internalType":"string","name":"currency","type":"string"},{"internalType":"address","name":"source","type":"address"}],"internalType":"struct InvoiceWorkflow.Invoice","name":"","type":"tuple"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"uint256","name":"cnt","type":"uint256"}],"name":"getLatest","outputs":[{"components":[{"internalType":"uint256","name":"id","type":"uint256"},{"internalType":"uint64","name":"status","type":"uint64"},{"internalType":"string","name":"name","type":"string"},{"internalType":"string","name":"amount","type":"string"},{"internalType":"address","name":"from","type":"address"},{"internalType":"address","name":"to","type":"address"},{"internalType":"address","name":"accountant","type":"address"},{"internalType":"string","name":"receipt","type":"string"},{"internalType":"string","name":"currency","type":"string"},{"internalType":"address","name":"source","type":"address"}],"internalType":"struct InvoiceWorkflow.Invoice[]","name":"","type":"tuple[]"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"uint256","name":"id","type":"uint256"}],"name":"getName","outputs":[{"internalType":"string","name":"","type":"string"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"uint256","name":"cursor","type":"uint256"},{"internalType":"uint256","name":"howMany","type":"uint256"},{"internalType":"bool","name":"onlyMine","type":"bool"}],"name":"getPage","outputs":[{"components":[{"internalType":"uint256","name":"id","type":"uint256"},{"internalType":"uint64","name":"status","type":"uint64"},{"internalType":"string","name":"name","type":"string"},{"internalType":"string","name":"amount","type":"string"},{"internalType":"address","name":"from","type":"address"},{"internalType":"address","name":"to","type":"address"},{"internalType":"address","name":"accountant","type":"address"},{"internalType":"string","name":"receipt","type":"string"},{"internalType":"string","name":"currency","type":"string"},{"internalType":"address","name":"source","type":"address"}],"internalType":"struct InvoiceWorkflow.Invoice[]","name":"","type":"tuple[]"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"uint256","name":"id","type":"uint256"}],"name":"getStatus","outputs":[{"internalType":"uint64","name":"","type":"uint64"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"uint256","name":"","type":"uint256"}],"name":"items","outputs":[{"internalType":"uint256","name":"id","type":"uint256"},{"internalType":"uint64","name":"status","type":"uint64"},{"internalType":"string","name":"name","type":"string"},{"internalType":"string","name":"amount","type":"string"},{"internalType":"address","name":"from","type":"address"},{"internalType":"address","name":"to","type":"address"},{"internalType":"address","name":"accountant","type":"address"},{"internalType":"string","name":"receipt","type":"string"},{"internalType":"string","name":"currency","type":"string"},{"internalType":"address","name":"source","type":"address"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"owner","outputs":[{"internalType":"address","name":"","type":"address"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"uint256","name":"id","type":"uint256"},{"internalType":"string","name":"receipt","type":"string"},{"internalType":"uint256","name":"processFee","type":"uint256"}],"name":"process","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"string","name":"name","type":"string"},{"internalType":"string","name":"amount","type":"string"},{"internalType":"string","name":"currency","type":"string"},{"internalType":"address","name":"from","type":"address"},{"internalType":"address","name":"to","type":"address"},{"internalType":"string","name":"receipt","type":"string"},{"internalType":"uint256","name":"processFee","type":"uint256"}],"name":"processExternal","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"","type":"address"}],"name":"sourceList","outputs":[{"internalType":"bool","name":"","type":"bool"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"","type":"address"}],"name":"toList","outputs":[{"internalType":"bool","name":"","type":"bool"}],"stateMutability":"view","type":"function"},{"stateMutability":"payable","type":"receive"}]');
const iface = new ethers.utils.Interface(abi);

var AZURE_STORAGE_CONNECTION_STRING = 
    process.env.AZURE_STORAGE_CONNECTION_STRING;
var AZURE_EVENTHUB_CONNECTION_STRING = 
    process.env.AZURE_EVENTHUB_CONNECTION_STRING;
var COVALENT_API_KEY = 
    process.env.COVALENT_API_KEY;

const producerClient = new EventHubProducerClient(AZURE_EVENTHUB_CONNECTION_STRING, "invoiceevent");

const blobServiceClient = BlobServiceClient.fromConnectionString(AZURE_STORAGE_CONNECTION_STRING);
const containerClient = blobServiceClient.getContainerClient("evmoscursor");

const blockBlobClient = containerClient.getBlockBlobClient("cursor");
const blobClient = containerClient.getBlobClient("cursor");

let blockHeights = {};
let lastSnapshots = {};

const setTimeoutPromise = timeout => new Promise(resolve => {        
    setTimeout(resolve, timeout);
  });

async function saveLatestBlock(chain){
    if (lastSnapshots[chain] > new Date(new Date().getTime() - 5 * 60000))
    {
      return;
    }
    try {
        var response = await fetch('https://api.covalenthq.com/v1/9000/block_v2/latest/?key=' + COVALENT_API_KEY);
        var resObj = await response.json();
        if (resObj.error)
        {
            console.log(resObj.error_message);
            //try later
            return;
        }
        await storeCursor(chain, resObj.data.items[0].height - 10);
        //pause for rate limiter
        await setTimeoutPromise(2000);
    }catch (error) {
        console.error(`Error: ${error.message}`);
    }
}

async function getLatestEvents() : Promise<boolean> {
    try{
        await saveLatestBlock("evmos");
        try{
            var response = await fetch('https://api.covalenthq.com/v1/9000/events/topics/0xb26d28972f7ddc50316da1f00018250b4c08e151b21666f77a3e827bb48afb34/?starting-block=' + (blockHeights["evmos"] + 1) + '&ending-block=latest&key=' + COVALENT_API_KEY);
            var resObj = await response.json();
            if (resObj.error)
            {
                console.log(resObj.error_message);
                return false;
            }
            let output = [];
            let latestBlockHeight = 0;
            resObj.data.items.forEach((item:any) => {

                var logi = iface.parseLog({ data: item.raw_log_data, topics: item.raw_log_topics });
                let id = Number(logi.args.id.toString());
                let from = logi.args.from;
                let to = logi.args.to;
                let article = logi.args.article;
                let currency = logi.args.currency;
                let amount = logi.args.amount;
                output.push({
                    contract: item.sender_address,
                    from: from,
                    to : to,
                    article : article,
                    receiptId : item.tx_hash,
                    id : id,
                    amount : amount,
                    currency : currency
                  });
                latestBlockHeight = item.block_height;
            });
            if (output.length) {
                console.log(`We caught fresh invoices!`)    
                const eventDataBatch = await producerClient.createBatch();
                for (let invoice of output)
                {
                eventDataBatch.tryAdd({ body: invoice });
                }
                await producerClient.sendBatch(eventDataBatch);
                if (latestBlockHeight > 0)
                {
                    await storeCursor("evmos", latestBlockHeight);
                }
            }
        }
        catch(error)
        {
            console.error(`Error: ${error.message}`);
            return false;
        }
    }catch(err){
        console.log("Error: " + err);
        return false;
    }
    return true;
}
(async () => {
  
    try{
        await containerClient.createIfNotExists();
        const downloadBlockBlobResponse = await blobClient.download();
        blockHeights["evmos"] = Number((await streamToBuffer(downloadBlockBlobResponse.readableStreamBody)).toString());
        lastSnapshots["evmos"] = new Date(new Date().getTime() - 2 * 60000);
        console.log("Downloaded evmos cursor with content:", blockHeights["evmos"]);    
    }
    catch(ex)
    {
      if (ex.details.errorCode == "BlobNotFound")
      {
        await saveLatestBlock("evmos");
        console.log("No cursor found, using default");
      }
      else
      {
        throw ex;
      }
    }
    try{
        while (true){
            var response = await getLatestEvents();
            await setTimeoutPromise(response ? 2000 : 5000);
        }
    }
    catch(ex)
    {
      console.error(ex);
    }
    producerClient.close();
  })().catch( e => { console.error(e) });

  
async function storeCursor(chain, cursor) {
    if (blockHeights[chain] >= cursor)
    {
        console.log("not storing older cursor");
        return;
    }
    console.log("Storing " + chain + " cursor at ", cursor);
    var data = (cursor).toString();
    await blockBlobClient.upload(data, data.length);
    blockHeights[chain] = cursor;
    lastSnapshots[chain] = new Date();
    console.log(chain + " cursor stored");
  }

async function streamToBuffer(readableStream) {
    return new Promise((resolve, reject) => {
      const chunks = [];
      readableStream.on("data", (data) => {
        chunks.push(data instanceof Buffer ? data : Buffer.from(data));
      });
      readableStream.on("end", () => {
        resolve(Buffer.concat(chunks));
      });
      readableStream.on("error", reject);
    });
  }
  