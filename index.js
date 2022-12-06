"use strict";
var __awaiter = (this && this.__awaiter) || function (thisArg, _arguments, P, generator) {
    function adopt(value) { return value instanceof P ? value : new P(function (resolve) { resolve(value); }); }
    return new (P || (P = Promise))(function (resolve, reject) {
        function fulfilled(value) { try { step(generator.next(value)); } catch (e) { reject(e); } }
        function rejected(value) { try { step(generator["throw"](value)); } catch (e) { reject(e); } }
        function step(result) { result.done ? resolve(result.value) : adopt(result.value).then(fulfilled, rejected); }
        step((generator = generator.apply(thisArg, _arguments || [])).next());
    });
};
var __generator = (this && this.__generator) || function (thisArg, body) {
    var _ = { label: 0, sent: function() { if (t[0] & 1) throw t[1]; return t[1]; }, trys: [], ops: [] }, f, y, t, g;
    return g = { next: verb(0), "throw": verb(1), "return": verb(2) }, typeof Symbol === "function" && (g[Symbol.iterator] = function() { return this; }), g;
    function verb(n) { return function (v) { return step([n, v]); }; }
    function step(op) {
        if (f) throw new TypeError("Generator is already executing.");
        while (g && (g = 0, op[0] && (_ = 0)), _) try {
            if (f = 1, y && (t = op[0] & 2 ? y["return"] : op[0] ? y["throw"] || ((t = y["return"]) && t.call(y), 0) : y.next) && !(t = t.call(y, op[1])).done) return t;
            if (y = 0, t) op = [op[0] & 2, t.value];
            switch (op[0]) {
                case 0: case 1: t = op; break;
                case 4: _.label++; return { value: op[1], done: false };
                case 5: _.label++; y = op[1]; op = [0]; continue;
                case 7: op = _.ops.pop(); _.trys.pop(); continue;
                default:
                    if (!(t = _.trys, t = t.length > 0 && t[t.length - 1]) && (op[0] === 6 || op[0] === 2)) { _ = 0; continue; }
                    if (op[0] === 3 && (!t || (op[1] > t[0] && op[1] < t[3]))) { _.label = op[1]; break; }
                    if (op[0] === 6 && _.label < t[1]) { _.label = t[1]; t = op; break; }
                    if (t && _.label < t[2]) { _.label = t[2]; _.ops.push(op); break; }
                    if (t[2]) _.ops.pop();
                    _.trys.pop(); continue;
            }
            op = body.call(thisArg, _);
        } catch (e) { op = [6, e]; y = 0; } finally { f = t = 0; }
        if (op[0] & 5) throw op[1]; return { value: op[0] ? op[1] : void 0, done: true };
    }
};
exports.__esModule = true;
var fetch = require('node-fetch');
var event_hubs_1 = require("@azure/event-hubs");
var BlobServiceClient = require('@azure/storage-blob').BlobServiceClient;
var ethers_1 = require("ethers");
var abi = JSON.parse('[{"inputs":[],"stateMutability":"nonpayable","type":"constructor"},{"anonymous":false,"inputs":[{"indexed":false,"internalType":"uint256","name":"id","type":"uint256"},{"indexed":true,"internalType":"address","name":"from","type":"address"},{"indexed":true,"internalType":"address","name":"to","type":"address"},{"indexed":false,"internalType":"string","name":"article","type":"string"},{"indexed":false,"internalType":"string","name":"currency","type":"string"},{"indexed":false,"internalType":"string","name":"amount","type":"string"}],"name":"IssueInvoice","type":"event"},{"anonymous":false,"inputs":[{"indexed":false,"internalType":"uint256","name":"_id","type":"uint256"}],"name":"ItemUpdated","type":"event"},{"stateMutability":"payable","type":"fallback"},{"inputs":[{"internalType":"address","name":"","type":"address"}],"name":"accountantList","outputs":[{"internalType":"bool","name":"","type":"bool"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"adr","type":"address"}],"name":"addAccountant","outputs":[{"internalType":"address","name":"","type":"address"}],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"adr","type":"address"}],"name":"addSource","outputs":[{"internalType":"address","name":"","type":"address"}],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"","type":"address"}],"name":"fromList","outputs":[{"internalType":"bool","name":"","type":"bool"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"string","name":"name","type":"string"},{"internalType":"string","name":"amount","type":"string"},{"internalType":"string","name":"currency","type":"string"},{"internalType":"address","name":"from","type":"address"},{"internalType":"address","name":"to","type":"address"}],"name":"generate","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"uint256","name":"id","type":"uint256"}],"name":"getId","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"uint256","name":"id","type":"uint256"}],"name":"getItem","outputs":[{"components":[{"internalType":"uint256","name":"id","type":"uint256"},{"internalType":"uint64","name":"status","type":"uint64"},{"internalType":"string","name":"name","type":"string"},{"internalType":"string","name":"amount","type":"string"},{"internalType":"address","name":"from","type":"address"},{"internalType":"address","name":"to","type":"address"},{"internalType":"address","name":"accountant","type":"address"},{"internalType":"string","name":"receipt","type":"string"},{"internalType":"string","name":"currency","type":"string"},{"internalType":"address","name":"source","type":"address"}],"internalType":"struct InvoiceWorkflow.Invoice","name":"","type":"tuple"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"uint256","name":"cnt","type":"uint256"}],"name":"getLatest","outputs":[{"components":[{"internalType":"uint256","name":"id","type":"uint256"},{"internalType":"uint64","name":"status","type":"uint64"},{"internalType":"string","name":"name","type":"string"},{"internalType":"string","name":"amount","type":"string"},{"internalType":"address","name":"from","type":"address"},{"internalType":"address","name":"to","type":"address"},{"internalType":"address","name":"accountant","type":"address"},{"internalType":"string","name":"receipt","type":"string"},{"internalType":"string","name":"currency","type":"string"},{"internalType":"address","name":"source","type":"address"}],"internalType":"struct InvoiceWorkflow.Invoice[]","name":"","type":"tuple[]"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"uint256","name":"id","type":"uint256"}],"name":"getName","outputs":[{"internalType":"string","name":"","type":"string"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"uint256","name":"cursor","type":"uint256"},{"internalType":"uint256","name":"howMany","type":"uint256"},{"internalType":"bool","name":"onlyMine","type":"bool"}],"name":"getPage","outputs":[{"components":[{"internalType":"uint256","name":"id","type":"uint256"},{"internalType":"uint64","name":"status","type":"uint64"},{"internalType":"string","name":"name","type":"string"},{"internalType":"string","name":"amount","type":"string"},{"internalType":"address","name":"from","type":"address"},{"internalType":"address","name":"to","type":"address"},{"internalType":"address","name":"accountant","type":"address"},{"internalType":"string","name":"receipt","type":"string"},{"internalType":"string","name":"currency","type":"string"},{"internalType":"address","name":"source","type":"address"}],"internalType":"struct InvoiceWorkflow.Invoice[]","name":"","type":"tuple[]"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"uint256","name":"id","type":"uint256"}],"name":"getStatus","outputs":[{"internalType":"uint64","name":"","type":"uint64"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"uint256","name":"","type":"uint256"}],"name":"items","outputs":[{"internalType":"uint256","name":"id","type":"uint256"},{"internalType":"uint64","name":"status","type":"uint64"},{"internalType":"string","name":"name","type":"string"},{"internalType":"string","name":"amount","type":"string"},{"internalType":"address","name":"from","type":"address"},{"internalType":"address","name":"to","type":"address"},{"internalType":"address","name":"accountant","type":"address"},{"internalType":"string","name":"receipt","type":"string"},{"internalType":"string","name":"currency","type":"string"},{"internalType":"address","name":"source","type":"address"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"owner","outputs":[{"internalType":"address","name":"","type":"address"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"uint256","name":"id","type":"uint256"},{"internalType":"string","name":"receipt","type":"string"},{"internalType":"uint256","name":"processFee","type":"uint256"}],"name":"process","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"string","name":"name","type":"string"},{"internalType":"string","name":"amount","type":"string"},{"internalType":"string","name":"currency","type":"string"},{"internalType":"address","name":"from","type":"address"},{"internalType":"address","name":"to","type":"address"},{"internalType":"string","name":"receipt","type":"string"},{"internalType":"uint256","name":"processFee","type":"uint256"}],"name":"processExternal","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"","type":"address"}],"name":"sourceList","outputs":[{"internalType":"bool","name":"","type":"bool"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"","type":"address"}],"name":"toList","outputs":[{"internalType":"bool","name":"","type":"bool"}],"stateMutability":"view","type":"function"},{"stateMutability":"payable","type":"receive"}]');
var iface = new ethers_1.ethers.utils.Interface(abi);
var AZURE_STORAGE_CONNECTION_STRING = process.env.AZURE_STORAGE_CONNECTION_STRING;
var AZURE_EVENTHUB_CONNECTION_STRING = process.env.AZURE_EVENTHUB_CONNECTION_STRING;
var COVALENT_API_KEY = process.env.COVALENT_API_KEY;
var producerClient = new event_hubs_1.EventHubProducerClient(AZURE_EVENTHUB_CONNECTION_STRING, "invoiceevent");
var blobServiceClient = BlobServiceClient.fromConnectionString(AZURE_STORAGE_CONNECTION_STRING);
var containerClient = blobServiceClient.getContainerClient("evmoscursor");
var blockBlobClient = containerClient.getBlockBlobClient("cursor");
var blobClient = containerClient.getBlobClient("cursor");
var blockHeights = {};
var lastSnapshots = {};
var setTimeoutPromise = function (timeout) { return new Promise(function (resolve) {
    setTimeout(resolve, timeout);
}); };
function saveLatestBlock(chain) {
    return __awaiter(this, void 0, void 0, function () {
        var response, resObj, error_1;
        return __generator(this, function (_a) {
            switch (_a.label) {
                case 0:
                    if (lastSnapshots[chain] > new Date(new Date().getTime() - 5 * 60000)) {
                        return [2 /*return*/];
                    }
                    _a.label = 1;
                case 1:
                    _a.trys.push([1, 6, , 7]);
                    return [4 /*yield*/, fetch('https://api.covalenthq.com/v1/9000/block_v2/latest/?key=' + COVALENT_API_KEY)];
                case 2:
                    response = _a.sent();
                    return [4 /*yield*/, response.json()];
                case 3:
                    resObj = _a.sent();
                    if (resObj.error) {
                        console.log(resObj.error_message);
                        //try later
                        return [2 /*return*/];
                    }
                    return [4 /*yield*/, storeCursor(chain, resObj.data.items[0].height - 10)];
                case 4:
                    _a.sent();
                    //pause for rate limiter
                    return [4 /*yield*/, setTimeoutPromise(2000)];
                case 5:
                    //pause for rate limiter
                    _a.sent();
                    return [3 /*break*/, 7];
                case 6:
                    error_1 = _a.sent();
                    console.error("Error: ".concat(error_1.message));
                    return [3 /*break*/, 7];
                case 7: return [2 /*return*/];
            }
        });
    });
}
function getLatestEvents() {
    return __awaiter(this, void 0, void 0, function () {
        var response, resObj, output_2, latestBlockHeight_1, eventDataBatch, _i, output_1, invoice, error_2, err_1;
        return __generator(this, function (_a) {
            switch (_a.label) {
                case 0:
                    _a.trys.push([0, 11, , 12]);
                    return [4 /*yield*/, saveLatestBlock("evmos")];
                case 1:
                    _a.sent();
                    _a.label = 2;
                case 2:
                    _a.trys.push([2, 9, , 10]);
                    return [4 /*yield*/, fetch('https://api.covalenthq.com/v1/9000/events/topics/0xb26d28972f7ddc50316da1f00018250b4c08e151b21666f77a3e827bb48afb34/?starting-block=' + (blockHeights["evmos"] + 1) + '&ending-block=latest&key=' + COVALENT_API_KEY)];
                case 3:
                    response = _a.sent();
                    return [4 /*yield*/, response.json()];
                case 4:
                    resObj = _a.sent();
                    if (resObj.error) {
                        console.log(resObj.error_message);
                        return [2 /*return*/, false];
                    }
                    output_2 = [];
                    latestBlockHeight_1 = 0;
                    resObj.data.items.forEach(function (item) {
                        var logi = iface.parseLog({ data: item.raw_log_data, topics: item.raw_log_topics });
                        var id = Number(logi.args.id.toString());
                        var from = logi.args.from;
                        var to = logi.args.to;
                        var article = logi.args.article;
                        var currency = logi.args.currency;
                        var amount = logi.args.amount;
                        output_2.push({
                            contract: item.sender_address,
                            from: from,
                            to: to,
                            article: article,
                            receiptId: item.tx_hash,
                            id: id,
                            amount: amount,
                            currency: currency
                        });
                        latestBlockHeight_1 = item.block_height;
                    });
                    if (!output_2.length) return [3 /*break*/, 8];
                    console.log("We caught fresh invoices!");
                    return [4 /*yield*/, producerClient.createBatch()];
                case 5:
                    eventDataBatch = _a.sent();
                    for (_i = 0, output_1 = output_2; _i < output_1.length; _i++) {
                        invoice = output_1[_i];
                        eventDataBatch.tryAdd({ body: invoice });
                    }
                    return [4 /*yield*/, producerClient.sendBatch(eventDataBatch)];
                case 6:
                    _a.sent();
                    if (!(latestBlockHeight_1 > 0)) return [3 /*break*/, 8];
                    return [4 /*yield*/, storeCursor("evmos", latestBlockHeight_1)];
                case 7:
                    _a.sent();
                    _a.label = 8;
                case 8: return [3 /*break*/, 10];
                case 9:
                    error_2 = _a.sent();
                    console.error("Error: ".concat(error_2.message));
                    return [2 /*return*/, false];
                case 10: return [3 /*break*/, 12];
                case 11:
                    err_1 = _a.sent();
                    console.log("Error: " + err_1);
                    return [2 /*return*/, false];
                case 12: return [2 /*return*/, true];
            }
        });
    });
}
(function () { return __awaiter(void 0, void 0, void 0, function () {
    var downloadBlockBlobResponse, _a, _b, _c, ex_1, response, ex_2;
    return __generator(this, function (_d) {
        switch (_d.label) {
            case 0:
                _d.trys.push([0, 4, , 8]);
                return [4 /*yield*/, containerClient.createIfNotExists()];
            case 1:
                _d.sent();
                return [4 /*yield*/, blobClient.download()];
            case 2:
                downloadBlockBlobResponse = _d.sent();
                _a = blockHeights;
                _b = "evmos";
                _c = Number;
                return [4 /*yield*/, streamToBuffer(downloadBlockBlobResponse.readableStreamBody)];
            case 3:
                _a[_b] = _c.apply(void 0, [(_d.sent()).toString()]);
                lastSnapshots["evmos"] = new Date(new Date().getTime() - 2 * 60000);
                console.log("Downloaded evmos cursor with content:", blockHeights["evmos"]);
                return [3 /*break*/, 8];
            case 4:
                ex_1 = _d.sent();
                if (!(ex_1.details.errorCode == "BlobNotFound")) return [3 /*break*/, 6];
                return [4 /*yield*/, saveLatestBlock("evmos")];
            case 5:
                _d.sent();
                console.log("No cursor found, using default");
                return [3 /*break*/, 7];
            case 6: throw ex_1;
            case 7: return [3 /*break*/, 8];
            case 8:
                _d.trys.push([8, 13, , 14]);
                _d.label = 9;
            case 9:
                if (!true) return [3 /*break*/, 12];
                return [4 /*yield*/, getLatestEvents()];
            case 10:
                response = _d.sent();
                return [4 /*yield*/, setTimeoutPromise(response ? 2000 : 5000)];
            case 11:
                _d.sent();
                return [3 /*break*/, 9];
            case 12: return [3 /*break*/, 14];
            case 13:
                ex_2 = _d.sent();
                console.error(ex_2);
                return [3 /*break*/, 14];
            case 14:
                producerClient.close();
                return [2 /*return*/];
        }
    });
}); })()["catch"](function (e) { console.error(e); });
function storeCursor(chain, cursor) {
    return __awaiter(this, void 0, void 0, function () {
        var data;
        return __generator(this, function (_a) {
            switch (_a.label) {
                case 0:
                    if (blockHeights[chain] >= cursor) {
                        console.log("not storing older cursor");
                        return [2 /*return*/];
                    }
                    console.log("Storing " + chain + " cursor at ", cursor);
                    data = (cursor).toString();
                    return [4 /*yield*/, blockBlobClient.upload(data, data.length)];
                case 1:
                    _a.sent();
                    blockHeights[chain] = cursor;
                    lastSnapshots[chain] = new Date();
                    console.log(chain + " cursor stored");
                    return [2 /*return*/];
            }
        });
    });
}
function streamToBuffer(readableStream) {
    return __awaiter(this, void 0, void 0, function () {
        return __generator(this, function (_a) {
            return [2 /*return*/, new Promise(function (resolve, reject) {
                    var chunks = [];
                    readableStream.on("data", function (data) {
                        chunks.push(data instanceof Buffer ? data : Buffer.from(data));
                    });
                    readableStream.on("end", function () {
                        resolve(Buffer.concat(chunks));
                    });
                    readableStream.on("error", reject);
                })];
        });
    });
}
