import { createLogger, format, transports } from 'winston';
import { Db, MongoClient, ObjectId } from 'mongodb';
import { FtxUsClient } from 'ccxws';
const { combine, timestamp, printf } = format;

// tslint:disable-next-line:no-shadowed-variable
const myFormat = printf(({ level, message, timestamp }) => {
    return `${timestamp} ${level}: ${message}`;
});

const logger = createLogger({
    format: combine(
        timestamp(),
        myFormat
    ),
    transports: [new transports.Console()]
});

// eslint-disable-next-line @typescript-eslint/no-var-requires
logger.info('Initializing.');
require('dotenv').config();
const exchangeName = 'ftxus';
const domain = 'ftx.us';
const mongodbUrl = process.env.MONGODBURL ? process.env.MONGODBURL : '';
const mongodbName = process.env.MONGODBNAME ? process.env.MONGODBNAME : '';
const timeframeList = process.env.TIMEFRAMES ? process.env.TIMEFRAMES : '';
const timeframeNameList = process.env.TIMEFRAMENAMES ? process.env.TIMEFRAMENAMES : '';
const enableLog = process.env.ENABLELOG ? process.env.ENABLELOG === "true" : false;
let db: Db;
let exchangeId: ObjectId | undefined;
let heartbeat: Date = new Date();
let streaming: boolean = false;

interface HashTable<T> {
    [key: string]: T
}

enum Type {
    FOREX = 'Forex',
    CRYPTO = 'Crypto',
    STOCK = 'STOCK'
}

interface Instrument {
    _id: ObjectId | undefined,
    symbol: string,
    type: Type,
    exchange: string,
    marketSymbol: string,
    pricePrecision: number,
    quantityPrecision: number,
    minimumNotional: number | null,
    maximumNotional: number | null,
    minimumQuantity: number | null,
    maximumQuantity: number | null
    exchangeId: ObjectId | undefined
}

interface Market {
    _id: ObjectId | undefined,
    symbol: string,
    type: Type,
    exchange: string,
    marketSymbol: string,
    epoch: number,
    timestamp: Date,
    bid: number,
    ask: number,
    exchangeId: ObjectId | undefined
}

interface Timeframe {
    _id: ObjectId | undefined,
    exchange: string,
    symbol: string,
    marketSymbol: string,
    timeframe: string,
    minutes: number,
    candlestick: Candlestick | undefined,
    exchangeId: ObjectId | undefined
}

interface Candlestick {
    _id: ObjectId | undefined,
    timestamp: Date,
    epoch: number,
    nextTimestamp: Date,
    symbol: string,
    timeframe: string,
    open: number,
    high: number,
    low: number,
    close: number,
    volume: number,
    exchange: string,
    timeframeId: ObjectId | undefined,
    instrumentId: ObjectId | undefined,
    exchangeId: ObjectId | undefined
}

interface Ticker {
    _id: ObjectId | undefined,
    symbol: string,
    epoch: number,
    timestamp: Date,
    bid: number,
    bidVolume: number,
    ask: number,
    askVolume: number,
    instrumentId: ObjectId | undefined,
    exchangeId: ObjectId | undefined
}

const Instruments: HashTable<Instrument> = {};
const Markets: HashTable<Market> = {};
const Timeframes: HashTable<Timeframe> = {};

const timeframes: string[] = timeframeList.split(',');
const timeframeNames: string[] = timeframeNameList.split(',');
const mongoClient = new MongoClient(mongodbUrl);

const instrumentList: string[] = process.env.INSTRUMENTS ? process.env.INSTRUMENTS.split(',') : [];
logger.info('Instruments: ' + instrumentList);
instrumentList.map(instrument => {
    const symbol = instrument;
    Instruments[symbol] = {
        _id: new ObjectId(),
        exchange: exchangeName,
        symbol,
        type: Type.CRYPTO,
        marketSymbol: instrument,
        exchangeId: undefined,
        pricePrecision: 0.00001,
        quantityPrecision: 1,
        minimumNotional: null,
        maximumNotional: null,
        minimumQuantity: 10000,
        maximumQuantity: null
    }
    Markets[symbol] = {
        _id: new ObjectId(),
        exchange: exchangeName,
        symbol,
        type: Type.CRYPTO,
        marketSymbol: instrument,
        exchangeId: undefined,
        epoch: new Date().getTime(),
        timestamp: new Date(),
        bid: 0,
        ask: 0
    }

    // tslint:disable-next-line:prefer-for-of
    for (let i = 0; i < timeframes.length; i++) {
        Timeframes[symbol + '-' + timeframeNames[i]] = {
            _id: new ObjectId(),
            timeframe: timeframeNames[i],
            minutes: Number.parseInt(timeframes[i], 10),
            symbol,
            marketSymbol: instrument,
            exchange: exchangeName,
            candlestick: undefined,
            exchangeId: undefined
        }
    }
});

const ftxus = new FtxUsClient();
ftxus.on('ticker', (Ticker: any, Market: any) => {
    try {
        if (streaming === false) {
            streaming = true;
        }
        const ticker: Ticker = {
            _id: undefined,
            symbol: Market.id,
            epoch: new Date(Ticker.timestamp).getTime(),
            timestamp: new Date(Ticker.timestamp),
            bid: Number.parseFloat(Ticker.bid),
            ask: Number.parseFloat(Ticker.ask),
            bidVolume: Number.parseFloat(Ticker.bidVolume),
            askVolume: Number.parseFloat(Ticker.askVolume),
            instrumentId: Instruments[Market.id]._id,
            exchangeId
          };
          if (Math.abs(heartbeat.getTime() - new Date(Ticker.timestamp).getTime()) > 5000) {
            heartbeat = new Date(Ticker.timestamp);
            db.collection('exchange').updateOne({name: exchangeName}, {
              $set: {
                timestamp: new Date(Ticker.timestamp),
                epoch: new Date(Ticker.timestamp).getTime(),
                heartbeat: new Date(Ticker.timestamp).getTime()
              }
            })
          }
          if (enableLog === true) {
            logger.info(JSON.stringify(ticker));
          }
          ProcessTicker(ticker);
    } catch (err) {
        logger.error(err);
    }
});

const subscribeTickers = () => {
  instrumentList.map(instrument => {
    var pair = instrument.split('/');
    var subscription = {
      id: instrument,
      base: pair[0],
      quote: pair[1]
    };
    ftxus.subscribeTicker(subscription);
  });
}

const connect = async () => {
    try {
        logger.info('Connecting to: ' + mongodbUrl);
        await mongoClient.connect();
        logger.info('Connected to database.');
        db = mongoClient.db(mongodbName);
        const exchange = db.collection('exchange');
        await exchange.findOne({ name: exchangeName }, (err, exchangeItem) => {
            if (err) {
                logger.error(err);
                return;
            }
            if (exchangeItem === null) {
                // Create exchange
                exchange.insertOne({
                    _id: new ObjectId(),
                    name: exchangeName,
                    heartbeat: new Date().getTime()
                }, (insertErr, exchangeInsert) => {
                    if (insertErr) {
                        logger.error(insertErr);
                        return;
                    }
                    exchangeId = exchangeInsert?.insertedId;
                    UpdateInstruments();
                });
            }
            else {
                exchangeId = exchangeItem?._id;
                UpdateInstruments();
            }
            logger.info('Connecting to: ' + domain);
            subscribeTickers();
        })
    }
    catch (err) {
        logger.error(err);
    }
}

const UpdateInstruments = () => {
  // Make sure all instruments are accounted for
    Object.keys(Instruments).forEach(async key => {
        Instruments[key].exchangeId = exchangeId;
        const object = Instruments[key];
        const collection = db.collection('instrument');
        try {
            await collection.updateOne({
                exchange: exchangeName,
                symbol: object.symbol,
                marketSymbol: object.marketSymbol,
                exchangeId
            }, {
                $setOnInsert: object
            }, {
                upsert: true
            });
            const instrument = await collection.findOne({
                symbol: object.symbol,
                exchangeId
            });
            if (instrument !== null) {
                Instruments[key]._id = instrument._id;
            }
        } catch (err) {
            logger.error(err);
        }
    });
    Object.keys(Markets).forEach(async key => {
        Markets[key].exchangeId = exchangeId;
        const object = Markets[key];
        const collection = db.collection('market');
        try {
            await collection.updateOne({
                exchange: exchangeName,
                symbol: object.symbol,
                marketSymbol: object.marketSymbol,
                exchangeId
            }, {
                $setOnInsert: object
            }, {
                upsert: true
            });
            const market = await collection.findOne({
                symbol: object.symbol,
                exchangeId
            });
            if (market !== null) {
                Markets[key]._id = market._id;
            }
        } catch (err) {
            logger.error(err);
        }
    });
    Object.keys(Timeframes).forEach(async key => {
        Timeframes[key].exchangeId = exchangeId;
        const object = Timeframes[key];
        const collection = db.collection('timeframe');
        try {
            await collection.updateOne({
                exchange: exchangeName,
                symbol: Timeframes[key].symbol,
                timeframe: object.timeframe,
                exchangeId
            }, {
                $setOnInsert: object
            }, {
                upsert: true
            });
            const timeframe = await collection.findOne({
                symbol: Timeframes[key].symbol,
                exchangeId
            })
            if (timeframe !== null) {
                Timeframes[key]._id = timeframe._id;
                const initialTimestamp = new Date(GetInitialTime(Timeframes[key]));
                const nextTimestamp = new Date(initialTimestamp.getTime() + (Timeframes[key].minutes * 60000));
                const collection = db.collection('candlestick');
                const dbCandle = await collection.findOne({
                    symbol: Timeframes[key].symbol,
                    timeframe: Timeframes[key].timeframe,
                    timestamp: initialTimestamp,
                    timeframeId: timeframe._id,
                    instrumentId: Instruments[Timeframes[key].symbol]._id,
                    exchangeId
                });
                Timeframes[key].candlestick = {
                    _id: new ObjectId(),
                    symbol: Timeframes[key].symbol,
                    timeframe: Timeframes[key].timeframe,
                    timestamp: initialTimestamp,
                    epoch: initialTimestamp.getTime(),
                    nextTimestamp,
                    open: dbCandle !== null ? dbCandle.open : 0,
                    high: dbCandle !== null ? dbCandle.high : Number.MIN_VALUE,
                    low: dbCandle !== null ? dbCandle.low : Number.MAX_VALUE,
                    close: 0,
                    volume: 0,
                    exchange: exchangeName,
                    timeframeId: timeframe._id,
                    instrumentId: Instruments[Timeframes[key].symbol]._id,
                    exchangeId
                }
            }
        } catch (err) {
            logger.error(err);
        }
    });
}

const ProcessTicker = (ticker: Ticker) => {
    try {
        if (enableLog === true) logger.info(JSON.stringify(ticker));
        // const tick = db.collection('tick');
        if (ticker.instrumentId === undefined) return;
        // tick.insertOne({
        //    symbol: ticker.symbol,
        //    bid: ticker.bid,
        //    ask: ticker.ask,
        //    epoch: new Date(ticker.timestamp).getTime(),
        //    timestamp: new Date(ticker.timestamp),
        //    instrumentId: ticker.instrumentId,
        //    exchangeId
        // });
        Markets[ticker.symbol].bid = ticker.bid;
        Markets[ticker.symbol].ask = ticker.ask;
        Markets[ticker.symbol].timestamp = new Date(ticker.timestamp);
        Markets[ticker.symbol].epoch = new Date(ticker.timestamp).getTime();

        Object.keys(Timeframes).forEach(key => {
            const timeframe = Timeframes[key];
            if (timeframe.symbol === ticker.symbol) {
                if (timeframe.candlestick === undefined) return;
                if (ticker.timestamp.getTime() < timeframe.candlestick.nextTimestamp.getTime()) {
                    // Update Candlestick
                    if (timeframe.candlestick.open === 0) {
                        timeframe.candlestick.open = ticker.bid;
                    }
                    timeframe.candlestick.close = ticker.bid;
                    timeframe.candlestick.high = Math.max(timeframe.candlestick.high, ticker.bid);
                    timeframe.candlestick.low = Math.min(timeframe.candlestick.low, ticker.bid);
                    timeframe.candlestick.volume += ticker.bidVolume;
                } else {
                    // New Candlestick
                    db.collection('candlestick').updateOne({
                        timestamp: timeframe.candlestick.timestamp,
                        symbol: timeframe.candlestick.symbol,
                        timeframe: timeframe.candlestick.timeframe,
                        exchangeId
                    }, {
                        $set: {
                            high: timeframe.candlestick.high,
                            low: timeframe.candlestick.low,
                            close: timeframe.candlestick.close,
                            volume: timeframe.candlestick.volume
                        }
                    }, (err) => {
                        if (err) {
                            logger.error(err);
                        }
                    });
                    timeframe.candlestick._id = new ObjectId();
                    timeframe.candlestick.timestamp = timeframe.candlestick.nextTimestamp;
                    timeframe.candlestick.epoch = timeframe.candlestick.nextTimestamp.getTime();
                    timeframe.candlestick.nextTimestamp = new Date(timeframe.candlestick.nextTimestamp.getTime() + (timeframe.minutes * 60000));
                    timeframe.candlestick.open = ticker.bid;
                    timeframe.candlestick.close = ticker.bid;
                    timeframe.candlestick.high = ticker.bid;
                    timeframe.candlestick.low = ticker.bid;
                    timeframe.candlestick.volume = ticker.bidVolume;
                    db.collection('candlestick').insertOne(timeframe.candlestick, {
                        writeConcern: {
                            w: 0,
                            j: false,
                            wtimeout: 500
                        }
                    }, (err) => {
                        if (err) {
                            logger.error(err);
                        }
                    });
                    db.collection('timeframe').updateOne({
                        symbol: timeframe.symbol,
                        timeframe: timeframe.timeframe,
                        minutes: timeframe.minutes
                    }, {
                        $set: {
                            candlestick: timeframe.candlestick
                        }
                    }, {
                        writeConcern: {
                            w: 0,
                            j: false,
                            wtimeout: 500
                        }
                    });
                }
                if (enableLog === true) logger.info(JSON.stringify(timeframe.candlestick));
            }
        });
    } catch (err) {
        logger.error(err);
    }
};

setInterval(() => {
    // We have to throttle updates due to speed
    let marketUpdates: any = [];
    if (db === undefined || db === null) return;
    Object.keys(Markets).forEach(key => {
        const market = Markets[key];
        marketUpdates.push({
            updateOne: {
                filter: {
                    exchange: exchangeName,
                    symbol: market.symbol
                },
                update: {
                    $set: {
                        bid: market.bid,
                        ask: market.ask,
                        timestamp: market.timestamp,
                        epoch: market.epoch
                    }
                }
            }
        });
    });
    db.collection('market').bulkWrite(marketUpdates, err => {
        logger.error(err);
    });
    let candlestickUpdates: any = [];
    let timeframeUpdates: any = [];
    Object.keys(Timeframes).forEach(key => {
        const timeframe = Timeframes[key];
        const candlestick = timeframe.candlestick;
        if (candlestick === undefined) return;
        candlestickUpdates.push({
            updateOne: {
                filter: {
                    exchange: exchangeName,
                    timeframe: candlestick.timeframe,
                    symbol: candlestick.symbol,
                    timestamp: candlestick.timestamp
                },
                update: {
                    $set: {
                        high: candlestick.high,
                        low: candlestick.low,
                        close: candlestick.close,
                        volume: candlestick.volume
                    }
                }
            }
        });
        timeframeUpdates.push({
            updateOne: {
                filter: {
                    exchange: exchangeName,
                    timeframe: candlestick.timeframe,
                    symbol: candlestick.symbol
                },
                update: {
                    $set: {
                        candlestick: candlestick
                    }
                }
            }
        });
    });
    db.collection('candlestick').bulkWrite(candlestickUpdates, err => {
        logger.error(err);
    });
    db.collection('timeframe').bulkWrite(timeframeUpdates, err => {
        logger.error(err);
    });
}, 100)

const GetInitialTime = (timeframe: Timeframe) => {
    let lastMinute = new Date().setSeconds(0, 0);
    let lastHour = new Date().setMinutes(0, 0, 0);
    const lastDay = new Date().setHours(0, 0, 0, 0);
    switch (timeframe.minutes) {
    case 1:
        return new Date(lastMinute + 60000).getTime();
    case 5:
        if (new Date(lastMinute).getMinutes() % timeframe.minutes > 0) {
            do {
                lastMinute -= 60000;
            } while ((new Date(lastMinute).getMinutes() % timeframe.minutes) > 0);
        }
        return new Date(lastMinute).getTime();
    case 10:
        if (new Date(lastMinute).getMinutes() % timeframe.minutes > 0) {
            do {
                lastMinute -= 60000;
            } while ((new Date(lastMinute).getMinutes() % timeframe.minutes) > 0);
        }
        return new Date(lastMinute).getTime();
    case 15:
        if ((new Date(lastMinute).getMinutes() % timeframe.minutes) > 0) {
            do {
                lastMinute -= 60000;
            } while ((new Date(lastMinute).getMinutes() % timeframe.minutes) > 0);
        }
        return new Date(lastMinute).getTime();
    case 30:
        if (new Date(lastMinute).getMinutes() % timeframe.minutes > 0) {
            do {
                lastMinute -= 60000;
            } while ((new Date(lastMinute).getMinutes() % timeframe.minutes) > 0);
        }
        return new Date(lastMinute).getTime();
    case 60:
        return new Date(lastHour).getTime();
    case 120:
        if ((new Date(lastHour).getHours() % 2) > 0) {
            do {
                lastHour -= (60000 * 60);
            } while ((new Date(lastHour).getHours() % 2) > 0);
        }
        return new Date(lastHour + (60000 * 2)).getTime();
    case 240:
        if (new Date(lastHour).getHours() % 4 > 0) {
            do {
                lastHour -= (60000 * 60);
            } while (new Date(lastHour).getHours() % 4 !== 0);
        }
        return new Date(lastHour).getTime();
    case 360:
        if (new Date(lastHour).getHours() % 6 > 0) {
            do {
                lastHour -= (60000 * 60);
            } while (new Date(lastHour).getHours() % 6 !== 0);
        }
        return new Date(lastHour).getTime();
    case 720:
        if (new Date(lastHour).getHours() % 12 > 0) {
            do {
                lastHour -= (60000 * 60);
            } while (new Date(lastHour).getHours() % 12 !== 0);
        }
        return new Date(lastHour).getTime();
    case 1440:
        return new Date(lastDay).getTime();
    }
    return new Date().getTime();
};

logger.info('Connecting to database.');
connect();