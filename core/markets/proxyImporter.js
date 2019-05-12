/**
 * Import Candles from another Gekko host.
 */

var _ = require('lodash');
var moment = require('moment');

var GekkoApiClient = require('gekko-api-client');

const ProxyImporter = function (from, to, watch, config) {
  this.from = from;
  this.cursor = from;
  this.to = to;
  this.watch = watch;
  this.client = new GekkoApiClient(config.host, config.port);
};

ProxyImporter.prototype = {
  fetch: async function (onDone, onCandles) {
    let next = this.from;

    try {
      while (next.isBefore(this.to)) {
        let candles = await this.fetchNext(next);
        onCandles(candles);
        var lastCandle = _.last(candles);
        if (!lastCandle) {
          break;
        }
        next = moment.unix(lastCandle.start).utc();
      }
      onDone();
    } catch (e) {
      console.log('ERROR ProxyImporter::fetch ', e);
      onDone(e);
    }
  },

  fetchNext: async function (from) {
    const segmentEnd = from.clone().add(48, 'h');
    const to = moment.min(segmentEnd, this.to);
    const segment = await this.getNextSegment(from, to);
    if (!segment.cached) {
      await this.warmCache(segment.from, segment.to);
    }

    return this.client.getCandles(segment.from, segment.to, 1, this.watch);
  },

  getNextSegment: async function (from, to) {
    let cachedRanges = await this.client.scan(this.watch);
    return toNextSegment(from, to, cachedRanges);
  },

  warmCache: async function (from, to) {
    let fromPadded = from.clone().subtract(1, 'm');
    let toPadded = to.clone().add(1, 'm');
    let toMinimum = fromPadded.clone().add(24, 'h').add(1, 'm');
    toPadded = moment.max(toMinimum, toPadded);
    await this.client.importAndWait(fromPadded, toPadded, this.watch);
  }
};

function toNextSegment(from, to, cachedRanges) {
  const fromS = from.unix();
  const toS = to.unix();
  cachedRanges = _.filter(cachedRanges, function (range) {
    return range.to > fromS && range.from < toS;
  });

  cachedRanges = _.sortBy(cachedRanges, 'to');

  let cached = _.first(cachedRanges);
  if (!cached) {
    return { from: from, to: to, cached: 0 };
  }

  const cachedFrom = moment.unix(cached.from);
  if (cachedFrom.isAfter(from)) {
    return { from: from, to: cachedFrom, cached: 0 };
  }

  for (let i = 1; i < cachedRanges.length; i++) {
    let next = cachedRanges[i];
    if (next.from <= cached.to + 1) {
      cached = {from: cached.from, to: next.to};
    }
  }

  const cacheCeiling = moment.unix(Math.min(cached.to, toS));
  return { from: from, to: cacheCeiling, cached: 1 };
}

ProxyImporter.toNextSegment = toNextSegment;
module.exports = ProxyImporter;