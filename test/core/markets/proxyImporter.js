var chai = require('chai');
var expect = chai.expect;
var should = chai.should;
var sinon = require('sinon');
const moment = require('moment');

var _ = require('lodash');

const ProxyImporter = require('../../../core/markets/proxyImporter');

describe('Proxy Importer', function() {

  it('should skip over leading out of bounds segments', function() {
    let ranges = [
      {from: 1557600000, to: 1557600010},
      {from: 1557600011, to: 1557600020},
      {from: 1557600021, to: 1557600030},
      {from: 1557600031, to: 1557600090},
    ];
    let from = moment.unix(1557600040);
    let to =   moment.unix(1557600050);
    let segment = ProxyImporter.toNextSegment(from, to, ranges);
    expect(segment.from.unix()).to.equal(1557600040);
    expect(segment.to.unix()).to.equal(1557600050);
    expect(!!segment.cached).to.be.true;
  });

  
  it('should mark a leading gap as uncached', function() {
    let ranges = [
      {from: 1557600011, to: 1557600020},
      {from: 1557600021, to: 1557600030}
    ];
    let from = moment.unix(1557600000);
    let to =   moment.unix(1557600050);
    let segment = ProxyImporter.toNextSegment(from, to, ranges);
    expect(segment.from.unix()).to.equal(1557600000);
    expect(segment.to.unix()).to.equal(1557600011);
    expect(!!segment.cached).to.be.false;
  });


  it('should mark a leading segment as cached', function() {
    let ranges = [
      {from: 1557600010, to: 1557600020},
      {from: 1557600022, to: 1557600030}
    ];
    let from = moment.unix(1557600011);
    let to =   moment.unix(1557600050);
    let segment = ProxyImporter.toNextSegment(from, to, ranges);
    expect(segment.from.unix()).to.equal(1557600011);
    expect(segment.to.unix()).to.equal(1557600020);
    expect(!!segment.cached).to.be.true;
  });


  it('should mark a flush leading segment as cached', function() {
    let ranges = [
      {from: 1557600011, to: 1557600020},
      {from: 1557600022, to: 1557600030}
    ];
    let from = moment.unix(1557600011);
    let to =   moment.unix(1557600050);
    let segment = ProxyImporter.toNextSegment(from, to, ranges);
    expect(segment.from.unix()).to.equal(1557600011);
    expect(segment.to.unix()).to.equal(1557600020);
    expect(!!segment.cached).to.be.true;
  });


  it('should mark contiguous segments as cached', function() {
    let ranges = [
      {from: 1557600011, to: 1557600020},
      {from: 1557600021, to: 1557600030}
    ];
    let from = moment.unix(1557600011);
    let to =   moment.unix(1557600050);
    let segment = ProxyImporter.toNextSegment(from, to, ranges);
    expect(segment.from.unix()).to.equal(1557600011);
    expect(segment.to.unix()).to.equal(1557600030);
    expect(!!segment.cached).to.be.true;
  });


  it('should not return a segment larger than the to/from bounds', function() {
    let ranges = [
      {from: 1557600000, to: 1557600100}
    ];
    let from = moment.unix(1557600011);
    let to =   moment.unix(1557600050);
    let segment = ProxyImporter.toNextSegment(from, to, ranges);
    expect(segment.from.unix()).to.equal(1557600011);
    expect(segment.to.unix()).to.equal(1557600050);
    expect(!!segment.cached).to.be.true;
  });


  it('should find the uncached gaps', function() {
    let ranges = [
      {from:1549066320, to:1549149120},
      {from:1549199520, to:1556885520}
    ];

    let from = moment.utc('2019-02-01 00:00');
    const fromS = from.unix();
    let to =   moment.utc('2019-02-10 00:00');
    let segment = ProxyImporter.toNextSegment(from, to, ranges);
    let fromExpected = moment.utc('2019-02-01 00:00').unix();
    expect(segment.from.unix()).to.equal(fromExpected);
    expect(segment.to.unix()).to.equal(1549066320);
    expect(!!segment.cached).to.be.false;

    ranges = [
      {from: fromS, to: 1549149120},
      {from:1549199520, to:1556885520}
    ];

    from = moment.utc('2019-02-01 00:00');
    to =   moment.utc('2019-02-10 00:00');
    segment = ProxyImporter.toNextSegment(from, to, ranges);
    expect(segment.from.unix()).to.equal(fromS);
    expect(segment.to.unix()).to.equal(1549149120);
    expect(!!segment.cached).to.be.true;

    from = moment.unix(1549149120);
    to =   moment.utc('2019-02-10 00:00');
    segment = ProxyImporter.toNextSegment(from, to, ranges);
    expect(segment.from.unix()).to.equal(1549149120);
    expect(segment.to.unix()).to.equal(1549199520);
    expect(!!segment.cached).to.be.false;

    ranges = [
      {from: fromS, to:1556885520}
    ];

    from = moment.unix(1549149120);
    to =   moment.utc('2019-02-10 00:00');
    segment = ProxyImporter.toNextSegment(from, to, ranges);
    expect(segment.from.unix()).to.equal(1549149120);
    expect(segment.to.unix()).to.equal(moment.utc('2019-02-10 00:00').unix());
    expect(!!segment.cached).to.be.true;
  });


  it('should pad import date ranges', function() {
    let from = moment.utc('2019-02-01 00:00');
    let to =   moment.utc('2019-02-03 00:00');

    let importer = new ProxyImporter(from, to, {}, {host: 'localhost', port: 1234});
    importer.client = {
      importAndWait: async function (fromPadded, toPadded) {
        expect(fromPadded.format()).to.equal(moment.utc('2019-02-01 00:00').subtract(1, 'm').format());
        expect(toPadded.format()).to.equal(moment.utc('2019-02-03 00:00').add(1, 'm').format());
        expect(from.format()).to.equal(moment.utc('2019-02-01 00:00').format());
        expect(to.format()).to.equal(moment.utc('2019-02-03 00:00').format());
      }
    };

    return importer.warmCache(from, to);
  });


  it('should always import at least 24 hours of data', function() {
    let from = moment.utc('2019-02-02 00:00');
    let to =   moment.utc('2019-02-02 20:00');

    let importer = new ProxyImporter(from, to, {}, {host: 'localhost', port: 1234});
    importer.client = {
      importAndWait: async function (fromPadded, toPadded) {
        expect(fromPadded.format()).to.equal(moment.utc('2019-02-01 23:59').format());
        expect(toPadded.format()).to.equal(moment.utc('2019-02-03 00:00').format());
        expect(from.format()).to.equal(moment.utc('2019-02-02 00:00').format());
        expect(to.format()).to.equal(moment.utc('2019-02-02 20:00').format());
      }
    };

    return importer.warmCache(from, to);    
  });

});
