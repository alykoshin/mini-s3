const debug = require('debug')('mini-s3');

const {sanitizeArray} = require('@utilities/array');
const {asyncForEach}  = require('@utilities/async');
const {ChainableNode} = require('@utilities/object');

const S3wrapper = require('./s3wrapper');

//AWS.config.loadFromPath('./s3config.json');

//const s3 = new AWS.S3();

//const USER_NAME = 'google.order.dev';

//const ACCESS_KEY_ID     = 'AKIAQ5R6JLRRG5RBRG5C';
//const SECRET_ACCESS_KEY = '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';

//

class BucketManager extends ChainableNode {

  constructor(s3, params, ...args) {
    super(s3, params, ...args);
    //this._bucket     = params.bucket;
    //this._bucketName = params.bucket;
    //this._args   = { ...this._args,   ...args };
    //this._params = { ...this._params, ...params };
  }

  //async chainedExec(fnName, params, ...args) {
  //  return super.chainedExec(fnName, fullParams, ...args);
  //}

  bucket(bucket)     { return new BucketManager(   this._parent, { bucket }); }
  object(key)        { return new ObjectManager(   this,         { key }); }
  file(fileName)     { return new FilesManager(    this,         { filenames: fileName }); }
  files(filenames)   { return new FilesManager(    this,         { filenames }); }
  directory(dirName) { return new DirectoryManager(this,         { localDir: dirName }); }

  //list(cb)           { return this._parent.listObjects({ bucket: this._bucket }, cb); }
  list(cb)           { return this.chainedExec('listObjects', {}, cb); }
  //listNames(cb)      { return this._parent.listNames({ bucket: this._bucket }, cb); }
  listNames(cb)      { return this.chainedExec('listNames',   {}, cb); }
  listKeys(cb)       { return this.chainedExec('listKeys',    {}, cb); }
  deleteAll(cb)      { return this.chainedExec('deleteAll',   {}, cb); }
}

//

class ObjectManager extends ChainableNode {

  constructor(bucketManager, params) {
    super(bucketManager, params);
    this._key = params.key;
  }

  async delete (cb) {
    return this.chainedExec('deleteObject', {/* bucket: this._parent._bucketName, key: this._key */}, cb);
  }

}

//

class DirectoryManager extends ChainableNode {

  constructor(bucketManager, params) {
    super(bucketManager, params);
    //this._localDir = params.local;
  }

  async upload (cb) {
    return this.chainedExec('uploadDir', { /*localDir: this._localDir*/ }, cb);
  }

}

//

class FilesManager extends ChainableNode {

  constructor(bucketManager, params) {
    super(bucketManager, params);
    //this._filenames = sanitizeArray(params.filenames);
  }

  async delete (cb) {
    return this.chainedExec('deleteFile', { /*filenames: this._filenames*/ }, cb);
  }

  async upload (cb) {
    return this.chainedExec('upload', { /*filenames: this._filenames*/ }, cb);
  }

}

//

class MiniS3 extends S3wrapper {

  constructor (options) {
    super(options);
  }

  buckets() {
    return {
      list: this.listBuckets.bind(this),
    }
  }

  bucket(bucket) {
    return new BucketManager(this, { bucket });
  }

}

//

module.exports = MiniS3;
