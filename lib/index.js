const debug = require('debug')('mini-s3');

const {sanitizeArray} = require('@utilities/array');
const {asyncForEach}  = require('@utilities/async');

const S3wrapper = require('./s3wrapper');

//AWS.config.loadFromPath('./s3config.json');


//const s3 = new AWS.S3();

//const USER_NAME = 'google.order.dev';

//const ACCESS_KEY_ID     = 'AKIAQ5R6JLRRG5RBRG5C';
//const SECRET_ACCESS_KEY = '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';



//

class GenericManager {
  constructor(genericManager) {
    this._parent = genericManager;
    this.root = false;
  }
  isRoot() {
    return !(this._parent instanceof GenericManager);
  }
  async _proxy(fnName, ...args) {
    return this.isRoot()
           ? await this._parent[fnName](...args)
           : await this._parent._proxy(fnName, ...args)
      ;
  }
}


class BucketManager extends GenericManager {
  constructor(s3, bucket) {
    super(s3);
    this._bucket = bucket;
    this._bucketName = bucket;
  }
  async _proxy(fnName, ...args) {
    args[0].bucket = this._bucket;
    return super._proxy(fnName, ...args);
  }
  bucket(bucket) { return new BucketManager(this._parent, bucket); }
  object(key)        { return new ObjectManager(this, key); }
  file(fileName)     { return new FilesManager(this, fileName); }
  files(fileNames)   { return new FilesManager(this, fileNames); }
  directory(dirName) { return new DirectoryManager(this, dirName); }

  //list(cb)           { return this._parent.listObjects({ bucket: this._bucket }, cb); }
  list(cb)           { return this._proxy('listObjects', {}, cb); }
  //listNames(cb)      { return this._parent.listNames({ bucket: this._bucket }, cb); }
  listNames(cb)      { return this._proxy('listNames', {}, cb); }
  listKeys(cb)       { return this._proxy('listKeys',  {}, cb); }
  deleteAll(cb)      { return this._proxy('deleteAll', {}, cb); }
}


class ObjectManager extends GenericManager {
  constructor(bucketManager, key) {
    super(bucketManager);
    this._key = key;
  }
  async delete (cb) {
    //return this._parent._parent.deleteObject(this._parent._bucketName, this._key, cb);
    return this._proxy('deleteObject', this._parent._bucketName, this._key, cb);
  }
}


class DirectoryManager extends GenericManager {
  constructor(bucketManager, localDir) {
    super(bucketManager);
    this._localDir = localDir;
  }
  async upload (cb) {
    return this._proxy('uploadDir', { localDir: this._localDir }, cb);
    //return this._parent._parent.uploadDir(this._dirName, this._parent._bucketName, cb);
  }
}


class FilesManager extends GenericManager {
  constructor(bucketManager, filenames) {
    super(bucketManager);
    this._filenames = sanitizeArray(filenames);
  }
  async delete (cb) {
    return this._proxy('deleteFile', { filenames: this._filenames }, cb);
    //  return asyncForEach(
  //    this._filenames,
  //    //async (fname) => await this._parent._parent.deleteFile.call(this, this._parent._bucketName, fname, cb)
  //  async (fname) => this._proxy('deleteFile', { filename: fname }, cb)
  //)
  }
  async upload (cb) {
    return this._proxy('upload', { filenames: this._filenames }, cb);
    //  return asyncForEach(
  //    this._fileNames,
  //    //async (fname) => await this._parent._parent.upload.call(this, fname, this._parent._bucketName, cb)
  //  async (fname) => this._proxy('upload', { filename: fname }, cb)
  //)
  }
}



class MiniS3 extends S3wrapper {

  constructor (options) {
    super(options);
  }

  buckets() {
    return {
      list: this.listBuckets.bind(this),
    }
  }

  bucket(bucketName) {
    return new BucketManager(this, bucketName);
  }

}

//

module.exports = MiniS3;
