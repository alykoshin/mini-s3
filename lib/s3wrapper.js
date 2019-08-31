const fs = require('fs');
const path = require('path');
const util = require('util');

const AWS = require('aws-sdk');
const slugify = require('slugify');
const debug = require('debug')('mini-s3');
const async = require('async');

const {sanitizeArray} = require('@utilities/array');
const {asyncForEach}  = require('@utilities/async');
const {dirListFilenames}  = require('@utilities/fs');


const ensureProp = (obj, prop) => {
  if (!obj[prop]) throw new Error(`Expecting object to have property "${prop}"`);
  return true;
};


class S3wrapper {

  constructor (options) {
    ensureProp(options, 'accessKeyId');
    ensureProp(options, 'secretAccessKey');
    this.options = options;
    this.s3 = new AWS.S3({
      accessKeyId:     this.options.accessKeyId,
      secretAccessKey: this.options.secretAccessKey,
    });
  }


  _filenameToKey (fname) {
    fname = path.basename(fname);
    //fname     = fname.replace(/[\/\\]/g, '-'); // replace slashes and backslashes, otherwise they'll be removed
    const key = slugify(fname);
    debug(`filenameToKey: ${key}`);
    //console.log('key:', encodeURIComponent(fname));
    return key;
  };

  _listBuckets (params, cb) {
    debug('_listBuckets');
    return this.s3.listBuckets((err, data) => {
      if (err) {
        console.error('_listBuckets: ERROR:', err);
        return cb && cb(err);
      }
      //return console.log('SUCCESS', data.Buckets);
      const buckets = data.Buckets;
      const names = buckets.map(b => b[ 'Name' ]);
      //console.log('SUCCESS', data);
      debug('_listBuckets: SUCCESS:', names);
      return cb && cb(null, names, data);
    });
  };
  async listBuckets (params, cb) {
    return this._wrapAsyncSafe(this._listBuckets, params, cb);
  };

  __listObjects ({bucket}, cb) {
    // Sets the maximum number of keys returned in the response body.
    // If you want to retrieve fewer than the default 1,000 keys, you can add this to your request.
    const params = {
      Bucket:  bucket,
      MaxKeys: 10,
    };
    debug('__listObjects: params:', params);
    return this.s3.listObjects(params, (err, data) => {
      if (err) {
        console.error('_listObjects: ERROR:', err);
        return cb && cb(err);
      }
      const contents = data.Contents;
      const keys = contents && contents.map(b => b[ 'Key' ]);
      debug('__listObjects: SUCCESS:', keys);           // successful response
      return cb && cb(null, keys, data);
    });
  };
  _listKeys (params, cb) {
    return this.__listObjects(params, (err, keys, data) => cb(err, keys));
  }
  _listNames (params, cb) { return this._listKeys(params, cb); }
  _listObjects (params, cb) {
    return this.__listObjects(params, (err, keys, data) => cb(err, data));
  }
  async listObjects (params, cb) {
    return this._wrapAsyncSafe(this._listObjects, params, cb);
  };
  async listKeys (params, cb) {
    return this._wrapAsyncSafe(this._listKeys, params, cb);
  };
  listNames (params, cb) { return this.listKeys(params, cb); }

  //_wrapAsyncSafe (fn, ...args) {
  //  //const cb =
  //  return
  //}

  _uploadOne ({ filename, bucket }, cb) {
    debug(`_uploadOne: fname: "${filename}" bucketName: "${bucket}"`);

    const key    = this._filenameToKey(filename);
    const body   = fs.createReadStream(filename)
          //.pipe(zlib.createGzip())
    ;
    const params = {
      Body:   body,
      Key:    key,
      Bucket: bucket,
    };
    const options = {
      //partSize: 1 * 1024 * 1024, //  partSize must be greater than 5242880
      //queueSize: 1,
    };
    //console.log('_upload: params:', params, '; options:', options);

    return this.s3.upload(params, options)
      .on('httpUploadProgress', (evt) => {
        debug('_upload: on(httpUploadProgress):', evt);
      })
      .send((err, data) => {
        if (err) {
          console.error('_upload: send: ERROR:', err);
          return cb && cb(err);
        }
        debug('_upload: send: SUCCESS:', data);
        return cb && cb(null, data);
      });
  };

  _upload({ filenames, bucket }, cb) {
    const MAX_SIM_UPLOADS = 2;
    //fnames = sanitizeArray(fnames);
    filenames = sanitizeArray(filenames);
    debug(`_upload: fname: [${filenames.join(',')}], bucketName: "${bucket}"`);

    return async.eachLimit(
      filenames,
      MAX_SIM_UPLOADS,
      (filename, callback) => this._uploadOne({filename, bucket}, callback),
      cb  // A callback which is called after all the iteratee functions have finished. Result will be either true or false depending on the values of the async tests. Invoked with (err, result).
    );
  }

  async upload (params, cb) {
    return this._wrapAsyncSafe(this._upload, params, cb);
  };

  //_listLocalFiles(localDir) {
  //  let list = fs.readdirSync(localDir, { withFileTypes: true });
  //  list = list
  //    .filter(dirent => dirent.isFile())
  //    .map(dirent => path.join( localDir, dirent.name ))
  //  ;
  //  debug('_listLocalFiles:', list);
  //  return list;
  //}

  async uploadDir ({localDir, bucket}, cb) {
    debug(`uploadDir(): localDir: ${localDir}, bucket: ${bucket}`);
    //const filenames = this._listLocalFiles(localDir);
    const filenames = dirListFilenames(localDir, {addPath:'joinBase'});
    debug(`uploadDir(): filenames: [${filenames.join(',')}]`);
    return await this.upload({ filenames, bucket }, cb);
  }

  _deleteObject ({ bucket, key }, cb) {
    const params = {
      Bucket: bucket,
      Key:    key,
    };
    debug('_deleteObject(): params:', params);
    return this.s3.deleteObject(params, (err, data) => {
      if (err) {
        console.error('_deleteObject(): ERROR:', err);
        return cb && cb(err);
      }
      debug('_deleteObject(): SUCCESS:', data);
      this.result = data;
      return cb && cb(null, this);
    });
  };

  _deleteObjects ({ bucket, keys }, cb) {
    keys = sanitizeArray(keys);
    debug('_deleteObjects(): keys:', keys);
    if (keys.length === 0) {
      debug('_deleteObjects(): keys.length === 0');
      return cb && cb(null, this);
    }
    const objects = keys.map(key => ({ Key: key })); // extract filenames
    const params = {
      Bucket: bucket,
      Delete: {
        Objects: objects,
        Quiet:   false,
      }
    };
    debug('_deleteObjects(): params:', params);
    return this.s3.deleteObjects(params, (err, data) => {
      if (err) {
        console.error('_deleteObjects(): ERROR:', err);
        return cb && cb(err);
      }
      debug('_deleteObjects(): SUCCESS:', data);
      this.result = data;
      return cb && cb(null, this);
    });
  };

  _wrapAsyncSafe(fn, params, cb) {
    return cb
           ? fn.call(this, params, cb)
           : util.promisify(fn.bind(this))(params)
      ;
  }

  async deleteObject (params, cb) {
    return this._wrapAsyncSafe(this._deleteObject, params, cb);
  };
  async deleteObjects (params, cb) {
    return this._wrapAsyncSafe(this._deleteObjects, params, cb);
  };
  async deleteFile ({ bucket, filename }, cb) {
    const key = this._filenameToKey(filename);
    return await this.deleteObject(bucket, key, cb);
  };

  async deleteFiles ({ bucket, filenames }, cb) {
    const keys = sanitizeArray(filenames)
      .map(f => this._filenameToKey(f))
    ;
    return this.deleteObjects({ bucket, keys });
    //return asyncForEach(
    //  filenames,
    //  async (filename) => await this.deleteFile.call(this, { bucket, filename }, cb)
    //);
  }

  async deleteAll({ bucket }) {
    const keysToDelete = await this.listKeys({ bucket });
    debug('deleteAll(): keysToDelete:', keysToDelete);

    await this.deleteObjects({ bucket, keys: keysToDelete });

    const keysFinal = await this.listKeys({ bucket });
    if (keysFinal.length !== 0) throw new Error('deleteAll: keysFinal.length !== 0');

    debug('deleteAll: SUCCESS');
    return keysToDelete;
  }

}


module.exports = S3wrapper;
