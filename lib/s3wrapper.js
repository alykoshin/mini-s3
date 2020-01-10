const fs = require('fs');
const path = require('path');
const util = require('util');

const _ = require('lodash');
const AWS = require('aws-sdk');
const slugify = require('slugify');
const debug = require('debug')('mini-s3');
const async = require('async');

const {sanitizeArray,peek} = require('@utilities/array');
const {asyncForEach}  = require('@utilities/async');
const {dirListFilenames}  = require('@utilities/fs');

const LIST_OBJECTS_REQUEST_MAX_KEYS   = 1000;
const MAX_SIM_UPLOADS = 20;


const ensureProp = (obj, prop) => {
  if (typeof obj[prop]==='undefined' || obj[prop]===null) throw new Error(`Expecting object to have property "${prop}"`);
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

  __listObjectsRecursive({ prefix, bucket, marker=undefined, result=[] }, cb) {
    const params = {
      Prefix:  prefix,
      Bucket:  bucket,
      Marker:  marker,
      // Sets the maximum number of keys returned in the response body.
      // If you want to retrieve fewer than the default 1,000 keys, you can add this to your request.
      MaxKeys: LIST_OBJECTS_REQUEST_MAX_KEYS,
    };
    debug('__listObjects: params:', params);

    return this.s3.listObjects(params, (err, data={}) => {
      if (err) {
        console.error('_listObjects: ERROR:', err);
        return cb && cb(err);
      }
      const isTruncated = data.IsTruncated;
      // When response is truncated (the IsTruncated element value in the response is true), you can use the key name in this field as marker in the subsequent request to get next set of objects. Amazon S3 lists objects in alphabetical order
      // Note: This element is returned only if you have delimiter request parameter specified. If response does not include the NextMaker and it is truncated, you can use the value of the last Key in the response as the marker in the subsequent request to get the next set of object keys.
      const nextMarker  = data.NextMarker;
      const contents    = data.Contents;
      const keys = contents && contents.map(b => b[ 'Key' ]);
      const nextKey = peek(keys);

      debug('__listObjects: isTruncated:', isTruncated,
        ', nextMarker:', nextMarker,
        ', nextMarker:', nextMarker,
        ', keys:', keys);

      result = result.concat(keys);

      if (isTruncated) {
        return this.__listObjectsRecursive({ prefix, bucket, marker: nextKey, result }, cb)

      } else {

        //const keys = contents && contents.map(b => b[ 'Key' ]);
        debug('__listObjects: SUCCESS: result:', result);           // successful response

        return cb && cb(null, result, data);
      }
    });
  }

  __listObjects ({bucket}, cb) {
    return this.__listObjectsRecursive({ bucket }, cb)
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

  _uploadOne ({ filename, bucket, remoteDir='' }, cb) {
    debug(`_uploadOne: filenames: "${filename}" bucketName: "${bucket}"`);

    const key    = this._filenameToKey(filename);
    const body   = fs.createReadStream(filename)
          //.pipe(zlib.createGzip())
    ;
    const fullKey = remoteDir ? remoteDir + '/' + key : key;
    const params = {
      Body:   body,
      Key:    fullKey,
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

  _getContentOne({ filename, bucket }, cb) {
    debug(`_getContentOne: filenames: "${filename}" bucketName: "${bucket}"`);

    const key    = this._filenameToKey(filename);
    //const body   = fs.createReadStream(filename)
    //      //.pipe(zlib.createGzip())
    //;
    const params = {
      //Body:   body,
      Bucket: bucket,
      Key:    key,
    };
    //const options = {
    //  //partSize: 1 * 1024 * 1024, //  partSize must be greater than 5242880
    //  //queueSize: 1,
    //};
    //console.log('_upload: params:', params, '; options:', options);

    return this.s3.getObject(params, function(err, data) {
      //console.log('>>>>>>>>>>>>>> _getContentOne cb', err,data)
      if (err) {
        console.error('_getContentOne: send: ERROR:', err);
        return cb && cb(err);
      }
      debug('_getContentOne: send: SUCCESS:', data);

      const encoding = 'utf8';
      const bodyStr = data.Body ? data.Body.toString(encoding) : '';
      debug('_getContentOne: send: SUCCESS: bodyStr', bodyStr);

      return cb && cb(null, bodyStr, data);
      /*
      data = {
       AcceptRanges: "bytes",
       ContentLength: 3191,
       ContentType: "image/jpeg",
       ETag: "\"6805f2cfc46c0f04559748bb039d69ae\"",
       LastModified: <Date Representation>,
       Metadata: {
       },
       TagCount: 2,
       VersionId: "null"
      }
      */
    });
  };

  _getContent({ filenames, bucket }, cb) {
    //fnames = sanitizeArray(fnames);
    filenames = sanitizeArray(filenames);
    debug(`_getContent: filenames: [${filenames.join(',')}], bucketName: "${bucket}"`);

    return async.mapLimit(
      filenames,
      MAX_SIM_UPLOADS,
      (filename, callback) => this._getContentOne({filename, bucket}, callback),
      cb  // A callback which is called after all the iteratee functions have finished. Result will be either true or false depending on the values of the async tests. Invoked with (err, result).
      //(...args) =>{
      //  console.log('_getContent: cb:', ...args);
      //  cb(...args);
      //}
    );
  }

  ////async getObject(params, cb) {
  ////  //console.log('>>>>>>>>>>>>>>', params)
  ////  return this._wrapAsyncSafe(this._getObject, params, cb);
  ////};
  //
  //async getContentOne(params, cb) {
  //  return this._wrapAsyncSafe(this._getContentOne, params, cb);
  //};

  async getContent(params, cb) {
    return this._wrapAsyncSafe(this._getContent, params, cb);
  };


  _upload({ filenames, bucket }, cb) {
    //fnames = sanitizeArray(fnames);
    filenames = sanitizeArray(filenames);
    debug(`_upload: filenames: [${filenames.join(',')}], bucketName: "${bucket}"`);

    return async.mapLimit(
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

  _deleteObjects({ bucket, keys }, cb) {
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
      //: util.promisify(fn.bind(this))(params)
           : new Promise((resolve, reject) => fn.call(this, params, (...cbArgs) => {
        if (cbArgs[0]) {
          return reject(cbArgs[0]);
        } else {
          resolve(cbArgs[1]);
        }

      }))
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
    const allKeys = await this.listKeys({ bucket });
    debug('deleteAll(): allKeys:', allKeys);

    await this.deleteObjects({ bucket, keys: allKeys });

    const keysFinal = await this.listKeys({ bucket });
    if (keysFinal.length !== 0) throw new Error('deleteAll: keysFinal.length !== 0');

    debug('deleteAll: SUCCESS');
    return allKeys;
  }

  async deleteFiltered({ bucket, filter }) {
    debug('deleteFiltered(): filter:', filter);
    const allKeys = await this.listKeys({ bucket });
    debug('deleteFiltered(): allKeys:', allKeys);

    const filteredKeys = _.filter(allKeys, filter);
    debug('deleteFiltered(): filteredKeys:', allKeys);

    await this.deleteObjects({ bucket, keys: filteredKeys });

    const keysFinal = await this.listKeys({ bucket });
    if (keysFinal.length !== allKeys.length - filteredKeys.length) {
      throw new Error(`deleteFiltered: keysFinal.length: ${keysFinal.length}`+
        `, allKeys.length: ${allKeys.length}`+
        `, filteredKeys.length: ${filteredKeys.length}`);
    }

    debug('deleteFiltered: SUCCESS');
    return allKeys;
  }

}


module.exports = S3wrapper;
