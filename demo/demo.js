const MiniS3 = require('../index');


const test_me = async () => {
  const credentials = require('../credentials.json');

  const bucket = credentials.bucket;

  const miniS3 = new MiniS3({
    accessKeyId:     credentials.accessKeyId,
    secretAccessKey: credentials.secretAccessKey,
  });

  const dir   = './data';
  const fname = dir + '/test.txt';

  // List buckets

  const buckets = await miniS3
    .buckets()
    .list();
  console.log('buckets:', buckets);

  // Select bucket by name

  const s3bucket = await miniS3
          .bucket(bucket)
        //.then(s3bucket =>
        //  s3bucket
  ;

  // Delete all keys in the bucket (and list keys to verify)

  const deletedKeys = await s3bucket.deleteAll();
  console.log('deletedKeys:', deletedKeys);
  //await s3bucket.files(filenames).delete();

  // Upload all files in directory to the bucket

  ////await s3bucket.file(fname1).upload();
  ////await s3bucket.file( fname ).upload();
  ////await s3bucket.file(markerFile).upload();
  await s3bucket.directory(dir).upload();

  // List keys in the bucket

  //const filenames = await s3bucket.listNames();
  //console.log('filenames:', filenames);
  //await s3bucket.list();
  const finalKeys = await s3bucket.listKeys();
  console.log('finalKeys:', finalKeys);

  console.log('DONE');
};
test_me();
