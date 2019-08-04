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

  //

  //await miniS3.buckets().list();

  const s3bucket = await miniS3
          .bucket(bucket)
        //.then(s3bucket =>
        //  s3bucket
  ;

  const deletedKeys = await s3bucket.deleteAll();
  console.log('deletedKeys:', deletedKeys);

  //await s3bucket.file(fname1).upload();
  //await s3bucket.file(markerFile).upload();
  await s3bucket.directory(dir).upload();

  await s3bucket.file( fname ).upload();

  const finalKeys = await s3bucket.listKeys();
  console.log('finalKeys:', finalKeys);

  //const filenames = await s3bucket.listNames();
  //console.log('filenames:', filenames);
  //
  ////await asyncForEach(filenames, async fname => await s3bucket.file(fname).delete());
  //await s3bucket.files(filenames).delete();
  //
  //await s3bucket.list();

  console.log('DONE');
};
test_me();
