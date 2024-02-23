let express = require('express');
let multer = require('multer')
let multerS3 = require('multer-s3')
let client = require('./redis')
const Bull = require('bull');
const cors = require('cors');
let AWS = require('aws-sdk');
const s3 = new AWS.S3();

let app = express();
require('dotenv').config();
const port = 3002;
app.use(cors());

AWS.config.update({
    accessKeyId: process.env.AWS_ACCESS_KEY_ID,
    secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY
});

let myBucket = process.env.AWS_BUCKET_NAME;

// Create a Bull queue instance
const fileUploadQueue = new Bull('fileUploadQueue');

let upload = multer({
    storage: multerS3({
        s3: s3,
        bucket: myBucket,
        acl: "public-read",
        contentType: multerS3.AUTO_CONTENT_TYPE,
        key: function (req, file, cb) {
            const fileName = `${Date.now().toString()}-${file.originalname}`;
            cb(null, fileName);
        }
    })
});



// Process uploaded files asynchronously
fileUploadQueue.process(async (job) => {
    const files = job.data.files;

    // Process each uploaded file
    for (const file of files) {
        const s3Params = {
            Bucket: myBucket,
            Key: file.originalname
        };

        // Get signed URL for the uploaded file
        const signedUrl = await s3.getSignedUrlPromise('getObject', s3Params);
        // console.log('Signed URL for', file.originalname, ':', signedUrl);

        // // Save the signed URL to Redis 
        await client.set(file.originalname, signedUrl);
        await client.expire(file.originalname, 60); // Expire in 60 seconds
    }
});

// To upload the multiple files
app.post("/uploads", upload.array("images", 200), (req, res, next)=>{
    const files = req.files.map(file => ({ originalname: file.originalname }));
    fileUploadQueue.add({ files });
    res.json({ message: `Successfully uploaded ${req.files.length} files` });
});

// To get the all files from the s3 bucket
app.get("/album", async (req, res, next) => {
        const baseURL = `https://${myBucket}.s3.ap-southeast-2.amazonaws.com/`
        const cacheValue = await client.get('cachedData');
        if (cacheValue) {
          console.log('Data fetched from cache');
          const urlArr = JSON.parse(cacheValue).Contents.map(e => baseURL + e.Key);
          return res.json(urlArr);
        }
    s3.listObjects({Bucket: myBucket})
    .promise()
    .then(data => {
        console.log(data)
        let urlArr = data.Contents.map(e => baseURL + e.Key);
        client.set('cachedData', JSON.stringify(data));
        client.expire('cachedData', 120);
        console.log('Data fetched from API and cached');
        res.json(urlArr)
    })
    .catch(err => console.log(err));
})


app.listen(port, () => {
    console.log(`Server is running on port ${port}`);
})

