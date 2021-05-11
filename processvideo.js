/** Worker for video upload processvideo.js
@version 0.3
@author Jesse Thompson
Worker for processing video upload to S3 server and database recording
*/

const cluster = require('cluster');
const ffmpeg = require('ffmpeg');
const path = require('path');
const fs = require('fs');
const cp = require('child_process');
const mongoose = require('mongoose');
const session = require('express-session');
const uuidv4 = require('uuid/v4');
const MongoDBStore = require('connect-mongodb-session')(session);
const neo = require('./neo.js');

const User = require('../models/user');
const Chat = require('../models/chat');
const Video = require('../models/video');
const redis = require('../redis');
const redisclient = redis.redisclient;
const servecloudfront = require('./servecloudfront.js');
const { resolveLogging } = require('../scripts/logging.js');

// file upload
const aws = require('aws-sdk');
const s3Cred = require('./api/s3credentials.js');
const multer = require('multer');
aws.config.update(s3Cred.awsConfig);
aws.config.apiVersions = {
    rekognition: '2016-06-27'
}
const s3 = new aws.S3();
const recognition = new aws.Rekognition();
const videoMpdBucket = "minifs";

// Resolutions for video conversion
const resolutions = [2048, 1440, 1080, 720, 480, 360, 240];
const audioCodecs = ["aac", "ac3", "als", "sls", "mp3", "mp2", "mp1", "celp", "hvxc", "pcm_s16le"];
const supportedContainers = ["mov", "3gpp", "mp4", "avi", "flv", "webm", "mpegts", "wmv", "matroska"];
Object.freeze(resolutions); Object.freeze(audioCodecs);

const storage = multer.memoryStorage();

const createObj = (obj) => {
    let newObj = {};
    return Object.assign(newObj, obj);
}

const mongoOptions = {
    auth: {authdb: s3Cred.mongo.authDb },
    user: s3Cred.mongo.u,
    pass: s3Cred.mongo.p
};

// connect mongoose
mongoose.connect(s3Cred.mongo.address, mongoOptions)
    .then(() => resolveLogging() ? console.log('MongoDB Connected(processvideo.js)') : null)
    .catch(err => console.log(err));

const db = mongoose.connection;
//mongo error
db.on('error', console.error.bind(console, 'connection error:'));

// mongo store
const store = new MongoDBStore(
    {
        uri: s3Cred.mongo.addressAuth,
        databaseName: 'minipost',
        collection: 'sessions'
    }
);

let io = null;
const convertVideos = async function(i, originalVideo, objUrls, generatedUuid, encodeAudio, room, body, socket, job) {
    console.log("running conversion");
    /* If encode audio is set to true, encode audio and run convertVideos at same iteration, then set encode audio to false at same iteration. Add support in future for multiple audio encodings. */
    console.log(i, resolutions.length);
    if (i < resolutions.length) { // Convert if iteration is less than the length of resolutions constant array
        try {
            let process = new ffmpeg(originalVideo, {maxBuffer: 512 * 1000})
                .catch((err) => {
                    console.log(err);
                });
            const format = "mp4";
            const audioFormat = "mp4";
            console.log("encode Audio: " + encodeAudio);
            if (encodeAudio) {
                process.then(async function(audio) {
                    if (audioCodecs.indexOf(audio.metadata.audio.codec.toLowerCase()) >= 0) { // Determine if current audio codec is supported
                        job.progress(room + ";converting audio");
                        let rawPath = "temp/" + generatedUuid + "-audio" + "-raw" + "." + audioFormat;
                        audio.addCommand('-vn'); // Video none
                        audio.addCommand('-c:a', "aac"); // Convert all audio to aac, ensure consistency of format
                        audio.addCommand('-b:a', '256k'); 
                        if (audio.metadata.audio.channels.value == null || audio.metadata.audio.channels.value == 0) {
                            audio.addCommand('-ac', '6'); // If channels value is null or equal to 0, convert to surround sound
                        }
                        audio.save("./" + rawPath, async function (err, file) { // Creates audio file at path specified. Log file for path.
                            if (!err) {
                                objUrls.push({
                                    "path" : rawPath,
                                    "detail" : "aac"
                                });
                                return convertVideos(i, originalVideo, objUrls, generatedUuid, false, room, body, socket, job);
                            } else {
                                console.log(err);
                                deleteJob(false, job, null, room);
                                deleteVideoArray(objUrls, originalVideo, room, 15000);
                                job.progress(room + ";something went wrong");
                                return err;
                            }
                        });
                    } else {
                        deleteJob(false, job, null, room);
                        deleteVideoArray(objUrls, originalVideo, room, 15000);
                        console.log("Audio codec not supported");
                        job.progress(room + "audio codec not supported");
                    }
                })
                .catch((err) => {
                    console.log(err);  
                    deleteJob(false, job, null, room);
                    deleteVideoArray(objUrls, originalVideo, room, 15000);
                    console.log("Audio codec not supported");
                    job.progress(room + "audio codec not supported");
                });
            } else {
                process.then(async function (video) {
                    job.progress(room + ";converting " + resolutions[i] + "p video");
                    let rawPath = "temp/" + generatedUuid + "-" + resolutions[i] + "-raw" + "." + format;
                    video.setVideoSize("?x" + resolutions[i], true, true).setDisableAudio();
                    video.addCommand('-vcodec', 'libx264');
                    if (video.metadata.video.codec == "mpeg2video") {
                        video.addCommand('-preset', 'medium');
                    } else {
                        video.addCommand('-preset', 'faster');
                    }
                    video.addCommand('-crf', '24');
                    video.addCommand('-tune', 'film');
                    video.addCommand('-x264-params', 'keyint=24:min-keyint=24:no-scenecut');
                    video.save("./" + rawPath, async function (err, file) { // Creates new file at path specified. Log file for file location
                        objUrls.push({
                            "path" : rawPath,
                            "detail" : resolutions[i]
                        });
                        if (!err) {
                            return convertVideos(i+1, originalVideo, objUrls, generatedUuid, false, room, body, socket, job);
                        } else {
                            console.log(err);
                            deleteJob(false, job, null, room);
                            deleteVideoArray(objUrls, originalVideo, room, 15000);
                            job.progress(room + ";video conversion error");
                            return err;
                        }
                    });
                })
                .catch((err) => {
                    console.log(err);  
                    deleteJob(false, job, null, room);
                    deleteVideoArray(objUrls, originalVideo, room, 15000);
                    console.log("Audio codec not supported");
                    job.progress(room + "audio codec not supported");
                });
            }
        } catch (e) {
            console.log("Error msg: " + e.msg);
            deleteJob(false, job, null, room);
            deleteVideoArray(objUrls, originalVideo, room, 15000);
            job.progress(room + ";conversion error");
            return e;
        }
    } else {
        makeMpd(objUrls, originalVideo, room, body, generatedUuid, socket, job);
    }
    return objUrls;
}

// This produces the mpd with all relevant relative pathing to files and other details
const makeMpd = async function(objUrls, originalVideo, room, body, generatedUuid, socket, job) {
    let delArr = [];
    try {
        const exec_options = {
            cwd: null,
            env: null,
            encoding: 'utf8',
            timeout: 0,
            maxBuffer: 200 * 1024
        };

        const captureName = /([a-z].*)\/([a-z0-9].*)-/;
        const matchPathExcludePeriod = /([a-z].*)([a-z0-9]*)[.]([a-z].*)/;
        const rawObjUrls = [];
        if (objUrls) {
            for (let i = 0; i < objUrls.length; i++) { // Go through all uri's and create references in array to all raw obj uri's
                rawObjUrls[i] = createObj(objUrls[i]);
            }
        }
        // This is the single area that we need to reference shaka packager's exe to build mpd's
        // let command = "cd scripts/src/out/Release && packager.exe"; 
        let command = "packager";
        let args = "";
        let tempM3u8 = [];
        for (obj of objUrls) {
            let detail = obj.detail;
            let fileType = "";
            if (resolutions.toString().indexOf(obj.detail) >= 0) {
                fileType = "video";
            } else if (audioCodecs.toString().indexOf(obj.detail) >= 0) {
                fileType = "audio";
                detail = "audio";
            } else {
                fileType = "text";
            }
            // Relative is the directories relative to the packager.exe file. 
            // Obj.path is the path of the files passed in the array
            // File type is specified for packager to understand what it needs to do
            // Detail is the added info to specify a file (audio, 1080, 720)
            // in is input, output is output
            console.log(obj.path);
            args += "in=" + obj.path + ",stream=" + fileType + ",output=" + obj.path.match(/([\/a-z0-9]*)-([a-z0-9]*)-([a-z]*)/)[1] + "-" + detail + ".mp4"; 
            args += ",playlist_name=" + obj.path.match(/([\/a-z0-9]*)-([a-z0-9]*)-([a-z]*)/)[1] + "-" + detail + ".m3u8"; // HLS
            // All HLS functionality for both if conditions
            if (detail == "audio") { 
                args += ",hls_group_id=audio,hls_name=ENGLISH ";
                tempM3u8.push({ "path": obj.path.match(/([\/a-z0-9]*)-([a-z0-9]*)-([a-z]*)/)[1] + "-" + detail + ".m3u8", "detail": "audio" });
            } else {
                args += ",iframe_playlist_name=" + obj.path.match(/([\/a-z0-9]*)-([a-z0-9]*)-([a-z]*)/)[1] + "-" + detail + "_iframe.m3u8 ";
                tempM3u8.push({ "path": obj.path.match(/([\/a-z0-9]*)-([a-z0-9]*)-([a-z]*)/)[1] + "-" + detail + ".m3u8", "detail": "video" });
                tempM3u8.push({ "path": obj.path.match(/([\/a-z0-9]*)-([a-z0-9]*)-([a-z]*)/)[1] + "-" + detail + "_iframe.m3u8", "detail": "video" });
            }
            obj.path = obj.path.match(/([\/a-z0-9]*)-([a-z0-9]*)-([a-z]*)/)[1] + "-" + detail + ".mp4"; // Change the path to the object to reference for aws s3 transfer
        }
        objUrls = objUrls.concat(tempM3u8);
        tempM3u8 = null;
        const expectedMpdPath = objUrls[0].path.match(/([\/a-z0-9]*)-([a-z0-9]*)/)[1] + "-mpd.mpd"; // make expected mpd file string
        const expectedHlsPath = objUrls[0].path.match(/([\/a-z0-9]*)-([a-z0-9]*)/)[1] + "-hls.m3u8"; // make expected hls file string (iOS)
        args += "--mpd_output " + expectedMpdPath + " --hls_master_playlist_output " + expectedHlsPath; // add expected relative mpd output path. Relative means where ever the mpd is existing these files must also be existing in that folder. E.g if the mpd is in a folder, the files must be relative to that folder. Ideally just put them in the same folder. HLS aswell
        console.log(command + " " + args);
        // Log above variables to see full child process command to be run
        let data = cp.exec(command + " " + args, {maxBuffer: 1024 * 8000}, function(err, stdout, stderr) { // 8000kb max buffer
            if (err) {
                console.log(expectedMpdPath, expectedHlsPath);
                console.log(err);
                console.log("Something went wrong, mpd was not created");
                job.progress(room + ";something went wrong");
                deleteJob(false, job, null, room);
                deleteVideoArray(objUrls, originalVideo, room, 15000);
            } else {
                try {
                    if (fs.existsSync("./" + expectedMpdPath)) {
                        let mpdObj = {
                            "path" : expectedMpdPath,
                            "detail" : "mpd"
                        };
                        let hlsObj = {
                            "path" : expectedHlsPath,
                            "detail" : "hls"
                        }
                        objUrls.push(mpdObj);
                        objUrls.push(hlsObj);
                        uploadAmazonObjects(objUrls, originalVideo, room, body, generatedUuid, rawObjUrls, socket, job);
                    } else {
                        console.log("Something went wrong, mpd was not created");
                        job.progress(room + ";something went wrong");
                        delArr.push(...objUrls, ...rawObjUrls);
                        deleteJob(false, job, null, room);
                        deleteVideoArray(delArray, originalVideo, room, 15000);
                    }
                } catch (err) {
                    console.log(err);
                    console.log("Something went wrong, mpd was not created");
                    job.progress(room + ";conversion error");
                    delArr.push(...objUrls, ...rawObjUrls);
                    deleteJob(false, job, null, room);
                    deleteVideoArray(delArray, originalVideo, room, 15000);
                }
            }
        });
    } catch (err) {
        console.log(err);
        console.log("Something went wrong, mpd was not created");
        job.progress(room + ";conversion error");
        delArr.push(...objUrls, ...rawObjUrls);
        deleteJob(false, job, null, room);
        deleteVideoArray(delArray, originalVideo, room, 15000);
    }
}

// Uploads individual amazon objects in array to amazon
const uploadAmazonObjects = async function(objUrls, originalVideo, room, body, generatedUuid, rawObjUrls, socket, job) {
    // The locations array will hold the location of the file once uploaded and "detail"
    // Detail will tell the resolution if its a video, the language if its audio or language-s if its a subtitle
    // Use the locations array to build the dash mpd file

    // Do a check of the user record here to see if they've decided to delete it. If so abort the whole upload
    // 
    job.progress(room + ";sending converted files to our servers");
    let s3Objects = [];
    if (objUrls.length == 0) {
        job.progress(room + ";something went wrong");
        deleteJob(false, job, null, room);
        deleteVideoArray(delArray, originalVideo, room, 15000);
    }
    let delArr = [];
    const keyRegex = /[a-z].*\/([a-z0-9].*)/; // Matches entire key
    const resoRegex = /-([a-z0-9].*)\./; // Matches the detail data at the end of the object key
    let uploadData;
    for (let i = 0; i < objUrls.length; i++) {
        try {
            let data = fs.createReadStream(objUrls[i].path);
            uploadData = await s3.upload({ Bucket: 'minifs', Key: objUrls[i].path.match(keyRegex)[1], Body: data }).promise();
            if (await uploadData) { // Wait for data to be uploaded to S3
                s3Objects.push({location: uploadData.Location, detail: uploadData.Key.match(resoRegex)[1]});
                console.log(uploadData);
                if (data) {
                    if (i == objUrls.length-1) {
                        delArr.push(...objUrls, ...rawObjUrls);
                        makeVideoRecord(s3Objects, body, room, generatedUuid, socket, job, delArr, originalVideo);
                        if (io) {
                            io.to(room).emit('uploadUpdate', "upload complete");
                        }
                    }
                }
            } else {
                console.log("Something went wrong, not all objects uploaded to s3");
                job.progress(room + ";something went wrong");
                delArr.push(...objUrls, ...rawObjUrls);
                deleteJob(false, job, null, room);
                deleteVideoArray(delArray, originalVideo, room, 15000);
            }
        } catch (err) {
            console.log("Something went wrong, not all objects uploaded to s3");
            job.progress(room + ";something went wrong");
            delArr.push(...objUrls, ...rawObjUrls);
            deleteJob(false, job, null, room);
            deleteVideoArray(delArray, originalVideo, room, 15000);
        }
    }
};

const makeVideoRecord = async function(s3Objects, body, room, generatedUuid, socket, job, delArr, originalVideo) {
    let objLocations = [];
    let mpdLoc = "";
    let hlsLoc = "";
    let videoCheck = null;
    for (obj of s3Objects) {
        objLocations.push(obj.location);
        if (obj.location.match(/.*(mpd).*/)) {
            mpdLoc = obj.location.match(/.*(mpd).*/)[0]; // Match mpd location for mongoDb record
        } else if (obj.location.match(/.*(hls).*/)) {
            hlsLoc = obj.location.match(/.*(hls).*/)[0]; // Match hls location for mongoDb record
        } else if (videoCheck == null && obj.location.match(/.*(\/)([a-zA-Z0-9-].*)(mp4).*/)) {
            if (!obj.location.match(/.*(\/)([a-zA-Z0-9-].*)(\-audio).(mp4).*/)) {
                videoCheck = obj.location.match(/.*(\/)([a-zA-Z0-9-].*)(mp4).*/)[2] + obj.location.match(/.*(\/)([a-zA-Z0-9-].*)(mp4).*/)[3]; // Matches first video found in objLocations for profanity/porn check. Necessary as without filtering it could match audio or the mpd.
            }
        }
    }
    let awaitingInfo = function(videoRecord) { // If title is present dont append anything past state, else append awaitinginfo
        if (videoRecord) {
            if (videoRecord.title.length == 0) {
                return ";awaitinginfo";
            } else {
                return "";
            }
        } else {
            return "";
        }
    }
    let createVideoData = { mpd: mpdLoc, hls: hlsLoc, locations: objLocations, state: Date.parse(new Date) };
    if (job.data.advertisement) {
        createVideoData.advertisement = true;
    }
    let videoRecord = await Video.findOneAndUpdate({ _id: generatedUuid }, {$set: createVideoData }, { new: true });
    if (await videoRecord) {
        let mpd;
        if (videoRecord.mpd) mpd = videoRecord.mpd;
        let userObj = await User.findOne({ username: body.username });
        for (let i = 0; i < userObj.videos.length; i++) {
            if (userObj.videos[i].id == generatedUuid) { // Go through all of users videos and find the match for this video
                // Updates user record based on whether or not video document is waiting for info (has a title) or not.
                let jobId = null;
                if (userObj.videos[i].jobId) {
                    jobId = userObj.videos[i].jobId;
                }
                let updateRec = { "videos.$" : {id: generatedUuid, state: Date.parse(new Date).toString() + awaitingInfo(videoRecord), jobId: jobId }};
                if (job.data.advertisement) {
                    updateRec = { "videos.$" : {id: generatedUuid, state: Date.parse(new Date).toString() + awaitingInfo(videoRecord), advertisement: true, jobId: jobId }};
                }
                let userVideoRecord = await User.findOneAndUpdate({ username: body.username, "videos.id": generatedUuid }, {$set: updateRec }, { upsert: true, new: true});
                if (await userVideoRecord) {
                    let userUuid = await User.findOne({ username: body.username }).then((user) => { return user._id });
                    let advertisement = null;
                    if (job.data.advertisement) {
                        advertisement = job.data.advertisement;
                    }
                    neo.createOneVideo(body.username, userUuid, generatedUuid, null, null, null, null, null, null, null, null, advertisement, null)
                        .then((data) => {
                            if (advertisement) {
                                return profanityCheck(videoCheck, generatedUuid, "AdVideo"); // Set profanity check job after ad record is created on neo
                            } else {
                                return profanityCheck(videoCheck, generatedUuid); // Set profanity check job after record is created on neo
                            }
                        })
                        .then((data) => {
                            return neo.updateChannelNotifications(userUuid, generatedUuid, "video");
                        })
                        .then((data) => {
                            deleteJob(true, job, mpd, room); // Success
                            deleteVideoArray(delArr, originalVideo, room, 10000);
                        });
                }
                break;
            }
        }
    } else {
        
    }
}

// This sends a request to amazon web services to start the content moderation service on the video uploaded to the bucket.
const profanityCheck = async (record, generatedUuid, advertisement = null) => {
    const roleArnId = s3Cred.awsConfig.roleArnId;
    const snsTopicArnId = s3Cred.awsConfig.snsTopicArnId;
    const params = {
        Video: {
            S3Object: {
                Bucket: videoMpdBucket,
                Name: record
            }
        },
        ClientRequestToken: generatedUuid,
        JobTag: 'detectProfanityVideo',
        NotificationChannel: {
            SNSTopicArn: 'arn:aws:sns:us-east-2:546584803456:AmazonRekognition',
            RoleArn: 'arn:aws:iam::546584803456:role/minifsrekognitionaccess'
        }
    }
    return await recognition.startContentModeration(params, function(err, data) {
        if (err) {
            console.log(err);
            return null;
        }
        if (data) {
            let jobId = data.JobId;
            return neo.setProfanityCheck(generatedUuid, jobId, null, advertisement); // Sets profanity check reference on neo object to check later for job completion
        }
    });
}

// Deletes originally converted videos from temporary storage (usually after they have been uploaded to an object storage) Waits for brief period of time after amazon upload to ensure files are not being used.
const deleteVideoArray = function(videos, original, room, delay) {
    try {
        setTimeout(() => {
            for (let i = 0; i < videos.length; i++) {
                try {
                    if (videos[i].path) {
                        if (typeof videos[i].path === 'string' && videos[i].path !== undefined) {
                            if (videos.length > 0) { // Very crucial, can run into critical errors if this is not checked
                                let object = videos[i].path;
                                if (!object) {
                                    object = "null path";
                                }
                                fs.unlink(videos[i].path, (err) => {
                                    if (err) {
                                        console.log(err);
                                    } else {
                                        console.log(object + " deleted from temp storage");
                                    }
                                });
                            }
                        }
                    }
                } catch (err) {
                    console.log(err);
                }
            };
            try {
                if (original) {
                    if (typeof original === 'string' && original !== undefined) {
                        fs.unlink(original, (err) => {
                            if (err) {
                                if (typeof original === 'string' && original !== undefined) {
                                    setTimeout((original) => {
                                        try {
                                            if (original) {
                                                fs.unlink(original, (err) => {
                                                    console.log("Original video deleted from temp storage on second try");
                                                });
                                            }
                                        } catch (err) {
                                            // fail silently
                                        }
                                    }, delay);
                                }
                            } else {
                                console.log("Original video deleted from temp storage");
                            }
                        });
                    }
                }
            } catch (err) {
                console.log(err);
            }
        }, delay);
    } catch (err) {
        // Something was undefined as it may have already been deleted
    }
}

// Cleanly deletes redis record. This may be called after job completion but may screw up and prevent bull from finding the lock.
// Try and allow bull to delete lock itself
const deleteRedisRecord = async (job, room) => {
    const searchQuery = "*:video transcoding:" + job.id;
    redisclient.keys(searchQuery, (err, reply) => {
        if (reply) {
            reply.forEach((el, i) => {
                redisclient.del(reply[i]);
            })
        }
    })
    if (room) {
        const roomSearchQuery = room;
        redisclient.keys(roomSearchQuery, (err, reply) => {
            if (reply) {
                reply.forEach((el, i) => {
                    redisclient.del(reply[i]);
                })
            }
        })
    }
}

/* Completes job and sends message to remove job from queue */
const deleteJob = async (complete, job, mpd, room) => {
    if (complete && mpd) {
        job.progress(room + ";video ready;" + servecloudfront.serveCloudfrontUrl(mpd));
    } else {
        job.progress(room + ";video process failed;null");
        let error = {
            message: 'video process failed'
        };
        job.moveToFailed(error, true);
    }
    // setTimeout(() => {
    //     deleteRedisRecord(job, room);
    // }, 500);
}

exports.convertVideos = convertVideos;
exports.deleteOne = deleteOne;
exports.createObj = createObj;
exports.deleteVideoArray = deleteVideoArray;
exports.resolutions = resolutions;
exports.audioCodecs = audioCodecs;
exports.supportedContainers = supportedContainers;
