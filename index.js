/****
    gulp-s3-upload
    gulp plugin for uploading assets into AWS's S3 service.
****/

var es          = require('event-stream'),
    AWS         = require('aws-sdk'),
    {
        S3, HeadObjectCommand, PutObjectCommand
    } = require("@aws-sdk/client-s3"),
    { Upload }  = require("@aws-sdk/lib-storage"),
    path        = require('path'),
    mime        = require('mime'),
    hasha       = require('hasha'),
    _           = require('underscore'),
    helper      = require('./src/helper.js'),
    PluginError = require('plugin-error'),
    fancyLog    = require('fancy-log'),
    colors      = require('ansi-colors'),
    gulpPrefixer;

const PLUGIN_NAME = 'gulp-s3-upload';

gulpPrefixer = function (AWS, credential) {

    return function (options, s3conf) {
        fancyLog(colors.gray("config"), s3conf);
        var stream
        ,   _s3         = new S3({credentials: credential, region: 'ap-northeast-1', endpoint: `https://s3-ap-northeast-1.amazonaws.com/${options.Bucket || options.bucket}`})
        ,   the_bucket  = options.Bucket || options.bucket
        ;

        fancyLog(colors.gray("start....."), "A");

        if(!the_bucket) {
            throw new PluginError(PLUGIN_NAME, "Missing S3 bucket name!");
        }

        fancyLog(colors.gray("file uploading start....."), "A");
        //  Async File Uploading

        stream = es.map(function (file, callback) {

            var keyTransform, keyname, keyparts, filename,
                mimetype, mime_lookup_name, hash, nohash
                metadata = null, content_encoding = null
            ;

            if(file.isNull()) {
                fancyLog(colors.gray("Do nothing if no contents....."), keyname);
                //  Do nothing if no contents
                return callback(null);
            }

            //  =====================================================
            //  ============= METHOD TRANSFORMS & LOOKUPS ===========
            //  =====================================================

            //  === Key Transform ===================================
            //  Allow for either keyTransform or nameTransform.
            //  We're using Key to be consistent with AWS-S3.

            keyTransform = options.keyTransform || options.nameTransform;   // old option name

            fancyLog(colors.gray("key transform....."), "A");
            if(keyTransform) {

                //  Allow the transform function to take the
                //  complete path in case the user wants to change
                //  the path of the file, too.

                keyname = keyTransform(file.relative);

            } else {
                // ...Otherwise keep it exactly parallel.

                keyparts = helper.parsePath(file.relative);
                keyname  = helper.buildName(keyparts.dirname, keyparts.basename + keyparts.extname);
            }


            keyname = keyname.replace(/\\/g, "/"); // JIC Windows (uses backslashes)


            // === Mime Lookup/Transform ============================

            mime_lookup_name = keyname;

            if (options.mimeTypeLookup) {
                mime_lookup_name = options.mimeTypeLookup(keyname);
            }

            mimetype = mime.lookup(mime_lookup_name);

            // === Charset ==========================================
            // JIC text files get garbled. Appends to mimetype.
            // `charset` field gets filtered out later.

            if (options.charset) {
                mimetype += ';charset=' + options.charset;
            }

            //  === metadataMap =====================================
            //  Map files (using the keyname) to a metadata object.
            //  ONLY if `options.Metadata` is undefined.
            //  ** WILL DEPRICATE IN 2.0.0 **

            if (_.isFunction(options.metadataMap)) {
                metadata = options.metadataMap;
            } else if(_.isObject(options.metadataMap)) {
                options.Metadata = options.metadataMap;
            }

            //  *Note: `options.Metadata` is not filtered out later.

            //  === manualContentEncoding ===========================
            //  Similar to metadataMap to put global / individual
            //  headers on each file object (only if
            //  options.ContentEncoding) is undefined. (1.2)
            //  ** WILL DEPRICATE IN 2.0.0 **

            if(_.isFunction(options.manualContentEncoding)) {
                content_encoding = options.manualContentEncoding;
            } else if(_.isString(options.manualContentEncoding)) {
                options.ContentEncoding = options.manualContentEncoding;
            }

            //  Check the file that's up in the bucket already

            fancyLog(colors.gray("heade object....."), "A");

            const headObjectCommand = new HeadObjectCommand({
                Bucket: the_bucket,
                Key: keyname
            });
            
            _s3.send(headObjectCommand, {
                'Bucket': the_bucket,
                'Key': keyname,
            }, function (head_err, head_data) {

                var obj_opts;

                fancyLog(colors.gray("obj_opts....."), "A");

                // //  If object doesn't exist then S3 returns 404 or 403 depending on whether you have s3:ListBucket permission.
                // //  See http://docs.aws.amazon.com/AmazonS3/latest/API/RESTObjectHEAD.html#rest-object-head-permissions
                // if(head_err && !(head_err.statusCode === 404 || head_err.statusCode === 403)) {
                //     fancyLog(colors.gray("s3 headobject error....."), head_err);
                //     return callback(new PluginError(PLUGIN_NAME, "S3 headObject Error: " + head_err.stack));
                // }

                //  === ETag Hash Comparison =============================
                //  Do a local hash comparison to reduce
                //  the overhead from calling upload anyway.
                //  Add the option for a different algorithm
                //  JIC for some reason the algorithm is not MD5.
                //  Available algorithms are those available w/ default
                //  node `crypto` plugin. (run `crypto.getCiphers()`)

                if(!options.etag_hash) {
                    //  If not defined, default to md5.
                    options.etag_hash = 'md5';
                }

                fancyLog(colors.gray("hsash compare....."), "A");

                //  Hashing requires us to have the entire contents of
                //  the file. This is not possible for streams.
                nohash = file.isStream() || options.etag_hash == 'none';

                hash = nohash ? 'nohash' : hasha(file._contents, {'algorithm': options.etag_hash});

        

                    /*** FILE "LOOP" ***/

                    obj_opts = _.extend({}, helper.filterOptions(options)); // always make sure clean hash

                    obj_opts.Bucket     = the_bucket;
                    obj_opts.Key        = keyname;
                    obj_opts.Body       = file.contents;

                    if(mimetype.length) {
                        //  A check in case of map ContentType
                        obj_opts.ContentType = mimetype;
                    }

                    if(_.isFunction(metadata)) {
                        // existing obj_opts.Metadata gets overwrriten
                        obj_opts.Metadata = metadata(keyname);
                    }

                    if(_.isFunction(content_encoding)) {
                        // existing obj_opts.ContentEncoding gets overwrriten
                        obj_opts.ContentEncoding = content_encoding(keyname);
                    }

                    //  === maps.ParamNames =================================
                    //  This is a new mapper object that, if given in the
                    //  options as `maps.ParamName`, and is a function, will
                    //  run the given function and map that param data, given
                    //  that the return value of the `maps.ParamName` function
                    //  returns the appropriate type for that give putObject Param
                    //  { Bucket: ... maps: { 'CacheControl': function()..., 'Expires': function()... }, etc. }
                    //  See: http://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/S3.html#putObject-property
                    //  This will end up overwriting old Metadata and ContentEncoding
                    //  if they were included in maps hash.

                    if(!_.isUndefined(options.maps)) {
                        _.each(options.maps, function(mapRoutine, param_name) {
                            if(_.isFunction(mapRoutine)) {
                                obj_opts[param_name] = mapRoutine(keyname);
                            }
                        });
                    }

                    if (options.uploadNewFilesOnly && !head_data || !options.uploadNewFilesOnly) {

                        //  When using streams, the ContentLength must be
                        //  known to S3. This is only possible if the incoming
                        //  vinyl file somehow carries the byte length.

                        if (file.isStream()) {
                            if (file.stat) {
                                obj_opts.ContentLength = file.stat.size;
                            } else {
                                return callback(new PluginError(PLUGIN_NAME, "S3 Upload of streamObject must have a ContentLength"));
                            }
                        }

                        fancyLog(colors.cyan("Uploading ..... "), keyname);

                        const putObjectCommand = new PutObjectCommand(obj_opts);

                        // const uploadS3 = new Upload({
                        //     client: _s3,
                        //     queueSize: 4,
                        //     leavePartsOnError: false,
                        //     params: {
                        //         Bucket: the_bucket,
                        //         Key: keyname,
                        //         Body: file.contents,
                        //     }
                        // });

                        // uploadS3.on("httpUploadProgress", (progress) => {
                        //     console.log(progress);
                        // });

                        // uploadS3.done().then(() => {
                        //     // Doesn't exist in bucket; the object is new to the bucket
                        //     fancyLog(colors.green("Uploaded! ..... "), keyname);

                        //     if (options.onNew && typeof options.onNew === 'function') {
                        //         options.onNew.call(this, keyname);
                        //     }
                        //     callback(null);
                        // });

                        _s3.send(putObjectCommand, function (err, data) {
                            if (err) {
                                return callback(new PluginError(PLUGIN_NAME, "S3 putObject Error: " + err.stack));
                            }

                            if (head_data) {
                                if (head_data.ETag !== data.ETag) {
                                    fancyLog(colors.yellow("Updated ....... "), keyname);

                                    if (options.onChange && typeof options.onChange === 'function') {
                                        options.onChange.call(this, keyname);
                                    }

                                } else {
                                    fancyLog(colors.gray("No Change ..... "), keyname);

                                    if (options.onNoChange && typeof options.onNoChange === 'function') {
                                        options.onNoChange.call(this, keyname);
                                    }

                                }
                            } else {
                                // Doesn't exist in bucket; the object is new to the bucket
                                fancyLog(colors.green("Uploaded! ..... "), keyname);

                                if (options.onNew && typeof options.onNew === 'function') {
                                    options.onNew.call(this, keyname);
                                }
                            }

                            callback(null);
                        });

                    } else {
                        fancyLog(colors.gray("Skipping Upload of Existing File ..... "), keyname);

                        callback(null);
                    }

                    /*** END FILE LOOP ***/
                
            });
        });

        return stream;
    };
};

// ===== EXPORTING MAIN PLUGIN FUNCTION =====
// `config` now takes the paramters from the AWS-SDK constructor:
// http://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/Config.html#constructor-property

module.exports = function(config, s3_config) {
    var aws_config = {};

    if(_.isUndefined(config)) {
        config = {};
    }

    //  Maintain backwards compatibility with legacy key and secret options
    if(config.key) {
        aws_config.accessKeyId = config.key;
    }

    if(config.secret) {
        aws_config.secretAccessKey = config.secret;
    }

    if(config.region) {
        aws_config.region = config.region;
    }

    //  If using IAM

    if(_.has(config, 'useIAM') && config.useIAM) {
        config = {};
    }

    //  Intentionally not mandating the accessKeyId and secretAccessKey as they
    //  will be loaded automatically by the SDK from either environment variables
    //  or the credentials file.
    //  http://docs.aws.amazon.com/AWSJavaScriptSDK/guide/node-configuring.html

    // Configure the proxy if an environment variable is present.

    if(process.env.HTTPS_PROXY) {
        fancyLog("Setting https proxy to %s", process.env.HTTPS_PROXY);

        if(!aws_config.httpOptions) {
            aws_config.httpOptions = {};
        }

        var HttpsProxyAgent = require('https-proxy-agent');

        aws_config.httpOptions.agent = new HttpsProxyAgent(process.env.HTTPS_PROXY);
    }

    //  Update the global AWS config if we have any overrides

    AWS.config.update(_.extend({region: 'ap-northeast-1'}, config, aws_config));

    return gulpPrefixer(AWS.config, config);
};
