// const path = require('path');
// const https = require('https');
const request = require('request');
const async = require('async');
// const azure = require("@azure/storage-blob");
const {
        Aborter,
        ContainerURL,
        ServiceURL,
        StorageURL,
        // AnonymousCredential,
        SharedKeyCredential,
        BlobURL,
        // SharedAccessBlobPolicy,
        IBlobSASSignatureValues,
        generateBlobSASQueryParameters,
        // IBlobSASSignatureValues
        BlockBlobURL,
        // TokenCredential 
    } = require("@azure/storage-blob");


const BUCKET = process.env['BUCKET'];
const BACKEND = process.env["BACKEND"];
const STORAGE_ACCOUNT = process.env['STORAGE_ACCOUNT'];
const ACCOUNTKEY = process.env['ACCOUNT_KEY'];
const sharedKeyCredential = new SharedKeyCredential(STORAGE_ACCOUNT, ACCOUNTKEY);

var containerURL;

const getContentTypeHeader = function (headers){
    let output = 'text/plain'
    Object.keys(headers).forEach(k => {
        if (k.toLowerCase()=='content-type' ) {
            output = headers[k];
            // mimetype = headers[k];
            // if (mimetype.toLowerCase().indexOf('application/json')>=0) {
            //     return mimetype;
            // }
        }
    });
    return output;
}
const getSasString = function(permissions, blobName) {
    let signValues = {
        startTime: new Date(),
        expiryTime: new Date(Date.now()+300000),
        permissions: permissions,
        blobName: blobName,
        containerName: BUCKET,
    }
    sasString = generateBlobSASQueryParameters(
        signValues,
        sharedKeyCredential,
    ).toString();
    return sasString;

}
const handleResponse = function (x, context) {

    // context.res = {
    //     // status: 500,
    //     body: `typeof ${typeof x} body:${JSON.stringify(x)}`,
    //     // headers:{'content-type':'application/json'}
    // };
    // context.done();
    // return;

    // let x = {}
    // try {
    //     x = JSON.parse(body)
    // } catch (e){
    //     context.res = {
    //         status: 400,
    //         body: `${e}`,
    //         // headers:{'content-type':'application/json'}
    //     };
    //     context.done();
    //     return;
    // }
    
    // context.res = {
    //     body: x,
    //     headers:{'content-type': 'application/json'}
    // };
    // context.done()

    


    

    if (!x['Send']) {
        context.res = {
            // status: 500,
            body: x
        };
        context.done();
        return;
    }

    let output = [];

    // context.res = {
    //     body: x,
    //     headers:{'content-type':'application/json'}
    // };
    // context.done();
    // return;
    async.each(x['Send'], function(element, callback1){
        switch (element.Service) {
            case 's3':
                s3Action(element).then(result=>{
                    output.push(result)
                    callback1();
                }, err=>{
                    callback1(err)
                });
                break;        
            default:
                callback1(null, element);
                
        }
    }, function(err){
        if (err){
            context.res = {
                status: 400,
                body: err
            };
        } else {
            context.res = {
                body: output
            };

        }
        context.done();

    })

}
const returnContainerURL = function() {

    if (containerURL) {
        return containerURL;
    }
    // const account = STORAGE_ACCOUNT;

    // const pipeline = StorageURL.newPipeline(new AnonymousCredential(), {
    //     retryOptions: { maxTries: 4 }, // Retry options
    //     // telemetry: { value: "HighLevelSample V1.0.0" } // Customized telemetry string
    // });


    // Use SharedKeyCredential with storage account and account key


    // Use sharedKeyCredential, tokenCredential or anonymousCredential to create a pipeline
    const pipeline = StorageURL.newPipeline(sharedKeyCredential);

  

    const serviceURL = new ServiceURL(
        `https://${STORAGE_ACCOUNT}.blob.core.windows.net`,
        pipeline
    );

    containerURL = ContainerURL.fromServiceURL(serviceURL, BUCKET);
    return containerURL;
}

const s3Action = function(a) {

// a.containerURL = containerURL;
// context.res = {
//     body: a,
//     headers: {
//         'Content-Type': 'application/json'
//     }
// };
// context.done()
// return;

        try{
            switch (a.Action) {
                case "listObjects":
                
                    return returnContainerURL().listBlobFlatSegment(Aborter.none).then(result=>{
                        return {Contents: result['segment']['blobItems'].map(x=>{
                            return {
                                Key: x.name,
                                Size: x.properties.contentLength,
                                LastModified: x.properties.lastModified,
                            }
                        })}
                    });
        
                case "putObject":
                    let putBlob = BlobURL.fromContainerURL(returnContainerURL(),a.Params['Key']);
                    let putSasString = getSasString('w',a.Params['Key']);
                    
                    return Promise.resolve(putBlob.url+'?'+putSasString);
        
            
                case "deleteObject":
                    let deleteBlob = BlockBlobURL.fromContainerURL(returnContainerURL(),a.Params['Key']);
                    return deleteBlob.delete(Aborter.none);
        
                    
                case "getObject":
        
                    let getBlob = BlobURL.fromContainerURL(returnContainerURL(),a.Params['Key']);
                    let sasString = getSasString('r',a.Params['Key']);
                    return Promise.resolve(getBlob.url+'?'+sasString);
        
            
                default:
               
            }
            return Promise.resolve(a)
        } catch(e){
            console.log(e)
            return Promise.reject(e)
        }
       
}

    
module.exports = function (context, req) {

    let mimetype = getContentTypeHeader(req.headers);
    let isJson = mimetype && mimetype.indexOf('application/json')>=0;
    
    var options = {
        uri: `${BACKEND}/${req.query.path||''}`,
        method: req.method,
        body: req.body,
        json: isJson,
    };

    if (mimetype) {
        options.headers= {'content-type': mimetype};
    }
    

    request(options, function (err, res, body) {

        if (err){
            context.res = {
                status: 400,
                body: `${err}`
            };
            context.done();
            return;
        }
 
        mimetype = getContentTypeHeader(res.headers);
        isJson = mimetype.indexOf('application/json')>=0;
        // if (mimetype) {
        //     options.headers= {'content-type': mimetype};
        // } else {
        //     isJson = false;
        // }
        
        if (isJson ) {
            handleResponse(body, context);
        } else {
            context.res = {
                body: res.body,
                headers: {'Content-Type': mimetype,'cache-control':'max-age=3600'}
            };
            context.done();    
        }
        
    });
}


        


