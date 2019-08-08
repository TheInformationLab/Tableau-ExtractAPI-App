from boto3.session import Session
import boto3

def getS3File(
    accessKey,
    secretKey,
    bucket,
    filename,
    destination,
    folder=''
):

    session = Session(aws_access_key_id=accessKey,
                  aws_secret_access_key=secretKey)
    s3 = session.resource('s3')
    your_bucket = s3.Bucket(bucket)

    filepath = ''
    if( folder != '' and folder != None) :
        filepath = folder + '/'
    filepath = filepath + filename

    return your_bucket.download_file(filepath, destination + '/' + filename)
