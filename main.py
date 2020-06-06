
import json
import boto3



def get_data():
    """
    Get scraped comments from the comment folder in my S3 bucket
    :return: comments
    """
    s3 = boto3.resource('s3')
    bucket = s3.Bucket('diegos-reddit-bucket')

    comments = []
    dict_comments = []

    for obj in bucket.objects.filter(Prefix='comments'):
        body = json.load(obj.get()['Body'])
        dict_comments = list(body.values())

    for dict_comment in dict_comments:
        comment = list(dict_comment.values())
        comments.append(comment[1])

    return comments


def convert_and_put(comments):
    """
    Convert list of strings to one text blob for easier sentiment analysis
    Format text blob of newline characters
    Write text blob to a text file to send to S3 bucket & AWS Comprehend
    """
    print("Creating text blob...")
    text_blob = "".join(str(comment) for comment in comments)
    text_blob = text_blob.replace("\n", "")
    print("Text blob created!")

    print("Attempting to send file to S3 bucket...")
    s3 = boto3.resource("s3")
    bucket_name = "lambda-comprehend-sent-diego"
    key = "blob/blob.txt"
    object = s3.Object(bucket_name, key)
    object.put(Body=text_blob)
    print("File sent to S3 bucket!")



if __name__ == '__main__':
    comments = get_data()
    convert_and_put(comments)