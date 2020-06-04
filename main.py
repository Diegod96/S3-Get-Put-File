
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


def convert_data(comments):
    """
    Convert list of strings to one text blob for easier sentiment analysis
    Format text blob of newline characters
    Write text blob to a text file to send to S3 bucket & AWS Comprehend
    """
    print("Creating text blob...")
    text_blob = "".join(str(comment) for comment in comments)
    text_blob = text_blob.replace("\n", "")
    # result = re.sub(r'[^a-zA-Z]', "", text_blob)
    print("Text blob created!")

    print("Creating text file...")
    with open('blob.txt', 'w', encoding='utf-8') as f_out:
        f_out.write(text_blob)
    print("Text file created!")


def put_in_bucket():
    """
    Put the blob.txt file in my S3 bucket
    """
    data = open('blob.txt', 'rb')
    s3 = boto3.resource('s3')
    s3.Bucket('lambda-comprehend-sent-diego').put_object(Key='blob.txt', Body=data)
    print("blob.txt put into bucket!")



def lambda_handler(x, y):
    comments = get_data()
    convert_data(comments)
    put_in_bucket()
