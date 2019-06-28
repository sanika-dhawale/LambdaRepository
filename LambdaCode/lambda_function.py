import boto3
import datetime

def lambda_handler(event, context):

    now = datetime.datetime.now()
    s3 = boto3.resource('s3')
    
    cw = boto3.client('cloudwatch')
    s3client = boto3.client('s3')
    
    # Get a list of all buckets
    allbuckets = s3client.list_buckets()
    
    # Header Line for the output going to standard out
    print('Bucket'.ljust(45) + 'Size'.rjust(25))
    
    id=[]
    id =  boto3.client('sts').get_caller_identity().get('Account')

    my_session = boto3.session.Session()
    my_region = my_session.region_name
    
    current_date=datetime.datetime.now().strftime('%Y-%m-%d') 

    #bucket_name = "sng1-mithiinternal-reports"
    bucket_name = "sanika12"
    file_name = id+"-"+my_region+"-"+"review-S3BucketDetails-"+current_date+".csv"
    lambda_path = "/tmp/" + file_name
    s3_path = "BucketSizeDetails/"+file_name
    
    with open(lambda_path, 'w') as file:
        file.write('Bucket Name '+','+'Bucket Size')
        file.write('\r\n')
    
        # Iterate through each bucket
        for bucket in allbuckets['Buckets']:
            # For each bucket item, look up the cooresponding metrics from CloudWatch
            response = cw.get_metric_statistics(Namespace='AWS/S3',
                                                MetricName='BucketSizeBytes',
                                                Dimensions=[
                                                    {'Name': 'BucketName', 'Value': bucket['Name']},
                                                    {'Name': 'StorageType', 'Value': 'StandardStorage'}
                                                ],
                                                Statistics=['Average'],
                                                Period=3600,
                                                StartTime=(now-datetime.timedelta(days=2)).isoformat(),
                                                EndTime=now.isoformat()
                                                )
            # The cloudwatch metrics will have the single datapoint, so we just report on it. 
            for item in response["Datapoints"]:
                if (item["Average"] < 1000):
                    file.write(bucket["Name"] +' , '+str(item["Average"])+ 'Bytes')
                    file.write('\r\n')
                    print(bucket["Name"].ljust(45) + str("{:,}".format(int(item["Average"]))).rjust(25), 'Bytes')
        
                elif (1000 <= item["Average"] < 1000000):
                    
                    size=('%.1f' %float(item["Average"]/1000))
                    file.write(bucket["Name"] +' , '+str(size)+ 'KB')
                    file.write('\r\n')
                    print(bucket["Name"].ljust(45) + str(size).rjust(25), 'KB')
                        
                elif (1000000 <= item["Average"] < 1000000000):
                    size='%.1f' % float(item["Average"]/1000000)
                    file.write(bucket["Name"] +' , '+str(size)+ 'MB')
                    file.write('\r\n')
                    print(bucket["Name"].ljust(45) + str(size).rjust(25), 'MB')
                    
                elif (1000000000 <= item["Average"] < 1000000000000):
                    size='%.1f' % float(item["Average"]/1000000000)
                    file.write(bucket["Name"] +' , '+str(size)+ 'GB')
                    file.write('\r\n')
                    print(bucket["Name"].ljust(45) + str(size).rjust(25), 'GB')
                        
                elif (1000000000000 <= item["Average"]):
                    size='%.1f' % float(item["Average"]/1000000000000)
                    file.write(bucket["Name"] +' , '+str(size)+ 'TB')
                    file.write('\r\n')
                    print(bucket["Name"].ljust(45) + str(size).rjust(25), 'TB')
                    
                
                
    file.close()    
    
    s3.meta.client.upload_file(lambda_path, bucket_name, s3_path)
            
    return "This code works"
