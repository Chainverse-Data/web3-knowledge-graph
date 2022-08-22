import pandas as pd
import s3fs


def write_df_to_s3(df, BUCKET, file_name, resource, s3, ACL="public-read"):
    df.to_csv(f"s3://{BUCKET}/{file_name}", index=False)
    object_acl = resource.ObjectAcl(BUCKET, file_name)
    response = object_acl.put(ACL=ACL)
    location = s3.get_bucket_location(Bucket=BUCKET)["LocationConstraint"]
    url = "https://s3-%s.amazonaws.com/%s/%s" % (location, BUCKET, file_name)
    return url


def read_df_from_s3(BUCKET, file_name):
    df = pd.read_csv(f"s3://{BUCKET}/{file_name}", lineterminator='\n')
    return df


def set_object_private(BUCKET, file_name, resource):
    object_acl = resource.ObjectAcl(BUCKET, file_name)
    response = object_acl.put(ACL="private")


def split_dataframe(df, chunk_size=10000):
    chunks = list()
    num_chunks = len(df) // chunk_size + (1 if len(df) % chunk_size else 0)
    for i in range(num_chunks):
        chunks.append(df[i * chunk_size : (i + 1) * chunk_size])
    return chunks
