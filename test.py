from src.box import jwt, BoxClient, BoxTarget,jwt_auth, DEFAULT_CONFIG
import uuid
import pandas as pd

client = BoxClient.from_settings_file(DEFAULT_CONFIG,use_redis=False)

bt = BoxTarget('/EMU/_patient_manifest.csv', file_id=588757437066, auth=client.auth)

print('reading manifest')

with bt.open('r') as f:
    content = pd.read_csv(f)

print('uploading manifest2.csv')

bt2 = BoxTarget('/EMU/_patient_manifest2.csv',auth=client.auth)

# with bt2.temporary_path() as tmp:
#     content.to_csv(tmp,index=False)
