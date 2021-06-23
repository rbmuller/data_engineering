from data catalog 
database 'spectrumdb' 
iam_role 'arn:aws:iam::account_id_aws:role/mySpectrumRole'
create external database if not exists;

create external schema datalake 
from data catalog 
database 'spectrumdb' 
iam_role 'arn:aws:iam::account_id_aws:role/mySpectrumRole'
create external database if not exists;