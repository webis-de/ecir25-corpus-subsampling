# Create SubCorpora From WARC Files

Here are a set of scripts to 

ray job submit --working-dir . -- AWS_ACCESS_KEY=XYZ AWS_SECRET=XYZ python3 filter-clueweb-web-tracks.py 01-access-files clueweb09
ray job submit --working-dir . -- AWS_ACCESS_KEY=XYZ AWS_SECRET=XYZ python3 filter-clueweb-web-tracks.py 01-access-files clueweb12



ray job submit --working-dir . -- AWS_ACCESS_KEY=XYZ AWS_SECRET=XYZ python3 filter-clueweb-web-tracks.py 02-persist-files clueweb09



md5sum clueweb12-trec-top-100.zip
dfc008d0f46cb85a82d8f7a93963eaf0  clueweb12-trec-top-100.zip

md5sum clueweb09-trec-top-100.zip
c3a1a356a875d00c172c6674cd42020b  clueweb09-trec-top-100.zip
