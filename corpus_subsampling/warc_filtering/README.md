# Create SubCorpora From WARC Files

Here are a set of scripts to 

ray job submit --working-dir . -- AWS_ACCESS_KEY=XYZ AWS_SECRET=XYZ python3 filter-clueweb-web-tracks.py 01-access-files clueweb09
ray job submit --working-dir . -- AWS_ACCESS_KEY=XYZ AWS_SECRET=XYZ python3 filter-clueweb-web-tracks.py 01-access-files clueweb12



ray job submit --working-dir . -- AWS_ACCESS_KEY=XYZ AWS_SECRET=XYZ python3 filter-clueweb-web-tracks.py 02-persist-files clueweb09
