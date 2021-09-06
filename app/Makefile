all: test build deploy

test:
	pytest tests

build:
	rm -rf ./dist && mkdir ./dist
	cp ./main.py ./dist
	cp ./config.json ./dist
	zip -r dist/jobs.zip jobs
	zip -r dist/shared.zip shared
#	docker run --rm -v $(PWD):/tmp -w /tmp python:2 \
#	pip install -r requirements.txt -t ./dist/libs
#	cd ./dist/libs && zip -r -D ../libs.zip .
#	rm -rf ./dist/libs

deploy:
	ssh root@sandbox-hdp.hortonworks.com -p 2222 -f \
    'rm -rf /usr/tmp/hw_app && mkdir /usr/tmp/hw_app'
	scp -P 2222 -rp dist/* root@sandbox-hdp.hortonworks.com:/usr/tmp/hw_app
	ssh root@sandbox-hdp.hortonworks.com -p 2222 -f \
	'cd /usr/tmp/hw_app && spark-submit --py-files jobs.zip,shared.zip --files config.json main.py --job $(HW_JOB)'
#	'cd /usr/tmp/hw_app && spark-submit --py-files jobs.zip,shared.zip,libs.zip --files config.json main.py --job $(HW_JOB)'
