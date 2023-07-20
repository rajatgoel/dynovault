export AWS_ACCESS_KEY_ID := "id"
export AWS_SECRET_ACCESS_KEY := "key"

feastle/venv:
	python3.11 -m venv feastle/venv
	. feastle/venv/bin/activate; pip install -r feastle/requirements.txt

feastle/test_repo: feastle/venv
	. feastle/venv/bin/activate; cd feastle; feast init test_repo
	rm feastle/test_repo/feature_repo/feature_store.yaml
	cp feastle/feature_store.yaml feastle/test_repo/feature_repo/

feastle-setup: feastle/test_repo feastle/venv

run-test-workflow: feastle-setup
	. feastle/venv/bin/activate; cd feastle/test_repo/feature_repo; python3.11 test_workflow.py || true

kill-server:
	test -f main.pid && pkill -P `cat main.pid` ||  rm -f main.pid

run-server: kill-server
	go run server/main.go & echo $$! > main.pid

run: run-server run-test-workflow kill-server

clean: kill-server
	rm -rf feastle/venv
	rm -rf feastle/test_repo

run-loadgen: go run cmd/loadgen/main.go

loadgen: run-server run-loadgen kill-server