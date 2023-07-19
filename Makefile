export AWS_ACCESS_KEY_ID := "id"
export AWS_SECRET_ACCESS_KEY := "key"

feastle/venv:
	python3.11 -m venv feastle/venv
	. feastle/venv/bin/activate; pip install -r feastle/requirements.txt

kill-server:
	test -f main.pid && pkill -P `cat main.pid` ||  rm -f main.pid

run-server: kill-server
	go run server/main.go & echo $$! > main.pid

run: feastle/venv run-server
	. feastle/venv/bin/activate; cd feastle/feature_repo; python3.11 test_workflow.py || true
	pkill -P `cat main.pid`
	rm main.pid

clean: kill-server
	rm -rf feastle/venv
	rm -f feastle/feature_repo/data/registry.db
