run:
	pkill main || true
	go run server/main.go &
	serverpid="$!"
	cd feastle/feature_repo; AWS_ACCESS_KEY_ID=id AWS_SECRET_ACCESS_KEY=key ../venv/bin/python3.11 test_workflow.py
	kill "$serverpid"
