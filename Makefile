run:
	pkill main || true
	go run server/main.go &
	serverpid="$!"
	cd feastle/feature_repo; ../venv/bin/python3.11 test_workflow.py
	kill "$serverpid"
