.PHONY: lock

lock:
	pipenv lock -r > harvest/requirements.txt
	pipenv lock -r --dev > harvest/requirements-dev.txt
