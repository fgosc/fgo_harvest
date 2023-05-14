.PHONY: lock

lock:
	pipenv requirements > harvest/requirements.txt
	pipenv requirements --dev > harvest/requirements-dev.txt
