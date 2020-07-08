.PHONY: test

test:
	pytest -v
	find -type f -name "*.py" | xargs mypy
