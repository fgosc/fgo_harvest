.PHONY: test deploy

test:
	pytest -v
	find -type f -name "*.py" | xargs mypy
	find -type f -name "*.py" | flake8

deploy:
	chalice deploy --no-autogen-policy
