
.PHONY: install run test lint

install:
	python -m venv .venv && . .venv/bin/activate && pip install -r requirements.txt

run:
	python -m iranseda.cli run --config configs/example.yaml

test:
	pytest -q

lint:
	python -m pyflakes src || true
