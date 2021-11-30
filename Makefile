.phony: install uninstall test clean lint

install:
	@echo 'Installing'
	@pip3 install ./

uninstall:
	@echo 'Uninstalling'
	pip3 uninstall rpncalc

test:
	@echo 'Testing'
	python3 -m unittest discover -v

clean:
	@echo 'Clean'
	@echo 'Cleaning __pycache__'
	find . -regex '^.*\(__pycache__\|\.py[co]\)' -delete
	@if [ -d "build" ]; then \
		echo 'Removing build'; \
		rm -r build; \
	fi
	@if [ -d "rpncalc.egg-info" ]; then \
		echo 'Removing rpncalc.egg-info'; \
		rm -r rpncalc.egg-info; \
	fi

lint:
	@echo "Lint"
	flake8
