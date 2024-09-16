FROM python:3.12
RUN pip install poetry
COPY . /src
WORKDIR /src
RUN poetry install --no-interaction --no-ansi

ENTRYPOINT [ "poetry", "run", "python", "etl.py"]