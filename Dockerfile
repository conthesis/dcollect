FROM tiangolo/uvicorn-gunicorn-fastapi:python3.8

COPY Pipfile Pipfile.lock /app/
WORKDIR /app
RUN pip install pipenv
RUN pipenv install --system --deploy
COPY tests dcollect /app/
CMD ["uvicorn", "dcollect.main:app"]
