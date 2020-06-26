FROM python:3.8
RUN pip3 install pipenv
COPY Pipfile Pipfile.lock /app/
WORKDIR /app
RUN pipenv install --three --deploy --ignore-pipfile
COPY dcollect /app/dcollect
CMD ["pipenv", "run", "python", "-u", "-m", "dcollect"]
