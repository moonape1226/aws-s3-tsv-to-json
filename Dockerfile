FROM python:3.7-alpine

RUN apk add tzdata

ENV TZ Asia/Taipei

COPY . /

WORKDIR /

RUN pip3 install -r requirements.txt

ENTRYPOINT ["python3", "app/converter.py"]
