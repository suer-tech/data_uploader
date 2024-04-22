FROM ubuntu:latest
LABEL authors="suer"

ENTRYPOINT ["top", "-b"]