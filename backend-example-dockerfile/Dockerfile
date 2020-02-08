FROM ubuntu:latest

RUN apt-get update 
RUN apt-get install curl -y

RUN curl -sL https://deb.nodesource.com/setup_10.x | bash - 
RUN apt-get install -y nodejs

COPY * ./

RUN npm install
RUN npm start

EXPOSE 8000
