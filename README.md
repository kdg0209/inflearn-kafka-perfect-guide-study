### 인프런 카프카 완벽 가이드 스터디

<hr>

https://www.inflearn.com/course/%EC%B9%B4%ED%94%84%EC%B9%B4-%EC%99%84%EB%B2%BD%EA%B0%80%EC%9D%B4%EB%93%9C-%EC%BD%94%EC%96%B4/dashboard

<br>

### 환경 구성

<hr>

- 로컬에 도커가 기본적으로 설치되어 있다는 가정하에 진행합니다.

1. ubuntu 20.04 이미지를 다운받습니다.

 ```
 docker pull ubuntu:20.04
 ```
2. 내려받은 이미지를 통해 컨테이너를 생성합니다. 이때 root 권한을 줍니다.

```
docker run -it --name ubuntu-server -p 9092:9092 --privileged=true ubuntu:20.04
```

3. docker container로 접속했다면 JDK 11버전을 설치합니다.

```
1. apt-get update && upgrade
2. apt-get install openjdk-11-jdk
3. java --version
```

4. home 디렉토리로 이동하여 Kafka를 설치받습니다.
```
1. apt-get install wget
2. wget https://packages.confluent.io/archive/7.1/confluent-community-7.1.2.tar.gz
3. tar -xzvf confluent-community-7.1.2.tar.gz
4. rm -rf confluent-community-7.1.2.tar.gz
```

5. 환경 변수를 지정합니다. 참고: 해당 파일의 맨 밑에 export를 추가합니다.
```
1. cd ~
2. vi .bashrc
3. export CONFLUENT_HOME=/home/confluent-7.1.2
4. export PATH=.:$PATH:$CONFLUENT_HOME/bin
5. wq!
6. . .bashrc(수정 내역 적용)
7. echo $CONFLUENT_HOME 테스트 입력 
```

6. 주키퍼 서버 실행 테스트
```
zookeeper-server-start $CONFLUENT_HOME/etc/kafka/zookeeper.properties
```

7. 카프카 서버 실행 테스트
```
kafka-server-start $CONFLUENT_HOME/etc/kafka/server.properties
```

8. 환경 설정
```
1. mkdir data (home 디렉토리에서 data 디렉토리 생성)
2. mkdir zookeeper (data 디렉토리에서 생성)
3. mkdir kafka-logs (data 디렉토리에서 생성)
4. cd $CONFLUENT_HOME/etc/kafka
5. vi server.properties
  log.dirs=/home/data/kafka-logs 기존 로그 디렉토리를 이와 같이 변경
6. vi zookeeper.properties
  dataDir=/home/data/zookeeper 기존 데이터 디렉토리를 이와 같이 변경

다시 주키퍼와 카프카를 실행시켜 로그에서 디렉토리가 잘 변경되었는지 살펴본다.
```

9. 간편한 쉘 스크립트 작성
```
1. vi zoo_start.sh
  $CONFLUENT_HOME/bin/zookeeper-server-start $CONFLUENT_HOME/etc/kafka/zookeeper.properties 입력 후 저장
2. vi kafka_start.sh
  $CONFLUENT_HOME/bin/kafka-server-start $CONFLUENT_HOME/etc/kafka/server.properties 입력 후 저장
3. chmod +x *.sh
```

10. 토픽 만들어지는지 테스트
```
1. kafka-topics --bootstrap-server localhost:9092 --create --topic welcome-topic
Created topic welcome-topic. 뜨면 정상적으러 만들어진것
```

### 참고

- 로컬에서 프로듀서 서버가 카프카에 접속하기 위해서는 server.properties의 dvertised.listeners를 수정해야함

```
#advertised.listeners=PLAINTEXT://your.host.name:9092 
advertised.listeners=PLAINTEXT://localhost:9092 << 이거처럼 수정
```

