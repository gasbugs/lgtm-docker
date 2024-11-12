#!/bin/bash

# 서버 주소와 포트 설정 (필요에 따라 수정)
SERVER="http://ec2-52-91-67-190.compute-1.amazonaws.com:5000"

# 사용 가능한 경로들의 배열
paths=(
    "/health"
    "/"
    "/to_stack"
    "/cpu_task"
    "/random_status"
    "/random_sleep"
    "/error_test"
    "/complex-operation"
)

# 무한 루프
while true; do
    # 배열에서 무작위로 경로 선택
    random_path=${paths[$RANDOM % ${#paths[@]}]}
    
    # 선택된 경로로 curl 요청 보내기
    echo "Sending request to: $SERVER$random_path"
    curl -s "$SERVER$random_path"
    echo  # 새 줄 추가
    
    # 요청 사이에 짧은 대기 시간 추가 (선택사항, 필요에 따라 조정)
    sleep 1
done
