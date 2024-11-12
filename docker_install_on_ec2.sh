DOCKER_CONFIG=${DOCKER_CONFIG:-$HOME/.docker}

cat <<EOF > docker_compose_install.sh
yum install docker git -y
service docker restart 
systemctl enable docker --now

mkdir -p $DOCKER_CONFIG/cli-plugins
curl -SL https://github.com/docker/compose/releases/download/v2.30.3/docker-compose-linux-x86_64 -o $DOCKER_CONFIG/cli-plugins/docker-compose
chmod +x $DOCKER_CONFIG/cli-plugins/docker-compose
docker compose version
EOF

bash docker_compose_install.sh
