cd /volume1/docker/projects/nas-transcoder/
sudo docker compose down chonk-service
sudo docker compose build chonk-service
sudo docker compose up -d chonk-service
